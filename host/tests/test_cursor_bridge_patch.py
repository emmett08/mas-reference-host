"""Regression tests for the optional bridge's native Headers source repair."""
from __future__ import annotations

import csv
import hashlib
import importlib.metadata
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from mas_host import cursor_bridge_patch as repair


class BridgeRepairTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.site = Path(self.temporary.name)
        self.root = self.site / "cursor_sdk" / "_vendor" / "bridge"
        self.root.mkdir(parents=True)
        self.manifest, _ = repair._manifest()
        # Tiny dependency trees exercise the destructive-operation guard without
        # vendoring the obsolete packages into this repository's test fixtures.
        for index, (name, item) in enumerate(self.manifest["files"].items()):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            old = ("reviewed preimage " + str(index)).encode()
            path.write_bytes(old)
            item["before_sha256"] = hashlib.sha256(old).hexdigest()
        for name, item in self.manifest["remove"].items():
            directory = self.root / name
            directory.mkdir(parents=True)
            (directory / "package.json").write_text('{"version":"old"}')
            (directory / "index.js").write_text("legacy source")
            item["tree_sha256"] = repair._tree_digest(directory)
        self.manifest_patch = patch.object(repair, "_manifest", return_value=(self.manifest, "fixture"))
        self.manifest_patch.start()
        self.addCleanup(self.manifest_patch.stop)

    def snapshot(self):
        return {str(path.relative_to(self.root)): path.read_bytes()
                for path in self.root.rglob("*") if path.is_file()}

    def test_complete_repair_removes_real_dependencies_and_is_idempotent(self):
        with self.assertRaises(repair.BridgePatchError):
            repair.validate_bridge_patch(self.root)
        repair.apply_bridge_patch(self.root)
        repair.validate_bridge_patch(self.root)
        before = self.snapshot()
        repair.apply_bridge_patch(self.root)
        self.assertEqual(self.snapshot(), before)
        for name in self.manifest["remove"]:
            self.assertFalse((self.root / name).exists())
        metadata = json.loads((self.root / repair._BASE / "package.json").read_text())
        self.assertNotIn("dependencies", metadata)
        self.assertEqual(metadata["masRuntimePatch"], "native-headers/1")
        for name in self.manifest["files"]:
            if name.endswith(".js"):
                source = (self.root / name).read_text()
                self.assertIn("Licensed under the Apache License", source)
                self.assertNotIn('"undici"', source)

    def test_unknown_source_rejects_before_any_mutation(self):
        name = next(iter(self.manifest["files"]))
        (self.root / name).write_text("unexpected upstream change")
        before = self.snapshot()
        with self.assertRaisesRegex(repair.BridgePatchError, "reviewed release"):
            repair.apply_bridge_patch(self.root)
        self.assertEqual(self.snapshot(), before)

    def test_unknown_dependency_rejects_before_any_mutation(self):
        name = next(iter(self.manifest["remove"]))
        (self.root / name / "unexpected.js").write_text("unreviewed code")
        before = self.snapshot()
        with self.assertRaisesRegex(repair.BridgePatchError, "reinstall cursor-sdk"):
            repair.apply_bridge_patch(self.root)
        self.assertEqual(self.snapshot(), before)

    def test_partial_deletion_requires_reinstall_and_remains_unusable(self):
        name = next(iter(self.manifest["remove"]))
        (self.root / name / "index.js").unlink()
        with self.assertRaisesRegex(repair.BridgePatchError, "reinstall cursor-sdk"):
            repair.apply_bridge_patch(self.root)
        with self.assertRaises(repair.BridgePatchError):
            repair.validate_bridge_patch(self.root)

    def test_known_atomic_file_replacement_can_be_resumed(self):
        name, item = next(iter(self.manifest["files"].items()))
        (self.root / name).write_text(item["after"])
        repair.apply_bridge_patch(self.root)
        repair.validate_bridge_patch(self.root)

    def test_symlink_is_rejected_without_touching_target(self):
        name = next(iter(self.manifest["files"]))
        target = self.site / "unrelated-file"
        target.write_text("preserve")
        (self.root / name).unlink()
        (self.root / name).symlink_to(target)
        with self.assertRaisesRegex(repair.BridgePatchError, "symbolic links"):
            repair.apply_bridge_patch(self.root)
        self.assertEqual(target.read_text(), "preserve")

    def test_postimage_tampering_and_reintroduced_dependencies_fail_preflight(self):
        repair.apply_bridge_patch(self.root)
        name, item = next(iter(self.manifest["files"].items()))
        (self.root / name).write_text("tampered")
        with self.assertRaises(repair.BridgePatchError):
            repair.validate_bridge_patch(self.root)
        (self.root / name).write_text(item["after"])
        (self.root / next(iter(self.manifest["remove"]))).mkdir()
        with self.assertRaises(repair.BridgePatchError):
            repair.validate_bridge_patch(self.root)

    def test_record_reflects_changes_and_does_not_read_unrelated_paths(self):
        repair.apply_bridge_patch(self.root)
        (self.root / "bin").mkdir()
        (self.root / "bin" / "node").write_bytes(b"verified executable")
        (self.root / "NODE_LICENSE").write_text("licence")
        record = self.site / "cursor_sdk-1.0.37.dist-info" / "RECORD"
        record.parent.mkdir()
        record.write_text("../../../unavailable,original-hash,1\n"
                          "cursor_sdk/_vendor/bridge/node_modules/undici/index.js,obsolete,1\n"
                          "cursor_sdk/_vendor/bridge/bin/node,old,1\n"
                          "cursor_sdk-1.0.37.dist-info/RECORD,,\n")
        repair.refresh_sdk_record(self.root)
        rows = list(csv.reader(io.StringIO(record.read_text())))
        self.assertIn(["../../../unavailable", "original-hash", "1"], rows)
        self.assertFalse(any("node_modules/undici/" in row[0] for row in rows))
        node = next(row for row in rows if row[0].endswith("/bin/node"))
        self.assertTrue(node[1].startswith("sha256="))
        self.assertEqual(node[2], str(len(b"verified executable")))
        before = record.read_bytes()
        repair.refresh_sdk_record(self.root)
        self.assertEqual(record.read_bytes(), before)


def sdk_available():
    try:
        return importlib.metadata.version("cursor-sdk") == "1.0.37"
    except importlib.metadata.PackageNotFoundError:
        return False


@unittest.skipUnless(sys.platform.startswith("linux") and sdk_available(), "optional pinned Cursor SDK")
class NativeHeadersTests(unittest.TestCase):
    def setUp(self):
        import cursor_sdk
        self.root = Path(cursor_sdk.__file__).resolve().parent / "_vendor" / "bridge"
        repair.validate_bridge_patch(self.root)
        self.node = self.root / "bin" / "node"
        from mas_host.cursor_local import _validate_node
        _validate_node(self.node)

    def test_cjs_and_esm_transports_use_native_headers_without_legacy_packages(self):
        cjs = str(self.root / repair._BASE / "dist/cjs/index.js")
        esm = (self.root / repair._BASE / "dist/esm/index.js").as_uri()
        source = f"""
import assert from 'node:assert/strict';
import {{createRequire}} from 'node:module';
const require = createRequire(import.meta.url);
const native = globalThis.Headers;
const cjs = require({json.dumps(cjs)});
const esm = await import({json.dumps(esm)});
assert.equal(globalThis.Headers, native);
for (const module of [cjs, esm]) {{
  const transport = module.createConnectTransport({{baseUrl:'http://127.0.0.1:9',httpVersion:'2'}});
  assert.equal(typeof transport.unary, 'function');
  assert.equal(typeof transport.stream, 'function');
}}
const headers = new Headers([['x-mas','ok']]);
assert.equal(headers.get('X-MAS'), 'ok');
"""
        subprocess.run([str(self.node), "--input-type=module", "-e", source],
                       check=True, capture_output=True, text=True, timeout=15)

    def test_cjs_and_esm_fail_without_native_headers(self):
        for module in ("cjs", "esm"):
            path = self.root / repair._BASE / f"dist/{module}/node-headers-polyfill.js"
            load = (f"require({json.dumps(str(path))})" if module == "cjs"
                    else f"await import({json.dumps(path.as_uri())})")
            source = "globalThis.Headers=undefined;" + load
            args = [str(self.node)] + (["--input-type=module"] if module == "esm" else [])
            result = subprocess.run([*args, "-e", source], capture_output=True, text=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires native Node Headers", result.stderr)


if __name__ == "__main__":
    unittest.main()
