"""Regressions for replacing Cursor's outdated native runtime safely."""
from __future__ import annotations

import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / "install-cursor-runtime.py"
SPEC = importlib.util.spec_from_file_location("install_cursor_node", SOURCE)
NODE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NODE)


class CursorNodeInstallerTests(unittest.TestCase):
    def fixture(self, symlink=False):
        files = {"node": b"verified executable", "LICENSE": b"licence fixture"}
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:xz") as archive:
            for name, relative in (("node", "bin/node"), ("LICENSE", "LICENSE")):
                member = tarfile.TarInfo("node-fixture/" + relative)
                member.size = len(files[name])
                if name == "node" and symlink:
                    member.type = tarfile.SYMTYPE
                    member.linkname = "/outside"
                    member.size = 0
                archive.addfile(member, io.BytesIO(files[name]) if member.isfile() else None)
        data = stream.getvalue()
        entry = {"archive_root": "node-fixture", "archive_sha256": NODE.digest(data),
                 "node_sha256": NODE.digest(files["node"]), "license_sha256": NODE.digest(files["LICENSE"])}
        return data, entry, files

    def test_changed_download_rejected_before_archive_parsing(self):
        data, entry, _ = self.fixture()
        with self.assertRaisesRegex(ValueError, "archive hash"):
            NODE.archive_files(data + b"changed", entry)

    def test_archive_symlink_cannot_replace_runtime(self):
        data, entry, _ = self.fixture(symlink=True)
        with self.assertRaisesRegex(ValueError, "invalid executable"):
            NODE.archive_files(data, entry)

    def test_archive_and_extracted_executable_require_independent_hashes(self):
        data, entry, _ = self.fixture()
        entry["node_sha256"] = hashlib.sha256(b"another executable").hexdigest()
        with self.assertRaisesRegex(ValueError, "node hash"):
            NODE.archive_files(data, entry)

    def test_changed_offline_source_rejected(self):
        _, entry, files = self.fixture()
        files["LICENSE"] += b"changed"
        with self.assertRaisesRegex(ValueError, "LICENSE hash"):
            NODE.verify_files(files, entry)

    def test_verified_archive_extracts_only_executable_and_licence(self):
        data, entry, files = self.fixture()
        self.assertEqual(NODE.archive_files(data, entry), files)

    def test_atomic_install_replaces_symlink_without_modifying_its_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            outside = directory / "outside"
            outside.write_bytes(b"unchanged")
            target = directory / "node"
            target.symlink_to(outside)
            NODE.write_file(target, b"verified executable", 0o755)
            self.assertFalse(target.is_symlink())
            self.assertEqual(target.read_bytes(), b"verified executable")
            self.assertEqual(outside.read_bytes(), b"unchanged")
            self.assertEqual(target.stat().st_mode & 0o777, 0o755)


if __name__ == "__main__":
    unittest.main()
