"""Keep each distributed variant bound to its tested and scanned OCI image."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from test_release_oci import archive_fixture, oci


SCRIPT = Path(__file__).resolve().parents[1] / "release-manifest.py"


class ReleaseManifestTests(unittest.TestCase):
    def fixture(self, root, variant="standard", architecture="amd64"):
        archive = root / f"mas-{variant}-{architecture}.oci.tar"
        image = archive_fixture(archive, architecture=architecture)
        binding = oci.verify_import(archive, oci.inspect_archive(archive, architecture), image)
        binding = oci.materialise_layout(archive, binding, root / "release-oci-layout")
        (root / "release-image-binding.json").write_text(json.dumps(binding))
        for name in ("release-audit.json", "release-sbom.cdx.json", "release-image-identity.json",
                     "release-import-tool.txt", "release-container-tests.log", "Dockerfile",
                     "Dockerfile.cursor", "docker/alpine-packages.json", "docker/wolfi-packages.json",
                     "docker/wolfi-indexes/x86_64/APKINDEX.tar.gz",
                     "docker/wolfi-indexes/aarch64/APKINDEX.tar.gz", ".protocol/rust-protocol/Cargo.lock", "protocol-revision.txt",
                     "host/pyproject.toml", "host/requirements-openai.txt", "host/requirements-cursor.txt",
                     "host/mas_host/cursor_runtime.json", "host/mas_host/cursor_bridge_patch.json"):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture " + name)
        (root / ".protocol-revision").write_text("b" * 40 + "\n")
        return archive

    def run_manifest(self, root, variant="standard", architecture="amd64"):
        return subprocess.run([sys.executable, str(SCRIPT)], cwd=root, capture_output=True, text=True,
                              env={**os.environ, "SOURCE_SHA": "a" * 40, "RELEASE_TAG": "v0.1.4",
                                   "RELEASE_VARIANT": variant, "RELEASE_PLATFORM": "linux/" + architecture})

    def test_four_release_profiles_bind_correct_image_inputs_and_checksums(self):
        for variant in ("standard", "cursor"):
            for architecture in ("amd64", "arm64"):
                with self.subTest(variant=variant, architecture=architecture), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    archive = self.fixture(root, variant, architecture)
                    result = self.run_manifest(root, variant, architecture)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    manifest = json.loads((root / "release-manifest.json").read_text())
                    self.assertEqual(manifest["protocol_source"]["commit"], "b" * 40)
                    self.assertEqual(manifest["source_commit"], "a" * 40)
                    self.assertEqual(manifest["variant"], variant)
                    self.assertEqual(manifest["platform"], "linux/" + architecture)
                    self.assertEqual(manifest["tested_image_binding"]["archive"], archive.name)
                    self.assertEqual(manifest["dockerfile"], "Dockerfile.cursor" if variant == "cursor" else "Dockerfile")
                    self.assertEqual("docker/wolfi-packages.json" in manifest["inputs"], variant == "cursor")
                    for line in (root / "SHA256SUMS").read_text().splitlines():
                        expected, name = line.split("  ", 1)
                        self.assertEqual(hashlib.sha256((root / name).read_bytes()).hexdigest(), expected)

    def test_wrong_variant_or_extra_archive_cannot_be_distributed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = self.fixture(root)
            self.assertNotEqual(self.run_manifest(root, "cursor").returncode, 0)
            (root / "mas-cursor-amd64.oci.tar").write_bytes(archive.read_bytes())
            self.assertNotEqual(self.run_manifest(root).returncode, 0)
            self.assertFalse((root / "release-manifest.json").exists())

    def test_changed_scanned_layout_prevents_manifest_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root, "cursor", "arm64")
            (root / "release-oci-layout/index.json").write_text("changed after scan")
            result = self.run_manifest(root, "cursor", "arm64")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("scanned OCI layout", result.stderr)
            self.assertFalse((root / "release-manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
