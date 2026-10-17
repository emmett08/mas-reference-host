"""Integrity failures must reject optional image packages before installation."""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "release-wolfi.py"
SPEC = importlib.util.spec_from_file_location("release_wolfi", SOURCE)
WOLFI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WOLFI)


class WolfiLockTests(unittest.TestCase):
    def lock(self):
        return {"format": "mas-wolfi-lock/1", "runtime": {"x86_64": [
            {"name": name, "filename": name + "-1-r0.apk", "version": "1-r0",
             "url": WOLFI.ORIGIN + "x86_64/" + name + "-1-r0.apk",
             "sha256": hashlib.sha256(b"verified").hexdigest()}
            for name in ("python-3.14", "libstdc++")]}}

    def test_wrong_architecture_url_and_path_escape_are_rejected(self):
        for field, value in (("url", WOLFI.ORIGIN + "aarch64/python-3.14-1-r0.apk"),
                             ("filename", "../outside.apk")):
            lock = self.lock()
            lock["runtime"]["x86_64"][0][field] = value
            with self.assertRaisesRegex(ValueError, "identity"):
                WOLFI.packages_for(lock, "x86_64")

    def test_missing_root_and_duplicate_package_are_rejected(self):
        for packages in ([self.lock()["runtime"]["x86_64"][0]],
                         self.lock()["runtime"]["x86_64"] * 2):
            lock = self.lock()
            lock["runtime"]["x86_64"] = packages
            with self.assertRaises(ValueError):
                WOLFI.packages_for(lock, "x86_64")

    def test_changed_package_is_rejected_before_write(self):
        package = self.lock()["runtime"]["x86_64"][0]
        with tempfile.TemporaryDirectory() as temporary, patch.object(WOLFI, "download", return_value=b"changed"):
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                WOLFI.fetch_package(package, Path(temporary))
            self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_verified_package_is_written(self):
        package = self.lock()["runtime"]["x86_64"][0]
        with tempfile.TemporaryDirectory() as temporary, patch.object(WOLFI, "download", return_value=b"verified"):
            WOLFI.fetch_package(package, Path(temporary))
            self.assertEqual((Path(temporary) / package["filename"]).read_bytes(), b"verified")

    def test_changed_signed_index_is_rejected_before_staging(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            relative = "docker/wolfi-indexes/x86_64/APKINDEX.tar.gz"
            source = root / relative
            source.parent.mkdir(parents=True)
            source.write_bytes(b"changed index")
            lock = {"indexes": {"x86_64": {"path": relative,
                    "sha256": hashlib.sha256(b"trusted index").hexdigest()}}}
            destination = root / "staging"
            with self.assertRaisesRegex(ValueError, "snapshot hash"):
                WOLFI.stage_index(lock, "x86_64", root, destination)
            self.assertFalse(destination.exists())


if __name__ == "__main__":
    unittest.main()
