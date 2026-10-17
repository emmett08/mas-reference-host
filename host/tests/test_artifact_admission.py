"""Deployment admission gates new artefacts while retained evidence stays usable."""
from __future__ import annotations

from collections import namedtuple
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host import artifacts
from mas_host.artifacts import ArtifactError, ArtifactStore
from mas_host.operations import DeploymentLedger, DeploymentPolicy


class ArtifactAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.ledger = DeploymentLedger(self.state, DeploymentPolicy(
            min_free_bytes=1, max_state_bytes=1_000_000))
        self.store = ArtifactStore(self.state / "run-one" / "artifacts",
                                   namespace="evidence", principal="writer")
        self.reference = self.store.put_bytes(b"existing")
        self.source = self.root / "source"

    @staticmethod
    def snapshot(root):
        return {str(path.relative_to(root)): path.read_bytes()
                for path in root.rglob("*") if path.is_file()}

    def thresholds(self):
        usage = namedtuple("Usage", "total used free")
        return {
            "free_bytes": patch("mas_host.operations.shutil.disk_usage",
                                return_value=usage(100, 100, 0)),
            "state_bytes": patch("mas_host.operations.DeploymentLedger._current_state_size",
                                 return_value=self.ledger.policy.max_state_bytes),
        }

    def test_closed_threshold_rejects_fresh_content_and_metadata_before_writes(self):
        self.source.write_bytes(b"new file")
        for name, threshold in self.thresholds().items():
            with self.subTest(threshold=name), threshold:
                before = self.snapshot(self.state)
                for publish in (
                    lambda: self.store.put_bytes(b"new bytes"),
                    lambda: self.store.put_file(self.source),
                    lambda: self.store.put_bytes(b"existing", media_type="text/plain"),
                    lambda: self.store.put_bytes(b"existing", readers=("reader",)),
                ):
                    with self.assertRaisesRegex(ArtifactError, "publication admission closed"):
                        publish()
                    self.assertEqual(self.snapshot(self.state), before)

    def test_closed_threshold_preserves_duplicate_publication_and_retention_release(self):
        self.source.write_bytes(b"existing")
        for name, threshold in self.thresholds().items():
            self.store.link("consumer", [self.reference], state="live")
            with self.subTest(threshold=name), threshold:
                reopened = ArtifactStore(self.store.root, namespace="evidence", principal="writer")
                before = self.snapshot(self.state)
                self.assertEqual(reopened.put_bytes(b"existing"), self.reference)
                self.assertEqual(reopened.put_file(self.source), self.reference)
                self.assertEqual(self.snapshot(self.state), before)
                self.assertEqual(reopened.read_bytes(self.reference), b"existing")
                released = reopened.link("consumer", [self.reference], state="released")
                self.assertEqual(released["state"], "released")

    def test_closed_threshold_rejects_new_namespace_without_allocating_directories(self):
        for name, threshold in self.thresholds().items():
            with self.subTest(threshold=name), threshold:
                before = self.snapshot(self.state)
                for root, namespace in ((self.store.root, "other"),
                                        (self.state / "new-run" / "artifacts", "evidence")):
                    with self.assertRaisesRegex(ArtifactError, "publication admission closed"):
                        ArtifactStore(root, namespace=namespace, principal="writer")
                    self.assertFalse((root / namespace).exists())
                    self.assertEqual(self.snapshot(self.state), before)
                self.assertFalse((self.state / "new-run").exists())

    def test_standalone_store_keeps_its_own_namespace_admission(self):
        for name, threshold in self.thresholds().items():
            with self.subTest(threshold=name), threshold:
                store = ArtifactStore(self.root / ("standalone-" + name),
                                      namespace="evidence", principal="writer")
                reference = store.put_bytes(b"standalone")
                self.assertEqual(store.read_bytes(reference), b"standalone")

    def test_file_changed_between_fingerprint_and_copy_never_publishes(self):
        self.source.write_bytes(b"original")
        before = self.snapshot(self.store.directory)
        original = artifacts._hash_source
        scans = []

        def change_after_scan(source, limit, **kwargs):
            result = original(source, limit, **kwargs)
            scans.append(result)
            if len(scans) == 1:
                self.source.write_bytes(b"changed")
            return result

        with (patch.object(artifacts, "_hash_source", side_effect=change_after_scan),
              patch('mas_host.artifact_content._hash_source', side_effect=change_after_scan)):
            with self.assertRaisesRegex(ArtifactError, "source changed"):
                self.store.put_file(self.source)
        self.assertEqual(len(scans), 2)
        self.assertEqual(self.snapshot(self.store.directory), before)


if __name__ == "__main__":
    unittest.main()
