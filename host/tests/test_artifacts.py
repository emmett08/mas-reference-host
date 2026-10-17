"""Real-file integrity, scope, crash, quota and deployment lifecycle checks."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host.artifacts import ArtifactError, ArtifactPolicy, ArtifactRef, ArtifactStore, has_retained_artifacts
from mas_host.operations import OperationsError, backup_state, prune_runs, restore_state
import test_operations


def publish_worker(args):
    path, number, limit = args
    try:
        store = ArtifactStore(Path(path), namespace="run", principal="worker",
                              policy=ArtifactPolicy(max_artifact_bytes=4, max_total_bytes=limit, max_artifacts=100))
        return store.put_bytes(str(number).encode()).to_dict()
    except ArtifactError:
        return None


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = ArtifactStore(self.root / "artifacts", namespace="run", principal="writer")

    def reader(self, principal, namespace="run", **kwargs):
        return ArtifactStore(self.root / "artifacts", namespace=namespace, principal=principal, **kwargs)

    def test_reference_provenance_grants_restart_and_no_url_fetch(self):
        digest = hashlib.sha256(b"input").hexdigest()
        ref = self.store.put_bytes(b"result", media_type="text/plain", input_sha256s=(digest,), readers=("reader",))
        self.assertEqual(ArtifactRef.from_dict(ref.to_dict()), ref)
        self.assertLess(len(json.dumps(ref.to_dict())), 65536)
        reader = self.reader("reader")
        self.assertEqual(reader.read_bytes(ref), b"result")
        self.assertTrue(reader.verify(ref.to_dict()))
        with reader.open(ref) as stream:
            self.assertEqual(stream.read(), b"result")
        self.assertEqual(ref.producer, "writer")
        self.assertEqual(reader.inspect(ref)["reference"]["input_sha256s"], [digest])
        self.assertEqual(self.store.put_bytes(b"result", media_type="text/plain", input_sha256s=(digest,), readers=("reader",)), ref)
        for bad in ({**ref.to_dict(), "url": "https://example.invalid/a"}, {**ref.to_dict(), "format": "mas-artifact/2"}):
            with self.assertRaises(ArtifactError):
                reader.read_bytes(bad)

    def test_existing_manifest_fingerprint_is_preserved_on_publication_and_reopen(self):
        producer = ArtifactStore(self.root / "artifacts", namespace="evidence-v1", principal="producer/worker")
        ref = producer.put_bytes("é雪".encode(), media_type="text/plain", input_sha256s=("0" * 64,),
                                 readers=("review/checker", "owner"))
        self.assertEqual(ref.artifact_id, "8c9b0919995782954916e6661c6f2a58dd49c5d7026496db369f37607a777203")
        reader = ArtifactStore(self.root / "artifacts", namespace="evidence-v1", principal="review/checker",
                               readonly=True)
        self.assertEqual(reader.read_bytes(ref), "é雪".encode())

    def test_excessively_nested_metadata_has_fixed_artifact_and_retention_errors(self):
        from mas_host.artifacts import _load_json

        # Parser recursion behaviour differs across supported Python versions.
        # Exercise its translation independently of a particular depth limit.
        with patch("mas_host.artifacts.json.loads", side_effect=RecursionError("parser detail")):
            with self.assertRaises(ArtifactError) as parser_error:
                _load_json(b"{}")
        self.assertEqual(str(parser_error.exception), "invalid artefact metadata")

        index = self.store.directory / "index.json"
        nested = b"[" * 20000 + b"0" + b"]" * 20000
        index.write_bytes(nested)
        with self.assertRaises(ArtifactError) as store_error:
            self.store.status()
        self.assertIn(str(store_error.exception), {
            "invalid artefact metadata", "invalid artefact store metadata"})
        with self.assertRaises(OperationsError) as retention_error:
            has_retained_artifacts(self.root)
        self.assertEqual(str(retention_error.exception), "artefact retention state cannot be verified")
        self.assertEqual(index.read_bytes(), nested)

    def test_principals_and_namespaces_are_isolated_even_with_known_reference(self):
        ref = self.store.put_bytes(b"private")
        denied = self.reader("intruder")
        with self.assertRaisesRegex(ArtifactError, "denied"):
            denied.read_bytes(ref)
        with self.assertRaises(ArtifactError):
            denied.inspect(ref)
        self.assertEqual(denied.status()["artifacts"], [])
        with self.assertRaises(ArtifactError):
            self.reader("writer", "another-run").read_bytes(ref)
        # Grants are bound into identity; publishing the same bytes with new
        # grants does not retrospectively broaden an earlier reference.
        shared = self.store.put_bytes(b"private", readers=("intruder",))
        self.assertNotEqual(shared.artifact_id, ref.artifact_id)
        self.assertEqual(denied.read_bytes(shared), b"private")
        with self.assertRaises(ArtifactError):
            denied.read_bytes(ref)

    def test_composed_actor_identities_are_preserved_in_grants_and_retention(self):
        producer = self.reader("coordinate/worker")
        ref = producer.put_bytes(b"checked result", readers=("review/checker",))
        self.assertEqual(ref.producer, "coordinate/worker")
        self.assertEqual(ArtifactRef.from_dict(ref.to_dict()), ref)
        reader = self.reader("review/checker")
        self.assertEqual(reader.read_bytes(ref.to_dict()), b"checked result")
        self.assertEqual(reader.inspect(ref)["readers"], ["review/checker"])
        reader.link("review-task", [ref], state="unresolved")
        self.assertTrue(has_retained_artifacts(self.root))
        self.assertEqual(reader.status()["links"]["review-task"]["owner"], "review/checker")
        restarted = self.reader("review/checker", readonly=True)
        self.assertEqual(restarted.read_bytes(ref), b"checked result")
        self.assertEqual(restarted.status()["links"]["review-task"]["owner"], "review/checker")
        with self.assertRaises(ArtifactError):
            self.reader("checker").read_bytes(ref)
        # Only opaque principals accept separators. Storage scopes and link
        # identities retain the original path-safe component contract.
        with self.assertRaises(ArtifactError):
            self.reader("review/checker", namespace="review/checker")
        with self.assertRaises(ArtifactError):
            reader.link("review/task", [ref])
        reader.link("review-task", [ref], state="released")
        self.assertFalse(has_retained_artifacts(self.root))

    def test_content_corruption_detected_before_read_or_idempotent_success(self):
        ref = self.store.put_bytes(b"original")
        blob = self.store.directory / (ref.sha256 + ".blob")
        blob.write_bytes(b"tampered")
        for method in (self.store.read_bytes, self.store.verify):
            with self.assertRaisesRegex(ArtifactError, "integrity"):
                method(ref)
        with self.assertRaisesRegex(ArtifactError, "integrity"):
            self.store.put_bytes(b"original")

    def test_reference_and_manifest_tampering_fail_closed(self):
        ref = self.store.put_bytes(b"original")
        with self.assertRaises(ArtifactError):
            self.store.read_bytes(replace(ref, media_type="text/plain"))
        index_path = self.store.directory / "index.json"
        index = json.loads(index_path.read_bytes())
        index["artifacts"][ref.artifact_id]["readers"] = ["intruder"]
        index_path.write_text(json.dumps(index))
        with self.assertRaisesRegex(ArtifactError, "integrity"):
            self.reader("intruder")

    def test_unknown_store_version_missing_index_and_frozen_policy(self):
        with self.assertRaisesRegex(ArtifactError, "immutable"):
            self.reader("writer", policy=ArtifactPolicy(max_total_bytes=128 * 1024 * 1024))
        ref = self.store.put_bytes(b"a")
        index_path = self.store.directory / "index.json"
        saved = index_path.read_bytes()
        index = json.loads(saved)
        index["format"] = "mas-artifact-store/2"
        index_path.write_text(json.dumps(index))
        with self.assertRaises(ArtifactError):
            self.store.read_bytes(ref)
        index_path.unlink()
        with self.assertRaisesRegex(ArtifactError, "missing"):
            self.reader("writer")
        index_path.write_bytes(saved)
        self.assertEqual(self.store.read_bytes(ref), b"a")

    def test_path_traversal_symlinks_hardlinks_special_files(self):
        for namespace in ("..", "../other", "/tmp", "a/b"):
            with self.assertRaises(ArtifactError):
                self.reader("writer", namespace)
        external = self.root / "external"
        external.write_bytes(b"secret")
        shortcut = self.root / "shortcut"
        shortcut.symlink_to(external)
        with self.assertRaises(ArtifactError):
            self.store.put_file(shortcut)
        linked = self.root / "hardlink"
        os.link(external, linked)
        with self.assertRaises(ArtifactError):
            self.store.put_file(linked)
        fifo = self.root / "pipe"
        os.mkfifo(fifo)
        with self.assertRaises(ArtifactError):
            self.store.put_file(fifo)
        dir_link = self.root / "dir-link"
        dir_link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ArtifactError):
            self.store.put_file(dir_link / "external")
        with self.assertRaises(ArtifactError):
            ArtifactStore(dir_link / "new", namespace="run", principal="writer")
        ref = self.store.put_bytes(b"safe")
        blob = self.store.directory / (ref.sha256 + ".blob")
        blob.unlink()
        blob.symlink_to(external)
        with self.assertRaises(ArtifactError):
            self.store.read_bytes(ref)
        self.assertEqual(external.read_bytes(), b"secret")

    def test_file_publication_bounds_and_cleanup(self):
        store = ArtifactStore(self.root / "bounded", namespace="run", principal="writer",
                              policy=ArtifactPolicy(max_artifact_bytes=4, max_total_bytes=6, max_artifacts=2))
        file = self.root / "source"
        file.write_bytes(b"1234")
        first = store.put_file(file)
        self.assertEqual(store.read_bytes(first), b"1234")
        store.put_bytes(b"56")
        with self.assertRaisesRegex(ArtifactError, "count"):
            store.put_bytes(b"x")
        file.write_bytes(b"12345")
        with self.assertRaisesRegex(ArtifactError, "per-object"):
            store.put_file(file)
        self.assertFalse(list(store.directory.glob(".pending-*")))
        self.assertEqual(store.put_bytes(b"1234"), first)

    def test_bytes_quota_counts_unique_blobs_and_crash_orphans(self):
        store = ArtifactStore(self.root / "bounded", namespace="run", principal="writer",
                              policy=ArtifactPolicy(max_artifact_bytes=4, max_total_bytes=5))
        store.put_bytes(b"abc")
        store.put_bytes(b"abc", media_type="text/plain")
        with self.assertRaisesRegex(ArtifactError, "total-byte"):
            store.put_bytes(b"def")
        (store.directory / ".pending-crash").write_bytes(b"xy")
        with self.assertRaisesRegex(ArtifactError, "total-byte"):
            store.put_bytes(b"z")

    def test_crash_before_manifest_publish_does_not_admit_reference(self):
        from mas_host import artifacts
        original = artifacts._atomic_json
        def fail_publication(directory, name, value):
            if value["artifacts"]:
                raise OSError("simulated interruption")
            return original(directory, name, value)
        with patch.object(artifacts, "_atomic_json", side_effect=fail_publication):
            with self.assertRaises(ArtifactError):
                self.store.put_bytes(b"orphan")
        self.assertEqual(self.reader("writer").status()["artifacts"], [])
        ref = self.store.put_bytes(b"orphan")
        self.assertEqual(self.store.read_bytes(ref), b"orphan")
        self.assertEqual(len(list(self.store.directory.glob("*.blob"))), 1)

    def test_cross_process_publication_is_serialised_and_quotas_atomic(self):
        path = str(self.root / "concurrent")
        with multiprocessing.get_context("fork").Pool(6) as pool:
            refs = pool.map(publish_worker, [(path, n, 8) for n in range(8)])
        self.assertEqual(sum(ref is not None for ref in refs), 8)
        same = str(self.root / "same")
        with multiprocessing.get_context("fork").Pool(4) as pool:
            refs = pool.map(publish_worker, [(same, 1, 4)] * 8)
        self.assertEqual(len({ref["artifact_id"] for ref in refs}), 1)
        bounded = str(self.root / "quota")
        with multiprocessing.get_context("fork").Pool(6) as pool:
            refs = pool.map(publish_worker, [(bounded, n, 4) for n in range(8)])
        self.assertEqual(sum(ref is not None for ref in refs), 4)

    def test_retention_links_frozen_membership_owned_and_explicitly_released(self):
        ref = self.store.put_bytes(b"result", readers=("reader",))
        ref2 = self.store.put_bytes(b"next", readers=("reader",))
        self.store.link("consumer", [ref], state="unresolved")
        self.assertTrue(has_retained_artifacts(self.root))
        with self.assertRaisesRegex(ArtifactError, "immutable"):
            self.store.link("consumer", [ref2])
        with self.assertRaisesRegex(ArtifactError, "immutable"):
            self.reader("reader").link("consumer", [ref], state="released")
        self.store.link("consumer", [ref], state="released")
        self.assertFalse(has_retained_artifacts(self.root))
        self.assertEqual(self.store.read_bytes(ref), b"result")

    def test_readonly_rejects_every_write(self):
        ref = self.store.put_bytes(b"saved")
        reader = self.reader("writer", readonly=True)
        self.assertEqual(reader.read_bytes(ref), b"saved")
        with self.assertRaisesRegex(ArtifactError, "read-only"):
            reader.put_bytes(b"new")
        with self.assertRaisesRegex(ArtifactError, "read-only"):
            reader.link("consumer", [ref])


class ArtifactOperationsTests(unittest.TestCase):
    setUp = test_operations.OperationsTests.setUp
    tearDown = test_operations.OperationsTests.tearDown
    run_directory = test_operations.OperationsTests.run_directory

    def test_nested_metadata_retains_run_with_structured_pruning_diagnostic(self):
        directory = self.run_directory()
        store = ArtifactStore(directory / "artifacts", namespace="run", principal="writer")
        (store.directory / "index.json").write_text("[" * 20000 + "0" + "]" * 20000)
        result = prune_runs(self.state, 0, apply=True)
        self.assertFalse(result["runs"][0]["deleted"])
        self.assertEqual(result["runs"][0]["reason"], "run busy or state cannot be verified")
        self.assertTrue(directory.is_dir())

    def test_backup_restore_and_retention_with_live_manifest(self):
        directory = self.run_directory()
        store = ArtifactStore(directory / "artifacts", namespace="run", principal="writer")
        ref = store.put_bytes(b"evidence")
        store.link("downstream", [ref], state="unresolved")
        result = prune_runs(self.state, 0, apply=True)
        self.assertFalse(result["runs"][0]["deleted"])
        self.assertIn("artefact", result["runs"][0]["reason"])
        backup = self.root / "backup"
        backup_state(self.state, backup)
        restored = self.root / "restored"
        restore_state(backup, restored)
        snapshot = ArtifactStore(restored / "r" / "artifacts", namespace="run", principal="writer", readonly=True)
        self.assertEqual(snapshot.read_bytes(ref), b"evidence")
        self.assertEqual((restored / "r" / "artifacts").stat().st_mode & 0o777, 0o700)
        with self.assertRaisesRegex(ArtifactError, "audit-only"):
            ArtifactStore(restored / "r" / "artifacts", namespace="run", principal="writer")
        store.link("downstream", [ref], state="released")
        self.assertTrue(prune_runs(self.state, 0, apply=True)["runs"][0]["deleted"])
