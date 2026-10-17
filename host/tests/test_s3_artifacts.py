"""Object-store contract faults, independent of optional SDK/network availability."""
from __future__ import annotations

import base64
from dataclasses import replace
import hashlib
from io import BytesIO
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from mas_host.artifacts import ArtifactError, ArtifactPolicy, ArtifactRepository, ArtifactStore, has_retained_artifacts
from mas_host.bridge_config import build_artifact_transfer
from mas_host.bridges.artifact_client import ArtifactTransferClient
from mas_host.bridges.artifact_transfer import ArtifactScope, ArtifactTransferService
from mas_host.context_snapshots import ContextPolicy, TokenCounter, build_context
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig
from mas_host.workspaces import WorkspaceInput, WorkspaceManager, WorkspacePolicy


class PreconditionFailed(Exception):
    response = {"ResponseMetadata": {"HTTPStatusCode": 412}}


class ObjectClient:
    """Finite transport double; hashes are independently checked at the boundary."""
    def __init__(self):
        self.objects, self.calls = {}, []
        self.lose_reply = False
        self.corrupt = False

    def put_object(self, **request):
        self.calls.append(("put", request["Key"]))
        assert request["IfNoneMatch"] == "*"
        if request["Key"] in self.objects:
            raise PreconditionFailed()
        data = request["Body"].read()
        assert len(data) == request["ContentLength"]
        assert hashlib.sha256(data).digest() == base64.b64decode(request["ChecksumSHA256"])
        self.objects[request["Key"]] = data
        if self.lose_reply:
            raise OSError("credential-SECRET arbitrary network failure")
        return {}

    def get_object(self, **request):
        self.calls.append(("get", request["Key"]))
        data = self.objects[request["Key"]]
        if self.corrupt:
            data = b"!" * len(data)
        return {"ContentLength": len(data), "Body": BytesIO(data)}


class S3ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = S3ContentConfig("https://objects.example", "test-bucket", "mas", "us-east-1",
                                      self.root / "no-credentials-needed-for-management")
        self.client = ObjectClient()
        self.backend = S3ArtifactContent(self.config, client=self.client)
        self.store = ArtifactStore(self.root / "run" / "artifacts", namespace="results", principal="writer",
                                   content_backend=self.backend)

    def reader(self, principal="reader", **kwargs):
        return ArtifactStore(self.store.root, namespace="results", principal=principal,
                             content_backend=self.backend, **kwargs)

    def test_same_reference_identity_and_grants_as_local_backend(self):
        local = ArtifactStore(self.root / "local", namespace="results", principal="writer")
        options = {"readers": ("reader",), "input_sha256s": ("0" * 64,), "media_type": "text/plain"}
        ref = self.store.put_bytes(b"immutable result", **options)
        self.assertEqual(ref, local.put_bytes(b"immutable result", **options))
        self.assertEqual(self.reader().read_bytes(ref), b"immutable result")
        self.assertTrue(isinstance(self.store, ArtifactRepository))
        self.assertFalse(list(self.store.directory.glob("*.blob")))
        before = len(self.client.calls)
        with self.assertRaises(ArtifactError):
            self.reader("outsider").read_bytes(ref)
        self.assertEqual(len(self.client.calls), before)
        self.assertEqual(self.reader("outsider").status()["artifacts"], [])

    def test_lost_put_reply_keeps_reserved_quota_and_reuses_original_object_key(self):
        self.client.lose_reply = True
        with self.assertRaisesRegex(ArtifactError, "S3 artefact content is unavailable") as failed:
            self.store.put_bytes(b"one", readers=("reader",))
        self.assertNotIn("SECRET", str(failed.exception))
        self.assertEqual(self.store.status()["artifacts"], [])
        manifest = json.loads((self.store.directory / "object-content.json").read_text())
        self.assertEqual(list(manifest["objects"].values()), [{"size": 3, "state": "reserved"}])
        self.client.lose_reply = False
        ref = self.reader("writer").put_bytes(b"one", readers=("reader",))
        self.assertEqual(self.reader().read_bytes(ref), b"one")
        self.assertEqual(len(self.client.objects), 1)
        self.assertEqual(len({key for method, key in self.client.calls if method == "put"}), 1)

    def test_failed_puts_cannot_bypass_byte_or_object_quota(self):
        store = ArtifactStore(self.root / "bounded", namespace="results", principal="writer",
            policy=ArtifactPolicy(max_artifact_bytes=3, max_total_bytes=3, max_artifacts=1),
            content_backend=self.backend)
        self.client.lose_reply = True
        with self.assertRaises(ArtifactError):
            store.put_bytes(b"one")
        calls = len(self.client.calls)
        with self.assertRaisesRegex(ArtifactError, "quota"):
            store.put_bytes(b"two")
        self.assertEqual(len(self.client.calls), calls)
        self.client.lose_reply = False
        self.assertEqual(store.read_bytes(store.put_bytes(b"one")), b"one")

    def test_corruption_before_publication_and_before_read_never_exposes_bytes(self):
        self.client.corrupt = True
        with self.assertRaisesRegex(ArtifactError, "integrity mismatch"):
            self.store.put_bytes(b"original")
        self.assertEqual(self.store.status()["artifacts"], [])
        self.client.corrupt = False
        ref = self.store.put_bytes(b"original")
        self.client.corrupt = True
        with self.assertRaisesRegex(ArtifactError, "integrity mismatch"):
            with self.store.open(ref):
                self.fail("unverified bytes escaped")
        self.assertTrue(self.store.inspect(ref))  # Metadata remains inspectable.

    def test_read_snapshot_is_independent_and_content_length_is_checked(self):
        data = b"a" * (1024 * 1024 + 17)
        ref = self.store.put_bytes(data)
        with self.store.open(ref) as snapshot:
            self.assertGreaterEqual(snapshot.fileno(), 0)
            self.client.objects.clear()
            self.assertEqual(snapshot.read(), data)
        with patch.object(self.client, "get_object", return_value={"ContentLength": ref.size + 1, "Body": BytesIO(data)}):
            with self.assertRaisesRegex(ArtifactError, "integrity mismatch"):
                self.store.verify(ref)

    def test_backend_rebinding_and_missing_reservations_fail_closed(self):
        ref = self.store.put_bytes(b"original")
        for backend in (None, S3ArtifactContent(replace(self.config, bucket="different-bucket"), client=self.client)):
            with self.assertRaises(ArtifactError):
                ArtifactStore(self.store.root, namespace="results", principal="writer", content_backend=backend)
        (self.store.directory / "object-content.json").unlink()
        with self.assertRaises(ArtifactError):
            self.reader("writer")

    def test_content_backend_cannot_replace_existing_local_bytes(self):
        local = ArtifactStore(self.root / "local", namespace="results", principal="writer")
        local.put_bytes(b"local")
        with self.assertRaises(ArtifactError):
            ArtifactStore(local.root, namespace="results", principal="writer", content_backend=self.backend)

    def test_context_workspace_retention_and_audit_restore_use_the_repository_port(self):
        ref = self.store.put_bytes(b"source text")
        context = build_context(self.store, (ref,), policy=ContextPolicy(),
                                tokeniser=TokenCounter("words", "1", lambda text: len(text.split())))
        manager = WorkspaceManager(self.root / "workspaces", principal="writer", artifacts=self.store)
        workspace = manager.prepare("run", "attempt", WorkspacePolicy("tests", "1", ("test",)),
                                    (WorkspaceInput("source.txt", ref),), context=context)
        manager.verify(workspace)
        self.assertTrue(has_retained_artifacts(self.root / "run"))
        target = self.root / "restored"
        shutil.copytree(self.root / "run", target)
        (target / ".restored-audit-only").write_text("audit")
        restored = ArtifactStore(target / "artifacts", namespace="results", principal="writer",
                                  content_backend=self.backend, readonly=True)
        self.assertEqual(restored.read_bytes(ref), b"source text")
        with self.assertRaises(ArtifactError):
            restored.put_bytes(b"new")

    def test_authorised_transfer_revocation_and_replay_with_remote_content(self):
        scope = ArtifactScope(self.store.root, "results", {"alice": "writer", "bob": "reader", "mallory": "outsider"},
                              content_backend=self.backend)
        service = ArtifactTransferService(self.root / "transfers", {"results": scope})
        local = ArtifactStore(self.root / "consumer", namespace="input", principal="consumer")
        data = b"large report" * 11000
        original = local.put_bytes(data)
        def client(principal):
            return ArtifactTransferClient(lambda op, args: service.dispatch(principal, op, args), descriptor={"peer": "test"})
        remote = client("alice").publish("results", local, original, readers=("bob",))
        copy = client("bob").materialize("results", remote, local)
        self.assertEqual(local.read_bytes(copy["reference"]), data)
        before = len(self.client.calls)
        with self.assertRaises(Exception):
            client("mallory").materialize("results", remote, local)
        self.assertEqual(len(self.client.calls), before)
        service.dispatch("alice", "revoke", {"scope": "results", "reference": remote.to_dict(), "readers": ["bob"]})
        with self.assertRaises(Exception):
            client("bob").materialize("results", remote, local)

    def test_configuration_is_explicit_and_credentials_do_not_enter_identity(self):
        different = replace(self.config, credentials_file=self.root / "rotated")
        self.assertEqual(different.descriptor, self.config.descriptor)
        self.assertNotIn("credentials", json.dumps(different.descriptor))
        config = {"artifacts": {"scopes": {"results": {"namespace": "results", "principals": {"alice": "writer"},
            "content_backend": {"type": "s3", "endpoint": self.config.endpoint, "bucket": self.config.bucket,
                "prefix": "mas", "region": "us-east-1", "credentials_file": str(different.credentials_file)}}}}}
        self.assertIsInstance(build_artifact_transfer(config, self.root / "service").scopes["results"].content_backend,
                              S3ArtifactContent)
        for invalid in ({"endpoint": "http://objects.example"}, {"endpoint": "https://user:secret@objects.example"},
                        {"endpoint": "https://objects.example/path"}, {"prefix": "../escape"},
                        {"read_timeout": True}, {"bucket": "bad/bucket"}):
            with self.assertRaises(ArtifactError):
                replace(self.config, **invalid)

    def test_crash_after_verified_content_before_index_publish_reuses_bytes(self):
        from mas_host import artifacts
        real = artifacts._atomic_json
        def fail(directory, name, value):
            if name == "index.json" and value["artifacts"]:
                raise OSError("interrupted metadata commit")
            return real(directory, name, value)
        with patch.object(artifacts, "_atomic_json", side_effect=fail):
            with self.assertRaises(ArtifactError):
                self.store.put_bytes(b"durable before metadata")
        self.assertEqual(self.store.status()["artifacts"], [])
        ref = self.reader("writer").put_bytes(b"durable before metadata")
        self.assertTrue(self.store.verify(ref))
        self.assertEqual(len(self.client.objects), 1)
