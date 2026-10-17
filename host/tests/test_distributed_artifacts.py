"""Explicit output contracts verify durable bytes and grants before receipt ack."""
from contextlib import closing
import json
import platform
import unittest

from mas_host.artifacts import ArtifactStore
from mas_host.contracts import Invocation
from mas_host.distributed import (ARTIFACT_OUTPUT_PROFILE, OUTPUT_PROFILE, WORKER_PROTOCOL,
    REMOTE, WorkerController, artifact_output, remote_binding)
from mas_host.distributed_worker import DurableWorker, SPOOL, WorkerClient, WorkerOperation
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError
from test_bridge_server import ServerFixture
import test_distributed_workers as fixtures

DIGEST = fixtures.DIGEST


class DurableOutputTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.DistributedWorkerTests.setUpClass.__func__)
    admitted = fixtures.DistributedWorkerTests.admitted

    def setUp(self):
        fixtures.DistributedWorkerTests.setUp(self)
        self.state = self.root / "artifact-controller"
        self.storage = self.root / "objects"
        self.controller = WorkerController(self.state, self.grants, lease_ms=1000,
            clock=lambda: self.now[0], artifact_repository=self.repository, artifact_store_id="test-objects/1")
        self.binding = remote_binding("alice", "echo", DIGEST, "test", {"n": 7},
            output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=("report",))

    def repository(self, namespace, principal):
        return ArtifactStore(self.storage, namespace=namespace, principal=principal)

    def reference(self, *, producer="worker-one", readers=("alice",), data=b"verified output"):
        return self.repository("results", producer).put_bytes(data, readers=readers).to_dict()

    def register(self, *, boot="boot1", profiles=(ARTIFACT_OUTPUT_PROFILE,)):
        return self.controller.register("worker-one", boot=boot, capabilities={"echo": DIGEST},
            architecture="amd64", runtime="python-" + platform.python_version(), trust_zone="test",
            protocol=WORKER_PROTOCOL, output_profiles=profiles)

    def dispatch(self):
        assignment = self.controller.claim("worker-one", boot="boot1")["assignment"]
        self.controller.dispatch("worker-one", boot="boot1", call_id=assignment["call_id"], fence=assignment["fence"])
        return assignment

    def receipt(self, assignment, reference):
        refs = {"report": reference}
        return {"id": "artifact-receipt", "call_id": assignment["call_id"], "boot": assignment["boot"],
            "fence": assignment["fence"], "status": "completed", "output": artifact_output({"ok": True}, refs),
            "evidence": {"observed": True}, "output_profile": ARTIFACT_OUTPUT_PROFILE, "artifacts": refs}

    def test_unknown_protocol_runtime_and_profiles_reject_without_registration(self):
        base = {"boot": "boot", "capabilities": {"echo": DIGEST}, "architecture": "amd64",
            "runtime": "python-" + platform.python_version(), "trust_zone": "test", "protocol": WORKER_PROTOCOL}
        for change, code in (({"protocol": None}, "unsupported_worker_protocol"),
                             ({"protocol": "mas-worker/99"}, "unsupported_worker_protocol"),
                             ({"runtime": "python-3.99.0"}, "unsupported_worker_runtime"),
                             ({"output_profiles": ["unknown/1"]}, "unsupported_worker_output_contract")):
            with self.subTest(change=change), self.assertRaises(ServiceError) as failure:
                self.controller.register("worker-one", **{**base, **change})
            self.assertEqual(failure.exception.code, code)
        with closing(connect(self.controller.path, REMOTE, readonly=True)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM workers").fetchone()[0], 0)

    def test_unnegotiated_persisted_worker_cannot_claim(self):
        self.register()
        with closing(connect(self.controller.path, REMOTE)) as db, db:
            descriptor = json.loads(db.execute("SELECT descriptor FROM workers").fetchone()[0])
            descriptor.pop("protocol")
            db.execute("UPDATE workers SET descriptor=?", (json.dumps(descriptor),))
        with self.assertRaisesRegex(ServiceError, "unsupported worker protocol"):
            self.controller.claim("worker-one", boot="boot1")

    def test_profile_is_placement_constraint(self):
        self.register(profiles=(OUTPUT_PROFILE,))
        def callback(call_id):
            self.assertIsNone(self.controller.claim("worker-one", boot="boot1")["assignment"])
            self.controller.abandon("alice", call_id)
            return Invocation({}, {})
        self.admitted(callback)

    def test_missing_grants_missing_bytes_corruption_and_names_block_ack(self):
        self.register()
        private = self.reference(readers=())
        foreign = self.reference(producer="worker-two")
        reference = self.reference()
        path = self.storage / "results" / (reference["sha256"] + ".blob")
        original = path.read_bytes()
        def callback(call_id):
            assignment = self.dispatch()
            for value in (private, foreign):
                with self.assertRaisesRegex(ServiceError, "worker artifact unavailable"):
                    self.controller.receipt("worker-one", receipt=self.receipt(assignment, value))
            good = self.receipt(assignment, reference)
            with self.assertRaisesRegex(ServiceError, "worker output contract mismatch"):
                self.controller.receipt("worker-one", receipt={**good, "artifacts": {}})
            with self.assertRaisesRegex(ServiceError, "worker output contract mismatch"):
                self.controller.receipt("worker-one", receipt={k: v for k, v in {**good, "output_profile": OUTPUT_PROFILE}.items() if k != "artifacts"})
            path.unlink()
            with self.assertRaisesRegex(ServiceError, "worker artifact unavailable"):
                self.controller.receipt("worker-one", receipt=good)
            path.write_bytes(b"X" * len(original)); path.chmod(0o600)
            with self.assertRaisesRegex(ServiceError, "worker artifact unavailable"):
                self.controller.receipt("worker-one", receipt=good)
            self.assertEqual(self.controller.evidence("alice", call_id), [])
            self.assertEqual(self.controller.inspect("alice", call_id)["state"], "dispatched")
            path.write_bytes(original)
            ack = self.controller.receipt("worker-one", receipt=good)
            self.assertTrue(ack["authoritative"])
            self.assertEqual(self.controller.receipt("worker-one", receipt=good), ack)
            self.assertEqual(self.controller.inspect("alice", call_id)["output"], good["output"])
            store = self.repository("results", "alice")
            links = store.inspect(reference)["links"]
            self.assertTrue(links)
            store.link(links[0], [reference], state="released")
            self.controller.receipt("worker-one", receipt=good)
            self.assertEqual(store.status()["links"][links[0]]["state"], "released")
            self.assertEqual(self.controller.evidence("alice", call_id), [good])
            with self.assertRaisesRegex(ServiceError, "receipt conflict"):
                self.controller.receipt("worker-one", receipt={**good, "evidence": {"changed": True}})
            return Invocation(good["output"], {})
        self.admitted(callback)

    def test_expired_artifact_receipt_remains_evidence_and_never_reopens_authority(self):
        self.register()
        reference = self.reference()
        def callback(call_id):
            assignment = self.dispatch()
            self.now[0] += 1001
            self.register(boot="boot2")
            receipt = self.receipt(assignment, reference)
            ack = self.controller.receipt("worker-one", receipt=receipt)
            self.assertFalse(ack["authoritative"])
            self.assertEqual(self.controller.evidence("alice", call_id), [receipt])
            self.assertEqual(self.controller.inspect("alice", call_id)["state"], "unknown")
            self.assertIsNone(self.controller.claim("worker-one", boot="boot2")["assignment"])
            self.assertTrue(self.repository("results", "alice").inspect(reference)["links"])
            return Invocation({}, {})
        self.admitted(callback)

    def test_failed_storage_then_reconnect_keeps_original_spooled_result(self):
        reference = self.reference()
        path = self.storage / "results" / (reference["sha256"] + ".blob")
        original = path.read_bytes()
        calls = []
        output = artifact_output({"ok": True}, {"report": reference})
        def effect(_input, call_id):
            calls.append(call_id)
            path.unlink()  # Simulate storage becoming unavailable after publication.
            return Invocation(output, {"observed": True})
        operations = [WorkerOperation("echo", DIGEST, effect, ARTIFACT_OUTPUT_PROFILE, ("report",))]
        with ServerFixture(worker_controller=self.controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            worker = DurableWorker(client, self.root / "spool", operations, trust_zone="test")
            def callback(call_id):
                with self.assertRaisesRegex(ServiceError, "worker artifact unavailable"):
                    worker.run_once()
                with closing(connect(worker.path, SPOOL, readonly=True)) as db:
                    before = json.loads(db.execute("SELECT receipt FROM attempts WHERE state='receipt'").fetchone()[0])
                path.write_bytes(original); path.chmod(0o600)
                resumed = DurableWorker(client, self.root / "spool", operations, trust_zone="test")
                self.assertIsNone(resumed.run_once())
                self.assertEqual(calls, [call_id])
                self.assertEqual(self.controller.evidence("alice", call_id), [before])
                self.assertEqual(self.controller.inspect("alice", call_id)["state"], "unknown")
                return Invocation({}, {})
            self.admitted(callback)

    def test_lost_ack_replays_exact_artifact_receipt_once(self):
        reference = self.reference()
        calls, lost = [], []
        output = artifact_output({"ok": True}, {"report": reference})
        operations = [WorkerOperation("echo", DIGEST,
            lambda _input, call: calls.append(call) or Invocation(output, {}),
            ARTIFACT_OUTPUT_PROFILE, ("report",))]
        with ServerFixture(worker_controller=self.controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            original_call = client.call
            def lose_ack(operation, **payload):
                result = original_call(operation, **payload)
                if operation == "receipt" and not lost:
                    lost.append(payload["receipt"])
                    raise ServiceError("transport_timeout")
                return result
            client.call = lose_ack
            worker = DurableWorker(client, self.root / "lost-ack", operations, trust_zone="test")
            def callback(call_id):
                with self.assertRaisesRegex(ServiceError, "transport timeout"):
                    worker.run_once()
                resumed = DurableWorker(client, self.root / "lost-ack", operations, trust_zone="test")
                self.assertIsNone(resumed.run_once())
                self.assertEqual(calls, [call_id])
                self.assertEqual(self.controller.evidence("alice", call_id), lost)
                self.assertEqual(self.controller.inspect("alice", call_id)["output"], output)
                return Invocation(output, {})
            self.admitted(callback)

    def test_registered_worker_transports_explicit_artifact_envelope(self):
        reference = self.reference()
        output = artifact_output({"ok": True}, {"report": reference})
        operation = WorkerOperation.registered("echo", fixtures.echo_spec(), gateway_bin=self.gateway,
            timeout_seconds=5, output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=("report",))
        self.grants = {"worker-one": {"capabilities": {"echo": operation.implementation},
            "trust_zone": "test", "capacity": 1}}
        self.state = self.root / "registered-artifact-controller"
        self.controller = WorkerController(self.state, self.grants, lease_ms=10000,
            artifact_repository=self.repository, artifact_store_id="test-objects/1")
        self.binding = remote_binding("alice", "echo", operation.implementation, "test", output,
            output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=("report",))
        with ServerFixture(worker_controller=self.controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            worker = DurableWorker(client, self.root / "registered-spool", [operation], trust_zone="test")
            def callback(call_id):
                self.assertTrue(worker.run_once()["receipt_stored"])
                observed = self.controller.inspect("alice", call_id)
                self.assertEqual(observed["state"], "completed")
                self.assertEqual(observed["output"], output)
                self.assertEqual(observed["evidence"]["artifacts"], {"report": reference})
                return Invocation(output, {})
            self.admitted(callback)

    def test_worker_rejects_unknown_controller_negotiation_before_claim(self):
        from test_worker_spool_boundaries import Client
        client = Client(large=False)
        original_call = client.call
        def unsupported(operation, **payload):
            response = original_call(operation, **payload)
            return {**response, "protocol": "mas-worker/99"} if operation == "register" else response
        client.call = unsupported
        operation = WorkerOperation("echo", DIGEST, lambda *_: self.fail("effect must not execute"))
        worker = DurableWorker(client, self.root / "bad-negotiation", [operation], trust_zone="test")
        with self.assertRaisesRegex(ServiceError, "invalid remote response"):
            worker.run_once()
        self.assertEqual(client.calls, ["register"])

    def test_inline_reference_shapes_do_not_trigger_storage_verification(self):
        self.binding = remote_binding("alice", "echo", DIGEST, "test", {})
        self.register(profiles=(OUTPUT_PROFILE,))
        def callback(call_id):
            assignment = self.dispatch()
            reference = self.reference()
            receipt = {"id": "inline", "call_id": call_id, "boot": assignment["boot"],
                "fence": assignment["fence"], "status": "completed", "output": reference,
                "evidence": {}, "output_profile": OUTPUT_PROFILE}
            (self.storage / "results" / (reference["sha256"] + ".blob")).unlink()
            self.assertTrue(self.controller.receipt("worker-one", receipt=receipt)["authoritative"])
            return Invocation(reference, {})
        self.admitted(callback)


if __name__ == "__main__":
    unittest.main()
