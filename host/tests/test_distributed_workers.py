"""Remote effects stay charged, fenced and recoverable through real Runner calls."""
from contextlib import closing
import hashlib
import json
import multiprocessing
import os
import platform
from pathlib import Path
import sys
import tempfile
import time
import unittest

from mas_host import Invocation, Outcome, Runner, RunnerError
from mas_host.distributed import REMOTE, WORKER_PROTOCOL, RemoteWorkerAdapter, WorkerController, remote_binding
from mas_host.distributed_worker import DurableWorker, SPOOL, WorkerClient, WorkerOperation
from mas_host.executable_worker import WorkerSpec
from mas_host.operations import backup_state, restore_state
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError
from mas_host.bridges.authentication import StaticTokenVerifier
from test_bridge_server import ServerFixture
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests

TOKEN1 = "worker-one-test-token-000000000000000"
TOKEN2 = "worker-two-test-token-000000000000000"
DIGEST = hashlib.sha256(b"test-remote-echo/1").hexdigest()


def echo_spec():
    return WorkerSpec("echo", "1", (sys.executable, "-m", "mas_host.worker_entry"),
        factory="echo", configuration={}, cwd=Path(__file__).resolve().parents[2],
        environment={"PYTHONPATH": str(Path(__file__).resolve().parents[1])})


def worker_process(endpoint, token_file, state_dir, gateway, stopped):
    client = WorkerClient(endpoint, token_file=token_file, allow_loopback_http=True)
    operation = WorkerOperation.registered("echo", echo_spec(), gateway_bin=gateway, timeout_seconds=5)
    worker = DurableWorker(client, state_dir, [operation], trust_zone="test")
    while not stopped.is_set():
        worker.run_once()
        stopped.wait(.02)


class DistributedWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "controller"
        self.now = [1000]
        self.grants = {principal: {"capabilities": {"echo": DIGEST}, "trust_zone": "test", "capacity": 1}
                       for principal in ("worker-one", "worker-two")}
        self.controller = WorkerController(self.state, self.grants, lease_ms=1000, clock=lambda: self.now[0])
        self.binding = remote_binding("alice", "echo", DIGEST, "test", {"n": 7})
        self.tokens = {}
        for name, value in (("worker-one", TOKEN1), ("worker-two", TOKEN2)):
            path = self.root / name
            path.write_text(value)
            path.chmod(0o600)
            self.tokens[name] = path
        self.verifier = StaticTokenVerifier([
            {"sha256": hashlib.sha256(token.encode()).hexdigest(), "principal": principal, "scopes": ["mas:worker"]}
            for principal, token in (("worker-one", TOKEN1), ("worker-two", TOKEN2))])

    def register(self, principal="worker-one", boot="boot1", controller=None):
        return (controller or self.controller).register(principal, boot=boot, capabilities={"echo": DIGEST},
            architecture="test-cpu", runtime="python-" + platform.python_version(), protocol=WORKER_PROTOCOL, trust_zone="test")

    def admitted(self, callback):
        def run(context, request):
            def effect():
                self.controller.submit_admitted(context.invocation_id, self.binding)
                return callback(context.invocation_id)
            return Outcome(context.invoke(effect, input=self.binding, reservation_microusd=5))
        adapters = {name: FunctionAdapter(run if name == "b" else lambda _c, request: Outcome(request.input))
                    for name in ("a", "b", "c")}
        with Runner(topology(), adapters, self.state, supervised=False, mas_bin=self.mas, gateway_bin=self.gateway) as runner:
            value = runner.request("a", "b", "echo", {"n": 7})
            return value, runner.status()

    def receipt(self, assignment, *, identity="receipt1", output=None):
        return {"id": identity, "call_id": assignment["call_id"], "boot": assignment["boot"],
            "fence": assignment["fence"], "status": "completed", "output": output or {"n": 8},
            "evidence": {"observed": True}, "output_profile": "inline-json/1"}

    def test_only_exact_admitted_invocation_can_enter_queue(self):
        with self.assertRaisesRegex(ServiceError, "invocation not admitted"):
            self.controller.submit_admitted("invented", self.binding)
        def callback(call_id):
            with self.assertRaisesRegex(ServiceError, "invocation not admitted"):
                self.controller.submit_admitted(call_id, {**self.binding, "input": {"n": 9}})
            self.assertEqual(self.controller.inspect("alice", call_id)["state"], "pending")
            with self.assertRaisesRegex(ServiceError, "not found"):
                self.controller.inspect("bob", call_id)
            self.controller.abandon("alice", call_id)
            return Invocation({"ok": True}, {})
        _, status = self.admitted(callback)
        self.assertEqual(len(status["calls"]), 1)

    def test_expired_pre_dispatch_claim_can_move_but_old_fence_cannot_dispatch(self):
        self.register()
        self.register("worker-two", "boot2")
        def callback(call_id):
            old = self.controller.claim("worker-one", boot="boot1")["assignment"]
            self.now[0] += 1001
            current = self.controller.claim("worker-two", boot="boot2")["assignment"]
            self.assertGreater(current["fence"], old["fence"])
            with self.assertRaisesRegex(ServiceError, "stale worker assignment"):
                self.controller.dispatch("worker-one", boot="boot1", call_id=call_id, fence=old["fence"])
            self.assertTrue(self.controller.dispatch("worker-two", boot="boot2", call_id=call_id, fence=current["fence"])["execute"])
            self.assertFalse(self.controller.dispatch("worker-two", boot="boot2", call_id=call_id, fence=current["fence"])["execute"])
            with self.assertRaisesRegex(ServiceError, "invalid worker receipt"):
                self.controller.receipt("worker-two", receipt={**self.receipt(current), "output_profile": "artifact/1"})
            ack = self.controller.receipt("worker-two", receipt=self.receipt(current))
            self.assertTrue(ack["authoritative"])
            return Invocation(self.controller.inspect("alice", call_id)["output"], {})
        result, _ = self.admitted(callback)
        self.assertEqual(result.output, {"n": 8})

    def test_dispatch_expiry_and_new_boot_retain_late_evidence_without_reexecution(self):
        self.register()
        def callback(call_id):
            old = self.controller.claim("worker-one", boot="boot1")["assignment"]
            self.controller.dispatch("worker-one", boot="boot1", call_id=call_id, fence=old["fence"])
            self.now[0] += 1001
            reopened = WorkerController(self.state, self.grants, lease_ms=1000, clock=lambda: self.now[0])
            self.register(boot="boot2", controller=reopened)
            self.assertIsNone(reopened.claim("worker-one", boot="boot2")["assignment"])
            self.assertEqual(reopened.inspect("alice", call_id)["state"], "unknown")
            with self.assertRaisesRegex(ServiceError, "stale worker epoch"):
                self.register(boot="boot1", controller=reopened)
            receipt = self.receipt(old)
            ack = reopened.receipt("worker-one", receipt=receipt)
            self.assertFalse(ack["authoritative"])
            self.assertEqual(reopened.receipt("worker-one", receipt=receipt), ack)
            self.assertEqual(reopened.evidence("alice", call_id), [receipt])
            self.assertEqual(reopened.inspect("alice", call_id)["state"], "unknown")
            with self.assertRaisesRegex(ServiceError, "receipt conflict"):
                reopened.receipt("worker-one", receipt={**receipt, "output": {"n": 99}})
            return Invocation({"test": "evidence retained"}, {})
        self.admitted(callback)

    def test_registration_capability_and_drain_are_enforced(self):
        with self.assertRaisesRegex(ServiceError, "worker forbidden"):
            self.register("unregistered")
        with self.assertRaisesRegex(ServiceError, "worker capability denied"):
            self.controller.register("worker-one", boot="x", capabilities={"admin": DIGEST},
                architecture="cpu", runtime="python-" + platform.python_version(), protocol=WORKER_PROTOCOL, trust_zone="test")
        self.register()
        self.controller.drain("worker-one", boot="boot1")
        def callback(call_id):
            self.assertIsNone(self.controller.claim("worker-one", boot="boot1")["assignment"])
            self.controller.drain("worker-one", boot="boot1", draining=False)
            self.assertEqual(self.controller.claim("worker-one", boot="boot1")["assignment"]["call_id"], call_id)
            self.controller.abandon("alice", call_id)
            return Invocation({}, {})
        self.admitted(callback)

    def test_lost_dispatch_reply_never_executes_provider(self):
        calls = []
        with ServerFixture(worker_controller=self.controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            original = client.call
            def lost(operation, **payload):
                value = original(operation, **payload)
                if operation == "dispatch":
                    raise ServiceError("transport_timeout")
                return value
            client.call = lost
            worker = DurableWorker(client, self.root / "spool", [WorkerOperation("echo", DIGEST,
                lambda value, call_id: calls.append(call_id) or Invocation(value, {}))], trust_zone="test")
            def callback(call_id):
                worker.run_once()
                self.assertEqual(calls, [])
                self.assertEqual(self.controller.inspect("alice", call_id)["state"], "unknown")
                self.assertIsNone(worker.run_once())
                return Invocation({}, {})
            self.admitted(callback)

    def test_lost_receipt_ack_resends_spooled_observation_without_effect(self):
        calls, lost = [], [False]
        with ServerFixture(worker_controller=self.controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            original = client.call
            def drop(operation, **payload):
                result = original(operation, **payload)
                if operation == "receipt" and not lost[0]:
                    lost[0] = True
                    raise ServiceError("transport_timeout")
                return result
            client.call = drop
            operations = [WorkerOperation("echo", DIGEST, lambda value, call_id: calls.append(call_id) or Invocation(value, {}))]
            worker = DurableWorker(client, self.root / "spool", operations, trust_zone="test")
            def callback(call_id):
                with self.assertRaisesRegex(ServiceError, "transport timeout"):
                    worker.run_once()
                with closing(connect(worker.path, SPOOL, readonly=True)) as db:
                    self.assertEqual(db.execute("SELECT state FROM attempts").fetchone()[0], "receipt")
                restarted = DurableWorker(client, self.root / "spool", operations, trust_zone="test")
                self.assertIsNone(restarted.run_once())
                self.assertEqual(calls, [call_id])
                self.assertEqual(len(self.controller.evidence("alice", call_id)), 1)
                return Invocation(self.controller.inspect("alice", call_id)["output"], {})
            self.admitted(callback)

    def test_real_runner_and_two_process_workers_use_registered_rust_supervision(self):
        implementation = WorkerOperation.registered("echo", echo_spec(), gateway_bin=self.gateway, timeout_seconds=5).implementation
        grants = {name: {"capabilities": {"echo": implementation}, "trust_zone": "test", "capacity": 1}
                  for name in self.grants}
        self.state = self.root / "process-controller"
        # This success-path integration starts two Python workers, HTTP helper
        # processes and Rust guardians on the installed image's single CPU.
        # Its lease must outlast the five-second executor budget plus receipt
        # delivery; deterministic clock tests above exercise one-second expiry.
        controller = WorkerController(self.state, grants, lease_ms=10000)
        adapter = RemoteWorkerAdapter(controller, principal="alice", operation="echo",
                                      implementation=implementation, trust_zone="test")
        context = multiprocessing.get_context("spawn")
        stopped, processes = context.Event(), []
        with ServerFixture(worker_controller=controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            try:
                for name in grants:
                    process = context.Process(target=worker_process,
                        args=(server.endpoint, self.tokens[name], self.root / (name + "-spool"), self.gateway, stopped))
                    process.start()
                    processes.append(process)
                with Runner(topology(wall_seconds=15), {"a": FunctionAdapter(lambda _c, r: Outcome(r.input)),
                    "b": adapter, "c": adapter}, self.state, supervised=False, mas_bin=self.mas, gateway_bin=self.gateway) as runner:
                    self.assertEqual(runner.request("a", "b", "echo", {"n": 12}).output, {"n": 12})
                    with closing(connect(controller.path, REMOTE, readonly=True)) as db:
                        first = db.execute("SELECT worker,boot FROM jobs WHERE state='completed'").fetchone()
                    controller.drain(first["worker"], boot=first["boot"])
                    self.assertEqual(runner.request("a", "b", "echo", {"n": 13}).output, {"n": 13})
                    with closing(connect(controller.path, REMOTE, readonly=True)) as db:
                        self.assertEqual({row[0] for row in db.execute("SELECT worker FROM jobs")}, set(grants))
                    self.assertEqual(len(runner.status()["calls"]), 2)
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    with closing(connect(controller.path, REMOTE, readonly=True)) as db:
                        if db.execute("SELECT count(*) FROM workers").fetchone()[0] == 2:
                            break
                    time.sleep(.02)
                self.assertEqual(controller.operations.health()["usage"]["calls"], 2)
            finally:
                stopped.set()
                for process in processes:
                    process.join(8)
                    if process.is_alive():
                        process.kill()
                        process.join(2)
                    self.assertEqual(process.exitcode, 0)

    def test_worker_scope_does_not_confer_submit_or_cross_principal_authority(self):
        with ServerFixture(worker_controller=self.controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            worker = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            with self.assertRaisesRegex(ServiceError, "invalid worker request"):
                worker.call("register", principal="worker-two", boot="boot", capabilities={"echo": DIGEST},
                            architecture="cpu", runtime="python-" + platform.python_version(), protocol=WORKER_PROTOCOL, trust_zone="test")
            client = TaskServiceClient(server.endpoint, token_file=self.tokens["worker-one"], principal="alice", allow_loopback_http=True)
            with self.assertRaisesRegex(ServiceError, "insufficient scope"):
                client.submit("alice", "test-key", "workflow", {})
        reader = StaticTokenVerifier([{"sha256": hashlib.sha256(TOKEN1.encode()).hexdigest(),
            "principal": "worker-one", "scopes": ["mas:read"]}])
        with ServerFixture(worker_controller=self.controller, verifier=reader, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            with self.assertRaisesRegex(ServiceError, "insufficient scope"):
                client.call("claim", boot="boot")

    def test_controller_policy_and_grants_cannot_diverge_between_processes(self):
        changed = {**self.grants, "worker-three": self.grants["worker-one"]}
        for kwargs in ({"worker_grants": changed}, {"max_outstanding": 10}, {"lease_ms": 2000}):
            options = {"worker_grants": self.grants, "lease_ms": 1000, **kwargs}
            with self.assertRaisesRegex(ServiceError, "worker policy changed"):
                WorkerController(self.state, **options)
        self.assertEqual(self.controller.register("worker-one", boot="boot", capabilities={"echo": DIGEST},
            architecture="cpu", runtime="python-" + platform.python_version(), protocol=WORKER_PROTOCOL, trust_zone="test")["generation"], 1)

    def test_oversized_result_is_bounded_unknown_and_never_repeated(self):
        calls = []
        with ServerFixture(worker_controller=self.controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            operation = WorkerOperation("echo", DIGEST,
                lambda value, call_id: calls.append(call_id) or Invocation({"data": "x" * 200000}, {}))
            worker = DurableWorker(client, self.root / "spool", [operation], trust_zone="test")
            def callback(call_id):
                worker.run_once()
                self.assertEqual(self.controller.inspect("alice", call_id)["state"], "unknown")
                self.assertLess(len(json.dumps(self.controller.evidence("alice", call_id))), 1000)
                self.assertIsNone(worker.run_once())
                self.assertEqual(calls, [call_id])
                return Invocation({}, {})
            self.admitted(callback)

    def test_unknown_work_holds_principal_and_global_caps_without_unadmitted_dispatch(self):
        self.state = self.root / "quota-controller"
        self.controller = WorkerController(self.state, self.grants, max_outstanding=2, per_principal=1,
                                           lease_ms=1000, clock=lambda: self.now[0])
        self.register()
        self.register("worker-two", "boot2")
        def uncertain(call_id):
            principal, boot = ("worker-one", "boot1") if self.binding["principal"] == "alice" else ("worker-two", "boot2")
            assignment = self.controller.claim(principal, boot=boot)["assignment"]
            self.controller.dispatch(principal, boot=boot, call_id=call_id, fence=assignment["fence"])
            self.controller.abandon(self.binding["principal"], call_id)
            raise RunnerError("test interrupted after remote dispatch")
        def saturated(principal):
            adapter = RemoteWorkerAdapter(self.controller, principal=principal, operation="echo",
                implementation=DIGEST, trust_zone="test", reservation_microusd=5)
            adapters = {"a": FunctionAdapter(lambda _c, r: Outcome(r.input)), "b": adapter, "c": adapter}
            with Runner(topology(), adapters, self.state, supervised=False, mas_bin=self.mas, gateway_bin=self.gateway) as runner:
                with self.assertRaises(RunnerError):
                    runner.request("a", "b", "echo", {})
                report = inspect_run(runner.run_dir)
                self.assertFalse(report["recovery_required"])
                self.assertEqual(report["invocations"][0]["state"], "failed_known")
                self.assertEqual(report["invocations"][0]["evidence"]["provider_outcome"]["certainty"], "not_sent")
        with self.assertRaises(RunnerError):
            self.admitted(uncertain)
        saturated("alice")  # Principal cap, while one global slot remains.
        self.binding = remote_binding("bob", "echo", DIGEST, "test", {"n": 7})
        with self.assertRaises(RunnerError):
            self.admitted(uncertain)
        saturated("carol")  # Both global slots are occupied by unknown effects.
        with closing(connect(self.controller.path, REMOTE, readonly=True)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM jobs WHERE state='unknown'").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT count(*) FROM claims WHERE dispatched=1").fetchone()[0], 2)
        self.assertEqual(self.controller.operations.health()["usage"]["calls"], 4)

    def test_old_unknown_effect_keeps_worker_capacity_after_a_new_boot(self):
        self.register()
        self.register("worker-two", "boot2")
        def uncertain(call_id):
            assignment = self.controller.claim("worker-one", boot="boot1")["assignment"]
            self.controller.dispatch("worker-one", boot="boot1", call_id=call_id, fence=assignment["fence"])
            raise RunnerError("test lost effect acknowledgement")
        with self.assertRaises(RunnerError):
            self.admitted(uncertain)
        self.register(boot="replacement")
        self.binding = remote_binding("bob", "echo", DIGEST, "test", {"n": 7})
        def another(call_id):
            self.assertIsNone(self.controller.claim("worker-one", boot="replacement")["assignment"])
            assignment = self.controller.claim("worker-two", boot="boot2")["assignment"]
            self.assertEqual(assignment["call_id"], call_id)
            self.controller.abandon("bob", call_id)
            return Invocation({}, {})
        self.admitted(another)

    def test_registered_structured_mock_preserves_versioned_operation_and_one_allocation(self):
        root = Path(__file__).resolve().parents[2]
        schema = {"type": "object", "properties": {"answer": {"type": "integer"}},
                  "required": ["answer"], "additionalProperties": False}
        configuration = {"actor": "worker", "agent": {"provider": "mock", "model": "fixture",
            "reservation_microusd": 0, "instructions": "Return a checked result.", "operation": "solve/1"},
            "contract": {"name": "solve", "version": "1", "input_schema": {"type": "object"},
                         "output_schema": schema, "allowed_callers": ["controller"]}, "mock_output": {"answer": 42}}
        spec = WorkerSpec("structured", "1", (sys.executable, "-m", "mas_host.worker_entry"),
            factory="structured", configuration=configuration, cwd=root,
            environment={"PYTHONPATH": str(root / "host")})
        operation = WorkerOperation.registered("solve/1", spec, gateway_bin=self.gateway, reservation_microusd=7)
        self.state = self.root / "structured-controller"
        controller = WorkerController(self.state, {"worker-one": {"capabilities": {"solve/1": operation.implementation},
            "trust_zone": "test", "capacity": 1}})
        with ServerFixture(worker_controller=controller, verifier=self.verifier, request_log=lambda _r: None) as server:
            client = WorkerClient(server.endpoint, token_file=self.tokens["worker-one"], allow_loopback_http=True)
            worker = DurableWorker(client, self.root / "structured-spool", [operation], trust_zone="test")
            submit = controller.submit_admitted
            def process(call_id, binding):
                accepted = submit(call_id, binding)
                worker.run_once()
                return accepted
            controller.submit_admitted = process
            adapter = RemoteWorkerAdapter(controller, principal="alice", operation="solve/1",
                implementation=operation.implementation, trust_zone="test", reservation_microusd=7)
            with Runner(topology(), {"a": FunctionAdapter(lambda _c, r: Outcome(r.input)), "b": adapter, "c": adapter},
                        self.state, supervised=False, mas_bin=self.mas, gateway_bin=self.gateway) as runner:
                self.assertEqual(runner.request("a", "b", "solve/1", {}).output, {"answer": 42})
                self.assertEqual(runner.status()["usage"]["calls"], 1)
                report = inspect_run(runner.run_dir)
                self.assertEqual(report["invocations"][0]["reservation_microusd"], 7)

    def test_operator_reconciliation_releases_only_exact_host_resolved_job_and_preserves_late_evidence(self):
        self.register()
        retained = {}
        def interrupted(call_id):
            assignment = self.controller.claim("worker-one", boot="boot1")["assignment"]
            self.controller.dispatch("worker-one", boot="boot1", call_id=call_id, fence=assignment["fence"])
            self.controller.abandon("alice", call_id)
            retained.update(assignment)
            raise RunnerError("test lost provider acknowledgement")
        with self.assertRaises(RunnerError):
            self.admitted(interrupted)
        from mas_host.schema import LEDGER
        with closing(connect(self.controller.operations.path, LEDGER, readonly=True)) as db:
            run = db.execute("SELECT c.run_id,r.directory FROM calls c JOIN runs r USING(run_id) WHERE call_id=?", (retained["call_id"],)).fetchone()
        arguments = {"run_id": run["run_id"], "call_id": retained["call_id"], "operator": "test-operator"}
        with self.assertRaisesRegex(ServiceError, "remote resolution not ready"):
            self.controller.reconcile(**arguments)
        with self.assertRaisesRegex(ServiceError, "not found"):
            self.controller.reconcile(**{**arguments, "run_id": "another-run"})
        with self.assertRaisesRegex(ServiceError, "invalid worker request"):
            self.controller.handle("worker-one", "reconcile", arguments)
        resolve_invocation(self.state / run["directory"], retained["call_id"], decision="confirmed_no_effect",
                           evidence={"reference": "operator-established-provider-evidence"})
        result = self.controller.reconcile(**arguments)
        self.assertTrue(result["reconciled"])
        self.assertFalse(result["execution_resumed"])
        self.assertEqual(result["decision"], "confirmed_no_effect")
        self.assertEqual(len(result["evidence_sha256"]), 64)
        self.assertEqual(self.controller.reconcile(**arguments), result)
        self.assertEqual(self.controller.inspect("alice", retained["call_id"])["state"], "reconciled")
        ack = self.controller.receipt("worker-one", receipt=self.receipt(retained, identity="late-reconciled"))
        self.assertFalse(ack["authoritative"])
        self.assertEqual(self.controller.inspect("alice", retained["call_id"])["state"], "reconciled")
        self.assertEqual(len(self.controller.evidence("alice", retained["call_id"])), 1)
        def next_call(call_id):
            assignment = self.controller.claim("worker-one", boot="boot1")["assignment"]
            self.assertEqual(assignment["call_id"], call_id)
            self.controller.abandon("alice", call_id)
            return Invocation({}, {})
        self.admitted(next_call)
        self.assertEqual(self.controller.operations.health()["usage"]["calls"], 2)
        from mas_host.diagnostics import operational_metrics
        metrics = operational_metrics(self.state)
        self.assertTrue(metrics["ok"])
        self.assertEqual(metrics["counts"]["remote_jobs"]["reconciled"], 1)

    def test_restored_controller_cannot_create_or_mutate_worker_authority(self):
        self.register()
        backup = self.root / "backup"
        backup_state(self.state, backup)
        restored = self.root / "restored"
        restore_state(backup, restored)
        before = (restored / "remote-workers.sqlite").read_bytes()
        with self.assertRaisesRegex(ServiceError, "controller admission closed"):
            WorkerController(restored, self.grants)
        self.assertEqual((restored / "remote-workers.sqlite").read_bytes(), before)
