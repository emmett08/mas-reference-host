"""Operator transport requires both scope and explicit owner/admin grants."""
from contextlib import redirect_stdout
from io import StringIO
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mas_host.bridge_cli import main as cli
from mas_host.bridges.authentication import StaticTokenVerifier
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.operator_service import OperatorService, operator_grants
from mas_host.operations import backup_state, restore_state
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService
from mas_host.worker_control import WorkerControl
from test_bridge_server import ServerFixture
from test_definition_versions import registered, Executor
import test_deadlines as deadlines_fixture
import test_recovery_service as recovery_fixture

TOKEN = "operator-test-token-00000000000000000000"
OWNER = "owner-test-token-00000000000000000000000"
READER = "reader-test-token-0000000000000000000000"
UNGRANTED = "ungranted-test-token-00000000000000000000"


class OperatorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.workflow = registered()
        self.service = DurableTaskService(self.state, {"leaf": self.workflow},
            {"alice": {"leaf"}, "bob": {"leaf"}}, executor=Executor())
        self.task = self.service.submit("alice", "submission", "leaf", {})
        self.operator = OperatorService(self.service, operators={
            "admin": {"owners": ["alice"], "deployment_admin": True},
            "reviewer": {"owners": ["alice"], "deployment_admin": False}})
        self.token = self.root / "token"
        self.token.write_text(TOKEN); self.token.chmod(0o600)
        self.verifier = StaticTokenVerifier([
            {"sha256": hashlib.sha256(token.encode()).hexdigest(), "principal": principal, "scopes": scopes}
            for token, principal, scopes in ((TOKEN, "admin", ["mas:admin"]),
                (OWNER, "alice", ["mas:read", "mas:submit"]), (READER, "reviewer", ["mas:admin"]),
                (UNGRANTED, "outsider", ["mas:admin"]))])

    def call(self, operation, payload=None, operator="admin"):
        return self.operator.dispatch(operator, operation, payload or {})

    def test_scope_and_explicit_grants_both_required_without_principal_spoofing(self):
        with ServerFixture(self.service, verifier=self.verifier, operator_service=self.operator, request_log=lambda _: None) as server:
            def request(operation, payload=None, token=TOKEN):
                return server.request("/service/operator/" + operation, payload or {}, token=token)
            self.assertEqual(request("capabilities")[0], 200)
            self.assertEqual(request("capabilities", token=OWNER)[0], 403)
            self.assertEqual(request("capabilities", token=UNGRANTED)[0], 403)
            self.assertEqual(request("budget", token=READER)[0], 403)
            self.assertEqual(request("task", {"principal": "bob", "task_id": self.task["task_id"]})[0], 403)
            self.assertEqual(request("storage", {"operator": "admin"}, token=UNGRANTED)[0], 403)
            self.assertEqual(request("storage", {"state_dir": "/etc"})[0], 400)
            status, _, result = request("tasks", {"principal": "alice"}, token=READER)
            self.assertEqual(status, 200)
            self.assertEqual(result["result"]["tasks"][0]["task_id"], self.task["task_id"])
            client = TaskServiceClient(server.endpoint, token_file=self.token, principal="admin", allow_loopback_http=True)
            self.assertEqual(client.operator("admin", "task", {"principal": "alice", "task_id": self.task["task_id"]}), self.service.inspect("alice", self.task["task_id"]))
            with self.assertRaisesRegex(ServiceError, "principal mismatch"):
                client.operator("another", "storage", {})
            output = StringIO()
            with redirect_stdout(output):
                code = cli(["operator", "worker_status", "--endpoint", server.endpoint,
                            "--token-file", str(self.token), "--operator", "admin", "--allow-loopback-http"])
            self.assertEqual(code, 0)
            self.assertFalse(json.loads(output.getvalue())["draining"])

    def test_budget_amendment_identity_and_retention_preview(self):
        policy = self.call("budget")["health"]["policy"]
        payload = {"request_key": "fund", "reason": "Approved trial", "expected_revision": 0,
                   "limits": {"max_calls": policy["max_calls"] + 10}}
        receipt = self.call("budget_amend", payload)
        self.assertEqual(receipt["principal"], "admin")
        self.assertEqual(self.call("budget_amend", payload), receipt)
        with self.assertRaises(ServiceError):
            self.call("budget_amend", {**payload, "limits": {"max_calls": policy["max_calls"] + 20}})
        self.assertEqual(self.call("budget")["amendments"]["items"], [receipt])
        for kind in ("tasks", "runs", "workflows"):
            self.assertTrue(self.call("archive_preview", {"kind": kind, "older_than_seconds": 0})["dry_run"])
        self.assertEqual(self.service.inspect("alice", self.task["task_id"])["state"], "queued")

    def test_period_and_charge_administration_bind_authenticated_actor(self):
        policy = {"scope": "deployment", "policy": {"period_seconds": 86400, "max_calls": 100,
                  "max_reserved_microusd": 1000}, "request_key": "period", "reason": "Trial limits"}
        self.assertEqual(self.call("period_configure", policy)["actor"], "admin")
        self.assertEqual(self.call("periods")["items"][0]["scope"], "deployment")
        with self.assertRaisesRegex(ServiceError, "forbidden"):
            self.call("periods", operator="reviewer")
        self.service.operations.admit_run("invoice-run", self.state / "invoice-run", principal="alice")
        self.service.operations.admit_call("invoice-run", "invoice-call", 10)
        charge = {"provider": "provider", "charge_id": "charge1", "run_id": "invoice-run",
                  "call_id": "invoice-call", "actual_microusd": 7, "observed_at": int(time.time()), "evidence": "invoice/1"}
        receipt = self.call("charge_record", charge)
        self.assertEqual(receipt["actor"], "admin")
        self.assertFalse(receipt["allocation_changed"])
        with self.assertRaisesRegex(ServiceError, "invalid operator request"):
            self.call("charge_record", {**charge, "actor": "another"})
        report = self.call("charge_reconcile", {"run_id": "invoice-run", "call_id": "invoice-call"})
        self.assertEqual(report["observed_charge_microusd"], 7)
        self.assertFalse(report["invoice_complete"])
        self.assertEqual(self.service.operations.health()["usage"]["reserved_microusd"], 10)

    def test_definition_status_retirement_protects_current_and_live_work(self):
        args = {"kind": "workflow", "name": "leaf", "identity": self.workflow.identity}
        report = self.call("definition", args)
        self.assertTrue(report["current"] and report["available"])
        with self.assertRaisesRegex(ServiceError, "definition in use"):
            self.call("definition_retire", args)
        self.service.cancel("alice", self.task["task_id"])
        with self.assertRaisesRegex(ServiceError, "definition is current"):
            self.call("definition_retire", args)
        replacement = DurableTaskService(self.state, {"leaf": registered("2")}, {"alice": {"leaf"}}, executor=Executor())
        admin = OperatorService(replacement, operators={"admin": {"owners": ["alice"], "deployment_admin": True}})
        retired = admin.dispatch("admin", "definition_retire", args)
        self.assertTrue(retired["retired"])
        self.assertFalse(retired["available"])

    def test_drain_idempotency_restart_and_audit_only_restore(self):
        payload = {"request_key": "drain", "reason": "Maintenance", "expected_revision": 0, "draining": True}
        receipt = self.call("worker_drain", payload)
        self.assertEqual(self.call("worker_drain", payload), receipt)
        self.assertTrue(WorkerControl(self.service.operations).status()["draining"])
        with self.assertRaisesRegex(ServiceError, "worker control conflict"):
            self.call("worker_drain", {**payload, "draining": False})
        with self.operator.workers.admission() as admitted:
            self.assertFalse(admitted)
        self.assertFalse(self.call("worker_drain", {**payload, "request_key": "resume", "expected_revision": 1, "draining": False})["draining"])
        bundle, restored = self.root / "backup", self.root / "restored"
        backup_state(self.state, bundle); restore_state(bundle, restored)
        from mas_host.operations import DeploymentLedger
        control = WorkerControl(DeploymentLedger(restored, readonly=True))
        self.assertEqual(control.status()["revision"], 2)
        with self.assertRaisesRegex(ServiceError, "audit only"):
            control.set(operator="admin", **{**payload, "request_key": "restore", "expected_revision": 2})

    def test_drain_receipt_waits_for_process_launch_barrier(self):
        started, finished = threading.Event(), threading.Event()
        result = []
        def drain():
            started.set()
            result.append(self.call("worker_drain", {"request_key": "barrier", "reason": "Maintenance", "expected_revision": 0, "draining": True}))
            finished.set()
        with self.operator.workers.admission() as admitted:
            self.assertTrue(admitted)
            thread = threading.Thread(target=drain)
            thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(finished.wait(.05))
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertTrue(result[0]["draining"])
        with self.operator.workers.admission() as admitted:
            self.assertFalse(admitted)

    def test_peer_writes_require_separate_management_grant(self):
        from mas_host.peer_registry import PeerRegistration
        registration = PeerRegistration(name="peer", version="1", profile="mas-http/1",
            endpoint="https://peer.example", outbound_principal="worker", deployment_id="peer-deployment",
            workflows={"leaf": self.workflow.identity}, callers=["alice"]).descriptor
        command = {"request_key": "peer1", "reason": "Approved federation", "expected_revision": 0, "registration": registration}
        with self.assertRaisesRegex(ServiceError, "forbidden"):
            self.call("peer_register", command)
        peers = OperatorService(self.service, operators={"admin": {"owners": [], "deployment_admin": False, "manage_peers": True}})
        self.assertEqual(peers.dispatch("admin", "peer_register", command)["revision"], 1)
        status = peers.dispatch("admin", "peer_status", {})
        self.assertEqual(status["peers"]["items"][0]["registration"], registration)
        peers.dispatch("admin", "peer_revoke", {"request_key": "revoke1", "reason": "Credentials retired",
            "expected_revision": 1, "name": "peer", "version": "1"})
        self.assertTrue(peers.dispatch("admin", "peer_status", {})["peers"]["items"][0]["revoked"])

    def test_request_epoch_retirement_records_operator_and_owner(self):
        from mas_host.unified_service import UnifiedTaskService
        from test_definition_versions import source
        service = UnifiedTaskService(self.root / "epochs", {"leaf": self.workflow}, {"plan": source()},
            {"alice": {"leaf", "plan"}}, executor=Executor())
        task = service.submit("alice", "request1", "leaf", {}, request_epoch="trial1")
        service.cancel("alice", task["task_id"])
        admin = OperatorService(service, operators={"admin": {"owners": ["alice"], "deployment_admin": False}})
        command = {"principal": "alice", "epoch": "trial1", "reason": "Trial complete", "prune": False}
        receipt = admin.dispatch("admin", "request_epoch_retire", command)
        self.assertEqual(receipt["authoriser"], "admin")
        self.assertEqual(receipt["principal"], "alice")
        self.assertEqual(admin.dispatch("admin", "request_epoch_status", {"principal": "alice", "epoch": "trial1"})["state"], "retired")
        with self.assertRaises(ServiceError):
            service.submit("alice", "late", "leaf", {}, request_epoch="trial1")

    def test_grant_configuration_rejects_wildcard_and_incorrect_types(self):
        for value in ({"admin": {"owners": ["*"], "deployment_admin": True}},
                      {"admin": {"owners": "alice", "deployment_admin": True}},
                      {"admin": {"owners": ["alice"], "deployment_admin": 1}}):
            with self.assertRaises(ServiceError):
                operator_grants(value)


class OperatorDeadlineTests(unittest.TestCase):
    setUp = deadlines_fixture.DeadlineTests.setUp

    def test_deadline_operator_http_preserves_task_and_audit(self):
        admin = OperatorService(self.service, operators={"alice": {"owners": ["alice"], "deployment_admin": False}})
        self.service.run_once()
        self.clock[0] = 15000
        with ServerFixture(self.service, operator_service=admin, request_log=lambda _: None) as server:
            status, _, response = server.request("/service/operator/deadline_extend",
                {"principal": "alice", "task_id": self.task["task_id"], **self.args})
            self.assertEqual(status, 200, response)
            self.assertFalse(response["result"]["execution_resumed"])
            self.assertEqual(response["result"]["operator"], "alice")
            self.assertEqual(self.service.inspect("alice", self.task["task_id"])["state"], "input_required")


class OperatorRecoveryTests(unittest.TestCase):
    setUpClass = classmethod(recovery_fixture.RecoveryServiceTests.setUpClass.__func__)
    setUp = recovery_fixture.RecoveryServiceTests.setUp

    def test_inbox_and_resolution_use_same_durable_recovery_authority(self):
        admin = OperatorService(self.service, operators={"alice": {"owners": ["alice"], "deployment_admin": False}})
        with ServerFixture(self.service, operator_service=admin, request_log=lambda _: None) as server:
            status, _, inbox = server.request("/service/operator/recovery_inbox", {"principal": "alice", "task_id": self.task})
            self.assertEqual(status, 200, inbox)
            self.assertEqual(inbox["result"]["entries"]["items"][0]["call_id"], self.call)
            status, _, result = server.request("/service/operator/recovery_resolve", {"principal": "alice", "task_id": self.task,
                "call_id": self.call, "request_key": "resolve", "decision": "completed", "evidence": self.evidence, "output": {"answer": 42}})
            self.assertEqual(status, 200, result)
            self.assertFalse(result["result"]["execution_resumed"])
            self.assertEqual(self.calls, ["sent"])
            self.assertEqual(self.service.inspect("alice", self.task)["state"], "unknown")


if __name__ == "__main__":
    unittest.main()
