"""Local readiness observes actual ingress, loop ownership and bounded work."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mas_host.bridge_config import build_service
from mas_host.bridge_health import (STATUS_FILE, WorkerMonitor, ingress_health,
                                    worker_health, EXECUTION_OVERHEAD_SECONDS, _state_health)
from mas_host.operations import OperationsError, backup_state
from mas_host.service_contracts import ServiceError


@unittest.skipUnless(sys.platform == "linux", "Linux worker identity and locking")
class BridgeHealthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        verifier = self.root / "verifier.json"
        verifier.write_text(json.dumps({"version": 1, "credentials": [{
            "sha256": hashlib.sha256(b"a-private-offline-test-token").hexdigest(),
            "principal": "alice", "scopes": ["mas:read"]}]}))
        verifier.chmod(0o600)
        self.config = {"version": 1, "mode": "mock", "public_url": f"http://127.0.0.1:{self.port}",
                       "grants": {"alice": ["invoice_mesh"]},
                       "auth": {"type": "static", "file": str(verifier)}}
        self.config_file = self.root / "config.json"
        self.config_file.write_text(json.dumps(self.config))
        self.service = build_service(self.config, self.state)

    def process(self, command, *extra):
        child = subprocess.Popen([sys.executable, "-m", "mas_host", "bridge", command,
            "--config", str(self.config_file), "--state-dir", str(self.state), *extra],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        def cleanup():
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            child.stderr.close()
        self.addCleanup(cleanup)
        return child

    def await_ready(self, check, child):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if child.poll() is not None:
                self.fail("bridge process exited: " + child.stderr.read().decode())
            try:
                return check()
            except Exception:
                time.sleep(0.025)
        self.fail("bridge readiness timed out")

    def test_http_readiness_requires_real_ingress_and_valid_state(self):
        check = lambda: ingress_health(self.state, self.config, self.port)
        with self.assertRaises(OSError):
            check()
        child = self.process("serve", "--port", str(self.port), "--allow-loopback-http")
        self.assertTrue(self.await_ready(check, child)["bridge_ready"])
        before = self.service.operations.health()["usage"]
        self.assertTrue(check()["ok"])
        self.assertEqual(self.service.operations.health()["usage"], before)
        with closing(sqlite3.connect(self.state / "task-service.sqlite")) as db, db:
            db.execute("PRAGMA user_version=999")
        with self.assertRaises(Exception):
            check()

    def test_missing_state_health_does_not_initialise_a_deployment(self):
        missing = self.root / "missing"
        with self.assertRaises(OperationsError):
            ingress_health(missing, self.config, self.port)
        self.assertFalse(missing.exists())

    def test_cli_service_types_share_public_readiness_card_without_construction(self):
        from mas_host.bridges.a2a import A2ABridge
        from mas_host.task_service import DurableTaskService
        from mas_host.unified_service import UnifiedTaskService
        from mas_host.shared_task_service import SharedTaskService
        from mas_host.shared_service import SharedTaskFacade
        endpoint = self.config["public_url"] + "/a2a"
        expected = A2ABridge(self.service, endpoint=endpoint, allow_http=True).agent_card()
        for service_type in (DurableTaskService, UnifiedTaskService,
                             SharedTaskService, SharedTaskFacade):
            with self.subTest(service=service_type.__name__), \
                    patch.object(service_type, "__init__", side_effect=AssertionError("probe constructed service")), \
                    patch.object(service_type, "discover", side_effect=AssertionError("probe read private registrations")):
                actual = A2ABridge(service_type, endpoint=endpoint, allow_http=True).agent_card()
                self.assertEqual(actual, expected)

    def test_shared_health_reads_native_authority_without_local_provision_or_renewal(self):
        calls = []
        class Authority:
            def query(self, operation, parameters):
                calls.append((operation, parameters))
                if operation == 'authority.status':
                    return {'draining': False, 'lease_until_ms': 1, 'database_time_ms': 2}
                return {'admission_open': True, 'admission_state': 'active'}
            def close(self): calls.append(('close', {}))
        missing = self.root / 'shared-cache'
        with patch('mas_host.bridge_config.build_shared_authority', return_value=Authority()), \
                patch('mas_host.bridge_health.DeploymentLedger', side_effect=AssertionError('local authority forbidden')):
            health = _state_health(missing, {'shared': {}})
        self.assertFalse(health['admission_open'])
        self.assertEqual(health['admission_state'], 'lease_expired')
        self.assertEqual(calls, [('operator.storage', {}), ('authority.status', {}), ('close', {})])
        self.assertFalse(missing.exists())

    def test_live_worker_stops_being_ready_after_sigterm(self):
        child = self.process("worker", "--interval", "0.05")
        report = self.await_ready(lambda: worker_health(self.state), child)
        self.assertTrue(report["worker_ready"])
        self.assertGreaterEqual(report["completed_polls"], 1)
        child.send_signal(signal.SIGTERM)
        self.assertEqual(child.wait(timeout=5), 0)
        self.assertEqual(json.loads((self.state / STATUS_FILE).read_text())["phase"], "stopped")
        with self.assertRaises(OperationsError):
            worker_health(self.state)

    def test_dead_worker_is_detected_without_a_final_heartbeat(self):
        child = self.process("worker", "--interval", "0.05")
        self.await_ready(lambda: worker_health(self.state), child)
        child.kill()
        child.wait(timeout=5)
        with self.assertRaises(OperationsError):
            worker_health(self.state)

    def test_duplicate_worker_cannot_replace_the_live_monitor(self):
        child = self.process("worker", "--interval", "0.05")
        self.await_ready(lambda: worker_health(self.state), child)
        identity = json.loads((self.state / STATUS_FILE).read_text())["process"]
        duplicate = self.process("worker", "--once")
        self.assertEqual(duplicate.wait(timeout=5), 1)
        report = worker_health(self.state)
        self.assertTrue(report["ok"])
        self.assertEqual(json.loads((self.state / STATUS_FILE).read_text())["process"], identity)

    def test_stale_copied_and_unowned_status_is_not_ready(self):
        monitor = WorkerMonitor(self.state)
        monitor.polls = 1
        with monitor.ownership():
            monitor.write("idle")
            self.assertTrue(worker_health(self.state)["ok"])
            with patch("mas_host.bridge_health.time.monotonic", return_value=time.monotonic() + 31):
                with self.assertRaises(OperationsError):
                    worker_health(self.state)
            path = self.state / STATUS_FILE
            value = json.loads(path.read_text())
            value["state_inode"] += 1
            path.write_text(json.dumps(value))
            with self.assertRaises(OperationsError):
                worker_health(self.state)
            monitor.write("idle")
        with self.assertRaises(OperationsError):
            worker_health(self.state)

    def test_execution_health_has_a_deadline_and_preserves_cancellation_contract(self):
        monitor = WorkerMonitor(self.state)
        monitor.polls = 1
        execution = {"task_id": "task"}
        workflow = SimpleNamespace(topology={"wall_seconds": 180})
        sentinel = object()
        cancelled = lambda: False
        class Executor:
            def execute(inner, actual_workflow, actual_execution, **kwargs):
                self.assertIs(actual_workflow, workflow)
                self.assertIs(actual_execution, execution)
                self.assertEqual(kwargs, {"cancelled": cancelled, "ownership_descriptor": 42})
                observed = time.monotonic()
                with patch("mas_host.bridge_health.time.monotonic", return_value=observed + 31):
                    self.assertEqual(worker_health(self.state)["phase"], "executing")
                with patch("mas_host.bridge_health.time.monotonic",
                           return_value=observed + 180 + EXECUTION_OVERHEAD_SECONDS + 1):
                    with self.assertRaises(OperationsError):
                        worker_health(self.state)
                return sentinel
        with monitor.ownership():
            actual = monitor.observe(Executor()).execute(workflow, execution, cancelled=cancelled,
                                                         ownership_descriptor=42)
            self.assertIs(actual, sentinel)
            self.assertEqual(worker_health(self.state)["phase"], "polling")

    def test_observation_preserves_optional_input_validation_before_admission(self):
        calls = []
        class ValidatedExecutor:
            def validate_input(inner, workflow, run_id, value):
                calls.append((workflow, run_id, value))
                raise ServiceError("input_too_large")
        monitor = WorkerMonitor(self.state)
        self.service.executor = monitor.observe(ValidatedExecutor())
        before = self.service.operations.health()["usage"]
        with self.assertRaises(ServiceError) as raised:
            self.service.submit("alice", "blocked", "invoice_mesh", {"text": "oversized for this executor"})
        self.assertEqual(raised.exception.code, "input_too_large")
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], self.service.workflows["invoice_mesh"])
        self.assertTrue(calls[0][1].startswith("run-service-"))
        self.assertEqual(calls[0][2], {"text": "oversized for this executor"})
        self.assertEqual(self.service.list_tasks("alice"), [])
        self.assertEqual(self.service.operations.health()["usage"], before)
        self.assertFalse((self.state / STATUS_FILE).exists())
        # A non-Rust executor with no extra preflight keeps that optionality.
        self.assertIsNone(getattr(monitor.observe(object()), "validate_input", None))

    def test_finite_worker_is_not_a_ready_continuous_service(self):
        child = self.process("worker", "--once")
        self.assertEqual(child.wait(timeout=5), 0)
        with self.assertRaises(OperationsError):
            worker_health(self.state)

    def test_backup_excludes_ephemeral_worker_status(self):
        WorkerMonitor(self.state).write("idle")
        destination = self.root / "backup"
        backup_state(self.state, destination)
        self.assertFalse((destination / STATUS_FILE).exists())
        manifest = json.loads((destination / "backup-manifest.json").read_text())
        self.assertNotIn(STATUS_FILE, manifest["files"])


if __name__ == "__main__":
    unittest.main()
