"""Deployment opening preserves live ingress and exclusive maintenance."""
from contextlib import closing
import hashlib
import http.client
import json
import multiprocessing
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from mas_host.bridge_config import build_service
from mas_host.operations import DeploymentLedger, _file_lock, backup_state, restore_state
from mas_host.schema import LEDGER
from mas_host.task_service import ExecutionResult


def open_ledger(state, started, result):
    started.set()
    try:
        result.send(("ok", DeploymentLedger(Path(state)).deployment_id))
    except Exception as error:
        result.send((type(error).__name__, str(error)))
    finally:
        result.close()


class WaitingExecutor:
    """Hold the actual worker lifetime lock without contacting a provider."""
    def __init__(self, entered, release):
        self.entered, self.release = entered, release

    def execute(self, *args, cancelled, **kwargs):
        self.entered.set()
        if not self.release.wait(20):
            raise RuntimeError("test executor safety timeout")
        return ExecutionResult("cancelled" if cancelled() else "completed", {})


def run_worker(config, state, entered, release):
    service = build_service(config, Path(state))
    service.executor = WaitingExecutor(entered, release)
    service.run_once()


class LedgerStartupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.context = multiprocessing.get_context("fork")

    def child_open(self):
        parent, child = self.context.Pipe(duplex=False)
        started = self.context.Event()
        process = self.context.Process(target=open_ledger,
            args=(str(self.state), started, child))
        process.start()
        child.close()

        def stop():
            process.join(3)
            if process.is_alive():
                process.kill()
                process.join(3)
            parent.close()
        self.addCleanup(stop)
        self.assertTrue(started.wait(5))
        return process, parent

    def test_current_open_coexists_with_worker_lock_and_does_not_write(self):
        original = DeploymentLedger(self.state)
        before = original.path.read_bytes()
        with _file_lock(self.state / ".deployment.lock", False):
            process, result = self.child_open()
            self.assertTrue(result.poll(3), "current-ledger opening blocked behind worker")
            self.assertEqual(result.recv(), ("ok", original.deployment_id))
        process.join(3)
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(original.path.read_bytes(), before)

    def test_current_open_waits_for_exclusive_maintenance(self):
        original = DeploymentLedger(self.state)
        with _file_lock(self.state / ".deployment.lock", True):
            process, result = self.child_open()
            self.assertFalse(result.poll(0.2), "opening bypassed maintenance exclusion")
        self.assertTrue(result.poll(3))
        self.assertEqual(result.recv(), ("ok", original.deployment_id))
        process.join(3)
        self.assertEqual(process.exitcode, 0)

    def test_migration_waits_for_quiescence_and_preserves_identity(self):
        original = DeploymentLedger(self.state)
        identity = original.deployment_id
        with closing(sqlite3.connect(original.path)) as db:
            # Reconstruct the actual legacy layout, not a current layout with
            # an unsupported old version stamp.
            db.execute('DROP TABLE provider_charges')
            db.execute('DROP TABLE period_allocations')
            db.execute('DROP TABLE period_policies')
            db.execute('DROP TABLE budget_amendments')
            db.execute('DROP TABLE principal_quotas')
            db.execute('DROP TABLE run_principals')
            db.execute("PRAGMA application_id=0")
            db.execute("PRAGMA user_version=1")
        before = original.path.read_bytes()
        with _file_lock(self.state / ".deployment.lock", False):
            process, result = self.child_open()
            self.assertFalse(result.poll(0.2), "migration bypassed active execution")
            self.assertEqual(original.path.read_bytes(), before)
        self.assertTrue(result.poll(3))
        self.assertEqual(result.recv(), ("ok", identity))
        process.join(3)
        self.assertEqual(process.exitcode, 0)
        with closing(sqlite3.connect(original.path)) as db:
            self.assertEqual(db.execute("PRAGMA application_id").fetchone()[0], LEDGER.application_id)
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], LEDGER.version)

    def test_invalid_existing_schema_rejects_without_waiting_for_worker(self):
        for alteration in ("PRAGMA user_version=99", "CREATE TABLE unexpected(value TEXT)"):
            with self.subTest(alteration=alteration):
                self.state = self.root / str(abs(hash(alteration)))
                ledger = DeploymentLedger(self.state)
                with closing(sqlite3.connect(ledger.path)) as db:
                    db.execute(alteration)
                before = ledger.path.read_bytes()
                with _file_lock(self.state / ".deployment.lock", False):
                    process, result = self.child_open()
                    self.assertTrue(result.poll(3), "invalid schema waited for migration lock")
                    self.assertEqual(result.recv()[0], "OperationsError")
                process.join(3)
                self.assertEqual(ledger.path.read_bytes(), before)

    def test_concurrent_initialisation_retains_one_identity(self):
        # Parent SH allows every process to inspect the absent ledger before
        # releasing them to contend for initialisation's exclusive lock.
        self.state.mkdir(mode=0o700)
        with _file_lock(self.state / ".deployment.lock", False):
            children = [self.child_open() for _ in range(4)]
            self.assertTrue(all(not result.poll(0.05) for _, result in children))
        identities = []
        for process, result in children:
            self.assertTrue(result.poll(5))
            status, identity = result.recv()
            self.assertEqual(status, "ok")
            identities.append(identity)
            process.join(3)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(len(set(identities)), 1)
        self.assertEqual(DeploymentLedger(self.state).deployment_id, identities[0])

    def test_interrupted_empty_initialisation_is_recoverable(self):
        self.state.mkdir(mode=0o700)
        path = self.state / "deployment.sqlite"
        with closing(sqlite3.connect(path)) as db:
            db.execute("VACUUM")  # Valid SQLite header, no committed schema.
        self.assertGreater(path.stat().st_size, 0)
        self.assertTrue(DeploymentLedger(self.state).admission_enabled)

    def test_backup_and_restore_exclude_live_worker_health_records(self):
        DeploymentLedger(self.state)
        ephemeral = (".bridge-worker-health.json", ".bridge-worker-health.json.pending",
                     ".bridge-worker-monitor.lock", ".reconciler-health.json")
        for name in ephemeral:
            (self.state / name).write_text('{"stale":true}')
        backup, restored = self.root / "backup", self.root / "restored"
        backup_state(self.state, backup)
        manifest = json.loads((backup / "backup-manifest.json").read_text())
        self.assertTrue(all(name not in manifest["files"] for name in ephemeral))
        restore_state(backup, restored)
        self.assertTrue(all(not (backup / name).exists() for name in ephemeral))
        self.assertTrue(all(not (restored / name).exists() for name in ephemeral))
        self.assertFalse(DeploymentLedger(restored).admission_enabled)

    def test_http_restart_can_inspect_and_cancel_active_worker(self):
        from test_runner import RunnerIntegrationTests
        RunnerIntegrationTests.setUpClass()
        self.enterContext(patch.dict(os.environ, {
            "MAS_BIN": RunnerIntegrationTests.mas,
            "MAS_GATEWAY_BIN": RunnerIntegrationTests.gateway}))
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        token = secrets.token_urlsafe(48)
        verifier = self.root / "verifier.json"
        verifier.write_text(json.dumps({"version": 1, "credentials": [{
            "sha256": hashlib.sha256(token.encode()).hexdigest(),
            "principal": "client", "scopes": ["mas:read", "mas:cancel"]}]}))
        verifier.chmod(0o600)
        config = {"version": 1, "mode": "mock",
                  "public_url": f"http://127.0.0.1:{port}",
                  "grants": {"client": ["invoice_mesh"]},
                  "auth": {"type": "static", "file": str(verifier)}}
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(config))
        service = build_service(config, self.state)
        task = service.submit("client", "request", "invoice_mesh", {})
        entered, release = self.context.Event(), self.context.Event()
        worker = self.context.Process(target=run_worker,
            args=(config, str(self.state), entered, release))
        worker.start()
        server = None
        try:
            self.assertTrue(entered.wait(5))
            env = dict(os.environ)
            env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
            server = subprocess.Popen([sys.executable, "-m", "mas_host.bridge_cli", "serve",
                "--config", str(config_path), "--state-dir", str(self.state),
                "--port", str(port), "--allow-loopback-http"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
            deadline = time.monotonic() + 5
            while True:
                self.assertIsNone(server.poll(), "HTTP ingress exited during startup")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        self.fail("HTTP startup blocked behind active worker")
                    time.sleep(0.02)

            def request(operation):
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
                try:
                    connection.request("POST", "/service/" + operation,
                        body=json.dumps({"task_id": task["task_id"]}),
                        headers={"Authorization": "Bearer " + token,
                                 "Content-Type": "application/json"})
                    response = connection.getresponse()
                    result = json.loads(response.read())
                    self.assertEqual(response.status, 200, result)
                    return result["result"]
                finally:
                    connection.close()

            self.assertEqual(request("inspect")["state"], "running")
            receipt = request("cancel")
            self.assertEqual(receipt["state"], "running")
            self.assertTrue(receipt["cancel_requested"])
            self.assertTrue(worker.is_alive(), "worker finished before API assertions")
            release.set()
            worker.join(5)
            self.assertEqual(worker.exitcode, 0)
            self.assertEqual(request("inspect")["state"], "cancelled")
        finally:
            release.set()
            if server is not None:
                server.terminate()
                try:
                    server.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.communicate(timeout=3)
            worker.join(3)
            if worker.is_alive():
                worker.kill()
                worker.join(3)


if __name__ == "__main__":
    unittest.main()
