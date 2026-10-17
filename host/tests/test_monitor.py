"""Real watcher readiness and finite-check failure propagation."""
from contextlib import closing, redirect_stdout
import io
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from mas_host.admin import main
from mas_host.monitor import STATUS_FILE, ReconcilerMonitor, reconciler_health
from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError, backup_state


@unittest.skipUnless(sys.platform == "linux", "Linux process identity readiness")
class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.ledger = DeploymentLedger(self.state)

    def failing_run(self):
        run = self.state / "run"
        run.mkdir(mode=0o700)
        self.ledger.admit_run("r", run)
        with closing(sqlite3.connect(run / "state.sqlite")) as db:
            db.execute("CREATE TABLE runs(id TEXT)")
            db.execute("INSERT INTO runs VALUES('r')")
            db.commit()

    def watcher(self, *, missing_binary=False):
        env = dict(os.environ)
        if missing_binary:
            env["MAS_BIN"] = "definitely-absent-mas-command"
        child = subprocess.Popen([sys.executable, "-m", "mas_host", "admin", "--state-dir",
                                  str(self.state), "watch", "--interval", "0.1"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        def cleanup():
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
        self.addCleanup(cleanup)
        return child

    def until_status(self, status):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                value = json.loads((self.state / STATUS_FILE).read_text())
                if value["status"] == status:
                    return value
            except FileNotFoundError:
                pass
            time.sleep(0.025)
        self.fail("watcher did not reach expected state")

    def test_failed_finite_scan_has_failed_report_and_exit_code(self):
        self.failing_run()
        output = io.StringIO()
        with patch.dict(os.environ, {"MAS_BIN": "definitely-absent-mas-command"}), redirect_stdout(output):
            result = main(["--state-dir", str(self.state), "watch", "--iterations", "1", "--interval", "0.1"])
        self.assertEqual(result, 1)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertFalse(records[0]["ok"])
        self.assertFalse(records[-1]["ok"])
        self.assertEqual(records[-1]["failed_scans"], 1)

    def test_live_watch_is_ready_then_sigterm_stops_readiness(self):
        child = self.watcher()
        self.until_status("healthy")
        self.assertTrue(reconciler_health(self.state)["reconciler_ready"])
        child.send_signal(signal.SIGTERM)
        self.assertEqual(child.wait(timeout=3), 0)
        self.until_status("stopped")
        with self.assertRaises(OperationsError):
            reconciler_health(self.state)

    def test_sigkill_is_detected_without_a_final_heartbeat(self):
        child = self.watcher()
        self.until_status("healthy")
        child.kill()
        child.wait(timeout=3)
        self.assertEqual(json.loads((self.state / STATUS_FILE).read_text())["status"], "healthy")
        with self.assertRaises(OperationsError):
            reconciler_health(self.state)

    def test_failed_reconciliation_is_unhealthy_while_watcher_remains_alive(self):
        self.failing_run()
        child = self.watcher(missing_binary=True)
        self.until_status("degraded")
        self.assertIsNone(child.poll())
        with self.assertRaises(OperationsError):
            reconciler_health(self.state)

    def test_missing_stale_and_copied_status_are_rejected(self):
        with self.assertRaises(OperationsError):
            reconciler_health(self.state)
        monitor = ReconcilerMonitor(self.state)
        monitor.write("healthy", scans=1, failures=0)
        self.assertTrue(reconciler_health(self.state)["ok"])
        path = self.state / STATUS_FILE
        value = json.loads(path.read_text())
        value["observed_monotonic"] -= 60
        path.write_text(json.dumps(value))
        with self.assertRaises(OperationsError):
            reconciler_health(self.state)
        monitor.write("healthy", scans=1, failures=0)
        value = json.loads(path.read_text())
        value["state_inode"] += 1
        path.write_text(json.dumps(value))
        with self.assertRaises(OperationsError):
            reconciler_health(self.state)

    def test_quota_closure_is_separate_from_reconciler_readiness(self):
        self.state = self.root / "closed-state"
        ledger = DeploymentLedger(self.state, DeploymentPolicy(max_state_bytes=1))
        self.assertFalse(ledger.health()["admission_open"])
        self.watcher()
        self.until_status("healthy")
        self.assertTrue(reconciler_health(self.state)["reconciler_ready"])

    def test_backup_excludes_ephemeral_monitor_status(self):
        ReconcilerMonitor(self.state).write("healthy", scans=1, failures=0)
        destination = self.root / "backup"
        backup_state(self.state, destination)
        self.assertFalse((destination / STATUS_FILE).exists())
        manifest = json.loads((destination / "backup-manifest.json").read_text())
        self.assertNotIn(STATUS_FILE, manifest["files"])

    def test_second_watcher_cannot_overwrite_live_ownership(self):
        self.watcher()
        self.until_status("healthy")
        output = io.StringIO()
        with redirect_stdout(output):
            result = main(["--state-dir", str(self.state), "watch", "--iterations", "1"])
        self.assertEqual(result, 1)
        self.assertTrue(reconciler_health(self.state)["reconciler_ready"])


if __name__ == "__main__":
    unittest.main()
