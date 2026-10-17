"""Composition observations participate in operational health and readiness."""
from contextlib import closing, redirect_stdout
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

from mas_host.admin import main
from mas_host.operations import (DeploymentLedger, OperationsError, backup_state,
                                 composition_counts, reconcile_state, restore_state)
from mas_host.workflow_store import COMPOSITION, WorkflowStore
import test_monitor


STATES = ("queued", "running", "input_required", "auth_required", "unknown", "blocked",
          "completed", "failed", "cancelled", "expired")


def records(state_dir):
    return {str(path.relative_to(state_dir)): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in state_dir.rglob("*") if path.is_file()}


class CompositionMonitoringTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.ledger = DeploymentLedger(self.state)

    def composition(self):
        store = WorkflowStore(self.state)
        with store.transaction() as db:
            for index, state in enumerate(STATES):
                identity = "flow-" + str(index)
                db.execute("INSERT INTO instances(id,principal,request_key,plan,identity,input,state,created,deadline) VALUES(?,?,?,?,?,?,?,?,?)",
                           (identity, "owner", str(index), "example", "binding", "{}", state, 1, 100))
                db.execute("INSERT INTO attempts(instance,path,revision,workflow,identity,run_directory,input,wall_seconds,state) VALUES(?,?,?,?,?,?,?,?,?)",
                           (identity, "root", 0, "leaf", "binding", "run-" + str(index), "{}", 60, state))
                db.execute("INSERT INTO nodes(instance,path,kind,input,state) VALUES(?,?,?,?,?)",
                           (identity, "root", "task", "{}", state))
            db.execute("INSERT INTO nodes(instance,path,kind,input,state) VALUES(?,?,?,?,?)",
                       ("flow-0", "root/next", "task", "{}", "waiting"))
            db.executemany("INSERT INTO allocations VALUES(?,?,?,?)",
                           [("flow-0", "run-0", "call-1", 125), ("flow-0", "run-0", "call-2", 0)])
        return store.path

    def assert_unverified(self):
        before = records(self.state)
        with self.assertRaises(OperationsError):
            composition_counts(self.state)
        with self.assertRaises(OperationsError):
            self.ledger.health()
        report = reconcile_state(self.state)
        self.assertFalse(report["ok"])
        self.assertTrue(report["bridge_observations"]["ok"])
        self.assertEqual(report["composition_observations"], {
            "ok": False, "error": "durable workflow composition state cannot be verified"})
        self.assertEqual(records(self.state), before)

    def test_missing_optional_store_stays_absent(self):
        before = records(self.state)
        counts = composition_counts(self.state)
        self.assertTrue(all(value == 0 for group in counts.values() for value in group.values()))
        self.assertEqual(self.ledger.health()["composition"], counts)
        report = reconcile_state(self.state)
        self.assertTrue(report["ok"])
        self.assertEqual(report["composition_observations"], {"ok": True, **counts})
        self.assertEqual(records(self.state), before)
        absent = self.root / "does-not-exist"
        self.assertEqual(composition_counts(absent), counts)
        self.assertFalse(absent.exists())

    def test_health_counts_retained_states_and_allocations_without_mutation(self):
        self.composition()
        before = records(self.state)
        counts = composition_counts(self.state)
        self.assertEqual(counts["instances"], dict.fromkeys(STATES, 1))
        self.assertEqual(counts["attempts"], dict.fromkeys(STATES, 1))
        self.assertEqual(counts["nodes"], dict.fromkeys((*STATES, "waiting"), 1))
        self.assertEqual(counts["allocations"], {"calls": 2, "reserved_microusd": 125})
        health = self.ledger.health()
        self.assertEqual(health["composition"], counts)
        self.assertEqual(health["usage"], {"runs": 0, "calls": 0, "reserved_microusd": 0})
        report = reconcile_state(self.state)
        self.assertTrue(report["ok"])
        self.assertFalse(report["bridge_observations"]["remote_refreshed"])
        self.assertEqual(report["composition_observations"], {"ok": True, **counts})
        self.assertEqual(records(self.state), before)

    def test_unknown_version_or_layout_is_not_healthy(self):
        path = self.composition()
        original = path.read_bytes()
        for statement in ("PRAGMA user_version=999", "DROP TABLE events"):
            with self.subTest(statement=statement):
                path.write_bytes(original)
                with closing(sqlite3.connect(path)) as db, db:
                    db.execute(statement)
                self.assert_unverified()

    def test_corrupt_database_is_not_healthy(self):
        path = self.composition()
        path.write_bytes(b"not a sqlite database: PRIVATE-CORRUPTION-DATA")
        self.assert_unverified()

    def test_unknown_observation_states_and_invalid_allocations_fail_closed(self):
        path = self.composition()
        original = path.read_bytes()
        statements = [f"UPDATE {table} SET state='PRIVATE-INVALID-STATE'"
                      for table in ("instances", "attempts", "nodes")]
        statements += ["UPDATE allocations SET amount=-1", "UPDATE allocations SET amount='invalid'"]
        for statement in statements:
            with self.subTest(statement=statement):
                path.write_bytes(original)
                with closing(sqlite3.connect(path)) as db, db:
                    db.execute(statement)
                self.assert_unverified()

    def test_dangling_store_symlink_is_not_an_absent_store(self):
        (self.state / "composition.sqlite").symlink_to(self.root / "missing")
        with self.assertRaises(OperationsError):
            composition_counts(self.state)
        report = reconcile_state(self.state)
        self.assertFalse(report["ok"])
        self.assertFalse(report["composition_observations"]["ok"])

    def test_restored_health_retains_composition_counts_and_audit_only_mode(self):
        self.composition()
        expected = composition_counts(self.state)
        bundle, restored = self.root / "backup", self.root / "restored"
        backup_state(self.state, bundle)
        report = restore_state(bundle, restored)
        self.assertEqual(report["health"]["composition"], expected)
        self.assertEqual(report["health"]["admission_state"], "restored_audit_only")
        self.assertFalse(report["health"]["admission_open"])

    def test_failed_finite_watcher_keeps_scan_report_and_sanitised_health_error(self):
        path = self.composition()
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("PRAGMA user_version=999")
        output = io.StringIO()
        with redirect_stdout(output):
            result = main(["--state-dir", str(self.state), "watch", "--iterations", "1", "--interval", "0.1"])
        self.assertEqual(result, 1)
        reports = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(reports), 2)
        self.assertFalse(reports[0]["ok"])
        self.assertFalse(reports[0]["composition_observations"]["ok"])
        self.assertEqual(reports[0]["health"], {
            "ok": False, "error": "deployment health cannot be verified"})
        self.assertEqual(reports[-1]["failed_scans"], 1)


@unittest.skipUnless(sys.platform == "linux", "Linux process identity readiness")
class CompositionWatcherTests(unittest.TestCase):
    setUp = test_monitor.MonitorTests.setUp
    watcher = test_monitor.MonitorTests.watcher
    until_status = test_monitor.MonitorTests.until_status

    def test_corrupt_composition_keeps_live_watcher_degraded_until_repair(self):
        from mas_host.monitor import reconciler_health
        store = WorkflowStore(self.state)
        with closing(sqlite3.connect(store.path)) as db, db:
            db.execute("PRAGMA user_version=999")
        child = self.watcher()
        self.until_status("degraded")
        self.assertIsNone(child.poll())
        with self.assertRaises(OperationsError):
            reconciler_health(self.state)
        with closing(sqlite3.connect(store.path)) as db, db:
            db.execute(f"PRAGMA user_version={COMPOSITION.version}")
        self.until_status("healthy")
        self.assertTrue(reconciler_health(self.state)["reconciler_ready"])


if __name__ == "__main__":
    unittest.main()
