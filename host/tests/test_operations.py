"""Real SQLite and filesystem tests of deployment-wide operational controls."""
from __future__ import annotations

from contextlib import closing
from dataclasses import replace
import json
import multiprocessing
from pathlib import Path
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from mas_host.journal import Journal
from mas_host.operations import (DeploymentLedger, DeploymentPolicy, OperationsError,
    _file_lock, backup_state, prune_runs, restore_state)


def admit_worker(parameters):
    state, number = parameters
    try:
        return DeploymentLedger(Path(state)).admit_call("r", "call-" + str(number))
    except OperationsError:
        return False


class OperationsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.policy = DeploymentPolicy(max_runs=3, max_calls=3, max_reserved_microusd=300,
                                       min_free_bytes=1, max_state_bytes=10_000_000,
                                       per_call_reservation_microusd=100)
        self.ledger = DeploymentLedger(self.state, self.policy)

    def tearDown(self):
        self.temporary.cleanup()

    def run_directory(self, identity="r", unknown=False):
        directory = self.state / identity
        directory.mkdir(mode=0o700)
        self.ledger.admit_run(identity, directory)
        with closing(sqlite3.connect(directory / "state.sqlite")) as db, db:
            db.executescript("CREATE TABLE tasks(state TEXT); INSERT INTO tasks VALUES('completed'); CREATE TABLE calls(id TEXT,state TEXT,result TEXT);")
        journal = Journal(directory, {"run_id": identity, "workflow_version": "operations-fixture/1",
                                      "topology": {}, "max_depth": 8})
        if unknown:
            journal.step("request:1", "invoke", {}, {})
            call = journal.create_invocation("request:1", "worker", 0)
            journal.update_invocation(call["call_id"], "unknown")
        journal.close()
        (directory / "gateway.json").write_text('{"private":"credential-value"}')
        return directory

    def test_admissions_idempotent_and_policy_frozen(self):
        directory = self.run_directory()
        self.assertFalse(self.ledger.admit_run("r", directory))
        self.assertTrue(self.ledger.admit_call("r", "first"))
        self.assertFalse(self.ledger.admit_call("r", "first"))
        with self.assertRaises(OperationsError):
            self.ledger.admit_call("r", "first", 50)
        with self.assertRaises(OperationsError):
            DeploymentLedger(self.state, replace(self.policy, max_calls=4))
        self.assertEqual(DeploymentLedger(self.state).health()["usage"], {"runs": 1, "calls": 1, "reserved_microusd": 100})

    def test_zero_spend_still_consumes_call_quota(self):
        self.run_directory()
        self.ledger.admit_call("r", "local", 0)
        self.assertEqual(self.ledger.health()["usage"]["calls"], 1)
        self.assertEqual(self.ledger.health()["usage"]["reserved_microusd"], 0)

    def test_independent_processes_cannot_overrun_quota(self):
        self.run_directory()
        with multiprocessing.get_context("fork").Pool(6) as pool:
            admitted = pool.map(admit_worker, [(str(self.state), n) for n in range(18)])
        self.assertEqual(sum(admitted), 3)
        self.assertEqual(self.ledger.health()["usage"]["reserved_microusd"], 300)

    def test_unknown_or_purged_run_cannot_admit_call(self):
        with self.assertRaises(OperationsError):
            self.ledger.admit_call("missing", "c")
        self.run_directory()
        prune_runs(self.state, 0, apply=True)
        with self.assertRaises(OperationsError):
            self.ledger.admit_call("r", "c")
        with self.assertRaises(OperationsError):
            self.ledger.admit_run("r", self.state / "r")

    def test_actual_disk_threshold_closes_admission(self):
        with patch("mas_host.operations.shutil.disk_usage") as usage:
            usage.return_value.free = 0
            with self.assertRaisesRegex(OperationsError, "free disk"):
                self.ledger.admit_run("r", self.state / "r")
        self.assertEqual(self.ledger.health()["usage"]["runs"], 0)

    def test_disappearing_sqlite_journal_does_not_close_admission(self):
        self.run_directory()
        journal = self.state / "r" / "state.sqlite-journal"
        journal.write_bytes(b"temporary SQLite journal")
        original_stat = os.stat
        def vanished(path, *args, **kwargs):
            if path == str(journal) or path == journal:
                journal.unlink(missing_ok=True)
                raise FileNotFoundError("concurrent SQLite transaction removed journal")
            return original_stat(path, *args, **kwargs)
        with patch('mas_host.storage_accounting.os.stat', side_effect=vanished):
            self.assertTrue(self.ledger.admit_call("r", "allowed"))
        self.assertFalse(journal.exists())

    def test_state_size_limit_closes_admission(self):
        (self.state / "excess").write_bytes(b"a" * self.policy.max_state_bytes)
        with self.assertRaisesRegex(OperationsError, "state size"):
            self.ledger.admit_run("r", self.state / "r")

    def test_spend_budget_is_separate_from_call_quota(self):
        self.run_directory()
        self.ledger.admit_call("r", "first", 250)
        with self.assertRaisesRegex(OperationsError, "expenditure"):
            self.ledger.admit_call("r", "second", 51)
        self.ledger.admit_call("r", "second", 50)
        self.assertEqual(self.ledger.health()["usage"]["calls"], 2)

    def test_backup_restore_preserves_charge_and_content(self):
        original = self.run_directory()
        self.ledger.admit_call("r", "first")
        bundle = self.root / "backup"
        backup_state(self.state, bundle)
        restored = self.root / "restored"
        result = restore_state(bundle, restored)
        self.assertEqual(result["health"]["usage"], self.ledger.health()["usage"])
        self.assertEqual(result["health"]["deployment_id"], self.ledger.health()["deployment_id"])
        self.assertEqual((restored / "r/gateway.json").read_bytes(), (original / "gateway.json").read_bytes())
        self.assertEqual((restored / "r/gateway.json").stat().st_mode & 0o777, 0o600)
        self.assertEqual(result["health"]["admission_state"], "restored_audit_only")
        self.assertFalse(result["health"]["admission_open"])
        with self.assertRaisesRegex(OperationsError, "audit-only"):
            DeploymentLedger(restored).admit_call("r", "first")
        with self.assertRaisesRegex(OperationsError, "audit-only"):
            DeploymentLedger(restored).admit_run("fresh", restored / "fresh")
        with self.assertRaises(OperationsError):
            restore_state(bundle, self.state)

    def test_interrupted_restore_never_publishes_active_ledger(self):
        self.run_directory()
        bundle = self.root / "backup"
        backup_state(self.state, bundle)
        import shutil
        real_copy = shutil.copyfile
        seen_quarantine = []
        def interrupted(source, destination):
            result = real_copy(source, destination)
            if Path(destination).name == "deployment.sqlite":
                staged = Path(destination).parent
                seen_quarantine.append((staged / ".restored-audit-only").is_file())
                self.assertFalse(DeploymentLedger(staged).admission_enabled)
                raise OSError("injected interrupted restore")
            return result
        with patch("mas_host.operations.shutil.copyfile", side_effect=interrupted):
            with self.assertRaises(OSError):
                restore_state(bundle, self.root / "restored")
        self.assertEqual(seen_quarantine, [True])
        self.assertEqual(list((self.root / "restored").iterdir()), [])

    def test_backup_requires_runner_quiescence(self):
        directory = self.run_directory()
        with _file_lock(directory / ".host.lock", True):
            with self.assertRaisesRegex(OperationsError, "busy"):
                backup_state(self.state, self.root / "backup")
        self.assertFalse((self.root / "backup").exists())

    def test_backup_integrity_and_path_checks(self):
        self.run_directory()
        bundle = self.root / "backup"
        backup_state(self.state, bundle)
        (bundle / "r/gateway.json").write_text("changed")
        with self.assertRaises(OperationsError):
            restore_state(bundle, self.root / "restored")
        self.assertFalse((self.root / "restored").exists())

    def test_backup_symlink_rejected(self):
        self.run_directory()
        (self.state / "secret-link").symlink_to(self.root / "external")
        with self.assertRaisesRegex(OperationsError, "symlink"):
            backup_state(self.state, self.root / "backup")

    def test_prune_dry_run_then_preserves_charges(self):
        directory = self.run_directory()
        self.ledger.admit_call("r", "first")
        preview = prune_runs(self.state, 0)
        self.assertTrue(preview["dry_run"])
        self.assertTrue(preview["runs"][0]["eligible"])
        self.assertTrue(directory.exists())
        result = prune_runs(self.state, 0, apply=True)
        self.assertTrue(result["runs"][0]["deleted"])
        self.assertFalse(directory.exists())
        self.assertEqual(self.ledger.health()["usage"], {"runs": 1, "calls": 1, "reserved_microusd": 100})

    def test_unknown_effect_blocks_prune(self):
        directory = self.run_directory(unknown=True)
        result = prune_runs(self.state, 0, apply=True)
        self.assertFalse(result["runs"][0]["eligible"])
        self.assertTrue(directory.exists())

    def test_open_task_blocks_prune(self):
        directory = self.run_directory()
        with closing(sqlite3.connect(directory / "state.sqlite")) as db, db:
            db.execute("UPDATE tasks SET state='open'")
        self.assertFalse(prune_runs(self.state, 0, apply=True)["runs"][0]["eligible"])

    def test_missing_ledger_cannot_reset_existing_run_quota(self):
        self.run_directory()
        (self.state / "deployment.sqlite").unlink()
        with self.assertRaisesRegex(OperationsError, "no deployment ledger"):
            DeploymentLedger(self.state)

    def test_private_state_required(self):
        path = self.root / "public"
        path.mkdir(mode=0o755)
        with self.assertRaisesRegex(OperationsError, "private"):
            DeploymentLedger(path)

    def test_policy_types_and_bounds(self):
        for value in [True, 0, -1, 2**64]:
            with self.assertRaises(OperationsError):
                DeploymentPolicy(max_runs=value)


if __name__ == "__main__":
    unittest.main()
