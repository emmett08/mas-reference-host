"""Durable cloud work participates in deployment backup and host retention."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from mas_host.cursor_cloud import CloudError, CloudJobSpec
from mas_host.durable_jobs import JobStore
from mas_host.journal import Journal
from mas_host.operations import (DeploymentLedger, OperationsError, backup_state,
                                 prune_runs, restore_state)


class CursorOperationsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.ledger = DeploymentLedger(self.state)
        self.store = JobStore(self.state)
        self.spec = CloudJobSpec(repository="https://github.com/example/repository", ref="main",
                                 model="test-model", prompt="Plan a change")

    def tearDown(self):
        self.temporary.cleanup()

    def host_run(self):
        directory = self.state / "run-fixture"
        directory.mkdir(mode=0o700)
        self.ledger.admit_run("run-fixture", directory)
        with sqlite3.connect(directory / "state.sqlite") as db:
            db.executescript("CREATE TABLE tasks(state TEXT); INSERT INTO tasks VALUES('completed');"
                            "CREATE TABLE calls(id TEXT,state TEXT,result TEXT);")
        Journal(directory, {"run_id": "run-fixture", "workflow_version": "cursor-retention-test/1",
                            "topology": {}, "max_depth": 8}).close()
        prepared = self.store.prepare("job", self.spec)
        snapshot = json.dumps({"agent_id": prepared["agent_id"], "run_id": "run-remote", "remote_status": "RUNNING"})
        with self.store.open() as db, db:
            db.execute("UPDATE jobs SET state='active',host_run_dir=?,run_id='run-remote',snapshot=? WHERE job_id='job'",
                       (str(directory), snapshot))
        return directory

    def test_active_and_unknown_jobs_retain_completed_submission_host_state(self):
        directory = self.host_run()
        agent_id = self.store.status("job")["agent_id"]
        for state in ("active", "launch_unknown"):
            with self.store.open() as db, db:
                if state == "launch_unknown":
                    db.execute("UPDATE jobs SET state=?,run_id=NULL,snapshot=NULL WHERE job_id='job'", (state,))
            report = prune_runs(self.state, 0, apply=True)
            self.assertFalse(report["runs"][0]["eligible"])
            self.assertIn("external cloud job", report["runs"][0]["reason"])
            self.assertTrue(directory.exists())
        with self.store.open() as db, db:
            snapshot = json.dumps({"agent_id": agent_id, "run_id": "run-remote", "remote_status": "FINISHED"})
            db.execute("UPDATE jobs SET state='terminal',run_id='run-remote',snapshot=? WHERE job_id='job'", (snapshot,))
        self.assertTrue(prune_runs(self.state, 0, apply=True)["runs"][0]["deleted"])
        self.assertFalse(directory.exists())
        self.assertEqual(self.store.status("job")["state"], "terminal")

    def test_health_reports_local_observation_without_provider_call(self):
        self.assertEqual(sum(self.ledger.health()["cursor_jobs"].values()), 0)
        self.store.prepare("job", self.spec)
        self.assertEqual(self.ledger.health()["cursor_jobs"],
                         {"prepared": 1, "launch_unknown": 0, "active": 0, "terminal": 0})

    def test_backup_and_restore_keep_jobs_but_block_restored_remote_mutations(self):
        self.store.prepare("job", self.spec)
        before = self.store.status("job")
        bundle, restored = self.root / "backup", self.root / "restored"
        backup_state(self.state, bundle)
        manifest = json.loads((bundle / "backup-manifest.json").read_text())
        self.assertIn("cursor-jobs.sqlite", manifest["files"])
        restore_state(bundle, restored)
        copy = JobStore(restored)
        self.assertEqual(copy.status("job"), before)
        with self.assertRaises((CloudError, OperationsError)):
            copy.prepare("another", self.spec)
        self.assertEqual(self.store.status("job"), before)

    def test_incompatible_jobs_schema_rejected_before_backup_or_health(self):
        self.store.prepare("job", self.spec)
        with sqlite3.connect(self.store.path) as db:
            db.execute("PRAGMA user_version=999")
        before = self.store.path.read_bytes()
        with self.assertRaises(OperationsError):
            backup_state(self.state, self.root / "backup")
        with self.assertRaises(OperationsError):
            self.ledger.health()
        self.assertEqual(self.store.path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
