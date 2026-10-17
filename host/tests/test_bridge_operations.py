"""Bridge receipts participate in deployment maintenance without remote effects."""
import json
import hashlib
from pathlib import Path
from contextlib import closing
import sqlite3
import tempfile
import unittest

from mas_host.bridges.remote import RemoteTaskStore, REMOTE_TASKS, digest
from mas_host.cursor_cloud import CloudJobSpec
from mas_host.durable_jobs import JobStore
from mas_host.journal import Journal, canonical
from mas_host.operations import (DeploymentLedger, OperationsError, _file_lock,
    backup_state, prune_runs, reconcile_state, restore_state)
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError
from mas_host.task_store import TaskStore


class BridgeOperationsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.ledger = DeploymentLedger(self.state)

    def tearDown(self):
        self.temporary.cleanup()

    def run_directory(self, identity="run-service-1-0"):
        directory = self.state / identity
        directory.mkdir(mode=0o700)
        self.ledger.admit_run(identity, directory)
        with closing(sqlite3.connect(directory / "state.sqlite")) as db, db:
            db.executescript("CREATE TABLE tasks(state TEXT); INSERT INTO tasks VALUES('completed');"
                            "CREATE TABLE calls(id TEXT,state TEXT,result TEXT);")
        Journal(directory, {"run_id": identity, "workflow_version": "bridge-operations/1",
                            "topology": {}, "max_depth": 8}).close()
        return directory

    def service(self, directories=()):
        identity = self.ledger.deployment_id
        store = TaskStore(self.state, deployment_id=identity)
        with store.transaction() as db:
            db.execute("INSERT INTO workflows VALUES('test','test/1')")
            db.execute("INSERT INTO tasks(id,principal,request_key,workflow,workflow_identity,state,input,metadata,created_at,updated_at) VALUES('task','principal','request','test','test/1','unknown','{}','{}',0,0)")
            db.execute("INSERT INTO requests(principal,request_key,digest,task_id,kind) VALUES('principal','request','digest','task','submit')")
            db.execute("INSERT INTO events(task_id,event,data,created_at) VALUES('task','submitted','{}',0)")
            for revision, directory in enumerate(directories):
                db.execute("INSERT INTO executions(task_id,revision,run_id,run_directory,input,state) VALUES('task',?,?,?,'{}','completed')",
                           (revision, directory.name, directory.name))
        return store

    def remote(self, directory):
        store = RemoteTaskStore(self.state)
        self.remote_key = hashlib.sha256(b"operations-test\x00remote").hexdigest()
        descriptor, payload = {"provider": "test"}, {"message": "test"}
        with store.open() as db, db:
            db.execute("INSERT INTO remote_tasks(key,logical_key,namespace,binding,descriptor,input,state,remote_id,host_run_dir,call_id,created,updated) VALUES(?,'remote','operations-test',?,?,?,'running','remote-id',?,'call',0,0)",
                       (self.remote_key, digest({"descriptor": descriptor, "input": payload}), canonical(descriptor), canonical(payload), directory.name))
        return store

    def test_health_observes_stores_without_creating_them(self):
        health = self.ledger.health()
        self.assertEqual(sum(health["task_service"].values()), 0)
        self.assertEqual(sum(health["remote_tasks"]["states"].values()), 0)
        self.assertFalse((self.state / "task-service.sqlite").exists())
        self.assertFalse((self.state / "remote-tasks.sqlite").exists())
        self.service()
        self.assertEqual(self.ledger.health()["task_service"]["unknown"], 1)

    def test_all_unresolved_service_revisions_survive_retention(self):
        directories = [self.run_directory(), self.run_directory("run-service-1-1")]
        store = self.service(directories)
        for state in ("queued", "running", "input_required", "auth_required", "unknown"):
            with store.transaction() as db:
                db.execute("UPDATE tasks SET state=?", (state,))
            report = prune_runs(self.state, 0, apply=True)
            self.assertTrue(all(not row["eligible"] and "bridge" in row["reason"] for row in report["runs"]))
            self.assertTrue(all(directory.exists() for directory in directories))
        with store.transaction() as db:
            db.execute("UPDATE tasks SET state='completed'")
        self.assertTrue(all(row["deleted"] for row in prune_runs(self.state, 0, apply=True)["runs"]))
        with store.transaction(readonly=True) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM requests").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM events").fetchone()[0], 1)

    def test_remote_lifecycle_unknown_and_pending_actions_retain_host_evidence(self):
        directory = self.run_directory()
        store = self.remote(directory)
        for state in ("queued", "running", "input_required", "auth_required", "unknown"):
            with store.open() as db, db:
                db.execute("UPDATE remote_tasks SET state=?", (state,))
            self.assertFalse(prune_runs(self.state, 0, apply=True)["runs"][0]["eligible"])
        with store.open() as db, db:
            db.execute("UPDATE remote_tasks SET state='completed',pending_action='cancel',cancel_requested=1")
        self.assertFalse(prune_runs(self.state, 0, apply=True)["runs"][0]["eligible"])
        self.assertEqual(self.ledger.health()["remote_tasks"]["pending_actions"], 1)
        with store.open() as db, db:
            db.execute("UPDATE remote_tasks SET pending_action=NULL")
        self.assertTrue(prune_runs(self.state, 0, apply=True)["runs"][0]["deleted"])

    def test_unknown_remote_operation_retains_its_separate_host_run(self):
        directory, poll = self.run_directory(), self.run_directory("run-poll")
        store = self.remote(directory)
        with store.open() as db, db:
            db.execute("UPDATE remote_tasks SET state='completed'")
            db.execute("INSERT INTO operations VALUES(?,'cancel:one','cancel','binding','unknown',NULL,'cancel-call',?)", (self.remote_key, poll.name))
        health = self.ledger.health()
        self.assertEqual(health["remote_tasks"]["unknown_operations"], 1)
        results = {row["run_id"]: row for row in prune_runs(self.state, 0, apply=True)["runs"]}
        self.assertTrue(results[directory.name]["deleted"])
        self.assertFalse(results[poll.name]["eligible"])
        self.assertTrue(poll.exists())

    def test_snapshot_and_restore_preserve_bridge_receipts_and_local_run_references(self):
        directory = self.run_directory()
        service, remote = self.service([directory]), self.remote(directory)
        before_service, before_remote = service.path.read_bytes(), remote.path.read_bytes()
        bundle, restored = self.root / "backup", self.root / "restored"
        backup_state(self.state, bundle)
        manifest = json.loads((bundle / "backup-manifest.json").read_text())
        self.assertTrue({"task-service.sqlite", "remote-tasks.sqlite"}.issubset(manifest["files"]))
        report = restore_state(bundle, restored)
        self.assertEqual(report["health"]["admission_state"], "restored_audit_only")
        self.assertFalse(prune_runs(restored, 0)["runs"][0]["eligible"])
        with self.assertRaisesRegex(OperationsError, 'audit-only'):
            prune_runs(restored, 0, apply=True)
        copy = RemoteTaskStore(restored)
        self.assertEqual(copy.status(self.remote_key)["host_run_dir"], directory.name)
        with self.assertRaises(ServiceError):
            with copy.open():
                self.fail("restored remote store admitted a mutation")
        self.assertEqual(service.path.read_bytes(), before_service)
        self.assertEqual(remote.path.read_bytes(), before_remote)
        self.assertTrue(directory.exists())

    def test_live_bridge_operation_prevents_partial_snapshot(self):
        self.service()
        with _file_lock(self.state / ".deployment.lock", False):
            with self.assertRaisesRegex(OperationsError, "busy"):
                backup_state(self.state, self.root / "backup")
        self.assertFalse((self.root / "backup").exists())

    def test_newer_bridge_schemas_rejected_without_mutation_and_watcher_fails(self):
        service = self.service()
        remote = self.state / "remote-tasks.sqlite"
        connect(remote, REMOTE_TASKS, initialise=lambda _: None).close()
        for path in (service.path, remote):
            with self.subTest(database=path.name):
                with closing(sqlite3.connect(path)) as db, db:
                    db.execute("PRAGMA user_version=999")
                before = path.read_bytes()
                with self.assertRaises(OperationsError):
                    self.ledger.health()
                with self.assertRaises(OperationsError):
                    backup_state(self.state, self.root / ("backup-" + path.name))
                scan = reconcile_state(self.state)
                self.assertFalse(scan["ok"])
                self.assertFalse(scan["bridge_observations"]["ok"])
                self.assertFalse(scan["bridge_observations"]["remote_refreshed"])
                self.assertEqual(path.read_bytes(), before)
                with closing(sqlite3.connect(path)) as db, db:
                    db.execute("PRAGMA user_version=1")

    def test_unsafe_run_reference_blocks_retention(self):
        directory = self.run_directory()
        store = self.service([directory])
        with store.transaction() as db:
            db.execute("UPDATE executions SET run_directory='../other'")
        report = prune_runs(self.state, 0, apply=True)
        self.assertFalse(report["runs"][0]["eligible"])
        self.assertTrue(directory.exists())

    def test_restored_cursor_absolute_reference_still_protects_copied_run(self):
        directory = self.run_directory()
        store = JobStore(self.state)
        store.prepare("job", CloudJobSpec(repository="https://github.com/example/repository", ref="main",
                                         model="test-model", prompt="Test retention"))
        with store.open() as db, db:
            db.execute("UPDATE jobs SET state='launch_unknown',host_run_dir=?", (str(directory),))
        bundle, restored = self.root / "backup", self.root / "restored"
        backup_state(self.state, bundle)
        restore_state(bundle, restored)
        result = prune_runs(restored, 0)["runs"][0]
        self.assertFalse(result["eligible"])
        self.assertIn("cloud job", result["reason"])
        self.assertTrue((restored / directory.name).exists())
        self.assertTrue(directory.exists())


if __name__ == "__main__":
    unittest.main()
