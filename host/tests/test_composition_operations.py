"""Operational backup and retention honour the new composition repository."""
from pathlib import Path
import sqlite3
import unittest

from mas_host.operations import OperationsError, backup_state, prune_runs, restore_state
from mas_host.workflow_store import WorkflowStore
from mas_host.schema import SchemaError
import test_operations


class CompositionOperationsTests(unittest.TestCase):
    setUp = test_operations.OperationsTests.setUp
    tearDown = test_operations.OperationsTests.tearDown
    run_directory = test_operations.OperationsTests.run_directory

    def composition(self):
        store = WorkflowStore(self.state)
        with store.transaction() as db:
            db.execute("INSERT INTO attempts(instance,path,revision,workflow,identity,run_directory,input,wall_seconds,state,output) VALUES(?,?,?,?,?,?,?,?,?,?)",
                       ("parent", "child", 0, "workflow-v1", "identity", "r", "{}", 60, "completed", "{}"))
        return store

    def test_history_preserves_terminal_child_run_and_survives_backup(self):
        self.run_directory()
        self.composition()
        result = prune_runs(self.state, 0, apply=True)
        self.assertFalse(result["runs"][0]["deleted"])
        self.assertEqual(result["runs"][0]["reason"], "workflow composition history retained")
        backup = self.root / "backup"
        backup_state(self.state, backup)
        restored = self.root / "restored"
        restore_state(backup, restored)
        with sqlite3.connect(restored / "composition.sqlite") as db:
            self.assertEqual(db.execute("SELECT run_directory FROM attempts").fetchone(), ("r",))

    def test_unknown_composition_schema_blocks_backup_and_pruning(self):
        self.run_directory()
        self.composition()
        with sqlite3.connect(self.state / "composition.sqlite") as db:
            db.execute("PRAGMA user_version=999")
        result = prune_runs(self.state, 0, apply=True)
        self.assertFalse(result["runs"][0]["eligible"])
        self.assertIn("cannot be verified", result["runs"][0]["reason"])
        with self.assertRaises(OperationsError):
            backup_state(self.state, self.root / "backup")

    def test_dangling_composition_symlink_does_not_bypass_retention(self):
        self.run_directory()
        (self.state / "composition.sqlite").symlink_to(self.root / "missing")
        result = prune_runs(self.state, 0, apply=True)
        self.assertFalse(result["runs"][0]["eligible"])

    def test_readonly_store_cannot_start_a_write_transaction(self):
        self.composition()
        store = WorkflowStore(self.state, readonly=True)
        with store.transaction(readonly=True) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 1)
        with self.assertRaisesRegex(SchemaError, 'read-only'):
            with store.transaction():
                self.fail('read-only store admitted a write transaction')
