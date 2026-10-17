"""Real-file repository compatibility and read-only rejection regressions."""
from contextlib import closing, redirect_stdout
from dataclasses import asdict
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from mas_host.admin import main
from mas_host.journal import Journal, JournalError, inspect_run
from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError, _db
from mas_host.schema import JOURNAL, LEDGER, LEDGER_V2, Schema, SchemaError, connect


META = {"run_id": "r", "workflow_version": "schema-test/1", "topology": {}, "max_depth": 8}


def snapshot(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        return (hashlib.sha256(path.read_bytes()).hexdigest(), tuple(db.iterdump()),
                db.execute("PRAGMA application_id").fetchone()[0],
                db.execute("PRAGMA user_version").fetchone()[0])


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def legacy_ledger(self, name="state"):
        state = self.root / name
        state.mkdir(mode=0o700)
        with closing(sqlite3.connect(state / "deployment.sqlite")) as db:
            for statement in LEDGER_V2.statements:
                db.execute(statement)
            db.execute("PRAGMA user_version=1")
            db.executemany("INSERT INTO metadata VALUES(?,?)", [
                ("policy", json.dumps(asdict(DeploymentPolicy()))),
                ("deployment_id", "deployment-original"), ("created_at", "123"),
                ("admission_state", "active")])
            db.execute("INSERT INTO runs VALUES('r','r',123,NULL)")
            db.execute("INSERT INTO calls VALUES('r','c',100,124)")
            db.commit()
        return state

    def legacy_journal(self, name="run"):
        run = self.root / name
        run.mkdir(mode=0o700)
        with closing(sqlite3.connect(run / "host.sqlite")) as db:
            for statement in JOURNAL.statements:
                db.execute(statement)
            db.executemany("INSERT INTO metadata VALUES(?,?)",
                           [(key, json.dumps(value)) for key, value in META.items()])
            db.execute("INSERT INTO steps VALUES('step','request','{}','{}')")
            db.execute("INSERT INTO invocations VALUES('call','step','worker','completed','{}','{}',100)")
            db.commit()
        return run

    def test_readonly_health_accepts_legacy_without_migration_or_creation(self):
        state = self.legacy_ledger()
        before = snapshot(state / "deployment.sqlite")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--state-dir", str(state), "health"]), 0)
        self.assertEqual(before, snapshot(state / "deployment.sqlite"))
        absent = self.root / "absent"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--state-dir", str(absent), "health"]), 1)
        self.assertFalse(absent.exists())

    def test_legacy_ledger_migration_preserves_identity_policy_and_charges(self):
        state = self.legacy_ledger()
        before = DeploymentLedger(state, readonly=True).health()
        ledger = DeploymentLedger(state)
        after = ledger.health()
        # Filesystem headroom changes independently of this migration, including
        # while another deployment writes. Compare the durable contract only.
        for field in ("deployment_id", "policy", "usage", "admission_state", "retained_runs"):
            self.assertEqual(before[field], after[field], field)
        data = snapshot(ledger.path)
        self.assertEqual(data[2:], (LEDGER.application_id, LEDGER.version))
        with _db(ledger.path) as db:
            self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0], 3)
        self.assertFalse(ledger.admit_call("r", "c", 100))

    def test_legacy_journal_migrates_preserving_checkpoints(self):
        run = self.legacy_journal()
        before = inspect_run(run)
        journal = Journal(run)
        self.assertEqual(journal.metadata(), META)
        journal.close()
        self.assertEqual(before, inspect_run(run))
        self.assertEqual(snapshot(run / "host.sqlite")[2:], (JOURNAL.application_id, JOURNAL.version))

    def test_unsupported_ledger_never_changes_on_open_or_health(self):
        alterations = ["PRAGMA user_version=99", "PRAGMA application_id=42",
                      "CREATE TABLE foreign_table(value TEXT)",
                      "ALTER TABLE calls ADD COLUMN unexpected TEXT",
                      "CREATE TRIGGER hidden AFTER INSERT ON calls BEGIN DELETE FROM calls; END"]
        for index, statement in enumerate(alterations):
            with self.subTest(statement=statement):
                state = self.legacy_ledger(str(index))
                with closing(sqlite3.connect(state / "deployment.sqlite")) as db:
                    db.execute(statement)
                    db.commit()
                before = snapshot(state / "deployment.sqlite")
                with self.assertRaises(OperationsError):
                    DeploymentLedger(state)
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(main(["--state-dir", str(state), "health"]), 1)
                self.assertEqual(before, snapshot(state / "deployment.sqlite"))

    def test_unsupported_journal_never_changes_on_open_or_inspect(self):
        alterations = ["PRAGMA user_version=99", "PRAGMA application_id=42",
                      "CREATE TABLE foreign_table(value TEXT)",
                      "ALTER TABLE steps ADD COLUMN unexpected TEXT"]
        for index, statement in enumerate(alterations):
            with self.subTest(statement=statement):
                run = self.legacy_journal(str(index))
                with closing(sqlite3.connect(run / "host.sqlite")) as db:
                    db.execute(statement)
                    db.commit()
                before = snapshot(run / "host.sqlite")
                with self.assertRaises(JournalError):
                    Journal(run)
                with self.assertRaises(JournalError):
                    inspect_run(run)
                self.assertEqual(before, snapshot(run / "host.sqlite"))

    def test_bootstrap_initialises_empty_file_and_checks_existing_metadata(self):
        run = self.root / "run"
        run.mkdir(mode=0o700)
        (run / "host.sqlite").touch()
        Journal(run, META).close()
        before = snapshot(run / "host.sqlite")
        Journal(run, META).close()
        self.assertEqual(before, snapshot(run / "host.sqlite"))
        with self.assertRaises(JournalError):
            Journal(run, {**META, "workflow_version": "different"})
        self.assertEqual(before, snapshot(run / "host.sqlite"))

    def test_incomplete_legacy_metadata_needs_explicit_matching_bootstrap(self):
        run = self.legacy_journal()
        with closing(sqlite3.connect(run / "host.sqlite")) as db:
            db.execute("DELETE FROM metadata")
            db.commit()
        before = snapshot(run / "host.sqlite")
        with self.assertRaises(JournalError):
            Journal(run)
        self.assertEqual(before, snapshot(run / "host.sqlite"))
        Journal(run, META).close()
        journal = Journal(run)
        self.assertEqual(journal.metadata(), META)
        journal.close()

    def test_policy_mismatch_does_not_migrate_legacy(self):
        state = self.legacy_ledger()
        before = snapshot(state / "deployment.sqlite")
        with self.assertRaises(OperationsError):
            DeploymentLedger(state, DeploymentPolicy(max_runs=99))
        self.assertEqual(before, snapshot(state / "deployment.sqlite"))

    def literal_schema(self, source):
        return Schema('literal test', 0x4D41535A, 1, (), (source,))

    def test_layout_preserves_quoted_literal_case_spaces_and_keyword_text(self):
        expected = "CREATE TABLE values_table(value TEXT DEFAULT 'A B', CHECK(value != 'IF NOT EXISTS'))"
        for index, altered in enumerate((expected.replace("'A B'", "'a b'"),
                                         expected.replace("'A B'", "'AB'"),
                                         expected.replace("'IF NOT EXISTS'", "''"))):
            with self.subTest(source=altered):
                path = self.root / f'literal-{index}.sqlite'
                connect(path, self.literal_schema(altered), initialise=lambda db: None).close()
                before = snapshot(path)
                for readonly in (False, True):
                    with self.assertRaises(SchemaError):
                        connect(path, self.literal_schema(expected), readonly=readonly)
                self.assertEqual(before, snapshot(path))

    def test_layout_ignores_only_unquoted_case_spacing_comments_and_optional_create_guard(self):
        source = "CREATE TABLE values_table(value TEXT DEFAULT 'It''s A B', CHECK(value != 'IF NOT EXISTS'))"
        equivalent = """create table IF NOT EXISTS values_table (
            value text /* harmless layout annotation */ default 'It''s A B',
            check ( value != 'IF NOT EXISTS' ) )"""
        path = self.root / 'equivalent.sqlite'
        connect(path, self.literal_schema(equivalent), initialise=lambda db: None).close()
        before = snapshot(path)
        connect(path, self.literal_schema(source), readonly=True).close()
        self.assertEqual(before, snapshot(path))

    def test_foreign_keys_are_enforced_by_repository_connections(self):
        path = self.root / 'foreign-keys.sqlite'
        schema = Schema('foreign key test', 0x4D41535A, 1, (), (
            'CREATE TABLE parents(id TEXT PRIMARY KEY)',
            'CREATE TABLE children(id TEXT PRIMARY KEY,parent TEXT REFERENCES parents(id))',
        ))
        with closing(connect(path, schema, initialise=lambda db: None)) as db:
            self.assertEqual(db.execute('PRAGMA foreign_keys').fetchone()[0], 1)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("INSERT INTO children VALUES('child','missing')")
            db.rollback()
            db.execute("INSERT INTO parents VALUES('parent')")
            db.execute("INSERT INTO children VALUES('child','parent')")
            db.commit()
        with closing(connect(path, schema, readonly=True)) as db:
            self.assertEqual(db.execute('PRAGMA foreign_keys').fetchone()[0], 1)

    def hot_journal(self, path, schema):
        with closing(connect(path, schema, initialise=lambda db: None)) as db:
            db.execute('INSERT INTO durable VALUES(?)', (b'committed' * 16000,))
            db.commit()
        # Spill an uncommitted transaction onto real pages, then exit without
        # closing SQLite. This is the recovery condition found by the VM gate.
        child = """import os, sqlite3, sys
db = sqlite3.connect(sys.argv[1])
db.execute('PRAGMA cache_size=2')
db.execute('BEGIN IMMEDIATE')
db.execute('UPDATE durable SET value=?', (b'uncommitted' * 16000,))
os._exit(0)
"""
        subprocess.run([sys.executable, '-c', child, str(path)], check=True, timeout=20)
        self.assertGreater(Path(str(path) + '-journal').stat().st_size, 512)

    def test_writable_preflight_recovers_hot_journal_but_diagnostic_does_not_write(self):
        schema = Schema('crash recovery', 0x4D41535A, 1, (), ('CREATE TABLE durable(value BLOB)',))
        path = self.root / 'crashed.sqlite'
        self.hot_journal(path, schema)
        journal = Path(str(path) + '-journal')
        before = (path.read_bytes(), journal.read_bytes())
        with self.assertRaises(sqlite3.OperationalError) as caught:
            connect(path, schema, readonly=True)
        self.assertEqual(caught.exception.sqlite_errorname, 'SQLITE_READONLY_ROLLBACK')
        self.assertEqual(before, (path.read_bytes(), journal.read_bytes()))
        with closing(connect(path, schema)) as db:
            self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0], 3)
            self.assertEqual(db.execute('SELECT value FROM durable').fetchone()[0], b'committed' * 16000)
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
        self.assertFalse(journal.exists())

    def test_recovered_unknown_schema_is_rejected_without_initialising_or_migrating(self):
        actual = Schema('foreign crash state', 0x4D41535B, 77, (), ('CREATE TABLE durable(value BLOB)',))
        path = self.root / 'foreign-crashed.sqlite'
        self.hot_journal(path, actual)
        with self.assertRaises(SchemaError):
            connect(path, JOURNAL, initialise=lambda _db: self.fail('unknown schema initialised'))
        with closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute('PRAGMA application_id').fetchone()[0], actual.application_id)
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], 77)
            self.assertEqual(db.execute('SELECT value FROM durable').fetchone()[0], b'committed' * 16000)
            self.assertEqual(db.execute('SELECT name FROM sqlite_master').fetchall(), [('durable',)])

    def test_ledger_owner_recovers_interrupted_policy_write_without_losing_allocations(self):
        state = self.legacy_ledger()
        owner = DeploymentLedger(state)
        before = owner.health()
        child = """import os, sqlite3, sys
db = sqlite3.connect(sys.argv[1])
db.execute('PRAGMA cache_size=2')
db.execute('BEGIN IMMEDIATE')
db.execute("UPDATE metadata SET value=? WHERE key='policy'", ('uncommitted' * 16000,))
os._exit(0)
"""
        subprocess.run([sys.executable, '-c', child, str(owner.path)], check=True, timeout=20)
        journal = Path(str(owner.path) + '-journal')
        raw = (owner.path.read_bytes(), journal.read_bytes())
        with self.assertRaises(sqlite3.OperationalError):
            DeploymentLedger(state, readonly=True)
        self.assertEqual(raw, (owner.path.read_bytes(), journal.read_bytes()))
        recovered = DeploymentLedger(state).health()
        for field in ('deployment_id', 'policy', 'usage', 'admission_state', 'retained_runs'):
            self.assertEqual(before[field], recovered[field], field)
        self.assertFalse(journal.exists())


if __name__ == "__main__":
    unittest.main()
