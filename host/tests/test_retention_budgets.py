"""Lifetime accounting and request identity survive history compaction."""
from contextlib import closing
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from mas_host.operations import (DeploymentLedger, DeploymentPolicy, OperationsError,
                                 archive_workflows, backup_state, restore_state, _file_lock)
from mas_host.schema import LEDGER_V2, Schema, SchemaError, connect, validate
from mas_host.service_contracts import ServiceError
from mas_host.workflow_store import COMPOSITION, COMPOSITION_V1, WorkflowStore


class RetentionBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / 'state'
        self.ledger = DeploymentLedger(self.state, DeploymentPolicy(
            max_runs=1, max_calls=1, max_reserved_microusd=100,
            per_call_reservation_microusd=100, min_free_bytes=1))

    def amend(self, **changes):
        return self.ledger.amend_budget(**{'request_key': 'raise-1', 'principal': 'operator',
            'reason': 'approved next workload', 'expected_revision': 0, 'limits': {'max_calls': 2}, **changes})

    def test_budget_increase_preserves_charges_and_refreshes_old_handles(self):
        old = DeploymentLedger(self.state)
        self.ledger.admit_run('r', self.state / 'r')
        self.ledger.admit_call('r', 'one')
        with self.assertRaises(OperationsError):
            old.admit_call('r', 'two')
        receipt = self.amend(limits={'max_calls': 2, 'max_reserved_microusd': 200})
        self.assertEqual(receipt['revision'], 1)
        self.assertEqual(old.health()['usage'], {'runs': 1, 'calls': 1, 'reserved_microusd': 100})
        self.assertTrue(old.admit_call('r', 'two'))
        self.assertEqual(old.health()['usage']['reserved_microusd'], 200)
        self.assertEqual(receipt, self.amend(limits={'max_calls': 2, 'max_reserved_microusd': 200}))
        with self.assertRaisesRegex(OperationsError, 'idempotency'):
            self.amend(limits={'max_calls': 3})
        with self.assertRaisesRegex(OperationsError, 'revision'):
            self.amend(request_key='raise-2')
        self.assertEqual(len(self.ledger.budget_history()), 1)

    def test_amendments_reject_decrease_noninteger_and_live_owner(self):
        for limits in ({'max_calls': True}, {'max_calls': 0}, {'min_free_bytes': 2}, {'max_calls': 1}):
            with self.subTest(limits=limits), self.assertRaises(OperationsError):
                self.amend(limits=limits)
        with _file_lock(self.state / '.deployment.lock', False), self.assertRaisesRegex(OperationsError, 'busy'):
            self.amend()
        self.assertEqual(self.ledger.budget_history(), [])

    def test_restore_preserves_budget_audit_without_enabling_amendment(self):
        receipt = self.amend()
        backup_state(self.state, self.root / 'backup')
        restore_state(self.root / 'backup', self.root / 'restored')
        restored = DeploymentLedger(self.root / 'restored')
        self.assertEqual(restored.budget_history(), [receipt])
        with self.assertRaisesRegex(OperationsError, 'audit-only'):
            restored.amend_budget(request_key='second', principal='operator', reason='restore',
                                  expected_revision=1, limits={'max_calls': 3})

    def test_new_ledger_rejects_old_reader_and_old_ledger_migrates_without_refund(self):
        with closing(sqlite3.connect(self.ledger.path)) as db:
            with self.assertRaises(SchemaError):
                validate(db, LEDGER_V2)
        state = self.root / 'legacy'
        state.mkdir(mode=0o700)
        with closing(sqlite3.connect(state / 'deployment.sqlite')) as db:
            for statement in LEDGER_V2.statements:
                db.execute(statement)
            db.execute(f'PRAGMA application_id={LEDGER_V2.application_id}')
            db.execute(f'PRAGMA user_version={LEDGER_V2.version}')
            db.executemany('INSERT INTO metadata VALUES(?,?)', [
                ('policy', json.dumps(asdict(self.ledger.policy))), ('deployment_id', 'old'),
                ('created_at', '1'), ('admission_state', 'active')])
            db.execute("INSERT INTO runs VALUES('r','r',1,NULL)")
            db.execute("INSERT INTO calls VALUES('r','c',100,1)")
            db.commit()
        before = (state / 'deployment.sqlite').read_bytes()
        self.assertEqual(DeploymentLedger(state, readonly=True).budget_history(), [])
        self.assertEqual(before, (state / 'deployment.sqlite').read_bytes())
        upgraded = DeploymentLedger(state)
        self.assertEqual(upgraded.health()['usage']['reserved_microusd'], 100)
        self.assertEqual(upgraded.budget_history(), [])

    def completed(self, store, key='key'):
        with store.transaction() as db:
            identity = store.create_instance(db, 'alice', key, 'plan', 'definition', {'payload': 'private'},
                wall_seconds=100, maximum=1, metadata={'context_id': 'original-context'})
            db.execute("UPDATE instances SET state='completed',output='{}' WHERE id=?", (identity,))
            db.execute('INSERT INTO allocations VALUES(?,?,?,?)', (identity, 'run', 'call', 100))
        return identity

    def test_archival_releases_history_capacity_preserving_dedup_and_allocations(self):
        store = WorkflowStore(self.state)
        identity = self.completed(store)
        with store.transaction() as db, self.assertRaisesRegex(ServiceError, 'workflow limit'):
            store.create_instance(db, 'alice', 'second', 'plan', 'definition', {}, wall_seconds=100, maximum=1)
        before = store.path.read_bytes()
        preview = archive_workflows(self.state, 0)
        self.assertTrue(preview['instances'][0]['eligible'])
        self.assertEqual(before, store.path.read_bytes())
        self.assertTrue(archive_workflows(self.state, 0, apply=True)['instances'][0]['archived'])
        with store.transaction() as db:
            old = store.submitted(db, 'alice', 'key')
            self.assertEqual(old['id'], identity)
            self.assertTrue(store.submission_matches(old, {'payload': 'private'}))
            self.assertFalse(store.submission_matches(old, {'payload': 'changed'}))
            self.assertIsNone(store.submitted(db, 'bob', 'key'))
            graph = store.graph(db, old)
            self.assertTrue(graph['archived'])
            self.assertFalse(graph['result_available'])
            self.assertEqual(graph['metadata'], {'context_id': 'original-context'})
            self.assertEqual(graph['usage']['reserved_microusd'], 100)
            self.assertEqual(db.execute('SELECT sum(amount) FROM allocations').fetchone()[0], 100)
            self.assertEqual(db.execute('SELECT count(*) FROM instances').fetchone()[0], 0)
            with self.assertRaisesRegex(ServiceError, 'event cursor expired'):
                store.events(db, identity, 0, 100)
            store.create_instance(db, 'alice', 'second', 'plan', 'definition', {}, wall_seconds=100, maximum=1)

    def test_thousand_historical_instances_archive_without_refunding_authority(self):
        # These terminal historical fixtures retain actual admitted run/call
        # allocations. Their creation is not a provider or throughput test.
        from mas_host.workflow_engine import WorkflowEngine
        state = self.root / 'long-history'
        ledger = DeploymentLedger(state, DeploymentPolicy(max_runs=1002, max_calls=1002,
            max_reserved_microusd=1002, per_call_reservation_microusd=1, min_free_bytes=1))
        source = {'format': 'mas-workflow/1', 'name': 'history', 'version': '1',
            'limits': {'max_steps': 1, 'max_parallel': 1, 'wall_seconds': 300,
                       'max_calls': 1, 'max_reserved_microusd': 1},
            'root': {'kind': 'transform', 'fields': {'value': {'path': ['value'], 'type': 'integer'}}}}
        engine = WorkflowEngine(state, {'history': source}, {}, {'alice': {'history'}}, operations=ledger)
        identities = []
        for number in range(1000):
            run = f'historical-{number}'
            ledger.admit_run(run, state / run, principal='alice')
            ledger.admit_call(run, 'call', 1)
            with engine.store.transaction() as db:
                identity = engine.store.create_instance(db, 'alice', str(number), 'history',
                    engine.bindings['history']['identity'], {'value': number}, wall_seconds=300, maximum=1000)
                db.execute("UPDATE instances SET state='completed',output='{}' WHERE id=?", (identity,))
                db.execute('INSERT INTO allocations VALUES(?,?,?,?)', (identity, run, 'call', 1))
            identities.append(identity)
        with self.assertRaisesRegex(ServiceError, 'workflow limit'):
            engine.submit('alice', 'next', 'history', {'value': 1000})
        before = ledger.health()['usage']
        self.assertEqual(before, {'runs': 1000, 'calls': 1000, 'reserved_microusd': 1000})
        archived = archive_workflows(state, 0, apply=True)['instances']
        self.assertEqual(sum(item['archived'] for item in archived), 1000)
        # Reopening both public owners cannot replenish historical allowance.
        ledger = DeploymentLedger(state)
        engine = WorkflowEngine(state, {'history': source}, {}, {'alice': {'history'}}, operations=ledger)
        fresh = engine.submit('alice', 'next', 'history', {'value': 1000})
        self.assertEqual(engine.run('alice', fresh['instance_id'])['output'], {'value': 1000})
        for number in (0, 499, 999):
            replay = engine.submit('alice', str(number), 'history', {'value': number})
            self.assertEqual(replay['instance_id'], identities[number])
            self.assertTrue(replay['archived'])
            with self.assertRaisesRegex(ServiceError, 'request conflict'):
                engine.submit('alice', str(number), 'history', {'value': -1})
        self.assertEqual(ledger.health()['usage'], before)
        with engine.store.transaction(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 1000)
            self.assertEqual(db.execute('SELECT count(*) FROM instances').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT sum(amount) FROM allocations').fetchone()[0], 1000)

    def test_unresolved_work_is_retained_even_under_terminal_parent(self):
        store = WorkflowStore(self.state)
        identity = self.completed(store)
        self.ledger.admit_run('r', self.state / 'r')
        with store.transaction() as db:
            db.execute("INSERT INTO attempts VALUES(?, 'root', 0, 'leaf', 'v1', 'r', '{}', 100, 'unknown', NULL, NULL)", (identity,))
        result = archive_workflows(self.state, 0, apply=True)['instances'][0]
        self.assertFalse(result['eligible'])
        self.assertFalse(result['archived'])

    def test_archival_preserves_continuation_key_and_conflict_identity(self):
        store = WorkflowStore(self.state)
        identity = self.completed(store)
        with store.transaction() as db:
            db.execute('INSERT INTO continuations(instance,request_key,path,input,revision) VALUES(?,?,?,?,?)',
                       (identity, 'approved', 'root/0', '{"approved":true}', 1))
        archive_workflows(self.state, 0, apply=True)
        with store.transaction(readonly=True) as db:
            old = store.continuation(db, identity, 'approved')
            self.assertEqual(old['revision'], 1)
            self.assertNotIn('input', old)
            self.assertTrue(store.continuation_matches(old, 'root/0', {'approved': True}))
            self.assertFalse(store.continuation_matches(old, 'root/0', {'approved': False}))
            self.assertFalse(store.continuation_matches(old, 'root/1', {'approved': True}))

    def test_composition_v1_migration_preserves_rows_and_adds_empty_metadata(self):
        with closing(sqlite3.connect(self.state / 'composition.sqlite')) as db:
            for statement in COMPOSITION_V1.statements:
                db.execute(statement)
            db.execute(f'PRAGMA application_id={COMPOSITION_V1.application_id}')
            db.execute('PRAGMA user_version=1')
            db.execute("INSERT INTO instances VALUES('flow-old','alice','key','plan','identity','{}','completed','{}',NULL,1,2,0)")
            db.commit()
        readonly = WorkflowStore(self.state, readonly=True)
        self.assertEqual(readonly.read_instance('flow-old')['metadata'], {})
        upgraded = WorkflowStore(self.state)
        self.assertEqual(upgraded.read_instance('flow-old')['metadata'], {})
        with closing(sqlite3.connect(upgraded.path)) as db:
            self.assertFalse(validate(db, COMPOSITION))
            with self.assertRaises(SchemaError):
                validate(db, COMPOSITION_V1)

    def test_failed_schema_migration_rolls_back_all_ddl(self):
        path = self.root / 'migration.sqlite'
        old = Schema('fixture', 123, 1, (), ('CREATE TABLE records(id TEXT)',))
        db = connect(path, old, initialise=lambda db: db.execute("INSERT INTO records VALUES('kept')"))
        db.close()
        bad = Schema('fixture', 123, 2, (), (*old.statements, 'CREATE TABLE added(id TEXT)'),
                     ((old, ('CREATE TABLE added(id TEXT)', 'INVALID SQL')),))
        with self.assertRaises(sqlite3.Error):
            connect(path, bad)
        with closing(sqlite3.connect(path)) as db:
            self.assertFalse(validate(db, old))
            self.assertEqual(db.execute('SELECT * FROM records').fetchall(), [('kept',)])


if __name__ == '__main__':
    unittest.main()
