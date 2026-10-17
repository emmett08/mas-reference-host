"""Archival frees payload capacity without forgetting submission identities."""
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from mas_host.operations import DeploymentLedger, compact_history
from mas_host.schema import validate
from mas_host.service_contracts import ServiceError
from mas_host.task_history import archive_tasks
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from mas_host.task_store import SERVICE, SERVICE_V1, TaskStore


class Adapter:
    def handle(self, _context, _request):
        raise AssertionError('injected executor')


class Executor:
    def execute(self, *_args, **_kwargs):
        return ExecutionResult('completed', {'large': 'payload'})


class TaskHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / 'state'
        self.workflow = Workflow('1', {'agents': ['a', 'b']}, {'a': Adapter(), 'b': Adapter()}, 'a', 'b', 'echo')

    def service(self, **kwargs):
        return DurableTaskService(self.state, {'leaf': self.workflow}, {'alice': {'leaf'}, 'bob': {'leaf'}},
                                  executor=Executor(), max_tasks=1, **kwargs)

    def test_capacity_reopens_with_original_key_and_receipt_retained(self):
        service = self.service()
        first = service.submit('alice', 'key', 'leaf', {'private': 1}, metadata={'context_id': 'ctx'})
        self.assertEqual(service.run_once()['state'], 'completed')
        with self.assertRaises(ServiceError) as error:
            service.submit('alice', 'second', 'leaf', {})
        self.assertEqual(error.exception.code, 'task_limit')
        before = service.store.path.read_bytes()
        self.assertTrue(archive_tasks(self.state, 0)['tasks'][0]['eligible'])
        self.assertEqual(before, service.store.path.read_bytes())
        self.assertTrue(archive_tasks(self.state, 0, True)['tasks'][0]['archived'])
        receipt = service.submit('alice', 'key', 'leaf', {'private': 1}, metadata={'context_id': 'ctx'})
        self.assertEqual(receipt['task_id'], first['task_id'])
        self.assertEqual(receipt['state'], 'completed')
        self.assertTrue(receipt['archived'])
        self.assertFalse(receipt['result_available'])
        self.assertEqual(receipt['metadata'], {'context_id': 'ctx'})
        self.assertEqual(service.list_tasks('alice')[0], receipt)
        for operation in (lambda: service.result('alice', first['task_id']),
                          lambda: service.events('alice', first['task_id'], 0),
                          lambda: service.events_page('alice', first['task_id'], 0),
                          lambda: service.inspect('bob', first['task_id']),
                          lambda: service.submit('alice', 'key', 'leaf', {'private': 2})):
            with self.assertRaises(ServiceError):
                operation()
        self.assertNotEqual(service.submit('alice', 'second', 'leaf', {})['task_id'], first['task_id'])
        with service.store.transaction(readonly=True) as db:
            before_compaction = tuple(db.iterdump())
        self.assertTrue(compact_history(self.state)['ok'])
        with service.store.transaction(readonly=True) as db:
            self.assertEqual(tuple(db.iterdump()), before_compaction)
            archived = db.execute('SELECT input,result FROM tasks WHERE id=?', (first['task_id'],)).fetchone()
            self.assertEqual(tuple(archived), ('{}', None))
            self.assertEqual(db.execute('SELECT count(*) FROM requests WHERE task_id=?', (first['task_id'],)).fetchone()[0], 1)

    def test_unknown_child_cannot_be_hidden_by_terminal_parent(self):
        service = self.service()
        first = service.submit('alice', 'key', 'leaf', {})
        with service.store.transaction() as db:
            db.execute("UPDATE tasks SET state='failed' WHERE id=?", (first['task_id'],))
            db.execute("UPDATE executions SET state='unknown' WHERE task_id=?", (first['task_id'],))
        result = archive_tasks(self.state, 0, True)['tasks'][0]
        self.assertFalse(result['eligible'])
        self.assertFalse(result['archived'])

    def test_cancelled_never_started_task_archives_without_a_run(self):
        service = self.service()
        first = service.submit('alice', 'key', 'leaf', {})
        service.cancel('alice', first['task_id'])
        self.assertTrue(archive_tasks(self.state, 0, True)['tasks'][0]['archived'])
        self.assertEqual(service.operations.health()['usage']['runs'], 0)

    def test_v1_reader_and_writer_migration_preserve_request_records(self):
        ledger = DeploymentLedger(self.state)
        path = self.state / 'task-service.sqlite'
        with closing(sqlite3.connect(path)) as db:
            for statement in SERVICE_V1.statements:
                db.execute(statement)
            db.execute(f'PRAGMA application_id={SERVICE_V1.application_id}')
            db.execute('PRAGMA user_version=1')
            db.execute('INSERT INTO metadata VALUES(?,?)', ('deployment_id', ledger.deployment_id))
            db.execute("INSERT INTO requests VALUES('alice','key','digest','task-old','submit')")
            db.commit()
        before = path.read_bytes()
        TaskStore(self.state, deployment_id=ledger.deployment_id, readonly=True)
        self.assertEqual(path.read_bytes(), before)
        store = TaskStore(self.state, deployment_id=ledger.deployment_id)
        with store.transaction(readonly=True) as db:
            self.assertFalse(validate(db, SERVICE))
            self.assertEqual(db.execute('SELECT digest FROM requests').fetchone()[0], 'digest')


if __name__ == '__main__':
    unittest.main()
