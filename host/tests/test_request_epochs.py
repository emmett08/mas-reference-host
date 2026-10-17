"""Independent versioned keys and interruption-safe epoch retirement."""
from contextlib import closing
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mas_host.operations import backup_state, restore_state
from mas_host.task_history import archive_tasks
from mas_host.request_namespaces import RequestScope, VERSIONED
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService
from mas_host.task_store import SERVICE_V3, TaskStore
from mas_host.unified_service import UnifiedTaskService
from mas_host.workflow_store import COMPOSITION_V4, WorkflowStore
from test_definition_versions import Executor, registered, source


class RequestEpochTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workflow = registered()
        self.executor = Executor()
        self.service = self.build()

    def build(self):
        return UnifiedTaskService(self.root / 'state', {'leaf': self.workflow}, {'plan': source()},
            {'alice': {'leaf', 'plan'}, 'bob': {'leaf', 'plan'}}, executor=self.executor)

    def test_new_unified_namespace_and_explicit_epoch_never_alias_legacy_strings(self):
        legacy = self.service.leaves.submit('alice', 'mas-unified/1:release:key', 'leaf', {'v': 1})
        retry = self.service.submit('alice', 'mas-unified/1:release:key', 'leaf', {'v': 1})
        self.assertEqual(legacy['task_id'], retry['task_id'])
        self.assertEqual(retry['request_namespace'], 'legacy')
        versioned = self.service.submit('alice', 'new', 'leaf', {})
        self.assertEqual(versioned['request_namespace'], VERSIONED)
        first = self.service.submit('alice', 'mas-unified/1:release:key', 'leaf', {}, request_epoch='release')
        second = self.service.submit('alice', 'mas-unified/1:release:key', 'plan', {}, request_epoch='later')
        self.assertNotEqual(first['task_id'], legacy['task_id'])
        self.assertNotEqual(second['task_id'], first['task_id'])
        with self.assertRaisesRegex(ServiceError, 'request conflict'):
            self.service.submit('alice', 'mas-unified/1:release:key', 'plan', {}, request_epoch='release')
        self.assertEqual(first, self.service.submit('alice', 'mas-unified/1:release:key', 'leaf', {}, request_epoch='release'))

    def test_retirement_blocks_duplicates_new_keys_and_direct_owner_ingress(self):
        receipt = self.service.submit('alice', 'key', 'leaf', {}, request_epoch='release')
        with self.assertRaisesRegex(ServiceError, 'epoch in use'):
            self.service.retire_request_epoch('alice', 'release', reason='release closed')
        self.service.cancel('alice', receipt['task_id'])
        retired = self.service.retire_request_epoch('alice', 'release', reason='release closed', authoriser='operator')
        self.assertEqual(retired['state'], 'retired')
        self.assertEqual(retired['authoriser'], 'operator')
        for action in (
            lambda: self.service.submit('alice', 'key', 'leaf', {}, request_epoch='release'),
            lambda: self.service.submit('alice', 'other', 'plan', {}, request_epoch='release'),
            lambda: self.service.leaves.submit('alice', 'direct', 'leaf', {}, request_epoch='release'),
            lambda: self.service.compositions.submit('alice', 'direct', 'plan', {}, request_epoch='release'),
            lambda: self.service.update('alice', receipt['task_id'], 'later', {}),
        ):
            with self.assertRaisesRegex(ServiceError, 'epoch retired'):
                action()
        self.assertEqual(self.service.inspect('alice', receipt['task_id'])['state'], 'cancelled')
        other = self.service.submit('bob', 'key', 'leaf', {}, request_epoch='release')
        self.assertEqual(other['state'], 'queued')

    def test_retirement_intent_survives_crash_between_owner_acknowledgements(self):
        import mas_host.request_namespaces as namespaces
        receipt = self.service.submit('alice', 'key', 'leaf', {}, request_epoch='release')
        self.service.cancel('alice', receipt['task_id'])
        original = namespaces.retire_owner
        def fail_second(store, kind, principal, scope, *, prune):
            if kind == 'plan':
                raise RuntimeError('crash after first owner acknowledgement')
            return original(store, kind, principal, scope, prune=prune)
        with mock.patch.object(namespaces, 'retire_owner', side_effect=fail_second):
            with self.assertRaises(RuntimeError):
                self.service.retire_request_epoch('alice', 'release', reason='closed')
        self.assertEqual(self.service.request_epoch_status('alice', 'release')['state'], 'closing')
        # The composition owner is still missing its retirement row. The
        # committed central intent nevertheless rejects direct submission.
        with self.assertRaisesRegex(ServiceError, 'epoch retired'):
            self.service.compositions.submit('alice', 'direct', 'plan', {}, request_epoch='release')
        resumed = self.build()
        self.assertEqual(resumed.request_epoch_status('alice', 'release')['state'], 'retired')
        with resumed.compositions.store.transaction(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM request_epoch_retirements').fetchone()[0], 1)
        self.assertEqual(resumed.inspect('alice', receipt['task_id'])['task_id'], receipt['task_id'])

    def test_terminal_parent_with_unknown_child_cannot_retire(self):
        receipt = self.service.submit('alice', 'key', 'leaf', {}, request_epoch='release')
        with self.service.leaves.store.transaction() as db:
            db.execute("UPDATE tasks SET state='failed' WHERE id=?", (receipt['task_id'],))
            db.execute("UPDATE executions SET state='unknown' WHERE task_id=?", (receipt['task_id'],))
        with self.assertRaisesRegex(ServiceError, 'epoch in use'):
            self.service.retire_request_epoch('alice', 'release', reason='unsafe')

    def test_compaction_only_for_archived_retired_epoch_and_legacy_retained(self):
        legacy = self.service.leaves.submit('alice', 'old', 'leaf', {})
        scoped = self.service.submit('alice', 'new', 'leaf', {}, request_epoch='release')
        for receipt in (legacy, scoped):
            self.service.cancel('alice', receipt['task_id'])
        archive_tasks(self.root / 'state', 0, True)
        self.service.retire_request_epoch('alice', 'release', reason='closed', prune=True)
        with self.service.leaves.store.transaction(readonly=True) as db:
            rows = db.execute('SELECT request_key,request_namespace FROM requests').fetchall()
        self.assertEqual([tuple(row) for row in rows], [('old', 'legacy')])
        self.assertEqual(self.service.inspect('alice', scoped['task_id'])['task_id'], scoped['task_id'])
        self.assertEqual(self.service.leaves.submit('alice', 'old', 'leaf', {})['task_id'], legacy['task_id'])

    def test_invalid_epoch_values_fail_without_submission(self):
        for epoch in ('', False, 3, [], '\n', 'x' * 129):
            with self.subTest(epoch=repr(epoch)), self.assertRaises(ServiceError):
                self.service.submit('alice', 'key', 'leaf', {}, request_epoch=epoch)
        with self.assertRaises(ServiceError):
            self.service.retire_request_epoch('alice', None, reason='legacy')

    def test_task_v3_and_composition_v4_migrations_preserve_original_keys(self):
        task_state = self.root / 'old-tasks'
        task_state.mkdir()
        with closing(connect(task_state / 'task-service.sqlite', SERVICE_V3,
                initialise=lambda db: db.execute("INSERT INTO metadata VALUES('deployment_id','original')"))) as db:
            with db:
                db.execute("INSERT INTO tasks(id,principal,request_key,workflow,workflow_identity,state,input,metadata,created_at,updated_at) VALUES('task-original','alice','arbitrary:key','leaf','identity','cancelled','{}','{}',1,1)")
                db.execute("INSERT INTO requests VALUES('alice','arbitrary:key','digest','task-original','submit')")
        with TaskStore(task_state, deployment_id='original').transaction(readonly=True) as db:
            task = dict(db.execute('SELECT * FROM tasks').fetchone())
            request = dict(db.execute('SELECT * FROM requests').fetchone())
        self.assertEqual((task['id'], request['request_key'], request['digest']),
                         ('task-original', 'arbitrary:key', 'digest'))
        self.assertEqual((request['request_namespace'], request['request_epoch']), ('legacy', ''))
        plan_state = self.root / 'old-plans'
        plan_state.mkdir()
        with closing(connect(plan_state / 'composition.sqlite', COMPOSITION_V4, initialise=lambda _: None)) as db:
            with db:
                db.execute("INSERT INTO instances(id,principal,request_key,plan,identity,input,state,created,deadline) VALUES('flow-original','alice','arbitrary:key','plan','identity','{}','cancelled',1,2)")
        with WorkflowStore(plan_state).transaction(readonly=True) as db:
            plan = WorkflowStore.submitted(db, 'alice', 'arbitrary:key')
        self.assertEqual((plan['id'], plan['request_namespace'], plan['request_epoch']),
                         ('flow-original', 'legacy', ''))

    def test_plan_discovery_returns_enforced_input_contract(self):
        specification = source()
        specification['input_schema'] = {'type': 'object', 'required': ['question'],
                                         'properties': {'question': {'type': 'string'}}}
        service = UnifiedTaskService(self.root / 'schema-state', {'leaf': self.workflow}, {'plan': specification},
                                     {'alice': {'plan'}}, executor=self.executor)
        contract = service.discover('alice')['registrations'][0]['contract']
        self.assertEqual(contract['input_schema'], specification['input_schema'])
        with self.assertRaisesRegex(ServiceError, 'input schema mismatch'):
            service.submit('alice', 'invalid', 'plan', {})
        accepted = service.submit('alice', 'valid', 'plan', {'question': 'What changed?'})
        self.assertEqual(accepted['state'], 'queued')

    def test_plan_output_contract_is_checked_after_execution(self):
        specification = source()
        specification['root'] = {'kind': 'transform', 'fields': {'answer': {'literal': 1}}}
        specification['output_schema'] = {'type': 'object', 'required': ['verified'],
                                         'properties': {'verified': {'const': True}}}
        service = UnifiedTaskService(self.root / 'output-state', {}, {'plan': specification},
                                     {'alice': {'plan'}}, executor=self.executor)
        accepted = service.submit('alice', 'output', 'plan', {})
        result = service.run_once(accepted['task_id'])
        self.assertEqual(result['state'], 'failed')
        with service.compositions.store.transaction(readonly=True) as db:
            root = db.execute("SELECT error FROM nodes WHERE path='root'").fetchone()[0]
        self.assertIn('workflow_output_schema_mismatch', root)

    def test_http_mcp_a2a_forward_explicit_epoch(self):
        from mas_host.bridges.server import _service_call
        from mas_host.bridges.mcp import MCPBridge
        from mas_host.bridges.a2a import A2ABridge
        from test_mcp_bridge import request as mcp_request
        http = _service_call(self.service, 'alice', 'submit',
            {'request_key': 'http', 'workflow': 'leaf', 'input': {}, 'request_epoch': 'http-epoch'})
        self.assertEqual(http['request_epoch'], 'http-epoch')
        mcp = MCPBridge(self.service).handle('alice', mcp_request('tools/call',
            {'name': 'mas.submit', 'arguments': {'request_key': 'mcp', 'workflow': 'leaf',
                                               'input': {}, 'request_epoch': 'mcp-epoch'}}))
        self.assertNotIn('error', mcp, mcp)
        self.assertEqual(self.service.inspect('alice', mcp['result']['taskId'])['request_epoch'], 'mcp-epoch')
        a2a = A2ABridge(self.service, endpoint='http://127.0.0.1/a2a', allow_http=True).handle('alice',
            {'jsonrpc': '2.0', 'id': 1, 'method': 'SendMessage', 'params': {
                'message': {'messageId': 'a2a', 'role': 'ROLE_USER', 'parts': [
                    {'mediaType': 'application/json', 'data': {'workflow': 'leaf', 'input': {}, 'request_epoch': 'a2a-epoch'}}]},
                'configuration': {'returnImmediately': True}}})
        self.assertNotIn('error', a2a, a2a)
        self.assertEqual(self.service.inspect('alice', a2a['result']['task']['id'])['request_epoch'], 'a2a-epoch')

    def test_backup_preserves_retirement_and_audit_history(self):
        receipt = self.service.submit('alice', 'key', 'leaf', {}, request_epoch='release')
        self.service.cancel('alice', receipt['task_id'])
        self.service.retire_request_epoch('alice', 'release', reason='closed')
        backup_state(self.root / 'state', self.root / 'backup')
        restore_state(self.root / 'backup', self.root / 'restored')
        restored = UnifiedTaskService(self.root / 'restored', {}, {}, {'alice': {'leaf'}}, executor=self.executor)
        self.assertEqual(restored.request_epoch_status('alice', 'release')['state'], 'retired')
        self.assertEqual(restored.inspect('alice', receipt['task_id'])['state'], 'cancelled')


if __name__ == '__main__':
    unittest.main()
