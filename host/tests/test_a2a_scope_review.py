"""Trusted outbound identities remain isolated at an actual durable A2A peer."""
import copy
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome, TaskRequest
from mas_host.bridges.a2a import A2AAdapter, A2ABridge, A2AClient
from mas_host.bridges.remote import RemoteTaskService, RemoteTaskStore
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionBinding, ExecutionResult, Workflow
from test_remote_tasks import AdmittedContext
from test_runner import FunctionAdapter, topology


class PeerTransport:
    """A shared authenticated connection to the real peer protocol/service."""
    def __init__(self, bridge):
        self.bridge, self.calls = bridge, []

    def descriptor(self):
        return {'endpoint': 'https://peer.example/a2a'}

    def request(self, method, payload, *, headers):
        self.calls.append(copy.deepcopy(payload))
        return self.bridge.handle('shared-service-account', payload, headers)


class InterruptedExecutor:
    def execute(self, workflow, execution, **_options):
        value = execution['input']['value']
        return ExecutionResult('input_required' if value < 8 else
                               'auth_required' if value == 8 else 'completed', {'value': value})


class A2AOwnerScopeTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.local = self.root / 'local'
        self.local.mkdir(mode=0o700)
        self.context = AdmittedContext(self.local)
        self.addCleanup(self.context.journal.close)
        workflow = Workflow('echo/1', topology(),
            {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ('a', 'b', 'c')},
            'a', 'b', 'echo', continue_input=lambda _previous, supplied: supplied)
        self.peer = DurableTaskService(self.root / 'peer', {'echo': workflow},
            {'shared-service-account': {'echo'}}, executor=InterruptedExecutor())
        self.transport = PeerTransport(A2ABridge(self.peer, workflow='echo'))
        self.client = A2AClient(self.transport, client_identity='shared-service-account')

    def service(self, owner='alice', client=None):
        binding = ExecutionBinding(owner, 'local-flow', 'local-flow/1', 'task-' + owner,
                                   0, self.context.host, self.local)
        return RemoteTaskService(RemoteTaskStore(self.local), client or self.client,
                                 namespace=binding.remote_namespace('bridge'))

    def adapter(self, owner='alice', client=None):
        return A2AAdapter(self.service(owner, client), host_run_dir=self.context.host)

    @staticmethod
    def message(*, value=7, context='conversation', task=None):
        result = {'messageId': 'same-caller-message', 'role': 'ROLE_USER',
                  'parts': [{'data': {'workflow': 'echo', 'input': {'value': value}},
                             'mediaType': 'application/json'}]}
        if context is not None:
            result['contextId'] = context
        if task is not None:
            result['taskId'] = task
        return result

    def call(self, adapter, operation, **body):
        return adapter.handle(self.context,
            TaskRequest('a', 'bridge', 'a2a.' + operation, body, 'local-task', 'local-message')).output

    def submit(self, adapter, key='job', **message):
        return self.call(adapter, 'submit', key=key, message=self.message(**message))

    def test_shared_account_preserves_owner_scope_and_cancel_isolation(self):
        alice, bob = self.adapter('alice'), self.adapter('bob')
        first, second = self.submit(alice), self.submit(bob)
        self.assertNotEqual(first['remote_id'], second['remote_id'])
        self.assertNotEqual(first['context_id'], second['context_id'])
        self.assertEqual(len(self.peer.list_tasks('shared-service-account')), 2)
        self.assertEqual(self.call(bob, 'cancel', key='job', request_key='cancel')['state'], 'cancelled')
        self.assertEqual(self.call(alice, 'get', key='job')['state'], 'queued')
        sent = [call['params']['message'] for call in self.transport.calls if call['method'] == 'SendMessage']
        self.assertNotEqual(sent[0]['messageId'], sent[1]['messageId'])
        self.assertTrue(all(message['messageId'].startswith('mas-a2a-v1-') for message in sent))

    def test_task_keys_are_distinct_while_owner_context_alias_groups_tasks(self):
        adapter = self.adapter()
        first, second = self.submit(adapter, key='one'), self.submit(adapter, key='two')
        self.assertNotEqual(first['remote_id'], second['remote_id'])
        self.assertEqual(first['context_id'], second['context_id'])
        self.assertNotEqual(first['context_id'], 'conversation')
        self.assertEqual(len(self.peer.list_tasks('shared-service-account')), 2)

    def test_initial_peer_context_value_is_always_treated_as_local_alias(self):
        first = self.submit(self.adapter('alice'))
        second = self.submit(self.adapter('bob'), context=first['context_id'])
        self.assertNotEqual(first['context_id'], second['context_id'])
        self.assertNotEqual(first['remote_id'], second['remote_id'])

    def test_exact_retry_and_reopened_adapter_keep_binding_without_resubmission(self):
        adapter = self.adapter()
        original = self.message()
        first = self.call(adapter, 'submit', key='job', message=original)
        self.assertEqual(self.call(adapter, 'submit', key='job', message=original), first)
        self.assertEqual(self.call(self.adapter(), 'submit', key='job', message=original), first)
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(original, self.message())
        with self.assertRaisesRegex(ServiceError, 'idempotency conflict'):
            self.submit(self.adapter(), value=8)
        self.assertEqual(len(self.transport.calls), 1)

    def test_continuations_accept_original_alias_or_saved_context_and_scope_operation_ids(self):
        adapter = self.adapter()
        first = self.submit(adapter)
        self.peer.run_once()
        self.assertEqual(self.call(adapter, 'get', key='job')['state'], 'input_required')
        supplied = self.message(value=8, task=first['remote_id'])
        updated = self.call(adapter, 'update', key='job', request_key='turn-2', message=supplied)
        self.assertEqual(updated['context_id'], first['context_id'])
        # Reopening must reproduce the exact admitted operation binding.
        self.assertEqual(self.call(self.adapter(), 'update', key='job', request_key='turn-2', message=supplied), updated)
        self.peer.run_once()
        self.assertEqual(self.call(adapter, 'get', key='job')['state'], 'auth_required')
        before = len(self.transport.calls)
        with self.assertRaisesRegex(ServiceError, 'remote identity conflict'):
            self.call(adapter, 'update', key='job', request_key='bad-context',
                      message=self.message(value=9, context='someone-else'))
        self.assertEqual(len(self.transport.calls), before)
        self.call(adapter, 'update', key='job', request_key='turn-3',
                  message=self.message(value=9, context=first['context_id']))
        self.peer.run_once()
        self.assertEqual(self.call(adapter, 'get', key='job')['state'], 'completed')
        sent = [call['params']['message'] for call in self.transport.calls if call['method'] == 'SendMessage']
        self.assertEqual(len(sent), 3)
        self.assertEqual(len({message['messageId'] for message in sent}), 3)
        self.assertEqual({message['contextId'] for message in sent}, {first['context_id']})
        self.assertTrue(all(message['taskId'] == first['remote_id'] for message in sent[1:]))

    def test_omitted_context_stays_omitted_on_update(self):
        adapter = self.adapter()
        first = self.submit(adapter, context=None)
        self.peer.run_once()
        self.call(adapter, 'get', key='job')
        updated = self.call(adapter, 'update', key='job', request_key='turn-2',
                            message=self.message(value=8, context=None))
        self.assertEqual(updated['context_id'], first['context_id'])
        self.assertNotIn('contextId', self.transport.calls[0]['params']['message'])
        self.assertNotIn('contextId', self.transport.calls[-1]['params']['message'])

    def test_update_retry_stays_bound_when_reply_or_poll_first_establishes_context(self):
        original_request = self.transport.request
        lose_update_reply = False

        def request(method, payload, *, headers):
            response = original_request(method, payload, headers=headers)
            if payload['method'] == 'SendMessage':
                message = payload['params']['message']
                if 'taskId' not in message:
                    # A fast peer may pause before its first reply, while
                    # legitimately omitting the optional Task.contextId.
                    self.peer.run_once()
                    task = self.transport.bridge.handle('shared-service-account', {
                        'jsonrpc': '2.0', 'id': 'fixture', 'method': 'GetTask',
                        'params': {'id': response['result']['task']['id']}}, headers)['result']
                    task.pop('contextId')
                    response['result']['task'] = task
                elif lose_update_reply:
                    raise ServiceError('lost_response')
            return response

        self.transport.request = request
        for lost in (False, True):
            with self.subTest(lost_reply=lost):
                key = 'lost' if lost else 'received'
                adapter = self.adapter()
                first = self.submit(adapter, key=key, context=None)
                self.assertIsNone(first['context_id'])
                self.assertEqual(first['state'], 'input_required')
                lose_update_reply = lost
                kwargs = {'key': key, 'request_key': 'turn-2',
                          'message': self.message(value=8, context=None)}
                if lost:
                    with self.assertRaisesRegex(ServiceError, 'lost response'):
                        self.call(adapter, 'update', **kwargs)
                    learned = self.call(adapter, 'get', key=key)
                else:
                    learned = self.call(adapter, 'update', **kwargs)
                self.assertIsNotNone(learned['context_id'])
                before = len(self.transport.calls)
                if lost:
                    with self.assertRaisesRegex(ServiceError, 'remote outcome unknown'):
                        self.call(self.adapter(), 'update', **kwargs)
                else:
                    self.assertEqual(self.call(self.adapter(), 'update', **kwargs), learned)
                self.assertEqual(len(self.transport.calls), before)
                # Leave the independent peer queue empty before the next case.
                self.peer.run_once()

    def test_legacy_descriptor_cannot_dispatch_existing_receipt_under_new_profile(self):
        class LegacyClient(A2AClient):
            def descriptor(self):
                return super().descriptor() | {'contract': 'bounded-json-polling/1'}
        legacy = self.service(client=LegacyClient(self.transport, client_identity='shared-service-account'))
        legacy.submit('job', {'message': self.message()}, context=self.context,
                      host_run_dir=self.context.host, reservation_microusd=0)
        adapter = self.adapter()
        for action, body in (
                ('submit', {'message': self.message()}),
                ('get', {}), ('cancel', {'request_key': 'cancel'}),
                ('update', {'request_key': 'turn-2', 'message': self.message(value=8)})):
            with self.subTest(action=action), self.assertRaises(ServiceError) as raised:
                self.call(adapter, action, key='job', **body)
            self.assertEqual(raised.exception.code,
                'idempotency_conflict' if action == 'submit' else 'remote_identity_conflict')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.peer.list_tasks('shared-service-account')[0]['state'], 'queued')

    def test_task_references_and_forged_task_ids_do_not_grant_peer_authority(self):
        adapter = self.adapter()
        for operation, extra in (('submit', {}), ('update', {'request_key': 'turn-2'})):
            message = self.message() | {'referenceTaskIds': ['foreign-peer-task']}
            with self.subTest(operation=operation), self.assertRaisesRegex(ServiceError, 'unsupported capability'):
                self.call(adapter, operation, key='job', message=message, **extra)
        self.assertEqual(self.transport.calls, [])
        with self.assertRaisesRegex(ServiceError, 'invalid remote message'):
            self.submit(adapter, key='forged-submit', task='foreign-peer-task')
        self.assertEqual(self.transport.calls, [])
        self.submit(adapter)
        self.peer.run_once()
        self.call(adapter, 'get', key='job')
        before = len(self.transport.calls)
        with self.assertRaisesRegex(ServiceError, 'remote identity conflict'):
            self.call(adapter, 'update', key='job', request_key='forged-update',
                      message=self.message(value=8, task='foreign-peer-task'))
        self.assertEqual(len(self.transport.calls), before)


if __name__ == '__main__':
    unittest.main()
