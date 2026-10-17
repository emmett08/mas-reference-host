"""Opaque protocol identifiers survive JSON, persistence and exact-task routing."""
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome, TaskRequest
from mas_host.bridges.a2a import A2AAdapter, A2ABridge, A2AClient
from mas_host.bridges.remote import (RemoteSnapshot, RemoteTaskService, RemoteTaskStore,
                                     peer_identifier, token)
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from test_a2a_bridge import send, validate
from test_remote_tasks import AdmittedContext
from test_runner import FunctionAdapter, topology


PEER_TASK = 'tasks/雪/one\n\x00'
PEER_CONTEXT = 'contexts/雪/one\n\x00'


class OpaquePeer:
    state = 'TASK_STATE_WORKING'
    lose_submit = False

    def __init__(self):
        self.calls = []

    def descriptor(self):
        return {'endpoint': 'https://peer.example/a2a'}

    def request(self, method, payload, **kwargs):
        self.calls.append(payload)
        if self.lose_submit and payload['method'] == 'SendMessage':
            raise ServiceError('lost_response')
        task = {'id': PEER_TASK, 'contextId': PEER_CONTEXT,
                'status': {'state': self.state},
                'artifacts': [{'artifactId': 'artifacts/雪\n\x00', 'parts': [{'text': 'answer'}]}]}
        validate('Task', task)
        return {'jsonrpc': '2.0', 'id': payload['id'],
                'result': {'task': task} if payload['method'] == 'SendMessage' else task}


class PeerIdentifierTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.context = AdmittedContext(self.root)
        self.addCleanup(self.context.journal.close)
        self.options = {'context': self.context, 'host_run_dir': self.context.host, 'reservation_microusd': 0}
        self.peer = OpaquePeer()
        self.client = A2AClient(self.peer, client_identity='test')
        self.service = RemoteTaskService(RemoteTaskStore(self.root), self.client, namespace='test')
        self.payload = {'message': {'messageId': 'message/雪\n\x00', 'role': 'ROLE_USER',
                                    'parts': [{'text': 'question'}]}}

    def test_opaque_peer_ids_persist_and_route_all_lifecycle_operations_exactly(self):
        first = self.service.submit('job', self.payload, **self.options)
        self.assertEqual((first['remote_id'], first['context_id']), (PEER_TASK, PEER_CONTEXT))
        self.service = RemoteTaskService(RemoteTaskStore(self.root), self.client, namespace='test')
        self.assertEqual(self.service.status('job'), first)
        self.peer.state = 'TASK_STATE_INPUT_REQUIRED'
        self.service.poll('job', **self.options)
        self.assertEqual(self.peer.calls[-1]['params']['id'], PEER_TASK)
        self.peer.state = 'TASK_STATE_WORKING'
        update = {'message': self.payload['message'] | {'messageId': 'message/雪/next', 'contextId': PEER_CONTEXT}}
        self.service.update('job', request_key='next', payload=update, **self.options)
        self.assertEqual(self.peer.calls[-1]['params']['message']['taskId'], PEER_TASK)
        self.assertEqual(self.peer.calls[-1]['params']['message']['contextId'], PEER_CONTEXT)
        self.peer.state = 'TASK_STATE_CANCELED'
        cancelled = self.service.cancel('job', **self.options)
        self.assertEqual(self.peer.calls[-1]['params']['id'], PEER_TASK)
        self.assertEqual((cancelled['state'], cancelled['remote_id']), ('cancelled', PEER_TASK))

    def test_evidenced_attach_accepts_opaque_peer_id_without_new_submission(self):
        self.peer.lose_submit = True
        with self.assertRaisesRegex(ServiceError, 'lost response'):
            self.service.submit('job', self.payload, **self.options)
        self.peer.lose_submit = False
        receipt = self.service.attach('job', PEER_TASK, evidence={'receipt': 'operator-checked'}, **self.options)
        self.assertEqual(receipt['remote_id'], PEER_TASK)
        self.assertEqual([call['method'] for call in self.peer.calls], ['SendMessage', 'GetTask'])

    def test_byte_bound_encoding_and_local_key_restrictions_are_separate(self):
        self.assertEqual(peer_identifier('雪' * 341), '雪' * 341)
        self.assertEqual(peer_identifier(PEER_TASK), PEER_TASK)
        for invalid in ('', '雪' * 342, '\ud800', None, True):
            with self.subTest(invalid=repr(invalid)), self.assertRaises(ServiceError):
                peer_identifier(invalid)
        with self.assertRaises(ServiceError):
            token(PEER_TASK)
        self.assertEqual(RemoteSnapshot(PEER_TASK, 'running', context_id=PEER_CONTEXT).validate().remote_id, PEER_TASK)

    def make_local_service(self):
        class Executor:
            def execute(self, workflow, execution, **kwargs):
                value = execution['input']['value']
                return ExecutionResult('input_required' if value == 7 else 'completed', {'value': value})
        workflow = Workflow('opaque/1', topology(),
            {actor: FunctionAdapter(lambda _c, request: Outcome(request.input)) for actor in ('a', 'b', 'c')},
            'a', 'b', 'echo', continue_input=lambda previous, supplied: supplied)
        return DurableTaskService(self.root / 'inbound', {'echo': workflow}, {'alice': {'echo'}}, executor=Executor())

    def test_inbound_opaque_message_and_context_ids_deduplicate_and_continue(self):
        service = self.make_local_service()
        bridge = A2ABridge(service, workflow='echo')
        message_id = 'message/雪\n\x00' + 'x' * 500
        request = send(message_id=message_id, context_id=PEER_CONTEXT)
        validate('Send Message Request', request['params'])
        first = bridge.handle('alice', request)['result']['task']
        self.assertEqual(first['contextId'], PEER_CONTEXT)
        self.assertEqual(bridge.handle('alice', request)['result']['task']['id'], first['id'])
        self.assertEqual(bridge.handle('alice', send(message_id=message_id, context_id=PEER_CONTEXT, value=9))['error']['code'], -32602)
        service.run_once()
        updated = send(message_id='next/雪\n\x00', context_id=PEER_CONTEXT, task_id=first['id'], value=8)
        bridge.handle('alice', updated)
        service.run_once()
        self.assertEqual(bridge.handle('alice', updated)['result']['task']['status']['state'], 'TASK_STATE_COMPLETED')
        listing = bridge.handle('alice', {'jsonrpc': '2.0', 'id': 'list', 'method': 'ListTasks',
                                         'params': {'contextId': PEER_CONTEXT}})['result']
        self.assertEqual([task['id'] for task in listing['tasks']], [first['id']])
        legacy = bridge.handle('alice', send())['result']['task']
        self.assertEqual(service.inspect('alice', legacy['id'])['request_key'], 'a2a:first')

    def test_adapter_scopes_opaque_caller_ids_before_real_peer_dispatch(self):
        service = self.make_local_service()
        bridge = A2ABridge(service, workflow='echo')
        class Transport:
            def descriptor(self):
                return {'endpoint': 'https://peer.example/a2a'}
            def request(self, method, payload, **kwargs):
                return bridge.handle('alice', payload)
        remote = RemoteTaskService(RemoteTaskStore(self.root), A2AClient(Transport(), client_identity='test'), namespace='adapter')
        adapter = A2AAdapter(remote, host_run_dir=self.context.host)
        message = send(message_id='message/雪\n\x00', context_id=PEER_CONTEXT)['params']['message']
        receipt = adapter.handle(self.context, TaskRequest('a', 'b', 'a2a.submit',
            {'key': 'job', 'message': message}, 'task', 'message')).output
        self.assertNotEqual(receipt['context_id'], PEER_CONTEXT)
        self.assertEqual(service.list_tasks('alice')[0]['task_id'], receipt['remote_id'])
