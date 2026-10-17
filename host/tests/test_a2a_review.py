"""Independent regressions for valid A2A messages at durable boundaries."""
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.bridges.a2a import A2ABridge, A2AClient
from mas_host.bridges.remote import RemoteTaskService, RemoteTaskStore
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from test_a2a_bridge import send, validate
from test_remote_tasks import AdmittedContext
from test_runner import FunctionAdapter, topology


class TaskTransport:
    def __init__(self):
        self.calls = []
        self.state = 'TASK_STATE_WORKING'
        self.context = None

    def descriptor(self):
        return {'endpoint': 'https://peer.example/a2a'}

    def request(self, method, payload, **kwargs):
        self.calls.append(payload['method'])
        task = {'id': 'peer-task-1', 'status': {'state': self.state}}
        if self.context is not None:
            task['contextId'] = self.context
        validate('Task', task)
        result = {'task': task} if payload['method'] == 'SendMessage' else task
        return {'jsonrpc': '2.0', 'id': payload['id'], 'result': result}


class A2AContractReviewTests(unittest.TestCase):
    def test_optional_task_context_preserves_identity_through_lifecycle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            admitted = AdmittedContext(root)
            self.addCleanup(admitted.journal.close)
            transport = TaskTransport()
            client = A2AClient(transport, client_identity='review')
            service = RemoteTaskService(RemoteTaskStore(root), client, namespace='review')
            options = {'context': admitted, 'host_run_dir': admitted.host, 'reservation_microusd': 0}
            payload = {'message': {'messageId': 'first', 'role': 'ROLE_USER', 'parts': [{'text': 'question'}]}}
            receipt = service.submit('job', payload, **options)
            self.assertEqual((receipt['remote_id'], receipt['state'], receipt['pending_action']),
                             ('peer-task-1', 'running', None))
            self.assertIsNone(receipt['context_id'])
            # A later observation may establish context; subsequent omissions
            # must preserve it, while a conflicting supplied context is rejected.
            transport.state, transport.context = 'TASK_STATE_AUTH_REQUIRED', 'context-1'
            self.assertEqual(service.poll('job', **options)['context_id'], 'context-1')
            transport.state, transport.context = 'TASK_STATE_WORKING', None
            continuation = {'message': payload['message'] | {'messageId': 'second'}}
            updated = service.update('job', request_key='turn-2', payload=continuation, **options)
            self.assertEqual((updated['state'], updated['context_id']), ('running', 'context-1'))
            transport.context = 'wrong-context'
            with self.assertRaisesRegex(ServiceError, 'remote identity conflict'):
                service.poll('job', **options)
            self.assertEqual(service.status('job')['context_id'], 'context-1')
            transport.state, transport.context = 'TASK_STATE_CANCELED', None
            self.assertEqual(service.cancel('job', **options)['state'], 'cancelled')
            self.assertEqual(transport.calls.count('SendMessage'), 2)

    def test_explicit_request_context_still_requires_matching_reply(self):
        client = A2AClient(TaskTransport(), client_identity='review')
        with self.assertRaisesRegex(ServiceError, 'remote identity conflict'):
            client.submit(send(context_id='requested-context')['params'])

    def make_service(self, root):
        class UnknownExecutor:
            def execute(self, *args, **kwargs):
                return ExecutionResult('unknown', error={'code': 'effect_unknown'})
        workflow = Workflow('review/1', topology(),
            {actor: FunctionAdapter(lambda _c, request: Outcome(request.input)) for actor in ('a', 'b', 'c')},
            'a', 'b', 'echo')
        return DurableTaskService(root, {'echo': workflow}, {'alice': {'echo'}}, executor=UnknownExecutor())

    def test_blocking_unknown_is_deadline_error_and_polling_still_returns_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            service = self.make_service(Path(directory))
            bridge = A2ABridge(service, workflow='echo', wait_seconds=1)
            receipt = bridge.handle('alice', send())['result']['task']
            service.run_once()
            result = bridge.handle('alice', send(immediate=False))
            self.assertEqual(result['error']['message'], 'response_deadline_reached_repeat_same_message')
            polled = bridge.handle('alice', send())['result']['task']
            self.assertEqual(polled['id'], receipt['id'])
            self.assertEqual(polled['status']['state'], 'TASK_STATE_UNSPECIFIED')
            self.assertEqual(len(service.list_tasks('alice')), 1)

    def test_cancel_accepts_official_optional_metadata_and_keeps_principal_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            service = self.make_service(Path(directory))
            bridge = A2ABridge(service, workflow='echo')
            receipt = bridge.handle('alice', send())['result']['task']
            params = {'id': receipt['id'], 'metadata': {'reason': 'operator'}}
            validate('Cancel Task Request', params)
            request = {'jsonrpc': '2.0', 'id': 2, 'method': 'CancelTask', 'params': params}
            self.assertEqual(bridge.handle('bob', request)['error']['code'], -32001)
            result = bridge.handle('alice', request)['result']
            validate('Task', result)
            self.assertEqual(result['status']['state'], 'TASK_STATE_CANCELED')
            self.assertEqual(bridge.handle('alice', request | {'params': params | {'metadata': []}})['error']['code'], -32602)
