"""A2A 1.0 wire fixtures checked against the pinned official schema bundle."""
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import jsonschema
from referencing import Registry, Resource

from mas_host import Outcome
from mas_host.bridges.a2a import A2ABridge, A2AClient
from mas_host.bridges.remote_http import BoundedJSONHTTP
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


BUNDLE = Path(__file__).with_name('fixtures') / 'a2a-v1.0.1.schema.json'
SCHEMAS = json.loads(BUNDLE.read_text())['definitions']
REGISTRY = Registry().with_resources([
    (('google.protobuf.' if name in ('Struct', 'Timestamp', 'Value') else 'lf.a2a.v1.')
     + name.replace(' ', '') + '.jsonschema.json', Resource.from_contents(schema))
    for name, schema in SCHEMAS.items()
])


def validate(name, instance):
    jsonschema.Draft202012Validator(SCHEMAS[name], registry=REGISTRY).validate(instance)


def send(message_id='first', value=7, *, task_id=None, context_id=None, immediate=True):
    message = {'messageId': message_id, 'role': 'ROLE_USER',
               'parts': [{'data': {'workflow': 'echo', 'input': {'value': value}}, 'mediaType': 'application/json'}]}
    if task_id:
        message['taskId'] = task_id
    if context_id:
        message['contextId'] = context_id
    return {'jsonrpc': '2.0', 'id': 1, 'method': 'SendMessage',
            'params': {'message': message, 'configuration': {'returnImmediately': immediate}}}


class A2ABridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.workflow = Workflow('echo/1', topology(),
            {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ('a', 'b', 'c')}, 'a', 'b', 'echo')
        self.service = self.make_service()
        self.bridge = A2ABridge(self.service, endpoint='https://mas.example/a2a', workflow='echo', wait_seconds=1)

    def make_service(self, workflow=None):
        return DurableTaskService(self.state, {'echo': workflow or self.workflow},
                                  {'alice': {'echo'}, 'bob': {'echo'}},
                                  mas_bin=self.mas, gateway_bin=self.gateway, supervised=False)

    def call(self, request, principal='alice'):
        return self.bridge.handle(principal, request, {'A2A-Version': '1.0'})

    def test_agent_card_validates_official_schema_and_advertises_actual_capabilities(self):
        card = self.bridge.agent_card()
        validate('Agent Card', card)
        self.assertEqual(card['supportedInterfaces'][0]['protocolVersion'], '1.0')
        self.assertFalse(card['capabilities']['streaming'])
        self.assertFalse(card['capabilities']['pushNotifications'])
        self.assertIn('securityRequirements', card)
        self.assertNotIn('security', card)

    def test_send_poll_artifact_and_restart_match_official_schema(self):
        request = send(context_id='client-context')
        validate('Send Message Request', request['params'])
        result = self.call(request)['result']
        validate('Send Message Response', result)
        task_id = result['task']['id']
        self.assertEqual(result['task']['status']['state'], 'TASK_STATE_SUBMITTED')
        self.assertEqual(result['task']['contextId'], 'client-context')
        self.service.run_once()
        self.bridge = A2ABridge(self.make_service(), endpoint='https://mas.example/a2a')
        polled = self.call({'jsonrpc': '2.0', 'id': 2, 'method': 'GetTask', 'params': {'id': task_id, 'historyLength': 0}})['result']
        validate('Task', polled)
        self.assertEqual(polled['status']['state'], 'TASK_STATE_COMPLETED')
        self.assertEqual(polled['artifacts'][0]['parts'][0]['data'], {'value': 7})
        self.assertNotIn('history', polled)
        self.assertEqual(self.call(request)['result']['task']['id'], task_id)

    def test_same_message_id_changed_payload_or_context_conflicts(self):
        self.call(send(context_id='one'))
        self.assertEqual(self.call(send(value=8, context_id='one'))['error']['code'], -32602)
        self.assertEqual(self.call(send(context_id='two'))['error']['code'], -32602)
        self.assertEqual(len(self.service.list_tasks('alice')), 1)

    def test_principal_is_not_a_body_claim(self):
        task = self.call(send())['result']['task']
        denied = self.call({'jsonrpc': '2.0', 'id': 2, 'method': 'GetTask', 'params': {'id': task['id']}}, 'bob')
        self.assertEqual(denied['error']['code'], -32001)
        other = self.call(send(), 'bob')['result']['task']
        self.assertNotEqual(other['id'], task['id'])

    def test_cancellation_and_known_unsupported_capabilities(self):
        task = self.call(send())['result']['task']
        result = self.call({'jsonrpc': '2.0', 'id': 3, 'method': 'CancelTask', 'params': {'id': task['id']}})['result']
        validate('Task', result)
        self.assertEqual(result['status']['state'], 'TASK_STATE_CANCELED')
        for method, code in [('SendStreamingMessage', -32004), ('SubscribeToTask', -32004),
                             ('CreateTaskPushNotificationConfig', -32003), ('message/send', -32601)]:
            self.assertEqual(self.call({'jsonrpc': '2.0', 'id': 3, 'method': method})['error']['code'], code)

    def test_cancel_in_progress_does_not_claim_remote_work_stopped(self):
        receipt = self.call(send())['result']['task']
        with self.service.store.transaction() as db:
            db.execute("UPDATE tasks SET state='running' WHERE id=?", (receipt['id'],))
        result = self.call({'jsonrpc': '2.0', 'id': 3, 'method': 'CancelTask', 'params': {'id': receipt['id']}})
        self.assertEqual(result['error']['code'], -32002)
        self.assertTrue(self.service.inspect('alice', receipt['id'])['cancel_requested'])

    def test_blocking_default_waits_for_interruption_or_completion(self):
        # Worker is independently responsible for execution. Handler only polls.
        ready = {'task_id': None}
        original = self.service.inspect
        def inspect(principal, task_id):
            ready['task_id'] = task_id
            receipt = original(principal, task_id)
            return receipt | {'state': 'completed', 'result': {'value': 7}}
        request = send(immediate=False)
        request['params'].pop('configuration')
        with patch.object(self.service, 'inspect', side_effect=inspect), patch.object(self.service, 'run_once', side_effect=AssertionError('handler executed worker')):
            result = self.call(request)['result']['task']
        self.assertEqual(result['status']['state'], 'TASK_STATE_COMPLETED')
        self.assertIsNotNone(ready['task_id'])

    def test_deadline_does_not_duplicate_submission(self):
        first = self.call(send(immediate=False))
        self.assertEqual(first['error']['code'], -32603)
        self.service.run_once()
        second = self.call(send(immediate=False))
        self.assertEqual(second['result']['task']['status']['state'], 'TASK_STATE_COMPLETED')
        self.assertEqual(len(self.service.list_tasks('alice')), 1)

    def test_list_keyset_cursor_filters_and_artifact_omission(self):
        for i in range(3):
            self.call(send(message_id='m-' + str(i), context_id='group'))
        request = {'jsonrpc': '2.0', 'id': 4, 'method': 'ListTasks', 'params': {'contextId': 'group', 'pageSize': 2}}
        page = self.call(request)['result']
        validate('List Tasks Response', page)
        self.assertEqual(page['totalSize'], 3)
        self.assertEqual(len(page['tasks']), 2)
        self.assertTrue(page['nextPageToken'])
        self.assertTrue(all('artifacts' not in task for task in page['tasks']))
        second = self.call(request | {'params': request['params'] | {'pageToken': page['nextPageToken']}})['result']
        self.assertEqual(len(second['tasks']), 1)
        self.assertEqual(second['nextPageToken'], '')
        self.assertFalse({t['id'] for t in second['tasks']} & {t['id'] for t in page['tasks']})
        altered = request | {'params': request['params'] | {'contextId': 'different', 'pageToken': page['nextPageToken']}}
        self.assertEqual(self.call(altered)['error']['code'], -32602)

    def test_invalid_versions_float_data_and_unsupported_parts_rejected(self):
        self.assertEqual(self.bridge.handle('alice', send(), {'a2a-version': '0.3'})['error']['code'], -32009)
        self.assertEqual(self.call(send(value=0.5))['error']['code'], -32602)
        request = send()
        request['params']['message']['parts'] = [{'url': 'https://evil.example/private', 'mediaType': 'application/json'}]
        self.assertEqual(self.call(request)['error']['code'], -32005)
        self.assertEqual(self.service.list_tasks('alice'), [])

    def test_continuation_keeps_context_and_deduplicates_complete_message(self):
        # No remote effects: pure fixture makes first revision ask for input.
        workflow = replace(self.workflow, version='continue/1',
                           classify=lambda outcome: 'input_required' if outcome.output['value'] == 7 else 'completed',
                           continue_input=lambda previous, supplied: supplied)
        # New state required because workflow identity is frozen.
        self.state = self.state / 'continuation'
        self.service = self.make_service(workflow)
        self.bridge = A2ABridge(self.service, workflow='echo')
        first = self.call(send(context_id='conversation'))['result']['task']
        self.service.run_once()
        request = send(message_id='turn-2', value=8, task_id=first['id'], context_id='conversation')
        updated = self.call(request)['result']['task']
        self.assertEqual(updated['id'], first['id'])
        self.service.run_once()
        final = self.call(request)['result']['task']
        self.assertEqual(final['status']['state'], 'TASK_STATE_COMPLETED')
        self.assertEqual(final['contextId'], 'conversation')
        self.assertEqual(self.call(send(message_id='turn-2', value=9, task_id=first['id'], context_id='conversation'))['error']['code'], -32602)

    def test_actual_http_client_and_server_round_trip(self):
        bridge = self.bridge
        captures = []
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                captures.append((self.headers.get('A2A-Version'), body['method']))
                result = bridge.handle('alice', body, dict(self.headers))
                data = json.dumps(result).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            def log_message(self, *_args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        client = A2AClient(BoundedJSONHTTP('http://127.0.0.1:%s/a2a' % server.server_port, allow_http=True), client_identity='test-client')
        submitted = client.submit(send()['params'])
        self.assertEqual(submitted.state, 'queued')
        self.service.run_once()
        completed = client.poll(submitted.remote_id)
        self.assertEqual(completed.state, 'completed')
        self.assertEqual(completed.result['task']['artifacts'][0]['parts'][0]['data'], {'value': 7})
        self.assertEqual(captures, [('1.0', 'SendMessage'), ('1.0', 'GetTask')])


class A2AClientFixtureTests(unittest.TestCase):
    def test_direct_message_response_and_credentials_absent_from_descriptor(self):
        class Transport:
            def descriptor(self): return {'endpoint': 'https://peer.example/a2a'}
            def request(self, method, payload, **kwargs):
                validate('Send Message Request', payload['params'])
                return {'jsonrpc': '2.0', 'id': payload['id'], 'result': {'message': {
                    'messageId': 'reply', 'contextId': 'context-1', 'role': 'ROLE_AGENT', 'parts': [{'text': 'done'}]}}}
        client = A2AClient(Transport(), client_identity='client')
        result = client.submit(send()['params'])
        self.assertEqual(result.state, 'completed')
        self.assertIsNone(result.remote_id)
        validate('Send Message Response', result.result)

    def test_official_schema_bytes_match_recorded_provenance(self):
        provenance = json.loads(BUNDLE.with_name('a2a-v1.0.1.provenance.json').read_text())
        self.assertEqual(hashlib.sha256(BUNDLE.read_bytes()).hexdigest(), provenance['sha256'])
        self.assertEqual(provenance['licence'], 'Apache-2.0')

    def test_old_wire_schema_cannot_silently_be_accepted(self):
        with self.assertRaises(jsonschema.ValidationError):
            validate('Message', {'messageId': 'old', 'role': 'agent', 'parts': [{'kind': 'text', 'text': 'bad'}]})
        with self.assertRaises(jsonschema.ValidationError):
            validate('Task', {'id': 'old', 'contextId': 'c', 'status': {'state': 'working'}})


class BlockedInterfaceCompatibilityTests(unittest.TestCase):
    """Unavailable definitions remain visible without becoming terminal failures."""

    def test_blocked_a2a_task_round_trips_and_submitted_default_stays_queued(self):
        from mas_host.bridges.a2a import _remote_task
        bridge = A2ABridge(object(), endpoint='https://mas.example/a2a')
        wire = bridge._task({'task_id': 'task-one', 'state': 'blocked',
                             'workflow': 'echo', 'updated_at': 0,
                             'cancel_requested': False, 'metadata': {}, 'result': None})
        validate('Task', wire)
        self.assertEqual(wire['status']['state'], 'TASK_STATE_SUBMITTED')
        self.assertEqual(wire['metadata']['mas']['state'], 'blocked')
        self.assertEqual(_remote_task(wire).state, 'blocked')
        del wire['metadata']['mas']['state']
        self.assertEqual(_remote_task(wire).state, 'queued')

    def test_remote_snapshot_and_orchestrator_receipt_accept_blocked(self):
        from mas_host.bridges.remote import RemoteSnapshot
        from mas_host.bridges.orchestrator import receipt
        snapshot = RemoteSnapshot('task-one', 'blocked').validate()
        self.assertEqual(snapshot.state, 'blocked')
        value = {'task_id': 'task-one', 'state': 'blocked'}
        self.assertEqual(receipt(value, 'task-one'), value)
        with self.assertRaises(ServiceError):
            receipt({**value, 'state': 'unrecognised'})
        with self.assertRaises(ServiceError):
            RemoteSnapshot('task-one', 'unrecognised').validate()

    @unittest.skipUnless(importlib.util.find_spec('langgraph') is not None,
                         'optional LangGraph SDK is not installed')
    def test_langgraph_blocked_receipt_interrupts_and_allows_poll(self):
        from unittest.mock import Mock
        from mas_host.bridges.langgraph import make_langgraph_node
        blocked = {'task_id': 'task-one', 'state': 'blocked',
                   'capabilities': {'cancel': True, 'update': False}}
        connector = Mock()
        connector.submit.return_value = blocked
        connector.inspect.return_value = blocked
        with patch('langgraph.types.interrupt', return_value={'action': 'poll'}) as interrupt:
            node = make_langgraph_node(connector, 'echo', 'node-one')
            result = node({'mas_operation_id': 'op-one', 'input': {}},
                          {'configurable': {'thread_id': 'thread-one'}})
        interrupt.assert_called_once_with({'mas_receipt': blocked, 'actions': ['poll', 'cancel']})
        connector.inspect.assert_called_once_with('task-one')
        connector.wait.assert_not_called()
        self.assertEqual(result, {'mas_receipt': blocked})
