"""Composed approvals cross HTTP/MCP/A2A with a real standalone CLI worker."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from mas_host.bridge_config import build_service
from mas_host.bridges.a2a import A2ABridge
from mas_host.bridges.mcp import MCPBridge
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.operations import archive_workflows
from mas_host.service_contracts import ServiceError
from test_bridge_server import ServerFixture, TOKEN
from test_mcp_bridge import request as mcp_request
import test_runner as runner_tests


class ComposedBridgeLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / 'state'
        self.config = {'version': 1, 'factory': 'fixtures.unified_workflows:workflows',
                       'grants': {'alice': ['pipeline', 'echo']}}
        self.path = self.root / 'config.json'
        self.path.write_text(json.dumps(self.config))
        self.token = self.root / 'token'
        self.token.write_text(TOKEN); self.token.chmod(0o600)
        self.service = build_service(self.config, self.state)

    def worker(self):
        root = Path(__file__).resolve().parents[2]
        env = {**os.environ, 'PYTHONPATH': str(root/'host')+os.pathsep+str(root/'host/tests'),
               'MAS_BIN': str(runner_tests.RunnerIntegrationTests.mas),
               'MAS_GATEWAY_BIN': str(runner_tests.RunnerIntegrationTests.gateway)}
        result = subprocess.run([sys.executable, '-m', 'mas_host', 'bridge', 'worker',
            '--config', str(self.path), '--state-dir', str(self.state), '--once', '--interval', '0.01'],
            env=env, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_http_mcp_and_a2a_approvals_replay_after_archive_without_metadata_loss(self):
        with ServerFixture(self.service) as fixture:
            client = TaskServiceClient(fixture.endpoint, principal='alice', token_file=self.token, allow_loopback_http=True)
            task = client.submit('alice', 'http-submit', 'pipeline', {'value': 1}, metadata={'context_id': 'http-context'})
            self.worker()
            paused = client.inspect('alice', task['task_id'])
            self.assertEqual(paused['state'], 'input_required')
            path = paused['result']['pending_tasks'][0]['path']
            self.assertEqual(client.inspect_task('alice', task['task_id'], path)['state'], 'input_required')
            mcp = MCPBridge(client)
            response = mcp.handle('alice', mcp_request('tools/call', {'name': 'mas.update', 'arguments': {
                'task_id': task['task_id'], 'request_key': 'mcp-approval', 'input': {'path': path, 'input': {'approved': True}}}}))
            self.assertNotIn('error', response, response)
            self.worker()
            self.assertTrue(client.result('alice', task['task_id'])['branches']['left']['approved'])
            self.assertTrue(client.events_page('alice', task['task_id'], limit=1)['has_more'])

            a2a = A2ABridge(client, endpoint=fixture.endpoint+'/a2a', allow_http=True)
            def send(message_id, input, task_id=None):
                message = {'messageId': message_id, 'role': 'ROLE_USER',
                    'parts': [{'mediaType': 'application/json', 'data': {'workflow': 'pipeline', 'input': input}}]}
                if task_id:
                    message['taskId'] = task_id
                return a2a.handle('alice', {'jsonrpc': '2.0', 'id': 1, 'method': 'SendMessage',
                    'params': {'message': message, 'configuration': {'returnImmediately': True}}})
            first = send('a2a-submit', {'value': 2})
            self.assertNotIn('error', first, first)
            identity = first['result']['task']['id']
            self.worker()
            continuation = {'path': 'root/s0', 'input': {'approved': True}}
            accepted = send('a2a-approval', continuation, identity)
            self.assertNotIn('error', accepted, accepted)
            self.assertEqual(send('a2a-approval', continuation, identity), accepted)
            self.worker()
            self.assertEqual(client.inspect('alice', identity)['state'], 'completed')
            archive_workflows(self.state, 0, True)
            replayed = send('a2a-approval', continuation, identity)
            self.assertNotIn('error', replayed, replayed)
            self.assertEqual(replayed['result']['task']['id'], identity)
            self.assertIn('error', send('a2a-approval', {'path': 'root/s0', 'input': {'approved': False}}, identity))
            with self.assertRaises(ServiceError) as conflict:
                client.update('alice', task['task_id'], 'mcp-approval',
                    {'path': path, 'input': {'approved': True}}, metadata={'changed': 'request metadata'})
            self.assertEqual(conflict.exception.code, 'request_conflict')
            self.assertEqual(client.inspect('alice', task['task_id'])['metadata'], {'context_id': 'http-context'})


if __name__ == '__main__':
    unittest.main()
