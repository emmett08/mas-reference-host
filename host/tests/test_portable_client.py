"""Remote lifecycle clients do not require Linux execution facilities."""
import subprocess
import sys
import unittest

from mas_host.bridges.service_client import TaskServiceClient
from mas_host.service_contracts import ServiceError
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

from mas_host.bridges.service_client import TaskServiceClient
from mas_host.service_contracts import ServiceError


class Endpoint(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        self.server.observations.append((self.path, self.headers.get('Authorization'), json.loads(body)))
        if self.path == '/service/events_page':
            result = {'events': [{'id': 1, 'state': 'completed'}], 'next_after': 1, 'has_more': False}
        else:
            result = {'principal': 'alice', 'deployment_id': 'test-deployment', 'state': 'completed'}
        data = json.dumps({'result': result}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class PortableClientTests(unittest.TestCase):
    def test_provider_rotates_and_validates_before_each_transport(self):
        credentials = iter(['a' * 32, 'b' * 32, 'private\ninvalid'])
        headers = []
        def transport(endpoint, **options):
            headers.append(options['headers']['Authorization'])
            return 200, {'content-type': 'application/json'}, b'{"result":{}}'
        client = TaskServiceClient('https://peer.example', principal='alice',
            token_provider=lambda: next(credentials), transport=transport)
        client.discover('alice')
        client.discover('alice')
        with self.assertRaises(ServiceError) as caught:
            client.discover('alice')
        self.assertEqual(caught.exception.code, 'credential_provider_failed')
        self.assertEqual(headers, ['Bearer ' + 'a' * 32, 'Bearer ' + 'b' * 32])

    def test_provider_errors_are_sanitised_and_credential_sources_exclusive(self):
        def provider():
            raise RuntimeError('private credential value')
        client = TaskServiceClient('https://peer.example', principal='alice', token_provider=provider)
        with self.assertRaises(ServiceError) as caught:
            client.discover('alice')
        self.assertEqual(caught.exception.code, 'credential_provider_failed')
        for options in ({}, {'token_provider': 'not callable'},
                        {'token_file': 'private-token', 'token_provider': provider}):
            with self.subTest(options=list(options)):
                with self.assertRaises(ServiceError) as caught:
                    TaskServiceClient('https://peer.example', principal='alice', **options)
                self.assertEqual(caught.exception.code, 'invalid_credential_configuration')

    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Endpoint)
        self.server.daemon_threads = True
        self.server.observations = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = 'http://127.0.0.1:' + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def test_http_client_imports_without_posix_execution_module(self):
        program = '''
import builtins
original = builtins.__import__
def restricted(name, *args, **kwargs):
    if name == 'fcntl':
        raise ImportError('fcntl is unavailable on Windows')
    return original(name, *args, **kwargs)
builtins.__import__ = restricted
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.bridges.artifact_client import ArtifactTransferClient
'''
        result = subprocess.run([sys.executable, '-c', program], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_http_lifecycle_and_token_rotation(self):
        token = ['first-private-token']
        client = TaskServiceClient(self.endpoint, principal='alice', token_provider=lambda: token[0],
                                   allow_loopback_http=True)
        self.assertEqual(client.discover('alice')['principal'], 'alice')
        token[0] = 'second-private-token'
        self.assertEqual(client.inspect('alice', 'task-original')['state'], 'completed')
        self.assertEqual(list(client.iter_events('alice', 'task-original'))[0]['id'], 1)
        self.assertEqual(self.server.observations[0][1], 'Bearer first-private-token')
        self.assertEqual(self.server.observations[1],
            ('/service/inspect', 'Bearer second-private-token', {'task_id': 'task-original'}))
        with self.assertRaises(ServiceError):
            client.inspect('mallory', 'task-original')
        self.assertEqual(len(self.server.observations), 3)

    def test_native_cli_uses_private_input_without_execution_imports(self):
        result = subprocess.run([sys.executable, '-m', 'mas_host.client_cli', self.endpoint,
            'discover', '--principal', 'alice', '--token-stdin', '--allow-loopback-http'],
            input='cli-private-token\n', capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['principal'], 'alice')
        self.assertNotIn('cli-private-token', result.stdout + result.stderr)
        self.assertEqual(self.server.observations[0][1], 'Bearer cli-private-token')

    def test_cli_rejects_invalid_credentials_before_network(self):
        result = subprocess.run([sys.executable, '-m', 'mas_host.client_cli', self.endpoint,
            'discover', '--principal', 'alice', '--token-stdin', '--allow-loopback-http'],
            input='not valid secret\n', capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 2)
        self.assertNotIn('not valid secret', result.stderr)
        self.assertEqual(self.server.observations, [])


if __name__ == '__main__':
    unittest.main()
