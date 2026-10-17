"""Real bounded multi-chunk transfer, authorisation and corruption regressions."""
from __future__ import annotations

import base64
import hashlib
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host.artifacts import ArtifactError, ArtifactPolicy, ArtifactRef, ArtifactStore, has_retained_artifacts
from mas_host.bridge_config import build_artifact_transfer
from mas_host.bridges.artifact_transfer import ArtifactScope, ArtifactTransferService, CHUNK_BYTES
from mas_host.bridges.artifact_client import ArtifactTransferClient, ArtifactTransferIncomplete
from mas_host.bridges.mcp import MCPBridge
from mas_host.bridges.mcp_client import MCPClient
from mas_host.bridges.mcp_transport import StreamableHTTPTransport
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.numeric_artifacts import open_float64, put_float64
from mas_host.service_contracts import ServiceError
from test_bridge_server import MemoryService, ServerFixture, TOKEN, READ, OTHER


class ArtifactTransferTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote = ArtifactStore(self.root / 'remote' / 'artifacts', namespace='result', principal='writer')
        self.local = ArtifactStore(self.root / 'local' / 'artifacts', namespace='input', principal='consumer')
        self.scopes = {'results': ArtifactScope(self.remote.root, 'result',
            {'alice': 'writer', 'bob': 'reader', 'mallory': 'outsider'})}
        self.service = ArtifactTransferService(self.root / 'transfers', self.scopes)
        self.data = bytes(range(256)) * 385  # Three full chunks and a partial chunk.
        self.ref = self.remote.put_bytes(self.data, readers=('reader',))

    def client(self, principal='bob', service=None, exchange=None):
        service = service or self.service
        return ArtifactTransferClient(exchange or (lambda operation, payload: service.dispatch(principal, operation, payload)),
                                      descriptor={'peer': 'configured-test-peer', 'principal': principal})

    def call(self, operation, payload, principal='alice', service=None):
        return (service or self.service).dispatch(principal, operation, payload)

    def test_download_materializes_durable_provenance_and_protects_retention(self):
        receipt = self.client().materialize('results', self.ref, self.local)
        self.assertEqual(self.local.read_bytes(receipt['reference']), self.data)
        self.assertEqual(receipt['reference']['producer'], 'consumer')
        self.assertEqual(receipt['reference']['input_sha256s'], [self.ref.sha256])
        self.assertEqual(json.loads(self.local.read_bytes(receipt['receipt']))['source']['reference'], self.ref.to_dict())
        self.assertTrue(has_retained_artifacts(self.root / 'local'))
        repeated = self.client().materialize('results', self.ref, self.local)
        self.assertEqual(repeated, receipt)
        self.assertEqual(list((self.root / 'transfers').iterdir()), [])
        self.local.link(receipt['link_id'], [receipt['reference'], receipt['receipt']], state='released')
        self.assertFalse(has_retained_artifacts(self.root / 'local'))

    def test_reference_token_and_namespace_do_not_grant_cross_principal_access(self):
        for reference in (self.ref.to_dict(), {**self.ref.to_dict(), 'producer': 'outsider'},
                          {**self.ref.to_dict(), 'namespace': 'other'}):
            with self.assertRaises(ServiceError):
                self.call('download_begin', {'scope': 'results', 'reference': reference}, 'mallory')
        opened = self.call('download_begin', {'scope': 'results', 'reference': self.ref.to_dict()}, 'bob')
        for operation in ('download_chunk', 'close'):
            payload = {'transfer_id': opened['transfer_id']}
            if operation == 'download_chunk':
                payload['offset'] = 0
            with self.assertRaisesRegex(ServiceError, 'artifact unavailable'):
                self.call(operation, payload, 'mallory')

    def test_chunk_tampering_never_publishes_usable_local_reference(self):
        def corrupt(operation, payload):
            result = self.call(operation, payload, 'bob')
            if operation == 'download_chunk':
                data = bytearray(base64.b64decode(result['data']))
                data[0] ^= 1
                result['data'] = base64.b64encode(data).decode()
            return result
        with self.assertRaisesRegex(ServiceError, 'artifact integrity mismatch'):
            self.client(exchange=corrupt).materialize('results', self.ref, self.local)
        self.assertEqual(self.local.status()['artifacts'], [])
        self.assertEqual(list((self.root / 'transfers').iterdir()), [])

    def test_upload_duplicate_chunk_and_commit_survive_service_restart(self):
        data = b'upload-content'
        digest = hashlib.sha256(data).hexdigest()
        begun = self.call('upload_begin', {'scope': 'results', 'sha256': digest, 'size': len(data),
            'media_type': 'text/plain', 'readers': ['bob']})
        chunk = {'transfer_id': begun['transfer_id'], 'offset': 0, 'data': base64.b64encode(data).decode()}
        self.assertEqual(self.call('upload_chunk', chunk), self.call('upload_chunk', chunk))
        with self.assertRaises(ServiceError):
            self.call('upload_chunk', {**chunk, 'data': base64.b64encode(b'other-content!').decode()})
        reopened = ArtifactTransferService(self.root / 'transfers', self.scopes)
        result = self.call('upload_commit', {'transfer_id': begun['transfer_id']}, service=reopened)
        self.assertEqual(result, self.call('upload_commit', {'transfer_id': begun['transfer_id']}))
        reader = self.scopes['results'].repository('bob', readonly=True)
        self.assertEqual(reader.read_bytes(result['reference']), data)
        self.assertEqual(result['reference']['producer'], 'writer')

    def test_upload_integrity_failure_expiry_and_reservation_bound(self):
        now = [100]
        service = ArtifactTransferService(self.root / 'sessions', self.scopes, ttl_seconds=5,
            max_sessions=1, max_staging_bytes=8, clock=lambda: now[0])
        begin = {'scope': 'results', 'sha256': hashlib.sha256(b'correct!').hexdigest(),
                 'size': 8, 'media_type': 'text/plain'}
        session = self.call('upload_begin', begin, service=service)
        with self.assertRaisesRegex(ServiceError, 'artifact staging full'):
            self.call('upload_begin', begin, service=service)
        self.call('upload_chunk', {'transfer_id': session['transfer_id'], 'offset': 0,
            'data': base64.b64encode(b'wrong!!!').decode()}, service=service)
        with self.assertRaises(ServiceError):
            self.call('upload_commit', {'transfer_id': session['transfer_id']}, service=service)
        now[0] += 6
        replacement = self.call('upload_begin', begin, service=service)
        self.assertNotEqual(replacement['transfer_id'], session['transfer_id'])
        self.assertFalse((service.root / (session['transfer_id'] + '.part')).exists())
        with self.assertRaises(ServiceError):
            self.call('upload_commit', {'transfer_id': session['transfer_id']}, service=service)

    def test_scope_rebinding_revokes_live_session_and_restore_is_audit_only(self):
        value = self.call('download_begin', {'scope': 'results', 'reference': self.ref.to_dict()}, 'bob')
        changed = ArtifactScope(self.remote.root, 'result', {'alice': 'writer', 'bob': 'outsider'})
        reopened = ArtifactTransferService(self.root / 'transfers', {'results': changed})
        with self.assertRaises(ServiceError):
            self.call('download_chunk', {'transfer_id': value['transfer_id'], 'offset': 0}, 'bob', service=reopened)
        (self.root / '.restored-audit-only').write_text('restored')
        with self.assertRaisesRegex(ServiceError, 'audit only restore'):
            self.call('download_chunk', {'transfer_id': value['transfer_id'], 'offset': 0}, 'bob')
        self.assertEqual(ArtifactStore(self.remote.root, namespace='result', principal='reader', readonly=True).read_bytes(self.ref), self.data)

    def token_file(self, name, token):
        path = self.root / name
        path.write_text(token)
        path.chmod(0o600)
        return path

    def test_real_authenticated_http_publish_download_and_scope_enforcement(self):
        with ServerFixture(artifact_transfer=self.service, request_log=lambda event: None) as fixture:
            endpoint = 'http://127.0.0.1:' + str(fixture.port)
            uploader = TaskServiceClient(endpoint, principal='alice', token_file=self.token_file('alice', TOKEN), allow_loopback_http=True)
            reader = TaskServiceClient(endpoint, principal='bob', token_file=self.token_file('bob', OTHER), allow_loopback_http=True)
            original = self.local.put_bytes(b'full-duplex-' * 7000)
            remote = ArtifactTransferClient.from_service(uploader).publish('results', self.local, original, readers=('bob',))
            output = ArtifactTransferClient.from_service(reader).materialize('results', remote, self.local)
            self.assertEqual(self.local.read_bytes(output['reference']), self.local.read_bytes(original))
            readonly = TaskServiceClient(endpoint, principal='alice', token_file=self.token_file('read', READ), allow_loopback_http=True)
            with self.assertRaisesRegex(ServiceError, 'insufficient scope'):
                ArtifactTransferClient.from_service(readonly).publish('results', self.local, original)

    def test_real_mcp_http_tool_materialization_and_integer_json_profile(self):
        bridge = MCPBridge(MemoryService(), artifact_transfer=self.service)
        with ServerFixture(mcp_bridge=bridge, artifact_transfer=self.service, request_log=lambda event: None) as fixture:
            transport = StreamableHTTPTransport('http://127.0.0.1:' + str(fixture.port) + '/mcp',
                token=OTHER, allow_loopback_http=True)
            client = MCPClient(transport)
            self.assertIn('mas.artifact.download_chunk', [item['name'] for item in client.list_tools()['tools']])
            output = ArtifactTransferClient.from_mcp(client).materialize('results', self.ref, self.local)
            self.assertEqual(self.local.read_bytes(output['reference']), self.data)
            with self.assertRaises(Exception):
                client.request('tools/call', {'name': 'mas.artifact.download_chunk',
                    'arguments': {'transfer_id': '0' * 32, 'offset': 1.5}})

    def test_verified_open_is_disk_backed_and_snapshot_is_independent(self):
        with patch.object(self.remote, 'read_bytes', side_effect=AssertionError('must not buffer full blob')):
            with self.remote.open(self.ref) as stream:
                self.assertGreaterEqual(stream.fileno(), 0)
                (self.remote.directory / (self.ref.sha256 + '.blob')).write_bytes(b'corruption')
                self.assertEqual(stream.read(), self.data)
        with self.assertRaises(ArtifactError):
            with self.remote.open(self.ref):
                self.fail('corrupt data was exposed')

    def test_numeric_array_binary_roundtrip_and_shape_nonfinite_rejection(self):
        ref = put_float64(self.local, [math.pi, -0.0, 1e-200, 1e200], shape=(2, 2))
        with open_float64(self.local, ref) as (shape, values):
            self.assertEqual(shape, (2, 2))
            self.assertEqual(list(values), [math.pi, -0.0, 1e-200, 1e200])
        for values, shape in (([float('nan')], (1,)), ([1.0], (2,)), ([1.0], (True,))):
            with self.assertRaises(ArtifactError):
                put_float64(self.local, values, shape=shape)

    def test_configuration_is_explicit_and_rejects_path_escape_and_actor_alias(self):
        config = {'artifacts': {'scopes': {'results': {'namespace': 'results',
                  'principals': {'alice': 'writer'}}}}}
        self.assertIsInstance(build_artifact_transfer(config, self.root), ArtifactTransferService)
        config['artifacts']['scopes']['results']['path'] = '../escape'
        with self.assertRaises(ServiceError):
            build_artifact_transfer(config, self.root)
        with self.assertRaises(ServiceError):
            ArtifactScope(self.root, 'data', {'alice': 'writer', 'mallory': 'writer'})

    def test_cross_scope_aliases_cannot_share_one_repository_actor(self):
        original = ArtifactScope(self.root / 'objects', 'data', {'alice': 'writer'})
        alias = ArtifactScope(self.root / 'objects' / 'unused' / '..', 'data', {'bob': 'writer'})
        with self.assertRaises(ServiceError):
            ArtifactTransferService(self.root / 'transfers', {'first': original, 'alias': alias})
        same_owner = ArtifactScope(self.root / 'objects', 'data', {'alice': 'writer'})
        ArtifactTransferService(self.root / 'transfers', {'first': original, 'same': same_owner})
        separate = ArtifactScope(self.root / 'separate', 'data', {'bob': 'writer'})
        ArtifactTransferService(self.root / 'transfers', {'first': original, 'separate': separate})

    def test_producer_revocation_blocks_existing_and_new_download_sessions(self):
        session = self.call('download_begin', {'scope': 'results', 'reference': self.ref.to_dict()}, 'bob')
        with self.assertRaises(ServiceError):
            self.call('revoke', {'scope': 'results', 'reference': self.ref.to_dict(), 'readers': ['alice']}, 'bob')
        result = self.call('revoke', {'scope': 'results', 'reference': self.ref.to_dict(), 'readers': ['bob']})
        self.assertEqual(result, {'revoked': True})
        reopened = ArtifactTransferService(self.root / 'transfers', self.scopes)
        for operation, payload in [('download_begin', {'scope': 'results', 'reference': self.ref.to_dict()}),
                ('download_chunk', {'transfer_id': session['transfer_id'], 'offset': 0})]:
            with self.assertRaises(ServiceError):
                self.call(operation, payload, 'bob', service=reopened)
        # The publisher can still fetch the original bytes; earlier local copies
        # cannot be recalled and the local immutable manifest is not rewritten.
        receipt = self.client('alice', service=reopened).materialize('results', self.ref, self.local)
        self.assertEqual(self.local.read_bytes(receipt['reference']), self.data)

    def test_lost_commit_ack_keeps_exact_session_for_inspection_without_reupload(self):
        ref = self.local.put_bytes(b'published but acknowledgement lost')
        calls = []
        def lose(operation, payload):
            calls.append(operation)
            result = self.call(operation, payload)
            if operation == 'upload_commit':
                raise ServiceError('transport_timeout')
            return result
        with self.assertRaises(ArtifactTransferIncomplete) as lost:
            self.client('alice', exchange=lose).publish('results', self.local, ref, readers=('bob',))
        self.assertNotIn('close', calls)
        value = self.client('alice').inspect_transfer(lost.exception.transfer_id)
        self.assertEqual(value['reference']['sha256'], ref.sha256)
        repeated = self.call('upload_commit', {'transfer_id': lost.exception.transfer_id})
        self.assertEqual(repeated['reference'], value['reference'])
        self.assertEqual(calls.count('upload_begin'), 1)

    def test_a2a_data_part_preserves_reference_for_explicit_authenticated_materialization(self):
        from mas_host.bridges.a2a import A2ABridge
        from test_a2a_bridge import validate
        service = MemoryService()
        def inspect(principal, task_id):
            if principal != 'bob' or task_id != 'task-result':
                raise ServiceError('not_found')
            return {'task_id': task_id, 'state': 'completed', 'updated_at': 1000,
                    'workflow': 'analysis', 'cancel_requested': False,
                    'result': {'array': self.ref.to_dict()}, 'metadata': {}}
        service.inspect = inspect
        bridge = A2ABridge(service, endpoint='https://configured.example/a2a')
        response = bridge.handle('bob', {'jsonrpc': '2.0', 'id': 1, 'method': 'GetTask',
            'params': {'id': 'task-result'}}, {'A2A-Version': '1.0'})
        validate('Task', response['result'])
        reference = response['result']['artifacts'][0]['parts'][0]['data']['array']
        self.assertEqual(reference, self.ref.to_dict())
        receipt = self.client().materialize('results', reference, self.local)
        self.assertEqual(self.local.read_bytes(receipt['reference']), self.data)
