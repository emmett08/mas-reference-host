"""Acknowledged transfer continuity across real PostgreSQL authority epochs."""
import base64
import concurrent.futures
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest
import uuid

from mas_host.artifacts import ArtifactError, ArtifactPolicy
from mas_host.contracts import RunnerError
from mas_host.service_contracts import ServiceError
from mas_host.shared_artifact_transfer import SharedArtifactScope, SharedArtifactTransferService
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_backup import postgres_environment
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig
import test_s3_artifacts as fixtures


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedTransferTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.schema = 'mas_transfers_' + uuid.uuid4().hex
        self.authority = self.authority_for('controller', 1)
        self.authority.initialise()
        self.client = fixtures.ObjectClient()
        self.backend = S3ArtifactContent(S3ContentConfig('https://objects.example', 'test-bucket',
            'mas', 'us-east-1', self.root / 'unused-credentials'), client=self.client)
        self.policy = ArtifactPolicy(max_artifact_bytes=131072, max_total_bytes=262144,
                                    max_artifacts=8, max_links=8)
        self.options = {'max_sessions': 8, 'max_staging_bytes': 262144}
        self.service = self.transfer_service()

    def authority_for(self, controller, epoch):
        authority = SharedAuthority(binary=os.environ['MAS_SHARED_BIN'],
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace=self.schema,
            deployment='deployment', controller=controller, epoch=epoch,
            secret_key=b'fixture-secret' * 4, secret_reference='fixture/1')
        self.addCleanup(authority.close)
        return authority

    def transfer_service(self):
        scope = SharedArtifactScope(self.authority, self.root / ('cache-' + uuid.uuid4().hex),
            'results', {'alice': 'writer', 'bob': 'reader'}, self.policy, self.backend)
        return SharedArtifactTransferService(self.root / ('transfers-' + uuid.uuid4().hex),
                                            {'results': scope}, **self.options)

    def replace_controller(self):
        controller = 'replacement-' + uuid.uuid4().hex
        binding = self.authority.handover(controller)
        self.authority = self.authority_for(controller, binding['epoch'])
        self.service = self.transfer_service()

    def begin(self, data, **kwargs):
        return self.service.upload_begin('alice', 'results', hashlib.sha256(data).hexdigest(),
            len(data), 'application/octet-stream', readers=['bob'], **kwargs)['transfer_id']

    def chunk(self, transfer, offset, data):
        return self.service.upload_chunk('alice', transfer, offset, base64.b64encode(data).decode())

    def upload(self, data):
        transfer = self.begin(data)
        for offset in range(0, len(data), 32768):
            self.chunk(transfer, offset, data[offset:offset + 32768])
        return transfer, self.service.upload_commit('alice', transfer)['reference']

    def fixture_sql(self, source):
        psql = shutil.which('psql')
        self.assertIsNotNone(psql, 'CI must install native PostgreSQL client tools')
        result = subprocess.run([psql, '--no-psqlrc', '--no-password', '--tuples-only',
            '--no-align', '--quiet', '--set=ON_ERROR_STOP=1'], input=source,
            text=True, env=postgres_environment(self.authority), stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, timeout=30)
        self.assertEqual(result.returncode, 0, 'native transfer fixture query failed')
        return result.stdout.strip()

    def test_acknowledged_ids_offsets_and_downloads_survive_empty_replacement_cache(self):
        data = b'abcdef' * 7000
        transfer = self.begin(data)
        self.chunk(transfer, 0, data[:32768])
        old = self.service
        self.replace_controller()
        self.assertEqual(self.service.status('alice', transfer)['next_offset'], 32768)
        self.assertEqual(self.chunk(transfer, 100, data[100:500]), {'next_offset': 32768})
        with self.assertRaises(ServiceError):
            self.chunk(transfer, 100, b'changed')
        with self.assertRaises(ServiceError):
            self.chunk(transfer, 32769, b'x')
        with self.assertRaises(ServiceError):
            self.service.status('bob', transfer)
        with self.assertRaises(ServiceError):
            old.status('alice', transfer)
        self.chunk(transfer, 32768, data[32768:])
        ref = self.service.upload_commit('alice', transfer)['reference']
        download = self.service.download_begin('bob', 'results', ref)['transfer_id']
        self.replace_controller()
        received = b''
        for offset in range(0, len(data), 32768):
            received += base64.b64decode(self.service.download_chunk('bob', download, offset)['data'])
        self.assertEqual(received, data)
        self.assertEqual(self.service.close('bob', download), {'closed': True})
        self.replace_controller()
        with self.assertRaises(ServiceError):
            self.service.status('bob', download)
        self.assertEqual(self.service.close('bob', download), {'closed': True})
        self.assertFalse(list(self.root.rglob('*.json')))
        self.assertFalse(list(self.root.rglob('*.part')))
        self.assertFalse(list(self.root.rglob('*.sqlite')))

    def test_lost_chunk_and_completion_replies_recover_exact_acknowledged_state(self):
        data = b'acknowledged bytes'
        transfer = self.begin(data)
        original = self.authority.command
        lost = {'transfer.chunk', 'transfer.complete'}
        def lose_reply(operation, parameters, **kwargs):
            result = original(operation, parameters, **kwargs)
            if operation in lost:
                lost.remove(operation)
                raise RunnerError('lost committed reply')
            return result
        self.authority.command = lose_reply
        with self.assertRaises(ServiceError):
            self.chunk(transfer, 0, data)
        self.assertEqual(self.service.status('alice', transfer)['next_offset'], len(data))
        self.assertEqual(self.chunk(transfer, 0, data)['next_offset'], len(data))
        with self.assertRaises(ServiceError):
            self.service.upload_commit('alice', transfer)
        reference = self.service.status('alice', transfer)['reference']
        self.replace_controller()
        self.assertEqual(self.service.upload_commit('alice', transfer)['reference'], reference)

    def test_handover_during_s3_publication_retains_uploaded_chunks(self):
        transfer = self.begin(b'published before fencing')
        self.chunk(transfer, 0, b'published before fencing')
        original = self.client.put_object
        successor = []
        def put(**request):
            result = original(**request)
            successor.append(self.authority.handover('replacement'))
            return result
        self.client.put_object = put
        with self.assertRaises(ArtifactError):
            self.service.upload_commit('alice', transfer)
        self.client.put_object = original
        self.authority = self.authority_for('replacement', successor[0]['epoch'])
        self.service = self.transfer_service()
        self.assertIsNone(self.service.status('alice', transfer)['reference'])
        reference = self.service.upload_commit('alice', transfer)['reference']
        self.assertEqual(reference['sha256'], hashlib.sha256(b'published before fencing').hexdigest())

    def test_cached_snapshot_reauthorises_grants_and_is_rebuilt_after_replacement(self):
        _, ref = self.upload(b'x' * 65536)
        download = self.service.download_begin('bob', 'results', ref)['transfer_id']
        self.service.download_chunk('bob', download, 0)
        before = len(self.client.calls)
        self.service.download_chunk('bob', download, 32768)
        self.assertEqual(len(self.client.calls), before)  # verified snapshot, no GET per chunk
        self.service.revoke('alice', 'results', ref, ['bob'])
        with self.assertRaises(ServiceError):
            self.service.download_chunk('bob', download, 0)
        self.replace_controller()
        with self.assertRaises(ServiceError):
            self.service.download_chunk('bob', download, 0)

    def test_database_expiry_and_global_staging_quota_release_only_on_close_or_expiry(self):
        # A separate namespace provisions a different immutable transfer policy.
        self.schema = 'mas_transfer_expiry_' + uuid.uuid4().hex
        self.authority = self.authority_for('controller', 1)
        self.authority.initialise()
        self.options = {'max_sessions': 2, 'max_staging_bytes': 100, 'ttl_seconds': 3600, 'clock': lambda: 0}
        self.service = self.transfer_service()
        first = self.begin(b'a' * 70)
        self.chunk(first, 0, b'a' * 70)
        with self.assertRaises(ServiceError):
            self.begin(b'b' * 40)
        self.service.close('alice', first)
        second = self.begin(b'b' * 40)
        self.chunk(second, 0, b'b' * 40)
        self.assertEqual(self.service.status('alice', second)['next_offset'], 40)
        with self.assertRaises(ServiceError):
            self.begin(b'c' * 90)
        # Set one retained deadline behind the authoritative database clock.
        # A one-second TTL can expire a fresh replacement between native
        # commands; that timing race says nothing about quota reclamation.
        expired = self.fixture_sql(f'''UPDATE {self.schema}.artifact_transfers
            SET expires_at=floor(extract(epoch FROM clock_timestamp()))::bigint-1
            WHERE id='{second}' RETURNING id;''')
        self.assertEqual(expired, second)
        with self.assertRaises(ServiceError) as rejected:
            self.service.status('alice', second)
        self.assertEqual(rejected.exception.code, 'artifact_unavailable')
        with self.assertRaises(ServiceError):
            self.chunk(second, 0, b'b' * 40)
        with self.assertRaises(ServiceError):
            self.service.upload_commit('alice', second)
        third = self.begin(b'c' * 90)
        self.assertEqual(self.service.status('alice', third)['size'], 90)
        retained = self.fixture_sql(f'''SELECT state || ':' ||
            (SELECT count(*) FROM {self.schema}.artifact_transfer_chunks WHERE transfer_id='{second}')
            FROM {self.schema}.artifact_transfers WHERE id='{second}';''')
        self.assertEqual(retained, 'expired:0')

    def test_concurrent_exact_duplicates_do_not_advance_or_rewrite_offset(self):
        transfer = self.begin(b'exact bytes')
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.chunk(transfer, 0, b'exact bytes'), range(4)))
        self.assertEqual(results, [{'next_offset': 11}] * 4)
        self.assertEqual(self.service.status('alice', transfer)['next_offset'], 11)
        with self.assertRaises(ServiceError) as rejected:
            self.authority.command('transfer.chunk', {'principal': 'bob', 'transfer_id': transfer,
                'offset': 0, 'hex': b'exact bytes'.hex()})
        self.assertEqual(rejected.exception.code, 'artifact_unavailable')
        self.assertEqual(self.service.status('alice', transfer)['next_offset'], 11)

    def test_changed_configuration_and_oversize_native_chunks_are_rejected(self):
        transfer = self.begin(b'valid')
        with self.assertRaises(ServiceError):
            self.authority.command('transfer.chunk', {'principal': 'alice', 'transfer_id': transfer,
                'offset': 0, 'hex': (b'x' * 32769).hex()})
        self.options['max_sessions'] = 7
        with self.assertRaises(ServiceError):
            self.transfer_service()
        self.assertEqual(self.service.status('alice', transfer)['next_offset'], 0)

    def test_stalled_snapshot_does_not_block_other_download_or_close(self):
        _, first = self.upload(b'first object')
        _, second = self.upload(b'second object')
        slow = self.service.download_begin('bob', 'results', first)['transfer_id']
        fast = self.service.download_begin('bob', 'results', second)['transfer_id']
        entered, release = threading.Event(), threading.Event()
        original = self.client.get_object
        def get(**request):
            if first['sha256'] in request['Key']:
                entered.set()
                if not release.wait(10):
                    raise OSError('test snapshot timeout')
            return original(**request)
        self.client.get_object = get
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(self.service.download_chunk, 'bob', slow, 0)
            try:
                self.assertTrue(entered.wait(5))
                other = pool.submit(self.service.download_chunk, 'bob', fast, 0).result(timeout=5)
                self.assertEqual(base64.b64decode(other['data']), b'second object')
                self.assertEqual(self.service.close('bob', slow), {'closed': True})
                self.assertIn(slow, self.service._snapshots)  # in-flight bytes remain reserved
            finally:
                release.set()
            with self.assertRaises(ServiceError):
                pending.result(timeout=5)
        self.assertNotIn(slow, self.service._snapshots)

    def test_namespace_retirement_waits_for_transfers_and_rejects_new_uploads(self):
        upload, reference = self.upload(b'retained readable object')
        self.service.close('alice', upload)
        download = self.service.download_begin('bob', 'results', reference)['transfer_id']
        retirement = {'namespace': 'results', 'operator': 'admin',
            'request_key': 'retire-results', 'reason': 'Close publication after completed work'}
        with self.assertRaises(ServiceError):
            self.authority.command('artifact.retire', retirement)
        self.service.close('bob', download)
        self.authority.command('artifact.retire', retirement)
        with self.assertRaises(ServiceError):
            self.begin(b'cannot publish after retirement')
        # Retirement closes publication; retained authorised reads remain valid.
        download = self.service.download_begin('bob', 'results', reference)['transfer_id']
        chunk = self.service.download_chunk('bob', download, 0)
        self.assertEqual(base64.b64decode(chunk['data']), b'retained readable object')

    def test_cross_scope_alias_rejected_before_configuration_and_on_legacy_session_read(self):
        _, reference = self.upload(b'original owner bytes')
        session = self.service.download_begin('alice', 'results', reference)['transfer_id']
        self.service.download_chunk('alice', session, 0)  # populate verified cache
        original = self.service.scopes['results']
        escaped = SharedArtifactScope(self.authority, self.root / 'different-disposable-cache',
            'results', {'bob': 'writer'}, self.policy, self.backend)
        with self.assertRaises(ServiceError) as caught:
            SharedArtifactTransferService(self.root / 'replacement', {'results': original, 'escaped': escaped})
        self.assertEqual(caught.exception.code, 'invalid_artifact_configuration')
        scopes = {name: {'namespace': scope.namespace, 'principals': scope.principals,
            'binding': scope.binding(), 'max_artifact_bytes': scope.policy.max_artifact_bytes}
            for name, scope in {'results': original, 'escaped': escaped}.items()}
        with self.assertRaises(ServiceError) as caught:
            self.authority.command('transfer.configure', {'scopes': scopes, 'ttl_seconds': 3600,
                'max_sessions': self.options['max_sessions'], 'max_staging_bytes': self.options['max_staging_bytes']})
        self.assertEqual(caught.exception.code, 'invalid_artifact_configuration')
        # Simulate a retained experimental configuration accepted by an older
        # writer. Current reads must reject it even without reconstruction.
        sql = f'''UPDATE {self.schema}.transfer_configuration SET descriptor=jsonb_set(
            descriptor::jsonb,'{{scopes,escaped}}',
            (descriptor::jsonb->'scopes'->'results') || '{{"principals":{{"bob":"writer"}}}}'::jsonb)::text;'''
        psql = shutil.which('psql')
        self.assertIsNotNone(psql, 'CI must install native PostgreSQL client tools')
        updated = subprocess.run([psql, '--no-psqlrc', '--no-password', '--set=ON_ERROR_STOP=1'],
            input=sql, text=True, env=postgres_environment(self.authority),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        self.assertEqual(updated.returncode, 0)
        with self.assertRaises(ServiceError):
            self.service.download_chunk('alice', session, 0)
        with self.assertRaises(ServiceError):
            self.authority.command('transfer.begin', {'principal': 'bob', 'transfer_id': uuid.uuid4().hex,
                'scope': 'escaped', 'kind': 'download', 'sha256': reference['sha256'], 'size': reference['size'],
                'media_type': reference['media_type'], 'input_sha256s': [], 'readers': [], 'reference': reference})


if __name__ == '__main__':
    unittest.main()
