"""Real PostgreSQL metadata with fault-controlled S3 transport boundaries."""
import concurrent.futures
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from mas_host.artifacts import ArtifactError, ArtifactPolicy, ArtifactRef, _manifest_identity
from mas_host.shared_artifacts import SharedArtifactStore
from mas_host.shared_authority import SharedAuthority
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig
import test_s3_artifacts as fixtures


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.schema = 'mas_artifact_' + uuid.uuid4().hex
        self.authority = self.authority_for('controller', 1)
        self.authority.initialise()
        self.client = fixtures.ObjectClient()
        self.config = S3ContentConfig('https://objects.example', 'test-bucket', 'mas', 'us-east-1',
                                      self.root / 'credentials-not-used-by-transport-double')
        self.backend = S3ArtifactContent(self.config, client=self.client)
        self.policy = ArtifactPolicy(max_artifact_bytes=1024, max_total_bytes=2048, max_artifacts=2, max_links=4)
        self.writer = self.store('writer')

    def authority_for(self, controller, epoch):
        authority = SharedAuthority(binary=os.environ['MAS_SHARED_BIN'],
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace=self.schema,
            deployment='deployment', controller=controller, epoch=epoch,
            secret_key=b'fixture-secret' * 4, secret_reference='test-key/1')
        self.addCleanup(authority.close)
        return authority

    def store(self, principal, *, authority=None, policy=None):
        return SharedArtifactStore(authority or self.authority, namespace='results', principal=principal,
            content_backend=self.backend, policy=policy or self.policy)

    def lifecycle_service(self, owners=('writer',)):
        from mas_host.shared_task_service import SharedTaskService
        from mas_host.shared_service import SharedTaskFacade
        from test_definition_versions import registered
        return SharedTaskFacade(SharedTaskService(self.authority, self.root / 'lifecycle-cache',
            {'leaf': registered()}, {owner: {'leaf'} for owner in owners}, supervised=False))

    def test_typed_consumers_scope_archival_and_legacy_binding_is_audited(self):
        from mas_host.operator_service import OperatorService
        from mas_host.service_contracts import ServiceError
        service = self.lifecycle_service()
        tasks = [service.submit('writer', 'request-' + str(n), 'leaf', {})['task_id'] for n in range(3)]
        for task in tasks:
            service.cancel('writer', task)
        operator = OperatorService(service, operators={'admin': {'owners': ['writer'], 'deployment_admin': True}})
        reference = self.writer.put_bytes(b'reusable context')
        first = self.writer.with_retention_target(kind='task', id=tasks[0], principal='writer')
        second = self.writer.with_retention_target(kind='task', id=tasks[1], principal='writer')
        a = first.link('context-shared', [reference])
        b = second.link('context-shared', [reference])
        self.assertNotEqual(a['link_id'], b['link_id'])
        self.assertEqual(first.link('context-shared', [reference]), a)
        def archive(index):
            return operator.dispatch('admin', 'archive', {'kind': 'tasks', 'principal': 'writer',
                'task_id': tasks[index], 'request_key': 'archive-' + str(index), 'reason': 'Compact completed history',
                'older_than_seconds': 0})
        with self.assertRaises(ServiceError):
            archive(0)
        first.link('context-shared', [reference], state='released')
        self.assertTrue(archive(0)['archived'])  # unrelated second consumer remains retained
        with self.assertRaises((ArtifactError, ServiceError)):
            first.link('new-edge-after-archive', [reference])
        self.writer.link('legacy-edge', [reference])
        with self.assertRaises(ServiceError):
            archive(2)  # an unmapped legacy consumer remains conservative
        payload = {'principal': 'writer', 'task_id': tasks[1], 'namespace': 'results',
            'artifact_principal': 'writer', 'link_id': 'legacy-edge', 'request_key': 'bind-old',
            'reason': 'Verified complete owning lifecycle'}
        bound = operator.dispatch('admin', 'artifact_bind', payload)
        self.assertEqual(operator.dispatch('admin', 'artifact_bind', payload), bound)
        self.assertTrue(archive(2)['archived'])
        retirement = {'namespace': 'results', 'request_key': 'retire-results', 'reason': 'Close completed namespace'}
        with self.assertRaises(ServiceError):
            operator.dispatch('admin', 'artifact_retire', retirement)
        before = self.writer.status()['content']
        for link in (b['link_id'], 'legacy-edge'):
            operator.dispatch('admin', 'artifact_release', {'principal': 'writer', 'namespace': 'results',
                'artifact_principal': 'writer', 'link_id': link, 'request_key': 'release-' + link,
                'reason': 'Consumer completed and verified'})
        operator.dispatch('admin', 'artifact_retire', retirement)
        self.assertTrue(self.writer.status()['closed'])
        self.assertEqual(self.writer.status()['content'], before)
        self.assertEqual(self.writer.read_bytes(reference), b'reusable context')
        with self.assertRaises((ArtifactError, ServiceError)):
            self.writer.put_bytes(b'closed publication')

    def test_lifecycle_actor_mapping_requires_frozen_service_owner_grant(self):
        from mas_host.shared_artifact_transfer import SharedArtifactScope, SharedArtifactTransferService
        from mas_host.service_contracts import ServiceError
        service = self.lifecycle_service(('alice', 'other'))
        alice = service.submit('alice', 'request-a', 'leaf', {})['task_id']
        other = service.submit('other', 'request-b', 'leaf', {})['task_id']
        scope = SharedArtifactScope(self.authority, self.root / 'objects', 'results', {'alice': 'writer'}, self.policy, self.backend)
        SharedArtifactTransferService(self.root / 'transfer-cache', {'results': scope})
        reference = self.writer.put_bytes(b'owner mapping')
        bound = self.writer.with_retention_target(kind='task', id=alice, principal='alice')
        self.assertEqual(bound.link('mapped-owner', [reference])['retention_target']['principal'], 'alice')
        denied = self.writer.with_retention_target(kind='task', id=other, principal='other')
        with self.assertRaises((ArtifactError, ServiceError)):
            denied.link('wrong-owner', [reference])

    def test_legacy_shared_actor_configuration_fails_closed_for_all_lifecycle_access(self):
        import json
        import shutil
        import subprocess
        from mas_host.shared_backup import postgres_environment
        from mas_host.shared_artifact_transfer import SharedArtifactScope, SharedArtifactTransferService
        from mas_host.operator_service import OperatorService
        from mas_host.service_contracts import ServiceError
        service = self.lifecycle_service(('alice', 'bob'))
        alice = service.submit('alice', 'request-a', 'leaf', {})['task_id']
        scope = SharedArtifactScope(self.authority, self.root / 'objects', 'results',
            {'alice': 'writer'}, self.policy, self.backend)
        SharedArtifactTransferService(self.root / 'transfer-cache', {'results': scope})
        reference = self.writer.put_bytes(b'legacy shared actor')
        a = self.writer.with_retention_target(kind='task', id=alice, principal='alice').link('context', [reference])
        self.writer.link('unmapped-owner', [reference])
        # Simulate a pre-fix retained configuration: distinct scopes aliased two
        # public owners to the same backing namespace/actor. New configuration
        # rejects this; no normal command is used to manufacture the old state.
        old_scope = {'namespace': 'results', 'principals': {'bob': 'writer'},
            'binding': '2' * 64, 'max_artifact_bytes': 1024}
        source = "SET search_path TO " + self.schema + ",pg_catalog; UPDATE transfer_configuration SET descriptor=jsonb_set(descriptor::jsonb,'{scopes,bob-scope}','" + json.dumps(old_scope).replace("'", "''") + "'::jsonb)::text;"
        result = subprocess.run([shutil.which('psql'), '--no-psqlrc', '--no-password', '--set=ON_ERROR_STOP=1'],
            input=source, text=True, env=postgres_environment(self.authority), stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, timeout=30)
        self.assertEqual(result.returncode, 0)
        operator = OperatorService(service, operators={'admin': {'owners': ['alice'], 'deployment_admin': True}})
        owner = {'principal': 'alice', 'namespace': 'results', 'artifact_principal': 'writer'}
        with self.assertRaises(ServiceError):
            operator.dispatch('admin', 'artifact_links', owner)
        for link in (a['link_id'], 'unmapped-owner'):
            with self.assertRaises(ServiceError):
                operator.dispatch('admin', 'artifact_release', {**owner, 'link_id': link,
                    'request_key': 'release-' + link, 'reason': 'Insufficient owner evidence'})
        with self.assertRaises(ServiceError):
            operator.dispatch('admin', 'artifact_bind', {**owner, 'link_id': 'unmapped-owner',
                'task_id': alice, 'request_key': 'bind-ambiguous', 'reason': 'Cannot prove original owner'})

    def test_replacement_cache_grants_hashes_revocation_and_retention(self):
        ref = self.writer.put_bytes(b'precise bytes', readers=('reader',))
        reader = self.store('reader')
        self.assertEqual(reader.read_bytes(ref), b'precise bytes')
        self.assertFalse(list(self.root.rglob('*.sqlite')))
        with self.assertRaises(ArtifactError):
            self.store('intruder').read_bytes(ref)
        with self.assertRaises(ArtifactError):
            self.store('intruder').link('fake-release', [ref], state='released')
        reader.ensure_link('consumer', [ref])
        reader.link('consumer', [ref], state='released')
        self.assertEqual(reader.ensure_link('consumer', [ref])['state'], 'released')
        self.writer.revoke(ref, 'reader')
        with self.assertRaises(ArtifactError):
            reader.read_bytes(ref)
        self.assertEqual(self.writer.put_bytes(b'precise bytes', readers=('reader',)), ref)
        with self.assertRaises(ArtifactError):
            reader.read_bytes(ref)  # re-publication cannot undo a revocation
        self.client.corrupt = True
        with self.assertRaises(ArtifactError):
            self.writer.verify(ref)

    def test_lost_put_retains_charge_without_publishing_then_recovers(self):
        self.client.lose_reply = True
        with self.assertRaises(ArtifactError):
            self.writer.put_bytes(b'known immutable input')
        status = self.writer.status()
        self.assertEqual(status['artifacts'], [])
        self.assertEqual(status['content'], {'objects': 1, 'reserved_bytes': 21, 'unverified': 1})
        self.client.lose_reply = False
        replacement = self.store('writer')
        ref = replacement.put_bytes(b'known immutable input')
        self.assertEqual(replacement.read_bytes(ref), b'known immutable input')
        self.assertEqual(replacement.status()['content']['objects'], 1)

    def test_epoch_change_during_upload_cannot_publish_from_stale_controller(self):
        put = self.client.put_object
        binding = []
        def fenced_put(**request):
            result = put(**request)
            binding.append(self.authority.handover('replacement'))
            return result
        self.client.put_object = fenced_put
        with self.assertRaises(ArtifactError):
            self.writer.put_bytes(b'escaped original upload')
        self.client.put_object = put
        successor = self.authority_for('replacement', binding[0]['epoch'])
        replacement = self.store('writer', authority=successor)
        self.assertEqual(replacement.status()['artifacts'], [])
        self.assertEqual(replacement.status()['content']['unverified'], 1)
        ref = replacement.put_bytes(b'escaped original upload')
        self.assertEqual(replacement.read_bytes(ref), b'escaped original upload')
        self.assertEqual(replacement.status()['content']['objects'], 1)
        with self.assertRaises(ArtifactError):
            self.writer.put_bytes(b'new stale bytes')

    def test_concurrent_namespace_reservations_hold_total_byte_limit(self):
        narrow = ArtifactPolicy(max_artifact_bytes=1024, max_total_bytes=1024, max_artifacts=4, max_links=4)
        store = SharedArtifactStore(self.authority, namespace='bounded', principal='writer',
                                    content_backend=self.backend, policy=narrow)
        def reserve(index):
            data = bytes([index]) * 700
            provisional = ArtifactRef('bounded', '0'*64, hashlib.sha256(data).hexdigest(), len(data),
                                       'application/octet-stream', 'writer')
            ref = replace(provisional, artifact_id=_manifest_identity(provisional, []))
            try:
                store._command('reserve', {'reference': ref.to_dict(), 'readers': []})
                return True
            except ArtifactError:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(reserve, range(4)))
        self.assertEqual(sum(results), 1)
        self.assertEqual(store.status()['content'], {'objects': 1, 'reserved_bytes': 700, 'unverified': 1})
        self.assertEqual(self.client.calls, [])

    def test_forged_reference_or_changed_backend_cannot_reuse_namespace(self):
        ref = self.writer.put_bytes(b'valid')
        forged = replace(ref, artifact_id='f'*64)
        with self.assertRaises(ArtifactError):
            self.writer._command('reserve', {'reference': forged.to_dict(), 'readers': []})
        with self.assertRaises(ArtifactError):
            SharedArtifactStore(self.authority, namespace='results', principal='writer', policy=self.policy,
                content_backend=S3ArtifactContent(replace(self.config, prefix='changed'), client=self.client))
        self.assertEqual(self.writer.status()['content']['objects'], 1)


if __name__ == '__main__':
    unittest.main()
