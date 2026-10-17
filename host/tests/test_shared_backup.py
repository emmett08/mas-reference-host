"""Real PostgreSQL native archive/audit/activation qualification (no database mock)."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest
from unittest import mock
import uuid

from mas_host.contracts import RunnerError
from mas_host.journal import canonical
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_backup import SharedBackupManager, SharedBackupError, postgres_environment


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedBackupIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.key = b'independent-recovery-fixture-key/' * 2
        binary = os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared')
        self.args = dict(binary=binary, namespace='mas_recovery_' + uuid.uuid4().hex,
            deployment='deployment', controller='controller-one', epoch=1,
            secret_key=b'controller-fixture-key' * 2, secret_reference='runtime-key/1')
        self.source = SharedAuthority(dsn=os.environ['MAS_TEST_POSTGRES_DSN'], **self.args)
        self.source.initialise()
        self.addCleanup(self.source.close)
        for scope in ('deployment', 'principal/alice'):
            self.source.configure_scope(scope, max_runs=10, max_calls=10, max_microusd=1000)
        topology = {'agents': ['a', 'b'], 'max_calls': 2, 'wall_seconds': 300}
        class Adapter:
            def handle(self, _context, _request):
                raise AssertionError('archive fixture never dispatches providers')
        runner = self.source.runner(topology, {'a': Adapter(), 'b': Adapter()},
            Path(self.temp.name) / 'cache', principal='alice', workflow_version='backup/1', supervised=False)
        owner, run = runner._session.owner, runner.run_id
        self.source.command('step', {'run': run, 'owner': owner, 'key': 'effect', 'kind': 'invoke', 'input': {}, 'initial': {}})
        self.source.command('prepare_invocation', {'run': run, 'owner': owner, 'step_key': 'effect',
            'actor': 'a', 'call_id': 'uncertain-call', 'reservation_microusd': 70})
        self.source.command('admit_invocation', {'run': run, 'owner': owner, 'call_id': 'uncertain-call'})
        runner.close()
        self.run = run
        from mas_host.shared_artifacts import SharedArtifactStore
        from mas_host.s3_artifacts import S3ArtifactContent
        from mas_host.s3_transport import S3ContentConfig
        from mas_host.artifacts import ArtifactPolicy
        import test_s3_artifacts as fixtures
        self.object_client = fixtures.ObjectClient()
        self.content = S3ArtifactContent(S3ContentConfig('https://objects.example', 'test-bucket',
            'mas', 'us-east-1', Path(self.temp.name) / 'unused-transport-double-secret'), client=self.object_client)
        self.artifacts = SharedArtifactStore(self.source, namespace='retained', principal='alice',
            content_backend=self.content, policy=ArtifactPolicy())
        self.reference = self.artifacts.put_bytes(b'retained exact object bytes')
        self.manager = SharedBackupManager(self.source, recovery_key=self.key, key_reference='recovery-key/1')
        self.target_name = 'mas_restore_' + uuid.uuid4().hex
        self._sql('CREATE DATABASE ' + self.target_name)
        self.addCleanup(self._sql, 'DROP DATABASE ' + self.target_name + ' WITH (FORCE)')
        self.target = SharedAuthority(dsn=os.environ['MAS_TEST_POSTGRES_DSN'] + " dbname='" + self.target_name + "'", **self.args)
        self.addCleanup(self.target.close)
        self.restore = SharedBackupManager(self.target, recovery_key=self.key, key_reference='recovery-key/1')

    def _sql(self, statement):
        psql = shutil.which('psql')
        self.assertIsNotNone(psql, 'CI must install native PostgreSQL client tools')
        result = subprocess.run([psql, '--no-psqlrc', '--no-password', '--set=ON_ERROR_STOP=1'],
            input=statement, text=True, env=postgres_environment(self.source),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        self.assertEqual(result.returncode, 0, 'isolated PostgreSQL fixture administration failed')

    def proof(self, verified, source):
        payload = {'format': 'mas-fencing-proof/1', **verified, 'source': source,
            'verified_object_digest': verified['object_digest'], 'operator': 'fencing-operator',
            'key_reference': 'recovery-key/1', 'fencing_evidence_reference': 'fixture/source-permanently-sealed',
            'next_controller': 'controller-two', 'expires_at_ms': int(time.time() * 1000) + 300_000}
        # This is the independent test fencing authority, not the recovery client.
        return {'payload': payload, 'signature': hmac.new(self.key, canonical(payload).encode(), hashlib.sha256).hexdigest()}

    def test_final_archive_restores_all_state_audit_closed_and_requires_external_proof(self):
        archive = Path(self.temp.name) / 'backup'
        manifest = self.manager.archive(archive, operator='operator', request_key='final-1',
            evidence={'fencing_change_reference': 'fixture/final-source'})
        before = self.manager.inspect()
        self.assertEqual(before['mode'], 'sealed')
        with self.assertRaises(RunnerError): self.source.renew()
        with self.assertRaises(RunnerError): self.source.takeover('controller-other')
        with self.assertRaises(RunnerError): self.source.query('scope', {'scope': 'deployment'})
        self.restore.restore_audit(archive)
        audit = self.restore.inspect()
        self.assertEqual(audit['mode'], 'audit')
        self.assertEqual(audit['state'], before['state'])
        with self.assertRaises(RunnerError): self.target.query('scope', {'scope': 'deployment'})
        with self.assertRaises(RunnerError): self.target.takeover('controller-other')
        def reader(namespace, digest, size):
            self.assertEqual((namespace, digest, size),
                (self.reference.namespace, self.reference.sha256, self.reference.size))
            return self.content.verify_object(self.artifacts.store_id, self.reference, collect=True)
        self.object_client.corrupt = True
        with self.assertRaises(Exception): self.restore.verify_objects(reader)
        self.object_client.corrupt = False
        verified = self.restore.verify_objects(reader)
        invalid = self.proof(verified, before['database'])
        invalid['payload']['state_digest'] = '0' * 64
        with self.assertRaises(SharedBackupError): self.restore.activate(fencing_proof=invalid, read_object=reader)
        self.assertEqual(self.restore.inspect()['mode'], 'audit')
        unsigned = self.proof(verified, before['database'])
        unsigned['signature'] = '0' * 64
        with self.assertRaises(SharedBackupError): self.restore.activate(fencing_proof=unsigned, read_object=reader)
        started = time.monotonic()
        binding = self.restore.activate(fencing_proof=self.proof(verified, before['database']), read_object=reader)
        self.assertLess(time.monotonic() - started, 60, 'reference activation gate exceeded declared 60s target')
        successor = SharedAuthority(dsn=self.target._dsn, **{**self.args,
            'controller': binding['controller'], 'epoch': binding['epoch']})
        self.addCleanup(successor.close)
        self.assertTrue(successor.authority_status()['draining'])
        self.assertEqual(successor.query('scope', {'scope': 'deployment'})['microusd'], 70)
        self.assertEqual(successor.query('invocation', {'run': self.run, 'call_id': 'uncertain-call'})['state'], 'executing')
        self.assertEqual(successor._request('status', run=self.run)['calls'][0]['state'], 'reserved')
        with self.assertRaises(RunnerError): self.source.handover('controller-three')
        self.assertEqual(manifest['payload']['seal']['payload']['state'], before['state'])

    def test_tampered_archive_and_nonempty_restore_target_are_rejected(self):
        archive = Path(self.temp.name) / 'backup'
        self.manager.archive(archive, operator='operator', request_key='final-1', evidence={'ticket': 'fixture'})
        with (archive / 'authority.pgdump').open('ab') as output: output.write(b'tamper')
        with self.assertRaises(SharedBackupError): self.restore.restore_audit(archive)
        self.assertEqual(self.manager.inspect()['mode'], 'sealed')
        valid = Path(self.temp.name) / 'valid'
        self.manager.archive(valid, operator='operator', request_key='final-1', evidence={'ticket': 'fixture'})
        self.restore.restore_audit(valid)
        with self.assertRaises(SharedBackupError): self.restore.restore_audit(valid)
        self.assertEqual(self.restore.inspect()['mode'], 'audit')

    def test_restore_uses_verified_descriptor_after_source_path_changes(self):
        archive = Path(self.temp.name) / 'backup'
        self.manager.archive(archive, operator='operator', request_key='final-1', evidence={'ticket': 'fixture'})
        original = self.restore._request
        def changed(operation, **fields):
            result = original(operation, **fields)
            if operation == 'recovery_target':
                (archive / 'authority.pgdump').write_bytes(b'unverified replacement is never restored')
            return result
        with mock.patch.object(self.restore, '_request', side_effect=changed):
            self.restore.restore_audit(archive)
        self.assertEqual(self.restore.inspect()['mode'], 'audit')

    def test_interruption_after_native_restore_remains_permanently_sealed(self):
        archive = Path(self.temp.name) / 'backup'
        manifest = self.manager.archive(archive, operator='operator', request_key='final-1', evidence={'ticket': 'fixture'})
        original = self.restore._request
        def interrupted(operation, **fields):
            if operation == 'recovery_audit':
                raise SharedBackupError('injected postrestore interruption')
            return original(operation, **fields)
        with mock.patch.object(self.restore, '_request', side_effect=interrupted):
            with self.assertRaises(SharedBackupError): self.restore.restore_audit(archive)
        self.assertEqual(self.restore.inspect()['mode'], 'sealed')
        with self.assertRaises(RunnerError): self.target.renew()
        with self.assertRaises(RunnerError): self.target.query('scope', {'scope': 'deployment'})
        payload = manifest['payload']
        original('recovery_audit', parameters={'seal': payload['seal'], 'archive_sha256': payload['archive_sha256']})
        self.assertEqual(self.restore.inspect()['mode'], 'audit')

    def test_symlink_and_shared_mode_archive_files_are_rejected(self):
        archive = Path(self.temp.name) / 'backup'
        self.manager.archive(archive, operator='operator', request_key='final-1', evidence={'ticket': 'fixture'})
        path = archive / 'authority.pgdump'
        path.chmod(0o644)
        with self.assertRaises(SharedBackupError): self.restore.restore_audit(archive)
        path.chmod(0o600)
        moved = archive / 'real.pgdump'
        path.rename(moved)
        path.symlink_to(moved.name)
        with self.assertRaises(SharedBackupError): self.restore.restore_audit(archive)
