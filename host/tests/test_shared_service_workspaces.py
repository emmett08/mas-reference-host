"""Native shared workspace lifecycle with real exec and process-shared S3 doubles."""
import base64
import hashlib
from io import BytesIO
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid

from mas_host.artifacts import ArtifactPolicy, ArtifactRef
from mas_host.executable_worker import ProfileExecutor
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig
from mas_host.shared_artifacts import SharedArtifactStore
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_service import create_shared_service
from mas_host.workspaces import WorkspaceAdapter, WorkspaceRef
from test_s3_artifacts import PreconditionFailed
from test_service_workspaces import ROOT, LostServiceReceipt, WorkspaceRegistration


GATEWAY = os.environ.get('MAS_GATEWAY_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-gateway')


class ProcessSharedObjectClient:
    """Test-only S3 bytes outside disposable execution caches.

    A child process publishes a complete temporary file then links it into the
    immutable object namespace. Existing names return S3's conditional-write
    failure, and readers never observe a partially written object.
    """
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def _path(self, request):
        key = json.dumps([request['Bucket'], request['Key']], separators=(',', ':')).encode()
        return self.root / hashlib.sha256(key).hexdigest()

    def put_object(self, **request):
        data = request['Body'].read()
        if (request['IfNoneMatch'] != '*' or len(data) != request['ContentLength']
                or hashlib.sha256(data).digest() != base64.b64decode(request['ChecksumSHA256'], validate=True)):
            raise ValueError('invalid object publication')
        temporary = self.root / ('.pending-' + uuid.uuid4().hex)
        try:
            with temporary.open('xb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, self._path(request))
            except FileExistsError:
                raise PreconditionFailed() from None
        finally:
            temporary.unlink(missing_ok=True)
        return {}

    def get_object(self, **request):
        data = self._path(request).read_bytes()
        return {'ContentLength': len(data), 'Body': BytesIO(data)}


@unittest.skipUnless('fork' in multiprocessing.get_all_start_methods(), 'requires forked composition workers')
class WorkspaceObjectTransportTests(unittest.TestCase):
    def test_forked_publication_is_visible_to_parent_and_cannot_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = ProcessSharedObjectClient(root / 'objects')
            backend = S3ArtifactContent(S3ContentConfig('https://objects.example', 'test-bucket',
                'mas', 'us-east-1', root / 'unused-credentials'), client=client)
            data = b'published by a composition child'
            reference = ArtifactRef('results', '0' * 64, hashlib.sha256(data).hexdigest(),
                                    len(data), 'text/plain', 'alice')
            process = multiprocessing.get_context('fork').Process(target=backend.publish_object,
                args=('0' * 32, reference, BytesIO(data), ArtifactPolicy()))
            process.start()
            try:
                process.join(10)
                self.assertFalse(process.is_alive(), 'object fixture publication hung')
                self.assertEqual(process.exitcode, 0)
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(5)
                process.close()
            self.assertEqual(backend.verify_object('0' * 32, reference, collect=True), data)
            key = {'Bucket': 'test-bucket', 'Key': 'conditional-write', 'IfNoneMatch': '*',
                   'ContentLength': len(data), 'ChecksumSHA256': base64.b64encode(hashlib.sha256(data).digest()).decode()}
            client.put_object(**key, Body=BytesIO(data))
            with self.assertRaises(PreconditionFailed):
                client.put_object(**key, Body=BytesIO(data))
            invalid = {**key, 'Key': 'invalid-checksum', 'ChecksumSHA256': base64.b64encode(b'x' * 32).decode()}
            with self.assertRaises(ValueError):
                client.put_object(**invalid, Body=BytesIO(data))
            self.assertFalse(list(client.root.glob('.pending-*')))


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedWorkspaceServiceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.config = {'binary': os.environ.get('MAS_SHARED_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-shared'),
            'dsn': os.environ['MAS_TEST_POSTGRES_DSN'], 'namespace': 'mas_workspace_' + uuid.uuid4().hex,
            'deployment': 'deployment', 'controller': 'controller', 'epoch': 1,
            'secret_key': b'workspace-fixture-secret' * 2, 'secret_reference': 'fixture/1'}
        self.authority = self.authority_for()
        self.authority.initialise()
        self.backend = S3ArtifactContent(S3ContentConfig('https://objects.example', 'test-bucket',
            'mas', 'us-east-1', self.directory / 'unused-credentials'),
            client=ProcessSharedObjectClient(self.directory / 'object-storage-fixture'))

    def authority_for(self, **overrides):
        authority = SharedAuthority(**(self.config | overrides))
        self.addCleanup(authority.close)
        return authority

    def service(self, cache, authority=None):
        authority = authority or self.authority
        artifacts = SharedArtifactStore(authority, namespace='project', principal='alice', content_backend=self.backend)
        fixture = WorkspaceRegistration(self.directory / cache, artifacts=artifacts)
        selected = fixture.registration()
        sequence = {'format': 'mas-workflow/1', 'name': 'sequence', 'version': '1',
            'limits': {'max_steps': 10, 'max_parallel': 1, 'wall_seconds': 120,
                       'max_calls': 10, 'max_reserved_microusd': 20},
            'root': {'kind': 'sequence', 'steps': [{'kind': 'task', 'workflow': 'uppercase'}] * 2}}
        service = create_shared_service(authority, self.directory / cache, {'uppercase': selected},
            {'alice': {'uppercase', 'sequence'}}, plans={'sequence': sequence}, gateway_bin=GATEWAY,
            allocation_limits={scope: {'max_runs': 20, 'max_calls': 20, 'max_microusd': 100}
                               for scope in ('deployment', 'principal/alice')})
        return service, fixture, selected

    def test_replacement_replays_completed_workspace_from_native_journal_and_shared_artefacts(self):
        service, fixture, selected = self.service('first-cache')
        queued = service.submit('alice', 'lost-workspace', 'uppercase', {})
        execute = ProfileExecutor.execute

        def lose_receipt(executor, adapter, *args, **kwargs):
            outcome = execute(executor, adapter, *args, **kwargs)
            if isinstance(adapter, WorkspaceAdapter):
                # Re-enter WorkspaceExecutor during recovery: only the native
                # invocation checkpoint exists, not the root terminal reply.
                raise LostServiceReceipt()
            return outcome

        with mock.patch.object(ProfileExecutor, 'execute', new=lose_receipt):
            with self.assertRaises(LostServiceReceipt):
                service.run_once(queued['task_id'])
        original_run = service.recovery_runs('alice', queued['task_id'])[0]['run_id']
        self.assertEqual(service.inspect('alice', queued['task_id'])['state'], 'running')
        output_file = fixture.output_path(fixture.references[-1])
        self.assertEqual(output_file.read_bytes(), b'APPROVED SOURCE')
        output_file.unlink()
        before = self.authority.query('scope', {'scope': 'deployment'})
        steps = self.authority.query('steps', {'run': original_run})
        self.assertFalse(any(row['data'].get('terminal') for row in steps if row['kind'] == 'request'))
        handover = self.authority.handover('replacement')
        successor = self.authority_for(controller='replacement', epoch=handover['epoch'])
        restarted, replacement_fixture, rebound = self.service('empty-replacement-cache', successor)
        self.assertEqual(rebound.identity, selected.identity)
        done = restarted.run_once(queued['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        result = restarted.result('alice', queued['task_id'])
        reference = ArtifactRef.from_dict(result['artifacts'][0])
        self.assertEqual(replacement_fixture.artifacts.read_bytes(reference), b'APPROVED SOURCE')
        self.assertEqual(reference.input_sha256s, (fixture.source.sha256,))
        self.assertFalse(output_file.exists())
        self.assertFalse(replacement_fixture.output_path(replacement_fixture.references[-1]).exists(),
                         'replacement executed the workspace tool instead of replaying its native checkpoint')
        self.assertEqual(restarted.recovery_runs('alice', queued['task_id'])[0]['run_id'], original_run)
        self.assertEqual(successor.query('scope', {'scope': 'deployment'}), before)
        calls = successor.query('invocations', {'run': original_run})
        self.assertEqual([(call['state'], call['reservation_microusd']) for call in calls], [('completed', 1)])
        self.assertFalse(list(self.directory.rglob('*.sqlite')), 'shared ownership leaked into local SQLite')

    def test_shared_composition_dispatches_separate_registered_workspace_attempts(self):
        service, fixture, _selected = self.service('composition-cache')
        queued = service.submit('alice', 'workspace-sequence', 'sequence', {})
        done = service.run_once(queued['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        result = service.result('alice', queued['task_id'])
        self.assertEqual(fixture.artifacts.read_bytes(ArtifactRef.from_dict(result['artifacts'][0])), b'APPROVED SOURCE')
        # The factory executes in composition child processes. Inspect their
        # retained manifests, not the parent's unchanged Python list.
        references = [WorkspaceRef(json.loads(path.read_text()))
                      for path in fixture.manager.root.glob('ws-*/manifest.json')]
        self.assertEqual(len(references), 2)
        self.assertNotEqual(references[0].identity, references[1].identity)
        runs = service.recovery_runs('alice', queued['task_id'])
        self.assertEqual(len(runs), 2)
        by_run = {reference.descriptor['run']: reference for reference in references}
        self.assertEqual(set(by_run), {run['run_id'] for run in runs})
        for run in runs:
            reference = by_run[run['run_id']]
            self.assertEqual(reference.descriptor['inputs'],
                             [{'path': 'source.txt', 'reference': fixture.source.to_dict()}])
            calls = self.authority.query('invocations', {'run': run['run_id']})
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]['state'], 'completed')
            self.assertEqual(calls[0]['output']['workspace'], reference.identity)
            self.assertEqual(fixture.artifacts.read_bytes(ArtifactRef.from_dict(calls[0]['output']['artifacts'][0])),
                             b'APPROVED SOURCE')
        self.assertIn(result['workspace'], {reference.identity for reference in references})
        self.assertFalse(list(self.directory.rglob('*.sqlite')))


if __name__ == '__main__':
    unittest.main()
