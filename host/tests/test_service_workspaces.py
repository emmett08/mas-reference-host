"""Trusted workspace execution through durable service ownership and replay."""
from dataclasses import replace
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from mas_host import Outcome
from mas_host.artifacts import ArtifactRef, ArtifactStore
from mas_host.executable_worker import ProfileExecutor, WorkerSpec
from mas_host.journal import inspect_run
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from mas_host.workspaces import (WorkspaceAdapter, WorkspaceInput, WorkspaceManager,
    WorkspaceOutput, WorkspacePolicy, WorkspaceRef, WorkspaceTool)
from test_runner import topology


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).resolve().parent / 'fixtures/workspace_worker.py'
MAS = os.environ.get('MAS_BIN', str(ROOT / 'rust-protocol/target/release/mas'))
GATEWAY = os.environ.get('MAS_GATEWAY_BIN', str(ROOT / 'rust-protocol/target/release/mas-gateway'))


class Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


class LostServiceReceipt(BaseException):
    pass


class WorkspaceRegistration:
    """Trusted factory fixes capabilities while binding each assigned attempt."""
    def __init__(self, directory, *, artifacts=None):
        self.directory = Path(directory)
        self.artifacts = artifacts or ArtifactStore(self.directory / 'artifacts', namespace='project', principal='alice')
        self.manager = WorkspaceManager(self.directory / 'workspaces', principal='alice', artifacts=self.artifacts)
        self.source = self.artifacts.put_bytes(b'approved source', media_type='text/plain')
        self.inputs = (WorkspaceInput('source.txt', self.source),)
        self.policy = WorkspacePolicy('coding', '1', ('uppercase',))
        self.outputs = (WorkspaceOutput('result.txt', 'text/plain'),)
        self.tool = self.make_tool()
        self.dispatch_tool = self.tool
        self.references = []

    @staticmethod
    def make_tool(mode='normal'):
        worker = WorkerSpec('uppercase', '1', (sys.executable, str(SCRIPT)),
            factory='workspace-fixture', configuration={'mode': mode}, cwd=ROOT,
            environment={'PYTHONPATH': str(ROOT / 'host')})
        return WorkspaceTool('uppercase', worker, reservation_microusd=1)

    def adapters(self, reference, policy, tool, outputs):
        return {'a': Echo(), 'b': WorkspaceAdapter(self.manager, reference, policy, tool, outputs), 'c': Echo()}

    def registration(self, *, tool=None, policy=None, outputs=None):
        tool, policy = tool or self.tool, policy or self.policy
        outputs = self.outputs if outputs is None else outputs
        template = WorkspaceRef({'format': 'mas-workspace/1', 'principal': 'alice',
            'run': 'registration', 'attempt': 'registration', 'policy': policy.descriptor,
            'inputs': [{'path': item.path, 'reference': item.reference.to_dict()} for item in self.inputs],
            'context': None})

        def assigned(binding):
            if binding.principal != self.manager.principal:
                raise ServiceError('forbidden')
            reference = self.manager.prepare(binding.run_dir.name, 'revision-' + str(binding.revision),
                policy, self.inputs)
            self.references.append(reference)
            return self.adapters(reference, policy, self.dispatch_tool, outputs)

        return Workflow('workspace-service/1', topology(), self.adapters(template, policy, tool, outputs),
                        'a', 'b', 'uppercase', adapter_factory=assigned)

    def output_path(self, reference):
        return self.manager.root / reference.workspace_id / 'outputs/result.txt'


@unittest.skipUnless(sys.platform.startswith('linux'), 'workspace workers require Linux')
class WorkspaceServiceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.fixture = WorkspaceRegistration(self.directory / 'service')
        self.selected = self.fixture.registration()

    def service(self):
        return DurableTaskService(self.directory / 'service', {'uppercase': self.selected},
            {'alice': {'uppercase'}}, mas_bin=MAS, gateway_bin=GATEWAY)

    def test_service_binds_distinct_attempts_and_publishes_verified_artefacts(self):
        service = self.service()
        references = []
        for key in ('first', 'second'):
            queued = service.submit('alice', key, 'uppercase', {'untrusted_tool': 'ignored'})
            done = service.run_once(queued['task_id'])
            self.assertEqual(done['state'], 'completed', done)
            self.assertEqual(done['workflow_identity'], self.selected.identity)
            output = service.result('alice', queued['task_id'])
            reference = self.fixture.references[-1]
            references.append(reference)
            self.assertEqual(output['workspace'], reference.identity)
            self.assertEqual(output['result']['cwd'], reference.workspace_id)
            self.assertNotEqual(reference.descriptor['run'], 'registration')
            published = ArtifactRef.from_dict(output['artifacts'][0])
            self.assertEqual(self.fixture.artifacts.read_bytes(published), b'APPROVED SOURCE')
            self.assertEqual(published.input_sha256s, (self.fixture.source.sha256,))
            execution = service.recovery_runs('alice', queued['task_id'])[0]
            calls = inspect_run(service.state_dir / execution['run_directory'])['invocations']
            self.assertEqual([(call['state'], call['reservation_microusd']) for call in calls], [('completed', 1)])
        self.assertNotEqual(references[0].identity, references[1].identity)
        usage = service.operations.health()['usage']
        self.assertEqual((usage['runs'], usage['calls'], usage['reserved_microusd']), (2, 2, 2))

    def test_lost_service_receipt_replays_recorded_workspace_output_without_reexecution(self):
        service = self.service()
        queued = service.submit('alice', 'lost-receipt', 'uppercase', {})
        execute = ProfileExecutor.execute

        def lose_receipt(executor, adapter, *args, **kwargs):
            outcome = execute(executor, adapter, *args, **kwargs)
            if isinstance(adapter, WorkspaceAdapter):
                # Invocation completion is durable, but the enclosing request's
                # terminal and the task completion receipt are not yet saved.
                raise LostServiceReceipt()
            return outcome

        with mock.patch.object(ProfileExecutor, 'execute', new=lose_receipt):
            with self.assertRaises(LostServiceReceipt):
                service.run_once(queued['task_id'])
        self.assertEqual(service.inspect('alice', queued['task_id'])['state'], 'running')
        before = service.recovery_runs('alice', queued['task_id'])[0]
        reference = self.fixture.references[-1]
        output_file = self.fixture.output_path(reference)
        self.assertEqual(output_file.read_bytes(), b'APPROVED SOURCE')
        output_file.unlink()
        usage_before = service.operations.health()['usage']
        resumed = self.service()
        done = resumed.run_once(queued['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        result = resumed.result('alice', queued['task_id'])
        self.assertEqual(result['workspace'], reference.identity)
        self.assertEqual(self.fixture.artifacts.read_bytes(ArtifactRef.from_dict(result['artifacts'][0])), b'APPROVED SOURCE')
        self.assertFalse(output_file.exists(), 'durable replay executed the approved tool again')
        self.assertEqual(resumed.recovery_runs('alice', queued['task_id'])[0]['run_directory'], before['run_directory'])
        self.assertEqual(resumed.operations.health()['usage'], usage_before)
        self.assertEqual(len(inspect_run(resumed.state_dir / before['run_directory'])['invocations']), 1)
        self.assertEqual(resumed.submit('alice', 'lost-receipt', 'uppercase', {})['task_id'], queued['task_id'])

    def test_static_tool_policy_and_output_changes_have_distinct_workflow_identities(self):
        replacements = (
            self.fixture.registration(tool=self.fixture.make_tool('nested-effect')),
            self.fixture.registration(policy=replace(self.fixture.policy, max_output_bytes=128)),
            self.fixture.registration(outputs=(WorkspaceOutput('result.txt', 'application/octet-stream'),)),
        )
        for changed in replacements:
            with self.subTest(identity=changed.identity):
                self.assertNotEqual(self.selected.identity, changed.identity)
                self.assertEqual(self.selected.verified_implementation.digest, changed.verified_implementation.digest)
                with self.assertRaises(ServiceError):
                    self.selected.verify_adapters(dict(changed.adapters))

    def test_factory_tool_configuration_substitution_is_rejected_before_effect_admission(self):
        service = self.service()
        queued = service.submit('alice', 'changed-tool', 'uppercase', {})
        self.fixture.dispatch_tool = self.fixture.make_tool('nested-effect')
        with self.assertRaises(ServiceError) as error:
            service.run_once(queued['task_id'])
        self.assertEqual(error.exception.code, 'implementation_worker_binding_mismatch')
        usage = service.operations.health()['usage']
        self.assertEqual((usage['runs'], usage['calls'], usage['reserved_microusd']), (0, 0, 0))
        self.assertFalse(self.fixture.output_path(self.fixture.references[-1]).exists())
        self.assertEqual(service.inspect('alice', queued['task_id'])['workflow_identity'], self.selected.identity)


if __name__ == '__main__':
    unittest.main()
