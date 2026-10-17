"""Container policy and admitted transfers; real isolation has a separate gate."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import time
import unittest
from unittest.mock import Mock

from mas_host.artifacts import ArtifactStore, ArtifactError
from mas_host.container_execution import (ContainerSpec, ContainerError, DockerRuntime,
    container_name, decode_result, encode_request)
from mas_host.contracts import TaskRequest
from mas_host.workspaces import (WorkspaceAdapter, WorkspaceError, WorkspaceExecutor,
    WorkspaceInput, WorkspaceManager, WorkspaceOutput, WorkspacePolicy, WorkspaceTool)


IMAGE = 'sha256:' + 'a' * 64
REQUEST = TaskRequest('a', 'b', 'test', {'attempt': 1}, 'task', 'message')


def result(data=b'passed', path='result.txt', exit_code=0):
    header = json.dumps({'format': 'mas-container-result/1', 'exit_code': exit_code,
        'outputs': [{'path': path, 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()}]}).encode()
    return struct.pack('!I', len(header)) + header + data


class Context:
    def __init__(self):
        self.admissions, self.saved = 0, None

    def invoke(self, operation, *, input, reservation_microusd):
        if self.saved is not None:
            return self.saved
        self.admissions += 1
        self.saved = operation().output
        return self.saved


class Runtime:
    def __init__(self, response=None, fail=False):
        self.calls, self.response, self.fail = [], response or result(), fail

    def ready(self, spec):
        pass

    def run(self, spec, workspace, payload, **limits):
        self.calls.append((spec, workspace, payload, limits))
        if self.fail:
            raise ContainerError('container receipt unavailable')
        return self.response


class ContainerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.artifacts = ArtifactStore(self.root / 'artifacts', namespace='coding', principal='alice')
        self.manager = WorkspaceManager(self.root / 'workspaces', principal='alice', artifacts=self.artifacts)
        self.policy = WorkspacePolicy('isolated', '1', ('test',), profile='isolated-container-execution')
        self.spec = ContainerSpec('test', '1', IMAGE, ('/usr/local/bin/python', '/workspace/inputs/code.py'))
        self.input = self.artifacts.put_bytes(b'print("passed")', media_type='text/x-python')
        self.reference = self.manager.prepare('run', 'attempt', self.policy, (WorkspaceInput('code.py', self.input),))
        self.adapter = WorkspaceAdapter(self.manager, self.reference, self.policy,
            WorkspaceTool('test', self.spec, reservation_microusd=7), (WorkspaceOutput('result.txt'),))

    def test_registered_image_command_and_network_are_not_request_options(self):
        for image in ('python:latest', 'python:3.14', 'sha256:abc', 'host --privileged'):
            with self.assertRaises(ContainerError):
                replace(self.spec, image=image)
        for field, value in (('pids', True), ('cpus', 0), ('memory_bytes', 1), ('command', ('relative',))):
            with self.assertRaises(ContainerError):
                replace(self.spec, **{field: value})
        with self.assertRaises(WorkspaceError):
            replace(self.policy, allow_external_effects=True)
        with self.assertRaises(WorkspaceError):
            WorkspaceTool('test', self.spec, external_effects=True)
        with self.assertRaises(WorkspaceError):
            WorkspaceAdapter(self.manager, self.reference, replace(self.policy, profile='trusted-code-execution'),
                             self.adapter.tool, self.adapter.outputs)
        args = DockerRuntime.create_arguments('fixed', self.spec, self.reference.identity, 4096, 20)
        self.assertEqual(args[args.index('--network')+1], 'none')
        self.assertEqual(args[args.index('--pull')+1], 'never')
        self.assertEqual(args[args.index('--memory-swap')+1], str(self.spec.memory_bytes))
        self.assertIn('--read-only', args)
        self.assertIn('no-new-privileges=true', args)
        self.assertEqual([args[index+1] for index, value in enumerate(args) if value == '--cap-add'],
                         ['SETUID', 'SETGID', 'CHOWN', 'DAC_READ_SEARCH'])
        self.assertIn('nr_inodes=20', args[args.index('--tmpfs')+1])
        self.assertNotIn('--privileged', args)
        self.assertNotIn('--mount', args)
        self.assertNotIn('--volume', args)
        self.assertEqual(container_name(self.reference.identity, self.spec), container_name(self.reference.identity, self.spec))
        self.assertNotEqual(container_name(self.reference.identity, self.spec), container_name(self.reference.identity, replace(self.spec, version='2')))

    def test_complete_transfer_is_admitted_published_and_replayed(self):
        runtime, context = Runtime(), Context()
        executor = WorkspaceExecutor(None, container_runtime=runtime)
        first = executor.execute(self.adapter, context, REQUEST, deadline=time.monotonic()+10)
        self.assertEqual(first.output['result']['exit_code'], 0)
        self.assertEqual(len(first.output['artifacts']), 1)
        self.assertEqual(context.admissions, 1)
        self.assertEqual(len(runtime.calls), 1)
        self.assertEqual(self.manager.artifacts.read_bytes(first.output['artifacts'][0]), b'passed')
        repeated = executor.execute(self.adapter, context, REQUEST, deadline=time.monotonic()+10)
        self.assertEqual(repeated, first)
        self.assertEqual(len(runtime.calls), 1)
        payload = runtime.calls[0][2]
        size = struct.unpack('!I', payload[:4])[0]
        header = json.loads(payload[4:4+size])
        self.assertEqual(header['command'], list(self.spec.command))
        self.assertEqual(header['inputs'][0]['sha256'], self.input.sha256)
        self.assertEqual(header['request']['input'], REQUEST.input)
        self.assertEqual(payload[4+size:], b'print("passed")')

    def test_lost_receipt_never_restarts_attempt(self):
        runtime, context = Runtime(fail=True), Context()
        executor = WorkspaceExecutor(None, container_runtime=runtime)
        with self.assertRaises(ContainerError):
            executor.execute(self.adapter, context, REQUEST, deadline=time.monotonic()+10)
        state = json.loads((self.manager.root/self.reference.workspace_id/'state.json').read_text())
        self.assertEqual(state['state'], 'unknown')
        self.assertEqual(state['container_name'], container_name(self.reference.identity, self.spec))
        with self.assertRaisesRegex(WorkspaceError, 'already executed'):
            executor.execute(self.adapter, context, REQUEST, deadline=time.monotonic()+10)
        self.assertEqual(len(runtime.calls), 1)

    def test_output_traversal_corruption_oversize_and_cross_principal_never_publish(self):
        for response in (result(path='../escape'), result()+b'extra', result()[:-1], result(b'x'*10)):
            with self.subTest(response=response), self.assertRaises((ContainerError, WorkspaceError)):
                decode_result(response, self.adapter.outputs, 8)
        runtime = Runtime(response=result(path='../escape'))
        with self.assertRaises((ContainerError, WorkspaceError)):
            WorkspaceExecutor(None, container_runtime=runtime).execute(self.adapter, Context(), REQUEST, deadline=time.monotonic()+10)
        self.assertEqual(len(self.artifacts.status()['artifacts']), 1)
        bob = ArtifactStore(self.root/'artifacts', namespace='coding', principal='bob')
        manager = WorkspaceManager(self.root/'other', principal='bob', artifacts=bob)
        with self.assertRaises(ArtifactError):
            manager.prepare('run', 'attempt', self.policy, (WorkspaceInput('code.py', self.input),))

    def test_daemon_readiness_rejects_missing_isolation_controls(self):
        runtime = object.__new__(DockerRuntime)
        good = {'OSType': 'linux', 'CgroupVersion': '2', 'MemoryLimit': True, 'SwapLimit': True,
                'PidsLimit': True, 'CpuCfsQuota': True, 'SecurityOptions': ['name=seccomp,profile=builtin']}
        for key, value in (('OSType', 'windows'), ('CgroupVersion', '1'), ('MemoryLimit', False),
                           ('PidsLimit', False), ('CpuCfsQuota', False), ('SecurityOptions', [])):
            runtime.exchange = Mock(return_value=json.dumps({**good, key: value}).encode())
            with self.assertRaises(ContainerError):
                runtime.ready(self.spec)
        image = {'Id': IMAGE, 'Os': 'linux', 'Config': {'Labels': {'org.openai.mas.container-protocol': '1'}}}
        runtime.exchange = Mock(side_effect=[json.dumps(good).encode(), json.dumps(image).encode()])
        runtime.ready(self.spec)
        self.assertEqual(runtime.exchange.call_count, 2)
        image['Config']['Volumes'] = {'/undeclared': {}}
        runtime.exchange = Mock(side_effect=[json.dumps(good).encode(), json.dumps(image).encode()])
        with self.assertRaisesRegex(ContainerError, 'image violates'):
            runtime.ready(self.spec)

    def test_cleanup_is_attempted_with_exact_receipt_and_never_changes_identity(self):
        runtime = object.__new__(DockerRuntime)
        identifier = 'c'*64
        runtime.exchange = Mock(side_effect=[identifier.encode(), ContainerError('lost'), identifier.encode()])
        with self.assertRaises(ContainerError):
            runtime.run(self.spec, self.reference.identity, b'', scratch_bytes=4096,
                        scratch_files=20, output_limit=1000, deadline=time.monotonic()+10)
        self.assertEqual(runtime.exchange.call_args_list[-1].args[0], ['rm', '--force', identifier])
        runtime.exchange = Mock(side_effect=[ContainerError('lost create receipt')])
        with self.assertRaises(ContainerError):
            runtime.run(self.spec, self.reference.identity, b'', scratch_bytes=4096,
                        scratch_files=20, output_limit=1000, deadline=time.monotonic()+10)
        self.assertEqual(runtime.exchange.call_count, 1)


if __name__ == '__main__':
    unittest.main()
