#!/usr/bin/env python3
"""Required real-daemon containment gate; absence is a failed gate, never a skip."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'host'))

from mas_host import Runner
from mas_host.artifacts import ArtifactStore
from mas_host.container_execution import ContainerSpec, DockerRuntime
from mas_host.executable_worker import RegisteredProcessExecutor
from mas_host.workspaces import (WorkspaceAdapter, WorkspaceExecutor, WorkspaceInput,
    WorkspaceManager, WorkspaceOutput, WorkspacePolicy, WorkspaceTool)
from mas_host.journal import inspect_run


PROBE = b'''import json, os, pathlib, socket
assert os.getuid() == os.getgid() == 65532
assert os.getgroups() == []
status = pathlib.Path('/proc/self/status').read_text()
for field in ('CapEff', 'CapPrm', 'CapInh', 'CapAmb'):
    assert field + ':\\t0000000000000000' in status
try:
    os.kill(os.getppid(), 0)
except PermissionError:
    pass
else:
    raise AssertionError('application can signal its root deadline supervisor')
try:
    pathlib.Path('/workspace/inputs/probe.py').write_text('changed')
except PermissionError:
    pass
else:
    raise AssertionError('input snapshot was writable')
try:
    pathlib.Path('__HOST_SECRET_PATH__').read_bytes()
except FileNotFoundError:
    pass
else:
    raise AssertionError('unapproved host file was visible')
try:
    socket.create_connection(('192.0.2.1', 443), timeout=1)
except OSError:
    pass
else:
    raise AssertionError('external network was reachable')
routes = pathlib.Path('/proc/net/route').read_text().splitlines()[1:]
assert all(row.split()[0] == 'lo' for row in routes), 'external IP route exists'
assert not pathlib.Path('/var/run/docker.sock').exists()
private = pathlib.Path('/workspace/outputs/private')
private.mkdir(mode=0o700)
result = private / 'result.json'
result.write_text(json.dumps({'isolated': True}))
result.chmod(0o600)
print('permitted test command passed')
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True, help='retained sha256 image ID or repository@sha256 digest')
    parser.add_argument('--docker', default='/usr/bin/docker')
    parser.add_argument('--socket', default='/var/run/docker.sock')
    parser.add_argument('--mas', default=os.environ.get('MAS_BIN', 'mas'))
    parser.add_argument('--gateway', default=os.environ.get('MAS_GATEWAY_BIN', 'mas-gateway'))
    args = parser.parse_args()
    runtime = DockerRuntime(args.docker, socket_path=args.socket)
    controls = json.loads(runtime.exchange(['info', '--format', '{{json .}}'],
        deadline=time.monotonic()+10, limit=65536))
    # Fixed control names only: never dump registry/auth/daemon endpoint data.
    print(json.dumps({'container_controls': {key: controls.get(key) for key in (
        'OSType', 'CgroupVersion', 'MemoryLimit', 'SwapLimit', 'PidsLimit', 'CpuCfsQuota', 'SecurityOptions')}}),
        file=sys.stderr)
    spec = ContainerSpec('probe', '1', args.image, ('/usr/local/bin/python', '-I', '/workspace/inputs/probe.py'))
    runtime.ready(spec)
    gateway = shutil.which(args.gateway)
    mas = shutil.which(args.mas)
    if not gateway or not mas:
        raise RuntimeError('Rust protocol binaries are required for the containment gate')
    topology = {'agents': ['a', 'b'], 'edges': [['a', 'b'], ['b', 'a']],
        'max_messages': 20, 'max_calls': 4, 'max_bytes': 100000,
        'mailbox_capacity': 10, 'wall_seconds': 60, 'application': 'agent-task/1'}
    with tempfile.TemporaryDirectory(prefix='mas-container-gate-') as temporary:
        root = Path(temporary)
        secret = root / 'host-secret'
        secret.write_text('host-only-boundary-test')
        secret.chmod(0o600)
        store = ArtifactStore(root/'artifacts', namespace='container-gate', principal='owner')
        manager = WorkspaceManager(root/'workspaces', principal='owner', artifacts=store)
        policy = WorkspacePolicy('contained', '1', ('probe',), profile='isolated-container-execution')
        source = store.put_bytes(PROBE.replace(b'__HOST_SECRET_PATH__', str(secret).encode()), media_type='text/x-python')
        reference = manager.prepare('container-run', 'attempt-1', policy, (WorkspaceInput('probe.py', source),))
        adapter = WorkspaceAdapter(manager, reference, policy, WorkspaceTool('probe', spec, reservation_microusd=1),
            (WorkspaceOutput('private/result.json', 'application/json'), WorkspaceOutput('stdout.txt', 'text/plain')))
        actors = topology['agents']
        adapters = {actor: adapter for actor in actors}
        executor = WorkspaceExecutor(RegisteredProcessExecutor(gateway), container_runtime=runtime)
        with Runner(topology, adapters, root/'run', mas_bin=mas, gateway_bin=gateway,
                    workflow_version='container-gate/1', executor=executor) as runner:
            sender, recipient = topology['edges'][0]
            completed = runner.request(sender, recipient, 'probe', {})
            result = completed.output
            if result['result']['exit_code'] != 0:
                raise RuntimeError('container isolation probe failed')
            if len(result['artifacts']) != 2:
                raise RuntimeError('container outputs were not published')
            if json.loads(store.read_bytes(result['artifacts'][0])) != {'isolated': True}:
                raise RuntimeError('container probe evidence differs')
            run_dir = runner.run_dir
        with Runner.resume(run_dir, adapters, workflow_version='container-gate/1',
                mas_bin=mas, gateway_bin=gateway, executor=executor) as resumed:
            if resumed.request(sender, recipient, 'probe', {}).output != result:
                raise RuntimeError('container completion replay changed')
        calls = inspect_run(run_dir)['invocations']
        if len(calls) != 1 or calls[0]['state'] != 'completed':
            raise RuntimeError('container replay duplicated an admitted invocation')
        remaining = runtime.exchange(['ps', '--all', '--filter', 'label=mas.workspace='+reference.identity,
                                      '--format', '{{.ID}}'], deadline=time.monotonic()+5, limit=1024)
        if remaining.strip():
            raise RuntimeError('container cleanup left an owned container')
    print(json.dumps({'format': 'mas-container-qualification/1', 'passed': True,
                      'image': args.image, 'checks': ['nonroot', 'capabilities-cleared', 'deadline-supervisor-protected',
                      'readonly-inputs', 'host-path-absent', 'network-denied', 'no-daemon-socket',
                      'permitted-tool', 'restrictive-output-permissions', 'verified-artifacts', 'replay', 'cleanup']}))


if __name__ == '__main__':
    main()
