"""Fixed Linux container strategy for isolated, network-free workspace tools.

The daemon and kernel are trusted. Container effects consume the enclosing host
invocation; neither image, executable, mounts nor network policy come from input.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import struct
import subprocess
import tempfile
import time

from .codec import integer_json
from .contracts import Invocation, Outcome, RunnerError


class ContainerError(RunnerError):
    """Sanitised container configuration, lifecycle or transfer failure."""


@dataclass(frozen=True)
class ContainerSpec:
    name: str
    version: str
    image: str
    command: tuple[str, ...]
    memory_bytes: int = 256 * 1024 * 1024
    cpu_seconds: int = 60
    cpus: int = 1
    pids: int = 64
    file_bytes: int = 16 * 1024 * 1024

    def __post_init__(self):
        if any(type(value) is not str or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}', value) is None
               for value in (self.name, self.version)):
            raise ContainerError('invalid container registration')
        if (type(self.image) is not str or len(self.image) > 512
                or re.fullmatch(r'(?:[a-z0-9][a-z0-9./:_-]*@)?sha256:[0-9a-f]{64}', self.image) is None):
            raise ContainerError('container image must have an immutable sha256 identity')
        if (type(self.command) is not tuple or not 1 <= len(self.command) <= 32
                or any(type(value) is not str or not value or '\0' in value or len(value) > 4096 for value in self.command)
                or not self.command[0].startswith('/')):
            raise ContainerError('invalid container command')
        for field, lower, upper in (('memory_bytes', 64*1024*1024, 16*1024**3),
                                   ('cpu_seconds', 1, 86400), ('cpus', 1, 64),
                                   ('pids', 8, 256), ('file_bytes', 1024, 256*1024*1024)):
            if type(getattr(self, field)) is not int or not lower <= getattr(self, field) <= upper:
                raise ContainerError('invalid container resource bound')

    @property
    def descriptor(self):
        return {**asdict(self), 'command': list(self.command), 'format': 'mas-container-tool/1',
                'network': 'none', 'host_mounts': [], 'image_root': 'readonly',
                'application_uid': 65532, 'configuration': {}}

    @property
    def identity(self):
        return hashlib.sha256(integer_json(self.descriptor).encode()).hexdigest()


def container_name(workspace, spec):
    return 'mas-ws-' + hashlib.sha256((workspace + ':' + spec.identity).encode()).hexdigest()[:48]


class DockerRuntime:
    """Explicit local daemon; no ambient Docker contexts, plugins or credentials."""

    def __init__(self, executable='/usr/bin/docker', *, socket_path='/var/run/docker.sock'):
        path = Path(executable)
        if (not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK)
                or type(socket_path) is not str or not socket_path.startswith('/')
                or any(character in socket_path for character in ('\0', '\n', '\r'))):
            raise ContainerError('container runtime unavailable')
        self.executable = str(path)
        self.digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.socket = socket_path

    def _command(self, arguments, config):
        if hashlib.sha256(Path(self.executable).read_bytes()).hexdigest() != self.digest:
            raise ContainerError('container runtime changed')
        return [self.executable, '--config', str(config), '--host', 'unix://' + self.socket, *arguments]

    def exchange(self, arguments, payload=b'', *, deadline, limit, tick=None):
        """Bound both pipe directions and stop without accumulating untrusted logs."""
        with tempfile.TemporaryDirectory(prefix='mas-docker-config-') as configuration:
            process = subprocess.Popen(self._command(arguments, configuration),
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env={'PATH': os.defpath}, close_fds=True, start_new_session=True)
            received, offset = bytearray(), 0
            try:
                with selectors.DefaultSelector() as selector:
                    os.set_blocking(process.stdout.fileno(), False)
                    selector.register(process.stdout, selectors.EVENT_READ)
                    if payload:
                        os.set_blocking(process.stdin.fileno(), False)
                        selector.register(process.stdin, selectors.EVENT_WRITE)
                    else:
                        process.stdin.close()
                    while selector.get_map():
                        if time.monotonic() >= deadline:
                            raise ContainerError('container operation deadline exceeded')
                        if tick is not None:
                            tick()
                        for key, _ in selector.select(min(.05, max(0, deadline-time.monotonic()))):
                            if key.fileobj is process.stdout:
                                chunk = os.read(key.fd, 65536)
                                if not chunk:
                                    selector.unregister(key.fileobj)
                                    process.stdout.close()
                                else:
                                    received.extend(chunk)
                                    if len(received) > limit:
                                        raise ContainerError('container response exceeds its bound')
                            else:
                                try:
                                    offset += os.write(key.fd, memoryview(payload)[offset:offset+65536])
                                except BrokenPipeError:
                                    offset = len(payload)
                                if offset == len(payload):
                                    selector.unregister(key.fileobj)
                                    process.stdin.close()
                    process.wait(timeout=max(.001, deadline-time.monotonic()))
                    if process.returncode != 0:
                        raise ContainerError('container operation failed')
                    return bytes(received)
            finally:
                if process.poll() is None:
                    process.kill()
                try:
                    process.wait(timeout=3)
                finally:
                    process.stdin.close()
                    process.stdout.close()

    def ready(self, spec):
        deadline = time.monotonic() + 10
        try:
            raw = self.exchange(['info', '--format', '{{json .}}'], deadline=deadline, limit=65536)
            info = json.loads(raw)
            if (info.get('OSType') != 'linux' or info.get('CgroupVersion') != '2'
                    or not all(info.get(key) is True for key in ('MemoryLimit', 'SwapLimit', 'PidsLimit', 'CpuCfsQuota'))
                    or not any('seccomp' in value for value in info.get('SecurityOptions', ()) if type(value) is str)):
                raise ContainerError('container daemon lacks required isolation controls')
            # --pull=never below ensures this cannot become an implicit network pull.
            image = json.loads(self.exchange(['image', 'inspect', '--format', '{{json .}}', spec.image],
                                            deadline=deadline, limit=65536))
            if (type(image) is not dict or image.get('Os') != 'linux'
                    or image.get('Config', {}).get('Volumes')
                    or image.get('Config', {}).get('Labels', {}).get('org.openai.mas.container-protocol') != '1'
                    or spec.image.startswith('sha256:') and image.get('Id') != spec.image
                    or '@' in spec.image and spec.image not in image.get('RepoDigests', ())):
                raise ContainerError('container image violates the registered execution profile')
        except (ValueError, AttributeError, TypeError, OSError, subprocess.TimeoutExpired):
            raise ContainerError('container readiness unavailable') from None

    @staticmethod
    def create_arguments(name, spec, workspace, scratch_bytes, scratch_files=256):
        return ['create', '--name', name, '--pull', 'never', '--network', 'none',
            '--read-only', '--cap-drop', 'ALL', '--cap-add', 'SETUID', '--cap-add', 'SETGID',
            '--cap-add', 'CHOWN', '--cap-add', 'DAC_READ_SEARCH',
            '--security-opt', 'no-new-privileges=true', '--cgroupns', 'private', '--ipc', 'private',
            '--user', '0:0', '--memory', str(spec.memory_bytes), '--memory-swap', str(spec.memory_bytes),
            '--cpus', str(spec.cpus), '--pids-limit', str(spec.pids), '--shm-size', '1048576',
            '--ulimit', 'nofile=64:64', '--ulimit', 'core=0:0', '--log-driver', 'none',
            '--no-healthcheck', '--restart', 'no', '--interactive',
            '--tmpfs', '/workspace:rw,noexec,nosuid,nodev,size=' + str(scratch_bytes) + ',nr_inodes=' + str(scratch_files) + ',mode=0700',
            '--label', 'mas.workspace=' + workspace, '--label', 'mas.tool=' + spec.identity,
            '--workdir', '/workspace', '--entrypoint', '/usr/local/bin/python',
            spec.image, '-I', '/opt/mas/container_entry.py']

    def run(self, spec, workspace, payload, *, scratch_bytes, scratch_files, output_limit, deadline, tick=None):
        name = container_name(workspace, spec)
        created = False
        try:
            identifier = self.exchange(self.create_arguments(name, spec, workspace, scratch_bytes, scratch_files),
                                      deadline=deadline, limit=256, tick=tick).decode().strip()
            if re.fullmatch(r'[0-9a-f]{64}', identifier) is None:
                raise ContainerError('invalid container receipt')
            created = True
            result = self.exchange(['start', '--attach', '--interactive', name], payload,
                                   deadline=deadline, limit=output_limit, tick=tick)
            # Use the observed immutable daemon ID, never a caller-supplied name.
            self.exchange(['rm', '--force', identifier], deadline=time.monotonic()+5, limit=256)
            created = False
            return result
        finally:
            if created:
                # This is cleanup of our observed container. Failure remains
                # uncertain and cannot authorise another start or invocation.
                self.exchange(['rm', '--force', identifier], deadline=time.monotonic()+5, limit=256)


def encode_request(spec, request, sources, outputs, *, remaining_ms, output_bytes):
    header = {'format': 'mas-container-transfer/1', 'command': list(spec.command),
              'request': asdict(request), 'remaining_ms': remaining_ms,
              'cpu_seconds': spec.cpu_seconds, 'file_bytes': spec.file_bytes,
              'output_bytes': output_bytes, 'outputs': [item.path for item in outputs],
              'inputs': [{'path': path, 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
                         for path, data in sources]}
    source = integer_json(header, 65536).encode()
    return struct.pack('!I', len(source)) + source + b''.join(data for _, data in sources)


def decode_result(source, outputs, maximum):
    from .workspaces import _parts
    try:
        if len(source) < 4:
            raise ValueError()
        size = struct.unpack('!I', source[:4])[0]
        if not 0 < size <= 65536 or len(source) < size+4:
            raise ValueError()
        header = json.loads(source[4:4+size])
        if (type(header) is not dict or set(header) != {'format', 'exit_code', 'outputs'}
                or header['format'] != 'mas-container-result/1' or type(header['exit_code']) is not int
                or not -255 <= header['exit_code'] <= 255 or type(header['outputs']) is not list
                or len(header['outputs']) != len(outputs)):
            raise ValueError()
        expected, values, offset, total = {item.path for item in outputs}, {}, 4+size, 0
        for item in header['outputs']:
            if type(item) is not dict or set(item) != {'path', 'size', 'sha256'}:
                raise ValueError()
            _parts(item['path'])
            if item['path'] not in expected or item['path'] in values or type(item['size']) is not int or not 0 <= item['size'] <= maximum:
                raise ValueError()
            total += item['size']
            if total > maximum or offset + item['size'] > len(source):
                raise ValueError()
            data = source[offset:offset+item['size']]
            offset += item['size']
            if hashlib.sha256(data).hexdigest() != item['sha256']:
                raise ValueError()
            values[item['path']] = data
        if offset != len(source) or set(values) != expected:
            raise ValueError()
        return header['exit_code'], values
    except (ValueError, TypeError, KeyError):
        raise ContainerError('invalid container output transfer') from None


def execute_workspace(runtime, adapter, context, request, *, deadline, tick=None):
    """Admit once; publish only complete, checked bytes after confirmed cleanup."""
    from .workspaces import _read, _write, WorkspaceError
    manager, reference, spec = adapter.manager, adapter.reference, adapter.tool.worker
    runtime.ready(spec)
    with manager.lease(reference) as (directory, _lease):
        manager._verify_inputs(reference, directory)
        sources = [('inputs/' + item['path'], _read(directory, 'inputs/' + item['path'], item['reference']['size']))
                   for item in reference.descriptor['inputs']]
        if reference.descriptor['context']:
            sources.append(('context.json', _read(directory, 'context.json', reference.descriptor['context']['size'])))
        state = json.loads(_read(directory, 'state.json', 65536))
        if state.get('state') == 'released':
            raise WorkspaceError('released workspace cannot execute')

        def invoke():
            if state.get('state') != 'prepared':
                raise WorkspaceError('workspace attempt already executed; reconcile or prepare a new attempt')
            name = container_name(reference.identity, spec)
            # This durable intent precedes even docker create. Lost create/start
            # receipts leave unknown work under the same name and host call ID.
            manager._state(directory, 'running', container_name=name, container_tool=spec.identity)
            try:
                remaining = int((deadline-time.monotonic())*1000)
                if not 0 < remaining <= 86_400_000:
                    raise ContainerError('invalid container execution deadline')
                payload = encode_request(spec, request, sources, adapter.outputs,
                    remaining_ms=remaining, output_bytes=adapter.policy.max_output_bytes)
                result = runtime.run(spec, reference.identity, payload,
                    scratch_bytes=adapter.policy.max_workspace_bytes,
                    scratch_files=adapter.policy.max_workspace_files,
                    output_limit=adapter.policy.max_output_bytes+65540, deadline=deadline, tick=tick)
                exit_code, outputs = decode_result(result, adapter.outputs, adapter.policy.max_output_bytes)
                for path, data in outputs.items():
                    _write(directory, 'outputs/' + path, data)
                references = manager._publish(reference, directory, adapter.policy, adapter.outputs)
                manager._state(directory, 'quiescent', artifacts=references, container_name=name)
                return Invocation({'result': {'exit_code': exit_code}, 'artifacts': references,
                    'workspace': reference.identity}, {'workspace': reference.identity, 'worker': spec.identity,
                    'container_name': name, 'image': spec.image, 'tool': adapter.tool.name,
                    'input_sha256s': sorted(hashlib.sha256(data).hexdigest() for _, data in sources)})
            except BaseException:
                manager._state(directory, 'unknown', container_name=name, container_tool=spec.identity)
                raise
        return Outcome(context.invoke(invoke, input={'adapter': adapter.descriptor(), 'request': asdict(request)},
                                      reservation_microusd=adapter.tool.reservation_microusd))
