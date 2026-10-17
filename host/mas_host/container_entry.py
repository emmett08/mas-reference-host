"""Trusted entrypoint installed at /opt/mas/container_entry.py in pinned images.

This module imports only the standard library. The application runs as uid65532
with empty groups and cleared capabilities; its root parent enforces deadlines
and transfers only declared regular output files. No host filesystem is mounted.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import stat
import struct
import subprocess
import sys


MAX_HEADER = 65536
MAX_INPUT = 64 * 1024 * 1024
MAX_OUTPUT = 64 * 1024 * 1024


def parts(value):
    if (type(value) is not str or len(value) > 512 or '\\' in value or value.startswith('/')
            or not 1 <= len(value.split('/')) <= 8
            or any(re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', part) is None or part in ('.', '..')
                   for part in value.split('/'))):
        raise ValueError('path')
    return value.split('/')


def read_exact(stream, size):
    result = bytearray()
    while len(result) < size:
        block = stream.read(min(65536, size-len(result)))
        if not block:
            raise ValueError('truncated')
        result.extend(block)
    return bytes(result)


def file_bytes(root, relative, limit):
    """Walk each output component without following links or special files."""
    sequence = parts(relative)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in sequence[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        file = os.open(sequence[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
        with os.fdopen(file, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
                raise ValueError('output')
            data = stream.read(limit+1)
            if len(data) > limit:
                raise ValueError('output')
            return data
    finally:
        os.close(descriptor)


def run():
    # A container whose attaching host dies before sending a frame also exits.
    # PID1 ignores some default signal dispositions; install an explicit exit
    # handler so the deadline does not depend on the namespace-init exception.
    signal.signal(signal.SIGALRM, lambda _signal, _frame: os._exit(124))
    signal.alarm(30)
    stream = sys.stdin.buffer
    size = struct.unpack('!I', read_exact(stream, 4))[0]
    if not 0 < size <= MAX_HEADER:
        raise ValueError('header')
    header = json.loads(read_exact(stream, size))
    required = {'format', 'command', 'request', 'remaining_ms', 'cpu_seconds',
                'file_bytes', 'output_bytes', 'outputs', 'inputs'}
    if type(header) is not dict or set(header) != required or header['format'] != 'mas-container-transfer/1':
        raise ValueError('format')
    for name, lower, upper in (('remaining_ms', 1, 86400000), ('cpu_seconds', 1, 86400),
                               ('file_bytes', 1024, 256*1024*1024), ('output_bytes', 1, MAX_OUTPUT)):
        if type(header[name]) is not int or not lower <= header[name] <= upper:
            raise ValueError('bound')
    signal.alarm(max(1, math.ceil(header['remaining_ms']/1000)))
    if (type(header['command']) is not list or not 1 <= len(header['command']) <= 32
            or any(type(item) is not str or not item or '\0' in item or len(item) > 4096 for item in header['command'])
            or not header['command'][0].startswith('/') or type(header['request']) is not dict
            or type(header['inputs']) is not list or len(header['inputs']) > 32
            or type(header['outputs']) is not list or len(header['outputs']) > 32
            or len(set(header['outputs'])) != len(header['outputs'])):
        raise ValueError('manifest')
    for item in header['outputs']:
        parts(item)
    root = Path('/workspace')
    if os.geteuid() != 0 or not root.is_dir():
        raise ValueError('profile')
    # UID65532 may traverse the root but cannot replace the immutable snapshots,
    # request or context. Only work and outputs are writable to the application.
    root.chmod(0o755)
    for name in ('inputs', 'work', 'outputs'):
        (root / name).mkdir(mode=0o755)
    # Create these while the supervisor owns the directory: DAC_READ_SEARCH
    # permits later reads/traversal, not directory writes after ownership drops.
    for name in ('stdout.txt', 'stderr.txt'):
        (root / 'outputs' / name).touch(mode=0o600, exist_ok=False)
    for name in ('work', 'outputs'):
        (root / name).chmod(0o700)
        os.chown(root / name, 65532, 65532)
    total, paths = 0, set()
    for item in header['inputs']:
        if type(item) is not dict or set(item) != {'path', 'size', 'sha256'}:
            raise ValueError('input')
        sequence = parts(item['path'])
        if (item['path'] in paths or (sequence[0] != 'inputs' and sequence != ['context.json'])
                or type(item['size']) is not int or not 0 <= item['size'] <= MAX_INPUT):
            raise ValueError('input')
        paths.add(item['path'])
        total += item['size']
        if total > MAX_INPUT:
            raise ValueError('input')
        data = read_exact(stream, item['size'])
        if hashlib.sha256(data).hexdigest() != item['sha256']:
            raise ValueError('digest')
        path = root.joinpath(*sequence)
        path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        with path.open('xb') as destination:
            destination.write(data)
        path.chmod(0o444)
    request = root / 'request.json'
    request.write_text(json.dumps(header['request'], separators=(',', ':')))
    request.chmod(0o444)

    def restrict():
        os.setgroups([])
        os.setgid(65532)
        os.setuid(65532)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
        resource.setrlimit(resource.RLIMIT_CPU, (header['cpu_seconds'], header['cpu_seconds']))
        resource.setrlimit(resource.RLIMIT_FSIZE, (header['file_bytes'], header['file_bytes']))

    # Output descriptors are intentionally the only writable inherited handles.
    with (root / 'outputs/stdout.txt').open('wb') as stdout, (root / 'outputs/stderr.txt').open('wb') as stderr:
        process = subprocess.Popen(header['command'], stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
            cwd=root / 'work', close_fds=True, start_new_session=True, preexec_fn=restrict,
            env={'PATH': '/usr/local/bin:/usr/bin:/bin', 'HOME': '/workspace/work', 'TMPDIR': '/workspace/work',
                 'MAS_INPUTS': '/workspace/inputs', 'MAS_OUTPUTS': '/workspace/outputs',
                 'MAS_REQUEST': '/workspace/request.json',
                 'MAS_CONTEXT': '/workspace/context.json' if 'context.json' in paths else ''})
        exit_code = process.wait()
    # The application cannot signal this uid0 parent. Its alarm is independent
    # of application cooperation; exiting the container's PID1 kills descendants.
    values = []
    total = 0
    for path in header['outputs']:
        data = file_bytes(root / 'outputs', path, header['output_bytes']-total)
        total += len(data)
        values.append((path, data))
    output = {'format': 'mas-container-result/1', 'exit_code': exit_code,
              'outputs': [{'path': path, 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
                          for path, data in values]}
    encoded = json.dumps(output, separators=(',', ':')).encode()
    if len(encoded) > MAX_HEADER:
        raise ValueError('output header')
    sys.stdout.buffer.write(struct.pack('!I', len(encoded)) + encoded)
    for _, data in values:
        sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(run())
    except (ValueError, OSError, KeyError, TypeError):
        raise SystemExit(70) from None
