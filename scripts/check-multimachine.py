#!/usr/bin/env python3
"""Qualify remote workers on two real QEMU machines; missing runtime is failure.

The controller stays on this host. Guests have independent kernels and disks;
SSH reverse tunnels expose only an authenticated loopback worker endpoint.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'host'), str(ROOT/'scripts/qualification')]
from mas_host import Runner
from mas_host.distributed import ARTIFACT_OUTPUT_PROFILE, REMOTE, RemoteWorkerAdapter, artifact_output
from mas_host.journal import inspect_run
from mas_host.schema import connect
from machine_fixture import controller, machine
from guest_job import completed_result


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def wait_for(predicate, *, seconds=60, description='condition'):
    deadline = time.monotonic()+seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.1)
    raise RuntimeError('timed out: '+description)


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Guest:
    def __init__(self, name, root, image, key, proxy_port):
        self.name, self.root, self.key = name, root/name, key
        self.root.mkdir()
        self.ssh_port, self.proxy_port = port(), proxy_port
        self.process = self.tunnel = None
        self.qmp_port = port()
        self.known_hosts = root/'known-hosts'
        subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(self.root/'host-key')], check=True)
        host_public = (self.root/'host-key.pub').read_text().split()[:2]
        with self.known_hosts.open('a') as stream:
            stream.write(f"[127.0.0.1]:{self.ssh_port} {' '.join(host_public)}\n")
        private = (self.root/'host-key').read_text()
        user_data = ('#cloud-config\nusers:\n  - default\nssh_pwauth: false\n'
            'ssh_authorized_keys:\n  - '+json.dumps(key.with_suffix('.pub').read_text().strip())+'\n'
            'ssh_keys:\n  ed25519_private: |\n'+''.join('    '+line+'\n' for line in private.splitlines())+
            '  ed25519_public: '+json.dumps(' '.join(host_public))+'\n')
        (self.root/'user-data').write_text(user_data)
        (self.root/'meta-data').write_text(f'instance-id: mas-{name}\nlocal-hostname: mas-{name}\n')
        subprocess.run(['cloud-localds', str(self.root/'seed.img'), str(self.root/'user-data'), str(self.root/'meta-data')], check=True)
        subprocess.run(['qemu-img', 'create', '-q', '-f', 'qcow2', '-F', 'qcow2', '-b', str(image),
                        str(self.root/'disk.qcow2'), '8G'], check=True)

    def start(self):
        # TCG deliberately works without KVM, mounts or elevated host privileges.
        self.log = (self.root/'serial.log').open('wb')
        self.process = subprocess.Popen(['qemu-system-x86_64', '-machine', 'q35,accel=tcg', '-cpu', 'max',
            '-m', '1024', '-smp', '1', '-display', 'none', '-monitor', 'none', '-serial', 'stdio',
            '-qmp', f'tcp:127.0.0.1:{self.qmp_port},server=on,wait=off',
            '-drive', f'file={self.root}/disk.qcow2,if=virtio,format=qcow2,cache=writeback',
            '-drive', f'file={self.root}/seed.img,if=virtio,format=raw,readonly=on',
            '-netdev', f'user,id=net0,restrict=on,hostfwd=tcp:127.0.0.1:{self.ssh_port}-:22',
            '-device', 'virtio-net-pci,netdev=net0,id=nic0'], stdin=subprocess.DEVNULL, stdout=self.log, stderr=subprocess.STDOUT)

    def ssh(self, *command):
        return ['ssh', '-F', '/dev/null', '-i', str(self.key), '-p', str(self.ssh_port),
                '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'GSSAPIAuthentication=no',
                '-o', 'KexAlgorithms=curve25519-sha256',
                '-o', 'StrictHostKeyChecking=yes', '-o', f'UserKnownHostsFile={self.known_hosts}',
                '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
                'ubuntu@127.0.0.1', *command]

    def ready(self):
        def probe():
            require(self.process.poll() is None, self.name+' QEMU exited before boot')
            try:
                result = subprocess.run(self.ssh('true'), capture_output=True, timeout=20)
                (self.root/'ssh-readiness.log').write_bytes(result.stderr[-4096:])
                return result.returncode == 0
            except subprocess.TimeoutExpired:
                (self.root/'ssh-readiness.log').write_text('SSH handshake exceeded 20 seconds\n')
                return False
        wait_for(probe, seconds=600, description=self.name+' SSH boot')
        subprocess.run(self.ssh('sudo -n cloud-init status --wait'), check=True, timeout=300,
                       stdout=subprocess.DEVNULL)

    def install(self, archive, token):
        with archive.open('rb') as stream:
            subprocess.run(self.ssh('sudo -n mkdir -p /opt/mas /var/lib/mas-qualification && sudo -n tar -xzf - -C /opt/mas'),
                           stdin=stream, check=True, timeout=120)
        subprocess.run(self.ssh("sudo -n sh -c 'umask 077; cat > /var/lib/mas-qualification/token'"),
                       input=token.encode(), check=True, timeout=10)
        self.connect()

    def connect(self):
        if self.tunnel:
            self.tunnel.terminate()
            self.tunnel.wait(timeout=10)
        args = self.ssh()
        self.tunnel = subprocess.Popen(args[:-1] + ['-o', 'ExitOnForwardFailure=yes', '-N',
            '-R', f'127.0.0.1:18766:127.0.0.1:{self.proxy_port}', args[-1]],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=(self.root/'tunnel.log').open('ab'))
        # Verify the listener inside the guest, not a fixed start-up sleep.
        def available():
            require(self.tunnel.poll() is None, self.name+' SSH tunnel exited')
            try:
                return subprocess.run(self.ssh("python3 -c \"import socket; socket.create_connection(('127.0.0.1',18766),2).close()\""),
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode == 0
            except subprocess.TimeoutExpired:
                return False
        wait_for(available, seconds=60, description=self.name+' private tunnel')

    def call(self, action, case='initial', payload=None, timeout=60, *, job=None):
        arguments = ['sudo', '-n', '/usr/bin/python3', '/opt/mas/scripts/qualification/machine_fixture.py',
                     action, '--case', case]
        if job is not None:
            arguments.extend(['--job', job])
        command = shlex.join(arguments)
        result = subprocess.run(self.ssh(command), input=json.dumps(payload).encode() if payload is not None else None,
                                capture_output=True, timeout=timeout)
        require(result.returncode == 0, self.name+' fixture failed: '+result.stderr.decode(errors='replace')[-1500:])
        return json.loads(result.stdout)

    def control(self, command):
        with socket.socket() as sock:
            sock.settimeout(5)
            sock.connect(('127.0.0.1', self.qmp_port))
            stream = sock.makefile('rwb')
            json.loads(stream.readline())
            for message in ({'execute': 'qmp_capabilities'}, command):
                stream.write(json.dumps(message).encode()+b'\n'); stream.flush()
                while True:
                    response = json.loads(stream.readline())
                    if 'return' in response:
                        break
                    require('error' not in response, 'QMP fault injection failed')
            return response['return']

    def storage(self):
        writable = [item['inserted'] for item in self.control({'execute': 'query-block'})
                    if item.get('inserted') and item['inserted']['ro'] is False]
        require(len(writable) == 1 and writable[0]['file'] == str(self.root/'disk.qcow2'),
                self.name+' unexpected writable block device')
        cache = writable[0]['cache']
        require(cache == {'writeback': True, 'direct': False, 'no-flush': False},
                self.name+' disk cache must honour guest flushes')
        return {'driver': writable[0]['drv'], 'cache': cache}

    def link(self, up):
        self.control({'execute': 'set_link', 'arguments': {'name': 'nic0', 'up': up}})

    def reboot(self):
        before = self.call('describe')['kernel_boot_id']
        self.control({'execute': 'system_reset'})
        def changed():
            try:
                result = subprocess.run(self.ssh('cat /proc/sys/kernel/random/boot_id'),
                                        capture_output=True, timeout=20)
                (self.root/'reboot-readiness.log').write_bytes(result.stderr[-4096:])
                value = result.stdout.decode().strip()
                return value if result.returncode == 0 and value and value != before else False
            except subprocess.TimeoutExpired:
                (self.root/'reboot-readiness.log').write_text('SSH handshake exceeded 20 seconds\n')
                return False
        after = wait_for(changed, seconds=300, description=self.name+' new kernel after hard reset')
        self.ready()
        self.connect()
        return {'before': before, 'after': after}

    def close(self):
        for process in (self.tunnel, self.process):
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=5)
        if hasattr(self, 'log'):
            self.log.close()


class FaultProxy(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        length = int(self.headers.get('Content-Length', '0'))
        require(0 <= length <= 131072, 'proxy request bound')
        source = self.rfile.read(length)
        if self.path == '/workers/receipt' and self.server.block_receipts:
            self.send_error(503)
            return
        connection = http.client.HTTPConnection('127.0.0.1', self.server.backend, timeout=10)
        try:
            connection.request('POST', self.path, source,
                {'Authorization': self.headers.get('Authorization', ''), 'Content-Type': 'application/json'})
            response = connection.getresponse()
            body = response.read(131073)
            require(len(body) <= 131072, 'proxy response bound')
            if self.path == '/workers/receipt' and self.server.lose_ack:
                self.server.lose_ack = False
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
            self.send_response(response.status)
            self.send_header('Content-Type', response.getheader('Content-Type', 'application/json'))
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        finally:
            connection.close()


class Case:
    def __init__(self, name, root, descriptions, tokens, proxy, args):
        self.name, self.root, self.proxy, self.args = name, root/name, proxy, args
        self.root.mkdir(mode=0o700)
        self.config = {'state': str(self.root/'state'), 'port': proxy.backend, 'lease_ms': 8000,
            'grants': {node: {'capabilities': description['capabilities'], 'trust_zone': 'vm-fixture', 'capacity': 1}
                       for node, description in descriptions.items()},
            'verifier': [{'principal': node, 'sha256': hashlib.sha256(token.encode()).hexdigest(), 'scopes': ['mas:worker']}
                         for node, token in tokens.items()]}
        self.path = self.root/'controller.json'
        self.path.write_text(json.dumps(self.config))
        self.authority = controller(self.config)
        self.process = None
        self.results, self.futures = [], []
        self.pool = ThreadPoolExecutor(max_workers=2)
        self.start()

    def start(self):
        self.log = (self.root/'controller.log').open('ab')
        self.process = subprocess.Popen([sys.executable, str(ROOT/'scripts/qualification/machine_fixture.py'),
            'serve', '--config', str(self.path)], stdout=self.log, stderr=subprocess.STDOUT)
        def available():
            require(self.process.poll() is None, 'controller exited before listening')
            try:
                with socket.create_connection(('127.0.0.1', self.config['port']), timeout=.2):
                    return True
            except OSError:
                return False
        wait_for(available, seconds=20, description='controller RPC listener')

    def restart(self):
        self.process.kill(); self.process.wait(timeout=5); self.log.close()
        self.start()

    def submit(self, node='worker-one', *, output=None, delay_ms=0, artifact=False):
        count = len(self.futures)
        operation = 'report' if artifact else 'probe'
        binding = {'principal': 'alice', 'operation': operation,
            'implementation': self.config['grants'][node]['capabilities'][operation], 'trust_zone': 'vm-fixture',
            'reservation_microusd': 5}
        if artifact:
            binding.update(output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=('report',))
        def execute():
            adapter = RemoteWorkerAdapter(self.authority, **binding)
            topology = {'agents': ['a', 'b'], 'edges': [['a', 'b'], ['b', 'a']], 'application': 'agent-task/1',
                'mailbox_capacity': 10, 'max_messages': 20, 'max_bytes': 100000, 'max_calls': 10, 'wall_seconds': 90}
            with Runner(topology, {'a': adapter, 'b': adapter}, self.root/'state', supervised=False,
                    mas_bin=self.args.mas, gateway_bin=self.args.gateway) as runner:
                result = {'run_dir': str(runner.run_dir)}
                try:
                    result['output'] = runner.request('a', 'b', operation,
                        {'case': self.name, 'delay_ms': delay_ms, 'output': output or {'ok': True}}).output
                except Exception as error:
                    result['error'] = type(error).__name__
                    result['error_detail'] = str(error)[:500]
                result['calls'] = runner.status()['calls']
                self.results.append(result)
                return result
        future = self.pool.submit(execute)
        self.futures.append(future)
        def new_job():
            with closing(connect(self.authority.path, REMOTE, readonly=True)) as db:
                jobs = db.execute('SELECT call_id FROM jobs ORDER BY created,call_id').fetchall()
                if len(jobs) <= count and future.done():
                    raise RuntimeError('submission ended before queue: '+json.dumps(future.result()))
                return jobs[-1]['call_id'] if len(jobs) > count else False
        # Saturation creates no job; the caller can inspect the known failure.
        return future, new_job

    def report(self, call_id):
        return self.authority.inspect('alice', call_id)

    def close(self):
        self.pool.shutdown(wait=True)
        if self.process and self.process.poll() is None:
            self.process.terminate(); self.process.wait(timeout=10)
        self.log.close()
        self.proxy.lose_ack = self.proxy.block_receipts = False


def effects(node, case, call_id):
    return [item for item in node.call('snapshot', case)['effects'] if item['call_id'] == call_id]


def observe_original_job(node, case, launch):
    # This is a separate, bounded observation budget. The original child already
    # has its immutable 60-second guest deadline, which these reads cannot extend.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        observed = node.call('job', case, timeout=min(15, remaining), job=launch['nonce'])
        require(observed.get('launch') == launch, 'original guest job identity changed')
        if observed.get('supervisor_failure') is not None or observed.get('completion') is not None:
            return observed
        time.sleep(.1)
    raise RuntimeError('timed out observing original guest job completion')


def failure_evidence(case, node, call_id, observations, error):
    """Retain bounded diagnostics even if a guest control connection is lost."""
    result = {'case': case.name, 'passed': False, 'call_id': call_id,
              'error': type(error).__name__, 'detail': str(error)[-1500:],
              'observations': observations}
    def capture(name, read):
        try:
            result[name] = {'available': True, 'value': read()}
        except Exception as unavailable:
            result[name] = {'available': False, 'error': type(unavailable).__name__,
                            'detail': str(unavailable)[-1500:]}
        (case.root/'failure.json').write_text(json.dumps(result, indent=2, sort_keys=True)+'\n')
    # Save local facts first; a later guest timeout must not erase them.
    capture('controller', lambda: case.report(call_id) if call_id else None)
    capture('usage', lambda: case.authority.operations.health()['usage'])
    capture('receipts', lambda: case.authority.evidence('alice', call_id) if call_id else [])
    capture('invocations', lambda: [inspect_run(item['run_dir'])['invocations'] for item in case.results])
    capture('spool', lambda: node.call('snapshot', case.name, timeout=15))
    if observations.get('job_nonce'):
        capture('original_job', lambda: node.call('job', case.name, timeout=15,
                                                  job=observations['job_nonce']))


def check_all(nodes, descriptions, tokens, proxy, args, root):
    evidence = []
    with ThreadPoolExecutor(max_workers=2) as workers:
        for name in ('twoworkers', 'lostack', 'partition', 'spoolloss', 'artifactloss'):
            print('qualifying '+name, file=sys.stderr, flush=True)
            case = Case(name, root, descriptions, tokens, proxy, args)
            call_id = None
            observations = {'case': name, 'timing': {}}
            def save_observations():
                (case.root/'observations.json').write_text(json.dumps(observations, indent=2, sort_keys=True)+'\n')
            try:
                reboot = None
                output, original_bytes, reference = None, None, None
                if name == 'artifactloss':
                    store = case.authority.artifact_repository('reports', 'worker-one')
                    reference = store.put_bytes(b'durable remote result', readers=('alice',))
                    output = artifact_output({'ok': True}, {'report': reference.to_dict()})
                    object_path = store.directory/(reference.sha256+'.blob')
                    original_bytes = object_path.read_bytes()
                    object_path.unlink()
                future, pending = case.submit(output=output, artifact=name == 'artifactloss',
                                               delay_ms=12000 if name == 'partition' else 0)
                call_id = wait_for(pending, description=name+' admitted job')
                observations['call_id'] = call_id
                save_observations()
                if name == 'lostack':
                    proxy.lose_ack = True
                if name == 'spoolloss':
                    proxy.block_receipts = True
                if name == 'partition':
                    observations['job_nonce'] = secrets.token_hex(16)
                    save_observations()
                    launch = nodes['worker-one'].call('start', name, job=observations['job_nonce'])
                    require(launch['case'] == name and launch['nonce'] == observations['job_nonce'],
                            'guest launched a different case or nonce')
                    observations['job_launch'] = launch
                    save_observations()
                    observations['effect_start'] = wait_for(
                        lambda: effects(nodes['worker-one'], name, call_id), description='effect started in guest')
                    save_observations()
                    nodes['worker-one'].link(False)
                    try:
                        observations['timing']['link_down_monotonic_ns'] = time.monotonic_ns()
                        save_observations()
                        # A real virtual NIC disconnect, lasting beyond the lease.
                        time.sleep(case.config['lease_ms']/1000+1)
                        observations['controller_during_partition'] = case.report(call_id)
                        save_observations()
                        require(observations['controller_during_partition']['state'] == 'unknown',
                                'partition did not retain unknown')
                    finally:
                        nodes['worker-one'].link(True)
                        observations['timing']['link_up_monotonic_ns'] = time.monotonic_ns()
                        save_observations()
                    nodes['worker-one'].connect()
                    observations['job_completion'] = observe_original_job(nodes['worker-one'], name, launch)
                    save_observations()
                    worker_result = completed_result(observations['job_completion'], launch)
                else:
                    execution = workers.submit(nodes['worker-one'].call, 'run', name)
                    worker_result = execution.result(timeout=60)
                snapshot = nodes['worker-one'].call('snapshot', name)
                observations.update(worker_result=worker_result, spool=snapshot)
                save_observations()
                receipts = [item['receipt'] for item in snapshot['attempts'] if item['receipt']]
                if name in ('lostack', 'spoolloss', 'artifactloss'):
                    require(len(receipts) == 1, name+' did not retain unacknowledged receipt')
                if name == 'lostack':
                    require(case.report(call_id)['state'] == 'completed', 'ack loss changed committed completion')
                    case.restart()
                    reboot = nodes['worker-one'].reboot()
                    nodes['worker-one'].call('run', name)
                    require(case.authority.evidence('alice', call_id) == receipts, 'receipt replay changed identity or duplicated evidence')
                elif name == 'spoolloss':
                    case.restart()
                    nodes['worker-one'].call('lose-spool', name)
                    proxy.block_receipts = False
                    nodes['worker-one'].call('run', name)
                    require(case.report(call_id)['state'] == 'unknown', 'lost spool released effect uncertainty')
                    require(case.authority.evidence('alice', call_id) == [], 'lost receipt was invented')
                elif name == 'artifactloss':
                    require(case.authority.evidence('alice', call_id) == [], 'missing artifact receipt was acknowledged')
                    object_path.write_bytes(original_bytes); object_path.chmod(0o600)
                    nodes['worker-one'].call('run', name)
                    require(case.report(call_id)['state'] == 'unknown', 'rebooted artifact receipt regained authority')
                    require(case.authority.evidence('alice', call_id) == receipts, 'artifact recovery changed receipt identity')
                elif name == 'partition':
                    nodes['worker-one'].call('run', name)
                    require(case.report(call_id)['state'] == 'unknown', 'delayed receipt renewed an expired grant')
                    retained = case.authority.evidence('alice', call_id)
                    require(len(retained) == 1, 'late observation not retained exactly once')
                    late = retained[0]
                    rejected = nodes['worker-one'].call('rpc', name, {'operation': 'heartbeat',
                        'boot': late['boot'], 'call_id': call_id, 'fence': late['fence']})
                    require('error' in rejected, 'stale heartbeat regained authority')
                first = future.result(timeout=30)
                require(len(first['calls']) == 1, 'original invocation allocation was duplicated')
                require(len(effects(nodes['worker-one'], name, call_id)) == 1, 'application effect repeated')
                if name == 'twoworkers':
                    second, pending = case.submit('worker-two')
                    second_id = wait_for(pending, description='second worker job')
                    nodes['worker-two'].call('run', name)
                    require(second.result(timeout=30).get('output') == {'ok': True}, 'second VM did not complete')
                    require(len(effects(nodes['worker-two'], name, second_id)) == 1, 'second VM effect count')
                    require(first.get('output') == {'ok': True}, 'first VM did not complete')
                elif name in ('partition', 'spoolloss', 'artifactloss'):
                    require(nodes['worker-two'].call('run', name) is None, 'unknown call was reassigned')
                    denied, _pending = case.submit('worker-two')
                    require('error' in denied.result(timeout=30), 'unknown work released per-principal capacity')
                    with closing(connect(case.authority.path, REMOTE, readonly=True)) as db:
                        require(db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1, 'quota denial created a second job')
                usage = case.authority.operations.health()['usage']
                expected = 1 if name == 'lostack' else 2
                require(usage['calls'] == expected, 'call allocation lost or duplicated')
                require(usage['reserved_microusd'] == expected*5, 'reservation refunded or duplicated')
                evidence.append({'case': name, 'passed': True, 'call_id': call_id,
                    'state': case.report(call_id)['state'], 'calls': usage['calls'],
                    'worker_reboot': reboot,
                    'reserved_microusd': usage['reserved_microusd'],
                    'receipt_ids': [item['id'] for item in case.authority.evidence('alice', call_id)],
                    'invocations': [inspect_run(item['run_dir'])['invocations'] for item in case.results]})
                (case.root/'case.json').write_text(json.dumps(evidence[-1], indent=2, sort_keys=True)+'\n')
            except BaseException as error:
                try:
                    failure_evidence(case, nodes['worker-one'], call_id, observations, error)
                except Exception as evidence_error:
                    print('failure evidence unavailable: '+type(evidence_error).__name__, file=sys.stderr, flush=True)
                raise
            finally:
                case.close()
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', type=Path, required=True, help='downloaded image matching qualification lock')
    parser.add_argument('--state-dir', type=Path, required=True, help='new disposable private directory')
    parser.add_argument('--mas', default=str(ROOT/'rust-protocol/target/release/mas'))
    parser.add_argument('--gateway', default=str(ROOT/'rust-protocol/target/release/mas-gateway'))
    args = parser.parse_args()
    args.image = args.image.resolve(strict=True)
    args.state_dir = args.state_dir.resolve()
    args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    lock = json.loads((ROOT/'scripts/qualification/ubuntu-cloud-image.json').read_text())
    require(digest(args.image) == lock['sha256'], 'cloud image differs from immutable qualification lock')
    for tool in ('qemu-system-x86_64', 'qemu-img', 'cloud-localds', 'ssh', 'ssh-keygen'):
        require(shutil.which(tool) is not None, 'required runtime unavailable: '+tool)
    for binary in (args.mas, args.gateway):
        require(Path(binary).is_file() and os.access(binary, os.X_OK), 'Rust binary unavailable')
    key = args.state_dir/'ssh-key'
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
    archive = args.state_dir/'runtime.tar.gz'
    with tarfile.open(archive, 'w:gz') as target:
        for source in ('host/mas_host', 'scripts/qualification'):
            target.add(ROOT/source, arcname=source,
                       filter=lambda item: None if '__pycache__' in item.name or item.name.endswith('.pyc') else item)
        target.add(Path(args.gateway), arcname='rust-protocol/target/release/mas-gateway')
    proxy = ThreadingHTTPServer(('127.0.0.1', 0), FaultProxy)
    proxy.backend, proxy.lose_ack, proxy.block_receipts = port(), False, False
    proxy.daemon_threads = True
    proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True); proxy_thread.start()
    nodes = {name: Guest(name, args.state_dir, args.image, key, proxy.server_port)
             for name in ('worker-one', 'worker-two')}
    tokens = {name: secrets.token_urlsafe(32) for name in nodes}
    report = {'format': 'mas-multimachine-qualification/1', 'passed': False, 'profile': 'qemu-tcg-independent-kernels',
              'disk_cache': 'writeback', 'disk_flush_enabled': True,
              'image_sha256': lock['sha256'], 'controller': machine(),
              'git_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'git_tracked_dirty': bool(subprocess.check_output(
                  ['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT, text=True)),
              'coordinator_sha256': digest(__file__),
              'runtime_archive_sha256': digest(archive), 'mas_sha256': digest(args.mas),
              'gateway_sha256': digest(args.gateway),
              'qemu_version': subprocess.check_output(['qemu-system-x86_64', '--version'], text=True).splitlines()[0]}
    report_path = args.state_dir/'qualification.json'
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
    try:
        for node in nodes.values():
            node.start()
        with ThreadPoolExecutor(max_workers=2) as pool:
            for future in [pool.submit(node.ready) for node in nodes.values()]:
                future.result()
            for future in [pool.submit(node.install, archive, tokens[name]) for name, node in nodes.items()]:
                future.result()
        descriptions = {name: node.call('describe') for name, node in nodes.items()}
        require(len({item['kernel_boot_id'] for item in [report['controller'], *descriptions.values()]}) == 3,
                'controller and workers must have independent kernel boot identities')
        require(descriptions['worker-one']['capabilities'] == descriptions['worker-two']['capabilities'],
                'guest implementation identities differ')
        report['workers'] = descriptions
        report['storage'] = {name: node.storage() for name, node in nodes.items()}
        report['cases'] = check_all(nodes, descriptions, tokens, proxy, args, args.state_dir)
        report['passed'] = True
    finally:
        for node in nodes.values():
            node.close()
        proxy.shutdown(); proxy.server_close(); proxy_thread.join(timeout=5)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
    print(json.dumps({'passed': True, 'evidence': str(args.state_dir/'qualification.json')}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
