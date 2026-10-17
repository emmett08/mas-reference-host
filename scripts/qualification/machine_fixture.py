"""Trusted, disposable guest/controller fixture for the real VM release gate.

This is never an application-selected factory. Each guest has its own kernel,
token, spool and effect log; only its SSH tunnel reaches the controller RPC.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'host'))
from mas_host.contracts import Invocation, Outcome
from mas_host.artifacts import ArtifactStore
from mas_host.distributed import ARTIFACT_OUTPUT_PROFILE, WorkerController
from mas_host.distributed_worker import DurableWorker, SPOOL, WorkerClient, WorkerOperation
from mas_host.executable_worker import WorkerSpec
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError

GUEST_STATE = Path('/var/lib/mas-qualification')


def machine():
    return {'kernel_boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'kernel': platform.release(), 'architecture': platform.machine(),
            'python': platform.python_version()}


class Probe:
    def __init__(self, configuration):
        if configuration:
            raise ValueError('fixture configuration')

    def handle(self, context, request):
        def perform():
            observation = {'call_id': context.invocation_id, 'case': request.input['case'], **machine()}
            with (GUEST_STATE / 'effects.jsonl').open('a') as stream:
                stream.write(json.dumps(observation, sort_keys=True) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
            delay = request.input.get('delay_ms', 0)
            if type(delay) is not int or not 0 <= delay <= 20000:
                raise ValueError('fixture delay')
            time.sleep(delay / 1000)
            return Invocation(request.input['output'], observation)
        return Outcome(context.invoke(perform, input=request.input, reservation_microusd=0))


def operations():
    source = Path(__file__).resolve()
    spec = WorkerSpec('machine-probe', '1', (sys.executable, str(source), 'leaf'),
        factory='probe', configuration={}, cwd=ROOT,
        environment={'PYTHONPATH': str(ROOT / 'host')},
        pinned_files={str(source): hashlib.sha256(source.read_bytes()).hexdigest()})
    return [WorkerOperation.registered('probe', spec,
                gateway_bin=ROOT / 'rust-protocol/target/release/mas-gateway', timeout_seconds=30),
            WorkerOperation.registered('report', spec,
                gateway_bin=ROOT / 'rust-protocol/target/release/mas-gateway', timeout_seconds=30,
                output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=('report',))]


def controller(config):
    root = Path(config['state'])
    def repositories(namespace, principal):
        return ArtifactStore(root / 'artifacts', namespace=namespace, principal=principal)
    return WorkerController(root, config['grants'], max_outstanding=2, per_principal=1,
        lease_ms=config['lease_ms'], artifact_repository=repositories, artifact_store_id='vm-fixture/1')


def main():
    if sys.argv[1:] == ['leaf']:
        from mas_host.worker_entry import run_worker
        return run_worker({'probe': Probe})
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['describe', 'run', 'snapshot', 'lose-spool', 'rpc', 'serve',
                                          'start', 'job', 'supervise'])
    parser.add_argument('--case', default='initial')
    parser.add_argument('--config', type=Path)
    parser.add_argument('--job', help='unique original guest job nonce')
    args = parser.parse_args()
    if args.action == 'serve':
        from mas_host.bridges.authentication import StaticTokenVerifier
        from mas_host.bridges.server import BridgeHTTPServer
        config = json.loads(args.config.read_text())
        verifier = StaticTokenVerifier(config['verifier'])
        with BridgeHTTPServer(('127.0.0.1', config['port']), service=None, verifier=verifier,
                worker_controller=controller(config), public_url=f"http://127.0.0.1:{config['port']}",
                allow_loopback_http=True, request_log=lambda _record: None) as server:
            server.serve_forever(poll_interval=.05)
        return 0
    GUEST_STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not args.case.isalnum():
        raise ValueError('invalid case')
    spool = GUEST_STATE / args.case
    if args.action in ('start', 'job', 'supervise'):
        # Leaf execution does not import this management-only helper.
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from guest_job import inspect_job, launch_job, supervise_job
        directory = spool / 'job'
        command = [sys.executable, str(Path(__file__).resolve())]
        if args.action == 'start':
            # DurableWorker requires its spool itself (not only GUEST_STATE)
            # to be private; mkdir(parents=True) would default this ancestor.
            spool.mkdir(mode=0o700, exist_ok=True)
            result = launch_job(directory, args.case, args.job,
                command + ['supervise', '--case', args.case, '--job', args.job])
        elif args.action == 'job':
            result = inspect_job(directory, args.job)
        else:
            supervise_job(directory, args.job, command + ['run', '--case', args.case])
            return 0
    elif args.action == 'describe':
        result = {**machine(), 'capabilities': {op.name: op.implementation for op in operations()}}
    elif args.action == 'snapshot':
        log = GUEST_STATE / 'effects.jsonl'
        result = {'effects': [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else [],
                  'attempts': []}
        if (spool / 'worker-spool.sqlite').exists():
            with closing(connect(spool / 'worker-spool.sqlite', SPOOL, readonly=True)) as db:
                for row in db.execute('SELECT call_id,fence,state,receipt,ack FROM attempts ORDER BY call_id,fence'):
                    # DurableWorker reserves a fixed-size BLOB before dispatch,
                    # then writes padded JSON bytes. Never treat its storage type
                    # as evidence that an observed receipt is absent.
                    result['attempts'].append({'call_id': row['call_id'], 'fence': row['fence'], 'state': row['state'],
                        'receipt': json.loads(row['receipt']) if row['state'] in ('receipt', 'acknowledged') else None,
                        'ack': json.loads(row['ack']) if row['state'] == 'acknowledged' else None})
    elif args.action == 'lose-spool':
        if spool.exists():
            shutil.rmtree(spool)
        result = {'spool_removed': True, 'effects_retained': (GUEST_STATE / 'effects.jsonl').exists()}
    else:
        client = WorkerClient('http://127.0.0.1:18766', token_file=GUEST_STATE/'token',
                              timeout_seconds=3, allow_loopback_http=True)
        try:
            if args.action == 'rpc':
                payload = json.loads(sys.stdin.buffer.read(131073))
                result = client.call(payload.pop('operation'), **payload)
            else:
                result = DurableWorker(client, spool, operations(), trust_zone='vm-fixture').run_once()
        except ServiceError as error:
            result = {'error': error.code}
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
