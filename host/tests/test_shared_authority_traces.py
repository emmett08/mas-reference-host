"""Replay the bounded authority corpus against native PostgreSQL transitions.

Expected observations come from the reviewed formal corpus, not a Python copy of
the transition function. This is selected implementation correspondence, not a
proof that every concrete implementation history refines the model.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import uuid

from mas_host import RunnerError
from mas_host.distributed import remote_binding, WORKER_PROTOCOL
from mas_host.service_contracts import ServiceError
from mas_host.shared_authority import SharedAuthority, SharedAuthorityRejected
from mas_host.shared_control import SharedWorkerController
from mas_host.topology import compile_topology


ROOT = Path(__file__).resolve().parents[2]


class ObservedAuthority(SharedAuthority):
    """Retain the native rejection before a public worker port sanitises it."""
    rejection_code = None

    def _request(self, *args, **kwargs):
        try:
            return super()._request(*args, **kwargs)
        except (RunnerError, ServiceError) as error:
            self.rejection_code = getattr(error, 'code', None)
            raise


class NativeAuthorityTrace:
    def __init__(self, testcase, root):
        self.testcase, self.root = testcase, root
        self.settings = {'binary': os.environ.get('MAS_SHARED_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-shared'),
            'dsn': os.environ['MAS_TEST_POSTGRES_DSN'], 'namespace': 'mas_trace_' + uuid.uuid4().hex,
            'deployment': 'trace-deployment', 'secret_key': b'authority-trace-fixture' * 2,
            'secret_reference': 'trace-key/1'}
        self.current = 0
        self.authorities = {0: self.authority(0, 1)}
        self.authorities[0].initialise()
        self.authorities[0].renew(lease_seconds=300)
        for scope in ('deployment', 'principal/alice', 'ancestor/root', 'ancestor/leaf'):
            self.authorities[0].configure_scope(scope, max_runs=10, max_calls=10, max_microusd=10)
        digest = hashlib.sha256(b'formal-authority-fixture').hexdigest()
        self.grants = {f'worker-{worker}': {'trust_zone': 'trusted', 'capacity': 1,
            'capabilities': {'work': digest}} for worker in range(2)}
        self.binding = remote_binding('alice', 'work', digest, 'trusted', {}, 1)
        self.controllers = {0: SharedWorkerController(self.authorities[0], self.grants, lease_ms=300000)}
        self.boots = [0, 0]
        for worker in range(2):
            self.register(worker)
        self.tasks = [None, None]
        self.assignments = [None, None]
        self.authoritative = [False, False]
        self.rejection_code = None

    def authority(self, controller, epoch):
        authority = ObservedAuthority(**self.settings, controller=f'controller-{controller}', epoch=epoch)
        self.testcase.addCleanup(authority.close)
        return authority

    def close(self):
        for authority in self.authorities.values():
            authority.close()

    def register(self, worker):
        return self.controllers[self.current].register(f'worker-{worker}',
            boot=f'boot-{worker}-{self.boots[worker]}', capabilities=self.grants[f'worker-{worker}']['capabilities'],
            architecture='x86_64', runtime='python-3.14.0', trust_zone='trusted', protocol=WORKER_PROTOCOL)

    def act(self, operation, args):
        self.rejection_code = None
        for authority in self.authorities.values():
            authority.rejection_code = None
        try:
            if operation == 'admit':
                controller, task = args
                authority = self.authorities[controller]
                configuration = compile_topology({'agents': ['a', 'b'], 'application': 'agent-task/1',
                    'max_calls': 2, 'wall_seconds': 300})
                run, owner, call = f'trace-run-{task}', f'trace-owner-{task}', f'trace-call-{task}'
                # Exercise the native admission command directly: Runner's
                # constructor deliberately sanitises service exceptions.
                authority.command('create_run', {'run': run, 'principal': 'alice',
                    'ancestors': ['ancestor/root', 'ancestor/leaf'], 'configuration': configuration,
                    'metadata': {'run_id': run, 'workflow_version': 'authority-trace/1',
                        'topology': configuration, 'max_depth': 8,
                        'secret_reference': authority.secret_reference, 'principal': 'alice'}})
                authority.command('claim_run', {'run': run, 'owner': owner})
                authority.command('step', {'run': run, 'owner': owner, 'key': 'effect',
                    'kind': 'invoke', 'input': {'input': self.binding}, 'initial': {}})
                authority.command('prepare_invocation', {'run': run, 'owner': owner,
                    'step_key': 'effect', 'actor': 'b', 'call_id': call, 'reservation_microusd': 1})
                authority.command('admit_invocation', {'run': run, 'owner': owner, 'call_id': call})
                self.controllers[controller].submit_admitted(call, self.binding)
                self.tasks[task] = call
                return True
            if operation == 'claim':
                controller, worker, task = args
                assignment = self.controllers[controller].claim(f'worker-{worker}',
                    boot=f'boot-{worker}-{self.boots[worker]}')['assignment']
                if assignment is None:
                    self.rejection_code = 'no_assignment'
                    return False
                self.testcase.assertEqual(assignment['call_id'], self.tasks[task],
                                          'native scheduling selected a different logical task')
                self.assignments[task] = assignment
                return True
            if operation in ('dispatch', 'replay_dispatch'):
                controller, worker, task = args
                assignment = self.assignments[task]
                result = self.controllers[controller].dispatch(f'worker-{worker}',
                    **{key: assignment[key] for key in ('boot', 'call_id', 'fence')})
                self.testcase.assertIs(type(result['execute']), bool)
                if not result['execute']:
                    self.rejection_code = result.get('reason')
                return result['execute']
            if operation == 'receipt':
                controller, worker, task = args
                assignment = self.assignments[task]
                receipt = {'id': f'receipt-{task}', 'call_id': self.tasks[task],
                    'boot': assignment['boot'], 'fence': assignment['fence'], 'status': 'completed',
                    'output': {'task': task}, 'evidence': {'fixture': True}, 'output_profile': 'inline-json/1'}
                ack = self.controllers[controller].receipt(f'worker-{worker}', receipt=receipt)
                self.authoritative[task] = ack['authoritative']
                return ack['stored']
            if operation == 'drain':
                controller, draining = args
                self.authorities[controller].set_draining(draining, operator='fixture', evidence={'trace': True})
                return True
            if operation == 'handover':
                controller, = args
                binding = self.authorities[self.current].handover(f'controller-{controller}')
                self.authorities[controller] = self.authority(controller, binding['epoch'])
                self.controllers[controller] = SharedWorkerController(self.authorities[controller],
                    self.grants, lease_ms=300000)
                self.current = controller
                return True
            if operation == 'reboot':
                worker, = args
                self.boots[worker] += 1
                self.register(worker)
                return True
            raise AssertionError(f'unsupported formal corpus operation: {operation}')
        except (RunnerError, ServiceError) as error:
            native = {authority.rejection_code for authority in self.authorities.values()
                      if authority.rejection_code is not None}
            self.testcase.assertLessEqual(len(native), 1)
            self.rejection_code = next(iter(native), getattr(error, 'code', None))
            if self.rejection_code not in {'admission_draining', 'stale_protocol_authority',
                                           'stale_worker_assignment', 'stale_worker_epoch'}:
                raise AssertionError(f'unexpected native failure during {operation}: '
                                     f'{self.rejection_code or type(error).__name__}') from error
            return False

    def observe(self):
        authority = self.authorities[self.current]
        status = authority.authority_status()
        states, evidence = [], []
        for call in self.tasks:
            if call is None:
                states.append('absent')
                evidence.append(0)
            else:
                state = self.controllers[self.current].inspect('alice', call)['state']
                states.append('admitted' if state == 'pending' else state)
                evidence.append(len(self.controllers[self.current].evidence('alice', call)))
        allocations = [authority.query('scope', {'scope': scope})
                       for scope in ('deployment', 'principal/alice', 'ancestor/root', 'ancestor/leaf')]
        self.testcase.assertEqual({value['calls'] for value in allocations}, {allocations[0]['calls']})
        self.testcase.assertEqual({value['microusd'] for value in allocations}, {allocations[0]['calls']})
        return {'epoch': status['epoch'], 'controller': int(status['controller'].removeprefix('controller-')),
                'draining': status['draining'],
                'charges': allocations[0]['calls'], 'states': states,
                'authoritative': list(self.authoritative), 'stored_evidence': evidence}


class AuthorityRejectionBoundaryTests(unittest.TestCase):
    def authority(self):
        return SharedAuthority(binary=sys.executable, dsn='fixture-only', namespace='fixture',
            deployment='fixture', controller='controller', epoch=1,
            secret_key=b'fixture-secret' * 4, secret_reference='fixture-key/1')

    def test_native_fencing_codes_preserve_runner_error_compatibility(self):
        for code in SharedAuthorityRejected.CODES:
            response = subprocess.CompletedProcess([], 1, json.dumps(
                {'version': 1, 'ok': False, 'error': {'code': code}}), '')
            with self.subTest(code=code), mock.patch('subprocess.run', return_value=response):
                with self.assertRaises(SharedAuthorityRejected) as raised:
                    self.authority().authority_status()
                self.assertIsInstance(raised.exception, RunnerError)
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(str(raised.exception), code)

    def test_transport_and_malformed_replies_are_not_semantic_rejections(self):
        replies = ['not JSON', '{}', '[]', '{"version":1,"ok":false,"error":null}',
                   json.dumps({'version': True, 'ok': False, 'error': {'code': 'stale_protocol_authority'}}),
                   json.dumps({'version': 2, 'ok': False, 'error': {'code': 'stale_protocol_authority'}}),
                   json.dumps({'version': 1, 'ok': False, 'error': {'code': 'shared_authority_rejected'}})]
        for content in replies:
            response = subprocess.CompletedProcess([], 1, content, 'private-server-details')
            with self.subTest(reply=content), mock.patch('subprocess.run', return_value=response):
                with self.assertRaises(RunnerError) as raised:
                    self.authority().authority_status()
                self.assertNotIsInstance(raised.exception, SharedAuthorityRejected)
                self.assertFalse(hasattr(raised.exception, 'code'))
                self.assertNotIn('private-server-details', str(raised.exception))
        with mock.patch('subprocess.run', side_effect=OSError('private-connection-details')):
            with self.assertRaises(RunnerError) as raised:
                self.authority().authority_status()
            self.assertNotIsInstance(raised.exception, SharedAuthorityRejected)
            self.assertNotIn('private-connection-details', str(raised.exception))

    def test_trace_rejects_unrelated_runtime_and_service_failures(self):
        native = NativeAuthorityTrace.__new__(NativeAuthorityTrace)
        native.testcase = self
        native.authorities = {0: type('Authority', (), {'rejection_code': None})()}
        native.boots = [0, 0]
        controller = mock.Mock()
        native.controllers = {0: controller}
        for error in (RunnerError('transport failed'), ServiceError('worker_queue_full')):
            controller.claim.side_effect = error
            with self.subTest(error=type(error).__name__), self.assertRaisesRegex(
                    AssertionError, 'unexpected native failure'):
                native.act('claim', [0, 0, 0])


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedAuthorityTraceTests(unittest.TestCase):
    def test_formal_corpus_observations_match_native_shared_authority(self):
        corpus = json.loads((ROOT / 'formal/authority-traces.json').read_text())
        self.assertEqual(corpus['version'], 1)
        self.assertGreaterEqual(len(corpus['traces']), 6)
        for trace in corpus['traces']:
            with self.subTest(trace=trace['name']), tempfile.TemporaryDirectory() as root:
                native = NativeAuthorityTrace(self, Path(root))
                try:
                    for ordinal, action in enumerate(trace['actions']):
                        accepted = native.act(action['op'], action['args'])
                        self.assertEqual(native.rejection_code, action.get('reject_code'),
                                         f"unexpected rejection for {action['op']} {action['args']}")
                        actual = {'accepted': accepted, **native.observe()}
                        self.assertEqual(actual, action['expect'],
                            f"trace {trace['name']} step {ordinal}: {action['op']} {action['args']}")
                finally:
                    native.close()
