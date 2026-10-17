"""Actual PostgreSQL authority tests for peers, circuits and remote intents."""
import concurrent.futures
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock
import uuid

from mas_host import Outcome, RecoveryRequired, RunnerError
from mas_host.circuit_breaker import CircuitPolicy
from mas_host.peer_registry import PeerRegistration
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_remote import SharedCircuitBreaker, SharedPeerRegistry, SharedRemoteTaskService
from mas_host.bridges.remote import RemoteSnapshot
from mas_host.bridges.http_policy import HTTPTransportPolicy


class PeerClient:
    def __init__(self):
        self.calls = []
        self.lost = None

    def descriptor(self):
        return {'profile': 'test-remote/1', 'endpoint': 'https://peer.example/task', 'principal': 'alice'}

    def submit(self, payload):
        self.calls.append(('submit', payload))
        if self.lost == 'submit':
            raise OSError('injected lost first receipt')
        return RemoteSnapshot('remote-1', 'input_required', {'question': 'amount?'})

    def poll(self, remote_id):
        self.calls.append(('poll', remote_id))
        return RemoteSnapshot(remote_id, 'input_required', {'question': 'amount?'})

    def cancel(self, remote_id):
        self.calls.append(('cancel', remote_id))
        return RemoteSnapshot(remote_id, 'cancelled')

    def update(self, remote_id, payload):
        self.calls.append(('update', payload))
        if self.lost == 'update':
            raise OSError('injected lost continuation receipt')
        return RemoteSnapshot(remote_id, 'completed', payload)


class RemoteAdapter:
    def __init__(self, remote):
        self.remote = remote
        self.run_dir = None

    def handle(self, context, request):
        common = {'context': context, 'host_run_dir': self.run_dir, 'reservation_microusd': 10}
        if request.operation == 'submit':
            value = self.remote.submit('logical-job', request.input, **common)
        elif request.operation == 'update':
            value = self.remote.update('logical-job', request_key='answer', payload=request.input, **common)
        elif request.operation == 'attach':
            value = self.remote.attach('logical-job', 'remote-1', evidence={'ticket': 'verified-remote-id'}, **common)
        elif request.operation == 'poll':
            value = self.remote.poll('logical-job', **common)
        else:
            value = self.remote.cancel('logical-job', request_key='stop', **common)
        return Outcome(value)


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedRemoteTests(unittest.TestCase):
    def setUp(self):
        self.binary = os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared')
        self.namespace = 'mas_remote_' + uuid.uuid4().hex
        self.authority = self.authority_for('controller', 1)
        self.authority.initialise()
        for scope in ('deployment', 'principal/alice'):
            self.authority.configure_scope(scope, max_runs=30, max_calls=100, max_microusd=10000)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client = PeerClient()
        self.remote = SharedRemoteTaskService(self.authority, self.client, namespace='alice/analysis')
        self.runners = []
        self.addCleanup(self.close_runners)

    def authority_for(self, controller, epoch):
        return SharedAuthority(binary=self.binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace=self.namespace, deployment='deployment', controller=controller, epoch=epoch,
            secret_key=b'fixture-secret' * 4, secret_reference='test-key/1')

    def close_runners(self):
        for runner in self.runners:
            try:
                runner.close()
            except RunnerError:
                pass  # An intentionally fenced controller cannot release ownership.

    def runner(self, remote=None):
        adapter = RemoteAdapter(remote or self.remote)
        runner = self.authority.runner({'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 10, 'wall_seconds': 120}, {'a': adapter, 'b': adapter},
            Path(self.temp.name) / ('cache-' + uuid.uuid4().hex), principal='alice',
            workflow_version='shared-remote-test/1', supervised=False)
        adapter.run_dir = runner.run_dir
        self.runners.append(runner)
        return runner

    def test_peer_versions_revocation_and_current_authority_survive_handover(self):
        peers = SharedPeerRegistry(self.authority)
        registration = PeerRegistration(name='analysis', version='1', profile='mas-http/1',
            endpoint='https://analysis.example', outbound_principal='alice', deployment_id='remote-deployment',
            workflows={'analysis': 'a' * 64}, callers=['owner'])
        options = {'operator': 'owner', 'request_key': 'register', 'reason': 'Reviewed peer', 'expected_revision': 0}
        result = peers.register(registration=registration, **options)
        self.assertEqual(peers.register(registration=registration, **options), result)
        with self.assertRaises(RunnerError):
            peers.resolve('analysis', 'outsider')
        successor = self.authority.handover('controller-b')
        replacement = self.authority_for('controller-b', successor['epoch'])
        resumed = SharedPeerRegistry(replacement)
        self.assertEqual(resumed.resolve('analysis', 'owner').identity, registration.identity)
        with self.assertRaises(RunnerError):
            peers.status()
        resumed.revoke(operator='owner', request_key='revoke', reason='Access ended', expected_revision=1,
                       name='analysis', version='1')
        with self.assertRaises(RunnerError):
            resumed.resolve('analysis', 'owner')
        self.assertTrue(resumed.status()['peers'][0]['revoked'])

    def test_circuit_shared_half_open_exclusion_and_late_observation(self):
        policy = CircuitPolicy('provider/api', failure_threshold=1, open_seconds=1, half_open_seconds=1)
        circuit = SharedCircuitBreaker(self.authority, policy)
        self.assertTrue(circuit.admit('call-a'))
        self.assertTrue(circuit.admit('call-b'))
        circuit.observe('call-a', 'provider_failure')
        circuit.observe('call-b', 'success')  # Older generation cannot close it.
        self.assertEqual(circuit.status()['state'], 'open')
        self.assertFalse(circuit.admit('during-cooldown'))
        time.sleep(1.1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(circuit.admit, [f'probe-{i}' for i in range(4)]))
        self.assertEqual(sum(results), 1)
        winner = 'probe-' + str(results.index(True))
        circuit.observe(winner, 'success')
        self.assertEqual(circuit.status()['state'], 'closed')
        with self.assertRaises(RunnerError):
            SharedCircuitBreaker(self.authority, CircuitPolicy('provider/api', failure_threshold=2)).admit('changed')

    def test_peer_transport_binding_survives_shared_roundtrip_without_rebinding_legacy(self):
        peers = SharedPeerRegistry(self.authority)
        options = dict(name='analysis', profile='mas-http/1', endpoint='https://analysis.example',
            outbound_principal='alice', deployment_id='remote-deployment',
            workflows={'analysis': 'a' * 64}, callers=['owner'])
        legacy = PeerRegistration(version='1', **options)
        peers.register(registration=legacy, operator='owner', request_key='legacy',
            reason='Original direct binding', expected_revision=0)
        policy = HTTPTransportPolicy(proxy_url='http://proxy.example:8080',
            allowed_origins=('https://analysis.example',), proxy_authorization_file='/private/credential')
        current = PeerRegistration(version='2', transport_policy=policy.descriptor(), timeout_seconds=17, **options)
        peers.register(registration=current, operator='owner', request_key='configured',
            reason='Explicit transport binding', expected_revision=1)
        self.assertEqual(peers.resolve('analysis', 'owner')._encoded, current._encoded)
        self.assertEqual(peers.resolve('analysis', 'owner', version='1')._encoded, legacy._encoded)
        changed = PeerRegistration(version='2', transport_policy=policy.descriptor(), timeout_seconds=18, **options)
        with self.assertRaises(RunnerError):
            peers.register(registration=changed, operator='owner', request_key='changed',
                reason='Must require a new version', expected_revision=2)
        self.assertEqual(peers.resolve('analysis', 'owner').identity, current.identity)
        self.assertNotIn('/private/', str(peers.status()))
        phased = HTTPTransportPolicy(phase_timeouts_ms={
            'dns': 1000, 'connect': 2000, 'tls': 3000, 'headers': 4000, 'body': 5000})
        version_three = PeerRegistration(version='3', transport_policy=phased.descriptor(), **options)
        peers.register(registration=version_three, operator='owner', request_key='phased',
            reason='Explicit independent timeout phases', expected_revision=2)
        self.assertEqual(peers.resolve('analysis', 'owner')._encoded, version_three._encoded)
        self.assertEqual(peers.resolve('analysis', 'owner', version='2')._encoded, current._encoded)

    def test_remote_lifecycle_is_durable_without_local_shadow_databases(self):
        runner = self.runner()
        self.assertEqual(runner.request('a', 'b', 'submit', {'question': 'amount?'}).output['state'], 'input_required')
        self.assertEqual(runner.request('a', 'b', 'update', {'answer': 8}).output['result'], {'answer': 8})
        receipt = self.remote.status('logical-job')
        self.assertEqual(receipt['state'], 'completed')
        other = self.runner()
        self.assertEqual(other.request('a', 'b', 'submit', {'question': 'amount?'}).output, receipt)
        self.assertEqual([item[0] for item in self.client.calls], ['submit', 'update'])
        self.assertFalse(list(Path(self.temp.name).rglob('*.sqlite')))
        with self.assertRaises(RunnerError):
            SharedRemoteTaskService(self.authority, self.client, namespace='bob/analysis').status('logical-job')

    def test_lost_first_reply_is_never_repeated_and_attach_requires_admission(self):
        self.client.lost = 'submit'
        runner = self.runner()
        with self.assertRaises((RecoveryRequired, RunnerError)):
            runner.request('a', 'b', 'submit', {'question': 'amount?'})
        self.assertEqual(self.remote.status('logical-job')['state'], 'unknown')
        with self.assertRaises((RecoveryRequired, RunnerError)):
            self.runner().request('a', 'b', 'submit', {'question': 'amount?'})
        self.assertEqual([item[0] for item in self.client.calls], ['submit'])
        self.client.lost = None
        attached = self.runner().request('a', 'b', 'attach', {}).output
        self.assertEqual(attached['remote_id'], 'remote-1')
        self.assertEqual(attached['pending_action'], None)
        with self.assertRaises(RunnerError):
            self.authority.command('remote.operation_begin', {
                **self.remote._base('logical-job'), 'run': 'invented', 'call_id': 'invented', 'reservation': 0,
                'action': 'cancel', 'operation_key': 'cancel:forged', 'payload': {}, 'evidence': {}})

    def test_lost_begin_receipt_cannot_be_replayed_as_dispatch_permission(self):
        original = self.authority.command
        seen = []
        def lost(operation, parameters, **options):
            value = original(operation, parameters, **options)
            if operation == 'remote.submit_begin':
                seen.append((operation, parameters, options))
                raise RunnerError('injected lost remote intent acknowledgement')
            return value
        with mock.patch.object(self.authority, 'command', side_effect=lost):
            with self.assertRaises((RecoveryRequired, RunnerError)):
                self.runner().request('a', 'b', 'submit', {})
        operation, parameters, options = seen[0]
        replay = original(operation, parameters, **options)
        self.assertTrue(replay['replayed'])
        with self.assertRaisesRegex(Exception, 'remote outcome unknown'):
            self.remote._begin(replay)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 1)

    def test_lost_mutation_retains_pending_intent_until_evidenced_resolution(self):
        self.runner().request('a', 'b', 'submit', {})
        self.client.lost = 'update'
        with self.assertRaises((RecoveryRequired, RunnerError)):
            self.runner().request('a', 'b', 'update', {'answer': 8})
        with self.assertRaises((RecoveryRequired, RunnerError)):
            self.runner().request('a', 'b', 'update', {'answer': 8})
        self.assertEqual(self.remote.status('logical-job')['pending_action'], 'update')
        self.assertEqual([item[0] for item in self.client.calls], ['submit', 'update'])
        self.remote.resolve_operation('logical-job', 'update:answer', evidence={'ticket': 'peer-confirmed'},
                                      decision='not_applied')
        self.assertIsNone(self.remote.status('logical-job')['pending_action'])
        self.assertEqual(self.remote.inspect('logical-job')['operations'][1]['state'], 'completed')


if __name__ == '__main__':
    unittest.main()
