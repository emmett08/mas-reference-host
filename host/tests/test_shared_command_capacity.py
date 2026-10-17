"""Real PostgreSQL bounded receipts, recovery headroom and permanent replay."""
import concurrent.futures
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from mas_host import RunnerError
from mas_host.service_contracts import ServiceError
from mas_host.shared_authority import SharedAuthority


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedCommandCapacityTests(unittest.TestCase):
    def authority(self, normal=6, recovery=3, administration=2):
        binary = os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared')
        authority = SharedAuthority(binary=binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace='mas_capacity_' + uuid.uuid4().hex, deployment='deployment', controller='controller', epoch=1,
            secret_key=b'fixture-capacity-key/' * 2, secret_reference='fixture/1')
        policy = {'normal_limit': normal, 'recovery_limit': recovery, 'administration_limit': administration}
        authority.initialise(command_capacity=policy)
        self.addCleanup(authority.close)
        return authority, policy

    def assert_code(self, code, operation):
        with self.assertRaises(ServiceError) as caught:
            operation()
        self.assertEqual(caught.exception.code, code)

    def test_full_normal_pool_preserves_observation_replay_and_bounded_amendment(self):
        authority, policy = self.authority()
        for scope in ('deployment', 'principal/alice'):
            authority.configure_scope(scope, max_runs=10, max_calls=10, max_microusd=1000)
        # Frozen configuration replay uses a stable command ID, including after a restart.
        authority.configure_scope('deployment', max_runs=10, max_calls=10, max_microusd=1000)
        self.assertEqual(authority.query('command_capacity.status', {})['normal']['used'], 2)
        class Adapter:
            def handle(self, _context, _request):
                raise AssertionError('capacity fixture never dispatches providers')
        with tempfile.TemporaryDirectory() as directory:
            runner = authority.runner({'agents': ['a', 'b'], 'max_calls': 2, 'wall_seconds': 300},
                {'a': Adapter(), 'b': Adapter()}, directory, principal='alice', workflow_version='capacity/1', supervised=False)
            run, owner = runner.run_id, runner._session.owner
            authority.command('step', {'run': run, 'owner': owner, 'key': 'effect', 'kind': 'invoke', 'input': {}, 'initial': {}})
            authority.command('prepare_invocation', {'run': run, 'owner': owner, 'step_key': 'effect',
                'actor': 'a', 'call_id': 'call-one', 'reservation_microusd': 70})
            admission = {'run': run, 'owner': owner, 'call_id': 'call-one'}
            first = authority.command('admit_invocation', admission, command_id='admit-one')
            self.assertFalse(first['replayed'])
            full = authority.query('command_capacity.status', {})
            self.assertEqual(full['normal'], {'limit': 6, 'used': 6, 'remaining': 0})
            self.assert_code('command_capacity_exhausted', lambda: authority.configure_scope(
                'principal/bob', max_runs=1, max_calls=1, max_microusd=1))
            self.assertEqual(authority.query('command_capacity.status', {}), full)
            authority.command('observe_invocation', {**admission, 'state': 'unknown', 'output': None,
                'evidence': {'lost': 'fixture provider reply'}})
            runner.close()
        self.assertEqual(authority.query('command_capacity.status', {})['recovery']['remaining'], 0)
        self.assert_code('recovery_capacity_exhausted', lambda: authority.command('authority.drain', {'draining': True, 'operator': 'owner', 'evidence': {'ticket': 'fixture'}}))
        self.assertTrue(authority.command('admit_invocation', admission, command_id='admit-one')['replayed'])
        self.assertEqual(authority.query('scope', {'scope': 'deployment'})['microusd'], 70)
        self.assertEqual(authority._request('status', run=run)['calls'][0]['state'], 'reserved')
        amendment = {'request_key': 'grow-one', 'expected_revision': 0, 'normal_limit': 7,
            'recovery_limit': 4, 'operator': 'owner', 'evidence': {'capacity_review': 'fixture/1'}}
        receipt = authority.command('command_capacity.amend', amendment, command_id='amend-one')
        self.assertEqual(receipt['receipt']['status']['administration']['used'], 1)
        self.assertTrue(authority.command('command_capacity.amend', amendment, command_id='amend-one')['replayed'])
        authority.install_host(command_capacity=policy)
        self.assertEqual(authority.query('command_capacity.status', {})['normal']['limit'], 7)
        self.assert_code('command_capacity_conflict', lambda: authority.command('command_capacity.amend',
            {**amendment, 'request_key': 'shrink', 'expected_revision': 1, 'normal_limit': 6}))
        self.assertEqual(authority.query('command_capacity.status', {})['administration']['used'], 1)
        authority.command('command_capacity.amend', {**amendment, 'request_key': 'grow-two',
            'expected_revision': 1, 'normal_limit': 8}, command_id='amend-two')
        self.assert_code('administration_capacity_exhausted', lambda: authority.command('command_capacity.amend',
            {**amendment, 'request_key': 'grow-three', 'expected_revision': 2, 'normal_limit': 9}))
        self.assertTrue(authority.command('admit_invocation', admission, command_id='admit-one')['replayed'])
        self.assertFalse(authority.query('command_capacity.status', {})['receipts_pruned'])

    def test_concurrent_mutations_never_cross_frozen_pool_limit(self):
        authority, _ = self.authority(normal=2)
        def configure(index):
            try:
                authority.configure_scope('principal/' + str(index), max_runs=1, max_calls=1, max_microusd=1)
                return True
            except ServiceError as error:
                self.assertEqual(error.code, 'command_capacity_exhausted')
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            self.assertEqual(sum(pool.map(configure, range(5))), 2)
        self.assertEqual(authority.query('command_capacity.status', {})['normal'], {'limit': 2, 'used': 2, 'remaining': 0})

    def test_frozen_policy_cannot_be_replaced_by_restart(self):
        authority, policy = self.authority()
        authority.install_host(command_capacity=policy)
        with self.assertRaises(RunnerError):
            authority.install_host(command_capacity={**policy, 'normal_limit': 100})
        self.assertEqual(authority.query('command_capacity.status', {})['initial_policy'], policy)

    def test_full_normal_pool_keeps_subscription_closure_available(self):
        from mas_host.shared_events import SharedEventDelivery
        authority, _ = self.authority(normal=3, recovery=2)
        SharedEventDelivery(authority, object(), {}, max_subscriptions=3, page_size=10)
        for subscription in ('stop-me', 'expire-me'):
            authority.command('event.subscribe', {'principal': 'alice', 'subscription_id': subscription,
                'task_id': 'task-one', 'descriptor': {'principal': 'alice', 'task_id': 'task-one',
                    'after': 0}, 'after': 0})
        self.assertEqual(authority.query('command_capacity.status', {})['normal']['remaining'], 0)
        SharedEventDelivery(authority, object(), {}, max_subscriptions=3, page_size=10)
        self.assertEqual(authority.query('command_capacity.status', {})['normal']['used'], 3)
        scope = {'principal': 'alice', 'subscription_id': 'stop-me'}
        first = authority.command('event.stop', scope, command_id='stop-command')
        self.assertEqual(first['receipt']['state'], 'stopped')
        expired = authority.command('event.expire', {**scope, 'subscription_id': 'expire-me'})
        self.assertEqual(expired['receipt']['state'], 'resync_required')
        self.assertEqual(authority.query('command_capacity.status', {})['recovery']['remaining'], 0)
        self.assertTrue(authority.command('event.stop', scope, command_id='stop-command')['replayed'])
        self.assert_code('command_capacity_exhausted', lambda: authority.command('event.subscribe',
            {**scope, 'subscription_id': 'new-work', 'task_id': 'task-one',
             'descriptor': {'principal': 'alice', 'task_id': 'task-one', 'after': 0}, 'after': 0}))
        self.assertEqual(authority.query('event.read', scope)['record']['state'], 'stopped')

    def test_worker_and_artifact_ports_reconstruct_at_normal_capacity(self):
        from mas_host.shared_control import SharedWorkerController
        from mas_host.shared_artifacts import SharedArtifactStore
        from mas_host.s3_artifacts import S3ArtifactContent
        from mas_host.s3_transport import S3ContentConfig
        import test_s3_artifacts as fixtures
        authority, _ = self.authority(normal=2)
        grants = {'worker': {'trust_zone': 'trusted', 'capacity': 1, 'capabilities': {'work': 'a' * 64}}}
        backend = S3ArtifactContent(S3ContentConfig('https://objects.example', 'test-bucket', 'mas',
            'us-east-1', Path('/unused-fixture-credentials')), client=fixtures.ObjectClient())
        SharedWorkerController(authority, grants)
        first = SharedArtifactStore(authority, namespace='results', principal='alice', content_backend=backend)
        self.assertEqual(authority.query('command_capacity.status', {})['normal']['remaining'], 0)
        SharedWorkerController(authority, grants)
        restored = SharedArtifactStore(authority, namespace='results', principal='alice', content_backend=backend)
        self.assertEqual(restored.status(), first.status())
        self.assertEqual(authority.query('command_capacity.status', {})['normal']['used'], 2)
        self.assert_code('command_capacity_exhausted', lambda: authority.configure_scope(
            'new-work', max_runs=1, max_calls=1, max_microusd=1))
