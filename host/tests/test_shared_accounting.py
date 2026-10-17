"""Period admission and observations against the actual shared PostgreSQL port."""
import concurrent.futures
import os
from pathlib import Path
import tempfile
import time
import unittest
import uuid

from mas_host import Invocation, Outcome, RunnerError
from mas_host.accounting import PeriodPolicy
from mas_host.operations import OperationsError
from mas_host.shared_accounting import SharedAccountingAdministration
from mas_host.shared_authority import SharedAuthority


class Effect:
    def handle(self, context, request):
        return Outcome(context.invoke(lambda: Invocation(request.input, {'effect': 'fixture'}),
                                      input=request.input, reservation_microusd=10))


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedAccountingTests(unittest.TestCase):
    def setUp(self):
        binary = os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared')
        self.authority = SharedAuthority(binary=binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace='mas_accounting_' + uuid.uuid4().hex, deployment='deployment', controller='controller', epoch=1,
            secret_key=b'fixture-key' * 4, secret_reference='fixture/1')
        self.authority.initialise()
        for scope in ('deployment', 'principal/alice', 'principal/bob'):
            self.authority.configure_scope(scope, max_runs=100, max_calls=100, max_microusd=10000)
        self.admin = SharedAccountingAdministration(self.authority)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runners = []
        self.addCleanup(lambda: [runner.close() for runner in self.runners])

    def runner(self, principal='alice'):
        runner = self.authority.runner({'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 10, 'wall_seconds': 120}, {'a': Effect(), 'b': Effect()},
            Path(self.temp.name) / uuid.uuid4().hex, principal=principal,
            workflow_version='accounting/1', supervised=False)
        self.runners.append(runner)
        return runner

    def configure(self, scope='deployment', calls=2, amount=20, start=0):
        return self.admin.configure_period(scope, PeriodPolicy(86400, calls, amount, start),
            request_key='policy-' + scope, actor='owner', reason='Bound the fixture period')

    def test_scopes_reject_atomically_without_native_or_period_extra_charge(self):
        self.configure(calls=100, amount=1000)
        self.configure('principal:alice', calls=1)
        runner = self.runner()
        runner.request('a', 'b', 'effect', {'n': 1})
        with self.assertRaises(RunnerError):
            runner.request('a', 'b', 'effect', {'n': 2})
        self.assertEqual([p['calls'] for p in self.admin.periods()], [1, 1])
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 1)
        self.assertEqual(len(self.authority.query('invocations', {'run': runner.run_id})), 2)
        self.runner('bob').request('a', 'b', 'effect', {'n': 3})
        self.assertEqual([p['calls'] for p in self.admin.periods()], [2, 1])

    def test_configure_backfills_exact_timestamp_and_immutable_policy(self):
        runner = self.runner()
        runner.request('a', 'b', 'effect', {})
        call = self.authority.query('invocations', {'run': runner.run_id})[0]
        before = self.admin.reconcile(runner.run_id, call['call_id'])
        self.assertIsInstance(before['admitted_at'], int)
        self.assertEqual(before['timestamp_basis'], 'admission_timestamp')
        first = self.configure(calls=1)
        self.assertEqual(first, self.configure(calls=1))
        period = self.admin.periods()[0]
        self.assertEqual((period['calls'], period['reserved_microusd'], period['legacy_unknown_calls']), (1, 10, 0))
        with self.assertRaises(OperationsError):
            self.configure(calls=2)
        with self.assertRaises(RunnerError):
            self.runner().request('a', 'b', 'effect', {})
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 1)

    def test_concurrent_admissions_keep_period_bound_and_exact_permanent_count(self):
        self.configure(calls=2)
        runners = [self.runner() for _ in range(5)]
        def execute(item):
            try:
                item.request('a', 'b', 'effect', {})
                return True
            except RunnerError:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            self.assertEqual(sum(pool.map(execute, runners)), 2)
        self.assertEqual(self.admin.periods()[0]['calls'], 2)
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['microusd'], 20)

    def test_negative_bucket_uses_floor_and_charge_observations_never_refund(self):
        start = int(time.time()) + 86401
        receipt = self.configure(start=start)
        self.assertEqual(receipt['high_water'], PeriodPolicy(86400, 2, 20, start).bucket(receipt['created']))
        self.assertLess(receipt['high_water'], 0)
        runner = self.runner()
        runner.request('a', 'b', 'effect', {})
        call = self.authority.query('invocations', {'run': runner.run_id})[0]['call_id']
        charge = {'provider': 'provider', 'charge_id': 'invoice-one', 'run_id': runner.run_id,
            'call_id': call, 'actual_microusd': 3, 'observed_at': int(time.time()) - 1,
            'evidence': 'invoice/one', 'actor': 'owner'}
        self.assertEqual(self.admin.record_charge(**charge), self.admin.record_charge(**charge))
        with self.assertRaises(OperationsError):
            self.admin.record_charge(**(charge | {'actual_microusd': 2}))
        self.admin.record_charge(**(charge | {'charge_id': 'invoice-two', 'actual_microusd': 20}))
        report = self.admin.reconcile(runner.run_id, call)
        self.assertEqual(report['observed_charge_microusd'], 23)
        self.assertEqual(report['status'], 'exceeds_reservation')
        self.assertFalse(report['invoice_complete'])
        self.assertFalse(report['allocation_changed'])
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['microusd'], 10)
        self.assertEqual(self.admin.periods()[0]['reserved_microusd'], 10)
        with self.assertRaises(OperationsError):
            self.admin.record_charge(**(charge | {'charge_id': 'future', 'observed_at': 2**63 - 1}))
        with self.assertRaises(OperationsError):
            self.admin.record_charge(**(charge | {'charge_id': 'forged', 'call_id': 'not-admitted'}))


if __name__ == '__main__':
    unittest.main()
