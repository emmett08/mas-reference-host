"""Period ceilings and invoice observations never replenish lifetime authority."""
import multiprocessing
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from mas_host.accounting import AccountingAdministration, PeriodPolicy
from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError


def contend(root, number, queue):
    try:
        DeploymentLedger(Path(root)).admit_call('run-one', 'call-' + str(number), 1)
        queue.put(True)
    except OperationsError:
        queue.put(False)


class AccountingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = DeploymentLedger(self.root, DeploymentPolicy(min_free_bytes=1))
        self.ledger.admit_run('run-one', self.root / 'run-one', principal='alice')
        self.admin = AccountingAdministration(self.ledger)

    def configure(self, scope='deployment', calls=2, allocation=20):
        return self.admin.configure_period(scope, PeriodPolicy(60, calls, allocation),
            request_key=scope + '-policy', actor='operator', reason='reference test limits')

    def test_duplicate_call_is_not_counted_again_across_periods(self):
        with patch('time.time', return_value=120):
            self.configure(calls=1)
            self.assertTrue(self.ledger.admit_call('run-one', 'one', 10))
            with self.assertRaisesRegex(OperationsError, 'period limit'):
                self.ledger.admit_call('run-one', 'two', 10)
        with patch('time.time', return_value=180):
            self.assertFalse(self.ledger.admit_call('run-one', 'one', 10))
            self.assertTrue(self.ledger.admit_call('run-one', 'two', 10))
            self.assertEqual(self.admin.periods()[0]['calls'], 1)
        self.assertEqual(self.ledger.health()['usage']['calls'], 2)

    def test_clock_rollback_does_not_reopen_prior_period(self):
        with patch('time.time', return_value=120):
            self.configure()
        with patch('time.time', return_value=180):
            self.ledger.admit_call('run-one', 'one', 5)
        with patch('time.time', return_value=119):
            with self.assertRaisesRegex(OperationsError, 'clock moved backwards'):
                self.ledger.admit_call('run-one', 'two', 5)
        self.assertEqual(self.ledger.health()['usage']['calls'], 1)

    def test_installation_backfills_earlier_spending_and_policy_is_immutable(self):
        self.ledger.admit_call('run-one', 'old', 20)
        receipt = self.configure(allocation=20)
        self.assertEqual(receipt, self.configure(allocation=20))
        with self.assertRaisesRegex(OperationsError, 'period limit'):
            self.ledger.admit_call('run-one', 'new', 1)
        with self.assertRaisesRegex(OperationsError, 'policy conflict'):
            self.configure(allocation=21)

    def test_all_scope_checks_rollback_together_when_principal_limit_rejects(self):
        self.configure(calls=100)
        self.configure('principal:alice', calls=1)
        self.ledger.admit_call('run-one', 'one', 1)
        with self.assertRaises(OperationsError):
            self.ledger.admit_call('run-one', 'two', 1)
        self.assertEqual([row['calls'] for row in self.admin.periods()], [1, 1])

    def test_multi_process_period_admission_is_atomic(self):
        self.configure(calls=2)
        context = multiprocessing.get_context('fork')
        queue = context.Queue()
        processes = [context.Process(target=contend, args=(str(self.root), i, queue)) for i in range(6)]
        for process in processes:
            process.start()
        results = [queue.get(timeout=10) for _ in processes]
        for process in processes:
            process.join(10)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(sum(results), 2)
        self.assertEqual(self.admin.periods()[0]['calls'], 2)

    def test_charge_observation_replay_conflict_and_no_refund(self):
        self.ledger.admit_call('run-one', 'one', 10)
        command = dict(provider='provider', charge_id='invoice-one', run_id='run-one', call_id='one',
                       actual_microusd=3, observed_at=int(time.time()), evidence='invoice/one', actor='operator')
        self.assertEqual(self.admin.record_charge(**command), self.admin.record_charge(**command))
        self.assertEqual(self.admin.reconcile('run-one', 'one')['status'], 'within_reservation')
        with self.assertRaisesRegex(OperationsError, 'observation conflict'):
            self.admin.record_charge(**{**command, 'actual_microusd': 2})
        self.admin.record_charge(**{**command, 'charge_id': 'invoice-two', 'actual_microusd': 20})
        report = self.admin.reconcile('run-one', 'one')
        self.assertEqual(report['status'], 'exceeds_reservation')
        self.assertFalse(report['invoice_complete'])
        self.assertEqual(self.ledger.health()['usage']['reserved_microusd'], 10)

    def test_audit_only_cannot_add_policy_or_charge(self):
        self.ledger.admit_call('run-one', 'one', 10)
        (self.root / '.restored-audit-only').touch()
        with self.assertRaisesRegex(OperationsError, 'audit-only'):
            self.configure()
        with self.assertRaisesRegex(OperationsError, 'audit-only'):
            self.admin.record_charge(provider='provider', charge_id='one', run_id='run-one', call_id='one',
                actual_microusd=0, observed_at=int(time.time()), evidence='invoice/one', actor='operator')


if __name__ == '__main__':
    unittest.main()
