"""Aggregate principal caps survive concurrency, replay, archive and migration."""
from contextlib import closing
from dataclasses import asdict
import json
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from mas_host.operations import (DeploymentLedger, DeploymentPolicy, OperationsError,
                                 archive_workflows, backup_state, restore_state, _file_lock)
from mas_host.principal_quotas import PrincipalQuota, pending_count
from mas_host.schema import LEDGER_V3, SchemaError, validate
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from mas_host.unified_service import UnifiedTaskService
from mas_host.workflow_plan import WorkflowPlan
from mas_host.contracts import Invocation, Outcome
from test_definition_versions import registered, Executor
from test_workflow_engine_contracts import specification
import test_runner as runner_tests


def allocate(state, run_id, call_id, result):
    try:
        result.put(DeploymentLedger(Path(state)).admit_call(run_id, call_id, 30))
    except OperationsError:
        result.put(False)


def submit(state, key, result):
    try:
        service = DurableTaskService(Path(state), {'leaf': registered()}, {'alice': {'leaf'}}, executor=Executor())
        service.submit('alice', key, 'leaf', {})
        result.put('accepted')
    except ServiceError as error:
        result.put(error.code)


class PrincipalQuotaTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root / 'state'

    def ledger(self, **quotas):
        return DeploymentLedger(self.state, DeploymentPolicy(min_free_bytes=1), principal_quotas={
            owner: asdict(value) for owner, value in quotas.items()})

    def test_each_principal_and_deployment_charge_once_without_refund(self):
        ledger = self.ledger(alice=PrincipalQuota(2, 2, 60), bob=PrincipalQuota(2, 1, 90))
        ledger.admit_run('alice-run', self.state / 'alice-run', principal='alice')
        ledger.admit_run('bob-run', self.state / 'bob-run', principal='bob')
        self.assertTrue(ledger.admit_call('alice-run', 'one', 40))
        self.assertFalse(ledger.admit_call('alice-run', 'one', 40))
        with self.assertRaisesRegex(OperationsError, 'expenditure'):
            ledger.admit_call('alice-run', 'two', 21)
        self.assertTrue(ledger.admit_call('alice-run', 'two', 20))
        with self.assertRaisesRegex(OperationsError, 'call limit'):
            ledger.admit_call('alice-run', 'three', 0)
        self.assertTrue(ledger.admit_call('bob-run', 'one', 90))
        old = ledger.principal_usage('alice')
        restarted = DeploymentLedger(self.state)
        self.assertEqual(restarted.principal_usage('alice'), old)
        self.assertEqual(restarted.health()['usage']['reserved_microusd'], 150)
        with self.assertRaisesRegex(OperationsError, 'principal conflict'):
            ledger.admit_run('alice-run', self.state / 'alice-run', principal='bob')
        with self.assertRaisesRegex(OperationsError, 'attributed'):
            ledger.admit_run('unowned', self.state / 'unowned')
        with self.assertRaisesRegex(OperationsError, 'changed'):
            DeploymentLedger(self.state, principal_quotas={})

    def test_concurrent_calls_cannot_over_admit(self):
        ledger = self.ledger(alice=PrincipalQuota(4, 3, 60))
        for run in ('left', 'right'):
            ledger.admit_run(run, self.state / run, principal='alice')
        context = multiprocessing.get_context('fork')
        results = context.Queue()
        workers = [context.Process(target=allocate, args=(str(self.state), ('left', 'right')[i % 2], str(i), results))
                   for i in range(6)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(15)
            self.assertEqual(worker.exitcode, 0)
        self.assertEqual(sum(results.get(timeout=1) for _ in workers), 2)
        self.assertEqual(ledger.principal_usage('alice')['reserved_microusd'], 60)
        self.assertEqual(ledger.health()['usage']['calls'], 2)
        results.close()

    def test_quota_installation_requires_quiescence_and_saved_policy_is_validated(self):
        ledger = DeploymentLedger(self.state)
        quotas = {'alice': asdict(PrincipalQuota(1, 1, 10))}
        with _file_lock(self.state / '.deployment.lock', False):
            with self.assertRaisesRegex(OperationsError, 'busy'):
                DeploymentLedger(self.state, principal_quotas=quotas)
        self.assertIsNone(ledger.principal_policy('alice'))
        DeploymentLedger(self.state, principal_quotas=quotas)
        with _file_lock(self.state / '.deployment.lock', False):
            self.assertEqual(DeploymentLedger(self.state, principal_quotas=quotas).principal_policy('alice'), quotas['alice'])
        with closing(sqlite3.connect(ledger.path)) as db:
            db.execute("UPDATE principal_quotas SET policy='{}'")
            db.commit()
        before = ledger.path.read_bytes()
        with self.assertRaises(OperationsError):
            DeploymentLedger(self.state)
        self.assertEqual(ledger.path.read_bytes(), before)

    def test_queue_admission_is_shared_and_duplicate_replay_survives_exhaustion(self):
        ledger = self.ledger(alice=PrincipalQuota(1, 10, 100))
        service = UnifiedTaskService(self.state, {'echo': registered()},
            {'plan': WorkflowPlan({**specification(), 'name': 'plan', 'root': {'kind': 'task', 'workflow': 'echo'}})},
            {'alice': {'echo', 'plan'}, 'bob': {'echo', 'plan'}}, operations=ledger, executor=Executor())
        first = service.leaves.submit('alice', 'first', 'echo', {})
        self.assertEqual(service.leaves.submit('alice', 'first', 'echo', {}), first)
        with self.assertRaisesRegex(ServiceError, 'principal queue limit'):
            service.compositions.submit('alice', 'second', 'plan', {})
        service.leaves.cancel('alice', first['task_id'])
        plan = service.compositions.submit('alice', 'second', 'plan', {})
        with self.assertRaisesRegex(ServiceError, 'principal queue limit'):
            service.leaves.submit('alice', 'third', 'echo', {})
        self.assertEqual(pending_count(ledger, 'alice'), 1)
        self.assertEqual(service.leaves.submit('bob', 'other', 'echo', {})['state'], 'queued')
        with service.compositions.store.transaction() as db:
            db.execute("UPDATE instances SET state='input_required' WHERE id=?", (plan['instance_id'],))
        with self.assertRaisesRegex(ServiceError, 'principal queue limit'):
            service.leaves.submit('alice', 'paused-still-counts', 'echo', {})
        service.compositions.cancel('alice', plan['instance_id'])
        archive_workflows(self.state, 0, apply=True)
        self.assertEqual(pending_count(ledger, 'alice'), 0)
        self.assertEqual(service.leaves.submit('alice', 'third', 'echo', {})['state'], 'queued')

    def test_concurrent_submissions_obey_pending_limit(self):
        self.ledger(alice=PrincipalQuota(2, 10, 100))
        DurableTaskService(self.state, {'leaf': registered()}, {'alice': {'leaf'}}, executor=Executor())
        context = multiprocessing.get_context('fork')
        results = context.Queue()
        workers = [context.Process(target=submit, args=(str(self.state), str(i), results)) for i in range(6)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(20)
            self.assertEqual(worker.exitcode, 0)
        observed = [results.get(timeout=1) for _ in workers]
        self.assertEqual(observed.count('accepted'), 2)
        self.assertEqual(observed.count('principal_queue_limit'), 4)
        results.close()

    def test_unknown_legacy_ownership_counts_conservatively_and_backups_preserve_limits(self):
        self.state.mkdir(mode=0o700)
        policy = DeploymentPolicy(min_free_bytes=1)
        with closing(sqlite3.connect(self.state / 'deployment.sqlite')) as db:
            for statement in LEDGER_V3.statements:
                db.execute(statement)
            db.execute(f'PRAGMA application_id={LEDGER_V3.application_id}')
            db.execute(f'PRAGMA user_version={LEDGER_V3.version}')
            db.executemany('INSERT INTO metadata VALUES(?,?)', [
                ('policy', json.dumps(asdict(policy))), ('deployment_id', 'legacy'),
                ('created_at', '1'), ('admission_state', 'active')])
            db.execute("INSERT INTO runs VALUES('legacy','legacy',1,NULL)")
            db.execute("INSERT INTO calls VALUES('legacy','charged',40,1)")
            db.commit()
        ledger = self.ledger(alice=PrincipalQuota(3, 3, 60), bob=PrincipalQuota(3, 3, 60))
        for owner in ('alice', 'bob'):
            self.assertEqual(ledger.principal_usage(owner)['unattributed_reserved_microusd'], 40)
        with closing(sqlite3.connect(ledger.path)) as db, self.assertRaises(SchemaError):
            validate(db, LEDGER_V3)
        ledger.admit_run('new', self.state / 'new', principal='alice')
        with self.assertRaisesRegex(OperationsError, 'expenditure'):
            ledger.admit_call('new', 'new-charge', 21)
        ledger.admit_call('new', 'new-charge', 20)
        ledger.admit_run('legacy', self.state / 'legacy', principal='alice')
        self.assertEqual(ledger.principal_usage('alice')['reserved_microusd'], 60)
        self.assertEqual(ledger.principal_usage('bob')['reserved_microusd'], 0)
        # This fixture represents already-pruned runs: only immutable accounting
        # remains, so the backup must preserve it without recreating run data.
        with closing(sqlite3.connect(ledger.path)) as db:
            db.execute('UPDATE runs SET purged_at=2')
            db.commit()
        backup_state(self.state, self.root / 'backup')
        restore_state(self.root / 'backup', self.root / 'restored')
        restored = DeploymentLedger(self.root / 'restored')
        self.assertEqual(restored.principal_usage('alice'), ledger.principal_usage('alice'))
        self.assertEqual(restored.principal_policy('alice'), ledger.principal_policy('alice'))
        with self.assertRaisesRegex(OperationsError, 'audit-only'):
            restored.admit_call('new', 'after-restore', 0)


class PrincipalQuotaRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {'mas_bin': runner_tests.RunnerIntegrationTests.mas,
                       'gateway_bin': runner_tests.RunnerIntegrationTests.gateway}

    def test_real_rust_dispatch_is_bounded_across_leaf_and_composition(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / 'state'
            ledger = DeploymentLedger(state, DeploymentPolicy(min_free_bytes=1),
                principal_quotas={'alice': asdict(PrincipalQuota(10, 1, 50)),
                                  'bob': asdict(PrincipalQuota(10, 1, 50))})
            def handle(context, request):
                return Outcome(context.invoke(lambda: Invocation(request.input, {'fixture': True}),
                                              input=request.input, reservation_microusd=50))
            leaf = Workflow('quotas/1', runner_tests.topology(),
                {actor: runner_tests.FunctionAdapter(handle) for actor in ('a', 'b', 'c')}, 'a', 'b', 'echo')
            service = UnifiedTaskService(state, {'echo': leaf},
                {'plan': WorkflowPlan({**specification(), 'name': 'plan', 'root': {'kind': 'task', 'workflow': 'echo'}})},
                {'alice': {'echo', 'plan'}, 'bob': {'echo', 'plan'}}, operations=ledger, supervised=False, **self.options)
            first = service.submit('alice', 'leaf', 'echo', {'answer': 42})
            self.assertEqual(service.run_once()['state'], 'completed')
            composed = service.submit('alice', 'plan', 'plan', {'answer': 43})
            result = service.compositions.run('alice', composed['task_id'])
            self.assertEqual(result['state'], 'failed')
            self.assertEqual(ledger.principal_usage('alice')['calls'], 1)
            other = service.submit('bob', 'leaf', 'echo', {})
            self.assertEqual(service.run_once()['task_id'], other['task_id'])
            self.assertEqual(ledger.principal_usage('bob')['calls'], 1)
            self.assertEqual(service.result('alice', first['task_id']), {'answer': 42})
            archive_workflows(state, 0, apply=True)
            self.assertEqual(ledger.principal_usage('alice')['calls'], 1)


if __name__ == '__main__':
    unittest.main()
