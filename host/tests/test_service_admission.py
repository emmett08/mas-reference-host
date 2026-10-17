"""Both durable ingress APIs preserve admission and recovery at storage limits."""
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mas_host.contracts import Outcome
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError, backup_state, restore_state
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan
from test_workflow_engine_contracts import specification
import test_runner


class LostResponse:
    def __init__(self, receipt):
        self.receipt = receipt

    def handle(self, context, request):
        def effect():
            with self.receipt.open('ab') as stream:
                stream.write(b'observed\n')
            raise RuntimeError('fixture response lost after observed effect')
        return Outcome(context.invoke(effect, input={'fixture': 'lost-response'}, reservation_microusd=1))


class Target:
    """Present the same client actions to each public ingress API."""
    def __init__(self, kind, state, *, readonly=False, lost_response=False):
        self.kind, self.state = kind, state
        ledger = DeploymentLedger(state, readonly=readonly)
        echo = test_runner.FunctionAdapter(lambda _c, request: Outcome(request.input))
        worker = LostResponse(state.parent / 'effect-receipt') if lost_response else echo
        workflow = Workflow('admission-test/1', test_runner.topology(agents=('a', 'b')),
                            {'a': echo, 'b': worker}, 'a', 'b', 'echo',
                            classify=lambda outcome: 'completed' if outcome.output.get('done') else 'input_required',
                            continue_input=lambda previous, supplied: previous | supplied)
        options = {'operations': ledger, 'mas_bin': test_runner.RunnerIntegrationTests.mas,
                   'gateway_bin': test_runner.RunnerIntegrationTests.gateway}
        if kind == 'composition':
            self.owner = WorkflowEngine(state, {'example': WorkflowPlan(specification())},
                                        {'echo': workflow}, {'alice': {'example'}}, **options)
        else:
            self.owner = DurableTaskService(state, {'echo': workflow}, {'alice': {'echo'}},
                                            supervised=False, **options)

    def submit(self, key='request', value=None):
        return self.owner.submit('alice', key, 'example' if self.kind == 'composition' else 'echo',
                                 {} if value is None else value)

    def identity(self, receipt):
        return receipt['instance_id' if self.kind == 'composition' else 'task_id']

    def run(self, identifier):
        return self.owner.run('alice', identifier) if self.kind == 'composition' else self.owner.run_once()

    def continue_work(self, identifier, value=None):
        supplied = {'done': True} if value is None else value
        if self.kind == 'composition':
            return self.owner.continue_task('alice', identifier, 'root', 'decision', supplied)
        return self.owner.update('alice', identifier, 'decision', supplied)

    def counts(self):
        with self.owner.store.transaction(readonly=True) as db:
            tables = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            return {name: db.execute('SELECT count(*) FROM "' + name + '"').fetchone()[0] for name in tables}


class ServiceAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_runner.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def target(self, kind, *, name='', lost_response=False):
        state = self.root / (kind + name) / 'state'
        DeploymentLedger(state, DeploymentPolicy(min_free_bytes=1, max_state_bytes=2_000_000))
        return Target(kind, state, lost_response=lost_response)

    @contextmanager
    def closed_storage(self, target, threshold):
        if threshold == 'free':
            with patch('mas_host.operations.shutil.disk_usage', return_value=SimpleNamespace(free=0)):
                yield
        else:
            fill = target.state / 'capacity-fill'
            fill.write_bytes(b'x' * target.owner.operations.policy.max_state_bytes)
            try:
                yield
            finally:
                fill.unlink()

    def test_new_submission_rejected_before_any_request_or_intent_is_persisted(self):
        for kind in ('task', 'composition'):
            for threshold in ('free', 'state'):
                with self.subTest(kind=kind, threshold=threshold):
                    target = self.target(kind, name=threshold)
                    before = target.counts()
                    with self.closed_storage(target, threshold):
                        with self.assertRaises(OperationsError):
                            target.submit(value={'data': 'x' * 10000})
                    self.assertEqual(target.counts(), before)
                    self.assertFalse(list(target.state.glob('run-*')))

    def test_duplicate_submission_and_cancellation_remain_available(self):
        for kind in ('task', 'composition'):
            for threshold in ('free', 'state'):
                with self.subTest(kind=kind, threshold=threshold):
                    target = self.target(kind, name=threshold)
                    first = target.submit(value={'flag': True})
                    identifier = target.identity(first)
                    with self.closed_storage(target, threshold):
                        before = target.counts()
                        self.assertEqual(target.submit(value={'flag': True}), first)
                        with self.assertRaisesRegex(ServiceError, 'request conflict'):
                            target.submit(value={'flag': 1})
                        self.assertEqual(target.counts(), before)
                        cancelled = target.owner.cancel('alice', identifier)
                        self.assertEqual(cancelled['state'], 'cancelled')
                        self.assertEqual(target.submit(value={'flag': True})['state'], 'cancelled')

    def test_new_continuation_rejected_but_identical_revision_can_be_replayed(self):
        for kind in ('task', 'composition'):
            for threshold in ('free', 'state'):
                with self.subTest(kind=kind, threshold=threshold):
                    target = self.target(kind, name=threshold)
                    identifier = target.identity(target.submit())
                    self.assertEqual(target.run(identifier)['state'], 'input_required')
                    before = target.counts()
                    with self.closed_storage(target, threshold):
                        with self.assertRaises(OperationsError):
                            target.continue_work(identifier)
                    self.assertEqual(target.counts(), before)
                    continued = target.continue_work(identifier)
                    with self.closed_storage(target, threshold):
                        before = target.counts()
                        self.assertEqual(target.continue_work(identifier), continued)
                        with self.assertRaisesRegex(ServiceError, 'request conflict'):
                            target.continue_work(identifier, {'done': 1})
                        self.assertEqual(target.counts(), before)
                        self.assertEqual(target.owner.cancel('alice', identifier)['state'], 'cancelled')

    def test_evidenced_recovery_remains_available_without_repeating_effect(self):
        for kind in ('task', 'composition'):
            with self.subTest(kind=kind):
                target = self.target(kind, lost_response=True)
                identifier = target.identity(target.submit())
                self.assertEqual(target.run(identifier)['state'], 'unknown')
                run, = target.state.glob('run-*')
                call, = inspect_run(run)['invocations']
                with self.closed_storage(target, 'free'):
                    resolve_invocation(run, call['call_id'], decision='completed', output={'done': True},
                                       evidence={'receipt': 'local fixture observed'})
                    self.assertEqual(target.owner.recover('alice', identifier)['state'], 'queued')
                    self.assertEqual(target.run(identifier)['state'], 'completed')
                self.assertEqual((target.state.parent / 'effect-receipt').read_bytes(), b'observed\n')

    def test_readonly_handle_and_audit_restore_reject_all_mutations(self):
        for kind in ('task', 'composition'):
            with self.subTest(kind=kind):
                target = self.target(kind)
                identifier = target.identity(target.submit())
                bundle, restored = self.root / (kind + '-backup'), self.root / (kind + '-restored')
                backup_state(target.state, bundle)
                restore_state(bundle, restored)
                for restricted in (Target(kind, target.state, readonly=True), Target(kind, restored)):
                    before = restricted.counts()
                    self.assertEqual(restricted.owner.inspect('alice', identifier)['state'], 'queued')
                    for action in (restricted.submit,
                                   lambda: restricted.continue_work(identifier),
                                   lambda: restricted.owner.cancel('alice', identifier),
                                   lambda: restricted.owner.recover('alice', identifier),
                                   lambda: restricted.run(identifier)):
                        with self.assertRaisesRegex(ServiceError, 'audit.only'):
                            action()
                    self.assertEqual(restricted.counts(), before)
