"""Lifecycle policies survive continuation and expire before new dispatch."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mas_host.execution_policy import TimingPolicy
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult
from mas_host.workflow_engine import WorkflowEngine
from test_definition_versions import registered, source
from test_workflow_engine_contracts import workflow, specification
import test_runner as runner_tests


class TimedExecutor:
    def __init__(self, clock, advances):
        self.clock, self.advances, self.calls = clock, advances, 0

    def execute(self, _workflow, execution, **_kwargs):
        self.calls += 1
        assert execution['execution_deadline'] is not None
        self.clock[0] += self.advances[self.calls-1]
        return ExecutionResult('input_required', {'value': self.calls})


class ServiceTimingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.clock = [1000]
        self.stack = mock.patch('mas_host.service_timing.milliseconds', side_effect=lambda: self.clock[0])
        self.stack.start(); self.addCleanup(self.stack.stop)
        patch = mock.patch('mas_host.task_service.now', side_effect=lambda: self.clock[0])
        patch.start(); self.addCleanup(patch.stop)

    def service(self, advances):
        executor = TimedExecutor(self.clock, advances)
        leaf = registered(timing_policy=TimingPolicy(100, 4, 20, 20),
                          continue_input=lambda previous, supplied: previous | supplied)
        return DurableTaskService(self.root, {'leaf': leaf}, {'alice': {'leaf'}}, executor=executor), executor

    def test_pause_does_not_consume_active_budget_and_continuation_cannot_reset_it(self):
        service, executor = self.service([3000, 2000])
        task = service.submit('alice', 'first', 'leaf', {})
        paused = service.run_once()
        self.assertEqual(paused['timing']['active_ms'], 3000)
        self.clock[0] = 15000
        continued = service.update('alice', task['task_id'], 'continue', {})
        self.assertEqual(continued['timing']['remaining_active_ms'], 1000)
        ended = service.run_once()
        self.assertEqual(ended['state'], 'failed')
        self.assertEqual(ended['error']['code'], 'active_budget')
        self.assertEqual(ended['timing']['active_ms'], 5000)
        self.assertEqual(executor.calls, 2)

    def test_queue_expiry_creates_no_execution_effect_or_new_identity(self):
        service, executor = self.service([])
        task = service.submit('alice', 'first', 'leaf', {})
        self.clock[0] = 22000
        ended = service.run_once()
        self.assertEqual(ended['task_id'], task['task_id'])
        self.assertEqual(ended['error']['code'], 'queue_deadline')
        self.assertEqual(executor.calls, 0)
        self.assertEqual(service.operations.health()['usage']['runs'], 0)

    def test_expired_human_pause_is_scheduled_for_cleanup_without_dispatch(self):
        service, executor = self.service([1000])
        task = service.submit('alice', 'first', 'leaf', {})
        service.run_once()
        self.clock[0] = 23000
        self.assertEqual(service.pending()[0]['task_id'], task['task_id'])
        with self.assertRaises(ServiceError) as error:
            service.update('alice', task['task_id'], 'late', {})
        self.assertEqual(error.exception.code, 'pause_deadline')
        self.assertEqual(service.run_once()['error']['code'], 'pause_deadline')
        self.assertEqual(executor.calls, 1)

    def test_reconciled_effect_after_expiry_closes_without_new_dispatch(self):
        service, executor = self.service([1000])
        task = service.submit('alice', 'first', 'leaf', {})
        service.run_once()
        with service.store.transaction() as db:
            db.execute("UPDATE tasks SET state='unknown' WHERE id=?", (task['task_id'],))
            db.execute("UPDATE executions SET state='unknown' WHERE task_id=?", (task['task_id'],))
        self.clock[0] = 23000
        with mock.patch('mas_host.journal.inspect_run', return_value={'recovery_required': True}):
            with self.assertRaises(ServiceError):
                service.recover('alice', task['task_id'])
        with mock.patch('mas_host.journal.inspect_run', return_value={'recovery_required': False}):
            closed = service.recover('alice', task['task_id'])
        self.assertEqual(closed['state'], 'failed')
        self.assertEqual(closed['error']['effect_reconciliation'], 'resolved')
        self.assertEqual(service.pending(), [])
        self.assertEqual(executor.calls, 1)


class CompositionTimingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def test_policy_persists_business_deadline_and_active_clock_across_pause(self):
        with tempfile.TemporaryDirectory() as root:
            leaf = workflow(classify=lambda outcome: 'completed' if outcome.output.get('approved') else 'input_required',
                            continue_input=lambda previous, supplied: previous | supplied)
            plan = specification()
            plan['limits']['wall_seconds'] = 1
            plan['timing_policy'] = TimingPolicy(86400, 60, 600, 3600).descriptor
            engine = WorkflowEngine(root, {'example': plan}, {'echo': leaf}, {'alice': {'example'}},
                mas_bin=runner_tests.RunnerIntegrationTests.mas, gateway_bin=runner_tests.RunnerIntegrationTests.gateway)
            task = engine.submit('alice', 'first', 'example', {'value': 1})
            self.assertEqual(task['deadline']-task['created'], 86400)
            paused = engine.run('alice', task['instance_id'])
            self.assertEqual(paused['state'], 'input_required')
            self.assertEqual(paused['clock']['phase'], 'paused')
            original_clock = paused['clock']
            continued = engine.continue_task('alice', task['instance_id'], 'root', 'approval', {'approved': True})
            self.assertEqual(continued['clock']['created_ms'], original_clock['created_ms'])
            self.assertEqual(continued['clock']['active_ms'], original_clock['active_ms'])
            done = engine.run('alice', task['instance_id'])
            self.assertEqual(done['state'], 'completed')
            self.assertGreaterEqual(done['clock']['active_ms'], original_clock['active_ms'])
            self.assertEqual(done['clock']['phase'], 'terminal')

    def test_nested_scope_has_its_own_pause_allowance_and_expires_without_new_attempt(self):
        with tempfile.TemporaryDirectory() as root:
            leaf = workflow(classify=lambda _outcome: 'input_required',
                            continue_input=lambda previous, supplied: previous | supplied)
            parent, child = specification(), specification()
            parent['root'] = {'kind': 'subworkflow', 'workflow': 'child'}
            parent['timing_policy'] = TimingPolicy(86400, 60, 600, 3600).descriptor
            child['name'] = 'child'
            child['timing_policy'] = TimingPolicy(3600, 30, 300, 1).descriptor
            engine = WorkflowEngine(root, {'example': parent, 'child': child}, {'echo': leaf}, {'alice': {'example'}},
                mas_bin=runner_tests.RunnerIntegrationTests.mas, gateway_bin=runner_tests.RunnerIntegrationTests.gateway)
            task = engine.submit('alice', 'first', 'example', {})
            paused = engine.run('alice', task['instance_id'])
            self.assertEqual(paused['state'], 'input_required')
            with engine.store.transaction() as db:
                row = db.execute('SELECT path,clock FROM scopes WHERE instance=?', (task['instance_id'],)).fetchone()
                clock = json.loads(row['clock'])
                self.assertEqual(clock['phase'], 'paused')
                for field in ('created_ms', 'observed_ms', 'phase_started_ms'):
                    clock[field] -= 2000
                db.execute('UPDATE scopes SET clock=? WHERE instance=?', (json.dumps(clock), task['instance_id']))
            attempt = paused['attempts'][0]
            with self.assertRaises(ServiceError) as error:
                engine.continue_task('alice', task['instance_id'], attempt['path'], 'too-late', {})
            self.assertEqual(error.exception.code, 'pause_deadline')
            ended = engine.run('alice', task['instance_id'])
            self.assertEqual(ended['state'], 'expired')
            self.assertEqual(ended['error']['code'], 'scope_timing_expired')
            self.assertEqual(ended['error']['scopes'][0]['path'], row['path'])
            self.assertEqual(len(ended['attempts']), 1)


if __name__ == '__main__':
    unittest.main()
