"""Restart at the durable join/stop boundary with a lost worker receipt."""
from pathlib import Path
import tempfile
import time
import unittest

from mas_host import Invocation, Outcome
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan
from test_runner import FunctionAdapter
import test_runner as runner_tests
from test_workflow_engine_contracts import RecordedEffect, workflow
from test_workflow_joins import parallel, recover
from test_workflow_plan import spec


class SchedulerReplyLost(BaseException):
    pass


class WorkflowJoinRestartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def scenario(self, *, uncertain, failure=False):
        marker = self.root / 'worker-started'

        def selected(_context, _request):
            deadline = time.monotonic() + 10
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            if not marker.exists():
                raise RuntimeError('obsolete worker did not start')
            if failure:
                raise RuntimeError('known body failure before invocation')
            return Outcome({'accepted': True})

        def obsolete(context, _request):
            def action():
                with marker.open('a') as stream:
                    stream.write(context.invocation_id + '\n')
                time.sleep(20)
                return Invocation({'accepted': False}, {'receipt': marker.name})
            if uncertain:
                return Outcome(context.invoke(action, input={}, reservation_microusd=1))
            marker.write_text('no external invocation\n')
            time.sleep(20)
            return Outcome({'accepted': False})

        def registered(adapter):
            return workflow(adapters={'a': FunctionAdapter(lambda _context, request: Outcome(request.input)),
                                      'b': adapter})

        workflows = {'a': registered(FunctionAdapter(selected)), 'b': registered(FunctionAdapter(obsolete))}
        branches = {name: {'kind': 'task', 'workflow': name} for name in workflows}
        if failure:
            node = recover({'kind': 'parallel', 'branches': branches})
            workflows['compensate'] = registered(RecordedEffect(self.root / 'compensation', {'recorded': True}))
        else:
            node = parallel(branches=branches)
        source = spec(node)
        source['limits'].update(max_parallel=2, max_reserved_microusd=100)

        def reopen():
            return WorkflowEngine(self.root / 'state', {'pipeline': WorkflowPlan(source)}, workflows,
                {'owner': {'pipeline'}}, mas_bin=runner_tests.RunnerIntegrationTests.mas,
                gateway_bin=runner_tests.RunnerIntegrationTests.gateway)

        engine = reopen()
        instance = engine.submit('owner', 'request', 'pipeline', {})['instance_id']
        obsolete_path = 'root/body/p-b' if failure else 'root/p-b'
        original_save = engine.store.save_attempt

        def lose_worker_receipt(attempt, state, output=None, error=None):
            # The run journal survives; only the scheduler's terminal receipt
            # is lost. It therefore retains the old running assignment.
            if attempt['path'] == obsolete_path and state != 'running':
                return
            original_save(attempt, state, output, error)

        original_decision = engine.store.decide_join

        def lose_scheduler_reply(*arguments):
            original_decision(*arguments)
            raise SchedulerReplyLost()

        engine.store.save_attempt = lose_worker_receipt
        engine.store.decide_join = lose_scheduler_reply
        with self.assertRaises(SchedulerReplyLost):
            engine.run('owner', instance)
        retained = engine.inspect('owner', instance)
        attempt = next(item for item in retained['attempts'] if item['path'] == obsolete_path)
        self.assertEqual(attempt['state'], 'running')
        self.assertEqual(retained['state'], 'running')
        run_dir = engine.state_dir / attempt['run_directory']
        self.assertEqual(inspect_run(run_dir)['recovery_required'], uncertain)
        return reopen, instance, run_dir, marker

    def test_replacement_closes_known_obsolete_assignment_without_redispatch(self):
        reopen, instance, _run_dir, marker = self.scenario(uncertain=False)
        engine = reopen()
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['join']['selected'], ['a'])
        self.assertEqual([item['state'] for item in result['attempts']], ['completed', 'cancelled'])
        self.assertEqual(result['usage']['calls'], 0)
        self.assertEqual(marker.read_text(), 'no external invocation\n')
        self.assertEqual(reopen().run('owner', instance), result)

    def test_replacement_preserves_unknown_obsolete_effect_and_original_selection(self):
        reopen, instance, run_dir, marker = self.scenario(uncertain=True)
        engine = reopen()
        unknown = engine.run('owner', instance)
        self.assertEqual(unknown['state'], 'unknown', unknown)
        self.assertIsNone(unknown['output'])
        self.assertEqual(unknown['usage']['calls'], 1)
        call = inspect_run(run_dir)['invocations'][0]
        resolve_invocation(run_dir, call['call_id'], decision='completed', output={'accepted': False},
                           evidence={'receipt': marker.name})
        engine.recover('owner', instance)
        result = reopen().run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['join']['selected'], ['a'])
        self.assertEqual(result['usage']['calls'], 1)
        self.assertEqual(marker.read_text().splitlines(), [call['call_id']])

    def test_replacement_does_not_compensate_until_unknown_body_is_resolved(self):
        reopen, instance, run_dir, marker = self.scenario(uncertain=True, failure=True)
        engine = reopen()
        unknown = engine.run('owner', instance)
        self.assertEqual(unknown['state'], 'unknown', unknown)
        self.assertFalse((self.root / 'compensation').exists())
        call = inspect_run(run_dir)['invocations'][0]
        resolve_invocation(run_dir, call['call_id'], decision='completed', output={'accepted': False},
                           evidence={'receipt': marker.name})
        engine.recover('owner', instance)
        result = reopen().run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['result'], {'handled': True})
        self.assertEqual(result['usage']['calls'], 2)
        self.assertEqual(len((self.root / 'compensation').read_text().splitlines()), 1)
        self.assertEqual(marker.read_text().splitlines(), [call['call_id']])
        self.assertEqual(reopen().run('owner', instance), result)


if __name__ == '__main__':
    unittest.main()
