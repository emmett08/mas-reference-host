"""Cancellation closes future work while retaining completed and uncertain evidence."""
import json
import os
from pathlib import Path
import signal
import tempfile
import time
import unittest

from mas_host import Invocation, Outcome
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan
from test_runner import FunctionAdapter
from test_workflow_engine_contracts import specification, workflow
import test_runner as runner_tests


class _CancelAtDecision(WorkflowEngine):
    cancel_at_decision = False

    def _advance(self, instance, path, node, input, limits, deadline, ready):
        result = super()._advance(instance, path, node, input, limits, deadline, ready)
        if self.cancel_at_decision and result[0] == 'input_required':
            self.cancel_at_decision = False
            self.cancel('owner', instance)
        return result


class WorkflowCancellationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.children = set()
        self.addCleanup(self.cleanup_children)

    def cleanup_children(self):
        for pid in self.children:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass

    def engine(self, leaves, *, parallel=False, nested=False, engine_type=WorkflowEngine):
        source = specification()
        source['limits']['max_parallel'] = 1
        source['root'] = ({'kind': 'parallel', 'branches': {
            name: {'kind': 'task', 'workflow': name} for name in leaves}}
            if parallel else {'kind': 'task', 'workflow': next(iter(leaves))})
        if nested:
            source['root'] = {'kind': 'sequence', 'steps': [source['root']]}
        return engine_type(self.root / 'state', {'example': WorkflowPlan(source)}, leaves,
                           {'owner': {'example'}}, mas_bin=runner_tests.RunnerIntegrationTests.mas,
                           gateway_bin=runner_tests.RunnerIntegrationTests.gateway)

    @staticmethod
    def paused_workflow():
        return workflow(classify=lambda outcome: 'completed' if outcome.output.get('done') else 'input_required',
                        continue_input=lambda previous, supplied: previous | supplied)

    def submit(self, engine):
        return engine.submit('owner', 'request', 'example', {})['instance_id']

    def test_continue_then_cancel_closes_latest_revision_and_preserves_history(self):
        engine = self.engine({'leaf': self.paused_workflow()})
        instance = self.submit(engine)
        self.assertEqual(engine.run('owner', instance)['state'], 'input_required')
        engine.continue_task('owner', instance, 'root', 'decision', {'done': True})
        result = engine.cancel('owner', instance)
        self.assertEqual(result['state'], 'cancelled')
        self.assertEqual([attempt['state'] for attempt in result['attempts']], ['input_required', 'cancelled'])
        self.assertEqual(result['nodes'][0]['state'], 'cancelled')
        self.assertEqual(engine.run('owner', instance), result)
        self.assertEqual(engine.cancel('owner', instance), result)
        self.assertFalse((engine.state_dir / result['attempts'][1]['run_directory']).exists())

    def test_paused_parallel_cancellation_closes_unstarted_sibling(self):
        marker = self.root / 'unexpected-effect'
        def unexpected(_context, _request):
            marker.touch()
            return Outcome({})
        engine = self.engine({'a-pause': self.paused_workflow(),
                              'b-queued': workflow(adapters={name: FunctionAdapter(unexpected) for name in ('a', 'b')})},
                             parallel=True)
        instance = self.submit(engine)
        paused = engine.run('owner', instance)
        self.assertEqual([attempt['state'] for attempt in paused['attempts']], ['input_required', 'queued'])
        result = engine.cancel('owner', instance)
        self.assertEqual(result['state'], 'cancelled')
        self.assertEqual({attempt['state'] for attempt in result['attempts']}, {'cancelled'})
        self.assertEqual({node['state'] for node in result['nodes']}, {'cancelled'})
        engine.run('owner', instance)
        self.assertFalse(marker.exists())

    def test_cancellation_preserves_completed_branch_output(self):
        engine = self.engine({'a-completed': workflow(), 'b-pause': self.paused_workflow()}, parallel=True)
        instance = self.submit(engine)
        before = engine.run('owner', instance)
        self.assertEqual([attempt['state'] for attempt in before['attempts']], ['completed', 'input_required'])
        with engine.store.transaction(readonly=True) as db:
            evidence = dict(db.execute("SELECT * FROM attempts WHERE state='completed'").fetchone())
        result = engine.cancel('owner', instance)
        self.assertEqual([attempt['state'] for attempt in result['attempts']], ['completed', 'cancelled'])
        self.assertEqual(next(node for node in result['nodes'] if node['path'] == 'root/p-a-completed')['state'], 'completed')
        with engine.store.transaction(readonly=True) as db:
            self.assertEqual(dict(db.execute("SELECT * FROM attempts WHERE state='completed'").fetchone()), evidence)

    def test_final_publication_observes_concurrent_cancellation(self):
        engine = self.engine({'leaf': self.paused_workflow()}, engine_type=_CancelAtDecision)
        instance = self.submit(engine)
        self.assertEqual(engine.run('owner', instance)['state'], 'input_required')
        # Reconstruct a crash after the durable leaf receipt, before the parent
        # published its pause. There are no live workers or unrecorded effects.
        with engine.store.transaction() as db:
            db.execute("UPDATE instances SET state='running',error=NULL WHERE id=?", (instance,))
        engine.cancel_at_decision = True
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'cancelled')
        self.assertTrue(result['cancel_requested'])
        self.assertEqual(result['attempts'][0]['state'], 'cancelled')
        self.assertEqual(result['nodes'][0]['state'], 'cancelled')

    def test_running_effect_remains_unknown_and_unstarted_sibling_is_closed(self):
        marker, result_path = self.root / 'effect-receipt', self.root / 'result.json'
        def effect(context, _request):
            def invoke():
                with marker.open('w') as stream:
                    stream.write(context.invocation_id)
                    stream.flush()
                    os.fsync(stream.fileno())
                time.sleep(20)
                return Invocation({'done': True}, {'receipt': marker.name})
            return Outcome(context.invoke(invoke, input={}, reservation_microusd=1))
        engine = self.engine({'a-effect': workflow(adapters={name: FunctionAdapter(effect) for name in ('a', 'b')}),
                              'b-queued': workflow()}, parallel=True)
        instance = self.submit(engine)
        pid = os.fork()
        if pid == 0:
            try:
                result_path.write_text(json.dumps(engine.run('owner', instance)))
                os._exit(0)
            except BaseException:
                os._exit(1)
        self.children.add(pid)
        stop = time.monotonic() + 10
        while not marker.exists() and time.monotonic() < stop:
            time.sleep(.01)
        self.assertTrue(marker.exists(), 'the admitted effect never started')
        pending = engine.cancel('owner', instance)
        self.assertTrue(pending['cancel_requested'])
        self.assertNotEqual(pending['state'], 'cancelled', 'running work was prematurely declared cancelled')
        while not result_path.exists() and time.monotonic() < stop:
            time.sleep(.01)
        self.assertTrue(result_path.exists(), 'cancelled scheduler did not drain')
        _, status = os.waitpid(pid, 0)
        self.children.remove(pid)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        result = json.loads(result_path.read_text())
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual([attempt['state'] for attempt in result['attempts']], ['unknown', 'cancelled'])
        self.assertEqual(next(node for node in result['nodes'] if node['path'] == 'root')['state'], 'unknown')
        run = engine.state_dir / result['attempts'][0]['run_directory']
        self.assertTrue(inspect_run(run)['recovery_required'])
        self.assertFalse((engine.state_dir / result['attempts'][1]['run_directory']).exists())
        events = engine.events('owner', instance)
        self.assertEqual(engine.cancel('owner', instance), result)
        self.assertEqual(engine.events('owner', instance), events)

    def test_resolved_unknown_recovery_can_be_cancelled_with_consistent_nested_nodes(self):
        original_root = self.root
        for already_cancelled in (False, True):
            with self.subTest(already_cancelled=already_cancelled):
                self.root = original_root / str(already_cancelled)
                self.root.mkdir()
                marker = self.root / 'effect-receipt'
                def effect(context, _request):
                    def invoke():
                        with marker.open('a') as stream:
                            stream.write(context.invocation_id + '\n')
                            stream.flush()
                            os.fsync(stream.fileno())
                        raise RuntimeError('fixture response lost after effect')
                    return Outcome(context.invoke(invoke, input={}, reservation_microusd=1))
                engine = self.engine({'effect': workflow(adapters={
                    name: FunctionAdapter(effect) for name in ('a', 'b')})}, nested=True)
                instance = self.submit(engine)
                unknown = engine.run('owner', instance)
                self.assertEqual(unknown['state'], 'unknown')
                self.assertEqual({node['state'] for node in unknown['nodes']}, {'unknown'})
                if already_cancelled:
                    cancelled = engine.cancel('owner', instance)
                    self.assertEqual(cancelled['state'], 'unknown')
                    self.assertEqual({node['state'] for node in cancelled['nodes']}, {'unknown'})
                    events = engine.events('owner', instance)
                    self.assertEqual(engine.cancel('owner', instance), cancelled)
                    self.assertEqual(engine.events('owner', instance), events)
                run = engine.state_dir / unknown['attempts'][0]['run_directory']
                call = inspect_run(run)['invocations'][0]
                resolve_invocation(run, call['call_id'], decision='completed',
                                   output={'done': True}, evidence={'receipt': marker.name})
                reconciled = inspect_run(run)
                self.assertFalse(reconciled['recovery_required'])
                self.assertEqual(engine.recover('owner', instance)['state'], 'queued')
                result = engine.cancel('owner', instance)
                self.assertEqual(result['state'], 'cancelled')
                self.assertEqual(result['attempts'][0]['state'], 'cancelled')
                self.assertEqual({node['state'] for node in result['nodes']}, {'cancelled'})
                self.assertEqual({node['error']['code'] for node in result['nodes']}, {'cancelled'})
                self.assertEqual(inspect_run(run), reconciled)
                events = engine.events('owner', instance)
                self.assertEqual(sum(item['event'] == 'cancellation_requested' for item in events), 1)
                self.assertEqual(engine.cancel('owner', instance), result)
                self.assertEqual(engine.events('owner', instance), events)
                self.assertEqual(engine.run('owner', instance), result)
                self.assertEqual(len(marker.read_text().splitlines()), 1)


if __name__ == '__main__':
    unittest.main()
