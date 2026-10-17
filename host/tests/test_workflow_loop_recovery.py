"""Faults inside a reusable loop retain earlier effects and bounded iteration IDs."""
from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import time
import unittest

from mas_host import Invocation, Outcome
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.service_contracts import ServiceError
from mas_host.task_service import Workflow
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan
import test_runner


LOOP_PATH = 'root/p-work/sub-loop'


class _Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


class _IncrementEffect:
    """A durable fixture receipt precedes the deliberately interrupted reply."""
    def __init__(self, receipt, mode, release):
        self.receipt, self.mode, self.release = receipt, mode, release

    def handle(self, context, request):
        value = request.input['value']
        def effect():
            with self.receipt.open('ab', buffering=0) as stream:
                stream.write((json.dumps({'value': value, 'call_id': context.invocation_id}) + '\n').encode())
                os.fsync(stream.fileno())
            if value == 1 and self.mode == 'hold':
                stop = time.monotonic() + 20
                while not self.release.exists():
                    if time.monotonic() >= stop:
                        raise RuntimeError('fixture release deadline')
                    time.sleep(.01)
            if value == 1 and self.mode == 'lose_reply':
                raise RuntimeError('fixture response lost after effect')
            return Invocation({'value': value + 1}, {'receipt': self.receipt.name, 'value': value})
        return Outcome(context.invoke(effect, input={'increment': value}, reservation_microusd=3))


class _LoseIterationCompletion(WorkflowEngine):
    """Lose one composition receipt after Rust and the effect journal completed."""
    def _save_attempt(self, attempt, state, output=None, error=None):
        if attempt['path'] == LOOP_PATH + '/i1' and state == 'completed':
            try:
                descriptor = os.open(self.state_dir / 'lost-iteration-completion',
                                     os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(descriptor)
                return
        return super()._save_attempt(attempt, state, output, error)


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux workflow scheduler')
class WorkflowLoopRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_runner.RunnerIntegrationTests.setUpClass()
        cls.mas = test_runner.RunnerIntegrationTests.mas
        cls.gateway = test_runner.RunnerIntegrationTests.gateway

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.receipt = self.root / 'effects.jsonl'
        self.release = self.root / 'release'
        self.children = set()
        self.addCleanup(self.cleanup_children)

    def cleanup_children(self):
        for pid in tuple(self.children):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass

    def engine(self, *, mode='complete', iterations=3, engine_type=WorkflowEngine):
        topology = test_runner.topology(agents=('client', 'worker'), wall_seconds=60)
        effect = Workflow('loop-effect/1', topology,
                          {'client': _Echo(), 'worker': _IncrementEffect(self.receipt, mode, self.release)},
                          'client', 'worker', 'increment')
        sibling = Workflow('loop-sibling/1', topology, {'client': _Echo(), 'worker': _Echo()},
                           'client', 'worker', 'echo')
        def plan(name, root, max_steps, seconds):
            return WorkflowPlan({'format': 'mas-workflow/1', 'name': name, 'version': '1',
                'limits': {'max_steps': max_steps, 'max_parallel': 2, 'wall_seconds': seconds,
                           'max_calls': iterations, 'max_reserved_microusd': 3 * iterations},
                'root': root})
        child = plan('loop', {'kind': 'loop', 'body': {'kind': 'task', 'workflow': 'increment'},
                             'max_iterations': iterations, 'until': {'path': ['value'], 'equals': 3}},
                     1 + iterations, 45)
        parent = plan('parent', {'kind': 'parallel', 'branches': {
            'sibling': {'kind': 'task', 'workflow': 'sibling'},
            'work': {'kind': 'subworkflow', 'workflow': 'loop'}}}, 4 + iterations, 60)
        return engine_type(self.root / 'state', {'parent': parent, 'loop': child},
                           {'increment': effect, 'sibling': sibling}, {'owner': {'parent'}},
                           mas_bin=self.mas, gateway_bin=self.gateway)

    def submit(self, engine):
        return engine.submit('owner', 'request', 'parent', {'value': 0})['instance_id']

    def receipts(self):
        return [json.loads(line) for line in self.receipt.read_text().splitlines()] if self.receipt.exists() else []

    def until(self, predicate, message):
        stop = time.monotonic() + 10
        while time.monotonic() < stop:
            if predicate():
                return
            time.sleep(.01)
        self.fail(message)

    def spawn(self, engine, instance):
        result_path = self.root / 'scheduler-result.json'
        pid = os.fork()
        if pid == 0:
            pending = result_path.with_suffix('.pending')
            try:
                pending.write_text(json.dumps(engine.run('owner', instance)))
                os.replace(pending, result_path)
                os._exit(0)
            except BaseException as error:
                pending.write_text(json.dumps({'exception': type(error).__name__, 'message': str(error)}))
                os.replace(pending, result_path)
                os._exit(1)
        self.children.add(pid)
        return pid, result_path

    def finish(self, pid, result_path):
        self.until(result_path.exists, 'scheduler did not settle after cancellation')
        _, status = os.waitpid(pid, 0)
        self.children.remove(pid)
        result = json.loads(result_path.read_text())
        self.assertEqual(os.waitstatus_to_exitcode(status), 0, result)
        return result

    def wait_for_second_iteration(self, engine, instance):
        def second_started():
            status = engine.inspect('owner', instance)
            completed = {item['path'] for item in status['attempts'] if item['state'] == 'completed'}
            return (len(self.receipts()) == 2 and LOOP_PATH + '/i0' in completed
                    and 'root/p-sibling' in completed)
        self.until(second_started, 'second iteration never followed a completed iteration and sibling')
        return engine.inspect('owner', instance)

    def scopes(self, engine):
        with engine.store.transaction(readonly=True) as db:
            return [tuple(row) for row in db.execute('SELECT path,limits,deadline FROM scopes ORDER BY path')]

    def completed_checkpoints(self, engine, status):
        retained = {}
        for attempt in status['attempts']:
            if attempt['state'] == 'completed':
                run = engine.state_dir / attempt['run_directory']
                with closing(sqlite3.connect((run / 'state.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
                    retained[attempt['path']] = {
                        'journal': inspect_run(run),
                        'messages': db.execute('SELECT * FROM messages ORDER BY rowid').fetchall(),
                        'tasks': db.execute('SELECT * FROM tasks ORDER BY rowid').fetchall()}
        self.assertIn(LOOP_PATH + '/i0', retained)
        self.assertIn('root/p-sibling', retained)
        return retained

    def assert_iteration_ids_retained(self, earlier, later):
        assigned = {attempt['path']: attempt['run_directory'] for attempt in earlier['attempts']}
        final = {attempt['path']: attempt['run_directory'] for attempt in later['attempts']}
        self.assertEqual({path: final[path] for path in assigned}, assigned)
        self.assertTrue(all(attempt['revision'] == 0 for attempt in later['attempts']))

    def resolve_second(self, engine, status):
        attempt = next(item for item in status['attempts'] if item['path'] == LOOP_PATH + '/i1')
        run = engine.state_dir / attempt['run_directory']
        call = inspect_run(run)['invocations'][0]
        self.assertEqual(call['call_id'], self.receipts()[1]['call_id'])
        resolve_invocation(run, call['call_id'], decision='completed', output={'value': 2},
                           evidence={'receipt': self.receipt.name, 'observed_value': 1})
        return run

    def assert_completed_once(self, engine, instance, before, scopes, checkpoints):
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output'], {'branches': {'sibling': {'value': 0}, 'work': {'value': 3}}})
        self.assert_iteration_ids_retained(before, result)
        self.assertEqual(result['deadline'], before['deadline'])
        self.assertEqual(self.scopes(engine), scopes)
        self.assertEqual(self.completed_checkpoints(engine, before), checkpoints)
        self.assertEqual(result['usage'], {'steps': 7, 'calls': 3, 'reserved_microusd': 9})
        receipts = self.receipts()
        self.assertEqual([row['value'] for row in receipts], [0, 1, 2])
        self.assertEqual(len({row['call_id'] for row in receipts}), 3)
        self.assertEqual(engine.run('owner', instance), result)
        self.assertEqual(self.receipts(), receipts)
        return result

    def test_scheduler_sigkill_mid_loop_reconciles_same_iteration_then_joins(self):
        engine = self.engine(mode='hold')
        instance = self.submit(engine)
        pid, _ = self.spawn(engine, instance)
        before = self.wait_for_second_iteration(engine, instance)
        scopes = self.scopes(engine)
        checkpoints = self.completed_checkpoints(engine, before)
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        self.children.remove(pid)
        def unowned():
            try:
                with engine._ownership(instance):
                    return True
            except ServiceError as error:
                if error.code != 'workflow_busy':
                    raise
                return False
        self.until(unowned, 'interrupted worker or guardian retained scheduler ownership')
        unknown = engine.run('owner', instance)
        self.assertEqual(unknown['state'], 'unknown', unknown)
        self.assert_iteration_ids_retained(before, unknown)
        self.assertEqual([row['value'] for row in self.receipts()], [0, 1])
        with self.assertRaisesRegex(ServiceError, 'reconciliation required'):
            engine.recover('owner', instance)
        self.resolve_second(engine, unknown)
        self.release.touch()
        engine.recover('owner', instance)
        self.assert_completed_once(engine, instance, before, scopes, checkpoints)

    def test_uncertain_iteration_recovery_preserves_exhausted_loop_bound(self):
        engine = self.engine(mode='lose_reply', iterations=2)
        instance = self.submit(engine)
        before = engine.run('owner', instance)
        self.assertEqual(before['state'], 'unknown', before)
        self.assertEqual(next(item for item in before['attempts'] if item['path'] == LOOP_PATH + '/i0')['state'], 'completed')
        scopes = self.scopes(engine)
        checkpoints = self.completed_checkpoints(engine, before)
        self.resolve_second(engine, before)
        engine.recover('owner', instance)
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'failed', result)
        self.assertTrue(any(node['error'] == {'code': 'iteration_limit'} for node in result['nodes']))
        self.assert_iteration_ids_retained(before, result)
        self.assertEqual(len(result['attempts']), 3, 'recovery allocated an iteration beyond the bound')
        self.assertEqual(result['usage'], {'steps': 6, 'calls': 2, 'reserved_microusd': 6})
        self.assertEqual(result['deadline'], before['deadline'])
        self.assertEqual(self.scopes(engine), scopes)
        self.assertEqual(self.completed_checkpoints(engine, before), checkpoints)
        self.assertEqual([row['value'] for row in self.receipts()], [0, 1])
        self.assertEqual(engine.run('owner', instance), result)

    def test_active_loop_cancellation_preserves_unknown_and_stops_future_iterations(self):
        engine = self.engine(mode='hold')
        instance = self.submit(engine)
        pid, result_path = self.spawn(engine, instance)
        before = self.wait_for_second_iteration(engine, instance)
        scopes = self.scopes(engine)
        checkpoints = self.completed_checkpoints(engine, before)
        pending = engine.cancel('owner', instance)
        self.assertTrue(pending['cancel_requested'])
        self.assertNotEqual(pending['state'], 'cancelled')
        unknown = self.finish(pid, result_path)
        self.assertEqual(unknown['state'], 'unknown', unknown)
        self.assert_iteration_ids_retained(before, unknown)
        self.assertEqual([row['value'] for row in self.receipts()], [0, 1])
        self.assertTrue(all('/i2' not in item['path'] for item in unknown['attempts']))
        events = engine.events('owner', instance)
        self.assertEqual(engine.cancel('owner', instance), unknown)
        self.assertEqual(engine.events('owner', instance), events)
        run = self.resolve_second(engine, unknown)
        reconciled = inspect_run(run)
        self.release.touch()
        engine.recover('owner', instance)
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'cancelled', result)
        self.assert_iteration_ids_retained(before, result)
        self.assertEqual(result['deadline'], before['deadline'])
        self.assertEqual(self.scopes(engine), scopes)
        self.assertEqual(self.completed_checkpoints(engine, before), checkpoints)
        self.assertEqual(result['usage']['calls'], 2)
        self.assertEqual(result['usage']['reserved_microusd'], 6)
        self.assertEqual(inspect_run(run), reconciled)
        self.assertEqual([row['value'] for row in self.receipts()], [0, 1])
        self.assertEqual(len(result['attempts']), 3)
        self.assertEqual(next(item for item in result['attempts'] if item['path'] == LOOP_PATH + '/i0')['state'], 'completed')
        self.assertEqual(next(item for item in result['attempts'] if item['path'] == 'root/p-sibling')['state'], 'completed')
        self.assertFalse(any(node['state'] in {'queued', 'running', 'waiting', 'unknown'} for node in result['nodes']))

    def test_lost_mid_loop_completion_replays_cached_rust_result_without_repeating_effect(self):
        engine = self.engine(engine_type=_LoseIterationCompletion)
        instance = self.submit(engine)
        before = engine.run('owner', instance)
        self.assertEqual(before['state'], 'unknown', before)
        self.assertEqual([row['value'] for row in self.receipts()], [0, 1])
        second = next(item for item in before['attempts'] if item['path'] == LOOP_PATH + '/i1')
        self.assertFalse(inspect_run(engine.state_dir / second['run_directory'])['recovery_required'])
        scopes = self.scopes(engine)
        checkpoints = self.completed_checkpoints(engine, before)
        engine.recover('owner', instance)
        self.assert_completed_once(engine, instance, before, scopes, checkpoints)


if __name__ == '__main__':
    unittest.main()
