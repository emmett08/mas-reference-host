"""Ancestor limits remain effective when reusable plans are nested."""
from contextlib import ExitStack
import multiprocessing
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from mas_host import Invocation, Outcome
from mas_host.task_service import RunnerExecutor, Workflow
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


def plan(name, root, **limits):
    return WorkflowPlan({'format': 'mas-workflow/1', 'name': name, 'version': '1',
        'limits': {'max_steps': 100, 'max_parallel': 4, 'wall_seconds': 60,
                   'max_calls': 20, 'max_reserved_microusd': 100, **limits}, 'root': root})


class WorkflowScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def engine(self, child, handler):
        leaf = Workflow('leaf/1', topology(), {name: FunctionAdapter(handler) for name in ('a','b','c')}, 'a', 'b', 'work')
        parent = plan('parent', {'kind':'subworkflow','workflow':'child'})
        return WorkflowEngine(self.root/'state', {'parent':parent,'child':child}, {'leaf':leaf}, {'owner':{'parent'}},
                              mas_bin=self.mas, gateway_bin=self.gateway)

    def run_plan(self, engine):
        receipt=engine.submit('owner','once','parent',{})
        return engine.run('owner',receipt['instance_id'])

    def test_nested_call_budget_is_not_replaced_by_parent_allowance(self):
        marker=self.root/'calls'
        def handler(context, request):
            def effect():
                with marker.open('a') as stream: stream.write('effect\n')
                return Invocation({'ok':True},{'test':'count'})
            return Outcome(context.invoke(effect,input={'operation':'count'},reservation_microusd=1))
        child=plan('child',{'kind':'parallel','branches':{str(i):{'kind':'task','workflow':'leaf'} for i in range(3)}},max_calls=1)
        result=self.run_plan(self.engine(child,handler))
        self.assertNotEqual(result['state'],'completed')
        self.assertEqual(marker.read_text().splitlines(),['effect'])
        self.assertEqual(result['usage']['calls'],1)

    def test_nested_allocation_cap(self):
        def handler(context, request):
            return Outcome(context.invoke(lambda:Invocation({'ok':True},{}),input={},reservation_microusd=2))
        child=plan('child',{'kind':'task','workflow':'leaf'},max_reserved_microusd=1)
        result=self.run_plan(self.engine(child,handler))
        self.assertNotEqual(result['state'],'completed')
        self.assertEqual(result['usage']['calls'],0)

    def test_nested_concurrency_one_never_overlaps(self):
        marker=self.root/'order'
        def handler(context, request):
            with marker.open('a') as stream: stream.write('start\n')
            time.sleep(.05)
            with marker.open('a') as stream: stream.write('finish\n')
            return Outcome({})
        child=plan('child',{'kind':'parallel','branches':{str(i):{'kind':'task','workflow':'leaf'} for i in range(3)}},max_parallel=1)
        result=self.run_plan(self.engine(child,handler))
        self.assertEqual(result['state'],'completed')
        self.assertEqual(marker.read_text().splitlines(),['start','finish']*3)

    def test_nested_deadline_stops_long_adapter(self):
        child=plan('child',{'kind':'task','workflow':'leaf'},wall_seconds=1)
        started=time.monotonic()
        result=self.run_plan(self.engine(child,lambda c,r:(time.sleep(5),Outcome({}))[1]))
        self.assertEqual(result['state'],'expired')
        self.assertLess(time.monotonic()-started,3)

    def test_nested_deadline_during_bootstrap_expires_without_unknown_effect(self):
        child=plan('child',{'kind':'task','workflow':'leaf'},wall_seconds=1)
        marker=self.root/'dispatched'

        def handler(context, request):
            marker.write_text('called')
            return Outcome({})

        engine=self.engine(child,handler)
        bootstrap=RunnerExecutor._bootstrap
        entered=self.root/'bootstrap-entered'
        wall,monotonic=int(time.time()),time.monotonic()
        offset=multiprocessing.Value('d',0)

        def elapsed():
            if time.monotonic()-monotonic > 30:
                raise AssertionError('bootstrap deadline fixture did not finish')
            return offset.value

        clock=SimpleNamespace(time=lambda:wall+elapsed(),
                              monotonic=lambda:monotonic+elapsed(),sleep=time.sleep)

        def cross_deadline(executor, workflow, execution):
            path=bootstrap(executor,workflow,execution)
            entered.write_text(execution['run_id'])
            # Both scheduler and forked worker observe expiry only after the
            # real bootstrap has retained this identity. Native subprocess and
            # process-cleanup deadlines continue using their real clocks.
            offset.value=2
            return path

        with ExitStack() as patches:
            for module in ('workflow_engine','workflow_evaluator','workflow_store','workflow_allocation'):
                patches.enter_context(mock.patch('mas_host.'+module+'.time',clock))
            patches.enter_context(mock.patch.object(RunnerExecutor,'_bootstrap',cross_deadline))
            result=self.run_plan(engine)
        self.assertEqual(result['state'],'expired')
        self.assertFalse(marker.exists())
        self.assertEqual(engine.operations.health()['usage']['runs'],0)
        self.assertEqual(engine.operations.health()['usage']['calls'],0)
        with engine.store.transaction(readonly=True) as db:
            attempts=list(db.execute('SELECT state,run_directory FROM attempts'))
        self.assertEqual([row['state'] for row in attempts],['cancelled'])
        self.assertEqual(entered.read_text(),attempts[0]['run_directory'])
        self.assertTrue((engine.state_dir/attempts[0]['run_directory']/'bootstrap.json').is_file())

    def test_scope_deadline_is_not_renewed_on_inspection(self):
        child=plan('child',{'kind':'task','workflow':'leaf'})
        engine=self.engine(child,lambda c,r:Outcome({}))
        result=self.run_plan(engine)
        with engine.store.transaction(readonly=True) as db:
            before=[tuple(row) for row in db.execute('SELECT path,deadline FROM scopes')]
        engine.run('owner',result['instance_id'])
        with engine.store.transaction(readonly=True) as db:
            self.assertEqual(before,[tuple(row) for row in db.execute('SELECT path,deadline FROM scopes')])


if __name__ == '__main__':
    unittest.main()
