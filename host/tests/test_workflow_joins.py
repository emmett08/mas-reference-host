"""Durable acceptance decisions stop and drain obsolete work before success."""
from __future__ import annotations

import copy
from pathlib import Path
import tempfile
import time
import unittest

from mas_host import Invocation, Outcome
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.service_contracts import ServiceError
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_evaluator import PlanEvaluator
from mas_host.workflow_plan import WorkflowPlan, bind_plan
from test_runner import FunctionAdapter
import test_runner as runner_tests
from test_workflow_collections import CollectionJournal
from test_workflow_engine_contracts import RecordedEffect, workflow
from test_workflow_plan import leaf, spec


TASK = {'kind': 'task', 'workflow': 'compute'}
ACCEPTANCE = {'id': 'quality', 'version': '1', 'condition': {'path': ['accepted'], 'equals': True}}


def parallel(mode='first-accepted', branches=None, minimum=2):
    join = {'mode': mode, 'acceptance': copy.deepcopy(ACCEPTANCE)}
    if mode == 'quorum':
        join['minimum'] = minimum
    return {'kind': 'parallel', 'branches': branches or {'a': TASK, 'b': TASK, 'c': TASK}, 'join': join}


def recover(body=None, compensate=True):
    node = {'kind': 'recover', 'body': body or TASK,
            'on_failure': {'kind': 'transform', 'fields': {'handled': {'literal': True}}}}
    if compensate:
        node['compensation'] = {'kind': 'task', 'workflow': 'compensate'}
    return node


class JoinJournal(CollectionJournal):
    def __init__(self):
        super().__init__()
        self.decisions = {}

    def join_decision(self, instance, path):
        return self.decisions.get(path)

    def decide_join(self, instance, path, winners, obsolete):
        decision = {'winners': winners, 'obsolete': obsolete}
        if path in self.decisions:
            assert self.decisions[path] == decision
        self.decisions[path] = decision
        for candidate in self.attempts:
            if any(candidate == prefix or candidate.startswith(prefix + '/') for prefix in obsolete):
                result = self.results.get(candidate, {'state': 'queued'})
                if result['state'] in {'queued', 'input_required', 'auth_required'}:
                    self.results[candidate] = {'state': 'cancelled', 'output': None}
        return decision

    def join_drain_state(self, instance, path):
        obsolete = self.decisions[path]['obsolete']
        states = {result['state'] for candidate, result in self.results.items()
                  if any(candidate == prefix or candidate.startswith(prefix + '/') for prefix in obsolete)}
        return 'waiting' if 'running' in states else 'unknown' if 'unknown' in states else 'drained'


class WorkflowJoinTests(unittest.TestCase):
    def advance(self, node, journal=None, input=None):
        journal = journal or JoinJournal()
        source = WorkflowPlan(spec(node)).spec
        ready = []
        result = PlanEvaluator({}, journal).advance('instance', 'root', source['root'],
            input or {}, source['limits'], 1000, ready)
        return result, journal, ready

    def test_policy_identity_validation_and_conservative_expansion(self):
        node = parallel('quorum')
        source = WorkflowPlan(spec(node))
        node['join']['acceptance']['version'] = '2'
        self.assertNotEqual(source.identity, WorkflowPlan(spec(node)).identity)
        self.assertEqual(bind_plan('pipeline', {'pipeline': source}, {'compute': leaf()})['max_steps'], 4)
        for mutate in (lambda value: value['join'].update(minimum=0),
                       lambda value: value['join'].update(minimum=True),
                       lambda value: value['join'].update(mode='race'),
                       lambda value: value['join']['acceptance'].update(version='unversioned'),
                       lambda value: value['join']['acceptance'].pop('id')):
            value = parallel('quorum')
            mutate(value)
            with self.assertRaises(ServiceError):
                WorkflowPlan(spec(value))
        recovery = WorkflowPlan(spec(recover()))
        self.assertEqual(bind_plan('pipeline', {'pipeline': recovery},
                                   {'compute': leaf(), 'compensate': leaf()})['max_steps'], 4)
        with self.assertRaises(ServiceError):
            bind_plan('pipeline', {'pipeline': recovery}, {'compute': leaf()})
        with self.assertRaises(ServiceError):
            WorkflowPlan(spec({**recover(), 'compensation': {'kind': 'transform', 'fields': {}}}))
        with self.assertRaises(ServiceError):
            WorkflowPlan(spec(parallel(branches={str(index): TASK for index in range(33)})))

    def test_first_acceptance_persists_choice_and_closes_pending_siblings(self):
        node = parallel()
        _, journal, ready = self.advance(node)
        self.assertEqual(len(ready), 3)
        journal.results['root/p-b'] = {'state': 'completed', 'output': {'accepted': True, 'answer': 2}}
        result, _, ready = self.advance(node, journal)
        self.assertEqual(result[0], 'completed')
        self.assertEqual(result[1]['join']['selected'], ['b'])
        self.assertEqual(result[1]['join']['acceptance'], ACCEPTANCE)
        self.assertEqual(result[1]['branches'], {'b': {'accepted': True, 'answer': 2}})
        self.assertEqual(ready, [])
        self.assertEqual(journal.results['root/p-a']['state'], 'cancelled')
        journal.results['root/p-a'] = {'state': 'completed', 'output': {'accepted': True}}
        self.assertEqual(self.advance(node, journal)[0], result)

    def test_selected_result_waits_for_running_sibling_and_cannot_hide_unknown(self):
        node, journal = parallel(), JoinJournal()
        self.advance(node, journal)
        journal.results.update({'root/p-a': {'state': 'completed', 'output': {'accepted': True}},
                                'root/p-b': {'state': 'running', 'output': None}})
        self.assertEqual(self.advance(node, journal)[0], ('waiting', None))
        journal.results['root/p-b']['state'] = 'unknown'
        self.assertEqual(self.advance(node, journal)[0], ('unknown', None))
        journal.results['root/p-b']['state'] = 'cancelled'
        self.assertEqual(self.advance(node, journal)[0][0], 'completed')
        self.assertEqual(journal.decisions['root']['winners'], ['a'])

    def test_quorum_tie_is_name_order_and_rejected_outputs_are_not_winners(self):
        journal = JoinJournal()
        for name, accepted in [('a', False), ('b', True), ('c', True)]:
            journal.results['root/p-' + name] = {'state': 'completed', 'output': {'accepted': accepted}}
        result, _, _ = self.advance(parallel('quorum'), journal)
        self.assertEqual(result[1]['join']['selected'], ['b', 'c'])
        self.assertEqual(set(result[1]['branches']), {'b', 'c'})

    def test_all_requires_exact_predicate_and_failure_waits_for_unknown(self):
        node, journal = parallel('all'), JoinJournal()
        self.advance(node, journal)
        journal.results['root/p-a'] = {'state': 'completed', 'output': {'accepted': 1}}
        journal.results['root/p-b'] = {'state': 'unknown', 'output': None}
        self.assertEqual(self.advance(node, journal)[0], ('unknown', None))
        journal.results['root/p-b']['state'] = 'failed'
        self.assertEqual(self.advance(node, journal)[0], ('failed', None))
        self.assertEqual(journal.nodes['root']['error']['code'], 'workflow_acceptance_rejected')
        self.assertEqual(journal.decisions['root']['winners'], [])

    def test_all_accepts_every_branch_and_bounded_join_rejects_oversize(self):
        for size, expected in [(1, 'completed'), (34000, 'failed')]:
            journal = JoinJournal()
            for name in ('a', 'b'):
                journal.results['root/p-' + name] = {'state': 'completed',
                    'output': {'accepted': True, 'data': 'x' * size}}
            result, _, _ = self.advance(parallel('all', {'a': TASK, 'b': TASK}), journal)
            self.assertEqual(result[0], expected)

    def test_recover_drains_failed_parallel_before_admitting_compensation(self):
        body = {'kind': 'parallel', 'branches': {'a': TASK, 'b': TASK}}
        node, journal = recover(body), JoinJournal()
        self.advance(node, journal)
        journal.results.update({'root/body/p-a': {'state': 'failed', 'output': None},
                                'root/body/p-b': {'state': 'running', 'output': None}})
        self.assertEqual(self.advance(node, journal)[0], ('waiting', None))
        self.assertNotIn('root/compensate', journal.attempts)
        journal.results['root/body/p-b']['state'] = 'unknown'
        self.assertEqual(self.advance(node, journal)[0], ('unknown', None))
        self.assertNotIn('root/compensate', journal.attempts)
        journal.results['root/body/p-b']['state'] = 'cancelled'
        result, _, ready = self.advance(node, journal)
        self.assertEqual(result[0], 'queued')
        self.assertEqual([item['path'] for item in ready], ['root/compensate'])
        journal.results['root/compensate'] = {'state': 'completed', 'output': {'request_recorded': True}}
        result, _, _ = self.advance(node, journal)
        self.assertEqual(result[0], 'completed')
        self.assertEqual(result[1]['result'], {'handled': True})
        self.assertEqual(result[1]['recovery']['compensation']['workflow'], 'compensate')
        self.assertEqual(self.advance(node, journal)[0], result)

    def test_unknown_body_never_enters_recovery_and_success_passes_through(self):
        for state in ('unknown', 'input_required', 'completed'):
            journal = JoinJournal()
            journal.results['root/body'] = {'state': state, 'output': {'accepted': True} if state == 'completed' else None}
            result, _, _ = self.advance(recover(), journal)
            self.assertEqual(result[0], state)
            self.assertEqual(journal.decisions, {})
            self.assertNotIn('root/compensate', journal.attempts)

    def test_unknown_compensation_blocks_handler_and_resumes_same_action(self):
        journal = JoinJournal()
        journal.results['root/body'] = {'state': 'failed', 'output': None}
        journal.results['root/compensate'] = {'state': 'unknown', 'output': None}
        node = recover()
        self.assertEqual(self.advance(node, journal)[0], ('unknown', None))
        self.assertNotIn('root/on_failure', journal.nodes)
        journal.results['root/compensate'] = {'state': 'completed', 'output': {'recorded': True}}
        self.assertEqual(self.advance(node, journal)[0][0], 'completed')
        self.assertEqual(set(journal.attempts), {'root/body', 'root/compensate'})

    def test_recovery_envelope_limit_precedes_compensation_allocation(self):
        journal = JoinJournal()
        journal.results['root/body'] = {'state': 'failed', 'output': None}
        result, _, ready = self.advance(recover(), journal, input={'large': 'x' * 65500})
        self.assertEqual(result, ('failed', None))
        self.assertEqual(journal.nodes['root']['error']['code'], 'workflow_recovery_input_limit')
        self.assertEqual(ready, [])
        self.assertNotIn('root/compensate', journal.attempts)


class WorkflowJoinIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def engine(self, node, workflows, *, concurrency=2):
        source = spec(node)
        source['limits'].update(max_parallel=concurrency, max_reserved_microusd=100)
        return WorkflowEngine(self.root / 'state', {'pipeline': WorkflowPlan(source)}, workflows,
            {'owner': {'pipeline'}}, mas_bin=runner_tests.RunnerIntegrationTests.mas,
            gateway_bin=runner_tests.RunnerIntegrationTests.gateway)

    @staticmethod
    def effect(adapter):
        return workflow(adapters={'a': FunctionAdapter(lambda _context, request: Outcome(request.input)), 'b': adapter})

    def test_quorum_cancels_queued_sibling_and_replay_keeps_winners(self):
        workflows = {name: self.effect(RecordedEffect(self.root / name, {'accepted': True}))
                     for name in ('a', 'b', 'c')}
        node = parallel('quorum', {name: {'kind': 'task', 'workflow': name} for name in workflows})
        engine = self.engine(node, workflows, concurrency=1)
        instance = engine.submit('owner', 'request', 'pipeline', {})['instance_id']
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['join']['selected'], ['a', 'b'])
        self.assertEqual([item['state'] for item in result['attempts']], ['completed', 'completed', 'cancelled'])
        self.assertFalse((self.root / 'c').exists())
        self.assertEqual(result['usage']['calls'], 2)
        self.assertEqual(engine.run('owner', instance), result)
        self.assertEqual(len((self.root / 'a').read_text().splitlines()), 1)

    def test_paused_candidate_does_not_block_queued_accepted_alternative(self):
        workflows = {'a': workflow(classify=lambda _outcome: 'input_required'),
                     'b': self.effect(RecordedEffect(self.root / 'accepted', {'accepted': True}))}
        node = parallel(branches={name: {'kind': 'task', 'workflow': name} for name in workflows})
        engine = self.engine(node, workflows, concurrency=1)
        instance = engine.submit('owner', 'request', 'pipeline', {})['instance_id']
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['join']['selected'], ['b'])
        self.assertEqual([attempt['state'] for attempt in result['attempts']], ['cancelled', 'completed'])

    def test_first_accepted_drains_running_unknown_and_resolution_never_repeats_effect(self):
        marker = self.root / 'slow-effect'
        def winner(_context, _request):
            stop = time.monotonic() + 10
            while not marker.exists() and time.monotonic() < stop:
                time.sleep(.01)
            if not marker.exists():
                raise RuntimeError('slow effect never began')
            return Outcome({'accepted': True})
        def slow(context, _request):
            def effect():
                marker.write_text(context.invocation_id)
                time.sleep(20)
                return Invocation({'accepted': False}, {'receipt': marker.name})
            return Outcome(context.invoke(effect, input={}, reservation_microusd=1))
        workflows = {'a': self.effect(FunctionAdapter(winner)), 'b': self.effect(FunctionAdapter(slow))}
        engine = self.engine(parallel(branches={name: {'kind': 'task', 'workflow': name}
                                               for name in workflows}), workflows)
        instance = engine.submit('owner', 'request', 'pipeline', {})['instance_id']
        unknown = engine.run('owner', instance)
        self.assertEqual(unknown['state'], 'unknown', unknown)
        self.assertIsNone(unknown['output'])
        self.assertEqual([item['state'] for item in unknown['attempts']], ['completed', 'unknown'])
        run_dir = engine.state_dir / unknown['attempts'][1]['run_directory']
        calls = inspect_run(run_dir)['invocations']
        self.assertEqual(len(calls), 1)
        resolve_invocation(run_dir, calls[0]['call_id'], decision='completed', output={'accepted': False},
                           evidence={'receipt': marker.name})
        engine.recover('owner', instance)
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['join']['selected'], ['a'])
        self.assertEqual(result['usage']['calls'], 1)
        self.assertEqual(marker.read_text(), calls[0]['call_id'])

    def test_compensation_and_handler_are_admitted_once_after_known_failure(self):
        body = {'kind': 'transform', 'fields': {'missing': {'path': ['absent'], 'type': 'object'}}}
        node = recover(body)
        node['on_failure'] = {'kind': 'task', 'workflow': 'handle'}
        workflows = {name: self.effect(RecordedEffect(self.root / name, {'action': name}))
                     for name in ('compensate', 'handle')}
        engine = self.engine(node, workflows)
        instance = engine.submit('owner', 'request', 'pipeline', {'original': 7})['instance_id']
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['result'], {'action': 'handle'})
        self.assertEqual(result['usage']['calls'], 2)
        self.assertEqual([item['path'] for item in result['attempts']], ['root/compensate', 'root/on_failure'])
        self.assertEqual(engine.run('owner', instance), result)
        self.assertEqual([len((self.root / name).read_text().splitlines()) for name in workflows], [1, 1])

    def test_failed_parallel_does_not_compensate_until_unknown_sibling_resolves(self):
        marker, compensation_receipt = self.root / 'uncertain', self.root / 'compensation'
        def fail(_context, _request):
            stop = time.monotonic() + 10
            while not marker.exists() and time.monotonic() < stop:
                time.sleep(.01)
            raise RuntimeError('known failure')
        body = {'kind': 'parallel', 'branches': {
            'a': {'kind': 'task', 'workflow': 'fail'},
            'b': {'kind': 'task', 'workflow': 'uncertain'}}}
        workflows = {'fail': self.effect(FunctionAdapter(fail)),
            'uncertain': self.effect(RecordedEffect(marker, {'done': True}, lose_reply=True)),
            'compensate': self.effect(RecordedEffect(compensation_receipt, {'action_recorded': True}))}
        engine = self.engine(recover(body), workflows)
        instance = engine.submit('owner', 'request', 'pipeline', {})['instance_id']
        unknown = engine.run('owner', instance)
        self.assertEqual(unknown['state'], 'unknown', unknown)
        self.assertFalse(compensation_receipt.exists())
        uncertain = next(attempt for attempt in unknown['attempts'] if attempt['state'] == 'unknown')
        run_dir = engine.state_dir / uncertain['run_directory']
        call = inspect_run(run_dir)['invocations'][0]
        resolve_invocation(run_dir, call['call_id'], decision='completed', output={'done': True},
                           evidence={'receipt': marker.name})
        engine.recover('owner', instance)
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'completed', result)
        self.assertEqual(result['output']['result'], {'handled': True})
        self.assertEqual(result['usage']['calls'], 2)
        self.assertEqual(len(marker.read_text().splitlines()), 1)
        self.assertEqual(len(compensation_receipt.read_text().splitlines()), 1)

    def test_compensation_uses_same_call_budget_and_cannot_expand_admission(self):
        body = {'kind': 'transform', 'fields': {'missing': {'path': ['absent'], 'type': 'object'}}}
        node = recover(body)
        node['on_failure'] = {'kind': 'task', 'workflow': 'handle'}
        source = spec(node)
        source['limits'].update(max_calls=1, max_reserved_microusd=100)
        workflows = {name: self.effect(RecordedEffect(self.root / name, {'action': name}))
                     for name in ('compensate', 'handle')}
        engine = WorkflowEngine(self.root / 'state', {'pipeline': WorkflowPlan(source)}, workflows,
            {'owner': {'pipeline'}}, mas_bin=runner_tests.RunnerIntegrationTests.mas,
            gateway_bin=runner_tests.RunnerIntegrationTests.gateway)
        instance = engine.submit('owner', 'request', 'pipeline', {})['instance_id']
        result = engine.run('owner', instance)
        self.assertEqual(result['state'], 'failed', result)
        self.assertEqual(result['usage']['calls'], 1)
        self.assertTrue((self.root / 'compensate').exists())
        self.assertFalse((self.root / 'handle').exists())


if __name__ == '__main__':
    unittest.main()
