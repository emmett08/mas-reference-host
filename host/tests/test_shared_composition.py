"""Real PostgreSQL composition, replacement controller and frontier tests."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock
import uuid

from mas_host import Invocation, Outcome, RunnerError
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_composition_service import SharedCompositionService, SharedWorkflowStore
from mas_host.task_service import Workflow


class Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


class Increment:
    def __init__(self, receipt):
        self.receipt = receipt

    def handle(self, context, request):
        def effect():
            with self.receipt.open('a') as stream:
                stream.write(json.dumps({'call_id': context.invocation_id, 'input': request.input}) + '\n')
            return Invocation({'value': request.input['value'] + 1}, {'certainty': 'completed'})
        return Outcome(context.invoke(effect, input=request.input, reservation_microusd=1))


class Barrier:
    def __init__(self, directory):
        self.directory = directory

    def handle(self, context, request):
        def effect():
            self.directory.mkdir(exist_ok=True)
            (self.directory / str(os.getpid())).write_text('started')
            until = time.monotonic() + 3
            while len(list(self.directory.iterdir())) < 2 and time.monotonic() < until:
                time.sleep(.01)
            time.sleep(.1)
            return Invocation({'peers': len(list(self.directory.iterdir()))}, {'certainty': 'completed'})
        return Outcome(context.invoke(effect, input=request.input, reservation_microusd=1))


def classify_confirmation(outcome):
    return 'completed' if outcome.output.get('confirmed') else 'input_required'


def confirm(previous, supplied):
    return {**previous, **supplied}


class ControllerDied(BaseException):
    pass


def plan(root, name='sequence'):
    return {'format': 'mas-workflow/1', 'name': name, 'version': '1',
            'limits': {'max_steps': 100, 'max_parallel': 2, 'wall_seconds': 120,
                       'max_calls': 20, 'max_reserved_microusd': 100}, 'root': root}


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedCompositionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.receipt = self.directory / 'effects.txt'
        self.config = {'binary': os.environ.get('MAS_SHARED_BIN') or str(
            Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared'),
            'dsn': os.environ['MAS_TEST_POSTGRES_DSN'], 'namespace': 'mas_composition_' + uuid.uuid4().hex,
            'deployment': 'deployment', 'controller': 'controller', 'epoch': 1,
            'secret_key': b'composition-fixture-key' * 2, 'secret_reference': 'test-key/1'}
        self.authority = SharedAuthority(**self.config)
        self.authority.initialise()
        for scope in ('deployment', 'principal/alice'):
            self.authority.configure_scope(scope, max_runs=100, max_calls=100, max_microusd=1000)
        self.leaf = Workflow('1', {'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 5, 'wall_seconds': 120}, {'a': Echo(), 'b': Increment(self.receipt)}, 'a', 'b', 'work')
        self.plans = {'sequence': plan({'kind': 'sequence', 'steps': [{'kind': 'task', 'workflow': 'step'}] * 3})}

    def service(self, authority=None, cache='cache', plans=None, **options):
        return SharedCompositionService(authority or self.authority, self.directory / cache,
            plans or self.plans, {'step': self.leaf}, {'alice': set(plans or self.plans)}, supervised=False, **options)

    def effects(self):
        return [json.loads(line) for line in self.receipt.read_text().splitlines()] if self.receipt.exists() else []

    def replacement(self):
        binding = self.authority._request('handover', next_controller='replacement')
        return SharedAuthority(**{**self.config, 'controller': binding['controller'], 'epoch': binding['epoch']})

    def test_sequence_shared_frontier_and_nested_ancestor_allocation(self):
        nested = {'child': plan({'kind': 'task', 'workflow': 'step'}, 'child'),
                  'sequence': plan({'kind': 'sequence', 'steps': [
                      {'kind': 'subworkflow', 'workflow': 'child'}, {'kind': 'task', 'workflow': 'step'}]})}
        service = self.service(plans=nested)
        accepted = service.submit('alice', 'nested', 'sequence', {'value': 0})
        result = service.run_once(accepted['task_id'])
        self.assertEqual((result['state'], result['result']), ('completed', {'value': 2}))
        self.assertEqual(service.result('alice', accepted['task_id']), {'value': 2})
        self.assertEqual(self.authority.query('composition.expiring', {})['instances'], [])
        graph = service._graph('alice', accepted['task_id'])
        structural = self.authority.query('operator.graph', {'principal': 'alice',
            'task_id': accepted['task_id'], 'section': 'attempts', 'offset': 0, 'limit': 1})
        self.assertEqual(len(structural['items']), 1)
        self.assertNotIn('input', structural['items'][0])
        self.assertNotIn('output', structural['items'][0])
        self.assertEqual(len(graph['attempts'][0]['ancestors']), 2)
        self.assertEqual(len(graph['attempts'][1]['ancestors']), 1)
        for attempt in graph['attempts']:
            for ancestor in attempt['ancestors']:
                usage = self.authority.query('scope', {'scope': ancestor})
                self.assertGreaterEqual(usage['calls'], 1)
        self.assertEqual(len(self.effects()), 2)
        protocol = [event['data'] for event in service.events('alice', accepted['task_id']) if event['event'] == 'protocol']
        self.assertTrue(protocol)
        self.assertEqual(len(protocol), len({(event['run_id'], event['message_id']) for event in protocol}))
        self.assertTrue(all(set(event) == {'run_id', 'message_id', 'sender', 'recipient', 'kind', 'task_id', 'revision'} for event in protocol))
        self.assertFalse(list(self.directory.rglob('*.sqlite')))
        self.assertEqual(service.submit('alice', 'nested', 'sequence', {'value': 0})['task_id'], accepted['task_id'])

    def test_native_submission_rechecks_alias_after_stale_controller_read(self):
        from mas_host.service_contracts import ServiceError
        original = self.service()
        accepted = original.submit('alice', 'old-alias', 'sequence', {'value': 0})
        changed = json.loads(json.dumps(self.plans))
        changed['sequence']['version'] = '2'
        self.service(plans=changed)
        with mock.patch.object(original.definitions, 'current', return_value=accepted['workflow_identity']):
            with self.assertRaises((ServiceError, RunnerError)):
                original.submit('alice', 'stale-new-request', 'sequence', {'value': 0})
        self.assertEqual(original.submit('alice', 'old-alias', 'sequence', {'value': 0})['task_id'], accepted['task_id'])

    def test_capacity_wait_polling_preserves_receipts_until_slot_release(self):
        from mas_host.scheduler_capacity import SchedulerPolicy
        from mas_host.shared_scheduler import configure_policy
        configure_policy(self.authority, {'alice': {'sequence'}},
                         scheduler_policy=SchedulerPolicy(max_workers=1, max_per_principal=1))
        service = self.service()
        first = service.submit('alice', 'held-slot', 'sequence', {'value': 0})['task_id']
        second = service.submit('alice', 'waiting-slot', 'sequence', {'value': 10})['task_id']
        item = service._frontier('alice', first)
        with service._claim(item) as owner:
            store = SharedWorkflowStore(self.authority, owner, {'step': self.leaf})
            attempt = store.attempt(first, 'root/s0', 'step', {'value': 0}, item['deadline'])
            store.command('start_attempt', first, path='root/s0', run_id=attempt['run_id'])
            self.assertEqual(service.run_once(second)['state'], 'queued')
            before = self.authority.query('command_capacity.status', {})
            for _ in range(3):
                self.assertIsNone(service.run_once(second))
            self.assertEqual(self.authority.query('command_capacity.status', {}), before)
            store.command('save_attempt', first, path='root/s0', run_id=attempt['run_id'],
                          state='cancelled', output=None, error=None)
            store.command('finish', first, state='cancelled', output=None, error=None)
        self.assertEqual(service.run_once(second)['state'], 'completed')
        self.assertEqual(len(self.effects()), 3)

    def test_archive_compacts_payloads_preserving_identity_and_cumulative_allocation(self):
        from mas_host.service_contracts import ServiceError
        service = self.service(max_instances=1)
        first = service.submit('alice', 'archive-original', 'sequence', {'value': 0})
        task = first['task_id']
        service.run_once(task)
        prior = service._graph('alice', task)
        runs = service.recovery_runs('alice', task)
        scopes = self.authority.query('composition.scopes', {'instance': task})
        archive = {'kind': 'workflows', 'principal': 'alice', 'task_id': task,
            'operator': 'operator', 'reason': 'settled fixture history',
            'request_key': 'archive-fixture', 'older_than_seconds': 0}
        response = self.authority.command('operator.archive', archive)['receipt']
        self.assertEqual(self.authority.command('operator.archive', archive)['receipt'], response)
        saved = service.inspect('alice', task)
        self.assertTrue(saved['archived'])
        self.assertFalse(saved['result_available'])
        self.assertEqual(saved['progress']['nodes'], len(prior['nodes']))
        self.assertEqual(service.recovery_runs('alice', task), runs)
        retained_scopes = self.authority.query('composition.scopes', {'instance': task})
        self.assertEqual([{k: v for k, v in row.items() if k != 'state'} for row in retained_scopes],
                         [{k: v for k, v in row.items() if k != 'state'} for row in scopes])
        self.assertEqual(service._graph('alice', task)['nodes'], [])
        self.assertTrue(service.submit('alice', 'archive-original', 'sequence', {'value': 0})['archived'])
        with self.assertRaises(ServiceError):
            service.submit('alice', 'archive-original', 'sequence', {'value': 9})
        with self.assertRaises(ServiceError) as error:
            service.result('alice', task)
        self.assertEqual(error.exception.code, 'result_unavailable')
        with self.assertRaises((ServiceError, RunnerError)):
            service.events('alice', task)
        audit = service.events('alice', task, saved['event_floor'])
        self.assertEqual([event['event'] for event in audit], ['archived'])
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 3)
        next_task = service.submit('alice', 'after-archive', 'sequence', {'value': 10})
        self.assertNotEqual(next_task['task_id'], task)

    def test_unknown_child_cannot_be_hidden_archived_or_lose_its_definition(self):
        from mas_host.service_contracts import ServiceError
        service = self.service()
        task = service.submit('alice', 'uncertain-child', 'sequence', {'value': 0})['task_id']
        item = service._frontier('alice', task)
        with service._claim(item) as owner:
            store = SharedWorkflowStore(self.authority, owner, {'step': self.leaf})
            attempt = store.attempt(task, 'root/0', 'step', {'value': 0}, item['deadline'])
            store.command('save_attempt', task, path='root/0', run_id=attempt['run_id'],
                          state='unknown', output=None, error={'code': 'reconciliation_required'})
            with self.assertRaises((ServiceError, RunnerError)):
                store.command('finish', task, state='completed', output={}, error=None)
            store.command('finish', task, state='unknown', output=None, error=None)
        changed = json.loads(json.dumps(self.plans))
        changed['sequence']['version'] = '2'
        successor = self.service(plans=changed)
        self.assertNotEqual(successor.bindings['sequence']['identity'], item['identity'])
        with self.assertRaises((ServiceError, RunnerError)):
            successor.retire_definition('sequence', item['identity'])
        with self.assertRaises((ServiceError, RunnerError)):
            self.authority.command('operator.archive', {'kind': 'workflows',
                'principal': 'alice', 'task_id': task, 'operator': 'operator',
                'reason': 'must keep uncertain child', 'request_key': 'blocked-archive',
                'older_than_seconds': 0})
        self.assertFalse(successor.inspect('alice', task).get('archived', False))

    def test_replacement_replays_original_leaf_after_completion_reply_lost(self):
        service = self.service()
        task = service.submit('alice', 'interruption', 'sequence', {'value': 0})['task_id']
        original = SharedWorkflowStore.command
        def fail_before_publication(store, operation, instance, **kwargs):
            if operation == 'save_attempt' and kwargs['state'] == 'completed':
                raise ControllerDied()
            return original(store, operation, instance, **kwargs)
        from mas_host.workflow_evaluator import PlanEvaluator
        item = service._graph('alice', task)
        with service._claim(item) as owner:
            store = SharedWorkflowStore(self.authority, owner, {'step': self.leaf})
            ready = []
            PlanEvaluator(service.plans, store).advance(task, 'root', self.plans['sequence']['root'],
                item['input'], item['limits'], item['deadline'], ready)
            attempt = store.command('start_attempt', task, path=ready[0]['path'], run_id=ready[0]['run_id'])
            with mock.patch.object(SharedWorkflowStore, 'command', fail_before_publication):
                with self.assertRaises(ControllerDied):
                    service._execute(store, item, attempt)
        retained = service._graph('alice', task)['attempts'][0]
        self.assertEqual(retained['state'], 'running')
        self.assertEqual(len(self.effects()), 1)
        replacement = self.replacement()
        with self.assertRaises(RunnerError):
            service.run('alice', task)
        resumed = self.service(replacement, 'replacement-cache')
        result = resumed.run('alice', task)
        self.assertEqual(result['result'], {'value': 3})
        graph = resumed._graph('alice', task)
        self.assertEqual(graph['attempts'][0]['run_id'], retained['run_id'])
        self.assertEqual(len(self.effects()), 3)
        frontier = replacement.query('composition.frontier', {'instance': task})
        self.assertEqual(frontier['attempts'], [])
        self.assertNotIn('nodes', frontier)
        self.assertEqual(frontier['progress']['nodes'], len(graph['nodes']))
        self.assertEqual(frontier['progress']['completed_nodes'], len(graph['nodes']))
        cursor = replacement.query('composition.progress', {'instance': task, 'path': 'root'})
        self.assertEqual(cursor['next_index'], 3)
        self.assertEqual(replacement.query('scope', {'scope': 'deployment'})['calls'], 3)
        self.assertFalse(list(self.directory.rglob('*.sqlite')))

    def test_lost_admission_reply_stays_unknown_and_retains_charges(self):
        service = self.service()
        task = service.submit('alice', 'uncertain', 'sequence', {'value': 0})['task_id']
        original = self.authority._request
        def lose(operation, **kwargs):
            result = original(operation, **kwargs)
            if operation == 'command' and kwargs.get('command') == 'admit_invocation':
                raise RunnerError('injected lost reply')
            return result
        with mock.patch.object(self.authority, '_request', side_effect=lose):
            self.assertEqual(service.run('alice', task)['state'], 'unknown')
        self.assertEqual(self.effects(), [])
        replacement = self.service(self.replacement(), 'replacement-cache')
        self.assertEqual(replacement.run('alice', task)['state'], 'unknown')
        with self.assertRaises(RunnerError):
            replacement.recover('alice', task)
        self.assertEqual(len(replacement._graph('alice', task)['attempts']), 1)
        self.assertEqual(replacement.authority.query('scope', {'scope': 'deployment'})['calls'], 1)

    def test_checkpoint_cannot_skip_unfinished_child_and_owner_is_fenced(self):
        service = self.service()
        task = service.submit('alice', 'prefix', 'sequence', {'value': 0})['task_id']
        item = service._graph('alice', task)
        with service._claim(item) as owner:
            store = SharedWorkflowStore(self.authority, owner, {'step': self.leaf})
            store.node(task, 'root', 'sequence', {'value': 0}, self.plans['sequence']['limits'])
            store.node(task, 'root/s0', 'task', {'value': 0}, self.plans['sequence']['limits'])
            with self.assertRaises(RunnerError):
                store.checkpoint(task, 'root', 'sequence', 1, {'value': 1})
            with self.assertRaises(RunnerError):
                self.authority.command('composition.claim', {'instance': task, 'owner': 'other-owner'})
            store.finish_node(task, 'root/s0', 'completed', {'value': 1})
            store.checkpoint(task, 'root', 'sequence', 1, {'value': 1})
            self.assertEqual(store.progress(task, 'root', 'sequence')['next_index'], 1)

    def test_principal_isolation_and_native_request_conflict(self):
        service = self.service()
        task = service.submit('alice', 'request', 'sequence', {'value': 0})['task_id']
        with self.assertRaises(RunnerError):
            self.authority.query('composition.graph', {'instance': task, 'principal': 'mallory'})
        with self.assertRaises(Exception):
            service.submit('alice', 'request', 'sequence', {'value': 10})
        service.cancel('alice', task)
        self.assertEqual(service.run('alice', task)['state'], 'cancelled')
        self.assertEqual(self.effects(), [])

    def test_parallel_scopes_share_elapsed_clock_and_sum_worker_time(self):
        self.leaf = Workflow('barrier/1', self.leaf.topology,
            {'a': Echo(), 'b': Barrier(self.directory / 'barrier')}, 'a', 'b', 'work')
        parallel = plan({'kind': 'parallel', 'branches': {
            'first': {'kind': 'task', 'workflow': 'step'}, 'second': {'kind': 'task', 'workflow': 'step'}}})
        parallel['timing_policy'] = {'format': 'mas-timing-policy/1', 'business_seconds': 120,
            'active_seconds': 30, 'queue_seconds': 30, 'pause_seconds': 30}
        service = self.service(plans={'sequence': parallel})
        task = service.submit('alice', 'parallel', 'sequence', {})['task_id']
        result = service.run('alice', task)
        self.assertEqual(result['state'], 'completed')
        self.assertEqual([v['peers'] for v in result['result']['branches'].values()], [2, 2])
        self.assertGreater(result['timing']['worker_ms'], result['timing']['active_ms'])
        self.assertEqual(result['timing']['phase'], 'terminal')

    def test_shared_extension_preserves_creation_and_exact_request_replay(self):
        from mas_host.shared_deadlines import SharedDeadlineService
        from mas_host.service_contracts import ServiceError
        timed = dict(self.plans['sequence'], timing_policy={'format': 'mas-timing-policy/1',
            'business_seconds': 60, 'active_seconds': 30, 'queue_seconds': 30, 'pause_seconds': 30})
        service = self.service(plans={'sequence': timed})
        task = service.submit('alice', 'extension', 'sequence', {'value': 0})['task_id']
        original = service._graph('alice', task)['clock']
        admin = SharedDeadlineService(service, operators={'operator': {'alice'}})
        arguments = dict(request_key='extend/1', reason='operator reviewed wait', expected_revision=0,
                         limits={'business_seconds': 120})
        receipt = admin.extend('operator', 'alice', task, **arguments)
        self.assertFalse(receipt['execution_resumed'])
        self.assertEqual(admin.extend('operator', 'alice', task, **arguments), receipt)
        clock = service._graph('alice', task)['clock']
        self.assertEqual(clock['created_ms'], original['created_ms'])
        self.assertEqual(clock['policy'], original['policy'])
        self.assertEqual(clock['extensions'], {'business_seconds': 120})
        with self.assertRaises(ServiceError):
            admin.extend('alice', 'alice', task, **arguments)
        with self.assertRaises(RunnerError):
            admin.extend('operator', 'alice', task, **dict(arguments, reason='changed'))
        events = service.events('alice', task)
        self.assertEqual(sum(e['event'] == 'deadline_extended' for e in events), 1)
        service.run('alice', task)
        self.assertEqual(admin.extend('operator', 'alice', task, **arguments), receipt)

    def test_continuation_preserves_history_and_idempotent_revision(self):
        self.leaf = Workflow('confirmation/1', self.leaf.topology, {'a': Echo(), 'b': Echo()},
            'a', 'b', 'confirm', classify=classify_confirmation, continue_input=confirm)
        service = self.service()
        task = service.submit('alice', 'paused', 'sequence', {'value': 0})['task_id']
        self.assertEqual(service.run('alice', task)['state'], 'input_required')
        first = service.inspect_task('alice', task, 'root/s0', 0)
        request = {'path': 'root/s0', 'input': {'confirmed': True}}
        updated = service.update('alice', task, 'continue/1', request)
        self.assertEqual(service.update('alice', task, 'continue/1', request)['revision'], updated['revision'])
        self.assertEqual(service.run('alice', task)['state'], 'completed')
        current = service.inspect_task('alice', task, 'root/s0')
        self.assertEqual(current['revision'], 1)
        self.assertNotEqual(first['run_id'], current['run_id'])
        self.assertEqual(service.inspect_task('alice', task, 'root/s0', 0)['state'], 'input_required')
        self.authority.command('operator.archive', {'kind': 'workflows', 'principal': 'alice',
            'task_id': task, 'operator': 'operator', 'reason': 'all revisions settled',
            'request_key': 'archive-continuation', 'older_than_seconds': 0})
        self.assertTrue(service.update('alice', task, 'continue/1', request)['archived'])
        attempt = self.authority.query('composition.attempt', {'instance': task, 'path': 'root/s0'})
        self.assertEqual(attempt['input'], {})
        self.assertIsNone(attempt['output'])
        self.assertEqual(len(attempt['input_digest']), 64)
