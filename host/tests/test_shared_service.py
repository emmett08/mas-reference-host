"""Native task receipts, ownership and cache-loss recovery on actual PostgreSQL."""
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock
import uuid

from mas_host import Invocation, Outcome, RunnerError
from mas_host.service_contracts import ServiceError
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_service import create_shared_service
from mas_host.task_service import Workflow


class SharedEcho:
    def handle(self, context, request):
        return Outcome(request.input)


class SharedEffect:
    def __init__(self, effects):
        self.effects = effects

    def handle(self, context, request):
        def effect():
            self.effects.append(request.input)
            return Invocation({'answer': request.input['x'] + 1}, {'certainty': 'completed'})
        return Outcome(context.invoke(effect, input=request.input))


def paused(_outcome):
    return 'input_required'


def continued(previous, supplied):
    return supplied


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.authority = SharedAuthority(
            binary=os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared'),
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace='mas_task_' + uuid.uuid4().hex,
            deployment='deployment', controller='controller', epoch=1,
            secret_key=b'test-secret-only' * 3, secret_reference='key/1', default_reservation_microusd=1)
        self.authority.initialise()
        self.addCleanup(self.authority.close)
        self.effects = []
        self.workflow = Workflow('shared-task/1', {'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 5, 'wall_seconds': 30}, {'a': SharedEcho(), 'b': SharedEffect(self.effects)}, 'a', 'b', 'work')
        self.limits = {name: {'max_runs': 100, 'max_calls': 100, 'max_microusd': 1000}
                       for name in ('deployment', 'principal/alice', 'principal/bob')}
        self.service = self.build('cache-one')

    def build(self, cache, *, workflow=None, versions=None, authority=None, grants=None, supervised=False):
        return create_shared_service(authority or self.authority, Path(self.temp.name) / cache,
            {'work': workflow or self.workflow}, grants or {'alice': {'work'}, 'bob': {'work'}},
            workflow_versions=versions, allocation_limits=self.limits, supervised=supervised)

    def isolated_policy(self, *, quotas=None, scheduler=None, workflow=None):
        authority = SharedAuthority(binary=self.authority._binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace='mas_policy_' + uuid.uuid4().hex, deployment='deployment', controller='controller', epoch=1,
            secret_key=b'test-secret-only' * 3, secret_reference='key/1', default_reservation_microusd=1)
        authority.initialise()
        self.addCleanup(authority.close)
        service = create_shared_service(authority, Path(self.temp.name) / ('policy-' + uuid.uuid4().hex),
            {'work': workflow or self.workflow}, {'alice': {'work'}, 'bob': {'work'}},
            allocation_limits=self.limits, principal_quotas=quotas,
            scheduler_policy=scheduler, supervised=False)
        return service

    def test_principal_pending_and_retained_limits_use_shared_atomic_counts(self):
        from concurrent.futures import ThreadPoolExecutor
        service = self.isolated_policy(quotas={'alice': {'max_pending': 1, 'max_tasks': 1}})
        def submit(key):
            try:
                return service.submit('alice', key, 'work', {'x': 1})
            except ServiceError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=2) as workers:
            values = list(workers.map(submit, ['one', 'two']))
        accepted = [v for v in values if isinstance(v, dict)]
        self.assertEqual(len(accepted), 1)
        self.assertIn('principal_task_limit', values)
        self.assertEqual(service.submit('bob', 'one', 'work', {'x': 1})['state'], 'queued')
        self.assertEqual(service.submit('alice', accepted[0]['request_key'], 'work', {'x': 1})['task_id'], accepted[0]['task_id'])

    def test_native_capacity_and_round_robin_cover_independent_principals(self):
        from mas_host.scheduler_capacity import SchedulerPolicy
        service = self.isolated_policy(scheduler=SchedulerPolicy(max_workers=2, max_per_principal=1))
        a = service.submit('alice', 'a', 'work', {'x': 1})['task_id']
        a2 = service.submit('alice', 'a2', 'work', {'x': 2})['task_id']
        b = service.submit('bob', 'b', 'work', {'x': 3})['task_id']
        authority = service.authority
        first = authority.command('task.claim', {'owner': 'a-owner', 'task': a, 'workflows': ['work']})['receipt']
        self.assertIsNotNone(first)
        self.assertIsNone(authority.command('task.claim', {'owner': 'a2-owner', 'task': a2, 'workflows': ['work']})['receipt'])
        with mock.patch.object(authority, 'command', wraps=authority.command) as command:
            self.assertIsNone(service.run_once(a2))
        command.assert_not_called()
        self.assertEqual(service.pending()[0]['principal'], 'bob')
        self.assertIsNotNone(authority.command('task.claim', {'owner': 'b-owner', 'task': b, 'workflows': ['work']})['receipt'])
        self.assertEqual(authority.query('task.scheduler', {})['occupied'], 2)
        authority.command('task.complete', {'principal': 'alice', 'task': a, 'owner': 'a-owner',
            'state': 'cancelled', 'result': None, 'error': None})
        self.assertIsNotNone(authority.command('task.claim', {'owner': 'a2-owner', 'task': a2, 'workflows': ['work']})['receipt'])

    def test_resource_capacity_is_shared_across_principals(self):
        from mas_host.scheduler_capacity import SchedulerPolicy
        service = self.isolated_policy(workflow=replace(self.workflow, resource_keys=('gpu',)),
            scheduler=SchedulerPolicy(max_workers=4, max_per_principal=2, resources={'gpu': 1}))
        a = service.submit('alice', 'a', 'work', {'x': 1})['task_id']
        b = service.submit('bob', 'b', 'work', {'x': 2})['task_id']
        self.assertIsNotNone(service.authority.command('task.claim', {'owner': 'a-owner', 'task': a, 'workflows': ['work']})['receipt'])
        self.assertIsNone(service.authority.command('task.claim', {'owner': 'b-owner', 'task': b, 'workflows': ['work']})['receipt'])

    def test_receipts_and_result_survive_replaced_cache_without_effect_repeat(self):
        submitted = self.service.submit('alice', 'key', 'work', {'x': 7})
        task = submitted['task_id']
        self.assertEqual(self.service.run_once(task)['result'], {'answer': 8})
        replacement = self.build('cache-two')
        self.assertEqual(replacement.submit('alice', 'key', 'work', {'x': 7})['task_id'], task)
        self.assertEqual(replacement.result('alice', task), {'answer': 8})
        self.assertIsNone(replacement.run_once())
        self.assertEqual(self.effects, [{'x': 7}])
        events = replacement.events('alice', task)
        self.assertTrue(any(item['event'] == 'protocol' for item in events))
        self.assertTrue(all('input' not in item['data'] and 'output' not in item['data'] for item in events))
        self.assertFalse(list(Path(self.temp.name).rglob('*.sqlite')))
        with self.assertRaises(ServiceError) as denied:
            replacement.inspect('bob', task)
        self.assertEqual(denied.exception.code, 'task_not_found')

    def test_supervised_factory_executes_with_separate_lease_guardian(self):
        service = self.build('supervised-cache', supervised=True)
        task = service.submit('alice', 'supervised', 'work', {'x': 3})['task_id']
        self.assertEqual(service.run_once(task)['state'], 'completed')
        self.assertEqual(service.result('alice', task), {'answer': 4})

    def test_idle_leaf_poll_does_not_create_command_receipts(self):
        with mock.patch.object(self.authority, 'command', wraps=self.authority.command) as command:
            for _ in range(3):
                self.assertIsNone(self.service.leaves.run_once())
        command.assert_not_called()

    def test_lost_submission_and_completion_replies_reconcile_original_receipt(self):
        original = self.authority.command
        def lost_submit(operation, parameters, **kwargs):
            reply = original(operation, parameters, **kwargs)
            if operation == 'task.submit':
                raise RunnerError('injected lost committed response')
            return reply
        with mock.patch.object(self.authority, 'command', side_effect=lost_submit):
            with self.assertRaises(RunnerError):
                self.service.submit('alice', 'key', 'work', {'x': 1})
        task = self.service.submit('alice', 'key', 'work', {'x': 1})['task_id']
        def lost_complete(operation, parameters, **kwargs):
            reply = original(operation, parameters, **kwargs)
            if operation == 'task.complete':
                raise RunnerError('injected lost committed response')
            return reply
        with mock.patch.object(self.authority, 'command', side_effect=lost_complete):
            with self.assertRaises(RunnerError):
                self.service.run_once(task)
        self.assertEqual(self.service.result('alice', task), {'answer': 2})
        self.assertIsNone(self.build('cache-two').run_once(task))
        self.assertEqual(self.effects, [{'x': 1}])

    def test_queue_claim_fences_second_worker_and_stale_epoch_completion(self):
        task = self.service.submit('alice', 'key', 'work', {'x': 2})['task_id']
        first = self.authority.command('task.claim', {'owner': 'worker-one', 'task': task, 'workflows': ['work']})['receipt']
        self.assertIsNotNone(first)
        self.assertGreaterEqual(first['queue_wait_ms'], 0)
        self.assertFalse(first['started'])
        self.assertIsNone(self.authority.command('task.claim', {'owner': 'worker-two', 'task': task, 'workflows': ['work']})['receipt'])
        binding = self.authority.handover('successor')
        successor = SharedAuthority(binary=self.authority._binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace=self.authority.namespace, deployment='deployment', controller='successor', epoch=binding['epoch'],
            secret_key=b'test-secret-only' * 3, secret_reference='key/1', default_reservation_microusd=1)
        self.addCleanup(successor.close)
        with self.assertRaises(RunnerError):
            self.authority.command('task.complete', {'principal': 'alice', 'task': task,
                'owner': 'worker-one', 'state': 'completed', 'result': {}, 'error': None})
        replacement = self.build('cache-two', authority=successor)
        self.assertEqual(replacement.run_once(task)['result'], {'answer': 3})
        self.assertEqual(replacement.recovery_runs('alice', task)[0]['run_id'], first['run_id'])
        self.assertEqual(self.effects, [{'x': 2}])

    def test_revoked_queued_assignment_closes_without_dispatch(self):
        task = self.service.submit('alice', 'revoked', 'work', {'x': 9})['task_id']
        replacement = self.build('revoked-cache', grants={'alice': set(), 'bob': set()})
        finished = replacement.run_once(task)
        self.assertEqual((finished['state'], finished['error']['code']), ('failed', 'permission_revoked'))
        self.assertEqual(self.effects, [])
        self.assertNotIn('queue_wait_ms', finished)
        with self.assertRaises(ServiceError) as unavailable:
            self.service.result('alice', task)
        self.assertEqual(unavailable.exception.code, 'result_unavailable')
        with self.assertRaises(ServiceError) as denied:
            replacement.inspect('alice', task)
        self.assertEqual(denied.exception.code, 'forbidden')

    def test_revoked_started_assignment_preserves_original_run_for_reconciliation(self):
        task = self.service.submit('alice', 'revoked-started', 'work', {'x': 4})['task_id']
        original = self.authority.command
        def interrupt_completion(operation, parameters, **kwargs):
            if operation == 'task.complete':
                raise RunnerError('injected loss before task completion')
            return original(operation, parameters, **kwargs)
        with mock.patch.object(self.authority, 'command', side_effect=interrupt_completion):
            with self.assertRaises(RunnerError):
                self.service.run_once(task)
        run = self.service.recovery_runs('alice', task)[0]['run_id']
        binding = self.authority.handover('successor')
        successor = SharedAuthority(binary=self.authority._binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace=self.authority.namespace, deployment='deployment', controller='successor', epoch=binding['epoch'],
            secret_key=b'test-secret-only' * 3, secret_reference='key/1', default_reservation_microusd=1)
        self.addCleanup(successor.close)
        replacement = self.build('revoked-started-cache', authority=successor,
            grants={'alice': set(), 'bob': set()})
        claims = []
        command = successor.command
        def capture_claim(operation, parameters, **kwargs):
            response = command(operation, parameters, **kwargs)
            if operation == 'task.claim':
                claims.append(response['receipt'])
            return response
        with mock.patch.object(successor, 'command', side_effect=capture_claim):
            finished = replacement.run_once(task)
        self.assertEqual((finished['state'], finished['error']['code']),
            ('unknown', 'permission_revoked_reconciliation_required'))
        self.assertEqual(claims[0]['run_id'], run)
        self.assertTrue(claims[0]['started'])
        self.assertNotIn('queue_wait_ms', claims[0])
        self.assertEqual(self.effects, [{'x': 4}])

    def test_versioned_epoch_retirement_is_shared_and_owner_scoped(self):
        task = self.service.submit('alice', 'same', 'work', {'x': 3}, request_epoch='spring')['task_id']
        with self.assertRaises(ServiceError) as active:
            self.service.retire_request_epoch('alice', 'spring', reason='done')
        self.assertEqual(active.exception.code, 'request_epoch_in_use')
        self.service.run_once(task)
        self.service.retire_request_epoch('alice', 'spring', reason='done')
        replacement = self.build('cache-two')
        for key in ('same', 'new'):
            with self.assertRaises(ServiceError) as closed:
                replacement.submit('alice', key, 'work', {'x': 3}, request_epoch='spring')
            self.assertEqual(closed.exception.code, 'request_epoch_retired')
        self.assertEqual(replacement.inspect('alice', task)['state'], 'completed')
        self.assertNotEqual(replacement.submit('bob', 'same', 'work', {'x': 3}, request_epoch='spring')['task_id'], task)

    def test_missing_historical_implementation_blocks_execution_but_keeps_receipt(self):
        task = self.service.submit('alice', 'old', 'work', {'x': 4})['task_id']
        version2 = replace(self.workflow, version='shared-task/2')
        replacement = self.build('cache-two', workflow=version2)
        self.assertEqual(replacement.run_once(task)['state'], 'blocked')
        restored = self.build('cache-three', workflow=version2, versions={'work': (self.workflow,)})
        self.assertEqual(restored.run_once(task)['result'], {'answer': 5})
        self.assertEqual(self.effects, [{'x': 4}])

    def test_unavailable_blocked_definition_does_not_starve_fresh_work_or_write_idle_receipts(self):
        old = self.service.submit('alice', 'old-queue', 'work', {'x': 1})['task_id']
        replacement = self.build('fresh-code', workflow=replace(self.workflow, version='shared-task/2'))
        fresh = replacement.submit('alice', 'fresh-queue', 'work', {'x': 2})['task_id']
        page = self.authority.query('task.pending', {'limit': 1})
        self.assertEqual([item['task_id'] for item in page['tasks']], [old])
        later = self.authority.query('task.pending', {'limit': 1, 'after': page['next']})
        self.assertEqual([item['task_id'] for item in later['tasks']], [fresh])
        self.assertIsNone(later['next'])
        self.assertEqual(replacement.run_once()['task_id'], fresh)
        self.assertEqual(replacement.inspect('alice', old)['state'], 'blocked')
        with mock.patch.object(self.authority, 'command', wraps=self.authority.command) as command:
            self.assertIsNone(replacement.run_once())
            self.assertIsNone(replacement.run_once(old))
        command.assert_not_called()
        self.assertEqual(self.effects, [{'x': 2}])

    def test_history_replays_without_code_and_new_pin_is_not_applied_to_old_request(self):
        old = self.service.submit('alice', 'historical', 'work', {'x': 4})
        newer = self.build('newer', workflow=replace(self.workflow, version='shared-task/2'))
        self.assertEqual(newer.submit('alice', 'historical', 'work', {'x': 4}), old)
        with self.assertRaises(ServiceError) as stale:
            self.service.submit('alice', 'stale-process', 'work', {'x': 4})
        self.assertEqual(stale.exception.code, 'workflow_changed')
        with self.assertRaises(ServiceError) as conflict:
            newer.submit('alice', 'historical', 'work', {'x': 4}, expected_identity=newer.leaves.identities['work'])
        self.assertEqual(conflict.exception.code, 'workflow_identity_mismatch')
        history = create_shared_service(self.authority, Path(self.temp.name) / 'history', {},
            {'alice': {'work'}, 'bob': {'work'}}, allocation_limits=self.limits, supervised=False)
        self.assertEqual(history.submit('alice', 'historical', 'work', {'x': 4}), old)
        with self.assertRaises(ServiceError) as missing:
            history.submit('alice', 'new', 'work', {'x': 4})
        self.assertEqual(missing.exception.code, 'definition_unavailable')

    def test_invalid_pin_and_epoch_retirement_options_fail_without_new_work(self):
        for identity in (False, 12, {}, [], 'wrong'):
            with self.subTest(identity=identity), self.assertRaises(ServiceError) as mismatch:
                self.service.submit('alice', 'bad-pin', 'work', {'x': 1}, expected_identity=identity)
            self.assertEqual(mismatch.exception.code, 'workflow_identity_mismatch')
        self.assertEqual(self.service.list_tasks('alice'), [])
        for options in ({'epoch': None}, {'epoch': 'test', 'prune': 'yes'}, {'epoch': 'test', 'authoriser': 12}):
            with self.subTest(options=options), self.assertRaises(ServiceError):
                self.service.retire_request_epoch('alice', reason='closed', **options)
        retired = self.service.retire_request_epoch('alice', 'empty', reason='closed')
        self.assertEqual(self.service.retire_request_epoch('alice', 'empty', reason='closed'), retired)
        with self.assertRaises(ServiceError) as conflict:
            self.service.retire_request_epoch('alice', 'empty', reason='changed')
        self.assertEqual(conflict.exception.code, 'request_conflict')

    def test_unchanged_definition_and_scheduler_registration_is_read_only(self):
        from mas_host.shared_scheduler import configure_policy
        with mock.patch.object(self.authority, 'command', wraps=self.authority.command) as command:
            self.service.definitions.remember('workflow', 'work', self.workflow.identity,
                descriptor=self.workflow.descriptor, implementation_digest=self.workflow.verified_implementation.digest,
                implementation_manifest=self.workflow.verified_implementation.manifest, current=True)
            configure_policy(self.authority, self.service.grants)
        command.assert_not_called()

    def test_archival_releases_retained_quota_and_retired_epoch_stays_closed(self):
        service = self.isolated_policy(quotas={'alice': {'max_tasks': 1, 'max_pending': 1}})
        original = service.submit('alice', 'archived', 'work', {'x': 3}, request_epoch='release')
        task = original['task_id']
        self.assertEqual(service.run_once(task)['state'], 'completed')
        service.authority.command('operator.archive', {'kind': 'tasks', 'task_id': task,
            'principal': 'alice', 'operator': 'operator', 'reason': 'retention elapsed',
            'request_key': 'archive-one', 'older_than_seconds': 0})
        archived = service.inspect('alice', task)
        self.assertTrue(archived['archived'])
        self.assertFalse(archived['result_available'])
        self.assertIsNone(archived['result'])
        self.assertEqual(len(archived['result_digest']), 64)
        self.assertEqual(service.submit('alice', 'archived', 'work', {'x': 3}, request_epoch='release'), archived)
        with self.assertRaises(ServiceError) as missing:
            service.result('alice', task)
        self.assertEqual(missing.exception.code, 'result_unavailable')
        with self.assertRaises(ServiceError) as cursor:
            service.events_page('alice', task, after=0)
        self.assertEqual(cursor.exception.code, 'event_cursor_expired')
        legacy = service.leaves.submit('alice', 'legacy', 'work', {'x': 4}, _request_namespace='legacy')
        retired = service.retire_request_epoch('alice', 'release', reason='closed', prune=True)
        self.assertTrue(retired['prune'])
        for key in ('archived', 'new'):
            with self.assertRaises(ServiceError) as closed:
                service.submit('alice', key, 'work', {'x': 3}, request_epoch='release')
            self.assertEqual(closed.exception.code, 'request_epoch_retired')
        self.assertEqual(service.leaves.submit('alice', 'legacy', 'work', {'x': 4},
            _request_namespace='legacy')['task_id'], legacy['task_id'])
        self.assertEqual(service.inspect('alice', task)['task_id'], task)

    def test_queue_clock_expires_without_dispatch_after_cache_replacement(self):
        timed = replace(self.workflow, timing_policy={'format': 'mas-timing-policy/1',
            'business_seconds': 20, 'active_seconds': 5, 'queue_seconds': 1, 'pause_seconds': 5})
        timed_service = self.build('clock-one', workflow=timed)
        task = timed_service.submit('alice', 'timed', 'work', {'x': 5})['task_id']
        time.sleep(1.05)
        replacement = self.build('clock-two', workflow=timed)
        self.assertEqual(replacement.run_once(task)['error']['code'], 'queue_deadline')
        self.assertEqual(self.effects, [])

    def test_idle_facade_expires_paused_tasks_without_new_provider_calls(self):
        workflow = replace(self.workflow, classify=paused, continue_input=continued,
            timing_policy={'format': 'mas-timing-policy/1', 'business_seconds': 180,
                'active_seconds': 60, 'queue_seconds': 60, 'pause_seconds': 1})
        service = self.build('paused-expiry', workflow=workflow)
        task = service.submit('alice', 'pause-expiry', 'work', {'x': 2})['task_id']
        self.assertEqual(service.run_once(task)['state'], 'input_required')
        time.sleep(1.05)
        self.assertIsNone(service.run_once())
        self.assertEqual(service.inspect('alice', task)['error']['code'], 'pause_deadline')
        self.assertEqual(self.effects, [{'x': 2}])

    def test_paused_clock_and_continuation_share_original_task(self):
        workflow = replace(self.workflow, classify=paused, continue_input=continued,
            timing_policy={'format': 'mas-timing-policy/1', 'business_seconds': 180,
                'active_seconds': 60, 'queue_seconds': 60, 'pause_seconds': 60})
        service = self.build('pause-one', workflow=workflow)
        task = service.submit('alice', 'start', 'work', {'x': 8})['task_id']
        first = service.run_once(task)
        self.assertEqual(first['state'], 'input_required', first)
        active = first['timing']['active_ms']
        time.sleep(.05)
        replacement = self.build('pause-two', workflow=workflow)
        self.assertEqual(replacement.inspect('alice', task)['timing']['active_ms'], active)
        updated = replacement.update('alice', task, 'continue', {'x': 9})
        self.assertEqual((updated['task_id'], updated['revision']), (task, 1))
        self.assertEqual(replacement.run_once(task)['state'], 'input_required')
        self.assertEqual(len(replacement.recovery_runs('alice', task)), 2)
