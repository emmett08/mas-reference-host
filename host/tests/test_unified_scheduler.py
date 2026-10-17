"""One lifecycle namespace and lifetime-bounded admission across execution kinds."""
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mas_host.operations import archive_workflows
from mas_host.scheduler_capacity import SchedulerCapacity, SchedulerPolicy
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult
from mas_host.unified_service import UnifiedTaskService
from test_definition_versions import registered, source, Executor
from test_workflow_engine_contracts import workflow, specification
import test_runner as runner_tests


class CapacityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_global_principal_workflow_and_resource_limits_share_one_authority(self):
        capacity = SchedulerCapacity(self.root, SchedulerPolicy(3, 2, {'slow': 1}, {'gpu': 1}))
        with capacity.acquire('one', 'alice', 'slow', resources=('gpu',)):
            self.assertIsNone(capacity.acquire('same-workflow', 'bob', 'slow'))
            self.assertIsNone(capacity.acquire('same-device', 'bob', 'fast', resources=('gpu',)))
            with capacity.acquire('two', 'alice', 'fast'):
                self.assertIsNone(capacity.acquire('principal-full', 'alice', 'fast'))
                with capacity.acquire('three', 'bob', 'fast'):
                    self.assertIsNone(capacity.acquire('global-full', 'carol', 'fast'))
        with capacity.acquire('reused', 'carol', 'slow', resources=('gpu',)):
            pass

    def test_inherited_lock_survives_original_owner_close_without_expiry(self):
        capacity = SchedulerCapacity(self.root, SchedulerPolicy(1, 1))
        lease = capacity.acquire('original', 'alice', 'leaf')
        ready_read, ready_write = os.pipe()
        release_read, release_write = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(ready_read); os.close(release_write)
                os.write(ready_write, b'1')
                os.read(release_read, 1)
            finally:
                os._exit(0)
        os.close(ready_write); os.close(release_read)
        try:
            self.assertEqual(os.read(ready_read, 1), b'1')
            lease.close()
            other = SchedulerCapacity(self.root)
            self.assertIsNone(other.acquire('replacement', 'bob', 'leaf'))
        finally:
            os.write(release_write, b'1')
            os.close(release_write); os.close(ready_read)
            os.waitpid(pid, 0)
            lease.close()
        with capacity.acquire('replacement', 'bob', 'leaf'):
            pass

    def test_principal_round_robin_retains_fifo_within_each_owner(self):
        capacity = SchedulerCapacity(self.root)
        with capacity.acquire('last', 'alice', 'leaf'):
            rows = [{'principal': 'alice', 'sequence': 1}, {'principal': 'bob', 'sequence': 2},
                    {'principal': 'alice', 'sequence': 3}, {'principal': 'bob', 'sequence': 4}]
            self.assertEqual([row['sequence'] for row in capacity.fair_order(rows)], [2, 1, 4, 3])

    def test_service_can_execute_unrelated_task_while_one_is_owned(self):
        executor = Executor()
        service = DurableTaskService(self.root, {'leaf': registered()}, {'alice': {'leaf'}, 'bob': {'leaf'}},
                                     executor=executor)
        first = service.submit('alice', 'first', 'leaf', {})
        second = service.submit('bob', 'second', 'leaf', {})
        original = executor.execute
        called = []
        def nested(workflow, execution, **kwargs):
            if not called:
                called.append(True)
                self.assertEqual(service.run_once(second['task_id'])['task_id'], second['task_id'])
                with self.assertRaisesRegex(ServiceError, 'worker busy'):
                    service.run_once(first['task_id'])
            return original(workflow, execution, **kwargs)
        executor.execute = nested
        self.assertEqual(service.run_once(first['task_id'])['state'], 'completed')
        self.assertEqual(len(executor.calls), 2)

    def test_live_progress_is_visible_before_completion_and_replay_deduplicates(self):
        executor = Executor()
        service = DurableTaskService(self.root, {'leaf': registered()}, {'alice': {'leaf'}}, executor=executor)
        task = service.submit('alice', 'first', 'leaf', {})
        event = {'sender': 'a', 'recipient': 'b', 'kind': 'request', 'message_id': 'message-1', 'task_id': 'protocol-task'}
        observations = []
        def execute(_workflow, execution, **_kwargs):
            execution['event_sink'](event)
            observations.append(service.events('alice', task['task_id']))
            if len(observations) == 1:
                raise RuntimeError('worker lost after durable progress')
            execution['event_sink'](event)
            return ExecutionResult('completed', {}, events=(event,))
        executor.execute = execute
        with self.assertRaises(RuntimeError):
            service.run_once()
        self.assertEqual([item['event'] for item in observations[0]], ['submitted', 'running', 'protocol'])
        self.assertEqual(service.run_once()['state'], 'completed')
        progress = [item for item in service.events('alice', task['task_id']) if item['event'] == 'protocol']
        self.assertEqual(len(progress), 1)
        self.assertEqual(progress[0]['data']['revision'], 0)


class UnifiedServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def service(self, **kwargs):
        return UnifiedTaskService(self.root, {'echo': workflow()}, {'example': specification()},
            {'alice': {'echo', 'example'}, 'bob': {'echo'}},
            mas_bin=runner_tests.RunnerIntegrationTests.mas, gateway_bin=runner_tests.RunnerIntegrationTests.gateway, **kwargs)

    def test_one_namespace_pins_discovered_identity_and_pages_both_kinds(self):
        service = self.service()
        registered = {item['name']: item for item in service.discover('alice')['registrations']}
        leaf = service.submit('alice', 'leaf', 'echo', {'value': 1}, metadata={'tag': 'included'},
                              expected_identity=registered['echo']['identity'])
        plan = service.submit('alice', 'plan', 'example', {'value': 2}, metadata={'tag': 'included'})
        for name, key in [('example', 'leaf'), ('echo', 'plan')]:
            with self.assertRaisesRegex(ServiceError, 'request conflict'):
                service.submit('alice', key, name, {})
        with self.assertRaises(ServiceError) as error:
            service.submit('alice', 'wrong-version', 'echo', {}, expected_identity='0'*64)
        self.assertEqual(error.exception.code, 'workflow_identity_mismatch')
        page = service.list_task_page('alice', page_size=1, metadata={'tag': 'included'})
        next_page = service.list_task_page('alice', page_size=1, metadata={'tag': 'included'}, after=page['next'])
        self.assertEqual(page['total_size'], 2)
        self.assertEqual({page['tasks'][0]['task_id'], next_page['tasks'][0]['task_id']}, {leaf['task_id'], plan['task_id']})
        self.assertEqual(service.list_tasks('bob'), [])
        self.assertEqual(len(service.pending()), 2)

    def test_composition_executes_through_same_service_and_archives_remain_visible(self):
        service = self.service()
        plan = service.submit('alice', 'plan', 'example', {'value': 2}, metadata={'tag': 'old'})
        self.assertEqual(service.run_once(plan['task_id'])['state'], 'completed')
        self.assertEqual(service.result('alice', plan['task_id']), {'value': 2})
        self.assertEqual(service.inspect_task('alice', plan['task_id'], 'root')['state'], 'completed')
        page = service.events_page('alice', plan['task_id'], limit=1)
        self.assertTrue(page['has_more'])
        self.assertEqual(len(page['events']), 1)
        archived = archive_workflows(self.root, 0, True)
        self.assertTrue(archived['instances'][0]['archived'])
        historical = UnifiedTaskService(self.root, {}, {}, {'alice': {'echo', 'example'}}, executor=Executor())
        receipt = historical.list_tasks('alice')[0]
        self.assertTrue(receipt['archived'])
        self.assertEqual(receipt['metadata'], {'tag': 'old'})
        self.assertEqual(historical.submit('alice', 'plan', 'example', {'value': 2}, metadata={'tag': 'old'})['task_id'], plan['task_id'])
        with self.assertRaises(ServiceError):
            historical.result('alice', plan['task_id'])


if __name__ == '__main__':
    unittest.main()
