"""Deadline authority, atomic audit and identity-preserving clock regressions."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import tempfile
import unittest
from unittest import mock

from mas_host.deadlines import DeadlineService
from mas_host.execution_policy import TimingPolicy, clock_status, transition_clock
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService
from mas_host.task_store import SERVICE, SERVICE_V2, TaskStore
from mas_host.workflow_store import COMPOSITION, COMPOSITION_V2, WorkflowStore
from test_definition_versions import registered
from test_service_timing import TimedExecutor
from test_workflow_engine_contracts import specification, workflow
from mas_host.workflow_engine import WorkflowEngine
import test_runner as runner_tests


class DeadlineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.clock = [1000]
        for path in ('mas_host.service_timing.milliseconds', 'mas_host.task_service.now',
                     'mas_host.deadlines.milliseconds'):
            patch = mock.patch(path, side_effect=lambda: self.clock[0])
            patch.start(); self.addCleanup(patch.stop)
        self.workflow = registered(timing_policy=TimingPolicy(100, 4, 20, 20),
                                   continue_input=lambda previous, supplied: previous | supplied)
        self.executor = TimedExecutor(self.clock, [1000])
        self.service = DurableTaskService(self.root, {'leaf': self.workflow}, {'alice': {'leaf'}}, executor=self.executor)
        self.admin = DeadlineService(self.service, operators={'operator': {'alice'}})
        self.task = self.service.submit('alice', 'submission', 'leaf', {})
        self.args = {'request_key': 'extension', 'reason': 'Owner requested more response time',
                     'expected_revision': 0, 'limits': {'business_seconds': 200, 'pause_seconds': 40}}

    def extend(self, **changes):
        return self.admin.extend('operator', 'alice', self.task['task_id'], **(self.args | changes))

    def snapshot(self):
        with self.service.store.transaction(readonly=True) as db:
            return dict(db.execute('SELECT * FROM tasks WHERE id=?', (self.task['task_id'],)).fetchone())

    def test_extension_preserves_identity_clock_usage_and_exact_replay(self):
        self.service.run_once()
        self.clock[0] = 15000
        before = self.snapshot()
        first = self.extend()
        after = self.snapshot()
        self.assertEqual(first['revision'], 1)
        self.assertFalse(first['execution_resumed'])
        for key in ('id', 'workflow_identity', 'revision', 'state', 'input', 'result'):
            self.assertEqual(before[key], after[key])
        old, new = json.loads(before['clock']), json.loads(after['clock'])
        for key in ('created_ms', 'phase_started_ms', 'active_ms', 'worker_ms', 'policy'):
            self.assertEqual(old[key], new[key])
        second = self.extend(request_key='extension2', expected_revision=1, limits={'business_seconds': 300})
        self.assertEqual(second['revision'], 2)
        self.assertEqual(self.extend(), first)
        events = [item for item in self.service.events('alice', self.task['task_id']) if item['event'] == 'deadline_extended']
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]['data'], first)
        # The original pause would have expired; the extended allowance permits
        # a continuation without replacing the task or replenishing active time.
        self.clock[0] = 25000
        continued = self.service.update('alice', self.task['task_id'], 'continue', {})
        self.assertEqual(continued['timing']['remaining_active_ms'], 3000)
        self.assertEqual(continued['timing']['worker_ms'], 1000)

    def test_operator_and_owner_grants_are_both_required(self):
        for operator, principal in [('alice', 'alice'), ('operator', 'bob')]:
            with self.assertRaisesRegex(ServiceError, 'forbidden'):
                self.admin.extend(operator, principal, self.task['task_id'], **self.args)
        self.admin = DeadlineService(DurableTaskService(self.root, {'leaf': self.workflow},
            {'alice': set()}, executor=self.executor), operators={'operator': {'alice'}})
        with self.assertRaisesRegex(ServiceError, 'forbidden'):
            self.extend()

    def test_expired_unknown_running_cancelled_or_completed_never_revive(self):
        for state in ('running', 'unknown', 'cancelled', 'completed', 'failed'):
            with self.service.store.transaction() as db:
                db.execute('UPDATE tasks SET state=? WHERE id=?', (state, self.task['task_id']))
            with self.subTest(state=state), self.assertRaisesRegex(ServiceError, 'deadline extension not available'):
                self.extend()
        with self.service.store.transaction() as db:
            db.execute("UPDATE tasks SET state='queued' WHERE id=?", (self.task['task_id'],))
        self.clock[0] = 22000
        with self.assertRaisesRegex(ServiceError, 'deadline extension not available'):
            self.extend(limits={'queue_seconds': 100})

    def test_amendment_conflicts_and_monotonicity(self):
        self.extend()
        for changes, code in [({'limits': {'business_seconds': 300}}, 'deadline_amendment_conflict'),
                              ({'request_key': 'new'}, 'deadline_revision_conflict'),
                              ({'request_key': 'new', 'expected_revision': 1, 'limits': {'business_seconds': 100}}, 'deadline_extension_must_increase'),
                              ({'request_key': 'new', 'expected_revision': 1, 'limits': {'active_seconds': 20}}, 'invalid_deadline_amendment')]:
            with self.subTest(changes=changes), self.assertRaises(ServiceError) as error:
                self.extend(**changes)
            self.assertEqual(error.exception.code, code)

    def test_concurrent_amendments_have_one_revision_winner(self):
        def attempt(index):
            try:
                return self.extend(request_key='parallel' + str(index))
            except ServiceError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, range(2)))
        self.assertEqual(sum(type(result) is dict for result in results), 1)
        self.assertIn('deadline_revision_conflict', results)

    def test_event_failure_rolls_back_clock_and_receipt_together(self):
        before = self.snapshot()
        with mock.patch('mas_host.task_store.event', side_effect=RuntimeError('interrupted commit')):
            with self.assertRaises(RuntimeError):
                self.extend()
        self.assertEqual(self.snapshot(), before)
        with self.service.store.transaction(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM deadline_amendments').fetchone()[0], 0)
        self.assertEqual(self.extend()['revision'], 1)

    def test_archival_preserves_amendment_receipt(self):
        receipt = self.extend()
        self.service.cancel('alice', self.task['task_id'])
        with self.service.store.transaction() as db:
            task = self.service._owned(db, 'alice', self.task['task_id'])
            self.service.store.archive(db, task)
        self.assertEqual(self.extend(), receipt)

    def test_fixed_legacy_deadline_cannot_be_implicitly_migrated(self):
        with self.service.store.transaction() as db:
            db.execute('UPDATE tasks SET clock=NULL WHERE id=?', (self.task['task_id'],))
        with self.assertRaisesRegex(ServiceError, 'legacy deadline immutable'):
            self.extend()

    def test_restore_admission_gate_also_blocks_deadline_mutation(self):
        with mock.patch.object(type(self.service.operations), 'admission_enabled', new_callable=mock.PropertyMock, return_value=False):
            with self.assertRaisesRegex(ServiceError, 'audit only'):
                self.extend()


class WorkerClockTests(unittest.TestCase):
    def test_parallel_intervals_pause_and_restart_count_separate_resources(self):
        clock = TimingPolicy(100, 20, 10, 30).start(1000)
        clock = transition_clock(clock, 2000, 'running', active_branches=3)
        clock = transition_clock(clock, 6000, 'running', active_branches=2)
        clock = transition_clock(clock, 8000, 'paused')
        observed = clock_status(clock, 15000)
        self.assertEqual(observed['active_ms'], 6000)
        self.assertEqual(observed['worker_ms'], 16000)
        clock = transition_clock(clock, 16000, 'running', active_branches=1)
        observed = clock_status(json.loads(json.dumps(clock)), 19000)
        self.assertEqual(observed['active_ms'], 9000)
        self.assertEqual(observed['worker_ms'], 19000)
        self.assertTrue(observed['worker_time_complete'])

    def test_v1_migration_does_not_invent_lost_worker_multiplicity(self):
        clock = TimingPolicy(100, 20, 10, 30).start(1000)
        clock = transition_clock(clock, 2000, 'running', active_branches=3)
        clock = transition_clock(clock, 6000, 'running', active_branches=2)
        old = {key: value for key, value in clock.items()
               if key not in {'worker_ms', 'worker_time_complete', 'extensions', 'deadline_revision'}}
        old['format'] = 'mas-execution-clock/1'
        updated = transition_clock(old, 8000, 'paused')
        self.assertEqual(updated['created_ms'], 1000)
        self.assertEqual(updated['active_ms'], 6000)
        self.assertEqual(updated['worker_ms'], 8000)
        observed = clock_status(updated, 9000)
        self.assertIsNone(observed['worker_ms'])
        self.assertEqual(observed['worker_ms_lower_bound'], 8000)
        self.assertFalse(observed['worker_time_complete'])

    def test_schema_v2_migrations_add_receipts_without_rewriting_identities(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = connect(root / 'task-service.sqlite', SERVICE_V2,
                         initialise=lambda db: db.execute("INSERT INTO metadata VALUES('deployment_id','old-deployment')"))
            db.close()
            store = TaskStore(root, deployment_id='old-deployment')
            with store.transaction(readonly=True) as db:
                self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], SERVICE.version)
                self.assertEqual(db.execute('SELECT value FROM metadata').fetchone()[0], 'old-deployment')
            db = connect(root / 'composition.sqlite', COMPOSITION_V2, initialise=lambda db: None)
            db.close()
            store = WorkflowStore(root)
            with store.transaction(readonly=True) as db:
                self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], COMPOSITION.version)
                self.assertEqual(db.execute('SELECT count(*) FROM deadline_amendments').fetchone()[0], 0)


class CompositionDeadlineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def test_root_extension_is_atomic_and_scope_expiry_remains_authoritative(self):
        with tempfile.TemporaryDirectory() as folder:
            leaf = workflow(classify=lambda _: 'input_required', continue_input=lambda previous, supplied: previous | supplied)
            parent, child = specification(), specification()
            parent['root'] = {'kind': 'subworkflow', 'workflow': 'child'}
            parent['timing_policy'] = TimingPolicy(86400, 60, 600, 3600).descriptor
            child['name'] = 'child'
            child['timing_policy'] = TimingPolicy(3600, 30, 300, 60).descriptor
            engine = WorkflowEngine(folder, {'example': parent, 'child': child}, {'echo': leaf}, {'alice': {'example'}},
                                    mas_bin=runner_tests.RunnerIntegrationTests.mas, gateway_bin=runner_tests.RunnerIntegrationTests.gateway)
            admin = DeadlineService(engine, operators={'operator': {'alice'}})
            task = engine.submit('alice', 'first', 'example', {})
            paused = engine.run('alice', task['instance_id'])
            common = {'request_key': 'root-change', 'reason': 'Agreed longer review', 'expected_revision': 0,
                      'limits': {'business_seconds': 172800}}
            receipt = admin.extend('operator', 'alice', task['instance_id'], **common)
            after = engine.inspect('alice', task['instance_id'])
            self.assertGreater(after['deadline'], paused['deadline'])
            self.assertEqual(after['clock']['policy'], paused['clock']['policy'])
            self.assertEqual(after['attempts'], paused['attempts'])
            self.assertEqual(admin.extend('operator', 'alice', task['instance_id'], **common), receipt)
            with engine.store.transaction(readonly=True) as db:
                row = db.execute('SELECT path,clock FROM scopes').fetchone()
                path = row['path']
            scope_receipt = admin.extend('operator', 'alice', task['instance_id'], request_key='scope-change',
                                        reason='Longer nested review', expected_revision=0,
                                        limits={'pause_seconds': 120}, path=path)
            self.assertEqual(scope_receipt['revision'], 1)
            with engine.store.transaction() as db:
                row = db.execute('SELECT clock FROM scopes WHERE path=?', (path,)).fetchone()
                clock = json.loads(row['clock'])
                for field in ('created_ms', 'observed_ms', 'phase_started_ms'):
                    clock[field] -= 130000
                db.execute('UPDATE scopes SET clock=? WHERE path=?', (json.dumps(clock), path))
            with self.assertRaisesRegex(ServiceError, 'deadline extension not available'):
                admin.extend('operator', 'alice', task['instance_id'], request_key='too-late', reason='Late',
                             expected_revision=1, limits={'business_seconds': 200000})
