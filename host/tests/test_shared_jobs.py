"""Real PostgreSQL Cursor Cloud lifecycle with controlled provider responses."""
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from mas_host import RunnerError
from mas_host.cursor_cloud import CloudError, run_snapshot
from mas_host.durable_jobs import CursorCloudJobAdapter
from mas_host.jobs_cli import submit_job
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_jobs import SharedJobStore
from mas_host.service_contracts import ServiceError
import test_cursor_jobs as fixtures


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedCloudJobsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.schema = 'mas_jobs_' + uuid.uuid4().hex
        self.authority = self.authority_for('controller', 1)
        self.authority.initialise()
        for scope in ('deployment', 'principal/alice', 'principal/bob'):
            self.authority.configure_scope(scope, max_runs=20, max_calls=20, max_microusd=10_000_000)
        self.client = fixtures.FakeCloud()
        self.store = SharedJobStore(self.authority, self.root / 'cache', principal='alice')
        self.service = self.store.service(self.client)
        self.runners = []
        self.addCleanup(self.close_runners)

    def authority_for(self, controller, epoch):
        authority = SharedAuthority(binary=os.environ['MAS_SHARED_BIN'],
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace=self.schema,
            deployment='deployment', controller=controller, epoch=epoch,
            secret_key=b'fixture-secret' * 4, secret_reference='fixture/1')
        self.addCleanup(authority.close)
        return authority

    def close_runners(self):
        for runner in self.runners:
            try:
                runner.close()
            except RunnerError:
                pass  # Deliberate epoch fencing also fences old cache cleanup.

    def runner(self):
        adapter = CursorCloudJobAdapter(self.service, fixtures.SPEC)
        runner = self.authority.runner({'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 5, 'wall_seconds': 120}, {'a': adapter, 'b': adapter},
            self.root / ('run-cache-' + uuid.uuid4().hex), principal='alice',
            workflow_version='shared-cloud-test/1', supervised=False)
        adapter.host_run_dir = runner.run_dir
        self.runners.append(runner)
        return runner

    def submit(self, runner=None):
        runner = runner or self.runner()
        result = runner.request('a', 'b', 'cloud.submit', {'job_id': 'job-1'})
        return runner, result

    def successor(self):
        binding = self.authority.handover('replacement')
        self.authority = self.authority_for('replacement', binding['epoch'])
        self.store = SharedJobStore(self.authority, self.root / 'replacement-cache', principal='alice')
        self.service = self.store.service(self.client)

    def test_lifecycle_exact_ids_terminal_immutability_and_no_local_database(self):
        runner, result = self.submit()
        self.assertTrue(result.output['submission_only'])
        before = self.store.inspect('job-1')
        for _ in range(3):
            self.service.poll('job-1')
        self.assertEqual(self.store.inspect('job-1'), before)
        requested = self.service.cancel('job-1')
        self.assertTrue(requested['cancel_requested'])
        self.assertFalse(requested['remote_complete'])
        with self.assertRaises(CloudError):
            self.service.cancel('job-1')
        self.client.status = 'CANCELLED'
        terminal = self.service.poll('job-1')
        count = len(self.client.calls)
        self.client.status = 'RUNNING'
        self.assertEqual(self.service.poll('job-1'), terminal)
        self.assertEqual(self.service.cancel('job-1'), terminal)
        self.assertEqual(len(self.client.calls), count)
        self.assertEqual(sum(c[0] == 'create' for c in self.client.calls), 1)
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 1)
        self.assertFalse(list(self.root.rglob('*.sqlite')))

    def test_lost_create_receipt_handover_attach_and_resolve_original_submission(self):
        self.client.error = CloudError('lost accepted create response')
        runner = self.runner()
        with self.assertRaises(Exception):
            self.submit(runner)
        saved = self.store.status('job-1')
        self.assertEqual(saved['state'], 'launch_unknown')
        recovery = self.authority.query('recovery_snapshot', {
            'run': saved['host_run_dir'], 'principal': 'alice'})
        observation = next(item for item in recovery['remote_observations'] if item['kind'] == 'cursor_job')
        self.assertEqual(observation['key'], 'job-1')
        self.assertEqual(observation['agent_id'], saved['agent_id'])
        self.assertTrue(observation['uncertain'])
        self.assertEqual(observation['pending_operations'][0]['action'], 'submit')
        self.assertNotIn('prompt', str(observation))
        runner.close()
        self.successor()
        self.assertEqual(self.store.status('job-1'), saved)
        self.client.error = None
        with self.assertRaises(CloudError):
            self.service.submit('job-1', fixtures.SPEC, call_id=saved['call_id'], host_run_dir=runner.run_dir)
        attached = self.service.attach('job-1', self.client.run_id, evidence={'ticket': 'verified-exact-run'})
        self.assertEqual(attached['agent_id'], saved['agent_id'])
        self.assertEqual(attached['run_id'], self.client.run_id)
        self.service.resolve_submission('job-1', evidence={'ticket': 'receipt-confirmed'})
        invocation = self.authority.query('invocation', {'run': saved['host_run_dir'], 'call_id': saved['call_id']})
        self.assertEqual(invocation['state'], 'resolved')
        self.assertTrue(invocation['output']['submission_only'])
        self.assertEqual(sum(c[0] == 'create' for c in self.client.calls), 1)
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['microusd'], fixtures.SPEC.reservation_microusd)

    def test_lost_begin_acknowledgement_never_authorises_dispatch(self):
        command = self.authority.command
        def lose(operation, parameters, **options):
            result = command(operation, parameters, **options)
            if operation == 'cursor_job.begin' and parameters['action'] == 'submit':
                raise RunnerError('discarded committed begin receipt')
            return result
        runner = self.runner()
        with patch.object(self.authority, 'command', lose), self.assertRaises(Exception):
            self.submit(runner)
        saved = self.store.status('job-1')
        with self.assertRaises(CloudError):
            self.service.submit('job-1', fixtures.SPEC, call_id=saved['call_id'], host_run_dir=runner.run_dir)
        self.assertEqual(saved['state'], 'launch_unknown')
        self.assertEqual(self.client.calls, [])

    def test_stale_controller_cannot_publish_create_response_or_relaunch(self):
        create = self.client.create
        successor = []
        def handover_after_create(payload):
            result = create(payload)
            successor.append(self.authority.handover('replacement'))
            return result
        self.client.create = handover_after_create
        with self.assertRaises(Exception):
            self.submit()
        old = self.store
        self.authority = self.authority_for('replacement', successor[0]['epoch'])
        self.store = SharedJobStore(self.authority, self.root / 'replacement', principal='alice')
        self.service = self.store.service(self.client)
        with self.assertRaises(CloudError):
            old.status('job-1')
        self.assertEqual(self.store.status('job-1')['state'], 'launch_unknown')
        self.service.attach('job-1', self.client.run_id, evidence={'ticket': 'post-fence-observation'})
        self.assertEqual(sum(c[0] == 'create' for c in self.client.calls), 1)

    def test_stale_poll_retained_without_overwriting_terminal(self):
        self.submit()
        first, old = self.service._begin('job-1', 'poll')
        running = run_snapshot(self.client._run(), self.client.agent_id, self.client.run_id)
        second, _ = self.service._begin('job-1', 'poll')
        self.client.status = 'FINISHED'
        finished = run_snapshot(self.client._run(), self.client.agent_id, self.client.run_id)
        self.service._complete('job-1', second, finished, {'observed': 'newer'})
        self.service._complete('job-1', first, running, {'observed': 'older'})
        report = self.store.inspect('job-1')
        self.assertEqual(report['remote_status'], 'FINISHED')
        self.assertEqual(report['audit'][-1]['event'], 'late_observation')

    def test_lost_cancel_acknowledgement_requires_explicit_same_run_retry(self):
        self.submit()
        self.client.error = CloudError('lost cancellation reply')
        with self.assertRaises(CloudError):
            self.service.cancel('job-1')
        self.assertTrue(self.store.status('job-1')['cancel_requested'])
        self.client.error = None
        with self.assertRaises(CloudError):
            self.service.cancel('job-1')
        report = self.service.cancel('job-1', retry=True)
        self.assertFalse(report['remote_complete'])
        cancels = [c for c in self.client.calls if c[0] == 'cancel']
        self.assertEqual(len(cancels), 2)
        self.assertEqual(cancels[0], cancels[1])

    def test_shared_cli_submission_uses_native_admission_and_reuses_original_job(self):
        first = submit_job(self.store, self.client, 'job-cli', fixtures.SPEC)
        self.assertTrue(first['ok'], first)
        again = submit_job(self.store, self.client, 'job-cli', fixtures.SPEC)
        self.assertTrue(again['reused'])
        self.assertEqual(first['submission'], again['submission'])
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 1)
        self.assertFalse(list(self.root.rglob('*.sqlite')))

    def test_terminal_standalone_job_archive_keeps_identities_and_allocations(self):
        runner, _result = self.submit()
        saved = self.store.status('job-1')
        runner.close()
        request = {'kind': 'runs', 'task_id': saved['host_run_dir'], 'principal': 'alice',
            'operator': 'admin', 'reason': 'Retain terminal identities and compact payloads',
            'request_key': 'archive-cloud-run', 'older_than_seconds': 0}
        with self.assertRaises(ServiceError):
            self.authority.command('operator.archive', request)
        # A lost read-only poll remains historical evidence. Once the exact
        # remote job is terminal it cannot block payload reclamation forever.
        self.client.error = CloudError('lost read-only observation')
        with self.assertRaises(CloudError):
            self.service.poll('job-1')
        self.client.error = None
        self.client.status = 'FINISHED'
        self.service.poll('job-1')
        usage = self.authority.query('scope', {'scope': 'deployment'})
        archived = self.authority.command('operator.archive', request)['receipt']
        self.assertTrue(archived['archived'])
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'}), usage)
        self.assertEqual(self.authority.command('operator.archive', request)['receipt'], archived)
        with self.assertRaises(CloudError):
            self.store.prepare('job-1', fixtures.SPEC)
        report = self.authority.query('recovery_snapshot', {
            'run': saved['host_run_dir'], 'principal': 'alice'})
        observed = next(item for item in report['remote_observations'] if item['kind'] == 'cursor_job')
        self.assertEqual(observed['agent_id'], saved['agent_id'])
        self.assertEqual(observed['remote_id'], saved['run_id'])
        self.assertEqual(observed['remote_status'], 'FINISHED')
        self.assertFalse(observed['uncertain'])
        self.assertEqual(observed['pending_operations'][0]['action'], 'poll')

    def test_principal_spec_and_unadmitted_submission_cannot_reuse_job(self):
        saved = self.store.prepare('job-1', fixtures.SPEC)
        self.assertEqual(self.store.prepare('job-1', fixtures.SPEC), saved)
        with self.assertRaises(CloudError):
            self.store.prepare('job-1', replace(fixtures.SPEC, prompt='changed'))
        with self.assertRaises(CloudError):
            self.service.submit('job-1', fixtures.SPEC, call_id='call-forged', host_run_dir=self.root / 'run-forged')
        bob = SharedJobStore(self.authority, self.root / 'bob', principal='bob')
        with self.assertRaises(CloudError):
            bob.status('job-1')
        independent = bob.prepare('job-1', fixtures.SPEC)
        self.assertNotEqual(independent['agent_id'], saved['agent_id'])
        self.assertEqual(self.client.calls, [])


if __name__ == '__main__':
    unittest.main()
