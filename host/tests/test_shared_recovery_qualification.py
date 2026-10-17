"""Populated PostgreSQL recovery rehearsal; external effects are bounded doubles.

The measured interval includes sealing, native dump/restore, external object
restoration, verification, activation and a fresh admitted task to completion.
It is a controlled-cutover fixture, not a database replication, production
fencing or availability measurement.
"""
import base64
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import time
import unittest
from unittest import mock

from mas_host import RunnerError
from mas_host.artifacts import ArtifactError, ArtifactPolicy, ArtifactRef
from mas_host.circuit_breaker import CircuitPolicy
from mas_host.cursor_cloud import CloudError
from mas_host.distributed import remote_binding, WORKER_PROTOCOL
from mas_host.durable_jobs import CursorCloudJobAdapter
from mas_host.event_delivery import WebhookTarget
from mas_host.peer_registry import PeerRegistration
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.service_contracts import ServiceError
from mas_host.shared_artifact_transfer import SharedArtifactScope, SharedArtifactTransferService
from mas_host.shared_artifacts import SharedArtifactStore
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_composition_service import SharedCompositionService
from mas_host.shared_control import SharedWorkerController
from mas_host.shared_events import SharedEventDelivery
from mas_host.shared_jobs import SharedJobStore
from mas_host.shared_remote import SharedCircuitBreaker, SharedPeerRegistry, SharedRemoteTaskService
from mas_host.shared_service import SharedTaskFacade
from mas_host.shared_task_service import SharedTaskService
from mas_host.task_service import Workflow
import test_cursor_jobs as cloud_fixtures
import test_shared_backup as backup_fixtures
import test_shared_composition as composition_fixtures
import test_shared_remote as remote_fixtures
import test_shared_service as service_fixtures
import test_s3_artifacts as object_fixtures


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class PopulatedRecoveryQualificationTests(unittest.TestCase):
    def setUp(self):
        # Compose the existing native archive fixture without inheriting/rerunning
        # its individual negative tests under a second test class.
        self.fixture = backup_fixtures.SharedBackupIntegrationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = Path(self.fixture.temp.name)
        self.authority = self.fixture.source
        self.authority.renew(lease_seconds=300)
        self.resumed_effects = self.root / 'resumed-effects.jsonl'
        self.runners = []
        self.addCleanup(self.close_runners)

    def close_runners(self):
        for runner in self.runners:
            try:
                runner.close()
            except RunnerError:
                pass  # The source is deliberately sealed by the rehearsal.

    def runner(self, adapter, name):
        runner = self.authority.runner({'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 4, 'wall_seconds': 300}, {'a': adapter, 'b': adapter},
            self.root / name, principal='alice', workflow_version='recovery/1', supervised=False)
        self.runners.append(runner)
        return runner

    def service(self, authority, cache):
        workflow = Workflow('recovery-leaf/1', {'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 2, 'wall_seconds': 300},
            {'a': service_fixtures.SharedEcho(), 'b': service_fixtures.SharedEcho()},
            'a', 'b', 'work', classify=composition_fixtures.classify_confirmation,
            continue_input=composition_fixtures.confirm)
        probe = Workflow('recovery-probe/1', {'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 1, 'wall_seconds': 300},
            {'a': service_fixtures.SharedEcho(),
             'b': composition_fixtures.Increment(self.resumed_effects)}, 'a', 'b', 'work')
        leaves = SharedTaskService(authority, self.root / cache / 'leaves',
            {'work': workflow, 'probe': probe}, {'alice': {'work', 'probe'}}, supervised=False)
        plans = {'sequence': composition_fixtures.plan({'kind': 'sequence', 'steps': [
            {'kind': 'task', 'workflow': 'work'}, {'kind': 'task', 'workflow': 'work'}]})}
        compositions = SharedCompositionService(authority, self.root / cache / 'compositions',
            plans, {'work': workflow}, {'alice': {'sequence'}}, supervised=False)
        return SharedTaskFacade(leaves, compositions)

    def transfers(self, authority, cache, content=None):
        scope = SharedArtifactScope(authority, self.root / cache, 'transfer-results',
            {'alice': 'alice'}, ArtifactPolicy(), content or self.fixture.content)
        return SharedArtifactTransferService(self.root / cache / 'sessions', {'results': scope})

    def test_populated_inventory_restores_original_obligations_and_measures_full_cutover(self):
        f = self.fixture
        service = self.service(self.authority, 'service-before')
        completed = service.submit('alice', 'completed', 'work', {'confirmed': True})['task_id']
        service.run_once(completed)
        paused = service.submit('alice', 'paused', 'work', {})['task_id']
        service.run_once(paused)
        queued = service.submit('alice', 'queued', 'work', {})['task_id']
        composition = service.submit('alice', 'composition', 'sequence', {})['task_id']
        service.run_once(composition)
        tasks = {task: service.inspect('alice', task) for task in (completed, paused, queued, composition)}
        self.assertEqual([tasks[t]['state'] for t in (completed, paused, queued, composition)],
                         ['completed', 'input_required', 'queued', 'input_required'])

        registration = PeerRegistration(name='analysis', version='1', profile='mas-http/1',
            endpoint='https://analysis.example', outbound_principal='alice', deployment_id='remote',
            workflows={'analysis': 'a' * 64}, callers=['owner'])
        SharedPeerRegistry(self.authority).register(registration=registration, operator='owner',
            request_key='register', reason='Recovery fixture', expected_revision=0)
        peer_client = remote_fixtures.PeerClient()
        peer_client.lost = 'submit'
        remote = SharedRemoteTaskService(self.authority, peer_client, namespace='alice/analysis')
        adapter = remote_fixtures.RemoteAdapter(remote)
        remote_runner = self.runner(adapter, 'remote-run')
        adapter.run_dir = remote_runner.run_dir
        with self.assertRaises(RunnerError):
            remote_runner.request('a', 'b', 'submit', {'question': 'pending'})
        remote_before = remote.status('logical-job')
        self.assertEqual(remote_before['state'], 'unknown')

        cloud_client = cloud_fixtures.FakeCloud()
        cloud_client.error = CloudError('accepted launch reply lost')
        cloud_store = SharedJobStore(self.authority, self.root / 'cloud-cache', principal='alice')
        adapter = CursorCloudJobAdapter(cloud_store.service(cloud_client),
                                       replace(cloud_fixtures.SPEC, reservation_microusd=10))
        cloud_runner = self.runner(adapter, 'cloud-run')
        adapter.host_run_dir = cloud_runner.run_dir
        with self.assertRaises(RunnerError):
            cloud_runner.request('a', 'b', 'cloud.submit', {'job_id': 'cloud-unknown'})
        cloud_before = cloud_store.status('cloud-unknown')
        self.assertEqual(cloud_before['state'], 'launch_unknown')

        digest = hashlib.sha256(b'bounded-recovery-worker').hexdigest()
        grants = {'worker': {'trust_zone': 'trusted', 'capacity': 1, 'capabilities': {'work': digest}}}
        controller = SharedWorkerController(self.authority, grants)
        binding = remote_binding('alice', 'work', digest, 'trusted', {'value': 1}, 10)
        worker_runner = self.runner(service_fixtures.SharedEcho(), 'worker-run')
        worker_runner._journal.step('worker-effect', 'invoke', {'input': binding}, {})
        call = worker_runner._journal.create_invocation('worker-effect', 'b', 10)
        worker_runner._session.admit_invocation(call['call_id'])
        controller.submit_admitted(call['call_id'], binding)
        controller.register('worker', boot='original-boot', capabilities={'work': digest},
            architecture='x86_64', runtime='python-3.14.0', trust_zone='trusted', protocol=WORKER_PROTOCOL)
        assignment = controller.claim('worker', boot='original-boot')['assignment']
        self.assertTrue(controller.dispatch('worker', **{k: assignment[k]
            for k in ('boot', 'call_id', 'fence')})['execute'])
        controller.register('worker', boot='replacement-boot', capabilities={'work': digest},
            architecture='x86_64', runtime='python-3.14.0', trust_zone='trusted', protocol=WORKER_PROTOCOL)
        receipt = {'id': 'late-worker-observation', 'call_id': call['call_id'],
            'boot': assignment['boot'], 'fence': assignment['fence'], 'status': 'completed',
            'output': {'value': 2}, 'evidence': {}, 'output_profile': 'inline-json/1'}
        self.assertFalse(controller.receipt('worker', receipt=receipt)['authoritative'])

        policy = CircuitPolicy('recovery/provider', failure_threshold=1, open_seconds=300)
        circuit = SharedCircuitBreaker(self.authority, policy)
        circuit.admit('circuit-fixture')
        circuit.observe('circuit-fixture', 'provider_failure')
        circuit_before = circuit.status()

        transfers = self.transfers(self.authority, 'transfers-before')
        data = b'acknowledged prefix and unfinished suffix'
        transfer = transfers.upload_begin('alice', 'results', hashlib.sha256(data).hexdigest(),
            len(data), 'application/octet-stream')['transfer_id']
        transfers.upload_chunk('alice', transfer, 0, base64.b64encode(data[:12]).decode())
        transfer_before = transfers.status('alice', transfer)

        token = self.root / 'webhook-token'
        token.write_text('recovery-fixture-token-0000000000000')
        token.chmod(0o600)
        targets = {'sink': WebhookTarget('sink', 'https://sink.example/events', token, timeout_seconds=1)}
        delivery = SharedEventDelivery(self.authority, service, targets)
        delivery.subscribe('alice', 'subscription', queued, 'sink')
        with mock.patch('mas_host.bridges.service_client.http_exchange',
                        side_effect=ServiceError('transport_failure')):
            delivery.deliver_once('alice', 'subscription')
        event_before = self.authority.query('event.read', {'principal': 'alice', 'subscription_id': 'subscription'})
        self.assertIsNotNone(event_before['pending'])
        allocations = {scope: self.authority.query('scope', {'scope': scope})
                       for scope in ('deployment', 'principal/alice')}
        self.assertFalse(self.resumed_effects.exists())

        # The database archive deliberately does not contain object bytes.
        # An independent operator retains content under the original identity.
        object_backup = self.root / 'retained-object.blob'
        object_backup.write_bytes(f.content.verify_object(f.artifacts.store_id,
            f.reference, collect=True))
        object_manifest = self.root / 'retained-object.json'
        object_manifest.write_text(json.dumps({'reference': f.reference.to_dict(),
            'store_id': f.artifacts.store_id, 'backend': f.content.descriptor,
            'file': object_backup.name}, sort_keys=True))

        stages = {}
        started = previous = time.monotonic()
        def stage(name):
            nonlocal previous
            current = time.monotonic()
            stages[name] = round((current - previous) * 1000, 3)
            previous = current

        manifest = f.manager.archive(self.root / 'populated-archive', operator='operator',
            request_key='populated-final', evidence={'fixture': 'controlled-source-cutover'})
        stage('seal_and_archive_ms')
        before = f.manager.inspect()
        counts = {item['table']: item['rows'] for item in before['state']['tables']}
        required = ('runs', 'messages', 'calls', 'host_runs', 'host_invocations',
            'service_definitions', 'service_requests', 'service_tasks', 'service_executions',
            'service_task_events', 'composition_instances', 'composition_attempts',
            'federation_peers', 'remote_effects', 'remote_operations', 'cloud_jobs',
            'worker_jobs', 'worker_claims', 'worker_receipts', 'provider_circuits',
            'provider_permits', 'artifact_objects', 'artifact_transfers',
            'artifact_transfer_chunks', 'event_subscriptions', 'event_pending')
        for table in required:
            self.assertGreater(counts.get(table, 0), 0, f'{table} must be populated before recovery')
        f.restore.restore_audit(self.root / 'populated-archive')
        self.assertEqual(f.restore.inspect()['state'], before['state'])
        stage('native_restore_and_inventory_ms')

        # Simulate loss of the source content service and rebuild an empty
        # service at its pinned logical identity from the retained backup only.
        f.object_client.objects.clear()
        restored_client = object_fixtures.ObjectClient()
        restored_content = S3ArtifactContent(f.content.config, client=restored_client)
        self.assertEqual(restored_client.objects, {})
        def reader(namespace, digest, size):
            self.assertEqual((namespace, digest, size),
                (f.reference.namespace, f.reference.sha256, f.reference.size))
            return restored_content.verify_object(f.artifacts.store_id, f.reference, collect=True)
        with self.assertRaises(ArtifactError):
            f.restore.verify_objects(reader)
        self.assertEqual(f.restore.inspect()['mode'], 'audit')
        retained = json.loads(object_manifest.read_text())
        self.assertEqual(retained['backend'], restored_content.descriptor)
        self.assertEqual(retained['store_id'], f.artifacts.store_id)
        self.assertEqual(ArtifactRef.from_dict(retained['reference']), f.reference)
        self.assertEqual(retained['file'], object_backup.name)
        with object_backup.open('rb') as source:
            restored_content.publish_object(retained['store_id'],
                ArtifactRef.from_dict(retained['reference']), source, ArtifactPolicy())
        self.assertEqual(len(restored_client.objects), 1)
        self.assertEqual(f.object_client.objects, {})
        stage('external_object_restore_ms')
        verified = f.restore.verify_objects(reader)
        stage('object_verification_ms')
        activated = f.restore.activate(fencing_proof=f.proof(verified, before['database']), read_object=reader)
        stage('proof_and_activation_ms')
        successor = SharedAuthority(dsn=f.target._dsn, **{**f.args,
            'controller': activated['controller'], 'epoch': activated['epoch']})
        self.addCleanup(successor.close)
        self.assertTrue(successor.authority_status()['draining'])
        recovered = self.service(successor, 'empty-successor-cache')
        for task, expected in tasks.items():
            actual = recovered.inspect('alice', task)
            self.assertEqual(actual, expected)
        self.assertEqual(recovered.result('alice', completed), {'confirmed': True})
        self.assertEqual(SharedPeerRegistry(successor).resolve('analysis', 'owner').identity, registration.identity)
        self.assertEqual(SharedRemoteTaskService(successor, peer_client, namespace='alice/analysis').status('logical-job'), remote_before)
        self.assertEqual(SharedJobStore(successor, self.root / 'new-cloud-cache', principal='alice').status('cloud-unknown'), cloud_before)
        current_worker = SharedWorkerController(successor, grants)
        self.assertEqual(current_worker.inspect('alice', call['call_id'])['state'], 'unknown')
        self.assertEqual(current_worker.evidence('alice', call['call_id']), [receipt])
        self.assertFalse(current_worker.receipt('worker', receipt=receipt)['authoritative'])
        self.assertEqual(SharedCircuitBreaker(successor, policy).status(), circuit_before)
        self.assertEqual(self.transfers(successor, 'empty-transfer-cache', restored_content).status('alice', transfer), transfer_before)
        chunk = successor.query('transfer.read', {'principal': 'alice', 'transfer_id': transfer, 'offset': 0})
        self.assertEqual((bytes.fromhex(chunk['hex']), chunk['next_offset']), (data[:12], 12))
        restored_artifacts = SharedArtifactStore(successor, namespace='retained', principal='alice',
            content_backend=restored_content, policy=ArtifactPolicy())
        self.assertEqual(restored_artifacts.read_bytes(f.reference), b'retained exact object bytes')
        self.assertEqual(successor.query('event.read', {'principal': 'alice', 'subscription_id': 'subscription'}), event_before)
        for scope, expected in allocations.items():
            self.assertEqual(successor.query('scope', {'scope': scope}), expected)
        self.assertEqual(successor.query('invocation', {'run': f.run, 'call_id': 'uncertain-call'})['state'], 'executing')
        self.assertEqual([item[0] for item in peer_client.calls], ['submit'])
        self.assertEqual([item[0] for item in cloud_client.calls], ['create'])
        self.assertFalse(list(self.root.rglob('*.sqlite')), 'controller caches must not contain authority')
        with self.assertRaises((RunnerError, ServiceError)):
            recovered.submit('alice', 'closed-admission', 'work', {})
        with self.assertRaises(RunnerError):
            self.authority.renew()
        stage('recovered_service_verification_ms')

        # Activation does not reopen admission. The operator separately resumes
        # this epoch, then the public service admits one new effect.
        successor.renew(lease_seconds=300)
        successor.set_draining(False, operator='recovery-operator',
            evidence={'fixture': 'restored-state-and-objects-verified'})
        self.assertFalse(successor.authority_status()['draining'])
        resumed = recovered.submit('alice', 'resumed-probe', 'probe', {'value': 41})['task_id']
        self.assertEqual(recovered.run_once(resumed)['state'], 'completed')
        self.assertEqual(recovered.result('alice', resumed), {'value': 42})
        effects = [json.loads(line) for line in self.resumed_effects.read_text().splitlines()]
        self.assertEqual(len(effects), 1)
        self.assertEqual(effects[0]['input'], {'value': 41})
        self.assertEqual(recovered.submit('alice', 'resumed-probe', 'probe', {'value': 41})['task_id'], resumed)
        self.assertIsNone(recovered.run_once(resumed))
        self.assertEqual(len(self.resumed_effects.read_text().splitlines()), 1)
        for scope, previous_allocation in allocations.items():
            expected = {**previous_allocation,
                **{name: previous_allocation[name] + 1 for name in ('runs', 'calls', 'microusd')}}
            self.assertEqual(successor.query('scope', {'scope': scope}), expected)
        # New admission never resolves or replays the prior uncertain effects.
        for task, expected in tasks.items():
            self.assertEqual(recovered.inspect('alice', task), expected)
        self.assertEqual(SharedRemoteTaskService(successor, peer_client, namespace='alice/analysis').status('logical-job'), remote_before)
        self.assertEqual(SharedJobStore(successor, self.root / 'new-cloud-cache', principal='alice').status('cloud-unknown'), cloud_before)
        self.assertEqual(current_worker.inspect('alice', call['call_id'])['state'], 'unknown')
        self.assertFalse(current_worker.dispatch('worker', **{k: assignment[k]
            for k in ('boot', 'call_id', 'fence')})['execute'])
        self.assertEqual(successor.query('invocation', {'run': f.run, 'call_id': 'uncertain-call'})['state'], 'executing')
        self.assertEqual([item[0] for item in peer_client.calls], ['submit'])
        self.assertEqual([item[0] for item in cloud_client.calls], ['create'])
        self.assertFalse(list(self.root.rglob('*.sqlite')), 'resumed service must still use shared authority')
        stage('explicit_resume_and_task_completion_ms')
        total_ms = round((time.monotonic() - started) * 1000, 3)
        self.assertLess(total_ms, 60_000, 'controlled populated recovery exceeded the 60s fixture gate')
        self.assertEqual(manifest['payload']['seal']['payload']['state'], before['state'])
        print(json.dumps({'format': 'mas-controlled-recovery-qualification/1',
            'profile': 'sealed-source-local-postgresql-fixture', 'total_ms': total_ms,
            'stages_ms': stages, 'populated_tables': len([v for v in counts.values() if v]),
            'required_populated_tables': list(required), 'acknowledged_state_lost': False,
            'objects_restored_from_backup': len(restored_client.objects),
            'admission_open': True, 'fresh_tasks_completed': 1,
            'prior_uncertain_effects_replayed': False,
            'production_rpo_rto_established': False}, sort_keys=True))
