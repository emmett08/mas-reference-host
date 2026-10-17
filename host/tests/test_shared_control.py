"""Shared workers and webhook deliveries against the real PostgreSQL profile."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from mas_host.contracts import RunnerError
from mas_host.distributed import remote_binding, WORKER_PROTOCOL
from mas_host.event_delivery import EventDelivery, WebhookTarget
from mas_host.service_contracts import ServiceError
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_control import SharedWorkerController, SharedWorkerControl
from mas_host.shared_events import SharedEventDelivery


class SharedControlBoundaryTests(unittest.TestCase):
    def test_replayed_native_dispatch_is_not_fresh_execution_permission(self):
        class Authority:
            def command(self, name, payload, **kwargs):
                if name == 'worker.dispatch':
                    return {'replayed': True, 'receipt': {'execute': True}}
                return {'replayed': False, 'receipt': {}}
        controller = SharedWorkerController(Authority(), {})
        self.assertFalse(controller.dispatch('worker', boot='boot', call_id='call', fence=1)['execute'])

    def test_archived_receipt_replays_without_artifact_access_or_mutation(self):
        ack = {'stored': True, 'receipt_id': 'receipt', 'authoritative': True}
        class Authority:
            def command(self, name, payload, **kwargs):
                if name != 'worker.configure':
                    raise AssertionError('archived ACK replay must be read-only')
                return {'replayed': False, 'receipt': {}}
            def query(self, name, payload):
                return {'archived': True, 'acknowledgement': ack}
        controller = SharedWorkerController(Authority(), {'worker': {'trust_zone': 'trusted',
            'capacity': 1, 'capabilities': {'work': '1' * 64}}})
        receipt = {'id': 'receipt', 'call_id': 'call', 'boot': 'boot', 'fence': 1,
                   'status': 'completed', 'output': {}, 'evidence': {}, 'output_profile': 'inline-json/1'}
        with patch.object(controller, '_verify_artifacts', side_effect=AssertionError('archived references must not reactivate')):
            self.assertEqual(controller.receipt('worker', receipt=receipt), ack)

    def test_shared_operator_admin_reads_never_open_a_local_authority(self):
        from mas_host.operator_service import OperatorService
        class Authority:
            def __init__(self): self.operations = []
            def query(self, operation, payload):
                self.operations.append(operation)
                return {'native': operation}
        class Service:
            authority = Authority()
            operations = None
            state_dir = Path('/deliberately-absent-controller-cache')
            def recovery_service(self, **kwargs): return object()
            def deadline_service(self, **kwargs): return object()
        service = Service()
        operator = OperatorService(service, operators={'admin': {'owners': [], 'deployment_admin': True}})
        for operation, payload in (('budget', {}), ('storage', {}),
                ('archive_preview', {'kind': 'tasks', 'older_than_seconds': 0}), ('worker_status', {})):
            result = operator.dispatch('admin', operation, payload)
            self.assertTrue(result['native'].startswith('operator.'))
        self.assertEqual(service.authority.operations,
            ['operator.budget', 'operator.storage', 'operator.archive_preview', 'operator.status'])


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedControlIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.settings = dict(binary=os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared'),
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace='mas_control_' + uuid.uuid4().hex,
            deployment='deployment', controller='controller', epoch=1,
            secret_key=b'worker-test-secret' * 3, secret_reference='worker-key/1')
        self.authority = SharedAuthority(**self.settings)
        self.authority.initialise()
        self.addCleanup(self.authority.close)
        for scope in ('deployment', 'principal/alice'):
            self.authority.configure_scope(scope, max_runs=10, max_calls=20, max_microusd=200)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.digest = hashlib.sha256(b'implementation').hexdigest()
        self.grants = {'worker': {'trust_zone': 'trusted', 'capacity': 1, 'capabilities': {'work': self.digest}}}
        self.controller = SharedWorkerController(self.authority, self.grants)
        self.binding = remote_binding('alice', 'work', self.digest, 'trusted', {'x': 1}, 10)
        self.runner = self.authority.runner({'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 10, 'wall_seconds': 120}, {'a': type('Adapter', (), {'handle': lambda *_: None})(), 'b': type('Adapter', (), {'handle': lambda *_: None})()}, self.root / 'cache',
            principal='alice', workflow_version='worker-test/1', supervised=False)
        self.addCleanup(self._close_runner)

    def _close_runner(self):
        try:
            self.runner.close()
        except (RunnerError, ServiceError):
            pass  # Expected when testing a fenced original controller.

    def admit(self):
        key = 'remote-' + uuid.uuid4().hex
        self.runner._journal.step(key, 'invoke', {'input': self.binding}, {})
        call = self.runner._journal.create_invocation(key, 'b', 10)
        self.runner._session.admit_invocation(call['call_id'])
        self.controller.submit_admitted(call['call_id'], self.binding)
        return call['call_id']

    def register(self, boot='boot'):
        return self.controller.register('worker', boot=boot, capabilities={'work': self.digest},
            architecture='x86_64', runtime='python-3.14.0', trust_zone='trusted',
            protocol=WORKER_PROTOCOL)

    def assigned(self):
        call_id = self.admit()
        self.register()
        assignment = self.controller.claim('worker', boot='boot')['assignment']
        self.assertEqual(assignment['call_id'], call_id)
        return assignment

    @staticmethod
    def receipt(assignment, *, key='receipt', status='completed'):
        return {'id': key, 'call_id': assignment['call_id'], 'boot': assignment['boot'],
            'fence': assignment['fence'], 'status': status, 'output': {'ok': True} if status == 'completed' else None,
            'evidence': {}, 'output_profile': 'inline-json/1'}

    def dispatch(self, assignment):
        return self.controller.dispatch('worker', **{k: assignment[k] for k in ('boot', 'call_id', 'fence')})

    def test_dispatch_receipt_recreation_and_lost_grant_replay(self):
        assignment = self.assigned()
        self.assertTrue(self.dispatch(assignment)['execute'])
        self.assertFalse(self.dispatch(assignment)['execute'])
        receipt = self.receipt(assignment)
        ack = self.controller.receipt('worker', receipt=receipt)
        self.assertTrue(ack['authoritative'])
        replacement = SharedWorkerController(self.authority, self.grants)
        self.assertEqual(replacement.receipt('worker', receipt=receipt), ack)
        self.assertEqual(replacement.inspect('alice', assignment['call_id'])['state'], 'completed')
        with self.assertRaises((ServiceError, RunnerError)):
            replacement.receipt('worker', receipt={**receipt, 'output': {'changed': True}})
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 1)
        self.assertFalse(list(self.root.rglob('*.sqlite')))

    def test_reboot_retains_late_receipt_as_evidence_and_unknown_blocks_capacity(self):
        assignment = self.assigned(); self.dispatch(assignment)
        self.register('replacement-boot')
        self.assertEqual(self.controller.inspect('alice', assignment['call_id'])['state'], 'unknown')
        ack = self.controller.receipt('worker', receipt=self.receipt(assignment))
        self.assertFalse(ack['authoritative'])
        self.admit()
        self.assertIsNone(self.controller.claim('worker', boot='replacement-boot')['assignment'])
        self.assertEqual(len(self.controller.evidence('alice', assignment['call_id'])), 1)
        # The permanent receipt replays the earlier command without issuing a
        # fresh effect grant, even after the original boot has been fenced.
        self.assertFalse(self.dispatch(assignment)['execute'])
        # A new native command still validates the now-stale boot/assignment.
        with self.assertRaises((ServiceError, RunnerError)):
            self.authority.command('worker.dispatch', {'principal': 'worker',
                **{key: assignment[key] for key in ('boot', 'call_id', 'fence')}},
                command_id='different-command-after-reboot')

    def test_grants_and_exact_shared_admission_binding_fail_closed(self):
        call_id = self.admit()
        with self.assertRaises((ServiceError, RunnerError)):
            self.controller.submit_admitted(call_id, {**self.binding, 'input': {'x': 2}})
        with self.assertRaises((ServiceError, RunnerError)):
            self.controller.register('worker', boot='boot', capabilities={'work': self.digest},
                architecture='x86_64', runtime='python-9.0.0', trust_zone='trusted', protocol=WORKER_PROTOCOL)
        with self.assertRaises((ServiceError, RunnerError)):
            self.controller.inspect('mallory', call_id)

    def test_operator_drain_is_atomic_with_new_dispatch_and_preserves_receipts(self):
        assignment = self.assigned()
        control = SharedWorkerControl(self.authority)
        command = dict(operator='admin', request_key='maintenance', reason='Deploy replacement',
            expected_revision=0, draining=True)
        receipt = control.set(**command)
        self.assertTrue(receipt['draining'])
        self.assertEqual(control.set(**command), receipt)
        self.assertTrue(control.status()['draining'])
        with self.assertRaises((ServiceError, RunnerError)):
            self.dispatch(assignment)
        control.set(**{**command, 'request_key': 'resume', 'expected_revision': 1, 'draining': False})
        self.assertTrue(self.dispatch(assignment)['execute'])
        control.set(**{**command, 'request_key': 'drain-active', 'expected_revision': 2})
        self.assertTrue(self.controller.receipt('worker', receipt=self.receipt(assignment))['stored'])

    def test_epoch_handover_rejects_stale_commands_and_preserves_dispatched_obligation(self):
        assignment = self.assigned(); self.dispatch(assignment)
        self.authority.set_draining(True, operator='admin', evidence={'reason': 'handover'})
        binding = self.authority.handover('replacement')
        successor = SharedAuthority(**{**self.settings, 'controller': 'replacement', 'epoch': binding['epoch']})
        self.addCleanup(successor.close)
        with self.assertRaises((ServiceError, RunnerError)):
            self.controller.receipt('worker', receipt=self.receipt(assignment))
        current = SharedWorkerController(successor, self.grants)
        self.assertTrue(current.receipt('worker', receipt=self.receipt(assignment))['authoritative'])
        self.assertEqual(successor.query('scope', {'scope': 'deployment'})['calls'], 1)

    def test_shared_operator_ports_read_and_amend_native_state_without_local_repositories(self):
        from mas_host.operator_service import OperatorService
        from mas_host.shared_task_service import SharedTaskService
        from mas_host.shared_service import SharedTaskFacade
        from test_definition_versions import registered
        workflow = registered()
        service = SharedTaskFacade(SharedTaskService(self.authority, self.root / 'service-cache',
            {'leaf': workflow}, {'alice': {'leaf'}}, supervised=False))
        task = service.submit('alice', 'operator-task', 'leaf', {})
        with patch('mas_host.operator_service.RecoveryService', side_effect=AssertionError('local recovery forbidden')):
            operator = OperatorService(service, operators={'admin': {'owners': ['alice'], 'deployment_admin': True}})
        def call(operation, payload=None):
            return operator.dispatch('admin', operation, payload or {})
        old = call('budget')
        changed = call('budget_amend', {'request_key': 'fund', 'reason': 'Approved extension',
            'expected_revision': 0, 'limits': {'max_runs': 11}})
        self.assertEqual(changed['principal'], 'admin')
        self.assertEqual(call('budget')['health']['usage'], old['health']['usage'])
        self.assertEqual(call('budget')['health']['policy']['max_runs'], 11)
        # Restart may still supply the immutable pre-amendment baseline.
        self.authority.configure_scope('deployment', max_runs=10, max_calls=20, max_microusd=200)
        self.assertEqual(call('storage')['format'], 'mas-shared-health/1')
        self.assertEqual(call('peer_status')['peers']['items'], [])
        self.assertEqual(call('periods')['items'], [])
        self.assertFalse(call('worker_status')['draining'])
        inbox = call('recovery_inbox', {'principal': 'alice', 'task_id': task['task_id']})
        self.assertEqual(inbox['entries']['items'], [])
        preview = call('archive_preview', {'kind': 'tasks', 'older_than_seconds': 0})
        self.assertFalse(preview['items'][0]['eligible'])
        service.cancel('alice', task['task_id'])
        self.assertTrue(call('archive_preview', {'kind': 'tasks', 'older_than_seconds': 0})['items'][0]['eligible'])
        self.assertTrue(call('definition', {'kind': 'workflow', 'name': 'leaf', 'identity': workflow.identity})['available'])
        self.assertFalse(list(self.root.rglob('*.sqlite')))

    def test_shared_artifact_receipt_requires_verified_bytes_and_preserves_released_link(self):
        from mas_host.artifact_contracts import ArtifactPolicy
        from mas_host.distributed import artifact_output, ARTIFACT_OUTPUT_PROFILE
        from mas_host.shared_artifacts import SharedArtifactStore
        from mas_host.s3_artifacts import S3ArtifactContent
        from mas_host.s3_transport import S3ContentConfig
        from test_s3_artifacts import ObjectClient
        # Worker policy is immutable: create an independently provisioned shared
        # namespace with the artefact contract configured before placement.
        authority = SharedAuthority(**{**self.settings, 'namespace': 'mas_artworker_' + uuid.uuid4().hex})
        authority.initialise(); self.addCleanup(authority.close)
        for scope in ('deployment', 'principal/alice'):
            authority.configure_scope(scope, max_runs=10, max_calls=20, max_microusd=200)
        client = ObjectClient()
        backend = S3ArtifactContent(S3ContentConfig('https://objects.example', 'test-bucket', 'mas', 'us-east-1', self.root / 'unused'), client=client)
        policy = ArtifactPolicy(max_artifact_bytes=1024, max_total_bytes=2048, max_artifacts=4, max_links=8)
        def repository(namespace, principal):
            return SharedArtifactStore(authority, namespace=namespace, principal=principal, content_backend=backend, policy=policy)
        controller = SharedWorkerController(authority, self.grants, artifact_repository=repository, artifact_store_id='shared-output/1')
        binding = remote_binding('alice', 'work', self.digest, 'trusted', {}, 10, output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=('result',))
        adapters = {'a': type('Adapter', (), {'handle': lambda *_: None})(), 'b': type('Adapter', (), {'handle': lambda *_: None})()}
        runner = authority.runner({'agents': ['a', 'b'], 'application': 'agent-task/1', 'max_calls': 5, 'wall_seconds': 120}, adapters, self.root / 'artifact-cache', principal='alice', workflow_version='artifact-test/1', supervised=False)
        self.addCleanup(runner.close)
        runner._journal.step('effect', 'invoke', {'input': binding}, {})
        call = runner._journal.create_invocation('effect', 'b', 10)
        runner._session.admit_invocation(call['call_id'])
        controller.submit_admitted(call['call_id'], binding)
        controller.register('worker', boot='boot', capabilities={'work': self.digest}, architecture='x86_64', runtime='python-3.14.0', trust_zone='trusted', protocol=WORKER_PROTOCOL, output_profiles=(ARTIFACT_OUTPUT_PROFILE,))
        assignment = controller.claim('worker', boot='boot')['assignment']
        controller.dispatch('worker', **{k: assignment[k] for k in ('boot', 'call_id', 'fence')})
        reference = repository('results', 'worker').put_bytes(b'durable bytes', readers=('alice',)).to_dict()
        receipt = self.receipt(assignment)
        receipt.update(output_profile=ARTIFACT_OUTPUT_PROFILE, artifacts={'result': reference}, output=artifact_output({}, {'result': reference}))
        client.corrupt = True
        with self.assertRaises(ServiceError):
            controller.receipt('worker', receipt=receipt)
        self.assertEqual(controller.evidence('alice', call['call_id']), [])
        client.corrupt = False
        self.assertTrue(controller.receipt('worker', receipt=receipt)['authoritative'])
        consumer = repository('results', 'alice')
        link = next(iter(consumer.status()['links']))
        consumer.link(link, [reference], state='released')
        controller.receipt('worker', receipt=receipt)
        self.assertEqual(consumer.status()['links'][link]['state'], 'released')

    def test_shared_event_restart_reuses_exact_pending_body_and_acknowledges_once(self):
        authority = self.authority
        class Service:
            def __init__(self):
                self.authority = authority
            def inspect(self, principal, task_id):
                if principal != 'alice':
                    raise ServiceError('forbidden')
                return {'task_id': task_id}
            def events_page(self, principal, task_id, *, after=0, limit=100):
                self.inspect(principal, task_id)
                events = [{'id': 1, 'event': 'submitted', 'data': {'state': 'queued'}}] if after < 1 else []
                return {'events': events, 'next_after': 1, 'has_more': False}
        service = Service()
        token = self.root / 'webhook-token'; token.write_text('event-test-token-0000000000000'); token.chmod(0o600)
        targets = {'sink': WebhookTarget('sink', 'https://sink.example/events', token, timeout_seconds=1)}
        delivery = EventDelivery(self.root / 'unused-cache', service, targets)
        self.assertIsInstance(delivery, SharedEventDelivery)
        delivery.subscribe('alice', 'subscription', 'task1', 'sink')
        sent = []
        def lose(endpoint, **options):
            sent.append(options['body'])
            raise ServiceError('transport_failure')
        with patch('mas_host.bridges.service_client.http_exchange', side_effect=lose):
            self.assertIsNotNone(delivery.deliver_once('alice', 'subscription')['pending'])
        replacement = SharedEventDelivery(authority, service, targets)
        time.sleep(3.1)  # Attempt ownership expires; immutable delivery remains.
        def acknowledge(endpoint, **options):
            sent.append(options['body'])
            return 200, {}, json.dumps({'ack': json.loads(options['body'])['delivery_id']}).encode()
        with patch('mas_host.bridges.service_client.http_exchange', side_effect=acknowledge):
            report = replacement.deliver_once('alice', 'subscription')
        self.assertEqual(sent[0], sent[1])
        self.assertIsNone(report['pending'])
        self.assertEqual(report['cursor'], 1)
        self.assertFalse((self.root / 'unused-cache').exists())


if __name__ == '__main__':
    unittest.main()
