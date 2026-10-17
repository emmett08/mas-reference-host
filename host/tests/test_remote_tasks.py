"""Lost replies, independently admitted lifecycle work and storage guards."""
import json
from pathlib import Path
from contextlib import closing
import sqlite3
import tempfile
import unittest

from mas_host.bridges.remote import RemoteSnapshot, RemoteTaskStore, RemoteTaskService
from mas_host.journal import Journal
from mas_host.operations import DeploymentLedger
from mas_host.service_contracts import ServiceError


class AdmittedContext:
    actor = 'bridge'
    remaining_seconds = 30
    invocation_id = None

    def __init__(self, root):
        self.root = root
        self.host = root / 'run-1'
        self.host.mkdir(mode=0o700)
        self.ledger = DeploymentLedger(root)
        self.ledger.admit_run('run-1', self.host)
        self.journal = Journal(self.host, {'run_id': 'run-1', 'workflow_version': 'test', 'topology': {}, 'max_depth': 8})
        self.index = 0

    def invoke(self, callback, *, input=None, reservation_microusd=0):
        self.index += 1
        self.invocation_id = 'call-' + str(self.index)
        self.ledger.admit_call('run-1', self.invocation_id, reservation_microusd)
        with self.journal.db:
            self.journal.db.execute('INSERT INTO invocations VALUES(?,?,?,?,NULL,NULL,?)',
                                   (self.invocation_id, 'step-' + str(self.index), self.actor, 'executing', reservation_microusd))
        return callback().output


class RemoteFixture:
    def __init__(self):
        self.calls = []
        self.state = 'running'
        self.failure = None
        self.identity = 'peer-one'
        self.remote_id = 'server-task-1'

    def descriptor(self):
        return {'bridge': 'fixture', 'client_identity': self.identity, 'endpoint': 'https://peer.example/rpc'}

    def reply(self, action):
        self.calls.append(action)
        if self.failure:
            raise self.failure
        return RemoteSnapshot(self.remote_id, self.state, {'value': 7}, 'context-1')

    def submit(self, payload):
        return self.reply('submit')

    def poll(self, remote_id):
        return self.reply('poll')

    def cancel(self, remote_id):
        return self.reply('cancel')

    def update(self, remote_id, payload):
        return self.reply('update')


class RemoteTaskTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.context = AdmittedContext(self.root)
        self.addCleanup(self.context.journal.close)
        self.store = RemoteTaskStore(self.root)
        self.client = RemoteFixture()
        self.service = RemoteTaskService(self.store, self.client, namespace='alice/workflow/bridge')
        self.options = {'context': self.context, 'host_run_dir': self.context.host, 'reservation_microusd': 3}

    def submit(self, **changes):
        return self.service.submit('job-1', {'message': 'compute'}, **(self.options | changes))

    def test_committed_unknown_intent_precedes_dispatch_and_exact_retry_does_not_resubmit(self):
        original = self.client.submit
        def inspect(payload):
            with closing(sqlite3.connect(self.store.path)) as db, db:
                state, pending, run = db.execute('SELECT state,pending_action,host_run_dir FROM remote_tasks').fetchone()
            self.assertEqual((state, pending, run), ('unknown', 'submit', 'run-1'))
            return original(payload)
        self.client.submit = inspect
        first = self.submit()
        self.assertEqual(self.submit(), first)
        self.assertEqual(self.client.calls, ['submit'])
        self.assertEqual(self.context.index, 2)

    def test_lost_initial_receipt_survives_restart_without_resubmission(self):
        self.client.failure = ServiceError('lost_response')
        with self.assertRaises(ServiceError):
            self.submit()
        self.service = RemoteTaskService(RemoteTaskStore(self.root), self.client, namespace='alice/workflow/bridge')
        self.client.failure = None
        with self.assertRaisesRegex(ServiceError, 'remote outcome unknown'):
            self.submit()
        self.assertEqual(self.client.calls, ['submit'])
        self.assertEqual(self.service.status('job-1')['state'], 'unknown')

    def test_reconciliation_requires_evidence_and_admitted_exact_task_poll(self):
        self.client.failure = ServiceError('lost_response')
        with self.assertRaises(ServiceError):
            self.submit()
        self.client.failure = None
        with self.assertRaisesRegex(ServiceError, 'reconciliation evidence'):
            self.service.attach('job-1', 'server-task-1', evidence={}, **self.options)
        result = self.service.attach('job-1', 'server-task-1', evidence={'ticket': 'operator-7'}, **self.options)
        self.assertEqual(result['remote_id'], 'server-task-1')
        self.assertIsNone(result['pending_action'])
        self.assertEqual(self.client.calls, ['submit', 'poll'])

    def test_changed_input_and_peer_identity_reject_before_external_call(self):
        self.submit()
        with self.assertRaisesRegex(ServiceError, 'idempotency conflict'):
            self.service.submit('job-1', {'message': 'different'}, **self.options)
        self.client.identity = 'different-peer'
        with self.assertRaisesRegex(ServiceError, 'remote identity conflict'):
            self.service.poll('job-1', **self.options)
        self.assertEqual(self.client.calls, ['submit'])

    def test_missing_real_host_admission_never_dispatches(self):
        class BadContext:
            invocation_id = 'call-forged'
            def invoke(self, callback, **kwargs):
                return callback().output
        with self.assertRaisesRegex(ServiceError, 'admission required'):
            self.submit(context=BadContext())
        self.assertEqual(self.client.calls, [])

    def test_new_schema_rejected_without_modification(self):
        self.submit()
        with closing(sqlite3.connect(self.store.path)) as db, db:
            db.execute('PRAGMA user_version=99')
        before = self.store.path.read_bytes()
        with self.assertRaisesRegex(ServiceError, 'unsupported schema'):
            self.service.status('job-1')
        self.assertEqual(self.store.path.read_bytes(), before)

    def test_restore_marker_blocks_network_and_preserves_read_only_inspection(self):
        self.submit()
        (self.root / '.restored-audit-only').write_text('audit-only')
        self.assertEqual(self.service.status('job-1')['state'], 'running')
        with self.assertRaisesRegex(Exception, 'audit.only'):
            self.service.poll('job-1', **self.options)
        self.assertEqual(self.client.calls, ['submit'])

    def test_cancellation_acknowledgement_is_not_stopped_and_lost_cancel_is_not_retried(self):
        self.submit()
        self.client.failure = ServiceError('lost_cancel')
        with self.assertRaises(ServiceError):
            self.service.cancel('job-1', **self.options)
        row = self.service.status('job-1')
        self.assertEqual(row['state'], 'running')
        self.assertTrue(row['cancel_requested'])
        self.assertEqual(row['pending_action'], 'cancel')
        self.client.failure = None
        with self.assertRaisesRegex(ServiceError, 'remote outcome unknown'):
            self.service.cancel('job-1', **self.options)
        self.client.state = 'completed'
        row = self.service.poll('job-1', **self.options)
        self.assertEqual(row['state'], 'completed')
        self.assertEqual(row['pending_action'], 'cancel')
        self.assertEqual(self.client.calls, ['submit', 'cancel', 'poll'])

    def test_successful_cancel_that_returns_running_does_not_claim_stopped(self):
        self.submit()
        row = self.service.cancel('job-1', **self.options)
        self.assertEqual(row['state'], 'running')
        self.assertTrue(row['cancel_requested'])

    def test_lost_continuation_remains_unknown_and_cannot_automatically_repeat(self):
        self.client.state = 'input_required'
        self.submit()
        self.client.failure = ServiceError('lost_update')
        with self.assertRaises(ServiceError):
            self.service.update('job-1', request_key='turn-2', payload={'answer': 7}, **self.options)
        self.client.failure = None
        self.service.poll('job-1', **self.options)
        with self.assertRaises(ServiceError):
            self.service.update('job-1', request_key='turn-2', payload={'answer': 7}, **self.options)
        with self.assertRaisesRegex(ServiceError, 'task not continuable'):
            self.service.update('job-1', request_key='turn-3', payload={'answer': 7}, **self.options)
        self.assertEqual(self.client.calls, ['submit', 'update', 'poll'])

    def test_wrong_remote_identity_never_overwrites_known_receipt(self):
        self.submit()
        self.client.remote_id = 'other-task'
        with self.assertRaisesRegex(ServiceError, 'remote identity conflict'):
            self.service.poll('job-1', **self.options)
        self.assertEqual(self.service.status('job-1')['remote_id'], 'server-task-1')

    def test_immediate_completion_needs_no_remote_handle(self):
        self.client.submit = lambda payload: RemoteSnapshot(None, 'completed', {'text': 'done'}, 'context-1')
        result = self.submit()
        self.assertEqual(result['state'], 'completed')
        self.assertIsNone(result['remote_id'])
        self.assertEqual(self.submit(), result)

    def test_trusted_namespaces_isolate_same_logical_key_and_lifecycle(self):
        first = self.submit()
        other = RemoteTaskService(self.store, self.client, namespace='bob/workflow/bridge')
        with self.assertRaisesRegex(ServiceError, 'task not found'):
            other.poll('job-1', **self.options)
        second = other.submit('job-1', {'message': 'other'}, **self.options)
        self.assertEqual(first['key'], second['key'])
        self.assertNotEqual(first['binding'], second['binding'])
        with closing(sqlite3.connect(self.store.path)) as db, db:
            self.assertEqual(db.execute('SELECT count(*) FROM remote_tasks').fetchone()[0], 2)

    def test_applied_continuation_resolution_requires_observation_before_another_update(self):
        self.client.state = 'input_required'
        self.submit()
        self.client.failure = ServiceError('lost_response')
        with self.assertRaises(ServiceError):
            self.service.update('job-1', request_key='turn-2', payload={'answer': 7}, **self.options)
        self.client.failure = None
        resolved = self.service.resolve_operation('job-1', 'update:turn-2', evidence={'peer_audit': 'accepted-7'}, decision='applied')
        self.assertEqual(resolved['state'], 'unknown')
        with self.assertRaisesRegex(ServiceError, 'task not continuable'):
            self.service.update('job-1', request_key='turn-3', payload={'answer': 8}, **self.options)
        # Original operation identity is resolved and can never dispatch again.
        self.service.update('job-1', request_key='turn-2', payload={'answer': 7}, **self.options)
        self.assertEqual(self.client.calls, ['submit', 'update'])
        self.client.state = 'completed'
        self.assertEqual(self.service.poll('job-1', **self.options)['state'], 'completed')

    def test_cancel_intent_blocks_new_continuation_and_not_applied_resolution_is_explicit(self):
        self.client.state = 'input_required'
        self.submit()
        self.client.failure = ServiceError('lost_response')
        with self.assertRaises(ServiceError):
            self.service.cancel('job-1', **self.options)
        self.client.failure = None
        with self.assertRaises(ServiceError):
            self.service.update('job-1', request_key='turn-2', payload={}, **self.options)
        with self.assertRaises(ServiceError):
            self.service.resolve_operation('job-1', 'cancel:cancel', evidence={}, decision='not_applied')
        result = self.service.resolve_operation('job-1', 'cancel:cancel', evidence={'peer_audit': 'not-received'}, decision='not_applied')
        self.assertFalse(result['cancel_requested'])
        self.service.update('job-1', request_key='turn-2', payload={}, **self.options)
        self.assertEqual(self.client.calls, ['submit', 'cancel', 'update'])

    def test_failed_observation_can_be_inspected_and_closed_without_clearing_pending_effect(self):
        self.submit()
        self.client.failure = ServiceError('lost_poll')
        with self.assertRaises(ServiceError):
            self.service.poll('job-1', **self.options)
        operations = self.service.inspect('job-1')['operations']
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0]['state'], 'unknown')
        key = operations[0]['operation_key']
        with self.assertRaises(ServiceError):
            self.service.resolve_operation('job-1', key, evidence={'checked': True}, decision='applied')
        self.service.resolve_operation('job-1', key, evidence={'reason': 'read-only reply lost'}, decision='observation_unavailable')
        self.assertEqual(self.service.inspect('job-1')['operations'][0]['state'], 'completed')
        self.assertEqual(self.service.status('job-1')['state'], 'running')
        self.assertEqual(self.client.calls, ['submit', 'poll'])

    def test_lost_update_blocks_cancel_without_overwriting_unresolved_intent(self):
        self.client.state = 'input_required'
        self.submit()
        self.client.failure = ServiceError('lost_update')
        with self.assertRaises(ServiceError):
            self.service.update('job-1', request_key='turn-2', payload={'answer': 7}, **self.options)
        self.client.failure = None
        with self.assertRaisesRegex(ServiceError, 'remote outcome unknown'):
            self.service.cancel('job-1', request_key='cancel-after-update', **self.options)
        self.assertEqual(self.service.status('job-1')['pending_action'], 'update')
        self.assertEqual(self.client.calls, ['submit', 'update'])
        operations = self.service.inspect('job-1')['operations']
        self.assertEqual([(item['operation_key'], item['state']) for item in operations], [('update:turn-2', 'unknown')])
        self.service.resolve_operation('job-1', 'update:turn-2', decision='not_applied',
                                       evidence={'peer_audit': 'update-not-applied'})
        self.service.cancel('job-1', request_key='cancel-after-update', **self.options)
        self.assertEqual(self.client.calls, ['submit', 'update', 'cancel'])
        self.assertIsNone(self.service.status('job-1')['pending_action'])

    def test_lost_cancel_can_resolve_after_poll_observes_terminal_without_reopening_task(self):
        self.submit()
        self.client.failure = ServiceError('lost_cancel')
        with self.assertRaises(ServiceError):
            self.service.cancel('job-1', request_key='cancel-1', **self.options)
        self.client.failure = None
        self.client.state = 'completed'
        observed = self.service.poll('job-1', **self.options)
        self.assertEqual(observed['state'], 'completed')
        self.assertEqual(observed['pending_action'], 'cancel')
        result = self.service.resolve_operation('job-1', 'cancel:cancel-1', decision='not_applied',
                                                evidence={'peer_audit': 'completed-before-cancel'})
        self.assertEqual(result['state'], 'completed')
        self.assertIsNone(result['pending_action'])
        self.assertFalse(result['cancel_requested'])
        self.assertEqual(self.client.calls, ['submit', 'cancel', 'poll'])
        self.assertTrue(all(item['state'] == 'completed' for item in self.service.inspect('job-1')['operations']))
