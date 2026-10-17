"""Operator archival is owner-bound, audited and replayable after interruption."""
from pathlib import Path
import tempfile
import json
import unittest
from unittest.mock import patch

from mas_host.operator_service import OperatorService
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService
from test_definition_versions import Executor, registered


class RetentionAdministrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.service = DurableTaskService(self.root, {'leaf': registered()},
            {'alice': {'leaf'}, 'bob': {'leaf'}}, executor=Executor(), max_tasks=2)
        self.first = self.service.submit('alice', 'first', 'leaf', {'value': 7})
        self.second = self.service.submit('bob', 'second', 'leaf', {'value': 8})
        self.service.run_once(self.first['task_id'])
        self.operators = {'admin': {'owners': ['alice'], 'deployment_admin': True},
                          'reviewer': {'owners': ['alice'], 'deployment_admin': False}}
        self.operator = OperatorService(self.service, operators=self.operators)
        self.payload = {'kind': 'tasks', 'principal': 'alice', 'task_id': self.first['task_id'],
                        'request_key': 'archive-one', 'reason': 'Retain compact completed history',
                        'older_than_seconds': 0}

    def test_archive_releases_retained_capacity_without_losing_key_or_refunding(self):
        before = self.service.operations.health()['usage']
        for actor, owner in (('reviewer', 'alice'), ('admin', 'bob')):
            with self.assertRaises(ServiceError) as denied:
                self.operator.dispatch(actor, 'archive', {**self.payload, 'principal': owner})
            self.assertEqual(denied.exception.code, 'forbidden')
        receipt = self.operator.dispatch('admin', 'archive', self.payload)
        self.assertEqual(receipt, self.operator.dispatch('admin', 'archive', self.payload))
        self.assertEqual(receipt['operator'], 'admin')
        self.assertTrue(receipt['archived'])
        self.assertFalse(self.service.inspect('bob', self.second['task_id']).get('archived', False))
        self.assertEqual(self.service.operations.health()['usage'], before)
        self.assertEqual(self.service.submit('alice', 'first', 'leaf', {'value': 7})['task_id'], self.first['task_id'])
        self.service.submit('alice', 'next', 'leaf', {})
        with self.assertRaises(ServiceError) as conflict:
            self.operator.dispatch('admin', 'archive', {**self.payload, 'reason': 'Different request'})
        self.assertEqual(conflict.exception.code, 'retention_request_conflict')

    def test_archived_error_retains_digest_without_arbitrary_body(self):
        import hashlib
        from mas_host.task_store import canonical
        error = {'code': 'application_failed', 'detail': 'sensitive classifier output'}
        with self.service.store.transaction() as db:
            db.execute('UPDATE tasks SET state=?,error=? WHERE id=?', ('failed', canonical(error), self.first['task_id']))
        self.operator.dispatch('admin', 'archive', self.payload)
        saved = self.service.inspect('alice', self.first['task_id'])
        self.assertEqual(saved['error'], {'archived': True, 'sha256': hashlib.sha256(canonical(error).encode()).hexdigest()})
        self.assertEqual(saved['state'], 'failed')

    def test_pending_webhook_body_blocks_archive_until_acknowledgement(self):
        from mas_host.event_delivery import EventDelivery, WebhookTarget
        token = self.root / 'event-token'
        token.write_text('a-private-token-of-sufficient-length'); token.chmod(0o600)
        delivery = EventDelivery(self.root, self.service, {'sink': WebhookTarget('sink', 'https://sink.example/events', token)})
        delivery.subscribe('alice', 'pending-page', self.first['task_id'], 'sink')
        with patch('mas_host.bridges.service_client.http_exchange', side_effect=ServiceError('transport_failure')):
            delivery.deliver_once('alice', 'pending-page')
        with self.assertRaises(ServiceError) as error:
            self.operator.dispatch('admin', 'archive', self.payload)
        self.assertEqual(error.exception.code, 'task_not_archivable')
        with delivery.transaction() as db:
            db.execute('UPDATE pending SET next_attempt=0')
        def acknowledge(endpoint, **options):
            return 200, {}, json.dumps({'ack': json.loads(options['body'])['delivery_id']}).encode()
        with patch('mas_host.bridges.service_client.http_exchange', side_effect=acknowledge):
            delivery.deliver_once('alice', 'pending-page')
        self.assertTrue(self.operator.dispatch('admin', 'archive', self.payload)['archived'])

    def test_interruption_after_owner_commit_repairs_original_intent(self):
        original = self.service.inspect
        def inspect(principal, task_id):
            receipt = original(principal, task_id)
            if receipt.get('archived'):
                raise RuntimeError('injected death after archive commit')
            return receipt
        with patch.object(self.service, 'inspect', side_effect=inspect):
            with self.assertRaises(RuntimeError):
                self.operator.dispatch('admin', 'archive', self.payload)
        self.assertTrue(original('alice', self.first['task_id'])['archived'])
        restarted = OperatorService(self.service, operators=self.operators)
        with patch('mas_host.task_history.archive_tasks', side_effect=AssertionError('already committed archive repeated')):
            receipt = restarted.dispatch('admin', 'archive', self.payload)
        self.assertTrue(receipt['archived'])
        self.assertEqual(receipt['request_key'], 'archive-one')


if __name__ == '__main__':
    unittest.main()
