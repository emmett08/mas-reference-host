"""Subscription progress, authority, lost receipts and bounded retries."""
import http.client
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mas_host.event_delivery import EventDelivery, WebhookTarget, wait_events
from mas_host.service_contracts import ServiceError
from test_bridge_server import JoinedBridgeHTTPServer, TOKEN, verifier


class Events:
    def __init__(self):
        self.items = []
        self.allowed = True
        self.expired = False

    def inspect(self, principal, task_id):
        if principal != 'alice' or not self.allowed:
            raise ServiceError('forbidden')
        return {'task_id': task_id}

    def events_page(self, principal, task_id, after=0, limit=100):
        self.inspect(principal, task_id)
        if self.expired:
            raise ServiceError('event_cursor_expired')
        events = [item for item in self.items if item['id'] > after][:limit]
        next_after = events[-1]['id'] if events else after
        return {'events': events, 'next_after': next_after,
                'has_more': any(item['id'] > next_after for item in self.items)}


class EventDeliveryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        token = self.root / 'secret'
        token.write_text(TOKEN)
        token.chmod(0o600)
        self.source = Events()
        self.target = WebhookTarget('observer', 'http://127.0.0.1/events', token, allow_loopback_http=True)
        self.delivery = EventDelivery(self.root, self.source, {'observer': self.target}, page_size=1)
        self.delivery.subscribe('alice', 'watch-one', 'task-one', 'observer')

    def test_lost_ack_restart_repeats_identical_bytes_then_advances_atomically(self):
        self.source.items = [{'id': 3, 'event': 'submitted'}, {'id': 5, 'event': 'completed'}]
        sent = []
        def exchange(_endpoint, **kwargs):
            sent.append(kwargs['body'])
            if len(sent) == 1:
                raise ServiceError('transport_closed')
            return 200, {}, json.dumps({'ack': json.loads(kwargs['body'])['delivery_id']}).encode()
        with patch('mas_host.bridges.service_client.http_exchange', side_effect=exchange):
            pending = self.delivery.deliver_once('alice', 'watch-one')
            self.assertEqual(pending['cursor'], 0)
            self.assertEqual(pending['pending']['attempts'], 1)
            resumed = EventDelivery(self.root, self.source, {'observer': self.target}, page_size=1)
            with patch('mas_host.event_delivery.time.time', return_value=time.time() + 10):
                acknowledged = resumed.deliver_once('alice', 'watch-one')
            self.assertEqual(sent[0], sent[1])
            self.assertEqual(acknowledged['cursor'], 3)
            self.assertIsNone(acknowledged['pending'])
            self.assertEqual(resumed.deliver_once('alice', 'watch-one')['cursor'], 5)
        with self.delivery.transaction(readonly=True) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM acknowledgements').fetchone()[0], 1)

    def test_ack_must_match_exact_delivery_and_backoff_survives_restart(self):
        self.source.items = [{'id': 1}]
        with patch('mas_host.bridges.service_client.http_exchange', return_value=(200, {}, b'{"ack":"wrong"}')) as send:
            first = self.delivery.deliver_once('alice', 'watch-one')
            second = EventDelivery(self.root, self.source, {'observer': self.target}).deliver_once('alice', 'watch-one')
            self.assertEqual(send.call_count, 1)
        self.assertEqual(first, second)
        self.assertEqual(second['cursor'], 0)

    def test_revocation_prevents_staged_bytes_being_retransmitted(self):
        self.source.items = [{'id': 1, 'data': 'private'}]
        with patch('mas_host.bridges.service_client.http_exchange', side_effect=ServiceError('timeout')) as send:
            self.delivery.deliver_once('alice', 'watch-one')
            self.source.allowed = False
            with self.assertRaisesRegex(ServiceError, 'forbidden'):
                self.delivery.deliver_once('alice', 'watch-one')
            self.assertEqual(send.call_count, 1)

    def test_target_binding_change_and_arbitrary_target_are_rejected(self):
        changed = WebhookTarget('observer', 'http://127.0.0.1/changed', self.target.token_file, allow_loopback_http=True)
        reopened = EventDelivery(self.root, self.source, {'observer': changed})
        with self.assertRaisesRegex(ServiceError, 'webhook target unavailable'):
            reopened.deliver_once('alice', 'watch-one')
        with self.assertRaises(ServiceError):
            reopened.subscribe('alice', 'other', 'task-one', 'http://untrusted.example')

    def test_cursor_expiry_requires_explicit_resynchronisation(self):
        self.source.expired = True
        with self.assertRaisesRegex(ServiceError, 'event cursor expired'):
            self.delivery.deliver_once('alice', 'watch-one')
        self.assertEqual(self.delivery.inspect('alice', 'watch-one')['state'], 'resync_required')

    def test_stalled_delivery_does_not_lock_inspection_or_other_subscription(self):
        self.source.items = [{'id': 1}]
        entered, release = threading.Event(), threading.Event()
        def stalled(*args, **kwargs):
            entered.set()
            release.wait(2)
            raise ServiceError('timeout')
        with patch('mas_host.bridges.service_client.http_exchange', side_effect=stalled):
            thread = threading.Thread(target=self.delivery.deliver_once, args=('alice', 'watch-one'))
            thread.start()
            try:
                self.assertTrue(entered.wait(1))
                self.assertEqual(self.delivery.inspect('alice', 'watch-one')['cursor'], 0)
                self.delivery.subscribe('alice', 'watch-two', 'task-two', 'observer')
            finally:
                release.set()
                thread.join(3)
            self.assertFalse(thread.is_alive())

    def test_audit_restore_disables_retransmission(self):
        (self.root / '.restored-audit-only').touch()
        before = {path.name: path.read_bytes() for path in self.root.iterdir() if path.is_file()}
        reopened = EventDelivery(self.root, self.source, {'observer': self.target})
        self.assertEqual(reopened.inspect('alice', 'watch-one')['state'], 'active')
        self.assertEqual(before, {path.name: path.read_bytes() for path in self.root.iterdir() if path.is_file()})
        with self.assertRaisesRegex(ServiceError, 'audit only'):
            reopened.stop('alice', 'watch-one')
        with patch('mas_host.bridges.service_client.http_exchange') as send:
            with self.assertRaisesRegex(ServiceError, 'audit only'):
                self.delivery.deliver_once('alice', 'watch-one')
            send.assert_not_called()

    def test_wait_delivers_new_event_and_rechecks_revocation(self):
        def publish():
            time.sleep(0.05)
            self.source.items.append({'id': 9})
        thread = threading.Thread(target=publish)
        thread.start()
        try:
            self.assertEqual(wait_events(self.source, 'alice', 'task-one', wait_ms=500)['next_after'], 9)
        finally:
            thread.join(1)
        with self.assertRaisesRegex(ServiceError, 'forbidden'):
            wait_events(self.source, 'bob', 'task-one', wait_ms=0)

    def test_http_subscription_does_not_consume_all_management_slots(self):
        server = JoinedBridgeHTTPServer(('127.0.0.1', 0), service=self.source, verifier=verifier(),
            public_url='http://127.0.0.1', allowed_hosts=['127.0.0.1'], allow_loopback_http=True,
            max_connections=2, max_subscriptions=1, request_log=lambda _: None)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        def request(operation, payload):
            client = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=2)
            try:
                client.request('POST', '/service/' + operation, json.dumps(payload),
                    {'Host': '127.0.0.1', 'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
                response = client.getresponse()
                return response.status, json.loads(response.read())
            finally:
                client.close()
        subscriber = threading.Thread(target=request, args=('events_wait', {'task_id': 'task-one', 'wait_ms': 500}))
        try:
            subscriber.start()
            deadline = time.monotonic() + 1
            while server._subscription_slots._value and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(request('inspect', {'task_id': 'task-one'})[0], 200)
            self.assertEqual(request('events_wait', {'task_id': 'task-one', 'wait_ms': 0})[0], 503)
        finally:
            subscriber.join(2)
            server.shutdown()
            server.server_close()
            thread.join(2)


if __name__ == '__main__':
    unittest.main()
