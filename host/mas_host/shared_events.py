"""Durable shared webhook outbox; only verified source events enter a delivery."""
import hashlib
import uuid

from .codec import integer_json
from .event_delivery import WebhookTarget, _identifier
from .observability import event_page_arguments, validate_event_page
from .service_contracts import ServiceError


class SharedEventDelivery:
    def __init__(self, authority, service, targets, *, max_subscriptions=1000, page_size=100):
        self.authority, self.service = authority, service
        self.targets = dict(targets)
        if any(not isinstance(target, WebhookTarget) or name != target.name for name, target in self.targets.items()):
            raise ServiceError('invalid_webhook_target')
        if type(max_subscriptions) is not int or not 1 <= max_subscriptions <= 10000:
            raise ServiceError('invalid_subscription_limit')
        event_page_arguments(0, page_size)
        self.page_size = page_size
        configuration = {'max_subscriptions': max_subscriptions, 'page_size': page_size}
        identity = hashlib.sha256(integer_json(configuration).encode()).hexdigest()
        self._command('configure', configuration, command_id='event-config-' + identity)

    def _command(self, operation, payload, *, command_id=None):
        return self.authority.command('event.' + operation, payload, command_id=command_id)['receipt']

    def _read(self, principal, subscription_id):
        value = self.authority.query('event.read', {'principal': principal, 'subscription_id': subscription_id})
        self.service.inspect(principal, value['record']['task_id'])
        return value

    def inspect(self, principal, subscription_id):
        value = self._read(principal, subscription_id)
        row, pending = value['record'], value['pending']
        return {'subscription_id': subscription_id, 'task_id': row['task_id'],
            'cursor': row['cursor'], 'state': row['state'], 'pending': None if pending is None else {
                'delivery_id': pending['delivery_id'], 'attempts': pending['attempts'],
                'next_attempt': pending['next_attempt'] // 1000, 'last_error': pending['last_error']}}

    def subscribe(self, principal, subscription_id, task_id, target, *, after=0):
        for value in (principal, subscription_id, task_id, target):
            _identifier(value)
        event_page_arguments(after, self.page_size)
        if target not in self.targets:
            raise ServiceError('webhook_target_unavailable')
        self.service.events_page(principal, task_id, after=after, limit=1)
        descriptor = {'target': self.targets[target].descriptor, 'principal': principal,
                      'task_id': task_id, 'after': after}
        self._command('subscribe', {'principal': principal, 'subscription_id': subscription_id,
            'task_id': task_id, 'descriptor': descriptor, 'after': after})
        return self.inspect(principal, subscription_id)

    def stop(self, principal, subscription_id):
        self._read(principal, subscription_id)
        self._command('stop', {'principal': principal, 'subscription_id': subscription_id})
        return self.inspect(principal, subscription_id)

    def deliver_once(self, principal, subscription_id):
        state = self._read(principal, subscription_id)
        row, pending = state['record'], state['pending']
        if row['state'] != 'active':
            return self.inspect(principal, subscription_id)
        descriptor = row['descriptor']['target']
        target = self.targets.get(descriptor['name'])
        if target is None or target.descriptor != descriptor:
            raise ServiceError('webhook_target_unavailable')
        scope = {'principal': principal, 'subscription_id': subscription_id}
        if pending is None:
            try:
                page = self.service.events_page(principal, row['task_id'], after=row['cursor'], limit=self.page_size)
                validate_event_page(page, after=row['cursor'], limit=self.page_size)
            except ServiceError as error:
                if error.code == 'event_cursor_expired':
                    self._command('expire', scope)
                raise
            if not page['events']:
                return self.inspect(principal, subscription_id)
            payload = {'format': 'mas-events/1', 'subscription_id': subscription_id,
                'task_id': row['task_id'], 'after': row['cursor'], **page}
            delivery_id = hashlib.sha256(integer_json(payload).encode()).hexdigest()
            body = integer_json({**payload, 'delivery_id': delivery_id}, 262144)
            pending = self._command('stage', {**scope, 'expected_cursor': row['cursor'],
                'delivery_id': delivery_id, 'body': body, 'next_after': page['next_after']})
        self.service.inspect(principal, row['task_id'])
        attempt_owner = 'attempt-' + uuid.uuid4().hex
        grant = self._command('begin_attempt', {**scope, 'delivery_id': pending['delivery_id'],
            'attempt_owner': attempt_owner, 'timeout_seconds': target.timeout_seconds})
        if not grant['send']:
            return self.inspect(principal, subscription_id)
        # A replay is never requested automatically. If sending or its reply is
        # lost, the retained immutable page is retried after the durable backoff.
        from .bridges.authentication import read_token_file
        from .bridges.service_client import http_exchange, decode_integer_json
        try:
            token = read_token_file(target.token_file)
            status, _, raw = http_exchange(target.endpoint, body=pending['body'].encode(),
                headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token,
                    'Idempotency-Key': pending['delivery_id']}, timeout_seconds=target.timeout_seconds,
                max_response_bytes=4096, allow_loopback_http=target.allow_loopback_http,
                transport_policy=target.transport_policy)
            if status != 200 or decode_integer_json(raw, max_bytes=4096) != {'ack': pending['delivery_id']}:
                raise ServiceError('webhook_not_acknowledged')
        except (ServiceError, OSError):
            return self.inspect(principal, subscription_id)
        self._command('ack', {**scope, 'delivery_id': pending['delivery_id'], 'attempt_owner': attempt_owner})
        return self.inspect(principal, subscription_id)
