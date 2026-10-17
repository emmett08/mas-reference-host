"""Bounded event subscriptions and durable, at-least-once webhook delivery.

Lifecycle events are committed with their source transition. A delivery stages
one immutable page before sending it; acknowledgement advances its cursor in
the same transaction that removes that page. A lost acknowledgement repeats
the same delivery ID and bytes. No network call holds a database transaction.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import re
import time

from .codec import integer_json
from .observability import event_page_arguments, validate_event_page
from .operations import _file_lock, _private_dir
from .schema import Schema, SchemaError, connect
from .service_contracts import ServiceError


def wait_events(service, principal, task_id, *, after=0, limit=100, wait_ms=20_000,
                cancelled=lambda: False):
    """Authorise every read, return on progress or a bounded subscription timeout."""
    event_page_arguments(after, limit)
    if type(wait_ms) is not int or not 0 <= wait_ms <= 25_000:
        raise ServiceError('invalid_subscription_timeout')
    deadline = time.monotonic() + wait_ms / 1000
    while True:
        value = service.events_page(principal, task_id, after=after, limit=limit)
        validate_event_page(value, after=after, limit=limit)
        remaining = deadline - time.monotonic()
        if value['events'] or remaining <= 0 or cancelled():
            return value
        time.sleep(min(0.1, remaining))


EVENT_DELIVERY = Schema('event deliveries', 0x4D41534E, 1, (), (
    'CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)',
    'CREATE TABLE subscriptions(id TEXT PRIMARY KEY,descriptor TEXT NOT NULL,principal TEXT NOT NULL,task_id TEXT NOT NULL,cursor INTEGER NOT NULL,state TEXT NOT NULL,created INTEGER NOT NULL)',
    'CREATE TABLE pending(subscription TEXT PRIMARY KEY,delivery_id TEXT UNIQUE NOT NULL,body TEXT NOT NULL,next_after INTEGER NOT NULL,attempts INTEGER NOT NULL,next_attempt INTEGER NOT NULL,last_error TEXT,FOREIGN KEY(subscription) REFERENCES subscriptions(id))',
    'CREATE TABLE acknowledgements(subscription TEXT PRIMARY KEY,delivery_id TEXT NOT NULL,through_event INTEGER NOT NULL,acknowledged INTEGER NOT NULL)',
))


def _identifier(value):
    if type(value) is not str or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,199}', value) is None:
        raise ServiceError('invalid_subscription')
    return value


@dataclass(frozen=True)
class WebhookTarget:
    """Operator-selected destination; service request bodies never select URLs."""
    name: str
    endpoint: str
    token_file: Path = field(repr=False)
    allow_loopback_http: bool = False
    timeout_seconds: int = 5
    transport_policy: object = field(default=None, repr=False)

    def __post_init__(self):
        from .bridges.service_client import validate_endpoint
        _identifier(self.name)
        validate_endpoint(self.endpoint, allow_loopback_http=self.allow_loopback_http)
        if type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 60:
            raise ServiceError('invalid_webhook_target')
        if type(self.allow_loopback_http) is not bool:
            raise ServiceError('invalid_webhook_target')
        if self.transport_policy is not None:
            from .bridges.http_policy import HTTPTransportPolicy
            if not isinstance(self.transport_policy, HTTPTransportPolicy):
                raise ServiceError('invalid_webhook_target')

    @property
    def descriptor(self):
        return {'format': 'mas-webhook-target/1', 'name': self.name,
                'endpoint': self.endpoint, 'timeout_seconds': self.timeout_seconds,
                'allow_loopback_http': self.allow_loopback_http,
                'transport': self.transport_policy.descriptor() if self.transport_policy else None}


class EventDelivery:
    """Durable bounded subscriptions; grants and secret resolution stay external.

    One subscription has at most one pending page. Requests are serialised only
    per subscription, including its network wait. Stored acknowledgements are
    compact, with the latest receipt retained per subscription. Source retention
    can expire a cursor; that stops the subscription for explicit resynchronisation.
    """
    def __new__(cls, state_dir, service, targets, **options):
        if cls is EventDelivery and hasattr(service, 'authority'):
            from .shared_events import SharedEventDelivery
            return SharedEventDelivery(service.authority, service, targets, **options)
        return super().__new__(cls)

    def __init__(self, state_dir, service, targets, *, max_subscriptions=1000, page_size=100):
        self.state_dir, self.service = _private_dir(Path(state_dir), create=True), service
        self.operations = getattr(service, 'operations', None)
        self.deployment_id = self.operations.deployment_id if self.operations is not None else 'standalone'
        if self.operations is not None and self.operations.state_dir.resolve() != self.state_dir.resolve():
            raise ServiceError('deployment_mismatch')
        self.targets = dict(targets)
        if any(not isinstance(v, WebhookTarget) or k != v.name for k, v in self.targets.items()):
            raise ServiceError('invalid_webhook_target')
        if type(max_subscriptions) is not int or not 1 <= max_subscriptions <= 10_000:
            raise ServiceError('invalid_subscription_limit')
        event_page_arguments(0, page_size)
        self.max_subscriptions, self.page_size = max_subscriptions, page_size
        self.path = self.state_dir / 'event-delivery.sqlite'
        if self.path.exists() or self.path.is_symlink():
            with self.transaction(readonly=True):
                pass
        else:
            self._admission()
            with _file_lock(self.state_dir / '.event-delivery-init.lock', True):
                with self.transaction(initialise=True):
                    pass

    @contextmanager
    def transaction(self, *, readonly=False, initialise=False):
        barrier = nullcontext() if readonly else _file_lock(self.state_dir / '.deployment.lock', False)
        with barrier:
            if not readonly:
                self._admission()
            try:
                db = connect(self.path, EVENT_DELIVERY, readonly=readonly,
                    initialise=(lambda db: db.execute("INSERT INTO metadata VALUES('deployment_id',?)",
                        (self.deployment_id,))) if initialise else None)
                try:
                    db.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
                    if dict(db.execute('SELECT key,value FROM metadata')) != {'deployment_id': self.deployment_id}:
                        raise ServiceError('deployment_mismatch')
                    yield db
                    if not readonly:
                        db.commit()
                except BaseException:
                    if not readonly:
                        db.rollback()
                    raise
                finally:
                    db.close()
            except SchemaError:
                raise ServiceError('unsupported_schema') from None

    def _admission(self):
        marker = self.state_dir / '.restored-audit-only'
        if (marker.exists() or marker.is_symlink() or self.operations is not None
                and (self.operations.readonly or not self.operations.admission_enabled)):
            raise ServiceError('audit_only')

    def subscribe(self, principal, subscription_id, task_id, target, *, after=0):
        for value in (principal, subscription_id, task_id, target):
            _identifier(value)
        event_page_arguments(after, self.page_size)
        if target not in self.targets:
            raise ServiceError('webhook_target_unavailable')
        # Establish ownership before reading or creating subscription state.
        self.service.events_page(principal, task_id, after=after, limit=1)
        descriptor = integer_json({'target': self.targets[target].descriptor,
                                   'principal': principal, 'task_id': task_id, 'after': after})
        with self.transaction() as db:
            old = db.execute('SELECT * FROM subscriptions WHERE id=?', (subscription_id,)).fetchone()
            if old is not None:
                if old['principal'] != principal:
                    raise ServiceError('forbidden')
                if old['descriptor'] != descriptor:
                    raise ServiceError('subscription_conflict')
            else:
                if db.execute('SELECT COUNT(*) FROM subscriptions').fetchone()[0] >= self.max_subscriptions:
                    raise ServiceError('subscription_limit')
                db.execute('INSERT INTO subscriptions VALUES(?,?,?,?,?,?,?)',
                           (subscription_id, descriptor, principal, task_id, after, 'active', int(time.time())))
        return self.inspect(principal, subscription_id)

    def _record(self, principal, subscription_id):
        with self.transaction(readonly=True) as db:
            row = db.execute('SELECT * FROM subscriptions WHERE id=?', (subscription_id,)).fetchone()
        if row is None or row['principal'] != principal:
            raise ServiceError('subscription_not_found')
        self.service.inspect(principal, row['task_id'])
        return dict(row)

    def inspect(self, principal, subscription_id):
        row = self._record(principal, subscription_id)
        with self.transaction(readonly=True) as db:
            pending = db.execute('SELECT delivery_id,attempts,next_attempt,last_error FROM pending WHERE subscription=?',
                                 (subscription_id,)).fetchone()
        return {'subscription_id': subscription_id, 'task_id': row['task_id'],
                'cursor': row['cursor'], 'state': row['state'], 'pending': dict(pending) if pending else None}

    def stop(self, principal, subscription_id):
        self._record(principal, subscription_id)
        with self.transaction() as db:
            db.execute("UPDATE subscriptions SET state='stopped' WHERE id=?", (subscription_id,))
        return self.inspect(principal, subscription_id)

    def deliver_once(self, principal, subscription_id):
        _identifier(subscription_id)
        lock_id = hashlib.sha256(subscription_id.encode()).hexdigest()
        # Maintenance excludes an in-flight delivery; independent subscriptions
        # and normal lifecycle transactions only share this deployment lock.
        with _file_lock(self.state_dir / '.deployment.lock', False):
            self._admission()
            with _file_lock(self.state_dir / ('.event-delivery-' + lock_id + '.lock'), True, nonblocking=True):
                return self._deliver_owned(principal, subscription_id)

    def _deliver_owned(self, principal, subscription_id):
        row = self._record(principal, subscription_id)
        if row['state'] != 'active':
            return self.inspect(principal, subscription_id)
        descriptor = json.loads(row['descriptor'])['target']
        target = self.targets.get(descriptor['name'])
        if target is None or target.descriptor != descriptor:
            raise ServiceError('webhook_target_unavailable')
        with self.transaction(readonly=True) as db:
            pending = db.execute('SELECT * FROM pending WHERE subscription=?', (subscription_id,)).fetchone()
        if pending is None:
            try:
                page = self.service.events_page(principal, row['task_id'], after=row['cursor'], limit=self.page_size)
                validate_event_page(page, after=row['cursor'], limit=self.page_size)
            except ServiceError as error:
                if error.code == 'event_cursor_expired':
                    with self.transaction() as db:
                        db.execute("UPDATE subscriptions SET state='resync_required' WHERE id=?", (subscription_id,))
                raise
            if not page['events']:
                return self.inspect(principal, subscription_id)
            payload = {'format': 'mas-events/1', 'subscription_id': subscription_id,
                       'task_id': row['task_id'], 'after': row['cursor'], **page}
            delivery_id = hashlib.sha256(integer_json(payload).encode()).hexdigest()
            body = integer_json({**payload, 'delivery_id': delivery_id}, 262144)
            with self.transaction() as db:
                db.execute('INSERT INTO pending VALUES(?,?,?,?,?,?,?)',
                           (subscription_id, delivery_id, body, page['next_after'], 0, 0, None))
                pending = db.execute('SELECT * FROM pending WHERE subscription=?', (subscription_id,)).fetchone()
        now = int(time.time())
        if pending['next_attempt'] > now:
            return self.inspect(principal, subscription_id)
        # Recheck visibility and cancellation immediately before dispatch. A
        # concurrent revocation cannot retract already transmitted bytes.
        self.service.inspect(principal, row['task_id'])
        with self.transaction() as db:
            active = db.execute('SELECT state FROM subscriptions WHERE id=?', (subscription_id,)).fetchone()[0] == 'active'
            if active:
                attempt = pending['attempts'] + 1
                db.execute('UPDATE pending SET attempts=?,next_attempt=?,last_error=? WHERE subscription=?',
                           (attempt, now + min(3600, 2 ** min(attempt, 12)), 'acknowledgement_unknown', subscription_id))
        if not active:
            return self.inspect(principal, subscription_id)
        from .bridges.authentication import read_token_file
        from .bridges.service_client import http_exchange, decode_integer_json
        try:
            token = read_token_file(target.token_file)
            status, _, raw = http_exchange(target.endpoint, body=pending['body'].encode(),
                headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token,
                         'Idempotency-Key': pending['delivery_id']},
                timeout_seconds=target.timeout_seconds, max_response_bytes=4096,
                allow_loopback_http=target.allow_loopback_http, transport_policy=target.transport_policy)
            acknowledgement = decode_integer_json(raw, max_bytes=4096)
            if status != 200 or acknowledgement != {'ack': pending['delivery_id']}:
                raise ServiceError('webhook_not_acknowledged')
        except (ServiceError, OSError):
            # Fixed diagnostics only; tokens, endpoints and peer bodies stay private.
            return self.inspect(principal, subscription_id)
        with self.transaction() as db:
            db.execute('INSERT OR REPLACE INTO acknowledgements VALUES(?,?,?,?)',
                       (subscription_id, pending['delivery_id'], pending['next_after'], int(time.time())))
            db.execute('UPDATE subscriptions SET cursor=? WHERE id=?', (pending['next_after'], subscription_id))
            db.execute('DELETE FROM pending WHERE subscription=? AND delivery_id=?', (subscription_id, pending['delivery_id']))
        return self.inspect(principal, subscription_id)
