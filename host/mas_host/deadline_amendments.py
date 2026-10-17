"""Atomic, idempotent clock extensions delegated by lifecycle store owners."""
from __future__ import annotations

import hashlib
import json

from .codec import canonical_json
from .execution_policy import clock_status, transition_clock
from .service_contracts import ServiceError


DEADLINE_AMENDMENTS = (
    'CREATE TABLE deadline_amendments(owner_id TEXT NOT NULL,path TEXT NOT NULL,'
    'request_key TEXT NOT NULL,digest TEXT NOT NULL,revision INTEGER NOT NULL,'
    'receipt TEXT NOT NULL,PRIMARY KEY(owner_id,request_key),UNIQUE(owner_id,path,revision))'
)
_FIELDS = {'business_seconds', 'queue_seconds', 'pause_seconds'}


def request(*, operator, principal, request_key, reason, expected_revision, limits, path=None):
    """Snapshot a closed, bounded operator request before opening a transaction."""
    for value in (operator, principal, request_key, reason):
        if type(value) is not str or not value.strip() or len(value.encode('utf-8')) > 512:
            raise ServiceError('invalid_deadline_amendment')
    if (type(expected_revision) is not int or not 0 <= expected_revision < 2**63
            or type(limits) is not dict or not limits or not set(limits) <= _FIELDS
            or any(type(value) is not int or not 1 <= value <= 315_360_000 for value in limits.values())
            or path is not None and (type(path) is not str or not path or len(path.encode('utf-8')) > 1024)):
        raise ServiceError('invalid_deadline_amendment')
    return {'operator': operator, 'owner': principal, 'request_key': request_key, 'reason': reason,
            'expected_revision': expected_revision, 'limits': dict(limits), 'path': path}


def replay(db, owner, command):
    """Recover the exact amendment receipt even after archival or later edits."""
    digest = hashlib.sha256(canonical_json(command).encode()).hexdigest()
    old = db.execute('SELECT digest,receipt FROM deadline_amendments WHERE owner_id=? AND request_key=?',
                     (owner['id'], command['request_key'])).fetchone()
    if old is not None:
        if old['digest'] != digest:
            raise ServiceError('deadline_amendment_conflict')
        return json.loads(old['receipt'])
    return None


def apply(db, owner, command, timestamp, *, scope=None):
    """Prepare and record an extension in the caller's lifecycle transaction.

    The caller commits the returned clock and its lifecycle audit event in this
    same transaction. The immutable registration policy and timestamps remain
    unchanged. Request receipts survive lifecycle archival and event pruning.
    """
    old = replay(db, owner, command)
    if old is not None:
        return None, old
    path = command['path'] or ''
    digest = hashlib.sha256(canonical_json(command).encode()).hexdigest()
    if (owner.get('archived') or owner.get('archived_at') is not None or owner.get('cancel_requested')
            or owner['state'] not in {'queued', 'blocked', 'input_required', 'auth_required'}):
        raise ServiceError('deadline_extension_not_available')
    target = scope if scope is not None else owner
    clock = target.get('clock')
    if clock is None:
        raise ServiceError('legacy_deadline_immutable')
    observed = clock_status(clock, timestamp)
    if clock['phase'] in {'running', 'terminal'} or observed['expired']:
        raise ServiceError('deadline_extension_not_available')
    if observed['deadline_revision'] != command['expected_revision']:
        raise ServiceError('deadline_revision_conflict')
    effective = clock['policy'] | clock.get('extensions', {})
    if any(value < effective[key] for key, value in command['limits'].items()) or not any(
            value > effective[key] for key, value in command['limits'].items()):
        raise ServiceError('deadline_extension_must_increase')
    updated = transition_clock(clock, timestamp, clock['phase'])
    updated['extensions'] = updated['extensions'] | command['limits']
    updated['deadline_revision'] += 1
    after = clock_status(updated, timestamp)
    receipt = {'format': 'mas-deadline-amendment/1', 'task_id': owner['id'], **command,
               'revision': updated['deadline_revision'], 'observed_at_ms': timestamp,
               'previous_limits': {key: effective[key] for key in sorted(_FIELDS)},
               'effective_limits': {key: (updated['policy'] | updated['extensions'])[key] for key in sorted(_FIELDS)},
               'business_expires_ms': after['business_expires_ms'], 'execution_resumed': False}
    db.execute('INSERT INTO deadline_amendments VALUES(?,?,?,?,?,?)',
               (owner['id'], path, command['request_key'], digest, updated['deadline_revision'], canonical_json(receipt)))
    return updated, receipt
