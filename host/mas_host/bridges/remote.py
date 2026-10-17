"""Durable protocol-neutral remote effects with admission before every dispatch.

A lost first receipt is unresolved. Neither reopening a store nor repeating a
submission sends it again. Lifecycle operations use a fresh admitted invocation;
no adapter context is retained by a background poller.
"""
from __future__ import annotations

from contextlib import contextmanager, ExitStack, nullcontext
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import time
from typing import Protocol

from ..codec import integer_json
from ..contracts import AdapterContext, Invocation
from ..journal import canonical
from ..operations import _file_lock, _private_dir
from ..schema import Schema, SchemaError, JOURNAL, LEDGER, connect
from ..service_contracts import ServiceError
from .remote_ownership import own_remote_task

STATES = frozenset({'queued', 'running', 'blocked', 'input_required', 'auth_required', 'completed', 'failed', 'cancelled', 'unknown'})
TERMINAL = frozenset({'completed', 'failed', 'cancelled'})
REMOTE_TASKS = Schema('remote bridge tasks', 0x4D415352, 1, (), (
    'CREATE TABLE remote_tasks(key TEXT PRIMARY KEY,logical_key TEXT NOT NULL,namespace TEXT NOT NULL,binding TEXT NOT NULL,descriptor TEXT NOT NULL,input TEXT NOT NULL,state TEXT NOT NULL,remote_id TEXT,context_id TEXT,result TEXT,host_run_dir TEXT NOT NULL,call_id TEXT NOT NULL,cancel_requested INTEGER NOT NULL DEFAULT 0,pending_action TEXT,created INTEGER NOT NULL,updated INTEGER NOT NULL)',
    'CREATE TABLE operations(key TEXT NOT NULL,operation_key TEXT NOT NULL,action TEXT NOT NULL,binding TEXT NOT NULL,state TEXT NOT NULL,output TEXT,call_id TEXT NOT NULL,host_run_dir TEXT NOT NULL,PRIMARY KEY(key,operation_key))',
    'CREATE TABLE audit(id INTEGER PRIMARY KEY,key TEXT NOT NULL,event TEXT NOT NULL,evidence TEXT NOT NULL,created INTEGER NOT NULL)',
))


def token(value: str) -> str:
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,199}', value):
        raise ServiceError('invalid_identifier')
    return value


def peer_identifier(value: str) -> str:
    """Keep peer-assigned JSON identities opaque within a UTF-8 byte bound.

    These values are persisted and compared exactly, never used as local paths,
    call IDs or unescaped HTTP headers. Local operator keys still use ``token``.
    """
    try:
        if type(value) is str and 1 <= len(value.encode('utf-8')) <= 1024:
            return value
    except UnicodeEncodeError:
        pass
    raise ServiceError('invalid_identifier')


def bounded(value: dict, limit: int = 48_000) -> dict:
    if type(value) is not dict:
        raise ServiceError('invalid_payload')
    try:
        return json.loads(integer_json(value, limit))
    except Exception:
        raise ServiceError('invalid_payload') from None


def digest(value: dict) -> str:
    return hashlib.sha256(canonical(bounded(value)).encode()).hexdigest()


@dataclass(frozen=True)
class RemoteSnapshot:
    remote_id: str | None
    state: str
    result: dict | None = None
    context_id: str | None = None

    def validate(self):
        if type(self.state) is not str or self.state not in STATES or (self.remote_id is None and self.state not in TERMINAL):
            raise ServiceError('invalid_remote_snapshot')
        if self.remote_id is not None:
            peer_identifier(self.remote_id)
        if self.context_id is not None:
            peer_identifier(self.context_id)
        if self.result is not None:
            bounded(self.result)
        return self


class RemoteClient(Protocol):
    def descriptor(self) -> dict: ...
    def submit(self, payload: dict) -> RemoteSnapshot: ...
    def poll(self, remote_id: str) -> RemoteSnapshot: ...
    def cancel(self, remote_id: str) -> RemoteSnapshot: ...
    def update(self, remote_id: str, payload: dict) -> RemoteSnapshot: ...


class RemoteTaskStore:
    def __init__(self, state_dir: Path):
        self.state_dir = Path(state_dir).absolute()
        self.path = self.state_dir / 'remote-tasks.sqlite'

    def require_mutable(self):
        if (self.state_dir / '.restored-audit-only').exists():
            raise ServiceError('audit_only_restore')
        ledger = self.state_dir / 'deployment.sqlite'
        if not ledger.exists():
            raise ServiceError('deployment_inactive')
        if ledger.exists():
            try:
                db = connect(ledger, LEDGER, readonly=True)
                try:
                    row = db.execute("SELECT value FROM metadata WHERE key='admission_state'").fetchone()
                    if row is None or row[0] != 'active':
                        raise ServiceError('deployment_inactive')
                finally:
                    db.close()
            except SchemaError:
                raise ServiceError('unsupported_schema') from None

    @contextmanager
    def open(self, *, readonly=False):
        """Open one short transaction boundary, never a network-call lifetime.

        Writers serialise initialisation and read/modify/write decisions. Readers
        retain a SQLite snapshot without taking the global remote writer lock.
        """
        _private_dir(self.state_dir, create=not readonly)
        writer = (nullcontext() if readonly else
                  _file_lock(self.state_dir / '.remote-tasks.lock', True))
        with _file_lock(self.state_dir / '.deployment.lock', False, nonblocking=True), \
                writer:
            if not readonly:
                self.require_mutable()
            try:
                db = connect(self.path, REMOTE_TASKS, readonly=readonly,
                             initialise=None if readonly else lambda _: None)
            except SchemaError:
                raise ServiceError('unsupported_schema') from None
            try:
                if readonly:
                    db.execute('BEGIN')
                yield db
            finally:
                db.close()

    def ownership(self, key):
        return own_remote_task(self.state_dir, key)

    def verify_admission(self, directory: Path, call_id: str, reservation: int):
        directory = Path(directory).absolute()
        if directory.parent != self.state_dir or directory.is_symlink():
            raise ServiceError('invalid_host_directory')
        token(call_id)
        try:
            journal = connect(directory / 'host.sqlite', JOURNAL, readonly=True)
            try:
                row = journal.execute('SELECT state,reservation_microusd FROM invocations WHERE call_id=?', (call_id,)).fetchone()
                run = journal.execute("SELECT value FROM metadata WHERE key='run_id'").fetchone()
                if row is None or row['state'] != 'executing' or row['reservation_microusd'] != reservation or run is None:
                    raise ServiceError('admission_required')
                run_id = json.loads(run[0])
            finally:
                journal.close()
            ledger = connect(self.state_dir / 'deployment.sqlite', LEDGER, readonly=True)
            try:
                row = ledger.execute('SELECT calls.reserved_microusd,runs.directory,runs.purged_at FROM calls JOIN runs USING(run_id) WHERE run_id=? AND call_id=?', (run_id, call_id)).fetchone()
                if row is None or row[0] != reservation or row[1] != directory.name or row[2] is not None:
                    raise ServiceError('admission_required')
            finally:
                ledger.close()
        except ServiceError:
            raise
        except Exception:
            raise ServiceError('admission_required') from None

    @staticmethod
    def row(db, key):
        row = db.execute('SELECT * FROM remote_tasks WHERE key=?', (token(key),)).fetchone()
        if row is None:
            raise ServiceError('task_not_found')
        result = dict(row)
        try:
            if result['key'] != hashlib.sha256((result['namespace'] + '\x00' + token(result['logical_key'])).encode()).hexdigest():
                raise ValueError()
            if result['state'] not in STATES or result['cancel_requested'] not in (0, 1):
                raise ValueError()
            if digest({'descriptor': json.loads(result['descriptor']), 'input': json.loads(result['input'])}) != result['binding']:
                raise ValueError()
            if (Path(result['host_run_dir']).name != result['host_run_dir']
                    or result['host_run_dir'] in ('', '.', '..')):
                raise ValueError()
            if result['remote_id'] is not None:
                peer_identifier(result['remote_id'])
            if result['context_id'] is not None:
                peer_identifier(result['context_id'])
            if result['result'] is not None:
                bounded(json.loads(result['result']))
        except Exception:
            raise ServiceError('inconsistent_remote_state') from None
        return result

    @staticmethod
    def public(row):
        return {'key': row['logical_key'], 'binding': row['binding'], 'state': row['state'],
                'remote_id': row['remote_id'], 'context_id': row['context_id'],
                'cancel_requested': bool(row['cancel_requested']), 'pending_action': row['pending_action'],
                'result': json.loads(row['result']) if row['result'] else None,
                'host_run_dir': row['host_run_dir'], 'call_id': row['call_id'],
                'created': row['created'], 'updated': row['updated']}

    @staticmethod
    def audit(db, key, event, evidence):
        db.execute('INSERT INTO audit(key,event,evidence,created) VALUES(?,?,?,?)',
                   (key, event, canonical(bounded(evidence)), int(time.time())))

    def status(self, key):
        with self.open(readonly=True) as db:
            return self.public(self.row(db, key))


class RemoteTaskService:
    """Admission-aware durable effects; lifecycle mapping belongs to the client."""
    def __init__(self, store: RemoteTaskStore, client: RemoteClient, *, namespace: str):
        if type(namespace) is not str or not 1 <= len(namespace.encode('utf-8')) <= 4096:
            raise ServiceError('trusted_namespace_required')
        self.namespace = hashlib.sha256(namespace.encode('utf-8')).hexdigest()
        self.store, self.client = store, client
        self.identity = bounded(client.descriptor(), 8192)

    def _key(self, logical_key):
        return hashlib.sha256((self.namespace + '\x00' + token(logical_key)).encode()).hexdigest()

    def _identity(self, row):
        if row['namespace'] != self.namespace:
            raise ServiceError('task_not_found')
        if canonical(bounded(self.client.descriptor(), 8192)) != canonical(self.identity):
            raise ServiceError('remote_identity_conflict')
        if canonical(self.identity) != row['descriptor']:
            raise ServiceError('remote_identity_conflict')

    @contextmanager
    def _dispatch_ownership(self, key):
        with ExitStack() as owner:
            try:
                owner.enter_context(self.store.ownership(key))
            except ServiceError as error:
                if error.code == 'remote_task_busy':
                    # Only acquisition failure proves this invocation never
                    # dispatched. Do not catch errors raised by the peer/body.
                    from ..provider_outcomes import not_sent
                    raise not_sent('custom', 'operation_busy') from None
                raise
            yield

    @staticmethod
    def _snapshot(db, row, snapshot, *, clear_pending=False):
        snapshot.validate()
        if row['remote_id'] is not None and snapshot.remote_id != row['remote_id']:
            raise ServiceError('remote_identity_conflict')
        if row['context_id'] is not None and snapshot.context_id not in (None, row['context_id']):
            raise ServiceError('remote_identity_conflict')
        if row['state'] in TERMINAL:
            return
        db.execute('UPDATE remote_tasks SET remote_id=?,context_id=?,state=?,result=?,pending_action=?,updated=? WHERE key=?',
                   (snapshot.remote_id, snapshot.context_id or row['context_id'], snapshot.state,
                    canonical(snapshot.result) if snapshot.result is not None else None,
                    None if clear_pending else row['pending_action'], int(time.time()), row['key']))

    def status(self, key):
        with self.store.open(readonly=True) as db:
            row = self.store.row(db, self._key(key))
            self._identity(row)
            return self.store.public(row)

    def inspect(self, key):
        """Owner-scoped receipt and operation keys for explicit reconciliation."""
        with self.store.open(readonly=True) as db:
            row = self.store.row(db, self._key(key))
            self._identity(row)
            return {'receipt': self.store.public(row),
                    'operations': [dict(item) for item in db.execute(
                        'SELECT operation_key,action,state,call_id,host_run_dir FROM operations WHERE key=? ORDER BY operation_key',
                        (row['key'],))],
                    'audit': [{'event': item['event'], 'evidence': json.loads(item['evidence']), 'created': item['created']}
                              for item in db.execute('SELECT event,evidence,created FROM audit WHERE key=? ORDER BY id', (row['key'],))]}

    def submit(self, key: str, payload: dict, *, context: AdapterContext, host_run_dir: Path,
               reservation_microusd: int) -> dict:
        logical_key = token(key)
        key = self._key(logical_key)
        payload = bounded(payload)
        fingerprint = digest({'descriptor': self.identity, 'input': payload})
        if type(reservation_microusd) is not int or not 0 <= reservation_microusd <= 100_000_000:
            raise ServiceError('invalid_reservation')
        def execute():
            call_id, directory = context.invocation_id, Path(host_run_dir).name
            with self._dispatch_ownership(key):
                with self.store.open() as db:
                    self.store.verify_admission(host_run_dir, call_id, reservation_microusd)
                    if canonical(bounded(self.client.descriptor(), 8192)) != canonical(self.identity):
                        raise ServiceError('remote_identity_conflict')
                    previous = db.execute('SELECT * FROM remote_tasks WHERE key=?', (key,)).fetchone()
                    if previous:
                        row = self.store.row(db, key)
                        if row['binding'] != fingerprint:
                            raise ServiceError('idempotency_conflict')
                        self._identity(row)
                        if row['remote_id'] is None and row['state'] not in TERMINAL:
                            raise ServiceError('remote_outcome_unknown')
                        return Invocation(self.store.public(row), {'remote_key': key, 'replayed': True})
                    # Unknown is committed before sending. SIGKILL at any later
                    # point cannot authorise a duplicate initial request.
                    with db:
                        now = int(time.time())
                        db.execute('INSERT INTO remote_tasks(key,logical_key,namespace,binding,descriptor,input,state,host_run_dir,call_id,pending_action,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                                   (key, logical_key, self.namespace, fingerprint, canonical(self.identity), canonical(payload), 'unknown',
                                    directory, call_id, 'submit', now, now))
                        self.store.audit(db, key, 'submit_intent', {'binding': fingerprint})
                snapshot = self.client.submit(payload).validate()
                with self.store.open() as db:
                    row = self.store.row(db, key)
                    self._identity(row)
                    if (row['binding'] != fingerprint or row['call_id'] != call_id
                            or row['host_run_dir'] != directory or row['pending_action'] != 'submit'
                            or row['remote_id'] is not None or row['state'] != 'unknown'):
                        raise ServiceError('remote_operation_changed')
                    with db:
                        self._snapshot(db, row, snapshot, clear_pending=True)
                        self.store.audit(db, key, 'submit_received', {'remote_id': snapshot.remote_id, 'state': snapshot.state})
                    output = self.store.public(self.store.row(db, key))
                return Invocation(output, {'remote_key': key, 'submission_only': snapshot.state not in TERMINAL})
        return context.invoke(execute, input={'remote': self.identity, 'namespace': self.namespace, 'key': logical_key, 'binding': fingerprint, 'action': 'submit'},
                              reservation_microusd=reservation_microusd)

    def _operate(self, key, action, *, context, host_run_dir, reservation_microusd=0,
                 request_key=None, payload=None, remote_id=None, evidence=None):
        logical_key = token(key)
        key = self._key(logical_key)
        if action in ('update', 'cancel'):
            token(request_key)
        payload = bounded(payload or {})
        evidence = bounded(evidence or {})
        operation_key = action + ':' + (request_key or context.actor + ':' + str(time.time_ns()))
        operation_binding = digest({'action': action, 'payload': payload, 'remote_id': remote_id, 'evidence': evidence})
        def execute():
            call_id, directory = context.invocation_id, Path(host_run_dir).name
            with self._dispatch_ownership(key):
                with self.store.open() as db:
                    self.store.verify_admission(host_run_dir, call_id, reservation_microusd)
                    row = self.store.row(db, key)
                    self._identity(row)
                    previous = db.execute('SELECT * FROM operations WHERE key=? AND operation_key=?', (key, operation_key)).fetchone()
                    if previous:
                        if previous['binding'] != operation_binding:
                            raise ServiceError('idempotency_conflict')
                        if previous['state'] != 'completed':
                            raise ServiceError('remote_outcome_unknown')
                        return Invocation(json.loads(previous['output']), {'remote_key': key, 'replayed': True})
                    if row['state'] in TERMINAL:
                        return Invocation(self.store.public(row), {'remote_key': key, 'terminal': True})
                    selected = row['remote_id']
                    if action == 'attach':
                        if selected is not None or not evidence:
                            raise ServiceError('reconciliation_evidence_required')
                        selected = peer_identifier(remote_id)
                    if selected is None:
                        raise ServiceError('remote_outcome_unknown')
                    if action == 'update' and (row['state'] not in ('input_required', 'auth_required') or row['pending_action'] or row['cancel_requested']):
                        raise ServiceError('task_not_continuable')
                    if action == 'cancel' and row['pending_action']:
                        # One unresolved mutation owns the task until evidenced
                        # resolution; cancellation cannot replace that intent.
                        raise ServiceError('remote_outcome_unknown')
                    if action == 'cancel' and row['cancel_requested']:
                        raise ServiceError('cancellation_already_requested')
                    initial_binding = row['binding']
                    with db:
                        db.execute('INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)',
                                   (key, operation_key, action, operation_binding, 'unknown', None,
                                    call_id, directory))
                        if action in ('update', 'cancel'):
                            db.execute('UPDATE remote_tasks SET pending_action=?,cancel_requested=?,updated=? WHERE key=?',
                                       (action, 1 if action == 'cancel' else row['cancel_requested'], int(time.time()), key))
                        self.store.audit(db, key, action + '_intent', {'operation_key': operation_key, 'remote_id': selected})
                if action in ('poll', 'attach'):
                    snapshot = self.client.poll(selected).validate()
                elif action == 'cancel':
                    snapshot = self.client.cancel(selected).validate()
                else:
                    snapshot = self.client.update(selected, payload).validate()
                if snapshot.remote_id != selected:
                    raise ServiceError('remote_identity_conflict')
                with self.store.open() as db:
                    current = self.store.row(db, key)
                    self._identity(current)
                    operation = db.execute('SELECT * FROM operations WHERE key=? AND operation_key=?', (key, operation_key)).fetchone()
                    if (current['binding'] != initial_binding or operation is None
                            or operation['state'] != 'unknown' or operation['action'] != action
                            or operation['binding'] != operation_binding or operation['call_id'] != call_id
                            or operation['host_run_dir'] != directory
                            or current['remote_id'] != row['remote_id']
                            or action in ('update', 'cancel') and current['pending_action'] != action):
                        raise ServiceError('remote_operation_changed')
                    with db:
                        self._snapshot(db, current, snapshot, clear_pending=action in ('update', 'cancel', 'attach'))
                        output = self.store.public(self.store.row(db, key))
                        db.execute("UPDATE operations SET state='completed',output=? WHERE key=? AND operation_key=?",
                                   (canonical(output), key, operation_key))
                        self.store.audit(db, key, action + '_received', {'state': snapshot.state, 'operator': evidence})
                return Invocation(output, {'remote_key': key, 'action': action})
        return context.invoke(execute, input={'remote': self.identity, 'namespace': self.namespace, 'key': logical_key, 'action': action,
                                             'request_key': request_key, 'binding': operation_binding},
                              reservation_microusd=reservation_microusd)

    def resolve_operation(self, key, operation_key, *, evidence: dict, decision: str) -> dict:
        """Evidenced operator resolution of a lost continuation/cancel reply.

        No request is sent. Failed read-only observations can be recorded as
        observation_unavailable without changing task state or pending effects.
        A not-applied decision permits a new operation key;
        repeating the original key returns this recorded resolution. Submission
        uncertainty must be resolved with attach and an admitted exact peer poll.
        """
        key = self._key(key)
        if type(operation_key) is not str or len(operation_key) > 256:
            raise ServiceError('invalid_identifier')
        evidence = bounded(evidence, 8192)
        if not evidence or decision not in ('applied', 'not_applied', 'observation_unavailable'):
            raise ServiceError('reconciliation_evidence_required')
        with self.store.ownership(key), self.store.open() as db:
            row = self.store.row(db, key)
            self._identity(row)
            op = db.execute('SELECT * FROM operations WHERE key=? AND operation_key=?', (key, operation_key)).fetchone()
            if op is None or op['action'] not in ('update', 'cancel', 'poll', 'attach') or op['state'] != 'unknown':
                raise ServiceError('operation_not_resolvable')
            observation = op['action'] in ('poll', 'attach')
            if (observation and decision != 'observation_unavailable') or (not observation and decision == 'observation_unavailable'):
                raise ServiceError('operation_not_resolvable')
            if not observation and row['pending_action'] != op['action']:
                raise ServiceError('operation_not_resolvable')
            with db:
                db.execute('UPDATE remote_tasks SET pending_action=?,cancel_requested=?,state=?,updated=? WHERE key=?',
                           (row['pending_action'] if observation else None,
                            0 if op['action'] == 'cancel' and decision == 'not_applied' else row['cancel_requested'],
                            'unknown' if op['action'] == 'update' and decision == 'applied' and row['state'] not in TERMINAL else row['state'],
                            int(time.time()), key))
                output = self.store.public(self.store.row(db, key))
                db.execute("UPDATE operations SET state='completed',output=? WHERE key=? AND operation_key=?",
                           (canonical(output), key, operation_key))
                self.store.audit(db, key, 'operator_resolved', {'operation_key': operation_key,
                                 'decision': decision, 'evidence': evidence})
            return output

    def poll(self, key, **kwargs):
        return self._operate(key, 'poll', **kwargs)

    def cancel(self, key, *, request_key='cancel', **kwargs):
        return self._operate(key, 'cancel', request_key=request_key, **kwargs)

    def update(self, key, *, request_key, payload, **kwargs):
        return self._operate(key, 'update', request_key=request_key, payload=payload, **kwargs)

    def attach(self, key, remote_id, *, evidence, **kwargs):
        return self._operate(key, 'attach', remote_id=remote_id, evidence=evidence, **kwargs)
