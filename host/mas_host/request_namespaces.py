"""Versioned request namespaces and durable, fail-closed epoch retirement.

Legacy arbitrary keys are never rewritten or forgotten. A retirement intent
closes all ingress before either owner compacts its epoch tombstones. Its two
owner acknowledgements are idempotent cross-store recovery, not one transaction.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
from pathlib import Path
import time

from .schema import Schema, SchemaError, connect
from .service_contracts import ServiceError


VERSIONED = 'mas-unified/1'
LEGACY = 'legacy'
OWNER_EPOCHS = "CREATE TABLE request_epoch_retirements(principal TEXT NOT NULL,namespace TEXT NOT NULL,epoch TEXT NOT NULL,retired_at INTEGER NOT NULL,PRIMARY KEY(principal,namespace,epoch))"
EPOCHS = Schema('request epochs', 0x4D415345, 1, (), (
    "CREATE TABLE retirements(principal TEXT NOT NULL,namespace TEXT NOT NULL,epoch TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('closing','retired')),reason TEXT NOT NULL,authoriser TEXT NOT NULL,prune INTEGER NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(principal,namespace,epoch))",
))


@dataclass(frozen=True)
class RequestScope:
    namespace: str = LEGACY
    epoch: str = ''

    def __post_init__(self):
        if (self.namespace not in {LEGACY, VERSIONED} or type(self.epoch) is not str
                or len(self.epoch.encode()) > 128 or any(ord(c) < 32 for c in self.epoch)
                or self.namespace == LEGACY and self.epoch):
            raise ServiceError('invalid_request_namespace')

    @classmethod
    def supplied(cls, namespace=None, epoch=None):
        if epoch is not None and (type(epoch) is not str or not epoch):
            raise ServiceError('invalid_request_epoch')
        return cls(namespace or (VERSIONED if epoch is not None else LEGACY), epoch if epoch is not None else '')

    @classmethod
    def retained(cls, item):
        return cls(item.get('request_namespace', LEGACY), item.get('request_epoch', ''))

    @property
    def values(self):
        return self.namespace, self.epoch

    def check_owner(self, db, principal):
        if self.epoch and db.execute('SELECT 1 FROM request_epoch_retirements WHERE principal=? AND namespace=? AND epoch=?',
                                     (principal, *self.values)).fetchone():
            raise ServiceError('request_epoch_retired')


def _lock_name(principal, scope):
    import json
    return '.request-epoch-' + hashlib.sha256(json.dumps([principal, *scope.values]).encode()).hexdigest() + '.lock'


class RequestEpochStore:
    def __init__(self, state_dir, *, readonly=False):
        self.state_dir, self.readonly = Path(state_dir), readonly
        self.path = self.state_dir / 'request-epochs.sqlite'
        self.absent = readonly and not self.path.exists()
        if self.absent:
            return
        from .operations import _file_lock
        from contextlib import nullcontext
        with (nullcontext() if readonly else _file_lock(self.state_dir / '.request-epochs-init.lock', True)):
            with self.transaction(readonly=readonly, initialise=not readonly):
                pass

    @contextmanager
    def transaction(self, *, readonly=False, initialise=False):
        if self.readonly and not readonly:
            raise ServiceError('audit_only')
        try:
            db = connect(self.path, EPOCHS, readonly=readonly,
                         initialise=(lambda _: None) if initialise else None)
            try:
                db.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
                yield db
                if not readonly:
                    db.commit()
            except BaseException:
                if not readonly:
                    db.rollback()
                raise
            finally:
                db.close()
        except SchemaError as error:
            raise ServiceError('unsupported_schema', str(error)) from None

    def inspect(self, principal, scope):
        if self.absent:
            return None
        with self.transaction(readonly=True) as db:
            row = db.execute('SELECT * FROM retirements WHERE principal=? AND namespace=? AND epoch=?',
                             (principal, *scope.values)).fetchone()
            return dict(row) if row else None

    def begin_retirement(self, principal, scope, reason, prune, authoriser=None):
        if scope.namespace != VERSIONED or not scope.epoch:
            raise ServiceError('request_namespace_not_retirable')
        authoriser = principal if authoriser is None else authoriser
        if (type(reason) is not str or not reason or len(reason.encode()) > 1024 or type(prune) is not bool
                or type(authoriser) is not str or not authoriser or len(authoriser.encode()) > 256):
            raise ServiceError('invalid_epoch_retirement')
        with self.transaction() as db:
            row = db.execute('SELECT * FROM retirements WHERE principal=? AND namespace=? AND epoch=?',
                             (principal, *scope.values)).fetchone()
            if row is not None:
                if row['reason'] != reason or bool(row['prune']) != prune:
                    raise ServiceError('request_conflict')
                return dict(row)
            db.execute("INSERT INTO retirements VALUES(?,?,?,'closing',?,?,?,?)",
                       (principal, *scope.values, reason, authoriser, int(prune), int(time.time() * 1000)))
        return self.inspect(principal, scope)

    def pending(self):
        if self.absent:
            return []
        with self.transaction(readonly=True) as db:
            return [dict(row) for row in db.execute("SELECT * FROM retirements WHERE state='closing'")]

    def complete(self, principal, scope):
        with self.transaction() as db:
            db.execute("UPDATE retirements SET state='retired' WHERE principal=? AND namespace=? AND epoch=?",
                       (principal, *scope.values))
        return self.inspect(principal, scope)


@contextmanager
def epoch_guard(operations, principal, scope):
    """Every ingress owner takes this guard before its owning transaction."""
    if not scope.epoch:
        yield
        return
    from .operations import _file_lock
    with _file_lock(operations.state_dir / _lock_name(principal, scope), True):
        store = RequestEpochStore(operations.state_dir, readonly=True)
        if store.inspect(principal, scope) is not None:
            raise ServiceError('request_epoch_retired')
        yield


def mark_owner_retired(db, principal, scope):
    db.execute('INSERT OR IGNORE INTO request_epoch_retirements VALUES(?,?,?,?)',
               (principal, *scope.values, int(time.time() * 1000)))


def retirement_guard(operations, principal, scope):
    """Same ownership as ingress, permitting inspection of a closing intent."""
    from .operations import _file_lock
    return _file_lock(operations.state_dir / _lock_name(principal, scope), True)


def owner_live(store, kind, principal, scope):
    table = 'tasks' if kind == 'leaf' else 'instances'
    child, foreign = ('executions', 'task_id') if kind == 'leaf' else ('attempts', 'instance')
    with store.transaction(readonly=True) as db:
        return bool(db.execute(f"SELECT 1 FROM {table} p WHERE principal=? AND request_namespace=? "
            "AND request_epoch=? AND (state NOT IN ('completed','failed','cancelled','expired') "
            f"OR EXISTS(SELECT 1 FROM {child} c WHERE c.{foreign}=p.id AND c.state IN ('running','unknown'))) LIMIT 1",
            (principal, *scope.values)).fetchone())


def retire_owner(store, kind, principal, scope, *, prune):
    with store.transaction() as db:
        mark_owner_retired(db, principal, scope)
        if not prune:
            return 0
        if kind == 'leaf':
            return db.execute("DELETE FROM requests WHERE principal=? AND request_namespace=? AND request_epoch=? "
                "AND task_id IN (SELECT id FROM tasks WHERE archived_at IS NOT NULL)",
                (principal, *scope.values)).rowcount
        return db.execute("DELETE FROM continuation_tombstones WHERE instance IN "
            "(SELECT id FROM archives WHERE principal=? AND request_namespace=? AND request_epoch=?)",
            (principal, *scope.values)).rowcount
