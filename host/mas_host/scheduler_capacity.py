"""Shared single-machine concurrency admission with inherited lifetime locks.

Rows describe occupied slots; locks establish whether they remain occupied.
Elapsed time and PID reuse never release a slot. A surviving adapter guardian
retains the same descriptor, preventing replacement admission until it drains.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
import fcntl
import hashlib
import json
import os
from pathlib import Path

from .schema import Schema, connect
from .service_contracts import ServiceError


CAPACITY = Schema('scheduler capacity', 0x4D415351, 1, (), (
    'CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)',
    'CREATE TABLE slots(assignment TEXT PRIMARY KEY,principal TEXT NOT NULL,workflow TEXT NOT NULL,resources TEXT NOT NULL)',
))


@dataclass(frozen=True)
class SchedulerPolicy:
    max_workers: int = 4
    max_per_principal: int = 2
    workflows: dict = field(default_factory=dict)
    resources: dict = field(default_factory=dict)

    def __post_init__(self):
        for value in (self.max_workers, self.max_per_principal):
            if type(value) is not int or not 1 <= value <= 256:
                raise ServiceError('invalid_scheduler_policy')
        for mapping in (self.workflows, self.resources):
            if (type(mapping) is not dict or len(mapping) > 256
                    or any(type(key) is not str or not key or len(key) > 256
                           or type(value) is not int or not 1 <= value <= 256 for key, value in mapping.items())):
                raise ServiceError('invalid_scheduler_policy')
        object.__setattr__(self, 'workflows', dict(self.workflows))
        object.__setattr__(self, 'resources', dict(self.resources))


class SlotLease:
    def __init__(self, descriptor):
        self.descriptor = descriptor

    def close(self):
        if self.descriptor is not None:
            # Do not LOCK_UN: forked descendants may still own this description.
            os.close(self.descriptor)
            self.descriptor = None

    def __enter__(self):
        return self

    def __exit__(self, *_exception):
        self.close()


class SchedulerCapacity:
    def __init__(self, state_dir, policy=None, *, readonly=False):
        from .operations import _file_lock
        self.state_dir = Path(state_dir)
        self.path = self.state_dir / 'scheduler-capacity.sqlite'
        self.readonly = readonly
        if readonly and not self.path.exists():
            self.absent = True
            self.policy = policy or SchedulerPolicy()
            return
        self.absent = False
        def initialise(db):
            db.execute('INSERT INTO metadata VALUES(?,?)', ('policy', json.dumps(asdict(policy or SchedulerPolicy()), sort_keys=True)))
            db.execute('INSERT INTO metadata VALUES(?,?)', ('last_principal', ''))
        with _file_lock(self.state_dir / '.scheduler-capacity-init.lock', True) if not readonly else _no_lock():
            db = connect(self.path, CAPACITY, readonly=readonly, initialise=None if readonly else initialise)
            try:
                self.policy = SchedulerPolicy(**json.loads(db.execute("SELECT value FROM metadata WHERE key='policy'").fetchone()[0]))
                if policy is not None and policy != self.policy:
                    raise ServiceError('scheduler_policy_changed')
            finally:
                db.close()

    @contextmanager
    def transaction(self):
        if self.readonly:
            raise ServiceError('audit_only')
        db = connect(self.path, CAPACITY)
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _lock(self, assignment):
        digest = hashlib.sha256(assignment.encode()).hexdigest()
        fd = os.open(self.state_dir / ('.capacity-' + digest + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return None
        return fd

    def _active(self, db, own=None):
        active = []
        for row in db.execute('SELECT * FROM slots').fetchall():
            if row['assignment'] == own:
                continue
            descriptor = self._lock(row['assignment'])
            if descriptor is None:
                active.append(dict(row))
            else:
                db.execute('DELETE FROM slots WHERE assignment=?', (row['assignment'],))
                os.close(descriptor)
        return active

    def acquire(self, assignment, principal, workflow, *, resources=()):
        if self.readonly:
            raise ServiceError('audit_only')
        if (not all(type(value) is str and value and len(value) <= 256 for value in (assignment, principal, workflow))
                or len(resources) > 32 or any(type(value) is not str or not value or len(value) > 128 for value in resources)):
            raise ServiceError('invalid_scheduler_assignment')
        descriptor = self._lock(assignment)
        if descriptor is None:
            return None
        try:
            admitted = False
            with self.transaction() as db:
                active = self._active(db, assignment)
                denied = (len(active) >= self.policy.max_workers
                        or sum(row['principal'] == principal for row in active) >= self.policy.max_per_principal
                        or sum(row['workflow'] == workflow for row in active) >= self.policy.workflows.get(workflow, self.policy.max_workers)
                        or any(sum(key in json.loads(row['resources']) for row in active) >= self.policy.resources[key]
                               for key in set(resources) & self.policy.resources.keys()))
                if not denied:
                    db.execute('INSERT OR REPLACE INTO slots VALUES(?,?,?,?)',
                               (assignment, principal, workflow, json.dumps(sorted(set(resources)))))
                    db.execute("UPDATE metadata SET value=? WHERE key='last_principal'", (principal,))
                    admitted = True
            if not admitted:
                os.close(descriptor)
                return None
            return SlotLease(descriptor)
        except BaseException:
            os.close(descriptor)
            raise

    def fair_order(self, rows, *, principal='principal'):
        if not rows or self.absent:
            return rows
        db = connect(self.path, CAPACITY, readonly=True)
        try:
            last = db.execute("SELECT value FROM metadata WHERE key='last_principal'").fetchone()[0]
        finally:
            db.close()
        owners = sorted({row[principal] for row in rows})
        owners = [owner for owner in owners if owner > last] + [owner for owner in owners if owner <= last]
        from collections import deque
        queues = {owner: deque() for owner in owners}
        for row in rows:
            queues[row[principal]].append(row)
        # Interleave owners, retaining FIFO within each queue. Grouping all of
        # one owner first would make a small dispatcher pool repeatedly launch
        # that owner's denied work before reaching another owner.
        ordered = []
        while queues:
            for owner in tuple(queues):
                ordered.append(queues[owner].popleft())
                if not queues[owner]:
                    del queues[owner]
        return ordered


@contextmanager
def _no_lock():
    yield
