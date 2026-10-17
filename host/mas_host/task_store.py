"""Version-aware repository for protocol-neutral submissions and task events.

Every transaction is short. Execution ownership is a separate process lock;
SQLite connections are never retained across a provider call or process fork.
Run directories are relative to the deployment, so audit restores cannot refer
back to their source deployment.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import time

from .schema import Schema, SchemaError, connect
from .service_contracts import ServiceError
from .deadline_amendments import DEADLINE_AMENDMENTS


SERVICE_V1 = Schema("task service", 0x4D415354, 1, (), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE workflows(name TEXT PRIMARY KEY,identity TEXT NOT NULL)",
    "CREATE TABLE tasks(id TEXT PRIMARY KEY,principal TEXT NOT NULL,request_key TEXT NOT NULL,workflow TEXT NOT NULL,workflow_identity TEXT NOT NULL,state TEXT NOT NULL,input TEXT NOT NULL,metadata TEXT NOT NULL,result TEXT,error TEXT,cancel_requested INTEGER NOT NULL DEFAULT 0,revision INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)",
    "CREATE TABLE executions(sequence INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,revision INTEGER NOT NULL,run_id TEXT UNIQUE NOT NULL,run_directory TEXT UNIQUE NOT NULL,input TEXT NOT NULL,state TEXT NOT NULL,UNIQUE(task_id,revision))",
    "CREATE TABLE requests(principal TEXT NOT NULL,request_key TEXT NOT NULL,digest TEXT NOT NULL,task_id TEXT NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(principal,request_key))",
    "CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,event TEXT NOT NULL,data TEXT NOT NULL,created_at INTEGER NOT NULL)",
    "CREATE INDEX task_owner ON tasks(principal,created_at,id)",
    "CREATE INDEX execution_queue ON executions(state,sequence)",
    "CREATE INDEX task_events ON events(task_id,id)",
))
_ARCHIVE_COLUMNS = (
    'ALTER TABLE tasks ADD COLUMN archived_at INTEGER',
    'ALTER TABLE tasks ADD COLUMN result_digest TEXT',
    'ALTER TABLE tasks ADD COLUMN event_floor INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE tasks ADD COLUMN clock TEXT',
)
SERVICE_V2 = Schema('task service', 0x4D415354, 2, (),
                 (*SERVICE_V1.statements, *_ARCHIVE_COLUMNS),
                 ((SERVICE_V1, _ARCHIVE_COLUMNS),))

SERVICE_V3 = Schema('task service', 0x4D415354, 3, (),
                 (*SERVICE_V2.statements, DEADLINE_AMENDMENTS),
                 ((SERVICE_V1, (*_ARCHIVE_COLUMNS, DEADLINE_AMENDMENTS)),
                  (SERVICE_V2, (DEADLINE_AMENDMENTS,))))

from .request_namespaces import OWNER_EPOCHS
_NAMESPACE_MIGRATION = (
    "ALTER TABLE tasks ADD COLUMN request_namespace TEXT NOT NULL DEFAULT 'legacy'",
    "ALTER TABLE tasks ADD COLUMN request_epoch TEXT NOT NULL DEFAULT ''",
    'ALTER TABLE requests RENAME TO requests_legacy',
    "CREATE TABLE requests(principal TEXT NOT NULL,request_key TEXT NOT NULL,digest TEXT NOT NULL,task_id TEXT NOT NULL,kind TEXT NOT NULL,request_namespace TEXT NOT NULL DEFAULT 'legacy',request_epoch TEXT NOT NULL DEFAULT '',PRIMARY KEY(principal,request_namespace,request_epoch,request_key))",
    'INSERT INTO requests(principal,request_key,digest,task_id,kind) SELECT principal,request_key,digest,task_id,kind FROM requests_legacy',
    'DROP TABLE requests_legacy', OWNER_EPOCHS,
)
SERVICE = Schema('task service', 0x4D415354, 4, (),
                 (*SERVICE_V3.statements, *_NAMESPACE_MIGRATION),
                 ((SERVICE_V1, (*_ARCHIVE_COLUMNS, DEADLINE_AMENDMENTS, *_NAMESPACE_MIGRATION)),
                  (SERVICE_V2, (DEADLINE_AMENDMENTS, *_NAMESPACE_MIGRATION)),
                  (SERVICE_V3, _NAMESPACE_MIGRATION)))


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def now() -> int:
    return int(time.time() * 1000)


def event(db, task_id: str, kind: str, data: dict) -> None:
    db.execute("INSERT INTO events(task_id,event,data,created_at) VALUES(?,?,?,?)",
               (task_id, kind, canonical(data), now()))


def decoded(row) -> dict:
    result = dict(row)
    for field in ("input", "result", "error", "data", "metadata", "clock"):
        if field in result and result[field] is not None:
            result[field] = json.loads(result[field])
    if "cancel_requested" in result:
        result["cancel_requested"] = bool(result["cancel_requested"])
    return result


class TaskStore:
    @staticmethod
    def extend_deadline(db, task, command, timestamp):
        from .deadline_amendments import apply
        clock, receipt = apply(db, task, command, timestamp)
        if clock is not None:
            db.execute('UPDATE tasks SET clock=?,updated_at=? WHERE id=?',
                       (canonical(clock), timestamp, task['id']))
            event(db, task['id'], 'deadline_extended', receipt)
        return receipt

    @staticmethod
    def retained_count(db):
        return db.execute('SELECT count(*) FROM tasks WHERE archived_at IS NULL').fetchone()[0]

    @staticmethod
    def check_event_cursor(task, after):
        if task.get('archived_at') is not None and after < task.get('event_floor', 0):
            raise ServiceError('event_cursor_expired')

    @staticmethod
    def archive(db, task):
        """Caller has exclusive deployment ownership and verified references."""
        import hashlib
        if task['state'] not in {'completed', 'failed', 'cancelled'} or task.get('archived_at') is not None:
            raise ServiceError('task_not_archivable')
        result = canonical(task['result']) if task['result'] is not None else None
        digest = hashlib.sha256(result.encode()).hexdigest() if result is not None else None
        floor = db.execute('SELECT coalesce(max(id),0) FROM events WHERE task_id=?', (task['id'],)).fetchone()[0]
        timestamp = now()
        archived_error = ({'archived': True, 'sha256': hashlib.sha256(canonical(task['error']).encode()).hexdigest()}
                          if task.get('error') is not None else None)
        db.execute("UPDATE tasks SET input='{}',result=NULL,error=?,archived_at=?,result_digest=?,event_floor=?,updated_at=? WHERE id=?",
                   (canonical(archived_error) if archived_error is not None else None, timestamp, digest, floor, timestamp, task['id']))
        db.execute('DELETE FROM executions WHERE task_id=?', (task['id'],))
        db.execute('DELETE FROM events WHERE task_id=?', (task['id'],))
        event(db, task['id'], 'archived', {'state': task['state'], 'result_available': False})

    @staticmethod
    def block_definition(db, task, execution, *, started):
        """Unavailable code closes dispatch, never erases an escaped effect."""
        state = "unknown" if started else "blocked"
        code = "definition_unavailable_reconciliation_required" if started else "definition_unavailable"
        db.execute("UPDATE tasks SET state=?,error=?,updated_at=? WHERE id=?",
                   (state, canonical({"code": code}), now(), task["id"]))
        db.execute("UPDATE executions SET state=? WHERE sequence=?", (state, execution["sequence"]))
        event(db, task["id"], "state_changed", {"state": state, "reason": code})

    @staticmethod
    def restore_definition(db, task_id, sequence):
        db.execute("UPDATE tasks SET state='queued',error=NULL,updated_at=? WHERE id=? AND state='blocked'",
                   (now(), task_id))
        db.execute("UPDATE executions SET state='queued' WHERE sequence=? AND state='blocked'", (sequence,))
        event(db, task_id, "definition_available", {"state": "queued"})

    def __init__(self, state_dir: Path, *, deployment_id: str, readonly: bool = False):
        from .operations import _file_lock
        self.path = Path(state_dir) / "task-service.sqlite"
        self.deployment_id = deployment_id

        def initialise(db):
            db.execute("INSERT INTO metadata VALUES('deployment_id',?)", (deployment_id,))

        try:
            from contextlib import nullcontext
            with (nullcontext() if readonly else _file_lock(Path(state_dir) / ".task-service-init.lock", True)):
                db = connect(self.path, SERVICE, readonly=readonly,
                             initialise=None if readonly else initialise,
                             validate_content=self._validate)
                db.close()
        except SchemaError as error:
            raise ServiceError("unsupported_schema", str(error)) from None

    def _validate(self, db):
        values = dict(db.execute("SELECT key,value FROM metadata"))
        if values != {"deployment_id": self.deployment_id}:
            raise ServiceError("deployment_mismatch")

    @contextmanager
    def transaction(self, *, readonly: bool = False):
        try:
            db = connect(self.path, SERVICE, readonly=readonly, validate_content=self._validate)
            try:
                if not readonly:
                    db.execute("BEGIN IMMEDIATE")
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
            raise ServiceError("unsupported_schema", str(error)) from None
