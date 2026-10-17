"""Durable local dispatcher drain with a shared admission/transition barrier."""
from contextlib import closing, contextmanager
import hashlib
import time

from .codec import canonical_json
from .operations import _file_lock
from .schema import Schema, connect
from .service_contracts import ServiceError

CONTROL = Schema("operator worker control", 0x4D41534C, 1, (), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE changes(revision INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,digest TEXT NOT NULL,operator TEXT NOT NULL,reason TEXT NOT NULL,draining INTEGER NOT NULL,created INTEGER NOT NULL)",
))


class WorkerControl:
    def __init__(self, operations):
        self.operations = operations
        self.path = operations.state_dir / "worker-control.sqlite"
        self.lock = operations.state_dir / ".worker-control.lock"
        # Inspection never creates state, including on an audit-only restore.

    def _open(self, *, readonly=False):
        db = connect(self.path, CONTROL, readonly=readonly,
            initialise=None if readonly else lambda db: db.execute("INSERT INTO metadata VALUES('deployment_id',?)", (self.operations.deployment_id,)))
        if dict(db.execute("SELECT key,value FROM metadata")) != {"deployment_id": self.operations.deployment_id}:
            db.close()
            raise ServiceError("deployment_mismatch")
        return db

    def status(self):
        if not self.path.exists() and not self.path.is_symlink():
            return {"format": "mas-worker-control/1", "revision": 0, "draining": False,
                    "active_effects_cancelled": False}
        with closing(self._open(readonly=True)) as db:
            row = db.execute("SELECT revision,request_key,operator,reason,draining,created FROM changes ORDER BY revision DESC LIMIT 1").fetchone()
        return {"format": "mas-worker-control/1", **(dict(row) if row else {"revision": 0, "draining": False}),
                "draining": bool(row["draining"]) if row else False, "active_effects_cancelled": False}

    @contextmanager
    def admission(self):
        # A drain receipt waits for any in-progress process launch. After the
        # receipt commits, no new launch crosses this barrier until resumed.
        with _file_lock(self.lock, False):
            yield not self.status()["draining"]

    def set(self, *, operator, request_key, reason, expected_revision, draining):
        from .recovery import _identity
        _identity(operator); _identity(request_key)
        if (type(reason) is not str or not reason.strip() or len(reason) > 2048
                or any(ord(char) < 32 for char in reason) or type(draining) is not bool
                or type(expected_revision) is not int or expected_revision < 0):
            raise ServiceError("invalid_worker_control")
        command = {"operator": operator, "reason": reason, "expected_revision": expected_revision, "draining": draining}
        digest = hashlib.sha256(canonical_json(command).encode()).hexdigest()
        with _file_lock(self.operations.state_dir / ".deployment.lock", False), _file_lock(self.lock, True):
            if self.operations.readonly or not self.operations.admission_enabled:
                raise ServiceError("audit_only")
            with closing(self._open()) as db, db:
                db.execute("BEGIN IMMEDIATE")
                old = db.execute("SELECT * FROM changes WHERE request_key=?", (request_key,)).fetchone()
                if old:
                    if old["digest"] != digest:
                        raise ServiceError("worker_control_conflict")
                    result = {key: old[key] for key in ("revision", "request_key", "operator", "reason", "draining", "created")}
                else:
                    revision = db.execute("SELECT coalesce(max(revision),0) FROM changes").fetchone()[0]
                    if revision != expected_revision:
                        raise ServiceError("worker_control_revision_conflict")
                    if revision >= 10000:
                        raise ServiceError("worker_control_history_full")
                    created = time.time_ns() // 1_000_000
                    db.execute("INSERT INTO changes VALUES(?,?,?,?,?,?,?)",
                        (revision + 1, request_key, digest, operator, reason, int(draining), created))
                    result = {"revision": revision + 1, "request_key": request_key, "operator": operator,
                              "reason": reason, "draining": draining, "created": created}
        return {"format": "mas-worker-control/1", **result, "draining": bool(result["draining"]),
                "active_effects_cancelled": False}
