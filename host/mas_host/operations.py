"""Single-host deployment admissions, health, private backup and retention.

Reserved expenditure is an admission allocation in micro-US dollars. It is not
an invoice, a price estimate, or a substitute for a provider account limit.
"""
from __future__ import annotations

from contextlib import closing, contextmanager, ExitStack
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import time
import uuid

from .schema import JOBS, JOURNAL, LEDGER, SchemaError, connect, validate


class OperationsError(RuntimeError):
    """Fixed, credential-free operational diagnostic."""


@dataclass(frozen=True)
class DeploymentPolicy:
    max_runs: int = 1000
    max_calls: int = 10000
    max_reserved_microusd: int = 100_000_000
    min_free_bytes: int = 64 * 1024 * 1024
    max_state_bytes: int = 1024 * 1024 * 1024
    per_call_reservation_microusd: int = 100_000

    def __post_init__(self):
        if any(type(value) is not int or value <= 0 or value > 2**63 - 1
               for value in asdict(self).values()):
            raise OperationsError("deployment policy values must be positive bounded integers")
        if self.per_call_reservation_microusd > self.max_reserved_microusd:
            raise OperationsError("call reservation exceeds deployment expenditure allocation")


def _private_dir(path: Path, create: bool = False) -> Path:
    path = Path(path).absolute()
    # Do not silently follow a symlink supplied as the state directory or any
    # existing parent; this also applies to backup and restoration paths.
    for parent in reversed([path, *path.parents]):
        if parent.is_symlink():
            raise OperationsError("state and backup paths must not contain symlinks")
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.is_dir() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise OperationsError("state and backup directories must be private (mode 0700)")
    return path


@contextmanager
def _file_lock(path: Path, exclusive: bool, nonblocking: bool = False):
    if path.is_symlink():
        raise OperationsError("lock path must not be a symlink")
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                    | (fcntl.LOCK_NB if nonblocking else 0))
        yield
    except BlockingIOError:
        raise OperationsError("deployment or run is busy; stop runners before this operation") from None
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _state_size(path: Path) -> int:
    from .storage_accounting import state_size, StorageAccountingError
    try:
        return state_size(path)
    except StorageAccountingError as error:
        raise OperationsError(str(error)) from None


@contextmanager
def _db(path: Path, readonly: bool = False):
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise OperationsError("database must be a regular file")
    existed = path.exists()
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=10) if readonly else sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys=ON')
    try:
        known = {"deployment.sqlite": LEDGER, "host.sqlite": JOURNAL,
                 "cursor-jobs.sqlite": JOBS}.get(path.name)
        # Import boundary repositories only when inspected. Their runtime
        # services use the operational locks, so importing them at module load
        # would introduce a circular dependency.
        if path.name == "task-service.sqlite":
            from .task_store import SERVICE
            known = SERVICE
        elif path.name == "remote-tasks.sqlite":
            from .bridges.remote import REMOTE_TASKS
            known = REMOTE_TASKS
        elif path.name == "composition.sqlite":
            from .workflow_store import COMPOSITION
            known = COMPOSITION
        elif path.name == "request-epochs.sqlite":
            from .request_namespaces import EPOCHS
            known = EPOCHS
        elif path.name == "definition-registry.sqlite":
            from .definitions import DEFINITIONS
            known = DEFINITIONS
        elif path.name == "scheduler-capacity.sqlite":
            from .scheduler_capacity import CAPACITY
            known = CAPACITY
        elif path.name == 'remote-workers.sqlite':
            from .distributed import REMOTE
            known = REMOTE
        elif path.name == 'provider-circuits.sqlite':
            from .circuit_breaker import CIRCUITS
            known = CIRCUITS
        elif path.name == 'peer-registry.sqlite':
            from .peer_registry import PEERS
            known = PEERS
        if known and existed:
            validate(connection, known)
    except SchemaError as error:
        connection.close()
        raise OperationsError(str(error)) from None
    except Exception:
        connection.close()
        raise
    connection.execute("PRAGMA busy_timeout=10000")
    if not readonly:
        connection.execute("PRAGMA synchronous=EXTRA")
        os.chmod(path, 0o600)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


class DeploymentLedger:
    """Atomic lifetime quotas shared by every host using one state directory.

    Policy changes require an explicit, audited budget amendment. Existing policy is loaded when no policy is
    supplied. Repeated admissions with the same identity are idempotent, and
    purging run data never returns an admission allocation.
    """
    def __init__(self, state_dir: Path, policy: DeploymentPolicy | None = None,
                 *, readonly: bool = False, principal_quotas: dict | None = None):
        self.state_dir = _private_dir(Path(state_dir), create=not readonly)
        self.path = self.state_dir / "deployment.sqlite"
        self._readonly = readonly

        def validate_metadata(db):
            values = dict(db.execute("SELECT key,value FROM metadata"))
            required = {"policy", "deployment_id", "created_at", "admission_state"}
            if not required.issubset(values) or set(values) - required - {"restored_at"}:
                raise OperationsError("incomplete or unsupported deployment metadata")
            if values["admission_state"] not in ("active", "restored_audit_only"):
                raise OperationsError("unsupported deployment admission state")
            try:
                saved = DeploymentPolicy(**json.loads(values["policy"]))
                if policy is not None and saved != policy:
                    raise OperationsError("deployment policy is immutable; use its existing policy")
            except (ValueError, TypeError):
                raise OperationsError("invalid saved deployment policy") from None
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='principal_quotas'").fetchone():
                from .principal_quotas import quota_configuration
                try:
                    stored = {row[0]: json.loads(row[1]) for row in db.execute(
                        'SELECT principal,policy FROM principal_quotas')}
                    if quota_configuration(stored) != stored:
                        raise OperationsError('incomplete saved principal quotas')
                except (ValueError, TypeError):
                    raise OperationsError('invalid saved principal quotas') from None

        def initialise(db):
            chosen = policy or DeploymentPolicy()
            db.executemany("INSERT INTO metadata VALUES(?,?)", [
                ("policy", json.dumps(asdict(chosen), sort_keys=True)),
                ("deployment_id", "deployment-" + uuid.uuid4().hex),
                ("created_at", str(int(time.time()))),
                ("admission_state", "active"),
            ])

        def open_ledger(*, read_only: bool, current_only: bool = False):
            if not self.path.exists() and (self.state_dir / ".restored-audit-only").exists():
                raise OperationsError("restored deployment ledger is incomplete; admissions are disabled")
            if not self.path.exists() and any(self.state_dir.glob("*/state.sqlite")):
                raise OperationsError("existing run state has no deployment ledger; migrate explicitly into a new deployment")
            try:
                connection = connect(self.path, LEDGER, readonly=read_only,
                                     initialise=None if read_only else initialise,
                                     validate_content=validate_metadata)
                try:
                    if current_only and validate(connection, LEDGER):
                        return False
                    return True
                finally:
                    connection.close()
            except SchemaError as error:
                raise OperationsError(str(error)) from None

        if readonly:
            open_ledger(read_only=True)
        else:
            # Opening an unchanged ledger must coexist with an executing worker,
            # which retains a shared deployment lock for its whole lifetime.
            # Read/validate under that same lock; maintenance still needs EX.
            unchanged = False
            with _file_lock(self.state_dir / ".deployment.lock", False):
                fresh = not self.path.exists()
                if self.path.is_symlink() or not fresh and not self.path.is_file():
                    raise OperationsError("database must be a regular file")
                if not fresh:
                    # A writable owner must allow SQLite's native crash rollback
                    # before the read-only unchanged-layout fast path.
                    with closing(sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True)) as check:
                        check.execute('PRAGMA query_only=ON')
                        fresh = (check.execute("PRAGMA application_id").fetchone()[0] == 0
                                 and check.execute("PRAGMA user_version").fetchone()[0] == 0
                                 and check.execute("SELECT count(*) FROM sqlite_master").fetchone()[0] == 0)
                    if not fresh and open_ledger(read_only=True, current_only=True):
                        unchanged = True
            # Initialisation and recognised migrations require quiescence.
            # Reopen and validate after acquiring EX: another constructor or
            # maintenance operation may have changed state since the SH read.
            if not unchanged:
                with _file_lock(self.state_dir / ".deployment.lock", True):
                    open_ledger(read_only=False)
        if principal_quotas is not None:
            self._check_principal_configuration(principal_quotas)

    def _check_principal_configuration(self, supplied):
        from .principal_quotas import quota_configuration
        requested = quota_configuration(supplied)
        with _db(self.path, readonly=True) as db:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='principal_quotas'").fetchone()
            installed = {row['principal']: json.loads(row['policy']) for row in db.execute(
                'SELECT principal,policy FROM principal_quotas')} if exists else {}
        if installed == requested:
            return
        if installed or self._readonly or not exists:
            raise OperationsError('principal quota policy changed or unavailable')
        # Initial policy installation is maintenance. Existing executing workers
        # and accepted submissions must drain before this authority changes.
        with _file_lock(self.state_dir / '.deployment.lock', True, nonblocking=True), _db(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            if not self._admission_enabled(db):
                raise OperationsError('restored deployment is audit-only; principal quota changes disabled')
            installed = {row['principal']: json.loads(row['policy']) for row in db.execute(
                'SELECT principal,policy FROM principal_quotas')}
            if installed:
                if installed != requested:
                    raise OperationsError('principal quota policy changed')
                return
            db.executemany('INSERT INTO principal_quotas VALUES(?,?)',
                           ((owner, json.dumps(policy, sort_keys=True)) for owner, policy in requested.items()))

    def principal_policy(self, principal):
        from .principal_quotas import principal_identity
        principal_identity(principal)
        with _db(self.path, readonly=True) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='principal_quotas'").fetchone():
                return None
            row = db.execute('SELECT policy FROM principal_quotas WHERE principal=?', (principal,)).fetchone()
            return json.loads(row['policy']) if row else None

    @staticmethod
    def _principal_usage(db, principal):
        row = db.execute('SELECT count(*),coalesce(sum(c.reserved_microusd),0) FROM calls c '
            'LEFT JOIN run_principals p ON c.run_id=p.run_id WHERE p.principal=? OR p.principal IS NULL',
            (principal,)).fetchone()
        unknown = db.execute('SELECT count(*),coalesce(sum(c.reserved_microusd),0) FROM calls c '
            'LEFT JOIN run_principals p ON c.run_id=p.run_id WHERE p.principal IS NULL').fetchone()
        return {'calls': row[0], 'reserved_microusd': row[1],
                'unattributed_calls': unknown[0], 'unattributed_reserved_microusd': unknown[1]}

    def principal_usage(self, principal):
        """Trusted operator inspection; uncertainty is included conservatively."""
        from .principal_quotas import principal_identity
        principal_identity(principal)
        with _db(self.path, readonly=True) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='run_principals'").fetchone():
                totals = self._limits(db)
                return {'calls': totals['calls'], 'reserved_microusd': totals['reserved_microusd'],
                        'unattributed_calls': totals['calls'], 'unattributed_reserved_microusd': totals['reserved_microusd']}
            return self._principal_usage(db, principal)

    @property
    def policy(self) -> DeploymentPolicy:
        """Load amendments for long-lived hosts; never cache admission limits."""
        with _db(self.path, readonly=True) as db:
            return self._policy(db)

    @staticmethod
    def _policy(db) -> DeploymentPolicy:
        return DeploymentPolicy(**json.loads(db.execute(
            "SELECT value FROM metadata WHERE key='policy'").fetchone()[0]))

    def budget_history(self) -> list[dict]:
        with _db(self.path, readonly=True) as db:
            # Read-only inspection of the previous format must not migrate it.
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='budget_amendments'").fetchone():
                return []
            return [{**dict(row), 'policy': json.loads(row['policy'])} for row in db.execute(
                'SELECT revision,request_key,principal,reason,policy,created FROM budget_amendments ORDER BY revision')]

    def amend_budget(self, *, request_key: str, principal: str, reason: str,
                     expected_revision: int, limits: dict) -> dict:
        """Increase lifetime allowances without rewriting consumed admissions.

        This is a trusted local operator action, performed on a quiescent
        deployment. Its key binds both the change and the operator attribution.
        A replay returns the original receipt even after later amendments.
        """
        if self._readonly:
            raise OperationsError('read-only deployment cannot amend budgets')
        for value, maximum in ((request_key, 256), (principal, 256), (reason, 2048)):
            if type(value) is not str or not value.strip() or len(value) > maximum or any(ord(c) < 32 for c in value):
                raise OperationsError('budget amendment requires bounded operator attribution')
        allowed = {'max_runs', 'max_calls', 'max_reserved_microusd'}
        if (type(expected_revision) is not int or expected_revision < 0 or type(limits) is not dict
                or not limits or set(limits) - allowed
                or any(type(v) is not int or not 0 < v <= 2**63 - 1 for v in limits.values())):
            raise OperationsError('invalid budget amendment')
        source = json.dumps({'principal': principal, 'reason': reason, 'limits': limits,
                             'expected_revision': expected_revision}, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(source.encode()).hexdigest()
        with _file_lock(self.state_dir / '.deployment.lock', True, nonblocking=True), _db(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            if not self._admission_enabled(db):
                raise OperationsError('restored deployment is audit-only; budget amendment disabled')
            existing = db.execute('SELECT * FROM budget_amendments WHERE request_key=?', (request_key,)).fetchone()
            if existing:
                if existing['digest'] != digest:
                    raise OperationsError('budget amendment idempotency conflict')
                return {key: json.loads(existing[key]) if key == 'policy' else existing[key]
                        for key in ('revision', 'request_key', 'principal', 'reason', 'policy', 'created')}
            revision = db.execute('SELECT coalesce(max(revision),0) FROM budget_amendments').fetchone()[0]
            if revision != expected_revision:
                raise OperationsError('budget amendment revision conflict')
            old = asdict(self._policy(db))
            if any(value < old[key] for key, value in limits.items()) or not any(value > old[key] for key, value in limits.items()):
                raise OperationsError('budget amendment must increase at least one lifetime limit without decreasing another')
            policy = asdict(DeploymentPolicy(**{**old, **limits}))
            encoded, now = json.dumps(policy, sort_keys=True), int(time.time())
            db.execute("UPDATE metadata SET value=? WHERE key='policy'", (encoded,))
            db.execute('INSERT INTO budget_amendments VALUES(?,?,?,?,?,?,?)',
                       (revision + 1, request_key, digest, principal, reason, encoded, now))
            return {'revision': revision + 1, 'request_key': request_key, 'principal': principal,
                    'reason': reason, 'policy': policy, 'created': now}

    @property
    def admission_enabled(self) -> bool:
        with _db(self.path, readonly=True) as db:
            return self._admission_enabled(db)

    @property
    def readonly(self) -> bool:
        """Whether this ledger handle permits mutations and admissions."""
        return self._readonly

    @property
    def deployment_id(self) -> str:
        """Read identity without scanning unrelated operational repositories."""
        with _db(self.path, readonly=True) as db:
            row = db.execute("SELECT value FROM metadata WHERE key='deployment_id'").fetchone()
            if row is None:
                raise OperationsError("deployment identity is missing")
            return row[0]

    def _admission_enabled(self, db: sqlite3.Connection) -> bool:
        row = db.execute("SELECT value FROM metadata WHERE key='admission_state'").fetchone()
        return not (self.state_dir / ".restored-audit-only").exists() and row is not None and row[0] == "active"

    def _limits(self, db: sqlite3.Connection) -> dict:
        return {
            "runs": db.execute("SELECT count(*) FROM runs").fetchone()[0],
            "calls": db.execute("SELECT count(*) FROM calls").fetchone()[0],
            "reserved_microusd": db.execute("SELECT coalesce(sum(reserved_microusd),0) FROM calls").fetchone()[0],
        }

    def _current_state_size(self):
        from .storage_accounting import StateSizeIndex, StorageAccountingError
        if not hasattr(self, '_storage_accounting'):
            self._storage_accounting = StateSizeIndex(self.state_dir)
        try:
            return self._storage_accounting.measure()
        except StorageAccountingError as error:
            raise OperationsError(str(error)) from None

    def check_storage_admission(self):
        """Reject new work when a storage admission threshold has been reached.

        These thresholds are not a physical storage quota: already admitted
        work still needs to record results, cancellation and reconciliation.
        Callers hold their deployment/repository mutation locks around checks
        and new input records. Identical retained requests need no new admission.
        """
        size = self._current_state_size()
        free = shutil.disk_usage(self.state_dir).free
        if free < self.policy.min_free_bytes:
            raise OperationsError("deployment admission closed: free disk threshold reached")
        if size >= self.policy.max_state_bytes:
            raise OperationsError("deployment admission closed: state size limit reached")

    def admit_run(self, run_id: str, run_dir: Path, *, principal: str | None = None) -> bool:
        if self._readonly:
            raise OperationsError("read-only deployment cannot admit work")
        if principal is not None:
            from .principal_quotas import principal_identity
            principal_identity(principal)
        run_dir = Path(run_dir).absolute()
        if not isinstance(run_id, str) or not run_id or len(run_id) > 128:
            raise OperationsError("invalid run identity")
        if run_dir.parent != self.state_dir or run_dir.is_symlink():
            raise OperationsError("run directory must be a direct private deployment child")
        with _file_lock(self.state_dir / ".deployment.lock", False), _db(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT directory,purged_at FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if existing:
                if existing[0] != run_dir.name or existing[1] is not None:
                    raise OperationsError("run admission identity is unavailable")
                binding = db.execute('SELECT principal FROM run_principals WHERE run_id=?', (run_id,)).fetchone()
                if binding is not None and principal is not None and binding[0] != principal:
                    raise OperationsError('run allocation principal conflict')
                if binding is None and principal is not None:
                    if not self._admission_enabled(db):
                        raise OperationsError('audit-only deployment cannot attribute run ownership')
                    db.execute('INSERT INTO run_principals VALUES(?,?)', (run_id, principal))
                return False
            if not self._admission_enabled(db):
                raise OperationsError("restored deployment is audit-only; new work admissions are disabled")
            self.check_storage_admission()
            if self._limits(db)["runs"] >= self.policy.max_runs:
                raise OperationsError("deployment admission closed: lifetime run limit reached")
            if principal is None and db.execute('SELECT 1 FROM principal_quotas LIMIT 1').fetchone():
                raise OperationsError('principal quota requires attributed run ownership')
            db.execute("INSERT INTO runs VALUES(?,?,?,NULL)", (run_id, run_dir.name, int(time.time())))
            if principal is not None:
                db.execute('INSERT INTO run_principals VALUES(?,?)', (run_id, principal))
            return True

    def admit_call(self, run_id: str, call_id: str, reservation_microusd: int | None = None) -> bool:
        if self._readonly:
            raise OperationsError("read-only deployment cannot admit work")
        allocation = self.policy.per_call_reservation_microusd if reservation_microusd is None else reservation_microusd
        if type(allocation) is not int or not 0 <= allocation <= self.policy.max_reserved_microusd:
            raise OperationsError("invalid call expenditure allocation")
        if not isinstance(call_id, str) or not call_id or len(call_id) > 256:
            raise OperationsError("invalid call identity")
        with _file_lock(self.state_dir / ".deployment.lock", False), _db(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            if not self._admission_enabled(db):
                raise OperationsError("restored deployment is audit-only; all call admissions are disabled")
            admitted = db.execute("SELECT purged_at FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if admitted is None or admitted[0] is not None:
                raise OperationsError("call requires an active deployment run admission")
            existing = db.execute("SELECT reserved_microusd FROM calls WHERE run_id=? AND call_id=?", (run_id, call_id)).fetchone()
            if existing:
                if existing[0] != allocation:
                    raise OperationsError("call allocation changed for an admitted identity")
                return False
            binding = db.execute('SELECT principal FROM run_principals WHERE run_id=?', (run_id,)).fetchone()
            if binding is None and db.execute('SELECT 1 FROM principal_quotas LIMIT 1').fetchone():
                raise OperationsError('principal quota requires attributed run ownership')
            if binding is not None:
                row = db.execute('SELECT policy FROM principal_quotas WHERE principal=?', (binding[0],)).fetchone()
                if row is not None:
                    policy = json.loads(row[0])
                    usage = self._principal_usage(db, binding[0])
                    if usage['calls'] >= policy['max_calls']:
                        raise OperationsError('principal lifetime call limit reached')
                    if allocation > policy['max_reserved_microusd'] - usage['reserved_microusd']:
                        raise OperationsError('principal expenditure allocation exhausted')
            self.check_storage_admission()
            usage = self._limits(db)
            if usage["calls"] >= self.policy.max_calls:
                raise OperationsError("deployment admission closed: lifetime call limit reached")
            if allocation > self.policy.max_reserved_microusd - usage["reserved_microusd"]:
                raise OperationsError("deployment admission closed: expenditure allocation exhausted")
            from .accounting import reserve_periods
            timestamp = int(time.time())
            reserve_periods(db, run_id, call_id, binding[0] if binding is not None else None, allocation, timestamp)
            db.execute("INSERT INTO calls VALUES(?,?,?,?)", (run_id, call_id, allocation, timestamp))
            return True

    def has_run(self, run_id: str) -> bool:
        """Check even purged admissions without changing a budget."""
        with _file_lock(self.state_dir / ".deployment.lock", False), _db(self.path, readonly=True) as db:
            return db.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone() is not None

    def health(self) -> dict:
        with _db(self.path, readonly=True) as db:
            db.execute("BEGIN")
            usage = self._limits(db)
            identity = db.execute("SELECT value FROM metadata WHERE key='deployment_id'").fetchone()[0]
            enabled = self._admission_enabled(db)
            retained = db.execute("SELECT count(*) FROM runs WHERE purged_at IS NULL").fetchone()[0]
        size, free = self._current_state_size(), shutil.disk_usage(self.state_dir).free
        return {"deployment_id": identity, "policy": asdict(self.policy), "usage": usage,
                "admission_state": "active" if enabled else "restored_audit_only",
                "retained_runs": retained, "state_bytes": size, "free_bytes": free,
                "cursor_jobs": cursor_job_counts(self.state_dir),
                "task_service": task_service_counts(self.state_dir),
                "remote_tasks": remote_task_counts(self.state_dir),
                "composition": composition_counts(self.state_dir),
                "admission_open": enabled and usage["runs"] < self.policy.max_runs and usage["calls"] < self.policy.max_calls
                and usage["reserved_microusd"] < self.policy.max_reserved_microusd
                and size < self.policy.max_state_bytes and free >= self.policy.min_free_bytes,
                "expenditure_semantics": "reserved allocation; actual provider invoices require separate account controls"}


def retained_runs(state_dir: Path) -> list[dict]:
    state_dir = _private_dir(Path(state_dir))
    with _db(state_dir / "deployment.sqlite", readonly=True) as db:
        return [dict(row) for row in db.execute("SELECT * FROM runs WHERE purged_at IS NULL ORDER BY admitted_at,run_id")]


def _retention_pending_delivery(state_dir: Path, task_id: str) -> bool:
    """Caller holds the deployment maintenance lock, excluding delivery writes."""
    path = state_dir / 'event-delivery.sqlite'
    if not path.exists() and not path.is_symlink():
        return False
    from contextlib import closing
    from .event_delivery import EVENT_DELIVERY
    from .schema import connect
    with closing(connect(path, EVENT_DELIVERY, readonly=True)) as db:
        return db.execute('SELECT 1 FROM pending p JOIN subscriptions s ON s.id=p.subscription WHERE s.task_id=? LIMIT 1', (task_id,)).fetchone() is not None


def _retention_run_settled(state_dir: Path, name: str) -> bool:
    if type(name) is not str or Path(name).name != name or name in ('.', '..'):
        raise OperationsError('unsafe history run reference')
    directory = state_dir / name
    if not directory.exists():
        if directory.is_symlink():
            raise OperationsError('unsafe history run reference')
        with _db(state_dir / 'deployment.sqlite', readonly=True) as db:
            admitted = db.execute('SELECT purged_at FROM runs WHERE directory=?', (name,)).fetchone()
        return admitted is None or admitted['purged_at'] is not None
    _private_dir(directory)
    return not (_pending_cloud_job(directory) or _pending_bridge_task(directory)
                or _pending_remote_worker(directory)
                or _pending_artifacts(directory)) and _terminal_run(directory)


def archive_workflows(state_dir: Path, older_than_seconds: int, apply: bool = False, *, task_id=None) -> dict:
    """Compact settled composition payloads, retaining request tombstones.

    Archive first, then prune unreferenced terminal run directories with the
    existing command. Neither action changes a lifetime allocation. Preview is
    read-only, including for a previous supported composition schema.
    """
    if type(older_than_seconds) is not int or older_than_seconds < 0:
        raise OperationsError('retention age must be a nonnegative integer')
    state_dir = _private_dir(Path(state_dir))
    ledger = DeploymentLedger(state_dir, readonly=True)
    if apply and not ledger.admission_enabled:
        raise OperationsError('restored deployment is audit-only; archival disabled')
    path = state_dir / 'composition.sqlite'
    if not path.exists():
        return {'ok': True, 'dry_run': not apply, 'instances': []}
    from .workflow_store import WorkflowStore
    from .workflow_evaluator import FINISHED_STATES
    results = []
    with _file_lock(state_dir / '.deployment.lock', True, nonblocking=True):
        store = WorkflowStore(state_dir, readonly=not apply)
        with store.transaction(readonly=not apply) as db:
            for row in db.execute('SELECT id FROM instances ORDER BY created,id').fetchall():
                if task_id is not None and row['id'] != task_id:
                    continue
                item = store.instance(db, row['id'], include_input=True)
                result = {'instance_id': item['id'], 'eligible': False, 'archived': False}
                latest = db.execute('SELECT coalesce(max(created),?) FROM events WHERE instance=?',
                                    (item['created'], item['id'])).fetchone()[0]
                if item['state'] not in FINISHED_STATES:
                    result['reason'] = 'workflow is not terminal'
                elif latest > time.time() - older_than_seconds:
                    result['reason'] = 'retention age not reached'
                elif _retention_pending_delivery(state_dir, item['id']):
                    result['reason'] = 'unacknowledged event delivery retained'
                else:
                    references = db.execute('SELECT run_directory,state,revision=(SELECT max(b.revision) FROM attempts b WHERE b.instance=a.instance AND b.path=a.path) AS latest FROM attempts a WHERE instance=?',
                                            (item['id'],)).fetchall()
                    settled = True
                    for reference in references:
                        if (not (reference['state'] in FINISHED_STATES or not reference['latest']
                                 and reference['state'] in {'input_required', 'auth_required'})
                                or not _retention_run_settled(state_dir, reference['run_directory'])):
                            settled = False
                    if not settled:
                        result['reason'] = 'unsettled run, remote effect or artefact reference retained'
                    else:
                        result['eligible'] = True
                        if apply:
                            store.archive(db, item)
                            result['archived'] = True
                results.append(result)
    return {'ok': True, 'dry_run': not apply, 'instances': results}


def cursor_job_counts(state_dir: Path) -> dict[str, int]:
    """Inspect local job observations without implying a fresh provider scan."""
    counts = {state: 0 for state in ("prepared", "launch_unknown", "active", "terminal")}
    path = state_dir / "cursor-jobs.sqlite"
    if path.exists():
        with _db(path, readonly=True) as db:
            for state, count in db.execute("SELECT state,count(*) FROM jobs GROUP BY state"):
                if state not in counts:
                    raise OperationsError("unrecognised durable cloud job state")
                counts[state] = count
    return counts


def _pending_cloud_job(directory: Path) -> bool:
    """Retain a submission's host evidence while its remote job is unresolved."""
    path = directory.parent / "cursor-jobs.sqlite"
    if not path.exists():
        return False
    with _db(path, readonly=True) as db:
        references = db.execute("SELECT host_run_dir FROM jobs WHERE state!='terminal' AND host_run_dir IS NOT NULL")
        return any(_references_run(reference[0], directory) for reference in references)


_TASK_STATES = ("queued", "running", "input_required", "auth_required", "unknown", "blocked",
                "completed", "failed", "cancelled")
_TASK_TERMINAL = ("completed", "failed", "cancelled")


def composition_counts(state_dir: Path) -> dict:
    """Read retained composition states and allocations without scheduling work.

    Attempts include historical continuation revisions. An absent optional
    repository has zero observations; an existing unsupported or corrupt store
    is an error, never an empty or healthy composition repository.
    """
    states = (*_TASK_STATES, "expired")
    counts = {"instances": dict.fromkeys(states, 0),
              "attempts": dict.fromkeys(states, 0),
              "nodes": dict.fromkeys((*states, "waiting"), 0),
              "allocations": {"calls": 0, "reserved_microusd": 0}}
    path = Path(state_dir) / "composition.sqlite"
    if not path.exists() and not path.is_symlink():
        return counts
    try:
        with _db(path, readonly=True) as db:
            db.execute("BEGIN")
            if db.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                raise OperationsError("durable workflow composition state cannot be verified")
            for table in ("instances", "attempts", "nodes"):
                for state, count in db.execute(f"SELECT state,count(*) FROM {table} GROUP BY state"):
                    if state not in counts[table]:
                        raise OperationsError("unrecognised durable workflow composition state")
                    counts[table][state] = count
            if db.execute("SELECT 1 FROM allocations WHERE typeof(amount)!='integer' OR amount<0 LIMIT 1").fetchone():
                raise OperationsError("invalid durable workflow composition allocation")
            calls, reserved = db.execute("SELECT count(*),coalesce(sum(amount),0) FROM allocations").fetchone()
            counts["allocations"] = {"calls": calls, "reserved_microusd": reserved}
    except sqlite3.Error:
        raise OperationsError("durable workflow composition state cannot be verified") from None
    return counts


def task_service_counts(state_dir: Path) -> dict:
    """Read durable ingress observations; no scheduler or network activity."""
    counts = {state: 0 for state in _TASK_STATES}
    path = state_dir / "task-service.sqlite"
    if path.exists():
        with _db(path, readonly=True) as db:
            for state, count in db.execute("SELECT state,count(*) FROM tasks GROUP BY state"):
                if state not in counts:
                    raise OperationsError("unrecognised durable task state")
                counts[state] = count
    return counts


def remote_task_counts(state_dir: Path) -> dict:
    """Report retained observations and uncertain lifecycle effects separately."""
    counts = {state: 0 for state in _TASK_STATES}
    pending_actions = unknown_operations = 0
    path = state_dir / "remote-tasks.sqlite"
    if path.exists():
        with _db(path, readonly=True) as db:
            for state, count in db.execute("SELECT state,count(*) FROM remote_tasks GROUP BY state"):
                if state not in counts:
                    raise OperationsError("unrecognised remote task state")
                counts[state] = count
            pending_actions = db.execute("SELECT count(*) FROM remote_tasks WHERE pending_action IS NOT NULL").fetchone()[0]
            for state, count in db.execute("SELECT state,count(*) FROM operations GROUP BY state"):
                if state not in ("unknown", "completed"):
                    raise OperationsError("unrecognised remote operation state")
                if state == "unknown":
                    unknown_operations = count
    return {"states": counts, "pending_actions": pending_actions, "unknown_operations": unknown_operations}


def _references_run(reference: str, directory: Path) -> bool:
    """Resolve evidence references only within the deployment being inspected.

    New bridge stores use direct-child relative names. Cursor's earlier format
    keeps absolute paths; on an audit-only restore its unchanged evidence still
    protects the corresponding copied run, without opening the source path.
    """
    if type(reference) is not str or not reference:
        raise OperationsError("unsafe durable run reference")
    path = Path(reference)
    if not path.is_absolute():
        if path.name != reference or reference in ("", ".", ".."):
            raise OperationsError("unsafe durable run reference")
        return reference == directory.name
    if path.parent == directory.parent:
        return path == directory
    if (directory.parent / ".restored-audit-only").is_file():
        return path.name == directory.name
    raise OperationsError("durable run reference belongs to another deployment")


def _pending_bridge_task(directory: Path) -> bool:
    """Retain every revision used by unresolved ingress or remote work."""
    service = directory.parent / "task-service.sqlite"
    if service.exists():
        with _db(service, readonly=True) as db:
            references = db.execute("SELECT executions.run_directory FROM executions JOIN tasks ON tasks.id=executions.task_id WHERE tasks.state NOT IN ('completed','failed','cancelled')")
            if any(_references_run(row[0], directory) for row in references):
                return True
    remote = directory.parent / "remote-tasks.sqlite"
    if remote.exists():
        with _db(remote, readonly=True) as db:
            references = db.execute("SELECT host_run_dir FROM remote_tasks WHERE state NOT IN ('completed','failed','cancelled') OR pending_action IS NOT NULL UNION SELECT host_run_dir FROM operations WHERE state!='completed'")
            if any(_references_run(row[0], directory) for row in references):
                return True
    return False


def reconcile_run(run_dir: Path, mas_bin: str = "mas", timeout: float = 10) -> dict:
    run_dir = _private_dir(Path(run_dir))
    with _db(run_dir / "state.sqlite", readonly=True) as db:
        ids = db.execute("SELECT id FROM runs").fetchall()
    if len(ids) != 1:
        raise OperationsError("run database must identify exactly one run")
    binary = shutil.which(str(mas_bin))
    if not binary:
        raise OperationsError("Rust mas binary is unavailable")
    try:
        with _file_lock(run_dir.parent / ".deployment.lock", False):
            result = subprocess.run([binary, "reconcile", str(run_dir / "state.sqlite"), ids[0][0]],
                                check=False, capture_output=True, text=True, timeout=timeout,
                                env={"PATH": os.environ.get("PATH", os.defpath)})
        response = json.loads(result.stdout)
        if result.returncode or response.get("ok") is not True:
            raise OperationsError("run reconciliation was rejected")
        return {"run_id": ids[0][0], "result": response["result"]}
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
        raise OperationsError("run reconciliation unavailable or interrupted") from None


def reconcile_state(state_dir: Path, mas_bin: str = "mas") -> dict:
    state_dir = _private_dir(Path(state_dir))
    with _file_lock(state_dir / ".deployment.lock", False):
        return _reconcile_state(state_dir, mas_bin)


def _reconcile_state(state_dir: Path, mas_bin: str = "mas") -> dict:
    state_dir = _private_dir(Path(state_dir))
    results = []
    for run in retained_runs(state_dir):
        path = state_dir / run["directory"]
        try:
            if not (path / "state.sqlite").is_file():
                raise OperationsError("admitted run initialisation is incomplete")
            results.append({"ok": True, **reconcile_run(path, mas_bin)})
        except (OperationsError, sqlite3.Error) as error:
            results.append({"ok": False, "run_id": run["run_id"], "error": str(error) if isinstance(error, OperationsError) else "run database unavailable"})
    # The local watcher cannot claim a fresh remote observation: only an
    # admitted protocol client can perform that operation. It still verifies
    # these durable repositories so corruption is not reported as healthy.
    try:
        bridge_observations = {"ok": True, "remote_refreshed": False,
                               "task_service": task_service_counts(state_dir),
                               "remote_tasks": remote_task_counts(state_dir),
                               "cursor_jobs": cursor_job_counts(state_dir)}
    except (OperationsError, sqlite3.Error):
        bridge_observations = {"ok": False, "remote_refreshed": False,
                               "error": "durable bridge state cannot be verified"}
    try:
        composition_observations = {"ok": True, **composition_counts(state_dir)}
    except OperationsError:
        composition_observations = {"ok": False,
                                    "error": "durable workflow composition state cannot be verified"}
    return {"ok": (all(item["ok"] for item in results) and bridge_observations["ok"]
                   and composition_observations["ok"]),
            "runs": results, "bridge_observations": bridge_observations,
            "composition_observations": composition_observations}


def _empty_destination(destination: Path) -> Path:
    destination = Path(destination).absolute()
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise OperationsError("destination must be absent or empty")
    return _private_dir(destination, create=True)


def _hash(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def backup_state(state_dir: Path, destination: Path) -> dict:
    """Take a quiescent deployment snapshot; never copy a live SQLite file."""
    state_dir = _private_dir(Path(state_dir))
    destination = Path(destination).absolute()
    if destination == state_dir or state_dir in destination.parents or destination in state_dir.parents:
        raise OperationsError("backup destination must be outside the deployment tree")
    # Nothing creates a destination before the source is known to be quiescent.
    with ExitStack() as stack:
        stack.enter_context(_file_lock(state_dir / ".deployment.lock", True, nonblocking=True))
        runs = retained_runs(state_dir)
        for run in runs:
            directory = _private_dir(state_dir / run["directory"])
            stack.enter_context(_file_lock(directory / ".host.lock", True, nonblocking=True))
        _state_size(state_dir)  # Reject every symlink and special file first.
        destination = _empty_destination(destination)
        files = {}
        try:
            for source in sorted(state_dir.rglob("*")):
                relative = source.relative_to(state_dir)
                if source.is_dir():
                    (destination / relative).mkdir(mode=0o700, parents=True, exist_ok=True)
                    continue
                if (source.name.endswith((".lock", "-wal", "-shm", "-journal"))
                        or source.name.startswith((".reconciler-health", ".bridge-worker-health"))):
                    continue
                target = destination / relative
                if source.suffix == ".sqlite":
                    with _db(source, readonly=True) as original, _db(target) as copied:
                        original.backup(copied)
                        if copied.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                            raise OperationsError("backup database integrity check failed")
                else:
                    shutil.copyfile(source, target)
                target.chmod(0o600)
                files[str(relative)] = {"sha256": _hash(target), "bytes": target.stat().st_size}
            manifest = {"format": "mas-deployment-backup/1", "created_at": int(time.time()), "quiescent": True, "files": files}
            manifest_path = destination / "backup-manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            manifest_path.chmod(0o600)
            return {"ok": True, "backup": str(destination), "files": len(files), "created_at": manifest["created_at"]}
        except Exception:
            # Leave the incomplete private directory for inspection; absence of
            # its final manifest prevents restoration.
            raise


def restore_state(bundle: Path, destination: Path) -> dict:
    bundle = _private_dir(Path(bundle))
    _state_size(bundle)
    try:
        manifest = json.loads((bundle / "backup-manifest.json").read_text(encoding="utf-8"))
        if manifest.get("format") != "mas-deployment-backup/1" or manifest.get("quiescent") is not True or type(manifest.get("files")) is not dict:
            raise OperationsError("invalid backup manifest")
        files = manifest["files"]
        if "deployment.sqlite" not in files:
            raise OperationsError("backup is missing its deployment ledger")
        actual = {str(path.relative_to(bundle)) for path in bundle.rglob("*") if path.is_file() and path.name != "backup-manifest.json"}
        if actual != set(files):
            raise OperationsError("backup file set differs from manifest")
        for relative, metadata in files.items():
            path = Path(relative)
            if path.is_absolute() or ".." in path.parts or "." in path.parts or str(path) != relative:
                raise OperationsError("unsafe backup path")
            source = bundle / relative
            if source.stat().st_size != metadata["bytes"] or _hash(source) != metadata["sha256"]:
                raise OperationsError("backup content integrity check failed")
            if source.suffix == ".sqlite":
                with _db(source, readonly=True) as db:
                    if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise OperationsError("backup database integrity check failed")
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error):
        raise OperationsError("backup verification failed") from None
    destination = Path(destination).absolute()
    if destination == bundle or bundle in destination.parents or destination in bundle.parents:
        raise OperationsError("restore destination must be outside the backup tree")
    destination = _empty_destination(destination)
    staging = Path(tempfile.mkdtemp(prefix="." + destination.name + "-restore-", dir=destination.parent))
    # Quarantine precedes even the first copied database. A crash or another
    # host examining this staging directory can never acquire spending rights.
    quarantine = staging / ".restored-audit-only"
    with quarantine.open("x", encoding="utf-8") as marker:
        marker.write("Restored evidence; new work admissions disabled.\n")
        marker.flush()
        os.fsync(marker.fileno())
    quarantine.chmod(0o600)
    descriptor = os.open(staging, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        for relative in files:
            if relative == ".restored-audit-only":
                continue
            target = staging / relative
            # mkdir(parents=True) applies its explicit mode only to the leaf.
            # Create each restored ancestor privately, including artefact roots.
            parent = staging
            for component in Path(relative).parts[:-1]:
                parent = parent / component
                parent.mkdir(mode=0o700, exist_ok=True)
            shutil.copyfile(bundle / relative, target)
            target.chmod(0o600)
        # A snapshot cannot know about charges committed after it was taken,
        # or whether the original deployment still exists. Never restore its
        # spending authority, even when all copied files are intact.
        with _db(staging / "deployment.sqlite") as db:
            db.execute("INSERT OR REPLACE INTO metadata VALUES('admission_state','restored_audit_only')")
            db.execute("INSERT OR REPLACE INTO metadata VALUES('restored_at',?)", (str(int(time.time())),))
        for path in staging.rglob("*"):
            if path.is_file():
                with path.open("rb") as stream:
                    os.fsync(stream.fileno())
        for path in [*reversed(sorted((p for p in staging.rglob("*") if p.is_dir()))), staging]:
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        # POSIX rename replaces only an empty destination directory. A host
        # which populated it concurrently makes this fail instead of overwriting.
        os.replace(staging, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    ledger = DeploymentLedger(destination)
    return {"ok": True, "destination": str(destination), "health": ledger.health(),
            "restored_at": int(time.time()), "snapshot_created_at": manifest["created_at"]}


def prune_runs(state_dir: Path, older_than_seconds: int, apply: bool = False) -> dict:
    """Remove terminal run files; retain every ledger admission forever."""
    if type(older_than_seconds) is not int or older_than_seconds < 0:
        raise OperationsError("retention age must be a nonnegative integer")
    state_dir = _private_dir(Path(state_dir))
    if apply and not DeploymentLedger(state_dir, readonly=True).admission_enabled:
        raise OperationsError('restored deployment is audit-only; pruning disabled')
    results = []
    with _file_lock(state_dir / ".deployment.lock", True, nonblocking=True):
        for run in retained_runs(state_dir):
            directory = _private_dir(state_dir / run["directory"])
            result = {"run_id": run["run_id"], "eligible": False, "deleted": False}
            try:
                with _file_lock(directory / ".host.lock", True, nonblocking=True):
                    latest_change = max([run["admitted_at"], *[path.stat().st_mtime for path in directory.rglob("*") if path.is_file() and not path.name.endswith(".lock")]])
                    if latest_change > time.time() - older_than_seconds:
                        result["reason"] = "retention age not reached"
                    elif _pending_cloud_job(directory):
                        result["reason"] = "active or unresolved external cloud job retained"
                    elif _pending_bridge_task(directory):
                        result["reason"] = "active or unresolved bridge task or operation retained"
                    elif _pending_remote_worker(directory):
                        result['reason'] = 'remote worker invocation or late evidence retained'
                    elif _composition_holds_run(directory):
                        result["reason"] = "workflow composition history retained"
                    elif _pending_artifacts(directory):
                        result["reason"] = "live or unresolved artefact references retained"
                    elif not _terminal_run(directory):
                        result["reason"] = "open tasks, incomplete journal or unresolved invocations retained"
                    else:
                        result["eligible"] = True
                        if apply:
                            # Record the tombstone first; failures retain the
                            # allocation and require manual file cleanup.
                            with _db(state_dir / "deployment.sqlite") as db:
                                db.execute("UPDATE runs SET purged_at=? WHERE run_id=?", (int(time.time()), run["run_id"]))
                            shutil.rmtree(directory)
                            result["deleted"] = True
            except (OperationsError, sqlite3.Error):
                result["reason"] = "run busy or state cannot be verified"
            results.append(result)
    return {"ok": True, "dry_run": not apply, "runs": results}


def _composition_holds_run(directory: Path) -> bool:
    if (directory.parent / "composition.sqlite").is_symlink():
        raise OperationsError("workflow composition state cannot be verified")
    from .workflow_store import holds_run
    try:
        return holds_run(directory.parent, directory.name)
    except SchemaError:
        raise OperationsError("workflow composition state cannot be verified") from None


def _pending_remote_worker(directory: Path) -> bool:
    path = directory.parent / 'remote-workers.sqlite'
    if not path.exists() and not path.is_symlink():
        return False
    with _db(path, readonly=True) as db, _db(directory.parent / 'deployment.sqlite', readonly=True) as ledger:
        run = ledger.execute('SELECT run_id FROM runs WHERE directory=?', (directory.name,)).fetchone()
        if run is None:
            return True
        return db.execute("SELECT 1 FROM jobs WHERE run_id=? AND (state IN ('pending','claimed','dispatched','unknown') "
                          "OR EXISTS(SELECT 1 FROM receipts WHERE receipts.call_id=jobs.call_id AND authoritative=0)) LIMIT 1",
                          (run['run_id'],)).fetchone() is not None


def compact_history(state_dir: Path) -> dict:
    """Reclaim deleted SQLite payload pages after supported archival.

    VACUUM changes no logical records. This requires quiescence and free space
    for SQLite's replacement file; it never deletes identity/accounting rows.
    """
    state_dir = _private_dir(Path(state_dir))
    ledger = DeploymentLedger(state_dir, readonly=True)
    results = []
    with _file_lock(state_dir / '.deployment.lock', True, nonblocking=True):
        if not ledger.admission_enabled:
            raise OperationsError('restored deployment is audit-only; compaction disabled')
        for name in ('task-service.sqlite', 'composition.sqlite'):
            path = state_dir / name
            if not path.exists():
                continue
            before = path.stat().st_size
            with _db(path) as db:
                if db.execute('PRAGMA quick_check(1)').fetchone()[0] != 'ok':
                    raise OperationsError('history database cannot be verified')
                db.execute('VACUUM')
            results.append({'repository': name, 'before_bytes': before, 'after_bytes': path.stat().st_size})
    return {'ok': True, 'repositories': results}


def _pending_artifacts(directory: Path) -> bool:
    # The optional local artefact repository sits below each run, so quiescent
    # deployment backups include it. Its explicit manifests can retain results
    # for consumers whose lifecycle outlasts the producing run.
    from .artifacts import has_retained_artifacts
    return has_retained_artifacts(directory)


def _terminal_run(directory: Path) -> bool:
    if not (directory / "host.sqlite").is_file() or not (directory / "state.sqlite").is_file():
        return False
    with _db(directory / "state.sqlite", readonly=True) as db:
        if db.execute("SELECT count(*) FROM tasks WHERE state='open'").fetchone()[0]:
            return False
        # Recorded unknown evidence still represents an unresolved external
        # effect, unless the host resolution journal supplies a decision.
        calls = db.execute("SELECT id,state,result FROM calls").fetchall()
    with _db(directory / "host.sqlite", readonly=True) as journal:
        columns = {row[1] for row in journal.execute("PRAGMA table_info(invocations)")}
        if not {"call_id", "state"}.issubset(columns):
            return False
        invocations = {row["call_id"]: row["state"] for row in journal.execute("SELECT call_id,state FROM invocations")}
        if any(state not in ("completed", "resolved", "retry_allowed", "not_executed", "failed_known") for state in invocations.values()):
            return False
        for call in calls:
            if call[0] not in invocations:
                return False
    return True
