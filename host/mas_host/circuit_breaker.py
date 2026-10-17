"""Cross-run provider circuit state; suppresses new dispatch, never retries it."""
from contextlib import contextmanager
from collections.abc import Mapping
from dataclasses import dataclass
import re
import time

from .contracts import RunnerError
from .journal import canonical
from .operations import _file_lock
from .schema import Schema, connect


CIRCUITS = Schema("provider circuits", 0x4D415359, 1, (), (
    "CREATE TABLE circuits(resource TEXT PRIMARY KEY,policy TEXT NOT NULL,failures INTEGER NOT NULL,"
    "open_until_ms INTEGER NOT NULL,generation INTEGER NOT NULL,probe_call TEXT,probe_deadline_ms INTEGER)",
    "CREATE TABLE permits(call_id TEXT PRIMARY KEY,resource TEXT NOT NULL,generation INTEGER NOT NULL,"
    "probe INTEGER NOT NULL,observation TEXT,created_ms INTEGER NOT NULL,observed_ms INTEGER)",
))


@dataclass(frozen=True)
class CircuitPolicy:
    resource_key: str
    failure_threshold: int = 5
    open_seconds: int = 30
    half_open_seconds: int = 60

    def __post_init__(self):
        if (type(self.resource_key) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", self.resource_key)
                or type(self.failure_threshold) is not int or not 1 <= self.failure_threshold <= 100
                or type(self.open_seconds) is not int or not 1 <= self.open_seconds <= 3600
                or type(self.half_open_seconds) is not int or not 1 <= self.half_open_seconds <= 600):
            raise RunnerError("invalid provider circuit policy")

    @property
    def descriptor(self):
        return {"format": "mas-provider-circuit/1", "resource_key": self.resource_key,
                "failure_threshold": self.failure_threshold, "open_seconds": self.open_seconds,
                "half_open_seconds": self.half_open_seconds}


def circuit_policies(value):
    if value is None:
        return {}
    if not isinstance(value, Mapping) or len(value) > 256:
        raise RunnerError("invalid provider circuit policies")
    result = {}
    for actor, policy in value.items():
        if type(actor) is not str or not actor or len(actor) > 200:
            raise RunnerError("invalid provider circuit actor")
        if type(policy) is dict:
            fields = dict(policy)
            if "format" in fields and fields.pop("format") != "mas-provider-circuit/1":
                raise RunnerError("invalid provider circuit policy")
            try:
                policy = CircuitPolicy(**fields)
            except TypeError:
                raise RunnerError("invalid provider circuit policy") from None
        if not isinstance(policy, CircuitPolicy):
            raise RunnerError("invalid provider circuit policy")
        result[actor] = policy
    return result


class CircuitBreaker:
    def __init__(self, operations, policy: CircuitPolicy):
        self.operations, self.policy = operations, policy
        self.state_dir = operations.state_dir
        self.path = self.state_dir / "provider-circuits.sqlite"

    @contextmanager
    def _open(self, *, readonly=False):
        with _file_lock(self.state_dir / ".deployment.lock", False), \
                _file_lock(self.state_dir / ".provider-circuits.lock", True):
            if not readonly and (self.operations.readonly or not self.operations.admission_enabled):
                raise RunnerError("deployment does not permit circuit mutations")
            db = connect(self.path, CIRCUITS, readonly=readonly,
                         initialise=None if readonly else lambda _: None)
            try:
                with db:
                    yield db
            finally:
                db.close()

    def _row(self, db):
        row = db.execute("SELECT * FROM circuits WHERE resource=?", (self.policy.resource_key,)).fetchone()
        if row is None:
            db.execute("INSERT INTO circuits VALUES(?,?,0,0,0,NULL,NULL)",
                       (self.policy.resource_key, canonical(self.policy.descriptor)))
            row = db.execute("SELECT * FROM circuits WHERE resource=?", (self.policy.resource_key,)).fetchone()
        if row["policy"] != canonical(self.policy.descriptor):
            raise RunnerError("provider circuit policy conflicts with retained resource")
        return row

    def admit(self, call_id, *, now_ms=None) -> bool:
        timestamp = int(time.time() * 1000) if now_ms is None else now_ms
        with self._open() as db:
            row = self._row(db)
            existing = db.execute("SELECT * FROM permits WHERE call_id=?", (call_id,)).fetchone()
            if existing is not None:
                return (existing["resource"] == self.policy.resource_key and existing["observation"] is None
                        and existing["generation"] == row["generation"])
            if row["open_until_ms"] > timestamp:
                return False
            probing = row["failures"] >= self.policy.failure_threshold
            if probing and row["probe_call"] is not None:
                if row["probe_deadline_ms"] <= timestamp:
                    # No conclusion about its effect follows from lease expiry.
                    # A fresh independent probe waits another full cooldown.
                    db.execute("UPDATE permits SET observation='abandoned',observed_ms=? WHERE call_id=? AND observation IS NULL",
                               (timestamp, row["probe_call"]))
                    db.execute("UPDATE circuits SET open_until_ms=?,generation=generation+1,probe_call=NULL,"
                               "probe_deadline_ms=NULL WHERE resource=?",
                               (timestamp + self.policy.open_seconds * 1000, self.policy.resource_key))
                return False
            db.execute("INSERT INTO permits VALUES(?,?,?,?,NULL,?,NULL)",
                       (call_id, self.policy.resource_key, row["generation"], int(probing), timestamp))
            if probing:
                db.execute("UPDATE circuits SET probe_call=?,probe_deadline_ms=? WHERE resource=?",
                           (call_id, timestamp + self.policy.half_open_seconds * 1000, self.policy.resource_key))
            return True

    def observe(self, call_id, category: str, *, now_ms=None):
        if category not in {"success", "provider_failure", "aborted"}:
            raise RunnerError("invalid provider circuit observation")
        timestamp = int(time.time() * 1000) if now_ms is None else now_ms
        with self._open() as db:
            row = self._row(db)
            permit = db.execute("SELECT * FROM permits WHERE call_id=?", (call_id,)).fetchone()
            if permit is None or permit["resource"] != self.policy.resource_key:
                raise RunnerError("provider circuit permit missing")
            if permit["observation"] is not None:
                return
            db.execute("UPDATE permits SET observation=?,observed_ms=? WHERE call_id=?", (category, timestamp, call_id))
            if permit["generation"] != row["generation"]:
                return  # A late pre-open result cannot close a newer circuit.
            if permit["probe"]:
                if row["probe_call"] != call_id:
                    raise RunnerError("provider circuit probe ownership changed")
                success = category == "success"
                db.execute("UPDATE circuits SET failures=?,open_until_ms=?,generation=generation+1,probe_call=NULL,"
                           "probe_deadline_ms=NULL WHERE resource=?",
                           (0 if success else max(row["failures"], self.policy.failure_threshold),
                            0 if success else timestamp + self.policy.open_seconds * 1000, self.policy.resource_key))
            elif category == "success":
                db.execute("UPDATE circuits SET failures=0 WHERE resource=?", (self.policy.resource_key,))
            elif category == "provider_failure":
                failures = row["failures"] + 1
                opened = failures >= self.policy.failure_threshold
                db.execute("UPDATE circuits SET failures=?,open_until_ms=?,generation=? WHERE resource=?",
                           (failures, timestamp + self.policy.open_seconds * 1000 if opened else 0,
                            row["generation"] + int(opened), self.policy.resource_key))

    def status(self):
        if not self.path.exists():
            return {"resource_key": self.policy.resource_key, "state": "closed", "failures": 0,
                    "open_until_ms": 0, "probe_call": None}
        with self._open(readonly=True) as db:
            row = db.execute("SELECT * FROM circuits WHERE resource=?", (self.policy.resource_key,)).fetchone()
            if row is None:
                return {"resource_key": self.policy.resource_key, "state": "closed", "failures": 0,
                        "open_until_ms": 0, "probe_call": None}
            return {"resource_key": row["resource"], "state": "half_open" if row["probe_call"] else
                "open" if row["failures"] >= self.policy.failure_threshold else "closed",
                "failures": row["failures"], "open_until_ms": row["open_until_ms"], "probe_call": row["probe_call"]}


def failure_observation(diagnostic):
    """Only trusted finite infrastructure diagnostics trip the shared circuit."""
    if (type(diagnostic) is dict and diagnostic.get("format") == "mas-provider-diagnostic/1"
            and diagnostic.get("category") in {"transport_failure", "transport_timeout", "provider_http_error"}):
        status = diagnostic.get("http_status")
        if status is None or status == 429 or 500 <= status <= 599:
            return "provider_failure"
    return "aborted"
