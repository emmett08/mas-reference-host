"""Authenticated leaf worker with a durable spool; no local MAS admission."""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import sqlite3
import threading
import time
import uuid

from .codec import canonical_json, integer_json
from .contracts import Invocation, InvocationTicket, RunnerError, TaskRequest
from .distributed import (ARTIFACT_OUTPUT_PROFILE, OUTPUT_PROFILE, WORKER_PROTOCOL,
    _DIGEST, _identifier, _integer, artifact_references, output_contract, validate_binding)
from .operations import _file_lock, _private_dir
from .provider_outcomes import ProviderOutcomeFailure, completed_cleanup_failure, validate_provider_outcome
from .schema import Schema, connect
from .service_contracts import ServiceError
from .bridges.authentication import read_token_file
from .bridges.service_client import decode_integer_json, http_exchange, validate_endpoint


SPOOL = Schema("remote worker spool", 0x4D415350, 1, (), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE attempts(call_id TEXT NOT NULL,fence INTEGER NOT NULL,assignment TEXT NOT NULL,state TEXT NOT NULL,receipt TEXT,ack TEXT,PRIMARY KEY(call_id,fence))",
))
_RECEIPT_BYTES = 131072 - 128
_ACK_BYTES = 1024
# One bounded assignment and receipt, plus enough free pages for SQLite to
# relocate their row during a state transition and grow its small indices.
_ATTEMPT_RESERVE = 2 * (65536 + _RECEIPT_BYTES + _ACK_BYTES) + 65536


@contextmanager
def _worker_ownership(path):
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ServiceError("worker_busy") from None
        yield descriptor
    finally:
        # The guardian inherits the same open-file-description lock. Closing
        # only our descriptor preserves ownership until it has reaped children.
        os.close(descriptor)


@dataclass(frozen=True)
class WorkerOperation:
    name: str
    implementation: str
    execute: object
    output_profile: str = OUTPUT_PROFILE
    required_outputs: tuple[str, ...] = ()

    def __post_init__(self):
        _identifier(self.name)
        object.__setattr__(self, "required_outputs", output_contract(self.output_profile, self.required_outputs))
        if type(self.implementation) is not str or not _DIGEST.fullmatch(self.implementation) or not callable(self.execute):
            raise ServiceError("invalid_worker_operation")

    @classmethod
    def registered(cls, name, spec, *, gateway_bin="mas-gateway", timeout_seconds=60,
                   reservation_microusd=0, output_profile=OUTPUT_PROFILE, required_outputs=()):
        executor = RegisteredLeafExecutor(spec, gateway_bin=gateway_bin,
            timeout_seconds=timeout_seconds, reservation_microusd=reservation_microusd, operation=name,
            output_profile=output_profile, required_outputs=required_outputs)
        return cls(name, executor.identity, executor, output_profile, executor.required_outputs)


class _IssuedInvocation:
    """Consume the controller's one ticket; never create an allocation."""
    def __init__(self, call_id, deadline, reservation, ownership_descriptors=()):
        self.actor, self.invocation_id = "worker", call_id
        self.deadline, self.reservation = deadline, reservation
        self.used, self.completed, self.known_outcome = False, None, None
        self.known_output = None
        self.ownership_descriptors = ownership_descriptors

    @property
    def remaining_seconds(self):
        return max(0, self.deadline - time.monotonic())

    def request(self, *_args, **_kwargs):
        raise RunnerError("remote leaf workers cannot delegate or admit new work")

    def prepare_invocation(self, *, input=None, reservation_microusd=None):
        if self.used or (reservation_microusd is not None and (
                type(reservation_microusd) is not int or not 0 <= reservation_microusd <= self.reservation)):
            raise RunnerError("remote leaf invocation authority is exhausted")
        # The configured executable identity and original task input are frozen
        # by the controller. Retain the exact inner binding for receipt audit.
        self.input_identity = canonical_json(input)
        self.used = True
        return InvocationTicket(self.invocation_id, None, True, self.invocation_id)

    def complete_invocation(self, ticket, invocation):
        if not self.used or ticket.call_id != self.invocation_id or self.completed is not None or self.known_outcome is not None:
            raise RunnerError("remote leaf invocation ticket is invalid")
        if not isinstance(invocation, Invocation):
            raise RunnerError("remote leaf invocation result is invalid")
        self.completed = invocation
        return invocation.output

    def fail_invocation(self, *_args, **_kwargs):
        raise RunnerError("remote leaf effect remains unknown")

    def reject_invocation(self, ticket, outcome, *, completed_output=None):
        if not self.used or ticket.call_id != self.invocation_id or self.completed is not None or self.known_outcome is not None:
            raise RunnerError("remote leaf invocation ticket is invalid")
        checked = ProviderOutcomeFailure(outcome, completed_output=completed_output)
        self.known_outcome, self.known_output = checked.outcome, checked.completed_output

    def retry_invocation(self, call_id):
        if call_id != self.invocation_id or self.known_outcome is None:
            raise RunnerError("remote leaf retry does not match its issued invocation")
        return None  # Only the controller may allocate a subsequent invocation.


class RegisteredLeafExecutor:
    """Rust-supervised registered process with one pre-issued invocation ticket."""
    def __init__(self, spec, *, gateway_bin="mas-gateway", timeout_seconds=60,
                 reservation_microusd=0, operation=None, output_profile=OUTPUT_PROFILE, required_outputs=()):
        from .executable_worker import RegisteredProcessExecutor, WorkerSpec
        if not isinstance(spec, WorkerSpec):
            raise ServiceError("invalid_worker_operation")
        self.spec = spec
        self.output_profile = output_profile
        self.required_outputs = output_contract(output_profile, required_outputs)
        self.operation = _identifier(spec.descriptor["name"] if operation is None else operation)
        self.timeout = _integer(timeout_seconds, 1, 86400)
        self.reservation = _integer(reservation_microusd, 0, 100_000_000)
        self.executor = RegisteredProcessExecutor(gateway_bin)

    @property
    def identity(self):
        return hashlib.sha256(canonical_json({"spec": self.spec.identity,
            "operation": self.operation, "timeout_seconds": self.timeout,
            "reservation_microusd": self.reservation, "output_profile": self.output_profile,
            **({"required_outputs": list(self.required_outputs)} if self.required_outputs else {})}).encode()).hexdigest()

    def __call__(self, input, call_id, *, ownership_descriptors=()):
        from .executable_worker import ExecutableAdapter, WorkerCleanupError
        context = _IssuedInvocation(call_id, time.monotonic() + self.timeout, self.reservation,
                                    ownership_descriptors)
        request = TaskRequest("controller", "worker", self.operation, input, call_id, call_id)
        try:
            outcome = self.executor.execute(ExecutableAdapter(self.spec), context, request, deadline=context.deadline)
        except Exception as error:
            if context.known_outcome is not None:
                observed = ProviderOutcomeFailure(context.known_outcome, completed_output=context.known_output)
                raise (observed.with_cleanup_failure() if isinstance(error, WorkerCleanupError) else observed) from None
            if isinstance(error, WorkerCleanupError) and context.completed is not None:
                raise completed_cleanup_failure("custom", context.completed.evidence, context.completed.output) from None
            raise
        if context.known_outcome is not None:
            raise ProviderOutcomeFailure(context.known_outcome, completed_output=context.known_output)
        if outcome.status != "completed":
            raise ServiceError("remote_leaf_incomplete")
        evidence = {"implementation": self.spec.identity, "execution_profile": "registered-leaf/1"}
        if context.completed is not None:
            evidence["invocation"] = context.completed.evidence
            evidence["input_sha256"] = hashlib.sha256(context.input_identity.encode()).hexdigest()
        return Invocation(outcome.output, evidence)


class WorkerClient:
    """Only worker RPCs. The token confers no task submission authority."""
    def __init__(self, endpoint, *, token_file, timeout_seconds=10, allow_loopback_http=False,
                 transport=None, transport_policy=None):
        parsed = validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
        if parsed.path not in ("", "/"):
            raise ServiceError("invalid_endpoint")
        self.endpoint, self.token_file = endpoint.rstrip("/"), token_file
        self.timeout = timeout_seconds
        self.allow_http, self.transport = allow_loopback_http, transport or http_exchange
        self.transport_policy = transport_policy

    def call(self, operation, **payload):
        if operation not in {"register", "claim", "dispatch", "heartbeat", "receipt", "drain"}:
            raise ServiceError("invalid_worker_request")
        options = {"transport_policy": self.transport_policy} if self.transport_policy is not None else {}
        status, headers, body = self.transport(self.endpoint + "/workers/" + operation,
            method="POST", body=integer_json(payload, 131072).encode(),
            headers={"Authorization": "Bearer " + read_token_file(self.token_file),
                     "Content-Type": "application/json"},
            timeout_seconds=self.timeout, max_response_bytes=131072,
            allow_loopback_http=self.allow_http, **options)
        if headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise ServiceError("invalid_remote_response")
        decoded = decode_integer_json(body, max_bytes=131072)
        if type(decoded) is not dict:
            raise ServiceError("invalid_remote_response")
        if status >= 400:
            failure = decoded.get("error")
            code = failure.get("code") if type(failure) is dict else None
            if type(code) is str and len(code) <= 128 and all(c.isascii() and (c.islower() or c.isdigit() or c == "_") for c in code):
                raise ServiceError(code)
            raise ServiceError("remote_worker_error")
        if not 200 <= status < 300 or set(decoded) != {"result"} or type(decoded["result"]) is not dict:
            raise ServiceError("invalid_remote_response")
        return decoded["result"]


class DurableWorker:
    """Run registered leaf effects once per durable dispatch boundary.

    The process supplies no AdapterContext to application code. Nested host
    calls and new allocations are therefore absent from this worker profile.
    """
    def __init__(self, client, state_dir, operations, *, trust_zone, boot=None,
                 max_spool_bytes=16 * 1024 * 1024, max_unacknowledged=100):
        self.client = client
        self.state_dir = _private_dir(Path(state_dir), create=True)
        self.path = self.state_dir / "worker-spool.sqlite"
        self.operations = {operation.name: operation for operation in operations}
        if not self.operations or len(self.operations) != len(operations):
            raise ServiceError("invalid_worker_operations")
        self.trust_zone, self.boot = _identifier(trust_zone), _identifier(boot or uuid.uuid4().hex)
        if (type(max_spool_bytes) is not int or max_spool_bytes < 131072
                or type(max_unacknowledged) is not int or not 1 <= max_unacknowledged <= 10000):
            raise ServiceError("invalid_worker_spool")
        self.max_spool_bytes, self.max_unacknowledged = max_spool_bytes, max_unacknowledged
        identity = canonical_json({"endpoint": client.endpoint, "trust_zone": trust_zone})
        with _file_lock(self.state_dir / ".worker-init.lock", True):
            with closing(self._connect(initialise=lambda db: db.execute(
                    "INSERT INTO metadata VALUES('identity',?)", (identity,)))) as db:
                if dict(db.execute("SELECT key,value FROM metadata")) != {"identity": identity}:
                    raise ServiceError("worker_spool_identity_changed")
        self.registered = False

    def _connect(self, **kwargs):
        db = connect(self.path, SPOOL, **kwargs)
        try:
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
            maximum = self.max_spool_bytes // page_size
            actual = db.execute(f"PRAGMA max_page_count={maximum}").fetchone()[0]
            if actual > maximum:
                raise ServiceError("worker_spool_full")
            return db
        except BaseException:
            db.close()
            raise

    def _reserve_capacity(self, db):
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        used = db.execute("PRAGMA page_count").fetchone()[0] - db.execute("PRAGMA freelist_count").fetchone()[0]
        if used + (_ATTEMPT_RESERVE + page_size - 1) // page_size > self.max_spool_bytes // page_size:
            raise ServiceError("worker_spool_full")

    def register(self):
        result = self.client.call("register", boot=self.boot,
            capabilities={name: operation.implementation for name, operation in self.operations.items()},
            architecture=platform.machine(), runtime="python-" + platform.python_version(),
            trust_zone=self.trust_zone, capacity=1, protocol=WORKER_PROTOCOL,
            output_profiles=sorted({op.output_profile for op in self.operations.values()}))
        if (result.get("boot") != self.boot or type(result.get("lease_ms")) is not int
                or result.get("protocol") != WORKER_PROTOCOL
                or result.get("output_profiles") != sorted({op.output_profile for op in self.operations.values()})):
            raise ServiceError("invalid_remote_response")
        self.registered = True

    def _save_receipt(self, assignment, status, output=None, evidence=None):
        receipt = {"id": uuid.uuid4().hex, "call_id": assignment["call_id"],
            "boot": assignment["boot"], "fence": assignment["fence"], "status": status,
            "output": output, "evidence": evidence or {"reason": "worker_effect_unknown"},
            "output_profile": assignment["binding"]["output_profile"]}
        if receipt["output_profile"] == ARTIFACT_OUTPUT_PROFILE:
            references = artifact_references(output) if output is not None else {}
            expected = assignment["binding"]["required_outputs"] if output is not None else []
            if set(references) != set(expected):
                raise ServiceError("worker_output_contract_mismatch")
            receipt["artifacts"] = references
        integer_json(receipt["output"])
        integer_json(receipt["evidence"])
        encoded = canonical_json(receipt, _RECEIPT_BYTES).encode("utf-8")
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT rowid,length(receipt) AS size FROM attempts WHERE call_id=? AND fence=?",
                             (assignment["call_id"], assignment["fence"])).fetchone()
            if row is None:
                raise ServiceError("worker_spool_missing_attempt")
            # The pre-dispatch reservation includes two complete row sizes,
            # allowing SQLite's bounded update growth on Python 3.10 as well.
            padded = encoded + b" " * (_RECEIPT_BYTES - len(encoded))
            db.execute("UPDATE attempts SET receipt=?,state='receipt' WHERE rowid=?", (padded, row["rowid"]))

    def _recover_interrupted(self):
        with closing(connect(self.path, SPOOL, readonly=True)) as db:
            rows = db.execute("SELECT * FROM attempts WHERE state IN ('claimed','dispatching','executing')").fetchall()
        for row in rows:
            assignment = json.loads(row["assignment"])
            if row["state"] == "claimed":
                # No dispatch request crossed the boundary in this state.
                with closing(self._connect()) as db, db:
                    db.execute("UPDATE attempts SET state='abandoned',ack=? WHERE call_id=? AND fence=?",
                               (canonical_json({"not_dispatched": True}), row["call_id"], row["fence"]))
            else:
                self._save_receipt(assignment, "unknown", evidence={"reason": "worker_restarted_after_dispatch_boundary"})

    def flush(self):
        """Repeat receipt delivery only; never repeat an application effect."""
        with closing(connect(self.path, SPOOL, readonly=True)) as db:
            rows = db.execute("SELECT * FROM attempts WHERE state='receipt' ORDER BY call_id,fence").fetchall()
        for row in rows:
            receipt = json.loads(row["receipt"])
            ack = self.client.call("receipt", receipt=receipt)
            if ack.get("stored") is not True or ack.get("receipt_id") != receipt["id"] or type(ack.get("authoritative")) is not bool:
                raise ServiceError("invalid_remote_receipt_acknowledgement")
            with closing(self._connect()) as db, db:
                db.execute("UPDATE attempts SET state='acknowledged',ack=? WHERE call_id=? AND fence=?",
                           (canonical_json(ack, _ACK_BYTES), row["call_id"], row["fence"]))

    def _heartbeat(self, assignment, stop):
        interval = max(0.02, min(5, assignment["lease_ms"] / 3000))
        while not stop.wait(interval):
            try:
                self.client.call("heartbeat", boot=self.boot, call_id=assignment["call_id"], fence=assignment["fence"])
            except Exception:
                # Losing authority cannot undo an already granted effect. Its
                # eventual output still enters the receipt spool.
                return

    def run_once(self):
        with _worker_ownership(self.state_dir / ".worker.lock") as ownership:
            if not self.registered:
                self.register()
                self._recover_interrupted()
            self.flush()
            with closing(self._connect()) as db, db:
                # Acknowledged observations have durable controller copies.
                db.execute("DELETE FROM attempts WHERE state IN ('acknowledged','abandoned')")
                pending = db.execute("SELECT count(*) FROM attempts").fetchone()[0]
                if pending >= self.max_unacknowledged:
                    raise ServiceError("worker_spool_full")
                self._reserve_capacity(db)
            response = self.client.call("claim", boot=self.boot)
            assignment = response.get("assignment")
            if assignment is None:
                return None
            if type(assignment) is not dict or set(assignment) != {"call_id", "boot", "fence", "binding", "lease_ms"}:
                raise ServiceError("invalid_remote_assignment")
            _identifier(assignment["call_id"])
            _integer(assignment["fence"], 1, 2**63 - 1)
            _integer(assignment["lease_ms"], 100, 300000)
            binding = assignment["binding"]
            operation = self.operations.get(binding.get("operation")) if type(binding) is dict else None
            if (assignment["boot"] != self.boot or operation is None
                    or operation.implementation != binding.get("implementation")
                    or binding.get("trust_zone") != self.trust_zone):
                raise ServiceError("invalid_remote_assignment")
            validate_binding(binding)
            if (binding["output_profile"] != operation.output_profile
                    or tuple(binding.get("required_outputs", ())) != operation.required_outputs):
                raise ServiceError("worker_output_contract_mismatch")
            if isinstance(operation.execute, RegisteredLeafExecutor) and operation.execute.reservation > binding["reservation_microusd"]:
                raise ServiceError("worker_reservation_exceeds_grant")
            try:
                with closing(self._connect()) as db, db:
                    db.execute("INSERT INTO attempts VALUES(?,?,?,'dispatching',zeroblob(?),zeroblob(?))",
                               (assignment["call_id"], assignment["fence"], canonical_json(assignment),
                                _RECEIPT_BYTES, _ACK_BYTES))
            except sqlite3.OperationalError as error:
                if getattr(error, "sqlite_errorcode", None) == getattr(sqlite3, "SQLITE_FULL", 13):
                    raise ServiceError("worker_spool_full") from None
                raise ServiceError("worker_spool_unavailable") from None
            stop, heartbeat = threading.Event(), None
            try:
                permit = self.client.call("dispatch", boot=self.boot, call_id=assignment["call_id"], fence=assignment["fence"])
                if (permit.get("execute") is not True or permit.get("call_id") != assignment["call_id"]
                        or permit.get("fence") != assignment["fence"]):
                    raise ServiceError("dispatch_not_granted")
                with closing(self._connect()) as db, db:
                    db.execute("UPDATE attempts SET state='executing' WHERE call_id=? AND fence=?",
                               (assignment["call_id"], assignment["fence"]))
                heartbeat = threading.Thread(target=self._heartbeat, args=(assignment, stop), name="mas-worker-heartbeat", daemon=True)
                heartbeat.start()
                if isinstance(operation.execute, RegisteredLeafExecutor):
                    invocation = operation.execute(binding["input"], assignment["call_id"],
                                                   ownership_descriptors=(ownership,))
                else:
                    invocation = operation.execute(binding["input"], assignment["call_id"])
                if not isinstance(invocation, Invocation) or type(invocation.output) is not dict or type(invocation.evidence) is not dict:
                    raise ServiceError("invalid_worker_output")
                self._save_receipt(assignment, "completed", invocation.output, invocation.evidence)
            except ProviderOutcomeFailure as error:
                self._save_receipt(assignment, "failed_known", output=error.completed_output, evidence={"provider_outcome": error.outcome})
            except Exception:
                self._save_receipt(assignment, "unknown")
            finally:
                stop.set()
                if heartbeat is not None:
                    heartbeat.join(self.client.timeout + 1)
            self.flush()
            return {"call_id": assignment["call_id"], "receipt_stored": True}
