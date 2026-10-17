"""Authorised recovery inbox and evidence decisions over retained run journals.

Inspection and queries never resume execution. An operator resolution is a
separate audited mutation; the normal task service still controls recovery.
The registry's query functions are trusted, read-only provider integrations,
not generation callbacks. Absence from a provider search proves nothing here.
"""
from __future__ import annotations

from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import re
from typing import Callable, Mapping

from .codec import integer_json
from .journal import JournalError, canonical, inspect_run, resolve_invocation
from .operations import _file_lock
from .service_contracts import ServiceError


def _identity(value):
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,199}", value):
        raise ServiceError("invalid_recovery_identity")
    return value


def _copy(value, limit=32_768):
    try:
        return json.loads(integer_json(value, limit))
    except Exception:
        raise ServiceError("invalid_recovery_evidence") from None


def validate_evidence(value: dict) -> dict:
    """Require a source, exact reference and observation time; exclude raw text."""
    if (type(value) is not dict or set(value) != {"source", "reference", "observed_at_ms"}
            or value["source"] not in {"provider_receipt", "provider_query", "operator_attestation"}
            or type(value["observed_at_ms"]) is not int or not 0 <= value["observed_at_ms"] < 2**63):
        raise ServiceError("invalid_recovery_evidence")
    _identity(value["reference"])
    return _copy(value)


def observation(value: dict, call_id: str, *, permits_no_effect=False) -> dict:
    if (type(value) is not dict or set(value) != {"call_id", "certainty", "evidence", "output"}
            or value["call_id"] != call_id
            or value["certainty"] not in {"unknown", "completed", "confirmed_no_effect"}
            or value["certainty"] == "confirmed_no_effect" and not permits_no_effect
            or value["certainty"] == "completed" and type(value["output"]) is not dict
            or value["certainty"] != "completed" and value["output"] is not None):
        raise ServiceError("invalid_recovery_observation")
    validate_evidence(value["evidence"])
    return _copy(value)


@dataclass(frozen=True)
class ReconciliationQuery:
    """A deployment-pinned read-only evidence contract, separate from retry."""
    name: str
    version: str
    query: Callable[[dict], dict]
    permits_no_effect: bool = False

    def __post_init__(self):
        _identity(self.name)
        _identity(self.version)
        if not callable(self.query) or type(self.permits_no_effect) is not bool:
            raise ServiceError("invalid_reconciliation_query")

    @property
    def descriptor(self):
        return {"format": "mas-reconciliation-query/1", "name": self.name, "version": self.version,
                "permits_no_effect": self.permits_no_effect}


class RecoveryService:
    """Local operator API with grants independent of task-owner permissions.

    ``operators`` maps authenticated operator IDs to task owners they may
    inspect and resolve. The caller supplies those identities from a trusted
    boundary, never from an untrusted task body. Query contracts are explicitly
    selected by name and version; no provider search silently becomes retry.
    """

    def __init__(self, service, *, operators: Mapping[str, set[str]],
                 queries: Mapping[str, ReconciliationQuery] | None = None):
        if not callable(getattr(service, "recovery_runs", None)):
            raise ServiceError("recovery_not_supported")
        self.service = service
        self.state_dir = Path(service.state_dir).resolve()
        self.operators = {_identity(actor): frozenset(_identity(owner) for owner in owners)
                          for actor, owners in operators.items()}
        self.queries = dict(queries or {})
        if any(not isinstance(query, ReconciliationQuery) or name != query.name
               for name, query in self.queries.items()):
            raise ServiceError("invalid_reconciliation_query")

    def _runs(self, operator, principal, task_id):
        _identity(operator)
        _identity(principal)
        if principal not in self.operators.get(operator, ()):
            raise ServiceError("forbidden")
        # This hook rechecks owner visibility and current workflow grants.
        runs = self.service.recovery_runs(principal, task_id)
        if type(runs) is not list or len(runs) > 10_000:
            raise ServiceError("invalid_recovery_runs")
        checked = []
        for item in runs:
            directory = item.get("run_directory")
            if (type(directory) is not str or Path(directory).name != directory
                    or directory in {"", ".", ".."}):
                raise ServiceError("invalid_recovery_runs")
            path = self.state_dir / directory
            if path.is_symlink() or path.parent.resolve() != self.state_dir:
                raise ServiceError("invalid_recovery_runs")
            checked.append((item, path))
        return checked

    @staticmethod
    def _stopped(directory):
        """Only a successfully acquired existing ownership lock proves local stop."""
        try:
            descriptor = os.open(directory / ".host.lock", os.O_RDWR | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
        finally:
            os.close(descriptor)

    def inbox(self, operator, principal, task_id):
        runs = self._runs(operator, principal, task_id)
        current = self.service.inspect(principal, task_id)
        entries, stopped, missing = [], [], False
        for binding, directory in runs:
            stopped.append(self._stopped(directory))
            if not (directory / "host.sqlite").is_file():
                missing = missing or binding["state"] not in {"queued", "prepared", "cancelled"}
                continue
            report = inspect_run(directory)
            for call in report["invocations"]:
                if call["state"] not in {"executing", "unknown"}:
                    continue
                entries.append({"run_id": report["run_id"], "path": binding.get("path"),
                    "revision": binding["revision"], "call_id": call["call_id"], "actor": call["actor"],
                    "state": call["state"], "evidence": call["evidence"],
                    "reserved_microusd": call["reservation_microusd"],
                    "admissible_decisions": ["completed", "confirmed_no_effect"],
                    "required_evidence": ["source", "reference", "observed_at_ms"],
                    "automatic_retry": False})
        local_stop = (False if False in stopped else None if None in stopped else True)
        if not runs:
            local_stop = current["state"] not in {"running", "unknown"}
        remote = self._remote_observations({directory.name for _, directory in runs})
        cancellation = {"requested": bool(current["cancel_requested"]),
            "future_dispatch_stopped": bool(current["cancel_requested"]) or current["state"] in
                {"completed", "failed", "cancelled", "unknown", "blocked", "input_required", "auth_required"},
            "local_execution_stopped": local_stop,
            "remote_cancellation": "acknowledged" if remote and all(
                item["state"] == "cancelled" for item in remote) else "not_established",
            "effect_reconciliation": "required" if entries or missing or any(
                item["uncertain"] for item in remote) else "not_required"}
        return _copy({"format": "mas-recovery-inbox/1", "task_id": task_id, "state": current["state"],
                      "cancellation": cancellation, "entries": entries, "remote_observations": remote,
                      "queries": [query.descriptor for query in self.queries.values()]}, 512 * 1024)

    def _remote_observations(self, directories):
        """Local receipts only; provider freshness and cancellation remain separate."""
        from contextlib import closing
        from .schema import JOBS, connect
        from .bridges.remote import REMOTE_TASKS
        observed = []
        path = self.state_dir / "remote-tasks.sqlite"
        if path.exists():
            with closing(connect(path, REMOTE_TASKS, readonly=True)) as db:
                for row in db.execute("SELECT * FROM remote_tasks"):
                    if row["host_run_dir"] not in directories:
                        continue
                    uncertain = bool(row["pending_action"]) or row["state"] == "unknown" or db.execute(
                        "SELECT 1 FROM operations WHERE key=? AND state='unknown' LIMIT 1", (row["key"],)).fetchone() is not None
                    observed.append({"kind": "remote_task", "key": row["logical_key"],
                        "remote_id": row["remote_id"], "state": row["state"], "uncertain": uncertain,
                        "cancel_requested": bool(row["cancel_requested"]), "observed_at_ms": row["updated"] * 1000})
        path = self.state_dir / "cursor-jobs.sqlite"
        if path.exists():
            from .durable_jobs import JobStore
            with closing(connect(path, JOBS, readonly=True)) as db:
                for row in db.execute("SELECT * FROM jobs"):
                    reference = row["host_run_dir"]
                    if reference is None or Path(reference).name not in directories:
                        continue
                    # Reuse the job store's binding and exact agent/run checks
                    # before treating its retained snapshot as remote evidence.
                    row = JobStore.row(db, row["job_id"])
                    snapshot = json.loads(row["snapshot"]) if row["snapshot"] else {}
                    status = snapshot.get("remote_status")
                    observed.append({"kind": "cursor_job", "key": row["job_id"],
                        "agent_id": row["agent_id"], "remote_id": row["run_id"],
                        "state": "cancelled" if status == "CANCELLED" else row["state"],
                        "remote_status": status,
                        "uncertain": row["state"] in {"prepared", "launch_unknown"} or
                            (row["state"] != "terminal" and bool(row["cancel_requested"])),
                        "cancel_requested": bool(row["cancel_requested"]), "observed_at_ms": row["updated"] * 1000})
        return observed

    def _invocation(self, operator, principal, task_id, call_id):
        _identity(call_id)
        matches = []
        for binding, directory in self._runs(operator, principal, task_id):
            if not (directory / "host.sqlite").is_file():
                continue
            report = inspect_run(directory)
            for call in report["invocations"]:
                if call["call_id"] == call_id:
                    matches.append((binding, directory, report, call))
        if len(matches) != 1:
            raise ServiceError("invocation_not_found")
        return matches[0]

    def query(self, operator, principal, task_id, call_id, *, query_name, query_version):
        _binding, _directory, report, call = self._invocation(operator, principal, task_id, call_id)
        contract = self.queries.get(query_name)
        if contract is None or contract.version != query_version:
            raise ServiceError("reconciliation_contract_mismatch")
        if call["state"] not in {"executing", "unknown"}:
            raise ServiceError("invocation_not_uncertain")
        request = {"task_id": task_id, "run_id": report["run_id"], "call_id": call_id,
                   "actor": call["actor"], "evidence": call["evidence"]}
        from .telemetry import span
        with span(self.state_dir, 'reconciliation',
                  task_id=task_id, run_id=report['run_id'], invocation_id=call_id):
            observed = observation(contract.query(_copy(request)), call_id,
                                   permits_no_effect=contract.permits_no_effect)
        return {"contract": contract.descriptor, "observation": observed, "resolved": False}

    def resolve(self, operator, principal, task_id, call_id, *, request_key,
                decision, evidence: dict, output: dict | None = None):
        _identity(request_key)
        if decision not in {"completed", "confirmed_no_effect"}:
            raise ServiceError("invalid_recovery_decision")
        supplied = validate_evidence(evidence)
        if ((decision == "completed" and type(output) is not dict)
                or (decision != "completed" and output is not None)):
            raise ServiceError("invalid_recovery_output")
        output = _copy(output) if output is not None else None
        binding, directory, report, call = self._invocation(operator, principal, task_id, call_id)
        audit = {"format": "mas-recovery-decision/1", "operator": operator, "owner": principal,
                 "task_id": task_id, "request_key": request_key, "observation": supplied}

        def existing_resolution(snapshot):
            matches = [item for item in snapshot["resolutions"] if item["call_id"] == call_id]
            if not matches:
                return False
            previous = matches[-1]
            saved = next(item for item in snapshot["invocations"] if item["call_id"] == call_id)
            if (previous["decision"] != decision or canonical(previous["evidence"]) != canonical(audit)
                    or canonical(saved["output"]) != canonical(output)):
                raise ServiceError("recovery_decision_conflict")
            return True

        operations = self.service.operations
        with _file_lock(self.state_dir / ".deployment.lock", False):
            if operations.readonly or not operations.admission_enabled:
                raise ServiceError("audit_only")
            if not existing_resolution(report):
                try:
                    resolve_invocation(directory, call_id, decision=decision, evidence=audit, output=output)
                except JournalError:
                    if not existing_resolution(inspect_run(directory)):
                        raise ServiceError("recovery_not_available") from None
        return {"task_id": task_id, "path": binding.get("path"), "revision": binding["revision"],
                "call_id": call_id, "decision": decision, "resolved": True, "execution_resumed": False}
