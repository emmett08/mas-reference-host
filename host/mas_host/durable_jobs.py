"""Durable external jobs, separate from a synchronous MAS task's submission.

Intent and the client-assigned remote agent ID precede dispatch. A lost create
reply never permits another create. Operators may attach an evidenced exact run;
polling and cancellation always address that run, never the agent's latest run.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

from .contracts import AdapterContext, Invocation, Outcome, TaskRequest
from .cursor_cloud import CloudClient, CloudError, CloudJobSpec, STATUSES, TERMINAL, identifier, run_snapshot
from .journal import canonical
from .operations import _file_lock, _private_dir
from .schema import JOBS, JOURNAL, LEDGER, SchemaError, connect


def job_identifier(value: str) -> str:
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", value):
        raise CloudError("job ID must contain 1 to 100 letters, digits, underscores or hyphens")
    return value


def binding(spec: CloudJobSpec) -> str:
    return hashlib.sha256(canonical(asdict(spec)).encode("utf-8")).hexdigest()


def _operator_evidence(evidence: dict):
    from .codec import integer_json
    if type(evidence) is not dict or not evidence:
        raise CloudError("cloud reconciliation requires nonempty operator evidence")
    try:
        integer_json(evidence, 8192)
    except Exception:
        raise CloudError("operator evidence must be a bounded integer JSON object") from None


class JobStore:
    """Short database sections and independent per-job network ownership.

    The deployment shared lock coordinates backup/restore and retention. The job
    lock serialises create, poll, attach and cancellation for one job, preventing
    stale polls from overwriting terminal state. The global database lock and
    SQLite connection never span a network wait.
    """
    def __init__(self, state_dir: Path):
        self.state_dir = Path(state_dir).absolute()
        self.path = self.state_dir / "cursor-jobs.sqlite"

    @contextmanager
    def open(self, *, readonly: bool = False):
        _private_dir(self.state_dir, create=not readonly)
        with _file_lock(self.state_dir / ".deployment.lock", False, nonblocking=True), \
                _file_lock(self.state_dir / ".cursor-jobs.lock", not readonly, nonblocking=True):
            if not readonly:
                self.require_mutable()
            try:
                db = connect(self.path, JOBS, readonly=readonly,
                             initialise=None if readonly else lambda _: None)
            except SchemaError as error:
                raise CloudError(str(error)) from None
            try:
                yield db
            finally:
                db.close()

    @contextmanager
    def operation(self, job_id: str):
        """Keep maintenance exclusion and one job owner through remote dispatch.

        Lock files are permanent: unlinking a live inode could create a second
        owner. Reads and intent preparation do not take this operation lock.
        """
        digest = hashlib.sha256(job_identifier(job_id).encode()).hexdigest()
        _private_dir(self.state_dir, create=True)
        with _file_lock(self.state_dir / ".deployment.lock", False, nonblocking=True), \
                _file_lock(self.state_dir / (".cursor-job-" + digest + ".lock"), True, nonblocking=True):
            self.require_mutable()
            yield

    def require_mutable(self):
        """Backups are audit-only: a copied job must never affect its original."""
        if (self.state_dir / ".restored-audit-only").exists():
            raise CloudError("restored cloud job state is audit-only")
        ledger = self.state_dir / "deployment.sqlite"
        if ledger.exists():
            try:
                db = connect(ledger, LEDGER, readonly=True)
                try:
                    row = db.execute("SELECT value FROM metadata WHERE key='admission_state'").fetchone()
                    if row is None or row[0] != "active":
                        raise CloudError("deployment does not permit cloud job mutations")
                finally:
                    db.close()
            except SchemaError as error:
                raise CloudError(str(error)) from None

    def host_directory(self, directory: Path) -> Path:
        directory = Path(directory).absolute()
        if directory.parent != self.state_dir or directory.is_symlink():
            raise CloudError("cloud job host directory must belong to its deployment")
        return directory

    def verify_admission(self, directory: Path, call_id: str, reservation: int, *, resolving=False):
        """Bind the job to the host checkpoint and charged deployment admission."""
        directory = self.host_directory(directory)
        try:
            _private_dir(directory)
            journal = connect(directory / "host.sqlite", JOURNAL, readonly=True)
            try:
                row = journal.execute("SELECT state,reservation_microusd FROM invocations WHERE call_id=?", (call_id,)).fetchone()
                run_row = journal.execute("SELECT value FROM metadata WHERE key='run_id'").fetchone()
                allowed = ("executing", "unknown") if resolving else ("executing",)
                if row is None or row["state"] not in allowed or row["reservation_microusd"] != reservation or run_row is None:
                    raise CloudError("cloud job does not match an admitted host invocation")
                run_id = json.loads(run_row[0])
            finally:
                journal.close()
            ledger = connect(self.state_dir / "deployment.sqlite", LEDGER, readonly=True)
            try:
                admitted = ledger.execute("SELECT calls.reserved_microusd,runs.directory,runs.purged_at FROM calls JOIN runs USING(run_id) WHERE run_id=? AND call_id=?", (run_id, call_id)).fetchone()
                if (admitted is None or admitted[0] != reservation or admitted[1] != directory.name
                        or admitted[2] is not None):
                    raise CloudError("cloud job has no matching deployment admission")
            finally:
                ledger.close()
        except CloudError:
            raise
        except Exception:
            raise CloudError("cloud job host admission could not be verified") from None

    @staticmethod
    def row(db, job_id):
        row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_identifier(job_id),)).fetchone()
        if row is None:
            raise CloudError("unknown local cloud job")
        result = dict(row)
        try:
            spec = CloudJobSpec(**json.loads(result["spec"]))
            identifier(result["agent_id"], "bc-")
            if result["binding"] != binding(spec) or result["state"] not in ("prepared", "launch_unknown", "active", "terminal"):
                raise ValueError()
            if result["cancel_requested"] not in (0, 1):
                raise ValueError()
            if result["state"] in ("prepared", "launch_unknown"):
                if result["run_id"] is not None or result["snapshot"] is not None:
                    raise ValueError()
            else:
                identifier(result["run_id"], "run-")
                snapshot = json.loads(result["snapshot"])
                if (snapshot["agent_id"] != result["agent_id"] or snapshot["run_id"] != result["run_id"]
                        or snapshot["remote_status"] not in STATUSES
                        or (snapshot["remote_status"] in TERMINAL) != (result["state"] == "terminal")):
                    raise ValueError()
        except (CloudError, KeyError, TypeError, ValueError):
            raise CloudError("unsupported or inconsistent durable cloud job state") from None
        return result

    @staticmethod
    def audit(db, job_id: str, event: str, evidence: dict):
        db.execute("INSERT INTO audit(job_id,event,evidence,created) VALUES(?,?,?,?)",
                   (job_id, event, canonical(evidence), int(time.time())))

    @staticmethod
    def public(row: dict, *, include_result=False) -> dict:
        result = {key: row[key] for key in ("job_id", "binding", "agent_id", "run_id", "state",
                                           "created", "updated", "host_run_dir", "call_id")}
        result["cancel_requested"] = bool(row["cancel_requested"])
        result["remote_complete"] = row["state"] == "terminal"
        snapshot = json.loads(row["snapshot"]) if row["snapshot"] else {}
        result.update({key: value for key, value in snapshot.items() if key != "result" or include_result})
        return result

    def prepare(self, job_id: str, spec: CloudJobSpec) -> dict:
        job_identifier(job_id)
        with self.open() as db:
            row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is not None:
                if row["binding"] != binding(spec) or row["spec"] != canonical(asdict(spec)):
                    raise CloudError("cloud job configuration differs from durable intent")
                return self.public(self.row(db, job_id))
            with db:
                now = int(time.time())
                db.execute("INSERT INTO jobs(job_id,spec,binding,agent_id,state,created,updated) VALUES(?,?,?,?,?,?,?)",
                           (job_id, canonical(asdict(spec)), binding(spec), "bc-" + str(uuid.uuid4()),
                            "prepared", now, now))
                self.audit(db, job_id, "intent", {"binding": binding(spec)})
            return self.public(self.row(db, job_id))

    def status(self, job_id: str, *, include_result=False) -> dict:
        with self.open(readonly=True) as db:
            return self.public(self.row(db, job_id), include_result=include_result)

    def inspect(self, job_id: str, *, include_result=False) -> dict:
        with self.open(readonly=True) as db:
            result = self.public(self.row(db, job_id), include_result=include_result)
            result["audit"] = [{"event": row["event"], "created": row["created"],
                                "evidence": json.loads(row["evidence"])}
                               for row in db.execute("SELECT * FROM audit WHERE job_id=? ORDER BY id", (job_id,))]
            return result

    def spec(self, job_id: str) -> CloudJobSpec:
        with self.open(readonly=True) as db:
            return CloudJobSpec(**json.loads(self.row(db, job_id)["spec"]))

    def service(self, client=None):
        return DurableJobService(self, client)


def receipt(row: dict) -> dict:
    if row["run_id"] is None:
        raise CloudError("cloud launch outcome unknown; attach an evidenced remote run before continuing")
    return {"job_id": row["job_id"], "provider": "cursor-cloud", "submitted": True, "submission_only": True,
            "cursor_agent_id": row["agent_id"], "cursor_run_id": row["run_id"]}


class DurableJobService:
    def __init__(self, store: JobStore, client: CloudClient | None = None):
        self.store, self.client = store, client

    def _remote(self) -> CloudClient:
        if self.client is None:
            raise CloudError("this cloud job operation requires a configured API client")
        return self.client

    @staticmethod
    def _snapshot(db, row: dict, snapshot: dict, event: str, evidence: dict):
        if row["state"] == "terminal":
            return  # Terminal records and their result are immutable.
        state = "terminal" if snapshot["remote_status"] in TERMINAL else "active"
        if event == "observed" and row["state"] == state and row["snapshot"] == canonical(snapshot):
            return  # Repeated unchanged polls must not grow the durable audit.
        with db:
            db.execute("UPDATE jobs SET run_id=?,state=?,snapshot=?,updated=? WHERE job_id=?",
                       (snapshot["run_id"], state, canonical(snapshot), int(time.time()), row["job_id"]))
            JobStore.audit(db, row["job_id"], event, evidence)

    def _correlated(self, db, expected):
        current = self.store.row(db, expected["job_id"])
        if current != expected:
            raise CloudError("cloud job changed during remote operation")
        return current

    def submit(self, job_id: str, spec: CloudJobSpec, *, call_id: str, host_run_dir: Path) -> Invocation:
        if type(call_id) is not str or not re.fullmatch(r"call-[A-Za-z0-9_-]{1,128}", call_id):
            raise CloudError("cloud submission requires an admitted MAS invocation")
        host_run_dir = self.store.host_directory(host_run_dir)
        with self.store.operation(job_id):
            with self.store.open() as db:
                self.store.verify_admission(host_run_dir, call_id, spec.reservation_microusd)
                row = self.store.row(db, job_id)
                if row["binding"] != binding(spec):
                    raise CloudError("cloud job configuration differs from durable intent")
                if row["state"] != "prepared":
                    # A new host invocation cannot claim an old charged submission.
                    if row["call_id"] != call_id or row["host_run_dir"] != str(Path(host_run_dir).absolute()):
                        raise CloudError("cloud job already belongs to another invocation; reconcile the original")
                    return self._invocation(row, spec)
                with db:
                    db.execute("UPDATE jobs SET state='launch_unknown',host_run_dir=?,call_id=?,updated=? WHERE job_id=?",
                               (str(Path(host_run_dir).absolute()), call_id, int(time.time()), job_id))
                    self.store.audit(db, job_id, "dispatch_intent", {"call_id": call_id})
                row = self.store.row(db, job_id)
            # launch_unknown is committed BEFORE POST, including hard process death.
            response = self._remote().create(spec.payload(row["agent_id"]))
            if (type(response) is not dict or type(response.get("agent")) is not dict
                    or response["agent"].get("id") != row["agent_id"]):
                raise CloudError("Cursor launch response does not match durable agent identity")
            snapshot = run_snapshot(response.get("run"), row["agent_id"])
            with self.store.open() as db:
                current = self._correlated(db, row)
                self._snapshot(db, current, snapshot, "submitted", {"run_id": snapshot["run_id"]})
                return self._invocation(self.store.row(db, job_id), spec)

    @staticmethod
    def _invocation(row, spec):
        return Invocation(receipt(row), {"provider": "cursor-cloud", "model": spec.model,
                          "contract": "repository-job-submission/1", "job_id": row["job_id"],
                          "cursor_agent_id": row["agent_id"], "cursor_run_id": row["run_id"],
                          "reserved_microusd": spec.reservation_microusd,
                          "spending_basis": "configured_reservation_not_provider_invoice"})

    def poll(self, job_id: str) -> dict:
        with self.store.operation(job_id):
            with self.store.open() as db:
                row = self.store.row(db, job_id)
                if row["state"] == "terminal":
                    return self.store.public(row)
                receipt(row)
            snapshot = run_snapshot(self._remote().run(row["agent_id"], row["run_id"]), row["agent_id"], row["run_id"])
            with self.store.open() as db:
                current = self._correlated(db, row)
                self._snapshot(db, current, snapshot, "observed", {"remote_status": snapshot["remote_status"]})
                return self.store.public(self.store.row(db, job_id))

    def attach(self, job_id: str, run_id: str, *, evidence: dict) -> dict:
        """Audited reconciliation of an uncertain launch; never infer latestRunId."""
        identifier(run_id, "run-")
        _operator_evidence(evidence)
        with self.store.operation(job_id):
            with self.store.open() as db:
                row = self.store.row(db, job_id)
                if row["state"] != "launch_unknown" or row["run_id"] is not None:
                    raise CloudError("only an uncertain launch without a run can be attached")
                spec = CloudJobSpec(**json.loads(row["spec"]))
            agent = self._remote().agent(row["agent_id"])
            if (type(agent) is not dict or agent.get("id") != row["agent_id"]
                    or type(agent.get("repos")) is not list or len(agent["repos"]) != 1
                    or type(agent["repos"][0]) is not dict
                    or agent["repos"][0].get("url") != spec.repository
                    or agent["repos"][0].get("startingRef") != spec.ref
                    or agent.get("autoCreatePR") is not spec.auto_create_pr
                    or agent.get("workOnCurrentBranch") is not False):
                raise CloudError("remote agent configuration does not match durable intent")
            snapshot = run_snapshot(self._remote().run(row["agent_id"], run_id), row["agent_id"], run_id)
            with self.store.open() as db:
                current = self._correlated(db, row)
                self._snapshot(db, current, snapshot, "operator_attached", evidence)
                return self.store.public(self.store.row(db, job_id))

    def cancel(self, job_id: str, *, retry: bool = False) -> dict:
        with self.store.operation(job_id):
            with self.store.open() as db:
                row = self.store.row(db, job_id)
                if row["state"] == "terminal":
                    return self.store.public(row)
                receipt(row)
                if row["cancel_requested"] and not retry:
                    raise CloudError("cancellation already requested; reconcile or explicitly retry cancellation")
                with db:
                    db.execute("UPDATE jobs SET cancel_requested=1,updated=? WHERE job_id=?", (int(time.time()), job_id))
                    self.store.audit(db, job_id, "cancel_requested", {"run_id": row["run_id"], "retry": retry})
                row = self.store.row(db, job_id)
            response = self._remote().cancel(row["agent_id"], row["run_id"])
            if type(response) is not dict or response.get("id") != row["run_id"]:
                raise CloudError("Cursor cancellation response does not match durable run identity")
            # Acknowledged request alone does not change remote state to CANCELLED.
            with self.store.open() as db:
                self._correlated(db, row)
                with db:
                    self.store.audit(db, job_id, "cancel_acknowledged", {"run_id": row["run_id"]})
                return self.store.public(self.store.row(db, job_id))

    def resolve_submission(self, job_id: str, *, evidence: dict) -> dict:
        """Resolve only the MAS submission receipt, never claim remote completion."""
        from .journal import resolve_invocation
        _operator_evidence(evidence)
        with self.store.open(readonly=True) as db:
            self.store.require_mutable()
            row = self.store.row(db, job_id)
            output = receipt(row)
            if not row["host_run_dir"] or not row["call_id"]:
                raise CloudError("cloud job has no recorded host invocation")
        # RunLock must not be acquired under job lock: the runner acquires them
        # in the opposite order while entering an adapter.
        host_dir = self.store.host_directory(Path(row["host_run_dir"]))
        self.store.verify_admission(host_dir, row["call_id"], self.store.spec(job_id).reservation_microusd, resolving=True)
        return resolve_invocation(host_dir, row["call_id"], decision="completed",
                                  output=output, evidence={"cloud_job": job_id, "submission_only": True,
                                                           "operator": evidence})


class CursorCloudJobAdapter:
    """MAS Adapter for one charged submission, returning a durable job receipt."""
    def __init__(self, service: DurableJobService, spec: CloudJobSpec, host_run_dir: Path | None = None):
        self.service, self.spec, self.host_run_dir = service, spec, host_run_dir

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        if request.operation != "cloud.submit" or set(request.input) != {"job_id"}:
            raise CloudError("unsupported cloud job operation or input")
        job_id = job_identifier(request.input["job_id"])
        if self.host_run_dir is None:
            raise CloudError("cloud adapter requires its host run directory")
        self.service.store.prepare(job_id, self.spec)
        output = context.invoke(lambda: self.service.submit(job_id, self.spec,
                                call_id=context.invocation_id, host_run_dir=self.host_run_dir),
                                input={"adapter": self.spec.descriptor(), "job_id": job_id,
                                       "binding": binding(self.spec)},
                                reservation_microusd=self.spec.reservation_microusd)
        return Outcome(output)
