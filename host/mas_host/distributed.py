"""Controller-owned remote execution of already admitted leaf invocations.

Worker leases fence new dispatch and authoritative receipts. They never prove
that an external effect stopped. The controller retains all Rust authority.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import re
import time

from .artifacts import ArtifactError, ArtifactRef
from .codec import canonical_json, integer_json
from .contracts import Invocation, Outcome, RecoveryRequired, RunnerError
from .operations import DeploymentLedger, _file_lock, _private_dir
from .provider_outcomes import ProviderOutcomeFailure, not_sent, validate_provider_outcome
from .schema import JOURNAL, LEDGER, Schema, connect
from .service_contracts import ServiceError


REMOTE = Schema("remote worker controller", 0x4D415358, 1, (), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE workers(principal TEXT PRIMARY KEY,boot TEXT NOT NULL,generation INTEGER NOT NULL,descriptor TEXT NOT NULL,draining INTEGER NOT NULL,updated INTEGER NOT NULL)",
    "CREATE TABLE boots(principal TEXT NOT NULL,boot TEXT NOT NULL,generation INTEGER NOT NULL,PRIMARY KEY(principal,boot))",
    "CREATE TABLE jobs(call_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,principal TEXT NOT NULL,binding TEXT NOT NULL,state TEXT NOT NULL,fence INTEGER,worker TEXT,boot TEXT,expires INTEGER,output TEXT,evidence TEXT,created INTEGER NOT NULL)",
    "CREATE TABLE claims(fence INTEGER PRIMARY KEY AUTOINCREMENT,call_id TEXT NOT NULL,worker TEXT NOT NULL,boot TEXT NOT NULL,dispatched INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE receipts(id TEXT PRIMARY KEY,call_id TEXT NOT NULL,fence INTEGER NOT NULL,worker TEXT NOT NULL,boot TEXT NOT NULL,payload TEXT NOT NULL,authoritative INTEGER NOT NULL,created INTEGER NOT NULL)",
    "CREATE TABLE reconciliations(call_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,operator TEXT NOT NULL,resolution_id INTEGER NOT NULL,decision TEXT NOT NULL,evidence_sha256 TEXT NOT NULL,created INTEGER NOT NULL)",
    "CREATE INDEX remote_pending ON jobs(state,created,call_id)",
    "CREATE INDEX remote_receipts ON receipts(call_id,fence)",
))
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.:/-]{1,200}\Z")
_DIGEST = re.compile(r"[a-f0-9]{64}\Z")
_ACTIVE = "('pending','claimed','dispatched','unknown')"
OUTPUT_PROFILE = "inline-json/1"
ARTIFACT_OUTPUT_PROFILE = "durable-artifacts/1"
OUTPUT_PROFILES = (OUTPUT_PROFILE, ARTIFACT_OUTPUT_PROFILE)
WORKER_PROTOCOL = "mas-worker/2"
ARTIFACT_OUTPUT_FORMAT = "mas-worker-artifact-output/1"
# Interpreter versions qualified by the package's supported Python range.
_RUNTIME = re.compile(r"python-3\.(?:10|11|12|13|14)\.[0-9]+\Z")


def output_contract(profile, required_outputs=()):
    if profile not in OUTPUT_PROFILES or type(required_outputs) not in (list, tuple):
        raise ServiceError("unsupported_worker_output_contract")
    if (len(required_outputs) > 32 or any(type(name) is not str or not _IDENTIFIER.fullmatch(name) for name in required_outputs)
            or len(set(required_outputs)) != len(required_outputs)
            or (profile == OUTPUT_PROFILE and required_outputs)
            or (profile == ARTIFACT_OUTPUT_PROFILE and not required_outputs)):
        raise ServiceError("invalid_worker_output_contract")
    return tuple(sorted(required_outputs))



def artifact_output(result, artifacts):
    """Explicit wire output for an operation configured for durable artefacts."""
    value = {"format": ARTIFACT_OUTPUT_FORMAT, "result": result, "artifacts": artifacts}
    artifact_references(value)
    integer_json(value)
    return value


def artifact_references(value):
    if (type(value) is not dict or set(value) != {"format", "result", "artifacts"}
            or value["format"] != ARTIFACT_OUTPUT_FORMAT or type(value["result"]) is not dict
            or type(value["artifacts"]) is not dict or not 1 <= len(value["artifacts"]) <= 32):
        raise ServiceError("invalid_worker_artifact_output")
    try:
        for name, reference in value["artifacts"].items():
            _identifier(name)
            ArtifactRef.from_dict(reference)
    except ArtifactError:
        raise ServiceError("invalid_worker_artifact_output") from None
    return value["artifacts"]

def _versions(protocol, runtime, profiles):
    if protocol != WORKER_PROTOCOL:
        raise ServiceError("unsupported_worker_protocol")
    if type(runtime) is not str or not _RUNTIME.fullmatch(runtime):
        raise ServiceError("unsupported_worker_runtime")
    if (type(profiles) not in (list, tuple) or not profiles or len(profiles) > len(OUTPUT_PROFILES)
            or any(type(item) is not str or item not in OUTPUT_PROFILES for item in profiles)
            or len(set(profiles)) != len(profiles)):
        raise ServiceError("unsupported_worker_output_contract")



def _identifier(value):
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise ServiceError("invalid_worker_request")
    return value


def _integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ServiceError("invalid_worker_request")
    return value


def remote_binding(principal, operation, implementation, trust_zone, input, reservation_microusd=0,
                   *, output_profile=OUTPUT_PROFILE, required_outputs=()):
    for value in (principal, operation, trust_zone):
        _identifier(value)
    if type(implementation) is not str or not _DIGEST.fullmatch(implementation) or type(input) is not dict:
        raise ServiceError("invalid_worker_request")
    _integer(reservation_microusd, 0, 100_000_000)
    names = output_contract(output_profile, required_outputs)
    extra = {"required_outputs": list(names)} if output_profile == ARTIFACT_OUTPUT_PROFILE else {}
    return json.loads(integer_json({"format": "mas-remote-invocation/1", "principal": principal,
        "operation": operation, "implementation": implementation, "trust_zone": trust_zone,
        "input": input, "reservation_microusd": reservation_microusd, "output_profile": output_profile, **extra}))


def validate_binding(binding):
    if type(binding) is not dict:
        raise ServiceError("invalid_worker_request")
    try:
        checked = remote_binding(binding["principal"], binding["operation"], binding["implementation"],
            binding["trust_zone"], binding["input"], binding["reservation_microusd"],
            output_profile=binding["output_profile"], required_outputs=binding.get("required_outputs", ()))
    except (KeyError, TypeError):
        raise ServiceError("invalid_worker_request") from None
    if checked != binding:
        raise ServiceError("invalid_worker_request")
    return checked


def validate_worker_receipt(receipt):
    """Validate bounded wire evidence before either persistence strategy."""
    if type(receipt) is not dict:
        raise ServiceError("invalid_worker_receipt")
    fields = {"id", "call_id", "boot", "fence", "status", "output", "evidence", "output_profile"}
    if receipt.get("output_profile") == ARTIFACT_OUTPUT_PROFILE:
        fields.add("artifacts")
    if set(receipt) != fields:
        raise ServiceError("invalid_worker_receipt")
    for field in ("id", "call_id", "boot"):
        _identifier(receipt[field])
    _integer(receipt["fence"], 1, 2**63 - 1)
    if (receipt["output_profile"] not in OUTPUT_PROFILES
            or receipt["status"] not in ("completed", "unknown", "failed_known") or type(receipt["evidence"]) is not dict
            or (receipt["status"] == "completed" and type(receipt["output"]) is not dict)
            or (receipt["status"] == "unknown" and receipt["output"] is not None)
            or (receipt["status"] == "failed_known" and receipt["output"] is not None and type(receipt["output"]) is not dict)):
        raise ServiceError("invalid_worker_receipt")
    if receipt["status"] == "failed_known":
        try:
            if set(receipt["evidence"]) != {"provider_outcome"}:
                raise ValueError()
            checked = validate_provider_outcome(receipt["evidence"]["provider_outcome"])
            failure = ProviderOutcomeFailure(checked, completed_output=receipt["output"])
            if failure.completed_output != receipt["output"]:
                raise ValueError()
        except (KeyError, ValueError):
            raise ServiceError("invalid_worker_receipt") from None
    # Only an explicitly negotiated artefact map denotes stored output.
    # Reference-shaped objects in ordinary inline JSON remain opaque.
    try:
        integer_json(receipt["output"])
        integer_json(receipt["evidence"])
        encoded = canonical_json(receipt, 131072 - 128)
    except RunnerError:
        raise ServiceError("invalid_worker_receipt") from None
    return encoded


class WorkerController:
    """Durable placement and receipts scoped by administrator-owned grants.

    Grants map worker principals to {capabilities:{operation:implementation},
    trust_zone, capacity}. They never come from a worker registration request.
    """
    def __init__(self, state_dir, worker_grants, *, max_outstanding=100, per_principal=10,
                 lease_ms=30000, clock=None, artifact_repository=None, artifact_store_id=None):
        self.state_dir = _private_dir(Path(state_dir), create=True)
        self.operations = DeploymentLedger(self.state_dir)
        if not self.operations.admission_enabled:
            raise ServiceError("controller_admission_closed")
        self.max_outstanding = _integer(max_outstanding, 1, 10000)
        self.per_principal = _integer(per_principal, 1, self.max_outstanding)
        self.lease_ms = _integer(lease_ms, 100, 300000)
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        if (artifact_repository is None) != (artifact_store_id is None) or (artifact_repository is not None and not callable(artifact_repository)):
            raise ServiceError("invalid_artifact_configuration")
        self.artifact_repository = artifact_repository
        self.artifact_store_id = _identifier(artifact_store_id) if artifact_store_id is not None else None
        if type(worker_grants) is not dict or len(worker_grants) > 10000:
            raise ServiceError("invalid_worker_grants")
        self.grants = json.loads(integer_json(worker_grants, 1_048_576))
        for principal, grant in self.grants.items():
            _identifier(principal)
            if type(grant) is not dict or set(grant) != {"capabilities", "trust_zone", "capacity"}:
                raise ServiceError("invalid_worker_grants")
            _identifier(grant["trust_zone"])
            _integer(grant["capacity"], 1, 64)
            capabilities = grant["capabilities"]
            if type(capabilities) is not dict or not 1 <= len(capabilities) <= 128:
                raise ServiceError("invalid_worker_grants")
            for name, digest in capabilities.items():
                _identifier(name)
                if type(digest) is not str or not _DIGEST.fullmatch(digest):
                    raise ServiceError("invalid_worker_grants")
        self.path = self.state_dir / "remote-workers.sqlite"
        self.metadata = {"deployment_id": self.operations.deployment_id,
            "policy": canonical_json({"max_outstanding": self.max_outstanding,
                                      "per_principal": self.per_principal, "lease_ms": self.lease_ms,
                                      "worker_grants": self.grants, **({"artifact_store_id": self.artifact_store_id} if self.artifact_store_id is not None else {})})}
        with _file_lock(self.state_dir / ".deployment.lock", False), _file_lock(self.state_dir / ".remote-init.lock", True):
            with closing(connect(self.path, REMOTE, initialise=lambda db: db.executemany(
                "INSERT INTO metadata VALUES(?,?)", self.metadata.items()))) as db:
                if dict(db.execute("SELECT key,value FROM metadata")) != self.metadata:
                    raise ServiceError("worker_policy_changed")

    @contextmanager
    def _transaction(self):
        with _file_lock(self.state_dir / ".deployment.lock", False):
            if not self.operations.admission_enabled:
                raise ServiceError("controller_admission_closed")
            with closing(connect(self.path, REMOTE)) as db:
                db.execute("BEGIN IMMEDIATE")
                try:
                    if dict(db.execute("SELECT key,value FROM metadata")) != self.metadata:
                        raise ServiceError("deployment_mismatch")
                    yield db
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise

    def _grant(self, principal):
        grant = self.grants.get(principal)
        if grant is None:
            raise ServiceError("worker_forbidden")
        return grant

    def _worker(self, db, principal, boot):
        grant = self._grant(principal)
        worker = db.execute("SELECT * FROM workers WHERE principal=?", (principal,)).fetchone()
        if worker is None or worker["boot"] != boot:
            raise ServiceError("stale_worker_epoch")
        descriptor = json.loads(worker["descriptor"])
        _versions(descriptor.get("protocol"), descriptor.get("runtime"), descriptor.get("output_profiles"))
        return worker, grant

    def _expire(self, db):
        # A claim without a dispatch grant is safe to make available again.
        # A granted effect remains unknown irrespective of lease expiry.
        now = self.clock()
        db.execute("UPDATE jobs SET state='pending',worker=NULL,boot=NULL,expires=NULL WHERE state='claimed' AND expires<=?", (now,))
        db.execute("UPDATE jobs SET state='unknown' WHERE state='dispatched' AND expires<=?", (now,))

    def _admitted(self, call_id, binding):
        """Check both allocation and the controller journal's exact input binding."""
        if not self.operations.admission_enabled:
            raise ServiceError("controller_admission_closed")
        with closing(connect(self.operations.path, LEDGER, readonly=True)) as ledger:
            rows = ledger.execute("SELECT c.run_id,c.reserved_microusd,r.directory FROM calls c JOIN runs r USING(run_id) WHERE c.call_id=? AND r.purged_at IS NULL", (call_id,)).fetchall()
        if len(rows) != 1 or rows[0]["reserved_microusd"] < binding["reservation_microusd"]:
            raise ServiceError("invocation_not_admitted")
        run = rows[0]
        directory = run["directory"]
        if Path(directory).name != directory:
            raise ServiceError("invocation_not_admitted")
        with closing(connect(self.state_dir / directory / "host.sqlite", JOURNAL, readonly=True)) as journal:
            row = journal.execute("SELECT i.state,s.input FROM invocations i JOIN steps s ON s.key=i.step_key WHERE i.call_id=?", (call_id,)).fetchone()
        if row is None or row["state"] != "executing" or json.loads(row["input"]).get("input") != binding:
            raise ServiceError("invocation_not_admitted")
        return run["run_id"]

    def submit_admitted(self, call_id, binding):
        _identifier(call_id)
        binding = validate_binding(binding)
        if binding["output_profile"] == ARTIFACT_OUTPUT_PROFILE and self.artifact_repository is None:
            raise ServiceError("worker_artifact_storage_unconfigured")
        with self._transaction() as db:
            run_id = self._admitted(call_id, binding)
            existing = db.execute("SELECT * FROM jobs WHERE call_id=?", (call_id,)).fetchone()
            if existing:
                if existing["binding"] != canonical_json(binding) or existing["run_id"] != run_id:
                    raise ServiceError("invocation_binding_conflict")
                return self._view(existing)
            count = db.execute("SELECT count(*) FROM jobs WHERE state IN " + _ACTIVE).fetchone()[0]
            owned = db.execute("SELECT count(*) FROM jobs WHERE principal=? AND state IN " + _ACTIVE,
                               (binding["principal"],)).fetchone()[0]
            if count >= self.max_outstanding or owned >= self.per_principal:
                raise ServiceError("worker_queue_full")
            self.operations.check_storage_admission()
            db.execute("INSERT INTO jobs(call_id,run_id,principal,binding,state,created) VALUES(?,?,?,?,'pending',?)",
                       (call_id, run_id, binding["principal"], canonical_json(binding), self.clock()))
            return self._view(db.execute("SELECT * FROM jobs WHERE call_id=?", (call_id,)).fetchone())

    @staticmethod
    def _view(job):
        return {"call_id": job["call_id"], "state": job["state"],
                "output": json.loads(job["output"]) if job["output"] is not None else None,
                "evidence": json.loads(job["evidence"]) if job["evidence"] is not None else None}

    def inspect(self, principal, call_id):
        with self._transaction() as db:
            self._expire(db)
            job = db.execute("SELECT * FROM jobs WHERE call_id=? AND principal=?", (call_id, principal)).fetchone()
            if job is None:
                raise ServiceError("not_found")
            return self._view(job)

    def abandon(self, principal, call_id):
        with self._transaction() as db:
            db.execute("UPDATE jobs SET state=CASE WHEN state IN ('pending','claimed') THEN 'cancelled' ELSE 'unknown' END WHERE call_id=? AND principal=? AND state IN ('pending','claimed','dispatched')", (call_id, principal))

    def register(self, principal, *, boot, capabilities, architecture, runtime, trust_zone, capacity=1,
                 protocol=None, output_profiles=(OUTPUT_PROFILE,)):
        grant = self._grant(principal)
        for item in (boot, architecture, runtime, trust_zone):
            _identifier(item)
        _integer(capacity, 1, grant["capacity"])
        if (type(capabilities) is not dict or not capabilities or len(capabilities) > 128
                or trust_zone != grant["trust_zone"]
                or any(grant["capabilities"].get(name) != digest for name, digest in capabilities.items())):
            raise ServiceError("worker_capability_denied")
        _versions(protocol, runtime, output_profiles)
        descriptor = canonical_json({"capabilities": capabilities, "architecture": architecture,
            "runtime": runtime, "protocol": protocol, "output_profiles": sorted(output_profiles),
            "trust_zone": trust_zone, "capacity": capacity})
        with self._transaction() as db:
            old = db.execute("SELECT * FROM workers WHERE principal=?", (principal,)).fetchone()
            if old and old["boot"] == boot:
                if old["descriptor"] != descriptor:
                    raise ServiceError("worker_registration_conflict")
                return {"worker": principal, "boot": boot, "generation": old["generation"], "lease_ms": self.lease_ms, "protocol": WORKER_PROTOCOL, "output_profiles": sorted(output_profiles)}
            if db.execute("SELECT 1 FROM boots WHERE principal=? AND boot=?", (principal, boot)).fetchone():
                raise ServiceError("stale_worker_epoch")
            generation = old["generation"] + 1 if old else 1
            db.execute("UPDATE jobs SET state='pending',worker=NULL,boot=NULL,expires=NULL WHERE worker=? AND state='claimed'", (principal,))
            db.execute("UPDATE jobs SET state='unknown' WHERE worker=? AND state='dispatched'", (principal,))
            db.execute("INSERT OR REPLACE INTO workers VALUES(?,?,?,?,0,?)", (principal, boot, generation, descriptor, self.clock()))
            db.execute("INSERT INTO boots VALUES(?,?,?)", (principal, boot, generation))
            return {"worker": principal, "boot": boot, "generation": generation, "lease_ms": self.lease_ms, "protocol": WORKER_PROTOCOL, "output_profiles": sorted(output_profiles)}

    def claim(self, principal, *, boot):
        with self._transaction() as db:
            worker, grant = self._worker(db, principal, boot)
            self._expire(db)
            if worker["draining"] or not self.operations.admission_enabled:
                return {"assignment": None}
            descriptor = json.loads(worker["descriptor"])
            active = db.execute("SELECT count(*) FROM jobs WHERE worker=? AND state IN ('claimed','dispatched','unknown')", (principal,)).fetchone()[0]
            if active >= min(grant["capacity"], descriptor["capacity"]):
                return {"assignment": None}
            for job in db.execute("SELECT * FROM jobs WHERE state='pending' ORDER BY created,call_id"):
                binding = json.loads(job["binding"])
                if (binding["output_profile"] not in descriptor["output_profiles"]
                        or binding["trust_zone"] != grant["trust_zone"]
                        or descriptor["capabilities"].get(binding["operation"]) != binding["implementation"]
                        or grant["capabilities"].get(binding["operation"]) != binding["implementation"]):
                    continue
                fence = db.execute("INSERT INTO claims(call_id,worker,boot) VALUES(?,?,?)", (job["call_id"], principal, boot)).lastrowid
                expires = self.clock() + self.lease_ms
                db.execute("UPDATE jobs SET state='claimed',worker=?,boot=?,fence=?,expires=? WHERE call_id=?",
                           (principal, boot, fence, expires, job["call_id"]))
                return {"assignment": {"call_id": job["call_id"], "boot": boot, "fence": fence,
                    "binding": binding, "lease_ms": self.lease_ms}}
            return {"assignment": None}

    def _assignment(self, db, principal, boot, call_id, fence):
        self._worker(db, principal, boot)
        _identifier(call_id)
        _integer(fence, 1, 2**63 - 1)
        self._expire(db)
        job = db.execute("SELECT * FROM jobs WHERE call_id=?", (call_id,)).fetchone()
        if (job is None or job["worker"] != principal or job["boot"] != boot or job["fence"] != fence
                or job["state"] not in ("claimed", "dispatched")):
            raise ServiceError("stale_worker_assignment")
        grant = self._grant(principal)
        binding = json.loads(job["binding"])
        if grant["capabilities"].get(binding["operation"]) != binding["implementation"] or grant["trust_zone"] != binding["trust_zone"]:
            raise ServiceError("worker_capability_denied")
        return job

    def dispatch(self, principal, *, boot, call_id, fence):
        with self._transaction() as db:
            job = self._assignment(db, principal, boot, call_id, fence)
            if job["state"] == "dispatched":
                return {"execute": False, "reason": "dispatch_already_granted"}
            self._admitted(call_id, json.loads(job["binding"]))
            db.execute("UPDATE jobs SET state='dispatched' WHERE call_id=?", (call_id,))
            db.execute("UPDATE claims SET dispatched=1 WHERE fence=?", (fence,))
            return {"execute": True, "call_id": call_id, "fence": fence, "lease_ms": self.lease_ms}

    def heartbeat(self, principal, *, boot, call_id, fence):
        with self._transaction() as db:
            self._assignment(db, principal, boot, call_id, fence)
            db.execute("UPDATE jobs SET expires=? WHERE call_id=?", (self.clock() + self.lease_ms, call_id))
            return {"lease_ms": self.lease_ms}

    def drain(self, principal, *, boot, draining=True):
        if type(draining) is not bool:
            raise ServiceError("invalid_worker_request")
        with self._transaction() as db:
            self._worker(db, principal, boot)
            db.execute("UPDATE workers SET draining=? WHERE principal=?", (int(draining), principal))
            return {"draining": draining}

    def receipt(self, principal, *, receipt):
        # Authentication is still mandatory; an old epoch may retain evidence
        # but has no authority to update the task. Revocation blocks new work;
        # retaining receipts requires an explicitly authenticated worker grant.
        grant = self._grant(principal)
        encoded = validate_worker_receipt(receipt)
        with self._transaction() as db:
            prior = db.execute("SELECT * FROM receipts WHERE id=?", (receipt["id"],)).fetchone()
            if prior:
                if prior["worker"] != principal or prior["payload"] != encoded:
                    raise ServiceError("receipt_conflict")
                job = db.execute("SELECT binding FROM jobs WHERE call_id=?", (receipt["call_id"],)).fetchone()
                self._verify_artifacts(principal, json.loads(job["binding"]), receipt, retain=False)
                return {"stored": True, "receipt_id": prior["id"], "authoritative": bool(prior["authoritative"])}
            claim = db.execute("SELECT * FROM claims WHERE fence=? AND call_id=? AND worker=? AND boot=?",
                (receipt["fence"], receipt["call_id"], principal, receipt["boot"])).fetchone()
            if claim is None or not claim["dispatched"] and receipt["status"] != "unknown":
                raise ServiceError("receipt_without_dispatch")
            self._expire(db)
            job = db.execute("SELECT * FROM jobs WHERE call_id=?", (receipt["call_id"],)).fetchone()
            worker = db.execute("SELECT * FROM workers WHERE principal=?", (principal,)).fetchone()
            binding = json.loads(job["binding"])
            if receipt["output_profile"] != binding["output_profile"]:
                raise ServiceError("worker_output_contract_mismatch")
            self._verify_artifacts(principal, binding, receipt)
            authoritative = (job["state"] == "dispatched" and job["fence"] == receipt["fence"]
                             and worker is not None and worker["boot"] == receipt["boot"]
                             and grant["capabilities"].get(binding["operation"]) == binding["implementation"]
                             and grant["trust_zone"] == binding["trust_zone"])
            if authoritative:
                try:
                    self._admitted(receipt["call_id"], json.loads(job["binding"]))
                except ServiceError:
                    authoritative = False
            if db.execute("SELECT count(*) FROM receipts WHERE call_id=?", (receipt["call_id"],)).fetchone()[0] >= 32:
                raise ServiceError("receipt_limit")
            self.operations.check_storage_admission()
            db.execute("INSERT INTO receipts VALUES(?,?,?,?,?,?,?,?)", (receipt["id"], receipt["call_id"],
                receipt["fence"], principal, receipt["boot"], encoded, int(authoritative), self.clock()))
            if authoritative:
                db.execute("UPDATE jobs SET state=?,output=?,evidence=? WHERE call_id=?",
                    (receipt["status"], canonical_json(receipt["output"]) if receipt["output"] is not None else None,
                     canonical_json({"worker": principal, "receipt_id": receipt["id"], "fence": receipt["fence"],
                                     "observation": receipt["evidence"],
                                     **({"output_profile": ARTIFACT_OUTPUT_PROFILE, "artifacts": receipt["artifacts"]}
                                        if receipt["output_profile"] == ARTIFACT_OUTPUT_PROFILE else {})}), receipt["call_id"]))
            return {"stored": True, "receipt_id": receipt["id"], "authoritative": authoritative}

    def _verify_artifacts(self, principal, binding, receipt, *, retain=True, retention_target=None):
        if receipt["output_profile"] != ARTIFACT_OUTPUT_PROFILE:
            return
        artifacts = receipt["artifacts"]
        required = binding["required_outputs"] if receipt["output"] is not None else []
        if receipt["output"] is not None and artifact_references(receipt["output"]) != artifacts:
            raise ServiceError("worker_output_contract_mismatch")
        if type(artifacts) is not dict or set(artifacts) != set(required):
            raise ServiceError("worker_output_contract_mismatch")
        if self.artifact_repository is None:
            raise ServiceError("worker_artifact_storage_unconfigured")
        # The store resolver is trusted deployment configuration, never a URL,
        # filesystem location or principal selected by a worker receipt.
        groups = {}
        try:
            for value in artifacts.values():
                ref = ArtifactRef.from_dict(value)
                publisher = self.artifact_repository(ref.namespace, principal)
                consumer = self.artifact_repository(ref.namespace, binding["principal"])
                if (publisher.namespace != ref.namespace or publisher.principal != principal
                        or consumer.namespace != ref.namespace or consumer.principal != binding["principal"]):
                    raise ArtifactError("artefact repository identity mismatch")
                publisher.inspect(ref)  # Authenticated worker has a read grant.
                if consumer.verify(ref) is not True:  # Full bytes/hash and consumer grant.
                    raise ArtifactError("artefact content integrity mismatch")
                if retention_target is not None:
                    consumer = consumer.with_retention_target(**retention_target)
                groups.setdefault(ref.namespace, (consumer, []))[1].append(ref)
            if not retain:
                return  # An acknowledged replay cannot reopen released retention.
            for namespace, (repository, references) in groups.items():
                # A crash between link and receipt leaves conservative retention.
                # Retrying the same receipt is idempotent; stale observations are
                # retained for explicit reconciliation, never granted authority.
                link = "remote-" + hashlib.sha256(canonical_json({"worker": principal,
                    "receipt": receipt["id"], "call_id": receipt["call_id"], "fence": receipt["fence"],
                    "namespace": namespace}).encode()).hexdigest()
                if callable(getattr(repository, "ensure_link", None)):
                    repository.ensure_link(link, references, state="unresolved")
                else:
                    repository.link(link, references, state="unresolved")
        except (ArtifactError, OSError, ValueError, TypeError, AttributeError):
            raise ServiceError("worker_artifact_unavailable") from None

    def evidence(self, principal, call_id):
        with self._transaction() as db:
            if db.execute("SELECT 1 FROM jobs WHERE call_id=? AND principal=?", (call_id, principal)).fetchone() is None:
                raise ServiceError("not_found")
            return [json.loads(row[0]) for row in db.execute("SELECT payload FROM receipts WHERE call_id=? ORDER BY created,id", (call_id,))]

    def reconcile(self, *, run_id, call_id, operator):
        """Release a remote obligation only from an existing host resolution.

        This is a local operator API, deliberately absent from worker RPC. It
        accepts no new outcome, does not dispatch work and never refunds calls.
        """
        for value in (run_id, call_id, operator):
            _identifier(value)
        with self._transaction() as db:
            job = db.execute("SELECT * FROM jobs WHERE call_id=? AND run_id=?", (call_id, run_id)).fetchone()
            if job is None:
                raise ServiceError("not_found")
            prior = db.execute("SELECT * FROM reconciliations WHERE call_id=?", (call_id,)).fetchone()
            if prior:
                return {"reconciled": True, "execution_resumed": False, **dict(prior)}
            self._expire(db)
            job = db.execute("SELECT * FROM jobs WHERE call_id=?", (call_id,)).fetchone()
            if job["state"] != "unknown":
                raise ServiceError("remote_resolution_not_ready")
            with closing(connect(self.operations.path, LEDGER, readonly=True)) as ledger:
                retained = ledger.execute("SELECT r.directory FROM calls c JOIN runs r USING(run_id) WHERE c.run_id=? AND c.call_id=? AND r.purged_at IS NULL", (run_id, call_id)).fetchone()
            if retained is None or Path(retained["directory"]).name != retained["directory"]:
                raise ServiceError("remote_resolution_not_ready")
            with closing(connect(self.state_dir / retained["directory"] / "host.sqlite", JOURNAL, readonly=True)) as journal:
                # One read snapshot supplies the exact decision and binding.
                # No RunLock inversion against a live Runner's remote write.
                journal.execute("BEGIN")
                identity = journal.execute("SELECT value FROM metadata WHERE key='run_id'").fetchone()
                invocation = journal.execute("SELECT i.state,i.output,s.input FROM invocations i JOIN steps s ON s.key=i.step_key WHERE i.call_id=?", (call_id,)).fetchone()
                resolution = journal.execute("SELECT * FROM resolutions WHERE call_id=? ORDER BY id DESC LIMIT 1", (call_id,)).fetchone()
                if (identity is None or json.loads(identity[0]) != run_id or invocation is None
                        or invocation["state"] not in ("resolved", "retry_allowed") or resolution is None
                        or resolution["decision"] != ("completed" if invocation["state"] == "resolved" else "confirmed_no_effect")
                        or json.loads(invocation["input"]).get("input") != json.loads(job["binding"])):
                    raise ServiceError("remote_resolution_not_ready")
                observed = {"run_id": run_id, "call_id": call_id, "resolution": dict(resolution),
                            "output": invocation["output"], "binding_sha256": hashlib.sha256(job["binding"].encode()).hexdigest()}
                digest = hashlib.sha256(canonical_json(observed, 262144).encode()).hexdigest()
            db.execute("INSERT INTO reconciliations VALUES(?,?,?,?,?,?,?)", (call_id, run_id, operator,
                resolution["id"], resolution["decision"], digest, self.clock()))
            db.execute("UPDATE jobs SET state='reconciled' WHERE call_id=?", (call_id,))
            return {"reconciled": True, "execution_resumed": False,
                    **dict(db.execute("SELECT * FROM reconciliations WHERE call_id=?", (call_id,)).fetchone())}

    def handle(self, principal, operation, payload):
        if operation not in {"register", "claim", "dispatch", "heartbeat", "receipt", "drain"} or type(payload) is not dict:
            raise ServiceError("invalid_worker_request")
        try:
            return getattr(self, operation)(principal, **payload)
        except TypeError:
            raise ServiceError("invalid_worker_request") from None


class RemoteWorkerAdapter:
    """One remote leaf effect under the ordinary Runner invocation journal."""
    def __init__(self, controller, *, principal, operation, implementation, trust_zone,
                 reservation_microusd=0, poll_seconds=0.05, output_profile=OUTPUT_PROFILE, required_outputs=()):
        self.controller = controller
        self.binding = remote_binding(principal, operation, implementation, trust_zone, {}, reservation_microusd,
            output_profile=output_profile, required_outputs=required_outputs)
        self.reservation = _integer(reservation_microusd, 0, 100_000_000)
        if type(poll_seconds) not in (int, float) or not 0.01 <= poll_seconds <= 5:
            raise ServiceError("invalid_worker_request")
        self.poll_seconds = poll_seconds

    def handle(self, context, request):
        binding = {**self.binding, "input": json.loads(integer_json(request.input))}
        def perform():
            call_id = context.invocation_id
            if not call_id:
                raise ServiceError("invocation_not_admitted")
            try:
                self.controller.submit_admitted(call_id, binding)
            except ServiceError as error:
                # Queue saturation is established before a job or claim exists.
                if error.code == "worker_queue_full":
                    raise not_sent("custom", "operation_busy") from None
                raise
            try:
                while context.remaining_seconds > 0:
                    report = self.controller.inspect(binding["principal"], call_id)
                    if report["state"] == "completed":
                        return Invocation(report["output"], report["evidence"])
                    if report["state"] == "failed_known":
                        raise ProviderOutcomeFailure(report["evidence"]["observation"]["provider_outcome"],
                                                     completed_output=report["output"])
                    if report["state"] == "cancelled":
                        # Only pending/claimed jobs enter cancelled. A dispatch
                        # grant can only become unknown, even after expiry.
                        raise not_sent("custom", "operation_busy")
                    if report["state"] == "unknown":
                        raise RecoveryRequired("remote invocation requires reconciliation")
                    time.sleep(min(self.poll_seconds, max(0, context.remaining_seconds)))
                self.controller.abandon(binding["principal"], call_id)
                if self.controller.inspect(binding["principal"], call_id)["state"] == "cancelled":
                    raise not_sent("custom", "operation_busy")
                raise RecoveryRequired("remote invocation deadline exceeded")
            finally:
                self.controller.abandon(binding["principal"], call_id)
        return Outcome(context.invoke(perform, input=binding, reservation_microusd=self.reservation))
