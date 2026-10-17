"""Crash-recoverable run creation before adapters receive execution authority.

An atomically published intent precedes deployment admission. Every subsequent
stage is idempotent and retains the original run identity, Rust deadline and
gateway credentials. A ready run never recreates missing state or credentials.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import tempfile
import uuid

from .codec import integer_json as _json
from .contracts import RunnerError
from .journal import Journal, RunLock


_FORMAT = "mas-host-bootstrap/1"
_STAGES = ("intent", "admitted", "protocol_ready", "journal_ready", "ready")


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json(path: Path, value: dict) -> None:
    """Publish only a complete private file, including its directory entry."""
    source = _json(value)
    temporary = path.with_name("." + path.name + ".pending")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(source + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    _sync_directory(path.parent)


def _read_json(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise RunnerError("bootstrap state must be a regular private file")
    value = json.loads(path.read_text(encoding="utf-8"))
    if type(value) is not dict:
        raise RunnerError("invalid bootstrap state")
    return value


def requires_reconciliation(run_dir: Path, expected_run_id: str, *, operations=None) -> bool:
    """Observe retained effects before stopping an assigned run, without writes.

    Missing state is safe only when durable bootstrap evidence places the run
    before journal publication. An already published or legacy journal cannot
    disappear and thereby turn uncertain work into confirmed cancellation.
    Callers must own the assigned run or its enclosing scheduler while reading.
    """
    from .journal import inspect_run
    directory = Path(run_dir)

    def admitted():
        if operations is not None:
            return operations.has_run(expected_run_id)
        # RunnerExecutor also permits Runner's implicit ledger selection. Do
        # not construct that ledger during cancellation: creation is a write.
        from contextlib import closing
        from .schema import LEDGER, connect
        ledger = directory.parent / 'deployment.sqlite'
        if ledger.is_symlink():
            return True
        if not ledger.exists():
            return False
        with closing(connect(ledger, LEDGER, readonly=True)) as db:
            return db.execute('SELECT 1 FROM runs WHERE run_id=?', (expected_run_id,)).fetchone() is not None

    try:
        if directory.is_symlink():
            return True
        if not directory.exists():
            return admitted()
        journal = directory / 'host.sqlite'
        if journal.is_symlink():
            return True
        if journal.exists():
            report = inspect_run(directory)
            return report['run_id'] != expected_run_id or report['recovery_required']
        manifest = directory / 'bootstrap.json'
        if not manifest.exists() and not manifest.is_symlink():
            return (admitted()
                    or any(path.name != '.bootstrap.json.pending' for path in directory.iterdir()))
        intent = _read_json(manifest)
        return (set(intent) != {'format', 'stage', 'run_id', 'workflow_version',
                                'max_depth', 'topology', 'gateway_id'}
                or intent['format'] != _FORMAT or intent['run_id'] != expected_run_id
                or intent['stage'] not in {'intent', 'admitted', 'protocol_ready'})
    except Exception:
        # Unreadable or malformed retained evidence cannot establish no effect.
        return True


class RunSession:
    """Own the run directory, exclusive lock, bootstrap and journal lifetime."""

    def __init__(self, state_dir: Path, *, operations, client, command,
                 topology: dict, workflow_version: str, max_depth: int,
                 actors: set[str], resume_dir: Path | None = None,
                 command_timeout: float = 10):
        self.journal = None
        self.lock = None
        self.run_id = "run-" + uuid.uuid4().hex
        self.run_dir = (Path(resume_dir).absolute() if resume_dir is not None else
                        Path(tempfile.mkdtemp(prefix=self.run_id + "-", dir=state_dir)))
        self.database = self.run_dir / "state.sqlite"
        self.configuration = self.run_dir / "gateway.json"
        self._manifest = self.run_dir / "bootstrap.json"
        self._operations = operations
        self._client = client
        self._command = command
        self._timeout = command_timeout
        self._intent = None
        try:
            self.run_dir.chmod(0o700)
            self.lock = RunLock(self.run_dir)
            if resume_dir is None:
                self._intent = {"format": _FORMAT, "stage": "intent", "run_id": self.run_id,
                    "workflow_version": workflow_version, "max_depth": max_depth,
                    "topology": json.loads(_json(topology)),
                    "gateway_id": "host-" + uuid.uuid4().hex[:27]}
                self._save_stage("intent")
            elif self._manifest.exists():
                self._intent = _read_json(self._manifest)
                if (set(self._intent) != {"format", "stage", "run_id", "workflow_version",
                                         "max_depth", "topology", "gateway_id"}
                        or self._intent["format"] != _FORMAT
                        or self._intent["stage"] not in _STAGES):
                    raise RunnerError("unsupported bootstrap format or stage")
                self.run_id = self._intent["run_id"]
                self._check_compatibility(self._intent, workflow_version, max_depth)
            if self._intent is None:
                # Legacy complete runs have no bootstrap manifest. Their original
                # journal is authoritative; missing legacy state cannot be invented.
                self._open_legacy(workflow_version, max_depth, actors)
            else:
                self._complete(actors)
        except BaseException:
            try:
                if resume_dir is None:
                    # Keep ownership while deciding and deleting, so a resumed
                    # owner cannot acquire this directory during cleanup.
                    self._remove_unadmitted()
            finally:
                self.close()
            raise

    @staticmethod
    def _check_compatibility(metadata: dict, workflow_version: str, max_depth: int):
        if (workflow_version == "unversioned" or metadata["workflow_version"] != workflow_version
                or metadata["max_depth"] != max_depth):
            raise RunnerError("resume requires the original explicit workflow version and max_depth")

    def _save_stage(self, stage: str) -> None:
        self._intent["stage"] = stage
        _write_json(self._manifest, self._intent)

    def _open_legacy(self, workflow_version, max_depth, actors):
        self.journal = Journal(self.run_dir)
        metadata = self.journal.metadata()
        self._check_compatibility(metadata, workflow_version, max_depth)
        self.run_id = metadata["run_id"]
        self._require_file(self.database)
        self._require_file(self.configuration)
        self._operations.admit_run(self.run_id, self.run_dir)
        self.snapshot = self._command([self._client.mas, "status", str(self.database), self.run_id])
        if self.snapshot["config"] != metadata["topology"]:
            raise RunnerError("frozen run configuration differs from recovery journal")
        self._check_actors(actors)
        self._load_credentials()
        self._client.validate_gateway(self.database, self.configuration, timeout=self._timeout)

    def _complete(self, actors):
        intent = self._intent
        stage = _STAGES.index(intent["stage"])
        if stage >= _STAGES.index("protocol_ready"):
            self._require_file(self.database)
        if stage >= _STAGES.index("journal_ready"):
            self._require_file(self.run_dir / "host.sqlite")
        if stage >= _STAGES.index("ready"):
            self._require_file(self.configuration)
        if stage < _STAGES.index("ready") and not self._operations.admission_enabled:
            raise RunnerError("restored deployment is audit-only; bootstrap cannot resume")
        self._operations.admit_run(self.run_id, self.run_dir)
        if stage < _STAGES.index("admitted"):
            self._save_stage("admitted")
        spec = self.run_dir / "topology.json"
        if stage < _STAGES.index("protocol_ready"):
            _write_json(spec, intent["topology"])
            # Rust init checks the frozen config and is idempotent. A committed
            # init with a lost reply retains its existing absolute deadline.
            self.snapshot = self._command([self._client.mas, "init", str(self.database), self.run_id, str(spec)])
            self._save_stage("protocol_ready")
        else:
            self.snapshot = self._command([self._client.mas, "status", str(self.database), self.run_id])
        self._check_actors(actors)
        metadata = {"run_id": self.run_id, "workflow_version": intent["workflow_version"],
                    "topology": self.snapshot["config"], "max_depth": intent["max_depth"]}
        if stage < _STAGES.index("journal_ready"):
            # Supplying the frozen metadata is idempotent even if creation
            # committed immediately before a crash. Foreign schemas reject it.
            self.journal = Journal(self.run_dir, metadata)
            self._save_stage("journal_ready")
        else:
            self.journal = Journal(self.run_dir)
        if self.journal.metadata() != metadata:
            raise RunnerError("frozen run configuration differs from recovery journal")
        if stage < _STAGES.index("ready"):
            self._provision()
        self._load_credentials()
        self._client.validate_gateway(self.database, self.configuration, timeout=self._timeout)
        if stage < _STAGES.index("ready"):
            self._save_stage("ready")

    def _check_actors(self, actors):
        if set(self.snapshot["config"]["agents"]) != actors:
            raise RunnerError("register exactly one adapter for every topology agent")

    @staticmethod
    def _require_file(path):
        if path.is_symlink() or not path.is_file():
            raise RunnerError("committed run state is missing or unavailable; inspect before recovery")

    def _policy(self):
        config = self.snapshot["config"]
        attempts = min(2**63 - 1, 100 + 6 * config["max_messages"] + 3 * config["max_calls"])
        return {"gateway_id": self._intent["gateway_id"], "max_attempts": attempts,
                "credentials": [{"credential_id": f"agent-{index}", "run_id": self.run_id,
                                 "actor": actor, "max_attempts": attempts}
                                for index, actor in enumerate(config["agents"])]}

    def _provision(self):
        if self.configuration.exists():
            return  # Published credentials are immutable, even before ready.
        policy = self.run_dir / "policy.json"
        _write_json(policy, self._policy())
        pending = self.run_dir / ".gateway.pending.json"
        if pending.exists():
            # Provisioning writes a new file. It may have been killed mid-write,
            # before publication or Rust policy registration. Only this private,
            # unpublished output may be recreated; no adapter has run yet.
            try:
                self._validate_credentials(_read_json(pending))
            except (ValueError, RunnerError, KeyError, TypeError):
                pending.unlink()
        if not pending.exists():
            self._command([self._client.gateway, "provision", str(policy), str(pending)])
        self._validate_credentials(_read_json(pending))
        os.replace(pending, self.configuration)
        _sync_directory(self.run_dir)

    def _validate_credentials(self, private):
        credentials = private.get("credentials")
        if type(credentials) is not list or any(type(item) is not dict for item in credentials):
            raise RunnerError("invalid private gateway configuration")
        if self._intent is not None:
            bare = {**private, "credentials": [{k: v for k, v in item.items() if k != "token"}
                                                for item in credentials]}
            expected = self._policy()
            bare["credentials"] = sorted(bare["credentials"], key=lambda item: item["credential_id"])
            expected["credentials"] = sorted(expected["credentials"], key=lambda item: item["credential_id"])
            if bare != expected:
                raise RunnerError("private gateway bindings differ from bootstrap intent")
        if any(type(item.get("token")) is not str or len(item["token"]) != 64
               or any(char not in "0123456789abcdef" for char in item["token"])
               for item in credentials):
            raise RunnerError("invalid private gateway configuration")

    def _load_credentials(self):
        private = _read_json(self.configuration)
        self._validate_credentials(private)
        self.tokens = {item["actor"]: item["token"] for item in private["credentials"]}

    def _remove_unadmitted(self):
        # An admission whose response was lost must be retained. Only known
        # unadmitted intent files are disposable; never delete ambiguous state.
        try:
            allowed = {".host.lock", "bootstrap.json", ".bootstrap.json.pending"}
            if (self.lock is not None and not self._operations.has_run(self.run_id)
                    and all(path.name in allowed for path in self.run_dir.iterdir())):
                discarded = self.run_dir.with_name(".unadmitted-" + uuid.uuid4().hex)
                # Atomic removal of the public path avoids a lock-file unlink
                # race with another process attempting to resume this intent.
                os.replace(self.run_dir, discarded)
                _sync_directory(self.run_dir.parent)
                shutil.rmtree(discarded)
        except Exception:
            pass

    def close(self):
        try:
            if self.journal is not None:
                self.journal.close()
                self.journal = None
        finally:
            if self.lock is not None:
                self.lock.close()
                self.lock = None
