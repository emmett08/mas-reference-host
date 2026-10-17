"""Trusted local bridge readiness; never a public health or scheduling API."""
from __future__ import annotations

from contextlib import closing, contextmanager
import http.client
import json
import math
import os
from pathlib import Path
import tempfile
import time
from urllib.parse import urlsplit

from .bridges.authentication import read_private_file
from .monitor import _process_identity
from .operations import DeploymentLedger, OperationsError, _file_lock, _private_dir
from .schema import connect
from .task_store import SERVICE


STATUS_FILE = ".bridge-worker-health.json"
# Bootstrap, SQLite contention and supervised cleanup have separate command
# bounds. This is an operational allowance, not an extension of a run deadline.
EXECUTION_OVERHEAD_SECONDS = 90


def _state_health(state_dir: Path, config=None):
    if config is not None and 'shared' in config:
        from .bridge_config import build_shared_authority
        authority = build_shared_authority(config)
        try:
            health = authority.query('operator.storage', {})
            status = authority.query('authority.status', {})
            # The database validates the configured controller/epoch. A probe
            # never renews leadership or creates controller-local authority.
            live = status['lease_until_ms'] > status['database_time_ms']
            health['admission_open'] = health['admission_open'] and not status['draining'] and live
            if not live:
                health['admission_state'] = 'lease_expired'
            health['task_service'] = {'backend': 'postgresql'}
            return health
        finally:
            authority.close()
    ledger = DeploymentLedger(state_dir, readonly=True)
    with closing(connect(Path(state_dir) / "task-service.sqlite", SERVICE, readonly=True)) as db:
        if dict(db.execute("SELECT key,value FROM metadata")) != {"deployment_id": ledger.deployment_id}:
            raise OperationsError("bridge deployment identity differs")
    return ledger.health()


def ingress_health(state_dir: Path, config: dict, port: int = 8765) -> dict:
    """Probe this container's HTTP accept loop and read its local durable stores.

    The public Agent Card contains no task data. Reading the private state here
    avoids placing an extra bearer secret in the ingress container. This check
    does not establish OAuth availability, provider progress or external TLS.
    """
    if type(port) is not int or not 1 <= port <= 65535:
        raise OperationsError("invalid readiness port")
    health = _state_health(state_dir, config)
    public_url = config.get("public_url", "http://127.0.0.1:8765")
    from .bridges.a2a import A2ABridge
    from .task_service import DurableTaskService
    # All CLI task-service implementations expose the same public discovery
    # capabilities. Inspect the class surface without constructing a service,
    # querying private registrations, or provisioning state during a probe.
    expected = A2ABridge(DurableTaskService, endpoint=public_url + "/a2a",
                         allow_http=public_url.startswith("http:")).agent_card()
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        connection.request("GET", "/.well-known/agent-card.json",
                           headers={"Host": urlsplit(public_url).netloc, "Connection": "close"})
        response = connection.getresponse()
        body = response.read(16385)
        if response.status != 200 or len(body) > 16384 or json.loads(body) != expected:
            raise OperationsError("bridge HTTP readiness failed")
    finally:
        connection.close()
    return {"ok": True, "bridge_ready": True, "admission_open": health["admission_open"],
            "admission_state": health["admission_state"], "task_service": health["task_service"]}


class WorkerMonitor:
    """Single-threaded progress journal; its writer exclusively owns this loop."""
    def __init__(self, state_dir: Path):
        self.state_dir = _private_dir(Path(state_dir))
        self.identity = _process_identity()
        self.polls = 0

    @contextmanager
    def ownership(self):
        with _file_lock(self.state_dir / ".bridge-worker-monitor.lock", True, nonblocking=True):
            yield self

    def write(self, phase: str, *, allowance: float = 30):
        details = self.state_dir.stat()
        observed = time.monotonic()
        value = {"format": "mas-bridge-worker-health/1", "phase": phase,
                 "observed_monotonic": observed, "deadline_monotonic": observed + allowance,
                 "state_device": details.st_dev, "state_inode": details.st_ino,
                 "process": self.identity, "completed_polls": self.polls}
        descriptor, temporary = tempfile.mkstemp(prefix=".bridge-worker-health-", dir=self.state_dir)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(value, stream, sort_keys=True)
            os.replace(temporary, self.state_dir / STATUS_FILE)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def observe(self, executor):
        return _ObservedExecutor(executor, self)


class _ObservedExecutor:
    """Decorator preserves execution and cancellation while bounding busy health."""
    def __init__(self, executor, monitor):
        self.executor, self.monitor = executor, monitor

    @property
    def validate_input(self):
        # Preserve the optional admission capability, including its absence.
        # The service obtains it with getattr(..., None) before any mutation.
        return self.executor.validate_input

    def execute(self, workflow, execution, *, cancelled, ownership_descriptor):
        # Match the Rust configuration default and accepted positive i64 bound.
        wall = workflow.topology.get("wall_seconds", 3600)
        if type(wall) is not int or not 1 <= wall <= 2**63 - 1:
            raise OperationsError("worker health requires a bounded workflow deadline")
        self.monitor.write("executing", allowance=wall + EXECUTION_OVERHEAD_SECONDS)
        result = self.executor.execute(workflow, execution, cancelled=cancelled,
                                       ownership_descriptor=ownership_descriptor)
        self.monitor.write("polling")
        return result


def worker_health(state_dir: Path, max_age_seconds: float = 30, *, config=None) -> dict:
    """Require ownership, the same live process and a bounded current phase.

    An executing phase is healthy until its frozen workflow allowance expires;
    it does not claim that an external provider is making progress.
    """
    if not math.isfinite(max_age_seconds) or not 0.1 <= max_age_seconds <= 3600:
        raise OperationsError("worker maximum age must be 0.1 to 3600 seconds")
    state_dir = _private_dir(Path(state_dir))
    health = _state_health(state_dir, config)
    try:
        value = json.loads(read_private_file(state_dir / STATUS_FILE, max_bytes=8192))
        details = state_dir.stat()
        current = time.monotonic()
        age = current - value["observed_monotonic"]
        phase = value["phase"]
        if (value["format"] != "mas-bridge-worker-health/1"
                or phase not in ("idle", "polling", "executing", "draining")
                or type(value["completed_polls"]) is not int or value["completed_polls"] < 0
                or (phase != "executing" and value["completed_polls"] == 0)
                or not math.isfinite(age) or age < 0
                or not math.isfinite(value["deadline_monotonic"])
                or current > value["deadline_monotonic"]
                or (phase != "executing" and age > max_age_seconds)
                or (value["state_device"], value["state_inode"]) != (details.st_dev, details.st_ino)
                or value["process"] != _process_identity(value["process"]["pid"])):
            raise OperationsError("bridge worker is failed, stale or stopped")
        # A recent record from a live but no-longer-owning process is not ready.
        try:
            with _file_lock(state_dir / ".bridge-worker-monitor.lock", True, nonblocking=True):
                owned = False
        except OperationsError as error:
            if "busy" not in str(error):
                raise
            owned = True
        if not owned:
            raise OperationsError("bridge worker has no scheduling loop owner")
        return {"ok": True, "worker_ready": True, "phase": phase,
                "age_seconds": round(age, 3), "completed_polls": value["completed_polls"],
                "admission_open": health["admission_open"] and phase != "draining"}
    except (OSError, KeyError, ValueError, TypeError, IndexError):
        raise OperationsError("bridge worker status is unavailable or invalid") from None
