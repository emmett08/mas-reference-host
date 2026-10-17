"""Readiness of one independent reconciler, separate from admission quotas."""
from __future__ import annotations

from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import tempfile
import time

from .operations import DeploymentLedger, OperationsError, _file_lock, _private_dir


STATUS_FILE = ".reconciler-health.json"


def _process_identity(pid: int | None = None) -> dict:
    # PID alone can be reused. Linux boot identity and process start ticks make
    # a stale heartbeat incapable of identifying a later process as its writer.
    if pid is not None and (type(pid) is not int or pid <= 0):
        raise OperationsError("invalid reconciler process identity")
    source = Path("/proc/self/stat" if pid is None else f"/proc/{pid}/stat").read_text(encoding="ascii")
    visible_pid = int(source.split(" ", 1)[0])
    fields = source[source.rfind(")") + 2:].split()
    if fields[0] in ("Z", "X"):
        raise OperationsError("reconciler process is not running")
    return {"pid": visible_pid, "start_ticks": int(fields[19]),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()}


class ReconcilerMonitor:
    """Atomic filesystem status adapter; one writer owns the watcher lock."""
    def __init__(self, state_dir: Path):
        self.state_dir = _private_dir(Path(state_dir))
        self.identity = _process_identity()

    def write(self, status: str, *, scans: int, failures: int):
        details = self.state_dir.stat()
        value = {"format": "mas-reconciler-health/1", "status": status,
                 "observed_monotonic": time.monotonic(), "observed_at": time.time(),
                 "state_device": details.st_dev, "state_inode": details.st_ino,
                 "process": self.identity, "scans": scans, "failed_scans": failures}
        descriptor, temporary = tempfile.mkstemp(prefix=".reconciler-health-", dir=self.state_dir)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(value, stream, sort_keys=True)
            os.replace(temporary, self.state_dir / STATUS_FILE)
        finally:
            Path(temporary).unlink(missing_ok=True)

    @contextmanager
    def ownership(self):
        with _file_lock(self.state_dir / ".reconciler.lock", True, nonblocking=True):
            yield self


def reconciler_health(state_dir: Path, max_age_seconds: float = 30) -> dict:
    """Fail closed for failed, stale, copied or no-longer-running watchers."""
    if not math.isfinite(max_age_seconds) or not 0.1 <= max_age_seconds <= 3600:
        raise OperationsError("reconciler maximum age must be 0.1 to 3600 seconds")
    state_dir = _private_dir(Path(state_dir))
    # This does not initialise or migrate a ledger when used by Docker health.
    DeploymentLedger(state_dir, readonly=True)
    try:
        path = state_dir / STATUS_FILE
        if path.is_symlink() or path.stat().st_size > 8192:
            raise OperationsError("invalid reconciler status file")
        value = json.loads(path.read_text(encoding="utf-8"))
        details = state_dir.stat()
        age = time.monotonic() - value["observed_monotonic"]
        if (value["format"] != "mas-reconciler-health/1" or value["status"] != "healthy"
                or not math.isfinite(age) or not 0 <= age <= max_age_seconds
                or (value["state_device"], value["state_inode"]) != (details.st_dev, details.st_ino)
                or value["process"] != _process_identity(value["process"]["pid"])):
            raise OperationsError("reconciler is failed, stale or stopped")
        return {"ok": True, "reconciler_ready": True, "age_seconds": round(age, 3),
                "scans": value["scans"], "failed_scans": value["failed_scans"]}
    except (OSError, KeyError, ValueError, TypeError, IndexError):
        raise OperationsError("reconciler status is unavailable or invalid") from None
