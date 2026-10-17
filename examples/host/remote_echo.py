"""Operator-owned factory for the registered remote leaf example.

Run with PYTHONPATH=host:examples/host. Task requests cannot select this module.
"""
import hashlib
import json
import os
from pathlib import Path
import sys

from mas_host.distributed_worker import WorkerOperation
from mas_host.executable_worker import WorkerSpec
import mas_host.worker_entry


def operations():
    root = Path(__file__).resolve().parents[2]
    files = [Path(__file__).resolve(), Path(mas_host.worker_entry.__file__).resolve()]
    spec = WorkerSpec("echo", "1", (sys.executable, "-m", "mas_host.worker_entry"),
        factory="echo", configuration={}, cwd=root,
        environment={"PYTHONPATH": str(root / "host")},
        pinned_files={str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files})
    return [WorkerOperation.registered("echo", spec,
        gateway_bin=os.environ.get("MAS_GATEWAY_BIN", "mas-gateway"), timeout_seconds=5)]


if __name__ == "__main__":
    print(json.dumps({operation.name: operation.implementation for operation in operations()}, sort_keys=True))
