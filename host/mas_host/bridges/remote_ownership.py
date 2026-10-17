"""Exclusive local ownership of one namespaced remote task during an effect.

The deployment's shared lock prevents maintenance from copying an in-flight
operation. The task lock serialises that task's lifecycle without excluding
unrelated remote work or read-only receipt inspection. Locks are ephemeral;
durable unknown intent, never a missing lock, determines replay eligibility.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import re

from ..operations import _file_lock, _private_dir
from ..service_contracts import ServiceError


@contextmanager
def own_remote_task(state_dir: Path, key: str):
    if type(key) is not str or re.fullmatch(r"[0-9a-f]{64}", key) is None:
        raise ServiceError("invalid_identifier")
    directory = _private_dir(Path(state_dir))
    with _file_lock(directory / ".deployment.lock", False, nonblocking=True):
        descriptor = os.open(directory / (".remote-task-" + key + ".lock"),
                             os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError("remote_task_busy") from None
            yield
        finally:
            # Do not explicitly unlock another inherited owner's descriptor.
            os.close(descriptor)
