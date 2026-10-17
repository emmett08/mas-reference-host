"""Linux process ownership for bounded workflow execution.

The scheduler owns both deployment and workflow locks. Workers and adapter
guardians retain copies of the same open file descriptions until local work
stops. Releasing an owner closes its descriptors; it never explicitly unlocks
another surviving owner's copy. This is process lifetime control, not a sandbox.
"""
from __future__ import annotations

import fcntl
import gc
import os
from pathlib import Path
import re
import select
import signal
import sys
import threading
import time
from typing import Callable, Hashable

from .service_contracts import ServiceError


class SchedulerOwnership:
    """Single-threaded owner of leaf workers and inherited execution locks.

    ``spawn`` passes the scheduler lifetime reader and both ownership
    descriptors to its child callback. A readable lifetime pipe means that the
    scheduler has closed or died. Callback code must observe it at admission
    and supervision boundaries, and pass the ownership descriptors to the
    existing adapter guardian. Ordinary adapters must never receive them.
    """

    def __init__(self, state_dir: Path, instance: str):
        if (type(instance) is not str
                or re.fullmatch(r"flow-[0-9a-f]{32}", instance) is None):
            raise ServiceError("invalid_workflow_identity")
        self.state_dir = Path(state_dir)
        self.instance = instance
        self.workflow_fd: int | None = None
        self.deployment_fd: int | None = None
        self.lifetime_read: int | None = None
        self._lifetime_write: int | None = None
        self._children: dict[Hashable, int] = {}
        self._pidfds: dict[Hashable, int] = {}
        self._owner_pid = os.getpid()
        self._entered = False

    @property
    def ownership_descriptors(self) -> tuple[int, int]:
        if self.workflow_fd is None or self.deployment_fd is None:
            raise ServiceError("scheduler_not_open")
        return self.workflow_fd, self.deployment_fd

    @property
    def active(self) -> frozenset[Hashable]:
        return frozenset(self._children)

    def _require_owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise ServiceError("scheduler_owner_mismatch")

    def __enter__(self):
        self._require_owner()
        if (not sys.platform.startswith("linux")
                or threading.current_thread() is not threading.main_thread()
                or threading.active_count() != 1):
            raise ServiceError("scheduler_requires_single_threaded_linux")
        if self._entered:
            raise ServiceError("scheduler_already_open")
        self._entered = True
        try:
            self.workflow_fd = os.open(
                self.state_dir / ("." + self.instance + ".lock"),
                os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                fcntl.flock(self.workflow_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError("workflow_busy") from None
            self.deployment_fd = os.open(self.state_dir / ".deployment.lock",
                                         os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            fcntl.flock(self.deployment_fd, fcntl.LOCK_SH)
            self.lifetime_read, self._lifetime_write = os.pipe()
            return self
        except BaseException:
            self.close()
            raise

    def spawn(self, key: Hashable,
              target: Callable[[int, tuple[int, int]], None]) -> None:
        self._require_owner()
        if self._lifetime_write is None or self.lifetime_read is None:
            raise ServiceError("scheduler_not_open")
        if key in self._children:
            raise ServiceError("duplicate_scheduler_worker")
        if not callable(target):
            raise ServiceError("invalid_scheduler_worker")
        if (threading.current_thread() is not threading.main_thread()
                or threading.active_count() != 1):
            raise ServiceError("scheduler_requires_single_threaded_linux")
        # Shutdown must not land after fork but before the parent records the
        # child. The owner's finally block then always knows whom to drain.
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
        try:
            pid = os.fork()
            if pid == 0:
                gc.freeze()
                code = 0
                try:
                    os.close(self._lifetime_write)
                    for descriptor in self._pidfds.values():
                        os.close(descriptor)
                    signal.signal(signal.SIGINT, signal.SIG_DFL)
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous)
                    target(self.lifetime_read, self.ownership_descriptors)
                except BaseException:
                    code = 1
                finally:
                    # Copied Python owners must not execute finalisers that
                    # unlock shared descriptions or affect sibling resources.
                    os._exit(code)
            self._children[key] = pid
            if hasattr(os, 'pidfd_open'):
                try:
                    self._pidfds[key] = os.pidfd_open(pid)
                except OSError:
                    # Older kernels or an already-exited child retain the
                    # existing bounded timer/reap path.
                    pass
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    def poll(self) -> dict[Hashable, int]:
        self._require_owner()
        exited = {}
        for key, pid in tuple(self._children.items()):
            try:
                done, status = os.waitpid(pid, os.WNOHANG)
            except InterruptedError:
                continue
            if done:
                del self._children[key]
                descriptor = self._pidfds.pop(key, None)
                if descriptor is not None:
                    os.close(descriptor)
                exited[key] = status
        return exited

    def wait(self, timeout: float) -> None:
        """Wake on a local worker exit or the bounded cancellation/expiry timer."""
        self._require_owner()
        if not 0 <= timeout <= 1:
            raise ServiceError('invalid_scheduler_wait')
        if not self._pidfds:
            time.sleep(timeout)
            return
        poller = select.poll()
        for descriptor in self._pidfds.values():
            poller.register(descriptor, select.POLLIN | select.POLLHUP | select.POLLERR)
        poller.poll(int(timeout * 1000))

    def close(self) -> None:
        self._require_owner()
        # Repeated shutdown signals must not interrupt ownership release while
        # an adapter guardian is stopping an admitted external invocation.
        blocked = (threading.current_thread() is threading.main_thread())
        previous = (signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
                    if blocked else None)
        try:
            if self._lifetime_write is not None:
                os.close(self._lifetime_write)
                self._lifetime_write = None
            stop = time.monotonic() + 5
            while self._children and time.monotonic() < stop:
                self.poll()
                if self._children:
                    time.sleep(0.02)
            for pid in self._children.values():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            for pid in tuple(self._children.values()):
                while True:
                    try:
                        os.waitpid(pid, 0)
                        break
                    except InterruptedError:
                        continue
                    except ChildProcessError:
                        break
            self._children.clear()
            for descriptor in self._pidfds.values():
                os.close(descriptor)
            self._pidfds.clear()
        finally:
            # Guardians retain the same locks until their own cleanup ends.
            for name in ("lifetime_read", "workflow_fd", "deployment_fd"):
                descriptor = getattr(self, name)
                if descriptor is not None:
                    os.close(descriptor)
                    setattr(self, name, None)
            if previous is not None:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    def __exit__(self, *_exception):
        self.close()
