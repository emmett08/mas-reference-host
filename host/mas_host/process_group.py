"""Linux process-group ownership independent of the host's continued execution.

The guardian owns the adapter process and kills its whole group when the host
closes its lifetime pipe, dies, or reaches the absolute deadline. Trusted
adapters must stay in that group. This is lifetime management, not a sandbox.
"""

from __future__ import annotations

import ctypes
import errno
import gc
import os
import select
import signal
import time
from typing import Callable


def _close_inherited(keep: set[int]) -> None:
    """Drop copied host descriptors without running host cleanup/unlock code."""
    # Fork copied Python owners as well as their descriptor numbers. After a
    # raw close, collecting an inherited file/SQLite cycle could close a number
    # already reused for an adapter lock or socket. Keep inherited tracked
    # objects out of child collections; new adapter objects remain collectable.
    # Both children exit with os._exit, so inherited finalisers never run there.
    # Freezing is child-local and leaves the host's GC policy unchanged.
    for name in os.listdir("/proc/self/fd"):
        descriptor = int(name)
        if descriptor > 2 and descriptor not in keep:
            try:
                os.close(descriptor)
            except OSError as error:
                if error.errno != errno.EBADF:
                    raise


def _kill_and_reap(worker: int) -> None:
    try:
        os.killpg(worker, signal.SIGKILL)
    except ProcessLookupError:
        # The worker may not yet have created its process group.
        try:
            os.kill(worker, signal.SIGKILL)
        except ProcessLookupError:
            pass
    while True:
        try:
            # As a subreaper the guardian also adopts and reaps ordinary
            # descendants when their adapter parent exits.
            os.waitpid(-1, 0)
        except InterruptedError:
            continue
        except ChildProcessError:
            return


def _guardian(lifetime: int, channel: int, target: Callable[[], None], deadline: float,
              ownership: tuple[int, ...]) -> None:
    worker = None

    def interrupted(_number, _frame):
        raise SystemExit(0)

    try:
        # Preserve the same open-file-description locks until cleanup is done.
        # A replacement owner must not acquire the run during group termination.
        _close_inherited({lifetime, channel, *ownership})
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            os._exit(1)
        for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(number, interrupted)
        signal.pthread_sigmask(signal.SIG_UNBLOCK,
                               {signal.SIGTERM, signal.SIGINT, signal.SIGHUP})
        guardian = os.getpid()
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK,
                                               {signal.SIGTERM, signal.SIGINT, signal.SIGHUP})
        worker = os.fork()
        if worker == 0:
            gc.freeze()
            try:
                _close_inherited({channel})
                os.setsid()
                for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                    signal.signal(number, signal.SIG_DFL)
                if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
                    os._exit(1)
                if os.getppid() != guardian:
                    os._exit(1)
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                target()
            finally:
                os._exit(0)
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        os.close(channel)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            readable, _, _ = select.select([lifetime], [], [], remaining)
            if readable:
                # EOF means the host died/closed. Any byte is an explicit stop.
                os.read(lifetime, 1)
                break
    except BaseException:
        pass
    finally:
        for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(number, signal.SIG_IGN)
        if worker is not None and worker > 0:
            _kill_and_reap(worker)
        os._exit(0)


class ProcessGroup:
    """The host owns one pipe endpoint; the guardian owns process cleanup."""

    def __init__(self, channel: int, target: Callable[[], None], deadline: float,
                 ownership: tuple[int, ...] = ()):
        reader, writer = os.pipe()
        try:
            guardian = os.fork()
        except BaseException:
            os.close(reader)
            os.close(writer)
            raise
        if guardian == 0:
            gc.freeze()
            _guardian(reader, channel, target, deadline, ownership)
        os.close(reader)
        self._guardian = guardian
        self._lifetime = writer

    def close(self) -> None:
        if self._lifetime is not None:
            os.close(self._lifetime)
            self._lifetime = None
        while self._guardian is not None:
            try:
                os.waitpid(self._guardian, 0)
                self._guardian = None
            except InterruptedError:
                continue
            except ChildProcessError:
                self._guardian = None
