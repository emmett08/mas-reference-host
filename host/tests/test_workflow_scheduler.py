"""Actual process ownership tests, independent of workflow-domain semantics."""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest

from mas_host.service_contracts import ServiceError
from mas_host.task_service import _OwnedContext
from mas_host.workflow_scheduler import SchedulerOwnership


INSTANCE = "flow-" + "a" * 32


def eventually(predicate, seconds=3):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("bounded process condition was not reached")


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux process ownership")
class SchedulerOwnershipTests(unittest.TestCase):
    def test_exclusive_workflow_and_shared_deployment_locks_release_on_close(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with SchedulerOwnership(path, INSTANCE) as owner:
                self.assertEqual(owner.ownership_descriptors,
                                 (owner.workflow_fd, owner.deployment_fd))
                with self.assertRaisesRegex(ServiceError, "workflow busy"):
                    with SchedulerOwnership(path, INSTANCE):
                        self.fail("same workflow acquired twice")
                descriptor = os.open(path / ".deployment.lock", os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(descriptor)
            with SchedulerOwnership(path, INSTANCE):
                pass
            descriptor = os.open(path / ".deployment.lock", os.O_RDWR)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)

    def test_two_workers_have_separate_exit_status_and_close_observes_lifetime(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            def completed(lifetime, descriptors):
                self.assertEqual(len(descriptors), 2)
                (path / "completed").write_text("done")
            def waiting(lifetime, descriptors):
                (path / "waiting").write_text("ready")
                if not select.select([lifetime], [], [], 3)[0]:
                    raise RuntimeError("scheduler lifetime did not close")
                (path / "stopped").write_text("done")
            with SchedulerOwnership(path, INSTANCE) as owner:
                owner.spawn("completed", completed)
                owner.spawn("waiting", waiting)
                self.assertEqual(owner.active, frozenset({"completed", "waiting"}))
                observed = {}
                def ended():
                    observed.update(owner.poll())
                    return "completed" in observed and (path / "waiting").exists()
                eventually(ended)
                self.assertTrue(os.WIFEXITED(observed["completed"]))
                self.assertEqual(os.WEXITSTATUS(observed["completed"]), 0)
                self.assertEqual(owner.active, frozenset({"waiting"}))
            self.assertTrue((path / "stopped").exists())
            self.assertEqual(owner.active, frozenset())

    @unittest.skipUnless(hasattr(os, 'pidfd_open'), 'Linux pidfd event waits')
    def test_event_wait_wakes_for_worker_exit_and_closes_pidfd(self):
        with tempfile.TemporaryDirectory() as directory:
            def immediate(_lifetime, _descriptors):
                return
            with SchedulerOwnership(Path(directory), INSTANCE) as owner:
                owner.spawn('worker', immediate)
                started = time.monotonic()
                owner.wait(1)
                self.assertLess(time.monotonic() - started, 0.5)
                eventually(lambda: bool(owner.poll()))
                self.assertEqual(owner._pidfds, {})

    def test_failed_worker_and_duplicate_live_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            def failed(_lifetime, _descriptors):
                raise RuntimeError("worker failed")
            with SchedulerOwnership(Path(directory), INSTANCE) as owner:
                owner.spawn("worker", failed)
                with self.assertRaisesRegex(ServiceError, "duplicate scheduler worker"):
                    owner.spawn("worker", failed)
                observed = {}
                eventually(lambda: bool(observed.update(owner.poll()) or observed))
                self.assertNotEqual(os.WEXITSTATUS(observed["worker"]), 0)

    def test_sigkill_owner_preserves_both_locks_until_worker_releases_them(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            script = r'''
import os, select, time
from pathlib import Path
from mas_host.workflow_scheduler import SchedulerOwnership
path = Path(os.environ['MAS_TEST_OWNER_DIRECTORY'])
def worker(lifetime, ownership):
    (path / 'worker-ready').write_text('ready')
    select.select([lifetime], [], [], 5)
    (path / 'owner-lost').write_text('lost')
    while not (path / 'release').exists():
        time.sleep(0.01)
with SchedulerOwnership(path, 'flow-' + 'a' * 32) as owner:
    owner.spawn('worker', worker)
    while True:
        time.sleep(0.1)
'''
            process = subprocess.Popen([sys.executable, "-c", script], env={
                **os.environ, "MAS_TEST_OWNER_DIRECTORY": directory},
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                eventually(lambda: (path / "worker-ready").exists())
                process.kill()
                process.wait(timeout=3)
                eventually(lambda: (path / "owner-lost").exists())
                for name in ("." + INSTANCE + ".lock", ".deployment.lock"):
                    descriptor = os.open(path / name, os.O_RDWR)
                    try:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    finally:
                        os.close(descriptor)
            finally:
                (path / "release").write_text("release")
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=3)
            def released():
                descriptor = os.open(path / ".deployment.lock", os.O_RDWR)
                try:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        return True
                    except BlockingIOError:
                        return False
                finally:
                    os.close(descriptor)
            eventually(released)

    def test_context_preserves_existing_and_multiple_owner_descriptors(self):
        context = SimpleNamespace(ownership_descriptors=(3, 4), actor="worker")
        self.assertEqual(_OwnedContext(context, 5).ownership_descriptors, (3, 4, 5))
        self.assertEqual(_OwnedContext(context, (4, 5, 6)).ownership_descriptors, (3, 4, 5, 6))
        for malformed in ((), (True,), (-1,), (5, 5), [5]):
            with self.subTest(malformed=malformed), self.assertRaises(ServiceError):
                _OwnedContext(context, malformed)


if __name__ == "__main__":
    unittest.main()
