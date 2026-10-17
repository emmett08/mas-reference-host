"""Deployment faults exercised with real processes, files and Rust binaries.

Run via unittest discovery after building release binaries. No API calls occur.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from mas_host import Invocation, Outcome, Runner, RunnerError


SPEC = {"agents": ["a", "b"], "edges": [["a", "b"], ["b", "a"]],
        "application": "agent-task/1", "mailbox_capacity": 8,
        "max_messages": 20, "max_bytes": 200_000, "max_calls": 4, "wall_seconds": 120}
WORKFLOW = "deployment-faults/1"


class Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


class Effect:
    def __init__(self, root: Path, pause: str = ""):
        self.root, self.pause = root, pause

    def handle(self, context, _request):
        def provider():
            with (self.root / "effects.txt").open("a", encoding="utf-8") as stream:
                stream.write("effect\n")
                stream.flush()
                os.fsync(stream.fileno())
            (self.root / "provider-pid").write_text(str(os.getpid()), encoding="utf-8")
            if self.pause == "unknown":
                (self.root / "ready").touch()
                while True:
                    time.sleep(0.05)
            return Invocation({"answer": 42}, {"provider": "acceptance-local", "response_id": "effect-1"})
        output = context.invoke(provider)
        if self.pause == "completed":
            (self.root / "ready").touch()
            while True:
                time.sleep(0.05)
        return Outcome(output)


def crash_worker(root: str, pause: str, mas: str, gateway: str) -> None:
    directory = Path(root)
    if pause.endswith("-cleanup"):
        # Test-only gate at the exact boundary the old fixture overlooked:
        # adapter processes are reaped, but the guardian still owns the lock.
        from mas_host import process_group
        original_cleanup = process_group._kill_and_reap

        def gated_cleanup(pid):
            original_cleanup(pid)
            (directory / "guardian-reaped").touch()
            deadline = time.monotonic() + 10
            while not (directory / "release-guardian").exists():
                if time.monotonic() >= deadline:
                    # The adapter group is already fully reaped. Bound a failed
                    # fixture's guardian lifetime without re-entering host code.
                    os._exit(3)
                time.sleep(0.01)

        process_group._kill_and_reap = gated_cleanup
        pause = pause.removesuffix("-cleanup")
    runner = Runner(SPEC, {"a": Echo(), "b": Effect(directory, pause)}, directory / "state",
                    mas_bin=mas, gateway_bin=gateway, workflow_version=WORKFLOW)
    (directory / "run-dir").write_text(str(runner.run_dir), encoding="utf-8")
    runner.request("a", "b", "effect", {})


class DeploymentAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        release = Path(__file__).resolve().parents[2] / "rust-protocol" / "target" / "release"
        cls.mas = os.environ.get("MAS_BIN") or shutil.which("mas") or str(release / "mas")
        cls.gateway = os.environ.get("MAS_GATEWAY_BIN") or shutil.which("mas-gateway") or str(release / "mas-gateway")
        if not all(Path(binary).is_file() for binary in (cls.mas, cls.gateway)):
            raise RuntimeError("Build the Rust release binaries before deployment acceptance")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def wait_until(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.025)
        self.fail("deployment fault synchronisation timed out")

    def ownership_available(self, run_dir):
        """Observe the actual cleanup boundary, without retrying Runner.resume."""
        descriptor = os.open(run_dir / ".host.lock", os.O_RDWR | os.O_NOFOLLOW)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                if error.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise
                return False
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return True
        finally:
            os.close(descriptor)

    def kill_worker(self, pause, *, during_cleanup=None):
        worker_mode = pause + "-cleanup" if during_cleanup is not None else pause
        worker = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "crash-worker",
                                   str(self.root), worker_mode, self.mas, self.gateway],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        def cleanup():
            (self.root / "release-guardian").touch()
            if worker.poll() is None:
                os.killpg(worker.pid, signal.SIGKILL)
            worker.communicate(timeout=5)
        self.addCleanup(cleanup)
        self.wait_until(lambda: (self.root / "ready").exists() or worker.poll() is not None)
        if worker.poll() is not None:
            _out, error = worker.communicate()
            self.fail("fault worker exited before injection: " + error.decode(errors="replace")[-2000:])
        run_dir = Path((self.root / "run-dir").read_text())
        child = int((self.root / "provider-pid").read_text())
        os.kill(worker.pid, signal.SIGKILL)
        worker.wait(timeout=5)
        # The independent guardian must terminate the supervised adapter.
        def terminated():
            try:
                os.kill(child, 0)
            except ProcessLookupError:
                return True
            proc = Path(f"/proc/{child}/stat")
            try:
                return proc.exists() and proc.read_text().split()[2] == "Z"
            except (FileNotFoundError, ProcessLookupError):
                # The child can be reaped after the signal probe or exists()
                # succeeds, but before procfs opens/reads its status file.
                return True
        self.wait_until(terminated)
        if during_cleanup is not None:
            try:
                self.wait_until(lambda: (self.root / "guardian-reaped").exists())
                during_cleanup(run_dir)
            finally:
                (self.root / "release-guardian").touch()
        # Child termination alone is insufficient: the guardian releases run
        # ownership only after group cleanup and its own process exit. Poll
        # that exact state; unexpected lock I/O errors remain test failures.
        self.wait_until(lambda: self.ownership_available(run_dir))
        return run_dir

    def resume(self, run_dir):
        return Runner.resume(run_dir, {"a": Echo(), "b": Effect(self.root)},
                             workflow_version=WORKFLOW, mas_bin=self.mas, gateway_bin=self.gateway)

    @unittest.skipUnless(sys.platform == "linux", "Linux parent-death supervision acceptance")
    def test_sigkill_after_recorded_output_resumes_without_duplicate_effect(self):
        run_dir = self.kill_worker("completed")
        with self.resume(run_dir) as runner:
            self.assertEqual(runner.request("a", "b", "effect", {}).output, {"answer": 42})
            self.assertEqual(len(runner.status()["calls"]), 1)
        self.assertEqual((self.root / "effects.txt").read_text().splitlines(), ["effect"])
        with sqlite3.connect(run_dir / "state.sqlite") as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    @unittest.skipUnless(sys.platform == "linux", "Linux parent-death supervision acceptance")
    def test_sigkill_during_effect_blocks_unknown_invocation_on_resume(self):
        run_dir = self.kill_worker("unknown")
        with self.resume(run_dir) as runner:
            with self.assertRaises(RunnerError):
                runner.request("a", "b", "effect", {})
            self.assertEqual(len(runner.status()["calls"]), 1)
        self.assertEqual((self.root / "effects.txt").read_text().splitlines(), ["effect"])

    @unittest.skipUnless(sys.platform == "linux", "Linux guardian ownership acceptance")
    def test_adapter_reaping_does_not_release_ownership_before_guardian_exit(self):
        def verify_cleanup_boundary(run_dir):
            self.assertFalse(self.ownership_available(run_dir))
            # A single premature resume must fail closed, even though the
            # adapter has already been reaped. This is expected runtime policy.
            with self.assertRaises(RunnerError):
                self.resume(run_dir)

        run_dir = self.kill_worker("unknown", during_cleanup=verify_cleanup_boundary)
        with self.resume(run_dir) as runner:
            with self.assertRaises(RunnerError):
                runner.request("a", "b", "effect", {})
            self.assertEqual(len(runner.status()["calls"]), 1)
        self.assertEqual((self.root / "effects.txt").read_text().splitlines(), ["effect"])

    def test_real_storage_write_failure_rolls_back_message_and_recovers(self):
        # Restrict only the Rust child, never the filesystem or host process.
        import resource
        with Runner(SPEC, {"a": Echo(), "b": Echo()}, self.root / "state",
                    mas_bin=self.mas, gateway_bin=self.gateway, workflow_version=WORKFLOW) as runner:
            envelope = {"message_id": "storage-fault", "run_id": runner.run_id,
                        "sender": "a", "recipient": "b", "version": "1", "kind": "Request",
                        "payload": {"task_id": "storage-task", "operation": "echo",
                                    "input": {"text": "x" * 50_000}}}
            wire = runner._command([self.mas, "encode", "-"], json.dumps(envelope))["text"]
            source = self.root / "large-message.txt"
            source.write_text(wire, encoding="utf-8")
            database = runner.run_dir / "state.sqlite"
            limit = database.stat().st_size
            def restrict_file_growth():
                resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
                signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            failed = subprocess.run([self.mas, "send", str(database), "a", str(source)],
                                    capture_output=True, text=True, timeout=10,
                                    preexec_fn=restrict_file_growth)
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(runner.status()["messages"], [])
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(runner.request("a", "b", "echo", {"value": 7}).output, {"value": 7})

    def test_sqlite_full_checkpoint_prevents_callback_admission(self):
        with Runner(SPEC, {"a": Echo(), "b": Effect(self.root)}, self.root / "state",
                    mas_bin=self.mas, gateway_bin=self.gateway, workflow_version=WORKFLOW) as runner:
            database = runner._journal.db
            pages = database.execute("PRAGMA page_count").fetchone()[0]
            database.execute(f"PRAGMA max_page_count={pages}")
            with self.assertRaises(sqlite3.OperationalError) as full:
                runner.request("a", "b", "effect", {"oversized_checkpoint": "x" * 40_000})
            self.assertEqual(full.exception.sqlite_errorcode, sqlite3.SQLITE_FULL)
            self.assertFalse((self.root / "effects.txt").exists())
            self.assertEqual(runner.status()["calls"], [])
            self.assertEqual(runner.status()["messages"], [])
            self.assertEqual(database.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_lost_finish_reply_reuses_known_output_after_explicit_resume(self):
        with Runner(SPEC, {"a": Echo(), "b": Effect(self.root)}, self.root / "state",
                    mas_bin=self.mas, gateway_bin=self.gateway, workflow_version=WORKFLOW) as runner:
            gateway = runner._gateway
            lost = []
            def lose_finish_reply(actor, **command):
                result = gateway(actor, **command)
                if command["command"] == "finish" and not lost:
                    lost.append(True)
                    raise RunnerError("injected lost command response")
                return result
            with mock.patch.object(runner, "_gateway", side_effect=lose_finish_reply):
                with self.assertRaises(RunnerError):
                    runner.request("a", "b", "effect", {})
            run_dir = runner.run_dir
            self.assertEqual(lost, [True])
            self.assertEqual(runner.status()["calls"][0]["state"], "recorded")
        with self.resume(run_dir) as resumed:
            self.assertEqual(resumed.request("a", "b", "effect", {}).output, {"answer": 42})
        self.assertEqual((self.root / "effects.txt").read_text().splitlines(), ["effect"])

    def test_backup_restore_retains_actual_run_and_global_charges(self):
        from mas_host.operations import DeploymentLedger, backup_state, restore_state
        state = self.root / "state"
        with Runner(SPEC, {"a": Echo(), "b": Effect(self.root)}, state,
                    mas_bin=self.mas, gateway_bin=self.gateway, workflow_version=WORKFLOW) as runner:
            self.assertEqual(runner.request("a", "b", "effect", {}).output, {"answer": 42})
            relative_run = runner.run_dir.relative_to(state)
            original_run_id = runner.run_id
        before = DeploymentLedger(state).health()
        bundle, restored = self.root / "backup", self.root / "restored"
        backup_state(state, bundle)
        restore_state(bundle, restored)
        after = DeploymentLedger(restored).health()
        # Verify exact durable accounting rather than free-space/time fields.
        for key in ("deployment_id", "usage", "policy"):
            self.assertIn(key, before)
            self.assertEqual(before[key], after[key])
        with self.resume(restored / relative_run) as resumed:
            self.assertEqual(resumed.run_id, original_run_id)
            self.assertEqual(resumed.request("a", "b", "effect", {}).output, {"answer": 42})
        self.assertEqual((self.root / "effects.txt").read_text().splitlines(), ["effect"])

    def test_independent_hosts_respect_shared_call_budget(self):
        from mas_host.acceptance import run_acceptance
        from mas_host.operations import DeploymentLedger, DeploymentPolicy
        state = self.root / "state"
        ledger = DeploymentLedger(state, DeploymentPolicy(max_runs=8, max_calls=4))
        report = run_acceptance(state_dir=state, runs=4, concurrency=2,
                                mas_bin=self.mas, gateway_bin=self.gateway)
        self.assertEqual(report["run_count"], 4)
        self.assertFalse(report["ok"])
        self.assertGreater(report["error_count"], 0)
        self.assertLessEqual(sum(run["calls"] or 0 for run in report["runs"]), 4)
        self.assertEqual(ledger.health()["usage"]["calls"], 4)

    def test_live_acceptance_without_key_fails_before_any_admission(self):
        from mas_host.acceptance import run_live_acceptance
        with mock.patch.dict(os.environ, {}, clear=True):
            report = run_live_acceptance(state_dir=self.root / "unused")
        self.assertFalse(report["ok"])
        self.assertEqual(report["status"], "blocked_missing_credential")
        self.assertEqual(report["paid_provider_calls"], 0)
        self.assertFalse((self.root / "unused").exists())


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "crash-worker":
        crash_worker(*sys.argv[2:])
    else:
        unittest.main()
