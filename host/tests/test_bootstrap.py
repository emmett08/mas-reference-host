"""Real SIGKILL boundaries while creating a private, durably admitted run."""
from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import signal
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from mas_host import Invocation, Outcome, Runner, RunnerError
from mas_host.bootstrap import RunSession, requires_reconciliation
from mas_host.journal import Journal
from mas_host.operations import DeploymentLedger
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


WORKFLOW = "bootstrap-crashes/1"


def _kill() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def crash_worker(state_dir: str, boundary: str, mas: str, gateway: str) -> None:
    original_stage = RunSession._save_stage
    original_command = Runner._command
    original_journal = Journal.__init__
    original_replace = os.replace

    def save_stage(self, stage):
        original_stage(self, stage)
        if boundary == stage:
            _kill()

    def command(self, args, source=None):
        if boundary == "gateway_partial" and args[1] == "provision":
            with Path(args[3]).open("w", encoding="utf-8") as stream:
                stream.write('{"credentials":')
                stream.flush()
                os.fsync(stream.fileno())
            _kill()
        result = original_command(self, args, source)
        if (boundary == "rust_init" and args[1] == "init"
                or boundary == "gateway_provisioned" and args[1] == "provision"):
            _kill()
        return result

    def journal(self, *args, **kwargs):
        original_journal(self, *args, **kwargs)
        if boundary == "journal_created":
            _kill()

    def replace(source, destination):
        original_replace(source, destination)
        if boundary == "gateway_published" and Path(destination).name == "gateway.json":
            _kill()

    RunSession._save_stage = save_stage
    Runner._command = command
    Journal.__init__ = journal
    os.replace = replace
    adapters = {actor: FunctionAdapter(lambda _c, request: Outcome(request.input)) for actor in ("a", "b", "c")}
    Runner(topology(), adapters, Path(state_dir), mas_bin=mas, gateway_bin=gateway,
           workflow_version=WORKFLOW)
    raise AssertionError("bootstrap fault boundary was not reached")


class BootstrapRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas = runner_tests.RunnerIntegrationTests.mas
        cls.gateway = runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.options = {"workflow_version": WORKFLOW, "mas_bin": self.mas,
                        "gateway_bin": self.gateway, "supervised": False}

    def crash_at(self, boundary):
        state = self.root / boundary
        result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "crash-worker",
                                 str(state), boundary, self.mas, self.gateway],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        self.assertEqual(result.returncode, -signal.SIGKILL, result.stderr.decode(errors="replace"))
        runs = [path for path in state.iterdir() if path.is_dir()]
        self.assertEqual(len(runs), 1)
        return state, runs[0]

    @unittest.skipUnless(sys.platform == "linux", "Linux SIGKILL acceptance")
    def test_each_bootstrap_boundary_recovers_identity_deadline_and_single_admission(self):
        boundaries = ("intent", "admitted", "rust_init", "protocol_ready", "journal_created",
                      "journal_ready", "gateway_partial", "gateway_provisioned", "gateway_published", "ready")
        for boundary in boundaries:
            with self.subTest(boundary=boundary):
                state, run_dir = self.crash_at(boundary)
                intent = json.loads((run_dir / "bootstrap.json").read_text())
                original_deadline = None
                if (run_dir / "state.sqlite").exists():
                    with closing(sqlite3.connect(run_dir / "state.sqlite")) as db:
                        row = db.execute("SELECT deadline FROM runs WHERE id=?", (intent["run_id"],)).fetchone()
                        original_deadline = row[0] if row else None
                private_path = (run_dir / "gateway.json" if (run_dir / "gateway.json").exists()
                                else run_dir / ".gateway.pending.json")
                credentials = (json.loads(private_path.read_text())
                               if private_path.exists() and boundary != "gateway_partial" else None)
                effects = []

                def handler(context, _request):
                    return Outcome(context.invoke(lambda: (effects.append(context.invocation_id) or
                        Invocation({"answer": 42}, {"receipt": "local"})), input={"effect": "once"},
                        reservation_microusd=0))

                adapters = {actor: FunctionAdapter(handler if actor == "b" else lambda c, r: Outcome(r.input))
                            for actor in ("a", "b", "c")}
                with Runner.resume(run_dir, adapters, **self.options) as resumed:
                    self.assertEqual(resumed.run_id, intent["run_id"])
                    snapshot = resumed.status()
                    if original_deadline is not None:
                        self.assertEqual(snapshot["deadline"], original_deadline)
                    self.assertEqual(snapshot["usage"]["calls"], 0)
                    self.assertEqual(DeploymentLedger(state).health()["usage"]["runs"], 1)
                    if credentials is not None:
                        self.assertEqual(json.loads((run_dir / "gateway.json").read_text()), credentials)
                    self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 42})
                with Runner.resume(run_dir, adapters, **self.options) as replay:
                    self.assertEqual(replay.request("a", "b", "solve", {}).output, {"answer": 42})
                    self.assertEqual(replay.status()["usage"]["calls"], 1)
                self.assertEqual(len(effects), 1)
                self.assertEqual(json.loads((run_dir / "bootstrap.json").read_text())["stage"], "ready")
                self.assertEqual(DeploymentLedger(state).health()["usage"],
                                 {"runs": 1, "calls": 1, "reserved_microusd": 0})

    def test_published_credentials_and_ready_state_are_never_recreated(self):
        for missing in ("gateway.json", "host.sqlite", "state.sqlite"):
            with self.subTest(missing=missing):
                state = self.root / missing
                adapters = {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")}
                with Runner(topology(), adapters, state, **self.options) as runner:
                    run_dir = runner.run_dir
                (run_dir / missing).unlink()
                with self.assertRaises(RunnerError):
                    Runner.resume(run_dir, adapters, **self.options)
                self.assertFalse((run_dir / missing).exists(), "ready state must not be recreated")

    def test_stopping_cannot_treat_missing_or_unreadable_published_journal_as_safe(self):
        adapters = {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ('a', 'b', 'c')}
        state = self.root / 'published'
        with Runner(topology(), adapters, state, **self.options) as runner:
            directory, run_id = runner.run_dir, runner.run_id
        ledger = DeploymentLedger(state)
        journal = directory / 'host.sqlite'
        original = journal.read_bytes()
        self.assertFalse(requires_reconciliation(directory, run_id, operations=ledger))
        for damage in ('malformed', 'missing', 'dangling_symlink'):
            with self.subTest(damage=damage):
                journal.unlink()
                if damage == 'malformed':
                    journal.write_bytes(b'not a SQLite database')
                elif damage == 'dangling_symlink':
                    journal.symlink_to(directory / 'missing-journal')
                self.assertTrue(requires_reconciliation(directory, run_id, operations=ledger))
                self.assertFalse((directory / 'missing-journal').exists())
                if journal.exists() or journal.is_symlink():
                    journal.unlink()
                journal.write_bytes(original)
        shutil.rmtree(directory)
        self.assertTrue(requires_reconciliation(directory, run_id, operations=ledger))
        self.assertTrue(requires_reconciliation(directory, run_id))
        self.assertFalse(directory.exists(), 'observation must not recreate missing evidence')

    def test_pre_journal_bootstrap_can_stop_without_becoming_unknown(self):
        for boundary in ('intent', 'admitted', 'protocol_ready'):
            with self.subTest(boundary=boundary):
                state, directory = self.crash_at(boundary)
                run_id = json.loads((directory / 'bootstrap.json').read_text())['run_id']
                ledger = DeploymentLedger(state)
                self.assertFalse(requires_reconciliation(directory, run_id, operations=ledger))
                self.assertFalse((directory / 'host.sqlite').exists())
                self.assertTrue(requires_reconciliation(directory, 'another-run', operations=ledger))

    def test_legacy_complete_unversioned_journal_migrates_without_repeating_effect(self):
        effects = []

        def handler(context, _request):
            return Outcome(context.invoke(lambda: (effects.append(True) or Invocation({"answer": 42}, {})),
                                          input={"effect": "once"}))

        adapters = {actor: FunctionAdapter(handler if actor == "b" else lambda _c, r: Outcome(r.input))
                    for actor in ("a", "b", "c")}
        with Runner(topology(), adapters, self.root, **self.options) as runner:
            runner.request("a", "b", "solve", {})
            run_dir = runner.run_dir
        (run_dir / "bootstrap.json").unlink()
        with closing(sqlite3.connect(run_dir / "host.sqlite")) as db, db:
            db.execute("PRAGMA application_id=0")
            db.execute("PRAGMA user_version=0")
        with Runner.resume(run_dir, adapters, **self.options) as resumed:
            self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(effects, [True])

    def test_new_bootstrap_still_rejects_changed_workflow_or_audit_only_restore(self):
        state, run_dir = self.crash_at("protocol_ready")
        adapters = {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")}
        with self.assertRaisesRegex(RunnerError, "original explicit"):
            Runner.resume(run_dir, adapters, **(self.options | {"workflow_version": "changed"}))
        with closing(sqlite3.connect(state / "deployment.sqlite")) as db, db:
            db.execute("UPDATE metadata SET value='restored_audit_only' WHERE key='admission_state'")
        with self.assertRaisesRegex(RunnerError, "audit-only"):
            Runner.resume(run_dir, adapters, **self.options)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "crash-worker":
        crash_worker(*sys.argv[2:])
    else:
        unittest.main()
