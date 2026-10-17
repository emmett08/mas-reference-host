"""Durable host replay against the authoritative Rust transport and call records."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from mas_host import AdapterError, Invocation, Outcome, RecoveryRequired, Runner, RunnerError
from mas_host.journal import JournalError, inspect_run, resolve_invocation
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


class RecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas = runner_tests.RunnerIntegrationTests.mas
        cls.gateway = runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.options = {"mas_bin": self.mas, "gateway_bin": self.gateway,
                        "workflow_version": "recovery-test/1", "supervised": False}

    def create(self, handler):
        self.adapters = {actor: FunctionAdapter(handler if actor == "b" else lambda _c, r: Outcome(r.input))
                         for actor in ("a", "b", "c")}
        runner = Runner(topology(), self.adapters, Path(self.temp.name), **self.options)
        self.addCleanup(runner.close)
        return runner

    def resume(self, runner, **overrides):
        runner.close()
        resumed = Runner.resume(runner.run_dir, self.adapters, **(self.options | overrides))
        self.addCleanup(resumed.close)
        return resumed

    def test_completed_invocation_survives_host_exit_before_terminal(self):
        effects = []
        first = [True]

        def handle(context, _request):
            result = context.invoke(lambda: (effects.append(context.invocation_id) or
                Invocation({"answer": 42}, {"response_id": "known"})), input={"prompt": "solve", "model": "fake"})
            if first.pop() if first else False:
                raise SystemExit("simulated host exit")
            return Outcome(result)

        runner = self.create(handle)
        with self.assertRaises(SystemExit):
            runner.request("a", "b", "solve", {"value": 7})
        deadline = runner.status()["deadline"]
        resumed = self.resume(runner)
        self.assertEqual(resumed.request("a", "b", "solve", {"value": 7}).output, {"answer": 42})
        self.assertEqual(len(effects), 1)
        self.assertIsNotNone(effects[0])
        self.assertEqual(resumed.status()["deadline"], deadline)
        self.assertEqual(len(resumed.status()["calls"]), 1)

    def test_unknown_blocks_replay_then_completed_resolution_uses_evidenced_output(self):
        effects = []

        def provider():
            effects.append(True)
            raise RuntimeError("private failure")

        runner = self.create(lambda c, _r: Outcome(c.invoke(provider, input={"effect": "one"})))
        with self.assertRaises(RecoveryRequired):
            runner.request("a", "b", "solve", {})
        resumed = self.resume(runner)
        with self.assertRaises(RecoveryRequired):
            resumed.request("a", "b", "solve", {})
        call_id = inspect_run(runner.run_dir)["invocations"][0]["call_id"]
        with self.assertRaises(JournalError):
            resolve_invocation(runner.run_dir, call_id, decision="completed", output={"answer": 42}, evidence={"record": "receipt-1"})
        resumed.close()
        resolved = resolve_invocation(runner.run_dir, call_id, decision="completed", output={"answer": 42}, evidence={"record": "receipt-1"})
        self.assertFalse(resolved["recovery_required"])
        resumed = self.resume(resumed)
        self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(effects, [True])
        self.assertEqual(resumed.status()["calls"][0]["evidence"]["outcome"], "unknown")
        with self.assertRaises(JournalError):
            resumed.close()
            resolve_invocation(runner.run_dir, call_id, decision="completed", output={}, evidence={"record": "conflict"})

    def test_confirmed_no_effect_allows_one_new_charged_attempt(self):
        effects = []

        def provider():
            effects.append(True)
            if len(effects) == 1:
                raise RuntimeError("uncertain")
            return Invocation({"answer": 9}, {"response_id": "retry-receipt"})

        runner = self.create(lambda c, _r: Outcome(c.invoke(provider, input={"effect": "one"})))
        with self.assertRaises(RecoveryRequired):
            runner.request("a", "b", "solve", {})
        call_id = inspect_run(runner.run_dir)["invocations"][0]["call_id"]
        runner.close()
        resolve_invocation(runner.run_dir, call_id, decision="confirmed_no_effect", evidence={"provider_audit": "no-record-confirmed"})
        resumed = self.resume(runner)
        self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 9})
        self.assertEqual(len(resumed.status()["calls"]), 2)
        self.assertEqual(len(effects), 2)
        with sqlite3.connect(Path(self.temp.name) / "deployment.sqlite") as db:
            self.assertEqual(db.execute("SELECT count(*) FROM calls").fetchone()[0], 2)
        replayed = self.resume(resumed)
        self.assertEqual(replayed.request("a", "b", "solve", {}).output, {"answer": 9})
        self.assertEqual(len(effects), 2)

    def test_workflow_version_changed_input_and_concurrent_ownership_are_rejected(self):
        runner = self.create(lambda _c, r: Outcome(r.input))
        runner.request("a", "b", "solve", {"value": 7})
        with self.assertRaises(RunnerError):
            Runner.resume(runner.run_dir, self.adapters, **self.options)
        runner.close()
        with self.assertRaises(RunnerError):
            Runner.resume(runner.run_dir, self.adapters, **(self.options | {"workflow_version": "changed"}))
        resumed = self.resume(runner)
        with self.assertRaisesRegex(RunnerError, "differs"):
            resumed.request("a", "b", "solve", {"value": 8})

    def test_changed_invocation_identity_is_rejected_before_reusing_output(self):
        selected = ["first"]
        stopped = [True]
        effects = []

        def handle(context, _request):
            output = context.invoke(lambda: (effects.append(True) or Invocation({"answer": 1}, {})),
                                    input={"model": selected[0]})
            if stopped.pop() if stopped else False:
                raise SystemExit
            return Outcome(output)

        runner = self.create(handle)
        with self.assertRaises(SystemExit):
            runner.request("a", "b", "solve", {})
        selected[0] = "different"
        resumed = self.resume(runner)
        with self.assertRaisesRegex(RunnerError, "differs"):
            resumed.request("a", "b", "solve", {})
        self.assertEqual(effects, [True])

    def test_lost_finish_reply_keeps_output_for_recovery(self):
        effects = []
        runner = self.create(lambda c, _r: Outcome(c.invoke(lambda: (
            effects.append(True) or Invocation({"answer": 42}, {"response_id": "receipt"})), input={})))
        original = runner._gateway

        def lose(actor, **command):
            result = original(actor, **command)
            if command["command"] == "finish":
                raise RunnerError("lost response")
            return result

        with mock.patch.object(runner, "_gateway", side_effect=lose):
            with self.assertRaises(RecoveryRequired):
                runner.request("a", "b", "solve", {})
        self.assertEqual(inspect_run(runner.run_dir)["invocations"][0]["state"], "completed")
        resumed = self.resume(runner)
        self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(effects, [True])

    def test_lost_request_and_ack_responses_reuse_identical_envelopes(self):
        runner = self.create(lambda _c, r: Outcome(r.input))
        original = runner._gateway
        lost = set()

        def lose(actor, **command):
            result = original(actor, **command)
            operation = command["command"]
            if operation in ("send", "ack") and operation not in lost:
                lost.add(operation)
                raise RunnerError("lost response")
            return result

        with mock.patch.object(runner, "_gateway", side_effect=lose):
            self.assertEqual(runner.request("a", "b", "solve", {"answer": 42}).output, {"answer": 42})
        self.assertEqual(runner.status()["usage"]["messages"], 2)
        self.assertTrue(all(item["acked"] for item in runner.status()["messages"]))

    def test_removed_replayed_step_is_rejected(self):
        execute = [True]
        stop = [True]

        def handle(context, _request):
            if execute[0]:
                context.invoke(lambda: Invocation({}, {}), input={"operation": "recorded"})
            if stop.pop() if stop else False:
                raise SystemExit
            return Outcome({})

        runner = self.create(handle)
        with self.assertRaises(SystemExit):
            runner.request("a", "b", "solve", {})
        execute[0] = False
        resumed = self.resume(runner)
        with self.assertRaisesRegex(RunnerError, "omitted"):
            resumed.request("a", "b", "solve", {})

    def test_supervised_unknown_can_be_resolved_without_rerunning_effect(self):
        effect_file = Path(self.temp.name) / "effects.txt"

        def provider():
            with effect_file.open("a") as stream:
                stream.write("effect\n")
            raise RuntimeError("uncertain remote result")

        self.options["supervised"] = True
        runner = self.create(lambda c, _r: Outcome(c.invoke(provider, input={"effect": "remote"})))
        with self.assertRaises(RecoveryRequired):
            runner.request("a", "b", "solve", {})
        self.assertEqual(runner.status()["tasks"][0]["state"], "open")
        call_id = inspect_run(runner.run_dir)["invocations"][0]["call_id"]
        runner.close()
        resolve_invocation(runner.run_dir, call_id, decision="completed", output={"answer": 42},
                           evidence={"provider_receipt": "audited"})
        resumed = self.resume(runner)
        self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(effect_file.read_text(), "effect\n")

    def test_audit_only_restore_returns_committed_terminal_but_blocks_incomplete_work(self):
        stop = [False]

        def handle(_context, request):
            if stop[0]:
                raise SystemExit
            return Outcome(request.input)

        runner = self.create(handle)
        runner.request("a", "b", "solve", {"answer": 42})
        stop[0] = True
        with self.assertRaises(SystemExit):
            runner.request("a", "b", "solve", {"answer": 43})
        runner.close()
        with sqlite3.connect(Path(self.temp.name) / "deployment.sqlite") as db:
            db.execute("UPDATE metadata SET value='restored_audit_only' WHERE key='admission_state'")
        resumed = self.resume(runner)
        self.assertEqual(resumed.request("a", "b", "solve", {"answer": 42}).output, {"answer": 42})
        with self.assertRaisesRegex(RunnerError, "audit-only"):
            resumed.request("a", "b", "solve", {"answer": 43})

    def test_completed_work_remains_readable_after_the_fixed_deadline(self):
        runner = self.create(lambda _c, r: Outcome(r.input))
        runner.request("a", "b", "solve", {"answer": 42})
        with sqlite3.connect(runner.run_dir / "state.sqlite") as db:
            db.execute("UPDATE runs SET deadline=0")
        resumed = self.resume(runner)
        self.assertTrue(resumed.status()["expired"])
        self.assertEqual(resumed.request("a", "b", "solve", {"answer": 42}).output, {"answer": 42})

    def test_rejected_run_admissions_do_not_leave_untracked_directories(self):
        from mas_host.operations import DeploymentLedger, DeploymentPolicy
        state = Path(self.temp.name)
        ledger = DeploymentLedger(state, DeploymentPolicy(max_runs=1))
        self.options["operations"] = ledger
        runner = self.create(lambda _c, r: Outcome(r.input))
        directories = sorted(path.name for path in state.iterdir() if path.is_dir())
        for _ in range(3):
            with self.assertRaises(RunnerError):
                Runner(topology(), self.adapters, state, **self.options)
        self.assertEqual(sorted(path.name for path in state.iterdir() if path.is_dir()), directories)
        self.assertEqual(ledger.health()["usage"]["runs"], 1)

    def test_rejected_call_is_recorded_as_not_executed(self):
        from mas_host.operations import DeploymentLedger, DeploymentPolicy
        state = Path(self.temp.name)
        self.options["operations"] = DeploymentLedger(state, DeploymentPolicy(max_calls=1))
        effects = []
        runner = self.create(lambda c, _r: Outcome(c.invoke(lambda: (
            effects.append(True) or Invocation({}, {})), input={"provider": "fake"})))
        runner.request("a", "b", "solve", {})
        with self.assertRaises(RunnerError):
            runner.request("a", "b", "solve", {})
        self.assertEqual(effects, [True])
        states = [call["state"] for call in inspect_run(runner.run_dir)["invocations"]]
        self.assertEqual(states, ["completed", "not_executed"])

    def test_prepared_reservation_response_loss_can_resume_before_any_effect(self):
        effects = []
        runner = self.create(lambda c, _r: Outcome(c.invoke(lambda: (
            effects.append(True) or Invocation({"answer": 42}, {"response_id": "receipt"})), input={})))
        original_gateway = runner._gateway
        original_status = runner.status
        lose_status = [False]

        def gateway(actor, **command):
            result = original_gateway(actor, **command)
            if command["command"] == "reserve":
                lose_status[0] = True
                raise RunnerError("lost reservation reply")
            return result

        def status():
            if lose_status[0]:
                lose_status[0] = False
                raise RunnerError("temporarily unavailable")
            return original_status()

        with mock.patch.object(runner, "_gateway", side_effect=gateway), mock.patch.object(runner, "status", side_effect=status):
            with self.assertRaises(RecoveryRequired):
                runner.request("a", "b", "solve", {})
        self.assertEqual(effects, [])
        self.assertEqual(inspect_run(runner.run_dir)["invocations"][0]["state"], "prepared")
        resumed = self.resume(runner)
        self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(effects, [True])
        self.assertEqual(len(resumed.status()["calls"]), 1)

    def test_catching_unknown_effect_cannot_authorise_more_work_or_commit_a_result(self):
        for supervised in (False, True):
            with self.subTest(supervised=supervised):
                self.options["supervised"] = supervised
                effect_file = Path(self.temp.name) / f"effects-{supervised}"

                def provider():
                    with effect_file.open("a", encoding="utf-8") as stream:
                        stream.write("effect\n")
                    raise RuntimeError("unknown external outcome")

                def handle(context, _request):
                    try:
                        context.invoke(provider, input={"effect": "one"})
                    except RecoveryRequired:
                        # Application exception handling cannot turn uncertainty
                        # into permission to retry or an authoritative result.
                        try:
                            context.invoke(provider, input={"effect": "one"})
                        except RecoveryRequired:
                            pass
                    return Outcome({"fallback": True})

                runner = self.create(handle)
                with self.assertRaises(RecoveryRequired):
                    runner.request("a", "b", "solve", {})
                self.assertEqual(effect_file.read_text().splitlines(), ["effect"])
                self.assertEqual(runner.status()["tasks"][0]["state"], "open")
                self.assertEqual(len(runner.status()["calls"]), 1)

    def test_interruption_after_executing_commit_before_ticket_assignment_stays_recoverable(self):
        from mas_host.journal import Journal
        from mas_host.supervision import AdapterInterrupted
        self.options["supervised"] = True
        effect_file = Path(self.temp.name) / "released-effects"

        def provider():
            effect_file.write_text("effect", encoding="utf-8")
            return Invocation({"answer": 42}, {"receipt": "once"})

        runner = self.create(lambda context, _request: Outcome(context.invoke(provider, input={"effect": "one"})))
        original = Journal.update_invocation

        def interrupt(journal, call_id, state, **kwargs):
            original(journal, call_id, state, **kwargs)
            if state == "executing":
                raise AdapterInterrupted("shutdown between checkpoint and ticket handover")

        with mock.patch.object(Journal, "update_invocation", interrupt):
            with self.assertRaises(RecoveryRequired):
                runner.request("a", "b", "solve", {})
        self.assertFalse(effect_file.exists())
        self.assertEqual(runner.status()["tasks"][0]["state"], "open")
        uncertain = inspect_run(runner.run_dir)["invocations"][0]
        self.assertEqual(uncertain["state"], "executing")
        runner.close()
        resolve_invocation(runner.run_dir, uncertain["call_id"], decision="confirmed_no_effect",
                           evidence={"test": "interrupted before provider permission"})
        resumed = self.resume(runner)
        self.assertEqual(resumed.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(effect_file.read_text(), "effect")
        self.assertEqual(len(resumed.status()["calls"]), 2)

    def test_caught_prepare_error_cannot_replace_pending_invocation_or_clear_uncertainty(self):
        from mas_host.journal import Journal
        original = Journal.update_invocation
        for supervised in (False, True):
            with self.subTest(supervised=supervised):
                self.options["supervised"] = supervised
                effect_file = Path(self.temp.name) / f"late-effect-{supervised}"
                interrupted = [False]

                def after_commit(journal, call_id, state, **kwargs):
                    original(journal, call_id, state, **kwargs)
                    if state == "executing" and not interrupted[0]:
                        interrupted[0] = True
                        # A recoverable IPC category deliberately exercises an
                        # adapter that catches the first error and tries again.
                        raise AdapterError("interrupted after durable admission")

                def provider():
                    effect_file.write_text("unexpected effect", encoding="utf-8")
                    return Invocation({}, {})

                def handle(context, _request):
                    try:
                        context.invoke(provider, input={"effect": "first"})
                    except AdapterError:
                        pass
                    try:
                        context.invoke(provider, input={"effect": "second"})
                    except AdapterError:
                        pass
                    return Outcome({"fallback": True})

                runner = self.create(handle)
                with mock.patch.object(Journal, "update_invocation", after_commit):
                    with self.assertRaises(RecoveryRequired):
                        runner.request("a", "b", "solve", {})
                self.assertFalse(effect_file.exists())
                self.assertEqual(runner.status()["tasks"][0]["state"], "open")
                self.assertEqual(len(runner.status()["calls"]), 1)
                calls = inspect_run(runner.run_dir)["invocations"]
                self.assertEqual([call["state"] for call in calls], ["executing"])


if __name__ == "__main__":
    unittest.main()
