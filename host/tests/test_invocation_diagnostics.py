"""Finite public failure stages survive IPC without weakening uncertainty."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mas_host.contracts import InvocationFailure, validate_invocation_diagnostic
from mas_host.invoice_cursor import InvoiceCursorProvider as CursorLocalProvider
from mas_host.cursor_session import _OwnedClient, _close_bridge
from mas_host.journal import inspect_run
from mas_host.provider_contract import ProviderError
from mas_host.runner import AdapterError, Invocation, Outcome, RecoveryRequired, Runner
from test_cursor_local import FakeClient, FakeRun, INVOICE, result
import test_runner as runner_tests


def fail(error):
    raise error


def diagnostic(stage, cleanup_failed=False):
    return {"provider": "cursor-local", "stage": stage, "cleanup_failed": cleanup_failed}


class CursorDiagnosticTests(unittest.TestCase):
    def test_each_failure_stage_is_finite_and_contains_no_sdk_details(self):
        for stage in ("launch", "create", "send", "stream", "wait", "result", "cleanup"):
            with self.subTest(stage=stage):
                client = FakeClient()
                error = RuntimeError("SECRET SDK detail")
                if stage == "create":
                    client.create = lambda **_kwargs: fail(error)
                elif stage == "send":
                    client.agent.send = lambda _prompt: fail(error)
                elif stage == "stream":
                    client.agent.run.error = error
                elif stage == "wait":
                    client.agent.run.wait = lambda: fail(error)
                elif stage == "result":
                    client.agent.run.response = result("SECRET invalid output")
                elif stage == "cleanup":
                    client.agent.close = lambda: fail(error)
                provider = CursorLocalProvider("model", "planner", api_key="SECRET key")
                with patch("mas_host.cursor_session.launch_client",
                           side_effect=error if stage == "launch" else None,
                           return_value=client), self.assertRaises(ProviderError) as raised:
                    provider.total(INVOICE)
                self.assertEqual(raised.exception.diagnostic, diagnostic(stage, stage == "cleanup"))
                self.assertNotIn("SECRET", str(raised.exception))
                if stage != "launch":
                    self.assertTrue(client.closed)

    def test_primary_stage_survives_all_cleanup_failures(self):
        client = FakeClient(FakeRun(error=RuntimeError("SECRET stream")))
        attempted = []
        def cleanup(name):
            attempted.append(name)
            raise RuntimeError("SECRET " + name)
        client.agent.run.cancel = lambda: cleanup("cancel")
        client.agent.close = lambda: cleanup("agent")
        client.close = lambda: cleanup("client")
        with patch("mas_host.cursor_session.launch_client", return_value=client), \
                self.assertRaises(ProviderError) as raised:
            CursorLocalProvider("model", "planner", api_key="key").total(INVOICE)
        self.assertEqual(attempted, ["cancel", "agent", "client"])
        self.assertEqual(raised.exception.diagnostic, diagnostic("stream", True))
        self.assertEqual(len(client.agent.prompts), 1)

    def test_partial_launch_cleanup_diagnostic_is_preserved(self):
        error = ProviderError("safe launch failure", diagnostic=diagnostic("launch", True))
        with patch("mas_host.cursor_session.launch_client", side_effect=error), \
                self.assertRaises(ProviderError) as raised:
            CursorLocalProvider("model", "planner", api_key="key").total(INVOICE)
        self.assertEqual(raised.exception.diagnostic, diagnostic("launch", True))

    def test_workspace_cleanup_keeps_primary_and_cleanup_only_is_failure(self):
        for prior in (None, ProviderError("safe", diagnostic=diagnostic("send"))):
            with self.subTest(prior=prior is not None):
                workspace = SimpleNamespace(name="unused", cleanup=lambda: fail(RuntimeError("SECRET cleanup")))
                provider = CursorLocalProvider("model", "planner", client=FakeClient())
                with patch("mas_host.cursor_session.tempfile.TemporaryDirectory", return_value=workspace), \
                        patch.object(provider, "_run", side_effect=prior,
                                     return_value=Invocation({"total_cents": 1}, {})), \
                        self.assertRaises(ProviderError) as raised:
                    provider.total(INVOICE)
                self.assertEqual(raised.exception.diagnostic, diagnostic("send" if prior else "cleanup", True))

    def test_primary_interruptions_survive_cleanup_errors(self):
        for interruption in (KeyboardInterrupt(), SystemExit(7)):
            with self.subTest(interruption=type(interruption).__name__):
                client = FakeClient(FakeRun(error=interruption))
                client.agent.close = lambda: fail(RuntimeError("SECRET cleanup"))
                provider = CursorLocalProvider("model", "planner", client=client)
                with self.assertRaises(type(interruption)) as raised:
                    provider.total(INVOICE)
                self.assertIs(raised.exception, interruption)
                self.assertTrue(client.agent.run.cancelled)

    def test_owned_client_attempts_every_cleanup_preserving_first_failure(self):
        owned = object.__new__(_OwnedClient)
        attempted = []
        first = KeyboardInterrupt()
        def cleanup(name, error):
            attempted.append(name)
            raise error
        owned._client = SimpleNamespace(close=lambda: cleanup("sdk", first))
        owned._owned_bridge = object()
        owned._stderr_thread = None
        owned._http_client = SimpleNamespace(close=lambda: cleanup("http", RuntimeError("SECRET http")))
        with patch("mas_host.cursor_session._close_bridge",
                   side_effect=lambda *_args: cleanup("bridge", RuntimeError("SECRET bridge"))), \
                self.assertRaises(KeyboardInterrupt) as raised:
            owned.close()
        self.assertIs(raised.exception, first)
        self.assertEqual(attempted, ["sdk", "bridge", "http"])

    def test_bridge_cleanup_attempts_remaining_resources_after_interrupt(self):
        attempted = []
        first = KeyboardInterrupt()
        def cleanup(name, error):
            attempted.append(name)
            raise error
        process = SimpleNamespace(poll=lambda: None,
            terminate=lambda: cleanup("process", first),
            stderr=SimpleNamespace(close=lambda: cleanup("stderr", RuntimeError("SECRET stderr"))))
        bridge = SimpleNamespace(process=process,
            _store_callback_server=SimpleNamespace(close=lambda: cleanup("store", RuntimeError("SECRET store"))),
            _tool_callback_server=SimpleNamespace(close=lambda: cleanup("tool", RuntimeError("SECRET tool"))))
        thread = SimpleNamespace(join=lambda **_kwargs: cleanup("join", RuntimeError("SECRET join")))
        with self.assertRaises(KeyboardInterrupt) as raised:
            _close_bridge(bridge, thread)
        self.assertIs(raised.exception, first)
        self.assertEqual(attempted, ["process", "join", "stderr", "store", "tool"])
        self.assertIs(bridge.process, process)


class InvocationDiagnosticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def runner(self, handler, supervised=True):
        adapters = {actor: runner_tests.FunctionAdapter(
            handler if actor == "b" else lambda _context, _request: Outcome({}))
            for actor in ("a", "b", "c")}
        runner = Runner(runner_tests.topology(), adapters, self.root,
                        workflow_version="diagnostic/1", supervised=supervised, **self.options)
        self.addCleanup(runner.close)
        return runner, adapters

    def test_provider_diagnostic_reaches_journal_and_blocks_retry(self):
        for supervised in (False, True):
            with self.subTest(supervised=supervised):
                marker = self.root / ("attempts-" + str(supervised))
                client = FakeClient(FakeRun(error=RuntimeError("SECRET provider error")))
                client.agent.close = lambda: fail(RuntimeError("SECRET close"))
                provider = CursorLocalProvider("model", "planner", client=client)
                def invoke():
                    with marker.open("a") as stream:
                        stream.write("attempt\n")
                    return provider.total(INVOICE)
                def handler(context, _request):
                    return Outcome(context.invoke(invoke, input={"provider": "cursor-local"},
                                                  reservation_microusd=100000))
                runner, adapters = self.runner(handler, supervised)
                with self.assertRaises(RecoveryRequired):
                    runner.request("a", "b", "diagnose", {})
                report = inspect_run(runner.run_dir)
                self.assertTrue(report["recovery_required"])
                call = report["invocations"][0]
                self.assertEqual(call["state"], "unknown")
                self.assertEqual(call["evidence"], {"outcome": "unknown", "error": "adapter_invocation_failed",
                                                  "diagnostic": diagnostic("stream", True)})
                self.assertEqual(runner.status()["calls"][0]["evidence"], call["evidence"])
                self.assertNotIn("SECRET", json.dumps(report))
                runner.close()
                with Runner.resume(runner.run_dir, adapters, workflow_version="diagnostic/1",
                                   supervised=supervised, **self.options) as resumed:
                    with self.assertRaises(RecoveryRequired):
                        resumed.request("a", "b", "diagnose", {})
                self.assertEqual(marker.read_text().splitlines(), ["attempt"])
                self.assertEqual(inspect_run(runner.run_dir)["invocations"][0]["evidence"], call["evidence"])

    def test_cleanup_only_failure_retains_known_valid_completion(self):
        client = FakeClient()
        client.agent.close = lambda: fail(RuntimeError("SECRET close"))
        provider = CursorLocalProvider("model", "planner", client=client)
        runner, _ = self.runner(lambda context, _request: Outcome(context.invoke(
            lambda: provider.total(INVOICE), input={"provider": "cursor-local"})))
        from mas_host.contracts import AdapterError
        with self.assertRaises(AdapterError) as failure:
            runner.request("a", "b", "diagnose", {})
        self.assertNotIsInstance(failure.exception, RecoveryRequired)
        call = inspect_run(runner.run_dir)["invocations"][0]
        self.assertEqual(call["state"], "failed_known")
        self.assertEqual(call["output"], {"total_cents": 2998})
        self.assertEqual(call["evidence"]["provider_outcome"]["output_status"], "valid")
        self.assertTrue(call["evidence"]["provider_outcome"]["cleanup_failed"])

    def test_failed_frames_accept_legacy_but_reject_unbounded_diagnostics(self):
        invalid = [None, [], diagnostic("SECRET"), diagnostic("stream") | {"detail": "SECRET"},
                   diagnostic("stream") | {"provider": "SECRET"},
                   diagnostic("stream") | {"cleanup_failed": 1},
                   {"provider": "cursor-local", "stage": "stream"}]
        for supplied in ["legacy", *invalid, diagnostic("wait")]:
            with self.subTest(supplied=supplied):
                def handler(context, _request):
                    context._channel.send({"op": "prepare", "input": {}, "reservation_microusd": 0})
                    context._channel.receive()
                    frame = {"op": "failed"}
                    if supplied != "legacy":
                        frame["diagnostic"] = supplied
                    context._channel.send(frame)
                    context._channel.receive()
                runner, _ = self.runner(handler)
                with self.assertRaises(RecoveryRequired):
                    runner.request("a", "b", "diagnose", {})
                call = inspect_run(runner.run_dir)["invocations"][0]
                self.assertEqual(call["state"], "unknown")
                expected = diagnostic("wait") if supplied == diagnostic("wait") else None
                self.assertEqual(call["evidence"].get("diagnostic"), expected)
                self.assertNotIn("SECRET", json.dumps(call))
                runner.close()

    def test_failed_frame_without_ticket_cannot_create_invocation_evidence(self):
        def handler(context, _request):
            context._channel.send({"op": "failed", "diagnostic": diagnostic("send")})
            context._channel.receive()
        runner, _ = self.runner(handler)
        with self.assertRaises(AdapterError):
            runner.request("a", "b", "diagnose", {})
        self.assertEqual(inspect_run(runner.run_dir)["invocations"], [])

    def test_error_schema_is_copied_and_mutated_exception_data_is_discarded(self):
        value = diagnostic("stream")
        error = InvocationFailure("SECRET", diagnostic=value)
        value["stage"] = "SECRET"
        self.assertEqual(error.diagnostic, diagnostic("stream"))
        error.diagnostic["detail"] = "SECRET"
        from mas_host.contracts import invocation_failure_diagnostic
        self.assertIsNone(invocation_failure_diagnostic(error))
        with self.assertRaises(ValueError):
            validate_invocation_diagnostic(error.diagnostic)


if __name__ == "__main__":
    unittest.main()
