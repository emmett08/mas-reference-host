"""Known provider completion and pre-dispatch failures cross the real boundary."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mas_host.contracts import AdapterError, KnownInvocationFailure, Outcome, RecoveryRequired, RunnerError
from mas_host.cursor_provider import CursorLocalProvider
from mas_host.journal import Journal, inspect_run
from mas_host.provider_outcomes import (ProviderOutcomeFailure, completed_unusable, completed_cleanup_failure, not_sent,
                                        validate_provider_outcome, validate_provider_diagnostic)
from mas_host.runner import Runner
from mas_host.schema import JOURNAL, Schema, SchemaError, validate
from mas_host.structured_openai import OpenAIStructuredProvider
from test_structured_providers import OpenAIClient, CursorClient, CursorRun, request
import test_runner as runner_tests


class ProviderObservationTests(unittest.TestCase):
    def test_readiness_and_missing_credentials_submit_no_inference(self):
        for provider in (OpenAIStructuredProvider("model", "worker", environ={}),
                         CursorLocalProvider("model", "worker", environ={})):
            original = provider.descriptor()
            for operation in (provider.readiness, lambda: provider.infer(request())):
                with self.subTest(provider=original["provider"], operation=operation):
                    with self.assertRaises(ProviderOutcomeFailure) as error:
                        operation()
                    self.assertEqual(error.exception.outcome["certainty"], "not_sent")
                    self.assertEqual(error.exception.outcome["category"], "credentials_unavailable")
                    self.assertEqual(provider.descriptor(), original)

    def test_readiness_with_injected_clients_is_effect_free(self):
        for provider, client in ((OpenAIStructuredProvider, OpenAIClient()),
                                 (CursorLocalProvider, CursorClient())):
            instance = provider("model", "worker", client=client)
            self.assertFalse(instance.readiness()["inference_submitted"])
            self.assertEqual(client.calls if isinstance(client, OpenAIClient) else client.prompts, [])

    def test_refusal_invalid_output_and_cleanup_keep_completion_evidence(self):
        cases = [(OpenAIClient(text="SECRET invalid output"), "invalid_output"),
                 (OpenAIClient(output=[SimpleNamespace(content=[SimpleNamespace(type="refusal")])]),
                  "refused_output")]
        for client, category in cases:
            client.close = lambda: (_ for _ in ()).throw(RuntimeError("SECRET cleanup"))
            with patch("mas_host.openai_client.create_client", return_value=client):
                provider = OpenAIStructuredProvider("model", "worker", environ={"OPENAI_API_KEY": "SECRET"})
                with self.assertRaises(ProviderOutcomeFailure) as error:
                    provider.infer(request(), request_id="call-test")
            outcome = error.exception.outcome
            self.assertEqual(outcome["certainty"], "completed")
            self.assertEqual(outcome["category"], category)
            self.assertTrue(outcome["cleanup_failed"])
            self.assertEqual(outcome["completion"]["response_id"], "resp_example")
            self.assertEqual(outcome["completion"]["usage"]["total_tokens"], 7)
            self.assertNotIn("SECRET", json.dumps(outcome))

    def test_cursor_invalid_output_retains_exact_run_and_completion(self):
        client = CursorClient(CursorRun(text="SECRET malformed"))
        client.close_agent = lambda: (_ for _ in ()).throw(RuntimeError("SECRET cleanup"))
        with self.assertRaises(ProviderOutcomeFailure) as error:
            CursorLocalProvider("model", "worker", client=client).infer(request(), request_id="call-cursor")
        outcome = error.exception.outcome
        self.assertEqual(outcome["certainty"], "completed")
        self.assertEqual(outcome["completion"]["run_id"], "run-example")
        self.assertEqual(outcome["completion"]["request_sha256"], request().identity)
        self.assertTrue(outcome["cleanup_failed"])
        self.assertNotIn("SECRET", json.dumps(outcome))

    def test_valid_completion_survives_client_agent_and_workspace_cleanup_failures(self):
        for kind in ("openai", "cursor-agent", "cursor-workspace"):
            with self.subTest(kind=kind):
                client = OpenAIClient() if kind == "openai" else CursorClient()
                def fail():
                    raise RuntimeError("SECRET cleanup")
                if kind == "openai":
                    client.close = fail
                    provider = OpenAIStructuredProvider("model", "worker", environ={"OPENAI_API_KEY": "SECRET"})
                    cleanup_patch = patch("mas_host.openai_client.create_client", return_value=client)
                else:
                    provider = CursorLocalProvider("model", "worker", client=client)
                    if kind == "cursor-agent":
                        client.close_agent = fail
                        cleanup_patch = patch("mas_host.cursor_session.tempfile.TemporaryDirectory",
                                              wraps=tempfile.TemporaryDirectory)
                    else:
                        workspace = SimpleNamespace(name="unused", cleanup=fail)
                        cleanup_patch = patch("mas_host.cursor_session.tempfile.TemporaryDirectory", return_value=workspace)
                with cleanup_patch, self.assertRaises(ProviderOutcomeFailure) as failure:
                    provider.infer(request(), request_id="call-completed")
                observed = failure.exception
                self.assertEqual(observed.outcome["certainty"], "completed")
                self.assertEqual(observed.outcome["output_status"], "valid")
                self.assertEqual(observed.outcome["category"], "cleanup_failed")
                self.assertEqual(observed.outcome["stage"], "cleanup")
                self.assertEqual(observed.outcome["recovery_action"], "complete_cleanup_before_using_output")
                self.assertTrue(observed.outcome["cleanup_failed"])
                self.assertEqual(observed.completed_output, {"accepted": True})
                observed.completed_output["accepted"] = False
                self.assertEqual(observed.with_cleanup_failure().completed_output, {"accepted": True})
                self.assertNotIn("SECRET", json.dumps(observed.outcome))
                self.assertNotIn("accepted", json.dumps(observed.outcome))

    def test_oversized_private_output_does_not_erase_completed_certainty(self):
        failure = completed_cleanup_failure("custom", {}, {"payload": "x" * 40_000})
        self.assertEqual(failure.outcome["certainty"], "completed")
        self.assertIsNone(failure.completed_output)
        with self.assertRaises(ValueError):
            ProviderOutcomeFailure(not_sent("custom", "operation_busy").outcome, completed_output={})

    def test_closed_outcomes_copy_and_reject_untrusted_fields(self):
        original = not_sent("openai", "credentials_unavailable")
        exposed = original.outcome
        exposed["category"] = "SECRET"
        self.assertEqual(original.outcome["category"], "credentials_unavailable")
        for changed in ({"detail": "SECRET"}, {"certainty": "unknown"}, {"automatic_retry": True},
                        {"completion": {"prompt": "SECRET"}}, {"cleanup_failed": 1},
                        {"certainty": "completed", "output_status": "unavailable"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                validate_provider_outcome(original.outcome | changed)

    def test_http_metadata_is_observable_without_claiming_non_execution(self):
        from mas_host.provider_contract import ProviderError
        failure = RuntimeError("SECRET request body")
        failure.status_code = 429
        failure.request_id = "request-limited"
        failure.response = SimpleNamespace(headers={"retry-after": "7", "authorization": "SECRET"})
        with self.assertRaises(ProviderError) as raised:
            OpenAIStructuredProvider("model", "worker", client=OpenAIClient(error=failure)).infer(request())
        self.assertNotIsInstance(raised.exception, ProviderOutcomeFailure)
        observed = raised.exception.diagnostic
        self.assertEqual(observed["http_status"], 429)
        self.assertEqual(observed["retry_after_seconds"], 7)
        self.assertEqual(observed["server_request_id"], "request-limited")
        self.assertEqual(observed["recovery_action"], "reconcile_before_repeating")
        self.assertNotIn("SECRET", json.dumps(observed))
        with self.assertRaises(ValueError):
            validate_provider_diagnostic(observed | {"detail": "SECRET"})


class ProviderOutcomeRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway,
                       "workflow_version": "provider-outcomes/1"}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def adapters(self, callback, *, catches=False):
        def handler(context, _request):
            try:
                return Outcome(context.invoke(callback, input={"effect": "fixture"}, reservation_microusd=37))
            except KnownInvocationFailure as error:
                if catches:
                    return Outcome({"category": error.outcome["category"]})
                raise
        return {actor: runner_tests.FunctionAdapter(handler if actor == "b" else
                lambda _context, _request: Outcome({})) for actor in ("a", "b", "c")}

    def test_known_failures_are_terminal_metered_and_never_redispatched(self):
        for supervised in (False, True):
            for kind in ("not_sent", "invalid", "refused", "openai-cleanup", "cursor-cleanup"):
                with self.subTest(supervised=supervised, kind=kind):
                    marker = self.root / (str(supervised) + kind)
                    def invoke():
                        with marker.open("a") as stream:
                            stream.write("attempt\n")
                        if kind == "not_sent":
                            return OpenAIStructuredProvider("model", "worker", environ={}).infer(request())
                        if kind == "openai-cleanup":
                            client = OpenAIClient()
                            client.close = lambda: (_ for _ in ()).throw(RuntimeError("SECRET cleanup"))
                            with patch("mas_host.openai_client.create_client", return_value=client):
                                return OpenAIStructuredProvider("model", "worker", api_key="SECRET").infer(request())
                        if kind == "cursor-cleanup":
                            client = CursorClient()
                            client.close_agent = lambda: (_ for _ in ()).throw(RuntimeError("SECRET cleanup"))
                            return CursorLocalProvider("model", "worker", client=client).infer(request())
                        client = OpenAIClient(text="SECRET invalid") if kind == "invalid" else OpenAIClient(
                            output=[SimpleNamespace(content=[SimpleNamespace(type="refusal")])])
                        return OpenAIStructuredProvider("model", "worker", client=client).infer(request())
                    adapters = self.adapters(invoke)
                    with Runner(runner_tests.topology(), adapters, self.root, supervised=supervised,
                                **self.options) as runner:
                        with self.assertRaises(AdapterError) as raised:
                            runner.request("a", "b", "observe", {})
                        self.assertNotIsInstance(raised.exception, RecoveryRequired)
                        run_dir = runner.run_dir
                        recorded = inspect_run(run_dir)
                        self.assertFalse(recorded["recovery_required"])
                        call = recorded["invocations"][0]
                        self.assertEqual(call["state"], "failed_known")
                        if kind.endswith("cleanup"):
                            self.assertEqual(call["output"], {"accepted": True})
                            self.assertEqual(call["evidence"]["provider_outcome"]["output_status"], "valid")
                        else:
                            self.assertIsNone(call["output"])
                        self.assertEqual(call["reservation_microusd"], 37)
                        self.assertEqual(runner.status()["usage"]["calls"], 1)
                        self.assertEqual(runner.status()["calls"][0]["evidence"], call["evidence"])
                        self.assertNotIn("SECRET", json.dumps(recorded))
                    with Runner.resume(run_dir, adapters, supervised=supervised, **self.options) as resumed:
                        with self.assertRaises(AdapterError) as raised:
                            resumed.request("a", "b", "observe", {})
                        self.assertNotIsInstance(raised.exception, RecoveryRequired)
                        self.assertEqual(resumed.status()["usage"]["calls"], 1)
                    self.assertEqual(marker.read_text().splitlines(), ["attempt"])

    def test_adapter_can_handle_known_outcomes_equally_in_both_strategies(self):
        def invoke():
            raise not_sent("custom", "operation_busy")
        for supervised in (False, True):
            with Runner(runner_tests.topology(), self.adapters(invoke, catches=True), self.root,
                        supervised=supervised, **self.options) as runner:
                self.assertEqual(runner.request("a", "b", "observe", {}).output, {"category": "operation_busy"})
                self.assertFalse(inspect_run(runner.run_dir)["recovery_required"])

    def test_lost_finish_reply_replays_known_checkpoint_without_second_call(self):
        for supervised in (False, True):
            marker = self.root / ("interrupted-" + str(supervised))
            def invoke():
                with marker.open("a") as stream:
                    stream.write("attempt\n")
                raise completed_unusable("custom", "invalid_output", {"usage": {"total_tokens": 8}})
            adapters = self.adapters(invoke, catches=True)
            with Runner(runner_tests.topology(), adapters, self.root, supervised=supervised, **self.options) as runner:
                with patch.object(runner, "_finish_recorded", side_effect=RunnerError("lost reply")):
                    with self.assertRaises(RecoveryRequired):
                        runner.request("a", "b", "observe", {})
                run_dir = runner.run_dir
                self.assertEqual(inspect_run(run_dir)["invocations"][0]["state"], "failed_known")
            with Runner.resume(run_dir, adapters, supervised=supervised, **self.options) as resumed:
                self.assertEqual(resumed.request("a", "b", "observe", {}).output, {"category": "invalid_output"})
            self.assertEqual(marker.read_text().splitlines(), ["attempt"])

    def test_ambiguous_transport_failure_stays_unknown(self):
        for supervised in (False, True):
            provider = OpenAIStructuredProvider("model", "worker", client=OpenAIClient(error=TimeoutError("SECRET")))
            with Runner(runner_tests.topology(), self.adapters(lambda: provider.infer(request())), self.root,
                        supervised=supervised, **self.options) as runner:
                with self.assertRaises(RecoveryRequired):
                    runner.request("a", "b", "observe", {})
                observation = inspect_run(runner.run_dir)
                self.assertTrue(observation["recovery_required"])
                self.assertEqual(observation["invocations"][0]["state"], "unknown")
                self.assertNotIn("SECRET", json.dumps(observation))

    def test_untrusted_known_failure_frame_cannot_clear_uncertainty(self):
        def handler(context, _request):
            context._channel.send({"op": "prepare", "input": {}, "reservation_microusd": 0})
            context._channel.receive()
            context._channel.send({"op": "known_failure", "outcome":
                                   not_sent("custom", "invalid_request").outcome | {"detail": "SECRET"}})
            context._channel.receive()
        adapters = {actor: runner_tests.FunctionAdapter(handler) for actor in ("a", "b", "c")}
        with Runner(runner_tests.topology(), adapters, self.root, **self.options) as runner:
            with self.assertRaises(RecoveryRequired):
                runner.request("a", "b", "observe", {})
            observation = inspect_run(runner.run_dir)
            self.assertTrue(observation["recovery_required"])
            self.assertNotIn("SECRET", json.dumps(observation))

    def test_v1_unknown_records_migrate_without_reclassification_and_old_readers_reject_v2(self):
        metadata = {"run_id": "r", "workflow_version": "old/1", "topology": {}, "max_depth": 8}
        journal = Journal(self.root, metadata)
        journal.step("s", "invoke", {}, {})
        call = journal.create_invocation("s", "a", 37)
        journal.update_invocation(call["call_id"], "unknown", evidence={"outcome": "unknown"})
        journal.db.execute("PRAGMA user_version=1")
        journal.close()
        before = inspect_run(self.root)
        Journal(self.root).close()
        self.assertEqual(inspect_run(self.root), before)
        old = Schema(JOURNAL.name, JOURNAL.application_id, 1, ((0, 0),), JOURNAL.statements)
        with closing(sqlite3.connect(self.root / "host.sqlite")) as db:
            with self.assertRaises(SchemaError):
                validate(db, old)


if __name__ == "__main__":
    unittest.main()
