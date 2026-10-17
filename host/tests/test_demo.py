"""Offline mesh integration, using the real Rust protocol and gateway binaries."""
from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host.demo import DEFAULT_INVOICE, DEFAULT_TOPOLOGY, CheckerAdapter, ReviewerAdapter, _load_json, main, run_demo
from mas_host.providers import MockProvider, ProviderError
from mas_host.runner import Invocation, Outcome, Runner, RunnerError, TaskRequest

ROOT = Path(__file__).resolve().parents[2]
MAS = ROOT / "rust-protocol/target/release/mas"
GATEWAY = ROOT / "rust-protocol/target/release/mas-gateway"
TOPOLOGY = ROOT / "examples/host/mesh.json"


class WrongProvider(MockProvider):
    def __init__(self):
        super().__init__("wrong-total-fixture")

    def total(self, invoice, **_kwargs):
        return Invocation(output={"total_cents": 1}, evidence={"provider": "mock", "fault": "wrong-total"})


class DemoContractTests(unittest.TestCase):
    def test_incomplete_provider_contract_rejected_before_run_admission(self):
        class Provider:
            def total(self, invoice, *, proposal=None, request_id=None):
                raise AssertionError("invalid bindings must never dispatch")

        with tempfile.TemporaryDirectory() as state:
            with self.assertRaises(ProviderError):
                run_demo(topology=DEFAULT_TOPOLOGY, state_dir=state,
                         planner_provider=Provider(), reviewer_provider=MockProvider("reviewer"))
            self.assertEqual(list(Path(state).iterdir()), [])

    def test_installed_default_matches_compose_topology(self):
        self.assertEqual(DEFAULT_TOPOLOGY, json.loads(TOPOLOGY.read_text()))

    def test_file_inputs_reject_duplicate_keys_and_non_integer_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            for source in ['{"edges":[],"edges":[["a","b"]]}', '{"quantity":1.0}', '{"quantity":NaN}']:
                path.write_text(source)
                with self.subTest(source=source), self.assertRaises(ValueError):
                    _load_json(str(path))

    def test_checker_exposes_disagreement_as_unresolved(self):
        request = TaskRequest(sender="reviewer", recipient="checker", operation="invoice.check",
            input={"invoice": DEFAULT_INVOICE, "planner_total_cents": 1, "reviewer_total_cents": 5497},
            task_id="task", message_id="message")
        result = CheckerAdapter().handle(None, request)
        self.assertEqual(result.status, "unresolved")
        self.assertEqual(result.output["total_cents"], 5497)
        self.assertFalse(result.output["verified"])
        self.assertFalse(result.output["planner_correct"])
        self.assertTrue(result.output["reviewer_correct"])

    def test_reviewer_provider_consumes_delivered_candidate(self):
        seen = []
        class Provider(MockProvider):
            def __init__(self):
                super().__init__("reviewer-fixture")

            def total(self, invoice, *, proposal=None, request_id=None):
                seen.append((invoice, proposal))
                return Invocation(output={"total_cents": 5497}, evidence={"provider": "fake"})
        class Context:
            invocation_id = "unit-test-call"
            def invoke(self, call, **_kwargs):
                return call().output
            def request(self, recipient, operation, inputs):
                self.submitted = (recipient, operation, inputs)
                return Outcome(output={"verified": True})
        context = Context()
        task = TaskRequest(sender="planner", recipient="reviewer", operation="invoice.review",
            input={"invoice": DEFAULT_INVOICE, "planner_total_cents": 1}, task_id="task", message_id="message")
        ReviewerAdapter(Provider()).handle(context, task)
        self.assertEqual(seen, [(DEFAULT_INVOICE, {"total_cents": 1})])
        self.assertEqual(context.submitted[0:2], ("checker", "invoice.check"))
        self.assertEqual(context.submitted[2]["planner_total_cents"], 1)

    def test_live_missing_secret_fails_without_provider_call(self):
        output = io.StringIO()
        with patch.dict(os.environ, {}, clear=True), redirect_stdout(output):
            result = main(["--mode", "openai", "--topology", str(TOPOLOGY)])
        self.assertEqual(result, 1)
        report = json.loads(output.getvalue().splitlines()[-1])
        self.assertFalse(report["ok"])
        self.assertIn("OPENAI_API_KEY", report["error"])

    def test_unexpected_error_is_safe_for_shared_console(self):
        output = io.StringIO()
        with patch.dict(os.environ, {}, clear=True), patch("mas_host.demo.run_demo", side_effect=RuntimeError("private-secret")), redirect_stdout(output):
            code = main(["--mode", "mock", "--topology", str(TOPOLOGY)])
        self.assertEqual(code, 1)
        self.assertNotIn("private-secret", output.getvalue())


@unittest.skipUnless(MAS.is_file() and GATEWAY.is_file(), "build Rust release binaries for native mesh integration")
class NativeMeshTests(unittest.TestCase):
    def run_mesh(self, state_dir, planner=None):
        return run_demo(topology=json.loads(TOPOLOGY.read_text()), state_dir=state_dir,
            planner_provider=planner or MockProvider("planner"), reviewer_provider=MockProvider("reviewer"),
            mas_bin=str(MAS), gateway_bin=str(GATEWAY))

    def test_mock_mesh_round_trip_with_real_gateway(self):
        with tempfile.TemporaryDirectory() as state:
            # Mock mode does not even import the optional provider SDK.
            with patch.dict("sys.modules", {"openai": None}):
                report = self.run_mesh(state)
            self.assertTrue(report["ok"])
            self.assertEqual(report["result"]["total_cents"], 5497)
            self.assertEqual(report["mode"], "mock")
            saved = json.loads(Path(report["report_path"]).read_text())
            self.assertEqual(saved["run_id"], report["run_id"])
            self.assertTrue(saved["ok"])
            serialized = json.dumps(report)
            for actor in ("planner", "reviewer", "checker"):
                self.assertIn(actor, serialized)
            self.assertNotIn('"token"', serialized)
            self.assertEqual({(event["sender"], event["recipient"]) for event in report["events"]},
                             {tuple(edge) for edge in DEFAULT_TOPOLOGY["edges"]})
            self.assertEqual(report["status"]["usage"]["calls"], 2)
            self.assertEqual(report["status"]["usage"]["messages"], 6)

    def test_demo_resume_reuses_calls_and_rejects_changed_provider_binding(self):
        with tempfile.TemporaryDirectory() as state:
            original = self.run_mesh(state)
            options = dict(topology=DEFAULT_TOPOLOGY, state_dir=state,
                planner_provider=MockProvider("planner"), reviewer_provider=MockProvider("reviewer"),
                resume_dir=Path(original["report_path"]).parent, mas_bin=str(MAS), gateway_bin=str(GATEWAY))
            resumed = run_demo(**options)
            self.assertTrue(resumed["ok"])
            self.assertEqual(resumed["run_id"], original["run_id"])
            self.assertEqual(resumed["status"]["usage"]["calls"], 2)
            class ChangedProvider(MockProvider):
                def descriptor(self):
                    return {**super().descriptor(), "model": "changed-model"}
            options["planner_provider"] = ChangedProvider("planner")
            with self.assertRaises(RunnerError):
                run_demo(**options)

    def test_provider_failure_retains_report_and_unknown_invocation(self):
        class FailedProvider(MockProvider):
            def __init__(self):
                super().__init__("failed-provider-fixture")

            def total(self, invoice, **_kwargs):
                raise RuntimeError("private-api-key")
        with tempfile.TemporaryDirectory() as state:
            report = self.run_mesh(state, FailedProvider())
            self.assertFalse(report["ok"])
            self.assertEqual(report["outcome"], "unknown")
            self.assertEqual(report["status"]["usage"]["calls"], 1)
            self.assertEqual(report["status"]["calls"][0]["evidence"]["outcome"], "unknown")
            self.assertEqual(json.loads(Path(report["report_path"]).read_text()), report)
            self.assertNotIn("private-api-key", json.dumps(report))

    def test_lost_reply_after_committed_result_reports_unknown_without_replay(self):
        original_command = Runner._command
        lost_replies = []

        def lose_final_result_reply(runner, args, source=None):
            result = original_command(runner, args, source)
            if source is not None and args[0] == runner._gateway_bin and args[1] == "serve":
                frame = json.loads(source)
                wire = frame.get("wire", "")
                if (frame.get("command") == "send" and 'kind "Result"' in wire
                        and 'from "planner" to "checker"' in wire):
                    lost_replies.append(True)
                    raise RunnerError("simulated lost gateway reply after durable commit")
            return result

        with tempfile.TemporaryDirectory() as state:
            with patch.object(Runner, "_command", lose_final_result_reply):
                report = self.run_mesh(state)
            self.assertEqual(len(lost_replies), 2)
            self.assertFalse(report["ok"])
            self.assertEqual(report["outcome"], "unknown")
            self.assertIn("exchange incomplete", report["error"])
            self.assertEqual(report["status"]["usage"]["calls"], 2)
            self.assertEqual(report["status"]["usage"]["messages"], 6)
            self.assertEqual([task["state"] for task in report["status"]["tasks"]], ["completed"] * 3)
            self.assertEqual(json.loads(Path(report["report_path"]).read_text()), report)

    def test_wrong_model_result_is_not_accepted_as_success(self):
        with tempfile.TemporaryDirectory() as state:
            report = self.run_mesh(state, WrongProvider())
            self.assertFalse(report["ok"])
            self.assertEqual(report["outcome"], "unresolved")
            self.assertEqual(report["result"]["total_cents"], 5497)
            self.assertFalse(report["result"]["planner_correct"])

    def test_fresh_runs_do_not_reuse_spent_invocations(self):
        with tempfile.TemporaryDirectory() as state:
            first, second = self.run_mesh(state), self.run_mesh(state)
            self.assertTrue(first["ok"] and second["ok"])
            self.assertNotEqual(first["run_id"], second["run_id"])
            self.assertNotEqual(first["report_path"], second["report_path"])


if __name__ == "__main__":
    unittest.main()
