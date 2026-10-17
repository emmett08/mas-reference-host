"""Integration checks against the real Rust binaries, with no provider calls."""

import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock

from mas_host import AdapterError, Invocation, Outcome, Runner, RunnerError


class FunctionAdapter:
    def __init__(self, function):
        self.function = function

    def handle(self, context, request):
        return self.function(context, request)


def topology(agents=("a", "b", "c"), **overrides):
    value = {
        "agents": list(agents),
        "edges": [[a, b] for a in agents for b in agents if a != b],
        "application": "agent-task/1",
        "mailbox_capacity": 10,
        "max_messages": 100,
        "max_bytes": 100_000,
        "max_calls": 10,
        "wall_seconds": 120,
    }
    value.update(overrides)
    return value


class RunnerIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[2]
        release = root / "rust-protocol" / "target" / "release"
        cls.mas = os.environ.get("MAS_BIN") or shutil.which("mas") or str(release / "mas")
        cls.gateway = os.environ.get("MAS_GATEWAY_BIN") or shutil.which("mas-gateway") or str(release / "mas-gateway")
        for binary in (cls.mas, cls.gateway):
            if not Path(binary).is_file():
                raise RuntimeError("Build the Rust release binaries before running host integration tests")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def runner(self, functions=None, spec=None, **kwargs):
        spec = spec or topology()
        functions = functions or {}
        adapters = {
            actor: FunctionAdapter(functions.get(actor, lambda _context, request: Outcome(request.input)))
            for actor in spec["agents"]
        }
        value = Runner(spec, adapters, Path(self.temp.name), mas_bin=self.mas,
                       gateway_bin=self.gateway, supervised=False, **kwargs)
        self.addCleanup(value.close)
        return value

    def assert_drained(self, runner, states):
        status = runner.status()
        self.assertEqual([task["state"] for task in status["tasks"]], states)
        self.assertTrue(all(message["received"] and message["acked"] for message in status["messages"]))

    def test_nested_mesh_revisits_actor_without_redispatching_outer_request(self):
        visits = []

        def peer_b(context, request):
            visits.append((context.actor, request.operation))
            if request.operation == "start":
                return context.request("c", "review", request.input)
            return Outcome({"checked": request.input["value"]})

        def peer_c(context, request):
            visits.append((context.actor, request.operation))
            return context.request("b", "check", request.input)

        runner = self.runner({"b": peer_b, "c": peer_c})
        result = runner.request("a", "b", "start", {"value": 17})
        self.assertEqual(result.output, {"checked": 17})
        self.assertEqual(visits, [("b", "start"), ("c", "review"), ("b", "check")])
        self.assert_drained(runner, ["completed"] * 3)
        self.assertEqual(len(runner.events), 6)
        self.assertEqual(runner.run_dir.stat().st_mode & 0o777, 0o700)
        self.assertEqual((runner.run_dir / "gateway.json").stat().st_mode & 0o777, 0o600)

    def test_forbidden_topology_route_is_not_dispatched(self):
        invoked = []
        spec = topology(edges=[["a", "b"], ["b", "a"]])
        runner = self.runner({"c": lambda *_args: invoked.append(True)}, spec)
        with self.assertRaises(RunnerError):
            runner.request("a", "c", "check", {})
        self.assertEqual(invoked, [])
        self.assertEqual(runner.status()["messages"], [])

    def test_invalid_provider_number_cannot_enter_protocol_result(self):
        runner = self.runner({"b": lambda *_args: Outcome({"value": 1.5})})
        with self.assertRaises(AdapterError):
            runner.request("a", "b", "check", {})
        self.assert_drained(runner, ["failed"])
        self.assertEqual([event["kind"] for event in runner.events], ["Request", "Failure"])

    def test_provider_reservation_commits_before_invocation_and_records_evidence(self):
        observed = []

        def provider():
            with sqlite3.connect(runner.run_dir / "state.sqlite") as db:
                observed.extend(db.execute("SELECT state FROM calls").fetchall())
            return Invocation({"answer": 42}, {"provider": "fake", "response_id": "response-1"})

        runner = self.runner({"b": lambda context, _request: Outcome(context.invoke(provider))})
        self.assertEqual(runner.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(observed, [("reserved",)])
        call = runner.status()["calls"][0]
        self.assertEqual(call["state"], "recorded")
        self.assertEqual(call["evidence"]["response_id"], "response-1")
        self.assert_drained(runner, ["completed"])

    def test_provider_failure_is_sanitised_and_never_retried(self):
        calls = []

        def provider():
            calls.append(True)
            raise RuntimeError("sk-secret-provider-key private prompt stack")

        runner = self.runner({"b": lambda context, _request: Outcome(context.invoke(provider))},
                             topology(max_calls=1))
        for _ in range(2):
            with self.assertRaises(AdapterError) as failure:
                runner.request("a", "b", "solve", {})
            self.assertNotIn("sk-secret", str(failure.exception))
        status = runner.status()
        self.assertEqual(calls, [True])
        self.assertEqual(status["usage"]["calls"], 1)
        self.assertEqual(status["calls"][0]["evidence"]["outcome"], "unknown")
        self.assertNotIn("sk-secret", json.dumps(status))
        self.assertEqual([task["state"] for task in status["tasks"]], ["open", "failed"])
        self.assertFalse(status["messages"][0]["acked"])

    def test_expiry_rejects_a_late_result_without_claiming_completion(self):
        def expire_during_work(_context, _request):
            # Deterministic fault injection models a host-observed deadline.
            with sqlite3.connect(runner.run_dir / "state.sqlite") as db:
                db.execute("UPDATE runs SET deadline=0")
            return Outcome({"late_answer": 42})

        runner = self.runner({"b": expire_during_work})
        with self.assertRaises(RunnerError):
            runner.request("a", "b", "solve", {})
        status = runner.status()
        self.assertTrue(status["expired"])
        self.assertEqual(status["tasks"][0]["state"], "expired_outcome_unknown")
        self.assertEqual([event["kind"] for event in runner.events], ["Request"])
        self.assertFalse(status["messages"][0]["acked"])

    def test_depth_limit_closes_recursive_tasks_with_failure(self):
        def recurse(context, _request):
            target = "b" if context.actor == "a" else "a"
            return context.request(target, "again", {})

        runner = self.runner({"a": recurse, "b": recurse}, max_depth=2)
        with self.assertRaises(AdapterError):
            runner.request("a", "b", "again", {})
        self.assert_drained(runner, ["failed", "failed"])
        self.assertEqual(runner.status()["usage"]["messages"], 4)

    def test_explicit_unresolved_result_is_not_upgraded_to_completed(self):
        runner = self.runner({"b": lambda *_args: Outcome({"reason": "insufficient_evidence"}, "unresolved")})
        result = runner.request("a", "b", "solve", {})
        self.assertEqual(result.status, "unresolved")
        self.assert_drained(runner, ["unresolved"])

    def test_public_reentry_and_stale_context_are_rejected(self):
        contexts = []

        def adapter(context, _request):
            contexts.append(context)
            with self.assertRaises(RunnerError):
                runner.request("b", "c", "reenter", {})
            return Outcome({})

        runner = self.runner({"b": adapter})
        runner.request("a", "b", "check", {})
        with self.assertRaises(RunnerError):
            contexts[0].request("c", "stale", {})
        self.assert_drained(runner, ["completed"])

    def test_provider_credentials_are_not_in_rust_child_environments(self):
        original = subprocess.run
        child_environments = []

        def capture(*args, **kwargs):
            child_environments.append(kwargs["env"])
            return original(*args, **kwargs)

        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "sk-not-a-real-key"}):
            with mock.patch("mas_host.protocol_client.subprocess.run", side_effect=capture):
                runner = self.runner()
                text = 'quoted "value"\nmessage "not-an-envelope";'
                self.assertEqual(runner.request("a", "b", "echo", {"text": text}).output, {"text": text})
        self.assertTrue(child_environments)
        self.assertTrue(all(set(environment) == {"PATH"} for environment in child_environments))

    def test_call_cannot_delegate_using_its_own_active_context(self):
        def adapter(context, _request):
            def provider():
                context.request("c", "nested_in_provider", {})
                return Invocation({}, {})
            return Outcome(context.invoke(provider))

        runner = self.runner({"b": adapter})
        with self.assertRaises(AdapterError):
            runner.request("a", "b", "solve", {})
        self.assertEqual(runner.status()["tasks"][0]["state"], "open")
        self.assertEqual(runner.status()["calls"][0]["evidence"]["outcome"], "unknown")
        self.assertEqual(len(runner.status()["calls"]), 1)

    def test_interrupted_command_reports_no_secret_input(self):
        runner = self.runner()
        exception = subprocess.TimeoutExpired("hidden-command", 10, output="secret stdout", stderr="secret stderr")
        with mock.patch("mas_host.protocol_client.subprocess.run", side_effect=exception):
            with self.assertRaises(RunnerError) as failure:
                runner.status()
        self.assertNotIn("secret", str(failure.exception))

    def test_lost_result_send_response_keeps_completed_task_without_replaying_provider(self):
        invocations = []

        def provider():
            invocations.append(True)
            return Invocation({"answer": 42}, {"provider": "fake", "response_id": "response-1"})

        runner = self.runner({"b": lambda context, _request: Outcome(context.invoke(provider))})
        original = runner._gateway
        lost = []

        def lose_committed_response(actor, **command):
            result = original(actor, **command)
            if (not lost and command["command"] == "send"
                    and 'kind "Result"' in command["wire"]):
                lost.append(True)
                raise RunnerError("protocol command unavailable or interrupted")
            return result

        with mock.patch.object(runner, "_gateway", side_effect=lose_committed_response):
            self.assertEqual(runner.request("a", "b", "solve", {}).output, {"answer": 42})
        self.assertEqual(lost, [True])
        self.assertEqual(invocations, [True])
        status = runner.status()
        self.assertEqual(status["tasks"][0]["state"], "completed")
        self.assertEqual(len(status["calls"]), 1)
        self.assertEqual(status["calls"][0]["state"], "recorded")
        self.assertEqual(status["usage"]["messages"], 2)
        self.assertTrue(all(message["acked"] for message in status["messages"]))


if __name__ == "__main__":
    unittest.main()
