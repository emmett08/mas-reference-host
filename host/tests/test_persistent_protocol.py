"""Conformance, transport faults and cleanup against the real Rust host IPC."""
import json
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import signal
import tempfile
import time
import unittest
from unittest.mock import patch

from mas_host import Outcome, Runner, RunnerError
from mas_host.protocol_client import PersistentProtocolClient, ProtocolClient
from test_runner import FunctionAdapter, topology
import test_runner


class PersistentProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_runner.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = test_runner.RunnerIntegrationTests.mas, test_runner.RunnerIntegrationTests.gateway

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def runner(self):
        client = PersistentProtocolClient(self.mas, self.gateway)
        self.addCleanup(client.close)
        value = Runner(topology(), {actor: FunctionAdapter(lambda _context, request: Outcome(request.input))
            for actor in ("a", "b", "c")}, Path(self.directory.name), mas_bin=self.mas,
            gateway_bin=self.gateway, supervised=False, protocol_client=client)
        self.addCleanup(value.close)
        return value, client

    def test_existing_contract_and_repeated_status_use_one_process(self):
        runner, client = self.runner()
        pid = client._process.pid
        outcome = runner.request("a", "b", "echo", {"text": "🧠 quoted \"value\"\nline"})
        self.assertEqual(outcome.output, {"text": "🧠 quoted \"value\"\nline"})
        expected = ProtocolClient(self.mas, self.gateway).command([self.mas, "status", str(runner._db), runner.run_id])
        with patch("mas_host.protocol_client.subprocess.Popen", side_effect=AssertionError("unexpected process launch")):
            for _ in range(12):
                self.assertEqual(runner.status(), expected)
        self.assertEqual(client._process.pid, pid)
        self.assertEqual(runner.status()["tasks"][0]["state"], "completed")

    def test_exact_wire_encode_and_capability_rejection_match_command_strategy(self):
        runner, client = self.runner()
        command = ProtocolClient(self.mas, self.gateway)
        envelope = json.dumps({"message_id": "wire-test", "run_id": runner.run_id,
            "sender": "a", "recipient": "b", "version": "1", "kind": "Notice",
            "payload": {"huge": 10**200, "text": "\n\"\\"}})
        args = [self.mas, "encode", "-"]
        self.assertEqual(client.command(args, envelope), command.command(args, envelope))
        for transport in (command, client):
            with self.assertRaises(RunnerError):
                transport.command([self.gateway, "serve", str(runner._db), str(runner._config)],
                                  json.dumps({"token": "00" * 32, "command": "receive"}) + "\n")
        with closing(sqlite3.connect(runner._db)) as db:
            used = db.execute("SELECT SUM(attempts) FROM admissions WHERE scope NOT LIKE '%/%/%'").fetchone()[0]
        self.assertEqual(used, 2)

    def test_lost_reply_is_not_retried_and_next_status_reads_committed_reservation(self):
        runner, client = self.runner()
        original = client._read
        first = True

        def lose_reply(size, deadline):
            nonlocal first
            body = original(size, deadline)
            if first:
                first = False
                raise TimeoutError("secret response deliberately lost")
            return body

        with patch.object(client, "_read", side_effect=lose_reply):
            with self.assertRaises(RunnerError) as error:
                runner._gateway("a", command="reserve", call_id="lost-reply")
        self.assertNotIn("secret", str(error.exception))
        self.assertIsNone(client._process)
        self.assertEqual([call["id"] for call in runner.status()["calls"]], ["lost-reply"])
        with closing(sqlite3.connect(runner._db)) as db:
            self.assertEqual(db.execute("SELECT attempts FROM admissions WHERE scope NOT LIKE '%/%/%'").fetchone()[0], 1)

    def test_bad_correlation_invalidates_session_and_does_not_dispatch_again(self):
        runner, client = self.runner()
        original = client._header

        def wrong_reply(deadline):
            result = original(deadline)
            result["request_id"] += 1
            return result

        with patch.object(client, "_header", side_effect=wrong_reply):
            with self.assertRaises(RunnerError):
                runner._gateway("a", command="reserve", call_id="wrong-correlation")
        self.assertIsNone(client._process)
        self.assertEqual([call["id"] for call in runner.status()["calls"]], ["wrong-correlation"])

    def test_timeout_terminates_child_and_never_exposes_content(self):
        runner, client = self.runner()
        process = client._process
        with patch.object(client, "_receive", side_effect=TimeoutError("secret reply")):
            with self.assertRaises(RunnerError) as error:
                runner.status()
        self.assertNotIn("secret", str(error.exception))
        self.assertIsNotNone(process.poll())
        self.assertIsNone(client._process)
        self.assertEqual(runner.status()["usage"]["calls"], 0)

    def test_explicit_close_reaps_child_and_rejects_reuse(self):
        _runner, client = self.runner()
        process = client._process
        client.close()
        self.assertIsNotNone(process.poll())
        self.assertTrue(process.stdin.closed)
        self.assertTrue(process.stdout.closed)
        with self.assertRaises(RunnerError):
            client.command([self.mas, "encode", "-"], "{}")

    @unittest.skipUnless(hasattr(signal, "SIGSTOP"), "requires Unix signals")
    def test_actual_stalled_child_obeys_deadline_and_is_reaped(self):
        runner, client = self.runner()
        process = client._process
        os.kill(process.pid, signal.SIGSTOP)
        started = time.monotonic()
        with self.assertRaises(RunnerError):
            client.command([self.mas, "status", str(runner._db), runner.run_id], timeout=0.03)
        self.assertLess(time.monotonic() - started, 1)
        self.assertIsNotNone(process.poll())
        self.assertEqual(runner.status()["usage"]["calls"], 0)

    def test_protocol_child_environment_contains_no_provider_credentials(self):
        from mas_host.protocol_client import subprocess
        original = subprocess.Popen
        environments = []

        def capture(*args, **kwargs):
            environments.append(kwargs["env"])
            return original(*args, **kwargs)

        with patch.dict(os.environ, {"OPENAI_API_KEY": "secret-key"}):
            with patch("mas_host.protocol_client.subprocess.Popen", side_effect=capture):
                self.runner()
        self.assertTrue(environments)
        self.assertTrue(all(set(environment) == {"PATH"} for environment in environments))

    @unittest.skipUnless(hasattr(os, "fork"), "requires fork")
    def test_forked_client_cannot_send_or_terminate_parent_child(self):
        runner, client = self.runner()
        pid = client._process.pid
        child = os.fork()
        if child == 0:
            try:
                try:
                    runner.status()
                except RunnerError:
                    try:
                        client.close()
                    except RunnerError:
                        os._exit(0)
                    os._exit(3)
                os._exit(1)
            except BaseException:
                os._exit(2)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(runner.status()["usage"]["calls"], 0)
        self.assertEqual(client._process.pid, pid)


class PersistentRunnerConformanceTests(test_runner.RunnerIntegrationTests):
    """Run the existing runner contract traces with the alternate transport."""

    def runner(self, functions=None, spec=None, **kwargs):
        # The original Runner already exposed this strategy port; keep the
        # transport foundation independently testable before new host features.
        client = PersistentProtocolClient(self.mas, self.gateway)
        self.addCleanup(client.close)
        return super().runner(functions, spec, protocol_client=client, **kwargs)

    def test_interrupted_command_reports_no_secret_input(self):
        runner = self.runner()
        with patch.object(runner._client, "_receive", side_effect=TimeoutError("secret response")):
            with self.assertRaises(RunnerError) as failure:
                runner.status()
        self.assertNotIn("secret", str(failure.exception))


if __name__ == "__main__":
    unittest.main()
