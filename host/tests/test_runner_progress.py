"""Live progress and journal outbox replay cannot change external effects."""
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import patch

from mas_host.contracts import AdapterInterrupted, Invocation, Outcome
from mas_host.journal import Journal
from mas_host.runner import Runner
import test_runner as runner_tests


@contextmanager
def shutdown_handler():
    def interrupted(_number, _frame):
        raise AdapterInterrupted('host shutdown')
    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


class RunnerProgressTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway,
                       "workflow_version": "progress/1", "supervised": False}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.effects, self.events = [], {}

    def sink(self, event):
        identity = (event["run_id"], event["message_id"])
        if identity in self.events:
            self.assertEqual(self.events[identity], event)
        self.events[identity] = event

    def adapters(self, *, assert_live=False):
        def b(context, request):
            return context.request("c", "leaf", request.input)
        def c(context, request):
            if assert_live:
                self.assertEqual(len(self.events), 2)
                self.assertTrue(all(event["kind"] == "Request" for event in self.events.values()))
            def effect():
                self.effects.append(True)
                return Invocation(request.input, {})
            return Outcome(context.invoke(effect))
        return {"a": runner_tests.FunctionAdapter(lambda _c, request: Outcome(request.input)),
                "b": runner_tests.FunctionAdapter(b), "c": runner_tests.FunctionAdapter(c)}

    def test_events_are_publicly_available_before_nested_effect_completes(self):
        with Runner(runner_tests.topology(), self.adapters(assert_live=True), self.root,
                    event_sink=self.sink, **self.options) as runner:
            self.assertEqual(runner.request("a", "b", "root", {"SECRET": "input"}).output, {"SECRET": "input"})
            self.assertEqual(len(self.events), 4)
            self.assertNotIn("SECRET", str(self.events))
            self.assertEqual(runner._journal.pending_events(), [])

    def test_completed_nested_events_recover_after_sink_outage_without_invocation_replay(self):
        def unavailable(_event):
            raise RuntimeError("private storage detail")
        adapters = self.adapters()
        with Runner(runner_tests.topology(), adapters, self.root, event_sink=unavailable, **self.options) as runner:
            runner.request("a", "b", "root", {})
            run = runner.run_dir
            self.assertEqual(len(runner._journal.pending_events()), 4)
        with Runner.resume(run, adapters, event_sink=self.sink, **self.options) as runner:
            self.assertEqual(len(self.events), 4)
            runner.request("a", "b", "root", {})
            self.assertEqual(len(self.events), 4)
        self.assertEqual(self.effects, [True])

    def test_outbox_gap_and_lost_publication_ack_are_repaired_by_stable_identity(self):
        adapters = self.adapters()
        with Runner(runner_tests.topology(), adapters, self.root, event_sink=self.sink, **self.options) as runner:
            with patch.object(Journal, "queue_event", side_effect=OSError("interrupted outbox")):
                runner.request("a", "b", "root", {})
            run = runner.run_dir
            self.assertEqual(self.events, {})
        with patch.object(Journal, "publish_event", side_effect=OSError("lost publication ACK")):
            with Runner.resume(run, adapters, event_sink=self.sink, **self.options) as runner:
                runner.request("a", "b", "root", {})
        with Runner.resume(run, adapters, event_sink=self.sink, **self.options) as runner:
            runner.request("a", "b", "root", {})
            self.assertEqual(runner._journal.pending_events(), [])
        self.assertEqual(len(self.events), 4)
        self.assertEqual(self.effects, [True])

    @unittest.skipUnless(os.name == 'posix', 'POSIX shutdown signals')
    def test_shutdown_during_event_publication_stops_before_external_effect(self):
        for boundary in ('queue_event', '_event_sink', 'publish_event'):
            with self.subTest(boundary=boundary):
                self.events.clear()
                self.effects.clear()
                with Runner(runner_tests.topology(), self.adapters(), self.root,
                            event_sink=self.sink, **self.options) as runner:
                    owner = runner if boundary == '_event_sink' else runner._journal
                    original = getattr(owner, boundary)
                    interrupted = []
                    def publish(*args):
                        if not interrupted:
                            interrupted.append(True)
                            os.kill(os.getpid(), signal.SIGTERM)
                        return original(*args)
                    with patch.object(owner, boundary, side_effect=publish), shutdown_handler():
                        with self.assertRaises(AdapterInterrupted):
                            runner.request('a', 'b', 'root', {})
                    self.assertEqual(interrupted, [True])
                    self.assertEqual(self.effects, [])
                    self.assertEqual(runner.status()['usage']['calls'], 0)

    @unittest.skipUnless(os.name == 'posix', 'POSIX shutdown signals')
    def test_shutdown_after_gateway_commit_does_not_retry_send_or_ack(self):
        for operation in ('send', 'ack'):
            with self.subTest(operation=operation):
                self.events.clear()
                with Runner(runner_tests.topology(), self.adapters(), self.root,
                            event_sink=self.sink, **self.options) as runner:
                    original = runner._gateway
                    interrupted = []
                    attempts = []
                    def gateway(actor, **command):
                        attempts.append((actor, command))
                        result = original(actor, **command)
                        if not interrupted and command['command'] == operation:
                            interrupted.append((actor, command))
                            os.kill(os.getpid(), signal.SIGTERM)
                        return result
                    with patch.object(runner, '_gateway', side_effect=gateway), shutdown_handler():
                        with self.assertRaises(AdapterInterrupted):
                            runner.request('a', 'b', 'root', {})
                    self.assertEqual(len(interrupted), 1)
                    self.assertEqual(attempts[-1], interrupted[0])
                    self.assertEqual(attempts.count(interrupted[0]), 1)

    @unittest.skipUnless(os.name == 'posix', 'POSIX shutdown signals')
    def test_shutdown_in_final_event_flush_releases_request_ownership(self):
        with Runner(runner_tests.topology(), self.adapters(), self.root,
                    event_sink=self.sink, **self.options) as runner:
            original = runner.flush_events
            interrupted = []
            def flush():
                step = runner._journal.get_step('root:0')
                if step is not None and step['done']:
                    interrupted.append(True)
                    os.kill(os.getpid(), signal.SIGTERM)
                return original()
            with patch.object(runner, 'flush_events', side_effect=flush), shutdown_handler():
                with self.assertRaises(AdapterInterrupted):
                    runner.request('a', 'b', 'root', {})
            self.assertEqual(interrupted, [True])
            self.assertFalse(runner._request_lock.locked())


if __name__ == "__main__":
    unittest.main()
