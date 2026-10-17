"""Policy clocks and bounded attempts exercised through real Rust admission."""
from pathlib import Path
import tempfile
import time
import unittest

from mas_host.contracts import AdapterError, AdapterTimeout, Invocation, Outcome, RecoveryRequired, RunnerError
from mas_host.execution_policy import RetryPolicy, TimingPolicy, clock_status, transition_clock
from mas_host.journal import inspect_run
from mas_host.provider_outcomes import completed_unusable, not_sent
from mas_host.runner import Runner
import test_runner as runner_tests


class ExecutionClockTests(unittest.TestCase):
    def test_pause_excludes_active_time_but_business_deadline_keeps_running(self):
        policy = TimingPolicy(business_seconds=100, active_seconds=10, queue_seconds=5, pause_seconds=30)
        clock = policy.start(1000)
        clock = transition_clock(clock, 3000, "running", active_branches=2)
        clock = transition_clock(clock, 7000, "paused")
        self.assertEqual(clock_status(clock, 20_000)["active_ms"], 4000)
        self.assertEqual(clock_status(clock, 37_000)["expired"], ["pause_deadline"])
        clock = transition_clock(clock, 30_000, "running", active_branches=1)
        self.assertEqual(clock_status(clock, 35_000)["active_ms"], 9000)
        self.assertEqual(clock_status(clock, 36_000)["expired"], ["active_budget"])
        self.assertIn("business_deadline", clock_status(clock, 101_000)["expired"])

    def test_restart_cannot_renew_queue_age_or_active_budget(self):
        policy = TimingPolicy(business_seconds=100, active_seconds=10, queue_seconds=5, pause_seconds=30)
        clock = transition_clock(policy.start(1000), 3000, "queued")
        self.assertEqual(clock["phase_started_ms"], 1000)
        self.assertEqual(clock_status(clock, 6000)["expired"], ["queue_deadline"])
        with self.assertRaises(RunnerError):
            transition_clock(clock, 6000, "running", active_branches=1)
        clock = transition_clock(policy.start(1000), 2000, "running", active_branches=3)
        self.assertEqual(clock_status(clock, 12_000)["active_ms"], 10_000)
        with self.assertRaises(RunnerError):
            transition_clock(clock, 1500, "paused")

    def test_closed_policy_and_clock_validation(self):
        for changes in ({"max_attempts": 9}, {"max_attempts": 2}, {"categories": ["timeout"]},
                        {"backoff_ms": -1}, {"max_attempts": True}):
            with self.subTest(changes=changes), self.assertRaises(RunnerError):
                RetryPolicy(**changes)
        clock = TimingPolicy(100, 10, 5, 30).start(1000)
        for changed in ({"active_branches": 1}, {"detail": "unbounded"}, {"active_ms": -1}):
            with self.subTest(changed=changed), self.assertRaises(RunnerError):
                clock_status(clock | changed, 1000)
        terminal = transition_clock(clock, 2000, "terminal")
        self.assertEqual(clock_status(terminal, 999_999), clock_status(terminal, 2000))
        with self.assertRaises(RunnerError):
            transition_clock(terminal, 3000, "running", active_branches=1)


class RetryIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway,
                       "workflow_version": "retry/1"}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.policy = RetryPolicy(max_attempts=3, categories=("operation_busy",), backoff_ms=0)

    def adapters(self, marker, failure, *, succeed_at=None):
        def effect():
            count = len(marker.read_text().splitlines()) if marker.exists() else 0
            with marker.open("a") as stream:
                stream.write("attempt\n")
            if succeed_at is not None and count + 1 >= succeed_at:
                return Invocation({"attempts": count + 1}, {"fixture": True})
            raise failure()
        def handle(context, _request):
            return Outcome(context.invoke(effect, input={"stable": True}, reservation_microusd=37))
        return {actor: runner_tests.FunctionAdapter(handle) for actor in ("a", "b", "c")}

    def test_transient_retry_has_distinct_metered_attempts_and_replays_without_effects(self):
        for supervised in (False, True):
            marker = self.root / str(supervised)
            adapters = self.adapters(marker, lambda: not_sent("custom", "operation_busy"), succeed_at=3)
            with Runner(runner_tests.topology(), adapters, self.root, supervised=supervised,
                        retry_policy=self.policy, **self.options) as runner:
                self.assertEqual(runner.request("a", "b", "retry", {}).output, {"attempts": 3})
                run_dir = runner.run_dir
                calls = inspect_run(run_dir)["invocations"]
                self.assertEqual([item["state"] for item in calls], ["failed_known", "failed_known", "completed"])
                self.assertEqual(len({item["call_id"] for item in calls}), 3)
                self.assertEqual(calls[-1]["attempt"]["number"], 3)
                self.assertEqual(calls[-1]["attempt"]["root_call_id"], calls[0]["call_id"])
                self.assertEqual(sum(item["reservation_microusd"] for item in calls), 111)
                self.assertEqual(runner.status()["usage"]["calls"], 3)
            with Runner.resume(run_dir, adapters, supervised=supervised, retry_policy=self.policy,
                               **self.options) as runner:
                self.assertEqual(runner.request("a", "b", "retry", {}).output, {"attempts": 3})
            self.assertEqual(len(marker.read_text().splitlines()), 3)

    def test_policy_attempt_limit_is_enforced(self):
        for supervised in (False, True):
            marker = self.root / str(supervised)
            adapters = self.adapters(marker, lambda: not_sent("custom", "operation_busy"))
            with Runner(runner_tests.topology(), adapters, self.root, supervised=supervised,
                        retry_policy=self.policy, **self.options) as runner:
                with self.assertRaises(AdapterError):
                    runner.request("a", "b", "retry", {})
                self.assertEqual(runner.status()["usage"]["calls"], 3)
                self.assertFalse(inspect_run(runner.run_dir)["recovery_required"])
            self.assertEqual(len(marker.read_text().splitlines()), 3)

    def test_completed_unknown_and_unconfigured_categories_are_never_retried(self):
        failures = [lambda: completed_unusable("custom", "invalid_output", {}),
                    lambda: TimeoutError("unknown effect"),
                    lambda: not_sent("custom", "invalid_request")]
        for supervised in (False, True):
            for index, failure in enumerate(failures):
                marker = self.root / f"{supervised}-{index}"
                adapters = self.adapters(marker, failure)
                with Runner(runner_tests.topology(), adapters, self.root, supervised=supervised,
                            retry_policy=self.policy, **self.options) as runner:
                    with self.assertRaises((AdapterError, RecoveryRequired)):
                        runner.request("a", "b", "retry", {})
                    self.assertEqual(runner.status()["usage"]["calls"], 1)
                self.assertEqual(len(marker.read_text().splitlines()), 1)

    def test_restart_preserves_retry_decision_and_backoff(self):
        policy = RetryPolicy(max_attempts=2, categories=("operation_busy",), backoff_ms=500)
        for supervised in (False, True):
            marker = self.root / str(supervised)
            adapters = self.adapters(marker, lambda: not_sent("custom", "operation_busy"), succeed_at=2)
            with Runner(runner_tests.topology(), adapters, self.root, supervised=supervised,
                        execution_deadline=time.monotonic() + 0.2, retry_policy=policy, **self.options) as runner:
                with self.assertRaises(AdapterTimeout):
                    runner.request("a", "b", "retry", {})
                run_dir = runner.run_dir
                before = inspect_run(run_dir)["invocations"][0]["retry_decision"]
                self.assertEqual(len(marker.read_text().splitlines()), 1)
            with Runner.resume(run_dir, adapters, supervised=supervised, retry_policy=policy,
                               **self.options) as runner:
                self.assertEqual(runner.request("a", "b", "retry", {}).output, {"attempts": 2})
                after = inspect_run(run_dir)["invocations"][0]["retry_decision"]
                self.assertEqual(before, after)
                self.assertGreaterEqual(int(time.time() * 1000), before["eligible_at_ms"])
            self.assertEqual(len(marker.read_text().splitlines()), 2)

    def test_retry_does_not_bypass_rust_call_budget(self):
        for supervised in (False, True):
            marker = self.root / str(supervised)
            adapters = self.adapters(marker, lambda: not_sent("custom", "operation_busy"))
            with Runner(runner_tests.topology(max_calls=1), adapters, self.root, supervised=supervised,
                        retry_policy=self.policy, **self.options) as runner:
                with self.assertRaises(AdapterError):
                    runner.request("a", "b", "retry", {})
                self.assertEqual(runner.status()["usage"]["calls"], 1)
            self.assertEqual(len(marker.read_text().splitlines()), 1)

    def test_proven_no_dispatch_retry_after_is_durable_but_cannot_extend_deadline(self):
        marker = self.root / "retry-after"
        adapters = self.adapters(marker, lambda: not_sent("custom", "operation_busy", retry_after_seconds=7))
        before = int(time.time() * 1000)
        with Runner(runner_tests.topology(), adapters, self.root, supervised=True,
                    execution_deadline=time.monotonic() + 0.2, retry_policy=self.policy, **self.options) as runner:
            with self.assertRaises(AdapterTimeout):
                runner.request("a", "b", "retry", {})
            plan = inspect_run(runner.run_dir)["invocations"][0]["retry_decision"]
            self.assertEqual(plan["retry_after_seconds"], 7)
            self.assertGreaterEqual(plan["eligible_at_ms"], before + 7000)
        self.assertEqual(marker.read_text().splitlines(), ["attempt"])

    def test_workflow_policy_reaches_runner_through_durable_service(self):
        from mas_host.task_service import DurableTaskService, Workflow
        marker = self.root / "service"
        workflow = Workflow("retry-service/1", runner_tests.topology(),
            self.adapters(marker, lambda: not_sent("custom", "operation_busy"), succeed_at=3),
            "a", "b", "retry", retry_policy=self.policy)
        service = DurableTaskService(self.root, {"retry": workflow}, {"alice": {"retry"}}, supervised=False,
            mas_bin=self.options["mas_bin"], gateway_bin=self.options["gateway_bin"])
        task = service.submit("alice", "same", "retry", {})
        self.assertEqual(service.run_once()["state"], "completed")
        self.assertEqual(service.result("alice", task["task_id"]), {"attempts": 3})


if __name__ == "__main__":
    unittest.main()
