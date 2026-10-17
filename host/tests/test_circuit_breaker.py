"""Shared circuit observations never authorise an ambiguous effect retry."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from mas_host.circuit_breaker import CircuitBreaker, CircuitPolicy
from mas_host.contracts import AdapterError, Invocation, Outcome, RecoveryRequired, RunnerError
from mas_host.journal import inspect_run
from mas_host.operations import DeploymentLedger
from mas_host.provider_contract import ProviderError
from mas_host.provider_outcomes import unknown_diagnostic
from mas_host.runner import Runner
import test_runner as runner_tests


class CircuitStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.operations = DeploymentLedger(Path(self.temp.name))
        self.policy = CircuitPolicy("provider:fixture", 1, 1, 1)
        self.breaker = CircuitBreaker(self.operations, self.policy)

    def test_stale_success_cannot_close_new_open_generation_and_probe_is_exclusive(self):
        self.assertTrue(self.breaker.admit("failure", now_ms=1000))
        self.assertTrue(self.breaker.admit("late", now_ms=1000))
        self.breaker.observe("failure", "provider_failure", now_ms=1100)
        self.breaker.observe("late", "success", now_ms=1200)
        self.assertFalse(self.breaker.admit("too-soon", now_ms=2099))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda call: self.breaker.admit(call, now_ms=2100), ["probe-a", "probe-b"]))
        self.assertEqual(sorted(results), [False, True])
        selected = self.breaker.status()["probe_call"]
        self.breaker.observe(selected, "success", now_ms=2200)
        self.assertEqual(self.breaker.status()["state"], "closed")
        self.assertTrue(self.breaker.admit("fresh", now_ms=2200))

    def test_crashed_probe_reopens_cooldown_without_reusing_its_attempt(self):
        self.breaker.admit("first", now_ms=1000)
        self.breaker.observe("first", "provider_failure", now_ms=1000)
        self.assertTrue(self.breaker.admit("probe", now_ms=2000))
        restarted = CircuitBreaker(self.operations, self.policy)
        self.assertFalse(restarted.admit("after-crash", now_ms=3000))
        self.assertFalse(restarted.admit("probe", now_ms=3000))
        self.assertFalse(restarted.admit("after-crash", now_ms=3999))
        self.assertTrue(restarted.admit("after-crash", now_ms=4000))
        restarted.observe("probe", "success", now_ms=4100)
        self.assertEqual(restarted.status()["probe_call"], "after-crash")

    def test_explicit_policy_is_pinned_and_unknown_categories_cannot_count(self):
        self.breaker.admit("first", now_ms=1000)
        self.breaker.observe("first", "aborted", now_ms=1000)
        self.assertEqual(self.breaker.status()["failures"], 0)
        self.breaker.observe("first", "provider_failure", now_ms=1100)
        self.assertEqual(self.breaker.status()["failures"], 0)
        changed = CircuitBreaker(self.operations, CircuitPolicy("provider:fixture", 2, 1, 1))
        with self.assertRaisesRegex(RunnerError, "policy conflicts"):
            changed.admit("other", now_ms=1200)


class CircuitRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway,
                       "workflow_version": "circuit/1"}

    def test_provider_failure_blocks_fresh_run_without_refunding_or_repeating_unknown(self):
        for supervised in (False, True):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                marker = root / "calls"
                def invoke():
                    with marker.open("a") as stream:
                        stream.write("attempt\n")
                    error = TimeoutError("SECRET")
                    raise ProviderError("safe failure", diagnostic=unknown_diagnostic("custom", "send", error))
                def handle(context, _request):
                    return Outcome(context.invoke(invoke, reservation_microusd=37))
                adapters = {actor: runner_tests.FunctionAdapter(handle) for actor in ("a", "b", "c")}
                policy = {"b": CircuitPolicy("provider:fixture", 1, 60, 60)}
                with Runner(runner_tests.topology(), adapters, root, supervised=supervised,
                            circuit_breakers=policy, **self.options) as first:
                    with self.assertRaises(RecoveryRequired):
                        first.request("a", "b", "call", {})
                    original = first.run_dir
                    self.assertEqual(first.status()["usage"]["calls"], 1)
                with Runner(runner_tests.topology(), adapters, root, supervised=supervised,
                            circuit_breakers=policy, **self.options) as second:
                    with self.assertRaises(AdapterError):
                        second.request("a", "b", "call", {})
                    self.assertEqual(second.status()["usage"]["calls"], 0)
                    observed = inspect_run(second.run_dir)["invocations"][0]
                    self.assertEqual(observed["state"], "not_executed")
                    self.assertEqual(observed["evidence"]["reason"], "provider_circuit_open")
                self.assertTrue(inspect_run(original)["recovery_required"])
                self.assertEqual(marker.read_text().splitlines(), ["attempt"])

    def test_unrelated_workflow_failure_does_not_trip_provider_circuit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, calls = Path(temporary), []
            def invoke():
                calls.append(True)
                raise RuntimeError("unrelated application bug")
            def handle(context, _request):
                return Outcome(context.invoke(invoke))
            adapters = {actor: runner_tests.FunctionAdapter(handle) for actor in ("a", "b", "c")}
            for _ in range(2):
                with Runner(runner_tests.topology(), adapters, root, supervised=False,
                    circuit_breakers={"b": CircuitPolicy("provider:fixture", 1)}, **self.options) as runner:
                    with self.assertRaises(RecoveryRequired):
                        runner.request("a", "b", "call", {})
            self.assertEqual(calls, [True, True])


if __name__ == "__main__":
    unittest.main()
