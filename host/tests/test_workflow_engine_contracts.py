"""Independent ownership, identity and recovery-boundary contract checks."""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

from mas_host import Invocation, Outcome, Runner, RunnerError
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.service_contracts import ServiceError
from mas_host.task_service import Workflow
from mas_host.task_store import canonical
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


def specification():
    return {"format": "mas-workflow/1", "name": "example", "version": "1",
            "limits": {"max_steps": 20, "max_parallel": 2, "wall_seconds": 60,
                       "max_calls": 5, "max_reserved_microusd": 1_000_000},
            "root": {"kind": "task", "workflow": "echo"}}


def workflow(**changes):
    adapter = FunctionAdapter(lambda _c, request: Outcome(request.input))
    base = Workflow("1", topology(agents=("a", "b")), {"a": adapter, "b": adapter}, "a", "b", "echo")
    return replace(base, **changes)


class RecordedEffect:
    """Independent fixture receipt distinguishes execution from replay."""
    def __init__(self, receipt, output, *, lose_reply=False):
        self.receipt, self.output, self.lose_reply = receipt, output, lose_reply

    def handle(self, context, request):
        def invoke():
            with self.receipt.open("ab", buffering=0) as stream:
                stream.write((json.dumps({"call_id": context.invocation_id}) + "\n").encode())
                os.fsync(stream.fileno())
            if self.lose_reply:
                raise RuntimeError("fixture response lost after effect")
            return Invocation(self.output, {"receipt": self.receipt.name})
        return Outcome(context.invoke(invoke, input={"fixture": self.receipt.name}, reservation_microusd=3))


class WorkflowDescriptorTests(unittest.TestCase):
    def test_omitted_contract_descriptor_preserves_verified_identity(self):
        registered = workflow()
        legacy = {"version": registered.version, "topology": registered.topology,
            "sender": registered.sender, "recipient": registered.recipient, "operation": registered.operation,
            "max_depth": registered.max_depth, "continuation": False, "classifier": False, "adapter_factory": False}
        legacy["implementation_digest"] = registered.verified_implementation.digest
        self.assertEqual(registered.identity, hashlib.sha256(canonical(legacy).encode()).hexdigest())
        self.assertEqual(replace(registered, contract_descriptor=None).identity, registered.identity)
        self.assertNotEqual(replace(registered, contract_descriptor={}).identity, registered.identity)

    def test_bound_contract_is_copied_and_later_drift_changes_fingerprint(self):
        source = {"operations": [{"name": "echo", "version": "1"}], "provider": {"model": "local"}}
        registered = workflow(contract_descriptor=source)
        identity = registered.identity
        source["operations"][0]["version"] = "2"
        self.assertEqual(registered.identity, identity)
        registered.contract_descriptor["operations"][0]["version"] = "3"
        self.assertNotEqual(registered.identity, identity)

    def test_descriptor_rejects_non_protocol_values_and_excessive_size(self):
        for descriptor in ([], {"fraction": 0.5}, {"payload": "x" * 65536}):
            with self.subTest(descriptor=type(descriptor)), self.assertRaises(ServiceError):
                workflow(contract_descriptor=descriptor)


class WorkflowEngineContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def engine(self, registered=None, grants=None):
        return WorkflowEngine(self.root / "state", {"example": WorkflowPlan(specification())},
            {"echo": registered or workflow()}, grants or {"alice": {"example"}, "bob": {"example"}},
            mas_bin=runner_tests.RunnerIntegrationTests.mas, gateway_bin=runner_tests.RunnerIntegrationTests.gateway)

    def test_request_keys_are_principal_scoped_and_operations_cannot_cross_owners(self):
        engine = self.engine()
        first = engine.submit("alice", "same-key", "example", {"private": "alice"})
        second = engine.submit("bob", "same-key", "example", {"private": "bob"})
        self.assertNotEqual(first["instance_id"], second["instance_id"])
        for operation in (engine.inspect, engine.cancel, engine.recover, engine.run):
            with self.subTest(operation=operation.__name__), self.assertRaises(ServiceError):
                operation("bob", first["instance_id"])
        self.assertEqual(engine.inspect("alice", first["instance_id"])["state"], "queued")

    def test_unowned_or_path_like_instance_never_creates_lock_file(self):
        engine = self.engine()
        unexpected = self.root / "outside.lock"
        malicious = "/../outside"
        for operation in (lambda: engine.recover("alice", malicious),
                          lambda: engine.continue_task("alice", malicious, "root", "key", {})):
            with self.assertRaises(ServiceError):
                operation()
            self.assertFalse(unexpected.exists(), "untrusted instance escaped the deployment directory")
        not_owned = "flow-" + "0" * 32
        with self.assertRaises(ServiceError):
            engine.recover("alice", not_owned)
        self.assertFalse((engine.state_dir / ("." + not_owned + ".lock")).exists())

    def test_idempotence_does_not_conflate_json_booleans_with_integers(self):
        engine = self.engine()
        engine.submit("alice", "request", "example", {"nested": [True]})
        with self.assertRaises(ServiceError) as raised:
            engine.submit("alice", "request", "example", {"nested": [1]})
        self.assertEqual(raised.exception.code, "request_conflict")

    def test_reordered_json_object_is_same_submission(self):
        engine = self.engine()
        first = engine.submit("alice", "request", "example", {"a": 1, "b": 2})
        self.assertEqual(engine.submit("alice", "request", "example", {"b": 2, "a": 1}), first)

    def test_engine_copies_source_registration_and_detects_internal_drift(self):
        registered = workflow(contract_descriptor={"operations": {"version": "1"}})
        engine = self.engine(registered)
        first = engine.submit("alice", "request", "example", {})
        registered.contract_descriptor["operations"]["version"] = "2"
        self.assertEqual(engine.inspect("alice", first["instance_id"])["identity"], first["identity"])
        engine.workflows["echo"].contract_descriptor["operations"]["version"] = "3"
        self.assertEqual(engine.inspect("alice", first["instance_id"])["identity"], first["identity"])
        self.assertEqual(engine.run("alice", first["instance_id"])["state"], "blocked")

    def test_continuation_is_idempotent_under_reordered_object_keys(self):
        def classify(outcome):
            return "completed" if outcome.output.get("done") else "input_required"
        registered = workflow(classify=classify, continue_input=lambda previous, supplied: previous | supplied)
        engine = self.engine(registered)
        first = engine.submit("alice", "request", "example", {})
        instance = first["instance_id"]
        self.assertEqual(engine.run("alice", instance)["state"], "input_required")
        resumed = engine.continue_task("alice", instance, "root", "decision", {"done": True, "value": 7})
        self.assertEqual(engine.continue_task("alice", instance, "root", "decision", {"value": 7, "done": True}), resumed)
        self.assertEqual(engine.run("alice", instance)["state"], "completed")

    def test_contract_change_is_rejected_on_restart_before_leaf_dispatch(self):
        original = workflow(contract_descriptor={"schema_version": "1"})
        engine = self.engine(original)
        first = engine.submit("alice", "request", "example", {})
        restarted = self.engine(replace(original, contract_descriptor={"schema_version": "2"}))
        self.assertEqual(restarted.run("alice", first["instance_id"])["state"], "blocked")
        self.assertFalse(any(engine.state_dir.glob("run-flow-*")))


    def parallel_engine(self, workflows):
        source = specification()
        source["root"] = {"kind": "parallel", "branches": {
            name: {"kind": "task", "workflow": name} for name in workflows}}
        return WorkflowEngine(self.root / "state", {"example": WorkflowPlan(source)}, workflows,
            {"alice": {"example"}}, mas_bin=runner_tests.RunnerIntegrationTests.mas,
            gateway_bin=runner_tests.RunnerIntegrationTests.gateway)

    def effect_workflow(self, adapter):
        return workflow(adapters={"a": FunctionAdapter(lambda _c, request: Outcome(request.input)), "b": adapter})

    def test_oversized_join_fails_deterministically_without_repeating_children(self):
        receipts = [self.root / "first-effect.jsonl", self.root / "second-effect.jsonl"]
        workflows = {str(index): self.effect_workflow(RecordedEffect(receipt, {"data": "x" * 34000}))
                     for index, receipt in enumerate(receipts)}
        engine = self.parallel_engine(workflows)
        instance = engine.submit("alice", "large", "example", {})["instance_id"]
        first = engine.run("alice", instance)
        self.assertEqual(first["state"], "failed", first)
        root = next(node for node in first["nodes"] if node["path"] == "root")
        self.assertEqual(root["error"], {"code": "composed_output_not_encodable"})
        self.assertEqual([attempt["state"] for attempt in first["attempts"]], ["completed", "completed"])
        self.assertEqual(first["usage"]["calls"], 2)
        self.assertEqual(engine.run("alice", instance), first)
        self.assertEqual([len(path.read_text().splitlines()) for path in receipts], [1, 1])

    def test_failed_parallel_branch_and_unknown_sibling_recover_without_dangling_work(self):
        def fail(_context, _request):
            raise RuntimeError("fixture deterministic failure")
        receipt = self.root / "uncertain-effect.jsonl"
        engine = self.parallel_engine({
            "a-failed": self.effect_workflow(FunctionAdapter(fail)),
            "b-unknown": self.effect_workflow(RecordedEffect(receipt, {"value": 42}, lose_reply=True))})
        instance = engine.submit("alice", "recover", "example", {})["instance_id"]
        first = engine.run("alice", instance)
        self.assertEqual(first["state"], "unknown", first)
        uncertain = next(attempt for attempt in first["attempts"] if attempt["state"] == "unknown")
        run_dir = engine.state_dir / uncertain["run_directory"]
        calls = inspect_run(run_dir)["invocations"]
        self.assertEqual(len(calls), 1)
        resolve_invocation(run_dir, calls[0]["call_id"], decision="completed", output={"value": 42},
                           evidence={"receipt": receipt.name})
        engine.recover("alice", instance)
        final = engine.run("alice", instance)
        self.assertEqual(final["state"], "failed", final)
        self.assertEqual({attempt["state"] for attempt in final["attempts"]}, {"failed", "completed"})
        self.assertFalse(any(node["state"] in {"queued", "running", "waiting", "unknown"} for node in final["nodes"]))
        self.assertEqual([attempt["run_directory"] for attempt in first["attempts"]],
                         [attempt["run_directory"] for attempt in final["attempts"]])
        self.assertEqual(len(receipt.read_text().splitlines()), 1)
        self.assertEqual(final["usage"]["calls"], 1)


class WorkflowExecutionDeadlineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def runner(self, deadline, *, seconds=30, directory="state", supervised=False, handler=None):
        adapter = FunctionAdapter(handler or (lambda context, _request:
            Outcome({"remaining_ms": int(context.remaining_seconds * 1000)})))
        value = Runner(topology(agents=("a", "b"), wall_seconds=seconds), {"a": adapter, "b": adapter},
            self.root / directory, execution_deadline=deadline, supervised=supervised,
            mas_bin=runner_tests.RunnerIntegrationTests.mas, gateway_bin=runner_tests.RunnerIntegrationTests.gateway)
        self.addCleanup(value.close)
        return value

    def test_invalid_execution_deadline_is_rejected_before_run_creation(self):
        for index, deadline in enumerate((True, "123", {}, float("nan"), float("inf"), -float("inf"), 10 ** 1000)):
            directory = "invalid-" + str(index)
            with self.subTest(kind=type(deadline).__name__), self.assertRaises(RunnerError):
                self.runner(deadline, directory=directory)
            self.assertFalse((self.root / directory).exists())

    def test_enclosing_deadline_tightens_context_remaining_time(self):
        runner = self.runner(time.monotonic() + 2)
        result = runner.request("a", "b", "remaining", {})
        self.assertGreater(result.output["remaining_ms"], 0)
        self.assertLessEqual(result.output["remaining_ms"], 2000)
        self.assertGreater(runner.status()["deadline"] - time.time(), 20)

    def test_enclosing_deadline_cannot_extend_rust_deadline(self):
        runner = self.runner(time.monotonic() + 600, seconds=5)
        result = runner.request("a", "b", "remaining", {})
        self.assertGreater(result.output["remaining_ms"], 0)
        self.assertLessEqual(result.output["remaining_ms"], 5000)

    def test_expired_enclosing_deadline_never_starts_supervised_adapter(self):
        receipt = self.root / "unexpected-adapter"
        def record(_context, request):
            receipt.write_text("executed")
            return Outcome(request.input)
        runner = self.runner(time.monotonic() - 1, supervised=True, handler=record)
        with self.assertRaises(RunnerError):
            runner.request("a", "b", "expired", {})
        self.assertFalse(receipt.exists())


if __name__ == "__main__":
    unittest.main()
