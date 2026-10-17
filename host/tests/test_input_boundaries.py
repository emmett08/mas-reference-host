"""Admission and result boundaries through the real Rust codec and gateway."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from mas_host.codec import integer_json
from mas_host.contracts import Invocation, Outcome
from mas_host.journal import inspect_run
from mas_host.protocol_client import ProtocolClient
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


class InputBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway
        cls.client = ProtocolClient(cls.mas, cls.gateway)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def workflow(self, callback=None):
        callback = callback or (lambda _context, _request: Outcome({"accepted": True}))
        return Workflow("boundary/1", topology(),
            {actor: FunctionAdapter(callback) for actor in ("a", "b", "c")}, "a", "b", "echo")

    def service(self, suffix="state", *, workflow=None, supervised=False):
        return DurableTaskService(self.root / suffix, {"echo": workflow or self.workflow()},
            {"alice": {"echo"}}, mas_bin=self.mas, gateway_bin=self.gateway, supervised=supervised)

    def frame_bytes(self, text, *, kind="Request"):
        # Independent measurement: ask Rust for the wire, then measure the
        # actual JSON command format. Generated identities have these widths.
        payload = ({"task_id": "task-" + "a" * 32, "operation": "echo", "input": {"text": text}}
                   if kind == "Request" else
                   {"task_id": "task-" + "a" * 32, "reply_to": "message-" + "b" * 32,
                    "status": "completed", "output": {"text": text}})
        envelope = {"message_id": "message-" + "c" * 32,
                    "run_id": "run-service-" + "d" * 32 + "-0",
                    "sender": "a", "recipient": "b", "version": "1", "kind": kind,
                    "payload": payload}
        wire = self.client.command([self.mas, "encode", "-"],
                                  json.dumps(envelope, ensure_ascii=False))["text"]
        frame = json.dumps({"token": "0" * 64, "command": "send", "wire": wire},
                           ensure_ascii=False, separators=(",", ":"))
        return len(frame.encode("utf-8"))

    def boundary_text(self, character="x", *, kind="Request"):
        remaining = 65_536 - self.frame_bytes("", kind=kind)
        cost = self.frame_bytes(character, kind=kind) - self.frame_bytes("", kind=kind)
        return character * (remaining // cost) + "x" * (remaining % cost)

    def durable_snapshot(self, service):
        with service.store.transaction(readonly=True) as db:
            records = {name: [tuple(row) for row in db.execute("SELECT * FROM " + name)]
                       for name in ("tasks", "executions", "requests", "events")}
        return records, service.operations.health()["usage"], sorted(service.state_dir.glob("run-*"))

    def assert_rejected_without_admission(self, service, payload, key="oversize"):
        before = self.durable_snapshot(service)
        with self.assertRaises(ServiceError) as raised:
            service.submit("alice", key, "echo", payload)
        self.assertEqual(raised.exception.code, "input_too_large")
        self.assertEqual(self.durable_snapshot(service), before)

    def test_application_limit_and_one_byte_over_are_rejected_before_admission(self):
        service = self.service()
        for amount in (65_525, 65_526):
            payload = {"text": "x" * amount}
            self.assertEqual(len(json.dumps(payload, separators=(",", ":")).encode()), amount + 11)
            self.assert_rejected_without_admission(service, payload, str(amount))

    def test_metadata_and_non_rust_executor_retain_the_application_byte_limit(self):
        payload = {"text": "x" * 65_525}
        self.assertEqual(len(integer_json(payload).encode()), 65_536)
        service = self.service()
        task = service.submit("alice", "metadata", "echo", {}, metadata=payload)
        self.assertEqual(service.run_once()["state"], "completed")
        self.assertEqual(service.inspect("alice", task["task_id"])["metadata"], payload)

        class ApplicationExecutor:
            def execute(self, workflow, execution, **options):
                return ExecutionResult("completed", execution["input"])

        other = DurableTaskService(self.root / "application", {"echo": self.workflow()},
            {"alice": {"echo"}}, executor=ApplicationExecutor())
        task = other.submit("alice", "full-application-input", "echo", payload)
        self.assertEqual(other.run_once()["state"], "completed")
        self.assertEqual(other.result("alice", task["task_id"]), payload)

    def test_exact_gateway_limit_multibyte_and_escaping_execute_without_rounding(self):
        for index, character in enumerate(("x", "🧠", '"', "\\", "\n")):
            with self.subTest(character=character):
                text = self.boundary_text(character)
                self.assertEqual(self.frame_bytes(text), 65_536)
                seen = []
                def accept(_context, request):
                    seen.append(request.input)
                    return Outcome({"accepted": True})
                service = self.service(str(index), workflow=self.workflow(accept))
                payload = {"text": text}
                self.assertLess(len(integer_json(payload).encode()), 65_536)
                self.assert_rejected_without_admission(service, {"text": text + "x"})
                task = service.submit("alice", "exact", "echo", payload)
                self.assertEqual(service.run_once()["state"], "completed")
                self.assertEqual(seen, [payload])
                self.assertEqual(service.result("alice", task["task_id"]), {"accepted": True})

    def test_oversized_continuation_preserves_paused_revision_and_allowances(self):
        workflow = replace(self.workflow(), classify=lambda _outcome: "input_required",
                           continue_input=lambda _previous, supplied: supplied)
        service = self.service(workflow=workflow)
        task = service.submit("alice", "initial", "echo", {})
        self.assertEqual(service.run_once()["state"], "input_required")
        before = self.durable_snapshot(service)
        with self.assertRaises(ServiceError) as raised:
            service.update("alice", task["task_id"], "oversize", {"text": "x" * 65_525})
        self.assertEqual(raised.exception.code, "input_too_large")
        self.assertEqual(self.durable_snapshot(service), before)

    def test_exact_result_limit_and_oversize_preserve_known_invocation_output(self):
        text = self.boundary_text(kind="Result")
        self.assertEqual(self.frame_bytes(text, kind="Result"), 65_536)
        for extra in (0, 1):
            with self.subTest(extra=extra):
                output = {"text": text + "x" * extra}
                effects = self.root / ("effect-" + str(extra))
                def execute(context, _request):
                    def effect():
                        with effects.open("a") as stream:
                            stream.write("one\n")
                        return Invocation(output, {"known": True})
                    return Outcome(context.invoke(effect, reservation_microusd=0))
                service = self.service("result-" + str(extra), workflow=self.workflow(execute), supervised=True)
                task = service.submit("alice", "result", "echo", {})
                finished = service.run_once()
                self.assertEqual(finished["state"], "completed" if extra == 0 else "failed")
                if extra == 0:
                    self.assertEqual(service.result("alice", task["task_id"]), output)
                self.assertEqual(effects.read_text(), "one\n")
                runs = list(service.state_dir.glob("run-service-*"))
                self.assertEqual(len(runs), 1)
                journal = inspect_run(runs[0])
                self.assertFalse(journal["recovery_required"])
                self.assertEqual(journal["invocations"][0]["state"], "completed")
                self.assertEqual(journal["invocations"][0]["output"], output)
                self.assertIsNone(service.run_once())
                self.assertEqual(effects.read_text(), "one\n")


if __name__ == "__main__":
    unittest.main()
