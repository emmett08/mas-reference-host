"""Independent MCP review regressions for continuation and admission boundaries."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.service_contracts import ServiceError
from mas_host.operations import DeploymentLedger
from mas_host.bridges.mcp import MCPBridge, metadata
from mas_host.bridges.mcp_client import MCPToolAdapter
from mas_host.bridges.remote import RemoteSnapshot, RemoteTaskService, RemoteTaskStore
from mas_host.contracts import TaskRequest
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from test_runner import FunctionAdapter, topology


class MCPContinuationReviewTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.state = Path(temporary.name) / "state"
        workflow = Workflow("review/1", topology(),
            {actor: FunctionAdapter(lambda _c, request: Outcome(request.input)) for actor in ("a", "b", "c")},
            "a", "b", "echo", continue_input=lambda _previous, supplied: supplied)
        class InterruptEveryRevision:
            def execute(self, *args, **kwargs):
                return ExecutionResult("input_required", {"input_requests": {"answer": {
                    "method": "elicitation/create", "params": {"mode": "form", "message": "Choose",
                    "requestedSchema": {"type": "object", "properties": {"n": {"type": "integer"}}}}}}})
        self.service = DurableTaskService(self.state, {"review": workflow}, {"alice": {"review"}},
                                          executor=InterruptEveryRevision())
        self.bridge = MCPBridge(self.service)

    def rpc(self, method, params):
        return self.bridge.handle("alice", {"jsonrpc": "2.0", "id": 1, "method": method,
                    "params": {**params, "_meta": metadata(elicitation=True)}})

    def submit_and_interrupt(self, key):
        receipt = self.service.submit("alice", key, "review", {})
        self.service.run_once()
        return receipt["task_id"]

    def response_for(self, task_id):
        observation = self.rpc("tasks/get", {"taskId": task_id})["result"]
        request_key = next(iter(observation["inputRequests"]))
        return {"taskId": task_id, "inputResponses": {
            request_key: {"action": "accept", "content": {"n": 1}}}}

    def test_identical_answers_on_distinct_tasks_are_independent(self):
        first = self.submit_and_interrupt("first")
        second = self.submit_and_interrupt("second")
        self.assertIn("result", self.rpc("tasks/update", self.response_for(first)))
        self.assertIn("result", self.rpc("tasks/update", self.response_for(second)))
        self.assertEqual(self.service.inspect("alice", first)["revision"], 1)
        self.assertEqual(self.service.inspect("alice", second)["revision"], 1)

    def test_old_answer_cannot_continue_a_later_revision_reusing_raw_input_id(self):
        task_id = self.submit_and_interrupt("first")
        first_answer = self.response_for(task_id)
        self.assertIn("result", self.rpc("tasks/update", first_answer))
        self.service.run_once()
        self.assertEqual(self.service.inspect("alice", task_id)["revision"], 1)
        # A delayed retry of the old answer must leave the new interruption open.
        self.assertIn("result", self.rpc("tasks/update", first_answer))
        self.assertEqual(self.service.inspect("alice", task_id)["state"], "input_required")
        second_answer = self.response_for(task_id)
        self.assertNotEqual(set(first_answer["inputResponses"]), set(second_answer["inputResponses"]))
        self.assertIn("result", self.rpc("tasks/update", second_answer))
        self.assertEqual(self.service.inspect("alice", task_id)["revision"], 2)


class MCPAdmissionReviewTests(unittest.TestCase):
    def test_a_context_that_calls_the_effect_without_admission_cannot_dispatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            calls = []
            class Peer:
                def descriptor(self):
                    return {"protocol": "mcp", "endpoint": "https://peer.example/mcp"}
                def submit(self, payload):
                    calls.append(payload)
                    return RemoteSnapshot("remote", "running")
            class UnadmittedContext:
                actor = "b"
                invocation_id = "call-without-admission"
                def invoke(self, effect, **kwargs):
                    return effect().output
            state = Path(temporary) / "state"
            DeploymentLedger(state)
            service = RemoteTaskService(RemoteTaskStore(state), Peer(), namespace="trusted-test")
            adapter = MCPToolAdapter(service, state / "run", reservation_microusd=0)
            request = TaskRequest("a", "b", "mcp.call", {"key": "one", "arguments": {}}, "task", "message")
            with self.assertRaises(ServiceError) as context:
                adapter.handle(UnadmittedContext(), request)
            self.assertEqual(context.exception.code, "admission_required")
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
