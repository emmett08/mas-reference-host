"""Execute pinned SDK activity contexts and a real checkpointed StateGraph."""
from __future__ import annotations

from dataclasses import replace
import importlib.util
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, create_autospec
from typing import TypedDict

from mas_host.bridges.orchestrator import OrchestratorConnector
from mas_host.bridges.temporal import TemporalTarget, TemporalWorkflowAdapter, TemporalWorkflowClient, temporal_activities
from mas_host.bridges.remote import RemoteTaskStore, RemoteTaskService
from mas_host.contracts import TaskRequest
from mas_host.service_contracts import ServiceError
from mas_host.operations import OperationsError
from test_orchestrator import ServiceDouble

TEMPORAL = importlib.util.find_spec("temporalio") is not None
LANGGRAPH = importlib.util.find_spec("langgraph") is not None


@unittest.skipUnless(TEMPORAL, "optional Temporal SDK is not installed")
class TemporalSDKTests(unittest.TestCase):
    def test_real_activity_environment_retry_has_one_submission(self):
        from temporalio import activity
        from temporalio.testing import ActivityEnvironment
        environment = ActivityEnvironment()
        service = ServiceDouble("completed")
        functions = temporal_activities(OrchestratorConnector(service, "worker"))
        registered = {activity._Definition.from_callable(fn).name: fn for fn in functions}
        first = environment.run(registered["mas.execute"], {"workflow": "invoice", "input": {"x": 2}})
        environment.info = replace(environment.info, attempt=2)
        second = environment.run(registered["mas.execute"], {"workflow": "invoice", "input": {"x": 2}})
        self.assertEqual(first, second)
        self.assertEqual(service.submissions, 1)
        self.assertEqual(len(registered), 7)

    def test_real_activity_heartbeat_and_cancellation(self):
        from temporalio.testing import ActivityEnvironment
        environment = ActivityEnvironment()
        heartbeat = []
        environment.on_heartbeat = lambda *details: heartbeat.append(details)
        environment.cancel()
        service = ServiceDouble("running")
        execute = temporal_activities(OrchestratorConnector(service, "worker"))[1]
        result = environment.run(execute, {"workflow": "invoice", "input": {}})
        self.assertTrue(result["cancel_requested"])
        self.assertEqual(result["state"], "running")
        self.assertTrue(heartbeat)

    def test_real_activity_conflict_is_non_retryable(self):
        from temporalio.testing import ActivityEnvironment
        from temporalio.exceptions import ApplicationError
        environment = ActivityEnvironment()
        submit = temporal_activities(OrchestratorConnector(ServiceDouble(), "worker"))[0]
        environment.run(submit, {"workflow": "invoice", "input": {"x": 1}})
        with self.assertRaises(ApplicationError) as raised:
            environment.run(submit, {"workflow": "invoice", "input": {"x": 2}})
        self.assertTrue(raised.exception.non_retryable)

    def client(self):
        from temporalio.client import Client, WorkflowHandle, WorkflowExecutionStatus
        sdk = create_autospec(Client, instance=True)
        handle = create_autospec(WorkflowHandle, instance=True)
        run = "01234567-0123-0123-0123-0123456789ab"
        handle.first_execution_run_id = run
        description = SimpleNamespace(run_id=run, status=WorkflowExecutionStatus.RUNNING,
                                      workflow_type="Invoice", memo=AsyncMock(return_value={}),
                                      raw_info=SimpleNamespace(first_run_id=run))
        handle.describe.return_value = description
        sdk.start_workflow.return_value = handle
        sdk.get_workflow_handle.return_value = handle
        client = TemporalWorkflowClient(TemporalTarget("localhost:7233", "default", "Invoice", "tasks", "deployment-1", tls=False),
                                        client_factory=AsyncMock(return_value=sdk))
        description.id = client.workflow_id("job")
        return client, sdk, handle, description

    def test_outbound_real_sdk_signatures_and_exact_cancellation_identity(self):
        from temporalio.common import WorkflowIDReusePolicy, WorkflowIDConflictPolicy
        client, sdk, handle, description = self.client()
        initial = client.submit({"request_key": "job", "input": {"x": 7}})
        options = sdk.start_workflow.call_args.kwargs
        self.assertEqual(options["id_reuse_policy"], WorkflowIDReusePolicy.REJECT_DUPLICATE)
        self.assertEqual(options["id_conflict_policy"], WorkflowIDConflictPolicy.FAIL)
        cancelled = client.cancel(initial.remote_id)
        self.assertEqual(cancelled.state, "running")
        self.assertEqual(sdk.get_workflow_handle.call_args.kwargs["run_id"], description.run_id)
        handle.cancel.assert_awaited_once()

    def test_outbound_lost_receipt_lookup_verifies_original_binding(self):
        client, sdk, handle, description = self.client()
        payload = {"request_key": "job", "input": {}}
        sdk.start_workflow.side_effect = TimeoutError()
        with self.assertRaisesRegex(ServiceError, "temporal outcome unknown"):
            client.submit(payload)
        description.memo.return_value = {"mas_binding": client._binding(payload)}
        remote = client.lookup(payload)
        self.assertEqual(client.poll(remote).state, "running")
        self.assertEqual(sdk.start_workflow.await_count, 1)
        description.memo.return_value = {"mas_binding": "other"}
        with self.assertRaisesRegex(ServiceError, "remote identity conflict"):
            client.lookup(payload)

    def test_recovery_requires_original_run_proof_and_checks_its_binding(self):
        from temporalio.client import WorkflowHandle
        client, sdk, original, description = self.client()
        payload = {"request_key": "job", "input": {}}
        description.memo.return_value = {"mas_binding": "different-original-input"}
        successor = create_autospec(WorkflowHandle, instance=True)
        successor.describe.return_value = SimpleNamespace(
            run_id="99999999-0123-0123-0123-0123456789ab",
            raw_info=SimpleNamespace(first_run_id=description.run_id),
            memo=AsyncMock(return_value={"mas_binding": client._binding(payload)}))
        sdk.get_workflow_handle.side_effect = [successor, original]
        with self.assertRaisesRegex(ServiceError, "remote identity conflict"):
            client.lookup(payload)
        self.assertEqual(sdk.get_workflow_handle.call_args.kwargs["run_id"], description.run_id)
        successor.describe.return_value.raw_info.first_run_id = ""
        sdk.get_workflow_handle.side_effect = [successor]
        with self.assertRaisesRegex(ServiceError, "invalid temporal identity"):
            client.lookup(payload)
        sdk.start_workflow.assert_not_awaited()

    def test_continue_as_new_and_invalid_results_do_not_claim_success(self):
        from temporalio.client import WorkflowExecutionStatus
        client, sdk, handle, description = self.client()
        initial = client.submit({"request_key": "job", "input": {}})
        description.status = WorkflowExecutionStatus.CONTINUED_AS_NEW
        self.assertEqual(client.poll(initial.remote_id).state, "unknown")
        description.status = WorkflowExecutionStatus.COMPLETED
        handle.result.return_value = {"fraction": 0.5}
        with self.assertRaises(ServiceError):
            client.poll(initial.remote_id)
        handle.result.return_value = {"answer": 42}
        self.assertEqual(client.poll(initial.remote_id).result, {"answer": 42})
        self.assertFalse(handle.result.call_args.kwargs["follow_runs"])

    def test_outbound_adapter_durable_admission_and_lost_reply_do_not_resubmit(self):
        from test_remote_tasks import AdmittedContext
        client, sdk, handle, description = self.client()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            context = AdmittedContext(root)
            try:
                service = RemoteTaskService(RemoteTaskStore(root), client, namespace="trusted-principal-and-workflow")
                adapter = TemporalWorkflowAdapter(service, context.host, reservation_microusd=3)
                request = TaskRequest("a", "b", "submit", {"request_key": "lost", "input": {}}, "t", "m")
                sdk.start_workflow.side_effect = TimeoutError()
                with self.assertRaises(ServiceError):
                    adapter.handle(context, request)
                self.assertEqual(service.status("lost")["state"], "unknown")
                forged = TaskRequest("a", "b", "workflow", {
                    "action": "attach", "request_key": "lost", "remote_id": "other",
                    "evidence": {"claim": "I am an operator"}}, "t", "m")
                with self.assertRaisesRegex(ServiceError, "unsupported capability"):
                    adapter.handle(context, forged)
                self.assertEqual(context.index, 1)
                sdk.start_workflow.side_effect = None
                with self.assertRaisesRegex(ServiceError, "remote outcome unknown"):
                    adapter.handle(context, request)
                self.assertEqual(sdk.start_workflow.await_count, 1)
                self.assertEqual(context.index, 2)
                (root / ".restored-audit-only").write_text("audit")
                with self.assertRaises(OperationsError):
                    adapter.handle(context, request)
                self.assertEqual(sdk.start_workflow.await_count, 1)
            finally:
                context.journal.close()


@unittest.skipUnless(LANGGRAPH, "optional LangGraph SDK is not installed")
class LangGraphSDKTests(unittest.TestCase):
    def graph(self, service):
        from langgraph.checkpoint.memory import InMemorySaver
        from langgraph.graph import StateGraph, START, END
        from mas_host.bridges.langgraph import make_langgraph_node
        class State(TypedDict):
            input: dict
            mas_operation_id: str
            mas_receipt: dict
        builder = StateGraph(State)
        builder.add_node("mas", make_langgraph_node(OrchestratorConnector(service, "worker"), "invoice", "invoice-node"))
        builder.add_edge(START, "mas")
        builder.add_edge("mas", END)
        return builder.compile(checkpointer=InMemorySaver())

    def test_real_checkpoint_interrupt_resume_updates_same_task(self):
        from langgraph.types import Command
        service = ServiceDouble("input_required")
        graph = self.graph(service)
        config = {"configurable": {"thread_id": "thread-1"}}
        pending = graph.invoke({"input": {"x": 1}, "mas_operation_id": "step-1"}, config)
        self.assertIn("__interrupt__", pending)
        finished = graph.invoke(Command(resume={"action": "input", "request_key": "update-1", "input": {"x": 2}}), config)
        self.assertEqual(finished["mas_receipt"]["state"], "completed")
        self.assertEqual(service.submissions, 1)
        self.assertEqual(len(service.updates), 1)

    def test_real_graph_new_logical_operation_has_distinct_identity(self):
        service = ServiceDouble("completed")
        graph = self.graph(service)
        config = {"configurable": {"thread_id": "thread-1"}}
        a = graph.invoke({"input": {}, "mas_operation_id": "step-1"}, config)
        b = graph.invoke({"input": {}, "mas_operation_id": "step-2"}, config)
        self.assertNotEqual(a["mas_receipt"]["task_id"], b["mas_receipt"]["task_id"])
        self.assertEqual(service.submissions, 2)

    def test_real_graph_cancel_retains_unknown_remote_stop(self):
        from langgraph.types import Command
        service = ServiceDouble("unknown")
        graph = self.graph(service)
        config = {"configurable": {"thread_id": "thread-1"}}
        graph.invoke({"input": {}, "mas_operation_id": "step-1"}, config)
        final = graph.invoke(Command(resume={"action": "cancel"}), config)
        self.assertTrue(final["mas_receipt"]["cancel_requested"])
        self.assertEqual(final["mas_receipt"]["state"], "unknown")


if __name__ == "__main__":
    unittest.main()
