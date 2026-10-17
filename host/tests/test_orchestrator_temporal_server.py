"""Opt-in network interop against the pinned real Temporal development server.

MAS_TEMPORAL_INTEROP=1 enables the test; MAS_TEMPORAL_SERVER may name an
already provisioned Temporal CLI 1.9.1 binary to avoid downloading it.
No provider credentials or model calls are used.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from mas_host.bridges.temporal import TemporalTarget, TemporalWorkflowClient
from mas_host.service_contracts import ServiceError

AVAILABLE = importlib.util.find_spec("temporalio") is not None
if AVAILABLE:
    from temporalio import workflow

    @workflow.defn(name="MASBridgeInterop")
    class InteropWorkflow:
        @workflow.run
        async def run(self, input: dict) -> dict:
            if input.get("wait"):
                await workflow.sleep(300)
            return {"answer": input["answer"]}

    @workflow.defn(name="MASBridgeContinueInterop")
    class ContinueWorkflow:
        @workflow.run
        async def run(self, input: dict) -> dict:
            if not input.get("continued"):
                workflow.continue_as_new({"continued": True})
            await workflow.sleep(300)
            return {"done": True}

    @workflow.defn(name="MASTemporalInboundInterop")
    class InboundWorkflow:
        @workflow.run
        async def run(self, input: dict) -> dict:
            from temporalio.common import RetryPolicy
            return await workflow.execute_activity(
                "mas.execute", input, result_type=dict,
                start_to_close_timeout=timedelta(seconds=20),
                schedule_to_close_timeout=timedelta(seconds=40),
                heartbeat_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100), maximum_attempts=2))


@unittest.skipUnless(AVAILABLE and os.environ.get("MAS_TEMPORAL_INTEROP") == "1",
                     "set MAS_TEMPORAL_INTEROP=1 for pinned real-server interoperability")
class TemporalServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_activity_worker_http_service_retry_and_exclusive_mas_worker(self):
        from temporalio import activity
        from temporalio.testing import WorkflowEnvironment
        from temporalio.worker import Worker, UnsandboxedWorkflowRunner
        from mas_host.bridge_config import build_service
        from mas_host.bridges.orchestrator import OrchestratorConnector
        from mas_host.bridges.service_client import TaskServiceClient
        from mas_host.bridges.temporal import temporal_activities
        from mas_host.demo import DEFAULT_INVOICE
        from test_bridge_server import ServerFixture, TOKEN
        from test_runner import RunnerIntegrationTests

        RunnerIntegrationTests.setUpClass()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state, token_file, config_file = root / "state", root / "token", root / "config.json"
            token_file.write_text(TOKEN)
            token_file.chmod(0o600)
            config = {"version": 1, "mode": "mock", "grants": {"alice": ["invoice_mesh"]}}
            config_file.write_text(json.dumps(config))
            service = build_service(config, state)
            worker = subprocess.Popen([sys.executable, "-m", "mas_host", "bridge", "worker",
                "--config", str(config_file), "--state-dir", str(state), "--interval", "0.01"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                env=os.environ | {"MAS_BIN": RunnerIntegrationTests.mas,
                                  "MAS_GATEWAY_BIN": RunnerIntegrationTests.gateway})
            environment = None
            try:
                with ServerFixture(service=service) as server:
                    submissions = []
                    class LostSubmitReplyClient(TaskServiceClient):
                        def submit(self, principal, request_key, workflow, input, **kwargs):
                            result = super().submit(principal, request_key, workflow, input, **kwargs)
                            submissions.append((activity.info().attempt, request_key, result["task_id"]))
                            if len(submissions) == 1:
                                raise ServiceError("discarded_accepted_reply")
                            return result
                    client = LostSubmitReplyClient(server.endpoint, token_file=token_file,
                        principal="alice", timeout_seconds=3, allow_loopback_http=True)
                    connector = OrchestratorConnector(client, "alice")
                    environment = await asyncio.wait_for(WorkflowEnvironment.start_local(
                        dev_server_existing_path=os.environ.get("MAS_TEMPORAL_SERVER"),
                        dev_server_download_version="v1.9.1"), timeout=60)
                    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="temporal-mas-interop") as executor:
                        async with Worker(environment.client, task_queue="mas-inbound-interop",
                            workflows=[InboundWorkflow], workflow_runner=UnsandboxedWorkflowRunner(),
                            activities=temporal_activities(connector), activity_executor=executor,
                            max_concurrent_activities=2):
                            result = await asyncio.wait_for(environment.client.execute_workflow(
                                InboundWorkflow.run,
                                {"workflow": "invoice_mesh", "input": {"invoice": DEFAULT_INVOICE},
                                 "timeout_seconds": 10, "poll_seconds": 1},
                                id="inbound-retry", task_queue="mas-inbound-interop"), timeout=45)
                    self.assertEqual(result["state"], "completed")
                    self.assertEqual(result["result"]["total_cents"], 5497)
                    self.assertEqual([attempt for attempt, _, _ in submissions], [1, 2])
                    self.assertEqual(submissions[0][1:], submissions[1][1:])
                    self.assertEqual(result["task_id"], submissions[0][2])
                    self.assertEqual(len(service.list_tasks("alice")), 1)
                    with self.assertRaisesRegex(ServiceError, "forbidden"):
                        service.list_tasks("bob")
                    self.assertIsNone(worker.poll(), "exclusive MAS worker exited before verification")
            finally:
                if environment is not None:
                    await environment.shutdown()
                worker.terminate()
                try:
                    _, diagnostics = worker.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.communicate(timeout=5)
                    self.fail("exclusive MAS worker did not stop within five seconds")
                self.assertEqual(worker.returncode, 0, diagnostics.decode())

    async def test_lost_start_lookup_recovers_first_run_after_continue_as_new(self):
        from temporalio.client import Client, WorkflowExecutionStatus
        from temporalio.testing import WorkflowEnvironment
        from temporalio.worker import Worker, UnsandboxedWorkflowRunner
        environment = await asyncio.wait_for(WorkflowEnvironment.start_local(
            dev_server_existing_path=os.environ.get("MAS_TEMPORAL_SERVER"),
            dev_server_download_version="v1.9.1"), timeout=60)
        try:
            target = TemporalTarget(environment.client.service_client.config.target_host,
                "default", "MASBridgeContinueInterop", "continue-interop", "test", tls=False)
            client = TemporalWorkflowClient(target)
            async with Worker(environment.client, task_queue=target.task_queue,
                              workflows=[ContinueWorkflow], workflow_runner=UnsandboxedWorkflowRunner()):
                accepted = []
                class LoseReply:
                    def __init__(self, actual):
                        self.actual = actual
                    async def start_workflow(self, *args, **kwargs):
                        handle = await self.actual.start_workflow(*args, **kwargs)
                        accepted.append(handle.first_execution_run_id)
                        raise TimeoutError("discarded accepted response")
                async def lost_client():
                    return LoseReply(await Client.connect(target.endpoint, namespace="default"))
                uncertain = TemporalWorkflowClient(target, client_factory=lost_client)
                payload = {"request_key": "continued-lost", "input": {}}
                with self.assertRaises(ServiceError):
                    await asyncio.to_thread(uncertain.submit, payload)
                workflow_id = client.workflow_id(payload["request_key"])
                for _ in range(100):
                    latest = await environment.client.get_workflow_handle(workflow_id).describe()
                    if latest.run_id != accepted[0]:
                        break
                    await asyncio.sleep(0.05)
                self.assertNotEqual(latest.run_id, accepted[0])
                self.assertEqual(latest.raw_info.first_run_id, accepted[0])
                self.assertEqual((await latest.memo())["mas_binding"], client._binding(payload))
                exact = await asyncio.to_thread(client.lookup, payload)
                self.assertEqual(exact, workflow_id + ":" + accepted[0])
                self.assertEqual((await asyncio.to_thread(client.poll, exact)).state, "unknown")
                # Cancelling the recovered original must never target its
                # still-running successor, even if the server rejects a cancel
                # on the already-closed original execution.
                try:
                    cancelled = await asyncio.to_thread(client.cancel, exact)
                    self.assertEqual(cancelled.state, "unknown")
                except ServiceError:
                    pass
                successor = await environment.client.get_workflow_handle(
                    workflow_id, run_id=latest.run_id).describe()
                self.assertEqual(successor.status, WorkflowExecutionStatus.RUNNING)
                self.assertEqual(len(accepted), 1)
        finally:
            await environment.shutdown()

    async def test_real_start_poll_cancel_and_lost_receipt_reconciliation(self):
        from temporalio.client import Client
        from temporalio.testing import WorkflowEnvironment
        from temporalio.worker import Worker, UnsandboxedWorkflowRunner
        existing = os.environ.get("MAS_TEMPORAL_SERVER")
        environment = await asyncio.wait_for(WorkflowEnvironment.start_local(
            dev_server_existing_path=existing, dev_server_download_version="v1.9.1"), timeout=60)
        try:
            target = TemporalTarget(environment.client.service_client.config.target_host,
                                    "default", "MASBridgeInterop", "bridge-interop", "test", tls=False)
            client = TemporalWorkflowClient(target)
            async with Worker(environment.client, task_queue=target.task_queue,
                              workflows=[InteropWorkflow], workflow_runner=UnsandboxedWorkflowRunner()):
                initial = await asyncio.to_thread(client.submit, {"request_key": "complete", "input": {"answer": 42}})
                async def terminal(remote_id):
                    for _ in range(100):
                        current = await asyncio.to_thread(client.poll, remote_id)
                        if current.state in {"completed", "cancelled", "failed"}:
                            return current
                        await asyncio.sleep(0.05)
                    self.fail("Temporal execution did not reach a terminal state")
                done = await terminal(initial.remote_id)
                self.assertEqual(done.state, "completed")
                self.assertEqual(done.result, {"answer": 42})
                slow = await asyncio.to_thread(client.submit, {"request_key": "cancel", "input": {"answer": 7, "wait": True}})
                await asyncio.to_thread(client.cancel, slow.remote_id)
                stopped = await terminal(slow.remote_id)
                self.assertEqual(stopped.state, "cancelled")

                # Send a real start request, discard its accepted reply, then
                # recover the exact run by the precommitted workflow ID/memo.
                class LoseReply:
                    def __init__(self, actual):
                        self.actual = actual
                    async def start_workflow(self, *args, **kwargs):
                        await self.actual.start_workflow(*args, **kwargs)
                        raise TimeoutError("discarded accepted response")
                async def lost_client():
                    return LoseReply(await Client.connect(target.endpoint, namespace="default"))
                uncertain = TemporalWorkflowClient(target, client_factory=lost_client)
                payload = {"request_key": "lost", "input": {"answer": 99}}
                with self.assertRaisesRegex(ServiceError, "temporal outcome unknown"):
                    await asyncio.to_thread(uncertain.submit, payload)
                exact = await asyncio.to_thread(client.lookup, payload)
                recovered = await terminal(exact)
                self.assertEqual(recovered.result, {"answer": 99})
                with self.assertRaises(ServiceError):
                    await asyncio.to_thread(client.submit, payload)
                self.assertEqual(await asyncio.to_thread(client.lookup, payload), exact)
        finally:
            await environment.shutdown()


if __name__ == "__main__":
    unittest.main()
