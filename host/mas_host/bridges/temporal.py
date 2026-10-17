"""Optional Temporal activities over TaskService, with stable retry identities."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
import re
from pathlib import Path

from .orchestrator import OrchestratorConnector, stable_request_key
from .remote import RemoteSnapshot, RemoteTaskService, bounded, digest, token
from ..contracts import Outcome
from ..service_contracts import ServiceError


def temporal_activities(connector: OrchestratorConnector) -> list:
    """Register these synchronous activities using a Worker activity executor.

    Keep RPC and polling work in activities, never in deterministic workflow
    code. Set heartbeat_timeout on ``mas.execute``/``mas.wait``. A retry reuses
    the workflow-run/activity identity even if its submit reply was lost.
    """
    from temporalio import activity
    from temporalio.exceptions import ApplicationError

    def checked(call, *args, **kwargs):
        try:
            return call(*args, **kwargs)
        except ServiceError as error:
            permanent = error.code.startswith("invalid_") or error.code in {
                "request_conflict", "forbidden", "not_found", "invalid_request",
                "unsupported_capability", "invalid_orchestration_identity",
                "invalid_poll_deadline", "invalid_poll_interval", "task_not_found",
                "workflow_changed", "result_unavailable", "audit_only", "peer_contract_mismatch",
                "workflow_identity_mismatch", "deployment_identity_mismatch"}
            raise ApplicationError(error.code, type="MAS." + error.code,
                                   non_retryable=permanent) from None

    def key(request: dict) -> str:
        info = activity.info()
        if "request_key" in request:
            return stable_request_key("temporal", info.namespace, request["request_key"])
        run = info.workflow_run_id or info.activity_run_id
        return stable_request_key("temporal", info.namespace, run, info.activity_id)

    def request_shape(request, fields):
        if type(request) is not dict or not fields.issubset(request):
            raise ApplicationError("invalid_request", type="MAS.invalid_request", non_retryable=True)
        for name in fields - {"input"}:
            if type(request[name]) is not str or not request[name]:
                raise ApplicationError("invalid_request", type="MAS.invalid_request", non_retryable=True)
        if "input" in fields and type(request["input"]) is not dict:
            raise ApplicationError("invalid_request", type="MAS.invalid_request", non_retryable=True)

    def observe(task_id: str, request: dict) -> dict:
        return checked(connector.wait, task_id,
                       timeout_seconds=request.get("timeout_seconds", 30),
                       poll_seconds=request.get("poll_seconds", 1),
                       heartbeat=activity.heartbeat, cancelled=activity.is_cancelled)

    @activity.defn(name="mas.submit")
    def submit(request: dict) -> dict:
        request_shape(request, {"workflow", "input"})
        return checked(connector.submit, checked(key, request), request["workflow"], request["input"])

    @activity.defn(name="mas.execute", no_thread_cancel_exception=True)
    def execute(request: dict) -> dict:
        current = submit(request)
        return observe(current["task_id"], request)

    @activity.defn(name="mas.wait", no_thread_cancel_exception=True)
    def wait(request: dict) -> dict:
        request_shape(request, {"task_id"})
        return observe(request["task_id"], request)

    @activity.defn(name="mas.inspect")
    def inspect(request: dict) -> dict:
        request_shape(request, {"task_id"})
        return checked(connector.inspect, request["task_id"])

    @activity.defn(name="mas.result")
    def result(request: dict) -> dict:
        request_shape(request, {"task_id"})
        return checked(connector.result, request["task_id"])

    @activity.defn(name="mas.cancel")
    def cancel(request: dict) -> dict:
        request_shape(request, {"task_id"})
        return checked(connector.cancel, request["task_id"])

    @activity.defn(name="mas.update")
    def update(request: dict) -> dict:
        request_shape(request, {"task_id", "input"})
        return checked(connector.update, request["task_id"], checked(key, request), request["input"])

    return [submit, execute, wait, inspect, result, cancel, update]


@dataclass(frozen=True)
class TemporalTarget:
    """Frozen administrator configuration, never derived from a task payload."""
    endpoint: str
    namespace: str
    workflow: str
    task_queue: str
    deployment_id: str
    tls: bool = True
    rpc_timeout_seconds: int = 10
    execution_timeout_seconds: int = 3600

    def __post_init__(self):
        for value in (self.namespace, self.workflow, self.task_queue, self.deployment_id):
            token(value)
        if (not re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]{1,5}", self.endpoint)
                or not 1 <= int(self.endpoint.rsplit(":", 1)[1]) <= 65535
                or type(self.tls) is not bool
                or type(self.rpc_timeout_seconds) is not int
                or not 1 <= self.rpc_timeout_seconds <= 60
                or type(self.execution_timeout_seconds) is not int
                or not 1 <= self.execution_timeout_seconds <= 86400):
            raise ServiceError("invalid_temporal_target")


class TemporalWorkflowClient:
    """Real SDK remote client with exact run identity and no application retry.

    Each call opens its SDK client inside the caller's bounded event loop, so
    no loop-bound client or background polling context crosses a host fork.
    Credentials are injected and never included in the public descriptor.
    """
    def __init__(self, target: TemporalTarget, *, api_key: str | None = None,
                 client_factory=None):
        self.target, self._api_key = target, api_key
        self._client_factory = client_factory

    def descriptor(self) -> dict:
        return {"provider": "temporal", "contract": "single-run-workflow/1",
                "endpoint": self.target.endpoint, "namespace": self.target.namespace,
                "workflow": self.target.workflow, "task_queue": self.target.task_queue,
                "deployment_id": self.target.deployment_id, "tls": self.target.tls,
                "rpc_timeout_seconds": self.target.rpc_timeout_seconds,
                "execution_timeout_seconds": self.target.execution_timeout_seconds,
                "sdk": "1.34.0", "continuation": False}

    def workflow_id(self, request_key: str) -> str:
        return "mas-" + digest({"target": self.descriptor(), "key": token(request_key)})

    def _binding(self, payload: dict) -> str:
        return digest({"descriptor": self.descriptor(), "input": bounded(payload)})

    async def _connect(self):
        if self._client_factory is not None:
            return await self._client_factory()
        from temporalio.client import Client
        return await Client.connect(self.target.endpoint, namespace=self.target.namespace,
                                    tls=self.target.tls, api_key=self._api_key)

    def _call(self, operation):
        async def bounded_call():
            return await asyncio.wait_for(operation(), timeout=self.target.rpc_timeout_seconds)
        try:
            # Adapter.handle runs synchronously. Async callers should dispatch
            # through a worker, rather than nesting loops or sharing a client.
            return asyncio.run(bounded_call())
        except ServiceError:
            raise
        except Exception:
            raise ServiceError("temporal_outcome_unknown") from None

    @staticmethod
    def _parts(remote_id):
        token(remote_id)
        parts = remote_id.split(":")
        if len(parts) != 2 or not re.fullmatch(r"mas-[0-9a-f]{64}", parts[0]) or not re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", parts[1]):
            raise ServiceError("invalid_temporal_identity")
        return parts

    async def _snapshot(self, handle, remote_id: str) -> RemoteSnapshot:
        from temporalio.client import WorkflowExecutionStatus
        description = await handle.describe(rpc_timeout=timedelta(seconds=self.target.rpc_timeout_seconds))
        if description.run_id != self._parts(remote_id)[1]:
            raise ServiceError("remote_identity_conflict")
        status = description.status
        if status == WorkflowExecutionStatus.COMPLETED:
            result = await handle.result(follow_runs=False,
                                         rpc_timeout=timedelta(seconds=self.target.rpc_timeout_seconds))
            return RemoteSnapshot(remote_id, "completed", bounded(result))
        if status == WorkflowExecutionStatus.CANCELED:
            return RemoteSnapshot(remote_id, "cancelled")
        if status in {WorkflowExecutionStatus.FAILED, WorkflowExecutionStatus.TIMED_OUT,
                      WorkflowExecutionStatus.TERMINATED}:
            return RemoteSnapshot(remote_id, "failed", {"error": "workflow_" + status.name.lower()})
        if status == WorkflowExecutionStatus.RUNNING:
            return RemoteSnapshot(remote_id, "running")
        # Continue-as-new changes execution identity. This profile deliberately
        # refuses to treat a new run as the admitted run or its cancellation target.
        return RemoteSnapshot(remote_id, "unknown")

    async def _lookup(self, client, payload: dict) -> str:
        workflow_id = self.workflow_id(payload["request_key"])
        handle = client.get_workflow_handle(workflow_id)
        latest = await handle.describe(rpc_timeout=timedelta(seconds=self.target.rpc_timeout_seconds))
        # A lookup without run_id resolves the latest execution. A successor
        # created by continue-as-new can inherit the same type and memo, so those
        # fields alone do not prove that it is the execution we admitted.
        first_run_id = latest.raw_info.first_run_id
        remote_id = workflow_id + ":" + first_run_id
        self._parts(remote_id)
        description = (latest if latest.run_id == first_run_id else
                       await client.get_workflow_handle(workflow_id, run_id=first_run_id).describe(
                           rpc_timeout=timedelta(seconds=self.target.rpc_timeout_seconds)))
        memo = await description.memo()
        if (description.id != workflow_id or description.run_id != first_run_id
                or description.raw_info.first_run_id != first_run_id
                or memo.get("mas_binding") != self._binding(payload)
                or description.workflow_type != self.target.workflow):
            raise ServiceError("remote_identity_conflict")
        return remote_id

    def lookup(self, payload: dict) -> str:
        """Read-only recovery of the first run with memo and type verification.

        A not-found or failed read remains uncertainty, never permission to start
        another workflow. Successor runs are never selected, even when they
        inherit the original memo. Feed the exact ID into durable ``attach``.
        """
        async def operation():
            return await self._lookup(await self._connect(), bounded(payload))
        return self._call(operation)

    def submit(self, payload: dict) -> RemoteSnapshot:
        payload = bounded(payload)
        if set(payload) != {"request_key", "input"} or type(payload["input"]) is not dict:
            raise ServiceError("invalid_payload")
        workflow_id = self.workflow_id(payload["request_key"])
        async def operation():
            from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
            client = await self._connect()
            handle = await client.start_workflow(
                self.target.workflow, payload["input"], id=workflow_id,
                task_queue=self.target.task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                execution_timeout=timedelta(seconds=self.target.execution_timeout_seconds),
                memo={"mas_binding": self._binding(payload)},
                rpc_timeout=timedelta(seconds=self.target.rpc_timeout_seconds))
            remote_id = workflow_id + ":" + handle.first_execution_run_id
            self._parts(remote_id)
            return RemoteSnapshot(remote_id, "running")
        return self._call(operation)

    def poll(self, remote_id: str) -> RemoteSnapshot:
        workflow_id, run_id = self._parts(remote_id)
        async def operation():
            client = await self._connect()
            return await self._snapshot(client.get_workflow_handle(workflow_id, run_id=run_id), remote_id)
        return self._call(operation)

    def cancel(self, remote_id: str) -> RemoteSnapshot:
        workflow_id, run_id = self._parts(remote_id)
        async def operation():
            client = await self._connect()
            handle = client.get_workflow_handle(workflow_id, run_id=run_id)
            await handle.cancel(rpc_timeout=timedelta(seconds=self.target.rpc_timeout_seconds))
            return await self._snapshot(handle, remote_id)
        return self._call(operation)

    def update(self, remote_id: str, payload: dict) -> RemoteSnapshot:
        raise ServiceError("unsupported_capability")


class TemporalWorkflowAdapter:
    """MAS outbound adapter composed with the shared durable remote-task store."""
    def __init__(self, service: RemoteTaskService, host_run_dir: Path, *,
                 reservation_microusd: int):
        if (type(reservation_microusd) is not int
                or not 0 <= reservation_microusd <= 100_000_000):
            raise ServiceError("invalid_reservation")
        self.service, self.host_run_dir = service, Path(host_run_dir)
        self.reservation_microusd = reservation_microusd

    def descriptor(self) -> dict:
        return {**self.service.identity, "reservation_microusd": self.reservation_microusd}

    def submission_payload(self, request_key: str, input: dict) -> dict:
        # Remote workflow IDs must be scoped too: separating only SQLite rows
        # would let two principals collide at the Temporal server.
        return {"request_key": stable_request_key("temporal", self.service.namespace, token(request_key)),
                "input": input}

    def handle(self, context, request) -> Outcome:
        key = token(request.input.get("request_key"))
        action = request.input.get("action") if request.operation == "workflow" else request.operation
        # Public lifecycle dispatch never grants operator reconciliation. An
        # attach operation must be selected by trusted registration/admin code,
        # rather than by a submitter's application payload.
        if request.operation == "workflow" and action not in {"submit", "poll", "cancel"}:
            raise ServiceError("unsupported_capability")
        common = {"context": context, "host_run_dir": self.host_run_dir,
                  "reservation_microusd": self.reservation_microusd}
        if action == "submit":
            value = self.service.submit(key, self.submission_payload(key, request.input.get("input", {})), **common)
        elif action == "poll":
            value = self.service.poll(key, **common)
        elif action == "cancel":
            value = self.service.cancel(key, request_key=request.input.get("cancel_key", "cancel"), **common)
        elif action == "attach":
            value = self.service.attach(key, request.input.get("remote_id"),
                                        evidence=request.input.get("evidence", {}), **common)
        else:
            raise ServiceError("unsupported_capability")
        # A successful submission operation returns a job receipt, not the
        # claim that the remote business workflow has completed successfully.
        return Outcome(value)
