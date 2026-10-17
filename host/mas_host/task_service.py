"""Durable principal-scoped ingress above the unchanged Rust MAS boundary.

One submitted revision owns one deterministic run and one root request. A
single Linux worker replays that root after interruption; transports never
execute Runner on their request threads. Unknown effects require an evidenced
journal resolution before explicit recovery, never a fresh submission.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import fcntl
import hashlib
import json
import os
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, Protocol
import uuid

from .codec import JsonSizeError, integer_json
from .contracts import Adapter, AdapterInterrupted, AdapterTimeout, Outcome, RecoveryRequired, RunnerError
from .execution_policy import RetryPolicy, TimingPolicy, retry_policy, timing_policy
from .circuit_breaker import circuit_policies
from .service_contracts import ServiceError
from .service_mutation import service_mutation
from .request_namespaces import RequestScope, epoch_guard
from .task_store import TaskStore, canonical, decoded, event, now


TERMINAL = frozenset({"completed", "failed", "cancelled"})
INTERRUPTED = frozenset({"input_required", "auth_required"})
STATES = TERMINAL | INTERRUPTED | {"queued", "running", "unknown", "blocked"}
PAGE_BYTES = 512 * 1024


def _bounded_items(values, *, limit=1000, overhead=2):
    """Consume a bounded JSON prefix, leaving room for its enclosing object."""
    selected, size = [], overhead
    for value in values:
        item_bytes = len(canonical(value).encode("utf-8")) + bool(selected)
        if len(selected) >= limit or size + item_bytes > PAGE_BYTES:
            if not selected:
                raise ServiceError("page_item_too_large")
            return selected, True
        selected.append(value)
        size += item_bytes
    return selected, False


def _text(value, name: str, maximum: int = 256):
    if type(value) is not str or not value or any(ord(c) < 32 for c in value):
        raise ServiceError("invalid_" + name)
    try:
        if len(value.encode("utf-8")) > maximum:
            raise ServiceError("invalid_" + name)
    except UnicodeError:
        raise ServiceError("invalid_" + name) from None
    return value


def _object(value):
    if type(value) is not dict:
        raise ServiceError("invalid_input")
    try:
        return json.loads(integer_json(value))
    except JsonSizeError:
        raise ServiceError("input_too_large", "application JSON exceeds its byte limit") from None
    except RunnerError:
        raise ServiceError("invalid_input", "input must be bounded integer-only JSON") from None


@dataclass(frozen=True)
class ExecutionBinding:
    """Trusted immutable identity supplied only by the durable scheduler.

    Factory code can bind provider adapters to the assigned run directory and
    scope remote receipts to an authenticated owner. These values never come
    from application input or a peer's claimed identity.
    """
    principal: str
    workflow: str
    workflow_identity: str
    task_id: str
    revision: int
    run_dir: Path
    state_dir: Path

    def remote_namespace(self, actor: str) -> str:
        _text(actor, "actor")
        return "owner-" + hashlib.sha256(canonical({
            "principal": self.principal, "workflow": self.workflow,
            "workflow_identity": self.workflow_identity, "actor": actor}).encode()).hexdigest()


@dataclass(frozen=True)
class Workflow:
    """Explicit trusted workflow registration, frozen for recovery.

    ``version`` identifies application intent; measured source-package bytes
    bind adapter implementation, classifiers and continuation logic. Changing
    package bytes changes identity even when the version label is unchanged.
    ``continue_input`` is pure: it combines the previous
    output with supplied input for a fresh, separately admitted revision.
    ``contract_descriptor`` optionally binds operation, provider and acceptance
    descriptors into recovery identity. It is copied on construction; any
    subsequent local mutation changes the identity rather than hiding drift.
    """
    version: str
    topology: dict
    adapters: Mapping[str, Adapter]
    sender: str
    recipient: str
    operation: str
    max_depth: int = 8
    classify: Callable[[Outcome], str] | None = None
    continue_input: Callable[[dict, dict], dict] | None = None
    adapter_factory: Callable[[ExecutionBinding], Mapping[str, Adapter]] | None = None
    contract_descriptor: dict | None = None
    implementation_digest: str | None = None
    retry_policy: RetryPolicy | dict | None = None
    timing_policy: TimingPolicy | dict | None = None
    resource_keys: tuple[str, ...] = ()
    circuit_breakers: dict | None = None
    implementation_sources: tuple[Callable, ...] = ()
    assessment_policies: dict | None = None
    _implementation: object = field(init=False, repr=False, compare=False)
    _worker_bindings: str = field(init=False, repr=False, compare=False)
    _workspace_bindings: str = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        _text(self.version, "workflow_version")
        if self.version == "unversioned":
            raise ServiceError("invalid_workflow_version")
        topology = _object(self.topology)
        if (type(self.max_depth) is not int or not 1 <= self.max_depth <= 64
                or set(topology.get("agents", [])) != set(self.adapters)
                or self.sender not in self.adapters or self.recipient not in self.adapters
                or any(not callable(getattr(adapter, "handle", None)) for adapter in self.adapters.values())):
            raise ServiceError("invalid_workflow")
        _text(self.operation, "operation")
        object.__setattr__(self, "topology", topology)
        object.__setattr__(self, "adapters", MappingProxyType(dict(self.adapters)))
        if self.assessment_policies is not None:
            from .assessment_journal import assessment_policies
            object.__setattr__(self, "assessment_policies", assessment_policies(self.assessment_policies, self.adapters))
        if self.contract_descriptor is not None:
            object.__setattr__(self, "contract_descriptor", _object(self.contract_descriptor))
        if self.implementation_digest is not None:
            import re
            if type(self.implementation_digest) is not str or not re.fullmatch(r"[0-9a-f]{64}", self.implementation_digest):
                raise ServiceError("invalid_implementation_digest")
        try:
            if self.retry_policy is not None:
                object.__setattr__(self, "retry_policy", retry_policy(self.retry_policy))
            if self.timing_policy is not None:
                object.__setattr__(self, "timing_policy", timing_policy(self.timing_policy))
            if self.circuit_breakers is not None:
                policies = circuit_policies(self.circuit_breakers)
                if not set(policies) <= set(self.adapters):
                    raise RunnerError("provider circuit references an unknown actor")
                object.__setattr__(self, "circuit_breakers", MappingProxyType(policies))
        except RunnerError:
            raise ServiceError("invalid_execution_policy") from None
        if (not isinstance(self.resource_keys, (list, tuple)) or len(self.resource_keys) > 32
                or any(type(key) is not str or not key or len(key) > 128
                       or any(character.isspace() or ord(character) < 32 for character in key)
                       for key in self.resource_keys)
                or len(set(self.resource_keys)) != len(self.resource_keys)):
            raise ServiceError("invalid_resource_keys")
        object.__setattr__(self, "resource_keys", tuple(sorted(self.resource_keys)))
        if (not isinstance(self.implementation_sources, tuple)
                or any(not callable(item) for item in self.implementation_sources)):
            raise ServiceError("invalid_implementation_sources")
        from .implementation import VerifiedImplementation, implementation_values
        implementation = VerifiedImplementation.capture(implementation_values(self))
        if self.implementation_digest is not None and self.implementation_digest != implementation.digest:
            raise ServiceError("implementation_digest_mismatch")
        object.__setattr__(self, "_implementation", implementation)
        from .executable_worker import ExecutableAdapter
        workers = {actor: adapter.spec.descriptor for actor, adapter in self.adapters.items()
                   if isinstance(adapter, ExecutableAdapter)}
        object.__setattr__(self, '_worker_bindings', canonical(workers))
        from .workspaces import WorkspaceAdapter
        workspaces = {actor: adapter.registration_descriptor for actor, adapter in self.adapters.items()
                      if isinstance(adapter, WorkspaceAdapter)}
        object.__setattr__(self, '_workspace_bindings', canonical(workspaces))

    @property
    def verified_implementation(self):
        return self._implementation

    def verify_implementation(self):
        from .implementation import implementation_values
        self._implementation.verify(implementation_values(self))
        self._verify_workers(self.adapters)

    def _verify_workers(self, adapters):
        from .executable_worker import ExecutableAdapter, WorkerSpec
        from .workspaces import WorkspaceAdapter
        workers = {actor: adapter.spec.descriptor for actor, adapter in adapters.items()
                   if isinstance(adapter, ExecutableAdapter)}
        workspaces = {actor: adapter.registration_descriptor for actor, adapter in adapters.items()
                      if isinstance(adapter, WorkspaceAdapter)}
        if canonical(workers) != self._worker_bindings or canonical(workspaces) != self._workspace_bindings:
            raise ServiceError('implementation_worker_binding_mismatch')
        try:
            checked = set()
            for adapter in adapters.values():
                worker = (adapter.spec if isinstance(adapter, ExecutableAdapter) else
                          adapter.tool.worker if isinstance(adapter, WorkspaceAdapter) else None)
                if isinstance(worker, WorkerSpec) and id(worker) not in checked:
                    worker.verify()
                    checked.add(id(worker))
        except RunnerError:
            raise ServiceError('implementation_worker_unavailable') from None

    def verify_adapters(self, adapters):
        """A trusted factory may construct adapters, but cannot replace pinned workers."""
        if (set(adapters) != set(self.adapters)
                or any(not callable(getattr(adapter, 'handle', None)) for adapter in adapters.values())):
            raise ServiceError('invalid_adapter_factory')
        self._implementation.verify_bindings(tuple(adapters.values()))
        self._verify_workers(adapters)

    @property
    def descriptor(self) -> dict:
        descriptor = {"version": self.version, "topology": self.topology,
            "sender": self.sender, "recipient": self.recipient, "operation": self.operation,
            "max_depth": self.max_depth, "continuation": self.continue_input is not None,
            "classifier": self.classify is not None, "adapter_factory": self.adapter_factory is not None}
        # Keep the established optional contract descriptor shape.
        if self.contract_descriptor is not None:
            descriptor["contract_descriptor"] = _object(self.contract_descriptor)
        descriptor["implementation_digest"] = self._implementation.digest
        if self._worker_bindings != '{}':
            descriptor['worker_bindings'] = json.loads(self._worker_bindings)
        if self._workspace_bindings != '{}':
            descriptor['workspace_bindings'] = json.loads(self._workspace_bindings)
        if self.retry_policy is not None:
            descriptor["retry_policy"] = self.retry_policy.descriptor
        if self.timing_policy is not None:
            descriptor["timing_policy"] = self.timing_policy.descriptor
        if self.resource_keys:
            descriptor["resource_keys"] = list(self.resource_keys)
        if self.circuit_breakers is not None:
            descriptor["circuit_breakers"] = {actor: policy.descriptor for actor, policy in self.circuit_breakers.items()}
        if self.assessment_policies:
            descriptor["assessment_policies"] = _object(self.assessment_policies)
        return descriptor

    @property
    def identity(self) -> str:
        return hashlib.sha256(canonical(self.descriptor).encode()).hexdigest()


@dataclass(frozen=True)
class ExecutionResult:
    state: str
    result: dict | None = None
    error: dict | None = None
    events: tuple[dict, ...] = ()


class WorkflowExecutor(Protocol):
    def execute(self, workflow: Workflow, execution: dict, *, cancelled: Callable[[], bool],
                ownership_descriptor: int | tuple[int, ...]) -> ExecutionResult: ...


class WorkflowInputValidator(Protocol):
    """Optional executor-specific preflight, without effects or durable writes.

    Executors with additional representation limits implement this capability.
    The service's common integer-JSON bound still applies to every executor.
    """
    def validate_input(self, workflow: Workflow, run_id: str, input: dict) -> None: ...


class _OwnedContext:
    def __init__(self, context, descriptor):
        values = descriptor if type(descriptor) is tuple else (descriptor,)
        if (not values or any(type(value) is not int or value < 0 for value in values)
                or len(values) != len(set(values))):
            raise ServiceError("invalid_ownership_descriptors")
        self._context, self._descriptors = context, values

    @property
    def ownership_descriptors(self):
        return tuple(dict.fromkeys((*self._context.ownership_descriptors, *self._descriptors)))

    def __getattr__(self, name):
        return getattr(self._context, name)


class _CancellableExecutor:
    def __init__(self, executor, cancelled, descriptor):
        self.executor, self.cancelled, self.descriptor = executor, cancelled, descriptor

    def execute(self, adapter, context, request, *, deadline, tick=None):
        def poll():
            if self.cancelled():
                raise AdapterInterrupted("task cancellation requested")
            if tick:
                tick()
        poll()
        return self.executor.execute(adapter, _OwnedContext(context, self.descriptor), request,
                                     deadline=deadline, tick=poll)


class RunnerExecutor:
    """Execute registrations through real Runner admission and durable replay."""
    def __init__(self, state_dir: Path, *, operations, mas_bin: str = "mas",
                 gateway_bin: str = "mas-gateway", supervised: bool = True,
                 command_timeout: float = 10):
        self.state_dir = Path(state_dir).absolute()
        self.operations = operations
        self.options = {"mas_bin": os.environ.get("MAS_BIN", mas_bin) if mas_bin == "mas" else mas_bin,
                        "gateway_bin": os.environ.get("MAS_GATEWAY_BIN", gateway_bin) if gateway_bin == "mas-gateway" else gateway_bin,
                        "command_timeout": command_timeout, "supervised": supervised}

    def validate_input(self, workflow: Workflow, run_id: str, input: dict) -> None:
        from .protocol_client import ProtocolClient
        from .runner import validate_request_input
        try:
            client = ProtocolClient(self.options["mas_bin"], self.options["gateway_bin"])
            validate_request_input(client, run_id, workflow.sender, workflow.recipient,
                                   workflow.operation, input)
        except JsonSizeError:
            raise ServiceError("input_too_large", "input does not fit the complete protocol frame") from None
        except RunnerError:
            raise ServiceError("input_not_encodable", "input could not be encoded for this workflow") from None

    def _bootstrap(self, workflow, execution):
        from .bootstrap import _write_json, _sync_directory, _read_json
        path = self.state_dir / execution["run_directory"]
        if path.parent != self.state_dir or path.is_symlink():
            raise ServiceError("invalid_run_directory")
        path.mkdir(mode=0o700, exist_ok=True)
        manifest = path / "bootstrap.json"
        if not manifest.exists():
            # The service's committed execution row authorises this exact
            # identity. Before this manifest exists, no admission/provider
            # invocation can have happened for this revision.
            if any(item.name != ".bootstrap.json.pending" for item in path.iterdir()):
                raise ServiceError("incomplete_bootstrap")
            _write_json(manifest, {"format": "mas-host-bootstrap/1", "stage": "intent",
                "run_id": execution["run_id"], "workflow_version": workflow.version,
                "max_depth": workflow.max_depth, "topology": workflow.topology,
                "gateway_id": "service-" + hashlib.sha256(execution["run_id"].encode()).hexdigest()[:24]})
            _sync_directory(self.state_dir)
        intent = _read_json(manifest)
        if (intent.get("run_id") != execution["run_id"]
                or intent.get("workflow_version") != workflow.version
                or intent.get("max_depth") != workflow.max_depth
                or intent.get("topology") != workflow.topology):
            raise ServiceError("run_assignment_conflict")
        return path

    def _stopped_result(self, execution):
        from .bootstrap import requires_reconciliation
        if requires_reconciliation(self.state_dir / execution['run_directory'], execution['run_id'],
                                   operations=self.operations):
            return ExecutionResult('unknown', error={'code': 'reconciliation_required'})
        return ExecutionResult("cancelled")

    def execute(self, workflow, execution, *, cancelled, ownership_descriptor):
        from .runner import Runner
        from .executable_worker import ProfileExecutor
        if cancelled():
            return self._stopped_result(execution)
        workflow.verify_implementation()
        path = self._bootstrap(workflow, execution)
        adapters = workflow.adapters
        if workflow.adapter_factory is not None:
            binding = ExecutionBinding(execution["principal"], execution["workflow"], workflow.identity,
                execution["task_id"], execution["revision"], path, self.state_dir)
            try:
                adapters = dict(workflow.adapter_factory(binding))
            except Exception:
                raise ServiceError("invalid_adapter_factory") from None
            workflow.verify_adapters(adapters)
        executor = ProfileExecutor(supervised=self.options['supervised'], gateway_bin=self.options['gateway_bin'])
        from .operations import DeploymentLedger
        from .principal_quotas import PrincipalAllocation
        allocation = PrincipalAllocation(self.operations or DeploymentLedger(self.state_dir), execution['principal'])
        try:
            runner = Runner.resume(path, adapters, workflow_version=workflow.version,
                           max_depth=workflow.max_depth, operations=allocation,
                           executor=_CancellableExecutor(executor, cancelled, ownership_descriptor),
                           execution_deadline=execution.get("execution_deadline"),
                           retry_policy=workflow.retry_policy,
                           assessment_policies=workflow.assessment_policies,
                           circuit_breakers=dict(workflow.circuit_breakers) if workflow.circuit_breakers is not None else None,
                           event_sink=execution.get("event_sink"),
                           **self.options)
        except Exception:
            # An ancestor may close admission during bootstrap. Construction
            # has not dispatched a worker and has already released its run
            # session; retained journal evidence still decides uncertainty.
            if cancelled():
                return self._stopped_result(execution)
            raise
        with runner:
            try:
                outcome = runner.request(workflow.sender, workflow.recipient,
                                         workflow.operation, execution["input"])
                state = workflow.classify(outcome) if workflow.classify else (
                    "completed" if outcome.status == "completed" else "failed")
                if state not in TERMINAL | INTERRUPTED:
                    raise ServiceError("invalid_workflow_state")
                return ExecutionResult(state, _object(outcome.output),
                    error={"code": "application_unresolved"} if not workflow.classify and state == "failed" else None,
                    events=tuple(runner.events))
            except RecoveryRequired:
                return ExecutionResult("unknown", error={"code": "reconciliation_required"},
                                       events=tuple(runner.events))
            except AdapterInterrupted:
                from .journal import inspect_run
                if inspect_run(path)["recovery_required"]:
                    return ExecutionResult("unknown", error={"code": "reconciliation_required"}, events=tuple(runner.events))
                # The supervised executor reports RecoveryRequired instead if
                # an external invocation may have escaped before cancellation.
                if cancelled():
                    return ExecutionResult("cancelled", events=tuple(runner.events))
                raise
            except AdapterTimeout:
                from .journal import inspect_run
                if inspect_run(path)["recovery_required"]:
                    return ExecutionResult("unknown", error={"code": "reconciliation_required"}, events=tuple(runner.events))
                return ExecutionResult("failed", error={"code": "deadline_exceeded"},
                                       events=tuple(runner.events))
            except RunnerError:
                from .journal import inspect_run
                if inspect_run(path)["recovery_required"]:
                    return ExecutionResult("unknown", error={"code": "reconciliation_required"}, events=tuple(runner.events))
                return ExecutionResult("failed", error={"code": "workflow_failed"},
                                       events=tuple(runner.events))


class DurableTaskService:
    def __init__(self, state_dir: Path, workflows: Mapping[str, Workflow],
                 grants: Mapping[str, set[str] | frozenset[str]], *, operations=None,
                 executor: WorkflowExecutor | None = None, max_pending: int = 1000,
                 max_tasks: int = 10000, max_revisions: int = 32,
                 mas_bin: str = "mas", gateway_bin: str = "mas-gateway",
                 supervised: bool = True, workflow_versions: Mapping[str, tuple[Workflow, ...]] | None = None,
                 scheduler_policy=None):
        from .operations import DeploymentLedger, _file_lock
        self.state_dir = Path(state_dir).absolute()
        self.operations = operations or DeploymentLedger(self.state_dir)
        if Path(self.operations.state_dir).absolute() != self.state_dir:
            raise ServiceError("deployment_mismatch")
        if any(type(x) is not int or not 1 <= x <= 1_000_000 for x in (max_pending, max_tasks, max_revisions)):
            raise ServiceError("invalid_service_limits")
        self.max_pending, self.max_tasks, self.max_revisions = max_pending, max_tasks, max_revisions
        self.workflows = MappingProxyType(dict(workflows))
        self.grants = MappingProxyType({_text(p, "principal"): frozenset(names) for p, names in grants.items()})
        for name, workflow in self.workflows.items():
            _text(name, "workflow")
            if not isinstance(workflow, Workflow):
                raise ServiceError("invalid_workflow")
        self.identities = {name: workflow.identity for name, workflow in self.workflows.items()}
        self.workflow_versions = {}
        for name, versions in (workflow_versions or {}).items():
            _text(name, "workflow")
            for workflow in versions:
                if not isinstance(workflow, Workflow):
                    raise ServiceError("invalid_workflow")
                self.workflow_versions[name, workflow.identity] = workflow
        self.workflow_versions.update({(name, workflow.identity): workflow for name, workflow in self.workflows.items()})
        active = not self.operations.readonly and self.operations.admission_enabled
        with _file_lock(self.state_dir / ".deployment.lock", False):
            self.store = TaskStore(self.state_dir, deployment_id=self.operations.deployment_id,
                                   readonly=not active)
            from .definitions import DefinitionRegistry
            self.definitions = DefinitionRegistry(self.state_dir, readonly=not active)
            with self.store.transaction(readonly=True) as db:
                retained = {(row[0], row[1]) for row in db.execute("SELECT name,identity FROM workflows")}
                retained.update((row[0], row[1]) for row in db.execute("SELECT DISTINCT workflow,workflow_identity FROM tasks"))
            known_names = set(self.workflows) | {name for name, _identity in retained} | {name for name, _identity in self.workflow_versions}
            if any(not names <= known_names for names in self.grants.values()):
                raise ServiceError("invalid_grant")
            if active:
                for name, identity in retained:
                    self.definitions.remember("workflow", name, identity)
                for (name, identity), workflow in self.workflow_versions.items():
                    self.definitions.remember("workflow", name, identity, descriptor=workflow.descriptor,
                        implementation_digest=workflow.verified_implementation.digest,
                        implementation_manifest=workflow.verified_implementation.manifest,
                        current=self.identities.get(name) == identity)
            with self.store.transaction(readonly=not active) as db:
                for name, identity in self.identities.items():
                    row = db.execute("SELECT identity FROM workflows WHERE name=?", (name,)).fetchone()
                    if active:
                        db.execute("INSERT INTO workflows VALUES(?,?) ON CONFLICT(name) DO UPDATE SET identity=excluded.identity", (name, identity))
                    elif not row:
                        # Audit views require no executable registration. An
                        # unrecorded supplied registration grants no new work.
                        continue
        self.executor = executor or RunnerExecutor(self.state_dir, operations=self.operations,
            mas_bin=mas_bin, gateway_bin=gateway_bin, supervised=supervised)
        from .scheduler_capacity import SchedulerCapacity
        self.capacity = SchedulerCapacity(self.state_dir, scheduler_policy, readonly=not active)

    def _mutation(self):
        return service_mutation(self.operations, self.store)

    def definition_status(self, name, identity):
        """Inspect retained source and registered executables without loading code."""
        record = self.definitions.inspect("workflow", name, identity)
        if record is None:
            raise ServiceError("definition_not_found")
        registered = (name, identity) in self.workflow_versions
        return {"kind": "workflow", "name": name, "identity": identity,
                "current": self.definitions.current("workflow", name) == identity,
                "retired": bool(record["retired"]), "source_present": record["descriptor"] is not None,
                "registered": registered, "implementation_provenance": record["implementation_provenance"],
                "available": registered and not record["retired"] and record["implementation_provenance"] == "verified_source_package",
                "implementation_reverified_at_dispatch": True}

    def retire_definition(self, workflow, identity):
        """Trusted management operation; refuses current or referenced definitions."""
        from .operations import _file_lock
        with _file_lock(self.state_dir / '.deployment.lock', True, nonblocking=True):
            if self.operations.readonly or not self.operations.admission_enabled:
                raise ServiceError('audit_only')
            with self.store.transaction(readonly=True) as db:
                if db.execute("SELECT 1 FROM tasks t WHERE workflow=? AND workflow_identity=? "
                              "AND (state NOT IN ('completed','failed','cancelled') OR EXISTS "
                              "(SELECT 1 FROM executions e WHERE e.task_id=t.id AND e.state IN ('running','unknown'))) LIMIT 1",
                              (workflow, identity)).fetchone():
                    raise ServiceError('definition_in_use')
            if (self.state_dir / 'composition.sqlite').exists():
                from .workflow_store import WorkflowStore
                with WorkflowStore(self.state_dir, readonly=True).transaction(readonly=True) as db:
                    references = db.execute("SELECT plan,identity FROM instances i WHERE state NOT IN "
                        "('completed','failed','cancelled','expired') OR EXISTS(SELECT 1 FROM attempts a "
                        "WHERE a.instance=i.id AND a.state IN ('running','unknown'))").fetchall()
                for reference in references:
                    record = self.definitions.inspect('plan', reference['plan'], reference['identity'])
                    if (record is None or record['descriptor'] is None
                            or record['descriptor']['workflows'].get(workflow) == identity):
                        raise ServiceError('definition_in_use')
            self.definitions.retire('workflow', workflow, identity)
            return self.definitions.inspect('workflow', workflow, identity)

    def _authorise(self, principal, workflow=None):
        _text(principal, "principal")
        if principal not in self.grants or workflow is not None and workflow not in self.grants[principal]:
            raise ServiceError("forbidden")

    def _resolve(self, name, identity, *, verify=False):
        workflow = self.workflow_versions.get((name, identity))
        if workflow is None:
            return None
        if workflow.identity != identity:
            raise ServiceError("workflow_changed")
        definition = self.definitions.inspect("workflow", name, identity)
        if not definition or definition["retired"] or definition["implementation_provenance"] != "verified_source_package":
            return None
        if verify:
            try:
                workflow.verify_implementation()
            except ServiceError:
                return None
        return workflow

    def _required_workflow(self, name, identity):
        workflow = self._resolve(name, identity, verify=True)
        if workflow is None:
            raise ServiceError("definition_unavailable")
        return workflow

    def _owned(self, db, principal, task_id):
        self._authorise(principal)
        _text(task_id, "task_id")
        row = db.execute("SELECT * FROM tasks WHERE id=? AND principal=?", (task_id, principal)).fetchone()
        if row is None:
            raise ServiceError("task_not_found")
        task = decoded(row)
        self._authorise(principal, task["workflow"])
        return task

    def _receipt(self, task):
        from .service_timing import receipt
        try:
            workflow = self._resolve(task["workflow"], task["workflow_identity"])
        except ServiceError:
            workflow = None
        # Historical inspection remains available even when an executable was
        # removed. Only executable capabilities depend on the pinned version.
        return {"task_id": task["id"], 'workflow_identity': task['workflow_identity'], **{key: task[key] for key in (
            "state", "workflow", "request_key", "created_at", "updated_at",
            "cancel_requested", "result", "error", "revision", "metadata")},
            "request_namespace": task.get("request_namespace", "legacy"),
            "request_epoch": task.get("request_epoch", ""),
            "capabilities": {"cancel": True,
                "update": workflow is not None and workflow.continue_input is not None,
                "events": True},
            **({"archived": True, "result_available": False, "archived_at": task['archived_at'],
                "result_digest": task['result_digest']} if task.get('archived_at') is not None else {}), **receipt(task)}

    def _capacity(self, db, *, new=False):
        if new and self.store.retained_count(db) >= self.max_tasks:
            raise ServiceError("task_limit")
        if db.execute("SELECT count(*) FROM tasks WHERE state NOT IN ('completed','failed','cancelled')").fetchone()[0] >= self.max_pending:
            raise ServiceError("queue_full")

    @staticmethod
    def _digest(kind, task_id, workflow, input, metadata=None):
        return hashlib.sha256(canonical({"kind": kind, "task_id": task_id,
            "workflow": workflow, "input": input, "metadata": metadata or {}}).encode()).hexdigest()

    def _duplicate(self, db, principal, key, digest, scope=RequestScope()):
        row = db.execute("SELECT digest,task_id FROM requests WHERE principal=? AND request_key=? "
                         "AND request_namespace=? AND request_epoch=?", (principal, key, *scope.values)).fetchone()
        if row:
            if row["digest"] != digest:
                raise ServiceError("request_conflict")
            return self._receipt(self._owned(db, principal, row["task_id"]))
        return None

    @staticmethod
    def _run_id(task_id, revision):
        stem = task_id.removeprefix("task-")
        return f"run-service-{stem}-{revision}"

    def _validate_execution_input(self, workflow, task_id, revision, input):
        validate = getattr(self.executor, "validate_input", None)
        if validate is not None:
            if not callable(validate):
                raise ServiceError("invalid_executor_validator")
            validate(workflow, self._run_id(task_id, revision), input)

    @staticmethod
    def _execution(db, task_id, revision, input):
        run_id = DurableTaskService._run_id(task_id, revision)
        db.execute("INSERT INTO executions(task_id,revision,run_id,run_directory,input,state) VALUES(?,?,?,?,?,'queued')",
                   (task_id, revision, run_id, run_id, canonical(input)))

    def submit(self, principal, request_key, workflow, input, *, metadata=None, expected_identity=None, expected_deployment_id=None,
               request_epoch=None, _request_namespace=None):
        _text(workflow, "workflow")
        self._authorise(principal, workflow)
        _text(request_key, "request_key")
        value = _object(input)
        metadata = _object(metadata) if metadata is not None else {}
        digest = self._digest("submit", None, workflow, value, metadata)
        scope = RequestScope.supplied(_request_namespace, request_epoch)
        from .principal_quotas import submission_mutation, check_submission_quota
        with epoch_guard(self.operations, principal, scope), submission_mutation(self.operations, self.store, principal) as db:
            scope.check_owner(db, principal)
            if expected_deployment_id is not None and expected_deployment_id != self.operations.deployment_id:
                raise ServiceError('deployment_identity_mismatch')
            duplicate = self._duplicate(db, principal, request_key, digest, scope)
            if duplicate:
                if expected_identity is not None and duplicate['workflow_identity'] != expected_identity:
                    raise ServiceError('workflow_identity_mismatch')
                return duplicate
            if workflow not in self.identities:
                raise ServiceError("definition_unavailable")
            selected = self._required_workflow(workflow, self.identities[workflow])
            if expected_identity is not None and selected.identity != expected_identity:
                raise ServiceError('workflow_identity_mismatch')
            alias = db.execute("SELECT identity FROM workflows WHERE name=?", (workflow,)).fetchone()
            if alias is None or alias[0] != selected.identity:
                raise ServiceError("workflow_changed")
            self.operations.check_storage_admission()
            check_submission_quota(self.operations, principal)
            self._capacity(db, new=True)
            task_id, timestamp = "task-" + uuid.uuid4().hex, now()
            self._validate_execution_input(selected, task_id, 0, value)
            clock = selected.timing_policy.start(timestamp) if selected.timing_policy else None
            db.execute("INSERT INTO tasks(id,principal,request_key,workflow,workflow_identity,state,input,metadata,created_at,updated_at,clock,request_namespace,request_epoch) VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?,?)",
                (task_id, principal, request_key, workflow, self.identities[workflow], canonical(value), canonical(metadata), timestamp, timestamp,
                 canonical(clock) if clock else None, *scope.values))
            db.execute("INSERT INTO requests VALUES(?,?,?,?,?,?,?)", (principal, request_key, digest, task_id, "submit", *scope.values))
            self._execution(db, task_id, 0, value)
            event(db, task_id, "submitted", {"state": "queued", "revision": 0})
            return self._receipt(self._owned(db, principal, task_id))

    def inspect(self, principal, task_id):
        with self.store.transaction(readonly=True) as db:
            return self._receipt(self._owned(db, principal, task_id))

    def discover(self, principal):
        self._authorise(principal)
        registrations = []
        for name in sorted(self.grants[principal]):
            workflow = self.workflows.get(name)
            registrations.append({'name': name, 'kind': 'workflow', 'available': workflow is not None,
                'identity': workflow.identity if workflow else None,
                'contract': workflow.contract_descriptor if workflow else None})
        return {'format': 'mas-task-service/1', 'principal': principal, 'deployment_id': self.operations.deployment_id,
                'capabilities': ['submit', 'inspect', 'result', 'cancel', 'update', 'events_page', 'recover', 'pinned_identity', 'request_epochs'],
                'registrations': registrations}

    def result(self, principal, task_id):
        receipt = self.inspect(principal, task_id)
        if receipt["state"] != "completed" or receipt.get('archived'):
            raise ServiceError("result_unavailable")
        return receipt["result"]

    def cancel(self, principal, task_id):
        """Close local queued/paused work; record intent for executing work.

        A paused revision has already stopped executing. Closing its future
        continuation does not cancel any separately tracked remote task, whose
        durable receipt and reconciliation obligations remain intact.
        """
        with self._mutation() as db:
            task = self._owned(db, principal, task_id)
            can_close = task["state"] in {"queued", "blocked", *INTERRUPTED}
            if task["state"] in TERMINAL or (task["cancel_requested"] and not can_close):
                return self._receipt(task)
            # Repeated cancellation also repairs a paused record written by an
            # older worker after cancellation intent was already recorded.
            state = "cancelled" if can_close else task["state"]
            db.execute("UPDATE tasks SET cancel_requested=1,state=?,updated_at=? WHERE id=?", (state, now(), task_id))
            if state == "cancelled":
                from .service_timing import transition
                transition(db, 'tasks', task, 'terminal')
                db.execute("UPDATE executions SET state='cancelled' WHERE task_id=? AND revision=?", (task_id, task["revision"]))
            event(db, task_id, "cancel_requested", {"state": state, "confirmed": state == "cancelled",
                                                    "scope": "local_workflow"})
            return self._receipt(self._owned(db, principal, task_id))

    def update(self, principal, task_id, request_key, input, *, metadata=None, expected_identity=None, expected_deployment_id=None):
        _text(request_key, "request_key")
        value = _object(input)
        metadata = _object(metadata) if metadata is not None else {}
        with self.store.transaction(readonly=True) as db:
            scope = RequestScope.retained(self._owned(db, principal, task_id))
        with epoch_guard(self.operations, principal, scope), self._mutation() as db:
            task = self._owned(db, principal, task_id)
            scope.check_owner(db, principal)
            if expected_deployment_id is not None and expected_deployment_id != self.operations.deployment_id:
                raise ServiceError('deployment_identity_mismatch')
            if expected_identity is not None and expected_identity != task['workflow_identity']:
                raise ServiceError('workflow_identity_mismatch')
            digest = self._digest("update", task_id, task["workflow"], value, metadata)
            duplicate = self._duplicate(db, principal, request_key, digest, scope)
            if duplicate:
                return duplicate
            from .service_timing import require_open, transition
            require_open(task)
            workflow = self._required_workflow(task["workflow"], task["workflow_identity"])
            if workflow.continue_input is None:
                raise ServiceError("unsupported_capability")
            if task["state"] not in INTERRUPTED or task["cancel_requested"]:
                raise ServiceError("invalid_task_state")
            if task["revision"] + 1 >= self.max_revisions:
                raise ServiceError("revision_limit")
            self.operations.check_storage_admission()
            # Pure application mapping, never a network effect or model call.
            try:
                combined = _object(workflow.continue_input(task["result"] or {}, value))
            except Exception:
                raise ServiceError("invalid_continuation") from None
            revision = task["revision"] + 1
            self._validate_execution_input(workflow, task_id, revision, combined)
            db.execute("INSERT INTO requests VALUES(?,?,?,?,?,?,?)", (principal, request_key, digest, task_id, "update", *scope.values))
            self._execution(db, task_id, revision, combined)
            transition(db, 'tasks', task, 'queued')
            db.execute("UPDATE tasks SET state='queued',revision=?,error=NULL,updated_at=? WHERE id=?", (revision, now(), task_id))
            event(db, task_id, "continued", {"state": "queued", "revision": revision})
            return self._receipt(self._owned(db, principal, task_id))

    def events(self, principal, task_id, after=0):
        """Return a byte-bounded prefix; resume after the last returned event ID."""
        if type(after) is not int or not 0 <= after <= 2**63 - 1:
            raise ServiceError("invalid_cursor")
        with self.store.transaction(readonly=True) as db:
            task = self._owned(db, principal, task_id)
            self.store.check_event_cursor(task, after)
            values = (decoded(row) for row in db.execute(
                "SELECT * FROM events WHERE task_id=? AND id>? ORDER BY id LIMIT 1000", (task_id, after)))
            return _bounded_items(values)[0]

    def events_page(self, principal, task_id, after=0, limit=100):
        """Return a byte-bounded page with an explicit resume position.

        Events are append-only. The cursor is a position, not a snapshot;
        callers can poll again after an empty page to observe later events.
        """
        from .observability import event_page_arguments
        event_page_arguments(after, limit)
        with self.store.transaction(readonly=True) as db:
            task = self._owned(db, principal, task_id)
            self.store.check_event_cursor(task, after)
            values = (decoded(row) for row in db.execute(
                "SELECT * FROM events WHERE task_id=? AND id>? ORDER BY id LIMIT ?",
                (task_id, after, limit + 1)))
            selected, more = _bounded_items(values, limit=limit, overhead=128)
            return {"events": selected, "next_after": selected[-1]["id"] if selected else after,
                    "has_more": more}

    def list_tasks(self, principal, limit=100, offset=0):
        """Return a byte-bounded prefix; advance offset by the returned count."""
        self._authorise(principal)
        if type(limit) is not int or not 1 <= limit <= 1000 or type(offset) is not int or not 0 <= offset <= 2**63 - 1:
            raise ServiceError("invalid_pagination")
        with self.store.transaction(readonly=True) as db:
            # Filter before pagination so revoking a workflow cannot reveal its
            # tasks, nor change another workflow's page boundaries by accident.
            allowed = sorted(self.grants[principal])
            if not allowed:
                return []
            values = (self._receipt(decoded(row)) for row in db.execute(
                "SELECT * FROM tasks WHERE principal=? AND workflow IN (" + ",".join("?" * len(allowed)) + ") ORDER BY created_at,id LIMIT ? OFFSET ?",
                (principal, *allowed, limit, offset)))
            return _bounded_items(values)[0]

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        """Keyset page ordered by last update, scoped before counts and paging.

        Cursors identify an observed position, not a historical snapshot: a
        task updated during traversal can move to an earlier page. The
        ``updated_after`` lower timestamp bound is inclusive. Pages are bounded
        by both count and encoded bytes; ``next`` covers either truncation.
        At most 64 metadata predicates may be supplied in one query.
        """
        self._authorise(principal)
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ServiceError("invalid_pagination")
        if state is not None and (type(state) is not str or state not in STATES):
            raise ServiceError("invalid_task_state")
        if updated_after is not None and (type(updated_after) is not int or not 0 <= updated_after <= 2**63 - 1):
            raise ServiceError("invalid_timestamp")
        if after is not None and (type(after) not in (tuple, list) or len(after) != 2
                or type(after[0]) is not int or not 0 <= after[0] <= 2**63 - 1
                or type(after[1]) is not str or not after[1]):
            raise ServiceError("invalid_cursor")
        filters = _object(metadata) if metadata is not None else {}
        if len(filters) > 64 or any(not key or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for c in key)
                                   or type(value) is not str for key, value in filters.items()):
            raise ServiceError("invalid_metadata_filter")
        allowed = sorted(self.grants[principal])
        if not allowed:
            return {"tasks": [], "total_size": 0, "next": None}
        clauses = ["principal=?", "workflow IN (" + ",".join("?" * len(allowed)) + ")"]
        values = [principal, *allowed]
        if state is not None:
            clauses.append("state=?")
            values.append(state)
        if updated_after is not None:
            clauses.append("updated_at>=?")
            values.append(updated_after)
        for key, value in sorted(filters.items()):
            clauses.append("json_extract(metadata,?)=?")
            values.extend(("$." + key, value))
        with self.store.transaction(readonly=True) as db:
            total = db.execute("SELECT count(*) FROM tasks WHERE " + " AND ".join(clauses), values).fetchone()[0]
            if after is not None:
                clauses.append("(updated_at<? OR (updated_at=? AND id<?))")
                values.extend((after[0], after[0], after[1]))
            rows = db.execute("SELECT * FROM tasks WHERE " + " AND ".join(clauses) + " ORDER BY updated_at DESC,id DESC LIMIT ?",
                              [*values, page_size + 1])
            # Reserve ample space for total_size and the bounded task cursor.
            selected, more = _bounded_items((self._receipt(decoded(row)) for row in rows),
                                           limit=page_size, overhead=1024)
            cursor = [selected[-1]["updated_at"], selected[-1]["task_id"]] if more else None
            return {"tasks": selected, "total_size": total, "next": cursor}

    def recover(self, principal, task_id):
        """Queue a previously unknown revision after evidenced journal resolution.

        The existing admin resolve-invocation operation owns that evidence. This
        method cannot authorise a retry and never changes invocation records.
        """
        from .journal import inspect_run
        with self._mutation() as db:
            task = self._owned(db, principal, task_id)
            if task["state"] != "unknown":
                raise ServiceError("invalid_task_state")
            execution = db.execute("SELECT * FROM executions WHERE task_id=? AND revision=?", (task_id, task["revision"])).fetchone()
            try:
                report = inspect_run(self.state_dir / execution["run_directory"])
            except Exception:
                raise ServiceError("reconciliation_required") from None
            if report["recovery_required"]:
                raise ServiceError("reconciliation_required")
            from .service_timing import status, transition
            timing = status(task)
            if timing and timing['expired']:
                transition(db, 'tasks', task, 'terminal')
                reason = {'code': timing['expired'][0], 'expired': timing['expired'], 'effect_reconciliation': 'resolved'}
                db.execute("UPDATE tasks SET state='failed',error=?,updated_at=? WHERE id=?", (canonical(reason), now(), task_id))
                db.execute("UPDATE executions SET state='failed' WHERE task_id=? AND revision=?", (task_id, task['revision']))
                event(db, task_id, 'recovery_closed', reason)
                return self._receipt(self._owned(db, principal, task_id))
            transition(db, 'tasks', task, 'queued')
            db.execute("UPDATE tasks SET state='queued',error=NULL,updated_at=? WHERE id=?", (now(), task_id))
            db.execute("UPDATE executions SET state='queued' WHERE task_id=? AND revision=?", (task_id, task["revision"]))
            event(db, task_id, "recovery_queued", {"state": "queued", "revision": task["revision"]})
            return self._receipt(self._owned(db, principal, task_id))

    def pending(self):
        """Authoritative durable scheduling intent, without request payloads."""
        with self.store.transaction(readonly=True) as db:
            from .service_timing import status
            rows = [dict(row) for row in db.execute("SELECT t.id AS task_id,t.principal,t.created_at "
                "FROM tasks t JOIN executions e ON e.task_id=t.id AND e.revision=t.revision "
                "WHERE e.state IN ('queued','running','blocked') ORDER BY e.sequence")]
            for row in db.execute("SELECT id,principal,created_at,clock FROM tasks WHERE state IN ('input_required','auth_required') AND clock IS NOT NULL"):
                item = decoded(row)
                if status(item)['expired']:
                    rows.append({'task_id': item['id'], 'principal': item['principal'], 'created_at': item['created_at']})
        return self.capacity.fair_order(rows)

    def recovery_runs(self, principal, task_id):
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, task_id)
            return [{'path': None, **dict(row)} for row in db.execute(
                'SELECT revision,run_directory,state FROM executions WHERE task_id=? ORDER BY revision',
                (task_id,))]

    @staticmethod
    def _protocol_event(db, task_id, execution, item):
        # Publish bounded envelope metadata, never request/result bodies.
        keys = ('sender', 'recipient', 'kind', 'message_id', 'task_id')
        data = {key: item[key] for key in keys if key in item}
        if not isinstance(data.get('message_id'), str) or any(type(value) is not str or len(value) > 256 for value in data.values()):
            raise ServiceError('invalid_protocol_event')
        data.update(run_id=execution['run_id'], revision=execution['revision'])
        if db.execute("SELECT 1 FROM events WHERE task_id=? AND event='protocol' "
                      "AND json_extract(data,'$.run_id')=? AND json_extract(data,'$.message_id')=? LIMIT 1",
                      (task_id, data['run_id'], data['message_id'])).fetchone():
            return
        event(db, task_id, 'protocol', data)
        db.execute('UPDATE tasks SET updated_at=? WHERE id=?', (now(), task_id))

    def run_once(self, task_id=None):
        from .operations import _file_lock
        descriptor, slot = None, None
        try:
            with _file_lock(self.state_dir / ".deployment.lock", False):
                if self.operations.readonly or not self.operations.admission_enabled:
                    raise ServiceError("audit_only")
                with self.store.transaction() as db:
                    query = "SELECT e.*,t.principal FROM executions e JOIN tasks t ON t.id=e.task_id AND t.revision=e.revision WHERE (e.state IN ('queued','running','blocked') OR (e.state IN ('input_required','auth_required') AND t.clock IS NOT NULL))"
                    values = ()
                    if task_id is not None:
                        _text(task_id, 'task_id')
                        query += ' AND t.id=?'
                        values = (task_id,)
                    rows = self.capacity.fair_order(db.execute(query + ' ORDER BY e.sequence', values).fetchall())
                    last_blocked = None
                    busy = False
                    for row in rows:
                        execution = decoded(row)
                        task = decoded(db.execute("SELECT * FROM tasks WHERE id=?", (execution["task_id"],)).fetchone())
                        from .service_timing import status, transition, execution_deadline
                        timing = status(task)
                        if task['state'] in INTERRUPTED and not (timing and timing['expired']):
                            continue
                        lock = self.state_dir / ('.task-owner-' + hashlib.sha256(task['id'].encode()).hexdigest() + '.lock')
                        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
                        try:
                            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            os.close(descriptor)
                            descriptor = None
                            busy = True
                            continue
                        if timing and timing['expired']:
                            from .bootstrap import requires_reconciliation
                            uncertain = requires_reconciliation(self.state_dir / execution['run_directory'], execution['run_id'],
                                                                operations=self.operations)
                            state = 'unknown' if uncertain else 'failed'
                            reason = {'code': 'reconciliation_required' if uncertain else timing['expired'][0],
                                      'expired': timing['expired']}
                            transition(db, 'tasks', task, 'paused' if uncertain else 'terminal')
                            db.execute('UPDATE tasks SET state=?,error=?,updated_at=? WHERE id=?',
                                       (state, canonical(reason), now(), task['id']))
                            db.execute('UPDATE executions SET state=? WHERE sequence=?', (state, execution['sequence']))
                            event(db, task['id'], 'deadline_expired', reason)
                            return self._receipt(self._owned(db, task['principal'], task['id']))
                        try:
                            workflow = self._resolve(task["workflow"], task["workflow_identity"], verify=True)
                        except ServiceError as error:
                            if error.code != "workflow_changed":
                                raise
                            workflow = None
                        if workflow is not None:
                            slot = self.capacity.acquire(execution['run_id'], task['principal'], task['workflow'],
                                                         resources=getattr(workflow, 'resource_keys', ()))
                            if slot is None:
                                os.close(descriptor)
                                descriptor = None
                                continue
                            if execution["state"] == "blocked":
                                self.store.restore_definition(db, task["id"], execution["sequence"])
                            break
                        if execution["state"] != "blocked":
                            started = execution["state"] == "running" or (self.state_dir / execution["run_directory"]).exists()
                            self.store.block_definition(db, task, execution, started=started)
                            last_blocked = self._receipt(decoded(db.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()))
                        os.close(descriptor)
                        descriptor = None
                    else:
                        if busy and last_blocked is None:
                            raise ServiceError('worker_busy')
                        return last_blocked
                    if task["workflow"] not in self.grants.get(task["principal"], ()):
                        # Revocation prevents further dispatch; it says nothing
                        # about effects that escaped before an interrupted
                        # worker. Keep those outcomes unresolved, including a
                        # previously recovered revision queued on its old run.
                        started = execution["state"] == "running" or (self.state_dir / execution["run_directory"]).exists()
                        state = "unknown" if started else "failed"
                        code = "permission_revoked_reconciliation_required" if started else "permission_revoked"
                        db.execute("UPDATE tasks SET state=?,error=?,updated_at=? WHERE id=?",
                                   (state, canonical({"code": code}), now(), task["id"]))
                        db.execute("UPDATE executions SET state=? WHERE sequence=?", (state, execution["sequence"]))
                        event(db, task["id"], "state_changed", {"state": state, "reason": code})
                        return self._receipt(decoded(db.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()))
                    db.execute("UPDATE tasks SET state='running',updated_at=? WHERE id=?", (now(), task["id"]))
                    transition(db, 'tasks', task, 'running', branches=1)
                    deadline = execution_deadline(task)
                    db.execute("UPDATE executions SET state='running' WHERE sequence=?", (execution["sequence"],))
                    if task["state"] != "running":
                        event(db, task["id"], "running", {"state": "running", "revision": task["revision"], "run_id": execution["run_id"]})

                def cancelled():
                    with self.store.transaction(readonly=True) as db:
                        return bool(db.execute("SELECT cancel_requested FROM tasks WHERE id=?", (task["id"],)).fetchone()[0])
                def progress(item):
                    with self.store.transaction() as db:
                        self._protocol_event(db, task['id'], execution, item)
                # BaseException and infrastructure failures leave running intent
                # for replay. They never create a new execution or run identity.
                from .telemetry import record, span
                correlations = {'task_id': task['id'], 'run_id': execution['run_id'],
                                'assignment_id': task['id'] + ':' + str(execution['revision'])}
                if task['state'] in {'queued', 'blocked'}:
                    record(self.state_dir, 'queue', max(0, now() - task['updated_at']) * 1000,
                           **correlations)
                with span(self.state_dir, 'dispatch', **correlations):
                    outcome = self.executor.execute(workflow, execution | {
                        "principal": task["principal"], "workflow": task["workflow"], 'execution_deadline': deadline,
                        'event_sink': progress},
                        cancelled=cancelled, ownership_descriptor=(descriptor, slot.descriptor))
                if not isinstance(outcome, ExecutionResult) or outcome.state not in STATES - {"queued", "running"}:
                    raise ServiceError("invalid_executor_result")
                output = _object(outcome.result) if outcome.result is not None else None
                error = _object(outcome.error) if outcome.error is not None else None
                with self.store.transaction() as db:
                    # Cancellation can arrive after the executor returns. A
                    # paused revision has stopped and can safely lose its future
                    # continuation, but an uncertain effect remains unknown.
                    requested = db.execute("SELECT cancel_requested FROM tasks WHERE id=?", (task["id"],)).fetchone()[0]
                    state = "cancelled" if requested and outcome.state in INTERRUPTED else outcome.state
                    current = self._owned(db, task['principal'], task['id'])
                    timing = status(current)
                    if timing and timing['expired'] and state != 'unknown':
                        state, error = 'failed', {'code': timing['expired'][0], 'expired': timing['expired']}
                    transition(db, 'tasks', current, 'paused' if state in INTERRUPTED | {'unknown', 'blocked'} else 'terminal')
                    for item in outcome.events:
                        self._protocol_event(db, task['id'], execution, item)
                    db.execute("UPDATE tasks SET state=?,result=?,error=?,updated_at=? WHERE id=?", (
                        state, canonical(output) if output is not None else None,
                        canonical(error) if error is not None else None, now(), task["id"]))
                    db.execute("UPDATE executions SET state=? WHERE sequence=?", (state, execution["sequence"]))
                    event(db, task["id"], "state_changed", {"state": state, "revision": task["revision"]})
                    return self._receipt(decoded(db.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()))
        finally:
            # close (rather than LOCK_UN) preserves the inherited lock until a
            # surviving guardian releases its copy after stopping descendants.
            if slot is not None:
                slot.close()
            if descriptor is not None:
                os.close(descriptor)
