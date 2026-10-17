"""Dependency-neutral host interfaces and values shared by execution strategies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol, runtime_checkable


class RunnerError(RuntimeError):
    """A fixed diagnostic safe to show without disclosing provider details."""


class AdapterError(RunnerError):
    """An adapter failed; inspect the durable task and invocation records."""


class RecoveryRequired(AdapterError):
    """An invocation may have affected the outside world; operator action required."""


class AdapterTimeout(AdapterError):
    """Local execution exceeded its inherited deadline."""


class AdapterInterrupted(AdapterError):
    """Host shutdown interrupted local execution."""


class KnownInvocationFailure(AdapterError):
    """A recorded unusable outcome; only an explicit host policy may retry it."""

    def __init__(self, outcome: dict, call_id: str | None = None, *, completed_output: dict | None = None):
        from .provider_outcomes import validate_provider_outcome, validate_completed_output
        self.outcome = validate_provider_outcome(outcome)
        self.completed_output = validate_completed_output(self.outcome, completed_output)
        self.call_id = call_id
        super().__init__("provider execution: " + self.outcome["category"])


def validate_invocation_diagnostic(value: object) -> dict:
    """Copy the finite public schema, excluding raw exception text and secrets."""
    if type(value) is dict and value.get("format") == "mas-provider-diagnostic/1":
        from .provider_outcomes import validate_provider_diagnostic
        return validate_provider_diagnostic(value)
    if (type(value) is not dict or set(value) != {"provider", "stage", "cleanup_failed"}
            or type(value["provider"]) is not str or value["provider"] != "cursor-local"
            or type(value["stage"]) is not str
            or value["stage"] not in {"launch", "create", "send", "stream", "wait", "result", "cleanup"}
            or type(value["cleanup_failed"]) is not bool):
        raise ValueError("invalid invocation diagnostic")
    return dict(value)


class InvocationFailure(RuntimeError):
    """An invocation error carrying only an optional validated public stage."""
    def __init__(self, message: str, *, diagnostic: dict | None = None):
        super().__init__(message)
        self.diagnostic = (validate_invocation_diagnostic(diagnostic)
                           if diagnostic is not None else None)


def invocation_failure_diagnostic(error: BaseException) -> dict | None:
    """Unrecognised or altered exception attributes never cross host boundaries."""
    if isinstance(error, InvocationFailure):
        try:
            if error.diagnostic is not None:
                return validate_invocation_diagnostic(error.diagnostic)
        except (AttributeError, TypeError, ValueError):
            pass
    return None


@dataclass(frozen=True)
class InvocationTicket:
    call_id: str
    cached_output: dict | None
    execute: bool
    step_key: str


@dataclass(frozen=True)
class TaskRequest:
    sender: str
    recipient: str
    operation: str
    input: dict
    task_id: str
    message_id: str


@dataclass(frozen=True)
class Outcome:
    output: dict
    status: str = "completed"


@dataclass(frozen=True)
class Invocation:
    output: dict
    evidence: dict


@runtime_checkable
class AdapterContext(Protocol):
    """Capabilities available to adapters under every execution strategy."""

    @property
    def actor(self) -> str: ...

    @property
    def invocation_id(self) -> str | None: ...

    @property
    def remaining_seconds(self) -> float: ...

    def request(self, recipient: str, operation: str, input: dict) -> Outcome: ...

    def assessment(self, operation: str, data: dict) -> dict: ...

    def invoke(self, call: Callable[[], Invocation], *, input: dict | None = None,
               reservation_microusd: int | None = None) -> dict: ...


class Adapter(Protocol):
    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome: ...


class HostContext(AdapterContext, Protocol):
    """Additional admission/checkpoint operations owned by the host process."""

    @property
    def ownership_descriptors(self) -> tuple[int, ...]:
        """Locks retained by the guardian until all adapter execution stops."""
        ...

    def prepare_invocation(self, *, input: dict | None = None,
                           reservation_microusd: int | None = None,
                           retry_of: str | None = None) -> InvocationTicket: ...

    def complete_invocation(self, ticket: InvocationTicket, invocation: Invocation) -> dict: ...

    def fail_invocation(self, ticket: InvocationTicket, *, diagnostic: dict | None = None) -> None: ...

    def reject_invocation(self, ticket: InvocationTicket, outcome: dict, *,
                          completed_output: dict | None = None) -> None: ...

    def retry_invocation(self, call_id: str) -> dict | None: ...


class AdapterExecutor(Protocol):
    """Execute an adapter without changing its request or invocation contract."""

    def execute(self, adapter: Adapter, context: HostContext, request: TaskRequest, *,
                deadline: float, tick: Callable[[], None] | None = None) -> Outcome: ...
