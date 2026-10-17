"""Domain-independent inference and an admitted, contract-checked adapter.

Provider methods are low-level effect implementations. Application code uses
StructuredAgentAdapter so every call is reserved and journalled by the host.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

from .codec import canonical_json, integer_json, is_actor_identity
from .contracts import AdapterContext, Invocation, Outcome, TaskRequest
from .operation_contracts import JsonObjectSchema, OperationContract
from .provider_contract import ProviderError
from .provider_outcomes import completed_unusable, not_sent

IDENTIFIER = re.compile(r"[A-Za-z0-9_.:/-]{1,200}")
MAX_OUTPUT_BYTES = 32_768


def _copy(value: Any) -> Any:
    try:
        return json.loads(integer_json(value))
    except Exception:
        raise ProviderError("structured inference requires bounded integer JSON") from None


def _canonical(value: Any) -> str:
    """Apply the provider diagnostic to the shared descriptor encoding."""
    try:
        return canonical_json(value)
    except Exception:
        raise ProviderError("structured inference requires bounded integer JSON") from None


def validate_trace_id(request_id: str | None) -> None:
    if request_id is not None and (type(request_id) is not str
            or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,128}", request_id)):
        raise ProviderError("invalid provider request trace identifier")


@dataclass(frozen=True, init=False, slots=True)
class StructuredRequest:
    """Copy input and schema at construction; callers cannot mutate the binding."""

    _source: str
    _schema: JsonObjectSchema

    def __init__(self, input: dict, instructions: str, output_schema: dict, *,
                 schema_name: str, schema_version: str):
        if type(input) is not dict:
            raise ProviderError("structured inference input must be an object")
        if (type(instructions) is not str or not instructions.strip()
                or len(instructions.encode("utf-8")) > 16_384):
            raise ProviderError("structured inference requires bounded instructions")
        if (type(schema_name) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", schema_name)
                or type(schema_version) is not str or not IDENTIFIER.fullmatch(schema_version)):
            raise ProviderError("structured output requires an explicit schema name and version")
        if type(output_schema) is not dict or output_schema.get("type") != "object":
            raise ProviderError("structured output schema must declare an object")
        schema = JsonObjectSchema(output_schema)
        descriptor = {"input": _copy(input), "instructions": instructions,
                      "output_schema": schema.descriptor, "schema_name": schema_name,
                      "schema_version": schema_version, "contract": "structured-inference/1"}
        object.__setattr__(self, "_source", canonical_json(descriptor))
        object.__setattr__(self, "_schema", schema)

    @property
    def descriptor(self) -> dict:
        return json.loads(self._source)

    @property
    def identity(self) -> str:
        return hashlib.sha256(self._source.encode("utf-8")).hexdigest()

    def validate_output(self, value: dict) -> dict:
        try:
            checked = self._schema.validate(value)
            integer_json(checked, MAX_OUTPUT_BYTES)
            return checked
        except Exception:
            raise ProviderError("provider output does not satisfy the structured contract") from None

    def parse_output(self, text: Any) -> dict:
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate field")
                result[key] = value
            return result

        def reject(_):
            raise ValueError("non-integer number")

        try:
            if type(text) is not str or len(text.encode("utf-8")) > MAX_OUTPUT_BYTES:
                raise ValueError("invalid body")
            output = json.loads(text, object_pairs_hook=unique,
                                parse_float=reject, parse_constant=reject)
            return self.validate_output(output)
        except Exception:
            raise ProviderError("provider returned an invalid structured output") from None


class StructuredProvider(Protocol):
    @property
    def reservation_microusd(self) -> int: ...

    def descriptor(self) -> dict: ...

    def infer(self, request: StructuredRequest, *, request_id: str | None = None) -> Invocation: ...


def structured_binding(provider: StructuredProvider) -> tuple[dict, int]:
    """Reject missing identity and missing admission allocations before invoking."""
    try:
        amount = provider.reservation_microusd
        if type(amount) is not int or not 0 <= amount <= 100_000_000:
            raise ValueError("reservation")
        descriptor = _copy(provider.descriptor())
        if (not callable(provider.infer) or type(descriptor) is not dict
                or any(type(descriptor.get(key)) is not str or not descriptor[key]
                       for key in ("provider", "model", "role", "contract", "implementation"))
                or descriptor["contract"] != "structured-inference/1"
                or type(descriptor.get("reservation_microusd")) is not int
                or descriptor["reservation_microusd"] != amount):
            raise ValueError("identity")
        return descriptor, amount
    except Exception:
        raise ProviderError("structured provider identity or reservation is invalid") from None


@dataclass(frozen=True, init=False, slots=True)
class StructuredAgentAdapter:
    """Immutable operation configuration and one explicitly admitted invocation.

Providers remain trusted implementations. Their declared runtime configuration
is checked against its registration snapshot before admission and execution.
"""

    provider: StructuredProvider
    contract: OperationContract
    actor: str
    instructions: str
    _provider_source: str

    def __init__(self, provider: StructuredProvider, *, instructions: str,
                 contract: OperationContract, actor: str | None = None):
        if not isinstance(contract, OperationContract):
            raise ProviderError("structured adapter requires an operation contract")
        descriptor, _ = structured_binding(provider)
        actor = descriptor["role"] if actor is None else actor
        if not is_actor_identity(actor):
            raise ProviderError("structured adapter requires a valid actor identity")
        # Validate instructions and output schema before any task can be admitted.
        StructuredRequest({}, instructions, contract.output_schema,
                          schema_name="operation_result", schema_version=contract.operation)
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "contract", contract)
        object.__setattr__(self, "actor", actor)
        object.__setattr__(self, "instructions", instructions)
        object.__setattr__(self, "_provider_source", _canonical(descriptor))

    def descriptor(self) -> dict:
        return _copy({"adapter": "structured-agent/1", "actor": self.actor,
                      "provider": json.loads(self._provider_source),
                      "instructions": self.instructions, "operation": self.contract.descriptor})

    def _binding(self) -> tuple[dict, int]:
        descriptor, reservation = structured_binding(self.provider)
        if _canonical(descriptor) != self._provider_source:
            raise ProviderError("structured provider binding changed after registration")
        return descriptor, reservation

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        if (context.actor != self.actor or request.recipient != self.actor
                or request.operation != self.contract.operation):
            raise ProviderError("structured adapter actor or operation does not match its binding")
        self.contract.authorise(request.sender)
        supplied = self.contract.validate_input(request.input)
        inference = StructuredRequest(supplied, self.instructions, self.contract.output_schema,
                                      schema_name="operation_result", schema_version=self.contract.operation)
        _, reservation = self._binding()
        effect = _copy({"adapter": self.descriptor(), "inference": inference.descriptor})
        def invoke_checked() -> Invocation:
            try:
                self._binding()
            except ProviderError:
                raise not_sent("custom", "invalid_request") from None
            invocation = self.provider.infer(inference, request_id=context.invocation_id)
            self._binding()
            if not isinstance(invocation, Invocation):
                raise ProviderError("structured provider returned an invalid invocation")
            if type(invocation.evidence) is not dict:
                raise ProviderError("structured provider returned invalid evidence")
            try:
                checked = inference.validate_output(invocation.output)
            except ProviderError:
                raise completed_unusable("custom", "invalid_output", invocation.evidence) from None
            return Invocation(checked, _copy(invocation.evidence))

        output = context.invoke(invoke_checked, input=effect, reservation_microusd=reservation)
        return Outcome(self.contract.validate_output(inference.validate_output(output)))


class MockStructuredProvider:
    """An explicit fixture result: deterministic, offline and recovery-bound."""

    reservation_microusd = 0

    def __init__(self, role: str, output: dict, *, model: str = "deterministic-structured"):
        if any(type(value) is not str or not IDENTIFIER.fullmatch(value) for value in (role, model)):
            raise ProviderError("mock requires explicit valid role and model identities")
        self.role, self.model = role, model
        if type(output) is not dict:
            raise ProviderError("mock structured output must be an object")
        self._output = integer_json(output)

    def descriptor(self) -> dict:
        return {"provider": "mock", "model": self.model, "role": self.role,
                "contract": "structured-inference/1", "implementation": "mock-structured/1",
                "reservation_microusd": 0, "fixture": json.loads(self._output)}

    def infer(self, request: StructuredRequest, *, request_id: str | None = None) -> Invocation:
        validate_trace_id(request_id)
        output = request.validate_output(json.loads(self._output))
        return Invocation(output, {"provider": "mock", "model": self.model,
                                  "role": self.role, "network": False,
                                  "request_sha256": request.identity})
