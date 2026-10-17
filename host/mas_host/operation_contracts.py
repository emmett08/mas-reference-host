"""Versioned application operations above the unchanged Rust message envelope.

Registrations and schemas are trusted host configuration, never executable
content selected by a request. Validation uses the optional ``jsonschema``
dependency, a fixed dialect and an empty reference registry. No schema fetch,
module import from configuration or expression evaluation is performed.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from types import MappingProxyType
from typing import Iterable

from .codec import canonical_json, integer_json, is_actor_identity, json_identity
from .contracts import Adapter, AdapterContext, AdapterError, Outcome, RunnerError, TaskRequest


class ContractError(AdapterError):
    """Fixed public diagnostics; schema errors never expose application data."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code.replace("_", " "))


def _name(value: str) -> str:
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,62}", value) is None:
        raise ContractError("invalid_contract_identity")
    return value


def _actor(value: str) -> str:
    if not is_actor_identity(value):
        raise ContractError("invalid_actor_identity")
    return value


_DIALECT = "https://json-schema.org/draft/2020-12/schema"
_SCHEMA_MAPS = frozenset({"$defs", "definitions", "properties", "patternProperties", "dependentSchemas"})
_SCHEMA_LISTS = frozenset({"allOf", "anyOf", "oneOf", "prefixItems"})
_SCHEMA_VALUES = frozenset({"additionalProperties", "unevaluatedProperties", "propertyNames", "contains",
                            "items", "not", "if", "then", "else", "unevaluatedItems", "contentSchema"})


def _local_schema(schema: object) -> None:
    """Walk schema locations, not arbitrary examples/default application data."""
    if type(schema) is not dict:
        return
    if "$id" in schema:
        # Local fragments must keep referring to this immutable schema document.
        raise ContractError("schema_resource_ids_disabled")
    if schema.get("$schema", _DIALECT) != _DIALECT:
        raise ContractError("unsupported_schema_dialect")
    for keyword in ("$ref", "$dynamicRef"):
        if keyword in schema and (type(schema[keyword]) is not str or not schema[keyword].startswith("#")):
            raise ContractError("external_schema_references_disabled")
    for keyword in _SCHEMA_MAPS:
        if type(schema.get(keyword)) is dict:
            for child in schema[keyword].values():
                _local_schema(child)
    for keyword in _SCHEMA_LISTS:
        if type(schema.get(keyword)) is list:
            for child in schema[keyword]:
                _local_schema(child)
    for keyword in _SCHEMA_VALUES:
        if keyword in schema:
            _local_schema(schema[keyword])


@dataclass(frozen=True, init=False)
class JsonObjectSchema:
    """Copy-protected 2020-12 schema for bounded integer-JSON objects.

    ``format`` is an annotation, as in the default JSON Schema dialect. Supply
    application validators when semantic checks beyond schema assertions are
    needed. The protocol codec remains authoritative for the complete frame.
    """
    _source: str

    def __init__(self, schema: dict | bool):
        if type(schema) not in (dict, bool):
            raise ContractError("invalid_schema")
        try:
            source = canonical_json(schema)
        except RunnerError:
            raise ContractError("invalid_schema") from None
        checked = json.loads(source)
        _local_schema(checked)
        validator = self._validator_type()
        try:
            validator.check_schema(checked)
        except Exception:
            raise ContractError("invalid_schema") from None
        object.__setattr__(self, "_source", source)

    @staticmethod
    def _validator_type():
        try:
            from jsonschema import Draft202012Validator
        except ImportError:
            raise ContractError("schema_validation_unavailable") from None
        return Draft202012Validator

    @property
    def descriptor(self) -> dict | bool:
        return json.loads(self._source)

    @property
    def schema(self) -> dict | bool:
        return self.descriptor

    @property
    def identity(self) -> str:
        return hashlib.sha256(self._source.encode("utf-8")).hexdigest()

    def validate(self, value: dict) -> dict:
        if type(value) is not dict:
            raise ContractError("invalid_operation_payload")
        try:
            checked = json.loads(integer_json(value))
        except RunnerError:
            raise ContractError("invalid_operation_payload") from None
        validator = self._validator_type()
        try:
            from referencing import Registry
            from referencing.exceptions import NoSuchResource

            def deny_fetch(uri):
                raise NoSuchResource(ref=uri)

            validator(self.descriptor, registry=Registry(retrieve=deny_fetch)).validate(checked)
        except Exception:
            raise ContractError("operation_schema_rejected") from None
        return checked


@dataclass(frozen=True, init=False)
class OperationContract:
    name: str
    version: str
    allowed_callers: frozenset[str]
    _input: JsonObjectSchema
    _output: JsonObjectSchema

    def __init__(self, name: str, version: str, input_schema: dict | bool,
                 output_schema: dict | bool, allowed_callers: Iterable[str]):
        object.__setattr__(self, "name", _name(name))
        object.__setattr__(self, "version", _name(version))
        if isinstance(allowed_callers, (str, bytes)):
            raise ContractError("invalid_allowed_callers")
        try:
            callers = frozenset(_actor(caller) for caller in allowed_callers)
        except TypeError:
            raise ContractError("invalid_allowed_callers") from None
        if not callers or len(callers) > 32:
            raise ContractError("invalid_allowed_callers")
        object.__setattr__(self, "allowed_callers", callers)
        object.__setattr__(self, "_input", JsonObjectSchema(input_schema))
        object.__setattr__(self, "_output", JsonObjectSchema(output_schema))
        # The entire descriptor must remain bounded, not just each schema.
        try:
            canonical_json(self.descriptor)
        except RunnerError:
            raise ContractError("invalid_operation_contract") from None

    @property
    def operation(self) -> str:
        return self.name + "/" + self.version

    @property
    def input_schema(self) -> dict | bool:
        return self._input.descriptor

    @property
    def output_schema(self) -> dict | bool:
        return self._output.descriptor

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-operation-contract/1", "name": self.name, "version": self.version,
                "operation": self.operation, "input_schema": self.input_schema,
                "output_schema": self.output_schema, "allowed_callers": sorted(self.allowed_callers)}

    @property
    def identity(self) -> str:
        return json_identity(self.descriptor)

    def authorise(self, caller: str) -> None:
        if type(caller) is not str or caller not in self.allowed_callers:
            raise ContractError("operation_caller_forbidden")

    def validate_input(self, value: dict) -> dict:
        return self._input.validate(value)

    def validate_output(self, value: dict) -> dict:
        return self._output.validate(value)


@dataclass(frozen=True)
class RegisteredOperation:
    contract: OperationContract
    handler: Adapter
    implementation_version: str

    def __post_init__(self):
        if type(self.contract) is not OperationContract or not callable(getattr(self.handler, "handle", None)):
            raise ContractError("invalid_operation_registration")
        _name(self.implementation_version)

    @property
    def descriptor(self) -> dict:
        return {"contract": self.contract.descriptor, "implementation_version": self.implementation_version}


@dataclass(frozen=True, init=False)
class OperationRegistry:
    """An actor adapter dispatching only explicitly registered versioned calls.

    Caller identity is the Runner's Rust-admitted request sender; payload
    claims never grant access. Host callers must not fabricate TaskRequest or
    context values at a network boundary. Registrations are immutable; handler
    code remains trusted and its changes require a new implementation version.
    """
    actor: str
    _registrations: object

    def __init__(self, actor: str, registrations: Iterable[RegisteredOperation]):
        object.__setattr__(self, "actor", _actor(actor))
        selected = {}
        for registration in registrations:
            if type(registration) is not RegisteredOperation:
                raise ContractError("invalid_operation_registration")
            operation = registration.contract.operation
            if operation in selected:
                raise ContractError("duplicate_operation_registration")
            selected[operation] = registration
            if len(selected) > 128:
                raise ContractError("too_many_operation_registrations")
        if not selected:
            raise ContractError("empty_operation_registry")
        object.__setattr__(self, "_registrations", MappingProxyType(selected))
        try:
            canonical_json(self.descriptor)
        except RunnerError:
            raise ContractError("invalid_operation_registry") from None

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-operation-registry/1", "actor": self.actor,
                "operations": [self._registrations[key].descriptor for key in sorted(self._registrations)]}

    @property
    def identity(self) -> str:
        return json_identity(self.descriptor)

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        if request.recipient != self.actor or context.actor != self.actor:
            raise ContractError("operation_actor_mismatch")
        if type(request.operation) is not str or request.operation not in self._registrations:
            raise ContractError("operation_not_registered")
        registration = self._registrations[request.operation]
        registration.contract.authorise(request.sender)
        checked = registration.contract.validate_input(request.input)
        copied = TaskRequest(request.sender, request.recipient, request.operation, checked,
                             request.task_id, request.message_id)
        result = registration.handler.handle(context, copied)
        if (type(result) is not Outcome or type(result.status) is not str
                or result.status not in {"completed", "unresolved"}):
            raise ContractError("invalid_operation_outcome")
        output = registration.contract.validate_output(result.output)
        return Outcome(output, result.status)
