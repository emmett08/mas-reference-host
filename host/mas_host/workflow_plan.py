"""Immutable declarative plans over explicitly registered MAS workflows.

Plans contain bounded integer JSON, never Python expressions or import paths.
Topology admission remains the leaf workflow's responsibility. This module
validates composition and conditions without executing adapters or tools.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import re
from typing import TYPE_CHECKING

from .codec import integer_json
from .contracts import RunnerError
from .execution_policy import timing_policy as load_timing_policy
from .operation_contracts import ContractError, JsonObjectSchema
from .service_contracts import ServiceError

if TYPE_CHECKING:
    from .task_service import Workflow


PLAN_FORMAT = "mas-workflow/1"
MAX_DEPTH = 16
MAX_REGISTRY = 256
MAX_COLLECTION_ITEMS = 1000
MAX_COLLECTION_INPUT_BYTES = 1_048_576
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_LIMITS = {
    "max_steps": (1, 10_000),
    "max_parallel": (1, 32),
    "wall_seconds": (1, 86_400),
    "max_calls": (1, 100_000),
    "max_reserved_microusd": (0, 1_000_000_000_000),
}


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _copy(value: object, code: str) -> object:
    try:
        return json.loads(integer_json(value))
    except RunnerError:
        raise ServiceError(code, "expected bounded integer-only JSON") from None


def _fields(value: object, required: set[str], code: str = "invalid_workflow_plan", *,
            optional: frozenset[str] = frozenset()) -> None:
    if (type(value) is not dict or not required.issubset(value)
            or set(value) - required - optional):
        raise ServiceError(code, "object fields do not match the declared contract")


def _name(value: object) -> str:
    if type(value) is not str or _NAME.fullmatch(value) is None:
        raise ServiceError("invalid_workflow_plan_name")
    return value


def _integer(value: object, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ServiceError("invalid_workflow_plan_limit")


def _condition(value: object) -> None:
    _fields(value, {"path", "equals"}, "invalid_workflow_condition")
    _path(value["path"], "invalid_workflow_condition")


def _join(value: object, branches: int) -> None:
    # Durable cancellation records contain one full path per obsolete branch.
    # Together with MAX_DEPTH/name bounds, 32 keeps that record below JSON_BYTES.
    if branches > 32:
        raise ServiceError("workflow_join_branch_limit")
    _fields(value, {"mode", "acceptance"}, "invalid_workflow_join",
            optional=frozenset({"minimum", "agreement"}))
    if value["mode"] not in ("all", "quorum", "first-accepted"):
        raise ServiceError("invalid_workflow_join")
    acceptance = value["acceptance"]
    _fields(acceptance, {"id", "version", "condition"}, "invalid_workflow_acceptance")
    _name(acceptance["id"])
    _name(acceptance["version"])
    if acceptance["version"] == "unversioned":
        raise ServiceError("invalid_workflow_acceptance")
    _condition(acceptance["condition"])
    if value["mode"] == "quorum":
        _integer(value.get("minimum"), 1, branches)
    elif "minimum" in value:
        raise ServiceError("invalid_workflow_join")
    if "agreement" in value:
        from .workflow_agreement import AGREEMENT_VERSION
        agreement = value["agreement"]
        _fields(agreement, {"version", "candidate", "context"}, "invalid_workflow_agreement")
        if value["mode"] != "quorum" or agreement["version"] != AGREEMENT_VERSION:
            raise ServiceError("invalid_workflow_agreement")
        _selector(agreement["candidate"], result_type="object")
        _selector(agreement["context"], result_type="object")


def _path(path: object, code: str) -> None:
    if type(path) is not list or len(path) > 32:
        raise ServiceError(code)
    for part in path:
        if type(part) is str:
            continue
        if type(part) is not int or not 0 <= part <= 2_147_483_647:
            raise ServiceError(code)


_JSON_TYPES = {"object": dict, "array": list, "string": str, "integer": int,
               "boolean": bool, "null": type(None)}


def _selector(value: object, *, result_type: str | None = None) -> None:
    """Selectors are literals or exact typed paths, never expressions."""
    if type(value) is dict and set(value) == {"literal"}:
        if result_type is not None and type(value["literal"]) is not _JSON_TYPES[result_type]:
            raise ServiceError("invalid_workflow_selector_type")
        return
    _fields(value, {"path", "type"}, "invalid_workflow_selector")
    _path(value["path"], "invalid_workflow_selector")
    if (type(value["type"]) is not str or value["type"] not in _JSON_TYPES
            or result_type is not None and value["type"] != result_type):
        raise ServiceError("invalid_workflow_selector_type")


def select_value(selector: dict, value: object) -> object:
    """Return a detached, strictly typed selection; absent paths are errors."""
    checked = _copy(selector, "invalid_workflow_selector")
    _selector(checked)
    if "literal" in checked:
        return checked["literal"]
    selected = value
    for part in checked["path"]:
        if type(selected) is dict and type(part) is str and part in selected:
            selected = selected[part]
        elif type(selected) is list and type(part) is int and part < len(selected):
            selected = selected[part]
        else:
            raise ServiceError("workflow_selection_missing")
    if type(selected) is not _JSON_TYPES[checked["type"]]:
        raise ServiceError("workflow_selection_type_mismatch")
    return _copy(selected, "invalid_workflow_value")


def transform_value(node: dict, value: dict) -> dict:
    """Project fields into an object and optionally enforce an output schema."""
    output = {key: select_value(selector, value) for key, selector in node["fields"].items()}
    if "schema" in node:
        try:
            output = JsonObjectSchema(node["schema"]).validate(output)
        except ContractError:
            raise ServiceError("workflow_transform_schema_mismatch") from None
    return _copy(output, "workflow_transform_size_limit")


def evaluate_condition(condition: dict, value: object) -> bool:
    """Match an exact JSON value at a path; absent or incompatible paths fail.

    An empty path selects the entire value. Integer path components select array
    positions, strings select object properties. Booleans differ from integers,
    including inside arrays and objects; no truthiness or coercion is applied.
    """
    checked = _copy(condition, "invalid_workflow_condition")
    _condition(checked)
    selected = _copy(value, "invalid_workflow_value")
    for part in checked["path"]:
        if type(selected) is dict and type(part) is str and part in selected:
            selected = selected[part]
        elif type(selected) is list and type(part) is int and part < len(selected):
            selected = selected[part]
        else:
            return False
    return _canonical(selected) == _canonical(checked["equals"])


def _validate_node(node: object, depth: int = 1) -> int:
    """Return a local upper bound, charging unresolved subworkflows one step."""
    if depth > MAX_DEPTH or type(node) is not dict:
        raise ServiceError("invalid_workflow_plan_depth")
    kind = node.get("kind")
    if type(kind) is not str:
        raise ServiceError("invalid_workflow_plan_kind")
    if kind in ("task", "subworkflow"):
        _fields(node, {"kind", "workflow"})
        _name(node["workflow"])
        return 1
    if kind == "transform":
        _fields(node, {"kind", "fields"}, optional=frozenset({"schema"}))
        if type(node["fields"]) is not dict or len(node["fields"]) > 128:
            raise ServiceError("invalid_workflow_mapping")
        for name, selector in node["fields"].items():
            _name(name)
            _selector(selector)
        if "schema" in node:
            if type(node["schema"]) is not dict or node["schema"].get("type") != "object":
                raise ServiceError("invalid_workflow_mapping_schema")
            try:
                JsonObjectSchema(node["schema"])
            except ContractError:
                raise ServiceError("invalid_workflow_mapping_schema") from None
        return 1
    if kind == "sequence":
        _fields(node, {"kind", "steps"})
        if type(node["steps"]) is not list or not node["steps"]:
            raise ServiceError("invalid_workflow_plan")
        return 1 + sum(_validate_node(child, depth + 1) for child in node["steps"])
    if kind == "parallel":
        _fields(node, {"kind", "branches"}, optional=frozenset({"join"}))
        branches = node["branches"]
        if type(branches) is not dict or not branches:
            raise ServiceError("invalid_workflow_plan")
        for name in branches:
            _name(name)
        if "join" in node:
            _join(node["join"], len(branches))
            if "agreement" in node["join"] and any(type(child) is not dict or child.get("kind") != "task"
                                                   for child in branches.values()):
                raise ServiceError("workflow_agreement_requires_tasks")
        return 1 + sum(_validate_node(child, depth + 1) for child in branches.values())
    if kind == "recover":
        _fields(node, {"kind", "body", "on_failure"}, optional=frozenset({"compensation"}))
        children = [node["body"], node["on_failure"]]
        if "compensation" in node:
            compensation = node["compensation"]
            if type(compensation) is not dict or compensation.get("kind") != "task":
                raise ServiceError("invalid_workflow_compensation")
            children.append(compensation)
        return 1 + sum(_validate_node(child, depth + 1) for child in children)
    if kind == "choose":
        _fields(node, {"kind", "condition", "then", "else"})
        _condition(node["condition"])
        return 1 + max(_validate_node(node["then"], depth + 1),
                       _validate_node(node["else"], depth + 1))
    if kind == "switch":
        _fields(node, {"kind", "select", "cases", "default"})
        _selector(node["select"])
        if type(node["cases"]) is not list or not 1 <= len(node["cases"]) <= 32:
            raise ServiceError("invalid_workflow_switch")
        names, values, costs = set(), set(), []
        for case in node["cases"]:
            _fields(case, {"name", "equals", "body"}, "invalid_workflow_switch")
            name, value = _name(case["name"]), _canonical(case["equals"])
            if name in names or value in values:
                raise ServiceError("duplicate_workflow_switch_case")
            names.add(name)
            values.add(value)
            costs.append(_validate_node(case["body"], depth + 1))
        return 1 + max(*costs, _validate_node(node["default"], depth + 1))
    if kind in {"map", "reduce"}:
        required = {"kind", "items", "body", "max_items", "max_input_bytes"}
        _fields(node, required | ({"initial"} if kind == "reduce" else set()))
        _selector(node["items"], result_type="array")
        if kind == "reduce":
            _selector(node["initial"], result_type="object")
        _integer(node["max_items"], 1, MAX_COLLECTION_ITEMS)
        _integer(node["max_input_bytes"], 1, MAX_COLLECTION_INPUT_BYTES)
        return 1 + node["max_items"] * _validate_node(node["body"], depth + 1)
    if kind in {"loop", "while"}:
        condition = "until" if kind == "loop" else "condition"
        _fields(node, {"kind", "body", condition, "max_iterations"})
        _condition(node[condition])
        _integer(node["max_iterations"], 1, 1000)
        return 1 + node["max_iterations"] * _validate_node(node["body"], depth + 1)
    raise ServiceError("invalid_workflow_plan_kind")


@dataclass(frozen=True, init=False, slots=True)
class WorkflowPlan:
    """A source plan whose identity is independent of mutable caller objects.

    ``identity`` covers this source only. Use ``bind_plan`` to bind all leaf and
    subworkflow registrations before durable execution; a source plan cannot
    establish the identity of a referenced adapter by itself.
    """
    _source: str

    def __init__(self, spec: dict):
        checked = _copy(spec, "invalid_workflow_plan")
        _fields(checked, {"format", "name", "version", "limits", "root"},
                optional=frozenset({"timing_policy", "input_schema", "output_schema"}))
        for field in ("input_schema", "output_schema"):
            if field in checked:
                try:
                    checked[field] = JsonObjectSchema(checked[field]).descriptor
                except ContractError:
                    raise ServiceError("invalid_workflow_contract_schema") from None
        if checked["format"] != PLAN_FORMAT:
            raise ServiceError("invalid_workflow_plan_format")
        _name(checked["name"])
        version = checked["version"]
        if (type(version) is not str or not version or version == "unversioned"
                or len(version.encode("utf-8")) > 128
                or any(ord(char) < 32 for char in version)):
            raise ServiceError("invalid_workflow_plan_version")
        _fields(checked["limits"], set(_LIMITS))
        for name, (minimum, maximum) in _LIMITS.items():
            _integer(checked["limits"][name], minimum, maximum)
        if "timing_policy" in checked:
            try:
                timing = load_timing_policy(checked["timing_policy"])
                if timing is None:
                    raise RunnerError("explicit timing policy is required")
                checked["timing_policy"] = timing.descriptor
            except RunnerError:
                raise ServiceError("invalid_workflow_timing_policy") from None
        if _validate_node(checked["root"]) > checked["limits"]["max_steps"]:
            raise ServiceError("workflow_plan_step_limit")
        object.__setattr__(self, "_source", _canonical(checked))

    @property
    def spec(self) -> dict:
        """A fresh application-owned copy of the validated source."""
        return json.loads(self._source)

    @property
    def contract_descriptor(self):
        spec = self.spec
        return {"input_schema": spec.get("input_schema", {"type": "object"}),
                "output_schema": spec.get("output_schema", {"type": "object"}),
                "json_profile": "bounded-integer-json"}

    @property
    def identity(self) -> str:
        return hashlib.sha256(self._source.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class _Expansion:
    cost: int
    depth: int
    plans: frozenset[str]
    workflows: frozenset[str]


def _registry(plans: Mapping[str, WorkflowPlan], workflows: Mapping[str, Workflow]):
    # A local import keeps Workflow independent from plan validation.
    from .task_service import Workflow

    if (not isinstance(plans, Mapping) or not isinstance(workflows, Mapping)
            or len(plans) > MAX_REGISTRY or len(workflows) > MAX_REGISTRY):
        raise ServiceError("invalid_workflow_plan_registry")
    specs = {}
    for name, plan in plans.items():
        _name(name)
        if not isinstance(plan, WorkflowPlan):
            raise ServiceError("invalid_workflow_plan_registry")
        specs[name] = plan.spec
        if specs[name]["name"] != name:
            raise ServiceError("workflow_plan_name_mismatch")
    identities = {}
    for name, workflow in workflows.items():
        _name(name)
        if not isinstance(workflow, Workflow):
            raise ServiceError("invalid_workflow_plan_registry")
        identities[name] = workflow.identity

    visiting: set[str] = set()
    expansions: dict[str, _Expansion] = {}

    def plan_expansion(name: str) -> _Expansion:
        if name in visiting:
            raise ServiceError("workflow_plan_cycle")
        if name not in specs:
            raise ServiceError("unknown_subworkflow")
        if name in expansions:
            return expansions[name]
        if len(visiting) >= MAX_DEPTH:
            raise ServiceError("invalid_workflow_plan_depth")
        visiting.add(name)
        expansion = node_expansion(specs[name]["root"])
        visiting.remove(name)
        if expansion.cost > specs[name]["limits"]["max_steps"]:
            raise ServiceError("workflow_plan_step_limit")
        if expansion.depth > MAX_DEPTH:
            raise ServiceError("invalid_workflow_plan_depth")
        expansions[name] = expansion
        return expansion

    def node_expansion(node: dict) -> _Expansion:
        kind = node["kind"]
        if kind == "task":
            if node["workflow"] not in identities:
                raise ServiceError("unknown_leaf_workflow")
            return _Expansion(1, 1, frozenset(), frozenset({node["workflow"]}))
        if kind == "transform":
            return _Expansion(1, 1, frozenset(), frozenset())
        if kind == "subworkflow":
            name = node["workflow"]
            child = plan_expansion(name)
            return _Expansion(1 + child.cost, 1 + child.depth,
                              child.plans | {name}, child.workflows)
        if kind == "sequence":
            children = [node_expansion(child) for child in node["steps"]]
        elif kind == "parallel":
            if "agreement" in node.get("join", {}):
                recipients = [workflows[child["workflow"]].recipient
                              for child in node["branches"].values()
                              if child["workflow"] in workflows]
                if len(set(recipients)) != len(recipients):
                    raise ServiceError("workflow_agreement_requires_distinct_actors")
            children = [node_expansion(child) for child in node["branches"].values()]
        elif kind == "recover":
            children = [node_expansion(node["body"]), node_expansion(node["on_failure"])]
            if "compensation" in node:
                children.append(node_expansion(node["compensation"]))
        elif kind == "choose":
            children = [node_expansion(node["then"]), node_expansion(node["else"])]
        elif kind == "switch":
            children = [node_expansion(case["body"]) for case in node["cases"]]
            children.append(node_expansion(node["default"]))
        else:  # WorkflowPlan has already validated the complete node ontology.
            children = [node_expansion(node["body"])]
        if kind in {"choose", "switch"}:
            cost = 1 + max(child.cost for child in children)
        elif kind in {"loop", "while"}:
            cost = 1 + node["max_iterations"] * children[0].cost
        elif kind in {"map", "reduce"}:
            cost = 1 + node["max_items"] * children[0].cost
        else:
            cost = 1 + sum(child.cost for child in children)
        return _Expansion(cost, 1 + max(child.depth for child in children),
                          frozenset().union(*(child.plans for child in children)),
                          frozenset().union(*(child.workflows for child in children)))

    for name in specs:
        plan_expansion(name)
    return specs, identities, expansions


def validate_registry(plans: Mapping[str, WorkflowPlan], workflows: Mapping[str, Workflow]) -> None:
    """Reject missing references, recursive plans and excessive expanded work.

    All registrations are checked, including currently unselected conditional
    branches. Limits are conservative: a loop is charged its declared maximum
    iterations, and a choice the more expensive branch. Parallelism is bounded
    globally by the scheduler rather than the number of declared branches.
    """
    _registry(plans, workflows)


def bind_plan(name: str, plans: Mapping[str, WorkflowPlan],
              workflows: Mapping[str, Workflow]) -> dict:
    """Return an inspectable binding and its transitive SHA-256 identity.

    Unrelated registry entries do not affect the identity. Referenced leaf
    identities include their registered version, route configuration and hooks;
    callers must version adapter implementations as required by ``Workflow``.
    """
    specs, identities, expansions = _registry(plans, workflows)
    _name(name)
    if name not in specs:
        raise ServiceError("unknown_workflow_plan")
    expansion = expansions[name]
    descriptor = {
        "format": "mas-workflow-binding/1",
        "name": name,
        "spec": specs[name],
        "plans": {key: plans[key].identity for key in sorted(expansion.plans)},
        "workflows": {key: identities[key] for key in sorted(expansion.workflows)},
        "max_steps": expansion.cost,
    }
    return {**descriptor, "identity": hashlib.sha256(
        _canonical(descriptor).encode("utf-8")).hexdigest()}
