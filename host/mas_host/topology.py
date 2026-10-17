"""Compile named logical graphs to the existing bounded Rust route configuration.

Composition is configuration, not a new transport or scheduler. Star and tree
relations permit requests and replies; mesh permits all intragroup pairs. Custom
edges and intergroup links are directed. Declared ``requests`` require both
directions without silently adding permission. Group/local names reserve ``/``.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json

from .codec import integer_json, is_actor_identity
from .contracts import RunnerError


FORMAT = "mas-topology/1"
MAX_AGENTS = 32
_DEFAULTS = {"max_messages": 100, "max_bytes": 100_000, "max_calls": 20,
             "wall_seconds": 3600, "mailbox_capacity": 100}
_COMMON = set(_DEFAULTS) | {"application"}


def _identity(value: object, *, component: bool = False) -> str:
    if (not is_actor_identity(value)
            or component and "/" in value):
        raise RunnerError("invalid topology identity")
    return value


def _keys(value: object, allowed: set[str], required: set[str]) -> dict:
    if type(value) is not dict or set(value) - allowed or not required <= set(value):
        raise RunnerError("invalid topology fields")
    return value


def _agents(value: object, *, component: bool = False) -> list[str]:
    if type(value) is not list or not 1 <= len(value) <= MAX_AGENTS:
        raise RunnerError("topology requires between 1 and 32 agents")
    agents = [_identity(item, component=component) for item in value]
    if len(set(agents)) != len(agents):
        raise RunnerError("duplicate topology agent")
    return sorted(agents)


def _pairs(value: object, agents: set[str]) -> set[tuple[str, str]]:
    if type(value) is not list or len(value) > MAX_AGENTS * (MAX_AGENTS - 1):
        raise RunnerError("topology routes must be a bounded array")
    result = set()
    for pair in value:
        if (type(pair) is not list or len(pair) != 2
                or any(type(actor) is not str or actor not in agents for actor in pair)
                or pair[0] == pair[1]):
            raise RunnerError("topology routes require two distinct registered agents")
        route = tuple(pair)
        if route in result:
            raise RunnerError("duplicate topology route")
        result.add(route)
    return result


def _star(group: dict, agents: list[str]) -> set[tuple[str, str]]:
    _keys(group, {"kind", "agents", "hub"}, {"kind", "agents", "hub"})
    hub = group["hub"]
    if type(hub) is not str or hub not in agents:
        raise RunnerError("star hub must be a registered group agent")
    return {(source, target) for actor in agents if actor != hub
            for source, target in ((hub, actor), (actor, hub))}


def _mesh(group: dict, agents: list[str]) -> set[tuple[str, str]]:
    _keys(group, {"kind", "agents"}, {"kind", "agents"})
    return {(a, b) for a in agents for b in agents if a != b}


def _tree(group: dict, agents: list[str]) -> set[tuple[str, str]]:
    _keys(group, {"kind", "agents", "parents"}, {"kind", "agents", "parents"})
    parents = group["parents"]
    if type(parents) is not dict or len(parents) != len(agents) - 1:
        raise RunnerError("tree requires exactly one parent for each non-root agent")
    routes = _pairs([[parent, child] for child, parent in parents.items()], set(agents))
    roots = set(agents) - set(parents)
    if len(roots) != 1:
        raise RunnerError("tree requires one root")
    for agent in agents:
        visited = set()
        while agent in parents:
            if agent in visited:
                raise RunnerError("tree parents contain a cycle")
            visited.add(agent)
            agent = parents[agent]
    return routes | {(child, parent) for parent, child in routes}


def _custom(group: dict, agents: list[str]) -> set[tuple[str, str]]:
    _keys(group, {"kind", "agents", "edges"}, {"kind", "agents", "edges"})
    return _pairs(group["edges"], set(agents))


_GROUP_COMPILERS = {"star": _star, "mesh": _mesh, "tree": _tree, "custom": _custom}


def _compose(spec: dict) -> tuple[list[str], set[tuple[str, str]]]:
    _keys(spec, _COMMON | {"format", "groups", "links", "requests"}, {"format", "groups"})
    if spec["format"] != FORMAT:
        raise RunnerError("unsupported topology format")
    groups = spec["groups"]
    if type(groups) is not dict or not 1 <= len(groups) <= MAX_AGENTS:
        raise RunnerError("topology requires between 1 and 32 named groups")
    agents, edges = [], set()
    for name, group in sorted(groups.items()):
        _identity(name, component=True)
        if type(group) is not dict or type(group.get("kind")) is not str:
            raise RunnerError("topology group requires a registered kind")
        compiler = _GROUP_COMPILERS.get(group["kind"])
        if compiler is None:
            raise RunnerError("unknown topology group kind")
        local = _agents(group.get("agents"), component=True)
        if len(agents) + len(local) > MAX_AGENTS:
            raise RunnerError("topology requires between 1 and 32 agents")
        names = {actor: _identity(f"{name}/{actor}") for actor in local}
        agents.extend(names.values())
        edges.update((names[a], names[b]) for a, b in compiler(group, local))
    links = _pairs(spec.get("links", []), set(agents))
    if any(a.split("/", 1)[0] == b.split("/", 1)[0] for a, b in links):
        raise RunnerError("topology links must connect different groups")
    edges.update(links)
    requests = _pairs(spec.get("requests", []), set(agents))
    if any((a, b) not in edges or (b, a) not in edges for a, b in requests):
        raise RunnerError("declared topology request requires forward and return routes")
    return sorted(agents), edges


def compile_topology(spec: dict) -> dict:
    """Validate and copy a flat Rust config or compile ``mas-topology/1``.

    A composed specification has named ``groups`` (kind, local agents and kind
    parameters), optional directed qualified ``links`` and optional ``requests``
    to check for return routes. Standard Rust budgets and application are top-level.
    The result always contains explicit, sorted edges. Only legacy flat input
    with omitted edges expands to all registered routes, preserving its meaning.
    Explicit null edges, duplicate declarations and unknown fields are rejected.
    Inputs and results use bounded integer JSON. Rust remains authoritative when
    the configuration is admitted; this compiler does not grant runtime access.
    """
    integer_json(spec)
    if type(spec) is not dict:
        raise RunnerError("topology must be an object")
    if "format" in spec or "groups" in spec:
        agents, edges = _compose(spec)
    else:
        # The administrative CLI accepts these two metadata fields and strips
        # them before Store admission; preserve that established flat input.
        _keys(spec, _COMMON | {"agents", "edges", "name", "description"}, {"agents"})
        agents = _agents(spec["agents"])
        edges = (_pairs(spec["edges"], set(agents)) if "edges" in spec else
                 {(a, b) for a in agents for b in agents if a != b})
    result = {"agents": agents, "edges": [list(pair) for pair in sorted(edges)]}
    application = spec.get("application")
    if application is not None and application != "agent-task/1":
        raise RunnerError("unsupported topology application profile")
    result["application"] = application
    for key, default in _DEFAULTS.items():
        value = spec.get(key, default)
        if (type(value) is not int or not 1 <= value <= 2**63 - 1
                or key == "mailbox_capacity" and value > 100):
            raise RunnerError("topology budgets must be positive i64; mailbox capacity at most 100")
        result[key] = value
    integer_json(result)
    return result


@dataclass(frozen=True, init=False)
class FrozenTopology:
    """Immutable effective configuration; each public dictionary is a fresh copy."""

    _canonical: str

    def __init__(self, spec: dict):
        compiled = compile_topology(spec)
        object.__setattr__(self, "_canonical", json.dumps(compiled, sort_keys=True,
                                                        separators=(",", ":")))

    @property
    def configuration(self) -> dict:
        return json.loads(self._canonical)

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self._canonical.encode("utf-8")).hexdigest()


def freeze_topology(spec: dict) -> FrozenTopology:
    """Freeze a validated effective graph, independent of caller-owned objects."""
    return FrozenTopology(spec)


def inspect_topology(spec: dict) -> dict:
    """Return the effective ACL and budgets with a canonical SHA-256 identity."""
    frozen = freeze_topology(spec)
    configuration = frozen.configuration
    return {"configuration": configuration, "fingerprint": frozen.fingerprint,
            "agent_count": len(configuration["agents"]),
            "edge_count": len(configuration["edges"])}


def topology_mermaid(spec: dict) -> str:
    """Render exact permitted directions with safe labels, not execution order."""
    configuration = compile_topology(spec)
    names = {actor: f"n{index}" for index, actor in enumerate(configuration["agents"])}
    lines = ["flowchart TD"]
    lines.extend(f'    {names[actor]}["{actor}"]' for actor in configuration["agents"])
    lines.extend(f"    {names[a]} --> {names[b]}" for a, b in configuration["edges"])
    return "\n".join(lines) + "\n"
