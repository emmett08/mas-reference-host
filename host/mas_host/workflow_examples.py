"""Offline, unrelated domains using the same topology and composition features.

These trusted deterministic adapters demonstrate execution, not model quality.
Every leaf request and delegation crosses the existing Rust gateway through
Runner; the composition engine schedules registered workflows above that layer.
"""
from __future__ import annotations

from .contracts import Outcome, RunnerError
from .task_service import Workflow
from .topology import compile_topology
from .workflow_plan import WorkflowPlan


TOPOLOGY = {
    "format": "mas-topology/1",
    "groups": {
        "entry": {"kind": "star", "agents": ["client", "hub"], "hub": "hub"},
        "review": {"kind": "mesh", "agents": ["worker", "checker"]},
        "record": {"kind": "tree", "agents": ["root", "leaf"], "parents": {"leaf": "root"}},
    },
    "links": [["entry/hub", "review/worker"], ["review/worker", "entry/hub"],
              ["review/checker", "record/root"], ["record/root", "review/checker"]],
    "requests": [["entry/client", "entry/hub"], ["entry/hub", "review/worker"],
                 ["review/worker", "review/checker"], ["review/checker", "record/root"],
                 ["record/root", "record/leaf"]],
    "application": "agent-task/1", "max_messages": 40, "max_bytes": 1_000_000,
    "max_calls": 1, "wall_seconds": 60, "mailbox_capacity": 20,
}


def _text(value: dict) -> tuple[str, bool]:
    if (set(value) != {"text", "uppercase"} or type(value["text"]) is not str
            or len(value["text"]) > 4096 or type(value["uppercase"]) is not bool):
        raise RunnerError("text example requires bounded text and an uppercase boolean")
    return value["text"], value["uppercase"]


def _numbers(value: dict) -> tuple[int, int]:
    if (not {"value", "target"} <= set(value) or set(value) - {"value", "target", "done"}
            or any(type(value[key]) is not int or not -1_000_000 <= value[key] <= 1_000_000
                   for key in ("value", "target"))
            or "done" in value and type(value["done"]) is not bool):
        raise RunnerError("arithmetic example requires bounded integer value and target")
    return value["value"], value["target"]


def _normalise(value):
    text, uppercase = _text(value)
    return {"text": " ".join(text.split()), "uppercase": uppercase}


def _upper(value):
    text, uppercase = _text(value)
    return {"text": text.upper(), "uppercase": uppercase}


def _identity(value):
    text, uppercase = _text(value)
    return {"text": text, "uppercase": uppercase}


def _summary(value):
    text, _ = _text(value)
    return {"text": text, "characters": len(text), "words": len(text.split())}


def _reverse(value):
    text, _ = _text(value)
    return {"text": text[::-1]}


def _increment(value):
    number, target = _numbers(value)
    return {"value": number + 1, "target": target, "done": number + 1 >= target}


def _square(value):
    number, _ = _numbers(value)
    return {"source": number, "value": number * number, "checked": True}


def _double(value):
    number, _ = _numbers(value)
    return {"source": number, "value": 2 * number, "checked": True}


_OPERATIONS = {"text.normalise": _normalise, "text.upper": _upper,
               "text.identity": _identity, "text.summary": _summary, "text.reverse": _reverse,
               "arithmetic.increment": _increment, "arithmetic.square": _square,
               "arithmetic.double": _double}


class _Ingress:
    def handle(self, context, request):
        raise RunnerError("example ingress receives only correlated replies")


class _Delegate:
    def __init__(self, recipient):
        self.recipient = recipient

    def handle(self, context, request):
        return context.request(self.recipient, request.operation, request.input)


class _Worker:
    def __init__(self, operation):
        self.operation = operation

    def handle(self, context, request):
        if request.operation != self.operation:
            raise RunnerError("example operation mismatch")
        candidate = _OPERATIONS[self.operation](request.input)
        return context.request("review/checker", self.operation,
                               {"source": request.input, "candidate": candidate})


class _Checker:
    def __init__(self, operation):
        self.operation = operation

    def handle(self, context, request):
        if request.operation != self.operation or set(request.input) != {"source", "candidate"}:
            raise RunnerError("example check contract mismatch")
        # Repeatable exact comparison demonstrates the checking route. The shared
        # implementation is deliberately not described as independent assurance.
        expected = _OPERATIONS[self.operation](request.input["source"])
        if request.input["candidate"] != expected:
            return Outcome({"verified": False}, "unresolved")
        return context.request("record/root", self.operation, expected)


class _Record:
    def handle(self, context, request):
        # Returning through Rust creates the durable result records; no external
        # file or remote effect is performed by this deterministic fixture.
        return Outcome(request.input)


def _workflow(operation):
    return Workflow(version=f"offline-composition/1:{operation}", topology=compile_topology(TOPOLOGY),
                    adapters={"entry/client": _Ingress(), "entry/hub": _Delegate("review/worker"),
                              "review/worker": _Worker(operation), "review/checker": _Checker(operation),
                              "record/root": _Delegate("record/leaf"), "record/leaf": _Record()},
                    sender="entry/client", recipient="entry/hub", operation=operation)


def _task(workflow):
    return {"kind": "task", "workflow": workflow}


def _plan(name, root):
    return WorkflowPlan({"format": "mas-workflow/1", "name": name, "version": "1",
                         "limits": {"max_steps": 64, "max_parallel": 2, "wall_seconds": 180,
                                    "max_calls": 20, "max_reserved_microusd": 0}, "root": root})


def example_registry() -> dict:
    """Trusted factory for CLI/tests; construction makes no provider calls."""
    plans = {
        "text-pipeline": _plan("text-pipeline", {"kind": "sequence", "steps": [
            _task("text-normalise"),
            {"kind": "choose", "condition": {"path": ["uppercase"], "equals": True},
             "then": _task("text-upper"), "else": _task("text-identity")},
            {"kind": "parallel", "branches": {"summary": _task("text-summary"),
                                               "reverse": _task("text-reverse")}},
        ]}),
        "bounded-increment": _plan("bounded-increment", {
            "kind": "loop", "body": _task("arithmetic-increment"), "max_iterations": 5,
            "until": {"path": ["done"], "equals": True}}),
        "arithmetic-evidence": _plan("arithmetic-evidence", {"kind": "sequence", "steps": [
            {"kind": "subworkflow", "workflow": "bounded-increment"},
            {"kind": "parallel", "branches": {"square": _task("arithmetic-square"),
                                               "double": _task("arithmetic-double")}},
        ]}),
    }
    workflows = {operation.replace(".", "-"): _workflow(operation) for operation in _OPERATIONS}
    return {"plans": plans, "workflows": workflows, "grants": {"demo": set(plans)}}
