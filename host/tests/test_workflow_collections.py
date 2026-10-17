"""Typed composition, bounded expansion and replay of collection workflows."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest

from mas_host import Invocation, Outcome
from mas_host.codec import integer_json
from mas_host.service_contracts import ServiceError
from mas_host.task_service import Workflow
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_evaluator import PlanEvaluator
from mas_host.workflow_plan import (WorkflowPlan, bind_plan, select_value,
                                    transform_value, validate_registry)
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests
from test_workflow_plan import leaf, spec


TASK = {"kind": "task", "workflow": "compute"}


def collection(kind="map", body=None, *, maximum=3, budget=65536):
    node = {"kind": kind, "items": {"path": ["items"], "type": "array"},
            "max_items": maximum, "max_input_bytes": budget, "body": body or TASK}
    if kind == "reduce":
        node["initial"] = {"literal": {"value": 0}}
    return node


class CollectionJournal:
    def __init__(self, execute=None):
        self.nodes, self.attempts, self.results = {}, {}, {}
        self.execute = execute

    def node(self, instance, path, kind, input, limits):
        if path in self.nodes:
            self_input = self.nodes[path]["input"]
            if integer_json(self_input) != integer_json(input):
                raise AssertionError("replay input changed")
        else:
            self.nodes[path] = {"state": "waiting", "output": None, "input": copy.deepcopy(input)}
        return self.nodes[path]

    def attempt(self, instance, path, workflow, input, deadline):
        if path not in self.attempts:
            self.attempts[path] = copy.deepcopy(input)
            if self.execute is not None:
                self.results[path] = {"state": "completed", "output": self.execute(input)}
        return {"path": path, "workflow": workflow, "error": None,
                **self.results.get(path, {"state": "queued", "output": None})}

    def finish_node(self, instance, path, state, output=None, error=None):
        self.nodes[path].update(state=state, output=copy.deepcopy(output), error=error)
        return state, output


class WorkflowCollectionTests(unittest.TestCase):
    def advance(self, node, input, journal=None):
        journal = CollectionJournal() if journal is None else journal
        source = WorkflowPlan(spec(node)).spec
        ready = []
        result = PlanEvaluator({}, journal).advance("instance", "root", source["root"],
                                                   input, source["limits"], 1000, ready)
        return result, journal, ready

    def assertCode(self, code, operation):
        with self.assertRaises(ServiceError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def test_selectors_distinguish_types_missing_null_and_literal_data(self):
        value = {"nested": [{"flag": True}], "nothing": None, "number": 1}
        self.assertEqual(select_value({"path": ["nested", 0, "flag"], "type": "boolean"}, value), True)
        self.assertIsNone(select_value({"path": ["nothing"], "type": "null"}, value))
        self.assertCode("workflow_selection_missing", lambda: select_value(
            {"path": ["absent"], "type": "null"}, value))
        self.assertCode("workflow_selection_type_mismatch", lambda: select_value(
            {"path": ["number"], "type": "boolean"}, value))
        self.assertCode("workflow_selection_missing", lambda: select_value(
            {"path": ["nested", "0"], "type": "object"}, value))
        literal = {"literal": {"path": ["not", "evaluated"]}}
        selected = select_value(literal, value)
        selected["path"].append("changed")
        self.assertEqual(literal["literal"]["path"], ["not", "evaluated"])

    def test_transform_projects_output_and_enforces_optional_schema(self):
        node = {"kind": "transform", "fields": {
            "answer": {"path": ["result", "number"], "type": "integer"},
            "source": {"literal": "verified"}},
            "schema": {"type": "object", "properties": {"answer": {"minimum": 2}}}}
        result, _, ready = self.advance(node, {"result": {"number": 4}, "unselected": "private"})
        self.assertEqual(result, ("completed", {"answer": 4, "source": "verified"}))
        self.assertEqual(ready, [])
        result, journal, _ = self.advance(node, {"result": {"number": 1}})
        self.assertEqual(result, ("failed", None))
        self.assertEqual(journal.nodes["root"]["error"], {"code": "workflow_transform_schema_mismatch"})

    def test_invalid_selectors_and_remote_schema_references_reject_plan(self):
        for selector in ({"path": [True], "type": "integer"}, {"path": [-1], "type": "integer"},
                         {"path": [], "type": "number"}, {"expression": "__import__('os')"}):
            with self.subTest(selector=selector), self.assertRaises(ServiceError):
                WorkflowPlan(spec({"kind": "transform", "fields": {"x": selector}}))
        with self.assertRaises(ServiceError):
            WorkflowPlan(spec({"kind": "transform", "fields": {},
                               "schema": {"type": "object", "$ref": "https://untrusted.example/schema"}}))

    def test_collection_count_and_byte_limits_precede_any_child_allocation(self):
        cases = ((collection(maximum=1), {"items": [1, 2]}, "workflow_collection_item_limit"),
                 (collection(budget=1), {"items": [1]}, "workflow_collection_input_limit"),
                 (collection(), {}, "workflow_selection_missing"),
                 (collection(), {"items": {}}, "workflow_selection_type_mismatch"))
        for node, input, code in cases:
            with self.subTest(code=code):
                result, journal, ready = self.advance(node, input)
                self.assertEqual(result, ("failed", None))
                self.assertEqual(journal.nodes["root"]["error"], {"code": code})
                self.assertEqual(set(journal.nodes), {"root"})
                self.assertEqual(ready, [])

    def test_map_has_stable_index_paths_and_input_order_regardless_of_completion(self):
        node = collection()
        input = {"items": [7, 2], "context": "retained"}
        result, journal, ready = self.advance(node, input)
        self.assertEqual(result, ("waiting", None))
        self.assertEqual([attempt["path"] for attempt in ready], ["root/m0", "root/m1"])
        self.assertEqual(journal.attempts["root/m1"], {"input": input, "item": 2, "index": 1})
        journal.results["root/m1"] = {"state": "completed", "output": {"result": 4}}
        self.assertEqual(self.advance(node, input, journal)[0], ("waiting", None))
        journal.results["root/m0"] = {"state": "completed", "output": {"result": 14}}
        self.assertEqual(self.advance(node, input, journal)[0],
                         ("completed", {"items": [{"result": 14}, {"result": 4}]}))
        journal.results.clear()
        self.assertEqual(self.advance(node, input, journal)[0][0], "completed")

    def test_map_retains_uncertainty_even_with_completed_siblings(self):
        journal = CollectionJournal()
        journal.results = {"root/m0": {"state": "unknown", "output": None},
                           "root/m1": {"state": "completed", "output": {"ok": True}}}
        result, journal, _ = self.advance(collection(), {"items": [1, 2]}, journal)
        self.assertEqual(result, ("unknown", None))
        self.assertEqual(journal.nodes["root/m1"]["output"], {"ok": True})

    def test_oversized_map_results_still_observe_unknown_sibling(self):
        journal = CollectionJournal()
        journal.results = {
            "root/m0": {"state": "completed", "output": {"text": "x" * 40000}},
            "root/m1": {"state": "completed", "output": {"text": "y" * 40000}},
            "root/m2": {"state": "unknown", "output": None}}
        node, input = collection(), {"items": [1, 2, 3]}
        self.assertEqual(self.advance(node, input, journal)[0], ("unknown", None))
        journal.results["root/m2"] = {"state": "completed", "output": {"ok": True}}
        self.assertEqual(self.advance(node, input, journal)[0], ("failed", None))
        self.assertEqual(journal.nodes["root"]["error"], {"code": "composed_output_not_encodable"})

    def test_empty_map_and_reduce_create_no_child_attempts(self):
        for kind, expected in (("map", {"items": []}), ("reduce", {"value": 0})):
            result, journal, ready = self.advance(collection(kind), {"items": []})
            self.assertEqual(result, ("completed", expected))
            self.assertEqual(journal.attempts, {})
            self.assertEqual(ready, [])

    def test_reduce_dispatches_only_after_previous_accumulator_is_complete(self):
        node, input = collection("reduce"), {"items": [3, 8]}
        _, journal, ready = self.advance(node, input)
        self.assertEqual([attempt["path"] for attempt in ready], ["root/r0"])
        self.assertEqual(journal.attempts["root/r0"]["accumulator"], {"value": 0})
        journal.results["root/r0"] = {"state": "completed", "output": {"value": 3}}
        _, journal, ready = self.advance(node, input, journal)
        self.assertEqual([attempt["path"] for attempt in ready], ["root/r1"])
        self.assertEqual(journal.attempts["root/r1"]["accumulator"], {"value": 3})
        journal.results["root/r1"] = {"state": "completed", "output": {"value": 11}}
        self.assertEqual(self.advance(node, input, journal)[0], ("completed", {"value": 11}))

    def test_reduce_reconstructs_consumed_expansion_bytes_after_restart(self):
        input = {"items": [3, 8]}
        first = {"input": input, "item": 3, "index": 0, "accumulator": {"value": 0}}
        second = {"input": input, "item": 8, "index": 1, "accumulator": {"value": 3}}
        budget = len(integer_json(first)) + len(integer_json(second)) - 1
        node = collection("reduce", budget=budget)
        _, journal, _ = self.advance(node, input)
        journal.results["root/r0"] = {"state": "completed", "output": {"value": 3}}
        result, journal, ready = self.advance(node, input, journal)
        self.assertEqual(result, ("failed", None))
        self.assertEqual(journal.nodes["root"]["error"], {"code": "workflow_collection_input_limit"})
        self.assertEqual(set(journal.attempts), {"root/r0"})
        self.assertEqual(ready, [])

    def test_precondition_loop_can_skip_or_stop_after_final_allowed_iteration(self):
        node = {"kind": "while", "condition": {"path": ["continue"], "equals": True},
                "max_iterations": 2, "body": TASK}
        result, journal, _ = self.advance(node, {"continue": False})
        self.assertEqual(result, ("completed", {"continue": False}))
        self.assertEqual(journal.attempts, {})
        journal = CollectionJournal(lambda value: {"count": value.get("count", 0) + 1,
                                                   "continue": value.get("count", 0) < 1})
        self.assertEqual(self.advance(node, {"continue": True}, journal)[0],
                         ("completed", {"count": 2, "continue": False}))
        self.assertEqual(set(journal.attempts), {"root/i0", "root/i1"})

    def test_precondition_loop_fails_at_bound_and_never_allocates_extra_iteration(self):
        node = {"kind": "while", "condition": {"path": ["continue"], "equals": True},
                "max_iterations": 2, "body": TASK}
        journal = CollectionJournal(lambda value: value)
        result, journal, _ = self.advance(node, {"continue": True}, journal)
        self.assertEqual(result, ("failed", None))
        self.assertEqual(set(journal.attempts), {"root/i0", "root/i1"})
        self.assertEqual(journal.nodes["root"]["error"], {"code": "iteration_limit"})

    def test_switch_selects_only_one_named_branch_and_validates_every_reference(self):
        constant = lambda value: {"kind": "transform", "fields": {"route": {"literal": value}}}
        node = {"kind": "switch", "select": {"path": ["type"], "type": "integer"},
                "cases": [{"name": "true", "equals": True, "body": constant("boolean")},
                          {"name": "one", "equals": 1, "body": constant("integer")}],
                "default": constant("default")}
        result, journal, _ = self.advance(node, {"type": 1})
        self.assertEqual(result, ("completed", {"route": "integer"}))
        self.assertEqual(set(journal.nodes), {"root", "root/case-one"})
        self.assertEqual(self.advance(node, {"type": 2})[0], ("completed", {"route": "default"}))
        node["default"] = {"kind": "task", "workflow": "missing"}
        plan = WorkflowPlan(spec(node))
        self.assertCode("unknown_leaf_workflow", lambda: validate_registry({"pipeline": plan}, {}))

    def test_new_nodes_have_conservative_transitive_costs_and_immutable_bindings(self):
        child = WorkflowPlan(spec({"kind": "sequence", "steps": [TASK, TASK]}, name="child"))
        parent = WorkflowPlan(spec(collection("map", {"kind": "subworkflow", "workflow": "child"})))
        bound = bind_plan("pipeline", {"pipeline": parent, "child": child}, {"compute": leaf()})
        self.assertEqual(bound["max_steps"], 1 + 3 * (1 + 3))
        changed = parent.spec
        changed["root"]["max_input_bytes"] = 10000
        self.assertNotEqual(parent.identity, WorkflowPlan(changed).identity)
        limited = parent.spec
        limited["limits"]["max_steps"] = 12
        self.assertCode("workflow_plan_step_limit", lambda: bind_plan("pipeline",
            {"pipeline": WorkflowPlan(limited), "child": child}, {"compute": leaf()}))

    def test_collection_limits_and_duplicate_routes_fail_at_registration(self):
        for update in ({"max_items": 0}, {"max_items": True}, {"max_items": 1001},
                       {"max_input_bytes": 0}, {"max_input_bytes": 1048577},
                       {"items": {"path": [], "type": "object"}}):
            with self.subTest(update=update), self.assertRaises(ServiceError):
                WorkflowPlan(spec({**collection(), **update}))
        cases = [{"name": "a", "equals": 1, "body": TASK},
                 {"name": "b", "equals": 1, "body": TASK}]
        self.assertCode("duplicate_workflow_switch_case", lambda: WorkflowPlan(spec({"kind": "switch",
            "select": {"path": [], "type": "object"}, "cases": cases, "default": TASK})))


class CollectionEffect:
    def __init__(self, receipt):
        self.receipt = receipt

    def handle(self, context, request):
        supplied = request.input

        def effect():
            with self.receipt.open("a") as stream:
                stream.write(json.dumps({"call_id": context.invocation_id, "input": supplied}) + "\n")
            return Invocation({"value": supplied.get("accumulator", {"value": 0})["value"]
                               + supplied["item"]}, {"operation": "collection-example"})

        return Outcome(context.invoke(effect, input=supplied, reservation_microusd=0))


class WorkflowCollectionIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.receipt = self.root / "effects.jsonl"

    def engine(self, root):
        worker = CollectionEffect(self.receipt)
        ingress = FunctionAdapter(lambda _context, request: Outcome(request.input))
        workflow = Workflow("collection/1", topology(agents=("a", "b")),
            {"a": ingress, "b": worker}, "a", "b", "compute")
        return WorkflowEngine(self.root / "state", {"pipeline": WorkflowPlan(spec(root))},
            {"compute": workflow}, {"owner": {"pipeline"}},
            mas_bin=runner_tests.RunnerIntegrationTests.mas,
            gateway_bin=runner_tests.RunnerIntegrationTests.gateway)

    def test_map_then_reduce_runs_real_admission_and_replays_without_repeating_effects(self):
        mapping = collection(body={"kind": "sequence", "steps": [TASK,
            {"kind": "transform", "fields": {"result": {"path": ["value"], "type": "integer"}}}]})
        reducing = collection("reduce", {"kind": "sequence", "steps": [
            {"kind": "transform", "fields": {
                "item": {"path": ["item", "result"], "type": "integer"},
                "accumulator": {"path": ["accumulator"], "type": "object"}}}, TASK]})
        engine = self.engine({"kind": "sequence", "steps": [mapping, reducing]})
        submitted = engine.submit("owner", "request", "pipeline", {"items": [2, 5, 8]})
        result = engine.run("owner", submitted["instance_id"])
        self.assertEqual(result["state"], "completed", result)
        self.assertEqual(result["output"], {"value": 15})
        receipts = self.receipt.read_text().splitlines()
        self.assertEqual(len(receipts), 6)
        self.assertEqual(len({json.loads(line)["call_id"] for line in receipts}), 6)
        self.assertEqual(result["usage"]["calls"], 6)
        self.assertEqual(engine.run("owner", submitted["instance_id"]), result)
        self.assertEqual(self.receipt.read_text().splitlines(), receipts)

    def test_oversized_collection_is_failed_without_run_allocation(self):
        engine = self.engine(collection(maximum=1))
        submitted = engine.submit("owner", "request", "pipeline", {"items": [2, 5]})
        result = engine.run("owner", submitted["instance_id"])
        self.assertEqual(result["state"], "failed", result)
        self.assertEqual(result["attempts"], [])
        self.assertEqual(result["usage"]["calls"], 0)
        self.assertFalse(self.receipt.exists())


if __name__ == "__main__":
    unittest.main()
