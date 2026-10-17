"""Composition rejects invalid work before any registered adapter is invoked."""
from dataclasses import FrozenInstanceError, replace
import unittest

from mas_host.service_contracts import ServiceError
from mas_host.task_service import Workflow
from mas_host.workflow_plan import WorkflowPlan, bind_plan, evaluate_condition, validate_registry


class NeverRun:
    def handle(self, context, request):
        raise AssertionError("plan validation must not invoke adapters")


def leaf(version="1"):
    return Workflow(version, {"agents": ["caller", "worker"],
        "edges": [["caller", "worker"], ["worker", "caller"]]},
        {"caller": NeverRun(), "worker": NeverRun()}, "caller", "worker", "compute")


def spec(root=None, *, name="pipeline", max_steps=100):
    return {"format": "mas-workflow/1", "name": name, "version": "1",
            "limits": {"max_steps": max_steps, "max_parallel": 4,
                       "wall_seconds": 600, "max_calls": 100,
                       "max_reserved_microusd": 0},
            "root": root or {"kind": "task", "workflow": "compute"}}


class WorkflowPlanTests(unittest.TestCase):
    def assertCode(self, code, callback):
        with self.assertRaises(ServiceError) as raised:
            callback()
        self.assertEqual(raised.exception.code, code)

    def test_source_is_immutable_and_hash_ignores_object_order(self):
        source = spec()
        plan = WorkflowPlan(source)
        identity = plan.identity
        source["root"]["workflow"] = "different"
        plan.spec["limits"]["max_steps"] = 1
        self.assertEqual(plan.spec["root"]["workflow"], "compute")
        self.assertEqual(plan.spec["limits"]["max_steps"], 100)
        self.assertEqual(plan.identity, identity)
        reordered = dict(reversed(list(plan.spec.items())))
        self.assertEqual(WorkflowPlan(reordered).identity, identity)
        with self.assertRaises(FrozenInstanceError):
            plan._source = "{}"

    def test_optional_timing_policy_is_normalised_and_bound_without_changing_legacy_sources(self):
        source = spec()
        legacy = WorkflowPlan(source)
        self.assertNotIn("timing_policy", legacy.spec)
        source["timing_policy"] = {"business_seconds": 7200, "active_seconds": 600,
                                   "queue_seconds": 60, "pause_seconds": 3600}
        timed = WorkflowPlan(source)
        self.assertEqual(timed.spec["timing_policy"]["format"], "mas-timing-policy/1")
        self.assertNotEqual(timed.identity, legacy.identity)
        self.assertEqual(WorkflowPlan(timed.spec).identity, timed.identity)
        for invalid in (None, {}, {**source["timing_policy"], "active_seconds": True}):
            with self.subTest(invalid=invalid):
                self.assertCode("invalid_workflow_timing_policy", lambda: WorkflowPlan(
                    {**source, "timing_policy": invalid}))

    def test_all_control_forms_have_conservative_expansion(self):
        task = {"kind": "task", "workflow": "compute"}
        choice = {"kind": "choose", "condition": {"path": ["needed"], "equals": True},
                  "then": task, "else": {"kind": "sequence", "steps": [task, task]}}
        repeated = {"kind": "loop", "until": {"path": ["done"], "equals": True},
                    "body": choice, "max_iterations": 3}
        root = {"kind": "sequence", "steps": [task,
                {"kind": "parallel", "branches": {"left": task, "right": repeated}}]}
        plan = WorkflowPlan(spec(root))
        binding = bind_plan("pipeline", {"pipeline": plan}, {"compute": leaf()})
        # task 1; choice 1+max(1,3)=4; loop 1+3*4=13;
        # parallel 1+1+13=15; root sequence 1+1+15=17.
        self.assertEqual(binding["max_steps"], 17)
        self.assertEqual(binding["plans"], {})
        self.assertEqual(set(binding["workflows"]), {"compute"})

    def test_bind_tracks_transitive_inputs_without_unrelated_registrations(self):
        child = WorkflowPlan(spec(name="child"))
        parent = WorkflowPlan(spec({"kind": "subworkflow", "workflow": "child"}))
        plans = {"pipeline": parent, "child": child}
        workflows = {"compute": leaf()}
        binding = bind_plan("pipeline", plans, workflows)
        self.assertEqual(binding["max_steps"], 2)
        self.assertEqual(binding["plans"], {"child": child.identity})
        self.assertNotEqual(binding["identity"], parent.identity)
        self.assertNotEqual(binding["identity"], bind_plan("pipeline", plans,
                            {"compute": leaf("2")})["identity"])
        changed_child = child.spec
        changed_child["version"] = "2"
        self.assertNotEqual(binding["identity"], bind_plan("pipeline",
            {**plans, "child": WorkflowPlan(changed_child)}, workflows)["identity"])
        unrelated = WorkflowPlan(spec(name="other"))
        self.assertEqual(binding["identity"], bind_plan("pipeline",
            {**plans, "other": unrelated}, {**workflows, "unused": leaf("2")})["identity"])
        binding["spec"]["root"]["workflow"] = "changed"
        self.assertEqual(parent.spec["root"]["workflow"], "child")

    def test_transitive_leaf_identity_includes_route_and_hook_changes(self):
        plan = WorkflowPlan(spec())
        workflow = leaf()
        original = bind_plan("pipeline", {"pipeline": plan}, {"compute": workflow})["identity"]
        changed = replace(workflow, max_depth=9)
        self.assertNotEqual(original, bind_plan("pipeline", {"pipeline": plan},
                           {"compute": changed})["identity"])

    def test_missing_references_and_cycles_are_rejected(self):
        plan = WorkflowPlan(spec())
        self.assertCode("unknown_leaf_workflow",
            lambda: validate_registry({"pipeline": plan}, {}))
        reference = WorkflowPlan(spec({"kind": "subworkflow", "workflow": "missing"}))
        self.assertCode("unknown_subworkflow",
            lambda: validate_registry({"pipeline": reference}, {}))
        self_reference = WorkflowPlan(spec({"kind": "subworkflow", "workflow": "pipeline"}))
        self.assertCode("workflow_plan_cycle",
            lambda: validate_registry({"pipeline": self_reference}, {}))
        a = WorkflowPlan(spec({"kind": "subworkflow", "workflow": "b"}, name="a"))
        b = WorkflowPlan(spec({"kind": "subworkflow", "workflow": "a"}, name="b"))
        self.assertCode("workflow_plan_cycle", lambda: validate_registry({"a": a, "b": b}, {}))

    def test_unselected_branches_still_require_valid_registrations(self):
        plan = WorkflowPlan(spec({"kind": "choose", "condition": {"path": [], "equals": {}},
            "then": {"kind": "task", "workflow": "compute"},
            "else": {"kind": "task", "workflow": "missing"}}))
        self.assertCode("unknown_leaf_workflow", lambda: validate_registry(
            {"pipeline": plan}, {"compute": leaf()}))

    def test_maximum_node_visits_are_checked_locally_and_transitively(self):
        repeated = {"kind": "loop", "body": {"kind": "task", "workflow": "compute"},
                    "max_iterations": 10, "until": {"path": [], "equals": {}}}
        self.assertCode("workflow_plan_step_limit", lambda: WorkflowPlan(spec(repeated, max_steps=10)))
        child = WorkflowPlan(spec(repeated, name="child"))
        parent = WorkflowPlan(spec({"kind": "subworkflow", "workflow": "child"}, max_steps=11))
        self.assertCode("workflow_plan_step_limit", lambda: validate_registry(
            {"pipeline": parent, "child": child}, {"compute": leaf()}))

    def test_expanded_depth_cannot_hide_inside_subworkflows(self):
        plans = {"last": WorkflowPlan(spec(name="last"))}
        previous = "last"
        for index in range(16):
            name = f"layer-{index}"
            plans[name] = WorkflowPlan(spec({"kind": "subworkflow", "workflow": previous}, name=name))
            previous = name
        self.assertCode("invalid_workflow_plan_depth", lambda: validate_registry(plans, {"compute": leaf()}))

    def test_boolean_limits_and_out_of_range_values_are_rejected(self):
        cases = {"max_steps": [False, 0, 10001], "max_parallel": [True, 0, 33],
                 "wall_seconds": [False, 0, 86401], "max_calls": [True, 0, 100001],
                 "max_reserved_microusd": [False, -1, 1_000_000_000_001]}
        for name, values in cases.items():
            for value in values:
                with self.subTest(name=name, value=value):
                    source = spec()
                    source["limits"][name] = value
                    self.assertCode("invalid_workflow_plan_limit", lambda: WorkflowPlan(source))

    def test_schema_rejects_unknown_keys_empty_nodes_and_unsafe_names(self):
        sources = []
        extra = spec()
        extra["python"] = "os.system"
        sources.append(extra)
        for root in ({"kind": "sequence", "steps": []},
                     {"kind": "parallel", "branches": {}},
                     {"kind": "task", "workflow": "compute", "timeout": 5},
                     {"kind": "eval", "expression": "x"},
                     {"kind": "parallel", "branches": {"../escape": {"kind": "task", "workflow": "compute"}}}):
            sources.append(spec(root))
        for source in sources:
            with self.subTest(source=source), self.assertRaises(ServiceError):
                WorkflowPlan(source)

    def test_bounds_reject_oversized_non_json_and_recursive_sources(self):
        source = spec()
        source["version"] = "x" * 65536
        with self.assertRaises(ServiceError):
            WorkflowPlan(source)
        source = spec({"kind": "choose", "condition": {"path": [], "equals": 1.5},
                       "then": {"kind": "task", "workflow": "compute"},
                       "else": {"kind": "task", "workflow": "compute"}})
        self.assertCode("invalid_workflow_plan", lambda: WorkflowPlan(source))
        recursive = spec()
        recursive["root"] = recursive
        self.assertCode("invalid_workflow_plan", lambda: WorkflowPlan(recursive))

    def test_registrations_require_matching_names_and_real_leaf_workflows(self):
        plan = WorkflowPlan(spec())
        self.assertCode("workflow_plan_name_mismatch", lambda: validate_registry({"alias": plan}, {}))
        self.assertCode("invalid_workflow_plan_registry", lambda: validate_registry(
            {"pipeline": plan}, {"compute": object()}))
        self.assertCode("unknown_workflow_plan", lambda: bind_plan("missing", {}, {}))


class WorkflowConditionTests(unittest.TestCase):
    def test_path_selects_only_explicit_container_types(self):
        value = {"results": [{"ok": True}], "0": 7, "null": None}
        self.assertTrue(evaluate_condition({"path": ["results", 0, "ok"], "equals": True}, value))
        self.assertFalse(evaluate_condition({"path": ["results", "0", "ok"], "equals": True}, value))
        self.assertFalse(evaluate_condition({"path": [0], "equals": 7}, value))
        self.assertFalse(evaluate_condition({"path": ["results", 1], "equals": None}, value))
        self.assertTrue(evaluate_condition({"path": ["null"], "equals": None}, value))
        self.assertFalse(evaluate_condition({"path": ["missing"], "equals": None}, value))
        self.assertFalse(evaluate_condition({"path": ["null", "child"], "equals": None}, value))

    def test_structural_equality_is_order_independent_and_type_exact(self):
        condition = {"path": [], "equals": {"a": [True], "b": 2}}
        self.assertTrue(evaluate_condition(condition, {"b": 2, "a": [True]}))
        self.assertFalse(evaluate_condition(condition, {"b": 2, "a": [1]}))
        self.assertFalse(evaluate_condition({"path": [], "equals": True}, 1))
        self.assertFalse(evaluate_condition({"path": [], "equals": False}, 0))

    def test_malformed_conditions_never_coerce_or_evaluate_expressions(self):
        for condition in ({"path": [True], "equals": True}, {"path": [-1], "equals": 1},
                          {"path": [2_147_483_648], "equals": 1},
                          {"path": "input.x", "equals": 1}, {"path": [], "equals": 1.0},
                          {"path": [], "equals": 1, "expression": "__import__('os')"}):
            with self.subTest(condition=condition), self.assertRaises(ServiceError):
                evaluate_condition(condition, {})
        with self.assertRaises(ServiceError):
            evaluate_condition({"path": [], "equals": None}, object())
