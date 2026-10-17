"""Decision regressions independent of the process scheduler and SQLite."""
import copy
import unittest
from unittest.mock import patch

from mas_host.workflow_evaluator import PlanEvaluator
from mas_host.workflow_plan import WorkflowPlan


def plan(name, root, *, seconds=60):
    return WorkflowPlan({"format": "mas-workflow/1", "name": name, "version": "1",
        "limits": {"max_steps": 20, "max_parallel": 2, "wall_seconds": seconds,
                   "max_calls": 10, "max_reserved_microusd": 0}, "root": root})


class ObservedJournal:
    """Controlled task observations; the evaluator owns no execution mechanism."""
    def __init__(self):
        self.nodes = {}
        self.observations = {}
        self.scope_deadlines = {}
        self.clock = 100

    def node(self, instance, path, kind, input, limits):
        return self.nodes.setdefault(path, {"state": "waiting", "output": None})

    def attempt(self, instance, path, workflow, input, deadline):
        return copy.deepcopy(self.observations[workflow])

    def finish_node(self, instance, path, state, output=None, error=None):
        self.nodes[path] = {"state": state, "output": copy.deepcopy(output), "error": error}
        return state, output

    def scope(self, instance, path, limits, deadline):
        return self.scope_deadlines.setdefault(path, min(deadline, self.clock + limits["wall_seconds"]))

    def observe(self, workflow, state, output=None):
        self.observations[workflow] = {"workflow": workflow, "state": state,
                                       "output": output, "error": None}


class WorkflowEvaluatorTests(unittest.TestCase):
    def test_completed_short_scope_survives_slower_parallel_sibling(self):
        child = plan("short", {"kind": "task", "workflow": "fast"}, seconds=1)
        parent = plan("parent", {"kind": "parallel", "branches": {
            "short": {"kind": "subworkflow", "workflow": "short"},
            "slow": {"kind": "task", "workflow": "slow"}}})
        journal = ObservedJournal()
        journal.observe("fast", "completed", {"verified": True})
        journal.observe("slow", "running")
        evaluator = PlanEvaluator({"parent": parent, "short": child}, journal)
        def advance():
            source = parent.spec
            with patch("mas_host.workflow_evaluator.time.time", return_value=journal.clock):
                return evaluator.advance("example", "root", source["root"], {},
                                         source["limits"], 160, [])
        self.assertEqual(advance(), ("waiting", None))
        self.assertEqual(journal.nodes["root/p-short"]["state"], "completed")
        self.assertEqual(journal.scope_deadlines["root/p-short/sub-short"], 101)
        journal.clock = 102  # Slow sibling takes two seconds; the child already finished.
        journal.observe("slow", "completed", {"slow": True})
        self.assertEqual(advance(), ("completed", {"branches": {
            "short": {"verified": True}, "slow": {"slow": True}}}))

    def test_failed_join_is_revisited_until_live_sibling_uncertainty_is_observed(self):
        source = plan("parent", {"kind": "parallel", "branches": {
            "failed": {"kind": "task", "workflow": "failed"},
            "live": {"kind": "task", "workflow": "live"}}}).spec
        journal = ObservedJournal()
        journal.observe("failed", "failed")
        journal.observe("live", "running")
        evaluator = PlanEvaluator({}, journal)
        def advance():
            return evaluator.advance("example", "root", source["root"], {},
                                     source["limits"], 160, [])
        self.assertEqual(advance(), ("failed", None))
        journal.observe("live", "unknown")
        self.assertEqual(advance(), ("unknown", None))
        self.assertEqual(journal.nodes["root/p-live"]["state"], "unknown")

    def test_completed_join_reuses_outputs_without_reopening_child_work(self):
        source = plan("parent", {"kind": "sequence", "steps": [
            {"kind": "task", "workflow": "leaf"}]}).spec
        journal = ObservedJournal()
        journal.observe("leaf", "completed", {"value": 7})
        evaluator = PlanEvaluator({}, journal)
        def advance():
            return evaluator.advance("example", "root", source["root"], {},
                                     source["limits"], 160, [])
        self.assertEqual(advance(), ("completed", {"value": 7}))
        journal.observations.clear()  # Any child re-observation would now fail.
        self.assertEqual(advance(), ("completed", {"value": 7}))
