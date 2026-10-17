"""Only distinct, candidate-bound, context-bound reviewer results form quorum."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.service_contracts import ServiceError
from mas_host.task_service import Workflow
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_evaluator import PlanEvaluator
from mas_host.workflow_plan import WorkflowPlan, bind_plan
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests
from test_workflow_joins import JoinJournal, parallel
from test_workflow_plan import leaf, spec


SUBJECT = {"id": "candidate-1", "sha256": "1" * 64}
CONTEXT = {"rubric": {"id": "rubric-1", "sha256": "2" * 64}, "evidence_sha256": "3" * 64}


def input_value():
    return {"candidate": copy.deepcopy(SUBJECT), "review_context": copy.deepcopy(CONTEXT)}


def vote():
    return {"accepted": True, "candidate": copy.deepcopy(SUBJECT), "context": copy.deepcopy(CONTEXT)}


def agreement():
    node = parallel("quorum", {name: {"kind": "task", "workflow": name} for name in ("a", "b", "c")})
    node["join"]["agreement"] = {"version": "mas-candidate-quorum/1",
        "candidate": {"path": ["candidate"], "type": "object"},
        "context": {"path": ["review_context"], "type": "object"}}
    return node


def registered(actor, output):
    return Workflow("1", topology(("caller", actor)),
        {"caller": FunctionAdapter(lambda _context, request: Outcome(request.input)),
         actor: FunctionAdapter(lambda _context, _request: Outcome(output))},
        "caller", actor, "review")


class WorkflowAgreementTests(unittest.TestCase):
    def advance(self, journal, supplied=None):
        plan = WorkflowPlan(spec(agreement())).spec
        ready = []
        result = PlanEvaluator({}, journal).advance("instance", "root", plan["root"],
            input_value() if supplied is None else supplied, plan["limits"], 1000, ready)
        return result, ready

    def test_foreign_candidates_and_changed_context_cannot_form_quorum(self):
        for field, replacement in (("candidate", {**SUBJECT, "id": "other"}),
                                   ("candidate", {**SUBJECT, "sha256": "0" * 64}),
                                   ("context", {**CONTEXT, "evidence_sha256": "0" * 64}),
                                   ("accepted", 1)):
            journal = JoinJournal()
            journal.results["root/p-a"] = {"state": "completed", "output": vote()}
            journal.results["root/p-b"] = {"state": "completed", "output": {**vote(), field: replacement}}
            result, _ = self.advance(journal)
            self.assertEqual(result[0], "waiting")
            self.assertEqual(journal.decisions, {})
            journal.results["root/p-c"] = {"state": "completed", "output": vote()}
            result, _ = self.advance(journal)
            self.assertEqual(result[0], "completed")
            self.assertEqual(result[1]["join"]["selected"], ["a", "c"])
            self.assertEqual(result[1]["join"]["agreement"]["candidate"], SUBJECT)

    def test_agreement_binding_is_validated_before_branch_allocation(self):
        for value in ({}, {"candidate": SUBJECT, "review_context": {}},
                      {"candidate": {"id": "candidate-1", "sha256": "bad"}, "review_context": CONTEXT}):
            journal = JoinJournal()
            result, ready = self.advance(journal, value)
            self.assertEqual(result[0], "failed")
            self.assertEqual(journal.attempts, {})
            self.assertEqual(ready, [])

    def test_distinct_registered_reviewer_targets_are_required(self):
        plan = WorkflowPlan(spec(agreement()))
        with self.assertRaises(ServiceError) as error:
            bind_plan("pipeline", {"pipeline": plan}, {name: leaf() for name in ("a", "b", "c")})
        self.assertEqual(error.exception.code, "workflow_agreement_requires_distinct_actors")
        binding = bind_plan("pipeline", {"pipeline": plan},
            {name: registered("reviewer-" + name, vote()) for name in ("a", "b", "c")})
        self.assertEqual(binding["max_steps"], 4)

    def test_old_predicate_count_plan_has_identical_source_and_identity(self):
        source = spec(parallel("quorum"))
        identity = hashlib.sha256(json.dumps(source, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        self.assertEqual(WorkflowPlan(source).identity, identity)
        self.assertEqual(WorkflowPlan(source).spec, source)

    def test_agreement_rejects_unknown_profile_and_non_task_branches(self):
        for change in (lambda node: node["join"]["agreement"].update(version="unknown"),
                       lambda node: node["branches"].update(a={"kind": "transform", "fields": {}}),
                       lambda node: node["join"].update(mode="first-accepted")):
            node = agreement()
            change(node)
            with self.assertRaises(ServiceError):
                WorkflowPlan(spec(node))


class WorkflowAgreementIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def test_candidate_bound_winner_decision_survives_store_reopen(self):
        with tempfile.TemporaryDirectory() as temporary:
            wrong = {**vote(), "candidate": {**SUBJECT, "id": "foreign-candidate"}}
            workflows = {name: registered("reviewer-" + name, wrong if name == "b" else vote())
                         for name in ("a", "b", "c")}
            source = spec(agreement())
            source["limits"]["max_parallel"] = 1
            def engine():
                return WorkflowEngine(Path(temporary), {"pipeline": WorkflowPlan(source)}, workflows,
                    {"owner": {"pipeline"}}, mas_bin=runner_tests.RunnerIntegrationTests.mas,
                    gateway_bin=runner_tests.RunnerIntegrationTests.gateway)
            first = engine()
            instance = first.submit("owner", "request-1", "pipeline", input_value())["instance_id"]
            completed = first.run("owner", instance)
            self.assertEqual(completed["state"], "completed", completed)
            self.assertEqual(completed["output"]["join"]["selected"], ["a", "c"])
            self.assertEqual([attempt["state"] for attempt in completed["attempts"]], ["completed"] * 3)
            self.assertEqual(engine().run("owner", instance), completed)


if __name__ == "__main__":
    unittest.main()
