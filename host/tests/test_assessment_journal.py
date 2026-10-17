"""Assessment authority, exact history and revision recovery against real Rust."""
from __future__ import annotations

import copy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mas_host import AdapterError, Invocation, Outcome, RecoveryRequired, Runner, RunnerError
from mas_host.assessment_journal import AssessmentJournal
from mas_host.codec import json_identity
from mas_host.journal import Journal, inspect_run, resolve_invocation
from mas_host.protocol_client import ProtocolClient
from mas_host.service_contracts import ServiceError
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


def reference(record):
    return {"id": record["id"], "sha256": json_identity(record)}


def candidate(name="candidate-1", output=None, parent=None, feedback=None):
    output = {"answer": 41} if output is None else output
    return {"schema_version": 1, "id": name, "output": output,
            "output_sha256": json_identity(output), "parent": parent, "feedback": feedback or []}


EVIDENCE = {"schema_version": 1, "id": "check-1", "content": {"expected": 42},
            "content_sha256": json_identity({"expected": 42})}
RUBRIC = {"schema_version": 1, "id": "rubric-1", "criteria": [
    {"id": "exact", "measurement": "deterministic", "minimum": 0, "maximum": 1}]}
EVALUATOR = {"id": "b", "version": "checker-1", "kind": "deterministic"}


def policy(visibility="full", max_revisions=1):
    return {"format": "mas-assessment-policy/1", "evaluator": copy.deepcopy(EVALUATOR),
            "rubric": copy.deepcopy(RUBRIC), "max_revisions": max_revisions,
            "candidate": {"path": ["candidate"], "type": "object"},
            "evidence": {"path": ["evidence"], "type": "array"},
            "feedback_visibility": visibility, "max_feedback": 4, "max_feedback_bytes": 12000}


def invocation_input():
    return {"question": "six times seven", "candidate": candidate(), "evidence": [copy.deepcopy(EVIDENCE)]}


def assessment(name="assessment-1", parent=None, verdict="fail"):
    return {"schema_version": 1, "id": name, "candidate": reference(parent or candidate()),
            "evaluator": copy.deepcopy(EVALUATOR), "rubric": reference(RUBRIC),
            "status": "completed", "findings": [{"criterion_id": "exact", "verdict": verdict,
                "score": int(verdict == "pass"), "evidence": [reference(EVIDENCE)],
                "explanation": "Compared the exact expected integer."}], "error": None}


class AssessmentJournalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.client = ProtocolClient(self.mas, self.gateway)
        self.journal = Journal(self.root, {"run_id": "run", "workflow_version": "test-1",
                                           "topology": {}, "max_depth": 8})
        self.addCleanup(lambda: self.journal.close())

    def session(self, selected_policy=None):
        return AssessmentJournal(self.journal, self.client.command, self.client.mas,
            scope={"run_id": "run", "actor": "b", "request_id": "request-1"},
            policy=selected_policy or policy(), input=invocation_input(), deadline=12345)

    def populated(self, selected_policy=None):
        session = self.session(selected_policy)
        session.execute("candidate", candidate())
        session.execute("evidence", EVIDENCE)
        session.execute("assess", assessment())
        return session

    def test_exact_duplicate_and_conflicting_identity_survive_reopen(self):
        session = self.populated()
        self.assertFalse(session.execute("assess", assessment())["inserted"])
        conflict = assessment(verdict="pass")
        with self.assertRaisesRegex(RunnerError, "identity content conflict"):
            session.execute("assess", conflict)
        independent = assessment("assessment-2", verdict="pass")
        self.assertTrue(session.execute("assess", independent)["inserted"])
        self.journal.close()
        self.journal = Journal(self.root)
        self.assertFalse(self.session().execute("assess", independent)["inserted"])

    def test_payload_cannot_choose_evaluator_rubric_or_stale_reference(self):
        session = self.populated()
        for mutate in (lambda value: value["evaluator"].update(version="forged"),
                       lambda value: value["evaluator"].update(id="peer"),
                       lambda value: value["rubric"].update(sha256="0" * 64),
                       lambda value: value["candidate"].update(sha256="0" * 64),
                       lambda value: value["findings"][0]["evidence"][0].update(sha256="0" * 64)):
            value = assessment("forged")
            mutate(value)
            with self.assertRaises(RunnerError):
                session.execute("assess", value)
        self.assertIsNone(session._row("record:forged"))

    def test_worker_cannot_nominate_foreign_candidate_or_evidence(self):
        session = self.populated()
        foreign = candidate("foreign-candidate", {"answer": 42})
        with self.assertRaisesRegex(RunnerError, "frozen assessment invocation"):
            session.execute("candidate", foreign)
        forged = assessment("foreign-assessment", parent=foreign)
        with self.assertRaisesRegex(RunnerError, "frozen assessment lineage"):
            session.execute("assess", forged)
        changed = copy.deepcopy(EVIDENCE)
        changed["content"] = {"expected": 41}
        changed["content_sha256"] = json_identity(changed["content"])
        with self.assertRaisesRegex(RunnerError, "frozen assessment invocation"):
            session.execute("evidence", changed)

    def test_inconclusive_record_replays_without_becoming_a_pass(self):
        session = self.populated()
        record = assessment("assessment-inconclusive")
        record["status"] = "inconclusive"
        record["findings"][0].update(verdict="inconclusive", score=None, evidence=[])
        self.assertFalse(session.execute("assess", record)["passes"])
        self.journal.close()
        self.journal = Journal(self.root)
        replay = self.session().execute("assess", record)
        self.assertFalse(replay["passes"])
        self.assertFalse(replay["inserted"])
        self.assertFalse(self.session().final_result({"assessment": replay["reference"]}, "unresolved")["accepted"])

    def test_error_assessment_is_retained_and_never_passes(self):
        session = self.populated()
        error = assessment("assessment-error")
        error.update(status="error", findings=[], error="checker unavailable")
        result = session.execute("assess", error)
        self.assertFalse(result["passes"])
        self.assertFalse(session.execute("assess", error)["inserted"])
        final = session.final_result({"assessment": result["reference"]}, "completed")
        self.assertFalse(final["accepted"])
        self.assertEqual(session.final_result({"assessment": result["reference"]}, "completed"), final)
        passed = session.execute("assess", assessment("different-final", verdict="pass"))
        with self.assertRaisesRegex(RunnerError, "final assessment result"):
            session.final_result({"assessment": passed["reference"]}, "completed")

    def test_revision_replays_original_feedback_and_frozen_context_after_restart(self):
        session = self.populated()
        request = {"request_key": "revision-1", "parent": reference(candidate())}
        original = session.execute("reserve_revision", request)
        session.execute("assess", assessment("later-assessment", verdict="pass"))
        self.journal.close()
        self.journal = Journal(self.root)
        session = self.session()
        self.assertEqual(session.execute("reserve_revision", request), original)
        self.assertEqual([item["id"] for item in original["feedback"]], ["assessment-1"])
        revised = candidate("candidate-2", {"answer": 42}, reference(candidate()), [reference(assessment())])
        result = session.execute("complete_revision", {"request_key": "revision-1", "candidate": revised})
        self.assertTrue(result["inserted"])
        self.assertFalse(session.execute("complete_revision", {"request_key": "revision-1", "candidate": revised})["inserted"])
        with self.assertRaisesRegex(RunnerError, "revision limit"):
            session.execute("reserve_revision", {"request_key": "revision-2", "parent": reference(revised)})
        with self.assertRaises(RunnerError):
            self.session(policy("withheld"))

    def test_withheld_feedback_cannot_be_added_to_revision_links(self):
        session = self.populated(policy("withheld"))
        context = session.execute("reserve_revision", {"request_key": "revision-1", "parent": reference(candidate())})
        self.assertEqual(context["feedback"], [])
        self.assertEqual(context["deadline_ms"], 12345)
        smuggled = candidate("candidate-2", {"answer": 42}, reference(candidate()), [reference(assessment())])
        with self.assertRaises(RunnerError):
            session.execute("complete_revision", {"request_key": "revision-1", "candidate": smuggled})
        resample = candidate("candidate-2", {"answer": 42}, reference(candidate()))
        self.assertTrue(session.execute("complete_revision", {"request_key": "revision-1", "candidate": resample})["inserted"])

    def test_feedback_bound_is_checked_before_reservation_is_consumed(self):
        selected = policy()
        selected["max_feedback"] = 0
        session = self.populated(selected)
        with self.assertRaisesRegex(RunnerError, "feedback count limit"):
            session.execute("reserve_revision", {"request_key": "revision-1", "parent": reference(candidate())})
        self.assertIsNone(session._row("revision:revision-1"))

    def test_registered_agreement_context_is_detached_and_bound_to_the_final_receipt(self):
        selected = policy()
        selected["context"] = {"path": ["review_context"], "type": "object"}
        supplied = {**invocation_input(), "review_context": {"experiment": "incident-study-1"}}
        session = AssessmentJournal(self.journal, self.client.command, self.client.mas,
            scope={"run_id": "run", "actor": "b", "request_id": "context-1"},
            policy=selected, input=supplied, deadline=12345)
        supplied["review_context"]["experiment"] = "changed"
        selected["evaluator"]["version"] = "changed"
        record = session.execute("assess", assessment())
        expected_context = {"rubric": reference(RUBRIC), "evidence": [reference(EVIDENCE)],
                            "application": {"experiment": "incident-study-1"}}
        output = session.final_result({"assessment": record["reference"],
            "context": expected_context}, "completed")
        self.assertEqual(output["context"], expected_context)
        self.assertEqual(output["rubric"], reference(RUBRIC))
        self.assertEqual(output["profile"], "mas-assessment-result/1")

    def test_identical_application_context_cannot_hide_different_rubric_or_evidence(self):
        from mas_host.workflow_agreement import agrees
        outputs = []
        for index in (1, 2):
            selected = policy()
            selected["context"] = {"path": ["review_context"], "type": "object"}
            supplied = {**invocation_input(), "review_context": {"experiment": "same-study"}}
            record = assessment("assessment-" + str(index), verdict="pass")
            if index == 2:
                selected["rubric"]["id"] = "rubric-2"
                supplied["evidence"][0]["id"] = "check-2"
                supplied["evidence"][0]["content"] = {"expected": 43}
                supplied["evidence"][0]["content_sha256"] = json_identity({"expected": 43})
                record["rubric"] = reference(selected["rubric"])
                record["findings"][0]["evidence"] = [reference(supplied["evidence"][0])]
            session = AssessmentJournal(self.journal, self.client.command, self.client.mas,
                scope={"run_id": "run", "actor": "b", "request_id": "agreement-" + str(index)},
                policy=selected, input=supplied, deadline=12345)
            checked = session.execute("assess", record)
            outputs.append(session.final_result({"assessment": checked["reference"]}, "completed"))
        self.assertEqual(outputs[0]["context"]["application"], outputs[1]["context"]["application"])
        self.assertNotEqual(outputs[0]["context"]["rubric"], outputs[1]["context"]["rubric"])
        self.assertNotEqual(outputs[0]["context"]["evidence"], outputs[1]["context"]["evidence"])
        binding = {"candidate": outputs[0]["candidate"], "context": outputs[0]["context"]}
        self.assertTrue(agrees(outputs[0], binding))
        self.assertFalse(agrees(outputs[1], binding))

    def test_registered_context_api_operates_through_parent_owned_process_ipc(self):
        def checker(context, _request):
            context.assessment("candidate", candidate())
            context.assessment("evidence", EVIDENCE)
            result = context.assessment("assess", assessment())
            return Outcome({"passes": result["passes"], "assessment": result["reference"]})
        adapters = {"a": FunctionAdapter(lambda *_args: Outcome({})), "b": FunctionAdapter(checker)}
        with Runner(topology(("a", "b")), adapters, self.root / "runs", mas_bin=self.mas,
                    gateway_bin=self.gateway, workflow_version="assessment-1",
                    assessment_policies={"b": policy()}) as runner:
            result = runner.request("a", "b", "check", invocation_input())
            self.assertFalse(result.output["passes"])
            directory = runner.run_dir
        changed = policy()
        changed["max_revisions"] = 2
        with self.assertRaises(RunnerError):
            Runner.resume(directory, adapters, mas_bin=self.mas, gateway_bin=self.gateway,
                          workflow_version="assessment-1", assessment_policies={"b": changed})

    def test_inline_adapter_cannot_mutate_the_frozen_invocation_input(self):
        def checker(context, request):
            request.input["candidate"] = candidate("foreign-candidate", {"answer": 42})
            try:
                context.assessment("candidate", request.input["candidate"])
            except RunnerError:
                result = context.assessment("assess", assessment())
                return Outcome({"rejected_foreign": True, "passes": result["passes"], "assessment": result["reference"]})
            raise AssertionError("mutated candidate was accepted")
        adapters = {"a": FunctionAdapter(lambda *_args: Outcome({})), "b": FunctionAdapter(checker)}
        with Runner(topology(("a", "b")), adapters, self.root / "runs", mas_bin=self.mas,
                gateway_bin=self.gateway, workflow_version="assessment-1", supervised=False,
                assessment_policies={"b": policy()}) as runner:
            output = runner.request("a", "b", "check", invocation_input()).output
            self.assertTrue(output["rejected_foreign"])
            self.assertFalse(output["passes"])
            self.assertEqual(output["candidate"], reference(candidate()))

    def test_registered_evaluator_cannot_skip_forge_or_contradict_final_assessment(self):
        for kind in ("skip", "forged", "contradiction", "integer-accepted", "completed-unresolved", "context"):
            def checker(context, _request):
                if kind == "skip":
                    return Outcome({"accepted": True})
                result = context.assessment("assess", assessment())
                output = {"assessment": result["reference"]}
                if kind == "forged":
                    output["assessment"] = {**result["reference"], "sha256": "0" * 64}
                elif kind == "contradiction":
                    output["accepted"] = True
                elif kind == "integer-accepted":
                    output["accepted"] = 0
                elif kind == "context":
                    output["context"] = {"untrusted": "other evidence"}
                return Outcome(output, "unresolved" if kind == "completed-unresolved" else "completed")
            adapters = {"a": FunctionAdapter(lambda *_args: Outcome({})), "b": FunctionAdapter(checker)}
            with Runner(topology(("a", "b")), adapters, self.root / kind, mas_bin=self.mas,
                    gateway_bin=self.gateway, workflow_version="assessment-1", supervised=False,
                    assessment_policies={"b": policy()}) as runner:
                with self.assertRaises(AdapterError):
                    runner.request("a", "b", "check", invocation_input())
                self.assertEqual(runner.status()["tasks"][0]["state"], "failed")

    def test_malformed_nominated_candidate_prevents_evaluator_and_provider_execution(self):
        calls = []
        def checker(context, _request):
            calls.append(True)
            return Outcome(context.invoke(lambda: Invocation({}, {}), reservation_microusd=0))
        adapters = {"a": FunctionAdapter(lambda *_args: Outcome({})), "b": FunctionAdapter(checker)}
        malformed = invocation_input()
        malformed["candidate"]["output_sha256"] = "0" * 64
        with Runner(topology(("a", "b")), adapters, self.root / "preflight", mas_bin=self.mas,
                gateway_bin=self.gateway, workflow_version="assessment-1", supervised=False,
                assessment_policies={"b": policy()}) as runner:
            with self.assertRaises(AdapterError):
                runner.request("a", "b", "check", malformed)
            self.assertEqual(runner.status()["calls"], [])
        self.assertEqual(calls, [])

    def test_shared_profile_rejects_authority_before_any_native_admission(self):
        from mas_host.shared_authority import SharedAuthority
        from mas_host.shared_task_service import SharedTaskService
        from mas_host.shared_composition_service import SharedCompositionService
        from mas_host.task_service import Workflow
        authority = object.__new__(SharedAuthority)
        authority.command = mock.Mock(side_effect=AssertionError("native mutation must not run"))
        adapters = {"a": FunctionAdapter(lambda *_args: Outcome({})), "b": FunctionAdapter(lambda *_args: Outcome({}))}
        with self.assertRaisesRegex(RunnerError, "local SQLite"):
            authority.runner(topology(("a", "b")), adapters, self.root / "shared", principal="owner",
                             assessment_policies={"b": policy()})
        workflow = Workflow("1", topology(("a", "b")), adapters, "a", "b", "check",
                            assessment_policies={"b": policy()})
        for constructor in (lambda: SharedTaskService(authority, self.root / "shared", {"check": workflow}, {}),
                            lambda: SharedTaskService(authority, self.root / "shared", {}, {}, workflow_versions={"check": (workflow,)}),
                            lambda: SharedCompositionService(authority, self.root / "shared", {}, {"check": workflow}, {})):
            with self.assertRaises(ServiceError) as error:
                constructor()
            self.assertEqual(error.exception.code, "assessment_profile_requires_local_journal")
        authority.command.assert_not_called()
        self.assertFalse((self.root / "shared").exists())

    def test_unknown_revision_execution_replays_original_context_without_new_call(self):
        marker = self.root / "generation"
        def reviser(context, _request):
            context.assessment("candidate", candidate())
            context.assessment("evidence", EVIDENCE)
            context.assessment("assess", assessment())
            frozen = context.assessment("reserve_revision", {"request_key": "revision-1", "parent": reference(candidate())})
            def generate():
                marker.write_text("called")
                raise RuntimeError("lost provider response")
            output = context.invoke(generate, input=frozen, reservation_microusd=0)
            revised = candidate("candidate-2", output, reference(candidate()), [reference(assessment())])
            context.assessment("complete_revision", {"request_key": "revision-1", "candidate": revised})
            checked = context.assessment("assess", assessment("assessment-2", parent=revised, verdict="pass"))
            return Outcome({"assessment": checked["reference"]})
        adapters = {"a": FunctionAdapter(lambda *_args: Outcome({})), "b": FunctionAdapter(reviser)}
        runner = Runner(topology(("a", "b")), adapters, self.root / "runs", mas_bin=self.mas,
                        gateway_bin=self.gateway, workflow_version="assessment-1", supervised=False,
                        assessment_policies={"b": policy()})
        directory = runner.run_dir
        with self.assertRaises(RecoveryRequired):
            runner.request("a", "b", "check", invocation_input())
        runner.close()
        call = inspect_run(directory)["invocations"][0]
        resolve_invocation(directory, call["call_id"], decision="completed", output={"answer": 42},
                           evidence={"provider_receipt": "independently-observed"})
        with Runner.resume(directory, adapters, mas_bin=self.mas, gateway_bin=self.gateway,
                workflow_version="assessment-1", supervised=False, assessment_policies={"b": policy()}) as resumed:
            result = resumed.request("a", "b", "check", invocation_input())
            self.assertEqual(result.output["candidate"]["id"], "candidate-2")
            self.assertTrue(result.output["accepted"])
            self.assertEqual(len(resumed.status()["calls"]), 1)
        self.assertEqual(marker.read_text(), "called")


if __name__ == "__main__":
    unittest.main()
