"""Durable assessment.v1 history and bounded feedback in the host journal.

Rust owns record and reference validation. This module owns frozen invocation
authority, exact-content persistence and revision-input selection only.
"""
from __future__ import annotations

import json

from .codec import canonical_json, integer_json, is_actor_identity, json_identity
from .contracts import RunnerError
from .journal import JournalError


def assessment_policies(value, actors):
    """Copy trusted registration policies without changing legacy descriptors."""
    if value is None:
        return {}
    if type(value) is not dict or not set(value) <= set(actors):
        raise RunnerError("invalid assessment policy registration")
    result = json.loads(integer_json(value))
    required = {"format", "evaluator", "rubric", "candidate", "evidence", "max_revisions", "feedback_visibility",
                "max_feedback", "max_feedback_bytes"}
    for actor, policy in result.items():
        if (type(policy) is not dict or not required <= set(policy) or set(policy) - required - {"context"}
                or policy["format"] != "mas-assessment-policy/1"
                or type(policy["feedback_visibility"]) is not str
                or policy["feedback_visibility"] not in {"full", "withheld"}
                or type(policy["evaluator"]) is not dict
                or policy["evaluator"].get("id") != actor
                or type(policy["rubric"]) is not dict):
            raise RunnerError("invalid assessment policy registration")
        for field, lower, upper in (("max_revisions", 0, 32), ("max_feedback", 0, 32),
                                    ("max_feedback_bytes", 1, 32768)):
            if type(policy[field]) is not int or not lower <= policy[field] <= upper:
                raise RunnerError("invalid assessment policy bound")
        selectors = [("candidate", "object"), ("evidence", "array")]
        if "context" in policy:
            selectors.append(("context", "object"))
        for field, expected_type in selectors:
            selector = policy[field]
            if (type(selector) is not dict or set(selector) != {"path", "type"}
                    or selector["type"] != expected_type or type(selector["path"]) is not list
                    or len(selector["path"]) > 32
                    or any(type(part) is not str and (type(part) is not int or not 0 <= part <= 2_147_483_647)
                           for part in selector["path"])):
                raise RunnerError("invalid assessment input selector")
    return result


def freeze_assessment_policies(journal, command, mas, policies):
    """Freeze optional trusted authority before any request can use it."""
    if not hasattr(journal, "db"):
        if policies:
            raise RunnerError("assessment journal requires local SQLite persistence")
        return
    prior = journal.get_step("assessment-policies")
    if not policies and prior is None:
        return
    for policy in policies.values():
        for kind in ("evaluator", "rubric"):
            command([mas, "assessment", "-"], integer_json({
                "operation": "record", "kind": kind, "record": policy[kind]}))
    if (prior is None and journal.db.execute(
            "SELECT 1 FROM steps WHERE kind='request' LIMIT 1").fetchone()):
        raise RunnerError("resume cannot add assessment authority")
    try:
        journal.step("assessment-policies", "assessment_policies", policies, {"registered": True})
    except JournalError:
        raise RunnerError("registered assessment authority changed") from None


class AssessmentJournal:
    """Use existing transactional steps; never create a second execution runtime."""

    def __init__(self, journal, command, mas, *, scope, policy, input, deadline):
        self.journal, self.command, self.mas = journal, command, mas
        scope = json.loads(integer_json(scope))
        input = json.loads(integer_json(input))
        self.prefix = "assessment:" + json_identity(scope) + ":"
        self.policy = policy = json.loads(integer_json(policy))
        from .workflow_plan import select_value
        from .service_contracts import ServiceError
        try:
            self.candidate = select_value(policy["candidate"], input)
            self.evidence = select_value(policy["evidence"], input)
        except ServiceError:
            raise RunnerError("assessment invocation input does not match registration") from None
        checked_candidate = self._validate(operation="record", kind="candidate", record=self.candidate)
        self.target = checked_candidate["reference"]
        checked_evidence = [self._validate(operation="record", kind="evidence", record=record)
                            for record in self.evidence]
        if len({item["reference"]["id"] for item in checked_evidence}) != len(checked_evidence):
            raise RunnerError("duplicate invocation evidence identity")
        self.evidence_refs = [item["reference"] for item in checked_evidence]
        rubric = self._validate(operation="record", kind="rubric", record=policy["rubric"])
        self.rubric_ref = rubric["reference"]
        self.review_context = {"rubric": self.rubric_ref, "evidence": self.evidence_refs}
        if "context" in policy:
            try:
                application_context = select_value(policy["context"], input)
            except ServiceError:
                raise RunnerError("assessment context differs from registration") from None
            if not application_context:
                raise RunnerError("assessment context must be nonempty")
            self.review_context["application"] = application_context
        self.binding = {"scope": scope, "policy": policy, "input": input, "deadline_ms": deadline}
        try:
            self.journal.step(self.prefix + "binding", "assessment_binding", self.binding, {})
        except JournalError:
            raise RunnerError("assessment invocation binding changed") from None
        with self.journal.db:
            self.journal.db.execute("BEGIN IMMEDIATE")
            self._record("candidate", self.candidate)
            for record in self.evidence:
                self._record("evidence", record)

    def _validate(self, **operation):
        return self.command([self.mas, "assessment", "-"], integer_json(operation))

    def _row(self, key):
        return self.journal.db.execute("SELECT kind,data FROM steps WHERE key=?",
                                       (self.prefix + key,)).fetchone()

    def _record(self, kind, record):
        key = "record:" + kind + ":" + record["id"]
        source = canonical_json(record)
        prior = self._row(key)
        if prior is not None:
            if prior["kind"] != "assessment_" + kind or prior["data"] != source:
                raise RunnerError("assessment identity content conflict")
            return False
        self.journal.db.execute("INSERT INTO steps VALUES(?,?,?,?)",
            (self.prefix + key, "assessment_" + kind, "{}", source))
        return True

    def _resolve(self, reference, kind):
        if (type(reference) is not dict or set(reference) != {"id", "sha256"}
                or not is_actor_identity(reference["id"])):
            raise RunnerError("invalid assessment reference")
        row = self._row("record:" + kind + ":" + reference["id"])
        if row is None or row["kind"] != "assessment_" + kind:
            raise RunnerError("assessment reference unavailable")
        record = json.loads(row["data"])
        checked = self._validate(operation="record", kind=kind, record=record)
        if checked["reference"] != reference:
            raise RunnerError("assessment reference content mismatch")
        return record

    def execute(self, operation, data):
        """Accept records or reserve immutable revision inputs under frozen policy.

        This method is host-owned. The adapter supplies records and references;
        evaluator authority, rubric and disclosure policy come from registration.
        """
        if type(operation) is not str or type(data) is not dict:
            raise RunnerError("invalid assessment operation data")
        # A single host transaction includes validation-dependent reads, exact
        # history insertion and revision allocation. Run ownership serialises
        # callers; SQLite remains the durable atomicity boundary.
        with self.journal.db:
            self.journal.db.execute("BEGIN IMMEDIATE")
            if operation in {"candidate", "evidence"}:
                checked = self._validate(operation="record", kind=operation, record=data)
                allowed = [self.target] if operation == "candidate" else self.evidence_refs
                if checked["reference"] not in allowed:
                    raise RunnerError("record differs from frozen assessment invocation")
                inserted = self._record(operation, checked["record"])
                return {"reference": checked["reference"], "inserted": inserted}
            if operation == "assess":
                self._authorised_candidate(data.get("candidate"))
                candidate = self._resolve(data.get("candidate"), "candidate")
                checked = self._validate(operation="assessment", assessment=data, candidate=candidate,
                    rubric=self.policy["rubric"], evidence=self.evidence, authority=self.policy["evaluator"])
                inserted = self._record("assessment", data)
                return {**checked, "inserted": inserted}
            if operation == "reserve_revision":
                return self._reserve_revision(data)
            if operation == "complete_revision":
                return self._complete_revision(data)
            raise RunnerError("unsupported assessment operation")

    def final_result(self, output, status):
        """Gate a registered evaluator's terminal reply on retained validation."""
        with self.journal.db:
            self.journal.db.execute("BEGIN IMMEDIATE")
            record = self._resolve(output.get("assessment"), "assessment")
            self._authorised_candidate(record["candidate"])
            candidate = self._resolve(record["candidate"], "candidate")
            checked = self._validate(operation="assessment", assessment=record, candidate=candidate,
                rubric=self.policy["rubric"], evidence=self.evidence, authority=self.policy["evaluator"])
            if status == "unresolved" and record["status"] == "completed":
                raise RunnerError("unresolved task cannot carry a completed assessment")
            for field in ("accepted", "passes"):
                if field in output and (type(output[field]) is not bool or output[field] != checked["passes"]):
                    raise RunnerError("assessment result contradicts retained judgement")
            if "context" in output and canonical_json(output["context"]) != canonical_json(self.review_context):
                raise RunnerError("assessment result context differs from frozen invocation")
            result = {**output, "profile": "mas-assessment-result/1", "assessment": checked["reference"],
                "accepted": checked["passes"], "passes": checked["passes"], "candidate": record["candidate"],
                "context": self.review_context, "rubric": self.rubric_ref, "evidence": self.evidence_refs}
            integer_json(result)
            key = self.prefix + "final"
            saved = self.journal.db.execute("SELECT input,data FROM steps WHERE key=?", (key,)).fetchone()
            source = canonical_json({"status": status, "output": result})
            if saved is not None and saved["data"] != source:
                raise RunnerError("final assessment result differs from retained reply")
            if saved is None:
                self.journal.db.execute("INSERT INTO steps VALUES(?,'assessment_final','{}',?)", (key, source))
            return result

    def _authorised_candidate(self, reference):
        if reference == self.target:
            return
        for row in self.journal.db.execute(
                "SELECT data FROM steps WHERE kind='assessment_revision' AND substr(key,1,?)=?",
                (len(self.prefix), self.prefix)):
            completed = json.loads(row[0])["candidate"]
            if completed is not None and completed == reference:
                return
        raise RunnerError("candidate differs from frozen assessment lineage")

    def _reserve_revision(self, data):
        if (set(data) != {"request_key", "parent"}
                or not is_actor_identity(data["request_key"])):
            raise RunnerError("invalid revision reservation")
        key = "revision:" + data["request_key"]
        prior = self._row(key)
        if prior is not None:
            saved = json.loads(prior["data"])
            if saved["request"] != data:
                raise RunnerError("revision reservation content conflict")
            return saved["context"]
        self._authorised_candidate(data["parent"])
        parent = self._resolve(data["parent"], "candidate")
        count = self.journal.db.execute(
            "SELECT count(*) FROM steps WHERE kind='assessment_revision' AND substr(key,1,?)=?",
            (len(self.prefix), self.prefix)).fetchone()[0]
        if count >= self.policy["max_revisions"]:
            raise RunnerError("assessment revision limit exceeded")
        feedback = []
        if self.policy["feedback_visibility"] == "full":
            for row in self.journal.db.execute(
                    "SELECT data FROM steps WHERE kind='assessment_assessment' AND substr(key,1,?)=? ORDER BY rowid",
                    (len(self.prefix), self.prefix)):
                record = json.loads(row[0])
                if record["candidate"] == data["parent"]:
                    feedback.append(record)
            if len(feedback) > self.policy["max_feedback"]:
                raise RunnerError("assessment feedback count limit exceeded")
            integer_json(feedback, self.policy["max_feedback_bytes"])
        context = {"format": "mas-revision-input/1", "input": self.binding["input"],
            "candidate": parent, "feedback": feedback, "policy": self.policy,
            "deadline_ms": self.binding["deadline_ms"], "revision": count + 1}
        integer_json(context)
        self.journal.db.execute("INSERT INTO steps VALUES(?,?,?,?)",
            (self.prefix + key, "assessment_revision", "{}",
             canonical_json({"request": data, "context": context, "candidate": None})))
        return context

    def _complete_revision(self, data):
        if (set(data) != {"request_key", "candidate"}
                or not is_actor_identity(data["request_key"])):
            raise RunnerError("invalid revision completion")
        key = "revision:" + data["request_key"]
        row = self._row(key)
        if row is None or row["kind"] != "assessment_revision":
            raise RunnerError("revision has no retained reservation")
        saved = json.loads(row["data"])
        checked = self._validate(operation="revision", candidate=data["candidate"],
            parent=saved["context"]["candidate"], feedback=saved["context"]["feedback"])
        if saved["candidate"] is not None and saved["candidate"] != checked["reference"]:
            raise RunnerError("revision reservation already completed")
        inserted = self._record("candidate", data["candidate"])
        saved["candidate"] = checked["reference"]
        self.journal.db.execute("UPDATE steps SET data=? WHERE key=?",
            (canonical_json(saved), self.prefix + key))
        return {**checked, "inserted": inserted}
