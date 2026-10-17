"""Explicit result acceptance, separate from execution's completed state.

Policies are registered trusted code. Their decisions bind to both the input
and output being assessed. Model agreement is only a configured review rule;
it does not establish factual correctness. Human identity is supplied by an
authenticated boundary, never inferred from application JSON.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Callable, Iterable, Protocol

from .codec import canonical_json, json_identity
from .contracts import RunnerError


class AcceptanceError(RunnerError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code.replace("_", " "))


STATES = frozenset({"accepted", "rejected", "unresolved"})


def _name(value: str) -> str:
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,62}", value) is None:
        raise AcceptanceError("invalid_acceptance_name")
    return value


def _digest(value: str) -> str:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise AcceptanceError("invalid_acceptance_identity")
    return value


def _text(value: str) -> str:
    if type(value) is not str or not value or any(ord(c) < 32 for c in value):
        raise AcceptanceError("invalid_acceptance_reference")
    try:
        if len(value.encode("utf-8")) > 256:
            raise AcceptanceError("invalid_acceptance_reference")
    except UnicodeError:
        raise AcceptanceError("invalid_acceptance_reference") from None
    return value


@dataclass(frozen=True, init=False)
class AssessmentSubject:
    """Immutable snapshots: callers must include all relevant input versions.

    A hash identifies supplied JSON, not the truth or completeness of its
    contents. File inputs should carry integrity-checked artefact references.
    """
    _input: str
    _output: str

    def __init__(self, input: dict, output: dict):
        if type(input) is not dict or type(output) is not dict:
            raise AcceptanceError("invalid_assessment_subject")
        try:
            object.__setattr__(self, "_input", canonical_json(input))
            object.__setattr__(self, "_output", canonical_json(output))
        except RunnerError:
            raise AcceptanceError("invalid_assessment_subject") from None

    @property
    def input(self) -> dict:
        return json.loads(self._input)

    @property
    def output(self) -> dict:
        return json.loads(self._output)

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-assessment-subject/1",
                "input_identity": hashlib.sha256(self._input.encode("utf-8")).hexdigest(),
                "output_identity": hashlib.sha256(self._output.encode("utf-8")).hexdigest()}

    @property
    def identity(self) -> str:
        return json_identity(self.descriptor)


@dataclass(frozen=True)
class AcceptanceDecision:
    """Decision whose complete descriptor fits the codec's JSON byte bound.

    Applications must still budget separately for any enclosing message.
    """
    status: str
    subject_identity: str
    policy_identity: str
    reason: str
    evidence_ids: tuple[str, ...] = ()

    def __post_init__(self):
        if type(self.status) is not str or self.status not in STATES:
            raise AcceptanceError("invalid_acceptance_status")
        _digest(self.subject_identity)
        _digest(self.policy_identity)
        _name(self.reason)
        if type(self.evidence_ids) not in (tuple, list) or len(self.evidence_ids) > 128:
            raise AcceptanceError("invalid_acceptance_evidence")
        refs = tuple(_text(value) for value in self.evidence_ids)
        if len(set(refs)) != len(refs):
            raise AcceptanceError("duplicate_acceptance_evidence")
        object.__setattr__(self, "evidence_ids", refs)
        # Per-reference bounds alone do not bound the encoded decision: JSON
        # escaping can double the size of an otherwise valid evidence string.
        try:
            canonical_json(self.descriptor)
        except RunnerError:
            raise AcceptanceError("invalid_acceptance_decision") from None

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-acceptance-decision/1", "status": self.status,
                "subject_identity": self.subject_identity, "policy_identity": self.policy_identity,
                "reason": self.reason, "evidence_ids": list(self.evidence_ids)}


class AcceptancePolicy(Protocol):
    """Common immutable identity; each policy declares its evidence contract."""
    @property
    def descriptor(self) -> dict: ...

    @property
    def identity(self) -> str: ...


def _subject(subject: AssessmentSubject) -> None:
    if type(subject) is not AssessmentSubject:
        raise AcceptanceError("invalid_assessment_subject")


@dataclass(frozen=True)
class DeterministicPolicy:
    name: str
    version: str
    validator: Callable[[dict, dict], bool | None]

    def __post_init__(self):
        _name(self.name)
        _name(self.version)
        if not callable(self.validator):
            raise AcceptanceError("invalid_acceptance_validator")

    @property
    def descriptor(self) -> dict:
        # Version covers the registered implementation; no introspection or
        # serialisation of arbitrary function objects is implied.
        return {"format": "mas-acceptance-policy/1", "kind": "deterministic",
                "name": self.name, "version": self.version}

    @property
    def identity(self) -> str:
        return json_identity(self.descriptor)

    def evaluate(self, subject: AssessmentSubject) -> AcceptanceDecision:
        _subject(subject)
        try:
            verdict = self.validator(subject.input, subject.output)
        except Exception:
            return AcceptanceDecision("unresolved", subject.identity, self.identity, "validator_failed")
        if verdict is True:
            status, reason = "accepted", "validator_passed"
        elif verdict is False:
            status, reason = "rejected", "validator_rejected"
        elif verdict is None:
            status, reason = "unresolved", "validator_inconclusive"
        else:
            status, reason = "unresolved", "invalid_validator_result"
        return AcceptanceDecision(status, subject.identity, self.identity, reason)


@dataclass(frozen=True)
class DesignatedReviewerPolicy:
    """Review by an authenticated person or admitted agent identity.

    The boundary calls ``evaluate`` with the identity established by its
    authentication/transport. Passing a body field as authenticated_reviewer
    would violate this interface; this class does not authenticate credentials.
    """
    name: str
    version: str
    reviewer: str

    def __post_init__(self):
        _name(self.name)
        _name(self.version)
        _text(self.reviewer)

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-acceptance-policy/1", "kind": "designated-reviewer",
                "name": self.name, "version": self.version, "reviewer": self.reviewer}

    @property
    def identity(self) -> str:
        return json_identity(self.descriptor)

    def evaluate(self, subject: AssessmentSubject, *, authenticated_reviewer: str,
                 decision: dict) -> AcceptanceDecision:
        _subject(subject)
        if type(authenticated_reviewer) is not str or authenticated_reviewer != self.reviewer:
            raise AcceptanceError("reviewer_forbidden")
        if type(decision) is not dict or set(decision) != {"status", "subject_identity", "reason", "evidence_ids"}:
            raise AcceptanceError("invalid_reviewer_decision")
        result = AcceptanceDecision(decision["status"], decision["subject_identity"], self.identity,
                                    decision["reason"], decision["evidence_ids"])
        if result.subject_identity != subject.identity:
            return AcceptanceDecision("unresolved", subject.identity, self.identity, "stale_reviewer_decision")
        return result


@dataclass(frozen=True, init=False)
class AggregatePolicy:
    """All-of or quorum over distinct registered policy identities.

    Missing evidence cannot count towards the threshold; duplicate policies
    never gain extra votes. Supply decisions from trusted policy executions or integrity-checked
    storage, not untrusted JSON asserting a policy's identity.
    """
    name: str
    version: str
    policies: tuple[str, ...]
    mode: str
    threshold: int

    def __init__(self, name: str, version: str, policies: Iterable[str], *,
                 mode: str = "all", threshold: int | None = None):
        object.__setattr__(self, "name", _name(name))
        object.__setattr__(self, "version", _name(version))
        if type(policies) not in (tuple, list, set, frozenset):
            raise AcceptanceError("invalid_aggregate_policies")
        selected = tuple(_digest(item) for item in policies)
        if not 1 <= len(selected) <= 32 or len(set(selected)) != len(selected):
            raise AcceptanceError("invalid_aggregate_policies")
        if type(mode) is not str or mode not in {"all", "quorum"}:
            raise AcceptanceError("invalid_aggregate_mode")
        if mode == "all":
            if threshold is not None:
                raise AcceptanceError("invalid_aggregate_threshold")
            threshold = len(selected)
        if type(threshold) is not int or not 1 <= threshold <= len(selected):
            raise AcceptanceError("invalid_aggregate_threshold")
        object.__setattr__(self, "policies", tuple(sorted(selected)))
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "threshold", threshold)

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-acceptance-policy/1", "kind": "aggregate", "name": self.name,
                "version": self.version, "policies": list(self.policies),
                "mode": self.mode, "threshold": self.threshold}

    @property
    def identity(self) -> str:
        return json_identity(self.descriptor)

    def evaluate(self, subject: AssessmentSubject,
                 decisions: Iterable[AcceptanceDecision]) -> AcceptanceDecision:
        _subject(subject)
        observed = {}
        stale = False
        for decision in decisions:
            if type(decision) is not AcceptanceDecision or decision.policy_identity not in self.policies:
                raise AcceptanceError("unregistered_acceptance_policy")
            if decision.policy_identity in observed:
                raise AcceptanceError("duplicate_acceptance_policy")
            observed[decision.policy_identity] = decision
            stale |= decision.subject_identity != subject.identity
        if stale:
            return AcceptanceDecision("unresolved", subject.identity, self.identity, "stale_acceptance_evidence")
        accepted = sum(d.status == "accepted" for d in observed.values())
        rejected = sum(d.status == "rejected" for d in observed.values())
        if accepted >= self.threshold:
            status, reason = "accepted", "acceptance_threshold_met"
        elif len(self.policies) - rejected < self.threshold:
            status, reason = "rejected", "acceptance_threshold_unreachable"
        else:
            status, reason = "unresolved", "acceptance_evidence_incomplete"
        # A bounded decision references child decision digests, preserving all
        # child evidence without flattening an unbounded reference collection.
        evidence = tuple("sha256:" + json_identity(observed[key].descriptor) for key in sorted(observed))
        return AcceptanceDecision(status, subject.identity, self.identity, reason, evidence)
