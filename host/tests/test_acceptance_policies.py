from dataclasses import FrozenInstanceError
import json
import unittest

from mas_host.acceptance_policies import (AcceptanceDecision, AcceptanceError, AggregatePolicy,
    AssessmentSubject, DesignatedReviewerPolicy, DeterministicPolicy)
from mas_host.codec import JSON_BYTES, canonical_json


class AcceptancePolicyTests(unittest.TestCase):
    def setUp(self):
        self.subject = AssessmentSubject({"revision": "abc", "value": 2}, {"value": 4})
        self.yes = DeterministicPolicy("double", "1", lambda i, o: o["value"] == 2 * i["value"])
        self.no = DeterministicPolicy("different", "1", lambda i, o: False)
        self.unknown = DeterministicPolicy("pending", "1", lambda i, o: None)

    def test_subject_tracks_both_input_and_output_and_is_copy_protected(self):
        data = {"revision": "abc", "value": 2}
        subject = AssessmentSubject(data, {"value": 4})
        identity = subject.identity
        data["revision"] = "def"
        subject.input["revision"] = "xyz"
        subject.output["value"] = 5
        self.assertEqual(subject.identity, identity)
        self.assertNotEqual(identity, AssessmentSubject(data, {"value": 4}).identity)
        self.assertNotEqual(identity, AssessmentSubject(subject.input, {"value": 5}).identity)
        # Object key order is not a different input.
        self.assertEqual(identity, AssessmentSubject({"value": 2, "revision": "abc"}, {"value": 4}).identity)

    def test_deterministic_boolean_none_and_failures_have_explicit_meanings(self):
        self.assertEqual(self.yes.evaluate(self.subject).status, "accepted")
        self.assertEqual(self.no.evaluate(self.subject).status, "rejected")
        self.assertEqual(self.unknown.evaluate(self.subject).status, "unresolved")
        invalid = DeterministicPolicy("invalid", "1", lambda i, o: 1)
        self.assertEqual(invalid.evaluate(self.subject).reason, "invalid_validator_result")
        def fail(i, o):
            raise RuntimeError("sensitive source material")
        decision = DeterministicPolicy("fails", "1", fail).evaluate(self.subject)
        self.assertEqual(decision.reason, "validator_failed")
        self.assertNotIn("sensitive", str(decision.descriptor))

    def test_mutating_validator_receives_snapshots(self):
        def mutate(i, o):
            i.clear()
            o.clear()
            return True
        policy = DeterministicPolicy("mutating", "1", mutate)
        identity = self.subject.identity
        policy.evaluate(self.subject)
        self.assertEqual(self.subject.identity, identity)

    def test_human_decision_requires_authenticated_external_identity(self):
        policy = DesignatedReviewerPolicy("owner-review", "1", "alice@example.com")
        payload = {"status": "accepted", "subject_identity": self.subject.identity,
                   "reason": "reviewed", "evidence_ids": ["review-1"]}
        with self.assertRaisesRegex(AcceptanceError, "reviewer forbidden"):
            policy.evaluate(self.subject, authenticated_reviewer="mallory", decision=payload)
        with self.assertRaisesRegex(AcceptanceError, "invalid reviewer decision"):
            policy.evaluate(self.subject, authenticated_reviewer="alice@example.com",
                            decision=payload | {"reviewer": "alice@example.com"})
        decision = policy.evaluate(self.subject, authenticated_reviewer="alice@example.com", decision=payload)
        payload["evidence_ids"].append("later")
        self.assertEqual(decision.evidence_ids, ("review-1",))
        self.assertEqual(decision.status, "accepted")
        self.assertEqual(decision.policy_identity, policy.identity)

    def test_stale_human_decision_cannot_accept_changed_input_or_output(self):
        policy = DesignatedReviewerPolicy("review", "1", "alice")
        payload = {"status": "accepted", "subject_identity": self.subject.identity,
                   "reason": "reviewed", "evidence_ids": []}
        changed = AssessmentSubject({"revision": "def", "value": 2}, {"value": 4})
        decision = policy.evaluate(changed, authenticated_reviewer="alice", decision=payload)
        self.assertEqual(decision.status, "unresolved")
        self.assertEqual(decision.subject_identity, changed.identity)

    def test_all_of_requires_every_declared_policy_and_rejection_blocks(self):
        policy = AggregatePolicy("all-checks", "1", [self.yes.identity, self.no.identity])
        self.assertEqual(policy.evaluate(self.subject, []).status, "unresolved")
        self.assertEqual(policy.evaluate(self.subject, [self.yes.evaluate(self.subject)]).status, "unresolved")
        self.assertEqual(policy.evaluate(self.subject, [self.no.evaluate(self.subject)]).status, "rejected")
        second_yes = DeterministicPolicy("other", "1", lambda i, o: True)
        success = AggregatePolicy("all-checks", "1", [self.yes.identity, second_yes.identity])
        self.assertEqual(success.evaluate(self.subject, [self.yes.evaluate(self.subject),
            second_yes.evaluate(self.subject)]).status, "accepted")

    def test_quorum_distinguishes_missing_evidence_from_unreachable_threshold(self):
        policy = AggregatePolicy("vote", "1", [self.yes.identity, self.no.identity, self.unknown.identity],
                                 mode="quorum", threshold=2)
        values = [self.yes.evaluate(self.subject), self.no.evaluate(self.subject), self.unknown.evaluate(self.subject)]
        self.assertEqual(policy.evaluate(self.subject, values).status, "unresolved")
        values[2] = AcceptanceDecision("accepted", self.subject.identity, self.unknown.identity, "resolved")
        self.assertEqual(policy.evaluate(self.subject, values).status, "accepted")
        values[2] = AcceptanceDecision("rejected", self.subject.identity, self.unknown.identity, "resolved")
        self.assertEqual(policy.evaluate(self.subject, values).status, "rejected")

    def test_duplicate_foreign_or_stale_decisions_do_not_count_as_votes(self):
        policy = AggregatePolicy("checks", "1", [self.yes.identity])
        accepted = self.yes.evaluate(self.subject)
        with self.assertRaisesRegex(AcceptanceError, "duplicate"):
            policy.evaluate(self.subject, [accepted, accepted])
        with self.assertRaisesRegex(AcceptanceError, "unregistered"):
            policy.evaluate(self.subject, [self.no.evaluate(self.subject)])
        changed = AssessmentSubject({"revision": "changed"}, {})
        stale = policy.evaluate(changed, [accepted])
        self.assertEqual(stale.status, "unresolved")
        self.assertEqual(stale.reason, "stale_acceptance_evidence")

    def test_aggregate_policy_order_does_not_change_identity(self):
        a = AggregatePolicy("checks", "1", [self.yes.identity, self.no.identity])
        b = AggregatePolicy("checks", "1", [self.no.identity, self.yes.identity])
        self.assertEqual(a.identity, b.identity)
        a.descriptor["policies"].clear()
        self.assertEqual(a.identity, b.identity)
        with self.assertRaises(FrozenInstanceError):
            a.threshold = 1
        first = a.evaluate(self.subject, [self.yes.evaluate(self.subject), self.no.evaluate(self.subject)])
        second = a.evaluate(self.subject, [self.no.evaluate(self.subject), self.yes.evaluate(self.subject)])
        self.assertEqual(first, second)

    def test_decision_checks_escaped_size_before_it_can_enter_an_aggregate(self):
        evidence = [str(i) + "\\" * (256 - len(str(i))) for i in range(128)]
        descriptor = {"format": "mas-acceptance-decision/1", "status": "accepted",
                      "subject_identity": self.subject.identity, "policy_identity": self.yes.identity,
                      "reason": "reviewed", "evidence_ids": evidence}
        encoded_size = len(json.dumps(descriptor, ensure_ascii=False, separators=(",", ":")).encode())
        self.assertGreater(encoded_size, JSON_BYTES)
        with self.assertRaises(AcceptanceError) as rejected:
            AcceptanceDecision("accepted", self.subject.identity, self.yes.identity, "reviewed", evidence)
        self.assertEqual(rejected.exception.code, "invalid_acceptance_decision")

        # Each removed backslash saves two encoded bytes. Exactly the complete
        # descriptor limit remains valid, including when aggregation hashes it.
        self.assertEqual((encoded_size - JSON_BYTES) % 2, 0)
        excess_characters = (encoded_size - JSON_BYTES) // 2
        evidence[-1] = evidence[-1][:-excess_characters]
        decision = AcceptanceDecision("accepted", self.subject.identity, self.yes.identity, "reviewed", evidence)
        self.assertEqual(len(canonical_json(decision.descriptor).encode()), JSON_BYTES)
        result = AggregatePolicy("all", "1", [self.yes.identity]).evaluate(self.subject, [decision])
        self.assertEqual(result.status, "accepted")

    def test_evidence_identifiers_count_unicode_bytes_and_use_domain_errors(self):
        evidence = tuple(str(i) + "雪" * ((256 - len(str(i))) // 3) for i in range(128))
        decision = AcceptanceDecision("accepted", self.subject.identity, self.yes.identity, "reviewed", evidence)
        self.assertIn("雪", canonical_json(decision.descriptor))
        self.assertEqual(AggregatePolicy("all", "1", [self.yes.identity]).evaluate(
            self.subject, [decision]).status, "accepted")
        for invalid in ("雪" * 86, "\ud800"):
            with self.subTest(invalid=ascii(invalid)), self.assertRaises(AcceptanceError):
                AcceptanceDecision("accepted", self.subject.identity, self.yes.identity, "reviewed", [invalid])

    def test_symbolic_names_report_acceptance_errors_without_contract_dependency(self):
        constructors = (
            lambda: DeterministicPolicy("bad/name", "1", lambda i, o: True),
            lambda: DesignatedReviewerPolicy("review", "bad/version", "alice"),
            lambda: AggregatePolicy("bad/name", "1", [self.yes.identity]),
            lambda: AcceptanceDecision("accepted", self.subject.identity, self.yes.identity, "bad/reason"),
        )
        for construct in constructors:
            with self.subTest(construct=construct), self.assertRaises(AcceptanceError) as rejected:
                construct()
            self.assertEqual(rejected.exception.code, "invalid_acceptance_name")

    def test_existing_subject_policy_and_aggregate_fingerprints_are_preserved(self):
        subject = AssessmentSubject({"path": "C:\\雪", "revision": "é"}, {"value": 7})
        policy = DeterministicPolicy("check", "1", lambda i, o: True)
        reviewer = DesignatedReviewerPolicy("review", "1", "é@example.com")
        self.assertEqual(subject.identity, "da3ed66205c180f4574101a184470f1828b757cf88cb7cbbc3ab83adcec00dbc")
        self.assertEqual(policy.identity, "be7c58b6c73a79366cb48f3f1040b6ded75ed4d07791319d26bd1d9381a09155")
        self.assertEqual(reviewer.identity, "b3b1879beda35454aa1b79f2bb9185c31d0406d1166a46bd0ad6c234efe87526")
        result = AggregatePolicy("all", "1", [policy.identity]).evaluate(subject, [policy.evaluate(subject)])
        self.assertEqual(result.policy_identity, "0a0b58b978035dda385120f4cfc44459a7950559b13b7211e27a0dfca24cd346")
        self.assertEqual(result.evidence_ids,
                         ("sha256:7c45c61667da0646dbf8aebbc84e0e807a24ef9f7cb84a579d5d2c6411a012f3",))

    def test_invalid_policy_and_subject_configuration_fail_closed(self):
        for threshold in (True, 0, 2, None):
            with self.subTest(threshold=threshold), self.assertRaises(AcceptanceError):
                AggregatePolicy("q", "1", [self.yes.identity], mode="quorum", threshold=threshold)
        for input in ([], {"float": 1.2}):
            with self.assertRaises(AcceptanceError):
                AssessmentSubject(input, {})
        with self.assertRaises(AcceptanceError):
            AggregatePolicy("q", "1", [self.yes.identity, self.yes.identity])
        with self.assertRaises(AcceptanceError):
            AcceptanceDecision("completed", self.subject.identity, self.yes.identity, "done")


if __name__ == "__main__":
    unittest.main()
