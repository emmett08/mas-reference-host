"""Operator evidence, cancellation observations and task recovery stay distinct."""
from pathlib import Path
from dataclasses import replace
import tempfile
import unittest

from mas_host.contracts import Invocation, Outcome
from mas_host.journal import RunLock, inspect_run
from mas_host.recovery import RecoveryService, ReconciliationQuery
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
import test_runner as runner_tests
import test_cursor_jobs as cloud_tests


class RecoveryServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calls = []
        def invoke():
            self.calls.append("sent")
            raise TimeoutError("SECRET ambiguous provider response")
        def handle(context, _request):
            return Outcome(context.invoke(invoke, input={"opaque": True}, reservation_microusd=37))
        adapters = {actor: runner_tests.FunctionAdapter(handle) for actor in ("a", "b", "c")}
        self.workflow = Workflow("recovery/1", runner_tests.topology(), adapters, "a", "b", "recover")
        self.service = DurableTaskService(self.root, {"recover": self.workflow},
            {"alice": {"recover"}, "bob": {"recover"}}, supervised=False, **self.options)
        self.task = self.service.submit("alice", "one", "recover", {})["task_id"]
        self.assertEqual(self.service.run_once()["state"], "unknown")
        self.recovery = RecoveryService(self.service, operators={"operator": {"alice"}})
        self.entry = self.recovery.inbox("operator", "alice", self.task)["entries"][0]
        self.call = self.entry["call_id"]
        self.directory = self.root / self.service.recovery_runs("alice", self.task)[0]["run_directory"]
        self.evidence = {"source": "operator_attestation", "reference": "audit-123", "observed_at_ms": 1234}

    def test_inspection_never_dispatches_and_operator_grants_are_separate(self):
        result = self.recovery.inbox("operator", "alice", self.task)
        self.assertEqual(result["cancellation"]["local_execution_stopped"], True)
        self.assertEqual(result["cancellation"]["effect_reconciliation"], "required")
        self.assertEqual(result["entries"][0]["admissible_decisions"], ["completed", "confirmed_no_effect"])
        self.assertNotIn("SECRET", str(result))
        for operator, principal in (("alice", "alice"), ("operator", "bob")):
            with self.assertRaises(ServiceError):
                self.recovery.inbox(operator, principal, self.task)
        self.assertEqual(self.calls, ["sent"])

    def test_completed_resolution_is_audited_idempotent_and_requires_explicit_resume(self):
        options = dict(request_key="resolution-1", decision="completed", evidence=self.evidence, output={"answer": 42})
        first = self.recovery.resolve("operator", "alice", self.task, self.call, **options)
        self.assertEqual(first, self.recovery.resolve("operator", "alice", self.task, self.call, **options))
        self.assertFalse(first["execution_resumed"])
        self.assertEqual(self.service.inspect("alice", self.task)["state"], "unknown")
        report = inspect_run(self.directory)
        self.assertEqual(len(report["resolutions"]), 1)
        self.assertEqual(report["resolutions"][0]["evidence"]["operator"], "operator")
        self.assertEqual(report["invocations"][0]["reservation_microusd"], 37)
        self.service.recover("alice", self.task)
        self.assertEqual(self.service.run_once()["result"], {"answer": 42})
        self.assertEqual(self.calls, ["sent"])
        with self.assertRaisesRegex(ServiceError, "recovery decision conflict"):
            self.recovery.resolve("operator", "alice", self.task, self.call, **(options | {"output": {"answer": 43}}))

    def test_no_effect_resolution_retains_allocation_and_explicit_recovery_uses_fresh_call(self):
        self.recovery.resolve("operator", "alice", self.task, self.call, request_key="no-effect",
                              decision="confirmed_no_effect", evidence=self.evidence)
        self.assertEqual(self.calls, ["sent"])
        self.service.recover("alice", self.task)
        self.assertEqual(self.service.run_once()["state"], "unknown")
        calls = inspect_run(self.directory)["invocations"]
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["state"], "retry_allowed")
        self.assertNotEqual(calls[0]["call_id"], calls[1]["call_id"])
        self.assertEqual(sum(call["reservation_microusd"] for call in calls), 74)

    def test_cancel_records_intent_without_claiming_remote_stop_or_resolving_effect(self):
        self.service.cancel("alice", self.task)
        status = self.recovery.inbox("operator", "alice", self.task)["cancellation"]
        self.assertTrue(status["requested"])
        self.assertTrue(status["future_dispatch_stopped"])
        self.assertTrue(status["local_execution_stopped"])
        self.assertEqual(status["remote_cancellation"], "not_established")
        self.assertEqual(status["effect_reconciliation"], "required")
        with self.assertRaises(ServiceError):
            self.service.recover("alice", self.task)
        self.assertEqual(self.calls, ["sent"])

    def _completed_submission_with_cloud_job(self):
        from mas_host.cursor_cloud import run_snapshot
        from mas_host.durable_jobs import DurableJobService, JobStore
        store = JobStore(self.root)
        prepared = store.prepare("cloud-job", replace(cloud_tests.SPEC, reservation_microusd=37))
        client = cloud_tests.FakeCloud()
        client.agent_id = prepared["agent_id"]
        cloud = DurableJobService(store, client)
        with store.open() as db:
            with db:
                db.execute("UPDATE jobs SET host_run_dir=?,call_id=? WHERE job_id=?",
                           (str(self.directory), self.call, "cloud-job"))
            row = store.row(db, "cloud-job")
            cloud._snapshot(db, row, run_snapshot(client._run(), client.agent_id, client.run_id),
                            "submitted", {"run_id": client.run_id})
        self.recovery.resolve("operator", "alice", self.task, self.call,
            request_key="submission-receipt", decision="completed", evidence=self.evidence,
            output={"cursor_agent_id": client.agent_id, "cursor_run_id": client.run_id,
                    "submission_only": True})
        return cloud, client

    def test_lost_cloud_cancel_reply_remains_in_recovery_after_submission_completed(self):
        cloud, client = self._completed_submission_with_cloud_job()
        client.error = cloud_tests.CloudError("lost cancellation reply")
        with self.assertRaises(cloud_tests.CloudError):
            cloud.cancel("cloud-job")
        calls = list(client.calls)
        result = self.recovery.inbox("operator", "alice", self.task)
        self.assertEqual(result["entries"], [])
        self.assertEqual(result["cancellation"]["effect_reconciliation"], "required")
        self.assertEqual(result["cancellation"]["remote_cancellation"], "not_established")
        remote = result["remote_observations"][0]
        self.assertTrue(remote["uncertain"])
        self.assertTrue(remote["cancel_requested"])
        self.assertEqual(remote["remote_id"], client.run_id)
        self.assertEqual(client.calls, calls)

    def test_cloud_cancellation_requires_exact_terminal_evidence(self):
        cloud, client = self._completed_submission_with_cloud_job()
        cloud.cancel("cloud-job")
        result = self.recovery.inbox("operator", "alice", self.task)
        self.assertEqual(result["cancellation"]["remote_cancellation"], "not_established")
        self.assertEqual(result["cancellation"]["effect_reconciliation"], "required")
        client.status = "CANCELLED"
        client.wrong_run = True
        with self.assertRaises(cloud_tests.CloudError):
            cloud.poll("cloud-job")
        self.assertEqual(self.recovery.inbox("operator", "alice", self.task), result)
        client.wrong_run = False
        cloud.poll("cloud-job")
        result = self.recovery.inbox("operator", "alice", self.task)
        self.assertEqual(result["cancellation"]["remote_cancellation"], "acknowledged")
        self.assertEqual(result["cancellation"]["effect_reconciliation"], "not_required")
        remote = result["remote_observations"][0]
        self.assertEqual((remote["agent_id"], remote["remote_id"]), (client.agent_id, client.run_id))
        self.assertEqual(remote["remote_status"], "CANCELLED")
        self.assertFalse(remote["uncertain"])
        self.assertNotIn("Untrusted result text", str(result))

    def test_cloud_completion_racing_cancel_does_not_claim_cancellation(self):
        cloud, client = self._completed_submission_with_cloud_job()
        cloud.cancel("cloud-job")
        client.status = "FINISHED"
        cloud.poll("cloud-job")
        result = self.recovery.inbox("operator", "alice", self.task)
        self.assertEqual(result["cancellation"]["remote_cancellation"], "not_established")
        self.assertEqual(result["cancellation"]["effect_reconciliation"], "not_required")
        self.assertEqual(result["remote_observations"][0]["remote_status"], "FINISHED")

    def test_live_run_cannot_be_resolved(self):
        with RunLock(self.directory):
            self.assertFalse(self.recovery.inbox("operator", "alice", self.task)["cancellation"]["local_execution_stopped"])
            with self.assertRaisesRegex(ServiceError, "recovery not available"):
                self.recovery.resolve("operator", "alice", self.task, self.call, request_key="live",
                    decision="confirmed_no_effect", evidence=self.evidence)
        self.assertTrue(inspect_run(self.directory)["recovery_required"])

    def test_query_cannot_treat_not_found_as_no_effect_or_automatically_resolve(self):
        queried = []
        def query(request):
            queried.append(request)
            return {"call_id": request["call_id"], "certainty": "completed", "evidence":
                self.evidence | {"source": "provider_query"}, "output": {"answer": 42}}
        registry = {"fixture": ReconciliationQuery("fixture", "1", query)}
        recovery = RecoveryService(self.service, operators={"operator": {"alice"}}, queries=registry)
        result = recovery.query("operator", "alice", self.task, self.call, query_name="fixture", query_version="1")
        self.assertFalse(result["resolved"])
        self.assertTrue(inspect_run(self.directory)["recovery_required"])
        self.assertEqual(len(queried), 1)
        with self.assertRaisesRegex(ServiceError, "reconciliation contract mismatch"):
            recovery.query("operator", "alice", self.task, self.call, query_name="fixture", query_version="2")
        for certainty in ("not_found", "confirmed_no_effect"):
            query = lambda request: {"call_id": request["call_id"], "certainty": certainty,
                                    "evidence": self.evidence, "output": None}
            recovery = RecoveryService(self.service, operators={"operator": {"alice"}},
                queries={"fixture": ReconciliationQuery("fixture", "1", query)})
            with self.assertRaisesRegex(ServiceError, "invalid recovery observation"):
                recovery.query("operator", "alice", self.task, self.call, query_name="fixture", query_version="1")
        self.assertEqual(self.calls, ["sent"])

    def test_bad_evidence_and_audit_only_cannot_mutate_resolution(self):
        for value in ({}, self.evidence | {"raw": "SECRET"}, self.evidence | {"observed_at_ms": True}):
            with self.assertRaises(ServiceError):
                self.recovery.resolve("operator", "alice", self.task, self.call, request_key="bad",
                    decision="confirmed_no_effect", evidence=value)
        self.service.operations._readonly = True
        with self.assertRaisesRegex(ServiceError, "audit only"):
            self.recovery.resolve("operator", "alice", self.task, self.call, request_key="bad",
                decision="confirmed_no_effect", evidence=self.evidence)
        self.assertTrue(inspect_run(self.directory)["recovery_required"])


if __name__ == "__main__":
    unittest.main()
