"""Shared retry, deadline and uncertainty contract for framework connectors."""
from __future__ import annotations

import json
import unittest

from mas_host.bridges.orchestrator import OrchestratorConnector, stable_request_key
from mas_host.service_contracts import ServiceError


class ServiceDouble:
    def __init__(self, state="queued"):
        self.state = state
        self.rows, self.inputs, self.updates = {}, {}, {}
        self.submissions = self.cancellations = 0
        self.observations = []

    def submit(self, principal, key, workflow, input):
        binding = json.dumps([principal, workflow, input], sort_keys=True)
        if key in self.inputs and self.inputs[key] != binding:
            raise ServiceError("request_conflict")
        if key not in self.rows:
            self.submissions += 1
            self.inputs[key] = binding
            self.rows[key] = {"task_id": key, "state": self.state, "workflow": workflow,
                              "request_key": key, "cancel_requested": False,
                              "capabilities": {"cancel": True, "update": True},
                              "result": {"answer": 42} if self.state == "completed" else None,
                              "error": "execution_failed" if self.state == "failed" else None}
        return dict(self.rows[key])

    def inspect(self, principal, task_id):
        if self.observations:
            self.rows[task_id]["state"] = self.observations.pop(0)
        return dict(self.rows[task_id])

    def result(self, principal, task_id):
        value = self.inspect(principal, task_id)
        if value["state"] != "completed":
            raise ServiceError("result_not_available")
        return value["result"]

    def cancel(self, principal, task_id):
        self.cancellations += 1
        self.rows[task_id]["cancel_requested"] = True
        return self.inspect(principal, task_id)

    def update(self, principal, task_id, key, input):
        if key not in self.updates:
            self.updates[key] = input
            self.rows[task_id]["state"] = "completed"
            self.rows[task_id]["result"] = input
        elif self.updates[key] != input:
            raise ServiceError("request_conflict")
        return self.inspect(principal, task_id)


class ConnectorTests(unittest.TestCase):
    def test_retry_identity_excludes_attempt_but_conflicts_on_changed_input(self):
        service = ServiceDouble()
        connector = OrchestratorConnector(service, "worker")
        key = stable_request_key("run-1", "operation-7")
        self.assertEqual(connector.submit(key, "invoice", {"x": 1}), connector.submit(key, "invoice", {"x": 1}))
        self.assertEqual(service.submissions, 1)
        with self.assertRaisesRegex(ServiceError, "request conflict"):
            connector.submit(key, "invoice", {"x": 2})

    def test_timeout_retains_running_and_does_not_cancel(self):
        service = ServiceDouble("running")
        now = [0]
        connector = OrchestratorConnector(service, "worker", clock=lambda: now[0],
                                           sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))
        connector.submit("task", "invoice", {})
        observed = []
        value = connector.wait("task", timeout_seconds=3, heartbeat=observed.append)
        self.assertEqual(value["state"], "running")
        self.assertEqual(service.cancellations, 0)
        self.assertEqual(now[0], 3)
        self.assertEqual(len(observed), 4)

    def test_cancellation_request_preserves_observed_state(self):
        service = ServiceDouble("running")
        connector = OrchestratorConnector(service, "worker")
        connector.submit("task", "invoice", {})
        value = connector.wait("task", cancelled=lambda: True)
        self.assertEqual(value["state"], "running")
        self.assertTrue(value["cancel_requested"])

    def test_uncertainty_and_interrupted_states_are_returned_without_retry(self):
        for state in ("unknown", "input_required", "auth_required", "failed", "cancelled"):
            service = ServiceDouble(state)
            connector = OrchestratorConnector(service, "worker")
            connector.submit("task", "invoice", {})
            self.assertEqual(connector.wait("task")["state"], state)
            self.assertEqual(service.submissions, 1)

    def test_result_cannot_turn_failure_into_success(self):
        service = ServiceDouble("failed")
        connector = OrchestratorConnector(service, "worker")
        connector.submit("task", "invoice", {})
        with self.assertRaisesRegex(ServiceError, "result unavailable"):
            connector.result("task")
        self.assertEqual(connector.inspect("task")["error"], "execution_failed")
        self.assertEqual(connector.inspect("task")["state"], "failed")

    def test_invalid_deadline_and_receipt_are_rejected(self):
        service = ServiceDouble()
        connector = OrchestratorConnector(service, "worker")
        connector.submit("task", "invoice", {})
        for timeout in (float("nan"), float("inf"), -1, True, 3601):
            with self.assertRaises(ServiceError):
                connector.wait("task", timeout_seconds=timeout)
        service.rows["task"]["task_id"] = "other"
        with self.assertRaisesRegex(ServiceError, "invalid task receipt"):
            connector.inspect("task")


if __name__ == "__main__":
    unittest.main()
