"""Pinned HTTP peer identity and outer retry after a lost accepted receipt."""
from dataclasses import replace
import importlib.util
from pathlib import Path
import tempfile
import unittest

from mas_host.bridges.orchestrator import OrchestratorConnector, PeerContract
from mas_host.bridges.service_client import TaskServiceClient, http_exchange
from mas_host.contracts import Outcome
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from test_bridge_server import ServerFixture, TOKEN
import test_runner as runner_tests


class FederationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.token = self.root / "token"
        self.token.write_text(TOKEN)
        self.token.chmod(0o600)
        self.workflow = Workflow("echo/1", runner_tests.topology(),
            {actor: runner_tests.FunctionAdapter(lambda _context, request: Outcome(request.input))
             for actor in ("a", "b", "c")}, "a", "b", "echo")
        self.service = self.make_service(self.workflow)
        self.pin = PeerContract(self.service.operations.deployment_id, {"echo": self.workflow.identity})

    def make_service(self, workflow):
        return DurableTaskService(self.root, {"echo": workflow}, {"alice": {"echo"}},
                                  supervised=False, **self.options)

    def client(self, server, transport=None):
        return TaskServiceClient(server.endpoint, token_file=self.token, principal="alice",
                                 transport=transport, allow_loopback_http=True)

    def test_http_lost_first_receipt_then_same_outer_key_returns_one_durable_task(self):
        submissions = []
        def lose_first(endpoint, **options):
            result = http_exchange(endpoint, **options)
            if endpoint.endswith("/submit"):
                submissions.append(options["body"])
                if len(submissions) == 1:
                    raise ServiceError("transport_failure")
            return result
        with ServerFixture(self.service) as server:
            connector = OrchestratorConnector(self.client(server, lose_first), "alice", peer_contract=self.pin)
            with self.assertRaisesRegex(ServiceError, "transport failure"):
                connector.submit("outer-operation", "echo", {"answer": 42})
            second = connector.submit("outer-operation", "echo", {"answer": 42})
            self.assertEqual(submissions[0], submissions[1])
            self.assertEqual(len(self.service.list_tasks("alice")), 1)
            self.assertEqual(self.service.run_once()["state"], "completed")
            self.assertEqual(connector.result(second["task_id"]), {"answer": 42})

    def test_deployment_and_workflow_pins_reject_before_new_admission(self):
        with ServerFixture(self.service) as server:
            client = self.client(server)
            for options in ({"expected_deployment_id": "other"}, {"expected_identity": "0" * 64}):
                with self.assertRaises(ServiceError):
                    client.submit("alice", "conflict", "echo", {}, **options)
            connector = OrchestratorConnector(client, "alice",
                peer_contract=PeerContract("other", {"echo": self.workflow.identity}))
            with self.assertRaisesRegex(ServiceError, "peer contract mismatch"):
                connector.submit("conflict", "echo", {})
            self.assertEqual(self.service.list_tasks("alice"), [])

    def test_registered_default_change_preserves_old_receipt_but_blocks_new_pinned_work(self):
        connector = OrchestratorConnector(self.service, "alice", peer_contract=self.pin)
        first = connector.submit("old", "echo", {})
        changed = self.make_service(replace(self.workflow, version="echo/2"))
        connector = OrchestratorConnector(changed, "alice", peer_contract=self.pin)
        self.assertEqual(connector.submit("old", "echo", {})["task_id"], first["task_id"])
        with self.assertRaises(ServiceError):
            connector.submit("new", "echo", {})
        self.assertEqual(len(changed.list_tasks("alice")), 1)

    @unittest.skipUnless(importlib.util.find_spec("temporalio") is not None,
                         "optional Temporal SDK is not installed")
    def test_temporal_activity_retry_after_real_http_acceptance_reuses_original_key(self):
        from temporalio.testing import ActivityEnvironment
        from mas_host.bridges.temporal import temporal_activities
        accepted = []
        def lose_first(endpoint, **options):
            result = http_exchange(endpoint, **options)
            if endpoint.endswith("/submit"):
                accepted.append(options["body"])
                if len(accepted) == 1:
                    raise ServiceError("transport_failure")
            return result
        with ServerFixture(self.service) as server:
            connector = OrchestratorConnector(self.client(server, lose_first), "alice", peer_contract=self.pin)
            submit = temporal_activities(connector)[0]
            environment = ActivityEnvironment()
            from temporalio.exceptions import ApplicationError
            with self.assertRaises(ApplicationError) as error:
                environment.run(submit, {"workflow": "echo", "input": {}})
            self.assertFalse(error.exception.non_retryable)
            environment.info = replace(environment.info, attempt=2)
            receipt = environment.run(submit, {"workflow": "echo", "input": {}})
            self.assertEqual(receipt["state"], "queued")
            self.assertEqual(accepted[0], accepted[1])
            self.assertEqual(len(self.service.list_tasks("alice")), 1)


if __name__ == "__main__":
    unittest.main()
