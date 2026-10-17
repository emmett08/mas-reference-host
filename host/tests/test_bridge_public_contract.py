"""Public client substitution, through HTTP and the actual durable service."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.bridges.a2a import A2ABridge
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.service_contracts import ServiceError, TaskLifecycle, TaskService, TaskWorker
from mas_host.task_service import DurableTaskService, Workflow
from test_bridge_server import ServerFixture, TOKEN
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


class PublicServiceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas = runner_tests.RunnerIntegrationTests.mas
        cls.gateway = runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.token = self.root / "token"
        self.token.write_text(TOKEN)
        self.token.chmod(0o600)
        self.workflow = Workflow("echo/1", topology(max_bytes=300000),
            {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")},
            "a", "b", "echo")

    def service(self, workflow=None):
        return DurableTaskService(self.root / "state", {"echo": workflow or self.workflow},
            {"alice": {"echo"}}, mas_bin=self.mas, gateway_bin=self.gateway, supervised=False)

    def client(self, fixture):
        return TaskServiceClient(fixture.endpoint, principal="alice", token_file=self.token,
                                 allow_loopback_http=True)

    def test_client_substitutes_public_service_without_worker_capability(self):
        service = self.service()
        with ServerFixture(service) as fixture:
            client = self.client(fixture)
            self.assertIsInstance(client, TaskLifecycle)
            self.assertIsInstance(client, TaskService)
            self.assertNotIsInstance(client, TaskWorker)
            self.assertIsInstance(service, TaskWorker)
            task = client.submit("alice", "public", "echo", {}, metadata={"tenant": "one"})
            self.assertEqual(task["metadata"], {"tenant": "one"})
            self.assertEqual(fixture.request("/service/run_once", {})[0], 404)
            self.assertEqual(service.inspect("alice", task["task_id"])["state"], "queued")

    def test_a2a_bridge_can_use_http_service_and_filtered_keyset_pages(self):
        service = self.service()
        with ServerFixture(service) as fixture:
            bridge = A2ABridge(self.client(fixture), endpoint=fixture.endpoint + "/a2a", allow_http=True)
            ids = set()
            for index in range(2):
                result = bridge.handle("alice", {"jsonrpc": "2.0", "id": index, "method": "SendMessage",
                    "params": {"message": {"messageId": "message-" + str(index), "role": "ROLE_USER",
                        "contextId": "shared-context", "parts": [{"mediaType": "application/json",
                            "data": {"workflow": "echo", "input": {"index": index}}}]},
                        "configuration": {"returnImmediately": True}, "metadata": {"source": "test"}}})
                self.assertNotIn("error", result)
                task = result["result"]["task"]
                ids.add(task["id"])
                self.assertEqual(service.inspect("alice", task["id"])["metadata"]["request_metadata"],
                                 {"source": "test"})
            params = {"contextId": "shared-context", "pageSize": 1}
            first = bridge.handle("alice", {"jsonrpc": "2.0", "id": 3, "method": "ListTasks", "params": params})
            self.assertNotIn("error", first)
            page = first["result"]
            self.assertEqual(page["totalSize"], 2)
            self.assertTrue(page["nextPageToken"])
            second = bridge.handle("alice", {"jsonrpc": "2.0", "id": 4, "method": "ListTasks",
                "params": params | {"pageToken": page["nextPageToken"]}})
            self.assertNotIn("error", second)
            self.assertEqual(second["result"]["nextPageToken"], "")
            self.assertEqual({page["tasks"][0]["id"], second["result"]["tasks"][0]["id"]}, ids)

    def test_continuation_metadata_and_result_survive_http_roundtrip(self):
        workflow = replace(self.workflow,
            classify=lambda outcome: "completed" if outcome.output.get("approved") else "input_required",
            continue_input=lambda previous, supplied: previous | supplied)
        service = self.service(workflow)
        with ServerFixture(service) as fixture:
            client = self.client(fixture)
            task = client.submit("alice", "first", "echo", {"n": 1}, metadata={"context_id": "context"})
            self.assertEqual(service.run_once()["state"], "input_required")
            updated = client.update("alice", task["task_id"], "continue", {"approved": True},
                                    metadata={"message_digest": "new"})
            self.assertEqual(updated["state"], "queued")
            self.assertEqual(client.update("alice", task["task_id"], "continue", {"approved": True},
                                           metadata={"message_digest": "new"}), updated)
            with self.assertRaisesRegex(ServiceError, "request conflict"):
                client.update("alice", task["task_id"], "continue", {"approved": True},
                              metadata={"message_digest": "changed"})
            self.assertEqual(service.run_once()["state"], "completed")
            self.assertEqual(client.result("alice", task["task_id"]), {"n": 1, "approved": True})
            self.assertEqual(client.inspect("alice", task["task_id"])["metadata"]["context_id"], "context")
            self.assertTrue(client.events("alice", task["task_id"]))

    def test_application_payload_metadata_and_receipt_have_separate_wire_budget(self):
        service = self.service()
        payload = {"data": "x" * 65000}
        metadata = {"context": "m" * 65000}
        with ServerFixture(service) as fixture:
            client = self.client(fixture)
            task = client.submit("alice", "r" * 256, "echo", payload, metadata=metadata)
            self.assertEqual(task["metadata"], metadata)
            self.assertEqual(service.run_once()["state"], "completed")
            receipt = client.inspect("alice", task["task_id"])
            self.assertEqual(receipt["result"], payload)
            self.assertEqual(receipt["metadata"], metadata)
            self.assertEqual(client.result("alice", task["task_id"]), payload)
            self.assertEqual(client.list_task_page("alice")["tasks"][0], receipt)


if __name__ == "__main__":
    unittest.main()
