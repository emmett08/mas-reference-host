"""Independent cross-principal and crash-window checks for remote bridges."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.bridges.remote import RemoteSnapshot, RemoteTaskService, RemoteTaskStore
from mas_host.bridges.a2a import A2AClient
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


class Peer:
    def __init__(self):
        self.submissions = []
        self.polls = []
        self.lose_reply = False
        self.states = {}

    def descriptor(self):
        return {"peer": "test", "endpoint": "https://agent.example.test", "principal": "shared-service-account"}

    def submit(self, payload):
        remote_id = f"remote-{len(self.submissions) + 1}"
        self.submissions.append(payload)
        self.states[remote_id] = "running"
        if self.lose_reply:
            raise RuntimeError("remote effect happened but receipt was lost")
        return RemoteSnapshot(remote_id, "running", {"owner_value": payload["value"]})

    def poll(self, remote_id):
        self.polls.append(remote_id)
        return RemoteSnapshot(remote_id, self.states[remote_id], {"observed": True})

    def cancel(self, remote_id):
        return RemoteSnapshot(remote_id, "cancelled")

    def update(self, remote_id, payload):
        return RemoteSnapshot(remote_id, "running")


class RemoteReviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / "state"
        self.peer = Peer()
        self.bindings = []
        prototypes = {actor: FunctionAdapter(lambda c, r: Outcome(r.input)) for actor in ("a", "b", "c")}
        def factory(binding):
            self.bindings.append(binding)
            remote = RemoteTaskService(RemoteTaskStore(binding.state_dir), self.peer,
                                       namespace=binding.remote_namespace("b"))
            def handle(context, request):
                arguments = {"context": context, "host_run_dir": binding.run_dir, "reservation_microusd": 0}
                if request.input["action"] == "submit":
                    output = remote.submit(request.input["key"], {"value": request.input["value"]}, **arguments)
                else:
                    output = remote.poll(request.input["key"], **arguments)
                return Outcome(output)
            return prototypes | {"b": FunctionAdapter(handle)}
        self.workflow = Workflow("remote-review/1", topology(), prototypes, "a", "b", "remote", adapter_factory=factory)
        self.service = DurableTaskService(self.state, {"remote": self.workflow},
            {"alice": {"remote"}, "bob": {"remote"}}, mas_bin=self.mas, gateway_bin=self.gateway, supervised=False)

    def submit(self, principal, request_key, **payload):
        task = self.service.submit(principal, request_key, "remote", payload)
        receipt = self.service.run_once()
        self.assertEqual(receipt["task_id"], task["task_id"])
        return receipt

    def test_factory_binds_actual_run_and_distinct_remote_owner_scope(self):
        first = self.submit("alice", "a1", action="submit", key="logical", value=10)
        second = self.submit("bob", "b1", action="submit", key="logical", value=10)
        self.assertEqual(first["state"], "completed")
        self.assertEqual(second["state"], "completed")
        self.assertNotEqual(first["result"]["remote_id"], second["result"]["remote_id"])
        self.assertEqual(len(self.peer.submissions), 2)
        self.assertNotEqual(self.bindings[0].run_dir, self.bindings[1].run_dir)
        for binding in self.bindings:
            self.assertTrue((binding.run_dir / "host.sqlite").is_file())
            self.assertEqual(binding.run_dir.parent, binding.state_dir)
        self.assertNotEqual(self.bindings[0].remote_namespace("b"), self.bindings[1].remote_namespace("b"))

    def test_foreign_principal_cannot_poll_guessed_logical_key(self):
        first = self.submit("alice", "a1", action="submit", key="logical", value=10)
        forbidden = self.submit("bob", "b1", action="poll", key="logical")
        self.assertNotEqual(forbidden["state"], "completed")
        self.assertEqual(self.peer.polls, [])
        authorised = self.submit("alice", "a2", action="poll", key="logical")
        self.assertEqual(authorised["state"], "completed")
        self.assertEqual(authorised["result"]["remote_id"], first["result"]["remote_id"])
        self.assertEqual(self.peer.polls, [first["result"]["remote_id"]])

    def test_lost_receipt_remains_unknown_across_new_host_runs(self):
        self.peer.lose_reply = True
        first = self.submit("alice", "a1", action="submit", key="logical", value=10)
        self.assertEqual(first["state"], "unknown")
        self.peer.lose_reply = False
        second = self.submit("alice", "a2", action="submit", key="logical", value=10)
        self.assertEqual(second["state"], "unknown")
        self.assertEqual(len(self.peer.submissions), 1)

    def test_owner_scope_is_stable_across_runs_but_changes_with_workflow_version(self):
        self.submit("alice", "a1", action="submit", key="logical", value=10)
        self.submit("alice", "a2", action="poll", key="logical")
        first, second = self.bindings
        self.assertEqual(first.remote_namespace("b"), second.remote_namespace("b"))
        self.assertNotEqual(first.remote_namespace("a"), first.remote_namespace("b"))
        changed = replace(first, workflow_identity="different-version")
        self.assertNotEqual(first.remote_namespace("b"), changed.remote_namespace("b"))


class A2AResponseIdentityReviewTests(unittest.TestCase):
    def test_explicit_context_cannot_be_replaced_by_task_or_direct_reply(self):
        for direct in (False, True):
            with self.subTest(direct=direct):
                class Transport:
                    def descriptor(self):
                        return {"endpoint": "https://peer.example.test"}
                    def request(self, method, payload, headers):
                        response = ({"message": {"messageId": "reply", "contextId": "other-context",
                            "role": "ROLE_AGENT", "parts": [{"text": "answer"}]}} if direct else
                            {"task": {"id": "remote-task", "contextId": "other-context",
                                      "status": {"state": "TASK_STATE_WORKING"}}})
                        return {"jsonrpc": "2.0", "id": payload["id"], "result": response}
                client = A2AClient(Transport(), client_identity="test-client")
                with self.assertRaisesRegex(ServiceError, "identity conflict"):
                    client.submit({"message": {"messageId": "request", "contextId": "expected-context",
                        "role": "ROLE_USER", "parts": [{"text": "question"}]}})

    def test_continuation_cannot_switch_task_identity(self):
        class Transport:
            def descriptor(self):
                return {"endpoint": "https://peer.example.test"}
            def request(self, method, payload, headers):
                return {"jsonrpc": "2.0", "id": payload["id"], "result": {
                    "task": {"id": "wrong-task", "contextId": "expected-context",
                             "status": {"state": "TASK_STATE_WORKING"}}}}
        client = A2AClient(Transport(), client_identity="test-client")
        with self.assertRaisesRegex(ServiceError, "identity conflict"):
            client.update("expected-task", {"message": {"messageId": "request", "contextId": "expected-context",
                "role": "ROLE_USER", "parts": [{"text": "answer"}]}})


if __name__ == '__main__':
    unittest.main()
