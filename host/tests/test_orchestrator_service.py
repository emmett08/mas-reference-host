"""Run connectors through durable TaskService and the real Rust gateway."""
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.bridges.orchestrator import OrchestratorConnector
from mas_host.bridges.remote import RemoteSnapshot, RemoteTaskService, RemoteTaskStore
from mas_host.bridges.temporal import TemporalWorkflowAdapter
from mas_host.task_service import DurableTaskService, Workflow
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


class TaskServiceOrchestratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def test_actual_task_service_completed_payload_and_idempotent_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            workflow = Workflow("echo/1", topology(),
                {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")},
                "a", "b", "echo")
            service = DurableTaskService(Path(temporary), {"echo": workflow}, {"worker": {"echo"}},
                                         mas_bin=self.mas, gateway_bin=self.gateway, supervised=False)
            connector = OrchestratorConnector(service, "worker")
            first = connector.submit("one-operation", "echo", {"answer": 42})
            self.assertEqual(connector.submit("one-operation", "echo", {"answer": 42})["task_id"], first["task_id"])
            self.assertEqual(service.run_once()["state"], "completed")
            self.assertEqual(connector.result(first["task_id"]), {"answer": 42})
            self.assertEqual(len(service.list_tasks("worker")), 1)

    def test_factory_binds_real_admission_directory_and_principal_scoped_jobs(self):
        class Client:
            def __init__(self): self.calls = []
            def descriptor(self): return {"provider": "temporal-fixture", "contract": "test/1"}
            def submit(self, payload):
                self.calls.append(("submit", payload))
                return RemoteSnapshot("remote-" + str(len(self.calls)), "running")
            def poll(self, remote_id):
                self.calls.append(("poll", remote_id))
                return RemoteSnapshot(remote_id, "completed", {"answer": 42})
        client = Client()
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            prototypes = {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")}
            bindings = []
            def factory(binding):
                bindings.append(binding)
                remote = RemoteTaskService(RemoteTaskStore(binding.state_dir), client,
                                           namespace=binding.remote_namespace("b"))
                return {**prototypes, "b": TemporalWorkflowAdapter(remote, binding.run_dir,
                                                                    reservation_microusd=3)}
            workflow = Workflow("remote/1", topology(), prototypes, "a", "b", "workflow", adapter_factory=factory)
            service = DurableTaskService(state, {"remote": workflow},
                {"alice": {"remote"}, "bob": {"remote"}}, mas_bin=self.mas, gateway_bin=self.gateway, supervised=False)
            task = service.submit("alice", "submit-1", "remote", {"action": "submit", "request_key": "job", "input": {}})
            submitted = service.run_once()
            self.assertEqual(submitted["state"], "completed")
            self.assertEqual(service.result("alice", task["task_id"])["state"], "running")
            service.submit("alice", "poll-1", "remote", {"action": "poll", "request_key": "job"})
            self.assertEqual(service.run_once()["result"]["state"], "completed")
            self.assertNotEqual(bindings[0].run_dir, bindings[1].run_dir)
            self.assertEqual(bindings[0].remote_namespace("b"), bindings[1].remote_namespace("b"))
            service.submit("bob", "submit-1", "remote", {"action": "submit", "request_key": "job", "input": {}})
            self.assertEqual(service.run_once()["result"]["remote_id"], "remote-3")
            self.assertNotEqual(bindings[0].remote_namespace("b"), bindings[2].remote_namespace("b"))
            self.assertEqual([action for action, _ in client.calls], ["submit", "poll", "submit"])
            self.assertNotEqual(client.calls[0][1]["request_key"], client.calls[2][1]["request_key"])


if __name__ == "__main__":
    unittest.main()
