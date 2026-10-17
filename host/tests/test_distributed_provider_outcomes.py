"""Known provider outcomes survive registered worker IPC, spool and controller."""
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from mas_host.contracts import AdapterError, Outcome
from mas_host.distributed import RemoteWorkerAdapter, WorkerController
from mas_host.distributed_worker import DurableWorker, WorkerClient, WorkerOperation
from mas_host.executable_worker import WorkerSpec
from mas_host.journal import inspect_run
from mas_host.runner import Runner
from mas_host.bridges.authentication import StaticTokenVerifier
from test_bridge_server import ServerFixture
from test_runner import FunctionAdapter, topology

ROOT = Path(__file__).resolve().parents[2]
GATEWAY = ROOT / "rust-protocol/target/release/mas-gateway"
MAS = ROOT / "rust-protocol/target/release/mas"
FIXTURE = Path(__file__).parent / "fixtures/registered_worker.py"
TOKEN = "worker-outcome-test-token-000000000000"


class DistributedProviderOutcomeTests(unittest.TestCase):
    def test_known_outcomes_survive_all_boundaries_without_worker_retry(self):
        for mode in ("not_sent", "completed_invalid", "completed_valid"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                state, marker = root / "controller", root / "attempts"
                spec = WorkerSpec("fixture", "1", (sys.executable, str(FIXTURE)), factory="fixture",
                    configuration={"mode": mode, "record_file": str(marker)}, cwd=ROOT,
                    environment={"PYTHONPATH": str(ROOT / "host")})
                operation = WorkerOperation.registered("observe", spec, gateway_bin=str(GATEWAY))
                if mode == "completed_valid":
                    from mas_host.executable_worker import WorkerCleanupError
                    execute = operation.execute.executor.execute
                    def cleanup_fails(*args, **kwargs):
                        execute(*args, **kwargs)
                        raise WorkerCleanupError("injected guardian cleanup failure")
                    operation.execute.executor.execute = cleanup_fails
                controller = WorkerController(state, {"worker": {"capabilities": {"observe": operation.implementation},
                    "trust_zone": "test", "capacity": 1}})
                token = root / "token"
                token.write_text(TOKEN)
                token.chmod(0o600)
                verifier = StaticTokenVerifier([{"sha256": hashlib.sha256(TOKEN.encode()).hexdigest(),
                                                 "principal": "worker", "scopes": ["mas:worker"]}])
                with ServerFixture(worker_controller=controller, verifier=verifier, request_log=lambda _r: None) as server:
                    client = WorkerClient(server.endpoint, token_file=token, allow_loopback_http=True)
                    worker = DurableWorker(client, root / "worker", [operation], trust_zone="test")
                    original_submit = controller.submit_admitted
                    def submit(call_id, binding):
                        accepted = original_submit(call_id, binding)
                        worker.run_once()
                        return accepted
                    controller.submit_admitted = submit
                    adapter = RemoteWorkerAdapter(controller, principal="alice", operation="observe",
                        implementation=operation.implementation, trust_zone="test", reservation_microusd=37)
                    adapters = {"a": FunctionAdapter(lambda _c, request: Outcome(request.input)), "b": adapter, "c": adapter}
                    with Runner(topology(), adapters, state, workflow_version="cleanup-observation/1",
                                supervised=False, mas_bin=str(MAS), gateway_bin=str(GATEWAY)) as runner:
                        with self.assertRaises(AdapterError):
                            runner.request("a", "b", "observe", {})
                        report = inspect_run(runner.run_dir)
                        self.assertFalse(report["recovery_required"])
                        call = report["invocations"][0]
                        self.assertEqual(call["state"], "failed_known")
                        outcome = call["evidence"]["provider_outcome"]
                        self.assertEqual(outcome["cleanup_failed"], mode == "completed_valid")
                        self.assertEqual(outcome["certainty"], "not_sent" if mode == "not_sent" else "completed")
                        if mode != "not_sent":
                            self.assertEqual(outcome["completion"]["response_id"], "fixture-completed")
                        if mode == "completed_valid":
                            self.assertEqual(outcome["output_status"], "valid")
                            self.assertEqual(call["output"], {"answer": "PRIVATE completed output"})
                            self.assertNotIn("PRIVATE", str(outcome))
                            self.assertNotIn("PRIVATE", str(runner.status()["calls"][0]["evidence"]))
                        self.assertEqual(runner.status()["usage"]["calls"], 1)
                        self.assertEqual(call["reservation_microusd"], 37)
                        run_dir = runner.run_dir
                    with Runner.resume(run_dir, adapters, workflow_version="cleanup-observation/1", supervised=False,
                                       mas_bin=str(MAS), gateway_bin=str(GATEWAY)) as runner:
                        with self.assertRaises(AdapterError):
                            runner.request("a", "b", "observe", {})
                        self.assertEqual(runner.status()["usage"]["calls"], 1)
                    self.assertIsNone(worker.run_once())
                    self.assertEqual(marker.read_text().splitlines(), ["attempt"])

    def test_exact_cleanup_failure_preserves_known_completion_from_registered_ipc(self):
        from mas_host.executable_worker import WorkerCleanupError
        from mas_host.provider_outcomes import ProviderOutcomeFailure
        spec = WorkerSpec("fixture", "1", (sys.executable, str(FIXTURE)), factory="fixture",
            configuration={"mode": "completed_invalid"}, cwd=ROOT,
            environment={"PYTHONPATH": str(ROOT / "host")})
        operation = WorkerOperation.registered("observe", spec, gateway_bin=str(GATEWAY))
        execute = operation.execute.executor.execute
        def cleanup_fails(*args, **kwargs):
            try:
                return execute(*args, **kwargs)
            finally:
                raise WorkerCleanupError("injected guardian cleanup failure")
        with patch.object(operation.execute.executor, "execute", cleanup_fails):
            with self.assertRaises(ProviderOutcomeFailure) as failure:
                operation.execute({}, "call-cleanup")
        outcome = failure.exception.outcome
        self.assertTrue(outcome["cleanup_failed"])
        self.assertEqual(outcome["certainty"], "completed")
        self.assertEqual(outcome["completion"]["response_id"], "fixture-completed")


if __name__ == "__main__":
    unittest.main()
