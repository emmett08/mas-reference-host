"""Process dispatcher concurrency, restart ownership and graceful drain."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from mas_host.bridge_config import build_service
from mas_host.bridge_health import worker_health
from mas_host.operations import OperationsError
from mas_host.service_contracts import ServiceError
from mas_host.service_worker import ServiceDispatcher


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux process lifetime tests")
class ServiceDispatcherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.config = {"version": 1, "factory": "fixtures.dispatcher_workflows:workflows",
            "grants": {"alice": ["barrier"], "bob": ["barrier"]},
            "scheduler": {"max_workers": 2, "max_per_principal": 2}}
        self.path = self.root / "config.json"
        self.path.write_text(json.dumps(self.config))
        self.environment = {**os.environ, "PYTHONPATH": str(ROOT / "host") + os.pathsep + str(ROOT / "host/tests"),
            "MAS_BIN": str(ROOT / "rust-protocol/target/release/mas"),
            "MAS_GATEWAY_BIN": str(ROOT / "rust-protocol/target/release/mas-gateway")}
        with patch.dict(os.environ, self.environment):
            self.service = build_service(self.config, self.state)
        self.records = []
        self.addCleanup(self.release_workers)

    def release_workers(self):
        for item in self.records:
            item["release"].touch()
            if item["started"].exists():
                try:
                    pid = json.loads(item["started"].read_text())["service"]
                    if pid > 1 and pid != os.getpid():
                        os.kill(pid, signal.SIGTERM)
                except (OSError, ValueError, KeyError):
                    pass

    def submit(self, name, principal="alice"):
        item = {"started": self.root / f"{name}-started.json", "release": self.root / f"{name}-release"}
        receipt = self.service.submit(principal, name, "barrier",
            {"started": str(item["started"]), "release": str(item["release"]), "value": name})
        item.update(task_id=receipt["task_id"], principal=principal)
        self.records.append(item)
        return item

    def worker(self, *args):
        process = subprocess.Popen([sys.executable, "-m", "mas_host", "bridge", "worker",
            "--config", str(self.path), "--state-dir", str(self.state), "--interval", "0.02",
            "--drain-seconds", "0.2", *args], env=self.environment,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        def close():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            process.stderr.close()
        self.addCleanup(close)
        return process

    def wait(self, predicate, process=None, timeout=6):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if process is not None and process.poll() is not None:
                self.fail("dispatcher exited: " + process.stderr.read().decode())
            try:
                if predicate():
                    return
            except (OSError, ValueError, ServiceError, OperationsError):
                pass
            time.sleep(0.02)
        self.fail("dispatcher condition timed out")

    def completed(self, item):
        return self.service.inspect(item["principal"], item["task_id"])["state"] == "completed"

    @staticmethod
    def alive(pid):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True

    def wait_for_shutdown(self, process):
        try:
            self.assertEqual(process.wait(timeout=5), 0)
        except subprocess.TimeoutExpired as error:
            diagnostics = {"dispatcher_pid": process.pid, "tasks": []}
            for item in self.records:
                task = {"task_id": item["task_id"]}
                try:
                    task["state"] = self.service.inspect(item["principal"], item["task_id"])["state"]
                    if item["started"].exists():
                        task["processes"] = {role: {"pid": pid, "alive": self.alive(pid)}
                            for role, pid in json.loads(item["started"].read_text()).items()}
                except (OSError, ValueError, ServiceError, OperationsError) as diagnostic_error:
                    task["diagnostic_error"] = str(diagnostic_error)
                diagnostics["tasks"].append(task)
            message = "shutdown diagnostics: " + json.dumps(diagnostics, sort_keys=True)
            if hasattr(error, "add_note"):
                error.add_note(message)
            else:  # Python 3.10 has no exception notes.
                print(message, file=sys.stderr)
            raise

    def test_independent_assignments_overlap_and_process_limit_holds(self):
        first = self.submit("first")
        second = self.submit("second", "bob")
        third = self.submit("third")
        process = self.worker()
        self.wait(lambda: first["started"].exists() and second["started"].exists(), process)
        self.assertFalse(third["started"].exists())
        self.assertTrue(worker_health(self.state)["worker_ready"])
        for item in self.records:
            item["release"].touch()
        self.wait(lambda: all(self.completed(item) for item in self.records), process)
        self.assertEqual(self.service.operations.health()["usage"]["calls"], 3)

    def test_dispatcher_restart_counts_surviving_assignment_processes(self):
        first, second, third = self.submit("first"), self.submit("second", "bob"), self.submit("third")
        process = self.worker()
        self.wait(lambda: first["started"].exists() and second["started"].exists(), process)
        process.kill()
        process.wait(timeout=5)
        replacement = self.worker()
        self.wait(lambda: worker_health(self.state)["completed_polls"] >= 4, replacement)
        self.assertFalse(third["started"].exists())
        for item in self.records:
            item["release"].touch()
        self.wait(lambda: all(self.completed(item) for item in self.records), replacement)
        self.assertEqual(self.service.operations.health()["usage"]["calls"], 3)

    def test_shutdown_stops_new_dispatch_and_interrupts_after_drain_allowance(self):
        first, second = self.submit("first"), self.submit("second")
        process = self.worker("--processes", "1")
        self.wait(lambda: first["started"].exists(), process)
        process.terminate()
        self.wait_for_shutdown(process)
        self.assertFalse(second["started"].exists())
        self.assertEqual(self.service.inspect("alice", second["task_id"])["state"], "queued")
        state = self.service.inspect("alice", first["task_id"])["state"]
        self.assertIn(state, {"unknown", "running"})

    def test_shutdown_during_telemetry_stops_dispatch_and_reaps_adapter(self):
        first, second = self.submit("first"), self.submit("second")
        entered = self.root / "telemetry-entered"
        gate = self.root / "telemetry-gate.json"
        gate.write_text(json.dumps({"started": str(first["started"]), "entered": str(entered)}))
        self.environment["MAS_TEST_TELEMETRY_GATE"] = str(gate)
        process = self.worker("--processes", "1")
        self.wait(entered.exists, process)
        owners = json.loads(first["started"].read_text())
        self.assertTrue(all(self.alive(pid) for pid in owners.values()))
        process.terminate()
        self.wait_for_shutdown(process)
        self.assertFalse(second["started"].exists())
        self.assertEqual(self.service.inspect("alice", second["task_id"])["state"], "queued")
        self.assertEqual(self.service.inspect("alice", first["task_id"])["state"], "unknown")
        for role, pid in owners.items():
            self.assertFalse(self.alive(pid), f"{role} process {pid} survived dispatcher shutdown")

    def test_operator_drain_survives_restart_and_preserves_active_work(self):
        from mas_host.worker_control import WorkerControl
        control = WorkerControl(self.service.operations)
        def set_state(key, revision, draining):
            return control.set(operator="operator", request_key=key, reason="Maintenance",
                               expected_revision=revision, draining=draining)
        set_state("initial-drain", 0, True)
        first = self.submit("first")
        process = self.worker()
        self.wait(lambda: worker_health(self.state)["phase"] == "draining", process)
        self.assertFalse(first["started"].exists())
        self.assertFalse(worker_health(self.state)["admission_open"])
        set_state("resume", 1, False)
        self.wait(lambda: first["started"].exists(), process)
        set_state("drain-active", 2, True)
        second = self.submit("second")
        self.wait(lambda: worker_health(self.state)["phase"] == "draining", process)
        self.assertFalse(second["started"].exists())
        first["release"].touch()
        self.wait(lambda: self.completed(first), process)
        self.assertEqual(self.service.inspect("alice", second["task_id"])["state"], "queued")
        set_state("resume-again", 3, False)
        self.wait(lambda: second["started"].exists(), process)
        second["release"].touch()
        self.wait(lambda: self.completed(second), process)

    def test_configuration_change_prevents_new_child_execution(self):
        item = self.submit("first")
        dispatcher = ServiceDispatcher(self.service, self.path, self.config)
        modified = {**self.config, "max_pending": 30}
        self.path.write_text(json.dumps(modified))
        with patch.dict(os.environ, self.environment):
            self.assertTrue(dispatcher._launch(item["task_id"]))
        deadline = time.monotonic() + 5
        result = []
        while not result and time.monotonic() < deadline:
            result = dispatcher._reap()
            time.sleep(0.01)
        self.assertEqual(result[0]["error"], "worker_configuration_changed")
        self.assertFalse(item["started"].exists())


if __name__ == "__main__":
    unittest.main()
