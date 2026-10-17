"""Real exec, resource, descriptor, timeout and parent-death supervision tests."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from mas_host import RecoveryRequired, Runner, RunnerError
from mas_host.contracts import AdapterTimeout
from mas_host.executable_worker import RegisteredProcessExecutor, WorkerLimits, WorkerRegistry, WorkerSpec
from mas_host.supervision import _Channel
from test_runner import topology


ROOT = Path(__file__).resolve().parents[2]
BINARIES = ROOT / "rust-protocol" / "target" / "release"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "registered_worker.py"


@unittest.skipUnless(sys.platform.startswith("linux"), "Rust process ownership requires Linux")
class ExecutableWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def spec(self, name="fixture", *, mode="invoke", configuration=None, limits=WorkerLimits()):
        return WorkerSpec(name, "1", (sys.executable, str(FIXTURE)), factory="fixture",
            configuration=configuration or {"mode": mode}, cwd=ROOT,
            environment={"PYTHONPATH": str(ROOT / "host"), "APPROVED_ENVIRONMENT": "fixture"}, limits=limits)

    def runner(self, spec=None, *, state=None, **options):
        registry = WorkerRegistry([spec or self.spec()])
        name = next(iter(registry._registrations))
        runner = Runner(topology(), {actor: registry.adapter(name) for actor in ("a", "b", "c")},
            state or self.directory / "state", mas_bin=str(BINARIES / "mas"),
            gateway_bin=str(BINARIES / "mas-gateway"), protocol_strategy="persistent",
            executor=RegisteredProcessExecutor(BINARIES / "mas-gateway"), **options)
        self.addCleanup(runner.close)
        return runner

    @staticmethod
    def gone(pid):
        try:
            os.kill(pid, 0)
            return False
        except ProcessLookupError:
            return True

    def test_custom_registered_worker_uses_parent_admission_and_records_result(self):
        runner = self.runner()
        self.assertEqual(runner.request("a", "b", "invoke", {"answer": 42}).output, {"answer": 42})
        calls = runner.status()["calls"]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["state"], "recorded")
        self.assertEqual(calls[0]["evidence"]["call_id"], calls[0]["id"])

    def test_builtin_worker_registry_runs_echo_without_dynamic_imports(self):
        spec = WorkerSpec("echo", "1", (sys.executable, "-m", "mas_host.worker_entry"),
            factory="echo", configuration={}, cwd=ROOT, environment={"PYTHONPATH": str(ROOT / "host")})
        runner = self.runner(spec)
        self.assertEqual(runner.request("a", "b", "echo", {"data": "hello"}).output, {"data": "hello"})
        self.assertEqual(runner.status()["calls"], [])

    def test_builtin_structured_provider_factory_uses_registered_contract(self):
        schema = {"type": "object", "properties": {"answer": {"type": "integer"}},
                  "required": ["answer"], "additionalProperties": False}
        configuration = {"actor": "b", "agent": {"provider": "mock", "model": "fixture",
            "reservation_microusd": 0, "instructions": "Return the checked result.", "operation": "solve/1"},
            "contract": {"name": "solve", "version": "1", "input_schema": {"type": "object"},
                         "output_schema": schema, "allowed_callers": ["a"]}, "mock_output": {"answer": 42}}
        spec = WorkerSpec("structured", "1", (sys.executable, "-m", "mas_host.worker_entry"),
            factory="structured", configuration=configuration, cwd=ROOT,
            environment={"PYTHONPATH": str(ROOT / "host")})
        runner = self.runner(spec)
        self.assertEqual(runner.request("a", "b", "solve/1", {}).output, {"answer": 42})
        self.assertEqual(len(runner.status()["calls"]), 1)

    def test_threaded_host_clients_use_exec_without_python_fork(self):
        def work(index):
            registry = WorkerRegistry([self.spec()])
            with Runner(topology(), {actor: registry.adapter("fixture") for actor in ("a", "b", "c")},
                    self.directory / str(index), mas_bin=str(BINARIES / "mas"),
                    gateway_bin=str(BINARIES / "mas-gateway"), protocol_strategy="persistent",
                    executor=RegisteredProcessExecutor(BINARIES / "mas-gateway")) as runner:
                return runner.request("a", "b", "invoke", {"index": index}).output
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(work, range(2))), [{"index": 0}, {"index": 1}])

    def test_explicit_environment_resources_and_no_host_database_descriptors(self):
        limits = WorkerLimits(memory_bytes=256*1024*1024, cpu_seconds=7, file_bytes=10_000)
        runner = self.runner(self.spec(mode="inspect", limits=limits))
        result = runner.request("a", "b", "inspect", {}).output
        self.assertIn("APPROVED_ENVIRONMENT", result["environment"])
        self.assertNotIn("OPENAI_API_KEY", result["environment"])
        self.assertEqual(result["memory"], limits.memory_bytes)
        self.assertEqual(result["cpu"], limits.cpu_seconds)
        self.assertEqual(result["file"], limits.file_bytes)
        self.assertFalse(any("sqlite" in value or "lock" in value
                             for value in result["descriptors"].values()))

    def test_file_resource_limit_is_enforced_during_admitted_call(self):
        output = self.directory / "bounded-output.bin"
        runner = self.runner(self.spec(configuration={"mode": "filelimit", "output_file": str(output)},
                                       limits=WorkerLimits(file_bytes=1024)))
        with self.assertRaises(RecoveryRequired):
            runner.request("a", "b", "write", {})
        self.assertLessEqual(output.stat().st_size, 1024)

    def test_success_reaps_background_descendant(self):
        record = self.directory / "pids.json"
        runner = self.runner(self.spec(configuration={"mode": "spawn", "record_file": str(record)}))
        self.assertEqual(runner.request("a", "b", "spawn", {}).output, {"spawned": True})
        pids = json.loads(record.read_text())
        self.assertTrue(self.gone(pids["worker"]))
        self.assertTrue(self.gone(pids["child"]))

    def test_deadline_reaps_worker_tree_and_preserves_uncertainty(self):
        record = self.directory / "pids.json"
        runner = self.runner(self.spec(configuration={"mode": "stall", "record_file": str(record)}),
                             execution_deadline=time.monotonic() + 15)
        remaining = _Channel.remaining
        armed = False

        def after_admission(channel):
            nonlocal armed
            # The fixture writes this marker inside context.invoke's callback,
            # after the parent commits admission and sends the execution grant.
            if not armed and record.exists():
                channel.deadline = min(channel.deadline, time.monotonic() + 0.1)
                armed = True
            return remaining(channel)

        with mock.patch.object(_Channel, "remaining", after_admission), self.assertRaises(RecoveryRequired):
            runner.request("a", "b", "stall", {})
        self.assertTrue(armed)
        pids = json.loads(record.read_text())
        self.assertTrue(self.gone(pids["worker"]))
        self.assertTrue(self.gone(pids["child"]))
        self.assertEqual(runner._journal.invocation(runner.status()["calls"][0]["id"])["state"], "unknown")

    def test_expired_deadline_before_dispatch_has_no_uncertain_invocation(self):
        record = self.directory / "pids.json"
        runner = self.runner(self.spec(configuration={"mode": "stall", "record_file": str(record)}),
                             execution_deadline=time.monotonic() - 1)
        with self.assertRaises(AdapterTimeout):
            runner.request("a", "b", "stall", {})
        self.assertFalse(record.exists())
        self.assertEqual(runner.status()["calls"], [])

    def test_host_death_keeps_guardian_until_descendants_are_reaped(self):
        record = self.directory / "pids.json"
        source = '''
from pathlib import Path
import sys
from mas_host import Runner
from mas_host.executable_worker import WorkerSpec,WorkerRegistry,RegisteredProcessExecutor
from test_runner import topology
root,state,record,fixture=map(Path,sys.argv[1:])
spec=WorkerSpec('fixture','1',(sys.executable,str(fixture)),factory='fixture',configuration={'mode':'stall','record_file':str(record)},cwd=root,environment={'PYTHONPATH':str(root/'host')})
registry=WorkerRegistry([spec])
with Runner(topology(),{a:registry.adapter('fixture') for a in ('a','b','c')},state,mas_bin=str(root/'rust-protocol/target/release/mas'),gateway_bin=str(root/'rust-protocol/target/release/mas-gateway'),executor=RegisteredProcessExecutor(root/'rust-protocol/target/release/mas-gateway')) as runner:
    runner.request('a','b','stall',{})
'''
        process = subprocess.Popen([sys.executable, "-c", source, str(ROOT), str(self.directory / "state"),
            str(record), str(FIXTURE)], env={"PYTHONPATH": str(ROOT / "host") + os.pathsep + str(ROOT / "host/tests")},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 5
            while not record.exists() and time.monotonic() < deadline and process.poll() is None:
                time.sleep(0.01)
            self.assertTrue(record.exists())
            pids = json.loads(record.read_text())
            process.kill()
            process.wait(timeout=5)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not all(self.gone(pids[key]) for key in ("worker", "child")):
                time.sleep(0.01)
            self.assertTrue(self.gone(pids["worker"]))
            self.assertTrue(self.gone(pids["child"]))
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)

    def test_guardian_termination_signal_reaps_its_worker_tree(self):
        record = self.directory / "signal-pids.json"
        runner = self.runner(self.spec(configuration={"mode": "stall", "record_file": str(record)}))

        def terminate():
            deadline = time.monotonic() + 5
            while not record.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if record.exists():
                os.kill(json.loads(record.read_text())["guardian"], signal.SIGTERM)

        watcher = threading.Thread(target=terminate)
        watcher.start()
        try:
            with self.assertRaises(RecoveryRequired):
                runner.request("a", "b", "stall", {})
        finally:
            watcher.join(timeout=6)
        pids = json.loads(record.read_text())
        self.assertTrue(self.gone(pids["worker"]))
        self.assertTrue(self.gone(pids["child"]))

    def test_registration_changes_and_unknown_workers_fail_before_dispatch(self):
        registry = WorkerRegistry([self.spec()])
        with self.assertRaises(RunnerError):
            registry.adapter("request-selected-module")
        with self.assertRaises(RunnerError):
            WorkerSpec("bad", "1", ("python",), factory="echo", configuration={}, cwd=ROOT)
        spec = self.spec()
        copied = spec.descriptor
        copied["configuration"]["mode"] = "stall"
        self.assertEqual(spec.descriptor["configuration"]["mode"], "invoke")

    def test_pinned_script_changes_fail_before_worker_admission(self):
        import hashlib
        script = self.directory / "worker.py"
        script.write_bytes(FIXTURE.read_bytes())
        spec = WorkerSpec("pinned", "1", (sys.executable, str(script)), factory="fixture",
            configuration={"mode": "invoke"}, cwd=ROOT,
            environment={"PYTHONPATH": str(ROOT / "host")},
            pinned_files={str(script): hashlib.sha256(script.read_bytes()).hexdigest()})
        runner = self.runner(spec)
        script.write_text("raise RuntimeError('changed')")
        with self.assertRaises(RunnerError):
            runner.request("a", "b", "invoke", {})
        self.assertEqual(runner.status()["calls"], [])


if __name__ == "__main__":
    unittest.main()
