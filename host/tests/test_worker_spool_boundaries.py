"""Configured spool ceilings and inherited ownership across process death."""
from contextlib import closing
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from mas_host.contracts import Invocation
from mas_host.distributed import WORKER_PROTOCOL, remote_binding
from mas_host.distributed_worker import DurableWorker, SPOOL, WorkerOperation
from mas_host.executable_worker import WorkerSpec
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError


ROOT = Path(__file__).resolve().parents[2]
GATEWAY = ROOT / "rust-protocol/target/release/mas-gateway"
FIXTURE = ROOT / "host/tests/fixtures/registered_worker.py"
DIGEST = "a" * 64


class Client:
    endpoint = "https://controller.invalid"
    timeout = 1

    def __init__(self, *, implementation=DIGEST, count=1, large=True):
        self.implementation, self.remaining, self.large = implementation, count, large
        self.calls, self.receipts = [], []
        self.ordinal = 0

    def call(self, operation, **payload):
        self.calls.append(operation)
        if operation == "register":
            return {"boot": payload["boot"], "lease_ms": 300000, "protocol": WORKER_PROTOCOL, "output_profiles": payload["output_profiles"]}
        if operation == "claim":
            if not self.remaining:
                return {"assignment": None}
            self.remaining -= 1
            self.ordinal += 1
            return {"assignment": {"call_id": f"call-{self.ordinal}", "boot": payload["boot"],
                "fence": self.ordinal, "lease_ms": 300000,
                "binding": remote_binding("alice", "echo", self.implementation, "zone",
                                          {"data": "i" * 60000} if self.large else {})}}
        if operation == "dispatch":
            return {"execute": True, "call_id": payload["call_id"], "fence": payload["fence"]}
        if operation == "receipt":
            self.receipts.append(payload["receipt"])
            return {"stored": True, "receipt_id": payload["receipt"]["id"], "authoritative": True}
        if operation == "heartbeat":
            return {}
        raise AssertionError(operation)


def run_stalled_worker(root, record):
    spec = WorkerSpec("fixture", "1", (sys.executable, str(FIXTURE)), factory="fixture",
        configuration={"mode": "stall", "record_file": str(record)}, cwd=ROOT,
        environment={"PYTHONPATH": str(ROOT / "host")})
    operation = WorkerOperation.registered("echo", spec, gateway_bin=GATEWAY, timeout_seconds=30)
    client = Client(implementation=operation.implementation, large=False)
    DurableWorker(client, root, [operation], trust_zone="zone").run_once()


class WorkerSpoolBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_tight_spool_rejects_before_claim_or_dispatch(self):
        calls = []
        operation = WorkerOperation("echo", DIGEST,
            lambda *_: calls.append("effect") or Invocation({"data": "o" * 60000}, {"data": "e" * 60000}))
        client = Client()
        worker = DurableWorker(client, self.root, [operation], trust_zone="zone", max_spool_bytes=196608)
        with self.assertRaisesRegex(ServiceError, "worker spool full"):
            worker.run_once()
        self.assertEqual(calls, [])
        self.assertNotIn("claim", client.calls)
        self.assertNotIn("dispatch", client.calls)
        self.assertLessEqual(worker.path.stat().st_size, 196608)

    def test_bounded_large_results_reuse_preallocated_pages(self):
        calls = []
        operation = WorkerOperation("echo", DIGEST,
            lambda _, call: calls.append(call) or Invocation({"data": "o" * 60000}, {"data": "e" * 60000}))
        client = Client(count=12)
        worker = DurableWorker(client, self.root, [operation], trust_zone="zone", max_spool_bytes=524288)
        sizes = []
        for _ in range(12):
            self.assertTrue(worker.run_once()["receipt_stored"])
            sizes.append(worker.path.stat().st_size)
        self.assertEqual(len(calls), 12)
        self.assertEqual({receipt["status"] for receipt in client.receipts}, {"completed"})
        self.assertTrue(all(len(receipt["output"]["data"]) == 60000 and len(receipt["evidence"]["data"]) == 60000 for receipt in client.receipts))
        self.assertLessEqual(max(sizes), worker.max_spool_bytes)
        self.assertEqual(sizes[-1], sizes[1], "acknowledged rows must make storage reusable")
        with closing(worker._connect()) as db:
            self.assertLessEqual(db.execute("PRAGMA max_page_count").fetchone()[0]
                                 * db.execute("PRAGMA page_size").fetchone()[0], worker.max_spool_bytes)

    def test_reserved_receipt_survives_interrupted_effect_without_reexecution(self):
        calls = []
        def interrupted(_input, call_id):
            calls.append(call_id)
            raise KeyboardInterrupt()
        client = Client()
        operations = [WorkerOperation("echo", DIGEST, interrupted)]
        worker = DurableWorker(client, self.root, operations, trust_zone="zone", max_spool_bytes=524288)
        with self.assertRaises(KeyboardInterrupt):
            worker.run_once()
        with closing(connect(worker.path, SPOOL, readonly=True)) as db:
            row = db.execute("SELECT state,length(receipt) FROM attempts").fetchone()
            self.assertEqual(row[0], "executing")
            self.assertEqual(row[1], 131072 - 128)
        resumed = DurableWorker(client, self.root, operations, trust_zone="zone", max_spool_bytes=524288)
        self.assertIsNone(resumed.run_once())
        self.assertEqual(calls, ["call-1"])
        self.assertEqual(client.receipts[0]["status"], "unknown")
        self.assertLessEqual(resumed.path.stat().st_size, resumed.max_spool_bytes)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Rust ownership requires Linux")
    def test_parent_death_keeps_spool_owned_until_guardian_reaps(self):
        record, state = self.root / "pids.json", self.root / "worker"
        source = "from pathlib import Path; import sys; from test_worker_spool_boundaries import run_stalled_worker; run_stalled_worker(Path(sys.argv[1]),Path(sys.argv[2]))"
        process = subprocess.Popen([sys.executable, "-c", source, str(state), str(record)],
            env={"PYTHONPATH": str(ROOT / "host") + os.pathsep + str(ROOT / "host/tests")},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        pids = None
        try:
            deadline = time.monotonic() + 5
            while not record.exists() and time.monotonic() < deadline and process.poll() is None:
                time.sleep(.01)
            self.assertTrue(record.exists(), "registered effect did not start")
            pids = json.loads(record.read_text())
            os.kill(pids["guardian"], signal.SIGSTOP)
            process.kill()
            process.wait(timeout=5)
            calls = []
            operation = WorkerOperation("echo", DIGEST,
                lambda *_: calls.append("replacement") or Invocation({}, {}))
            client = Client(large=False)
            replacement = DurableWorker(client, state, [operation], trust_zone="zone")
            with self.assertRaisesRegex(ServiceError, "worker busy"):
                replacement.run_once()
            self.assertEqual(client.calls, [])
            self.assertEqual(calls, [])
            os.kill(pids["guardian"], signal.SIGCONT)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and any(Path(f"/proc/{pids[key]}").exists() for key in ("worker", "child")):
                time.sleep(.01)
            self.assertFalse(Path(f"/proc/{pids['worker']}").exists())
            self.assertFalse(Path(f"/proc/{pids['child']}").exists())
            client.remaining = 0
            deadline = time.monotonic() + 5
            while True:
                try:
                    result = replacement.run_once()
                    break
                except ServiceError as error:
                    if error.code != "worker_busy" or time.monotonic() >= deadline:
                        raise
                    time.sleep(.01)
            self.assertIsNone(result)
            self.assertEqual(calls, [])
            self.assertEqual(client.receipts[0]["status"], "unknown")
        finally:
            if pids is not None:
                try:
                    os.kill(pids["guardian"], signal.SIGCONT)
                except ProcessLookupError:
                    pass
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
