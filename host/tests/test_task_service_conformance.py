"""Execute the same finite observation traces checked against task-service.pml.

The real service and SQLite repository are used. A controllable executor stands
in for the separately tested Rust/host checkpoint boundary; this is finite
trace correspondence, not a refinement proof or a real remote-peer test.
"""
import json
from pathlib import Path
import tempfile
import threading
import unittest

from mas_host import Outcome
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from test_runner import FunctionAdapter, topology


CORPUS = Path(__file__).resolve().parent / "fixtures" / "task-service-traces.json"
PRINCIPALS = ("alice", "bob")


class CheckpointExecutor:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.attempts = set()
        self.checkpoints = {}
        self.crashing = False
        self.current = None
        self.reply = None

    def prepare(self):
        self.started.clear()
        self.release.clear()
        self.crashing = False
        self.reply = None

    def execute(self, workflow, execution, **options):
        self.current = execution["run_id"]
        if self.current in self.attempts:
            self.reply = self.checkpoints.get(self.current, "unknown")
        self.attempts.add(self.current)
        self.started.set()
        if not self.release.wait(5):
            raise AssertionError("trace did not release executor")
        if self.crashing:
            raise SystemExit("injected worker death")
        return ExecutionResult(self.reply, {"value": 1})

    def answer(self, state):
        self.reply = state
        self.checkpoints[self.current] = state


class TaskServiceTraceConformanceTests(unittest.TestCase):
    def test_shared_finite_traces(self):
        corpus = json.loads(CORPUS.read_text())
        self.assertEqual(corpus["version"], 1)
        for trace in corpus["traces"]:
            with self.subTest(trace=trace["name"]), tempfile.TemporaryDirectory() as directory:
                executor = CheckpointExecutor()
                workflow = Workflow("conformance/1", topology(),
                    {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")},
                    "a", "b", "echo", continue_input=lambda _previous, supplied: supplied)
                service = DurableTaskService(Path(directory) / "state", {"echo": workflow},
                    {principal: {"echo"} for principal in PRINCIPALS}, executor=executor, max_revisions=2)
                ids = {}
                accepted_input = {}
                revision_input = {}
                thread = None
                worker_result = []

                def run_worker():
                    try:
                        worker_result.append(service.run_once())
                    except BaseException as error:
                        worker_result.append(error)

                try:
                    for index, action in enumerate(trace["actions"]):
                        with self.subTest(step=index, action=action["op"]):
                            op, args, expected = action["op"], action["args"], action["expect"]
                            observed_error, response = None, None
                            try:
                                if op == "submit":
                                    response = service.submit(PRINCIPALS[args[0]], "shared", "echo", {"value": args[1]})
                                    if args[0] in ids:
                                        self.assertEqual(response["task_id"], ids[args[0]])
                                    ids[args[0]] = response["task_id"]
                                    accepted_input.setdefault(args[0], {"value": args[1]})
                                    revision_input.setdefault((args[0], 0), {"value": args[1]})
                                elif op == "inspect":
                                    response = service.inspect(PRINCIPALS[args[0]], ids[args[1]])
                                elif op == "cancel":
                                    response = service.cancel(PRINCIPALS[args[0]], ids[args[1]])
                                elif op == "update":
                                    response = service.update(PRINCIPALS[args[0]], ids[args[1]],
                                        "update-" + str(args[2]), {"value": args[3]})
                                    revision_input.setdefault((args[1], response["revision"]), {"value": args[3]})
                                elif op == "begin":
                                    executor.prepare()
                                    worker_result.clear()
                                    thread = threading.Thread(target=run_worker)
                                    thread.start()
                                    if expected.get("idle"):
                                        thread.join(5)
                                        self.assertFalse(thread.is_alive())
                                        self.assertEqual(worker_result, [None])
                                    else:
                                        self.assertTrue(executor.started.wait(5), worker_result)
                                        self.assertEqual(executor.current, self._run_id(service, ids[args[0]]))
                                elif op == "answer":
                                    executor.answer(args[0])
                                elif op in {"commit", "crash"}:
                                    executor.crashing = op == "crash"
                                    executor.release.set()
                                    thread.join(5)
                                    self.assertFalse(thread.is_alive())
                                    self.assertEqual(len(worker_result), 1)
                                    if op == "crash":
                                        self.assertIsInstance(worker_result[0], SystemExit)
                                    else:
                                        response = worker_result[0]
                                        self.assertIsInstance(response, dict)
                                else:
                                    self.fail("unknown trace operation: " + op)
                            except ServiceError as error:
                                observed_error = error.code
                            self.assertEqual(observed_error, expected.get("error"))
                            for owner in range(2):
                                if owner not in ids:
                                    self.assertEqual(expected["state"][owner], "absent")
                                    continue
                                receipt = service.inspect(PRINCIPALS[owner], ids[owner])
                                self.assertEqual(receipt["state"], expected["state"][owner])
                                self.assertEqual(receipt["revision"], expected["revision"][owner])
                                self.assertEqual(receipt["cancel_requested"], expected["cancelled"][owner])
                                events = service.events(PRINCIPALS[owner], ids[owner])
                                self.assertEqual(len(events), expected["events"][owner])
                                self.assertTrue(all(item["task_id"] == ids[owner] for item in events))
                                self.assertEqual([item["id"] for item in events], sorted({item["id"] for item in events}))
                                if isinstance(response, dict) and response.get("task_id") == ids[owner]:
                                    self.assertEqual(response, receipt)
                                with service.store.transaction(readonly=True) as db:
                                    rows = db.execute("SELECT revision,run_id,input FROM executions WHERE task_id=? ORDER BY revision", (ids[owner],)).fetchall()
                                    stored_input = json.loads(db.execute("SELECT input FROM tasks WHERE id=?", (ids[owner],)).fetchone()[0])
                                self.assertEqual(stored_input, accepted_input[owner])
                                self.assertEqual([row[0] for row in rows], list(range(receipt["revision"] + 1)))
                                self.assertEqual(len({row[1] for row in rows}), len(rows))
                                for row in rows:
                                    self.assertEqual(json.loads(row[2]), revision_input[(owner, row[0])])
                finally:
                    executor.release.set()
                    if thread is not None:
                        thread.join(5)

    @staticmethod
    def _run_id(service, task_id):
        with service.store.transaction(readonly=True) as db:
            return db.execute("SELECT run_id FROM executions WHERE task_id=? ORDER BY revision DESC LIMIT 1", (task_id,)).fetchone()[0]
