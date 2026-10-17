"""Failure and restart tests for durable cloud jobs; no live provider requests."""
from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import sqlite3
import signal
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mas_host.cursor_cloud import CloudError, CloudJobSpec, CursorCloudClient, run_snapshot
from mas_host.durable_jobs import DurableJobService, JobStore, receipt
from mas_host.jobs_cli import main, submit_job
from mas_host.journal import Journal
from mas_host.operations import DeploymentLedger, OperationsError, _file_lock


SPEC = CloudJobSpec("https://github.com/example/project", "0123456789abcdef", "available-model",
                    "Inspect README and describe the documented entry points. Do not modify files.")


class FakeCloud:
    def __init__(self):
        self.calls = []
        self.agent_id = None
        self.run_id = "run-example-1"
        self.status = "RUNNING"
        self.error = None
        self.wrong_agent = self.wrong_run = False

    def create(self, payload):
        self.calls.append(("create", payload))
        self.agent_id = payload["agentId"]
        if self.error:
            raise self.error
        return {"agent": {"id": self.agent_id}, "run": self._run()}

    def _run(self):
        return {"id": "run-unrelated" if self.wrong_run else self.run_id,
                "agentId": "bc-unrelated" if self.wrong_agent else self.agent_id,
                "status": self.status, "result": "Untrusted result text", "durationMs": 15}

    def run(self, agent_id, run_id):
        self.calls.append(("run", agent_id, run_id))
        if self.error:
            raise self.error
        return self._run()

    def agent(self, agent_id):
        self.calls.append(("agent", agent_id))
        return {"id": agent_id, "repos": [{"url": SPEC.repository, "startingRef": SPEC.ref}],
                "autoCreatePR": False, "workOnCurrentBranch": False,
                # Deliberately unrelated: attachment must not silently follow it.
                "latestRunId": "run-unrelated"}

    def cancel(self, agent_id, run_id):
        self.calls.append(("cancel", agent_id, run_id))
        if self.error:
            raise self.error
        return {"id": run_id}


class DurableCloudTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        ledger = DeploymentLedger(self.root)
        run_dir = self.root / "run-1"
        run_dir.mkdir(mode=0o700)
        ledger.admit_run("run-1", run_dir)
        ledger.admit_call("run-1", "call-example", SPEC.reservation_microusd)
        journal = Journal(run_dir, {"run_id": "run-1", "workflow_version": "fixture", "topology": {}, "max_depth": 8})
        with journal.db:
            journal.db.execute("INSERT INTO invocations VALUES('call-example','step-0','cloud','executing',NULL,NULL,?)", (SPEC.reservation_microusd,))
        journal.close()
        self.store = JobStore(self.root)
        self.client = FakeCloud()
        self.service = DurableJobService(self.store, self.client)
        self.store.prepare("job-1", SPEC)

    def submit(self):
        return self.service.submit("job-1", SPEC, call_id="call-example", host_run_dir=self.root / "run-1")

    def test_intent_before_network_and_stable_receipt(self):
        original = self.client.create
        def create(payload):
            with sqlite3.connect(self.store.path) as db:
                row = db.execute("SELECT state,agent_id,call_id FROM jobs").fetchone()
            self.assertEqual(row, ("launch_unknown", payload["agentId"], "call-example"))
            self.assertFalse(payload["autoCreatePR"])
            self.assertFalse(payload["workOnCurrentBranch"])
            self.assertNotIn("envVars", payload)
            return original(payload)
        self.client.create = create
        invocation = self.submit()
        self.assertTrue(invocation.output["submission_only"])
        self.assertEqual(self.store.status("job-1")["state"], "active")
        self.assertEqual(invocation.output, self.submit().output)
        self.assertEqual(len(self.client.calls), 1)

    def test_second_invocation_cannot_redispatch_same_job(self):
        self.submit()
        with self.assertRaises(CloudError):
            self.service.submit("job-1", SPEC, call_id="call-other", host_run_dir=self.root / "run-2")
        self.assertEqual(len(self.client.calls), 1)

    def test_lost_create_reply_stays_unknown_on_restart_and_is_not_retried(self):
        self.client.error = CloudError("ambiguous network failure")
        with self.assertRaises(CloudError):
            self.submit()
        restored = DurableJobService(JobStore(self.root), self.client)
        self.assertEqual(restored.store.status("job-1")["state"], "launch_unknown")
        self.client.error = None
        with self.assertRaises(CloudError):
            restored.submit("job-1", SPEC, call_id="call-example", host_run_dir=self.root / "run-1")
        self.assertEqual(len(self.client.calls), 1)

    def test_lost_local_checkpoint_after_remote_success_does_not_retry(self):
        with patch.object(self.service, "_snapshot", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                self.submit()
        self.assertEqual(self.store.status("job-1")["state"], "launch_unknown")
        with self.assertRaises(CloudError):
            self.submit()
        self.assertEqual(len(self.client.calls), 1)

    def test_audited_attachment_uses_explicit_run_not_latest(self):
        self.client.error = CloudError("unknown")
        with self.assertRaises(CloudError):
            self.submit()
        self.client.error = None
        result = self.service.attach("job-1", self.client.run_id, evidence={"ticket": "operator-reviewed-123"})
        self.assertEqual(result["run_id"], "run-example-1")
        self.assertEqual(result["state"], "active")
        self.assertEqual(self.store.inspect("job-1")["audit"][-1]["event"], "operator_attached")
        self.assertEqual(self.client.calls[-1], ("run", self.client.agent_id, self.client.run_id))
        with self.assertRaises(CloudError):
            self.service.attach("job-1", self.client.run_id, evidence={"again": True})

    def test_404_or_failed_attachment_never_authorises_create(self):
        self.client.error = CloudError("unknown")
        with self.assertRaises(CloudError):
            self.submit()
        with self.assertRaises(CloudError):
            self.service.attach("job-1", self.client.run_id, evidence={"ticket": "checked"})
        self.assertEqual(self.store.status("job-1")["state"], "launch_unknown")
        self.assertEqual(sum(call[0] == "create" for call in self.client.calls), 1)

    def test_attachment_rejects_wrong_repository_and_absent_evidence(self):
        self.client.error = CloudError("unknown")
        with self.assertRaises(CloudError):
            self.submit()
        self.client.error = None
        with self.assertRaises(CloudError):
            self.service.attach("job-1", self.client.run_id, evidence={})
        self.client.agent = lambda _: {"id": self.client.agent_id, "repos": [], "autoCreatePR": False,
                                        "workOnCurrentBranch": False}
        with self.assertRaises(CloudError):
            self.service.attach("job-1", self.client.run_id, evidence={"verified": True})
        self.assertEqual(self.store.status("job-1")["state"], "launch_unknown")

    def test_wrong_agent_or_run_poll_cannot_change_state(self):
        self.submit()
        old = self.store.status("job-1")
        for field in ("wrong_agent", "wrong_run"):
            setattr(self.client, field, True)
            with self.assertRaises(CloudError):
                self.service.poll("job-1")
            self.assertEqual(self.store.status("job-1"), old)
            setattr(self.client, field, False)

    def test_terminal_snapshot_is_immutable_and_poll_is_local(self):
        self.submit()
        self.client.status = "FINISHED"
        terminal = self.service.poll("job-1")
        count = len(self.client.calls)
        self.client.status = "RUNNING"
        self.assertEqual(self.service.poll("job-1"), terminal)
        self.assertEqual(self.service.cancel("job-1"), terminal)
        self.assertEqual(len(self.client.calls), count)
        self.assertNotIn("result", terminal)
        self.assertEqual(self.store.status("job-1", include_result=True)["result"], "Untrusted result text")

    def test_identical_nonterminal_polls_do_not_grow_audit(self):
        self.submit()
        before = self.store.inspect("job-1")
        for _ in range(3):
            self.service.poll("job-1")
        self.assertEqual(self.store.inspect("job-1"), before)

    def test_cancel_acknowledgement_is_not_terminal_confirmation(self):
        self.submit()
        state = self.service.cancel("job-1")
        self.assertTrue(state["cancel_requested"])
        self.assertFalse(state["remote_complete"])
        self.assertEqual(state["state"], "active")
        with self.assertRaises(CloudError):
            self.service.cancel("job-1")
        self.client.status = "CANCELLED"
        self.assertTrue(self.service.poll("job-1")["remote_complete"])

    def test_failed_cancel_still_preserves_request_intent(self):
        self.submit()
        self.client.error = CloudError("unknown")
        with self.assertRaises(CloudError):
            self.service.cancel("job-1")
        self.assertTrue(self.store.status("job-1")["cancel_requested"])
        self.assertFalse(self.store.status("job-1")["remote_complete"])
        self.client.error = None
        self.service.cancel("job-1", retry=True)
        self.assertEqual(sum(call[0] == "cancel" for call in self.client.calls), 2)

    def test_restored_state_blocks_every_mutation_and_host_resolution(self):
        self.submit()
        (self.root / ".restored-audit-only").touch()
        old = self.store.status("job-1")
        for operation in (lambda: self.store.prepare("job-2", SPEC), self.submit,
                          lambda: self.service.poll("job-1"), lambda: self.service.cancel("job-1"),
                          lambda: self.service.attach("job-1", self.client.run_id, evidence={"ticket": 1}),
                          lambda: self.service.resolve_submission("job-1", evidence={"ticket": 1})):
            with self.assertRaises(CloudError):
                operation()
            self.assertEqual(self.store.status("job-1"), old)
        self.assertEqual(len(self.client.calls), 1)

    def test_foreign_or_unadmitted_host_directory_rejected(self):
        for directory in (self.root.parent / "foreign-run", self.root / "absent-run"):
            with self.assertRaises(CloudError):
                self.service.submit("job-1", SPEC, call_id="call-example", host_run_dir=directory)
        self.assertFalse(self.client.calls)

    def test_changed_model_prompt_or_policy_rejected_before_dispatch(self):
        for spec in (replace(SPEC, model="changed"), replace(SPEC, prompt="changed"),
                     replace(SPEC, auto_create_pr=True), replace(SPEC, reservation_microusd=2)):
            with self.assertRaises(CloudError):
                self.store.prepare("job-1", spec)
        self.assertFalse(self.client.calls)

    def test_future_schema_rejected_without_modification(self):
        with sqlite3.connect(self.store.path) as db:
            db.execute("PRAGMA user_version=999")
        before = self.store.path.read_bytes()
        for operation in (lambda: self.store.status("job-1"), lambda: self.store.prepare("job-2", SPEC)):
            with self.assertRaises(CloudError):
                operation()
            self.assertEqual(self.store.path.read_bytes(), before)

    def test_live_job_lock_excludes_competing_processes_and_backup_lock(self):
        with _file_lock(self.root / ".cursor-jobs.lock", True, nonblocking=True):
            with self.assertRaises(OperationsError):
                self.submit()
        with _file_lock(self.root / ".deployment.lock", True, nonblocking=True):
            with self.assertRaises(OperationsError):
                self.submit()
        self.assertFalse(self.client.calls)

    def _assert_stalled_job_is_independent(self, action):
        if action in {"poll", "cancel"}:
            self.submit()
        elif action == "attach":
            self.client.error = CloudError("lost create reply")
            with self.assertRaises(CloudError):
                self.submit()
            self.client.error = None
        method = {"create": "create", "poll": "run", "cancel": "cancel", "attach": "agent"}[action]
        started, release = threading.Event(), threading.Event()
        original = getattr(self.client, method)
        failures = []
        def stalled(*args):
            started.set()
            if not release.wait(5):
                raise AssertionError("test did not release stalled provider")
            return original(*args)
        setattr(self.client, method, stalled)
        operation = {"create": self.submit, "poll": lambda: self.service.poll("job-1"),
            "cancel": lambda: self.service.cancel("job-1"),
            "attach": lambda: self.service.attach("job-1", self.client.run_id, evidence={"ticket": "exact-run"})}[action]
        def execute():
            try:
                operation()
            except BaseException as error:
                failures.append(error)
        thread = threading.Thread(target=execute)
        thread.start()
        try:
            self.assertTrue(started.wait(2))
            # The in-flight job's durable snapshot and intent stay readable.
            snapshot = self.store.inspect("job-1")
            expected = "launch_unknown" if action in {"create", "attach"} else "active"
            self.assertEqual(snapshot["state"], expected)
            self.assertEqual(self.store.prepare("job-1", SPEC)["agent_id"], snapshot["agent_id"])
            if action == "cancel":
                self.assertTrue(snapshot["cancel_requested"])
            calls = list(self.client.calls)
            for competing in (self.submit, lambda: self.service.poll("job-1"),
                              lambda: self.service.cancel("job-1")):
                with self.assertRaises(OperationsError):
                    competing()
            self.assertEqual(self.client.calls, calls)
            with self.assertRaises(OperationsError), _file_lock(
                    self.root / ".deployment.lock", True, nonblocking=True):
                self.fail("maintenance must not acquire an in-flight deployment")
            # A separate admitted job can prepare, submit and cancel during the
            # wait; only the first job's operation owner remains locked.
            other_dir = self.root / "run-2"
            other_dir.mkdir(mode=0o700)
            ledger = DeploymentLedger(self.root)
            ledger.admit_run("run-2", other_dir)
            ledger.admit_call("run-2", "call-other", SPEC.reservation_microusd)
            journal = Journal(other_dir, {"run_id": "run-2", "workflow_version": "fixture", "topology": {}, "max_depth": 8})
            with journal.db:
                journal.db.execute("INSERT INTO invocations VALUES('call-other','step-0','cloud','executing',NULL,NULL,?)",
                                   (SPEC.reservation_microusd,))
            journal.close()
            self.store.prepare("job-2", SPEC)
            other_client = FakeCloud()
            other_client.run_id = "run-example-2"
            other = DurableJobService(self.store, other_client)
            submitted = other.submit("job-2", SPEC, call_id="call-other", host_run_dir=other_dir)
            self.assertEqual(submitted.output["cursor_run_id"], "run-example-2")
            self.assertTrue(other.cancel("job-2")["cancel_requested"])
            self.assertEqual(self.store.status("job-1")["state"], expected)
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(self.store.status("job-1")["run_id"], self.client.run_id)
        # A terminal observation after the earlier operation is immutable.
        self.client.status = "CANCELLED"
        terminal = self.service.poll("job-1")
        self.client.status = "RUNNING"
        count = len(self.client.calls)
        self.assertEqual(self.service.poll("job-1"), terminal)
        self.assertEqual(self.service.cancel("job-1"), terminal)
        self.assertEqual(len(self.client.calls), count)

    def test_stalled_create_does_not_lock_unrelated_jobs(self):
        self._assert_stalled_job_is_independent("create")

    def test_stalled_poll_does_not_lock_unrelated_jobs(self):
        self._assert_stalled_job_is_independent("poll")

    def test_stalled_cancel_does_not_lock_unrelated_jobs(self):
        self._assert_stalled_job_is_independent("cancel")

    def test_stalled_attachment_does_not_lock_unrelated_jobs(self):
        self._assert_stalled_job_is_independent("attach")

    def test_changed_identity_cannot_receive_an_inflight_result(self):
        self.submit()
        original = self.client.run
        def changed(agent_id, run_id):
            # Model an invalid concurrent administrative edit. Normal job APIs
            # cannot acquire this job's ownership while the poll is in flight.
            with sqlite3.connect(self.store.path) as db:
                db.execute("UPDATE jobs SET call_id='call-changed' WHERE job_id='job-1'")
            return original(agent_id, run_id)
        self.client.run = changed
        self.client.status = "CANCELLED"
        with self.assertRaisesRegex(CloudError, "changed during remote operation"):
            self.service.poll("job-1")
        self.assertEqual(self.store.status("job-1")["state"], "active")

    def test_late_poll_cannot_overwrite_a_retained_terminal_snapshot(self):
        self.submit()
        original = self.client.run
        def changed(agent_id, run_id):
            response = original(agent_id, run_id)
            terminal = run_snapshot(response | {"status": "FINISHED"}, agent_id, run_id)
            with sqlite3.connect(self.store.path) as db:
                db.execute("UPDATE jobs SET state='terminal',snapshot=? WHERE job_id='job-1'",
                           (json.dumps(terminal),))
            return response
        self.client.run = changed
        self.client.status = "RUNNING"
        with self.assertRaisesRegex(CloudError, "changed during remote operation"):
            self.service.poll("job-1")
        retained = self.store.status("job-1")
        self.assertTrue(retained["remote_complete"])
        self.assertEqual(retained["remote_status"], "FINISHED")

    def test_status_cli_needs_no_credentials_and_hides_prompt(self):
        with patch.dict(os.environ, {}, clear=True), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(["--state-dir", str(self.root), "status", "job-1"]), 0)
        value = json.loads(output.getvalue())
        self.assertTrue(value["ok"])
        self.assertNotIn(SPEC.prompt, output.getvalue())

    def test_status_cli_combines_audit_and_result_without_remote_calls(self):
        self.submit()
        self.client.status = "FINISHED"
        self.service.poll("job-1")
        calls = list(self.client.calls)
        for flags, include_result in ((["--audit"], False), (["--audit", "--result"], True)):
            with self.subTest(flags=flags), patch.dict(os.environ, {}, clear=True), \
                    redirect_stdout(io.StringIO()) as output:
                self.assertEqual(main(["--state-dir", str(self.root), "status", "job-1", *flags]), 0)
            value = json.loads(output.getvalue())
            self.assertTrue(value["ok"])
            self.assertTrue(value["job"]["audit"])
            self.assertEqual("result" in value["job"], include_result)
            if include_result:
                self.assertEqual(value["job"]["result"], "Untrusted result text")
        self.assertEqual(self.client.calls, calls)

    def test_saved_submission_can_resolve_host_offline_without_api_key(self):
        self.submit()
        evidence = self.root / "evidence.json"
        evidence.write_text('{"ticket":"saved-receipt-verified"}')
        with patch.dict(os.environ, {}, clear=True), redirect_stdout(io.StringIO()) as output:
            code = main(["--state-dir", str(self.root), "reconcile", "job-1", "--resolve-host",
                         "--evidence", str(evidence)])
        self.assertEqual(code, 0, output.getvalue())
        self.assertEqual(len(self.client.calls), 1)
        with sqlite3.connect(self.root / "run-1" / "host.sqlite") as db:
            self.assertEqual(db.execute("SELECT state FROM invocations").fetchone()[0], "resolved")


class CloudContractTests(unittest.TestCase):
    def test_http_wall_deadline_bounds_headers_without_retry(self):
        class SlowHeaders:
            def __init__(self):
                self.calls = 0
                self.closed = False
            def request(self, *_args, **_kwargs):
                self.calls += 1
            def getresponse(self):
                # A peer can keep the inactivity timer alive with partial headers.
                time.sleep(3)
                raise AssertionError("wall deadline must interrupt header parsing")
            def close(self):
                self.closed = True
        connection = SlowHeaders()
        client = CursorCloudClient("fixture", timeout_seconds=1, connection_factory=lambda *_args, **_kwargs: connection)
        started = time.monotonic()
        with self.assertRaisesRegex(CloudError, "timed out"):
            client.run("bc-one", "run-one")
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(connection.calls, 1)
        self.assertTrue(connection.closed)

    def test_worker_thread_rejected_before_connection_or_effect(self):
        effects, failures = [], []
        client = CursorCloudClient("fixture", connection_factory=lambda *_a, **_k: effects.append(True))
        def worker():
            try:
                client.create({})
            except CloudError as error:
                failures.append(str(error))
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertFalse(effects)
        self.assertEqual(len(failures), 1)

    def test_http_preserves_callers_earlier_timer_and_handler(self):
        from mas_host.cursor_cloud import _wall_timeout
        previous_handler = signal.getsignal(signal.SIGALRM)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        handler = lambda *_args: None
        try:
            signal.signal(signal.SIGALRM, handler)
            signal.setitimer(signal.ITIMER_REAL, 5)
            with _wall_timeout(30):
                self.assertLessEqual(signal.getitimer(signal.ITIMER_REAL)[0], 5)
            self.assertIs(signal.getsignal(signal.SIGALRM), handler)
            self.assertGreater(signal.getitimer(signal.ITIMER_REAL)[0], 0)
            self.assertLessEqual(signal.getitimer(signal.ITIMER_REAL)[0], 5)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)

    def test_nonstring_and_malformed_specs_raise_fixed_error(self):
        for fields in ({"model": None}, {"model": []}, {"repository": "https://example.com:bad/repo"},
                       {"repository": "https://secret@example.com/repo"}, {"repository": None},
                       {"ref": ""}, {"ref": "main\n"}, {"prompt": []}, {"mode": []},
                       {"reservation_microusd": True}, {"auto_create_pr": "true"}):
            with self.subTest(fields=fields), self.assertRaises(CloudError):
                replace(SPEC, **fields)

    def test_unknown_remote_status_and_bad_result_rejected(self):
        base = {"id": "run-one", "agentId": "bc-one", "status": "FINISHED"}
        for fields in ({"status": "NEW_STATE"}, {"result": []}, {"result": "x" * 65_537},
                       {"result": "\ud800"}, {"durationMs": True}, {"id": "../../escape"}):
            with self.assertRaises(CloudError):
                run_snapshot({**base, **fields}, "bc-one")

    def test_http_fixed_origin_no_retries_redaction_and_bounded_response(self):
        class Response:
            status = 200
            def __init__(self, body):
                self.body = io.BytesIO(body)
            def read1(self, count):
                return self.body.read(count)
        class Connection:
            def __init__(self, response):
                self.response, self.calls, self.closed = response, [], False
            def request(self, *args, **kwargs):
                self.calls.append((args, kwargs))
            def getresponse(self):
                return self.response
            def close(self):
                self.closed = True
        connection = Connection(Response(b'{"id":"run-one"}'))
        def factory(host, **options):
            self.assertEqual(host, "api.cursor.com")
            self.assertEqual(options["timeout"], 30)
            return connection
        client = CursorCloudClient("secret-token", connection_factory=factory)
        self.assertEqual(client.cancel("bc-one", "run-one"), {"id": "run-one"})
        self.assertEqual(connection.calls[0][0], ("POST", "/v1/agents/bc-one/runs/run-one/cancel"))
        self.assertTrue(connection.closed)
        connection.getresponse = lambda: (_ for _ in ()).throw(RuntimeError("secret-token"))
        with self.assertRaises(CloudError) as failure:
            client.run("bc-one", "run-one")
        self.assertNotIn("secret-token", str(failure.exception))
        self.assertEqual(len(connection.calls), 2)
        connection.getresponse = lambda: Response(b"x" * (1024 * 1024 + 1))
        with self.assertRaises(CloudError):
            client.run("bc-one", "run-one")
        connection.getresponse = lambda: Response(b'{"id":"one","id":"two"}')
        with self.assertRaises(CloudError):
            client.run("bc-one", "run-one")


class CloudRunnerIntegrationTests(unittest.TestCase):
    def binaries(self):
        root = Path(__file__).resolve().parents[2]
        release = root / "rust-protocol" / "target" / "release"
        binaries = {"MAS_BIN": os.environ.get("MAS_BIN", str(release / "mas")),
                    "MAS_GATEWAY_BIN": os.environ.get("MAS_GATEWAY_BIN", str(release / "mas-gateway"))}
        for binary in binaries.values():
            if not Path(binary).is_file():
                raise RuntimeError("Build Rust release binaries before running cloud integration tests")
        return binaries

    def test_supervised_submission_admitted_once_and_reused_without_new_run(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, self.binaries()):
            store = JobStore(Path(directory))
            # Restart after creating the intent but before binding any MAS run.
            store.prepare("integration-job", SPEC)
            result = submit_job(store, FakeCloud(), "integration-job", SPEC)
            self.assertTrue(result["ok"], result)
            self.assertTrue(result["submission"]["submission_only"])
            repeated = submit_job(store, FakeCloud(), "integration-job", SPEC)
            self.assertTrue(repeated["reused"])
            with sqlite3.connect(Path(directory) / "deployment.sqlite") as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM calls").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT SUM(reserved_microusd) FROM calls").fetchone()[0], 100_000)

    def test_process_death_after_remote_acceptance_reconciles_without_redispatch(self):
        class CrashingCloud(FakeCloud):
            def __init__(self, receipt_file):
                super().__init__()
                self.receipt_file = receipt_file
            def create(self, payload):
                # Simulate external acceptance surviving abrupt adapter death.
                with self.receipt_file.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(payload) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os._exit(23)
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, self.binaries()):
            state_dir = Path(directory)
            remote_receipt = state_dir / "remote-fixture.jsonl"
            client = CrashingCloud(remote_receipt)
            original = submit_job(JobStore(state_dir), client, "crashed-job", SPEC)
            self.assertFalse(original["ok"])
            self.assertEqual(original["job"]["state"], "launch_unknown")
            with self.assertRaises(CloudError):
                submit_job(JobStore(state_dir), client, "crashed-job", SPEC)
            self.assertEqual(len(remote_receipt.read_text().splitlines()), 1)
            store = JobStore(state_dir)
            remote = FakeCloud()
            remote.agent_id = json.loads(remote_receipt.read_text())["agentId"]
            service = DurableJobService(store, remote)
            service.attach("crashed-job", remote.run_id, evidence={"ticket": "matched-remote-acceptance"})
            service.resolve_submission("crashed-job", evidence={"ticket": "matched-remote-acceptance"})
            resumed = submit_job(store, client, "crashed-job", SPEC, resume_dir=original["run_dir"])
            self.assertTrue(resumed["ok"], resumed)
            self.assertTrue(resumed["submission"]["submission_only"])
            self.assertEqual(len(remote_receipt.read_text().splitlines()), 1)
            with sqlite3.connect(state_dir / "deployment.sqlite") as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM calls").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
