"""Durable ingress contract and real Rust-runner recovery regressions."""
from dataclasses import replace
import json
import multiprocessing
from pathlib import Path
from contextlib import closing
import sqlite3
import tempfile
import time
import unittest
from unittest import mock

from mas_host import Invocation, Outcome, RunnerError
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.operations import OperationsError, backup_state, restore_state
from mas_host.service_contracts import ServiceError, TaskService
from mas_host.task_service import DurableTaskService, ExecutionResult, PAGE_BYTES, RunnerExecutor, Workflow
from mas_host.task_store import SERVICE, canonical, event
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


def cancel_when_marker(service, task_id, marker):
    deadline = time.monotonic() + 10
    while not Path(marker).exists() and time.monotonic() < deadline:
        time.sleep(.01)
    service.cancel("alice", task_id)


class TaskServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.workflow = Workflow("echo/1", topology(),
            {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")},
            "a", "b", "echo")

    def service(self, *, workflow=None, **options):
        return DurableTaskService(self.state, {"echo": workflow or self.workflow},
            {"alice": {"echo"}, "bob": {"echo"}}, mas_bin=self.mas,
            gateway_bin=self.gateway, supervised=False, **options)

    def execution(self, service, task_id):
        with service.store.transaction(readonly=True) as db:
            return dict(db.execute("SELECT * FROM executions WHERE task_id=? ORDER BY revision DESC", (task_id,)).fetchone())

    def test_protocol_and_principal_scoped_idempotency(self):
        service = self.service()
        self.assertIsInstance(service, TaskService)
        first = service.submit("alice", "request", "echo", {"a": 1, "b": 2})
        self.assertEqual(service.submit("alice", "request", "echo", {"b": 2, "a": 1}), first)
        with self.assertRaisesRegex(ServiceError, "request conflict"):
            service.submit("alice", "request", "echo", {"a": 2})
        other = service.submit("bob", "request", "echo", {"a": 1, "b": 2})
        self.assertNotEqual(other["task_id"], first["task_id"])
        self.assertEqual(len(service.list_tasks("alice")), 1)
        for operation in (service.inspect, service.result, service.cancel, service.events):
            with self.assertRaisesRegex(ServiceError, "task not found"):
                operation("bob", first["task_id"])

    def test_input_bounds_and_grants_precede_admission(self):
        service = self.service()
        for value in ({"float": .5}, {"big": "x" * 70000}, []):
            with self.assertRaises(ServiceError):
                service.submit("alice", "bad", "echo", value)
        for principal, workflow in (("unknown", "echo"), ("alice", "secret")):
            with self.assertRaisesRegex(ServiceError, "forbidden"):
                service.submit(principal, "bad", workflow, {})
        self.assertEqual(service.operations.health()["usage"]["runs"], 0)

    def test_fifo_real_runner_results_and_durable_events(self):
        service = self.service()
        first = service.submit("alice", "first", "echo", {"answer": 1})
        second = service.submit("alice", "second", "echo", {"answer": 2})
        self.assertEqual(service.run_once()["task_id"], first["task_id"])
        self.assertEqual(service.run_once()["task_id"], second["task_id"])
        self.assertIsNone(service.run_once())
        restored = self.service()
        self.assertEqual(restored.result("alice", first["task_id"]), {"answer": 1})
        events = restored.events("alice", first["task_id"])
        self.assertEqual([item["event"] for item in events], ["submitted", "running", "protocol", "protocol", "state_changed"])
        self.assertEqual(restored.events("alice", first["task_id"], events[-1]["id"]), [])
        self.assertEqual(restored.cancel("alice", first["task_id"])["state"], "completed")
        self.assertEqual(service.operations.health()["usage"]["runs"], 2)

    def test_worker_exclusivity(self):
        service = self.service()
        service.submit("alice", "first", "echo", {})
        original = service.executor.execute
        def execute(*args, **kwargs):
            with self.assertRaisesRegex(ServiceError, "worker busy"):
                service.run_once()
            return original(*args, **kwargs)
        with mock.patch.object(service.executor, "execute", side_effect=execute):
            self.assertEqual(service.run_once()["state"], "completed")

    def test_queued_cancellation_never_admits_run(self):
        service = self.service()
        task = service.submit("alice", "cancel", "echo", {})
        receipt = service.cancel("alice", task["task_id"])
        self.assertEqual(receipt["state"], "cancelled")
        self.assertTrue(receipt["cancel_requested"])
        self.assertIsNone(service.run_once())
        self.assertEqual(service.operations.health()["usage"]["runs"], 0)

    def test_cancellation_between_execution_and_commit_closes_paused_work(self):
        for interrupted in ("input_required", "auth_required"):
            with self.subTest(interrupted=interrupted):
                self.state = self.root / interrupted
                workflow = replace(self.workflow,
                    classify=lambda _outcome: interrupted,
                    continue_input=lambda previous, supplied: previous | supplied)
                service = self.service(workflow=workflow, max_pending=1)
                task = service.submit("alice", interrupted, "echo", {"remote_receipt": "retained"})
                execute = service.executor.execute

                def cancel_after_execution(*args, **kwargs):
                    outcome = execute(*args, **kwargs)
                    self.assertEqual(outcome.state, interrupted)
                    pending = service.cancel("alice", task["task_id"])
                    self.assertEqual(pending["state"], "running")
                    self.assertTrue(pending["cancel_requested"])
                    return outcome

                with mock.patch.object(service.executor, "execute", side_effect=cancel_after_execution):
                    receipt = service.run_once()
                self.assertEqual(receipt["state"], "cancelled")
                self.assertEqual(receipt["result"], {"remote_receipt": "retained"})
                self.assertTrue(receipt["cancel_requested"])
                self.assertEqual(self.execution(service, task["task_id"])["state"], "cancelled")
                events = service.events("alice", task["task_id"])
                self.assertEqual(events[-1]["data"]["state"], "cancelled")
                self.assertEqual(service.cancel("alice", task["task_id"]), receipt)
                self.assertEqual(service.events("alice", task["task_id"]), events)
                self.assertIsNone(service.run_once())
                next_task = service.submit("alice", "next", "echo", {})
                self.assertEqual(next_task["state"], "queued")

    def test_repeated_cancellation_repairs_legacy_cancel_requested_pause(self):
        for interrupted in ("input_required", "auth_required"):
            with self.subTest(interrupted=interrupted):
                self.state = self.root / interrupted
                workflow = replace(self.workflow,
                    classify=lambda _outcome: interrupted,
                    continue_input=lambda previous, supplied: previous | supplied)
                service = self.service(workflow=workflow, max_pending=1)
                task = service.submit("alice", interrupted, "echo", {"remote_receipt": "retained"})
                self.assertEqual(service.run_once()["state"], interrupted)
                # The previous finalisation race persisted this combination.
                with service.store.transaction() as db:
                    db.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (task["task_id"],))
                service = self.service(workflow=workflow, max_pending=1)
                receipt = service.cancel("alice", task["task_id"])
                self.assertEqual(receipt["state"], "cancelled")
                self.assertEqual(receipt["result"], {"remote_receipt": "retained"})
                self.assertEqual(self.execution(service, task["task_id"])["state"], "cancelled")
                events = service.events("alice", task["task_id"])
                self.assertEqual(events[-1]["data"], {"state": "cancelled", "confirmed": True,
                                                      "scope": "local_workflow"})
                self.assertEqual(service.cancel("alice", task["task_id"]), receipt)
                self.assertEqual(service.events("alice", task["task_id"]), events)
                self.assertIsNone(service.run_once())
                self.assertEqual(service.submit("alice", "next", "echo", {})["state"], "queued")

    def test_cancellation_between_unknown_execution_and_commit_preserves_uncertainty(self):
        effects = []

        def provider():
            effects.append(True)
            raise RuntimeError("lost effect reply")

        workflow = replace(self.workflow, adapters=self.workflow.adapters | {
            "b": FunctionAdapter(lambda c, r: Outcome(c.invoke(provider, input={"effect": "one"})))})
        service = self.service(workflow=workflow)
        task = service.submit("alice", "unknown", "echo", {})
        execute = service.executor.execute

        def cancel_after_execution(*args, **kwargs):
            outcome = execute(*args, **kwargs)
            self.assertEqual(outcome.state, "unknown")
            service.cancel("alice", task["task_id"])
            return outcome

        with mock.patch.object(service.executor, "execute", side_effect=cancel_after_execution):
            receipt = service.run_once()
        self.assertEqual(receipt["state"], "unknown")
        self.assertTrue(receipt["cancel_requested"])
        self.assertEqual(self.execution(service, task["task_id"])["state"], "unknown")
        events = service.events("alice", task["task_id"])
        self.assertEqual(service.cancel("alice", task["task_id"]), receipt)
        self.assertEqual(service.events("alice", task["task_id"]), events)
        self.assertIsNone(service.run_once())
        with self.assertRaisesRegex(ServiceError, "reconciliation required"):
            service.recover("alice", task["task_id"])
        self.assertEqual(effects, [True])

    def test_crash_after_effect_checkpoint_replays_same_run_without_second_effect(self):
        effects, crash = [], [True]
        def handle(context, request):
            output = context.invoke(lambda: effects.append(True) or Invocation(request.input, {"receipt": "original"}), input={"provider": "test"})
            if crash:
                crash.pop()
                raise SystemExit("host crashed after effect result committed")
            return Outcome(output)
        workflow = replace(self.workflow, adapters=self.workflow.adapters | {"b": FunctionAdapter(handle)})
        service = self.service(workflow=workflow)
        task = service.submit("alice", "crash", "echo", {"answer": 42})
        execution = self.execution(service, task["task_id"])
        with self.assertRaises(SystemExit):
            service.run_once()
        self.assertEqual(service.inspect("alice", task["task_id"])["state"], "running")
        recovered = self.service(workflow=workflow)
        self.assertEqual(recovered.run_once()["result"], {"answer": 42})
        self.assertEqual(effects, [True])
        self.assertEqual(self.execution(recovered, task["task_id"])["run_id"], execution["run_id"])
        self.assertEqual(recovered.operations.health()["usage"]["runs"], 1)

    def test_unknown_never_resubmits_until_evidenced_resolution(self):
        effects = []
        def provider():
            effects.append(True)
            raise RuntimeError("lost effect reply")
        workflow = replace(self.workflow, adapters=self.workflow.adapters | {
            "b": FunctionAdapter(lambda c, r: Outcome(c.invoke(provider, input={"effect": "one"})))})
        service = self.service(workflow=workflow)
        task = service.submit("alice", "unknown", "echo", {})
        self.assertEqual(service.run_once()["state"], "unknown")
        self.assertIsNone(service.run_once())
        self.assertEqual(service.submit("alice", "unknown", "echo", {})["state"], "unknown")
        with self.assertRaisesRegex(ServiceError, "reconciliation required"):
            service.recover("alice", task["task_id"])
        run = self.state / self.execution(service, task["task_id"])["run_directory"]
        call = inspect_run(run)["invocations"][0]
        resolve_invocation(run, call["call_id"], decision="completed", output={"answer": 7}, evidence={"receipt": "remote-query"})
        service.recover("alice", task["task_id"])
        self.assertEqual(service.run_once()["result"], {"answer": 7})
        self.assertEqual(effects, [True])

    def test_cancellation_after_lost_unknown_receipt_preserves_recovery(self):
        effects = []

        def provider():
            effects.append(True)
            raise RuntimeError('effect reply lost')

        workflow = replace(self.workflow, adapters=self.workflow.adapters | {
            'b': FunctionAdapter(lambda context, request: Outcome(context.invoke(provider, input={})))})
        service = self.service(workflow=workflow)
        task = service.submit('alice', 'lost-unknown-receipt', 'echo', {})
        execute = service.executor.execute

        def lose_receipt(*args, **kwargs):
            self.assertEqual(execute(*args, **kwargs).state, 'unknown')
            raise SystemExit('worker died before service receipt commit')

        with mock.patch.object(service.executor, 'execute', side_effect=lose_receipt):
            with self.assertRaises(SystemExit):
                service.run_once()
        self.assertEqual(service.inspect('alice', task['task_id'])['state'], 'running')
        execution = self.execution(service, task['task_id'])
        directory = self.state / execution['run_directory']
        self.assertTrue(inspect_run(directory)['recovery_required'])
        service.cancel('alice', task['task_id'])
        stopped = service.run_once()
        self.assertEqual(stopped['state'], 'unknown')
        self.assertTrue(stopped['cancel_requested'])
        self.assertTrue(inspect_run(directory)['recovery_required'])
        with self.assertRaisesRegex(ServiceError, 'reconciliation required'):
            service.recover('alice', task['task_id'])

        call = inspect_run(directory)['invocations'][0]
        resolve_invocation(directory, call['call_id'], decision='completed', output={'answer': 7},
                           evidence={'receipt': 'independent confirmation'})
        service.recover('alice', task['task_id'])
        self.assertEqual(service.run_once()['state'], 'cancelled')
        self.assertEqual(self.execution(service, task['task_id'])['run_id'], execution['run_id'])
        self.assertFalse(inspect_run(directory)['recovery_required'])
        self.assertEqual(effects, [True])
        self.assertEqual(service.operations.health()['usage']['calls'], 1)

    def test_executor_cancellation_before_bootstrap_never_creates_or_admits_run(self):
        service = self.service()
        task = service.submit('alice', 'never-started', 'echo', {})
        execution = self.execution(service, task['task_id'])
        result = service.executor.execute(self.workflow, execution, cancelled=lambda: True,
                                          ownership_descriptor=0)
        self.assertEqual(result.state, 'cancelled')
        self.assertFalse((self.state / execution['run_directory']).exists())
        self.assertEqual(service.operations.health()['usage']['runs'], 0)

    def test_executor_implicit_ledger_cancellation_is_readonly(self):
        missing = self.root / 'never-created'
        executor = RunnerExecutor(missing, operations=None)
        result = executor.execute(self.workflow, {'run_directory': 'run-never', 'run_id': 'run-never'},
                                  cancelled=lambda: True, ownership_descriptor=0)
        self.assertEqual(result.state, 'cancelled')
        self.assertFalse(missing.exists())

    def test_stop_during_runner_construction_preserves_unadmitted_identity(self):
        service = self.service()
        task = service.submit('alice', 'bootstrap-stop', 'echo', {})
        execution = {**self.execution(service, task['task_id']), 'principal': 'alice', 'workflow': 'echo'}
        stopped = False

        def close_admission(*args, **kwargs):
            nonlocal stopped
            stopped = True
            raise OperationsError('workflow admission closed')

        with mock.patch.object(service.operations, 'admit_run', side_effect=close_admission):
            result = service.executor.execute(self.workflow, execution, cancelled=lambda: stopped,
                                              ownership_descriptor=0)
        self.assertEqual(result.state, 'cancelled')
        directory = self.state / execution['run_directory']
        self.assertEqual(json.loads((directory / 'bootstrap.json').read_text())['run_id'], execution['run_id'])
        self.assertFalse((directory / 'host.sqlite').exists())
        self.assertEqual(service.operations.health()['usage']['runs'], 0)
        self.assertEqual(service.operations.health()['usage']['calls'], 0)

    def test_stop_during_runner_construction_preserves_original_unknown_call(self):
        effects = []

        def provider():
            effects.append(True)
            raise RuntimeError('effect reply lost')

        workflow = replace(self.workflow, adapters=self.workflow.adapters | {
            'b': FunctionAdapter(lambda c, r: Outcome(c.invoke(provider, input={})))})
        service = self.service(workflow=workflow)
        task = service.submit('alice', 'bootstrap-unknown', 'echo', {})
        self.assertEqual(service.run_once()['state'], 'unknown')
        execution = {**self.execution(service, task['task_id']), 'principal': 'alice', 'workflow': 'echo'}
        directory = self.state / execution['run_directory']
        original = inspect_run(directory)
        stopped = False

        def close_admission(*args, **kwargs):
            nonlocal stopped
            stopped = True
            raise OperationsError('workflow admission closed')

        with mock.patch.object(service.operations, 'admit_run', side_effect=close_admission):
            result = service.executor.execute(workflow, execution, cancelled=lambda: stopped,
                                              ownership_descriptor=0)
        self.assertEqual(result.state, 'unknown')
        self.assertEqual(result.error, {'code': 'reconciliation_required'})
        retained = inspect_run(directory)
        self.assertTrue(retained['recovery_required'])
        self.assertEqual(retained['run_id'], original['run_id'])
        self.assertEqual(retained['invocations'], original['invocations'])
        self.assertEqual(service.operations.health()['usage']['calls'], 1)
        self.assertEqual(effects, [True])

    def test_runner_construction_error_without_stop_still_propagates(self):
        service = self.service()
        task = service.submit('alice', 'bootstrap-error', 'echo', {})
        execution = {**self.execution(service, task['task_id']), 'principal': 'alice', 'workflow': 'echo'}
        with mock.patch.object(service.operations, 'admit_run', side_effect=OperationsError('unavailable')):
            with self.assertRaises(RunnerError):
                service.executor.execute(self.workflow, execution, cancelled=lambda: False,
                                         ownership_descriptor=0)

    def test_continuation_is_opt_in_and_has_own_run_with_idempotent_update(self):
        workflow = replace(self.workflow,
            classify=lambda outcome: "completed" if "answer" in outcome.output else "input_required",
            continue_input=lambda previous, supplied: previous | supplied)
        service = self.service(workflow=workflow)
        task = service.submit("alice", "question", "echo", {"question": "amount?"})
        self.assertEqual(service.run_once()["state"], "input_required")
        update = service.update("alice", task["task_id"], "response", {"answer": 8})
        self.assertEqual(update["revision"], 1)
        self.assertEqual(service.update("alice", task["task_id"], "response", {"answer": 8}), update)
        self.assertEqual(service.run_once()["result"], {"question": "amount?", "answer": 8})
        with self.assertRaisesRegex(ServiceError, "invalid task state"):
            service.update("alice", task["task_id"], "late", {"answer": 9})
        self.assertEqual(service.operations.health()["usage"]["runs"], 2)
        other_state = self.root / "other"
        unsupported = DurableTaskService(other_state, {"echo": self.workflow}, {"alice": {"echo"}})
        receipt = unsupported.submit("alice", "question", "echo", {})
        with self.assertRaisesRegex(ServiceError, "unsupported capability"):
            unsupported.update("alice", receipt["task_id"], "response", {})

    def test_version_upgrade_preserves_history_and_mutation_blocks_dispatch(self):
        service = self.service()
        task = service.submit("alice", "first", "echo", {})
        upgraded = self.service(workflow=replace(self.workflow, version="echo/2"))
        self.assertEqual(upgraded.inspect("alice", task["task_id"])["state"], "queued")
        self.workflow.topology["max_calls"] += 1
        self.assertEqual(service.run_once()["state"], "blocked")
        self.assertEqual(service.inspect("alice", task["task_id"])["state"], "blocked")
        self.assertEqual(service.operations.health()["usage"]["runs"], 0)

    def test_future_schema_is_rejected_without_writing(self):
        service = self.service()
        with closing(sqlite3.connect(service.store.path)) as db, db:
            db.execute("PRAGMA user_version=999")
        before = service.store.path.read_bytes()
        with self.assertRaisesRegex(ServiceError, "unsupported"):
            self.service()
        self.assertEqual(service.store.path.read_bytes(), before)

    def test_queue_limit_and_duplicate_do_not_consume_extra_capacity(self):
        service = self.service(max_pending=1)
        first = service.submit("alice", "first", "echo", {})
        self.assertEqual(service.submit("alice", "first", "echo", {}), first)
        with self.assertRaisesRegex(ServiceError, "queue full"):
            service.submit("alice", "second", "echo", {})
        service.run_once()
        self.assertEqual(service.submit("alice", "second", "echo", {})["state"], "queued")

    def test_audit_restore_permits_reads_and_blocks_all_mutations(self):
        service = self.service()
        task = service.submit("alice", "first", "echo", {"answer": 5})
        service.run_once()
        bundle, restored = self.root / "backup", self.root / "restored"
        backup_state(self.state, bundle)
        restore_state(bundle, restored)
        restored_service = DurableTaskService(restored, {"echo": self.workflow}, {"alice": {"echo"}})
        self.assertEqual(restored_service.result("alice", task["task_id"]), {"answer": 5})
        for operation in (
            lambda: restored_service.submit("alice", "new", "echo", {}),
            lambda: restored_service.cancel("alice", task["task_id"]),
            lambda: restored_service.update("alice", task["task_id"], "new", {}),
            lambda: restored_service.recover("alice", task["task_id"]),
            restored_service.run_once):
            with self.assertRaisesRegex(ServiceError, "audit only"):
                operation()

    def test_bootstrap_crash_before_runner_initialisation_keeps_same_identity(self):
        service = self.service()
        task = service.submit("alice", "crash", "echo", {})
        from mas_host.runner import Runner
        with mock.patch.object(Runner, "resume", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                service.run_once()
        original = self.execution(service, task["task_id"])
        self.assertEqual(service.run_once()["state"], "completed")
        self.assertEqual(self.execution(service, task["task_id"])["run_id"], original["run_id"])
        self.assertEqual(service.operations.health()["usage"]["runs"], 1)

    def test_metadata_binding_and_scoped_keyset_pages(self):
        service = self.service()
        first = service.submit("alice", "first", "echo", {}, metadata={"context_id": "conversation"})
        service.submit("alice", "other-context", "echo", {}, metadata={"context_id": "other"})
        service.submit("bob", "other-principal", "echo", {}, metadata={"context_id": "conversation"})
        second = service.submit("alice", "second", "echo", {}, metadata={"context_id": "conversation"})
        with self.assertRaisesRegex(ServiceError, "request conflict"):
            service.submit("alice", "first", "echo", {}, metadata={"context_id": "changed"})
        page = service.list_task_page("alice", page_size=1, metadata={"context_id": "conversation"})
        self.assertEqual(page["total_size"], 2)
        next_page = service.list_task_page("alice", page_size=1, metadata={"context_id": "conversation"}, after=page["next"])
        self.assertIsNone(next_page["next"])
        self.assertEqual({page["tasks"][0]["task_id"], next_page["tasks"][0]["task_id"]}, {first["task_id"], second["task_id"]})
        self.assertEqual(service.list_task_page("alice", state="completed")["total_size"], 0)

    def test_task_pages_bound_bytes_and_preserve_every_cursor(self):
        service = self.service()
        expected = {service.submit("alice", str(index), "echo", {},
                                   metadata={"large": "\N{SNOWMAN}" * 20000})["task_id"]
                    for index in range(10)}
        seen, after, pages = [], None, []
        while True:
            page = service.list_task_page("alice", page_size=100, after=after)
            self.assertLessEqual(len(canonical(page).encode("utf-8")), PAGE_BYTES)
            self.assertEqual(page["total_size"], len(expected))
            seen.extend(task["task_id"] for task in page["tasks"])
            pages.append(page)
            if page["next"] is None:
                break
            self.assertTrue(page["tasks"])
            after = page["next"]
        self.assertGreater(len(pages), 1)
        self.assertEqual(set(seen), expected)
        self.assertEqual(len(seen), len(expected))
        listed = []
        while True:
            batch = service.list_tasks("alice", limit=100, offset=len(listed))
            self.assertLessEqual(len(canonical(batch).encode("utf-8")), PAGE_BYTES)
            if not batch:
                break
            listed.extend(task["task_id"] for task in batch)
        self.assertEqual(set(listed), expected)
        self.assertEqual(len(listed), len(expected))

    def test_task_page_bounds_filter_count_before_sql_expression_limit(self):
        service = self.service()
        filters = {f"field_{index}": "value" for index in range(1000)}
        with self.assertRaises(ServiceError) as raised:
            service.list_task_page("alice", metadata=filters)
        self.assertEqual(raised.exception.code, "invalid_metadata_filter")
        self.assertEqual(service.list_task_page("alice", metadata={
            f"field_{index}": "value" for index in range(64)})["tasks"], [])

    def test_event_pages_bound_bytes_without_skipping_events(self):
        service = self.service()
        task = service.submit("alice", "first", "echo", {})
        with service.store.transaction() as db:
            for index in range(10):
                event(db, task["task_id"], "protocol", {"index": index, "value": "x" * 60000})
        observed, after, pages = [], 0, 0
        while True:
            batch = service.events("alice", task["task_id"], after=after)
            self.assertLessEqual(len(canonical(batch).encode("utf-8")), PAGE_BYTES)
            if not batch:
                break
            pages += 1
            observed.extend(item["data"]["index"] for item in batch if item["event"] == "protocol")
            after = batch[-1]["id"]
        self.assertGreater(pages, 1)
        self.assertEqual(observed, list(range(10)))

    def test_default_unresolved_is_business_failure_not_request_for_input(self):
        workflow = replace(self.workflow, adapters=self.workflow.adapters | {
            "b": FunctionAdapter(lambda c, r: Outcome({"reason": "inconsistent_totals"}, "unresolved"))})
        service = self.service(workflow=workflow)
        service.submit("alice", "first", "echo", {})
        receipt = service.run_once()
        self.assertEqual(receipt["state"], "failed")
        self.assertEqual(receipt["error"], {"code": "application_unresolved"})

    def test_revoked_grant_prevents_admission_of_queued_work(self):
        service = self.service()
        service.submit("alice", "first", "echo", {})
        worker = DurableTaskService(self.state, {"echo": self.workflow}, {"alice": set()},
            mas_bin=self.mas, gateway_bin=self.gateway, supervised=False)
        self.assertEqual(worker.run_once()["error"], {"code": "permission_revoked"})
        self.assertEqual(service.operations.health()["usage"]["runs"], 0)

    def test_repeated_worker_crashes_do_not_append_duplicate_running_events(self):
        service = self.service()
        receipt = service.submit("alice", "first", "echo", {})
        with mock.patch.object(service.executor, "execute", side_effect=SystemExit):
            for _ in range(5):
                with self.assertRaises(SystemExit):
                    service.run_once()
        self.assertEqual([item["event"] for item in service.events("alice", receipt["task_id"])], ["submitted", "running"])
        self.assertEqual(service.run_once()["state"], "completed")

    def test_pending_bootstrap_file_after_crash_is_recoverable(self):
        service = self.service()
        task = service.submit("alice", "first", "echo", {})
        execution = self.execution(service, task["task_id"])
        path = self.state / execution["run_directory"]
        path.mkdir(mode=0o700)
        (path / ".bootstrap.json.pending").write_text("partial intent")
        self.assertEqual(service.run_once()["state"], "completed")
        self.assertEqual(service.operations.health()["usage"]["runs"], 1)

    def test_crash_after_terminal_before_service_commit_replays_without_effect(self):
        service = self.service()
        task = service.submit("alice", "first", "echo", {"answer": 12})
        execute = service.executor.execute
        def crash_after_result(*args, **kwargs):
            execute(*args, **kwargs)
            raise SystemExit
        with mock.patch.object(service.executor, "execute", side_effect=crash_after_result):
            with self.assertRaises(SystemExit):
                service.run_once()
        self.assertEqual(service.run_once()["result"], {"answer": 12})
        self.assertEqual(service.operations.health()["usage"]["runs"], 1)
        self.assertEqual([e["event"] for e in service.events("alice", task["task_id"])].count("running"), 1)

    def test_existing_manifest_must_match_assigned_run_identity(self):
        service = self.service()
        task = service.submit("alice", "first", "echo", {})
        from mas_host.runner import Runner
        with mock.patch.object(Runner, "resume", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                service.run_once()
        execution = self.execution(service, task["task_id"])
        manifest = self.state / execution["run_directory"] / "bootstrap.json"
        value = json.loads(manifest.read_text())
        value["run_id"] = "run-other"
        manifest.write_text(json.dumps(value))
        with self.assertRaisesRegex(ServiceError, "run assignment conflict"):
            service.run_once()
        self.assertEqual(service.operations.health()["usage"]["runs"], 0)

    def test_supervised_cancellation_is_confirmed_only_without_uncertain_effect(self):
        for external in (False, True):
            with self.subTest(external=external):
                state = self.root / str(external)
                marker = self.root / (str(external) + "-started")
                def wait():
                    marker.write_text("started")
                    time.sleep(10)
                    return Invocation({"answer": 9}, {})
                def handle(context, request):
                    if external:
                        return Outcome(context.invoke(wait, input={"effect": "slow"}))
                    wait()
                    return Outcome({})
                workflow = replace(self.workflow, adapters=self.workflow.adapters | {"b": FunctionAdapter(handle)})
                service = DurableTaskService(state, {"echo": workflow}, {"alice": {"echo"}},
                    mas_bin=self.mas, gateway_bin=self.gateway, supervised=True)
                task = service.submit("alice", "cancel", "echo", {})
                process = multiprocessing.get_context("fork").Process(target=cancel_when_marker, args=(service, task["task_id"], str(marker)))
                process.start()
                try:
                    receipt = service.run_once()
                finally:
                    process.join(12)
                    if process.is_alive():
                        process.kill()
                        process.join()
                self.assertEqual(process.exitcode, 0)
                self.assertEqual(receipt["state"], "unknown" if external else "cancelled")
                self.assertTrue(receipt["cancel_requested"])
                self.assertIsNone(service.run_once())


if __name__ == "__main__":
    unittest.main()
