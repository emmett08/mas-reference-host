"""Real Rust runs under process failure, shared admission and bounded recovery.

No provider network calls occur. The durable effect file is an independent
receipt: its append precedes a deliberately withheld invocation response.
"""
from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import time
import unittest

from mas_host.contracts import Invocation, Outcome
from mas_host.journal import inspect_run, resolve_invocation
from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError, backup_state
from mas_host.service_contracts import ServiceError
from mas_host.task_service import Workflow
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan
import test_runner


class _Effect:
    def __init__(self, receipts: Path, release: Path | None = None):
        self.receipts, self.release = receipts, release

    def handle(self, context, request):
        def effect():
            row = {"pid": os.getpid(), "call_id": context.invocation_id,
                   "answer": 42, "receipt": "fixture-observed"}
            with self.receipts.open("ab", buffering=0) as stream:
                stream.write((json.dumps(row) + "\n").encode())
                os.fsync(stream.fileno())
            stop = time.monotonic() + 20
            while self.release is not None and not self.release.exists():
                if time.monotonic() >= stop:
                    raise RuntimeError("test release deadline")
                time.sleep(0.01)
            return Invocation({"answer": 42}, {"receipt": "fixture-observed"})
        return Outcome(context.invoke(effect, input={"operation": "fixture-effect"}, reservation_microusd=3))


class _Echo:
    def handle(self, context, request):
        return Outcome(request.input)


class _LoseCompletionOnce(WorkflowEngine):
    """Drop only the scheduler receipt after Rust has durably completed."""
    def _save_attempt(self, attempt, state, output=None, error=None):
        if state == "completed":
            try:
                descriptor = os.open(self.state_dir / "dropped-completion", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(descriptor)
                return
        return super()._save_attempt(attempt, state, output, error)


class _RecordWorker(WorkflowEngine):
    """Expose the worker's own PID without relying on /proc namespace layout."""
    def _worker(self, *args):
        (self.state_dir / 'worker-pid').write_text(str(os.getpid()))
        return super()._worker(*args)


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux workflow scheduler")
class WorkflowRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_runner.RunnerIntegrationTests.setUpClass()
        cls.mas = test_runner.RunnerIntegrationTests.mas
        cls.gateway = test_runner.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.children = set()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        for pid in list(self.children):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        self.children.clear()

    def engine(self, *, root=None, release=None, parallel=False, wall_seconds=30,
               max_calls=4, max_reserved=12, engine_type=WorkflowEngine, scope_seconds=None):
        root = root or self.root
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        receipts = root / "receipts.jsonl"
        adapter = _Effect(receipts, release)
        topology = test_runner.topology(agents=("client", "worker"), wall_seconds=60)
        workflow = Workflow(version="workflow-recovery/1", topology=topology,
                            adapters={"client": _Echo(), "worker": adapter},
                            sender="client", recipient="worker", operation="effect")
        task = {"kind": "task", "workflow": "effect"}
        node = {"kind": "parallel", "branches": {"a": task, "b": task}} if parallel else task
        plan = WorkflowPlan({"format": "mas-workflow/1", "name": "plan", "version": "1",
                             "limits": {"max_steps": 8, "max_parallel": 2, "wall_seconds": wall_seconds,
                                        "max_calls": max_calls, "max_reserved_microusd": max_reserved},
                             "root": node})
        plans = {'plan': plan}
        if scope_seconds is not None:
            child = plan.spec
            child['name'] = 'child'
            child['limits']['wall_seconds'] = scope_seconds
            parent = plan.spec
            parent['root'] = {'kind': 'subworkflow', 'workflow': 'child'}
            plans = {'plan': WorkflowPlan(parent), 'child': WorkflowPlan(child)}
        state = root / "state"
        operations = DeploymentLedger(state, DeploymentPolicy(min_free_bytes=1, max_state_bytes=100_000_000))
        engine = engine_type(state, plans, {"effect": workflow}, {"owner": {"plan"}},
                             operations=operations, mas_bin=self.mas, gateway_bin=self.gateway)
        return engine, receipts

    def submit(self, engine):
        return engine.submit("owner", "request-1", "plan", {})["instance_id"]

    def receipts(self, path):
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def until(self, predicate, seconds=10, message="condition was not reached"):
        stop = time.monotonic() + seconds
        while time.monotonic() < stop:
            if predicate():
                return
            time.sleep(0.01)
        self.fail(message)

    def spawn(self, engine, instance):
        result = self.root / ("result-" + str(len(self.children)) + ".json")
        pid = os.fork()
        if pid == 0:
            try:
                value = engine.run("owner", instance)
                result.write_text(json.dumps(value))
                os._exit(0)
            except BaseException as error:
                result.write_text(json.dumps({"exception": type(error).__name__, "message": str(error)}))
                os._exit(1)
        self.children.add(pid)
        return pid, result

    def finish(self, pid, result):
        self.until(result.exists, message="scheduler did not produce a bounded result")
        _, status = os.waitpid(pid, 0)
        self.children.discard(pid)
        value = json.loads(result.read_text())
        self.assertEqual(os.waitstatus_to_exitcode(status), 0, value)
        return value

    def wait_unowned(self, engine, instance):
        def unowned():
            try:
                with engine._ownership(instance):
                    return True
            except ServiceError as error:
                if error.code != "workflow_busy":
                    raise
                return False
        self.until(unowned, message="orphaned worker or guardian retained workflow ownership")

    def test_parallel_tasks_execute_concurrently_through_real_rust_runs(self):
        release = self.root / "release"
        engine, receipts = self.engine(release=release, parallel=True)
        instance = self.submit(engine)
        pid, result = self.spawn(engine, instance)
        self.until(lambda: len(self.receipts(receipts)) == 2, message="independent branches did not overlap")
        self.assertEqual(len({row["pid"] for row in self.receipts(receipts)}), 2)
        release.touch()
        finished = self.finish(pid, result)
        self.assertEqual(finished["state"], "completed", finished)
        self.assertEqual(finished["output"], {"branches": {"a": {"answer": 42}, "b": {"answer": 42}}})
        self.assertEqual(finished["usage"]["calls"], 2)
        self.assertEqual(len({attempt["run_directory"] for attempt in finished["attempts"]}), 2)

    def test_competing_scheduler_is_rejected_without_duplicate_effect(self):
        release = self.root / "release"
        engine, receipts = self.engine(release=release)
        instance = self.submit(engine)
        pid, result = self.spawn(engine, instance)
        self.until(lambda: len(self.receipts(receipts)) == 1)
        with self.assertRaisesRegex(ServiceError, "workflow busy"):
            engine.run("owner", instance)
        self.assertEqual(len(self.receipts(receipts)), 1)
        release.touch()
        self.assertEqual(self.finish(pid, result)["state"], "completed")

    def test_backup_is_busy_while_worker_is_active_and_succeeds_after_join(self):
        release = self.root / "release"
        engine, receipts = self.engine(release=release)
        instance = self.submit(engine)
        pid, result = self.spawn(engine, instance)
        self.until(lambda: len(self.receipts(receipts)) == 1)
        bundle = self.root / "backup"
        with self.assertRaisesRegex(OperationsError, "busy"):
            backup_state(engine.state_dir, bundle)
        self.assertFalse(bundle.exists())
        release.touch()
        self.assertEqual(self.finish(pid, result)["state"], "completed")
        self.assertTrue(backup_state(engine.state_dir, bundle)["ok"])
        self.assertTrue((bundle / "composition.sqlite").is_file())

    def test_killed_scheduler_retains_unknown_effect_and_reuses_same_run_after_resolution(self):
        release = self.root / "release"
        engine, receipts = self.engine(release=release)
        instance = self.submit(engine)
        original_deadline = engine.inspect("owner", instance)["deadline"]
        pid, _ = self.spawn(engine, instance)
        self.until(lambda: len(self.receipts(receipts)) == 1)
        assigned = engine.inspect("owner", instance)["attempts"][0]["run_directory"]
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        self.children.discard(pid)
        self.wait_unowned(engine, instance)
        unknown = engine.run("owner", instance)
        self.assertEqual(unknown["state"], "unknown", unknown)
        self.assertEqual(unknown["deadline"], original_deadline)
        self.assertEqual([attempt["run_directory"] for attempt in unknown["attempts"]], [assigned])
        self.assertEqual(len(self.receipts(receipts)), 1)
        with self.assertRaisesRegex(ServiceError, "reconciliation required"):
            engine.recover("owner", instance)
        run = engine.state_dir / assigned
        pending = inspect_run(run)
        self.assertTrue(pending["recovery_required"])
        receipt = self.receipts(receipts)[0]
        resolve_invocation(run, pending["invocations"][0]["call_id"], decision="completed",
                           output={"answer": receipt["answer"]}, evidence={"receipt": receipt["receipt"]})
        engine.recover("owner", instance)
        recovered = engine.run("owner", instance)
        self.assertEqual(recovered["state"], "completed", recovered)
        self.assertEqual(recovered["output"], {"answer": 42})
        self.assertEqual([attempt["run_directory"] for attempt in recovered["attempts"]], [assigned])
        self.assertEqual(recovered["usage"]["calls"], 1)
        self.assertEqual(len(self.receipts(receipts)), 1)

    def _stopped_after_scheduler_and_worker_loss(self, mode):
        engine, receipts = self.engine(release=self.root / 'release', engine_type=_RecordWorker,
                                       wall_seconds=2 if mode == 'expire' else 30,
                                       scope_seconds=2 if mode == 'scope_expire' else None)
        instance = self.submit(engine)
        pid, _ = self.spawn(engine, instance)
        self.until(lambda: len(self.receipts(receipts)) == 1)
        worker = int((engine.state_dir / 'worker-pid').read_text())
        before = engine.inspect('owner', instance)
        # Both receipt owners die after a real effect, before either can save
        # its composition outcome. Always kill the stopped scheduler, including
        # when an assertion or a signal operation fails during the probe.
        try:
            os.kill(pid, signal.SIGSTOP)
            os.kill(worker, signal.SIGKILL)
        finally:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            self.children.discard(pid)
        self.wait_unowned(engine, instance)
        stale = engine.inspect('owner', instance)
        self.assertEqual(stale['state'], 'running')
        self.assertEqual(stale['attempts'][0]['state'], 'running')
        directory = engine.state_dir / stale['attempts'][0]['run_directory']
        self.assertTrue(inspect_run(directory)['recovery_required'])
        if mode == 'cancel':
            self.assertTrue(engine.cancel('owner', instance)['cancel_requested'])
        else:
            deadline = (engine.store.scopes(instance, 'root/sub-child')[0]['deadline']
                        if mode == 'scope_expire' else before['deadline'])
            self.until(lambda: time.time() >= deadline, seconds=3)

        unknown = engine.run('owner', instance)
        self.assertEqual(unknown['state'], 'unknown', unknown)
        self.assertEqual(unknown['attempts'][0]['state'], 'unknown')
        if mode != 'scope_expire':
            self.assertEqual(unknown['nodes'][0]['state'], 'unknown')
        self.assertEqual(unknown['attempts'][0]['run_directory'], before['attempts'][0]['run_directory'])
        self.assertEqual(unknown['deadline'], before['deadline'])
        self.assertEqual(unknown['usage']['calls'], 1)
        self.assertTrue(inspect_run(directory)['recovery_required'])
        with self.assertRaisesRegex(ServiceError, 'reconciliation required'):
            engine.recover('owner', instance)

        # Evidenced reconciliation remains available despite stopped admission.
        call = inspect_run(directory)['invocations'][0]
        resolve_invocation(directory, call['call_id'], decision='completed', output={'answer': 42},
                           evidence={'receipt': self.receipts(receipts)[0]['receipt']})
        self.assertEqual(engine.recover('owner', instance)['state'], 'queued')
        final = engine.run('owner', instance)
        self.assertEqual(final['state'], 'cancelled' if mode == 'cancel' else 'expired')
        self.assertEqual(final['cancel_requested'], mode == 'cancel')
        self.assertEqual(final['deadline'], before['deadline'])
        self.assertEqual(final['attempts'][0]['run_directory'], before['attempts'][0]['run_directory'])
        self.assertEqual(final['usage']['calls'], 1)
        self.assertFalse(inspect_run(directory)['recovery_required'])
        self.assertEqual(len(self.receipts(receipts)), 1, 'recovery repeated an external effect')

    def test_scheduler_and_worker_loss_preserves_uncertainty_before_cancel(self):
        self._stopped_after_scheduler_and_worker_loss('cancel')

    def test_scheduler_and_worker_loss_preserves_uncertainty_before_expiry(self):
        self._stopped_after_scheduler_and_worker_loss('expire')

    def test_expired_subworkflow_still_exposes_uncertain_stale_attempt(self):
        self._stopped_after_scheduler_and_worker_loss('scope_expire')

    def test_lost_completion_recovers_cached_rust_result_without_new_run_or_effect(self):
        engine, receipts = self.engine(engine_type=_LoseCompletionOnce)
        instance = self.submit(engine)
        unknown = engine.run("owner", instance)
        self.assertEqual(unknown["state"], "unknown", unknown)
        self.assertEqual(len(self.receipts(receipts)), 1)
        assigned = unknown["attempts"][0]["run_directory"]
        self.assertFalse(inspect_run(engine.state_dir / assigned)["recovery_required"])
        engine.recover("owner", instance)
        complete = engine.run("owner", instance)
        self.assertEqual(complete["state"], "completed", complete)
        self.assertEqual(complete["output"], {"answer": 42})
        self.assertEqual([attempt["run_directory"] for attempt in complete["attempts"]], [assigned])
        self.assertEqual(len(self.receipts(receipts)), 1)
        self.assertEqual(complete["usage"]["calls"], 1)

    def test_queued_delay_cannot_refresh_absolute_deadline(self):
        engine, receipts = self.engine(wall_seconds=1)
        instance = self.submit(engine)
        deadline = engine.inspect("owner", instance)["deadline"]
        self.until(lambda: time.time() >= deadline, seconds=2)
        expired = engine.run("owner", instance)
        self.assertEqual(expired["state"], "expired", expired)
        self.assertEqual(expired["deadline"], deadline)
        self.assertEqual(expired["usage"]["calls"], 0)
        self.assertEqual(expired["attempts"], [])
        self.assertEqual(self.receipts(receipts), [])
        self.assertEqual(engine.run("owner", instance)["deadline"], deadline)

    def test_parallel_branches_cannot_overspend_shared_call_or_money_allocation(self):
        for cap, max_calls, max_reserved in (("calls", 1, 6), ("money", 2, 3)):
            with self.subTest(cap=cap):
                engine, receipts = self.engine(root=self.root / cap, parallel=True,
                                               max_calls=max_calls, max_reserved=max_reserved)
                instance = self.submit(engine)
                result = engine.run("owner", instance)
                self.assertEqual(result["state"], "failed", result)
                self.assertEqual(result["usage"]["calls"], 1)
                self.assertEqual(result["usage"]["reserved_microusd"], 3)
                self.assertEqual(len(self.receipts(receipts)), 1)
                with closing(sqlite3.connect(engine.state_dir / "deployment.sqlite")) as db:
                    self.assertEqual(db.execute("SELECT count(*), sum(reserved_microusd) FROM calls").fetchone(), (1, 3))
