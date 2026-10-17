"""Independent fault review of service identity and cancellation boundaries."""
from dataclasses import replace
import contextlib
import hashlib
import io
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.bridge_cli import main
from mas_host.bridge_config import load_config
from mas_host.bridges.remote import RemoteTaskStore, digest
from mas_host.journal import canonical, inspect_run
from mas_host.operations import prune_runs
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


class BridgeIndependentReviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state = Path(self.temporary.name) / "state"
        self.workflow = Workflow("review/1", topology(),
            {actor: FunctionAdapter(lambda c, r: Outcome(r.input)) for actor in ("a", "b", "c")},
            "a", "b", "echo")

    def service(self, workflow=None, grants=None):
        return DurableTaskService(self.state, {"echo": workflow or self.workflow},
            grants if grants is not None else {"alice": {"echo"}},
            mas_bin=self.mas, gateway_bin=self.gateway, supervised=False)

    @staticmethod
    def execution(service, task_id):
        with service.store.transaction(readonly=True) as db:
            return dict(db.execute("SELECT * FROM executions WHERE task_id=? ORDER BY revision DESC", (task_id,)).fetchone())

    def test_revocation_after_effect_crash_preserves_unknown_without_redispatch(self):
        effects = []
        def external_effect():
            effects.append("dispatched")
            raise SystemExit("worker died after dispatch")
        workflow = replace(self.workflow, adapters=self.workflow.adapters | {
            "b": FunctionAdapter(lambda c, r: Outcome(c.invoke(external_effect, input={"effect": "once"})))})
        service = self.service(workflow)
        task = service.submit("alice", "request", "echo", {})
        with self.assertRaises(SystemExit):
            service.run_once()
        execution = self.execution(service, task["task_id"])
        self.assertTrue(inspect_run(self.state / execution["run_directory"])["recovery_required"])
        revoked = self.service(workflow, grants={"alice": set()})
        receipt = revoked.run_once()
        self.assertEqual(receipt["state"], "unknown")
        self.assertEqual(receipt["error"]["code"], "permission_revoked_reconciliation_required")
        self.assertIsNone(revoked.run_once())
        self.assertEqual(effects, ["dispatched"])
        self.assertFalse(prune_runs(self.state, 0, apply=True)["runs"][0]["eligible"])

    def test_paused_cancellation_closes_local_continuation_and_retains_remote_work(self):
        for paused in ("input_required", "auth_required"):
            with self.subTest(paused=paused):
                self.state = Path(self.temporary.name) / paused
                workflow = replace(self.workflow, classify=lambda outcome: paused,
                                   continue_input=lambda previous, supplied: previous | supplied)
                service = self.service(workflow)
                task = service.submit("alice", "request", "echo", {"question": "continue?"})
                self.assertEqual(service.run_once()["state"], paused)
                execution = self.execution(service, task["task_id"])
                store = RemoteTaskStore(self.state)
                descriptor, payload = {"provider": "review-peer"}, {"action": "work"}
                remote_key = hashlib.sha256(b"independent-review\x00remote").hexdigest()
                with store.open() as db, db:
                    db.execute("INSERT INTO remote_tasks(key,logical_key,namespace,binding,descriptor,input,state,remote_id,host_run_dir,call_id,created,updated) VALUES(?,'remote','independent-review',?,?,?,'running','remote-id',?,'call',0,0)",
                        (remote_key, digest({"descriptor": descriptor, "input": payload}), canonical(descriptor),
                         canonical(payload), execution["run_directory"]))
                before = store.status(remote_key)
                receipt = service.cancel("alice", task["task_id"])
                self.assertEqual(receipt["state"], "cancelled")
                self.assertTrue(receipt["cancel_requested"])
                self.assertIsNone(service.run_once())
                with self.assertRaisesRegex(ServiceError, "invalid task state"):
                    service.update("alice", task["task_id"], "continue", {"answer": 1})
                self.assertEqual(store.status(remote_key), before)
                self.assertFalse(prune_runs(self.state, 0, apply=True)["runs"][0]["eligible"])
                self.assertEqual(service.events("alice", task["task_id"])[-1]["data"],
                                 {"confirmed": True, "scope": "local_workflow", "state": "cancelled"})

    def test_auth_creation_rejects_unusable_principal_before_writing(self):
        token, verifier = (Path(self.temporary.name) / name for name in ("token", "verifier"))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = main(["init-auth", "--principal", "alice smith", "--token-file", str(token),
                           "--verifier-file", str(verifier)])
        self.assertEqual(result, 1)
        self.assertFalse(token.exists())
        self.assertFalse(verifier.exists())

    def test_configuration_schema_version_is_integer_not_boolean(self):
        path = Path(self.temporary.name) / "config.json"
        path.write_text('{"version":true,"grants":{"alice":["echo"]}}')
        with self.assertRaises(ServiceError):
            load_config(path)


if __name__ == "__main__":
    unittest.main()
