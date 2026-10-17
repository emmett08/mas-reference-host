"""Live registrations remain manageable when provider execution is unavailable.

Only the leaf executor is substituted while preparing durable fixtures. The real
scheduler stores the live workflow identities and task results; each management
command subsequently imports the documented factory and opens that state again.
No model, provider runtime, Rust process or network request is executed.
"""
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from mas_host.operations import backup_state, restore_state
from mas_host.structured_examples import example_registry
from mas_host.task_service import ExecutionResult
from mas_host.workflow_cli import main
from mas_host.workflow_engine import WorkflowEngine


FACTORY = "mas_host.structured_examples:example_registry"
PROVIDERS = ("openai", "cursor")


def snapshot(directory):
    return {str(path.relative_to(directory)):
            (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
            for path in directory.rglob("*") if path.is_file()}


class ProviderManagementCliTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def configure(self, provider):
        source = Path(__file__).resolve().parents[2] / "examples/workflows/structured-agents.json"
        settings = json.loads(source.read_text())
        for actor, item in settings.items():
            item.update(provider=provider, model="fixture-" + actor,
                        reservation_microusd=100)
        config = self.directory / (provider + "-agents.json")
        config.write_text(json.dumps(settings))
        return settings, config, self.directory / (provider + "-state")

    @contextmanager
    def unavailable_execution(self, config):
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ,
                {"MAS_STRUCTURED_CONFIG_FILE": str(config)}, clear=True))
            stack.enter_context(patch.dict(sys.modules, {"openai": None, "cursor_sdk": None}))
            secrets = stack.enter_context(patch("mas_host.provider_credentials.read_api_key",
                side_effect=AssertionError("management read provider credentials")))
            runtime = stack.enter_context(patch("mas_host.cursor_session.check_runtime",
                side_effect=AssertionError("management inspected the Cursor runtime")))
            processes = stack.enter_context(patch("subprocess.Popen",
                side_effect=AssertionError("management launched an execution process")))
            yield
            secrets.assert_not_called()
            runtime.assert_not_called()
            processes.assert_not_called()

    def command(self, command, state=None, instance=None, extra=()):
        arguments = [command, "--factory", FACTORY]
        if command != "validate":
            arguments += ["--state-dir", str(state), "--principal", "demo",
                          "--instance-id", instance, "--mas-bin", "/absent/mas",
                          "--gateway-bin", "/absent/mas-gateway"]
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main([*arguments, *extra])
        self.assertEqual((status, stderr.getvalue()), (0, ""), stderr.getvalue())
        envelope = json.loads(stdout.getvalue())
        self.assertTrue(envelope["ok"])
        return envelope["result"]

    def engine(self, state):
        return WorkflowEngine(state, **example_registry(),
                              mas_bin="/absent/mas", gateway_bin="/absent/mas-gateway")

    def submit(self, engine):
        return engine.submit("demo", "management-test", "structured-evidence",
                             {"text": "hello", "value": 7})["instance_id"]

    def prepare_result(self, engine, result):
        instance = self.submit(engine)
        # Forked scheduler workers inherit this deterministic executor. All
        # ownership, scheduling and persistence code still runs normally.
        with patch("mas_host.workflow_engine.RunnerExecutor.execute", return_value=result):
            report = engine.run("demo", instance)
        self.assertEqual(report["state"], result.state, report)
        self.assertEqual(len(report["attempts"]), 2)
        return instance, report

    def test_live_registry_and_validation_need_no_credentials_sdk_or_runtime(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                settings, config, state = self.configure(provider)
                with self.unavailable_execution(config):
                    registry = example_registry(settings=settings, environ={})
                    identities = {name: leaf.identity for name, leaf in registry["workflows"].items()}
                    # Secret presence and a stale credential-file path do not
                    # participate in the durable workflow binding.
                    configured = example_registry(settings=settings, environ={
                        "OPENAI_API_KEY": "unused", "CURSOR_API_KEY": "unused",
                        "OPENAI_API_KEY_FILE": str(self.directory / "missing-openai-key"),
                        "CURSOR_API_KEY_FILE": str(self.directory / "missing-cursor-key")})
                    self.assertEqual(identities,
                        {name: leaf.identity for name, leaf in configured["workflows"].items()})
                    report = self.command("validate")
                    self.assertEqual(set(report["plans"]), {"structured-evidence"})
                    self.assertFalse(state.exists())

    def test_queued_live_instance_can_be_inspected_and_cancelled_without_execution(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                _, config, state = self.configure(provider)
                with self.unavailable_execution(config):
                    engine = self.engine(state)
                    instance = self.submit(engine)
                    before = engine.inspect("demo", instance)
                    with patch("mas_host.workflow_engine.RunnerExecutor.execute") as execute:
                        self.assertEqual(self.command("status", state, instance), before)
                        cancelled = self.command("cancel", state, instance)
                    execute.assert_not_called()
                    self.assertEqual(cancelled["state"], "cancelled")
                    self.assertTrue(cancelled["cancel_requested"])
                    self.assertEqual(cancelled["attempts"], [])
                    self.assertEqual(engine.inspect("demo", instance), cancelled)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux workflow scheduler")
    def test_completed_live_tasks_are_inspectable_without_execution(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                _, config, state = self.configure(provider)
                with self.unavailable_execution(config):
                    engine = self.engine(state)
                    instance, report = self.prepare_result(engine,
                        ExecutionResult("completed", {"fixture": "durable result"}))
                    with patch("mas_host.workflow_engine.RunnerExecutor.execute") as execute:
                        self.assertEqual(self.command("status", state, instance), report)
                        before = snapshot(state)
                        for attempt in report["attempts"]:
                            detail = self.command("task", state, instance,
                                ["--path", attempt["path"], "--revision", "0"])
                            expected = engine.inspect_task("demo", instance, attempt["path"], 0)
                            self.assertEqual(detail, expected)
                            self.assertEqual(detail["output"], {"fixture": "durable result"})
                        self.assertEqual(snapshot(state), before)
                    execute.assert_not_called()

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux workflow scheduler")
    def test_interrupted_live_tasks_can_be_recovered_without_execution(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                _, config, state = self.configure(provider)
                with self.unavailable_execution(config):
                    engine = self.engine(state)
                    # Infrastructure failed before a leaf run or provider
                    # invocation was created; there is no external effect to
                    # reconcile before the existing attempts can be requeued.
                    instance, report = self.prepare_result(engine,
                        ExecutionResult("unknown", error={"code": "fixture_interrupted"}))
                    with patch("mas_host.workflow_engine.RunnerExecutor.execute") as execute:
                        recovered = self.command("recover", state, instance)
                    execute.assert_not_called()
                    self.assertEqual(recovered["state"], "queued")
                    self.assertEqual({attempt["state"] for attempt in recovered["attempts"]}, {"queued"})
                    self.assertEqual([attempt["run_directory"] for attempt in recovered["attempts"]],
                                     [attempt["run_directory"] for attempt in report["attempts"]])
                    self.assertEqual(engine.inspect("demo", instance), recovered)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux workflow scheduler")
    def test_restored_audit_only_live_task_inspection_is_readonly_without_execution(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                _, config, state = self.configure(provider)
                with self.unavailable_execution(config):
                    engine = self.engine(state)
                    instance, report = self.prepare_result(engine,
                        ExecutionResult("completed", {"fixture": "restored result"}))
                    bundle = self.directory / (provider + "-backup")
                    restored = self.directory / (provider + "-restored")
                    backup_state(state, bundle)
                    restore_state(bundle, restored)
                    self.assertTrue((restored / ".restored-audit-only").is_file())
                    before = snapshot(restored)
                    with patch("mas_host.workflow_engine.RunnerExecutor.execute") as execute:
                        for attempt in report["attempts"]:
                            detail = self.command("task", restored, instance, ["--path", attempt["path"]])
                            self.assertEqual(detail, engine.inspect_task("demo", instance, attempt["path"]))
                    execute.assert_not_called()
                    self.assertEqual(snapshot(restored), before)


if __name__ == "__main__":
    unittest.main()
