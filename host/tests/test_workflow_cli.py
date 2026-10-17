"""Trusted CLI boundaries and offline example registrations."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

from mas_host import __main__
from mas_host.service_contracts import ServiceError
from mas_host.workflow_cli import load_registry, main
from mas_host.workflow_examples import TOPOLOGY, example_registry


FACTORY = "mas_host.workflow_examples:example_registry"


class WorkflowCliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.input = self.directory / "input.json"
        self.input.write_text('{"value":0,"target":3}')
        self.common = ["--factory", FACTORY, "--state-dir", str(self.directory / "state"),
                       "--principal", "demo", "--mas-bin", "/test/mas",
                       "--gateway-bin", "/test/mas-gateway"]

    def command(self, arguments, *, dispatch=False):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = __main__.main(["workflow", *arguments]) if dispatch else main(arguments)
        return status, stdout.getvalue(), stderr.getvalue()

    def engine(self):
        factory = MagicMock()
        instance = factory.return_value
        instance.submit.return_value = {"instance_id": "wf-test", "state": "queued"}
        instance.run.return_value = {"instance_id": "wf-test", "state": "completed"}
        instance.inspect.return_value = {"instance_id": "wf-test", "state": "queued"}
        instance.cancel.return_value = {"instance_id": "wf-test", "state": "cancelled"}
        instance.recover.return_value = {"instance_id": "wf-test", "state": "queued"}
        instance.continue_task.return_value = {"instance_id": "wf-test", "state": "queued"}
        module = types.ModuleType("mas_host.workflow_engine")
        module.WorkflowEngine = factory
        return factory, instance, patch.dict(sys.modules, {"mas_host.workflow_engine": module})

    def test_main_dispatch_validates_composition_without_state_or_provider_calls(self):
        status, stdout, stderr = self.command(["validate", "--factory", FACTORY], dispatch=True)
        self.assertEqual((status, stderr), (0, ""))
        result = json.loads(stdout)["result"]
        self.assertEqual(set(result["plans"]), {"text-pipeline", "arithmetic-evidence", "bounded-increment"})
        self.assertEqual(result["plans"]["arithmetic-evidence"]["max_steps"], 11)
        self.assertFalse((self.directory / "state").exists())

    def test_topology_validation_and_graph_have_no_engine_dependency(self):
        path = self.directory / "topology.json"
        path.write_text(json.dumps(TOPOLOGY))
        status, stdout, stderr = self.command(["validate", "--topology", str(path)])
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["result"]["agent_count"], 6)
        status, stdout, stderr = self.command(["graph", "--topology", str(path)])
        self.assertEqual((status, stderr), (0, ""))
        self.assertTrue(stdout.startswith("flowchart TD\n"))

    def test_submit_and_run_pass_only_trusted_cli_bindings(self):
        factory, engine, context = self.engine()
        with context:
            status, stdout, stderr = self.command(["run", *self.common, "--plan", "arithmetic-evidence",
                "--request-key", "one", "--input", str(self.input)])
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["result"]["state"], "completed")
        engine.submit.assert_called_once_with("demo", "one", "arithmetic-evidence", {"value": 0, "target": 3})
        engine.run.assert_called_once_with("demo", "wf-test")
        self.assertEqual(factory.call_args.kwargs["mas_bin"], "/test/mas")
        self.assertEqual(factory.call_args.kwargs["gateway_bin"], "/test/mas-gateway")

    def test_submit_does_not_execute(self):
        _, engine, context = self.engine()
        with context:
            status, stdout, stderr = self.command(["submit", *self.common, "--plan", "arithmetic-evidence",
                "--request-key", "one", "--input", str(self.input)])
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["result"]["state"], "queued")
        engine.run.assert_not_called()

    def test_resume_has_no_new_submission(self):
        _, engine, context = self.engine()
        with context:
            status, _, stderr = self.command(["run", *self.common, "--instance-id", "wf-existing"])
        self.assertEqual((status, stderr), (0, ""))
        engine.submit.assert_not_called()
        engine.run.assert_called_once_with("demo", "wf-existing")

    def test_lifecycle_commands_address_explicit_instance_and_leaf(self):
        for command, method in (("status", "inspect"), ("cancel", "cancel"), ("recover", "recover")):
            with self.subTest(command=command):
                _, engine, context = self.engine()
                with context:
                    status, _, stderr = self.command([command, *self.common, "--instance-id", "wf-existing"])
                self.assertEqual((status, stderr), (0, ""))
                getattr(engine, method).assert_called_once_with("demo", "wf-existing")
        _, engine, context = self.engine()
        with context:
            status, _, stderr = self.command(["continue", *self.common, "--instance-id", "wf-existing",
                "--path", "root/0", "--request-key", "decision-1", "--input", str(self.input)])
        self.assertEqual((status, stderr), (0, ""))
        engine.continue_task.assert_called_once_with("demo", "wf-existing", "root/0", "decision-1",
                                                     {"value": 0, "target": 3})

    def test_invalid_run_arguments_rejected_before_factory_import(self):
        for extra in (["--plan", "arithmetic-evidence"],
                      ["--instance-id", "wf-test", "--input", str(self.input)]):
            with self.subTest(extra=extra), patch("mas_host.workflow_cli.load_registry") as loader:
                status, stdout, stderr = self.command(["run", *self.common, *extra])
                self.assertEqual(status, 1)
                self.assertEqual(stdout, "")
                self.assertFalse(json.loads(stderr)["ok"])
                loader.assert_not_called()

    def test_duplicate_noninteger_oversized_and_missing_json_rejected_before_import(self):
        for source in ('{"value":0,"value":1}', '{"value":0.5}', '[]', '{"text":"' + "a" * 65_536 + '"}'):
            self.input.write_text(source)
            with self.subTest(source=source[:60]), patch("mas_host.workflow_cli.load_registry") as loader:
                status, _, _ = self.command(["submit", *self.common, "--plan", "arithmetic-evidence",
                    "--request-key", "one", "--input", str(self.input)])
                self.assertEqual(status, 1)
                loader.assert_not_called()

    def test_application_input_cannot_choose_factory_or_principal(self):
        self.input.write_text('{"factory":"evil:run","principal":"administrator"}')
        _, engine, context = self.engine()
        with context:
            status, _, stderr = self.command(["submit", *self.common, "--plan", "arithmetic-evidence",
                "--request-key", "one", "--input", str(self.input)])
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(engine.submit.call_args.args[:3], ("demo", "one", "arithmetic-evidence"))
        self.assertEqual(engine.submit.call_args.args[3]["factory"], "evil:run")

    def test_factory_uses_exact_static_entry_point_and_registry_fields(self):
        for target in ("os:system('bad')", "module.func", "a:b:c", "a:__dict__.clear"):
            with self.subTest(target=target), self.assertRaises(ServiceError):
                load_registry(target)
        with patch("mas_host.workflow_cli.importlib.import_module") as module:
            module.return_value.factory.return_value = {"plans": {}, "workflows": {}, "grants": {}, "extra": 1}
            with self.assertRaisesRegex(ServiceError, "registry"):
                load_registry("local:factory")

    def test_adapter_exception_details_are_not_printed(self):
        _, engine, context = self.engine()
        engine.run.side_effect = RuntimeError("provider token secret-value")
        with context:
            status, stdout, stderr = self.command(["run", *self.common, "--instance-id", "wf-test"])
        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr), {"ok": False, "error": "workflow_failed"})

    def test_validate_checks_grants_and_registry_even_without_plan_bindings(self):
        for grants in ({"client": ["missing-plan"]}, {"client": "text-pipeline"},
                       {"": []}, {"client": None}, []):
            registry = example_registry() | {"grants": grants}
            with self.subTest(grants=grants), patch("mas_host.workflow_cli.load_registry", return_value=registry):
                status, stdout, stderr = self.command(["validate", "--factory", FACTORY])
                self.assertEqual(status, 1)
                self.assertEqual(stdout, "")
                self.assertFalse(json.loads(stderr)["ok"])
        registry = {"plans": {}, "workflows": {"invalid": object()}, "grants": {}}
        with patch("mas_host.workflow_cli.load_registry", return_value=registry):
            status, _, stderr = self.command(["validate", "--factory", FACTORY])
        self.assertEqual(status, 1)
        self.assertEqual(json.loads(stderr)["error"], "invalid_workflow_plan_registry")

    def test_example_files_match_executable_factory(self):
        folder = Path(__file__).resolve().parents[2] / "examples/workflows"
        self.assertEqual(json.loads((folder / "topology.json").read_text()), TOPOLOGY)
        for name, plan in example_registry()["plans"].items():
            self.assertEqual(json.loads((folder / (name + ".json")).read_text()), plan.spec)


@unittest.skipUnless(sys.platform.startswith("linux"), "composition requires Linux process supervision")
class WorkflowExampleIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        release = Path(__file__).resolve().parents[2] / "rust-protocol/target/release"
        cls.mas = os.environ.get("MAS_BIN") or shutil.which("mas") or str(release / "mas")
        cls.gateway = os.environ.get("MAS_GATEWAY_BIN") or shutil.which("mas-gateway") or str(release / "mas-gateway")
        if not all(Path(binary).is_file() for binary in (cls.mas, cls.gateway)):
            raise unittest.SkipTest("build the Rust release binaries for workflow integration")

    def test_unrelated_examples_use_rust_and_replay_completed_instances(self):
        from mas_host.workflow_engine import WorkflowEngine
        cases = (
            ("text-pipeline", {"text": "  hello   world  ", "uppercase": True}, 4,
             {"branches": {"reverse": {"text": "DLROW OLLEH"},
                           "summary": {"text": "HELLO WORLD", "characters": 11, "words": 2}}}),
            ("arithmetic-evidence", {"value": 0, "target": 3}, 5,
             {"branches": {"square": {"source": 3, "value": 9, "checked": True},
                           "double": {"source": 3, "value": 6, "checked": True}}}),
        )
        for name, input, count, expected in cases:
            with self.subTest(plan=name), tempfile.TemporaryDirectory() as directory:
                engine = WorkflowEngine(directory, **example_registry(), mas_bin=self.mas, gateway_bin=self.gateway)
                submitted = engine.submit("demo", "request-1", name, input)
                result = engine.run("demo", submitted["instance_id"])
                self.assertEqual(result["state"], "completed", result)
                self.assertEqual(result["output"], expected)
                self.assertEqual(len(result["attempts"]), count)
                self.assertEqual(result["usage"]["calls"], 0)
                for attempt in result["attempts"]:
                    database = Path(directory) / attempt["run_directory"] / "state.sqlite"
                    with sqlite3.connect(database) as db:
                        self.assertEqual(db.execute("SELECT count(*) FROM messages").fetchone()[0], 10)
                        self.assertEqual(db.execute("SELECT count(*) FROM tasks WHERE state='completed'").fetchone()[0], 5)
                replay = WorkflowEngine(directory, **example_registry(), mas_bin=self.mas, gateway_bin=self.gateway)
                same = replay.submit("demo", "request-1", name, input)
                self.assertEqual(same["instance_id"], submitted["instance_id"])
                self.assertEqual(replay.run("demo", same["instance_id"]), result)

    def test_reusable_loop_stops_at_declared_iteration_bound(self):
        from mas_host.workflow_engine import WorkflowEngine
        with tempfile.TemporaryDirectory() as directory:
            engine = WorkflowEngine(directory, **example_registry(), mas_bin=self.mas, gateway_bin=self.gateway)
            submitted = engine.submit("demo", "unreachable", "bounded-increment", {"value": 0, "target": 100})
            result = engine.run("demo", submitted["instance_id"])
            self.assertEqual(result["state"], "failed")
            self.assertEqual(len(result["attempts"]), 5)
            self.assertTrue(any(node["error"] == {"code": "iteration_limit"} for node in result["nodes"]))


if __name__ == "__main__":
    unittest.main()
