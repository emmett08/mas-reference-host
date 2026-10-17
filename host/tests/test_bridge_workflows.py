"""The supplied deployment entry points execute real, replayable MAS workflows."""
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest

from mas_host.bridge_cli import main
from mas_host.bridge_config import load_config
from mas_host.bridge_workflows import invoice_workflows
from mas_host.demo import DEFAULT_INVOICE
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService


class BridgeWorkflowTests(unittest.TestCase):
    def test_three_topologies_use_runner_and_repeat_same_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            workflows = invoice_workflows()
            service = DurableTaskService(Path(temporary), workflows, {"alice": set(workflows)},
                mas_bin=os.environ.get("MAS_BIN", "mas"), gateway_bin=os.environ.get("MAS_GATEWAY_BIN", "mas-gateway"))
            for name in workflows:
                receipt = service.submit("alice", name, name, {"invoice": DEFAULT_INVOICE})
                finished = service.run_once()
                self.assertEqual(finished["state"], "completed")
                self.assertTrue(finished["result"]["verified"])
                self.assertEqual(finished["result"]["total_cents"], 5497)
                replay = service.submit("alice", name, name, {"invoice": DEFAULT_INVOICE})
                self.assertEqual(receipt["task_id"], replay["task_id"])
                self.assertEqual(service.result("alice", receipt["task_id"]), finished["result"])
            self.assertIsNone(service.run_once())

    def test_auth_generation_is_private_and_cannot_replace_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            token, verifier = [Path(temporary) / name for name in ("token", "credentials.json")]
            args = ["init-auth", "--token-file", str(token), "--verifier-file", str(verifier), "--principal", "alice"]
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(args), 0)
            self.assertNotIn(token.read_text().strip(), out.getvalue())
            self.assertEqual(token.stat().st_mode & 0o777, 0o600)
            self.assertEqual(verifier.stat().st_mode & 0o777, 0o600)
            saved = token.read_bytes()
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(args), 1)
            self.assertEqual(token.read_bytes(), saved)

    def test_configuration_rejects_duplicate_and_unknown_fields(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "config.json"
            for content in ('{"version":1,"version":1,"grants":{}}',
                            '{"version":1,"grants":{},"anonymous":true}',
                            '{"version":1,"grants":{"alice":"invoice_mesh"}}'):
                config.write_text(content)
                with self.assertRaises(ServiceError):
                    load_config(config)
