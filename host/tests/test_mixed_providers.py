"""Mixed Cursor/OpenAI results cross real Rust admission, routing and recovery."""
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from mas_host.cursor_local import CursorLocalProvider
from mas_host.demo import DEFAULT_TOPOLOGY, run_demo
from mas_host.providers import OpenAIProvider
from mas_host.runner import RunnerError
from test_cursor_local import FakeClient, FakeRun, result

ROOT = Path(__file__).resolve().parents[2]
MAS = Path(os.environ.get("MAS_BIN", ROOT / "rust-protocol/target/release/mas"))
GATEWAY = Path(os.environ.get("MAS_GATEWAY_BIN", ROOT / "rust-protocol/target/release/mas-gateway"))


class Responses:
    def __init__(self, error=False):
        self.error = error

    def create(self, **_options):
        if self.error:
            raise AssertionError("OpenAI must not be dispatched in this trace")
        return SimpleNamespace(status="completed", output_text='{"total_cents":5497}', output=[], id="resp_offline")


@unittest.skipUnless(MAS.is_file() and GATEWAY.is_file(), "build Rust binaries")
class MixedProviderTests(unittest.TestCase):
    def run_mesh(self, state, *, cursor_error=False, openai_error=False, model="cursor-fixture", resume=None):
        cursor = CursorLocalProvider(model, "planner", client=FakeClient(FakeRun(
            result('{"total_cents":5497}'), error=RuntimeError("private-failure") if cursor_error else None)))
        openai = OpenAIProvider("gpt-fixture", "reviewer", client=SimpleNamespace(responses=Responses(openai_error)))
        return run_demo(topology=DEFAULT_TOPOLOGY, state_dir=state, planner_provider=cursor,
                        reviewer_provider=openai, mas_bin=str(MAS), gateway_bin=str(GATEWAY),
                        mode="mixed", resume_dir=resume,
                        models={"planner": model, "reviewer": "gpt-fixture", "checker": "python"})

    def test_mixed_providers_and_replay_preserve_two_admitted_submissions(self):
        with tempfile.TemporaryDirectory() as state:
            first = self.run_mesh(state)
            self.assertTrue(first["ok"], first)
            self.assertEqual(first["providers"], {"planner": "cursor-local", "reviewer": "openai"})
            self.assertEqual(first["result"]["total_cents"], 5497)
            self.assertEqual(first["status"]["usage"]["calls"], 2)
            self.assertEqual({item["evidence"]["provider"] for item in first["status"]["calls"]},
                             {"cursor-local", "openai"})
            resume = Path(first["report_path"]).parent
            replay = self.run_mesh(state, cursor_error=True, openai_error=True, resume=resume)
            self.assertTrue(replay["ok"], replay)
            self.assertEqual(replay["status"]["usage"]["calls"], 2)
            self.assertEqual(replay["workflow_version"], first["workflow_version"])
            with self.assertRaises(RunnerError):
                self.run_mesh(state, model="changed-cursor-model", resume=resume)

    def test_unknown_cursor_effect_prevents_openai_fallback_and_replay(self):
        with tempfile.TemporaryDirectory() as state:
            first = self.run_mesh(state, cursor_error=True, openai_error=True)
            self.assertFalse(first["ok"])
            self.assertEqual(first["status"]["usage"]["calls"], 1)
            self.assertNotIn("private-failure", json.dumps(first))
            self.assertEqual(first["status"]["calls"][0]["evidence"]["outcome"], "unknown")
            replay = self.run_mesh(state, resume=Path(first["report_path"]).parent)
            self.assertFalse(replay["ok"])
            self.assertEqual(replay["status"]["usage"]["calls"], 1)


if __name__ == "__main__":
    unittest.main()
