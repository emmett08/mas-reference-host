"""Acceptance CLI must preserve evidence of paid and uncertain invocations."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host import acceptance


class AcceptanceStateTests(unittest.TestCase):
    def test_live_requires_explicit_state_before_any_provider_or_admission(self):
        errors = io.StringIO()
        with patch.object(acceptance, "run_live_acceptance") as live, \
                patch.object(acceptance.tempfile, "TemporaryDirectory") as temporary, \
                redirect_stderr(errors):
            with self.assertRaises(SystemExit) as stopped:
                acceptance.main(["--live", "--runs", "1", "--concurrency", "1"])
        self.assertEqual(stopped.exception.code, 2)
        self.assertIn("explicit persistent --state-dir", errors.getvalue())
        live.assert_not_called()
        temporary.assert_not_called()

    def test_live_unknown_outcome_preserves_supplied_state_and_report(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "deployment"
            report = Path(directory) / "report.json"

            def unknown(**options):
                self.assertEqual(Path(options["state_dir"]), state)
                state.mkdir()
                (state / "unknown-invocation").write_text("retained evidence", encoding="utf-8")
                return {"ok": False, "outcome": "unknown", "status": "failed_or_unknown"}

            with patch.object(acceptance, "run_live_acceptance", side_effect=unknown), \
                    redirect_stdout(io.StringIO()):
                code = acceptance.main(["--live", "--runs", "1", "--concurrency", "1",
                                        "--state-dir", str(state), "--report", str(report)])
            self.assertEqual(code, 1)
            self.assertEqual((state / "unknown-invocation").read_text(), "retained evidence")
            self.assertEqual(json.loads(report.read_text())["outcome"], "unknown")

    def test_live_interruption_preserves_supplied_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "deployment"

            def interrupted(**_options):
                state.mkdir()
                (state / "unknown-invocation").write_text("retained evidence", encoding="utf-8")
                raise RuntimeError("simulated lost response")

            with patch.object(acceptance, "run_live_acceptance", side_effect=interrupted), \
                    redirect_stdout(io.StringIO()):
                code = acceptance.main(["--live", "--runs", "1", "--concurrency", "1",
                                        "--state-dir", str(state)])
            self.assertEqual(code, 1)
            self.assertEqual((state / "unknown-invocation").read_text(), "retained evidence")


if __name__ == "__main__":
    unittest.main()
