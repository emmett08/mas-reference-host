"""Credential initialization is retryable after a failed second write."""
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host import bridge_cli


class AuthCreationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.token = self.root / "token"
        self.verifier = self.root / "verifier"

    def invoke(self):
        with redirect_stderr(StringIO()), redirect_stdout(StringIO()):
            return bridge_cli.main(["init-auth", "--principal", "alice", "--token-file", str(self.token),
                                    "--verifier-file", str(self.verifier)])

    def test_missing_second_parent_is_checked_before_first_create(self):
        self.verifier = self.root / "missing" / "verifier"
        self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.token.exists())
        self.verifier.parent.mkdir()
        self.assertEqual(self.invoke(), 0)

    def test_failed_second_write_removes_only_new_token_and_allows_retry(self):
        create = bridge_cli._private_create
        def fail_verifier(path, data):
            if path == self.verifier:
                raise OSError("injected second-write failure")
            return create(path, data)
        with patch.object(bridge_cli, "_private_create", side_effect=fail_verifier):
            self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.token.exists())
        self.assertFalse(self.verifier.exists())
        self.assertEqual(self.invoke(), 0)

    def test_partial_file_write_rolls_back_its_own_file(self):
        with patch.object(bridge_cli.os, "fsync", side_effect=OSError("injected write failure")):
            self.assertEqual(self.invoke(), 1)
        self.assertFalse(self.token.exists())
        self.assertFalse(self.verifier.exists())

    def test_rollback_preserves_replacement_file(self):
        create = bridge_cli._private_create
        def replace_token_and_fail(path, data):
            if path == self.verifier:
                replacement = self.root / "replacement"
                replacement.write_text("independent file")
                replacement.replace(self.token)
                raise OSError("injected second-write failure")
            return create(path, data)
        with patch.object(bridge_cli, "_private_create", side_effect=replace_token_and_fail):
            self.assertEqual(self.invoke(), 1)
        self.assertEqual(self.token.read_text(), "independent file")


if __name__ == "__main__":
    unittest.main()
