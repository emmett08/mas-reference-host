"""The shared host actor boundary agrees with Rust and preserves domain errors."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from mas_host.codec import is_actor_identity
from mas_host.contracts import Outcome, RunnerError
from mas_host.topology import compile_topology


VALID = ("a", "0", "review/worker", "A0_.:/-", "a" * 128)
INVALID = ("", "_actor", "/actor", "é", "aé", "a\\b", "a\n", "a\x00", "a" * 129,
           None, True, 1, [])


class ActorIdentityTests(unittest.TestCase):
    def test_exact_ascii_and_length_boundary(self):
        for value in VALID + INVALID:
            with self.subTest(value=value):
                self.assertEqual(is_actor_identity(value), value in VALID)
        allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.:/-"
        for code in range(128):
            with self.subTest(code=code):
                self.assertEqual(is_actor_identity("a" + chr(code)), chr(code) in allowed)
        class StringSubclass(str):
            pass
        self.assertFalse(is_actor_identity(StringSubclass("actor")))

    @unittest.skipUnless(importlib.util.find_spec("jsonschema"), "requires workflows/bridges extra")
    def test_public_boundaries_share_grammar_and_keep_specific_errors(self):
        from mas_host.agent_config import AgentConfig
        from mas_host.artifacts import ArtifactError, ArtifactRef
        from mas_host.operation_contracts import ContractError, OperationContract, OperationRegistry, RegisteredOperation
        from mas_host.provider_contract import ProviderError
        from mas_host.structured_provider import MockStructuredProvider, StructuredAgentAdapter

        schema = {"type": "object"}
        contract = OperationContract("example", "1", schema, schema, {"caller"})
        class Handler:
            def handle(self, context, request):
                return Outcome(request.input)
        operation = RegisteredOperation(contract, Handler(), "1")
        boundaries = (
            ("topology", lambda actor: compile_topology({"agents": [actor]}), RunnerError),
            ("registry", lambda actor: OperationRegistry(actor, [operation]), ContractError),
            ("caller", lambda actor: OperationContract("example", "1", schema, schema, [actor]), ContractError),
            ("config", lambda actor: AgentConfig(actor, "mock", "fixture", 0, "Return an object.", contract), ProviderError),
            ("adapter", lambda actor: StructuredAgentAdapter(MockStructuredProvider("role", {}), actor=actor,
                instructions="Return an object.", contract=contract), ProviderError),
            ("artefact", lambda actor: ArtifactRef("scope", "0" * 64, "0" * 64, 0, "text/plain", actor), ArtifactError),
        )
        for name, boundary, error in boundaries:
            for value in VALID:
                with self.subTest(boundary=name, value=value):
                    boundary(value)
            for value in INVALID:
                # None selects the documented default actor for this adapter.
                if value is None and name == "adapter":
                    continue
                with self.subTest(boundary=name, value=value), self.assertRaises(error):
                    boundary(value)

    def test_rust_configuration_admission_matches_host_boundary(self):
        release = Path(__file__).resolve().parents[2] / "rust-protocol/target/release/mas"
        binary = Path(os.environ.get("MAS_BIN") or shutil.which("mas") or str(release))
        if not binary.is_file():
            self.skipTest("Rust release binary unavailable")
        for value in VALID + INVALID:
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "topology.json"
                # Feed Rust directly so Python rejection cannot mask disagreement.
                config = {"agents": [value], "edges": [], "mailbox_capacity": 1,
                          "application": "agent-task/1"}
                path.write_text(json.dumps(config), encoding="utf-8")
                result = subprocess.run([str(binary), "init", str(Path(directory) / "state.sqlite"),
                    "identity-check", str(path)], capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode == 0, is_actor_identity(value), result.stdout)


if __name__ == "__main__":
    unittest.main()
