"""Real engine + Rust checks for generic provider, contract and evidence APIs."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host.artifacts import ArtifactStore
from mas_host.structured_examples import example_registry
from mas_host.workflow_engine import WorkflowEngine


class StructuredExampleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        release = Path(__file__).resolve().parents[2] / "rust-protocol" / "target" / "release"
        cls.options = {"mas_bin": os.environ.get("MAS_BIN", str(release / "mas")),
                       "gateway_bin": os.environ.get("MAS_GATEWAY_BIN", str(release / "mas-gateway"))}
        if any(not Path(binary).is_file() for binary in cls.options.values()):
            raise unittest.SkipTest("build Rust release binaries for structured workflow integration")

    def test_two_schema_operations_publish_accepted_evidence_and_replay(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch("mas_host.provider_credentials.read_api_key", side_effect=AssertionError("offline secret access")):
            registry = example_registry(environ={})
            engine = WorkflowEngine(directory, **registry, **self.options)
            submitted = engine.submit("demo", "accepted", "structured-evidence", {"text": "hello", "value": 7})
            report = engine.run("demo", submitted["instance_id"])
            self.assertEqual(report["state"], "completed", report)
            self.assertEqual(report["usage"]["calls"], 4)
            self.assertEqual(report["usage"]["reserved_microusd"], 0)
            owner = "owner-" + hashlib.sha256(b"demo").hexdigest()
            for actor, result in report["output"]["branches"].items():
                self.assertEqual(result["decision"]["status"], "accepted")
                attempt = next(row for row in report["attempts"] if row["path"] == "root/p-" + actor)
                store = ArtifactStore(Path(directory) / attempt["run_directory"] / "artifacts",
                                      namespace="evidence", principal=owner, readonly=True)
                self.assertTrue(store.verify(result["artifact"]))
                recorded = json.loads(store.read_bytes(result["artifact"]))
                self.assertEqual(recorded["result"], result["result"])
                self.assertEqual(recorded["decision"], result["decision"])
                self.assertTrue(store.inspect(result["artifact"])["links"])
            self.assertEqual(engine.run("demo", submitted["instance_id"]), report)

    def test_completed_execution_can_preserve_rejected_results(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = WorkflowEngine(directory, **example_registry(environ={}), **self.options)
            submitted = engine.submit("demo", "rejected", "structured-evidence", {"text": "goodbye", "value": 6})
            report = engine.run("demo", submitted["instance_id"])
            self.assertEqual(report["state"], "completed", report)
            self.assertTrue(all(result["decision"]["status"] == "rejected"
                                for result in report["output"]["branches"].values()))

    def test_published_actor_configuration_matches_default_registry(self):
        root = Path(__file__).resolve().parents[2]
        configured = example_registry(environ={"MAS_STRUCTURED_CONFIG_FILE":
            str(root / "examples" / "workflows" / "structured-agents.json")})
        default = example_registry(environ={})
        self.assertEqual({name: item.identity for name, item in configured["workflows"].items()},
                         {name: item.identity for name, item in default["workflows"].items()})


if __name__ == "__main__":
    unittest.main()
