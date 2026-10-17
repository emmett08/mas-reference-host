"""Unrelated application schemas share the public providers and Rust journal."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host import Outcome, Runner
from mas_host.cursor_provider import CursorLocalProvider
from mas_host.invoice_cursor import InvoiceCursorProvider
from mas_host.journal import inspect_run
from mas_host.operation_contracts import OperationContract
from mas_host.structured_openai import OpenAIStructuredProvider
from mas_host.structured_provider import StructuredAgentAdapter
from test_structured_providers import CursorClient, CursorRun, OpenAIClient


ROOT = Path(__file__).resolve().parents[2]
MAS = Path(os.environ.get("MAS_BIN", ROOT / "rust-protocol/target/release/mas"))
GATEWAY = Path(os.environ.get("MAS_GATEWAY_BIN", ROOT / "rust-protocol/target/release/mas-gateway"))


def object_schema(properties):
    return {"type": "object", "properties": properties, "required": sorted(properties),
            "additionalProperties": False}


CASES = [
    ("classify-text", {"text": "Please review the release instructions."}, {"category": "documentation"},
     object_schema({"text": {"type": "string"}}),
     object_schema({"category": {"type": "string", "enum": ["documentation", "other"]}}),
     "Classify the supplied text as documentation or other."),
    ("check-configuration", {"settings": {"feature_enabled": True, "workers": 3}},
     {"valid": True, "violations": []},
     object_schema({"settings": object_schema({"feature_enabled": {"type": "boolean"},
                                                "workers": {"type": "integer"}})}),
     object_schema({"valid": {"type": "boolean"},
                    "violations": {"type": "array", "items": {"type": "string"}}}),
     "Check that the configuration enables the feature and uses one through four workers."),
]


class GenericProviderBoundaryTests(unittest.TestCase):
    def test_invoice_compatibility_is_a_single_explicit_application_adapter(self):
        from mas_host.cursor_local import CursorLocalProvider as LegacyInvoiceProvider
        from mas_host.structured_cursor import CursorStructuredProvider
        self.assertIs(LegacyInvoiceProvider, InvoiceCursorProvider)
        self.assertIs(CursorStructuredProvider, CursorLocalProvider)
        generic = CursorLocalProvider("model", "reviewer", client=CursorClient())
        invoice = InvoiceCursorProvider("model", "reviewer", client=CursorClient())
        self.assertFalse(hasattr(generic, "total"))
        self.assertEqual(invoice.descriptor()["contract"], "invoice-total/2")
        self.assertEqual(invoice.descriptor()["max_output_bytes"], 4096)
        self.assertEqual(generic.descriptor()["contract"], "structured-inference/1")
        self.assertEqual(generic.descriptor()["max_output_bytes"], 32768)


@unittest.skipUnless(MAS.is_file() and GATEWAY.is_file(), "build Rust binaries")
class GenericProviderAdmissionTests(unittest.TestCase):
    def test_unrelated_operations_use_both_public_providers_with_admission_and_replay(self):
        class Ingress:
            def handle(self, context, request):
                return Outcome(request.input)

        for provider_name in ("openai", "cursor"):
            for name, supplied, expected, input_schema, output_schema, instructions in CASES:
                with self.subTest(provider=provider_name, operation=name), tempfile.TemporaryDirectory() as directory:
                    encoded = json.dumps(expected)
                    client = OpenAIClient(text=encoded) if provider_name == "openai" else CursorClient(CursorRun(text=encoded))
                    provider_type = OpenAIStructuredProvider if provider_name == "openai" else CursorLocalProvider
                    provider = provider_type("offline-contract-model", "application-specialist", client=client,
                                             reservation_microusd=123)
                    contract = OperationContract(name, "1", input_schema, output_schema, {"coordinator"})
                    adapter = StructuredAgentAdapter(provider, instructions=instructions,
                                                     contract=contract, actor="specialist")
                    adapters = {"coordinator": Ingress(), "specialist": adapter}
                    topology = {"agents": list(adapters), "application": "agent-task/1",
                                "edges": [["coordinator", "specialist"], ["specialist", "coordinator"]],
                                "max_calls": 1, "max_messages": 10, "max_bytes": 65536,
                                "wall_seconds": 30, "mailbox_capacity": 4}
                    options = {"mas_bin": str(MAS), "gateway_bin": str(GATEWAY),
                               "workflow_version": "generic-provider-admission/1", "supervised": False}
                    with Runner(topology, adapters, Path(directory), **options) as runner:
                        run_dir = runner.run_dir
                        self.assertEqual(runner.request("coordinator", "specialist", contract.operation, supplied),
                                         Outcome(expected))
                        self.assertEqual(runner.status()["usage"]["calls"], 1)
                    journal = inspect_run(run_dir)
                    self.assertEqual(len(journal["invocations"]), 1)
                    invocation = journal["invocations"][0]
                    self.assertEqual(invocation["state"], "completed")
                    self.assertEqual(invocation["output"], expected)
                    self.assertEqual(invocation["evidence"]["reserved_microusd"], 123)
                    self.assertIn("request_sha256", invocation["evidence"])
                    if provider_name == "openai":
                        self.assertEqual(client.calls[0]["input"][0]["content"], instructions)
                        self.assertEqual(client.calls[0]["text"]["format"]["schema"], output_schema)
                    else:
                        self.assertTrue(client.prompts[0].startswith(instructions))
                        self.assertNotIn("total_cents", client.prompts[0])
                        self.assertEqual(client.options[0]["tools"], [])
                    with patch.object(provider, "infer", side_effect=AssertionError("repeated inference")):
                        with Runner.resume(run_dir, adapters, **options) as resumed:
                            self.assertEqual(resumed.request("coordinator", "specialist", contract.operation, supplied),
                                             Outcome(expected))
                            self.assertEqual(resumed.status()["usage"]["calls"], 1)
                    self.assertEqual(inspect_run(run_dir)["invocations"], journal["invocations"])


if __name__ == "__main__":
    unittest.main()
