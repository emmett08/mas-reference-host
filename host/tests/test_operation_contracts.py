"""Application contract boundaries, including adversarial handler behaviour."""
from dataclasses import FrozenInstanceError
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mas_host.contracts import Outcome, TaskRequest
from mas_host.operation_contracts import (ContractError, JsonObjectSchema, OperationContract,
                                         OperationRegistry, RegisteredOperation)


SCHEMA = {"type": "object", "properties": {"value": {"type": "integer"}},
          "required": ["value"], "additionalProperties": False}


class Handler:
    def __init__(self, fn=None):
        self.calls = 0
        self.fn = fn or (lambda _c, request: Outcome(request.input))

    def handle(self, context, request):
        self.calls += 1
        return self.fn(context, request)


@unittest.skipUnless(importlib.util.find_spec("jsonschema"), "requires the workflows/bridges extra")
class OperationContractTests(unittest.TestCase):
    def contract(self, **changes):
        options = {"name": "example", "version": "1", "input_schema": SCHEMA,
                   "output_schema": SCHEMA, "allowed_callers": ["caller"]}
        return OperationContract(**(options | changes))

    def registry(self, handler=None):
        return OperationRegistry("worker", [RegisteredOperation(self.contract(), handler or Handler(), "1")])

    def request(self, **changes):
        options = {"sender": "caller", "recipient": "worker", "operation": "example/1",
                   "input": {"value": 7}, "task_id": "task", "message_id": "message"}
        return TaskRequest(**(options | changes))

    def test_registered_version_and_schema_run_without_domain_specific_interface(self):
        registry = self.registry()
        result = registry.handle(SimpleNamespace(actor="worker"), self.request())
        self.assertEqual(result, Outcome({"value": 7}))
        with self.assertRaisesRegex(ContractError, "operation not registered"):
            registry.handle(SimpleNamespace(actor="worker"), self.request(operation="example/2"))

    def test_untrusted_payload_cannot_select_caller_or_handler(self):
        handler = Handler()
        registry = self.registry(handler)
        with self.assertRaisesRegex(ContractError, "caller forbidden"):
            registry.handle(SimpleNamespace(actor="worker"), self.request(sender="outsider",
                            input={"value": 1, "caller": "caller"}))
        self.assertEqual(handler.calls, 0)
        with self.assertRaisesRegex(ContractError, "schema rejected"):
            registry.handle(SimpleNamespace(actor="worker"), self.request(input={"value": 1, "handler": "os.system"}))
        self.assertEqual(handler.calls, 0)

    def test_actor_context_and_recipient_ownership_are_both_checked(self):
        handler = Handler()
        registry = self.registry(handler)
        for actor, recipient in (("outsider", "worker"), ("worker", "outsider")):
            with self.subTest(actor=actor, recipient=recipient), self.assertRaisesRegex(ContractError, "actor mismatch"):
                registry.handle(SimpleNamespace(actor=actor), self.request(recipient=recipient))
        self.assertEqual(handler.calls, 0)

    def test_integer_json_is_stricter_than_python_and_json_schema_numbers(self):
        contract = self.contract()
        for invalid in (1.0, True, "1", None, [1]):
            with self.subTest(invalid=invalid), self.assertRaises(ContractError):
                contract.validate_input({"value": invalid})
        for invalid in ({1: 3}, {"value": 1, "other": float("inf")}, {"value": 1 << 4000}):
            with self.subTest(invalid=type(invalid)), self.assertRaises(ContractError):
                contract.validate_input(invalid)

    def test_schema_and_payload_mutations_do_not_change_effective_contract(self):
        source = {"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]}
        contract = self.contract(input_schema=source)
        identity = contract.identity
        source["properties"]["value"]["type"] = "string"
        contract.input_schema["properties"]["value"]["type"] = "boolean"
        contract.descriptor["allowed_callers"].append("outsider")
        self.assertEqual(contract.identity, identity)
        self.assertEqual(contract.validate_input({"value": 7}), {"value": 7})
        with self.assertRaises(FrozenInstanceError):
            contract.version = "2"
        original = {"value": 7}
        checked = contract.validate_input(original)
        checked["value"] = 8
        self.assertEqual(original, {"value": 7})

    def test_handler_cannot_mutate_request_or_later_output_snapshot(self):
        returned = {"value": 4}
        def mutate(_context, request):
            request.input["value"] = 99
            return Outcome(returned)
        request = self.request()
        result = self.registry(Handler(mutate)).handle(SimpleNamespace(actor="worker"), request)
        returned["value"] = 5
        self.assertEqual(request.input, {"value": 7})
        self.assertEqual(result.output, {"value": 4})

    def test_invalid_handler_outcome_is_not_completed(self):
        for returned in (None, {"output": {"value": 1}}, Outcome({"value": "7"}),
                         Outcome({"value": 1}, "accepted"), Outcome({"value": 1}, True)):
            with self.subTest(returned=returned), self.assertRaises(ContractError):
                self.registry(Handler(lambda _c, _r: returned)).handle(SimpleNamespace(actor="worker"), self.request())
        result = self.registry(Handler(lambda _c, _r: Outcome({"value": 1}, "unresolved"))).handle(
            SimpleNamespace(actor="worker"), self.request())
        self.assertEqual(result.status, "unresolved")

    def test_remote_references_are_refused_even_in_inactive_schema_branch(self):
        for schema in ({"anyOf": [True, {"$ref": "https://example.invalid/schema"}]},
                       {"$defs": {"x": {"$dynamicRef": "file:///etc/passwd"}}},
                       {"$id": "https://example.invalid/schema"},
                       {"$schema": "http://json-schema.org/draft-07/schema#"}):
            with self.subTest(schema=schema), self.assertRaises(ContractError):
                JsonObjectSchema(schema)

    def test_local_references_work_and_application_examples_are_data(self):
        schema = JsonObjectSchema({"$defs": {"count": {"type": "integer"}}, "type": "object",
            "properties": {"value": {"$ref": "#/$defs/count"}},
            "examples": [{"$ref": "https://application.example/document"}]})
        self.assertEqual(schema.validate({"value": 7}), {"value": 7})
        with self.assertRaises(ContractError):
            schema.validate({"value": "seven"})
        with self.assertRaisesRegex(ContractError, "schema rejected"):
            JsonObjectSchema({"$ref": "#/missing"}).validate({})

    def test_descriptors_distinguish_versions_implementations_and_callers(self):
        first = self.contract()
        self.assertNotEqual(first.identity, self.contract(version="2").identity)
        self.assertNotEqual(first.identity, self.contract(allowed_callers=["other"]).identity)
        a = self.registry()
        b = OperationRegistry("worker", [RegisteredOperation(first, Handler(), "2")])
        self.assertNotEqual(a.identity, b.identity)
        changed = a.descriptor
        changed["operations"].clear()
        self.assertEqual(len(a.descriptor["operations"]), 1)

    def test_existing_contract_fingerprint_preserves_unicode_schema_encoding(self):
        contract = OperationContract("echo", "1",
            {"type": "object", "title": "é雪", "properties": {"value": {"type": "string"}},
             "additionalProperties": False}, {"type": "object"}, ["review/checker", "coordinator"])
        self.assertEqual(contract.identity, "ba36296c9e6ea9bfad69bd48ca33e64532d70b3e441f47dc7ba9ddda28a99718")

    def test_duplicate_and_invalid_registrations_fail_at_configuration_time(self):
        operation = RegisteredOperation(self.contract(), Handler(), "1")
        with self.assertRaisesRegex(ContractError, "duplicate"):
            OperationRegistry("worker", [operation, operation])
        for changes in ({"name": "unsafe/name"}, {"version": "1@2"}, {"allowed_callers": "caller"},
                        {"allowed_callers": []}, {"allowed_callers": [None]}, {"input_schema": []}):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                self.contract(**changes)

    def test_oversized_schema_and_payload_are_rejected(self):
        with self.assertRaises(ContractError):
            JsonObjectSchema({"description": "x" * 65536})
        with self.assertRaises(ContractError):
            JsonObjectSchema(True).validate({"data": "x" * 65536})

    def test_combined_descriptor_bounds_report_contract_errors(self):
        schema = {"type": "object", "description": "x" * 33000}
        with self.assertRaises(ContractError) as rejected:
            self.contract(input_schema=schema, output_schema=schema)
        self.assertEqual(rejected.exception.code, "invalid_operation_contract")
        first = self.contract(input_schema=schema)
        second = self.contract(version="2", input_schema=schema)
        with self.assertRaises(ContractError) as rejected:
            OperationRegistry("worker", [RegisteredOperation(first, Handler(), "1"),
                                         RegisteredOperation(second, Handler(), "1")])
        self.assertEqual(rejected.exception.code, "invalid_operation_registry")


class MissingSchemaDependencyTests(unittest.TestCase):
    def test_unavailable_validator_is_explicit_and_never_silently_bypassed(self):
        with patch.dict("sys.modules", {"jsonschema": None}):
            with self.assertRaisesRegex(ContractError, "schema validation unavailable"):
                JsonObjectSchema(True)


@unittest.skipUnless(importlib.util.find_spec("jsonschema"), "requires the workflows/bridges extra")
class OperationContractIntegrationTests(unittest.TestCase):
    def test_rust_completed_task_and_semantically_rejected_result_are_distinct(self):
        from mas_host import Runner
        from mas_host.acceptance_policies import AssessmentSubject, DeterministicPolicy
        from test_runner import RunnerIntegrationTests, topology

        RunnerIntegrationTests.setUpClass()
        contract = OperationContract("double", "1", SCHEMA, SCHEMA, ["a"])
        registry = OperationRegistry("b", [RegisteredOperation(contract,
            Handler(lambda _c, request: Outcome({"value": 3 * request.input["value"]})), "1")])
        with tempfile.TemporaryDirectory() as directory:
            runner = Runner(topology(agents=("a", "b")), {"a": Handler(), "b": registry},
                Path(directory), mas_bin=RunnerIntegrationTests.mas,
                gateway_bin=RunnerIntegrationTests.gateway, supervised=False)
            try:
                result = runner.request("a", "b", contract.operation, {"value": 2})
                policy = DeterministicPolicy("double-check", "1", lambda i, o: o["value"] == 2 * i["value"])
                decision = policy.evaluate(AssessmentSubject({"value": 2}, result.output))
                self.assertEqual(result.status, "completed")
                self.assertEqual(decision.status, "rejected")
                self.assertEqual([task["state"] for task in runner.status()["tasks"]], ["completed"])
                self.assertEqual([event["kind"] for event in runner.events], ["Request", "Result"])
            finally:
                runner.close()


if __name__ == "__main__":
    unittest.main()
