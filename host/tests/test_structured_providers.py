"""Offline substitution, admission, schema and restricted-SDK regression checks."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mas_host.agent_config import (AgentConfig, create_agent_adapters, create_agent_providers,
                                  load_agent_configs)
from mas_host.contracts import Invocation, TaskRequest
from mas_host import Outcome, Runner
from mas_host.operation_contracts import ContractError, OperationContract
from mas_host.provider_contract import ProviderError
from mas_host.task_service import Workflow
from mas_host.structured_cursor import CursorStructuredProvider
from mas_host.cursor_provider import CursorLocalProvider
from mas_host.structured_openai import OpenAIStructuredProvider
from mas_host.structured_provider import (MAX_OUTPUT_BYTES, MockStructuredProvider,
    StructuredAgentAdapter, StructuredRequest, structured_binding)


INPUT = {"type": "object", "properties": {"text": {"type": "string"}},
         "required": ["text"], "additionalProperties": False}
OUTPUT = {"type": "object", "properties": {"accepted": {"type": "boolean"}},
          "required": ["accepted"], "additionalProperties": False}


def contract():
    return OperationContract("classify", "1", INPUT, OUTPUT, {"coordinator"})


def request():
    return StructuredRequest({"text": "untrusted data"}, "Classify the supplied text.", OUTPUT,
                             schema_name="classification", schema_version="1")


class FakeContext:
    actor = "specialist"
    invocation_id = None

    def __init__(self):
        self.calls = []

    def invoke(self, call, *, input=None, reservation_microusd=None):
        self.calls.append((input, reservation_microusd))
        self.invocation_id = "call-1"
        try:
            invocation = call()
            self.completed = invocation
            return invocation.output
        finally:
            self.invocation_id = None


class OpenAIClient:
    def __init__(self, *, text='{"accepted":true}', error=None, status="completed", output=None):
        self.responses, self.calls, self.error = self, [], error
        self.closed = False
        self.response = SimpleNamespace(status=status, output_text=text, output=output or [],
            id="resp_example", _request_id="request-example",
            usage=SimpleNamespace(input_tokens=3, output_tokens=4, total_tokens=7))

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.response

    def close(self):
        self.closed = True


class CursorRun:
    def __init__(self, *, text='{"accepted":true}', events=(), error=None):
        self.result = SimpleNamespace(status="finished", result=text, id="run-example")
        self.messages, self.error = events, error
        self.status, self.cancelled = "running", False

    def events(self):
        yield from self.messages
        if self.error:
            raise self.error
        self.status = "finished"

    def wait(self):
        return self.result

    def cancel(self):
        self.cancelled = True


class CursorClient:
    agent_id = "agent-example"

    def __init__(self, run=None):
        self.agents, self.run = self, run or CursorRun()
        self.options, self.prompts = [], []
        self.agent_closed, self.client_closed = False, False

    def create(self, *, options):
        self.options.append(options)
        return SimpleNamespace(agent_id=self.agent_id, send=self.send, close=self.close_agent)

    def send(self, prompt):
        self.prompts.append(prompt)
        return self.run

    def close_agent(self):
        self.agent_closed = True

    def close(self):
        self.client_closed = True


class StructuredProviderTests(unittest.TestCase):
    def test_existing_request_fingerprint_preserves_unicode_and_escaping(self):
        request = StructuredRequest({"z": 'é\\"', "a": {"雪": [True, None, -7]}}, "Classify é雪.",
            {"type": "object", "properties": {"value": {"type": "string"}},
             "required": ["value"], "additionalProperties": False},
            schema_name="result", schema_version="check/1")
        self.assertEqual(request.identity, "5cc97d31908aaa6bb56429585fa7ca68b39d59255b32882e620048cbe9f3dd9d")

    def test_request_snapshots_input_schema_and_contract_identity(self):
        supplied, schema = {"text": "one"}, json.loads(json.dumps(OUTPUT))
        first = StructuredRequest(supplied, "Classify.", schema, schema_name="result", schema_version="1")
        supplied["text"] = "two"
        schema["properties"]["accepted"]["type"] = "integer"
        descriptor = first.descriptor
        descriptor["input"]["text"] = "three"
        self.assertEqual(first.descriptor["input"], {"text": "one"})
        self.assertEqual(first.parse_output('{"accepted":true}'), {"accepted": True})
        changed = StructuredRequest({"text": "one"}, "Classify.", OUTPUT,
                                    schema_name="result", schema_version="2")
        self.assertNotEqual(first.identity, changed.identity)
        left = StructuredRequest({"a": 1, "b": 2}, "Classify.", OUTPUT,
                                 schema_name="result", schema_version="1")
        right = StructuredRequest({"b": 2, "a": 1}, "Classify.", OUTPUT,
                                  schema_name="result", schema_version="1")
        self.assertEqual(left.identity, right.identity)

    def test_strict_output_rejects_ambiguity_and_schema_violations(self):
        for text in ('{"accepted":true,"accepted":false}', '{"accepted":1}',
                     '{"accepted":true,"extra":2}', '{"accepted":1.0}',
                     '{"accepted":NaN}', '[]', '```json\n{}\n```', 'x' * (MAX_OUTPUT_BYTES + 1)):
            with self.subTest(text=text[:60]), self.assertRaises(ProviderError):
                request().parse_output(text)

    def test_admission_binds_every_effect_and_checks_actor_and_caller(self):
        provider = MockStructuredProvider("reviewer", {"accepted": True})
        adapter = StructuredAgentAdapter(provider, instructions="Classify.", contract=contract(), actor="specialist")
        task = TaskRequest("coordinator", "specialist", "classify/1", {"text": "data"}, "task", "message")
        context = FakeContext()
        self.assertEqual(adapter.handle(context, task).output, {"accepted": True})
        effect, amount = context.calls[0]
        self.assertEqual(amount, 0)
        self.assertEqual(effect["inference"]["instructions"], "Classify.")
        self.assertEqual(effect["inference"]["output_schema"], OUTPUT)
        self.assertEqual(effect["inference"]["input"], task.input)
        self.assertEqual(effect["adapter"]["provider"]["fixture"], {"accepted": True})
        for invalid in (TaskRequest("intruder", "specialist", "classify/1", task.input, "t", "m"),
                        TaskRequest("coordinator", "other", "classify/1", task.input, "t", "m"),
                        TaskRequest("coordinator", "specialist", "classify/2", task.input, "t", "m"),
                        TaskRequest("coordinator", "specialist", "classify/1", {"bad": True}, "t", "m")):
            with self.subTest(task=invalid), self.assertRaises((ProviderError, ContractError)):
                adapter.handle(context, invalid)
        self.assertEqual(len(context.calls), 1)

    def test_custom_provider_cannot_commit_invalid_output_before_validation(self):
        class Invalid(MockStructuredProvider):
            def infer(self, request, *, request_id=None):
                return Invocation({"accepted": 1}, {})
        adapter = StructuredAgentAdapter(Invalid("specialist", {}), instructions="Classify.", contract=contract())
        context = FakeContext()
        with self.assertRaises(ProviderError):
            adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1",
                                               {"text": "data"}, "t", "m"))
        self.assertFalse(hasattr(context, "completed"))

    def test_mutated_provider_binding_fails_before_admission(self):
        provider = MockStructuredProvider("specialist", {"accepted": True})
        adapter = StructuredAgentAdapter(provider, instructions="Classify.", contract=contract())
        provider.model = "changed"
        context = FakeContext()
        with self.assertRaises(ProviderError):
            adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1",
                                               {"text": "data"}, "t", "m"))
        self.assertEqual(context.calls, [])

    def test_adapter_configuration_cannot_drift_under_frozen_workflow_identity(self):
        provider = MockStructuredProvider("specialist", {"accepted": True})
        adapter = StructuredAgentAdapter(provider, instructions="Original instructions.", contract=contract())
        workflow = Workflow("fixture/1", {"agents": ["coordinator", "specialist"]},
            {"coordinator": adapter, "specialist": adapter}, "coordinator", "specialist", "classify/1",
            contract_descriptor={"specialist": adapter.descriptor()})
        identity = workflow.identity
        for field, value in (("instructions", "Changed instructions."), ("actor", "other"),
                             ("contract", OperationContract("classify", "2", INPUT, OUTPUT, {"coordinator"})),
                             ("provider", MockStructuredProvider("specialist", {"accepted": False})),
                             ("_provider_source", "{}")):
            with self.subTest(field=field), self.assertRaises(AttributeError):
                setattr(adapter, field, value)
        exposed = adapter.descriptor()
        exposed["instructions"] = "Changed instructions."
        exposed["provider"]["fixture"]["accepted"] = False
        context = FakeContext()
        adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1", {"text": "data"}, "t", "m"))
        self.assertEqual(workflow.identity, identity)
        self.assertEqual(context.calls[0][0]["inference"]["instructions"], "Original instructions.")
        self.assertEqual(context.completed.output, {"accepted": True})
        inference = request()
        for field, value in (("_source", "{}"), ("_schema", None)):
            with self.subTest(request_field=field), self.assertRaises(AttributeError):
                setattr(inference, field, value)

    def test_mutable_builtin_provider_runtime_configuration_is_bound(self):
        cases = [
            (lambda: OpenAIStructuredProvider("test-model", "specialist", client=OpenAIClient()),
             lambda p: p, {"model": "other-model", "role": "other-role", "reservation_microusd": 200000,
                           "max_output_tokens": 100}),
            (lambda: CursorStructuredProvider("test-model", "specialist", client=CursorClient()),
             lambda p: p._session, {"model": "other-model", "role": "other-role",
                                   "reservation_microusd": 200000,
                                   "_transport_policy": "environment-proxy-http1"}),
            (lambda: MockStructuredProvider("specialist", {"accepted": True}), lambda p: p,
             {"model": "other-model", "role": "other-role", "_output": '{"accepted":false}',
              "reservation_microusd": 1}),
        ]
        for factory, target, changes in cases:
            for field, value in changes.items():
                with self.subTest(factory=factory, field=field):
                    provider = factory()
                    adapter = StructuredAgentAdapter(provider, instructions="Classify.", contract=contract())
                    setattr(target(provider), field, value)
                    context = FakeContext()
                    with self.assertRaises(ProviderError):
                        adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1",
                                                           {"text": "data"}, "t", "m"))
                    self.assertEqual(context.calls, [])

    def test_provider_configuration_is_rechecked_at_execution_boundary(self):
        client = OpenAIClient()
        provider = OpenAIStructuredProvider("test-model", "specialist", client=client)
        adapter = StructuredAgentAdapter(provider, instructions="Classify.", contract=contract())

        class ChangedDuringAdmission(FakeContext):
            def invoke(self, call, **kwargs):
                provider.model = "changed-after-admission"
                return super().invoke(call, **kwargs)

        context = ChangedDuringAdmission()
        with self.assertRaises(ProviderError):
            adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1", {"text": "data"}, "t", "m"))
        self.assertEqual(len(context.calls), 1)
        self.assertEqual(client.calls, [])
        self.assertFalse(hasattr(context, "completed"))

    def test_nested_boolean_integer_provider_drift_is_not_equal(self):
        schema = {"type": "object", "properties": {"result": {"type": "object",
            "properties": {"flags": {"type": "array", "items": {"type": ["boolean", "integer"]}}},
            "required": ["flags"], "additionalProperties": False}},
            "required": ["result"], "additionalProperties": False}
        operation = OperationContract("classify", "1", INPUT, schema, {"coordinator"})
        provider = MockStructuredProvider("specialist", {"result": {"flags": [True]}})
        adapter = StructuredAgentAdapter(provider, instructions="Classify.", contract=operation)
        provider._output = '{"result":{"flags":[1]}}'
        context = FakeContext()
        with self.assertRaises(ProviderError):
            adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1",
                                               {"text": "data"}, "t", "m"))
        self.assertEqual(context.calls, [])

    def test_cached_outputs_are_revalidated(self):
        context = FakeContext()
        context.invoke = lambda *a, **kw: {"accepted": 1}
        adapter = StructuredAgentAdapter(MockStructuredProvider("specialist", {"accepted": True}),
                                         instructions="Classify.", contract=contract())
        with self.assertRaises(ProviderError):
            adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1",
                                               {"text": "data"}, "t", "m"))

    def test_openai_uses_structured_text_only_bounded_call(self):
        client = OpenAIClient()
        provider = OpenAIStructuredProvider("test-model", "any-specialism", client=client,
                                            reservation_microusd=123, max_output_tokens=321)
        self.assertEqual(client.calls, [])
        invocation = provider.infer(request(), request_id="call-1")
        self.assertEqual(invocation.output, {"accepted": True})
        call = client.calls[0]
        self.assertEqual(call["tools"], [])
        self.assertFalse(call["store"])
        self.assertEqual(call["max_output_tokens"], 321)
        self.assertEqual(call["text"]["format"]["schema"], OUTPUT)
        self.assertEqual(call["extra_headers"], {"X-Client-Request-Id": "call-1"})
        self.assertEqual(invocation.evidence["reserved_microusd"], 123)
        self.assertEqual(structured_binding(provider)[1], 123)
        self.assertFalse(client.closed, "injected OpenAI clients remain caller-owned")

    def test_openai_owned_clients_are_lazy_and_close_on_success_or_failure(self):
        clients = [OpenAIClient(), OpenAIClient(error=RuntimeError("SECRET")),
                   OpenAIClient(status="incomplete"), OpenAIClient(text='{"accepted":1}')]
        for index, client in enumerate(clients):
            with self.subTest(case=index), patch("mas_host.openai_client.create_client", return_value=client) as create:
                provider = OpenAIStructuredProvider("test-model", "review", environ={"OPENAI_API_KEY": "key"})
                binding = structured_binding(provider)
                create.assert_not_called()
                if index:
                    with self.assertRaises(ProviderError):
                        provider.infer(request())
                else:
                    self.assertEqual(provider.infer(request()).output, {"accepted": True})
                create.assert_called_once_with("key")
                self.assertTrue(client.closed)
                self.assertEqual(structured_binding(provider), binding)

    def test_failed_openai_cleanup_preserves_completion_without_reporting_success(self):
        client = OpenAIClient()
        client.close = lambda: (_ for _ in ()).throw(RuntimeError("SECRET cleanup"))
        provider = OpenAIStructuredProvider("test-model", "specialist", environ={"OPENAI_API_KEY": "key"})
        adapter = StructuredAgentAdapter(provider, instructions="Classify.", contract=contract())
        context = FakeContext()
        with patch("mas_host.openai_client.create_client", return_value=client), self.assertRaises(ProviderError) as error:
            adapter.handle(context, TaskRequest("coordinator", "specialist", "classify/1", {"text": "data"}, "t", "m"))
        self.assertNotIn("SECRET", str(error.exception))
        self.assertEqual(len(context.calls), 1)
        self.assertFalse(hasattr(context, "completed"))
        self.assertEqual(error.exception.outcome["certainty"], "completed")
        self.assertEqual(error.exception.outcome["output_status"], "valid")
        self.assertEqual(error.exception.completed_output, {"accepted": True})

    def test_openai_interruption_survives_cleanup_failure(self):
        primary = KeyboardInterrupt()
        client = OpenAIClient(error=primary)
        client.close = lambda: (_ for _ in ()).throw(RuntimeError("SECRET cleanup"))
        with patch("mas_host.openai_client.create_client", return_value=client), self.assertRaises(KeyboardInterrupt) as error:
            OpenAIStructuredProvider("test-model", "review", environ={"OPENAI_API_KEY": "key"}).infer(request())
        self.assertIs(error.exception, primary)

    def test_openai_errors_are_safe_and_never_retried(self):
        cases = [OpenAIClient(error=RuntimeError("SECRET")), OpenAIClient(status="incomplete"),
                 OpenAIClient(text='{"accepted":1}'),
                 OpenAIClient(output=[SimpleNamespace(content=[SimpleNamespace(type="refusal")])]),
                 OpenAIClient(output=[SimpleNamespace(type="function_call")])]
        for client in cases:
            with self.subTest(client=client), self.assertRaises(ProviderError) as caught:
                OpenAIStructuredProvider("test-model", "review", client=client).infer(request())
            self.assertNotIn("SECRET", str(caught.exception))
            self.assertEqual(len(client.calls), 1)

    def test_cursor_reuses_no_tool_policy_private_workspace_and_cleanup(self):
        client = CursorClient()
        provider = CursorStructuredProvider("test-model", "any-specialism", client=client)
        invocation = provider.infer(request(), request_id="call-1")
        self.assertEqual(invocation.output, {"accepted": True})
        self.assertEqual(len(client.prompts), 1)
        options = client.options[0]
        self.assertEqual(options["tools"], [])
        self.assertEqual(options["disallowed_tools"], ["mcp", "task"])
        self.assertEqual(options["mcp_servers"], {})
        self.assertEqual(options["local"]["setting_sources"], [])
        self.assertFalse(Path(options["local"]["cwd"]).exists())
        self.assertTrue(client.agent_closed)
        self.assertFalse(client.client_closed)
        self.assertEqual(invocation.evidence["request_sha256"], request().identity)

    def test_cursor_uses_shared_ownership_and_sanitised_unknown_errors(self):
        client = CursorClient(CursorRun(error=RuntimeError("SECRET")))
        provider = CursorStructuredProvider("test-model", "review", api_key="SECRET")
        with patch("mas_host.cursor_session.launch_client", return_value=client), self.assertRaises(ProviderError) as error:
            provider.infer(request())
        self.assertNotIn("SECRET", str(error.exception))
        self.assertEqual(error.exception.diagnostic["stage"], "stream")
        self.assertTrue(client.run.cancelled)
        self.assertTrue(client.agent_closed)
        self.assertTrue(client.client_closed)
        self.assertFalse(Path(client.options[0]["local"]["cwd"]).exists())

    def test_cursor_schema_failure_still_closes_owned_resources(self):
        client = CursorClient(CursorRun(text='{"accepted":1}'))
        with patch("mas_host.cursor_session.launch_client", return_value=client), self.assertRaises(ProviderError) as error:
            CursorStructuredProvider("test-model", "review", api_key="key").infer(request())
        self.assertEqual(error.exception.diagnostic["stage"], "result")
        self.assertTrue(client.agent_closed)
        self.assertTrue(client.client_closed)

    def test_invalid_trace_never_submits(self):
        openai, cursor = OpenAIClient(), CursorClient()
        for provider in (OpenAIStructuredProvider("test-model", "review", client=openai),
                         CursorStructuredProvider("test-model", "review", client=cursor)):
            with self.assertRaises(ProviderError):
                provider.infer(request(), request_id="bad\ntrace")
        self.assertEqual(openai.calls, [])
        self.assertEqual(cursor.options, [])


class AgentConfigurationTests(unittest.TestCase):
    def settings(self, provider="mock", actor="specialist"):
        return AgentConfig(actor, provider, "explicit-model", 0 if provider == "mock" else 123,
                           "Classify.", contract(), role="arbitrary-role")

    def test_arbitrary_registered_actor_offline_factory_and_json_loader(self):
        definitions = {"classify/1": contract()}
        configs = load_agent_configs({"specialist": {"provider": "mock", "model": "fixture-1",
            "reservation_microusd": 0, "instructions": "Classify.", "operation": "classify/1",
            "role": "arbitrary-specialism"}}, contracts=definitions)
        with patch("mas_host.provider_credentials.read_api_key", side_effect=AssertionError("read secret")):
            adapters = create_agent_adapters(configs, registered_actors={"specialist", "tool"},
                                              mock_outputs={"specialist": {"accepted": True}})
        self.assertEqual(adapters["specialist"].descriptor()["provider"]["role"], "arbitrary-specialism")
        self.assertEqual(adapters["specialist"].handle(FakeContext(), TaskRequest(
            "coordinator", "specialist", "classify/1", {"text": "data"}, "t", "m")).output,
            {"accepted": True})

    def test_rejects_unregistered_actors_and_invalid_fixture_before_clients(self):
        configs = {"specialist": self.settings()}
        for registered, fixtures in (({"other"}, {"specialist": {"accepted": True}}),
                                     ({"specialist"}, {}),
                                     ({"specialist"}, {"specialist": {"accepted": 1}})):
            with self.subTest(registered=registered), self.assertRaises((ProviderError, ContractError)):
                create_agent_providers(configs, registered_actors=registered, mock_outputs=fixtures)

    def test_live_factory_defers_credentials_and_keeps_provider_sources_separate(self):
        configs = {"a": self.settings("cursor", "a"), "b": self.settings("openai", "b")}
        environment = {}
        with patch("mas_host.provider_credentials.read_api_key", side_effect=AssertionError("read secret")), \
                patch("mas_host.cursor_session._sdk_module", side_effect=AssertionError("loaded SDK")):
            providers = create_agent_providers(configs, registered_actors=configs, environ=environment)
            identities = {name: provider.descriptor() for name, provider in providers.items()}
        self.assertIs(type(providers["a"]), CursorLocalProvider)
        self.assertIs(CursorStructuredProvider, CursorLocalProvider)
        # Credentials are read at execution, including rotation after registration.
        environment.update(CURSOR_API_KEY="cursor-key", OPENAI_API_KEY="openai-key")
        openai, cursor = OpenAIClient(), CursorClient()
        with patch("mas_host.openai_client.create_client", return_value=openai) as create, \
                patch("mas_host.cursor_session.launch_client", return_value=cursor):
            providers["a"].infer(request())
            providers["b"].infer(request())
        create.assert_called_once_with("openai-key")
        self.assertEqual(cursor.options[0]["api_key"], "cursor-key")
        self.assertTrue(openai.closed)
        self.assertTrue(cursor.client_closed)
        self.assertEqual(identities, {name: provider.descriptor() for name, provider in providers.items()})

    def test_unknown_and_missing_configuration_fields_rejected(self):
        settings = {"provider": "mock", "model": "fixture-1", "reservation_microusd": 0,
                    "instructions": "Classify.", "operation": "classify/1"}
        for invalid in ({**settings, "api_key": "SECRET"}, {**settings, "operation": "missing/1"},
                        {k: v for k, v in settings.items() if k != "model"}):
            with self.assertRaises(ProviderError) as error:
                load_agent_configs({"actor": invalid}, contracts={"classify/1": contract()})
            self.assertNotIn("SECRET", str(error.exception))

    def test_actor_and_provider_configuration_fail_before_factory(self):
        for actor in (".invalid", "x" * 129, "space invalid"):
            with self.subTest(actor=actor), self.assertRaises(ProviderError):
                self.settings(actor=actor)
        with self.assertRaises(ProviderError):
            AgentConfig("actor", [], "model", 0, "Classify.", contract())
        with self.assertRaises(ProviderError):
            AgentConfig("actor", "cursor", "model", 123, "Classify.", contract(), max_output_tokens=100)


class StructuredRunnerTests(unittest.TestCase):
    def test_structured_actor_uses_real_rust_admission_and_checkpoint_replay(self):
        root = Path(__file__).resolve().parents[2]
        release = root / "rust-protocol" / "target" / "release"
        mas = os.environ.get("MAS_BIN", str(release / "mas"))
        gateway = os.environ.get("MAS_GATEWAY_BIN", str(release / "mas-gateway"))
        if not Path(mas).is_file() or not Path(gateway).is_file():
            self.skipTest("build Rust release binaries for structured admission integration")

        class Echo:
            def handle(self, context, request):
                return Outcome(request.input)

        adapter = StructuredAgentAdapter(MockStructuredProvider("specialist", {"accepted": True}),
                                         instructions="Classify.", contract=contract())
        adapters = {"coordinator": Echo(), "specialist": adapter}
        topology = {"agents": list(adapters), "edges": [["coordinator", "specialist"],
                    ["specialist", "coordinator"]], "application": "agent-task/1",
                    "mailbox_capacity": 4, "max_messages": 10, "max_bytes": 65536,
                    "max_calls": 1, "wall_seconds": 30}
        options = {"mas_bin": mas, "gateway_bin": gateway, "workflow_version": "structured-test/1"}
        with tempfile.TemporaryDirectory() as directory:
            with Runner(topology, adapters, Path(directory), **options) as runner:
                run_dir = runner.run_dir
                output = runner.request("coordinator", "specialist", "classify/1", {"text": "data"})
                self.assertEqual(output.output, {"accepted": True})
                self.assertEqual(len(runner.status()["calls"]), 1)
                self.assertFalse(runner.status()["calls"][0]["evidence"]["network"])
            with patch.object(adapter.provider, "infer", side_effect=AssertionError("repeated inference")):
                with Runner.resume(run_dir, adapters, **options) as resumed:
                    self.assertEqual(resumed.request("coordinator", "specialist", "classify/1",
                                                     {"text": "data"}).output, output.output)
                    self.assertEqual(len(resumed.status()["calls"]), 1)


if __name__ == "__main__":
    unittest.main()
