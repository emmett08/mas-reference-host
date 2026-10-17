"""Trusted extension, configuration rejection and persisted-identity regressions."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import unittest
from unittest.mock import patch

from mas_host.agent_config import (AgentConfig, create_agent_adapters, create_agent_providers,
                                  load_agent_configs)
from mas_host.contracts import Invocation, TaskRequest
from mas_host.cursor_provider import CursorLocalProvider
from mas_host.operation_contracts import OperationContract
from mas_host.provider_contract import ProviderError
from mas_host.provider_registry import (DEFAULT_PROVIDER_REGISTRY, ProviderCapabilities,
    ProviderRegistration, ProviderRegistry)
from mas_host.structured_openai import OpenAIStructuredProvider
from mas_host.structured_provider import MockStructuredProvider


INPUT = {"type": "object", "properties": {"text": {"type": "string"}},
         "required": ["text"], "additionalProperties": False}
OUTPUT = {"type": "object", "properties": {"accepted": {"type": "boolean"}},
          "required": ["accepted"], "additionalProperties": False}


class KeywordProvider:
    """An application-owned fourth provider with actual deterministic inference."""

    reservation_microusd = 0

    def __init__(self, config):
        self.model, self.role = config.model, config.role

    def descriptor(self):
        return {"provider": "local-rules", "model": self.model, "role": self.role,
                "implementation": "keyword-classifier/1", "contract": "structured-inference/1",
                "reservation_microusd": 0}

    def infer(self, request, *, request_id=None):
        accepted = request.descriptor["input"]["text"].casefold() == "approved"
        return Invocation(request.validate_output({"accepted": accepted}),
                          {"network": False, "client_request_id": request_id})


class AdmissionContext:
    actor = "specialist"
    invocation_id = None

    def __init__(self):
        self.effects = []

    def invoke(self, operation, *, input, reservation_microusd):
        self.effects.append((input, reservation_microusd))
        self.invocation_id = "admitted-call-1"
        try:
            self.invocation = operation()
            return self.invocation.output
        finally:
            self.invocation_id = None


class ProviderRegistryTests(unittest.TestCase):
    def setUp(self):
        self.contract = OperationContract("classify", "1", INPUT, OUTPUT, {"coordinator"})
        self.factories = []

        def factory(config, context):
            self.factories.append((config, context))
            return KeywordProvider(config)

        self.registration = ProviderRegistration("local-rules", "keyword-classifier/1", factory,
            ProviderCapabilities(accounting="none"), aliases=("rules",))
        self.registry = DEFAULT_PROVIDER_REGISTRY.with_provider(self.registration)

    def settings(self, **changes):
        return {"provider": "local-rules", "model": "keywords-v1", "reservation_microusd": 0,
                "instructions": "Classify the supplied text.", "operation": "classify/1",
                "role": "reviewer", **changes}

    def load(self, settings=None, *, registry=None):
        return load_agent_configs({"specialist": self.settings() if settings is None else settings},
            contracts={"classify/1": self.contract}, registry=self.registry if registry is None else registry)

    def test_fourth_provider_selected_from_json_and_invoked_only_after_admission(self):
        configs = self.load()
        self.assertEqual(self.factories, [])
        environment = {"PRIVATE_TEST_KEY": "not-in-descriptors"}
        adapters = create_agent_adapters(configs, registered_actors={"specialist"},
                                         environ=environment, registry=self.registry)
        self.assertIs(self.factories[0][1].environ, environment)
        self.assertNotIn("not-in-descriptors", str(self.registry.descriptor()))
        self.assertNotIn("not-in-descriptors", str(adapters["specialist"].descriptor()))
        for text, accepted in (("APPROVED", True), ("other", False)):
            context = AdmissionContext()
            outcome = adapters["specialist"].handle(context, TaskRequest(
                "coordinator", "specialist", "classify/1", {"text": text}, "task", "message"))
            self.assertEqual(outcome.output, {"accepted": accepted})
            self.assertEqual(context.effects[0][1], 0)
            self.assertEqual(context.invocation.evidence["client_request_id"], "admitted-call-1")

    def test_alias_resolves_canonical_provider_identity(self):
        configs = self.load(self.settings(provider="rules"))
        provider = create_agent_providers(configs, registered_actors=configs,
                                           registry=self.registry)["specialist"]
        self.assertEqual(provider.descriptor()["provider"], "local-rules")
        self.assertEqual(self.registry.resolve("rules").identity, self.registration.identity)

    def test_default_registry_does_not_implicitly_authorise_custom_registration(self):
        with self.assertRaisesRegex(ProviderError, "not registered"):
            self.load(registry=DEFAULT_PROVIDER_REGISTRY)
        configs = self.load()
        with self.assertRaisesRegex(ProviderError, "not registered"):
            create_agent_providers(configs, registered_actors=configs)
        self.assertEqual(self.factories, [])

    def test_unsupported_capabilities_and_dialects_rejected_before_factory(self):
        for fields in ({"required_capabilities": ["tools"]},
                       {"required_capabilities": ["reconciliation"]},
                       {"required_capabilities": ["invented"]},
                       {"required_capabilities": ["sessions", "sessions"]},
                       {"required_capabilities": "streaming"},
                       {"structured_output_dialect": "unregistered-schema/1"},
                       {"max_output_tokens": 10},
                       {"cursor_transport": "environment-proxy-http1"},
                       {"reservation_microusd": 10}):
            with self.subTest(fields=fields), self.assertRaises(ProviderError):
                self.load(self.settings(**fields))
        self.assertEqual(self.factories, [])

    def test_json_cannot_select_imports_executables_factories_or_credentials(self):
        for fields in ({"provider": "arbitrary.module.factory"},
                       {"factory": "arbitrary.module.factory"},
                       {"executable": "/tmp/untrusted"},
                       {"endpoint": "https://untrusted.example"},
                       {"api_key": "PRIVATE"}):
            with self.subTest(fields=fields), self.assertRaises(ProviderError) as raised:
                self.load(self.settings(**fields))
            self.assertNotIn("PRIVATE", str(raised.exception))
        self.assertEqual(self.factories, [])

    def test_implementation_or_capability_change_rejects_stale_configuration(self):
        configs = self.load()
        for replacement in (replace(self.registration, implementation="keyword-classifier/2"),
                            replace(self.registration, capabilities=ProviderCapabilities(
                                accounting="none", streaming=True))):
            changed = ProviderRegistry((replacement,))
            with self.assertRaisesRegex(ProviderError, "registration changed"):
                create_agent_providers(configs, registered_actors=configs, registry=changed)
        self.assertEqual(self.factories, [])

    def test_reloaded_custom_capabilities_and_endpoint_changes_alter_durable_identity(self):
        configs = self.load()
        original = create_agent_adapters(configs, registered_actors=configs,
                                         registry=self.registry)["specialist"].descriptor()
        registration = replace(self.registration, capabilities=ProviderCapabilities(
            accounting="none", endpoint_policy="fixed", endpoints=("https://approved.example/v1",)))
        changed = ProviderRegistry((registration,))
        reloaded = self.load(registry=changed)
        adapter = create_agent_adapters(reloaded, registered_actors=reloaded,
                                         registry=changed)["specialist"]
        self.assertNotEqual(original, adapter.descriptor())
        self.assertNotEqual(configs["specialist"].descriptor(), reloaded["specialist"].descriptor())
        self.assertEqual(adapter.descriptor()["provider"]["registration"], registration.descriptor())

    def test_entire_configuration_batch_is_checked_before_first_factory(self):
        configs = self.load()
        configs["other"] = AgentConfig("other", "mock", "fixture", 0, "Classify.", self.contract)
        with self.assertRaisesRegex(ProviderError, "not registered"):
            create_agent_providers(configs, registered_actors=configs,
                                   registry=ProviderRegistry((self.registration,)))
        self.assertEqual(self.factories, [])

    def test_factory_cannot_substitute_provider_implementation_role_model_or_cost(self):
        for field, value in (("provider", "unregistered"), ("implementation", "wrong/1"),
                             ("role", "other"), ("model", "other"),
                             ("reservation_microusd", 123)):
            def broken(config, context):
                provider = KeywordProvider(config)
                descriptor = provider.descriptor()
                descriptor[field] = value
                provider.descriptor = lambda: descriptor
                return provider

            registry = ProviderRegistry((replace(self.registration, factory=broken),))
            configs = self.load(registry=registry)
            with self.subTest(field=field), self.assertRaises(ProviderError):
                create_agent_providers(configs, registered_actors=configs, registry=registry)

    def test_custom_policy_validator_rejects_unsupported_model(self):
        def validate(config):
            if config.model != "supported-v1":
                raise ProviderError("unsupported application model")

        registry = ProviderRegistry((replace(self.registration, validate_config=validate),))
        with self.assertRaisesRegex(ProviderError, "unsupported application model"):
            self.load(registry=registry)
        self.assertEqual(self.factories, [])

    def test_registry_and_metadata_are_immutable_and_aliases_cannot_shadow(self):
        with self.assertRaises(FrozenInstanceError):
            self.registry._names = {}
        with self.assertRaises(FrozenInstanceError):
            self.registration.capabilities.tools = True
        descriptor = self.registry.descriptor()
        descriptor["local-rules"]["capabilities"]["tools"] = True
        descriptor["local-rules"]["aliases"].append("evil")
        self.assertFalse(self.registry.resolve("rules").capabilities.tools)
        self.assertNotIn("evil", self.registry.resolve("rules").aliases)
        for registration in (self.registration, replace(self.registration, provider="other",
                              aliases=("mock",))):
            with self.assertRaisesRegex(ProviderError, "already registered"):
                self.registry.with_provider(registration)

    def test_builtin_descriptors_and_cursor_alias_remain_unchanged(self):
        fixtures = {"mock": {"accepted": True}}
        configs = {name: AgentConfig(name, name, "model", 0 if name == "mock" else 123,
                                    "Classify.", self.contract, role="reviewer")
                   for name in ("mock", "openai", "cursor")}
        with patch("mas_host.provider_credentials.read_api_key", side_effect=AssertionError("secret")):
            providers = create_agent_providers(configs, registered_actors=configs, mock_outputs=fixtures)
        expected = {"mock": MockStructuredProvider("reviewer", fixtures["mock"], model="model"),
                    "openai": OpenAIStructuredProvider("model", "reviewer", reservation_microusd=123),
                    "cursor": CursorLocalProvider("model", "reviewer", reservation_microusd=123)}
        self.assertEqual({key: item.descriptor() for key, item in providers.items()},
                         {key: item.descriptor() for key, item in expected.items()})
        self.assertIs(DEFAULT_PROVIDER_REGISTRY.resolve("cursor"),
                      DEFAULT_PROVIDER_REGISTRY.resolve("cursor-local"))
        self.assertNotIn("required_capabilities", configs["mock"].descriptor())
        self.assertNotIn("structured_output_dialect", configs["mock"].descriptor())

    def test_builtin_capabilities_describe_exposed_adapters(self):
        for name in ("mock", "openai", "cursor"):
            capabilities = DEFAULT_PROVIDER_REGISTRY.resolve(name).capabilities
            for unsupported in ("tools", "sessions", "cancellation", "reconciliation", "streaming"):
                self.assertFalse(capabilities.supports(unsupported))
        self.assertTrue(DEFAULT_PROVIDER_REGISTRY.resolve("openai").capabilities.output_tokens)
        self.assertFalse(DEFAULT_PROVIDER_REGISTRY.resolve("cursor").capabilities.output_tokens)
        configs = self.load(self.settings(provider="openai", reservation_microusd=123,
                                         required_capabilities=["output_tokens"], max_output_tokens=12))
        self.assertEqual(configs["specialist"].descriptor()["required_capabilities"], ["output_tokens"])


if __name__ == "__main__":
    unittest.main()
