"""Explicit arbitrary-actor configuration for structured inference strategies.

The original invoice ProviderConfig remains a compatibility interface. This
module accepts any registered actor and binds instructions and operation schemas
without introducing application-specific roles into the host.
"""
from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import Iterable, Mapping

from .codec import is_actor_identity
from .cursor_transport import SDK_DEFAULT, validate_transport_policy
from .operation_contracts import OperationContract
from .provider_contract import ProviderError
from .provider_registry import (DEFAULT_PROVIDER_REGISTRY, STRUCTURED_DIALECT, ProviderBuildContext,
                                ProviderRegistration, ProviderRegistry, is_default_registration)
from .structured_provider import (IDENTIFIER, StructuredAgentAdapter,
                                  StructuredProvider, StructuredRequest, _copy)


@dataclass(frozen=True)
class AgentConfig:
    actor: str
    provider: str
    model: str
    reservation_microusd: int
    instructions: str
    contract: OperationContract
    role: str = "agent"
    max_output_tokens: int | None = None
    cursor_transport: str = SDK_DEFAULT
    structured_output_dialect: str = STRUCTURED_DIALECT
    required_capabilities: tuple[str, ...] = ()
    registry: InitVar[ProviderRegistry | None] = None
    provider_registration: ProviderRegistration = field(init=False, repr=False)

    def __post_init__(self, registry: ProviderRegistry | None) -> None:
        registry = DEFAULT_PROVIDER_REGISTRY if registry is None else registry
        if not isinstance(registry, ProviderRegistry):
            raise ProviderError("agent requires a trusted provider registry")
        registration = registry.resolve(self.provider)
        if not is_actor_identity(self.actor):
            raise ProviderError("agent requires a valid registered actor identity")
        for value in (self.role, self.model):
            if type(value) is not str or not IDENTIFIER.fullmatch(value):
                raise ProviderError("agent requires explicit valid actor, role and model identities")
        if self.max_output_tokens is not None and (type(self.max_output_tokens) is not int
                or not 1 <= self.max_output_tokens <= 16_384):
            raise ProviderError("agent requires a bounded positive output token limit")
        try:
            validate_transport_policy(self.cursor_transport)
        except (ValueError, TypeError):
            raise ProviderError("invalid Cursor transport policy") from None
        if (type(self.structured_output_dialect) is not str
                or not IDENTIFIER.fullmatch(self.structured_output_dialect)
                or type(self.required_capabilities) is not tuple
                or len(self.required_capabilities) > 32
                or any(type(name) is not str or not IDENTIFIER.fullmatch(name)
                       for name in self.required_capabilities)
                or len(set(self.required_capabilities)) != len(self.required_capabilities)):
            raise ProviderError("agent requires valid structured dialect and capability identifiers")
        if not isinstance(self.contract, OperationContract):
            raise ProviderError("agent requires a registered operation contract")
        StructuredRequest({}, self.instructions, self.contract.output_schema,
                          schema_name="operation_result", schema_version=self.contract.operation)
        registration.validate(self)
        object.__setattr__(self, "provider_registration", registration)

    def descriptor(self) -> dict:
        # Everything is public configuration. Credentials are read separately.
        descriptor = {"actor": self.actor, "role": self.role, "provider": self.provider,
                      "model": self.model, "reservation_microusd": self.reservation_microusd,
                      "instructions": self.instructions, "contract": self.contract.descriptor,
                      "max_output_tokens": self.max_output_tokens,
                      "cursor_transport": self.cursor_transport}
        # Preserve persisted identities for the established default configuration.
        if self.structured_output_dialect != STRUCTURED_DIALECT or self.required_capabilities:
            descriptor.update(structured_output_dialect=self.structured_output_dialect,
                              required_capabilities=list(self.required_capabilities))
        if not is_default_registration(self.provider_registration):
            descriptor["provider_registration"] = self.provider_registration.descriptor()
        return _copy(descriptor)


def create_agent_providers(configs: Mapping[str, AgentConfig], *, registered_actors: Iterable[str],
                           mock_outputs: Mapping[str, dict] | None = None,
                           environ: Mapping[str, str] | None = None,
                           registry: ProviderRegistry | None = None) -> dict[str, StructuredProvider]:
    """Validate identities and fixtures; register live providers without opening clients.

Only configured model actors need providers: tool/human adapters may occupy the
remaining registered actors. Live credential sources are read only during an
admitted inference, so inspection and cancellation work without provider secrets
or an installed SDK/runtime. Credentials are not part of recovery descriptors.
"""
    registry = DEFAULT_PROVIDER_REGISTRY if registry is None else registry
    if not isinstance(registry, ProviderRegistry):
        raise ProviderError("agent providers require a trusted provider registry")
    registered = set(registered_actors)
    if (not configs or not set(configs).issubset(registered)
            or any(not isinstance(item, AgentConfig) or actor != item.actor
                   for actor, item in configs.items())):
        raise ProviderError("provider configuration must name matching registered actors")
    fixtures = {} if mock_outputs is None else dict(mock_outputs)
    # Validate the complete batch before invoking any trusted factory.
    for item in configs.values():
        registration = registry.resolve(item.provider)
        if registration.identity != item.provider_registration.identity:
            raise ProviderError("provider registration changed after configuration was loaded")
        registration.validate(item)
    if set(fixtures) != {actor for actor, item in configs.items()
                        if item.provider_registration.fixture_required}:
        raise ProviderError("each fixture provider actor requires exactly one explicit output")
    fixtures = {actor: configs[actor].contract.validate_output(output)
                for actor, output in fixtures.items()}
    return {actor: registry.build(item, ProviderBuildContext(environ=environ,
                                                            fixture=fixtures.get(actor)))
            for actor, item in configs.items()}


def create_agent_adapters(configs: Mapping[str, AgentConfig], *, registered_actors: Iterable[str],
                          mock_outputs: Mapping[str, dict] | None = None,
                          environ: Mapping[str, str] | None = None,
                          registry: ProviderRegistry | None = None) -> dict[str, StructuredAgentAdapter]:
    providers = create_agent_providers(configs, registered_actors=registered_actors,
                                      mock_outputs=mock_outputs, environ=environ, registry=registry)
    return {actor: StructuredAgentAdapter(provider, instructions=configs[actor].instructions,
                                         contract=configs[actor].contract, actor=actor)
            for actor, provider in providers.items()}


def load_agent_configs(value: Mapping[str, dict], *,
                       contracts: Mapping[str, OperationContract],
                       registry: ProviderRegistry | None = None) -> dict[str, AgentConfig]:
    """Load public JSON settings using only application-authorised registrations."""
    if type(value) is not dict or not value:
        raise ProviderError("agent configuration must be a nonempty object")
    required = {"provider", "model", "reservation_microusd", "instructions", "operation"}
    optional = {"role", "max_output_tokens", "cursor_transport",
                "structured_output_dialect", "required_capabilities"}
    configs = {}
    for actor, item in value.items():
        if (type(item) is not dict or not required.issubset(item)
                or set(item) - required - optional or type(item.get("operation")) is not str
                or item["operation"] not in contracts):
            raise ProviderError("agent configuration requires a registered operation and explicit fields")
        contract = contracts[item["operation"]]
        if contract.operation != item["operation"]:
            raise ProviderError("registered operation identity does not match configuration")
        supplied = {key: val for key, val in item.items() if key != "operation"}
        if "required_capabilities" in supplied:
            if type(supplied["required_capabilities"]) is not list:
                raise ProviderError("required provider capabilities must be an array")
            supplied["required_capabilities"] = tuple(supplied["required_capabilities"])
        configs[actor] = AgentConfig(actor=actor, contract=contract, registry=registry, **supplied)
    return configs
