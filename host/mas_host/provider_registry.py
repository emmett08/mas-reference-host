"""Trusted, immutable structured-provider registrations.

Applications register Python factories in trusted startup code. JSON configuration
can select a registration, but cannot name an import, executable or factory. A
registration describes the implementation actually exposed by the host adapter,
not every feature the vendor's API might offer.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Iterable, Mapping

from .codec import canonical_json
from .cursor_transport import SDK_DEFAULT, ENVIRONMENT_PROXY_HTTP1
from .provider_contract import ProviderError
from .structured_provider import (IDENTIFIER, MockStructuredProvider,
                                  StructuredProvider, StructuredRequest, structured_binding)

if TYPE_CHECKING:
    from .agent_config import AgentConfig


STRUCTURED_DIALECT = "mas-integer-json-schema/1"
_FEATURES = frozenset({"tools", "sessions", "cancellation", "reconciliation",
                       "streaming", "output_tokens"})


@dataclass(frozen=True)
class ProviderCapabilities:
    """Public capabilities of one registered implementation, copied on output.

    ``cancellation`` means supported remote effect cancellation; killing a local
    process does not qualify. ``reconciliation`` means a provider can look up an
    uncertain invocation. Neither claim is inferred from a vendor name.
    """

    structured_output_dialects: tuple[str, ...] = (STRUCTURED_DIALECT,)
    structured_output_enforcement: str = "local-schema-validation"
    tools: bool = False
    sessions: bool = False
    cancellation: bool = False
    reconciliation: bool = False
    streaming: bool = False
    output_tokens: bool = False
    endpoints: tuple[str, ...] = ()
    endpoint_policy: str = "none"
    accounting: str = "configured-reservation"
    transport_policies: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("structured_output_dialects", "endpoints", "transport_policies"):
            values = getattr(self, name)
            if (type(values) is not tuple or len(values) > 32
                    or any(type(item) is not str or not item or len(item) > 512 for item in values)
                    or len(set(values)) != len(values)):
                raise ProviderError("provider capabilities require bounded unique identifiers")
        if (not self.structured_output_dialects
                or type(self.structured_output_enforcement) is not str
                or self.structured_output_enforcement not in {
                    "local-schema-validation", "provider-and-local-schema-validation"}
                or type(self.accounting) is not str
                or self.accounting not in {"none", "configured-reservation"}
                or type(self.endpoint_policy) is not str
                or self.endpoint_policy not in {"none", "fixed", "sdk-managed"}
                or (self.endpoint_policy == "fixed") != bool(self.endpoints)
                or any(type(getattr(self, name)) is not bool for name in _FEATURES)):
            raise ProviderError("invalid structured provider capabilities")

    def descriptor(self) -> dict:
        return {"structured_output_dialects": list(self.structured_output_dialects),
                "structured_output_enforcement": self.structured_output_enforcement,
                **{name: getattr(self, name) for name in sorted(_FEATURES)},
                "endpoints": list(self.endpoints), "endpoint_policy": self.endpoint_policy,
                "accounting": self.accounting,
                "transport_policies": list(self.transport_policies)}

    def supports(self, name: str) -> bool:
        return type(name) is str and name in _FEATURES and getattr(self, name)


@dataclass(frozen=True)
class ProviderBuildContext:
    """Factory inputs; environment and fixtures never enter registry descriptors."""

    environ: Mapping[str, str] | None = None
    fixture: dict | None = None


ProviderFactory = Callable[["AgentConfig", ProviderBuildContext], StructuredProvider]


@dataclass(frozen=True)
class ProviderRegistration:
    provider: str
    implementation: str
    factory: ProviderFactory = field(repr=False, compare=False)
    capabilities: ProviderCapabilities = field(default_factory=ProviderCapabilities)
    aliases: tuple[str, ...] = ()
    fixture_required: bool = False
    validate_config: Callable[["AgentConfig"], None] | None = field(
        default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if (any(type(value) is not str or not IDENTIFIER.fullmatch(value)
                for value in (self.provider, self.implementation))
                or type(self.aliases) is not tuple or len(self.aliases) > 32
                or any(type(value) is not str or not IDENTIFIER.fullmatch(value)
                       for value in self.aliases)
                or len(set((self.provider, *self.aliases))) != len(self.aliases) + 1
                or not callable(self.factory)
                or not isinstance(self.capabilities, ProviderCapabilities)
                or type(self.fixture_required) is not bool
                or (self.validate_config is not None and not callable(self.validate_config))):
            raise ProviderError("invalid trusted provider registration")

    def descriptor(self) -> dict:
        return {"provider": self.provider, "implementation": self.implementation,
                "contract": "structured-inference/1", "capabilities": self.capabilities.descriptor(),
                "aliases": list(self.aliases), "fixture_required": self.fixture_required}

    @property
    def identity(self) -> str:
        return hashlib.sha256(canonical_json(self.descriptor()).encode("utf-8")).hexdigest()

    def validate(self, config: AgentConfig) -> None:
        capabilities = self.capabilities
        amount = config.reservation_microusd
        if (type(amount) is not int or not 0 <= amount <= 100_000_000
                or (capabilities.accounting == "none") != (amount == 0)):
            raise ProviderError("unmetered providers require zero reservation; metered providers require a positive bounded reservation")
        if config.max_output_tokens is not None and not capabilities.output_tokens:
            raise ProviderError("registered provider does not support output token limits")
        if (config.cursor_transport != SDK_DEFAULT
                and config.cursor_transport not in capabilities.transport_policies):
            raise ProviderError("registered provider does not support the selected transport policy")
        if config.structured_output_dialect not in capabilities.structured_output_dialects:
            raise ProviderError("registered provider does not support the requested structured output dialect")
        if any(not capabilities.supports(name) for name in config.required_capabilities):
            raise ProviderError("registered provider does not support all required capabilities")
        if self.validate_config is not None:
            self.validate_config(config)


@dataclass(frozen=True, init=False)
class ProviderRegistry:
    """An immutable allow-list; extending it creates a separate registry.

    Versions are explicit implementation IDs. Replacing a factory's semantics
    requires a new implementation ID, just as changing an adapter does. Factories
    must defer secrets, optional SDK imports and effects until admitted inference.
    """

    _providers: Mapping[str, ProviderRegistration] = field(repr=False)
    _names: Mapping[str, ProviderRegistration] = field(repr=False)

    def __init__(self, registrations: Iterable[ProviderRegistration] = ()):
        providers: dict[str, ProviderRegistration] = {}
        names: dict[str, ProviderRegistration] = {}
        for registration in registrations:
            if not isinstance(registration, ProviderRegistration):
                raise ProviderError("provider registry requires trusted registrations")
            for name in (registration.provider, *registration.aliases):
                if name in names:
                    raise ProviderError("provider registration or alias is already registered")
                names[name] = registration
            providers[registration.provider] = registration
        object.__setattr__(self, "_providers", MappingProxyType(providers))
        object.__setattr__(self, "_names", MappingProxyType(names))

    def resolve(self, provider: str) -> ProviderRegistration:
        if type(provider) is not str or provider not in self._names:
            raise ProviderError("provider is not registered by this application")
        return self._names[provider]

    def with_provider(self, registration: ProviderRegistration) -> ProviderRegistry:
        return ProviderRegistry((*self._providers.values(), registration))

    def descriptor(self) -> dict:
        return {name: registration.descriptor()
                for name, registration in sorted(self._providers.items())}

    def build(self, config: AgentConfig, context: ProviderBuildContext) -> StructuredProvider:
        registration = self.resolve(config.provider)
        if registration.identity != config.provider_registration.identity:
            raise ProviderError("provider registration changed after configuration was loaded")
        registration.validate(config)
        provider = registration.factory(config, context)
        descriptor, amount = structured_binding(provider)
        if (descriptor["provider"] != registration.provider
                or descriptor["implementation"] != registration.implementation
                or descriptor["model"] != config.model or descriptor["role"] != config.role
                or amount != config.reservation_microusd):
            raise ProviderError("provider factory result conflicts with its registered identity")
        # Built-in registrations preserve their established recovery descriptor.
        # Extension metadata is part of the durable adapter/effect identity, so
        # reloading configuration cannot silently discard a changed capability,
        # dialect or endpoint policy under the same implementation identifier.
        return provider if is_default_registration(registration) else RegisteredStructuredProvider(
            provider, registration)


@dataclass(frozen=True)
class RegisteredStructuredProvider:
    provider: StructuredProvider
    registration: ProviderRegistration

    @property
    def reservation_microusd(self):
        return self.provider.reservation_microusd

    def descriptor(self):
        descriptor, _ = structured_binding(self.provider)
        return {**descriptor, "registration": self.registration.descriptor()}

    def infer(self, request: StructuredRequest, *, request_id: str | None = None):
        return self.provider.infer(request, request_id=request_id)


def is_default_registration(registration: ProviderRegistration) -> bool:
    """Only exact shipped metadata receives the compatibility descriptor."""
    try:
        default = DEFAULT_PROVIDER_REGISTRY.resolve(registration.provider)
    except ProviderError:
        return False
    return registration.identity == default.identity


def _mock(config: AgentConfig, context: ProviderBuildContext) -> StructuredProvider:
    return MockStructuredProvider(config.role, context.fixture, model=config.model)


def _openai(config: AgentConfig, context: ProviderBuildContext) -> StructuredProvider:
    from .structured_openai import OpenAIStructuredProvider
    return OpenAIStructuredProvider(config.model, config.role, environ=context.environ,
        reservation_microusd=config.reservation_microusd,
        max_output_tokens=4096 if config.max_output_tokens is None else config.max_output_tokens)


def _cursor(config: AgentConfig, context: ProviderBuildContext) -> StructuredProvider:
    from .cursor_provider import CursorLocalProvider
    return CursorLocalProvider(config.model, config.role, environ=context.environ,
        reservation_microusd=config.reservation_microusd, transport_policy=config.cursor_transport)


def _cursor_config(config: AgentConfig) -> None:
    if config.model == "auto-smart":
        raise ProviderError("Cursor requires an explicit catalogue model")


DEFAULT_PROVIDER_REGISTRY = ProviderRegistry((
    ProviderRegistration("mock", "mock-structured/1", _mock,
        ProviderCapabilities(accounting="none"), fixture_required=True),
    ProviderRegistration("openai", "openai-responses-structured/1", _openai,
        ProviderCapabilities(structured_output_enforcement="provider-and-local-schema-validation",
                             output_tokens=True, endpoint_policy="fixed",
                             endpoints=("https://api.openai.com/v1",))),
    ProviderRegistration("cursor-local", "cursor-local-structured/1", _cursor,
        ProviderCapabilities(endpoint_policy="sdk-managed",
                             transport_policies=(SDK_DEFAULT, ENVIRONMENT_PROXY_HTTP1)),
        aliases=("cursor",),
        validate_config=_cursor_config),
))
