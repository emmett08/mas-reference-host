"""Validate per-agent provider settings and construct only the selected strategies."""
from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Callable, Mapping

from .provider_contract import ProviderError, TotalProvider
from .provider_credentials import CredentialError, read_api_key
from .cursor_transport import SDK_DEFAULT, validate_transport_policy
from .mock_provider import MockProvider
from .openai_provider import OpenAIProvider


@dataclass(frozen=True)
class ProviderConfig:
    role: str
    provider: str
    model: str
    reservation_microusd: int
    cursor_transport: str = SDK_DEFAULT

    def __post_init__(self) -> None:
        if self.role not in {"planner", "reviewer"}:
            raise ProviderError("provider role must be planner or reviewer")
        if self.provider not in {"mock", "openai", "cursor"}:
            raise ProviderError("provider must be mock, openai or cursor")
        if not isinstance(self.model, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", self.model):
            raise ProviderError("selected provider requires a valid explicit model identifier")
        amount = self.reservation_microusd
        if type(amount) is not int or not 0 <= amount <= 100_000_000:
            raise ProviderError("provider requires a bounded integer spending reservation")
        if (self.provider == "mock") != (amount == 0):
            raise ProviderError("mock reservations must be zero and live reservations must be positive")
        try:
            validate_transport_policy(self.cursor_transport)
        except ValueError:
            raise ProviderError("Cursor transport must be sdk-default or environment-proxy-http1") from None
        if self.provider != "cursor" and self.cursor_transport != SDK_DEFAULT:
            raise ProviderError("Cursor transport applies only to Cursor providers")


def load_provider_configs(mode: str = "mock", *, environ: Mapping[str, str] | None = None) -> dict[str, ProviderConfig]:
    """Mock stays offline; live modes supply defaults and mixed requires both choices."""
    environment = os.environ if environ is None else environ
    if mode not in {"mock", "openai", "cursor", "mixed"}:
        raise ProviderError("MAS_MODE must be mock, openai, cursor or mixed")
    configs = {}
    for role in ("planner", "reviewer"):
        prefix = "MAS_" + role.upper()
        provider = "mock" if mode == "mock" else environment.get(
            prefix + "_PROVIDER", "" if mode == "mixed" else mode)
        if provider not in {"mock", "openai", "cursor"}:
            raise ProviderError("mixed mode requires a valid provider for both planner and reviewer")
        if provider == "mock":
            # Preserve the original mock identity even when Compose supplies model defaults.
            model, amount = "deterministic-invoice", 0
        else:
            default_model = {"planner": "gpt-4.1-mini", "reviewer": "gpt-4o-mini"}[role] if provider == "openai" else ""
            model = environment.get(prefix + "_MODEL", default_model)
            try:
                amount = int(environment.get(prefix + "_RESERVATION_MICROUSD",
                    environment.get("MAS_CALL_RESERVATION_MICROUSD", "100000")))
            except (ValueError, TypeError):
                raise ProviderError("provider spending reservation must be an integer") from None
        transport = environment.get("MAS_CURSOR_TRANSPORT", SDK_DEFAULT) if provider == "cursor" else SDK_DEFAULT
        configs[role] = ProviderConfig(role, provider, model, amount, transport)
    return configs


def _mock(config: ProviderConfig, _key: str | None) -> TotalProvider:
    return MockProvider(config.role)


def _openai(config: ProviderConfig, key: str | None) -> TotalProvider:
    return OpenAIProvider(config.model, config.role, api_key=key,
                          reservation_microusd=config.reservation_microusd)


def _cursor(config: ProviderConfig, key: str | None) -> TotalProvider:
    from .invoice_cursor import InvoiceCursorProvider
    from .cursor_session import check_runtime
    check_runtime()
    return InvoiceCursorProvider(config.model, config.role, api_key=key,
                               reservation_microusd=config.reservation_microusd,
                               transport_policy=config.cursor_transport)


_FACTORIES: dict[str, Callable[[ProviderConfig, str | None], TotalProvider]] = {
    "mock": _mock, "openai": _openai, "cursor": _cursor,
}


def create_providers(configs: Mapping[str, ProviderConfig], *,
                     environ: Mapping[str, str] | None = None) -> dict[str, TotalProvider]:
    """Validate all credentials before constructing clients; construction submits no job."""
    if set(configs) != {"planner", "reviewer"} or any(
        not isinstance(config, ProviderConfig) or role != config.role for role, config in configs.items()
    ):
        raise ProviderError("provider configuration requires matching planner and reviewer roles")
    credentials = {name: read_api_key(name, environ=environ)
                   for name in sorted({item.provider for item in configs.values()} - {"mock"})}
    return {role: _FACTORIES[config.provider](config, credentials.get(config.provider))
            for role, config in configs.items()}


def selected_mode(configs: Mapping[str, ProviderConfig]) -> str:
    names = {config.provider for config in configs.values()}
    return next(iter(names)) if len(names) == 1 else "mixed"


def selected_models(configs: Mapping[str, ProviderConfig]) -> dict[str, str]:
    return {**{role: "mock" if config.provider == "mock" else config.model
               for role, config in configs.items()}, "checker": "python"}
