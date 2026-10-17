"""Public inference identity and admission contract, independent of any SDK."""
from __future__ import annotations

import json
from typing import Any, Protocol

from .codec import integer_json
from .contracts import Invocation, InvocationFailure

class ProviderError(InvocationFailure):
    """Safe public error: never include a provider response, prompt or credential."""


class TotalProvider(Protocol):
    """Explicit inference identity and admission allocation, including zero-cost mocks.

    The descriptor identifies every configuration value that can change the
    external effect. It is public recovery metadata and must contain no secrets.
    """

    @property
    def reservation_microusd(self) -> int: ...

    def descriptor(self) -> dict: ...

    def total(self, invoice: list[dict[str, Any]], *, proposal: dict[str, int] | None = None,
              request_id: str | None = None) -> Invocation: ...


def provider_binding(provider: TotalProvider) -> tuple[dict, int]:
    """Validate a provider before admission; never infer identity or free execution."""
    try:
        if not callable(provider.total) or not callable(provider.descriptor):
            raise ProviderError("provider must implement total and descriptor")
        amount = provider.reservation_microusd
        if type(amount) is not int or not 0 <= amount <= 100_000_000:
            raise ProviderError("provider must declare a bounded nonnegative spending reservation")
        descriptor = provider.descriptor()
        if type(descriptor) is not dict or any(
            type(descriptor.get(name)) is not str or not descriptor[name]
            for name in ("provider", "model", "contract")
        ):
            raise ProviderError("provider descriptor requires provider, model and contract identities")
        descriptor = json.loads(integer_json(descriptor))
        if "reservation_microusd" in descriptor and (
            type(descriptor["reservation_microusd"]) is not int
            or descriptor["reservation_microusd"] != amount
        ):
            raise ProviderError("provider descriptor conflicts with its spending reservation")
        # Existing explicit built-in bindings remain resumable. Zero is an
        # explicitly required provider attribute; it is never a missing-value default.
        if amount:
            descriptor["reservation_microusd"] = amount
        return descriptor, amount
    except ProviderError:
        raise
    except Exception:
        raise ProviderError("provider identity or spending reservation is invalid") from None
