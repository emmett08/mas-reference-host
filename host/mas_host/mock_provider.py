"""Deterministic offline implementation of the invoice provider contract."""
from __future__ import annotations

from typing import Any

from .contracts import Invocation
from .invoice_contract import exact_total, validate_total


class MockProvider:
    """Offline provider with the same invocation interface and output schema."""

    def __init__(self, role: str):
        self.role = role
        self.reservation_microusd = 0

    def descriptor(self) -> dict:
        return {"provider": "mock", "model": "deterministic-invoice", "role": self.role,
                "contract": "invoice-total/2"}

    def total(self, invoice: list[dict[str, Any]], *, proposal: dict[str, int] | None = None,
              request_id: str | None = None) -> Invocation:
        if proposal is not None:
            validate_total(proposal)
        return Invocation(
            output={"total_cents": exact_total(invoice)},
            evidence={"provider": "mock", "model": "deterministic-invoice", "role": self.role, "network": False},
        )
