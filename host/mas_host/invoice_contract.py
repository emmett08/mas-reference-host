"""Exact input and output contract shared by the invoice example providers."""
from __future__ import annotations

import json
from typing import Any

from .provider_contract import ProviderError

MAX_TOTAL_CENTS = 10**12


def validate_invoice(invoice: Any) -> list[dict[str, Any]]:
    """Keep this demo's application contract explicit and its arithmetic bounded."""
    if not isinstance(invoice, list) or not 1 <= len(invoice) <= 25:
        raise ValueError("invoice must contain between 1 and 25 lines")
    checked = []
    for line in invoice:
        if not isinstance(line, dict) or set(line) != {"item", "quantity", "unit_cents"}:
            raise ValueError("invoice lines require item, quantity and unit_cents")
        if not isinstance(line["item"], str) or not 1 <= len(line["item"]) <= 100:
            raise ValueError("invoice item must be a short nonempty string")
        quantity, unit_cents = line["quantity"], line["unit_cents"]
        if type(quantity) is not int or not 1 <= quantity <= 1000:
            raise ValueError("invoice quantity must be an integer between 1 and 1000")
        if type(unit_cents) is not int or not 0 <= unit_cents <= 10**7:
            raise ValueError("invoice unit_cents must be an integer between 0 and 10000000")
        checked.append(dict(line))
    if sum(line["quantity"] * line["unit_cents"] for line in checked) > MAX_TOTAL_CENTS:
        raise ValueError("invoice total exceeds the demo limit")
    return checked


def exact_total(invoice: Any) -> int:
    return sum(line["quantity"] * line["unit_cents"] for line in validate_invoice(invoice))


def validate_total(output: Any) -> dict[str, int]:
    if not isinstance(output, dict) or set(output) != {"total_cents"}:
        raise ProviderError("provider output must contain only total_cents")
    value = output["total_cents"]
    if type(value) is not int or not 0 <= value <= MAX_TOTAL_CENTS:
        raise ProviderError("provider total_cents must be a bounded nonnegative integer")
    return {"total_cents": value}


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _reject_number(_: str) -> None:
    raise ValueError("non-integer number")


def parse_total_output(text: Any, provider: str) -> dict[str, int]:
    """Reject oversized, ambiguous or non-integer application results."""
    try:
        valid_text = isinstance(text, str) and len(text.encode("utf-8")) <= 4096
    except UnicodeError:
        valid_text = False
    if not valid_text:
        raise ProviderError(f"{provider} returned an invalid output body")
    try:
        return validate_total(json.loads(text, object_pairs_hook=_unique_object,
                                        parse_float=_reject_number, parse_constant=_reject_number))
    except (ValueError, TypeError, ProviderError, RecursionError):
        raise ProviderError(f"{provider} returned an invalid invoice total") from None
