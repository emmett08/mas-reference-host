"""Trusted, bounded observations of provider execution, independent of retries.

An adapter may assert ``not_sent`` only before its provider dispatch boundary.
An observed completed response can be unusable without becoming an unknown
effect. These records never authorise automatic retry or refund allocations.
"""
from __future__ import annotations

import json
import re

from .codec import integer_json
from .contracts import RunnerError
from .provider_contract import ProviderError


_CATEGORIES = {
    "credentials_unavailable", "runtime_unavailable", "client_initialisation_failed",
    "invalid_request", "operation_busy", "invalid_output", "refused_output", "forbidden_tool_call", "cleanup_failed",
}
_PROVIDERS = {"openai", "cursor-local", "mock", "custom"}
_STAGES = {"readiness", "launch", "create", "send", "stream", "wait", "result", "cleanup"}
_IDS = {"client_request_id", "server_request_id", "response_id", "agent_id", "run_id"}
_TOKENS = {"input_tokens", "output_tokens", "total_tokens", "cache_read_tokens",
           "cache_write_tokens", "reasoning_tokens"}


def validate_provider_diagnostic(value: object) -> dict:
    """Public observations of a failed exchange; none establish non-execution."""
    required = {"format", "provider", "stage", "category", "cleanup_failed", "recovery_action"}
    optional = {"http_status", "server_request_id", "retry_after_seconds"}
    if (type(value) is not dict or not required.issubset(value) or set(value) - required - optional
            or value["format"] != "mas-provider-diagnostic/1"
            or type(value["provider"]) is not str or value["provider"] not in _PROVIDERS
            or type(value["stage"]) is not str or value["stage"] not in _STAGES
            or type(value["category"]) is not str
            or value["category"] not in {"transport_failure", "transport_timeout", "provider_http_error", "response_incomplete"}
            or type(value["cleanup_failed"]) is not bool
            or value["recovery_action"] != "reconcile_before_repeating"):
        raise ValueError("invalid provider diagnostic")
    if "http_status" in value and (type(value["http_status"]) is not int or not 100 <= value["http_status"] <= 599):
        raise ValueError("invalid provider diagnostic")
    if "server_request_id" in value and (type(value["server_request_id"]) is not str
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", value["server_request_id"])):
        raise ValueError("invalid provider diagnostic")
    if "retry_after_seconds" in value and (type(value["retry_after_seconds"]) is not int
            or not 0 <= value["retry_after_seconds"] <= 86400):
        raise ValueError("invalid provider diagnostic")
    return json.loads(integer_json(value, 2048))


def unknown_diagnostic(provider: str, stage: str, error: Exception, *, cleanup_failed=False) -> dict:
    """Whitelist SDK metadata; HTTP errors remain effect-uncertain by default."""
    value = {"format": "mas-provider-diagnostic/1", "provider": provider, "stage": stage,
             "category": "transport_timeout" if isinstance(error, TimeoutError) or
                type(error).__name__ == "APITimeoutError" else "transport_failure",
             "cleanup_failed": cleanup_failed, "recovery_action": "reconcile_before_repeating"}
    try:
        status = getattr(error, "status_code", None)
        if type(status) is int and 100 <= status <= 599:
            value.update(http_status=status, category="provider_http_error")
        identifier = getattr(error, "request_id", None)
        if type(identifier) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,200}", identifier):
            value["server_request_id"] = identifier
        headers = getattr(getattr(error, "response", None), "headers", {})
        retry = headers.get("retry-after")
        if type(retry) is str and retry.isascii() and retry.isdecimal() and len(retry) <= 5:
            seconds = int(retry)
            if seconds <= 86400:
                value["retry_after_seconds"] = seconds
    except Exception:
        pass  # Metadata extraction must never expose or replace the primary error.
    return validate_provider_diagnostic(value)


def validate_provider_outcome(value: object) -> dict:
    """Accept a closed public schema, never arbitrary SDK data or exception text."""
    required = {"format", "provider", "certainty", "output_status", "category", "stage",
                "recovery_action", "automatic_retry", "completion", "cleanup_failed"}
    if (type(value) is not dict or not required.issubset(value) or set(value) - required - {"retry_after_seconds"}
            or value["format"] != "mas-provider-outcome/1"
            or type(value["provider"]) is not str or value["provider"] not in _PROVIDERS
            or type(value["category"]) is not str or value["category"] not in _CATEGORIES
            or type(value["stage"]) is not str or value["stage"] not in _STAGES
            or type(value["cleanup_failed"]) is not bool
            or value["automatic_retry"] is not False):
        raise ValueError("invalid provider outcome")
    if "retry_after_seconds" in value and (value["certainty"] != "not_sent"
            or type(value["retry_after_seconds"]) is not int or not 0 <= value["retry_after_seconds"] <= 86400):
        raise ValueError("invalid provider outcome")
    certainty, validity = value["certainty"], value["output_status"]
    if certainty == "not_sent":
        if (validity != "unavailable" or value["completion"] != {}
                or value["recovery_action"] != "correct_configuration_and_submit_new_work"
                or value["category"] not in {"credentials_unavailable", "runtime_unavailable",
                                             "client_initialisation_failed", "invalid_request", "operation_busy"}
                or value["stage"] != "readiness"):
            raise ValueError("invalid provider outcome")
    elif certainty == "completed":
        if validity == "valid":
            if (value["category"] != "cleanup_failed" or value["stage"] != "cleanup"
                    or value["cleanup_failed"] is not True
                    or value["recovery_action"] != "complete_cleanup_before_using_output"):
                raise ValueError("invalid provider outcome")
        elif (validity not in ("invalid", "refused") or value["stage"] != "result"
                or value["recovery_action"] != "inspect_completion_and_submit_new_work"
                or (validity == "refused") != (value["category"] == "refused_output")
                or value["category"] not in {"invalid_output", "refused_output", "forbidden_tool_call"}):
            raise ValueError("invalid provider outcome")
    else:
        raise ValueError("invalid provider outcome")
    completion = value["completion"]
    if type(completion) is not dict or set(completion) - _IDS - {"usage", "request_sha256", "reserved_microusd"}:
        raise ValueError("invalid provider outcome")
    for key, item in completion.items():
        if key in _IDS:
            if type(item) is not str or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", item):
                raise ValueError("invalid provider outcome")
        elif key == "request_sha256":
            if type(item) is not str or not re.fullmatch(r"[0-9a-f]{64}", item):
                raise ValueError("invalid provider outcome")
        elif key == "reserved_microusd":
            if type(item) is not int or not 0 <= item <= 100_000_000:
                raise ValueError("invalid provider outcome")
        elif (type(item) is not dict or set(item) - _TOKENS
              or any(type(count) is not int or not 0 <= count <= 10**9 for count in item.values())):
            raise ValueError("invalid provider outcome")
    return json.loads(integer_json(value, 8192))


def completion_metadata(evidence: dict) -> dict:
    """Copy only declared public completion observations from trusted evidence."""
    result = {}
    for key in _IDS:
        value = evidence.get(key)
        if type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", value):
            result[key] = value
    digest = evidence.get("request_sha256")
    if type(digest) is str and re.fullmatch(r"[0-9a-f]{64}", digest):
        result["request_sha256"] = digest
    amount = evidence.get("reserved_microusd")
    if type(amount) is int and 0 <= amount <= 100_000_000:
        result["reserved_microusd"] = amount
    usage = evidence.get("usage")
    if type(usage) is dict:
        checked = {key: count for key, count in usage.items()
                   if key in _TOKENS and type(count) is int and 0 <= count <= 10**9}
        if checked:
            result["usage"] = checked
    return result


def validate_completed_output(outcome: dict, output: object) -> dict | None:
    """Private bounded payload, deliberately absent from public diagnostics."""
    if output is None:
        return None
    if outcome["certainty"] != "completed" or outcome["output_status"] != "valid" or type(output) is not dict:
        raise ValueError("invalid retained provider output")
    return json.loads(integer_json(output, 32_768))


class ProviderOutcomeFailure(ProviderError):
    """Trusted provider observation; the host revalidates it before checkpointing."""

    def __init__(self, outcome: dict, *, completed_output: dict | None = None):
        checked = validate_provider_outcome(outcome)
        output = validate_completed_output(checked, completed_output)
        legacy = ({"provider": "cursor-local", "stage": checked["stage"],
                   "cleanup_failed": checked["cleanup_failed"]}
                  if checked["provider"] == "cursor-local" and checked["stage"] != "readiness" else None)
        super().__init__("provider execution: " + checked["category"], diagnostic=legacy)
        self._outcome = integer_json(checked, 8192)
        self._completed_output = integer_json(output, 32_768) if output is not None else None

    @property
    def outcome(self) -> dict:
        return json.loads(self._outcome)

    @property
    def completed_output(self) -> dict | None:
        return json.loads(self._completed_output) if self._completed_output is not None else None

    def with_cleanup_failure(self):
        outcome = self.outcome
        outcome["cleanup_failed"] = True
        return ProviderOutcomeFailure(outcome, completed_output=self.completed_output)


def not_sent(provider: str, category: str, *, retry_after_seconds: int | None = None) -> ProviderOutcomeFailure:
    return ProviderOutcomeFailure({"format": "mas-provider-outcome/1", "provider": provider,
        "certainty": "not_sent", "output_status": "unavailable", "category": category,
        "stage": "readiness", "recovery_action": "correct_configuration_and_submit_new_work",
        "automatic_retry": False, "completion": {}, "cleanup_failed": False,
        **({"retry_after_seconds": retry_after_seconds} if retry_after_seconds is not None else {})})


def completed_unusable(provider: str, category: str, evidence: dict) -> ProviderOutcomeFailure:
    return ProviderOutcomeFailure({"format": "mas-provider-outcome/1", "provider": provider,
        "certainty": "completed", "output_status": "refused" if category == "refused_output" else "invalid",
        "category": category, "stage": "result",
        "recovery_action": "inspect_completion_and_submit_new_work", "automatic_retry": False,
        "completion": completion_metadata(evidence), "cleanup_failed": False})


def completed_cleanup_failure(provider: str, evidence: dict, output: dict) -> ProviderOutcomeFailure:
    """A valid response does not disappear when local resource cleanup fails."""
    outcome = {"format": "mas-provider-outcome/1", "provider": provider,
        "certainty": "completed", "output_status": "valid", "category": "cleanup_failed",
        "stage": "cleanup", "recovery_action": "complete_cleanup_before_using_output",
        "automatic_retry": False, "completion": completion_metadata(evidence), "cleanup_failed": True}
    try:
        retained = validate_completed_output(outcome, output)
    except (TypeError, ValueError, RecursionError, RunnerError):
        # Completion certainty survives even when a custom adapter's output is
        # outside the bounded private-output contract. Never copy it publicly.
        retained = None
    return ProviderOutcomeFailure(outcome, completed_output=retained)
