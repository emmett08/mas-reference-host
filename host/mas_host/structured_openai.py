"""Structured inference over the same bounded OpenAI Responses client policy."""
from __future__ import annotations

import json
import re
from typing import Any, Mapping

from .contracts import Invocation
from .openai_client import response_client
from .provider_contract import ProviderError
from .provider_outcomes import (ProviderOutcomeFailure, completed_unusable,
                                completed_cleanup_failure, unknown_diagnostic)
from .structured_provider import IDENTIFIER, MAX_OUTPUT_BYTES, StructuredRequest, validate_trace_id


class OpenAIStructuredProvider:
    def __init__(self, model: str, role: str, *, api_key: str | None = None,
                 client: Any = None, reservation_microusd: int = 100_000,
                 max_output_tokens: int = 4096, environ: Mapping[str, str] | None = None):
        if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= 16_384:
            raise ProviderError("structured output requires a bounded positive token limit")
        if type(model) is not str or not IDENTIFIER.fullmatch(model):
            raise ProviderError("model must be an explicit valid model identifier")
        if type(role) is not str or not IDENTIFIER.fullmatch(role):
            raise ProviderError("role must be an explicit valid role identifier")
        if type(reservation_microusd) is not int or not 1 <= reservation_microusd <= 100_000_000:
            raise ProviderError("OpenAI calls require a positive bounded spending reservation")
        self.model, self.role = model, role
        self.reservation_microusd = reservation_microusd
        self.max_output_tokens = max_output_tokens
        # A registered provider is a configuration binding, not an SDK session.
        # Secrets and optional dependencies are needed only by admitted calls.
        self._api_key, self._client, self._environ = api_key, client, environ

    def descriptor(self) -> dict:
        return {"provider": "openai", "model": self.model, "role": self.role,
                "contract": "structured-inference/1", "implementation": "openai-responses-structured/1",
                "endpoint": "https://api.openai.com/v1", "tools": [], "store": False,
                "timeout_seconds": 45, "max_retries": 0,
                "max_output_tokens": self.max_output_tokens, "max_output_bytes": MAX_OUTPUT_BYTES,
                "reservation_microusd": self.reservation_microusd}

    def infer(self, request: StructuredRequest, *, request_id: str | None = None) -> Invocation:
        validate_trace_id(request_id)
        supplied = request.descriptor
        stage = "readiness"
        invocation = None
        try:
            with response_client(client=self._client, api_key=self._api_key,
                                 environ=self._environ) as client:
                stage = "send"
                response = client.responses.create(
                    model=self.model,
                    input=[{"role": "developer", "content": supplied["instructions"]},
                           {"role": "user", "content": json.dumps(supplied["input"], separators=(",", ":"))}],
                    text={"format": {"type": "json_schema", "name": supplied["schema_name"],
                                     "strict": True, "schema": supplied["output_schema"]}},
                    tools=[], store=False, max_output_tokens=self.max_output_tokens,
                    extra_headers={"X-Client-Request-Id": request_id} if request_id else {})
                # Classify and retain completion before owned-client teardown;
                # a cleanup error cannot erase a known invalid/refused response.
                stage = "result"
                invocation = self._completed_response(response, request, request_id)
                stage = "cleanup"
                return invocation
        except ProviderOutcomeFailure:
            raise
        except BaseException as error:
            if stage == "cleanup" and invocation is not None:
                raise completed_cleanup_failure("openai", invocation.evidence, invocation.output) from None
            if not isinstance(error, Exception):
                raise
            diagnostic = unknown_diagnostic("openai", stage, error, cleanup_failed=stage == "cleanup")
            if stage == "result":
                diagnostic["category"] = "response_incomplete"
            raise ProviderError("OpenAI invocation failed; its external outcome may be unknown",
                                diagnostic=diagnostic) from None

    def readiness(self) -> dict:
        """Check local SDK/credential construction without submitting inference."""
        with response_client(client=self._client, api_key=self._api_key,
                             environ=self._environ):
            pass
        return {"provider": "openai", "ready": True, "inference_submitted": False}

    def _completed_response(self, response, request, request_id):
        if getattr(response, "status", None) != "completed":
            raise ProviderError("OpenAI response did not complete")
        evidence = {"provider": "openai", "model": self.model, "role": self.role,
                    "request_sha256": request.identity,
                    "reserved_microusd": self.reservation_microusd,
                    "spending_basis": "configured_reservation_not_provider_invoice"}
        if request_id:
            evidence["client_request_id"] = request_id
        for field, value, pattern in (
                ("server_request_id", getattr(response, "_request_id", None), r"[A-Za-z0-9_-]{1,200}"),
                ("response_id", getattr(response, "id", None), r"resp_[A-Za-z0-9_-]{1,200}")):
            if type(value) is str and re.fullmatch(pattern, value):
                evidence[field] = value
        usage = getattr(response, "usage", None)
        tokens = {field: getattr(usage, field) for field in ("input_tokens", "output_tokens", "total_tokens")
                  if type(getattr(usage, field, None)) is int and 0 <= getattr(usage, field) <= 10**9}
        if tokens:
            evidence["usage"] = tokens
        for item in getattr(response, "output", []) or []:
            if getattr(item, "type", None) in {"function_call", "custom_tool_call"}:
                raise completed_unusable("openai", "forbidden_tool_call", evidence)
            for content in getattr(item, "content", []) or []:
                if getattr(content, "type", None) == "refusal":
                    raise completed_unusable("openai", "refused_output", evidence)
        try:
            output = request.parse_output(getattr(response, "output_text", None))
        except ProviderError:
            raise completed_unusable("openai", "invalid_output", evidence) from None
        return Invocation(output, evidence)
