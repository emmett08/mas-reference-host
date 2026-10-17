"""Authenticated service client and bounded HTTP transport.

No automatic POST retries occur. Callers retain their original request key to
recover the same durable receipt after an interrupted submission.
"""
from __future__ import annotations

import base64
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit

from ..codec import integer_json
from ..contracts import RunnerError
from ..service_contracts import ServiceError
from ._http_worker import PHASES, TIMEOUT_EXIT
from .authentication import _principal, _unique, read_token_file, validate_token

MAX_WIRE_BYTES = 1_048_576

def validate_endpoint(endpoint, *, allow_loopback_http=False):
    try:
        parsed = urlsplit(endpoint)
        loopback = False
        try:
            loopback = ipaddress.ip_address(parsed.hostname or "").is_loopback
        except ValueError:
            pass
        if (type(endpoint) is not str or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment
                or any(ord(c) <= 32 or ord(c) >= 127 for c in endpoint)
                or parsed.scheme != "https" and not (
                    allow_loopback_http and parsed.scheme == "http" and loopback)):
            raise ValueError()
        parsed.port
        return parsed
    except (ValueError, TypeError):
        raise ServiceError("invalid_endpoint") from None


def http_exchange(endpoint, *, method="POST", body=None, headers=None,
                  timeout_seconds=5, max_response_bytes=65536, allow_loopback_http=False,
                  sse_response_id=None, transport_policy=None):
    """One explicit HTTP request with an absolute, thread-compatible deadline.

    A small disposable subprocess prevents socket/DNS implementation details
    from defeating the deadline. Private pipes carry authentication. The child
    uses stdlib HTTP with explicit proxy/CA policy, no ambient settings or redirects.
    """
    validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
    if (type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 300
            or type(max_response_bytes) is not int or not 1 <= max_response_bytes <= 1024 * 1024
            or method not in {"GET", "POST", "DELETE", "PUT"}
            or body is not None and (type(body) is not bytes or len(body) > 1024 * 1024)
            or sse_response_id is not None and type(sse_response_id) not in (int, str)):
        raise ServiceError("invalid_transport_configuration")
    header_map = dict(headers or {})
    for key, value in header_map.items():
        if (type(key) is not str or type(value) is not str or not key
                or any(ord(c) < 33 or ord(c) > 126 or c == ":" for c in key)
                or any(ord(c) < 32 or ord(c) > 126 for c in value)
                or len(value) > 16384):
            raise ServiceError("invalid_transport_configuration")
        if key.lower() == 'proxy-authorization':
            raise ServiceError('invalid_transport_configuration')
    policy_wire = None
    if transport_policy is not None:
        from .http_policy import HTTPTransportPolicy
        if not isinstance(transport_policy, HTTPTransportPolicy):
            raise ServiceError('invalid_transport_policy')
        policy_wire = transport_policy.wire(endpoint)
    wire = json.dumps({"endpoint": endpoint, "method": method, "headers": header_map,
                       "body": base64.b64encode(body).decode() if body is not None else None,
                       "timeout_seconds": timeout_seconds, "max_response_bytes": max_response_bytes,
                       "sse_response_id": sse_response_id, "transport_policy": policy_wire},
                      separators=(",", ":")).encode()
    process = None
    try:
        worker_environment = {"PATH": os.defpath, "LANG": "C.UTF-8"}
        if os.name == "nt":
            # Windows' socket/runtime assemblies need SystemRoot even with an
            # isolated interpreter. Preserve this OS binding only, never proxy,
            # Python startup, credential or arbitrary caller environment.
            system_root = os.environ.get("SystemRoot")
            if not system_root or not os.path.isabs(system_root):
                raise ServiceError("transport_runtime_unavailable")
            worker_environment["SystemRoot"] = system_root
        # -I excludes Python path/user-site injection; the worker is a fixed
        # sibling file and only imports standard-library modules.
        process = subprocess.Popen([sys.executable, "-I", str(Path(__file__).with_name("_http_worker.py"))],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                   close_fds=True, env=worker_environment)
        output, _unused = process.communicate(wire, timeout=timeout_seconds)
        # The fixed isolated worker exits on cumulative phase expiry, even if
        # DNS or a slow stream is blocked. These codes establish a timeout only;
        # they never establish that an external effect did not execute.
        if policy_wire is not None and 'phase_timeouts_ms' in policy_wire:
            if process.returncode == TIMEOUT_EXIT:
                raise ServiceError('transport_timeout')
            if process.returncode in range(TIMEOUT_EXIT + 1, TIMEOUT_EXIT + 1 + len(PHASES)):
                phase = PHASES[process.returncode - TIMEOUT_EXIT - 1]
                raise ServiceError('transport_' + phase + '_timeout')
        if len(output) > 2 * max_response_bytes + 65536:
            raise ServiceError("invalid_remote_response")
        response = json.loads(output, object_pairs_hook=_unique)
        if process.returncode or type(response) is not dict or "error" in response:
            raise ServiceError("transport_failure")
        status = response["status"]
        if type(status) is not int or not 100 <= status <= 599:
            raise ServiceError("invalid_remote_response")
        data = base64.b64decode(response["body"], validate=True)
        if len(data) > max_response_bytes:
            raise ServiceError("invalid_remote_response")
        # Following any redirect could disclose the inbound bearer to a peer.
        if 300 <= status <= 399:
            raise ServiceError("redirect_rejected")
        return status, response["headers"], data
    except subprocess.TimeoutExpired:
        raise ServiceError("transport_timeout") from None
    except ServiceError:
        raise
    except Exception:
        raise ServiceError("transport_failure") from None
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.communicate()


def decode_integer_json(payload, *, max_bytes=65536):
    if len(payload) > max_bytes:
        raise ServiceError("request_too_large")
    def bad(_value):
        raise ValueError()
    try:
        value = json.loads(payload, object_pairs_hook=_unique, parse_float=bad, parse_constant=bad)
        integer_json(value, max_bytes)
        return value
    except (ValueError, TypeError, UnicodeError, RunnerError, RecursionError):
        raise ServiceError("invalid_json") from None


class TaskServiceClient:
    """TaskService port over authenticated HTTP, bound to one local principal."""
    def __init__(self, endpoint, *, token_file=None, token_provider=None, principal, timeout_seconds=10,
                 allow_loopback_http=False, transport=None, transport_policy=None):
        parsed = validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
        if parsed.path not in ("", "/", "/service"):
            raise ServiceError("invalid_endpoint")
        if ((token_file is None) == (token_provider is None)
                or token_provider is not None and not callable(token_provider)):
            raise ServiceError("invalid_credential_configuration")
        self.endpoint = endpoint.rstrip("/").removesuffix("/service")
        self.principal = _principal(principal)
        self.token_file = token_file
        self._token_provider = token_provider
        self.timeout_seconds = timeout_seconds
        self.allow_loopback_http = allow_loopback_http
        self._transport = transport or http_exchange
        self.transport_policy = transport_policy

    def _token(self):
        if self._token_provider is None:
            return read_token_file(self.token_file)
        try:
            # Resolve for every request so a trusted credential source can
            # rotate tokens. Never expose provider exceptions or return values.
            return validate_token(self._token_provider())
        except Exception:
            raise ServiceError("credential_provider_failed") from None

    def _request(self, principal, operation, payload):
        if principal != self.principal:
            raise ServiceError("principal_mismatch")
        body = integer_json(payload, MAX_WIRE_BYTES).encode()
        options = {'transport_policy': self.transport_policy} if self.transport_policy is not None else {}
        status, _headers, raw = self._transport(
            self.endpoint + "/service/" + operation, method="POST", body=body,
            headers={"Authorization": "Bearer " + self._token(),
                     "Content-Type": "application/json", "Accept": "application/json"},
            timeout_seconds=self.timeout_seconds, max_response_bytes=MAX_WIRE_BYTES,
            allow_loopback_http=self.allow_loopback_http, **options)
        if _headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise ServiceError("invalid_remote_content_type")
        response = decode_integer_json(raw, max_bytes=MAX_WIRE_BYTES)
        if type(response) is not dict:
            raise ServiceError("invalid_remote_response")
        if status >= 400:
            error = response.get("error")
            if type(error) is dict and type(error.get("code")) is str:
                code = error["code"]
                if len(code) <= 128 and all(c.islower() or c.isdigit() or c == "_" for c in code):
                    raise ServiceError(code)
            raise ServiceError("remote_service_error")
        if not 200 <= status < 300 or set(response) != {"result"}:
            raise ServiceError("invalid_remote_response")
        result = response["result"]
        if operation in {"events", "list"}:
            if type(result) is not list or any(type(item) is not dict for item in result):
                raise ServiceError("invalid_remote_response")
        elif type(result) is not dict:
            raise ServiceError("invalid_remote_response")
        return result

    def submit(self, principal, request_key, workflow, input, *, metadata=None, expected_identity=None,
               expected_deployment_id=None, request_epoch=None):
        payload = {"request_key": request_key, "workflow": workflow, "input": input}
        if metadata is not None:
            payload["metadata"] = metadata
        if expected_identity is not None:
            payload["expected_identity"] = expected_identity
        if expected_deployment_id is not None:
            payload["expected_deployment_id"] = expected_deployment_id
        if request_epoch is not None:
            payload["request_epoch"] = request_epoch
        return self._request(principal, "submit", payload)

    def inspect(self, principal, task_id):
        return self._request(principal, "inspect", {"task_id": task_id})

    def result(self, principal, task_id):
        return self._request(principal, "result", {"task_id": task_id})

    def cancel(self, principal, task_id):
        return self._request(principal, "cancel", {"task_id": task_id})

    def update(self, principal, task_id, request_key, input, *, metadata=None, expected_identity=None,
               expected_deployment_id=None):
        payload = {"task_id": task_id, "request_key": request_key, "input": input}
        if metadata is not None:
            payload["metadata"] = metadata
        if expected_identity is not None:
            payload["expected_identity"] = expected_identity
        if expected_deployment_id is not None:
            payload["expected_deployment_id"] = expected_deployment_id
        return self._request(principal, "update", payload)

    def events(self, principal, task_id, after=0):
        return self._request(principal, "events", {"task_id": task_id, "after": after})

    def events_page(self, principal, task_id, after=0, limit=100):
        from ..observability import event_page_arguments, validate_event_page
        event_page_arguments(after, limit)
        value = self._request(principal, "events_page", {"task_id": task_id, "after": after, "limit": limit})
        return validate_event_page(value, after=after, limit=limit)

    def events_wait(self, principal, task_id, *, after=0, limit=100, wait_ms=1000):
        """One resumable long-poll; keep transport timeout longer than wait_ms."""
        from ..observability import event_page_arguments, validate_event_page
        event_page_arguments(after, limit)
        if type(wait_ms) is not int or not 0 <= wait_ms <= 25000:
            raise ServiceError('invalid_subscription_timeout')
        value = self._request(principal, 'events_wait',
                              {'task_id': task_id, 'after': after, 'limit': limit, 'wait_ms': wait_ms})
        return validate_event_page(value, after=after, limit=limit)

    def subscribe_events(self, principal, task_id, *, after=0, page_size=100,
                         wait_ms=1000, max_polls=1000):
        """Yield durable events with bounded lifetime and explicit reconnect cursor.

        A transport error ends the iterator. Resume using the last consumed
        event ID; duplicate events after an application crash are expected.
        """
        if type(max_polls) is not int or not 1 <= max_polls <= 10000:
            raise ServiceError('invalid_pagination')
        for _ in range(max_polls):
            page = self.events_wait(principal, task_id, after=after, limit=page_size, wait_ms=wait_ms)
            yield from page['events']
            after = page['next_after']

    def inspect_task(self, principal, task_id, path, revision=None):
        payload = {"task_id": task_id, "path": path}
        if revision is not None:
            payload["revision"] = revision
        return self._request(principal, "task", payload)

    def discover(self, principal):
        return self._request(principal, "discover", {})

    def inspect_graph(self, principal, task_id, *, section='nodes', offset=0, limit=20):
        return self._request(principal, 'graph', {'task_id': task_id, 'section': section,
                                                'offset': offset, 'limit': limit})

    def recover(self, principal, task_id):
        return self._request(principal, "recover", {"task_id": task_id})

    def iter_events(self, principal, task_id, *, after=0, page_size=100, max_pages=1000):
        """Read bounded pages until caught up; never retry a failed request."""
        if type(max_pages) is not int or not 1 <= max_pages <= 10000:
            raise ServiceError("invalid_pagination")
        for _ in range(max_pages):
            page = self.events_page(principal, task_id, after=after, limit=page_size)
            yield from page["events"]
            after = page["next_after"]
            if not page["has_more"]:
                return
        raise ServiceError("event_page_limit")

    def list_tasks(self, principal, limit=100, offset=0):
        return self._request(principal, "list", {"limit": limit, "offset": offset})

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        cursor = list(after) if type(after) is tuple else after
        return self._request(principal, "page", {"page_size": page_size, "after": cursor,
            "state": state, "metadata": metadata, "updated_after": updated_after})

    def operator(self, operator, operation, payload):
        """Operator identity stays bound to this client's authenticated token."""
        from ..operator_contracts import OPERATIONS
        if operation not in OPERATIONS:
            raise ServiceError("invalid_operator_request")
        return self._request(operator, "operator/" + operation, payload)

    def artifact(self, principal, operation, payload):
        from .artifact_protocol import OPERATIONS
        if operation not in OPERATIONS:
            raise ServiceError("invalid_artifact_request")
        return self._request(principal, "artifact/" + operation, payload)
