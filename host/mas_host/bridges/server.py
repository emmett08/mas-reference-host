"""Bounded authenticated HTTP and trusted-local stdio bridge entry points.

HTTP handlers only persist/inspect TaskService requests. A separate single-
threaded worker owns Runner execution and its process/fork lifecycle.
"""
from __future__ import annotations

import io
import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import sys
import threading
import time
import uuid
from urllib.parse import urlsplit

from ..codec import integer_json
from ..contracts import RunnerError
from ..service_contracts import ServiceError
from ..observability import RequestLog
from .authentication import AuthenticationError, SCOPES, _principal
from .service_client import MAX_WIRE_BYTES, decode_integer_json, validate_endpoint

_SERVICE_SCOPES = {"submit": "mas:submit", "inspect": "mas:read", "result": "mas:read",
                   "cancel": "mas:cancel", "update": "mas:update", "events": "mas:read",
                   "list": "mas:read", "page": "mas:read", "events_page": "mas:read",
                   "task": "mas:read", "discover": "mas:read", "recover": "mas:update",
                   "events_wait": "mas:read", "graph": "mas:read"}


class _DeadlineReader:
    """Socket reader whose header/body deadline is absolute, not inactivity."""
    def __init__(self, connection, deadline, header_limit):
        self.connection, self.deadline = connection, deadline
        self.header_limit, self.header_bytes = header_limit, 0
        self.buffer = bytearray()
        self.closed = False

    def _receive(self, maximum):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError()
        self.connection.settimeout(remaining)
        return self.connection.recv(maximum)

    def readline(self, limit=-1):
        maximum = min(65537, limit) if limit >= 0 else 65537
        while True:
            newline = self.buffer.find(b"\n", 0, maximum)
            if newline >= 0 or len(self.buffer) >= maximum:
                size = newline + 1 if newline >= 0 else maximum
                break
            chunk = self._receive(min(4096, maximum - len(self.buffer)))
            if not chunk:
                size = len(self.buffer)
                break
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        self.header_bytes += len(result)
        if self.header_bytes > self.header_limit:
            raise ServiceError("headers_too_large")
        return result

    def read(self, size=-1):
        if size < 0:
            raise ServiceError("invalid_content_length")
        while len(self.buffer) < size:
            chunk = self._receive(min(8192, size - len(self.buffer)))
            if not chunk:
                break
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        return result

    def close(self):
        self.closed = True


class BridgeHTTPServer(ThreadingMixIn, HTTPServer):
    """At most max_connections concurrent bounded I/O handlers; no task workers."""
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, *, service, verifier, public_url,
                 mcp_bridge=None, a2a_bridge=None, allowed_hosts=None, allowed_origins=(),
                 request_timeout=30, max_body_bytes=MAX_WIRE_BYTES, max_response_bytes=MAX_WIRE_BYTES,
                 max_header_bytes=16384,
                 max_connections=8, allow_loopback_http=False, public_agent_card=True,
                 ssl_context=None, request_log=None, artifact_transfer=None, worker_controller=None,
                 max_subscriptions=None, operator_service=None):
        parsed = validate_endpoint(public_url, allow_loopback_http=allow_loopback_http)
        if max_subscriptions is None and type(max_connections) is int:
            max_subscriptions = min(2, max(0, max_connections - 1))
        if parsed.path not in ("", "/", "/mcp"):
            raise ServiceError("invalid_public_url")
        if (type(request_timeout) not in (int, float) or not 0 < request_timeout <= 300
                or type(max_body_bytes) is not int or not 1024 <= max_body_bytes <= 1048576
                or type(max_response_bytes) is not int or not 1024 <= max_response_bytes <= MAX_WIRE_BYTES
                or type(max_header_bytes) is not int or not 1024 <= max_header_bytes <= 65536
                or type(max_connections) is not int or not 1 <= max_connections <= 64
                or type(max_subscriptions) is not int or not 0 <= max_subscriptions < max_connections):
            raise ServiceError("invalid_server_configuration")
        self.service, self.verifier = service, verifier
        self.telemetry_root = getattr(getattr(service, 'authority', None), 'observation_dir',
                                      getattr(service, 'state_dir', None))
        self.artifact_transfer = artifact_transfer
        self.worker_controller = worker_controller
        self.operator_service = operator_service
        self.public_url = public_url.rstrip("/").removesuffix("/mcp")
        self.mcp_bridge, self.a2a_bridge = mcp_bridge, a2a_bridge
        self.allowed_hosts = frozenset(allowed_hosts or [parsed.netloc.lower()])
        if any(type(host) is not str or not host or any(ord(c) <= 32 for c in host)
               or "/" in host or "@" in host for host in self.allowed_hosts):
            raise ServiceError("invalid_server_configuration")
        self.allowed_origins = frozenset(allowed_origins)
        for origin in self.allowed_origins:
            origin_parts = validate_endpoint(origin, allow_loopback_http=allow_loopback_http)
            if origin_parts.path not in ("", "/"):
                raise ServiceError("invalid_server_configuration")
        self.request_timeout = request_timeout
        self.max_body_bytes, self.max_header_bytes = max_body_bytes, max_header_bytes
        self.max_response_bytes = max_response_bytes
        self.public_agent_card = bool(public_agent_card)
        self.request_log = request_log if request_log is not None else RequestLog()
        if not callable(self.request_log):
            raise ServiceError("invalid_server_configuration")
        self._slots = threading.BoundedSemaphore(max_connections)
        self._subscription_slots = threading.BoundedSemaphore(max_subscriptions)
        super().__init__(server_address, _Handler)
        if ssl_context is not None:
            # Handshake is deferred to the bounded handler, so it cannot block
            # the accept loop while other clients need status/cancellation.
            self.socket = ssl_context.wrap_socket(self.socket, server_side=True, do_handshake_on_connect=False)

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            try:
                request.settimeout(0.1)
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()

    def handle_error(self, request, client_address):
        # Request headers/body and authentication errors never reach a log.
        pass


class _Handler(BaseHTTPRequestHandler):
    server_version = "MAS"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def setup(self):
        self.request.settimeout(self.server.request_timeout)
        super().setup()
        self.rfile.close()
        self.deadline = time.monotonic() + self.server.request_timeout
        self.rfile = _DeadlineReader(self.connection, self.deadline, self.server.max_header_bytes)

    def log_message(self, *_args):
        pass

    def handle_one_request(self):
        self.request_id = uuid.uuid4().hex
        self._started_at = time.monotonic()
        self._operation, self._response_status, self._error_code = "unparsed", 0, None
        self._correlations = {}
        try:
            super().handle_one_request()
        except ServiceError as exc:
            self._error(431 if exc.code == "headers_too_large" else 400, exc.code)
        except (OSError, TimeoutError):
            self.close_connection = True
            self._error_code = "transport_closed"
        finally:
            from ..telemetry import record
            record(self.server.telemetry_root, 'http',
                   max(0, int((time.monotonic() - self._started_at) * 1_000_000)),
                   outcome='returned' if 200 <= self._response_status < 400 else 'raised',
                   request_id=self.request_id, **self._correlations)
            # Only fixed route names and codes enter this record. Never include
            # a request target, body, token, principal name or exception text.
            try:
                self.server.request_log({"request_id": self.request_id, "operation": self._operation,
                    "status": self._response_status, "error_code": self._error_code,
                    "duration_ms": max(0, int((time.monotonic() - self._started_at) * 1000))})
            except Exception:
                # Observability cannot turn a committed admission into an
                # apparent failure or trigger an application retry.
                pass

    def _send(self, status, body, *, headers=None):
        self.close_connection = True
        if (200 <= status < 300 and self._operation in {
                'service.submit', 'service.inspect', 'service.update', 'service.cancel',
                'service.recover', 'service.task'}
                and type(body) is dict and type(body.get('result')) is dict):
            # Only known lifecycle receipt IDs; result payloads may contain
            # application fields named run_id and must never enter observations.
            self._correlations = {key: body['result'][key]
                for key in ('task_id', 'run_id', 'assignment_id', 'invocation_id') if key in body['result']}
        try:
            raw = integer_json(body, self.server.max_response_bytes).encode("utf-8") if body is not None else b""
        except RunnerError:
            status, raw = 500, b'{"error":{"code":"response_too_large"}}'
            self._error_code = "response_too_large"
        self._response_status = status
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Request-ID", self.request_id)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def _error(self, status, code, *, headers=None):
        self._error_code = (code if type(code) is str and len(code) <= 128
                            and all(c.isascii() and (c.islower() or c.isdigit() or c == "_") for c in code)
                            else "internal_error")
        # Initialise minimal state if failure occurred before header parsing.
        if not hasattr(self, "request_version"):
            self.request_version = "HTTP/1.1"
            self.command = ""
        self._send(status, {"error": {"code": code}}, headers=headers)

    def send_error(self, code, message=None, explain=None):
        self._error(code, "invalid_http_request")

    def _validate_headers(self):
        # Singletons are enforced before routing or authentication. Duplicate
        # values, transfer encodings and request smuggling are never tolerated.
        for key in ("Host", "Authorization", "Origin", "Content-Length", "Content-Type",
                    "MCP-Protocol-Version", "MCP-Method", "MCP-Name", "A2A-Version", "A2A-Extensions"):
            if len(self.headers.get_all(key, [])) > 1:
                raise ServiceError("duplicate_header")
        if (self.headers.get("Host", "").lower() not in self.server.allowed_hosts
                or "Transfer-Encoding" in self.headers or "Content-Encoding" in self.headers
                or "Expect" in self.headers):
            raise ServiceError("invalid_http_headers")
        origin = self.headers.get("Origin")
        if origin is not None and origin not in self.server.allowed_origins:
            raise ServiceError("origin_denied")
        if urlsplit(self.path).scheme or "?" in self.path or "#" in self.path:
            raise ServiceError("invalid_request_target")
        operation = self.path.removeprefix("/service/")
        if self.path.startswith("/service/") and operation in _SERVICE_SCOPES:
            self._operation = "service." + operation
        elif self.path.startswith("/workers/") and self.path.removeprefix("/workers/") in {
                "register", "claim", "dispatch", "heartbeat", "receipt", "drain"}:
            self._operation = "workers." + self.path.removeprefix("/workers/")
        else:
            self._operation = {"/mcp": "mcp", "/a2a": "a2a",
                "/.well-known/agent-card.json": "agent_card",
                "/.well-known/oauth-protected-resource": "oauth_metadata",
                "/.well-known/oauth-protected-resource/mcp": "oauth_metadata"}.get(self.path, "unknown")

    def _auth(self, scopes):
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer ") or authorization.count(" ") != 1:
            raise AuthenticationError(required_scopes=scopes)
        return self.server.verifier.verify(authorization[7:], scopes)

    def _challenge(self, exc):
        status = 503 if exc.code == "authentication_unavailable" else (403 if exc.code == "insufficient_scope" else 401)
        challenge = 'Bearer error="' + ("insufficient_scope" if status == 403 else "invalid_token") + '"'
        if callable(getattr(self.server.verifier, "metadata", None)):
            challenge += ', resource_metadata="' + self.server.public_url + '/.well-known/oauth-protected-resource"'
        if exc.required_scopes:
            challenge += ', scope="' + " ".join(sorted(exc.required_scopes)) + '"'
        self._error(status, exc.code, headers={"WWW-Authenticate": challenge})

    def do_GET(self):
        try:
            self._validate_headers()
            if self.path == '/console':
                from .console import console_response
                raw, headers = console_response()
                if len(raw) > self.server.max_response_bytes:
                    raise ServiceError('response_too_large')
                self._operation, self._response_status = 'console', 200
                self.close_connection = True
                self.send_response(200)
                for name, value in {'Content-Type': 'text/html; charset=utf-8',
                        'Content-Length': str(len(raw)), 'Connection': 'close',
                        'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
                        'X-Request-ID': self.request_id, **headers}.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(raw)
                return
            if self.path in ("/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"):
                metadata = getattr(self.server.verifier, "metadata", None)
                if not callable(metadata):
                    raise ServiceError("not_found")
                return self._send(200, metadata())
            if self.path == "/.well-known/agent-card.json" and self.server.a2a_bridge is not None:
                if not self.server.public_agent_card:
                    self._auth({"mas:read"})
                return self._send(200, self.server.a2a_bridge.agent_card())
            self._auth({"mas:read"})
            self._error(405, "method_not_allowed", headers={"Allow": "POST"})
        except AuthenticationError as exc:
            self._challenge(exc)
        except ServiceError as exc:
            self._error(404 if exc.code == "not_found" else 400, exc.code)
        except Exception:
            self._error(500, "internal_error")

    def do_POST(self):
        try:
            self._validate_headers()
            # Authenticate before parsing task input or revealing any state.
            principal = self._auth(set())
            content_type = self.headers.get("Content-Type", "").lower().replace(" ", "")
            if content_type not in {"application/json", "application/json;charset=utf-8"}:
                raise ServiceError("unsupported_media_type")
            length = self.headers.get("Content-Length", "")
            if not length.isascii() or not length.isdecimal() or len(length) > 10:
                raise ServiceError("invalid_content_length")
            size = int(length)
            if size > self.server.max_body_bytes:
                raise ServiceError("request_too_large")
            raw = self.rfile.read(size)
            if len(raw) != size:
                raise ServiceError("invalid_content_length")
            try:
                payload = decode_integer_json(raw, max_bytes=self.server.max_body_bytes)
            except ServiceError:
                if self.path in {"/mcp", "/a2a"}:
                    return self._send(400, {"jsonrpc": "2.0", "id": None,
                        "error": {"code": -32700, "message": "Invalid JSON"}})
                raise
            if type(payload) is not dict:
                raise ServiceError("invalid_json")
            if time.monotonic() >= self.deadline:
                raise ServiceError("request_timeout")
            if self.path.startswith("/workers/"):
                principal.require({"mas:worker"})
                if self.server.worker_controller is None:
                    raise ServiceError("unsupported_capability")
                value = self.server.worker_controller.handle(principal.name,
                    self.path.removeprefix("/workers/"), payload)
                return self._send(200, {"result": value})
            if self.path.startswith("/service/operator/"):
                principal.require({"mas:admin"})
                if self.server.operator_service is None:
                    raise ServiceError("unsupported_capability")
                operation = self.path.removeprefix("/service/operator/")
                from ..operator_service import OPERATIONS
                if operation not in OPERATIONS:
                    raise ServiceError("not_found")
                self._operation = "operator." + operation
                value = self.server.operator_service.dispatch(principal.name, operation, payload)
                return self._send(200, {"result": value})
            if self.path.startswith("/service/artifact/"):
                from .artifact_transfer import OPERATIONS, WRITE_OPERATIONS
                operation = self.path.removeprefix("/service/artifact/")
                if operation not in OPERATIONS or self.server.artifact_transfer is None:
                    raise ServiceError("not_found")
                self._operation = "artifact." + operation
                principal.require({"mas:submit" if operation in WRITE_OPERATIONS else "mas:read"})
                value = self.server.artifact_transfer.dispatch(principal.name, operation, payload)
                return self._send(200, {"result": value})
            if self.path.startswith("/service/"):
                operation = self.path.removeprefix("/service/")
                scope = _SERVICE_SCOPES.get(operation)
                if scope is None:
                    raise ServiceError("not_found")
                principal.require({scope})
                if operation == 'events_wait':
                    if not self.server._subscription_slots.acquire(blocking=False):
                        raise ServiceError('subscription_capacity')
                    try:
                        # Leave ordinary ingress slots and time for serialising
                        # the response. Every page read rechecks owner grants.
                        maximum = max(0, min(25000, int((self.deadline - time.monotonic() - 0.1) * 1000)))
                        value = _service_call(self.server.service, principal.name, operation, payload,
                                              subscription_limit_ms=maximum)
                    finally:
                        self.server._subscription_slots.release()
                    return self._send(200, {'result': value})
                value = _service_call(self.server.service, principal.name, operation, payload)
                return self._send(200, {"result": value})
            if self.path == "/mcp" and self.server.mcp_bridge is not None:
                principal.require({_mcp_scope(payload)})
                from .mcp import http_status
                value = self.server.mcp_bridge.handle(principal.name, payload, dict(self.headers.items()))
                return self._send(202 if value is None else http_status(value), value)
            if self.path == "/a2a" and self.server.a2a_bridge is not None:
                from .a2a import error_response, validate_request
                a2a_headers = {"a2a-version": self.headers.get("A2A-Version", ""),
                               "a2a-extensions": self.headers.get("A2A-Extensions", "")}
                try:
                    validate_request(payload, a2a_headers)
                    scope = _a2a_scope(payload)
                except ServiceError as error:
                    return self._send(200, error_response(payload.get("id"), error))
                principal.require({scope})
                value = self.server.a2a_bridge.handle(principal.name, payload, a2a_headers)
                selected = a2a_headers['a2a-extensions']
                headers = ({'A2A-Extensions': ','.join(item.strip() for item in selected.split(','))}
                           if selected and 'result' in value else None)
                return self._send(200, value, headers=headers)
            raise ServiceError("not_found")
        except AuthenticationError as exc:
            self._challenge(exc)
        except ServiceError as exc:
            status = _service_status(exc.code)
            self._error(status, exc.code)
        except (OSError, TimeoutError):
            self.close_connection = True
        except Exception:
            self._error(500, "internal_error")

    def handle_expect_100(self):
        self._error(417, "expectation_not_supported")
        return False

    def do_OPTIONS(self):
        self._error(405, "method_not_allowed")


def _service_status(code):
    if code == "unsupported_capability":
        return 501
    if code in {"not_found", "task_not_found", "workflow_not_found"}:
        return 404
    if code in {"forbidden", "principal_mismatch", "worker_forbidden", "worker_capability_denied"}:
        return 403
    if code in {"request_epoch_retired", "request_epoch_in_use", "idempotency_conflict", "request_key_conflict", "request_conflict", "task_not_ready", "invalid_state", "result_not_ready", "result_unavailable", "invalid_task_state", "stale_worker_epoch", "stale_worker_assignment", "receipt_conflict", "worker_registration_conflict", "worker_policy_changed", "invocation_binding_conflict"}:
        return 409
    if code in {"queue_full", "service_unavailable", "authentication_unavailable", "worker_queue_full", "controller_admission_closed", "subscription_capacity"}:
        return 503
    if code == "request_too_large":
        return 413
    if code == "unsupported_media_type":
        return 415
    if code == "request_timeout":
        return 408
    return 400


def _service_call(service, principal, operation, payload, *, subscription_limit_ms=25000):
    required = {"submit": {"request_key", "workflow", "input"}, "inspect": {"task_id"},
                "result": {"task_id"}, "cancel": {"task_id"},
                "update": {"task_id", "request_key", "input"}, "events": {"task_id"}, "events_page": {"task_id"},
                "list": set(), "page": set(), "task": {"task_id", "path"}, "graph": {"task_id"},
                "discover": set(), "recover": {"task_id"}, "events_wait": {"task_id"}}[operation]
    optional = {"submit": {"metadata", "expected_identity", "expected_deployment_id", "request_epoch"},
                "update": {"metadata", "expected_identity", "expected_deployment_id"},
                "events": {"after"}, "events_page": {"after", "limit"},
                "events_wait": {"after", "limit", "wait_ms"},
                "list": {"limit", "offset"}, "task": {"revision"}, "graph": {"section", "offset", "limit"},
                "page": {"page_size", "after", "state", "metadata", "updated_after"}}.get(operation, set())
    if not required.issubset(payload) or set(payload) - required - optional:
        raise ServiceError("invalid_parameters")
    if operation == 'events_wait':
        from ..event_delivery import wait_events
        wait_ms = payload.get('wait_ms', 20000)
        if type(wait_ms) is not int or not 0 <= wait_ms <= 25000:
            raise ServiceError('invalid_subscription_timeout')
        return wait_events(service, principal, **{**payload, 'wait_ms': min(wait_ms, subscription_limit_ms)})
    method = getattr(service, {"list": "list_tasks", "page": "list_task_page", "task": "inspect_task", "graph": "inspect_graph"}.get(operation, operation), None)
    if not callable(method):
        raise ServiceError("unsupported_capability")
    return method(principal, **payload)


def _mcp_scope(payload):
    method = payload.get("method")
    if method in {"tasks/cancel"}:
        return "mas:cancel"
    if method in {"tasks/update"}:
        return "mas:update"
    if method == "tools/call":
        params = payload.get("params")
        name = params.get("name") if type(params) is dict else None
        if type(name) is str and name.startswith("mas.artifact."):
            from .artifact_transfer import WRITE_OPERATIONS
            return "mas:submit" if name.removeprefix("mas.artifact.") in WRITE_OPERATIONS else "mas:read"
        return {"mas.submit": "mas:submit", "mas.cancel": "mas:cancel", "mas.update": "mas:update",
                "mas.recover": "mas:update"}.get(name, "mas:read")
    return "mas:read"


def _a2a_scope(payload):
    method = payload.get("method")
    if method == 'mas/RecoverTask':
        return 'mas:update'
    if method in {"CancelTask", "tasks/cancel"}:
        return "mas:cancel"
    if method == "SendMessage":
        from .a2a import send_task_id
        params = payload.get("params")
        message = params.get("message") if type(params) is dict else None
        return "mas:update" if send_task_id(message) is not None else "mas:submit"
    if method in {"SendStreamingMessage", "message/send", "message/stream"}:
        params = payload.get("params")
        message = params.get("message") if type(params) is dict else None
        return "mas:update" if type(message) is dict and "taskId" in message else "mas:submit"
    return "mas:read"


def serve_stdio(bridge, principal, *, input_stream=None, output_stream=None, max_bytes=65536):
    """Serve bounded JSON lines as one trusted OS identity, never body identity."""
    _principal(principal)
    incoming = input_stream if input_stream is not None else sys.stdin.buffer
    outgoing = output_stream if output_stream is not None else sys.stdout.buffer
    while True:
        raw = incoming.readline(max_bytes + 1)
        if not raw:
            return
        if len(raw) > max_bytes:
            # Terminate on framing loss; never treat the tail as another request.
            raise ServiceError("request_too_large")
        try:
            message = decode_integer_json(raw, max_bytes=max_bytes)
            if type(message) is not dict:
                raise ServiceError("invalid_json")
            response = bridge.handle(principal, message)
        except ServiceError:
            response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Invalid JSON"}}
        if response is not None:
            outgoing.write(integer_json(response, max_bytes).encode() + b"\n")
            outgoing.flush()
