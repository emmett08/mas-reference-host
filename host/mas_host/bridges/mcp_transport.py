"""Bounded MCP transports. Stdio children inherit the MAS guardian's group."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import selectors
import subprocess
import time

from ..codec import integer_json
from .mcp import MCPError, MAX_MESSAGE, bounded, decode_message, routing_headers


class StreamableHTTPTransport:
    """One POST per message; JSON and request-scoped SSE, no redirects/retries.

    A fixed endpoint and separately acquired bearer token form the security
    boundary. Discovery never rewrites that endpoint or obtains a token from an
    advertised URL. Corporate OAuth may provide/refresh the token externally.
    """
    def __init__(self, endpoint: str, *, token: str | None = None, timeout_seconds: int = 30,
                 allow_loopback_http=False, transport_policy=None):
        from .service_client import validate_endpoint
        validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise MCPError(-32602, "MCP HTTP requires a bounded timeout")
        if token is not None and (type(token) is not str or not token
                                  or any(ord(c) < 33 or ord(c) > 126 for c in token)):
            raise MCPError(-32602, "Invalid MCP bearer credential")
        self.endpoint, self._token = endpoint, token
        self.timeout_seconds, self.allow_loopback_http = timeout_seconds, allow_loopback_http
        if transport_policy is not None:
            from .http_policy import HTTPTransportPolicy
            if not isinstance(transport_policy, HTTPTransportPolicy):
                raise MCPError(-32602, 'Invalid explicit transport policy')
        self.transport_policy = transport_policy

    def descriptor(self):
        value = {"kind": "streamable-http", "endpoint": self.endpoint,
                 "timeout_seconds": self.timeout_seconds, "allow_loopback_http": self.allow_loopback_http}
        if self.transport_policy is not None:
            value['transport_policy'] = self.transport_policy.descriptor()
        return value

    def exchange(self, message: dict, *, headers=None):
        from .service_client import http_exchange
        outgoing = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                    **routing_headers(message)}
        if headers:
            if any(not name.lower().startswith("mcp-param-") for name in headers):
                raise MCPError(-32602, "Only declared MCP parameter headers may be added")
            outgoing.update(headers)
        if self._token is not None:
            outgoing["Authorization"] = "Bearer " + self._token
        try:
            status, response_headers, body = http_exchange(
                self.endpoint, method="POST", body=integer_json(bounded(message)).encode(), headers=outgoing,
                timeout_seconds=self.timeout_seconds, max_response_bytes=MAX_MESSAGE,
                allow_loopback_http=self.allow_loopback_http, sse_response_id=message["id"],
                transport_policy=self.transport_policy)
        except Exception:
            raise MCPError(1002, "MCP HTTP failed; external outcome may be unknown") from None
        # Protocol errors on 400/404 still have valuable structured error codes.
        if status not in (200, 400, 404):
            raise MCPError(1002, "MCP HTTP was rejected; inspect authentication or remote state")
        media = response_headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media == "text/event-stream" and status == 200:
            return _response_from_sse(body, message["id"])
        if media != "application/json":
            raise MCPError(1002, "Unsupported MCP response media type")
        response = decode_message(body)
        if status != 200 and "error" not in response:
            raise MCPError(1002, "Invalid MCP error response")
        return response


def _response_from_sse(body: bytes, request_id) -> dict:
    """Parse bounded SSE data fields; notifications never complete a request."""
    data, response, notifications = [], None, 0
    try:
        # SSE lines end only at CRLF, CR or LF. Unicode separators such as
        # U+2028 are valid JSON string content and must remain inside data.
        text = body.decode("utf-8-sig")
        # EOF cannot synthesize the empty line that completes an SSE event.
        lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")[:-1]
    except UnicodeError:
        raise MCPError(1002, "Invalid MCP event stream") from None
    for line in lines:
        if line == "":
            if not data:
                continue
            message = decode_message("\n".join(data))
            data = []
            if message.get("jsonrpc") != "2.0":
                raise MCPError(1002, "Invalid MCP event")
            if "id" in message:
                if message.get("id") != request_id or "method" in message or response is not None:
                    raise MCPError(1002, "Unexpected MCP stream response")
                response = message
            else:
                if type(message.get("method")) is not str:
                    raise MCPError(1002, "Invalid MCP notification")
                notifications += 1
                if notifications > 128 or response is not None:
                    raise MCPError(1002, "Unexpected MCP event after final response")
        else:
            field, separator, value = line.partition(":")
            if field == "data":
                data.append(value.removeprefix(" ") if separator else "")
        # Comments, event, retry, id and unknown fields have no semantic effect.
    if response is None:
        raise MCPError(1002, "MCP stream ended without a receipt; outcome may be unknown")
    return response


class StdioTransport:
    """A fixed trusted executable, no shell, no ambient credential inheritance.

    The subprocess must remain in the inherited process group. MAS supervision
    owns descendant cleanup after timeout/host death; this transport also reaps
    its immediate child on ordinary closure. Long-lived task state belongs in
    the server's durable store, never its connection identity.
    """
    def __init__(self, command: list[str], *, cwd: Path, environment: dict | None = None,
                 secret_environment: dict | None = None, timeout_seconds: int = 30):
        if (type(command) is not list or not command or any(type(x) is not str or not x or "\0" in x for x in command)
                or not Path(command[0]).is_absolute() or not 1 <= timeout_seconds <= 60):
            raise MCPError(-32602, "MCP stdio requires a fixed executable and bounded timeout")
        self.command, self.cwd = list(command), Path(cwd).resolve(strict=True)
        if not self.cwd.is_dir():
            raise MCPError(-32602, "MCP stdio working directory is unavailable")
        self.public_environment = dict(environment or {})
        self._environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", **self.public_environment,
                             **(secret_environment or {})}
        if any(type(k) is not str or type(v) is not str or "\0" in k + v or "=" in k
               for k, v in self._environment.items()):
            raise MCPError(-32602, "Invalid MCP stdio environment")
        self.timeout_seconds = timeout_seconds
        self._process = None
        self._buffer = b""

    def descriptor(self):
        return {"kind": "stdio", "command": self.command, "cwd": str(self.cwd),
                "environment": self.public_environment, "timeout_seconds": self.timeout_seconds,
                "executable_sha256": hashlib.sha256(Path(self.command[0]).read_bytes()).hexdigest()}

    def _start(self):
        if self._process is None:
            self._process = subprocess.Popen(self.command, cwd=self.cwd, env=self._environment,
                                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                             stderr=subprocess.DEVNULL, close_fds=True,
                                             start_new_session=False, bufsize=0)
            os.set_blocking(self._process.stdin.fileno(), False)
            os.set_blocking(self._process.stdout.fileno(), False)

    def exchange(self, message: dict, *, headers=None):
        source = (integer_json(bounded(message)) + "\n").encode()
        deadline = time.monotonic() + self.timeout_seconds
        try:
            self._start()
            process = self._process
            sent = 0
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdin, selectors.EVENT_WRITE)
                while sent < len(source):
                    if not selector.select(max(0, deadline - time.monotonic())) or time.monotonic() >= deadline:
                        raise MCPError(1002, "MCP stdio deadline exceeded; outcome may be unknown")
                    sent += os.write(process.stdin.fileno(), source[sent:])
                selector.unregister(process.stdin)
                selector.register(process.stdout, selectors.EVENT_READ)
                size, notifications = len(self._buffer), 0
                while True:
                    if b"\n" not in self._buffer:
                        if not selector.select(max(0, deadline - time.monotonic())) or time.monotonic() >= deadline:
                            raise MCPError(1002, "MCP stdio deadline exceeded; outcome may be unknown")
                        chunk = os.read(process.stdout.fileno(), 8192)
                        if not chunk:
                            raise MCPError(1002, "MCP stdio closed without a receipt; outcome may be unknown")
                        self._buffer += chunk
                        size += len(chunk)
                        if size > MAX_MESSAGE:
                            raise MCPError(1002, "MCP stdio response exceeds limit")
                        continue
                    line, self._buffer = self._buffer.split(b"\n", 1)
                    response = decode_message(line)
                    if "id" not in response and type(response.get("method")) is str:
                        notifications += 1
                        if notifications > 128:
                            raise MCPError(1002, "MCP stdio notification limit exceeded")
                        continue
                    return response
        except MCPError:
            self.close()
            raise
        except (OSError, ValueError):
            self.close()
            raise MCPError(1002, "MCP stdio failed; outcome may be unknown") from None

    def close(self):
        process, self._process = self._process, None
        self._buffer = b""
        if process is not None:
            process.stdin.close()
            try:
                process.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=1)
            process.stdout.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
