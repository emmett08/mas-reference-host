"""Private one-request stdlib transport worker (credentials arrive only on stdin).

The parent enforces a whole-operation deadline and kills/reaps this process.
This includes DNS, TLS, slow response headers and slow body delivery, even when
called by an orchestrator's worker thread. This helper never starts children.
"""
import base64
from contextlib import contextmanager, nullcontext
import http.client
import json
import os
import socket
import ssl
import sys
import threading
import time
from urllib.parse import urlsplit


# Fixed private-worker exit codes, also validated by service_client. No URL,
# credential or upstream exception is written by the watchdog. The process is
# disposable and the parent always reaps it, including a blocked DNS lookup.
PHASES = ('dns', 'connect', 'tls', 'headers', 'body')
TIMEOUT_EXIT = 80


class PhaseDeadline:
    """Cumulative wall deadlines, including blocked DNS and slow byte streams.

    Exactly one daemon watchdog belongs to this one-request worker. There are
    no resolver threads, child processes, retries or surviving background work.
    Socket idle timeouts alone cannot enforce a whole-header/body deadline.
    """
    def __init__(self, total_seconds, budgets):
        if (type(budgets) is not dict or set(budgets) != set(PHASES)
                or any(type(budgets[name]) is not int or not 1 <= budgets[name] <= 300_000
                       for name in PHASES)):
            raise ValueError()
        self.budgets = dict(budgets)
        self.absolute = time.monotonic() + total_seconds
        self.deadline, self.exit_code = self.absolute, TIMEOUT_EXIT
        self.condition = threading.Condition()
        self.stopped = False
        self.thread = threading.Thread(target=self._watch, daemon=True)
        self.thread.start()

    def _watch(self):
        with self.condition:
            while not self.stopped:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    os._exit(self.exit_code)
                self.condition.wait(remaining)

    @contextmanager
    def phase(self, name):
        with self.condition:
            if time.monotonic() >= self.absolute:
                os._exit(TIMEOUT_EXIT)
            requested = time.monotonic() + self.budgets[name] / 1000
            self.deadline = min(self.absolute, requested)
            self.exit_code = (TIMEOUT_EXIT + 1 + PHASES.index(name)
                              if requested < self.absolute else TIMEOUT_EXIT)
            self.condition.notify()
        try:
            yield
        finally:
            with self.condition:
                # A phase returning after its deadline must not reset the
                # watchdog before its thread next receives CPU time.
                if time.monotonic() >= self.deadline:
                    os._exit(self.exit_code)
                self.deadline, self.exit_code = self.absolute, TIMEOUT_EXIT
                self.condition.notify()

    def close(self):
        with self.condition:
            self.stopped = True
            self.condition.notify()
        self.thread.join()


class _PhasedConnection:
    """Separate resolution, TCP/CONNECT and TLS before any origin request."""
    def connect(self):
        with self.phases.phase('dns'):
            addresses = socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM)
        with self.phases.phase('connect'):
            last_error = None
            for family, kind, protocol, _canonical, address in addresses:
                sock = socket.socket(family, kind, protocol)
                try:
                    sock.settimeout(self.timeout)
                    sock.connect(address)
                    self.sock = sock
                    break
                except OSError as error:
                    last_error = error
                    sock.close()
            else:
                raise last_error or OSError('no endpoint addresses')
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self._tunnel_host:
                # The proxy's target DNS/TCP work is opaque to this client and
                # consumes this same connect budget; proxy auth stays here.
                self._tunnel()
        if isinstance(self, http.client.HTTPSConnection):
            with self.phases.phase('tls'):
                self.sock = self._context.wrap_socket(
                    self.sock, server_hostname=self._tunnel_host or self.host)


class PhasedHTTPConnection(_PhasedConnection, http.client.HTTPConnection):
    pass


class PhasedHTTPSConnection(_PhasedConnection, http.client.HTTPSConnection):
    pass


def read_response(response, maximum, sse_response_id=None):
    """Stop an opted-in SSE exchange at its complete correlated response event.

    Generic HTTP callers still read the bounded whole body. SSE comments and
    notifications count towards the same aggregate byte limit. This is framing
    only: the MCP caller subsequently validates the entire response semantics.
    """
    media = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
    if media != "text/event-stream" or sse_response_id is None:
        data = response.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError()
        return data
    captured, line, data_lines = bytearray(), bytearray(), []
    skip_lf, first_line = False, True

    def complete_line():
        nonlocal first_line
        value = bytes(line)
        line.clear()
        if first_line:
            value = value.removeprefix(b"\xef\xbb\xbf")
            first_line = False
        if value:
            field, separator, content = value.partition(b":")
            if field == b"data":
                data_lines.append(content.removeprefix(b" ") if separator else b"")
            return False
        if not data_lines:
            return False
        try:
            event = json.loads(b"\n".join(data_lines))
            return (type(event) is dict and event.get("jsonrpc") == "2.0"
                    and type(event.get("id")) is type(sse_response_id)
                    and event.get("id") == sse_response_id and "method" not in event
                    and ("result" in event or "error" in event))
        except (ValueError, UnicodeError):
            return False
        finally:
            data_lines.clear()

    while True:
        chunk = response.read1(min(8192, maximum + 1 - len(captured)))
        if not chunk:
            return bytes(captured)
        for byte in chunk:
            captured.append(byte)
            if len(captured) > maximum:
                raise ValueError()
            if skip_lf:
                skip_lf = False
                if byte == 10:
                    continue
            if byte in (10, 13):
                skip_lf = byte == 13
                if complete_line():
                    return bytes(captured)
            else:
                line.append(byte)


def connection_for(request, phases=None):
    """Construct explicit TLS/CONNECT routing without ambient configuration."""
    endpoint = urlsplit(request['endpoint'])
    policy = request.get('transport_policy') or {}
    options = {'timeout': request['timeout_seconds']}
    if endpoint.scheme == 'https':
        context = ssl.create_default_context()
        if policy.get('additional_ca_pem') is not None:
            context.load_verify_locations(cadata=policy['additional_ca_pem'])
        options['context'] = context
    proxy = policy.get('proxy_url')
    if proxy is not None:
        proxy = urlsplit(proxy)
        if endpoint.scheme != 'https' or proxy.scheme != 'http':
            raise ValueError()
        cls = PhasedHTTPSConnection if phases else http.client.HTTPSConnection
        connection = cls(proxy.hostname, proxy.port or 80, **options)
        connection.phases = phases
        headers = ({'Proxy-Authorization': policy['proxy_authorization']}
                   if policy.get('proxy_authorization') is not None else {})
        connection.set_tunnel(endpoint.hostname, endpoint.port or 443, headers=headers)
        return connection
    cls = ((PhasedHTTPSConnection if phases else http.client.HTTPSConnection)
           if endpoint.scheme == 'https' else
           (PhasedHTTPConnection if phases else http.client.HTTPConnection))
    connection = cls(endpoint.hostname, endpoint.port, **options)
    connection.phases = phases
    return connection


def main():
    connection, phases = None, None
    try:
        wire = sys.stdin.buffer.read(2 * 1024 * 1024 + 1)
        if len(wire) > 2 * 1024 * 1024:
            raise ValueError()
        request = json.loads(wire)
        endpoint = urlsplit(request["endpoint"])
        budgets = (request.get('transport_policy') or {}).get('phase_timeouts_ms')
        if budgets is not None:
            phases = PhaseDeadline(request['timeout_seconds'], budgets)
        connection = connection_for(request, phases)
        body = base64.b64decode(request["body"], validate=True) if request["body"] is not None else None
        if phases is not None:
            connection.connect()
        with phases.phase('headers') if phases else nullcontext():
            connection.request(request["method"], endpoint.path or "/", body=body, headers=request["headers"])
            response = connection.getresponse()
        maximum = request["max_response_bytes"]
        with phases.phase('body') if phases else nullcontext():
            data = read_response(response, maximum, request.get("sse_response_id"))
        value = {"status": response.status,
                 "headers": {k.lower(): v for k, v in response.getheaders()
                             if k.lower() in {"content-type", "www-authenticate", "retry-after", "location"}},
                 "body": base64.b64encode(data).decode("ascii")}
        sys.stdout.write(json.dumps(value, separators=(",", ":")))
    except Exception:
        # No upstream exception, credential, response body or URL is logged.
        sys.stdout.write('{"error":"transport_failure"}')
    finally:
        if connection is not None:
            connection.close()
        if phases is not None:
            phases.close()


if __name__ == "__main__":
    main()
