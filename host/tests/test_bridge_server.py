"""Offline fault tests for authentication, framing and durable ingress transport."""
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mas_host.service_contracts import ServiceError
from mas_host.bridges.authentication import (
    AuthenticationError, OAuthIntrospectionVerifier, SCOPES, StaticTokenVerifier,
    read_token_file,
)
from mas_host.bridges.server import BridgeHTTPServer, serve_stdio
from mas_host.bridges.service_client import TaskServiceClient, http_exchange

TOKEN = "test-only-local-bearer-token-000000000000"
READ = "test-only-local-read-token-00000000000000"
OTHER = "test-only-other-principal-000000000000000"


def verifier():
    return StaticTokenVerifier([{"sha256": hashlib.sha256(token.encode()).hexdigest(),
                                 "principal": principal, "scopes": list(scopes)}
                                for token, principal, scopes in [(TOKEN, "alice", SCOPES),
                                (READ, "alice", {"mas:read"}), (OTHER, "bob", SCOPES)]])


class MemoryService:
    def __init__(self):
        self.records = {}
        self.calls = []
        self.lock = threading.Lock()

    def submit(self, principal, request_key, workflow, input):
        with self.lock:
            self.calls.append((principal, request_key))
            key = (principal, request_key)
            record = {"task_id": principal + "-" + request_key, "input": input,
                      "workflow": workflow, "status": "queued"}
            if key in self.records and self.records[key] != record:
                raise ServiceError("idempotency_conflict")
            self.records[key] = record
            return record

    def inspect(self, principal, task_id):
        for (owner, _key), record in self.records.items():
            if owner == principal and record["task_id"] == task_id:
                return record
        raise ServiceError("not_found")

    def result(self, principal, task_id):
        self.inspect(principal, task_id)
        raise ServiceError("result_not_ready")

    def list_tasks(self, principal, limit=100, offset=0):
        return [r for (p, _k), r in self.records.items() if p == principal][offset:offset + limit]


class JoinedBridgeHTTPServer(BridgeHTTPServer):
    # Test fixtures must join their request handlers before later tests exercise
    # the supervisor's intentional single-threaded-host requirement.
    daemon_threads = False


class BoundedPeer:
    """Own a stoppable loopback peer, including the no-connect failure path.

    The listener stays open until its accept loop has terminated. Closing a
    listener in another thread does not portably wake accept and risks its
    descriptor being reused by a later test.
    """
    def __init__(self, respond):
        self.respond = respond
        self.stop = threading.Event()
        self.received = threading.Event()
        self.errors = []
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.settimeout(0.05)
        self.endpoint = "http://127.0.0.1:" + str(self.listener.getsockname()[1])
        self.thread = threading.Thread(target=self._serve, name="bounded-test-peer")

    def _serve(self):
        try:
            while not self.stop.is_set():
                try:
                    connection, _ = self.listener.accept()
                    break
                except socket.timeout:
                    continue
            else:
                return
            with connection:
                connection.settimeout(0.05)
                while not self.stop.is_set():
                    try:
                        data = connection.recv(65536)
                    except socket.timeout:
                        continue
                    if not data:
                        return
                    self.received.set()
                    self.respond(connection, self.stop)
                    return
        except (BrokenPipeError, ConnectionResetError):
            # The timeout path deliberately kills the HTTP child while the
            # peer is still writing its incomplete response.
            pass
        except Exception as error:
            self.errors.append(error)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.stop.set()
        self.thread.join(5)
        alive = self.thread.is_alive()
        if not alive:
            self.listener.close()
        if alive:
            raise AssertionError("loopback fixture peer did not terminate")
        if self.errors:
            raise AssertionError("loopback fixture failed") from self.errors[0]


class ServerFixture:
    def __init__(self, service=None, **options):
        self.service = service or MemoryService()
        self.server = JoinedBridgeHTTPServer(("127.0.0.1", 0), service=self.service, verifier=options.pop("verifier", verifier()),
            public_url="http://127.0.0.1", allow_loopback_http=True,
            request_timeout=options.pop("request_timeout", 2), **options)
        self.port = self.server.server_address[1]
        self.server.allowed_hosts = frozenset({"127.0.0.1:" + str(self.port)})
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        if self.thread.is_alive():
            raise AssertionError("bridge fixture accept loop did not terminate")

    @property
    def endpoint(self):
        return "http://127.0.0.1:" + str(self.port)

    def request(self, path="/service/submit", payload=None, *, token=TOKEN, headers=None, method="POST"):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        body = json.dumps(payload if payload is not None else {"request_key": "same", "workflow": "invoice", "input": {"n": 1}}).encode()
        values = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
        values.update(headers or {})
        connection.request(method, path, body=body if method == "POST" else None, headers=values)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), json.loads(response.read() or b"null")
        connection.close()
        return result


class AuthenticationTests(unittest.TestCase):
    def test_static_digest_scope_and_principal(self):
        auth = verifier()
        self.assertEqual(auth.verify(TOKEN, {"mas:submit"}).name, "alice")
        with self.assertRaises(AuthenticationError) as context:
            auth.verify(READ, {"mas:submit"})
        self.assertEqual(context.exception.code, "insufficient_scope")
        with self.assertRaises(AuthenticationError):
            auth.verify("wrong-local-bearer-token-00000")

    def test_private_file_rejects_mode_symlink_and_whitespace(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "token"
            file.write_text(TOKEN + "\n")
            file.chmod(0o600)
            self.assertEqual(read_token_file(file), TOKEN)
            link = Path(directory) / "link"
            link.symlink_to(file)
            with self.assertRaises(AuthenticationError):
                read_token_file(link)
            file.chmod(0o644)
            with self.assertRaises(AuthenticationError):
                read_token_file(file)
            file.chmod(0o600)
            file.write_text(TOKEN + "\n\n")
            with self.assertRaises(AuthenticationError):
                read_token_file(file)

    def test_config_rejects_duplicate_digest_and_duplicate_json_key(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "auth.json"
            file.write_text('{"version":1,"version":1,"credentials":[]}')
            file.chmod(0o600)
            with self.assertRaises(AuthenticationError):
                StaticTokenVerifier.from_file(file)
        item = {"sha256": "a" * 64, "principal": "alice", "scopes": ["mas:read"]}
        with self.assertRaises(AuthenticationError):
            StaticTokenVerifier([item, item])

    def test_oauth_audience_expiry_issuer_and_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "secret"
            secret.write_text(TOKEN)
            secret.chmod(0o600)
            payload = {"active": True, "sub": "alice", "aud": "https://mas.example/mcp",
                       "iss": "https://id.example", "scope": "mas:read mas:submit", "exp": int(time.time()) + 60}
            calls = []
            def exchange(endpoint, **kwargs):
                calls.append((endpoint, kwargs))
                return 200, {}, json.dumps(payload).encode()
            auth = OAuthIntrospectionVerifier(resource="https://mas.example/mcp", issuer="https://id.example",
                introspection_url="https://id.example/introspect", client_id="mas-service",
                client_secret_file=secret, transport=exchange)
            self.assertEqual(auth.verify(TOKEN, {"mas:submit"}).name, "alice")
            self.assertEqual(calls[0][0], "https://id.example/introspect")
            self.assertTrue(calls[0][1]["headers"]["Authorization"].startswith("Basic "))
            self.assertNotIn("Bearer " + TOKEN, calls[0][1]["headers"].values())
            for field, bad in [("aud", "https://elsewhere.example"), ("iss", "https://evil.example"),
                               ("exp", 1), ("active", 1), ("scope", "mas:read"), ("token_type", 7)]:
                original = payload.get(field)
                payload[field] = bad
                with self.subTest(field=field), self.assertRaises(AuthenticationError):
                    auth.verify(TOKEN, {"mas:submit"})
                if original is None:
                    payload.pop(field)
                else:
                    payload[field] = original
            with self.assertRaises(AuthenticationError):
                OAuthIntrospectionVerifier(resource="https://mas.example/mcp", issuer="https://id.example",
                    introspection_url="http://id.example/introspect", client_id="mas", client_secret_file=secret)


class BridgeServerTests(unittest.TestCase):
    def test_auth_before_lookup_and_caller_principal_never_trusted(self):
        with ServerFixture() as fixture:
            status, _, _ = fixture.request(token="bogus")
            self.assertEqual(status, 401)
            self.assertEqual(fixture.service.calls, [])
            status, _, body = fixture.request(headers={"X-Principal": "bob"})
            self.assertEqual(status, 200)
            self.assertEqual(body["result"]["task_id"], "alice-same")
            status, _, _ = fixture.request(payload={"request_key": "x", "workflow": "invoice", "input": {}, "principal": "bob"})
            self.assertEqual(status, 400)
            status, _, _ = fixture.request("/service/inspect", {"task_id": "alice-same"}, token=OTHER)
            self.assertEqual(status, 404)

    def test_insufficient_scope_host_origin_and_query_rejected(self):
        with ServerFixture() as fixture:
            self.assertEqual(fixture.request(token=READ)[0], 403)
            self.assertEqual(fixture.request(headers={"Host": "evil.example"})[0], 400)
            self.assertEqual(fixture.request(headers={"Origin": "https://evil.example"})[0], 400)
            self.assertEqual(fixture.request("/service/submit?access_token=secret")[0], 400)
            self.assertEqual(fixture.service.calls, [])

    def test_duplicate_json_keys_floats_and_duplicate_content_length_rejected(self):
        with ServerFixture() as fixture:
            for body in (b'{"input":{},"input":{}}', b'{"input":{"n":1.5}}'):
                connection = http.client.HTTPConnection("127.0.0.1", fixture.port)
                connection.request("POST", "/service/submit", body=body,
                    headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
                response = connection.getresponse()
                self.assertEqual(response.status, 400)
                response.read()
                connection.close()
            connection = http.client.HTTPConnection("127.0.0.1", fixture.port)
            connection.putrequest("POST", "/service/submit")
            connection.putheader("Authorization", "Bearer " + TOKEN)
            connection.putheader("Content-Type", "application/json")
            connection.putheader("Content-Length", "2")
            connection.putheader("Content-Length", "2")
            connection.endheaders(b"{}")
            response = connection.getresponse()
            self.assertEqual(response.status, 400)
            response.read()
            connection.close()
            self.assertEqual(fixture.service.calls, [])

    def test_client_dedup_conflict_principal_and_no_retry(self):
        with tempfile.TemporaryDirectory() as directory, ServerFixture() as fixture:
            token = Path(directory) / "token"
            token.write_text(TOKEN)
            token.chmod(0o600)
            client = TaskServiceClient(fixture.endpoint, token_file=token, principal="alice", allow_loopback_http=True)
            first = client.submit("alice", "stable", "invoice", {"n": 1})
            self.assertEqual(first, client.submit("alice", "stable", "invoice", {"n": 1}))
            self.assertEqual(len(fixture.service.records), 1)
            with self.assertRaises(ServiceError) as context:
                client.submit("alice", "stable", "invoice", {"n": 2})
            self.assertEqual(context.exception.code, "idempotency_conflict")
            self.assertEqual(len(fixture.service.calls), 3)
            with self.assertRaises(ServiceError):
                client.inspect("bob", first["task_id"])
            self.assertEqual(client.inspect("alice", first["task_id"]), first)
            with self.assertRaises(ServiceError) as context:
                client.result("alice", first["task_id"])
            self.assertEqual(context.exception.code, "result_not_ready")

    def test_mcp_header_check_and_no_streaming_route(self):
        from mas_host.bridges.mcp import MCPBridge, metadata, routing_headers
        service = MemoryService()
        bridge = MCPBridge(service)
        message = {"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {"_meta": metadata()}}
        with ServerFixture(service, mcp_bridge=bridge) as fixture:
            self.assertEqual(fixture.request("/mcp", message)[0], 400)
            status, _, body = fixture.request("/mcp", message, headers=routing_headers(message))
            self.assertEqual(status, 200)
            self.assertIn("result", body)
            self.assertEqual(fixture.request("/mcp", method="GET")[0], 405)

    def test_slow_headers_have_total_deadline_and_other_clients_continue(self):
        with ServerFixture(request_timeout=0.2) as fixture:
            slow = socket.create_connection(("127.0.0.1", fixture.port), timeout=1)
            slow.sendall(b"POST /service/submit HTTP/1.1\r\nHost:")
            self.assertEqual(fixture.request()[0], 200)
            time.sleep(0.25)
            try:
                slow.sendall(b" 127.0.0.1\r\n")
                self.assertEqual(slow.recv(4096), b"")
            except (BrokenPipeError, ConnectionResetError):
                pass
            slow.close()

    def test_bounded_connections_reject_overload(self):
        with ServerFixture(max_connections=1, request_timeout=0.4) as fixture:
            slow = socket.create_connection(("127.0.0.1", fixture.port), timeout=1)
            slow.sendall(b"POST /service/submit HTTP/1.1\r\n")
            deadline = time.monotonic() + 1
            while fixture.server._slots._value != 0 and time.monotonic() < deadline:
                time.sleep(0.005)
            other = socket.create_connection(("127.0.0.1", fixture.port), timeout=1)
            self.assertIn(b"503", other.recv(4096))
            slow.close()
            other.close()

    def test_oauth_discovery_challenge_and_failed_introspection_are_closed(self):
        class Verifier:
            def metadata(self):
                return {"resource": "https://mas.example/mcp", "authorization_servers": ["https://id.example"]}
            def verify(self, token, required_scopes=()):
                raise AuthenticationError("authentication_unavailable")
        with ServerFixture(verifier=Verifier()) as fixture:
            status, headers, _ = fixture.request(token="missing")
            self.assertEqual(status, 503)
            self.assertIn('resource_metadata="', headers["WWW-Authenticate"])
            status, _, body = fixture.request("/.well-known/oauth-protected-resource", method="GET", token="")
            self.assertEqual(status, 200)
            self.assertEqual(body["authorization_servers"], ["https://id.example"])
            self.assertEqual(fixture.service.calls, [])

    def test_a2a_version_is_protocol_error_and_continuation_needs_update_scope(self):
        from mas_host.bridges.a2a import A2ABridge
        service = MemoryService()
        bridge = A2ABridge(service, endpoint="https://mas.example/a2a")
        limited = StaticTokenVerifier([{"sha256": hashlib.sha256(TOKEN.encode()).hexdigest(),
                                       "principal": "alice", "scopes": ["mas:submit", "mas:read"]}])
        with ServerFixture(service, verifier=limited, a2a_bridge=bridge) as fixture:
            message = {"jsonrpc": "2.0", "id": 1, "method": "GetTask", "params": {"id": "unknown"}}
            status, _, body = fixture.request("/a2a", message, headers={"A2A-Version": "0.3"})
            self.assertEqual(status, 200)
            self.assertEqual(body["error"]["code"], -32009)
            message["method"] = "SendMessage"
            message["params"] = {"message": {"taskId": "unknown"}}
            status, headers, _ = fixture.request("/a2a", message, headers={"A2A-Version": "1.0"})
            self.assertEqual(status, 403)
            self.assertIn("mas:update", headers["WWW-Authenticate"])
            self.assertEqual(fixture.service.calls, [])
            self.assertEqual(fixture.request("/.well-known/agent-card.json", token="", method="GET")[0], 200)

    def test_header_and_body_limits_fail_before_dispatch(self):
        with ServerFixture(max_header_bytes=1024, max_body_bytes=1024) as fixture:
            self.assertEqual(fixture.request(headers={"X-Long": "x" * 1100})[0], 431)
            self.assertEqual(fixture.request(payload={"input": "x" * 1100})[0], 413)
            self.assertEqual(fixture.service.calls, [])

    def test_client_unknown_submission_is_never_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            token = Path(directory) / "token"
            token.write_text(TOKEN)
            token.chmod(0o600)
            calls = []
            def lose_response(endpoint, **kwargs):
                calls.append(json.loads(kwargs["body"]))
                raise ServiceError("transport_timeout")
            client = TaskServiceClient("https://mas.example", token_file=token, principal="alice", transport=lose_response)
            with self.assertRaises(ServiceError):
                client.submit("alice", "retain-this-key", "invoice", {"n": 1})
            self.assertEqual(calls, [{"request_key": "retain-this-key", "workflow": "invoice", "input": {"n": 1}}])

    def test_stdio_principal_fixed_and_oversize_terminates(self):
        class Bridge:
            def handle(self, principal, request):
                return {"principal": principal, "id": request.get("id")}
        output = io.BytesIO()
        serve_stdio(Bridge(), "alice", input_stream=io.BytesIO(b'{"id":1,"principal":"bob"}\n'), output_stream=output)
        self.assertEqual(json.loads(output.getvalue())["principal"], "alice")
        with self.assertRaises(ServiceError):
            serve_stdio(Bridge(), "alice", input_stream=io.BytesIO(b"x" * 20), output_stream=io.BytesIO(), max_bytes=10)

    def test_transport_deadline_covers_slow_headers_from_worker_thread(self):
        headers_started = threading.Event()
        def trickle_headers(connection, stop):
            connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Slow: ")
            headers_started.set()
            while not stop.wait(0.05):
                # Activity defeats an inactivity-only socket timeout. Only the
                # parent's whole-request deadline may complete this request.
                connection.sendall(b"x")
        result = []
        timeout = 5
        with BoundedPeer(trickle_headers) as peer:
            def invoke():
                started = time.monotonic()
                try:
                    http_exchange(peer.endpoint, timeout_seconds=timeout, allow_loopback_http=True)
                except ServiceError as exc:
                    result.append((exc.code, time.monotonic() - started))
            worker = threading.Thread(target=invoke, name="http-deadline-test-client")
            worker.start()
            try:
                # Do not pass vacuously when slow startup consumes the budget.
                self.assertTrue(headers_started.wait(timeout - 1), "HTTP worker never reached the slow-header response")
            finally:
                worker.join(timeout + 2)
                self.assertFalse(worker.is_alive(), "HTTP client fixture did not terminate")
            self.assertEqual(result[0][0], "transport_timeout")
            self.assertGreaterEqual(result[0][1], timeout - 0.1)
            self.assertLess(result[0][1], timeout + 2)
        self.assertFalse(peer.thread.is_alive())

    def test_timeout_before_connect_joins_peer_and_reaps_child(self):
        def unexpected_connection(connection, stop):
            raise AssertionError("delayed helper must never connect")
        real_popen = subprocess.Popen
        children = []
        def delayed_start(_command, **kwargs):
            # Deterministically reproduce slow Python/SSL startup on a loaded
            # arm64 runner without reducing the production deadline contract.
            process = real_popen([sys.executable, "-I", "-c", "import time; time.sleep(30)"], **kwargs)
            children.append(process)
            return process
        with BoundedPeer(unexpected_connection) as peer:
            with patch("mas_host.bridges.service_client.subprocess.Popen", side_effect=delayed_start):
                with self.assertRaises(ServiceError) as context:
                    http_exchange(peer.endpoint, timeout_seconds=0.05, allow_loopback_http=True)
            self.assertEqual(context.exception.code, "transport_timeout")
            self.assertFalse(peer.received.is_set())
            self.assertEqual(len(children), 1)
            self.assertIsNotNone(children[0].poll(), "deadline helper was not reaped")
        self.assertFalse(peer.thread.is_alive())

    def test_transport_redirect_rejected_without_followup(self):
        def redirect(connection, stop):
            connection.sendall(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: https://elsewhere.example/\r\nContent-Length: 0\r\n\r\n")
        with BoundedPeer(redirect) as peer:
            with self.assertRaises(ServiceError) as context:
                http_exchange(peer.endpoint, allow_loopback_http=True)
            self.assertEqual(context.exception.code, "redirect_rejected")
            self.assertTrue(peer.received.is_set())
        self.assertFalse(peer.thread.is_alive())


if __name__ == "__main__":
    unittest.main()
