"""Counterexamples discovered during independent MCP conformance re-audit."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import threading
import time
from pathlib import Path
import unittest

from mas_host.bridges.mcp import MCPError, validate_schema
from mas_host.bridges.mcp_client import MCPClient, MCPRemoteClient
from mas_host.bridges.mcp_transport import StreamableHTTPTransport, _response_from_sse

TOOL = {"name": "audit", "inputSchema": {"type": "object"}}
TASK = {"taskId": "durable-task", "createdAt": "2026-10-14T00:00:00Z",
        "lastUpdatedAt": "2026-10-14T00:00:00Z", "ttlMs": None}


class ReplyTransport:
    def __init__(self, result):
        self.result = result
        self.requests = []

    def descriptor(self):
        return {"kind": "audit-test"}

    def exchange(self, message, headers=None):
        self.requests.append(message)
        return {"jsonrpc": "2.0", "id": message["id"], "result": self.result}


class MCPReauditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import jsonschema
        fixtures = Path(__file__).parent / "fixtures"
        cls.validators = {}
        for name in ("mcp-2026-07-28-schema", "mcp-tasks-2026-07-28-schema"):
            path = fixtures / (name + ".json")
            schema = json.loads(path.read_text())
            provenance = json.loads(path.with_suffix(".provenance.json").read_text())
            if hashlib.sha256(path.read_bytes()).hexdigest() != provenance["sha256"]:
                raise AssertionError("Official schema fixture digest changed")
            for definition in schema["$defs"]:
                cls.validators[name, definition] = jsonschema.Draft202012Validator(
                    {**schema, "$ref": "#/$defs/" + definition})

    def validator(self, definition, *, tasks=False):
        profile = "mcp-tasks-2026-07-28-schema" if tasks else "mcp-2026-07-28-schema"
        return self.validators[profile, definition]

    def remote(self, result, tool=None):
        return MCPRemoteClient(MCPClient(ReplyTransport(result), elicitation=True), tool or TOOL)

    def test_all_base_task_seeds_preserve_known_identity_before_detailed_poll(self):
        # CreateTaskResult is Result & base Task, not Result & DetailedTask.
        # The Tasks extension permits every Task status in this seed shape.
        for state in ("working", "input_required", "completed", "failed", "cancelled"):
            with self.subTest(state=state):
                result = {**TASK, "resultType": "task", "status": state}
                self.validator("CreateTaskResult", tasks=True).validate(result)
                remote = self.remote(result)
                snapshot = remote.submit({"arguments": {}}).validate()
                self.assertEqual(snapshot.remote_id, TASK["taskId"])
                self.assertEqual(snapshot.state, "cancelled" if state == "cancelled" else "running")
                self.assertEqual(len(remote.client.transport.requests), 1)

    def test_array_scalar_and_null_outputs_conform_to_the_official_result_schema(self):
        cases = [({"type": "array", "items": {"type": "integer"}}, [1, 2]),
                 ({"type": "string"}, "answer"), ({"type": "integer"}, 42),
                 ({"type": "boolean"}, False), ({"type": "null"}, None)]
        for output_schema, value in cases:
            with self.subTest(value=value):
                result = {"resultType": "complete", "content": [], "structuredContent": value}
                self.validator("CallToolResult").validate(result)
                snapshot = self.remote(result, TOOL | {"outputSchema": output_schema}).submit({"arguments": {}})
                self.assertEqual(snapshot.state, "completed")
                self.assertEqual(snapshot.result["structuredContent"], value)

    def test_null_instance_is_validated_instead_of_skipping_schema_check(self):
        validate_schema({"type": "null"}, None)
        with self.assertRaises(MCPError):
            validate_schema({"type": "integer"}, None)
        with self.assertRaises(MCPError):
            self.remote({"resultType": "complete", "content": [], "structuredContent": None},
                        TOOL | {"outputSchema": {"type": "array"}}).submit({"arguments": {}})

    def test_noncomplete_lifecycle_results_cannot_establish_acknowledgement(self):
        result = {"resultType": "input_required", "requestState": "unfulfilled-consent"}
        self.validator("InputRequiredResult").validate(result)
        for action, definition in (("cancel", "CancelTaskResult"), ("update", "UpdateTaskResult")):
            with self.subTest(action=action):
                validator = self.validator(definition, tasks=True)
                validator.validate({"resultType": "complete"})
                self.assertFalse(validator.is_valid(result))
                remote = self.remote(result)
                with self.assertRaisesRegex(MCPError, "acknowledgement"):
                    if action == "cancel":
                        remote.cancel("durable-task")
                    else:
                        remote.update("durable-task", {"input_responses": {}})
                self.assertEqual(len(remote.client.transport.requests), 1)

    def test_task_poll_requires_complete_result_type(self):
        result = {**TASK, "resultType": "input_required", "status": "working"}
        validator = self.validator("GetTaskResult", tasks=True)
        validator.validate(result | {"resultType": "complete"})
        self.assertFalse(validator.is_valid(result))
        remote = self.remote(result)
        with self.assertRaisesRegex(MCPError, "observation"):
            remote.poll("durable-task")

    def test_sse_unicode_separators_are_content_not_wire_line_endings(self):
        for separator in ("\u0085", "\u2028", "\u2029"):
            for newline in ("\n", "\r", "\r\n"):
                with self.subTest(separator=ord(separator), newline=repr(newline)):
                    response = {"jsonrpc": "2.0", "id": "audit", "result": {
                        "resultType": "complete", "content": [{"type": "text", "text": "a" + separator + "b"}]}}
                    body = ("data: " + json.dumps(response, ensure_ascii=False) + newline * 2).encode()
                    self.assertEqual(_response_from_sse(body, "audit"), response)

    def test_sse_eof_cannot_complete_an_unterminated_event(self):
        response = {"jsonrpc": "2.0", "id": "audit", "result": {"resultType": "complete"}}
        data = "data: " + json.dumps(response)
        for ending in ("", "\n", "\r", "\r\n"):
            with self.subTest(ending=repr(ending)):
                with self.assertRaisesRegex(MCPError, "without a receipt"):
                    _response_from_sse((data + ending).encode(), "audit")

    def test_final_sse_receipt_does_not_wait_for_http_stream_termination(self):
        requests = []
        release = threading.Event()
        class Peer(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *_args):
                pass
            def do_POST(self):
                message = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append(message)
                result = {"resultType": "complete", "content": [], "structuredContent": "a\u2028b"}
                body = ("data: " + json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result},
                                             ensure_ascii=False) + "\n\n").encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                self.wfile.write(("%X\r\n" % len(body)).encode() + body + b"\r\n")
                self.wfile.flush()
                release.wait(3)
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Peer)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            transport = StreamableHTTPTransport(f"http://127.0.0.1:{server.server_port}/mcp",
                                                timeout_seconds=1, allow_loopback_http=True)
            started = time.monotonic()
            result = MCPClient(transport).call_tool(TOOL, {})
            self.assertLess(time.monotonic() - started, .9)
            self.assertEqual(result["structuredContent"], "a\u2028b")
            self.assertFalse(release.is_set())
            self.assertEqual(len(requests), 1)
        finally:
            release.set()
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
