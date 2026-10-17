"""Event paging and request diagnostics preserve ownership and bounded output."""
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest

from mas_host import Outcome
from mas_host.observability import RequestLog
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, PAGE_BYTES, Workflow
from mas_host.task_store import event
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.bridges.mcp import MCPBridge
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests
from test_bridge_server import ServerFixture, TOKEN, READ, MemoryService
from test_mcp_bridge import request


class EventPageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        workflow = Workflow("events/1", topology(),
            {actor: FunctionAdapter(lambda _c, r: Outcome(r.input)) for actor in ("a", "b", "c")},
            "a", "b", "echo")
        self.service = DurableTaskService(self.root / "state", {"echo": workflow},
            {"alice": {"echo"}, "bob": {"echo"}}, supervised=False,
            mas_bin=runner_tests.RunnerIntegrationTests.mas, gateway_bin=runner_tests.RunnerIntegrationTests.gateway)
        self.task_id = self.service.submit("alice", "one", "echo", {})["task_id"]
        self.token = self.root / "token"
        self.token.write_text(TOKEN)
        self.token.chmod(0o600)

    def test_pages_resume_after_last_event_and_observe_later_appends(self):
        with self.service.store.transaction() as db:
            for number in range(5):
                event(db, self.task_id, "progress", {"number": number})
        expected = self.service.events("alice", self.task_id)
        cursor, observed = 0, []
        while True:
            page = self.service.events_page("alice", self.task_id, cursor, 2)
            observed.extend(page["events"])
            self.assertLessEqual(len(page["events"]), 2)
            cursor = page["next_after"]
            if not page["has_more"]:
                break
        self.assertEqual(observed, expected)
        empty = self.service.events_page("alice", self.task_id, cursor)
        self.assertEqual(empty, {"events": [], "next_after": cursor, "has_more": False})
        with self.service.store.transaction() as db:
            event(db, self.task_id, "progress", {"number": 6})
        self.assertEqual(len(self.service.events_page("alice", self.task_id, cursor)["events"]), 1)

    def test_byte_bound_preserves_more_flag_and_ownership(self):
        with self.service.store.transaction() as db:
            for _ in range(12):
                event(db, self.task_id, "large", {"text": "x" * 65500})
        page = self.service.events_page("alice", self.task_id, limit=100)
        self.assertTrue(page["has_more"])
        self.assertLess(len(json.dumps(page, separators=(",", ":")).encode()), PAGE_BYTES)
        with self.assertRaisesRegex(ServiceError, "task not found"):
            self.service.events_page("bob", self.task_id)
        for invalid in (True, -1, 2**63, "0"):
            with self.assertRaisesRegex(ServiceError, "invalid cursor"):
                self.service.events_page("alice", self.task_id, after=invalid)
        for invalid in (True, 0, 1001, "1"):
            with self.assertRaisesRegex(ServiceError, "invalid pagination"):
                self.service.events_page("alice", self.task_id, limit=invalid)

    def test_http_client_and_mcp_have_matching_pages(self):
        with ServerFixture(self.service, request_log=lambda _record: None) as server:
            client = TaskServiceClient(server.endpoint, token_file=self.token,
                principal="alice", allow_loopback_http=True)
            expected = self.service.events("alice", self.task_id)
            self.assertEqual(list(client.iter_events("alice", self.task_id, page_size=1)), expected)
            self.assertEqual(client.events_page("alice", self.task_id),
                             self.service.events_page("alice", self.task_id))
        response = MCPBridge(self.service).handle("alice", request("tools/call", {
            "name": "mas.events_page", "arguments": {"task_id": self.task_id, "limit": 1}}))
        self.assertEqual(response["result"]["structuredContent"],
                         self.service.events_page("alice", self.task_id, limit=1))

    def test_client_rejects_nonadvancing_cursor_without_another_request(self):
        calls = []
        def transport(*_args, **_kwargs):
            calls.append(1)
            return 200, {"content-type": "application/json"}, json.dumps({"result": {
                "events": [], "next_after": 0, "has_more": True}}).encode()
        client = TaskServiceClient("https://example.test", token_file=self.token,
            principal="alice", transport=transport)
        with self.assertRaisesRegex(ServiceError, "invalid remote response"):
            list(client.iter_events("alice", self.task_id))
        self.assertEqual(len(calls), 1)

    def test_mcp_byte_truncation_preserves_cursor_and_more(self):
        with self.service.store.transaction() as db:
            for _ in range(4):
                event(db, self.task_id, "large", {"text": "x" * 20000})
        response = MCPBridge(self.service).handle("alice", request("tools/call", {
            "name": "mas.events_page", "arguments": {"task_id": self.task_id}}))
        page = response["result"]["structuredContent"]
        self.assertTrue(page["has_more"])
        self.assertEqual(page["next_after"], page["events"][-1]["id"])
        self.assertLess(len(json.dumps(response).encode()), 65536)


class RequestLoggingTests(unittest.TestCase):
    def test_request_id_correlates_sanitised_failure_and_success(self):
        records, finished = [], threading.Event()
        def record(value):
            records.append(value)
            finished.set()
        with ServerFixture(request_log=record) as server:
            status, headers, _ = server.request(payload={"request_key": "secret-key",
                "workflow": "secret-workflow", "input": {"password": "secret-password"}})
            self.assertEqual(status, 200)
            self.assertTrue(finished.wait(2))
            self.assertEqual(records[-1]["request_id"], headers["X-Request-ID"])
            self.assertEqual(records[-1]["operation"], "service.submit")
            finished.clear()
            status, _, _ = server.request(path="/secret-path", token="secret-token")
            self.assertEqual(status, 401)
            self.assertTrue(finished.wait(2))
        stream = io.StringIO()
        logger = RequestLog(stream)
        for value in records:
            logger(value)
        text = stream.getvalue()
        self.assertNotIn("secret", text)
        self.assertNotIn(TOKEN, text)
        parsed = [json.loads(line) for line in text.splitlines()]
        self.assertEqual(parsed[-1]["status"], 401)
        self.assertEqual(parsed[0]["schema"], "mas-request-log/1")
        self.assertIsInstance(parsed[0]["duration_ms"], int)

    def test_logging_failure_cannot_fail_accepted_work(self):
        def unavailable(_record):
            raise OSError("sink unavailable")
        with ServerFixture(request_log=unavailable) as server:
            status, _, body = server.request()
            self.assertEqual(status, 200)
            self.assertEqual(len(server.service.records), 1)
            self.assertIn("result", body)

    def test_missing_paging_capability_has_explicit_error(self):
        with ServerFixture(request_log=lambda _record: None) as server:
            status, _, body = server.request(path="/service/events_page", payload={"task_id": "missing"})
            self.assertEqual(status, 501)
            self.assertEqual(body["error"]["code"], "unsupported_capability")

    def test_recovery_requires_update_scope_before_dispatch(self):
        class RecoverableService(MemoryService):
            def recover(self, principal, task_id):
                self.calls.append((principal, task_id))
                return {"task_id": task_id, "state": "queued"}
        service = RecoverableService()
        with ServerFixture(service, request_log=lambda _record: None,
                           mcp_bridge=MCPBridge(service)) as server:
            status, _, _ = server.request(path="/service/recover", token=READ,
                                           payload={"task_id": "one"})
            self.assertEqual(status, 403)
            status, _, _ = server.request(path="/mcp", token=READ,
                payload=request("tools/call", {"name": "mas.recover", "arguments": {"task_id": "one"}}))
            self.assertEqual(status, 403)
            self.assertFalse(service.calls)
            self.assertEqual(server.request(path="/service/recover", payload={"task_id": "one"})[0], 200)
            self.assertEqual(service.calls, [("alice", "one")])
