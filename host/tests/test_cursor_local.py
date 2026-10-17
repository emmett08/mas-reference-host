"""Cursor local contracts and real bridge lifecycle; no model API is called."""
from __future__ import annotations

import importlib.metadata
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mas_host.invoice_cursor import InvoiceCursorProvider as CursorLocalProvider
from mas_host.cursor_session import (CURSOR_SDK_VERSION, MAX_STREAM_TEXT_BYTES,
                                   _bridge_child, launch_client as _launch_client,
                                   _validate_node, _runtime_identity, _runtime_manifest, _bridge_files)
from mas_host.providers import ProviderError, provider_binding

INVOICE = [{"item": "Widget", "quantity": 2, "unit_cents": 1499}]


def result(text='{"total_cents":2998}', **overrides):
    fields = {"id": "run-example", "status": "finished", "result": text,
              "usage": SimpleNamespace(input_tokens=7, output_tokens=3, total_tokens=10)}
    fields.update(overrides)
    return SimpleNamespace(**fields)


class FakeRun:
    def __init__(self, response=None, events=(), error=None):
        self.response, self.messages, self.error = response or result(), events, error
        self.status, self.cancelled = "running", False

    def events(self):
        yield from self.messages
        if self.error:
            raise self.error
        self.status = self.response.status

    def wait(self):
        return self.response

    def cancel(self):
        self.cancelled = True


class FakeAgent:
    agent_id = "agent-example"

    def __init__(self, run):
        self.run, self.prompts, self.closed = run, [], False

    def send(self, prompt):
        self.prompts.append(prompt)
        return self.run

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, run=None):
        self.agent, self.calls, self.closed = FakeAgent(run or FakeRun()), [], False
        self.agents = self

    def create(self, options=None, *, model=None, api_key=None, name=None, local=None,
               cloud=None, idempotency_key=None):
        self.calls.append(options)
        return self.agent

    def close(self):
        self.closed = True


def event(kind="assistant", text="", **fields):
    content = SimpleNamespace(type="text", text=text)
    message = SimpleNamespace(type=kind, text=text,
                              message=SimpleNamespace(content=[content]), **fields)
    return SimpleNamespace(sdk_message=message)


def sdk_available():
    try:
        return importlib.metadata.version("cursor-sdk") == CURSOR_SDK_VERSION
    except importlib.metadata.PackageNotFoundError:
        return False


class CursorLocalTests(unittest.TestCase):
    def test_no_effect_before_admission_and_explicit_policy_binding(self):
        with patch("mas_host.cursor_session.launch_client", side_effect=AssertionError("premature effect")):
            provider = CursorLocalProvider("composer-2.5", "planner", api_key="private-key")
            descriptor, amount = provider_binding(provider)
        self.assertEqual(amount, 100000)
        self.assertEqual(descriptor["provider"], "cursor-local")
        self.assertEqual(descriptor["model_retry_policy"], "sdk_managed")
        self.assertEqual(descriptor["tools"], [])
        self.assertEqual(descriptor["setting_sources"], [])
        self.assertNotIn("private-key", json.dumps(descriptor))
        self.assertNotEqual(descriptor, CursorLocalProvider("other", "planner", client=FakeClient()).descriptor())

    def test_restrictions_strict_result_and_truthful_evidence(self):
        client = FakeClient()
        provider = CursorLocalProvider("composer-2.5", "reviewer", client=client)
        invocation = provider.total(INVOICE, proposal={"total_cents": 12}, request_id="call_1")
        self.assertEqual(invocation.output, {"total_cents": 2998})
        self.assertEqual(len(client.calls), 1)
        options = client.calls[0]
        self.assertEqual(options["tools"], [])
        self.assertEqual(options["disallowed_tools"], ["mcp", "task"])
        self.assertEqual(options["mcp_servers"], {})
        self.assertEqual(options["agents"], {})
        self.assertEqual(options["local"]["setting_sources"], [])
        self.assertEqual(options["local"]["dirs"], [])
        self.assertEqual(options["local"]["custom_tools"], {})
        self.assertFalse(Path(options["local"]["cwd"]).exists())
        self.assertTrue(client.agent.closed)
        self.assertFalse(client.closed, "injected clients are caller owned")
        self.assertEqual(len(client.agent.prompts), 1)
        self.assertIn('"proposal":{"total_cents":12}', client.agent.prompts[0])
        self.assertEqual(invocation.evidence["client_request_id"], "call_1")
        self.assertEqual(invocation.evidence["spending_basis"], "configured_reservation_not_provider_invoice")
        self.assertEqual(invocation.evidence["submission_unit"], "agent_run")
        self.assertEqual(invocation.evidence["run_id"], "run-example")
        self.assertEqual(invocation.evidence["usage"], {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10})

    def test_one_send_and_cleanup_after_uncertain_stream_failure(self):
        client = FakeClient(FakeRun(error=RuntimeError("SECRET provider request body")))
        provider = CursorLocalProvider("composer-2.5", "planner", api_key="SECRET")
        with patch("mas_host.cursor_session.launch_client", return_value=client), self.assertRaises(ProviderError) as raised:
            provider.total(INVOICE)
        self.assertNotIn("SECRET", str(raised.exception))
        self.assertEqual(len(client.agent.prompts), 1)
        self.assertTrue(client.agent.run.cancelled)
        self.assertTrue(client.agent.closed)
        self.assertTrue(client.closed)
        self.assertFalse(Path(client.calls[0]["local"]["cwd"]).exists())

    def test_cleanup_client_even_when_agent_close_raises(self):
        client = FakeClient()
        client.agent.close = lambda: (_ for _ in ()).throw(RuntimeError("SECRET"))
        with patch("mas_host.cursor_session.launch_client", return_value=client), self.assertRaises(ProviderError):
            CursorLocalProvider("composer-2.5", "planner", api_key="key").total(INVOICE)
        self.assertTrue(client.closed)

    def test_output_validation_and_non_success_statuses(self):
        bodies = ['{"total_cents":true}', '{"total_cents":1.0}', '{"total_cents":NaN}',
                  '{"total_cents":1,"total_cents":2}', '{"total_cents":1,"extra":2}',
                  '{"total_cents":-1}', '{"total_cents":1000000000001}', '[]',
                  '```json\n{"total_cents":2998}\n```', 'x' * 4097, "\ud800"]
        for body in bodies:
            with self.subTest(body=repr(body)[:60]), self.assertRaises(ProviderError):
                client = FakeClient(FakeRun(result(body)))
                CursorLocalProvider("model", "planner", client=client).total(INVOICE)
            self.assertTrue(client.agent.closed)
        for status in ("running", "cancelled", "expired", "error", "completed", None):
            with self.subTest(status=status), self.assertRaises(ProviderError):
                CursorLocalProvider("model", "planner", client=FakeClient(FakeRun(result(status=status)))).total(INVOICE)

    def test_stream_limit_and_forbidden_tools_cancel_and_close(self):
        for messages in ([event("thinking", "x" * (MAX_STREAM_TEXT_BYTES + 1))],
                         [event("assistant", "x" * (MAX_STREAM_TEXT_BYTES // 2 + 1))] * 2,
                         [event("tool_call")]):
            client = FakeClient(FakeRun(events=messages))
            with self.subTest(messages=len(messages)), self.assertRaises(ProviderError):
                CursorLocalProvider("model", "planner", client=client).total(INVOICE)
            self.assertTrue(client.agent.run.cancelled)
            self.assertTrue(client.agent.closed)

    def test_invalid_configuration_and_input_never_launch(self):
        for amount in (True, 0, -1, 100000001):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                CursorLocalProvider("model", "planner", client=FakeClient(), reservation_microusd=amount)
        for model in ("", "bad\nmodel", "auto-smart", "x" * 201):
            with self.subTest(model=model[:20]), self.assertRaises(ValueError):
                CursorLocalProvider(model, "planner", client=FakeClient())
        with self.assertRaises(ProviderError):
            CursorLocalProvider("model", "planner")
        client = FakeClient()
        provider = CursorLocalProvider("model", "planner", client=client)
        for request_id in ("", "bad\ntrace", "x" * 129, True):
            with self.assertRaises(ProviderError):
                provider.total(INVOICE, request_id=request_id)
        with self.assertRaises(ValueError):
            provider.total([])
        with self.assertRaises(ProviderError):
            provider.total(INVOICE, proposal={"total_cents": False})
        self.assertEqual(client.calls, [])

    def test_invalid_evidence_omitted(self):
        client = FakeClient(FakeRun(result(id="SECRET\n", usage=SimpleNamespace(
            input_tokens=True, output_tokens=-1, total_tokens=10**10))))
        client.agent.agent_id = "SECRET\n"
        evidence = CursorLocalProvider("model", "planner", client=client).total(INVOICE).evidence
        self.assertNotIn("run_id", evidence)
        self.assertNotIn("agent_id", evidence)
        self.assertNotIn("usage", evidence)

    def test_child_environment_drops_credentials_and_ambient_overrides(self):
        with tempfile.TemporaryDirectory() as workspace, patch.dict(os.environ, {
            "OPENAI_API_KEY": "secret-openai", "CURSOR_API_KEY": "secret-cursor",
            "CURSOR_SDK_BRIDGE_BIN": "/untrusted/bridge", "NODE_OPTIONS": "--require /untrusted/hook",
            "HTTPS_PROXY": "http://proxy.example:8080", "HOME": "/untrusted/home",
        }), patch("mas_host.cursor_session._bridge_files", return_value=(Path("/sdk/node"), Path("/sdk/bridge.js"))), \
                patch("mas_host.cursor_session.os.execve") as execute:
            _bridge_child(workspace, ["--workspace", workspace])
            executable, arguments, environment = execute.call_args.args
            self.assertEqual(executable, "/sdk/node")
            self.assertEqual(arguments, ["/sdk/node", "/sdk/bridge.js", "--workspace", workspace])
            self.assertEqual(environment["HOME"], str(Path(workspace) / "home"))
            for key in ("OPENAI_API_KEY", "CURSOR_API_KEY", "CURSOR_SDK_BRIDGE_BIN", "NODE_OPTIONS"):
                self.assertNotIn(key, environment)
            self.assertEqual(environment["HTTPS_PROXY"], "http://proxy.example:8080")
            self.assertEqual(os.environ["HOME"], "/untrusted/home")

    def test_client_construction_failure_still_closes_bridge_and_transport(self):
        from unittest.mock import Mock
        http_client, bridge = Mock(), Mock()
        process = bridge.process
        sdk = SimpleNamespace(DefaultHttpxClient=Mock(return_value=http_client),
                              Bridge=SimpleNamespace(launch=Mock(return_value=bridge)),
                              CursorClient=Mock(side_effect=ValueError("SECRET transport failure")))
        with patch("mas_host.cursor_session._sdk_module", return_value=sdk), self.assertRaises(ValueError):
            _launch_client("/private/workspace")
        sdk.DefaultHttpxClient.assert_called_once_with(timeout=45, trust_env=False)
        bridge.close.assert_not_called()
        process.stderr.close.assert_called_once()
        http_client.close.assert_called_once()
        options = sdk.CursorClient.call_args.kwargs
        self.assertEqual(options["max_retries"], 0)
        self.assertFalse(options["allow_api_key_env_fallback"])
        launch = sdk.Bridge.launch.call_args.kwargs
        self.assertEqual(launch["host"], "127.0.0.1")
        self.assertEqual(launch["command"][-2:], ["--bridge-child", "/private/workspace"])

    def test_stderr_is_drained_without_buffering_provider_diagnostics(self):
        import subprocess
        from mas_host.cursor_session import _OwnedClient
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "after-output"
            process = subprocess.Popen([sys.executable, "-c",
                "import pathlib,sys,time; sys.stderr.write('private diagnostic'*20000); "
                "sys.stderr.flush(); pathlib.Path(sys.argv[1]).touch(); time.sleep(30)", str(marker)],
                stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
            bridge = SimpleNamespace(process=process, _tool_callback_server=None, _store_callback_server=None)
            client = SimpleNamespace(agents=None, close=lambda: None)
            owned = _OwnedClient(client, bridge, SimpleNamespace(close=lambda: None))
            try:
                until = time.monotonic() + 2
                while not marker.exists() and time.monotonic() < until:
                    time.sleep(0.01)
                self.assertTrue(marker.exists(), "stderr pipe blocked the subprocess")
            finally:
                owned.close()
            self.assertFalse(owned._stderr_thread.is_alive())
            self.assertIsNotNone(process.poll())

    def test_node_preflight_rejects_old_modified_or_wrong_architecture_without_execution(self):
        import hashlib
        approved = b"fixture approved patched executable"
        digest = hashlib.sha256(approved).hexdigest()
        manifest = {"version": "24.21.0", "architectures": {
            "x86_64": {"node_sha256": digest}, "aarch64": {"node_sha256": "a" * 64}}}
        with tempfile.TemporaryDirectory() as directory, \
                patch("mas_host.cursor_session._runtime_manifest", return_value=manifest), \
                patch("mas_host.cursor_session.platform.machine", return_value="x86_64"), \
                patch("mas_host.cursor_session.subprocess.Popen", side_effect=AssertionError("preflight executed code")):
            node = Path(directory) / "node"
            node.write_bytes(b"old bundled Node 24.5.0")
            with self.assertRaisesRegex(ProviderError, "patched Node"):
                _validate_node(node)
            node.write_bytes(approved)
            _validate_node(node)
            with patch("mas_host.cursor_session.platform.machine", return_value="aarch64"):
                with self.assertRaisesRegex(ProviderError, "patched Node"):
                    _validate_node(node)
            with patch("mas_host.cursor_session.platform.machine", return_value="riscv64"):
                with self.assertRaisesRegex(ProviderError, "only Linux"):
                    _validate_node(node)
            node.unlink()
            with self.assertRaisesRegex(ProviderError, "unavailable"):
                _validate_node(node)

    def test_bridge_preflight_rejects_unrepaired_dependencies_without_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "cursor_sdk"
            bridge = package / "_vendor" / "bridge"
            node, entry = bridge / "bin" / "node", bridge / "dist" / "bin" / "cursor-sdk-bridge.js"
            node.parent.mkdir(parents=True)
            entry.parent.mkdir(parents=True)
            node.write_bytes(b"approved-node-fixture")
            entry.write_text("bridge fixture")
            sdk = SimpleNamespace(__file__=str(package / "__init__.py"))
            with patch("mas_host.cursor_session._sdk_module", return_value=sdk), \
                    patch("mas_host.cursor_session._validate_node") as validate_node, \
                    patch("mas_host.cursor_session.validate_bridge_patch") as validate_patch, \
                    patch("mas_host.cursor_session.subprocess.Popen", side_effect=AssertionError("preflight executed code")):
                validate_patch.side_effect = ValueError("private patch failure details")
                with self.assertRaisesRegex(ProviderError, "verified dependency repair") as raised:
                    _bridge_files()
                self.assertNotIn("private", str(raised.exception))
                validate_patch.assert_called_once_with(bridge)
                validate_node.assert_called_once_with(node)
                validate_patch.side_effect = None
                self.assertEqual(_bridge_files(), (node, entry))

    def test_bridge_patch_is_bound_into_recovery_identity_without_a_cursor_install(self):
        provider = CursorLocalProvider("composer-2.5", "planner", client=FakeClient())
        with patch("mas_host.cursor_session.patch_identity", return_value={"repair": "first"}), \
                patch("mas_host.cursor_session._sdk_module", side_effect=AssertionError("injected client imported SDK")):
            first = provider.descriptor()
        with patch("mas_host.cursor_session.patch_identity", return_value={"repair": "second"}):
            second = provider.descriptor()
        self.assertNotEqual(first, second)
        self.assertEqual(first["bridge_patch"], {"repair": "first"})

    def test_runtime_release_is_bound_into_recovery_identity(self):
        first = {"version": "24.21.0", "architectures": {
            "x86_64": {"node_sha256": "a" * 64}, "aarch64": {"node_sha256": "b" * 64}}}
        provider = CursorLocalProvider("composer-2.5", "planner", client=FakeClient())
        with patch("mas_host.cursor_session._runtime_manifest", return_value=first):
            descriptor = provider.descriptor()
            self.assertEqual(descriptor["node_version"], "24.21.0")
            self.assertEqual(descriptor["node_executable_sha256"], {"x86_64": "a" * 64, "aarch64": "b" * 64})
            first["architectures"]["x86_64"]["node_sha256"] = "c" * 64
            self.assertNotEqual(descriptor, provider.descriptor())

    def test_runtime_manifest_schema_and_integrity_are_required(self):
        from unittest.mock import Mock
        valid = {"format": "mas-cursor-node/1", "version": "24.21.0", "architectures": {
            "x86_64": {"node_sha256": "a" * 64}, "aarch64": {"node_sha256": "b" * 64}}}
        self.addCleanup(_runtime_manifest.cache_clear)
        for document in ({}, {**valid, "format": "mas-cursor-node/2"},
                         {**valid, "version": "latest"}, {**valid, "architectures": {}},
                         {**valid, "architectures": {"x86_64": {"node_sha256": "bad"},
                                                     "aarch64": {"node_sha256": "b" * 64}}}):
            resource = Mock()
            resource.joinpath.return_value.read_text.return_value = json.dumps(document)
            _runtime_manifest.cache_clear()
            with patch("mas_host.cursor_session.importlib.resources.files", return_value=resource):
                with self.assertRaisesRegex(ProviderError, "manifest is invalid"):
                    _runtime_manifest()
        _runtime_manifest.cache_clear()

    def test_sdk_version_rejection(self):
        with patch("mas_host.cursor_session.importlib.metadata.version", return_value="0.0.0"):
            with self.assertRaises(ProviderError):
                _launch_client("/private/workspace")


@unittest.skipUnless(sys.platform.startswith("linux") and sdk_available(), "optional pinned Cursor SDK wheel")
class CursorBridgeLifecycleTests(unittest.TestCase):
    def test_pinned_sdk_create_send_and_result_wire_contract(self):
        from cursor_sdk import CursorClient

        class Transport:
            def __init__(self):
                self.requests = []

            def unary(self, service, method, request, **kwargs):
                self.requests.append((method, request))
                if method == "CreateAgent":
                    return {"agentId": "agent-wire", "model": {"id": "composer-2.5"}}
                if method == "CloseAgent":
                    return {}
                raise AssertionError("Unexpected RPC: " + method)

            def stream(self, service, method, request, **kwargs):
                self.requests.append((method, request))
                yield {"result": {"result": {"runId": "run-wire", "agentId": "agent-wire",
                    "status": "RUN_LIFECYCLE_STATUS_FINISHED", "result": '{"total_cents":2998}',
                    "usage": {"inputTokens": 2, "outputTokens": 3, "totalTokens": 5}}}}

            def close(self):
                pass

        transport = Transport()
        client = CursorClient(transport=transport)
        invocation = CursorLocalProvider("composer-2.5", "planner", client=client, api_key="test-key").total(INVOICE)
        self.assertEqual(invocation.output, {"total_cents": 2998})
        self.assertEqual([method for method, _ in transport.requests], ["CreateAgent", "Send", "CloseAgent"])
        options = transport.requests[0][1]["options"]
        self.assertEqual(options["tools"], {"names": []})
        self.assertEqual(options["disallowedTools"], ["mcp", "task"])
        self.assertEqual(options["apiKey"], "test-key")
        self.assertFalse(options["local"].get("settingSources"))
        self.assertNotIn("cloud", options)

    def test_supervision_reaps_bridge_after_timeout_or_adapter_crash(self):
        from mas_host.contracts import AdapterError, AdapterTimeout, TaskRequest
        from mas_host.supervision import execute
        request = TaskRequest("client", "planner", "test", {}, "task", "message")
        for crash in (False, True):
            with self.subTest(crash=crash), tempfile.TemporaryDirectory() as workspace:
                marker = Path(workspace) / "bridge-pid"

                def handle(context, request):
                    client = _launch_client(workspace)
                    marker.write_text(str(client._owned_bridge.process.pid))
                    if crash:
                        os._exit(17)
                    time.sleep(30)
                    raise AssertionError("guardian failed to stop the adapter")

                with self.assertRaises(AdapterError if crash else AdapterTimeout):
                    execute(SimpleNamespace(handle=handle), SimpleNamespace(actor="planner"), request,
                            deadline=time.monotonic() + 2)
                self.assertTrue(marker.exists(), "bridge must have started before the failure")
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(marker.read_text()), 0)

    def test_host_sigkill_keeps_lock_until_real_bridge_is_reaped(self):
        import ctypes
        import signal
        from mas_host.contracts import TaskRequest
        from mas_host.journal import JournalError, RunLock
        from mas_host.supervision import execute
        libc = ctypes.CDLL(None, use_errno=True)
        original = ctypes.c_int()
        if libc.prctl(37, ctypes.byref(original), 0, 0, 0) != 0:
            self.skipTest("subreaper is unavailable")
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        owner = guardian = None
        identities = {}
        try:
            with tempfile.TemporaryDirectory() as workspace:
                marker = Path(workspace) / "identities"
                owner = os.fork()
                if owner == 0:
                    try:
                        lock = RunLock(Path(workspace))

                        def handle(context, request):
                            client = _launch_client(workspace)
                            pending = marker.with_suffix(".pending")
                            pending.write_text(json.dumps({"bridge": client._owned_bridge.process.pid,
                                "guardian": os.getppid(), "worker": os.getpid()}))
                            # Existence signals readiness only after the complete
                            # identity record has been closed and published.
                            pending.replace(marker)
                            time.sleep(30)

                        execute(SimpleNamespace(handle=handle),
                                SimpleNamespace(actor="planner", ownership_descriptors=(lock.fileno(),)),
                                TaskRequest("a", "planner", "test", {}, "t", "m"),
                                deadline=time.monotonic() + 20)
                    finally:
                        os._exit(18)
                until = time.monotonic() + 5
                while not marker.exists() and time.monotonic() < until:
                    time.sleep(0.01)
                self.assertTrue(marker.exists(), "bridge did not start")
                identities = json.loads(marker.read_text())
                guardian = identities["guardian"]
                os.kill(guardian, signal.SIGSTOP)
                os.kill(owner, signal.SIGKILL)
                os.waitpid(owner, 0)
                owner = None
                with self.assertRaises(JournalError):
                    RunLock(Path(workspace))
                os.kill(guardian, signal.SIGCONT)
                until = time.monotonic() + 4
                while time.monotonic() < until:
                    if os.waitpid(guardian, os.WNOHANG)[0]:
                        guardian = None
                        break
                    time.sleep(0.01)
                self.assertIsNone(guardian, "guardian failed to clean up after host death")
                for pid in (identities["bridge"], identities["worker"]):
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)
                with RunLock(Path(workspace)):
                    pass
        finally:
            if owner is not None:
                os.kill(owner, signal.SIGKILL)
                os.waitpid(owner, 0)
            if guardian is not None:
                os.kill(guardian, signal.SIGCONT)
                os.kill(guardian, signal.SIGTERM)
                os.waitpid(guardian, 0)
            if identities:
                try:
                    os.killpg(identities["worker"], signal.SIGKILL)
                except ProcessLookupError:
                    pass
            libc.prctl(36, original.value, 0, 0, 0)

    def test_real_bridge_starts_and_closes_without_an_agent_or_credentials(self):
        with tempfile.TemporaryDirectory() as workspace:
            client = _launch_client(workspace)
            process = client._owned_bridge.process
            self.assertIsNotNone(process)
            try:
                self.assertIsNone(process.poll())
                self.assertEqual(client._owned_bridge.endpoint.pid, process.pid)
                self.assertEqual(os.getpgid(process.pid), os.getpgrp())
            finally:
                client.close()
            self.assertIsNotNone(process.poll())
            with self.assertRaises(ProcessLookupError):
                os.kill(process.pid, 0)


if __name__ == "__main__":
    unittest.main()
