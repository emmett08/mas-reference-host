"""Authenticated wire requests, separate worker, Rust execution and durable replay."""
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from mas_host.demo import DEFAULT_INVOICE
from mas_host.bridges.mcp import metadata, routing_headers
from mas_host.bridges.service_client import TaskServiceClient, http_exchange


class BridgeEndToEndTests(unittest.TestCase):
    def test_cli_mcp_a2a_and_service_share_durable_receipts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            token = "offline-test-token-" + "a" * 32
            token_file, verifier_file, config_file = [root / name for name in ("token", "verifier.json", "config.json")]
            token_file.write_text(token)
            token_file.chmod(0o600)
            verifier_file.write_text(json.dumps({"version": 1, "credentials": [{
                "sha256": hashlib.sha256(token.encode()).hexdigest(), "principal": "alice",
                "scopes": ["mas:submit", "mas:read", "mas:cancel", "mas:update"]}]}))
            verifier_file.chmod(0o600)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            endpoint = "http://127.0.0.1:" + str(port)
            config_file.write_text(json.dumps({"version": 1, "mode": "mock", "public_url": endpoint,
                "grants": {"alice": ["invoice_mesh"]}, "auth": {"type": "static", "file": str(verifier_file)}}))
            base = [sys.executable, "-m", "mas_host", "bridge"]
            common = ["--config", str(config_file), "--state-dir", str(root / "state")]
            process = subprocess.Popen([*base, "serve", *common, "--port", str(port), "--allow-loopback-http"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 10
                while True:
                    if process.poll() is not None:
                        self.fail("bridge startup failed: " + process.stderr.read().decode())
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                            break
                    except OSError:
                        if time.monotonic() > deadline:
                            self.fail("bridge startup timed out")
                        time.sleep(0.02)
                client = TaskServiceClient(endpoint, token_file=token_file, principal="alice", allow_loopback_http=True)
                message = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
                    "_meta": metadata(), "name": "mas.submit", "arguments": {
                        "request_key": "invoice-1", "workflow": "invoice_mesh", "input": {"invoice": DEFAULT_INVOICE}}}}
                headers = {**routing_headers(message), "Authorization": "Bearer " + token,
                           "Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
                status, _, wire = http_exchange(endpoint + "/mcp", body=json.dumps(message).encode(),
                    headers=headers, allow_loopback_http=True)
                self.assertEqual(status, 200, wire)
                self.assertNotIn("error", json.loads(wire))
                submitted = client.submit("alice", "invoice-1", "invoice_mesh", {"invoice": DEFAULT_INVOICE})
                self.assertEqual(submitted["state"], "queued")
                run = subprocess.run([*base, "worker", *common, "--once"], capture_output=True, timeout=30)
                self.assertEqual(run.returncode, 0, run.stderr.decode())
                self.assertEqual(client.result("alice", submitted["task_id"])["total_cents"], 5497)
                replay = client.submit("alice", "invoice-1", "invoice_mesh", {"invoice": DEFAULT_INVOICE})
                self.assertEqual(replay["task_id"], submitted["task_id"])
                self.assertEqual(replay["state"], "completed")
                self.assertTrue(client.events("alice", replay["task_id"]))
                a2a = {"jsonrpc": "2.0", "id": "a2a-1", "method": "SendMessage", "params": {
                    "message": {"messageId": "message-1", "role": "ROLE_USER", "parts": [{"data": {
                        "workflow": "invoice_mesh", "input": {"invoice": DEFAULT_INVOICE}}, "mediaType": "application/json"}]},
                    "configuration": {"returnImmediately": True}}}
                status, _, wire = http_exchange(endpoint + "/a2a", body=json.dumps(a2a).encode(),
                    headers={"A2A-Version": "1.0", "Authorization": "Bearer " + token,
                             "Content-Type": "application/json"}, allow_loopback_http=True)
                self.assertEqual(status, 200, wire)
                reply = json.loads(wire)
                self.assertNotIn("error", reply)
                # The wire task and service record describe the same durable work.
                task = reply["result"].get("task", reply["result"])
                self.assertEqual(client.inspect("alice", task["id"])["state"], "queued")
                self.assertEqual(client.cancel("alice", task["id"])["state"], "cancelled")
            finally:
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()
