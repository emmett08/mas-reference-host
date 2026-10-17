"""Explicit Cursor transport policy and isolated launch contracts, without SDK calls."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from mas_host.contracts import invocation_failure_diagnostic
from mas_host.invoice_cursor import InvoiceCursorProvider as CursorLocalProvider
from mas_host.cursor_session import _bridge_child, launch_client as _launch_client
from mas_host.cursor_transport import (ENVIRONMENT_PROXY_HTTP1, SDK_DEFAULT,
    bridge_transport, transport_identity, validate_transport_policy)
from mas_host.provider_contract import ProviderError


class CursorTransportTests(unittest.TestCase):
    def test_policy_is_explicit_and_closed(self):
        for policy in (None, True, [], "", "environment-proxy", "--require evil.js"):
            with self.subTest(policy=policy), self.assertRaises(ValueError):
                validate_transport_policy(policy)
            with patch("mas_host.cursor_session._sdk_module") as sdk, self.assertRaises(ValueError):
                _launch_client("/private/workspace", transport_policy=policy)
            sdk.assert_not_called()

    def test_default_preserves_command_environment_and_provider_identity(self):
        environment = {"HTTPS_PROXY": "http://proxy.invalid:80", "NO_PROXY": "upper.invalid",
                       "no_proxy": "lower.invalid"}
        arguments = ["--host", "127.0.0.1", "--tool-callback-auth-token", "local-fixture-token"]
        command, child = bridge_transport(Path("/node"), Path("/bridge"), arguments,
                                          environment, SDK_DEFAULT)
        self.assertEqual(command, ["/node", "/bridge", *arguments])
        self.assertEqual(child, environment)
        self.assertIsNot(child, environment)
        self.assertEqual(transport_identity(SDK_DEFAULT), {})
        provider = CursorLocalProvider("model", "planner", client=object())
        explicit = CursorLocalProvider("model", "planner", client=object(), transport_policy=SDK_DEFAULT)
        self.assertEqual(provider.descriptor(), explicit.descriptor())
        self.assertNotIn("transport_policy", provider.descriptor())

    def test_proxy_policy_binds_bootstrap_and_preserves_arguments(self):
        arguments = ["--workspace", "/private/with space", "--tool-callback-url",
                     "http://127.0.0.1:1234", "--tool-callback-auth-token", "local-fixture-token"]
        command, environment = bridge_transport(Path("/node"), Path("/bridge"), arguments,
            {"HTTPS_PROXY": "http://proxy.invalid:80"}, ENVIRONMENT_PROXY_HTTP1)
        self.assertEqual(command[:2], ["/node", "--use-env-proxy"])
        self.assertEqual(command[3:], ["/bridge", *arguments])
        self.assertEqual(environment["HTTPS_PROXY"], "http://proxy.invalid:80")
        provider = CursorLocalProvider("model", "planner", client=object(),
                                       transport_policy=ENVIRONMENT_PROXY_HTTP1)
        identity = provider.descriptor()["transport_policy"]
        self.assertEqual(identity, {"policy": ENVIRONMENT_PROXY_HTTP1,
            "revision": "mas-cursor-transport/1",
            "bootstrap_sha256": hashlib.sha256(Path(command[2]).read_bytes()).hexdigest()})
        self.assertNotIn("proxy.invalid", str(identity))

    def test_no_proxy_keeps_effective_precedence_and_exact_wildcard(self):
        cases = [({}, ""), ({"NO_PROXY": "upper.invalid:443"}, "upper.invalid:443"),
                 ({"NO_PROXY": "upper.invalid", "no_proxy": "lower.invalid"}, "lower.invalid"),
                 ({"NO_PROXY": "upper.invalid", "no_proxy": ""}, "upper.invalid"),
                 ({"NO_PROXY": "*"}, "*"), ({"NO_PROXY": "upper.invalid", "no_proxy": "*"}, "*")]
        for supplied, effective in cases:
            before = supplied.copy()
            with self.subTest(environment=supplied):
                _, child = bridge_transport(Path("/node"), Path("/bridge"), [], supplied,
                                             ENVIRONMENT_PROXY_HTTP1)
                expected = "*" if effective == "*" else ",".join(filter(None,
                    (effective, "localhost,127.0.0.1,::1,[::1]")))
                self.assertEqual(child["no_proxy"], expected)
                self.assertEqual(child["NO_PROXY"], expected)
                self.assertEqual(supplied, before)

    def test_proxy_child_does_not_inherit_secrets_node_flags_or_tls_overrides(self):
        supplied = {"HTTPS_PROXY": "http://proxy.invalid:80", "NO_PROXY": "upper.invalid",
                    "no_proxy": "lower.invalid", "CURSOR_API_KEY": "private-fixture-key",
                    "NODE_OPTIONS": "--require /untrusted/hook", "NODE_USE_ENV_PROXY": "0",
                    "NODE_TLS_REJECT_UNAUTHORIZED": "0", "NODE_EXTRA_CA_CERTS": "/approved/ca.pem"}
        with tempfile.TemporaryDirectory() as workspace, patch.dict(os.environ, supplied, clear=True), \
                patch("mas_host.cursor_session._bridge_files", return_value=(Path("/node"), Path("/bridge"))), \
                patch("mas_host.cursor_session.os.execve") as execute:
            _bridge_child(workspace, ["--workspace", workspace], ENVIRONMENT_PROXY_HTTP1)
            executable, command, child = execute.call_args.args
            self.assertEqual(executable, "/node")
            self.assertEqual(command[:2], ["/node", "--use-env-proxy"])
            self.assertEqual(command[3:], ["/bridge", "--workspace", workspace])
            for name in ("CURSOR_API_KEY", "NODE_OPTIONS", "NODE_USE_ENV_PROXY", "NODE_TLS_REJECT_UNAUTHORIZED"):
                self.assertNotIn(name, child)
            self.assertEqual(child["NODE_EXTRA_CA_CERTS"], "/approved/ca.pem")
            self.assertEqual(dict(os.environ), supplied)

    def test_launch_selects_internal_policy_without_proxying_python_loopback(self):
        sdk = SimpleNamespace(DefaultHttpxClient=Mock(), Bridge=SimpleNamespace(launch=Mock()),
                              CursorClient=Mock())
        with patch("mas_host.cursor_session._sdk_module", return_value=sdk), \
                patch("mas_host.cursor_session._OwnedClient", return_value=object()):
            _launch_client("/private/workspace", transport_policy=ENVIRONMENT_PROXY_HTTP1)
        command = sdk.Bridge.launch.call_args.kwargs["command"]
        self.assertEqual(command[-4:], ["--bridge-child", "/private/workspace",
                                       "--mas-transport-policy", ENVIRONMENT_PROXY_HTTP1])
        sdk.DefaultHttpxClient.assert_called_once_with(timeout=45, trust_env=False)
        self.assertFalse(sdk.CursorClient.call_args.kwargs["allow_api_key_env_fallback"])
        self.assertEqual(sdk.CursorClient.call_args.kwargs["max_retries"], 0)

    def test_partial_launch_failure_reports_cleanup_failure_without_details(self):
        for close_bridge_fails, close_http_fails in ((True, False), (False, True), (True, True)):
            with self.subTest(bridge=close_bridge_fails, http=close_http_fails):
                http, bridge = Mock(), Mock()
                if close_http_fails:
                    http.close.side_effect = ValueError("private cleanup fixture")
                sdk = SimpleNamespace(DefaultHttpxClient=Mock(return_value=http),
                    Bridge=SimpleNamespace(launch=Mock(return_value=bridge)),
                    CursorClient=Mock(side_effect=ValueError("private launch fixture")))
                with patch("mas_host.cursor_session._sdk_module", return_value=sdk), \
                        patch("mas_host.cursor_session._close_bridge",
                              side_effect=ValueError("private bridge fixture") if close_bridge_fails else None) as close, \
                        self.assertRaises(ProviderError) as failure:
                    _launch_client("/private/workspace")
                close.assert_called_once_with(bridge)
                http.close.assert_called_once()
                self.assertEqual(invocation_failure_diagnostic(failure.exception),
                    {"provider": "cursor-local", "stage": "launch", "cleanup_failed": True})
                self.assertNotIn("private", str(failure.exception))

    def test_launch_interrupt_survives_failed_cleanup(self):
        primary = KeyboardInterrupt()
        http, bridge = Mock(), Mock()
        http.close.side_effect = ValueError("private fixture")
        sdk = SimpleNamespace(DefaultHttpxClient=Mock(return_value=http),
            Bridge=SimpleNamespace(launch=Mock(return_value=bridge)),
            CursorClient=Mock(side_effect=primary))
        with patch("mas_host.cursor_session._sdk_module", return_value=sdk), \
                patch("mas_host.cursor_session._close_bridge", side_effect=ValueError("private fixture")), \
                self.assertRaises(KeyboardInterrupt) as failure:
            _launch_client("/private/workspace")
        self.assertIs(failure.exception, primary)
        http.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
