"""Regressions for installed release packaging and native bridge smoke gates."""
from __future__ import annotations

import importlib.machinery
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch


SOURCE = Path(__file__).resolve().parents[1] / "release-container-check.py"
SPEC = importlib.util.spec_from_file_location("release_container_check", SOURCE)
RUNTIME = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNTIME)


class RuntimePackageTests(unittest.TestCase):
    def check_fixture(self, present: tuple[str, ...], bootstrap_dirs: tuple[Path, ...] = ()) -> dict:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for name in present:
                package = directory / name
                package.mkdir()
                (package / "__init__.py").write_text("")
                if name == "ensurepip":
                    bundled = package / "_bundled"
                    bundled.mkdir()
                    (bundled / "pip-unpatched.whl").write_bytes(b"bootstrap fixture")
            def find_spec(name):
                return importlib.machinery.PathFinder.find_spec(name, [str(directory)])
            with patch.object(RUNTIME.importlib.util, "find_spec", side_effect=find_spec), \
                    patch.object(RUNTIME, "BOOTSTRAP_WHEEL_DIRS", bootstrap_dirs):
                return RUNTIME.runtime_package_identity()

    def test_installed_pip_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "package installers: pip"):
            self.check_fixture(("pip",))

    def test_bootstrap_bundle_is_rejected_after_pip_removal(self):
        with self.assertRaisesRegex(RuntimeError, "package installers: ensurepip"):
            self.check_fixture(("ensurepip",))

    def test_runtime_without_installers_is_accepted(self):
        self.assertEqual(self.check_fixture(()), {"package_installers": "absent", "bootstrap_wheels": "absent"})

    def test_external_wolfi_bootstrap_wheel_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "pip-bootstrap.whl").write_bytes(b"bootstrap wheel fixture")
            with self.assertRaisesRegex(RuntimeError, "bootstrap wheel payloads"):
                self.check_fixture((), (directory,))


class CursorBridgeSmokeTests(unittest.TestCase):
    def setUp(self):
        # Model an installed package, including the real transport selector and
        # bootstrap. No optional SDK, Node process or provider is needed here.
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.package_root = Path(temporary.name) / "mas_host"
        self.package_root.mkdir()
        host_package = SOURCE.parents[1] / "host" / "mas_host"
        for name in ("cursor_transport.py", "cursor_transport_bootstrap.mjs"):
            shutil.copy2(host_package / name, self.package_root / name)
        package = ModuleType("mas_host")
        package.__path__ = [str(self.package_root)]
        contract = ModuleType("mas_host.provider_contract")
        contract.ProviderError = RuntimeError
        local = ModuleType("mas_host.cursor_session")
        local.CURSOR_SDK_VERSION = "1.0.37"
        self.clients = []
        self.launch = local.launch_client = Mock(side_effect=self.make_client)
        spec = importlib.util.spec_from_file_location(
            "mas_host.cursor_transport", self.package_root / "cursor_transport.py")
        self.transport = importlib.util.module_from_spec(spec)
        modules = patch.dict(sys.modules, {"mas_host": package,
            "mas_host.provider_contract": contract, "mas_host.cursor_session": local,
            "mas_host.cursor_transport": self.transport})
        modules.start()
        self.addCleanup(modules.stop)
        spec.loader.exec_module(self.transport)
        for name in ("getpgid", "getpgrp"):
            group = patch.object(RUNTIME.os, name, return_value=900)
            group.start()
            self.addCleanup(group.stop)

    def make_client(self, workspace, *, transport_policy):
        self.assertTrue(Path(workspace).is_dir())
        self.assertIn(transport_policy, self.transport.TRANSPORT_POLICIES)
        process = SimpleNamespace(pid=1000 + len(self.clients), returncode=None)
        process.poll = lambda: process.returncode
        endpoint = SimpleNamespace(pid=process.pid, server_version="1.0.0")
        bridge = SimpleNamespace(process=process, endpoint=endpoint)
        # Deliberately expose no agent API: any provider call fails this test.
        client = SimpleNamespace(_owned_bridge=bridge,
            close=Mock(side_effect=lambda: setattr(process, "returncode", 0)))
        self.clients.append(client)
        return client

    def test_both_installed_transports_start_and_reap_without_provider_calls(self):
        result = RUNTIME.cursor_bridge_smoke()
        self.assertEqual([call.kwargs["transport_policy"] for call in self.launch.call_args_list],
                         ["sdk-default", "environment-proxy-http1"])
        workspaces = [call.args[0] for call in self.launch.call_args_list]
        self.assertEqual(len(set(workspaces)), 2)
        self.assertTrue(all(not Path(path).exists() for path in workspaces))
        self.assertEqual({key: result[key] for key in (
            "cursor_sdk", "bridge_version", "bridge_started_and_reaped", "provider_calls")},
            {"cursor_sdk": "1.0.37", "bridge_version": "1.0.0",
             "bridge_started_and_reaped": True, "provider_calls": 0})
        checks = result["cursor_transport_checks"]
        self.assertEqual(set(checks), {"sdk-default", "environment-proxy-http1"})
        for check, client in zip(checks.values(), self.clients):
            self.assertTrue(check["bridge_started_and_reaped"])
            self.assertEqual(check["provider_calls"], 0)
            client.close.assert_called_once_with()
            self.assertIsNotNone(client._owned_bridge.process.poll())
        self.assertEqual(checks["environment-proxy-http1"]["transport_policy"],
            self.transport.transport_identity("environment-proxy-http1")["transport_policy"])

    def test_missing_installed_bootstrap_fails_before_proxy_launch(self):
        (self.package_root / "cursor_transport_bootstrap.mjs").unlink()
        with self.assertRaisesRegex(RuntimeError, "transport bootstrap is unavailable"):
            RUNTIME.cursor_bridge_smoke()
        self.launch.assert_called_once()
        self.assertEqual(self.launch.call_args.kwargs["transport_policy"], "sdk-default")
        self.clients[0].close.assert_called_once_with()

    def test_proxy_launch_failure_does_not_report_success(self):
        def launch(workspace, *, transport_policy):
            if transport_policy == "environment-proxy-http1":
                raise RuntimeError("bootstrap import failed")
            return self.make_client(workspace, transport_policy=transport_policy)
        self.launch.side_effect = launch
        with self.assertRaisesRegex(RuntimeError, "bootstrap import failed"):
            RUNTIME.cursor_bridge_smoke()
        self.assertEqual(self.launch.call_count, 2)
        self.clients[0].close.assert_called_once_with()

    def test_failed_startup_still_closes_the_client(self):
        def launch(workspace, *, transport_policy):
            client = self.make_client(workspace, transport_policy=transport_policy)
            client._owned_bridge.process.returncode = 1
            return client
        self.launch.side_effect = launch
        with self.assertRaisesRegex(RuntimeError, "did not remain running"):
            RUNTIME.cursor_bridge_smoke()
        self.clients[0].close.assert_called_once_with()

    def test_wrong_bridge_process_is_rejected_and_closed(self):
        def launch(workspace, *, transport_policy):
            client = self.make_client(workspace, transport_policy=transport_policy)
            client._owned_bridge.endpoint.pid += 1
            return client
        self.launch.side_effect = launch
        with self.assertRaisesRegex(RuntimeError, "must own Node directly"):
            RUNTIME.cursor_bridge_smoke()
        self.clients[0].close.assert_called_once_with()

    def test_detached_bridge_is_rejected_and_closed(self):
        with patch.object(RUNTIME.os, "getpgid", return_value=901):
            with self.assertRaisesRegex(RuntimeError, "guardian's process group"):
                RUNTIME.cursor_bridge_smoke()
        self.clients[0].close.assert_called_once_with()

    def test_surviving_bridge_fails_the_gate(self):
        def launch(workspace, *, transport_policy):
            client = self.make_client(workspace, transport_policy=transport_policy)
            client.close.side_effect = None
            return client
        self.launch.side_effect = launch
        with self.assertRaisesRegex(RuntimeError, "survived client.close"):
            RUNTIME.cursor_bridge_smoke()
        self.clients[0].close.assert_called_once_with()

    def test_cleanup_failure_fails_the_gate(self):
        def launch(workspace, *, transport_policy):
            client = self.make_client(workspace, transport_policy=transport_policy)
            client.close.side_effect = RuntimeError("cleanup failed")
            return client
        self.launch.side_effect = launch
        with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
            RUNTIME.cursor_bridge_smoke()
        self.clients[0].close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
