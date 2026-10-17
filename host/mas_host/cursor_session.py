"""Domain-independent restricted Cursor transport, runtime and session lifecycle.

One MAS invocation submits one local agent run. Cursor can internally perform
multiple model requests; its reservation is an admission allowance, not a
supplier charge limit. Tool restrictions are not an operating-system sandbox.
"""
from __future__ import annotations

import importlib.metadata
import importlib.resources
import hashlib
from functools import lru_cache
import json
import os
from pathlib import Path
import platform
import re
import sys
import subprocess
import tempfile
import threading
from typing import Any, Callable, Mapping

from .contracts import Invocation
from .cursor_bridge_patch import patch_identity, validate_bridge_patch
from .cursor_transport import (SDK_DEFAULT, bridge_transport, transport_identity,
                               validate_transport_policy)
from .provider_contract import ProviderError
from .provider_outcomes import ProviderOutcomeFailure, completed_unusable, completed_cleanup_failure, not_sent

CURSOR_SDK_VERSION = "1.0.37"
MAX_STREAM_TEXT_BYTES = 65_536
RPC_TIMEOUT_SECONDS = 45


def _sdk_module():
    try:
        if importlib.metadata.version("cursor-sdk") != CURSOR_SDK_VERSION:
            raise ProviderError("Cursor local mode requires cursor-sdk==" + CURSOR_SDK_VERSION)
        import cursor_sdk
        return cursor_sdk
    except ImportError:
        raise ProviderError("Cursor local mode requires the optional cursor-sdk dependency") from None
    except importlib.metadata.PackageNotFoundError:
        raise ProviderError("Cursor local mode requires the optional cursor-sdk dependency") from None


@lru_cache(maxsize=1)
def _runtime_manifest() -> dict:
    """The same packaged release manifest drives installation and recovery identity."""
    try:
        value = json.loads(importlib.resources.files("mas_host").joinpath("cursor_runtime.json").read_text())
        if (type(value) is not dict or value.get("format") != "mas-cursor-node/1"
                or type(value.get("version")) is not str
                or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value["version"])
                or type(value.get("architectures")) is not dict
                or set(value["architectures"]) != {"x86_64", "aarch64"}):
            raise ValueError("invalid manifest")
        for entry in value["architectures"].values():
            if (type(entry) is not dict or type(entry.get("node_sha256")) is not str
                    or not re.fullmatch(r"[0-9a-f]{64}", entry["node_sha256"])):
                raise ValueError("invalid executable digest")
        return value
    except (OSError, ValueError, TypeError):
        raise ProviderError("Cursor Node runtime release manifest is invalid") from None


def _runtime_identity() -> dict:
    manifest = _runtime_manifest()
    return {"node_version": manifest["version"],
            "node_executable_sha256": {architecture: entry["node_sha256"]
                for architecture, entry in manifest["architectures"].items()}}


def _validate_node(node: Path) -> None:
    """Reject the old wheel binary or an unpinned replacement without executing it."""
    architecture = {"arm64": "aarch64", "amd64": "x86_64"}.get(platform.machine(), platform.machine())
    release = _runtime_manifest()["architectures"].get(architecture)
    if release is None:
        raise ProviderError("Cursor local mode supports only Linux x86_64 and aarch64")
    try:
        digest = hashlib.sha256()
        with node.open("rb") as binary:
            for block in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(block)
    except OSError:
        raise ProviderError("Cursor local Node executable is unavailable") from None
    if digest.hexdigest() != release["node_sha256"]:
        raise ProviderError("Cursor local mode requires the patched Node runtime; run scripts/install-cursor-runtime.py")


def _bridge_files() -> tuple[Path, Path]:
    """Use pinned bridge JavaScript and the verified patched Node replacement."""
    sdk = _sdk_module()
    root = Path(sdk.__file__).resolve().parent / "_vendor" / "bridge"
    node, entry = root / "bin" / "node", root / "dist" / "bin" / "cursor-sdk-bridge.js"
    if not node.is_file() or not entry.is_file():
        raise ProviderError("Cursor local mode requires the platform wheel with its bridge")
    _validate_node(node)
    try:
        validate_bridge_patch(root)
    except (OSError, ValueError, TypeError):
        raise ProviderError("Cursor local bridge requires its verified dependency repair; run scripts/install-cursor-runtime.py") from None
    return node, entry


def check_runtime() -> None:
    """Validate the optional pinned runtime without starting a process or call."""
    if not sys.platform.startswith("linux"):
        raise ProviderError("The supervised Cursor local host requires Linux")
    node, _ = _bridge_files()
    if not os.access(node, os.X_OK):
        raise ProviderError("Cursor local bridge executable is unavailable")


def _bridge_child(workspace: str, arguments: list[str], transport_policy: str = SDK_DEFAULT) -> None:
    """Exec the verified patched Node process in place, without inherited credentials.

    The SDK's shell launcher does not exec Node: terminating that shell alone
    can orphan Node. Direct exec also keeps the bridge in the host guardian's
    process group. Changes to the child environment never affect the caller.
    """
    validate_transport_policy(transport_policy)
    node, entry = _bridge_files()
    root = Path(workspace)
    if not root.is_absolute() or not root.is_dir():
        raise ProviderError("Cursor bridge requires its private workspace")
    private_home = root / "home"
    private_home.mkdir(mode=0o700)
    environment = {"PATH": os.defpath, "HOME": str(private_home),
                   "XDG_CONFIG_HOME": str(private_home / "config"),
                   "XDG_CACHE_HOME": str(private_home / "cache"),
                   "LANG": "C.UTF-8", "CURSOR_SDK_CLIENT_LANGUAGE": "python",
                   "CURSOR_SDK_PYTHON_VERSION": CURSOR_SDK_VERSION}
    for key in ("HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "no_proxy",
                "SSL_CERT_FILE", "SSL_CERT_DIR", "NODE_EXTRA_CA_CERTS"):
        if key in os.environ:
            environment[key] = os.environ[key]
    command, environment = bridge_transport(node, entry, arguments, environment, transport_policy)
    os.execve(str(node), command, environment)


class _OwnedClient:
    """Own the transport, native process, callback servers and stderr drainer."""

    def __init__(self, client, bridge, http_client):
        self.agents = client.agents
        self._client, self._owned_bridge, self._http_client = client, bridge, http_client
        self._stderr_thread = threading.Thread(target=_discard_stderr,
            args=(bridge.process.stderr.fileno(),), name="mas-cursor-stderr", daemon=True)
        self._stderr_thread.start()

    def close(self):
        primary = None
        for cleanup in (self._client.close,
                        lambda: _close_bridge(self._owned_bridge, self._stderr_thread),
                        self._http_client.close):
            try:
                cleanup()
            except BaseException as error:
                if primary is None:
                    primary = error
        if primary is not None:
            raise primary


def _discard_stderr(descriptor: int) -> None:
    """Drain post-discovery SDK diagnostics without buffering or exposing them."""
    try:
        while os.read(descriptor, 8192):
            pass
    except OSError:
        pass


def _close_bridge(bridge, stderr_thread=None):
    # SDK 1.0.37's close() constructs a proxy-aware loopback HTTP client before
    # its error handler; a bad proxy bypasses cleanup and can receive bridge
    # credentials. Termination needs no RPC. Own the process and callback
    # resources explicitly until they have stopped, including partial startup.
    process = bridge.process
    failure = None

    def attempt(cleanup):
        nonlocal failure
        try:
            cleanup()
        except BaseException as error:
            if failure is None:
                failure = error

    def stop_process():
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)

    def close_stderr():
        if process is not None and process.stderr is not None:
            process.stderr.close()

    attempt(stop_process)
    if stderr_thread is not None:
        attempt(lambda: stderr_thread.join(timeout=2))
    attempt(close_stderr)
    # The pinned SDK exposes no public standalone callback-server teardown.
    # Failure in one resource must not skip any remaining cleanup attempt.
    for name in ("_store_callback_server", "_tool_callback_server"):
        def close_callback(name=name):
            callback = getattr(bridge, name, None)
            if callback is not None:
                attempt(callback.close)
                attempt(lambda: setattr(bridge, name, None))
        attempt(close_callback)
    if failure is None:
        bridge.process = None
    if failure is not None:
        raise failure


def launch_client(workspace: str, transport_policy: str = SDK_DEFAULT):
    validate_transport_policy(transport_policy)
    sdk = _sdk_module()
    # Loopback RPC must not inherit internet proxies. Construct the transport
    # before the subprocess: SDK launch_bridge() leaks its bridge if client
    # construction subsequently fails (for example an unsupported SOCKS proxy).
    http_client = sdk.DefaultHttpxClient(timeout=RPC_TIMEOUT_SECONDS, trust_env=False)
    bridge = None
    try:
        command = [sys.executable, "-m", "mas_host.cursor_session", "--bridge-child", workspace]
        if transport_policy != SDK_DEFAULT:
            command.extend(["--mas-transport-policy", transport_policy])
        bridge = sdk.Bridge.launch(
            command=command,
            workspace=workspace, state_root=str(Path(workspace) / "state"), host="127.0.0.1", timeout=15,
        )
        client = sdk.CursorClient(
            bridge.endpoint, timeout=RPC_TIMEOUT_SECONDS, max_retries=0,
            http_client=http_client, allow_api_key_env_fallback=False,
        )
        return _OwnedClient(client, bridge, http_client)
    except BaseException as primary:
        cleanup_failed = False
        try:
            if bridge is not None:
                _close_bridge(bridge)
        except BaseException:
            cleanup_failed = True
        try:
            http_client.close()
        except BaseException:
            cleanup_failed = True
        if cleanup_failed and isinstance(primary, Exception):
            raise ProviderError("Cursor invocation failed; its external outcome may be unknown",
                diagnostic={"provider": "cursor-local", "stage": "launch", "cleanup_failed": True}) from None
        raise


def _safe_id(value: Any) -> str | None:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", value):
        return value
    return None


class CursorTextSession:
    """Text-only Cursor local agent, with one fresh private workspace per call.

    An injected SDK-compatible client is caller-owned and enables offline tests.
    Owned clients and their bridge are always closed. Local termination cannot
    establish that a provider has cancelled or stopped billing remote work.
    """

    def __init__(self, model: str, role: str, *, api_key: str | None = None,
                 client: Any = None, reservation_microusd: int = 100_000,
                 transport_policy: str = SDK_DEFAULT,
                 environ: Mapping[str, str] | None = None):
        self._transport_policy = validate_transport_policy(transport_policy)
        if not isinstance(model, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", model):
            raise ValueError("model must be a valid short model identifier")
        if model == "auto-smart":
            raise ValueError("Cursor Router requires an explicit optimisation policy; select a catalogue model")
        if type(reservation_microusd) is not int or not 1 <= reservation_microusd <= 100_000_000:
            raise ValueError("Cursor calls require a positive bounded spending reservation")
        self.model, self.role = model, role
        self.reservation_microusd = reservation_microusd
        self._api_key, self._client, self._environ = api_key, client, environ

    def descriptor(self) -> dict:
        return {"provider": "cursor-local", "model": self.model, "role": self.role,
                "contract": "restricted-cursor-session/1", "sdk_version": CURSOR_SDK_VERSION,
                **_runtime_identity(), "bridge_patch": patch_identity(),
                **transport_identity(self._transport_policy),
                "execution_policy": "isolated-text-only/3", "tools": [],
                "disallowed_tools": ["mcp", "task"], "setting_sources": [],
                "max_stream_text_bytes": MAX_STREAM_TEXT_BYTES,
                "rpc_timeout_seconds": RPC_TIMEOUT_SECONDS, "rpc_max_retries": 0,
                "model_retry_policy": "sdk_managed", "reservation_microusd": self.reservation_microusd}

    def execute_prompt(self, prompt: str, *, output_parser: Callable[[Any], dict],
                       request_id: str | None = None, classify_outcomes: bool = False) -> Invocation:
        """Execute trusted prompt construction with an application-owned parser.

        This is a low-level effect: applications call it through an admitted
        adapter. An injected client is caller-owned; otherwise credentials are
        resolved here and every newly created client/agent is closed here.
        """
        if (type(prompt) is not str or not prompt or not callable(output_parser)
                or request_id is not None and (type(request_id) is not str
                    or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,128}", request_id))):
            raise ProviderError("invalid Cursor text request")
        workspace = None
        invocation = None
        primary = None
        cleanup_failed = False
        try:
            workspace = tempfile.TemporaryDirectory(prefix="mas-cursor-local-")
            invocation = (self._run(workspace.name, prompt, request_id, output_parser, classify_outcomes=True)
                          if classify_outcomes else self._run(workspace.name, prompt, request_id, output_parser))
        except BaseException as error:
            primary = error
        finally:
            if workspace is not None:
                try:
                    workspace.cleanup()
                except BaseException as error:
                    cleanup_failed = True
                    if primary is None:
                        primary = error
        if primary is not None:
            if cleanup_failed and invocation is not None:
                raise completed_cleanup_failure("cursor-local", invocation.evidence, invocation.output) from None
            if not isinstance(primary, Exception):
                raise primary
            if isinstance(primary, ProviderOutcomeFailure):
                raise primary.with_cleanup_failure() if cleanup_failed else primary
            from .contracts import invocation_failure_diagnostic
            diagnostic = invocation_failure_diagnostic(primary) or {
                "provider": "cursor-local", "stage": "cleanup" if cleanup_failed else "launch",
                "cleanup_failed": False}
            diagnostic["cleanup_failed"] |= cleanup_failed
            raise ProviderError("Cursor invocation failed; its external outcome may be unknown",
                                diagnostic=diagnostic) from None
        return invocation

    def readiness(self) -> dict:
        """Validate optional local dependencies and credentials without a run."""
        if self._client is None:
            if self._api_key is None:
                from .provider_credentials import CredentialError, read_api_key
                try:
                    read_api_key("cursor", environ=self._environ)
                except CredentialError:
                    raise not_sent("cursor-local", "credentials_unavailable") from None
            try:
                check_runtime()
            except Exception:
                raise not_sent("cursor-local", "runtime_unavailable") from None
        return {"provider": "cursor-local", "ready": True, "inference_submitted": False}

    def _run(self, workspace: str, prompt: str, request_id: str | None,
             output_parser: Callable[[Any], dict], *, classify_outcomes: bool = False) -> Invocation:
        client = None
        invocation = None
        agent = run = None
        stage = "launch"
        primary = None
        cleanup_failed = False
        try:
            api_key = self._api_key
            if self._client is None and api_key is None:
                from .provider_credentials import read_api_key
                api_key = read_api_key("cursor", environ=self._environ)
            client = self._client if self._client is not None else launch_client(
                workspace, transport_policy=self._transport_policy)
            stage = "create"
            agent = client.agents.create(
                options={"model": self.model, "api_key": api_key,
                         "local": {"cwd": workspace, "dirs": [], "setting_sources": [], "custom_tools": {}},
                         "tools": [], "disallowed_tools": ["mcp", "task"], "mcp_servers": {}, "agents": {}},
            )
            stage = "send"
            run = agent.send(prompt)
            stage = "stream"
            observed_bytes = 0
            for event in run.events():
                message = getattr(event, "sdk_message", None)
                kind = getattr(message, "type", None)
                if kind == "tool_call":
                    raise ProviderError("Cursor attempted a tool call in text-only mode")
                texts = []
                if kind == "assistant":
                    blocks = getattr(getattr(message, "message", None), "content", ())
                    for block in blocks:
                        if getattr(block, "type", None) == "tool_use":
                            raise ProviderError("Cursor attempted a tool call in text-only mode")
                        texts.append(getattr(block, "text", ""))
                elif kind == "thinking":
                    texts.append(getattr(message, "text", ""))
                for text in texts:
                    if not isinstance(text, str):
                        raise ProviderError("Cursor returned an invalid stream body")
                    observed_bytes += len(text.encode("utf-8"))
                    if observed_bytes > MAX_STREAM_TEXT_BYTES:
                        raise ProviderError("Cursor exceeded the accepted stream text limit")
            stage = "wait"
            result = run.wait()
            stage = "result"
            if getattr(result, "status", None) != "finished":
                raise ProviderError("Cursor response did not complete")
            text = getattr(result, "result", None)
            evidence: dict[str, Any] = {
                "provider": "cursor-local", "model": self.model, "role": self.role,
                "sdk_version": CURSOR_SDK_VERSION, "node_version": _runtime_manifest()["version"],
                "bridge_patch": patch_identity(),
                "reserved_microusd": self.reservation_microusd,
                "spending_basis": "configured_reservation_not_provider_invoice",
                "submission_unit": "agent_run", "model_retry_policy": "sdk_managed",
            }
            if request_id:
                # Local SDK has no documented provider-side idempotency contract.
                evidence["client_request_id"] = request_id
            for field, value in (("agent_id", getattr(agent, "agent_id", None)),
                                 ("run_id", getattr(result, "id", None))):
                if _safe_id(value):
                    evidence[field] = value
            usage = getattr(result, "usage", None)
            tokens = {}
            for field in ("input_tokens", "output_tokens", "total_tokens", "cache_read_tokens",
                          "cache_write_tokens", "reasoning_tokens"):
                value = getattr(usage, field, None)
                if type(value) is int and 0 <= value <= 10**9:
                    tokens[field] = value
            if tokens:
                evidence["usage"] = tokens
            try:
                output = output_parser(text)
            except Exception:
                if classify_outcomes:
                    raise completed_unusable("cursor-local", "invalid_output", evidence) from None
                raise
            invocation = Invocation(output=output, evidence=evidence)
        except BaseException as error:
            if classify_outcomes and stage == "launch" and isinstance(error, Exception):
                from .provider_credentials import CredentialError
                primary = not_sent("cursor-local", "credentials_unavailable" if isinstance(error, CredentialError)
                                   else "runtime_unavailable" if isinstance(error, ProviderError)
                                   else "client_initialisation_failed")
            else:
                primary = error
        finally:
            # Always attempt all cleanup steps. The first failure keeps its
            # stage; later cleanup failures supply only a bounded boolean.
            def cancel_running():
                if run is not None and getattr(run, "status", None) == "running":
                    run.cancel()

            for cleanup in (cancel_running,
                            lambda: agent.close() if agent is not None else None,
                            lambda: client.close() if self._client is None and client is not None else None):
                try:
                    cleanup()
                except BaseException as error:
                    cleanup_failed = True
                    if primary is None:
                        primary, stage = error, "cleanup"
        if primary is not None:
            if cleanup_failed and invocation is not None:
                raise completed_cleanup_failure("cursor-local", invocation.evidence, invocation.output) from None
            if not isinstance(primary, Exception):
                raise primary
            if isinstance(primary, ProviderOutcomeFailure):
                raise primary.with_cleanup_failure() if cleanup_failed else primary
            from .contracts import invocation_failure_diagnostic
            earlier = invocation_failure_diagnostic(primary)
            if earlier is not None:
                cleanup_failed |= earlier["cleanup_failed"]
            raise ProviderError("Cursor invocation failed; its external outcome may be unknown",
                diagnostic={"provider": "cursor-local", "stage": stage,
                            "cleanup_failed": cleanup_failed}) from None
        return invocation


if __name__ == "__main__":
    try:
        if len(sys.argv) < 3 or sys.argv[1] != "--bridge-child":
            raise ProviderError("invalid restricted bridge invocation")
        arguments = sys.argv[3:]
        transport_policy = SDK_DEFAULT
        if arguments[:1] == ["--mas-transport-policy"]:
            if len(arguments) < 2:
                raise ProviderError("invalid restricted bridge transport")
            transport_policy, arguments = arguments[1], arguments[2:]
        _bridge_child(sys.argv[2], arguments, transport_policy)
    except Exception:
        # The SDK may incorporate bridge stderr into an exception. Never emit
        # paths, credentials, provider responses or inherited exception details.
        sys.stderr.write("Restricted Cursor bridge failed to start.\n")
        raise SystemExit(1) from None
