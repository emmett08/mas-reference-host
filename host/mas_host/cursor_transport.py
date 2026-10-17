"""Select the restricted Cursor bridge's explicit, replay-bound transport policy."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Mapping

from .provider_contract import ProviderError

SDK_DEFAULT = "sdk-default"
ENVIRONMENT_PROXY_HTTP1 = "environment-proxy-http1"
TRANSPORT_POLICIES = (SDK_DEFAULT, ENVIRONMENT_PROXY_HTTP1)
_LOOPBACK_BYPASS = "localhost,127.0.0.1,::1,[::1]"


def validate_transport_policy(policy: str) -> str:
    if type(policy) is not str or policy not in TRANSPORT_POLICIES:
        raise ValueError("Cursor transport policy must be sdk-default or environment-proxy-http1")
    return policy


def _bootstrap() -> Path:
    path = Path(__file__).with_name("cursor_transport_bootstrap.mjs")
    if not path.is_file():
        raise ProviderError("Cursor transport bootstrap is unavailable")
    return path


def transport_identity(policy: str) -> dict:
    """Preserve old default bindings; bind the opt-in code without SDK imports."""
    if validate_transport_policy(policy) == SDK_DEFAULT:
        return {}
    try:
        digest = hashlib.sha256(_bootstrap().read_bytes()).hexdigest()
    except OSError:
        raise ProviderError("Cursor transport bootstrap is unavailable") from None
    return {"transport_policy": {"policy": policy, "revision": "mas-cursor-transport/1",
                                 "bootstrap_sha256": digest}}


def bridge_transport(node: Path, entry: Path, arguments: list[str],
                     environment: Mapping[str, str], policy: str) -> tuple[list[str], dict[str, str]]:
    """Build a fixed command and a copy of the already restricted child environment.

    Node's startup proxy configuration selects the nonempty lowercase NO_PROXY
    value before uppercase. Preserve that effective list, including the exact
    wildcard, while ensuring authenticated local SDK callbacks stay direct.
    No parent environment, provider key or arbitrary Node option is introduced.
    """
    child_environment = dict(environment)
    if validate_transport_policy(policy) == SDK_DEFAULT:
        return [str(node), str(entry), *arguments], child_environment
    effective = child_environment.get("no_proxy") or child_environment.get("NO_PROXY") or ""
    # Node's fetch implementation recognises '*' only as the entire value.
    bypass = "*" if effective == "*" else ",".join(filter(None, (effective, _LOOPBACK_BYPASS)))
    child_environment["no_proxy"] = child_environment["NO_PROXY"] = bypass
    return [str(node), "--use-env-proxy", str(_bootstrap()), str(entry), *arguments], child_environment
