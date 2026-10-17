#!/usr/bin/env python3
"""Exercise installed release binaries/package using read-only test fixtures.

Run inside the release container with host/tests at /mounted-tests and examples
at /mounted-examples. All writable fixtures live under /tmp. Provider credentials
are removed before running tests; the container should also use --network none.
"""
from __future__ import annotations

import argparse
import json
import importlib.util
import ctypes
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import ssl
import struct
import subprocess
import sys
import tempfile

BOOTSTRAP_WHEEL_DIRS = (Path("/usr/share/python-wheels"), Path("/usr/local/share/python-wheels"))


def runtime_package_identity() -> dict:
    """The deployed runtime must not retain installer code or bootstrap wheels."""
    present = [name for name in ("pip", "ensurepip")
               if importlib.util.find_spec(name) is not None]
    if present:
        raise RuntimeError("Runtime image retains package installers: " + ", ".join(present))
    if any(any(directory.rglob("*.whl")) for directory in BOOTSTRAP_WHEEL_DIRS):
        raise RuntimeError("Runtime image retains bootstrap wheel payloads")
    return {"package_installers": "absent", "bootstrap_wheels": "absent"}


def runtime_identity(binaries: dict[str, Path]) -> dict:
    """Require the actual Alpine release to use its patched shared SQLite."""
    package_identity = runtime_package_identity()
    linkage = {}
    for name, binary in binaries.items():
        result = subprocess.run(["ldd", str(binary)], text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=10, check=False)
        listing = result.stdout
        if (result.returncode or "libsqlite3.so.0" not in listing or "musl" not in listing.lower()
                or "not found" in listing.lower() or "error loading" in listing.lower()):
            raise RuntimeError(f"{name} must link successfully to musl and libsqlite3.so.0")
        linkage[name] = listing.strip().splitlines()
    library = ctypes.CDLL("libsqlite3.so.0")
    library.sqlite3_libversion.argtypes = []
    library.sqlite3_libversion.restype = ctypes.c_char_p
    native_version = library.sqlite3_libversion().decode("ascii")
    if tuple(int(part) for part in native_version.split(".")) < (3, 53, 2):
        raise RuntimeError("the container SQLite runtime must be at least 3.53.2")
    if sqlite3.sqlite_version != native_version:
        raise RuntimeError("Python and the shared native SQLite runtime must have identical versions")
    return {"native_sqlite": native_version, "python_sqlite": sqlite3.sqlite_version,
            "rust_dynamic_linkage": linkage, **package_identity}


def cursor_runtime_identity(binaries: dict[str, Path]) -> dict:
    """Check the optional glibc image and exercise its actual native SDK bridge."""
    package_identity = runtime_package_identity()
    libc = ctypes.CDLL(None)
    try:
        libc.gnu_get_libc_version.restype = ctypes.c_char_p
        glibc_version = libc.gnu_get_libc_version().decode("ascii")
    except AttributeError:
        raise RuntimeError("Cursor SDK runtime requires glibc") from None
    for name, binary in binaries.items():
        # ELF PT_INTERP identifies a dynamic loader. Reading the bounded program
        # headers avoids adding an ldd shell or binutils to the minimal image.
        with binary.open("rb") as source:
            header = source.read(64)
            if len(header) != 64 or header[:6] != b"\x7fELF\x02\x01":
                raise RuntimeError(f"{name} must be a 64-bit little-endian ELF executable")
            offset, = struct.unpack_from("<Q", header, 32)
            entry_size, count = struct.unpack_from("<HH", header, 54)
            if not 1 <= count <= 256 or entry_size != 56 or offset + count * entry_size > binary.stat().st_size:
                raise RuntimeError(f"{name} has an invalid ELF program header table")
            source.seek(offset)
            headers = source.read(entry_size * count)
            if any(struct.unpack_from("<I", headers, i * entry_size)[0] == 3 for i in range(count)):
                raise RuntimeError(f"{name} must be a static musl executable in the Cursor image")
    ctypes.CDLL("libstdc++.so.6")
    from mas_host.cursor_session import _bridge_files
    node, _ = _bridge_files()
    node_version = subprocess.run([str(node), "--version"], text=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=10, check=True).stdout.strip()
    certificates = ssl.create_default_context().get_ca_certs()
    if not certificates:
        raise RuntimeError("Cursor runtime requires trusted TLS certificate roots")
    return {"runtime": "cursor-glibc", "rust_linkage": "static-musl-bundled-sqlite",
            "glibc": glibc_version, "node_version": node_version, "tls_roots": len(certificates),
            "python_sqlite": sqlite3.sqlite_version, **package_identity,
            **cursor_bridge_smoke()}


def cursor_bridge_smoke() -> dict:
    """Launch and reap both installed transports without creating an agent or API call."""
    from mas_host.cursor_session import CURSOR_SDK_VERSION, launch_client
    from mas_host.cursor_transport import (
        ENVIRONMENT_PROXY_HTTP1, SDK_DEFAULT, transport_identity,
    )
    checks = {}
    for policy in (SDK_DEFAULT, ENVIRONMENT_PROXY_HTTP1):
        # Read from the installed package so a missing bootstrap fails the
        # release gate. The launch also proves its imports work with this SDK.
        identity = transport_identity(policy)
        with tempfile.TemporaryDirectory(prefix="mas-cursor-smoke-", dir="/tmp") as directory:
            client = launch_client(directory, transport_policy=policy)
            try:
                bridge = client._owned_bridge
                process = bridge.process
                if process is None or process.poll() is not None:
                    raise RuntimeError("Cursor bridge did not remain running during startup")
                if bridge.endpoint.pid != process.pid:
                    raise RuntimeError("Cursor bridge supervision must own Node directly")
                if os.getpgid(process.pid) != os.getpgrp():
                    raise RuntimeError("Cursor bridge must remain in the guardian's process group")
                version = bridge.endpoint.server_version
            finally:
                client.close()
            if process.poll() is None:
                raise RuntimeError("Cursor bridge survived client.close()")
            checks[policy] = {"bridge_version": version, "bridge_started_and_reaped": True,
                              "provider_calls": 0, **identity}
    return {"cursor_sdk": CURSOR_SDK_VERSION,
            "bridge_version": checks[SDK_DEFAULT]["bridge_version"],
            "bridge_started_and_reaped": True, "provider_calls": 0,
            "cursor_transport_checks": checks}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cursor", action="store_true", help="check the optional glibc Cursor image")
    args = parser.parse_args()
    # Smoke checks must not inherit credentials or a user's provider selection.
    for name in list(os.environ):
        if (name.startswith(("OPENAI_", "CURSOR_"))
                or name in ("MAS_PLANNER_PROVIDER", "MAS_REVIEWER_PROVIDER", "MAS_PLANNER_MODEL", "MAS_REVIEWER_MODEL")):
            os.environ.pop(name, None)
    tests = Path("/mounted-tests")
    examples = Path("/mounted-examples")
    if not tests.is_dir() or not examples.is_dir():
        print("Mount host/tests at /mounted-tests and examples at /mounted-examples.", file=sys.stderr)
        return 2
    binaries = {name: Path("/usr/local/bin") / name for name in ("mas", "mas-gateway")}
    if not all(path.is_file() and os.access(path, os.X_OK) for path in binaries.values()):
        print("The installed release binaries are unavailable.", file=sys.stderr)
        return 2
    try:
        identities = cursor_runtime_identity(binaries) if args.cursor else runtime_identity(binaries)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Release runtime identity check failed: {error}", file=sys.stderr)
        return 2
    # An inherited source path would test the checkout instead of the installed
    # wheel, so remove it from the test subprocess environment.
    environment = dict(os.environ)
    for name in ("PYTHONPATH", "OPENAI_API_KEY", "OPENAI_API_KEY_FILE", "CURSOR_API_KEY", "CURSOR_API_KEY_FILE"):
        environment.pop(name, None)
    environment.update(MAS_BIN=str(binaries["mas"]),
                       MAS_GATEWAY_BIN=str(binaries["mas-gateway"]),
                       MAS_MODE="mock", PYTHONDONTWRITEBYTECODE="1")
    with tempfile.TemporaryDirectory(prefix="mas-release-check-", dir="/tmp") as directory:
        root = Path(directory)
        copied_tests = root / "host" / "tests"
        shutil.copytree(tests, copied_tests, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copytree(examples, root / "examples")
        release = root / "rust-protocol" / "target" / "release"
        release.mkdir(parents=True)
        for name, binary in binaries.items():
            (release / name).symlink_to(binary)
        print(json.dumps({"check": "installed-release-container", "uid": os.getuid(),
                          "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                          "platform": platform.platform(), "network_required": False,
                          **identities}), flush=True)
        # Isolated mode also ignores Python's user site and the current working
        # directory; unittest explicitly adds only the copied tests directory.
        return subprocess.run([sys.executable, "-I", "-m", "unittest", "discover",
                               "-s", str(copied_tests), "-v"], cwd=root,
                              env=environment, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
