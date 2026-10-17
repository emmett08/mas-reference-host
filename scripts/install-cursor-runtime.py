#!/usr/bin/env python3
"""Install the hash-locked, patched native runtime for Cursor's SDK bridge.

Run with the same interpreter as the installed cursor-sdk. Use --out to fetch
files for an offline image build and --source to install those verified files.
The installer replaces Node, applies the reviewed native-Headers bridge repair,
removes the obsolete undici/busboy packages and refreshes the wheel RECORD.
The official Node licence is retained; npm is not installed.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import io
import json
import os
from pathlib import Path
import platform
import re
import sys
import tarfile
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCK = ROOT / "host" / "mas_host" / "cursor_runtime.json"
ARCHES = {"amd64": "x86_64", "x86_64": "x86_64", "arm64": "aarch64", "aarch64": "aarch64"}
MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_NODE_BYTES = 256 * 1024 * 1024
SDK_VERSION = "1.0.37"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_lock(path: Path, architecture: str) -> tuple[dict, dict]:
    lock = json.loads(path.read_text())
    if lock.get("format") != "mas-cursor-node/1" or not re.fullmatch(r"\d+\.\d+\.\d+", lock.get("version", "")):
        raise ValueError("Unsupported Cursor Node runtime lock")
    entry = lock["architectures"][architecture]
    for field in ("node_sha256", "archive_sha256", "license_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}", entry.get(field, "")):
            raise ValueError("Invalid Cursor Node runtime digest")
    expected_root = f"node-v{lock['version']}-linux-{'x64' if architecture == 'x86_64' else 'arm64'}"
    if (entry.get("archive_root") != expected_root
            or entry.get("url") != f"https://nodejs.org/dist/v{lock['version']}/{expected_root}.tar.xz"):
        raise ValueError("Cursor Node must use the locked official Linux distribution")
    return lock, entry


def download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read(MAX_ARCHIVE_BYTES + 1)
    if len(data) > MAX_ARCHIVE_BYTES:
        raise ValueError("Node archive exceeds its bounded download size")
    return data


def archive_files(data: bytes, entry: dict) -> dict[str, bytes]:
    if digest(data) != entry["archive_sha256"]:
        raise ValueError("Node archive hash mismatch")
    files = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:xz") as archive:
        for name, relative, limit in (("node", "bin/node", MAX_NODE_BYTES), ("LICENSE", "LICENSE", 2 * 1024 * 1024)):
            member = archive.getmember(entry["archive_root"] + "/" + relative)
            if not member.isfile() or member.size > limit:
                raise ValueError("Node archive contains an invalid executable or licence")
            files[name] = archive.extractfile(member).read(limit + 1)
    verify_files(files, entry)
    return files


def verify_files(files: dict[str, bytes], entry: dict) -> None:
    for name, field in (("node", "node_sha256"), ("LICENSE", "license_sha256")):
        if digest(files[name]) != entry[field]:
            raise ValueError(f"Node {name} hash mismatch")


def write_file(path: Path, data: bytes, mode: int) -> None:
    """Replace atomically, without following a pre-existing destination symlink."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".mas-node-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), mode)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def sdk_directory() -> Path:
    if importlib.metadata.version("cursor-sdk") != SDK_VERSION:
        raise ValueError(f"Install cursor-sdk=={SDK_VERSION} with this interpreter first")
    spec = importlib.util.find_spec("cursor_sdk")
    if spec is None or spec.origin is None:
        raise ValueError("The installed Cursor SDK cannot be located")
    directory = Path(spec.origin).resolve().parent / "_vendor" / "bridge"
    if not (directory / "bin" / "node").is_file() or not (directory / "dist" / "bin" / "cursor-sdk-bridge.js").is_file():
        raise ValueError("The installed Cursor SDK platform wheel is incomplete")
    return directory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--arch", choices=tuple(ARCHES))
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--out", type=Path, help="fetch verified node and LICENSE into a directory")
    modes.add_argument("--source", type=Path, help="install verified node and LICENSE from a directory")
    args = parser.parse_args()
    requested = args.arch or platform.machine().lower()
    if requested not in ARCHES:
        parser.error("supported architectures are Linux amd64 and arm64")
    architecture = ARCHES[requested]
    if not args.out and (platform.system() != "Linux" or ARCHES.get(platform.machine().lower()) != architecture):
        parser.error("installation requires the matching Linux architecture")
    lock, entry = read_lock(args.lock, architecture)
    # Resolve SDK identity before network access.
    directory = None if args.out else sdk_directory()
    if directory is not None:
        # A checkout invocation uses its reviewed repair, even before mas-host
        # is installed. Container builds use the installed host wheel instead.
        if (ROOT / "host" / "mas_host" / "cursor_bridge_patch.py").is_file():
            sys.path.insert(0, str(ROOT / "host"))
        try:
            from mas_host.cursor_bridge_patch import apply_bridge_patch, refresh_sdk_record
        except ImportError:
            parser.error("Install the current mas-host package before repairing the Cursor runtime")
    if args.source:
        files = {name: (args.source / name).read_bytes() for name in ("node", "LICENSE")}
        verify_files(files, entry)
    else:
        files = archive_files(download(entry["url"]), entry)
    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
        write_file(args.out / "node", files["node"], 0o755)
        write_file(args.out / "LICENSE", files["LICENSE"], 0o644)
        target = args.out / "node"
    else:
        # Write the licence first; a failed binary replacement leaves the old
        # executable intact and runtime preflight continues to reject it.
        write_file(directory / "NODE_LICENSE", files["LICENSE"], 0o644)
        target = directory / "bin" / "node"
        write_file(target, files["node"], 0o755)
        apply_bridge_patch(directory)
        refresh_sdk_record(directory)
    print(json.dumps({"node_version": lock["version"], "architecture": architecture,
                      "node_sha256": entry["node_sha256"], "path": str(target)}))


if __name__ == "__main__":
    main()
