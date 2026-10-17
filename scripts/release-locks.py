#!/usr/bin/env python3
"""Refresh wheel-only dependency locks for the supported Linux container targets.

Run with CPython 3.14. Resolution is deliberately an explicit maintenance action;
normal builds only consume the committed, hash-checked result.
"""
from __future__ import annotations

import argparse
import email
import hashlib
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PLATFORMS = ("manylinux2014_x86_64", "manylinux2014_aarch64", "musllinux_1_1_x86_64", "musllinux_1_1_aarch64")
BRIDGE_PLATFORMS = (*PLATFORMS[:2], "musllinux_1_2_x86_64", "musllinux_1_2_aarch64")


def normalise(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def collect(directory: Path) -> dict[str, tuple[str, set[str]]]:
    packages: dict[str, tuple[str, set[str]]] = {}
    for path in sorted(directory.rglob("*.whl")):
        with zipfile.ZipFile(path) as wheel:
            metadata = email.message_from_bytes(wheel.read(next(
                name for name in wheel.namelist() if name.endswith(".dist-info/METADATA"))))
        name, version = normalise(metadata["Name"]), metadata["Version"]
        previous, hashes = packages.setdefault(name, (version, set()))
        if previous != version:
            raise ValueError(f"Platform versions disagree for {name}: {previous}, {version}")
        hashes.add(hashlib.sha256(path.read_bytes()).hexdigest())
    return packages


def refresh(filename: str, requirements: list[str], platforms: tuple[str, ...],
            keep_versions: bool = False) -> None:
    if keep_versions and (ROOT / "host" / filename).exists():
        requirements = [*requirements, *re.findall(
            r"^([a-z0-9-]+==[^\s]+)", (ROOT / "host" / filename).read_text(), re.MULTILINE)]
    with tempfile.TemporaryDirectory(prefix="mas-lock-") as temporary:
        directory = Path(temporary)
        for platform in platforms:
            subprocess.run([
                sys.executable, "-m", "pip", "download", "--only-binary=:all:",
                "--platform", platform, "--python-version", "3.14", "--implementation", "cp",
                "--abi", "cp314", "--dest", str(directory / platform), *requirements,
            ], check=True)
        packages = collect(directory)
    if any("musl" in platform for platform in platforms):
        scope = "CPython 3.14 Linux amd64/arm64, glibc/musl"
    elif len(platforms) > 1:
        scope = "CPython 3.14 Linux amd64/arm64, glibc only"
    else:
        scope = "CPython 3.14 Linux amd64 (CI tooling only)"
    lines = [f"# Generated from downloaded wheels; {scope}.",
             "# Refresh: python scripts/release-locks.py; review changes before committing.",
             "--only-binary=:all:", ""]
    for name, (version, hashes) in sorted(packages.items()):
        lines.append(f"{name}=={version} \\\n" + " \\\n".join(f"    --hash=sha256:{digest}" for digest in sorted(hashes)))
    (ROOT / "host" / filename).write_text("\n".join(lines) + "\n")


def check() -> None:
    metadata = tomllib.loads((ROOT / "host/pyproject.toml").read_text())
    for filename in ("requirements-openai.txt", "requirements-cursor.txt", "requirements-orchestrators.txt", "requirements-bridges.txt", "requirements-object-storage.txt", "requirements-build.txt", "requirements-security.txt"):
        contents = (ROOT / "host" / filename).read_text()
        if "--only-binary=:all:" not in contents:
            raise ValueError(f"{filename}: source distributions are forbidden")
        for line in contents.replace("\\\n", " ").splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "--only-binary")):
                continue
            if not re.fullmatch(r"[a-z0-9-]+==[a-zA-Z0-9.+]+(?:\s+--hash=sha256:[0-9a-f]{64})+", line):
                raise ValueError(f"Invalid locked requirement in {filename}: {line}")
    build = (ROOT / "host/requirements-build.txt").read_text()
    for provider in ("openai", "cursor", "object-storage"):
        runtime = (ROOT / "host" / f"requirements-{provider}.txt").read_text()
        for dependency in metadata["project"]["optional-dependencies"][provider]:
            if dependency + " " not in runtime:
                raise ValueError(f"SDK pin mismatch: {dependency}")
    for dependency in metadata["build-system"]["requires"]:
        if dependency + " " not in build:
            raise ValueError(f"Build pin mismatch: {dependency}")
    orchestrators = (ROOT / "host/requirements-orchestrators.txt").read_text()
    for profile in ("temporal", "langgraph"):
        for dependency in metadata["project"]["optional-dependencies"][profile]:
            if dependency + " " not in orchestrators:
                raise ValueError(f"Orchestrator SDK pin mismatch: {dependency}")
    bridges = (ROOT / "host/requirements-bridges.txt").read_text()
    for dependency in metadata["project"]["optional-dependencies"]["bridges"]:
        if dependency + " " not in bridges or dependency + " " not in orchestrators:
            raise ValueError(f"Bridge schema pin mismatch: {dependency}")
    for dependency in metadata["project"]["optional-dependencies"]["workflows"]:
        if dependency + " " not in bridges:
            raise ValueError(f"Workflow schema pin mismatch: {dependency}")
    installer_spec = importlib.util.spec_from_file_location("cursor_node_installer", ROOT / "scripts/install-cursor-runtime.py")
    installer = importlib.util.module_from_spec(installer_spec)
    installer_spec.loader.exec_module(installer)
    for architecture in ("x86_64", "aarch64"):
        installer.read_lock(ROOT / "host/mas_host/cursor_runtime.json", architecture)
    if not {"cursor_runtime.json", "cursor_bridge_patch.json"}.issubset(metadata["tool"]["setuptools"]["package-data"]["mas_host"]):
        raise ValueError("Cursor runtime identity must be included in the installed host wheel")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check lock structure and direct pins without network")
    parser.add_argument("--keep-versions", action="store_true",
                        help="refresh wheel hashes for the new Python ABI while preserving every locked version")
    parser.add_argument("--only", choices=("openai", "cursor", "bridges", "orchestrators", "object-storage", "build", "security"),
                        action="append", help="refresh only these locks; may be repeated")
    args = parser.parse_args()
    if not args.check:
        if sys.version_info[:2] != (3, 14):
            parser.error("refresh requires CPython 3.14")
        metadata = tomllib.loads((ROOT / "host/pyproject.toml").read_text())
        selected = set(args.only or ("openai", "cursor", "bridges", "orchestrators", "object-storage", "build", "security"))
        if "openai" in selected:
            refresh("requirements-openai.txt", metadata["project"]["optional-dependencies"]["openai"], PLATFORMS, args.keep_versions)
        # Resolve alongside OpenAI's pins so both optional providers can coexist.
        openai_pins = re.findall(r"^([a-z0-9-]+==[^\s]+)", (ROOT / "host/requirements-openai.txt").read_text(), re.MULTILINE)
        if "cursor" in selected:
            refresh("requirements-cursor.txt", [*metadata["project"]["optional-dependencies"]["cursor"], *openai_pins], PLATFORMS[:2], args.keep_versions)
        if "bridges" in selected:
            refresh("requirements-bridges.txt", metadata["project"]["optional-dependencies"]["bridges"], BRIDGE_PLATFORMS, args.keep_versions)
        if "orchestrators" in selected:
            refresh("requirements-orchestrators.txt", [
                *metadata["project"]["optional-dependencies"]["temporal"],
                *metadata["project"]["optional-dependencies"]["langgraph"],
                *metadata["project"]["optional-dependencies"]["bridges"], *openai_pins], PLATFORMS[:2], args.keep_versions)
        if "object-storage" in selected:
            refresh("requirements-object-storage.txt", metadata["project"]["optional-dependencies"]["object-storage"], PLATFORMS, args.keep_versions)
        if "build" in selected:
            refresh("requirements-build.txt", [*metadata["build-system"]["requires"], "pip==26.2.1", "wheel==0.48.0"], PLATFORMS, args.keep_versions)
        if "security" in selected:
            refresh("requirements-security.txt", ["pip-audit==2.10.1"], PLATFORMS[:1], args.keep_versions)
    check()
    print("Dependency lock structure and direct pins verified.")


if __name__ == "__main__":
    main()
