#!/usr/bin/env python3
"""Fetch the committed, hash-locked Wolfi runtime package closure.

The final image authenticates packages against the retained signed repository
index using its trusted Wolfi APK keys. Update the closure with Wolfi's APK solver;
never resolve floating package versions during a normal image build.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import re
import shutil
import urllib.request

ARCHES = {"amd64": "x86_64", "arm64": "aarch64"}
ROOT = Path(__file__).resolve().parents[1]
ORIGIN = "https://packages.wolfi.dev/os/"
MAX_PACKAGE_BYTES = 128 * 1024 * 1024


def packages_for(lock: dict, architecture: str) -> list[dict]:
    if lock.get("format") != "mas-wolfi-lock/1":
        raise ValueError("Unsupported Wolfi package lock")
    packages = lock["runtime"][architecture]
    if not packages or len(packages) > 256:
        raise ValueError("Invalid Wolfi package closure size")
    names = set()
    for package in packages:
        name, filename, version = package["name"], package["filename"], package["version"]
        if (name in names or not re.fullmatch(r"[a-zA-Z0-9_.+-]+", name)
                or not re.fullmatch(r"[A-Za-z0-9_.:+~-]+", version)
                or Path(filename).name != filename or filename != f"{name}-{version}.apk"
                or package["url"] != ORIGIN + architecture + "/" + filename
                or not re.fullmatch(r"[0-9a-f]{64}", package["sha256"])):
            raise ValueError("Invalid or duplicate Wolfi package identity")
        names.add(name)
    if not {"python-3.14", "libstdc++"}.issubset(names):
        raise ValueError("Wolfi package closure is missing the runtime roots")
    return packages


def download(url: str) -> bytes:
    if not url.startswith(ORIGIN):
        raise ValueError("Only official Wolfi package URLs are accepted")
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read(MAX_PACKAGE_BYTES + 1)
    if len(data) > MAX_PACKAGE_BYTES:
        raise ValueError("Wolfi package exceeds its bounded download size")
    return data


def fetch_package(package: dict, directory: Path) -> None:
    data = download(package["url"])
    if hashlib.sha256(data).hexdigest() != package["sha256"]:
        raise ValueError(f"Wolfi package hash mismatch: {package['filename']}")
    (directory / package["filename"]).write_bytes(data)


def index_path(lock: dict, architecture: str, index_root: Path) -> Path:
    index = lock["indexes"][architecture]
    relative = f"docker/wolfi-indexes/{architecture}/APKINDEX.tar.gz"
    if index.get("path") != relative or not re.fullmatch(r"[0-9a-f]{64}", index.get("sha256", "")):
        raise ValueError("Invalid Wolfi repository snapshot identity")
    source = index_root / relative
    if hashlib.sha256(source.read_bytes()).hexdigest() != index["sha256"]:
        raise ValueError("Wolfi repository snapshot hash mismatch")
    return source


def stage_index(lock: dict, architecture: str, index_root: Path, directory: Path) -> None:
    source = index_path(lock, architecture, index_root)
    directory.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, directory / "APKINDEX.tar.gz")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--arch", choices=ARCHES)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--index-root", type=Path, default=ROOT, help="root containing retained signed index paths")
    parser.add_argument("--check", action="store_true", help="validate both architecture locks without network")
    args = parser.parse_args()
    lock = json.loads(args.lock.read_text())
    roots = lock["roots"]
    if not roots or any(not re.fullmatch(r"[A-Za-z0-9_.+-]+=[A-Za-z0-9_.:+~-]+", root) for root in roots):
        raise ValueError("Wolfi roots must pin explicit package versions")
    if args.check:
        for architecture in ARCHES.values():
            packages = {package["name"]: package["version"] for package in packages_for(lock, architecture)}
            if any(packages.get(name) != version for name, version in (root.split("=", 1) for root in roots)):
                raise ValueError("Wolfi roots disagree with the retained package closure")
            index_path(lock, architecture, args.index_root)
        print("Wolfi package lock structure verified.")
        return
    if not args.arch or not args.out:
        parser.error("download requires --arch and --out")
    architecture = ARCHES[args.arch]
    packages = packages_for(lock, architecture)
    args.out.mkdir(parents=True, exist_ok=True)
    directory = args.out / architecture
    stage_index(lock, architecture, args.index_root, directory)
    # Pin the complete closure so an older preinstalled base package cannot
    # satisfy a dependency in place of the exact reviewed package version.
    pins = [package["name"] + "=" + package["version"] for package in packages]
    (args.out / "roots.txt").write_text("\n".join(pins) + "\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(fetch_package, package, directory) for package in packages]
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
