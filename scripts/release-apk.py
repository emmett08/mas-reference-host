#!/usr/bin/env python3
"""Fetch hash-locked Alpine build/runtime packages, or explicitly refresh them."""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import io
import json
from pathlib import Path
import tarfile
import urllib.request

PLANS = {
    "build": ("3.24", ["musl", "musl-dev", "pkgconf", "sqlite", "sqlite-dev", "sqlite-libs", "readline", "libncursesw", "ncurses-terminfo-base"]),
    "runtime": ("3.24", ["libgcc", "sqlite-libs"]),
}
ARCHES = {"amd64": "x86_64", "arm64": "aarch64"}


def download(url: str) -> bytes:
    if not url.startswith("https://dl-cdn.alpinelinux.org/alpine/v"):
        raise ValueError("Only official Alpine package URLs are accepted")
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def refresh(stage: str, arch: str) -> tuple[str, str, list[dict]]:
    release, names = PLANS[stage]
    prefix = f"https://dl-cdn.alpinelinux.org/alpine/v{release}/main/{arch}/"
    with tarfile.open(fileobj=io.BytesIO(download(prefix + "APKINDEX.tar.gz")), mode="r:gz") as archive:
        index = archive.extractfile("APKINDEX").read().decode()
    rows = {}
    for block in index.split("\n\n"):
        row = dict(line.split(":", 1) for line in block.splitlines() if ":" in line)
        if row.get("P") in names:
            rows[row["P"]] = row
    packages = []
    for name in names:
        row = rows[name]
        filename = f"{name}-{row['V']}.apk"
        data = download(prefix + filename)
        packages.append({"name": name, "version": row["V"], "filename": filename,
                         "url": prefix + filename, "sha256": hashlib.sha256(data).hexdigest(),
                         "dependencies": row.get("D", "")})
    return stage, arch, packages


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--arch", choices=ARCHES)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    if args.refresh:
        lock = {"format": "mas-apk-lock/1", "build": {}, "runtime": {}}
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(refresh, stage, arch) for stage in PLANS for arch in ARCHES.values()]
            for future in futures:
                stage, arch, packages = future.result()
                lock[stage][arch] = packages
        args.lock.parent.mkdir(parents=True, exist_ok=True)
        args.lock.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
        return
    if not args.arch or not args.out:
        parser.error("download requires --arch and --out")
    lock = json.loads(args.lock.read_text())
    if lock["format"] != "mas-apk-lock/1":
        raise ValueError("Unsupported package lock")
    for stage in PLANS:
        directory = args.out / stage
        directory.mkdir(parents=True, exist_ok=True)
        for package in lock[stage][ARCHES[args.arch]]:
            filename = package["filename"]
            if Path(filename).name != filename or not filename.endswith(".apk"):
                raise ValueError("Invalid package filename")
            data = download(package["url"])
            if hashlib.sha256(data).hexdigest() != package["sha256"]:
                raise ValueError(f"Package hash mismatch: {filename}")
            (directory / filename).write_bytes(data)
    # apk verifies the signed packages again during offline installation.


if __name__ == "__main__":
    main()
