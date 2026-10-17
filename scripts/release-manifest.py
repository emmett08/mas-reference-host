#!/usr/bin/env python3
"""Create checksums and a source identity record for a CI release candidate."""
import hashlib
import json
import os
from pathlib import Path
import re

sha = os.environ["SOURCE_SHA"]
protocol_sha = Path(".protocol-revision").read_text().strip()
if not re.fullmatch(r"[0-9a-f]{40}", protocol_sha):
    raise SystemExit("Expected the prepared protocol commit SHA")
tag = os.environ["RELEASE_TAG"]
variant = os.environ["RELEASE_VARIANT"]
platform = os.environ["RELEASE_PLATFORM"]
if not re.fullmatch(r"[0-9a-f]{40}", sha) or not re.fullmatch(r"v0\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", tag):
    raise SystemExit("Expected a full commit SHA and a v0.MINOR.PATCH release tag")
if variant not in ("standard", "cursor") or platform not in ("linux/amd64", "linux/arm64"):
    raise SystemExit("Unsupported release image variant or platform")
archive = Path(f"mas-{variant}-{platform.split('/')[1]}.oci.tar")
if set(Path(".").glob("mas-*.oci.tar")) != {archive}:
    raise SystemExit("Expected exactly the archive for the selected image variant and architecture")
files = sorted([archive, Path("release-audit.json"),
                Path("release-sbom.cdx.json"), Path("release-image-identity.json"),
                Path("release-image-binding.json"), Path("release-import-tool.txt"),
                Path("release-container-tests.log")])
def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
binding = json.loads(Path("release-image-binding.json").read_text())
if (binding.get("format") != "mas-release-image-binding/1"
        or binding.get("docker_identity_verified") is not True
        or binding.get("archive") != str(archive)
        or binding.get("archive_sha256") != "sha256:" + digest(archive)
        or f"{binding.get('os')}/{binding.get('architecture')}" != platform):
    raise SystemExit("Release archive and tested image do not have a verified identity binding")
layout = Path(binding.get("scanner_directory", ""))
if (binding.get("scanner_layout_verified") is not True
        or binding.get("scanner_input") != str(layout) + "@" + binding["manifest_digest"]
        or not binding.get("scanner_layout_files")
        or any("sha256:" + digest(layout / name) != identity
               for name, identity in binding["scanner_layout_files"].items())):
    raise SystemExit("The scanned OCI layout does not match the verified release archive")
dockerfile = Path("Dockerfile.cursor" if variant == "cursor" else "Dockerfile")
inputs = [dockerfile, Path("docker/alpine-packages.json"), Path(".protocol/rust-protocol/Cargo.lock"), Path("protocol-revision.txt"), Path(".protocol-revision"),
          Path("host/pyproject.toml"), *sorted(Path("host").glob("requirements-*.txt"))]
if variant == "cursor":
    inputs.extend([Path("docker/wolfi-packages.json"), Path("host/mas_host/cursor_runtime.json"),
                   Path("host/mas_host/cursor_bridge_patch.json"),
                   *sorted(Path("docker/wolfi-indexes").glob("*/APKINDEX.tar.gz"))])
manifest = {
    "format": "mas-release/2", "source_commit": sha, "tag": tag,
    "source_repository": "https://github.com/emmett08/mas-reference-host",
    "protocol_source": {"repository": "https://github.com/emmett08/mas", "commit": protocol_sha},
    "platform": platform, "variant": variant, "dockerfile": str(dockerfile),
    "files": {str(path): digest(path) for path in files},
    "inputs": {str(path): digest(path) for path in inputs},
    "provenance": "BuildKit provenance is embedded in the OCI archive; this JSON is an unsigned source and checksum record.",
    "tested_image_binding": binding,
}
Path("release-manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
files.append(Path("release-manifest.json"))
Path("SHA256SUMS").write_text("".join(f"{digest(path)}  {path}\n" for path in files))
