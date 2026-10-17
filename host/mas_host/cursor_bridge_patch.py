"""Audited removal of a legacy Node <18 Headers polyfill from Cursor's bridge.

The bridge requires the separately pinned Node 24 runtime. Its connect-node
dependency imports undici only for that obsolete polyfill. We retain the
upstream licence notices, record the modified dependency metadata, and remove
the actual unused packages. This is a source repair, not a scanner exclusion.
"""
from __future__ import annotations

import hashlib
import importlib.resources
import base64
import csv
import io
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

_BASE = "node_modules/@connectrpc/connect-node/"
_FILES = {_BASE + "dist/cjs/node-headers-polyfill.js",
          _BASE + "dist/esm/node-headers-polyfill.js", _BASE + "package.json"}
_REMOVED = {"node_modules/undici", "node_modules/@fastify/busboy"}


class BridgePatchError(ValueError):
    """The installed bridge does not match the reviewed release inputs."""


def _manifest() -> tuple[dict, str]:
    try:
        raw = importlib.resources.files("mas_host").joinpath("cursor_bridge_patch.json").read_bytes()
        value = json.loads(raw)
        if (value.get("format") != "mas-cursor-bridge-patch/1"
                or value.get("patch") != "native-headers/1"
                or value.get("sdk_version") != "1.0.37"
                or set(value["files"]) != _FILES or set(value["remove"]) != _REMOVED):
            raise ValueError()
        for item in value["files"].values():
            if (not re.fullmatch(r"[0-9a-f]{64}", item["before_sha256"])
                    or hashlib.sha256(item["after"].encode()).hexdigest() != item["after_sha256"]):
                raise ValueError()
        for item in value["remove"].values():
            if not re.fullmatch(r"[0-9a-f]{64}", item["tree_sha256"]):
                raise ValueError()
        return value, hashlib.sha256(raw).hexdigest()
    except (OSError, ValueError, TypeError, AttributeError, KeyError):
        raise BridgePatchError("Cursor bridge patch manifest is invalid") from None


def patch_identity() -> dict:
    """A portable recovery binding; does not import or execute the optional SDK."""
    manifest, digest = _manifest()
    return {"patch": manifest["patch"], "manifest_sha256": digest}


def _path(root: Path, relative: str) -> Path:
    current = root
    for part in Path(relative).parts:
        current = current / part
        if current.is_symlink():
            raise BridgePatchError("Cursor bridge patch refuses symbolic links")
    return current


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _tree_digest(directory: Path) -> str:
    """Bind every path and file byte before removing a reviewed dependency tree."""
    digest = hashlib.sha256()
    if not directory.is_dir():
        raise BridgePatchError("Cursor bridge dependency tree is invalid")
    for path in sorted(directory.rglob("*")):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise BridgePatchError("Cursor bridge dependency tree is invalid")
        if path.is_file():
            digest.update(path.relative_to(directory).as_posix().encode() + b"\0")
            digest.update(bytes.fromhex(_file_digest(path)))
    return digest.hexdigest()


def validate_bridge_patch(root: Path) -> None:
    """Fail closed unless all reviewed source changes and removals are present."""
    manifest, _ = _manifest()
    try:
        root = Path(root).resolve(strict=True)
        for relative, item in manifest["files"].items():
            if _file_digest(_path(root, relative)) != item["after_sha256"]:
                raise BridgePatchError("Cursor bridge requires the reviewed native Headers repair")
        for relative in manifest["remove"]:
            if _path(root, relative).exists():
                raise BridgePatchError("Cursor bridge retains an excluded legacy dependency")
    except OSError:
        raise BridgePatchError("Cursor bridge patch files are unavailable") from None


def apply_bridge_patch(root: Path) -> None:
    """Apply known preimages only, accepting already replaced source files.

    All inputs are checked before the first mutation. Known postimages are
    accepted for idempotency. An interrupted repair remains unusable until
    validation succeeds. An interruption during dependency deletion requires a
    fresh installation of the pinned SDK wheel before retrying this repair.
    """
    manifest, _ = _manifest()
    try:
        root = Path(root).resolve(strict=True)
        for relative, item in manifest["files"].items():
            if _file_digest(_path(root, relative)) not in {item["before_sha256"], item["after_sha256"]}:
                raise BridgePatchError("Cursor bridge source does not match the reviewed release")
        for relative, item in manifest["remove"].items():
            path = _path(root, relative)
            if path.exists() and _tree_digest(path) != item["tree_sha256"]:
                raise BridgePatchError("Cursor bridge dependency differs; reinstall cursor-sdk==1.0.37 before repair")
        for relative, item in manifest["files"].items():
            path = _path(root, relative)
            if _file_digest(path) == item["after_sha256"]:
                continue
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".mas-patch-", delete=False) as output:
                    temporary = Path(output.name)
                    output.write(item["after"].encode())
                    output.flush()
                    os.fchmod(output.fileno(), path.stat().st_mode & 0o777)
                    os.fsync(output.fileno())
                os.replace(temporary, path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        for relative in manifest["remove"]:
            path = _path(root, relative)
            if path.exists():
                shutil.rmtree(path)
        validate_bridge_patch(root)
    except OSError:
        raise BridgePatchError("Cursor bridge repair could not be completed") from None


def refresh_sdk_record(root: Path) -> None:
    """Record the real local repair without following paths supplied by RECORD.

    Only the fixed reviewed paths are read. Unrelated entries, including any
    wheel-installed scripts outside site-packages, retain their original rows.
    """
    try:
        root = Path(root).resolve(strict=True)
        validate_bridge_patch(root)
        if root.parts[-3:] != ("cursor_sdk", "_vendor", "bridge"):
            raise BridgePatchError("Cursor SDK installation layout is invalid")
        prefix = "cursor_sdk/_vendor/bridge/"
        record = root.parents[2] / "cursor_sdk-1.0.37.dist-info" / "RECORD"
        if record.is_symlink() or record.parent.is_symlink():
            raise BridgePatchError("Cursor SDK installation metadata is invalid")
        rows = list(csv.reader(io.StringIO(record.read_text())))
        if any(len(row) != 3 for row in rows):
            raise BridgePatchError("Cursor SDK installation metadata is invalid")
        changed = _FILES | {"bin/node", "NODE_LICENSE"}
        changed_names = {prefix + name for name in changed}
        removed_prefixes = tuple(prefix + name + "/" for name in _REMOVED)
        kept = [row for row in rows if row[0] not in changed_names
                and not row[0].startswith(removed_prefixes)]
        for relative in sorted(changed):
            path = _path(root, relative)
            digest = base64.urlsafe_b64encode(bytes.fromhex(_file_digest(path))).decode().rstrip("=")
            kept.append([prefix + relative, "sha256=" + digest, str(path.stat().st_size)])
        data = io.StringIO(newline="")
        csv.writer(data, lineterminator="\n").writerows(kept)
        with tempfile.NamedTemporaryFile(dir=record.parent, prefix=".mas-record-", delete=False) as output:
            temporary = Path(output.name)
            try:
                output.write(data.getvalue().encode())
                output.flush()
                os.fchmod(output.fileno(), record.stat().st_mode & 0o777)
                os.fsync(output.fileno())
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        try:
            os.replace(temporary, record)
        finally:
            temporary.unlink(missing_ok=True)
    except OSError:
        raise BridgePatchError("Cursor SDK installation metadata could not be updated") from None
