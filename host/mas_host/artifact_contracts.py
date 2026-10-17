"""Portable artefact references, policies and repository contracts."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
from typing import BinaryIO, ContextManager, Protocol, runtime_checkable

from .codec import is_actor_identity

class ArtifactError(RuntimeError):
    """Fixed diagnostic suitable for an application boundary."""


_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_MEDIA = re.compile(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+\Z")
_FORMAT = "mas-artifact/1"
_INDEX_FORMAT = "mas-artifact-store/1"
_MAX_INDEX = 16 * 1024 * 1024
_MAX_REF = 8192
_CHUNK = 1024 * 1024


def _identity(value: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise ArtifactError("invalid artefact identity")
    return value


def _principal(value: str) -> str:
    """Rust actor identity; opaque metadata, never a filesystem component."""
    if not is_actor_identity(value):
        raise ArtifactError("invalid artefact principal")
    return value


def _digest(value: str) -> str:
    if type(value) is not str or not _DIGEST.fullmatch(value):
        raise ArtifactError("invalid artefact digest")
    return value


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _load_json(data: bytes) -> object:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ArtifactError("duplicate artefact metadata key")
            result[key] = value
        return result
    try:
        return json.loads(data, object_pairs_hook=pairs)
    except (ValueError, UnicodeError, RecursionError):
        raise ArtifactError("invalid artefact metadata") from None


@dataclass(frozen=True)
class ArtifactPolicy:
    max_artifact_bytes: int = 16 * 1024 * 1024
    max_total_bytes: int = 64 * 1024 * 1024
    max_artifacts: int = 1024
    max_links: int = 4096

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 or v > 2**63 - 1 for v in asdict(self).values()):
            raise ArtifactError("artefact limits must be positive bounded integers")
        if self.max_artifact_bytes > self.max_total_bytes or self.max_artifacts > 4096 or self.max_links > 16384:
            raise ArtifactError("invalid artefact policy bounds")


@dataclass(frozen=True)
class ArtifactRef:
    namespace: str
    artifact_id: str
    sha256: str
    size: int
    media_type: str
    producer: str
    input_sha256s: tuple[str, ...] = ()
    format: str = _FORMAT

    def __post_init__(self):
        if self.format != _FORMAT:
            raise ArtifactError("unsupported artefact reference version")
        _identity(self.namespace)
        _principal(self.producer)
        _digest(self.artifact_id)
        _digest(self.sha256)
        if type(self.size) is not int or self.size < 0 or self.size > 2**63 - 1:
            raise ArtifactError("invalid artefact size")
        if type(self.media_type) is not str or len(self.media_type) > 128 or not _MEDIA.fullmatch(self.media_type):
            raise ArtifactError("invalid artefact media type")
        if type(self.input_sha256s) is not tuple or len(self.input_sha256s) > 32:
            raise ArtifactError("invalid artefact input digests")
        for item in self.input_sha256s:
            _digest(item)
        if tuple(sorted(set(self.input_sha256s))) != self.input_sha256s:
            raise ArtifactError("artefact input digests must be unique and sorted")
        if len(_json(self.to_dict())) > _MAX_REF:
            raise ArtifactError("artefact reference exceeds message bound")

    def to_dict(self) -> dict:
        return {**asdict(self), "input_sha256s": list(self.input_sha256s)}

    @classmethod
    def from_dict(cls, value: dict) -> "ArtifactRef":
        if type(value) is not dict or set(value) != set(cls.__dataclass_fields__):
            raise ArtifactError("invalid artefact reference fields")
        if type(value["input_sha256s"]) is not list:
            raise ArtifactError("invalid artefact input digests")
        return cls(**{**value, "input_sha256s": tuple(value["input_sha256s"])})


@runtime_checkable
class ArtifactRepository(Protocol):
    """Principal-bound repository; backends preserve authorisation and hashes."""

    principal: str
    namespace: str
    policy: ArtifactPolicy
    readonly: bool

    def put_bytes(self, data: bytes, *, media_type: str = "application/octet-stream",
                  input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef: ...
    def put_file(self, path: Path, *, media_type: str = "application/octet-stream",
                 input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef: ...
    def put_stream(self, source: BinaryIO, *, media_type: str = "application/octet-stream",
                   input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef: ...
    def read_bytes(self, reference: ArtifactRef | dict) -> bytes: ...
    def open(self, reference: ArtifactRef | dict) -> ContextManager[BinaryIO]: ...
    def verify(self, reference: ArtifactRef | dict) -> bool: ...
    def inspect(self, reference: ArtifactRef | dict) -> dict: ...
    def link(self, link_id: str, references: list[ArtifactRef | dict], *, state: str = "live") -> dict: ...
    def status(self) -> dict: ...


