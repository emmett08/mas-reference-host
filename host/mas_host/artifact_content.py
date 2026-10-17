"""Content persistence strategies beneath the principal-bound artefact store.

The metadata owner holds its namespace lock across these operations. Strategies
never grant access or publish references; successful publication means complete
content has been durably stored and verified. Failed attempts remain charged.
"""
from __future__ import annotations

import hashlib
import os
from typing import Protocol, runtime_checkable
import uuid

from .artifacts import ArtifactError, _CHUNK, _DIGEST, _hash_source, _open_file


@runtime_checkable
class ArtifactContentBackend(Protocol):
    @property
    def descriptor(self) -> dict: ...
    def check(self, directory, index, policy, *, initialise=False): ...
    def publish(self, directory, reference, source, policy): ...
    def verify(self, directory, reference, *, collect=False, destination=None): ...


class LocalArtifactContent:
    """Existing atomic files and physical-byte quotas, without format migration."""

    @property
    def descriptor(self):
        return {"format": "mas-local-content/1"}

    def check(self, directory, index, policy, *, initialise=False):
        if "object-content.json" in os.listdir(directory):
            raise ArtifactError("artefact content backend changed")

    def publish(self, directory, reference, source, policy):
        pending = ".pending-" + uuid.uuid4().hex
        try:
            blob = reference.sha256 + ".blob"
            names = os.listdir(directory)
            used = 0
            for name in names:
                if name.endswith(".blob"):
                    if not _DIGEST.fullmatch(name[:-5]):
                        raise ArtifactError("invalid artefact storage entry")
                    with os.fdopen(_open_file(directory, name), "rb") as stored:
                        used += os.fstat(stored.fileno()).st_size
                elif name.startswith(".pending-") and name != pending:
                    with os.fdopen(_open_file(directory, name), "rb") as orphan:
                        used += os.fstat(orphan.fileno()).st_size
                elif name not in ("index.json", pending):
                    raise ArtifactError("invalid artefact storage entry")
            if used + (0 if blob in names else reference.size) > policy.max_total_bytes:
                raise ArtifactError("artefact total-byte quota exhausted")
            if blob in names:
                self.verify(directory, reference)
                return
            with os.fdopen(_open_file(directory, pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as output:
                copied = _hash_source(source, policy.max_artifact_bytes, destination=output)
                if copied != (reference.size, reference.sha256):
                    raise ArtifactError("artefact source changed during publication")
                output.flush()
                os.fsync(output.fileno())
            os.replace(pending, blob, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            try:
                os.unlink(pending, dir_fd=directory)
            except FileNotFoundError:
                pass

    def verify(self, directory, reference, *, collect=False, destination=None):
        with os.fdopen(_open_file(directory, reference.sha256 + ".blob"), "rb") as source:
            if os.fstat(source.fileno()).st_size != reference.size:
                raise ArtifactError("artefact content integrity mismatch")
            return verify_stream(source, reference, collect=collect, destination=destination)


def verify_stream(source, reference, *, collect=False, destination=None):
    """Consume at most the declared size plus one; expose nothing until hashed."""
    digest, result, size = hashlib.sha256(), [], 0
    while chunk := source.read(min(_CHUNK, reference.size - size + 1)):
        if type(chunk) is not bytes:
            raise ArtifactError("artefact content integrity mismatch")
        size += len(chunk)
        if size > reference.size:
            raise ArtifactError("artefact content integrity mismatch")
        digest.update(chunk)
        if destination is not None:
            destination.write(chunk)
        if collect:
            result.append(chunk)
    if size != reference.size or digest.hexdigest() != reference.sha256:
        raise ArtifactError("artefact content integrity mismatch")
    return b"".join(result)
