"""Bounded immutable artefacts with explicit provenance and read grants.

The host supplies authenticated principals; this module does not authenticate
strings supplied by remote callers. Store below a run directory as ``artifacts``
to include metadata and local content in deployment backups and run retention.
Remote content requires the separately retained object backend. References are data,
not bearer capabilities or filesystem paths. No remote URLs are dereferenced.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid
from typing import BinaryIO, ContextManager, Iterator, Protocol, runtime_checkable

from .codec import is_actor_identity
from .operations import OperationsError, _file_lock


from .artifact_contracts import (ArtifactError, ArtifactPolicy, ArtifactRef, ArtifactRepository, _identity, _principal, _digest, _json, _load_json, _INDEX_FORMAT, _MAX_INDEX, _MAX_REF, _CHUNK, _FORMAT, _DIGEST, _ID, _MEDIA)


def _manifest_identity(reference: ArtifactRef, readers: list[str]) -> str:
    """Bind provenance and grants using the existing ASCII JSON store format."""
    fields = reference.to_dict()
    del fields["artifact_id"]  # The manifest identity cannot include itself.
    return hashlib.sha256(_json({"reference": fields, "readers": readers})).hexdigest()


def _open_dir(path: Path, *, create: bool = False) -> int:
    """Walk with directory descriptors: no symlink component is followed."""
    path = Path(path).absolute()
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            if part in ("", ".", ".."):
                raise ArtifactError("unsafe artefact path")
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_file(directory: int, name: str, flags: int = os.O_RDONLY) -> int:
    fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        os.close(fd)
        raise ArtifactError("artefact storage requires private regular files")
    return fd


def _read_file(directory: int, name: str, limit: int) -> bytes:
    with os.fdopen(_open_file(directory, name), "rb") as stream:
        if os.fstat(stream.fileno()).st_size > limit:
            raise ArtifactError("artefact metadata exceeds size bound")
        data = stream.read(limit + 1)
        if len(data) > limit:
            raise ArtifactError("artefact metadata exceeds size bound")
        return data


def _hash_source(source: BinaryIO, limit: int, *, destination: BinaryIO | None = None) -> tuple[int, str]:
    """Read bounded content, optionally copying the exact bytes being hashed."""
    digest, size = hashlib.sha256(), 0
    while chunk := source.read(min(_CHUNK, limit - size + 1)):
        size += len(chunk)
        if size > limit:
            raise ArtifactError("artefact exceeds per-object quota")
        if destination is not None:
            destination.write(chunk)
        digest.update(chunk)
    return size, digest.hexdigest()


def _atomic_json(directory: int, name: str, data: dict) -> None:
    encoded = _json(data)
    if len(encoded) > _MAX_INDEX:
        raise ArtifactError("artefact metadata exceeds size bound")
    pending = ".pending-" + uuid.uuid4().hex
    try:
        with os.fdopen(_open_file(directory, pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    finally:
        try:
            os.unlink(pending, dir_fd=directory)
        except FileNotFoundError:
            pass


def _validate_index(index: object, namespace: str) -> ArtifactPolicy:
    if type(index) is not dict or set(index) != {"format", "namespace", "policy", "artifacts", "links"}:
        raise ArtifactError("invalid artefact store metadata")
    if index["format"] != _INDEX_FORMAT or index["namespace"] != namespace:
        raise ArtifactError("unsupported artefact store version or namespace")
    if type(index["policy"]) is not dict or set(index["policy"]) != set(ArtifactPolicy.__dataclass_fields__):
        raise ArtifactError("invalid artefact policy metadata")
    policy = ArtifactPolicy(**index["policy"])
    artifacts, links = index["artifacts"], index["links"]
    if type(artifacts) is not dict or len(artifacts) > policy.max_artifacts or type(links) is not dict or len(links) > policy.max_links:
        raise ArtifactError("artefact store metadata exceeds policy")
    for key, manifest in artifacts.items():
        if type(manifest) is not dict or set(manifest) != {"reference", "readers", "artifact_id"}:
            raise ArtifactError("invalid artefact manifest")
        ref = ArtifactRef.from_dict(manifest["reference"])
        readers = manifest["readers"]
        if (type(readers) is not list or len(readers) > 32 or any(type(item) is not str for item in readers)
                or readers != sorted(set(readers))):
            raise ArtifactError("invalid artefact grants")
        for reader in readers:
            _principal(reader)
        identity = _manifest_identity(ref, readers)
        if key != ref.artifact_id or key != manifest["artifact_id"] or key != identity or ref.namespace != namespace or ref.size > policy.max_artifact_bytes:
            raise ArtifactError("artefact manifest integrity mismatch")
    for key, link in links.items():
        _identity(key)
        if type(link) is not dict or set(link) != {"owner", "state", "artifacts"} or link["state"] not in ("live", "unresolved", "released"):
            raise ArtifactError("invalid artefact retention manifest")
        _principal(link["owner"])
        refs = link["artifacts"]
        if (type(refs) is not list or not refs or len(refs) > 64 or any(type(item) is not str for item in refs)
                or refs != sorted(set(refs)) or any(item not in artifacts for item in refs)):
            raise ArtifactError("invalid artefact retention references")
    return policy


class ArtifactStore:
    """Per-namespace metadata owner with local or explicit remote content storage.

    Quotas count physical blobs, including crash-orphaned blobs. There is no
    automatic object deletion. Explicit live/unresolved links protect the whole
    containing run from operational retention; released links preserve history.
    Read grants and producer/input provenance are frozen at publication.
    """

    def __init__(self, root: Path, *, namespace: str, principal: str,
                 policy: ArtifactPolicy | None = None, readonly: bool = False,
                 content_backend=None):
        from .artifact_content import ArtifactContentBackend, LocalArtifactContent
        self.content_backend = content_backend if content_backend is not None else LocalArtifactContent()
        if not isinstance(self.content_backend, ArtifactContentBackend):
            raise ArtifactError("invalid artefact content backend")
        self.root = Path(root).absolute()
        self.namespace = _identity(namespace)
        self.principal = _principal(principal)
        self.readonly = readonly
        self.directory = self.root / self.namespace
        if policy is not None and not isinstance(policy, ArtifactPolicy):
            raise ArtifactError("invalid artefact policy")
        self.policy = policy
        with self._locked(write=not readonly, initialise=True) as (directory, index):
            stored = _validate_index(index, self.namespace)
            if policy is not None and policy != stored:
                raise ArtifactError("artefact policy is immutable")
            self.policy = stored

    def _deployment(self) -> Path | None:
        for parent in self.root.parents:
            if (parent / ".restored-audit-only").exists() or (parent / ".restored-audit-only").is_symlink() or (parent / "deployment.sqlite").exists():
                return parent
        return None

    def _writable(self) -> None:
        if self.readonly:
            raise ArtifactError("artefact store is read-only")
        for parent in (self.root, *self.root.parents):
            marker = parent / ".restored-audit-only"
            if marker.exists() or marker.is_symlink():
                raise ArtifactError("restored artefacts are audit-only")
        deployment = self._deployment()
        if deployment is not None:
            from .operations import DeploymentLedger
            if not DeploymentLedger(deployment, readonly=True).admission_enabled:
                raise ArtifactError("restored artefacts are audit-only")

    def _publication_capacity(self) -> None:
        deployment = self._deployment()
        if deployment is not None:
            from .operations import DeploymentLedger
            try:
                DeploymentLedger(deployment, readonly=True).check_storage_admission()
            except OperationsError:
                raise ArtifactError("artefact publication admission closed") from None

    @contextmanager
    def _locked(self, *, write: bool = False, initialise: bool = False):
        try:
            if write:
                self._writable()
            deployment = self._deployment()
            # Cooperate with backup/prune, which hold the deployment lock exclusively.
            lock = _file_lock(deployment / ".deployment.lock", False) if deployment is not None else _nothing()
            with lock:
                if write:
                    self._writable()
                    if initialise and not (self.directory / "index.json").exists():
                        self._publication_capacity()
                root_fd = _open_dir(self.root, create=write and initialise)
                try:
                    if stat.S_IMODE(os.fstat(root_fd).st_mode) & 0o077:
                        raise ArtifactError("artefact root must be private (mode 0700)")
                    if write and initialise:
                        try:
                            os.mkdir(self.namespace, 0o700, dir_fd=root_fd)
                            os.fsync(root_fd)
                        except FileExistsError:
                            pass
                    directory = os.open(self.namespace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
                finally:
                    os.close(root_fd)
                try:
                    if stat.S_IMODE(os.fstat(directory).st_mode) & 0o077:
                        raise ArtifactError("artefact namespace must be private (mode 0700)")
                    # Directory-inode locking needs no writable lock file, so
                    # audit-only inspection leaves restored evidence unchanged.
                    try:
                        fcntl.flock(directory, fcntl.LOCK_EX if write else fcntl.LOCK_SH)
                        try:
                            index = _load_json(_read_file(directory, "index.json", _MAX_INDEX))
                        except FileNotFoundError:
                            if not (write and initialise):
                                raise ArtifactError("artefact store metadata is missing") from None
                            # A missing index with existing data is corruption,
                            # not permission to reset quotas and authorisation.
                            if os.listdir(directory):
                                raise ArtifactError("artefact store metadata is missing") from None
                            index = {"format": _INDEX_FORMAT, "namespace": self.namespace,
                                     "policy": asdict(self.policy or ArtifactPolicy()), "artifacts": {}, "links": {}}
                            _atomic_json(directory, "index.json", index)
                        stored_policy = _validate_index(index, self.namespace)
                        if self.policy is not None and self.policy != stored_policy:
                            raise ArtifactError("artefact policy is immutable")
                        self.content_backend.check(directory, index, stored_policy, initialise=write and initialise)
                        yield directory, index
                    finally:
                        fcntl.flock(directory, fcntl.LOCK_UN)
                finally:
                    os.close(directory)
        except (OSError, OperationsError):
            raise ArtifactError("artefact store is unavailable or unsafe") from None

    def _authorised(self, reference: ArtifactRef | dict, index: dict) -> ArtifactRef:
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        manifest = index["artifacts"].get(ref.artifact_id)
        if (ref.namespace != self.namespace or manifest is None or manifest["reference"] != ref.to_dict()
                or self.principal not in (ref.producer, *manifest["readers"])):
            raise ArtifactError("artefact is unavailable or access is denied")
        return ref

    def put_bytes(self, data: bytes, *, media_type: str = "application/octet-stream",
                  input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef:
        if type(data) is not bytes:
            raise ArtifactError("artefact content must be bytes")
        if len(data) > self.policy.max_artifact_bytes:
            raise ArtifactError("artefact exceeds per-object quota")
        from io import BytesIO
        return self._put(BytesIO(data), media_type, input_sha256s, readers)

    def put_file(self, path: Path, *, media_type: str = "application/octet-stream",
                 input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef:
        path = Path(path).absolute()
        try:
            directory = _open_dir(path.parent)
            try:
                descriptor = _open_file(directory, path.name)
            finally:
                os.close(directory)
            with os.fdopen(descriptor, "rb") as source:
                if os.fstat(source.fileno()).st_size > self.policy.max_artifact_bytes:
                    raise ArtifactError("artefact exceeds per-object quota")
                return self._put(source, media_type, input_sha256s, readers)
        except OSError:
            raise ArtifactError("artefact source is unavailable or unsafe") from None

    def put_stream(self, source: BinaryIO, *, media_type: str = "application/octet-stream",
                   input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef:
        """Publish from a trusted seekable binary stream, with bounded two-pass hashing."""
        if not callable(getattr(source, 'seekable', None)) or not source.seekable():
            raise ArtifactError("artefact source must be seekable")
        return self._put(source, media_type, input_sha256s, readers)

    def _put(self, source: BinaryIO, media_type: str, inputs: tuple[str, ...], readers: tuple[str, ...]) -> ArtifactRef:
        if type(inputs) is not tuple or type(readers) is not tuple or len(inputs) > 32 or len(readers) > 32:
            raise ArtifactError("invalid artefact provenance or grants")
        for reader in readers:
            _principal(reader)
        inputs = tuple(sorted(set(_digest(item) for item in inputs)))
        grants = sorted(set(readers) - {self.principal})
        # Validate metadata before consuming any source bytes.
        ArtifactRef(self.namespace, "0" * 64, "0" * 64, 0, media_type, self.principal, inputs)
        with self._locked(write=True) as (directory, index):
            # Recognise a duplicate without allocating a temporary blob. New
            # publications obey deployment admission before writing any bytes.
            offset = source.tell()
            size, digest = _hash_source(source, self.policy.max_artifact_bytes)
            source.seek(offset)
            provisional = ArtifactRef(self.namespace, "0" * 64, digest, size, media_type, self.principal, inputs)
            identity = _manifest_identity(provisional, grants)
            ref = ArtifactRef(self.namespace, identity, digest, size, media_type, self.principal, inputs)
            if identity in index["artifacts"]:
                self._verify_content(directory, ref)
                return ref
            self._publication_capacity()
            if len(index["artifacts"]) >= self.policy.max_artifacts:
                raise ArtifactError("artefact count quota exhausted")
            self.content_backend.publish(directory, ref, source, self.policy)
            index["artifacts"][identity] = {"artifact_id": identity, "reference": ref.to_dict(), "readers": grants}
            _atomic_json(directory, "index.json", index)
            return ref

    def _verify_content(self, directory, reference, *, collect=False, destination=None):
        return self.content_backend.verify(directory, reference, collect=collect, destination=destination)

    def read_bytes(self, reference: ArtifactRef | dict) -> bytes:
        with self._locked() as (directory, index):
            ref = self._authorised(reference, index)
            return self._verify_content(directory, ref, collect=True)

    @contextmanager
    def open(self, reference: ArtifactRef | dict) -> Iterator[BinaryIO]:
        """Return a verified bounded snapshot, never an unverified filesystem handle."""
        from tempfile import TemporaryFile
        # A private disk-backed snapshot bounds RAM independently of object size.
        # Nothing is exposed to the caller until the exact copied bytes verify.
        with TemporaryFile(mode="w+b") as stream:
            with self._locked() as (directory, index):
                ref = self._authorised(reference, index)
                self._verify_content(directory, ref, destination=stream)
            stream.seek(0)
            yield stream

    def verify(self, reference: ArtifactRef | dict) -> bool:
        with self._locked() as (directory, index):
            self._verify_content(directory, self._authorised(reference, index))
            return True

    def inspect(self, reference: ArtifactRef | dict) -> dict:
        with self._locked() as (_, index):
            ref = self._authorised(reference, index)
            manifest = index["artifacts"][ref.artifact_id]
            return {"reference": ref.to_dict(), "readers": list(manifest["readers"]),
                    "links": sorted(key for key, link in index["links"].items() if ref.artifact_id in link["artifacts"])}

    def link(self, link_id: str, references: list[ArtifactRef | dict], *, state: str = "live") -> dict:
        """Publish or transition an owned retention manifest with a frozen ref set.

        Release only after its consumer has reached a resolved terminal state.
        No run/task state is inferred from model output or from an elapsed age.
        """
        _identity(link_id)
        if state not in ("live", "unresolved", "released") or type(references) is not list or not 1 <= len(references) <= 64:
            raise ArtifactError("invalid artefact retention manifest")
        with self._locked(write=True) as (directory, index):
            ids = sorted(set(self._authorised(ref, index).artifact_id for ref in references))
            current = index["links"].get(link_id)
            link = {"owner": self.principal, "state": state, "artifacts": ids}
            if current is not None and (current["owner"] != self.principal or current["artifacts"] != ids):
                raise ArtifactError("artefact retention identity is immutable")
            if current is None and len(index["links"]) >= self.policy.max_links:
                raise ArtifactError("artefact retention quota exhausted")
            index["links"][link_id] = link
            _atomic_json(directory, "index.json", index)
            return {"link_id": link_id, **link}

    def status(self) -> dict:
        """Only authorised references are listed; scope quotas are not capabilities."""
        with self._locked() as (_, index):
            visible = [entry["reference"] for entry in index["artifacts"].values()
                       if self.principal in (entry["reference"]["producer"], *entry["readers"])]
            return {"namespace": self.namespace, "policy": asdict(self.policy),
                    "artifacts": sorted(visible, key=lambda item: item["artifact_id"]),
                    "links": {key: value for key, value in index["links"].items() if value["owner"] == self.principal}}


@contextmanager
def _nothing():
    yield


def has_retained_artifacts(run_dir: Path) -> bool:
    """Read-only retention hook. Malformed evidence blocks run deletion."""
    root = Path(run_dir) / "artifacts"
    if not root.exists() and not root.is_symlink():
        return False
    try:
        directory = _open_dir(root)
        try:
            retained = False
            for namespace in os.listdir(directory):
                _identity(namespace)
                child = os.open(namespace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                try:
                    index = _load_json(_read_file(child, "index.json", _MAX_INDEX))
                    _validate_index(index, namespace)
                    retained = retained or any(link["state"] != "released" for link in index["links"].values())
                finally:
                    os.close(child)
            return retained
        finally:
            os.close(directory)
    except (OSError, ArtifactError):
        raise OperationsError("artefact retention state cannot be verified") from None
