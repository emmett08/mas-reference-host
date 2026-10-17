"""Authenticated bounded chunk exchange over the existing service/MCP channels.

Configured scopes select local repositories and map authenticated identities to
actors. References and transfer IDs are never capabilities. Transfer snapshots
expire; published artefacts retain the existing explicit retention semantics.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
from dataclasses import dataclass, field
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from ..artifacts import (ArtifactError, ArtifactPolicy, ArtifactRef, ArtifactStore,
    _atomic_json, _digest, _hash_source, _identity, _load_json, _open_dir,
    _open_file, _principal as actor_principal, _read_file)
from ..artifact_content import ArtifactContentBackend
from ..operations import _file_lock
from ..service_contracts import ServiceError
from .authentication import _principal

from .artifact_protocol import CHUNK_BYTES, OPERATIONS, WRITE_OPERATIONS
_ID = re.compile(r'[0-9a-f]{32}\Z')
_FORMAT = 'mas-artifact-transfer/1'


@dataclass(frozen=True)
class ArtifactScope:
    root: Path
    namespace: str
    principals: dict[str, str]
    policy: ArtifactPolicy = ArtifactPolicy()
    content_backend: ArtifactContentBackend | None = field(default=None, repr=False)

    def __post_init__(self):
        _identity(self.namespace)
        if type(self.principals) is not dict or not self.principals or len(self.principals) > 256:
            raise ServiceError('invalid_artifact_configuration')
        for name, actor in self.principals.items():
            _principal(name)
            actor_principal(actor)
        # Aliases would silently confer another authenticated user's grants.
        if len(set(self.principals.values())) != len(self.principals):
            raise ServiceError('invalid_artifact_configuration')
        if (not isinstance(self.policy, ArtifactPolicy)
                or self.content_backend is not None and not isinstance(self.content_backend, ArtifactContentBackend)):
            raise ServiceError('invalid_artifact_configuration')
        object.__setattr__(self, 'root', Path(self.root).absolute())
        object.__setattr__(self, 'principals', dict(self.principals))

    def binding(self):
        value = {'root': str(self.root), 'namespace': self.namespace,
                 'principals': self.principals, 'policy': self.policy.__dict__}
        if self.content_backend is not None:
            value["content_backend"] = self.content_backend.descriptor
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

    def authority_key(self):
        """The repository whose actor grants this scope exposes."""
        return ('local', os.path.normpath(str(self.root)), self.namespace)

    def repository(self, principal, *, readonly=False):
        actor = self.principals.get(principal)
        if actor is None:
            raise ServiceError('artifact_unavailable')
        return ArtifactStore(self.root, namespace=self.namespace, principal=actor,
                             policy=self.policy, readonly=readonly, content_backend=self.content_backend)


class ArtifactTransferService:
    def __init__(self, root: Path, scopes: dict[str, ArtifactScope], *, ttl_seconds=3600,
                 max_sessions=16, max_staging_bytes=64 * 1024 * 1024, clock=time.time):
        if (type(scopes) is not dict or not scopes or len(scopes) > 64
                or any(not isinstance(scope, ArtifactScope) for scope in scopes.values())
                or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 86400
                or type(max_sessions) is not int or not 1 <= max_sessions <= 256
                or type(max_staging_bytes) is not int or not 1 <= max_staging_bytes <= 2**40):
            raise ServiceError('invalid_artifact_configuration')
        for name in scopes:
            _identity(name)
        owners = {}
        for scope in scopes.values():
            for principal, actor in scope.principals.items():
                key = (scope.authority_key(), actor)
                if owners.setdefault(key, principal) != principal:
                    raise ServiceError('invalid_artifact_configuration')
        self.root, self.scopes = Path(root).absolute(), dict(scopes)
        self.ttl_seconds, self.max_sessions = ttl_seconds, max_sessions
        self.max_staging_bytes, self.clock = max_staging_bytes, clock

    def _scope(self, principal, name):
        _principal(principal)
        scope = self.scopes.get(name) if type(name) is str else None
        if scope is None or principal not in scope.principals:
            raise ServiceError('artifact_unavailable')
        return scope

    @contextmanager
    def _locked(self):
        # Staging and committed objects obey the same backup/admission barrier.
        deployment = next((p for p in (self.root, *self.root.parents)
                           if (p / 'deployment.sqlite').exists() or (p / '.restored-audit-only').exists()), None)
        if any((p / '.restored-audit-only').exists() or (p / '.restored-audit-only').is_symlink()
               for p in (self.root, *self.root.parents)):
            raise ServiceError('audit_only_restore')
        from contextlib import nullcontext
        lock = _file_lock(deployment / '.deployment.lock', False) if deployment else nullcontext()
        with lock:
            if deployment:
                from ..operations import DeploymentLedger
                ledger = DeploymentLedger(deployment, readonly=True)
                if not ledger.admission_enabled:
                    raise ServiceError('audit_only_restore')
            directory = _open_dir(self.root, create=True)
            try:
                if stat.S_IMODE(os.fstat(directory).st_mode) & 0o077:
                    raise ServiceError('artifact_storage_unsafe')
                fcntl.flock(directory, fcntl.LOCK_EX)
                self._cleanup(directory)
                yield directory
            finally:
                os.close(directory)

    def _cleanup(self, directory):
        now = int(self.clock())
        for name in os.listdir(directory):
            if name == 'revocations.json':
                self._revocations(directory)
                continue
            if name.endswith('.json') and _ID.fullmatch(name[:-5]):
                value = self._metadata(directory, name[:-5])
                if value['expires_at'] <= now:
                    self._remove(directory, name[:-5])
            elif name.endswith('.part') and _ID.fullmatch(name[:-5]):
                # No symlink is followed, including for orphan cleanup.
                try:
                    info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ServiceError('artifact_storage_unsafe')
                if not os.path.exists(self.root / (name[:-5] + '.json')) and info.st_mtime + self.ttl_seconds <= now:
                    os.unlink(name, dir_fd=directory)
            elif name.startswith('.pending-') and _ID.fullmatch(name[9:]):
                try:
                    info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ServiceError('artifact_storage_unsafe')
                if info.st_mtime + self.ttl_seconds <= now:
                    os.unlink(name, dir_fd=directory)
            else:
                raise ServiceError('artifact_storage_unsafe')

    def _metadata(self, directory, transfer_id):
        if type(transfer_id) is not str or not _ID.fullmatch(transfer_id):
            raise ServiceError('artifact_unavailable')
        try:
            value = _load_json(_read_file(directory, transfer_id + '.json', 32768))
        except FileNotFoundError:
            raise ServiceError('artifact_unavailable') from None
        fields = {'format', 'transfer_id', 'owner', 'kind', 'scope', 'sha256', 'size',
                  'media_type', 'input_sha256s', 'readers', 'reference', 'expires_at', 'scope_binding'}
        if (type(value) is not dict or set(value) != fields or value['format'] != _FORMAT
                or value['transfer_id'] != transfer_id or value['kind'] not in {'upload', 'download'}
                or type(value['expires_at']) is not int or type(value['size']) is not int
                or not 0 <= value['size'] <= self.max_staging_bytes):
            raise ServiceError('artifact_storage_unsafe')
        _principal(value['owner'])
        _identity(value['scope'])
        _digest(value['sha256'])
        if value['reference'] is not None:
            ref = ArtifactRef.from_dict(value['reference'])
            if (ref.sha256, ref.size, ref.media_type, list(ref.input_sha256s)) != (
                    value['sha256'], value['size'], value['media_type'], value['input_sha256s']):
                raise ServiceError('artifact_storage_unsafe')
        return value

    def _owned(self, directory, principal, transfer_id, kind=None):
        value = self._metadata(directory, transfer_id)
        if (value['owner'] != principal or value['expires_at'] <= int(self.clock())
                or kind is not None and value['kind'] != kind):
            raise ServiceError('artifact_unavailable')
        scope = self._scope(principal, value['scope'])
        if value['scope_binding'] != scope.binding():
            raise ServiceError('artifact_unavailable')
        if value['kind'] == 'download':
            self._check_grant(directory, value['scope'], scope, principal, ArtifactRef.from_dict(value['reference']))
        return value, scope

    def _capacity(self, directory, size):
        sessions, reserved = 0, 0
        for name in os.listdir(directory):
            if name == 'revocations.json':
                self._revocations(directory)
                continue
            if name.endswith('.json'):
                item = self._metadata(directory, name[:-5])
                sessions += 1
                reserved += item['size']
            elif not name.endswith('.part') or not os.path.exists(self.root / (name[:-5] + '.json')):
                reserved += os.stat(name, dir_fd=directory, follow_symlinks=False).st_size
        if sessions >= self.max_sessions or reserved + size > self.max_staging_bytes:
            raise ServiceError('artifact_staging_full')
        deployment = next((p for p in self.root.parents if (p / 'deployment.sqlite').exists()), None)
        if deployment:
            from ..operations import DeploymentLedger
            DeploymentLedger(deployment, readonly=True).check_storage_admission()

    @staticmethod
    def _remove(directory, transfer_id):
        for suffix in ('.part', '.json'):
            try:
                os.unlink(transfer_id + suffix, dir_fd=directory)
            except FileNotFoundError:
                pass
        os.fsync(directory)

    @staticmethod
    def _receipt(value):
        return {'transfer_id': value['transfer_id'], 'expires_at': value['expires_at'],
                'chunk_bytes': CHUNK_BYTES, 'size': value['size'], 'sha256': value['sha256'],
                'reference': value['reference']}

    def dispatch(self, principal, operation, payload):
        if type(operation) is not str or operation not in OPERATIONS or type(payload) is not dict:
            raise ServiceError('invalid_artifact_request')
        required, optional = OPERATIONS[operation]
        if not required <= set(payload) <= required | optional:
            raise ServiceError('invalid_artifact_request')
        _principal(principal)
        try:
            return getattr(self, operation)(principal, **payload)
        except (ArtifactError, OSError):
            raise ServiceError('artifact_unavailable') from None

    def upload_begin(self, principal, scope, sha256, size, media_type, input_sha256s=None, readers=None):
        selected = self._scope(principal, scope)
        if type(size) is not int or not 0 <= size <= min(selected.policy.max_artifact_bytes, self.max_staging_bytes):
            raise ServiceError('artifact_size_limit')
        if input_sha256s is None:
            input_sha256s = []
        if readers is None:
            readers = []
        if (type(input_sha256s) is not list or len(input_sha256s) > 32
                or any(type(item) is not str for item in input_sha256s)
                or type(readers) is not list or len(readers) > 32
                or any(type(reader) is not str or reader not in selected.principals for reader in readers)):
            raise ServiceError('invalid_artifact_request')
        inputs = tuple(sorted(set(input_sha256s)))
        ArtifactRef(selected.namespace, '0' * 64, sha256, size, media_type, selected.principals[principal], inputs)
        with self._locked() as directory:
            self._capacity(directory, size)
            value = {'format': _FORMAT, 'transfer_id': uuid.uuid4().hex, 'owner': principal,
                'kind': 'upload', 'scope': scope, 'scope_binding': selected.binding(), 'sha256': sha256, 'size': size,
                'media_type': media_type, 'input_sha256s': list(inputs), 'readers': sorted(set(readers)),
                'reference': None, 'expires_at': int(self.clock()) + self.ttl_seconds}
            transfer_id = value['transfer_id']
            try:
                _atomic_json(directory, transfer_id + '.json', value)
                os.close(_open_file(directory, transfer_id + '.part', os.O_CREAT | os.O_EXCL | os.O_WRONLY))
                os.fsync(directory)
            except BaseException:
                self._remove(directory, transfer_id)
                raise
            return self._receipt(value)

    def upload_chunk(self, principal, transfer_id, offset, data):
        if (type(offset) is not int or offset < 0 or type(data) is not str
                or not 1 <= len(data) <= ((CHUNK_BYTES + 2) // 3) * 4):
            raise ServiceError('invalid_artifact_chunk')
        try:
            chunk = base64.b64decode(data, validate=True)
        except ValueError:
            raise ServiceError('invalid_artifact_chunk') from None
        if not 1 <= len(chunk) <= CHUNK_BYTES:
            raise ServiceError('invalid_artifact_chunk')
        with self._locked() as directory:
            value, _scope = self._owned(directory, principal, transfer_id, 'upload')
            if value['reference'] is not None or offset + len(chunk) > value['size']:
                raise ServiceError('invalid_artifact_chunk')
            with os.fdopen(_open_file(directory, transfer_id + '.part', os.O_RDWR), 'r+b') as output:
                length = os.fstat(output.fileno()).st_size
                if offset < length:
                    output.seek(offset)
                    if offset + len(chunk) > length or output.read(len(chunk)) != chunk:
                        raise ServiceError('artifact_chunk_conflict')
                    return {'next_offset': length}
                if offset != length:
                    raise ServiceError('artifact_chunk_conflict')
                output.seek(offset)
                output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
                return {'next_offset': offset + len(chunk)}

    def upload_commit(self, principal, transfer_id):
        with self._locked() as directory:
            value, scope = self._owned(directory, principal, transfer_id, 'upload')
            repository = scope.repository(principal)
            if value['reference'] is not None:
                repository.verify(value['reference'])
                return {'reference': value['reference']}
            with os.fdopen(_open_file(directory, transfer_id + '.part'), 'rb') as source:
                if _hash_source(source, value['size']) != (value['size'], value['sha256']):
                    raise ServiceError('artifact_integrity_mismatch')
                source.seek(0)
                # The repository hashes the same open source again before its
                # atomic publication; a path substitution cannot change it.
                reference = repository.put_stream(source, media_type=value['media_type'], input_sha256s=tuple(value['input_sha256s']),
                    readers=tuple(scope.principals[name] for name in value['readers']))
            value['reference'] = reference.to_dict()
            _atomic_json(directory, transfer_id + '.json', value)
            os.unlink(transfer_id + '.part', dir_fd=directory)
            os.fsync(directory)
            return {'reference': value['reference']}

    def download_begin(self, principal, scope, reference):
        selected = self._scope(principal, scope)
        ref = ArtifactRef.from_dict(reference)
        repository = selected.repository(principal, readonly=True)
        repository.inspect(ref)  # Authorise before revealing transfer capacity.
        with self._locked() as directory:
            self._check_grant(directory, scope, selected, principal, ref)
            self._capacity(directory, ref.size)
            value = {'format': _FORMAT, 'transfer_id': uuid.uuid4().hex, 'owner': principal,
                'kind': 'download', 'scope': scope, 'scope_binding': selected.binding(), 'sha256': ref.sha256, 'size': ref.size,
                'media_type': ref.media_type, 'input_sha256s': list(ref.input_sha256s), 'readers': [],
                'reference': ref.to_dict(), 'expires_at': int(self.clock()) + self.ttl_seconds}
            transfer_id = value['transfer_id']
            try:
                _atomic_json(directory, transfer_id + '.json', value)
                with repository.open(ref) as source, os.fdopen(_open_file(directory, transfer_id + '.part',
                        os.O_CREAT | os.O_EXCL | os.O_WRONLY), 'wb') as output:
                    if _hash_source(source, ref.size, destination=output) != (ref.size, ref.sha256):
                        raise ServiceError('artifact_integrity_mismatch')
                    output.flush()
                    os.fsync(output.fileno())
                os.fsync(directory)
            except BaseException:
                self._remove(directory, transfer_id)
                raise
            return self._receipt(value)

    def download_chunk(self, principal, transfer_id, offset):
        if type(offset) is not int or offset < 0:
            raise ServiceError('invalid_artifact_chunk')
        with self._locked() as directory:
            value, scope = self._owned(directory, principal, transfer_id, 'download')
            scope.repository(principal, readonly=True).inspect(value['reference'])
            if offset > value['size']:
                raise ServiceError('invalid_artifact_chunk')
            with os.fdopen(_open_file(directory, transfer_id + '.part'), 'rb') as source:
                if os.fstat(source.fileno()).st_size != value['size']:
                    raise ServiceError('artifact_integrity_mismatch')
                source.seek(offset)
                chunk = source.read(min(CHUNK_BYTES, value['size'] - offset))
            return {'offset': offset, 'data': base64.b64encode(chunk).decode('ascii'),
                    'next_offset': offset + len(chunk), 'done': offset + len(chunk) == value['size']}

    def status(self, principal, transfer_id):
        with self._locked() as directory:
            value, _scope = self._owned(directory, principal, transfer_id)
            if value['kind'] == 'upload' and value['reference'] is None:
                with os.fdopen(_open_file(directory, transfer_id + '.part'), 'rb') as source:
                    offset = os.fstat(source.fileno()).st_size
                if offset > value['size']:
                    raise ServiceError('artifact_storage_unsafe')
            else:
                offset = value['size']
            return {**self._receipt(value), 'kind': value['kind'], 'next_offset': offset}

    def close(self, principal, transfer_id):
        with self._locked() as directory:
            self._owned(directory, principal, transfer_id)
            self._remove(directory, transfer_id)
            return {'closed': True}

    @staticmethod
    def _revocations(directory):
        try:
            value = _load_json(_read_file(directory, 'revocations.json', 512 * 1024))
        except FileNotFoundError:
            return {'format': 'mas-artifact-revocations/1', 'denied': []}
        if (type(value) is not dict or set(value) != {'format', 'denied'}
                or value['format'] != 'mas-artifact-revocations/1'
                or type(value['denied']) is not list or len(value['denied']) > 4096
                or any(type(item) is not str for item in value['denied'])
                or value['denied'] != sorted(set(value['denied']))):
            raise ServiceError('artifact_storage_unsafe')
        for item in value['denied']:
            _digest(item)
        return value

    @staticmethod
    def _grant_key(scope_name, scope, reference, actor):
        return hashlib.sha256(json.dumps([scope_name, scope.namespace,
            reference.artifact_id, actor], separators=(',', ':')).encode()).hexdigest()

    def _check_grant(self, directory, scope_name, scope, principal, reference):
        denied = self._revocations(directory)['denied']
        if self._grant_key(scope_name, scope, reference, scope.principals[principal]) in denied:
            raise ServiceError('artifact_unavailable')

    def revoke(self, principal, scope, reference, readers):
        selected = self._scope(principal, scope)
        ref = ArtifactRef.from_dict(reference)
        if (type(readers) is not list or not 1 <= len(readers) <= 32
                or any(type(reader) is not str or reader not in selected.principals for reader in readers)
                or ref.producer != selected.principals[principal]):
            raise ServiceError('artifact_unavailable')
        selected.repository(principal, readonly=True).inspect(ref)
        with self._locked() as directory:
            value = self._revocations(directory)
            value['denied'] = sorted(set(value['denied']) | {
                self._grant_key(scope, selected, ref, selected.principals[name]) for name in readers})
            if len(value['denied']) > 4096:
                raise ServiceError('artifact_revocation_limit')
            _atomic_json(directory, 'revocations.json', value)
            return {'revoked': True}
