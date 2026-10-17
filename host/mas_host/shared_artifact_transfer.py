"""Durable bounded transfer sessions over shared artefact authority."""
import base64
from collections import OrderedDict
import hashlib
import json
import threading
from tempfile import TemporaryFile
import uuid
import weakref

from .artifacts import ArtifactRef, _hash_source
from .contracts import RunnerError
from .bridges.authentication import _principal
from .bridges.artifact_protocol import CHUNK_BYTES
from .bridges.artifact_transfer import ArtifactScope, ArtifactTransferService, _ID
from .service_contracts import ServiceError
from .shared_artifacts import SharedArtifactStore


def _close_snapshots(entries):
    for entry in entries.values():
        if entry['source'] is not None:
            entry['source'].close()


class SharedArtifactScope(ArtifactScope):
    def __init__(self, authority, root, namespace, principals, policy, content_backend):
        super().__init__(root, namespace, principals, policy, content_backend)
        if content_backend is None:
            raise ServiceError('shared_artifacts_require_s3')
        object.__setattr__(self, 'authority', authority)

    def binding(self):
        return hashlib.sha256(json.dumps({'deployment': self.authority._binding['deployment'],
            'database_namespace': self.authority.namespace, 'namespace': self.namespace,
            'principals': self.principals, 'policy': self.policy.__dict__,
            'content_backend': self.content_backend.descriptor}, sort_keys=True).encode()).hexdigest()

    def authority_key(self):
        return ('shared', self.authority._binding['deployment'], self.authority.namespace, self.namespace)

    def repository(self, principal, *, readonly=False):
        actor = self.principals.get(principal)
        if actor is None:
            raise ServiceError('artifact_unavailable')
        return SharedArtifactStore(self.authority, namespace=self.namespace, principal=actor,
            policy=self.policy, readonly=readonly, content_backend=self.content_backend)


class SharedArtifactTransferService(ArtifactTransferService):
    """The local root is disposable; all acknowledged transfer state is shared."""
    def __init__(self, root, scopes, **options):
        super().__init__(root, scopes, **options)
        if any(not isinstance(scope, SharedArtifactScope) for scope in scopes.values()):
            raise ServiceError('invalid_artifact_configuration')
        self.authority = next(iter(scopes.values())).authority
        if any(scope.authority is not self.authority for scope in scopes.values()):
            raise ServiceError('invalid_artifact_configuration')
        configuration = {'ttl_seconds': self.ttl_seconds, 'max_sessions': self.max_sessions,
            'max_staging_bytes': self.max_staging_bytes, 'scopes': {
                name: {'namespace': scope.namespace, 'principals': scope.principals,
                    'binding': scope.binding(), 'max_artifact_bytes': scope.policy.max_artifact_bytes}
                for name, scope in scopes.items()}}
        identity = hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()
        self._command('configure', configuration, command_id='transfer-config-' + identity)
        self._snapshots = OrderedDict()
        self._snapshot_lock = threading.RLock()
        self._snapshot_finalizer = weakref.finalize(self, _close_snapshots, self._snapshots)

    def _command(self, operation, payload, **options):
        try:
            return self.authority.command('transfer.' + operation, payload, **options)['receipt']
        except RunnerError:
            raise ServiceError('artifact_unavailable') from None

    def _query(self, operation, payload):
        try:
            return self.authority.query('transfer.' + operation, payload)
        except RunnerError:
            raise ServiceError('artifact_unavailable') from None

    def _session(self, principal, transfer_id, kind=None):
        _principal(principal)
        if type(transfer_id) is not str or not _ID.fullmatch(transfer_id):
            raise ServiceError('artifact_unavailable')
        value = self._query('session', {'principal': principal, 'transfer_id': transfer_id})
        scope = self._scope(principal, value['scope'])
        if value['scope_binding'] != scope.binding() or kind is not None and value['kind'] != kind:
            raise ServiceError('artifact_unavailable')
        return value, scope

    def upload_begin(self, principal, scope, sha256, size, media_type, input_sha256s=None, readers=None):
        selected = self._scope(principal, scope)
        if type(size) is not int or not 0 <= size <= min(selected.policy.max_artifact_bytes,
                                                       self.max_staging_bytes, 65536 * CHUNK_BYTES):
            raise ServiceError('artifact_size_limit')
        inputs = [] if input_sha256s is None else input_sha256s
        readers = [] if readers is None else readers
        if (type(inputs) is not list or len(inputs) > 32 or any(type(item) is not str for item in inputs)
                or type(readers) is not list or len(readers) > 32
                or any(type(reader) is not str or reader not in selected.principals for reader in readers)):
            raise ServiceError('invalid_artifact_request')
        inputs = sorted(set(inputs))
        ArtifactRef(selected.namespace, '0' * 64, sha256, size, media_type, selected.principals[principal], tuple(inputs))
        value = self._command('begin', {'principal': principal, 'transfer_id': uuid.uuid4().hex,
            'scope': scope, 'kind': 'upload', 'sha256': sha256, 'size': size, 'media_type': media_type,
            'input_sha256s': inputs, 'readers': sorted(set(readers)), 'reference': None})
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
        self._session(principal, transfer_id, 'upload')
        return self._command('chunk', {'principal': principal, 'transfer_id': transfer_id,
                                      'offset': offset, 'hex': chunk.hex()})

    def upload_commit(self, principal, transfer_id):
        value, scope = self._session(principal, transfer_id, 'upload')
        repository = scope.repository(principal)
        if value['reference'] is not None:
            repository.verify(value['reference'])
            self._session(principal, transfer_id, 'upload')
            return {'reference': value['reference']}
        if value['next_offset'] != value['size']:
            raise ServiceError('artifact_integrity_mismatch')
        with TemporaryFile(mode='w+b') as source:
            offset = 0
            while offset < value['size']:
                part = self._query('read', {'principal': principal, 'transfer_id': transfer_id, 'offset': offset})
                chunk = bytes.fromhex(part['hex'])
                if not chunk or part['next_offset'] != offset + len(chunk):
                    raise ServiceError('artifact_integrity_mismatch')
                source.write(chunk)
                offset = part['next_offset']
            source.seek(0)
            if _hash_source(source, value['size']) != (value['size'], value['sha256']):
                raise ServiceError('artifact_integrity_mismatch')
            source.seek(0)
            reference = repository.put_stream(source, media_type=value['media_type'],
                input_sha256s=tuple(value['input_sha256s']),
                readers=tuple(scope.principals[name] for name in value['readers']))
        return self._command('complete', {'principal': principal, 'transfer_id': transfer_id,
                                         'reference': reference.to_dict()})

    def download_begin(self, principal, scope, reference):
        selected = self._scope(principal, scope)
        ref = ArtifactRef.from_dict(reference)
        selected.repository(principal, readonly=True).verify(ref)
        value = self._command('begin', {'principal': principal, 'transfer_id': uuid.uuid4().hex,
            'scope': scope, 'kind': 'download', 'sha256': ref.sha256, 'size': ref.size,
            'media_type': ref.media_type, 'input_sha256s': list(ref.input_sha256s),
            'readers': [], 'reference': ref.to_dict()})
        return self._receipt(value)

    def download_chunk(self, principal, transfer_id, offset):
        if type(offset) is not int or offset < 0:
            raise ServiceError('invalid_artifact_chunk')
        value, scope = self._session(principal, transfer_id, 'download')
        if offset > value['size']:
            raise ServiceError('invalid_artifact_chunk')
        with self._snapshot_lock:
            entry = self._snapshots.get(transfer_id)
            if entry is None:
                while (len(self._snapshots) >= self.max_sessions
                        or sum(item['size'] for item in self._snapshots.values()) + value['size'] > self.max_staging_bytes):
                    for key, candidate in list(self._snapshots.items()):
                        if candidate['lock'].acquire(blocking=False):
                            try:
                                candidate['closed'] = True
                                if candidate['source'] is not None:
                                    candidate['source'].close()
                                del self._snapshots[key]
                            finally:
                                candidate['lock'].release()
                            break
                    else:
                        raise ServiceError('artifact_staging_full')
                # Reservations include in-flight downloads until their I/O
                # finishes, even if a concurrent close invalidates the session.
                entry = {'lock': threading.Lock(), 'source': None,
                         'size': value['size'], 'closed': False}
                self._snapshots[transfer_id] = entry
            self._snapshots.move_to_end(transfer_id)
        try:
            with entry['lock']:
                if entry['closed']:
                    raise ServiceError('artifact_unavailable')
                self._session(principal, transfer_id, 'download')
                if entry['source'] is None:
                    source = TemporaryFile(mode='w+b')
                    try:
                        with scope.repository(principal, readonly=True).open(value['reference']) as verified:
                            if _hash_source(verified, value['size'], destination=source) != (value['size'], value['sha256']):
                                raise ServiceError('artifact_integrity_mismatch')
                    except BaseException:
                        source.close()
                        raise
                    entry['source'] = source
                source = entry['source']
                source.seek(offset)
                chunk = source.read(min(CHUNK_BYTES, value['size'] - offset))
        except BaseException:
            entry['closed'] = True
            raise
        finally:
            with self._snapshot_lock:
                if entry['closed'] and entry['lock'].acquire(blocking=False):
                    try:
                        if entry['source'] is not None:
                            entry['source'].close()
                        if self._snapshots.get(transfer_id) is entry:
                            del self._snapshots[transfer_id]
                    finally:
                        entry['lock'].release()
        # A revocation, closure, expiry or authority handover during S3 I/O
        # invalidates this read before any bytes leave the service.
        self._session(principal, transfer_id, 'download')
        return {'offset': offset, 'data': base64.b64encode(chunk).decode('ascii'),
                'next_offset': offset + len(chunk), 'done': offset + len(chunk) == value['size']}

    def status(self, principal, transfer_id):
        value, _scope = self._session(principal, transfer_id)
        return {**self._receipt(value), 'kind': value['kind'], 'next_offset': value['next_offset']}

    def close(self, principal, transfer_id):
        _principal(principal)
        result = self._command('close', {'principal': principal, 'transfer_id': transfer_id})
        with self._snapshot_lock:
            entry = self._snapshots.get(transfer_id)
            if entry is not None:
                entry['closed'] = True
                if entry['lock'].acquire(blocking=False):
                    try:
                        if entry['source'] is not None:
                            entry['source'].close()
                        del self._snapshots[transfer_id]
                    finally:
                        entry['lock'].release()
        return result

    def revoke(self, principal, scope, reference, readers):
        selected = self._scope(principal, scope)
        ref = ArtifactRef.from_dict(reference)
        if (type(readers) is not list or not 1 <= len(readers) <= 32
                or any(reader not in selected.principals for reader in readers)
                or ref.producer != selected.principals[principal]):
            raise ServiceError('artifact_unavailable')
        repository = selected.repository(principal)
        repository.inspect(ref)
        for reader in readers:
            repository.revoke(ref, selected.principals[reader])
        return {'revoked': True}
