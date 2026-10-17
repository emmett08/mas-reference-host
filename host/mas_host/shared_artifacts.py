"""Principal-bound PostgreSQL metadata and verified S3 object bytes.

No authoritative files live in the controller cache. A crash between PUT and
publication leaves a charged reservation; replacement authority can verify the
same bytes and publish the original reference. Stale epochs cannot publish.
"""
from contextlib import contextmanager
from copy import copy
from dataclasses import asdict
import hashlib
from io import BytesIO
import os
from pathlib import Path
from tempfile import TemporaryFile

from .artifact_contracts import ArtifactError, ArtifactPolicy, ArtifactRef, _identity, _principal, _digest, _json
from .artifacts import _hash_source, _manifest_identity, _open_dir, _open_file
from .contracts import RunnerError
from .s3_artifacts import S3ArtifactContent


class SharedArtifactStore:
    def __init__(self, authority, *, namespace, principal, content_backend,
                 policy=None, readonly=False, retention_target=None):
        if not isinstance(content_backend, S3ArtifactContent):
            raise ArtifactError('shared artefacts require the explicit S3 content profile')
        self.authority, self.content_backend = authority, content_backend
        self.namespace, self.principal = _identity(namespace), _principal(principal)
        self.readonly = readonly
        self.retention_target = self._target(retention_target)
        self.policy = policy or ArtifactPolicy()
        if not isinstance(self.policy, ArtifactPolicy):
            raise ArtifactError('invalid shared artefact policy')
        self.store_id = hashlib.sha256(_json([authority._binding['deployment'],
                                             authority.namespace, self.namespace])).hexdigest()[:32]
        expected = {'namespace': self.namespace, 'policy': asdict(self.policy),
                    'backend': content_backend.descriptor, 'store_id': self.store_id}
        if policy is None and not readonly:
            try:
                retained = self._query('namespace', {}, principal=False)
            except ArtifactError:
                # Creation still requires an epoch-validated transaction; an
                # unavailable authority cannot become a local default store.
                pass
            else:
                self.policy = ArtifactPolicy(**retained['policy'])
                expected['policy'] = retained['policy']
        if readonly:
            current = self._query('namespace', {}, principal=False)
            if policy is None:
                self.policy = ArtifactPolicy(**current['policy'])
                expected['policy'] = current['policy']
        else:
            identity = hashlib.sha256(_json(expected)).hexdigest()
            current = self._command('configure', expected, principal=False, command_id='artifact-config-' + identity)
        if any(current.get(k) != v for k, v in expected.items()):
            raise ArtifactError('immutable shared artefact configuration differs')

    def _query(self, operation, payload, *, principal=True):
        fields = {'namespace': self.namespace, **payload}
        if principal:
            fields['principal'] = self.principal
        try:
            return self.authority.query('artifact.' + operation, fields)
        except RunnerError:
            raise ArtifactError('shared artefact authority unavailable or access denied') from None

    def _command(self, operation, payload, *, principal=True, command_id=None):
        if self.readonly:
            raise ArtifactError('shared artefact store is read-only')
        fields = {'namespace': self.namespace, **payload}
        if principal:
            fields['principal'] = self.principal
        try:
            return self.authority.command('artifact.' + operation, fields, command_id=command_id)['receipt']
        except RunnerError:
            raise ArtifactError('shared artefact authority unavailable or command rejected') from None

    def put_bytes(self, data, **metadata):
        if type(data) is not bytes:
            raise ArtifactError('artefact content must be bytes')
        return self.put_stream(BytesIO(data), **metadata)

    def put_file(self, path, **metadata):
        path = Path(path).absolute()
        try:
            directory = _open_dir(path.parent)
            try:
                descriptor = _open_file(directory, path.name)
            finally:
                os.close(directory)
            with os.fdopen(descriptor, 'rb') as source:
                return self.put_stream(source, **metadata)
        except OSError:
            raise ArtifactError('artefact source unavailable or unsafe') from None

    def put_stream(self, source, *, media_type='application/octet-stream',
                   input_sha256s=(), readers=()):
        if (not callable(getattr(source, 'seekable', None)) or not source.seekable()
                or type(input_sha256s) is not tuple or type(readers) is not tuple
                or len(input_sha256s) > 32 or len(readers) > 32):
            raise ArtifactError('invalid artefact source, provenance or grants')
        inputs = tuple(sorted(set(_digest(item) for item in input_sha256s)))
        grants = sorted(set(_principal(item) for item in readers) - {self.principal})
        ArtifactRef(self.namespace, '0'*64, '0'*64, 0, media_type, self.principal, inputs)
        offset = source.tell()
        size, digest = _hash_source(source, self.policy.max_artifact_bytes)
        source.seek(offset)
        provisional = ArtifactRef(self.namespace, '0'*64, digest, size, media_type, self.principal, inputs)
        reference = ArtifactRef(self.namespace, _manifest_identity(provisional, grants),
                                digest, size, media_type, self.principal, inputs)
        request = {'reference': reference.to_dict(), 'readers': grants}
        # A retained intent charges the content before any remote upload. No
        # database transaction or namespace lock is held across network I/O.
        self._command('reserve', request)
        self.content_backend.publish_object(self.store_id, reference, source, self.policy)
        self._command('complete', request)
        return reference

    def _reference(self, reference):
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        self._query('inspect', {'reference': ref.to_dict()})
        return ref

    @contextmanager
    def open(self, reference):
        ref = self._reference(reference)
        with TemporaryFile(mode='w+b') as snapshot:
            self.content_backend.verify_object(self.store_id, ref, destination=snapshot)
            # A grant revoked during a download prevents handing out its bytes.
            # Already returned snapshots cannot be recalled.
            self._reference(ref)
            snapshot.seek(0)
            yield snapshot

    def read_bytes(self, reference):
        with self.open(reference) as source:
            return source.read()

    def verify(self, reference):
        with self.open(reference):
            return True

    def inspect(self, reference):
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        return self._query('inspect', {'reference': ref.to_dict()})

    @staticmethod
    def _target(target):
        if target is None:
            return None
        if (type(target) is not dict or set(target) != {'kind', 'id', 'principal'}
                or type(target['kind']) is not str or target['kind'] not in {'run', 'task', 'workflow'}):
            raise ArtifactError('invalid artefact retention target')
        return {'kind': target['kind'], 'id': _identity(target['id']),
                'principal': _principal(target['principal'])}

    def with_retention_target(self, *, kind, id, principal):
        """Trusted lifecycle binding; native commands validate owner and actor map.

        Reuse creates a distinct link per target; it cannot move an existing
        consumer edge. The original repository and its default stay unchanged.
        """
        bound = copy(self)
        bound.retention_target = self._target({'kind': kind, 'id': id, 'principal': principal})
        return bound

    def retention_link_id(self, link_id):
        original = _identity(link_id)
        if self.retention_target is None:
            return original
        return 'lifecycle-' + hashlib.sha256(_json([original, self.retention_target])).hexdigest()

    def link(self, link_id, references, *, state='live', preserve_released=False):
        if type(references) is not list or not 1 <= len(references) <= 64:
            raise ArtifactError('invalid artefact retention references')
        return self._command('link', {'link_id': self.retention_link_id(link_id),
            'references': [r.to_dict() if isinstance(r, ArtifactRef) else ArtifactRef.from_dict(r).to_dict()
                           for r in references], 'state': state, 'preserve_released': preserve_released,
            'retention_target': self.retention_target})

    def ensure_link(self, link_id, references, *, state='unresolved'):
        return self.link(link_id, references, state=state, preserve_released=True)

    def revoke(self, reference, reader):
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        return self._command('revoke', {'reference': ref.to_dict(), 'reader': _principal(reader)})

    def status(self):
        return self._query('status', {})
