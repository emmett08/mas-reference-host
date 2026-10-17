"""Immutable source metadata held by the shared authority, never local caches."""
from __future__ import annotations

import hashlib
from .definitions import canonical
from .service_contracts import ServiceError


class SharedDefinitionRegistry:
    def __init__(self, authority):
        self.authority = authority

    def remember(self, kind, name, identity, *, descriptor=None,
                 implementation_digest=None, implementation_manifest=None, current=False):
        if descriptor is not None and hashlib.sha256(canonical(descriptor).encode()).hexdigest() != identity:
            raise ServiceError('definition_identity_conflict')
        saved = self.inspect(kind, name, identity)
        if saved is not None and saved['retired'] and current:
            raise ServiceError('definition_retired')
        verified = (saved is not None and saved['implementation_digest'] == implementation_digest
            and saved['implementation_provenance'] == 'verified_source_package')
        if implementation_manifest is not None:
            source = canonical(implementation_manifest)
            if (implementation_manifest.get('format') != 'mas-python-implementation/1'
                    or hashlib.sha256(source.encode()).hexdigest() != implementation_digest):
                raise ServiceError('implementation_digest_mismatch')
            # Chunk immutable package metadata beneath the private protocol frame
            # bound. No pickle, executable import path or mutable local mirror.
            if len(source.encode()) > 16 * 1024 * 1024:
                raise ServiceError('implementation_package_too_large')
            if not verified:
                chunks = [source[n:n + 6000] for n in range(0, len(source), 6000)]
                for part, data in enumerate(chunks):
                    self.authority.command('definition.chunk', {'digest': implementation_digest,
                        'part': part, 'data': data}, command_id='manifest-' + implementation_digest + '-' + str(part))
                self.authority.command('definition.manifest', {'digest': implementation_digest,
                    'parts': len(chunks)}, command_id='manifest-' + implementation_digest)
        if (saved is not None and saved['descriptor'] == descriptor
                and saved['implementation_digest'] == implementation_digest
                and (not current or self.current(kind, name) == identity)):
            return
        self.authority.command('definition.register', {'kind': kind, 'name': name,
            'identity': identity, 'descriptor': descriptor,
            'implementation_digest': implementation_digest, 'current': current})

    def inspect(self, kind, name, identity):
        return self.authority.query('definition.inspect', {'kind': kind, 'name': name, 'identity': identity})

    def current(self, kind, name):
        return self.authority.query('definition.current', {'kind': kind, 'name': name})

    def names(self, kind):
        return set(self.authority.query('definition.names', {'kind': kind}))

    def retire(self, kind, name, identity):
        return self.authority.command('definition.retire', {'kind': kind, 'name': name, 'identity': identity})['receipt']
