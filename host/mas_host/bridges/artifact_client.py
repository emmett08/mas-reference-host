"""Explicit, authenticated artefact transfer; no references trigger URL fetches."""
from __future__ import annotations

import base64
import hashlib
import json
from tempfile import TemporaryFile
import time

from ..artifact_contracts import ArtifactError, ArtifactRef, ArtifactRepository
from ..codec import integer_json
from ..service_contracts import ServiceError
from .artifact_protocol import CHUNK_BYTES, OPERATIONS


def artifact_tools():
    string = {'type': 'string'}
    integer = {'type': 'integer', 'minimum': 0}
    fields = {'scope': string, 'sha256': string, 'size': integer, 'media_type': string,
              'input_sha256s': {'type': 'array', 'items': string, 'maxItems': 32},
              'readers': {'type': 'array', 'items': string, 'maxItems': 32},
              'transfer_id': string, 'offset': integer, 'data': string,
              'reference': {'type': 'object'}}
    return [{'name': 'mas.artifact.' + name,
             'description': 'Authenticated bounded artefact transfer: ' + name.replace('_', ' ') + '.',
             'inputSchema': {'type': 'object', 'additionalProperties': False,
                 'required': sorted(required),
                 'properties': {key: fields[key] for key in sorted(required | optional)}}}
            for name, (required, optional) in sorted(OPERATIONS.items())]


class ArtifactTransferIncomplete(ServiceError):
    """An upload has a retained owner-bound session; inspect before continuing."""
    def __init__(self, transfer_id):
        super().__init__('artifact_transfer_incomplete')
        self.transfer_id = transfer_id


class ArtifactTransferClient:
    """A configured peer transport, with bounded chunks and no implicit retries.

    `exchange` must enforce its own per-request deadline and authentication.
    The built-in HTTP and MCP constructors preserve their transport policies.
    The whole-transfer deadline is checked between every bounded request; a
    request already in flight can consume at most that transport's deadline.
    """
    def __init__(self, exchange, *, descriptor, max_bytes=16 * 1024 * 1024, timeout_seconds=300):
        if (not callable(exchange) or type(descriptor) is not dict or type(max_bytes) is not int or not 1 <= max_bytes <= 2**40
                or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 86400):
            raise ServiceError('invalid_artifact_configuration')
        self.exchange = exchange
        self.descriptor = json.loads(integer_json(descriptor, 8192))
        self.max_bytes, self.timeout_seconds = max_bytes, timeout_seconds

    @classmethod
    def from_service(cls, client, **options):
        descriptor = {'protocol': 'mas-artifact-http/1', 'endpoint': client.endpoint,
                      'principal': client.principal}
        if client.transport_policy is not None:
            descriptor['transport_policy'] = client.transport_policy.descriptor()
        return cls(lambda operation, payload: client.artifact(client.principal, operation, payload),
                   descriptor=descriptor, **options)

    @classmethod
    def from_mcp(cls, client, **options):
        tools = {item['name']: item for item in artifact_tools()}
        def exchange(operation, payload):
            result = client.call_tool(tools['mas.artifact.' + operation], payload)
            if result.get('resultType') != 'complete' or result.get('isError') is not False:
                raise ServiceError('artifact_transfer_failed')
            value = result.get('structuredContent')
            if type(value) is not dict:
                raise ServiceError('invalid_artifact_response')
            return value
        return cls(exchange, descriptor=client.descriptor(), **options)

    def _call(self, operation, payload, deadline):
        if time.monotonic() >= deadline:
            raise ServiceError('artifact_transfer_timeout')
        result = self.exchange(operation, payload)
        if type(result) is not dict:
            raise ServiceError('invalid_artifact_response')
        integer_json(result, 60000)
        return result

    @staticmethod
    def _session(value, reference=None):
        import re
        if (set(value) != {'transfer_id', 'expires_at', 'chunk_bytes', 'size', 'sha256', 'reference'}
                or type(value['transfer_id']) is not str or not re.fullmatch('[0-9a-f]{32}', value['transfer_id'])
                or type(value['expires_at']) is not int or type(value['chunk_bytes']) is not int
                or not 1 <= value['chunk_bytes'] <= CHUNK_BYTES):
            raise ServiceError('invalid_artifact_response')
        if reference is not None and (value['reference'] != reference.to_dict()
                or value['size'] != reference.size or value['sha256'] != reference.sha256):
            raise ServiceError('artifact_integrity_mismatch')
        return value['transfer_id'], value['chunk_bytes']

    def _close(self, transfer_id, deadline):
        try:
            self._call('close', {'transfer_id': transfer_id}, deadline)
        except Exception:
            # TTL bounds abandoned server snapshots, including lost close acks.
            pass

    def materialize(self, scope, reference, repository: ArtifactRepository, *, readers=()):
        """Verify bytes, then publish a local object and durable source receipt.

        The returned live link protects both objects from run retention. The
        application releases it after the consuming work is resolved terminal.
        """
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        if ref.size > min(self.max_bytes, repository.policy.max_artifact_bytes):
            raise ServiceError('artifact_size_limit')
        deadline = time.monotonic() + self.timeout_seconds
        value = self._call('download_begin', {'scope': scope, 'reference': ref.to_dict()}, deadline)
        transfer_id, chunk_bytes = self._session(value, ref)
        try:
            with TemporaryFile(mode='w+b') as output:
                offset, digest = 0, hashlib.sha256()
                while offset < ref.size:
                    chunk = self._call('download_chunk', {'transfer_id': transfer_id, 'offset': offset}, deadline)
                    if (set(chunk) != {'offset', 'data', 'next_offset', 'done'} or chunk['offset'] != offset
                            or type(chunk['data']) is not str or len(chunk['data']) > ((chunk_bytes + 2) // 3) * 4):
                        raise ServiceError('invalid_artifact_response')
                    try:
                        data = base64.b64decode(chunk['data'], validate=True)
                    except ValueError:
                        raise ServiceError('invalid_artifact_response') from None
                    if (not 1 <= len(data) <= chunk_bytes or offset + len(data) > ref.size
                            or type(chunk['next_offset']) is not int or chunk['next_offset'] != offset + len(data)
                            or type(chunk['done']) is not bool or chunk['done'] != (offset + len(data) == ref.size)):
                        raise ServiceError('invalid_artifact_response')
                    output.write(data)
                    digest.update(data)
                    offset += len(data)
                if offset != ref.size or digest.hexdigest() != ref.sha256:
                    raise ServiceError('artifact_integrity_mismatch')
                output.seek(0)
                # Source hash records the transfer dependency; source metadata
                # and authenticated peer binding are retained in the receipt.
                local = repository.put_stream(output, media_type=ref.media_type, input_sha256s=(ref.sha256,), readers=readers)
            receipt = {'format': 'mas-materialized-artifact/1', 'reference': local.to_dict(),
                'source': {'peer': self.descriptor, 'scope': scope, 'reference': ref.to_dict()}}
            evidence = repository.put_bytes(integer_json(receipt, 32768).encode(),
                media_type='application/vnd.mas.transfer-receipt+json', input_sha256s=(ref.sha256,), readers=readers)
            link_id = 'transfer-' + evidence.artifact_id
            repository.link(link_id, [local, evidence], state='live')
            return {**receipt, 'receipt': evidence.to_dict(), 'link_id': link_id}
        finally:
            self._close(transfer_id, deadline)

    def publish(self, scope, repository: ArtifactRepository, reference, *, readers=()):
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        if ref.size > self.max_bytes:
            raise ServiceError('artifact_size_limit')
        deadline = time.monotonic() + self.timeout_seconds
        # Authorisation and integrity are checked before any upload reservation.
        with repository.open(ref) as source:
            value = self._call('upload_begin', {'scope': scope, 'sha256': ref.sha256, 'size': ref.size,
                'media_type': ref.media_type, 'input_sha256s': [ref.sha256], 'readers': list(readers)}, deadline)
            transfer_id, chunk_bytes = self._session(value)
            if value['size'] != ref.size or value['sha256'] != ref.sha256 or value['reference'] is not None:
                raise ServiceError('invalid_artifact_response')
            complete = False
            try:
                offset = 0
                while data := source.read(chunk_bytes):
                    result = self._call('upload_chunk', {'transfer_id': transfer_id, 'offset': offset,
                        'data': base64.b64encode(data).decode('ascii')}, deadline)
                    offset += len(data)
                    if set(result) != {'next_offset'} or type(result['next_offset']) is not int or result['next_offset'] != offset:
                        raise ServiceError('invalid_artifact_response')
                result = self._call('upload_commit', {'transfer_id': transfer_id}, deadline)
                remote = ArtifactRef.from_dict(result.get('reference'))
                if (remote.sha256, remote.size, remote.media_type, remote.input_sha256s) != (
                        ref.sha256, ref.size, ref.media_type, (ref.sha256,)):
                    raise ServiceError('artifact_integrity_mismatch')
                complete = True
                return remote
            except Exception:
                # Keep the exact session after a lost chunk/commit receipt. A
                # status lookup or exact commit can recover it until expiry.
                raise ArtifactTransferIncomplete(transfer_id) from None
            finally:
                if complete:
                    self._close(transfer_id, deadline)

    def revoke(self, scope, reference, *, readers):
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        return self._call('revoke', {'scope': scope, 'reference': ref.to_dict(), 'readers': list(readers)},
                          time.monotonic() + self.timeout_seconds)

    def inspect_transfer(self, transfer_id):
        return self._call('status', {'transfer_id': transfer_id}, time.monotonic() + self.timeout_seconds)
