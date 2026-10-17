"""Bounded non-retrying JSON HTTP to an explicitly configured peer.

Endpoints are operator configuration, never supplied by messages or discovery.
Redirects and URL credentials are forbidden. Artifact URLs remain inert data.
"""
from __future__ import annotations

import http.client
import json
import ssl
from urllib.parse import urlsplit

from ..codec import integer_json
from ..cursor_cloud import _wall_timeout
from ..service_contracts import ServiceError


def endpoint_url(value: str, *, allow_http: bool = False) -> str:
    try:
        parsed = urlsplit(value)
        parsed.port
        if (type(value) is not str or len(value) > 2048 or not parsed.hostname
                or parsed.scheme not in (('https', 'http') if allow_http else ('https',))
                or parsed.username or parsed.password or parsed.fragment or parsed.query
                or any(ord(c) < 33 or ord(c) > 126 for c in value)):
            raise ValueError()
    except (TypeError, ValueError, AttributeError):
        raise ServiceError('invalid_remote_endpoint') from None
    return value


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ServiceError('invalid_remote_json')
        result[key] = value
    return result


class BoundedJSONHTTP:
    def __init__(self, endpoint: str, *, credential: str | None = None,
                 timeout_seconds: int = 30, max_bytes: int = 65_536,
                 allow_http: bool = False, connection_factory=None, transport_policy=None):
        self.endpoint = endpoint_url(endpoint, allow_http=allow_http)
        self._parsed = urlsplit(endpoint)
        if connection_factory is None:
            from .service_client import validate_endpoint
            validate_endpoint(endpoint, allow_loopback_http=allow_http)
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise ServiceError('invalid_remote_timeout')
        if type(max_bytes) is not int or not 1024 <= max_bytes <= 1_048_576:
            raise ServiceError('invalid_remote_limit')
        if credential is not None and (type(credential) is not str or not 1 <= len(credential) <= 4096
                                      or any(ord(c) < 33 or ord(c) > 126 for c in credential)):
            raise ServiceError('invalid_remote_credential')
        if credential and self._parsed.scheme != 'https':
            raise ServiceError('insecure_remote_credential')
        self._credential = credential
        self.timeout_seconds, self.max_bytes = timeout_seconds, max_bytes
        self._factory = connection_factory
        self._allow_http = allow_http
        self.transport_policy = transport_policy
        if transport_policy is not None:
            from .http_policy import HTTPTransportPolicy
            if not isinstance(transport_policy, HTTPTransportPolicy) or connection_factory is not None:
                raise ServiceError('invalid_transport_policy')

    def descriptor(self):
        result = {'endpoint': self.endpoint, 'timeout_seconds': self.timeout_seconds,
                  'max_bytes': self.max_bytes, 'transport': 'json-http/1'}
        if self.transport_policy is not None:
            result['transport_policy'] = self.transport_policy.descriptor()
        return result

    def request(self, method: str, payload: dict | None = None, *, headers: dict | None = None) -> dict:
        if method not in ('POST', 'GET'):
            raise ServiceError('unsupported_remote_method')
        connection = None
        try:
            body = integer_json(payload, self.max_bytes).encode() if payload is not None else None
            sent = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            for name, value in (headers or {}).items():
                if name.lower() not in ('a2a-version', 'mcp-protocol-version', 'mcp-session-id', 'accept', 'content-type', 'mcp-method'):
                    raise ServiceError('unsupported_remote_header')
                if type(value) is not str or len(value) > 4096 or '\r' in value or '\n' in value:
                    raise ServiceError('invalid_remote_header')
                sent[name] = value
            if self._credential:
                sent['Authorization'] = 'Bearer ' + self._credential
            if self._factory is None:
                from .service_client import http_exchange
                status, received_headers, data = http_exchange(
                    self.endpoint, method=method, body=body, headers=sent,
                    timeout_seconds=self.timeout_seconds, max_response_bytes=self.max_bytes,
                    allow_loopback_http=self._allow_http, transport_policy=self.transport_policy)
                if not 200 <= status < 300:
                    raise ServiceError('remote_http_failure')
                media = {k.lower(): v for k, v in received_headers.items()}.get('content-type', '').split(';')[0].strip().lower()
                if media not in ('application/json', 'application/a2a+json'):
                    raise ServiceError('unsupported_remote_content_type')
                value = json.loads(data, object_pairs_hook=_unique)
                integer_json(value, self.max_bytes)
                if type(value) is not dict:
                    raise ServiceError('invalid_remote_json')
                return value
            with _wall_timeout(self.timeout_seconds):
                if self._parsed.scheme == 'https':
                    factory = self._factory or http.client.HTTPSConnection
                    connection = factory(self._parsed.hostname, self._parsed.port or 443,
                                         timeout=self.timeout_seconds, context=ssl.create_default_context())
                else:
                    factory = self._factory or http.client.HTTPConnection
                    connection = factory(self._parsed.hostname, self._parsed.port or 80, timeout=self.timeout_seconds)
                connection.request(method, self._parsed.path or '/', body=body, headers=sent)
                response = connection.getresponse()
                if not 200 <= response.status < 300:
                    raise ServiceError('remote_http_failure')
                content_type = response.getheader('Content-Type', '').split(';')[0].strip().lower()
                if content_type not in ('application/json', 'application/a2a+json'):
                    raise ServiceError('unsupported_remote_content_type')
                data = response.read(self.max_bytes + 1)
                if len(data) > self.max_bytes:
                    raise ServiceError('remote_response_limit')
                value = json.loads(data, object_pairs_hook=_unique)
                integer_json(value, self.max_bytes)
                if type(value) is not dict:
                    raise ServiceError('invalid_remote_json')
                return value
        except ServiceError:
            raise
        except Exception:
            raise ServiceError('remote_outcome_unknown') from None
        finally:
            if connection is not None:
                connection.close()
