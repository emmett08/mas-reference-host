"""Explicit endpoint, HTTP CONNECT proxy and additional TLS trust policy.

Proxy credentials are private runtime inputs, never descriptor content. The
additional CA bytes are frozen into policy identity; rotating trust requires a
new policy. Ambient proxy and certificate environment variables remain ignored.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import ipaddress
from pathlib import Path
import re
import ssl
from types import MappingProxyType
from urllib.parse import urlsplit

from ..service_contracts import ServiceError
from .authentication import read_private_file


HTTP_PHASES = ('dns', 'connect', 'tls', 'headers', 'body')


def _phase_timeouts(value):
    if (not isinstance(value, (dict, MappingProxyType)) or set(value) != set(HTTP_PHASES)
            or any(type(value[name]) is not int or not 1 <= value[name] <= 300_000
                   for name in HTTP_PHASES)):
        raise ServiceError('invalid_transport_policy')
    return {name: value[name] for name in HTTP_PHASES}


def _origin(value, *, proxy=False):
    try:
        parsed = urlsplit(value)
        if (type(value) is not str or not parsed.hostname or len(value) > 2048
                or parsed.scheme not in ({'http'} if proxy else {'http', 'https'})
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.path not in ('', '/')
                or any(ord(c) < 33 or ord(c) > 126 for c in value)):
            raise ValueError()
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        hostname = parsed.hostname.lower()
        host = '[' + hostname + ']' if ':' in hostname else hostname
        return f'{parsed.scheme}://{host}:{port}'
    except (ValueError, TypeError, AttributeError):
        raise ServiceError('invalid_transport_policy') from None


@dataclass(frozen=True)
class HTTPTransportPolicy:
    """Opt-in trusted configuration; direct transport remains the default.

    ``proxy_url`` is an HTTP CONNECT proxy for HTTPS origins. Loopback origins
    always bypass it. An optional private file contains a complete Basic/Bearer
    Proxy-Authorization value; the origin never receives that header.
    """
    proxy_url: str | None = None
    additional_ca_pem: str | None = field(default=None, repr=False)
    allowed_origins: tuple[str, ...] = ()
    proxy_authorization_file: Path | None = field(default=None, repr=False)
    phase_timeouts_ms: dict | None = None

    @classmethod
    def from_config(cls, value):
        if (type(value) is not dict or set(value) - {
                'proxy_url', 'additional_ca_file', 'allowed_origins', 'proxy_authorization_file',
                'phase_timeouts_ms'}
                or type(value.get('allowed_origins', [])) is not list):
            raise ServiceError('invalid_transport_policy')
        pem = None
        if 'additional_ca_file' in value:
            try:
                with Path(value['additional_ca_file']).open('rb') as stream:
                    pem = stream.read(262145).decode('ascii')
            except (OSError, TypeError, ValueError):
                raise ServiceError('invalid_ca_bundle') from None
        return cls(proxy_url=value.get('proxy_url'), additional_ca_pem=pem,
                   allowed_origins=tuple(value.get('allowed_origins', [])),
                   proxy_authorization_file=value.get('proxy_authorization_file'),
                   phase_timeouts_ms=value.get('phase_timeouts_ms'))

    def __post_init__(self):
        if self.phase_timeouts_ms is not None:
            object.__setattr__(self, 'phase_timeouts_ms', MappingProxyType(
                _phase_timeouts(self.phase_timeouts_ms)))
        if self.proxy_url is not None:
            object.__setattr__(self, 'proxy_url', _origin(self.proxy_url, proxy=True))
        elif self.proxy_authorization_file is not None:
            raise ServiceError('invalid_transport_policy')
        if type(self.allowed_origins) is not tuple or len(self.allowed_origins) > 256:
            raise ServiceError('invalid_transport_policy')
        object.__setattr__(self, 'allowed_origins', tuple(sorted(set(
            _origin(item) for item in self.allowed_origins))))
        if self.additional_ca_pem is not None:
            try:
                if (type(self.additional_ca_pem) is not str
                        or not 1 <= len(self.additional_ca_pem.encode('ascii')) <= 262144):
                    raise ValueError()
                context = ssl.create_default_context()
                context.load_verify_locations(cadata=self.additional_ca_pem)
            except (ValueError, TypeError, UnicodeError, ssl.SSLError):
                raise ServiceError('invalid_ca_bundle') from None

    def descriptor(self):
        result = {'format': 'mas-http-policy/1', 'proxy_url': self.proxy_url,
                'proxy_authentication': self.proxy_authorization_file is not None,
                'loopback_proxy_bypass': True, 'allowed_origins': list(self.allowed_origins),
                'additional_ca_sha256': hashlib.sha256(self.additional_ca_pem.encode('ascii')).hexdigest()
                    if self.additional_ca_pem is not None else None}
        if self.phase_timeouts_ms is not None:
            result.update(format='mas-http-policy/2', phase_timeouts_ms=dict(self.phase_timeouts_ms))
        return result

    @classmethod
    def validate_descriptor(cls, value):
        """Validate a public binding without loading roots or resolving secrets."""
        version = value.get('format') if type(value) is dict else None
        expected_fields = set(cls().descriptor())
        if version == 'mas-http-policy/2':
            expected_fields.add('phase_timeouts_ms')
        if (type(version) is not str or version not in {'mas-http-policy/1', 'mas-http-policy/2'}
                or set(value) != expected_fields
                or type(value['allowed_origins']) is not list
                or type(value['proxy_authentication']) is not bool
                or value['loopback_proxy_bypass'] is not True
                or value['proxy_authentication'] and value['proxy_url'] is None
                or value['additional_ca_sha256'] is not None and (
                    type(value['additional_ca_sha256']) is not str
                    or re.fullmatch(r'[0-9a-f]{64}', value['additional_ca_sha256']) is None)):
            raise ServiceError('invalid_transport_policy')
        expected = cls(proxy_url=value['proxy_url'],
                       allowed_origins=tuple(value['allowed_origins']),
                       phase_timeouts_ms=_phase_timeouts(value['phase_timeouts_ms'])
                           if version == 'mas-http-policy/2' else None).descriptor()
        expected['proxy_authentication'] = value['proxy_authentication']
        expected['additional_ca_sha256'] = value['additional_ca_sha256']
        if expected != value:
            raise ServiceError('invalid_transport_policy')
        for origin in [*expected['allowed_origins'], *([expected['proxy_url']] if expected['proxy_url'] else [])]:
            hostname = urlsplit(origin).hostname
            if ':' in hostname:
                try:
                    if '%' in hostname:
                        raise ValueError()
                    ipaddress.IPv6Address(hostname)
                except ValueError:
                    raise ServiceError('invalid_transport_policy') from None
            elif re.fullmatch(r'[a-z0-9_.-]+', hostname) is None:
                raise ServiceError('invalid_transport_policy')
        return expected

    def wire(self, endpoint):
        parsed = urlsplit(endpoint)
        origin = _origin(parsed.scheme + '://' + parsed.netloc)
        if self.allowed_origins and origin not in self.allowed_origins:
            raise ServiceError('endpoint_denied')
        try:
            loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            loopback = parsed.hostname.lower() == 'localhost'
        proxy = self.proxy_url if not loopback else None
        if proxy is not None and parsed.scheme != 'https':
            raise ServiceError('proxy_requires_https_origin')
        authorization = None
        if proxy is not None and self.proxy_authorization_file is not None:
            try:
                raw = read_private_file(self.proxy_authorization_file, max_bytes=8193)
                authorization = raw.removesuffix(b'\n').decode('ascii')
                if re.fullmatch(r'(?:Basic [A-Za-z0-9+/]+=*|Bearer [A-Za-z0-9._~+/=-]+)', authorization) is None:
                    raise ValueError()
            except (UnicodeError, ValueError):
                raise ServiceError('invalid_proxy_credential') from None
        result = {'proxy_url': proxy, 'proxy_authorization': authorization,
                  'additional_ca_pem': self.additional_ca_pem}
        if self.phase_timeouts_ms is not None:
            result['phase_timeouts_ms'] = dict(self.phase_timeouts_ms)
        return result
