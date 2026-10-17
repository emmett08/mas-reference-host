"""Authenticated principals and scoped bearer validation at the network boundary.

OAuth uses RFC 7662 at one administrator-configured HTTPS endpoint. The resource
server never treats a caller-supplied identity or token audience as authority.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import os
import re
import stat
import time
from typing import Protocol
from urllib.parse import urlencode, urlsplit

from ..service_contracts import ServiceError

SCOPES = frozenset({"mas:submit", "mas:read", "mas:cancel", "mas:update", "mas:worker", "mas:admin"})
_TOKEN = re.compile(r"[A-Za-z0-9\-._~+/]+=*\Z")


class AuthenticationError(ServiceError):
    def __init__(self, code="invalid_token", *, required_scopes=()):
        self.required_scopes = frozenset(required_scopes)
        super().__init__(code)


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    name: str
    scopes: frozenset[str]

    def require(self, scopes):
        needed = frozenset(scopes)
        if not needed.issubset(self.scopes):
            raise AuthenticationError("insufficient_scope", required_scopes=needed)
        return self


class TokenVerifier(Protocol):
    def verify(self, token: str, required_scopes=()) -> AuthenticatedPrincipal: ...


def validate_token(token: str) -> str:
    if type(token) is not str or not 16 <= len(token) <= 8192 or not _TOKEN.fullmatch(token):
        raise AuthenticationError()
    return token


def read_private_file(path, *, max_bytes=65536) -> bytes:
    """Read exactly one owned regular file, without a symlink or mode widening."""
    if os.name != "posix":
        raise AuthenticationError("invalid_credential_file")
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK)
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) & 0o077 or info.st_size > max_bytes):
            raise AuthenticationError("invalid_credential_file")
        result = b""
        while len(result) <= max_bytes:
            chunk = os.read(fd, min(8192, max_bytes + 1 - len(result)))
            if not chunk:
                break
            result += chunk
        if len(result) > max_bytes:
            raise AuthenticationError("invalid_credential_file")
        return result
    except (OSError, ValueError, TypeError):
        raise AuthenticationError("invalid_credential_file") from None
    finally:
        if fd is not None:
            os.close(fd)


def read_token_file(path) -> str:
    try:
        raw = read_private_file(path, max_bytes=8193)
        # A single POSIX terminal newline is permitted. Whitespace is not trimmed.
        if raw.endswith(b"\n"):
            raw = raw[:-1]
        return validate_token(raw.decode("ascii"))
    except (UnicodeError, AuthenticationError):
        raise AuthenticationError("invalid_credential_file") from None


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _principal(name):
    try:
        valid = (type(name) is str and 1 <= len(name.encode("utf-8")) <= 256
                 and not any(ord(c) < 33 or ord(c) == 127 for c in name))
    except UnicodeError:
        valid = False
    if not valid:
        raise AuthenticationError("invalid_principal")
    return name


def read_client_secret(path):
    """OAuth client secrets are opaque values, not necessarily bearer syntax."""
    try:
        raw = read_private_file(path, max_bytes=8193)
        if raw.endswith(b"\n"):
            raw = raw[:-1]
        secret = raw.decode("utf-8")
        if not secret or any(ord(c) < 32 or ord(c) == 127 for c in secret):
            raise ValueError()
        return secret
    except (ValueError, UnicodeError):
        raise AuthenticationError("invalid_credential_file") from None


class StaticTokenVerifier:
    """Explicit service-token profile; stores SHA-256 digests, never bearer keys."""
    def __init__(self, credentials):
        self._credentials = []
        seen = set()
        for item in credentials:
            if (type(item) is not dict or set(item) != {"sha256", "principal", "scopes"}
                    or type(item["sha256"]) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
                    or type(item["scopes"]) is not list
                    or any(type(s) is not str or s not in SCOPES for s in item["scopes"])
                    or item["sha256"] in seen):
                raise AuthenticationError("invalid_verifier_configuration")
            principal = AuthenticatedPrincipal(_principal(item["principal"]), frozenset(item["scopes"]))
            self._credentials.append((item["sha256"], principal))
            seen.add(item["sha256"])
        if not 1 <= len(self._credentials) <= 256:
            raise AuthenticationError("invalid_verifier_configuration")

    @classmethod
    def from_file(cls, path):
        try:
            value = json.loads(read_private_file(path), object_pairs_hook=_unique)
            if (type(value) is not dict or set(value) != {"version", "credentials"}
                    or type(value["version"]) is not int or value["version"] != 1
                    or type(value["credentials"]) is not list):
                raise ValueError()
            return cls(value["credentials"])
        except (ValueError, TypeError, UnicodeError):
            raise AuthenticationError("invalid_verifier_configuration") from None

    def verify(self, token, required_scopes=()):
        digest = hashlib.sha256(validate_token(token).encode("ascii")).hexdigest()
        matched = None
        # Scan all records to avoid disclosing credential ordering.
        for stored, principal in self._credentials:
            if hmac.compare_digest(digest, stored):
                matched = principal
        if matched is None:
            raise AuthenticationError()
        return matched.require(required_scopes)


def _https_url(value):
    try:
        parsed = urlsplit(value)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment
                or any(ord(c) <= 32 or ord(c) == 127 for c in value)):
            raise ValueError()
        parsed.port
        return value.rstrip("/")
    except (ValueError, TypeError):
        raise AuthenticationError("invalid_verifier_configuration") from None


class OAuthIntrospectionVerifier:
    """RFC 7662 resource-server verifier, with explicit audience and issuer.

    A token remains accepted only while its issuing server reports it active.
    No introspection cache postpones revocation. Local principal grants still
    restrict which frozen MAS workflows this identity can invoke.
    """
    def __init__(self, *, resource, issuer, introspection_url, client_id,
                 client_secret_file, timeout_seconds=5, transport=None, transport_policy=None):
        self.resource = _https_url(resource)
        self.issuer = _https_url(issuer)
        self.introspection_url = _https_url(introspection_url)
        if type(client_id) is not str or not client_id or len(client_id) > 256:
            raise AuthenticationError("invalid_verifier_configuration")
        self.client_id = client_id
        self.client_secret_file = client_secret_file
        if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 30:
            raise AuthenticationError("invalid_verifier_configuration")
        self.timeout_seconds = timeout_seconds
        self._transport = transport
        if transport_policy is not None:
            from .http_policy import HTTPTransportPolicy
            if not isinstance(transport_policy, HTTPTransportPolicy):
                raise AuthenticationError('invalid_verifier_configuration')
        self.transport_policy = transport_policy

    def metadata(self):
        return {"resource": self.resource, "authorization_servers": [self.issuer],
                "scopes_supported": sorted(SCOPES), "bearer_methods_supported": ["header"]}

    def verify(self, token, required_scopes=()):
        validate_token(token)
        # Introspection client credentials are separate from the inbound bearer.
        secret = read_client_secret(self.client_secret_file)
        basic = base64.b64encode((urlencode({"": self.client_id})[1:] + ":"
                                 + urlencode({"": secret})[1:]).encode()).decode()
        body = urlencode({"token": token, "token_type_hint": "access_token"}).encode()
        try:
            if self._transport is None:
                from .service_client import http_exchange
                exchange = http_exchange
            else:
                exchange = self._transport
            options = {'transport_policy': self.transport_policy} if self.transport_policy is not None else {}
            status, _headers, payload = exchange(
                self.introspection_url, method="POST", body=body,
                headers={"Authorization": "Basic " + basic,
                         "Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
                timeout_seconds=self.timeout_seconds, max_response_bytes=65536, **options)
            if status != 200:
                raise AuthenticationError("authentication_unavailable")
            data = json.loads(payload, object_pairs_hook=_unique,
                              parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))
        except AuthenticationError:
            raise
        except Exception:
            raise AuthenticationError("authentication_unavailable") from None
        if type(data) is not dict or data.get("active") is not True:
            raise AuthenticationError()
        audience = data.get("aud")
        if type(audience) is str:
            audience = [audience]
        if type(audience) is not list or self.resource not in audience:
            raise AuthenticationError()
        # The endpoint is pinned to one issuer. If it supplies iss, it must agree.
        if "iss" in data and data["iss"] != self.issuer:
            raise AuthenticationError()
        now = time.time()
        if ("exp" in data and (type(data["exp"]) not in (int, float) or data["exp"] <= now)
                or "nbf" in data and (type(data["nbf"]) not in (int, float) or data["nbf"] > now)):
            raise AuthenticationError()
        if type(data.get("token_type", "Bearer")) is not str or data.get("token_type", "Bearer").lower() != "bearer":
            raise AuthenticationError()
        scope = data.get("scope", "")
        if type(scope) is not str:
            raise AuthenticationError()
        principal = _principal(data.get("sub"))
        return AuthenticatedPrincipal(principal, frozenset(scope.split())).require(required_scopes)
