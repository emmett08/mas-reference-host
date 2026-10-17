"""Explicit credential and HTTPS policy for the optional S3 content client."""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import ipaddress
import os
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit

from .artifacts import ArtifactError, _load_json, _open_dir, _open_file


@dataclass(frozen=True)
class S3ContentConfig:
    endpoint: str
    bucket: str
    prefix: str
    region: str
    credentials_file: Path = field(repr=False, compare=False)
    ca_file: Path | None = field(default=None, repr=False)
    connect_timeout: int = 5
    read_timeout: int = 30
    allow_loopback_http: bool = False
    _ca_sha256: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        try:
            parsed = urlsplit(self.endpoint)
            loopback = False
            try:
                loopback = ipaddress.ip_address(parsed.hostname).is_loopback
            except ValueError:
                pass
            if (type(self.endpoint) is not str or any(ord(character) <= 32 for character in self.endpoint) or parsed.username is not None or parsed.password is not None
                    or not parsed.hostname or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                    or parsed.scheme != "https" and not (self.allow_loopback_http and loopback and parsed.scheme == "http")
                    or type(self.allow_loopback_http) is not bool
                    or type(self.bucket) is not str or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket)
                    or ".." in self.bucket
                    or type(self.prefix) is not str or not re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9_/-]{0,255}", self.prefix)
                    or any(part in {"", ".", ".."} for part in self.prefix.split("/"))
                    or type(self.region) is not str or not re.fullmatch(r"[a-z0-9-]{1,64}", self.region)
                    or type(self.connect_timeout) is not int or not 1 <= self.connect_timeout <= 60
                    or type(self.read_timeout) is not int or not 1 <= self.read_timeout <= 300):
                raise ValueError()
            parsed.port  # Validate malformed/out-of-range ports before SDK construction.
            object.__setattr__(self, "endpoint", self.endpoint.rstrip("/"))
            object.__setattr__(self, "credentials_file", Path(self.credentials_file).absolute())
            if self.ca_file is not None:
                path = Path(self.ca_file).absolute()
                data = _private_read(path, private=False)
                object.__setattr__(self, "ca_file", path)
                object.__setattr__(self, "_ca_sha256", hashlib.sha256(data).hexdigest())
        except (ValueError, TypeError, OSError, AttributeError):
            raise ArtifactError("invalid S3 content configuration") from None

    @property
    def descriptor(self):
        # Credential location/value and CA path are not the object-store identity.
        # A rotated credential can access the same binding; a changed CA cannot.
        return {"format": "mas-s3-content/1", "endpoint": self.endpoint,
                "bucket": self.bucket, "prefix": self.prefix, "region": self.region,
                "ca_sha256": self._ca_sha256, "connect_timeout": self.connect_timeout,
                "read_timeout": self.read_timeout, "addressing": "path",
                "retries": 0, "publication": "conditional-put-sha256-get/1"}


def _private_read(path, *, private=True):
    directory = _open_dir(path.parent)
    try:
        descriptor = _open_file(directory, path.name)
    finally:
        os.close(directory)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if private and (stat.S_IMODE(info.st_mode) & 0o077 or info.st_uid != os.getuid()):
            raise ArtifactError("S3 credentials file must be private")
        value = stream.read(65537)
    if not value or len(value) > 65536:
        raise ArtifactError("S3 configuration file is unavailable")
    return value


def create_s3_client(config: S3ContentConfig):
    """Read only the explicitly selected secrets; prohibit ambient proxies/redirects."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise ArtifactError("S3 content requires the object-storage extra") from None
    try:
        credentials = _load_json(_private_read(config.credentials_file))
        if (type(credentials) is not dict or not {"access_key_id", "secret_access_key"} <= set(credentials)
                or set(credentials) - {"access_key_id", "secret_access_key", "session_token"}
                or any(type(value) is not str or not value or len(value) > 8192
                       or any(character.isspace() for character in value) for value in credentials.values())):
            raise ArtifactError("invalid S3 credentials file")
        if config.ca_file is not None and hashlib.sha256(_private_read(config.ca_file, private=False)).hexdigest() != config._ca_sha256:
            raise ArtifactError("S3 CA configuration changed")
        session = boto3.session.Session(aws_access_key_id=credentials["access_key_id"],
            aws_secret_access_key=credentials["secret_access_key"],
            aws_session_token=credentials.get("session_token"), region_name=config.region)
        client = session.client("s3", endpoint_url=config.endpoint, region_name=config.region,
            verify=str(config.ca_file) if config.ca_file else True,
            config=Config(signature_version="s3v4", connect_timeout=config.connect_timeout,
                read_timeout=config.read_timeout, proxies={}, retries={"total_max_attempts": 1},
                s3={"addressing_style": "path"}, request_checksum_calculation="when_required",
                response_checksum_validation="when_required"))
        origin = urlsplit(config.endpoint)
        def permitted(request, **_kwargs):
            observed = urlsplit(request.url)
            if (observed.scheme, observed.netloc) != (origin.scheme, origin.netloc):
                raise ArtifactError("S3 endpoint redirect is forbidden")
        client.meta.events.register("before-send.s3", permitted)
        return client
    except ArtifactError:
        raise
    except Exception:
        raise ArtifactError("S3 credentials or transport configuration unavailable") from None
