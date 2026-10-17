"""Versioned administrator-owned peer bindings and revocation tombstones."""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import json
import re
import time

from .codec import canonical_json, integer_json
from .operations import _file_lock
from .schema import Schema, connect
from .service_contracts import ServiceError


PEERS = Schema("federation peer registry", 0x4D415046, 1, (), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE peers(name TEXT NOT NULL,version TEXT NOT NULL,identity TEXT UNIQUE NOT NULL,descriptor TEXT NOT NULL,revoked INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(name,version))",
    "CREATE TABLE aliases(name TEXT PRIMARY KEY,version TEXT NOT NULL)",
    "CREATE TABLE changes(revision INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,digest TEXT NOT NULL,operator TEXT NOT NULL,reason TEXT NOT NULL,command TEXT NOT NULL,created INTEGER NOT NULL)",
))
PROFILES = frozenset({"mas-http/1", "mas-mcp/1", "mas-a2a/1"})
CAPABILITIES = frozenset({"submit", "inspect", "result", "cancel", "update", "pinned_identity"})


def identifier(value):
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}", value):
        raise ServiceError("invalid_peer_registration")
    return value


@dataclass(frozen=True, init=False)
class PeerRegistration:
    """Public binding; credentials are supplied independently by trusted startup."""
    _encoded: str

    def __init__(self, *, name, version, profile, endpoint, outbound_principal,
                 deployment_id, workflows, callers, capabilities=("submit", "inspect", "pinned_identity"),
                 artifact_scopes=(), allow_loopback_http=False, transport_policy=None, timeout_seconds=30):
        from .bridges.service_client import validate_endpoint
        from .bridges.http_policy import HTTPTransportPolicy
        for value in (name, version, outbound_principal, deployment_id):
            identifier(value)
        parsed = validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
        expected_paths = {"mas-http/1": ("", "/", "/service"), "mas-mcp/1": ("/mcp",), "mas-a2a/1": ("/a2a",)}
        if (type(profile) is not str or profile not in PROFILES or parsed.path not in expected_paths[profile]
                or type(allow_loopback_http) is not bool or type(workflows) is not dict or not workflows or len(workflows) > 128
                or any(type(name) is not str or not name or len(name) > 128
                       or type(value) is not str or not re.fullmatch(r"[0-9a-f]{64}", value) for name, value in workflows.items())
                or type(callers) not in (tuple, list) or not callers or len(callers) > 256
                or type(capabilities) not in (tuple, list) or any(type(item) is not str for item in capabilities)
                or not set(capabilities) <= CAPABILITIES
                or type(artifact_scopes) not in (tuple, list) or len(artifact_scopes) > 64
                or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60):
            raise ServiceError("invalid_peer_registration")
        for value in (*callers, *artifact_scopes):
            identifier(value)
        value = {"format": "mas-peer-registration/1", "name": name, "version": version,
            "profile": profile, "endpoint": endpoint.rstrip("/").removesuffix("/service") if profile == "mas-http/1" else endpoint,
            "outbound_principal": outbound_principal, "deployment_id": deployment_id,
            "workflows": workflows, "callers": sorted(set(callers)),
            "capabilities": sorted(set(capabilities) | {"pinned_identity"}),
            "artifact_scopes": sorted(set(artifact_scopes)), "allow_loopback_http": allow_loopback_http}
        if transport_policy is not None:
            value['transport_policy'] = HTTPTransportPolicy.validate_descriptor(transport_policy)
        if timeout_seconds != 30:
            value['timeout_seconds'] = timeout_seconds
        object.__setattr__(self, "_encoded", integer_json(value, 32768))

    @property
    def descriptor(self):
        return json.loads(self._encoded)

    @property
    def identity(self):
        return hashlib.sha256(self._encoded.encode()).hexdigest()

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or value.get("format") != "mas-peer-registration/1":
            raise ServiceError("invalid_peer_registration")
        try:
            result = cls(**{key: item for key, item in value.items() if key != "format"})
        except (TypeError, ValueError):
            raise ServiceError("invalid_peer_registration") from None
        if result.descriptor != value:
            raise ServiceError("invalid_peer_registration")
        return result


class PeerRegistry:
    """Trusted administrative writes; callers are supplied by authenticated code.

    Version and request-key tombstones are retained. Revocation closes future
    authorisation checks; a call already authorised can still finish remotely.
    It does not change remote state, cancel effects or pool independent budgets.
    """
    def __init__(self, operations):
        self.operations = operations
        self.path = operations.state_dir / "peer-registry.sqlite"

    @contextmanager
    def _open(self, *, write=False):
        with _file_lock(self.operations.state_dir / ".deployment.lock", False):
            if write and (self.operations.readonly or not self.operations.admission_enabled):
                raise ServiceError("audit_only")
            with closing(connect(self.path, PEERS, readonly=not write,
                    initialise=(lambda db: db.execute("INSERT INTO metadata VALUES('deployment_id',?)", (self.operations.deployment_id,))) if write else None)) as db:
                if dict(db.execute("SELECT key,value FROM metadata")) != {"deployment_id": self.operations.deployment_id}:
                    raise ServiceError("deployment_mismatch")
                if write:
                    db.execute("BEGIN IMMEDIATE")
                try:
                    yield db
                    if write:
                        db.commit()
                except BaseException:
                    db.rollback()
                    raise

    def status(self):
        if not self.path.exists() and not self.path.is_symlink():
            return {"format": "mas-peer-registry/1", "revision": 0, "peers": []}
        with self._open() as db:
            return {"format": "mas-peer-registry/1", "revision": db.execute("SELECT coalesce(max(revision),0) FROM changes").fetchone()[0],
                "peers": [{"identity": row["identity"], "revoked": bool(row["revoked"]),
                           "current": bool(row["current"]), "registration": json.loads(row["descriptor"])}
                          for row in db.execute("SELECT p.*,a.version=p.version AS current FROM peers p LEFT JOIN aliases a USING(name) ORDER BY name,version")]}

    def resolve(self, name, caller, *, version=None, identity=None):
        identifier(name); identifier(caller)
        if not self.path.exists():
            raise ServiceError("peer_unavailable")
        with self._open() as db:
            if version is None:
                alias = db.execute("SELECT version FROM aliases WHERE name=?", (name,)).fetchone()
                version = alias[0] if alias else None
            row = db.execute("SELECT * FROM peers WHERE name=? AND version=?", (name, version)).fetchone()
            if row is None or row["revoked"]:
                raise ServiceError("peer_unavailable")
            result = PeerRegistration.from_dict(json.loads(row["descriptor"]))
            if result.identity != row["identity"] or identity is not None and result.identity != identity:
                raise ServiceError("peer_binding_changed")
            if caller not in result.descriptor["callers"]:
                raise ServiceError("peer_forbidden")
            return result

    def register(self, *, operator, request_key, reason, expected_revision, registration):
        if not isinstance(registration, PeerRegistration):
            registration = PeerRegistration.from_dict(registration)
        return self._change(operator, request_key, reason, expected_revision,
                            {"action": "register", "registration": registration.descriptor})

    def revoke(self, *, operator, request_key, reason, expected_revision, name, version):
        identifier(name); identifier(version)
        return self._change(operator, request_key, reason, expected_revision,
                            {"action": "revoke", "name": name, "version": version})

    def _change(self, operator, request_key, reason, expected_revision, command):
        identifier(operator); identifier(request_key)
        if (type(reason) is not str or not reason.strip() or len(reason) > 2048
                or any(ord(character) < 32 for character in reason)
                or type(expected_revision) is not int or expected_revision < 0):
            raise ServiceError("invalid_peer_change")
        encoded = canonical_json(command)
        digest = hashlib.sha256(canonical_json({"operator": operator, "reason": reason,
            "expected_revision": expected_revision, "command": command}).encode()).hexdigest()
        with self._open(write=True) as db:
            old = db.execute("SELECT * FROM changes WHERE request_key=?", (request_key,)).fetchone()
            if old is not None:
                if old["digest"] != digest:
                    raise ServiceError("peer_change_conflict")
                return {"revision": old["revision"], "request_key": request_key, "command": json.loads(old["command"])}
            revision = db.execute("SELECT coalesce(max(revision),0) FROM changes").fetchone()[0]
            if revision != expected_revision:
                raise ServiceError("peer_revision_conflict")
            if revision >= 10000:
                raise ServiceError("peer_registry_full")
            self.operations.check_storage_admission()
            if command["action"] == "register":
                registration = PeerRegistration.from_dict(command["registration"])
                value = registration.descriptor
                old = db.execute("SELECT identity,revoked FROM peers WHERE name=? AND version=?", (value["name"], value["version"])).fetchone()
                if old is not None and (old["identity"] != registration.identity or old["revoked"]):
                    raise ServiceError("peer_version_immutable")
                if old is None:
                    db.execute("INSERT INTO peers VALUES(?,?,?,?,0)", (value["name"], value["version"], registration.identity, registration._encoded))
                db.execute("INSERT OR REPLACE INTO aliases VALUES(?,?)", (value["name"], value["version"]))
            else:
                if db.execute("SELECT 1 FROM peers WHERE name=? AND version=?", (command["name"], command["version"])).fetchone() is None:
                    raise ServiceError("peer_unavailable")
                db.execute("UPDATE peers SET revoked=1 WHERE name=? AND version=?", (command["name"], command["version"]))
            db.execute("INSERT INTO changes VALUES(?,?,?,?,?,?,?)", (revision + 1, request_key, digest, operator,
                reason, encoded, time.time_ns() // 1_000_000))
            return {"revision": revision + 1, "request_key": request_key, "command": command}
