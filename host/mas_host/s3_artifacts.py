"""Immutable S3 content with durable local reservations before remote publication.

Authorisation, reference publication and retention remain in ArtifactStore's
local metadata transaction. This backend distributes bytes, not writer authority.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
import os
import re
from tempfile import TemporaryFile
import uuid

from .artifact_content import verify_stream
from .artifacts import (ArtifactError, _DIGEST, _MAX_INDEX, _atomic_json,
                        _hash_source, _load_json, _open_file, _read_file)
from .s3_transport import S3ContentConfig, create_s3_client


_MANIFEST = "object-content.json"


class S3ArtifactContent:
    """Trusted optional strategy; all keys are derived from retained metadata.

The optional client is a trusted test/deployment dependency, never request data.
A namespace reserves each content identity once, including uncertain uploads.
There is no multipart upload, unbounded listing or automatic object deletion.
"""

    def __init__(self, config: S3ContentConfig, *, client=None):
        if not isinstance(config, S3ContentConfig):
            raise ArtifactError("invalid S3 content configuration")
        self.config, self._supplied_client = config, client

    @property
    def descriptor(self):
        return self.config.descriptor

    def _manifest(self, directory):
        value = _load_json(_read_file(directory, _MANIFEST, _MAX_INDEX))
        if (type(value) is not dict or set(value) != {"format", "backend", "store_id", "objects"}
                or value["format"] != "mas-object-reservations/1" or value["backend"] != self.descriptor
                or type(value["store_id"]) is not str or len(value["store_id"]) != 32
                or any(character not in "0123456789abcdef" for character in value["store_id"])
                or type(value["objects"]) is not dict or len(value["objects"]) > 4096):
            raise ArtifactError("artefact content backend changed or metadata is invalid")
        for digest, item in value["objects"].items():
            if (type(digest) is not str or not _DIGEST.fullmatch(digest) or type(item) is not dict or set(item) != {"size", "state"}
                    or type(item["size"]) is not int or not 0 <= item["size"] <= 5 * 1024**3
                    or item["state"] not in {"reserved", "verified"}):
                raise ArtifactError("invalid artefact object reservation")
        return value

    def check(self, directory, index, policy, *, initialise=False):
        if policy.max_artifact_bytes > 5 * 1024**3:
            raise ArtifactError("S3 content exceeds single-object upload bound")
        names = os.listdir(directory)
        for name in set(names) - {"index.json", _MANIFEST}:
            if not re.fullmatch(r"\.pending-[0-9a-f]{32}", name):
                raise ArtifactError("artefact content backend changed or storage is invalid")
            with os.fdopen(_open_file(directory, name), "rb") as orphan:
                if os.fstat(orphan.fileno()).st_size > _MAX_INDEX:
                    raise ArtifactError("invalid artefact metadata staging object")
            # Only atomic metadata writes use this directory. Holding the
            # exclusive initialisation lock establishes that no writer owns it.
            if initialise:
                os.unlink(name, dir_fd=directory)
        if initialise:
            os.fsync(directory)
        if _MANIFEST not in names:
            if not initialise or index["artifacts"] or index["links"]:
                raise ArtifactError("artefact object metadata is missing")
            _atomic_json(directory, _MANIFEST, {"format": "mas-object-reservations/1",
                "backend": self.descriptor, "store_id": uuid.uuid4().hex, "objects": {}})
        value = self._manifest(directory)
        if (len(value["objects"]) > policy.max_artifacts
                or sum(item["size"] for item in value["objects"].values()) > policy.max_total_bytes
                or any(item["size"] > policy.max_artifact_bytes for item in value["objects"].values())):
            raise ArtifactError("artefact object reservations exceed quota")
        for item in index["artifacts"].values():
            ref = item["reference"]
            if value["objects"].get(ref["sha256"]) != {"size": ref["size"], "state": "verified"}:
                raise ArtifactError("artefact published without verified object reservation")

    def _key(self, manifest, reference):
        return f"{self.config.prefix}/{manifest['store_id']}/{reference.namespace}/{reference.sha256}.blob"

    @contextmanager
    def _client(self):
        client = self._supplied_client if self._supplied_client is not None else create_s3_client(self.config)
        try:
            yield client
        except ArtifactError:
            raise
        except Exception:
            # SDK exception bodies may include endpoints, headers or application data.
            raise ArtifactError("S3 artefact content is unavailable") from None
        finally:
            if self._supplied_client is None:
                try:
                    client.close()
                except Exception:
                    raise ArtifactError("S3 artefact transport cleanup failed") from None

    def publish(self, directory, reference, source, policy):
        manifest = self._manifest(directory)
        current = manifest["objects"].get(reference.sha256)
        if current is not None and current["size"] != reference.size:
            raise ArtifactError("artefact object reservation conflicts")
        if current is None:
            if len(manifest["objects"]) >= policy.max_artifacts:
                raise ArtifactError("artefact object-count quota exhausted")
            if sum(item["size"] for item in manifest["objects"].values()) + reference.size > policy.max_total_bytes:
                raise ArtifactError("artefact total-byte quota exhausted")
            manifest["objects"][reference.sha256] = {"size": reference.size, "state": "reserved"}
            # This intent precedes PUT. A crash/lost reply cannot hide used quota.
            _atomic_json(directory, _MANIFEST, manifest)
        self.publish_object(manifest['store_id'], reference, source, policy)
        manifest["objects"][reference.sha256]["state"] = "verified"
        _atomic_json(directory, _MANIFEST, manifest)

    def publish_object(self, store_id, reference, source, policy):
        """Publish bytes after an authoritative owner has reserved this object.

        The shared PostgreSQL repository owns reservations in HA mode; local
        mode owns them in the manifest above. This method grants no read access.
        """
        if type(store_id) is not str or re.fullmatch('[0-9a-f]{32}', store_id) is None:
            raise ArtifactError('invalid object-store namespace identity')
        manifest = {'store_id': store_id}
        with TemporaryFile(mode="w+b") as snapshot:
            if _hash_source(source, policy.max_artifact_bytes, destination=snapshot) != (reference.size, reference.sha256):
                raise ArtifactError("artefact source changed during publication")
            snapshot.seek(0)
            with self._client() as client:
                try:
                    client.put_object(Bucket=self.config.bucket, Key=self._key(manifest, reference),
                        Body=snapshot, ContentLength=reference.size, IfNoneMatch="*",
                        ChecksumSHA256=base64.b64encode(bytes.fromhex(reference.sha256)).decode("ascii"))
                except Exception as error:
                    response = getattr(error, "response", {})
                    # An existing key is usable only if its actual bytes verify.
                    if type(response) is not dict or response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                        raise
                self._verify(client, manifest, reference)

    def verify_object(self, store_id, reference, *, collect=False, destination=None):
        """Verify a shared owner's authorised reference without local metadata."""
        if type(store_id) is not str or re.fullmatch('[0-9a-f]{32}', store_id) is None:
            raise ArtifactError('invalid object-store namespace identity')
        with self._client() as client:
            return self._verify(client, {'store_id': store_id}, reference,
                                collect=collect, destination=destination)

    def _verify(self, client, manifest, reference, *, collect=False, destination=None):
        response = client.get_object(Bucket=self.config.bucket, Key=self._key(manifest, reference))
        source = response.get("Body")
        if source is None:
            raise ArtifactError("artefact content integrity mismatch")
        try:
            if type(response.get("ContentLength")) is not int or response["ContentLength"] != reference.size:
                raise ArtifactError("artefact content integrity mismatch")
            return verify_stream(source, reference, collect=collect, destination=destination)
        finally:
            source.close()

    def verify(self, directory, reference, *, collect=False, destination=None):
        manifest = self._manifest(directory)
        if manifest["objects"].get(reference.sha256) != {"size": reference.size, "state": "verified"}:
            raise ArtifactError("artefact object is not published")
        with self._client() as client:
            return self._verify(client, manifest, reference, collect=collect, destination=destination)
