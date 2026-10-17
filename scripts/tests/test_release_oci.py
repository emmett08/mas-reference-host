"""Archive integrity and exact imported-image binding, without a Docker daemon."""
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("release_oci", Path(__file__).resolve().parents[1] / "release-oci.py")
oci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(oci)


def encoded(value):
    return json.dumps(value, sort_keys=True).encode()


def archive_fixture(path, *, architecture="amd64", compression="gzip", legacy=False,
                    provenance=True, duplicate=False, bad_diff=False, bad_blob=False,
                    bad_subject=False):
    files = {}

    def blob(value, media):
        value = value if isinstance(value, bytes) else encoded(value)
        sha = hashlib.sha256(value).hexdigest()
        files["blobs/sha256/" + sha] = value
        return {"mediaType": media, "digest": "sha256:" + sha, "size": len(value)}

    layer_bytes = io.BytesIO()
    with tarfile.open(fileobj=layer_bytes, mode="w") as layer:
        member = tarfile.TarInfo("test-file")
        member.size = 7
        layer.addfile(member, io.BytesIO(b"fixture"))
    layer_bytes = layer_bytes.getvalue()
    diff_id = "sha256:" + hashlib.sha256(layer_bytes).hexdigest()
    config = blob({"os": "linux", "architecture": architecture,
                   "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "0" * 64 if bad_diff else diff_id]},
                   "config": {"Cmd": ["/bin/true"]}}, oci.CONFIG)
    layer = blob(gzip.compress(layer_bytes, mtime=0) if compression == "gzip" else layer_bytes,
                 oci.LAYER + ("+" + compression if compression != "none" else ""))
    if bad_blob:
        name = "blobs/sha256/" + layer["digest"].split(":")[1]
        files[name] = b"x" * len(files[name])
    runtime = blob({"schemaVersion": 2, "mediaType": oci.MANIFEST,
                    "config": config, "layers": [layer]}, oci.MANIFEST)
    runtime["platform"] = {"os": "linux", "architecture": architecture}
    descriptors = [runtime, runtime] if duplicate else [runtime]
    if provenance:
        subject = "0" * 64 if bad_subject else runtime["digest"].split(":")[1]
        statement = blob({"_type": "https://in-toto.io/Statement/v1",
                          "predicateType": "https://slsa.dev/provenance/v1", "predicate": {},
                          "subject": [{"name": "runtime", "digest": {"sha256": subject}}]}, oci.INTOTO)
        attestation = {"schemaVersion": 2, "mediaType": oci.MANIFEST,
                       "config": blob({}, "application/vnd.oci.empty.v1+json"), "layers": [statement]}
        if not legacy:
            attestation.update(artifactType="application/vnd.docker.attestation.manifest.v1+json", subject=runtime)
        attestation_descriptor = blob(attestation, oci.MANIFEST)
        attestation_descriptor.update(platform={"os": "unknown", "architecture": "unknown"},
                                      annotations={"vnd.docker.reference.type": "attestation-manifest",
                                                   "vnd.docker.reference.digest": runtime["digest"]})
        descriptors.append(attestation_descriptor)
    nested = blob({"schemaVersion": 2, "mediaType": oci.INDEX, "manifests": descriptors}, oci.INDEX)
    files["index.json"] = encoded({"schemaVersion": 2, "mediaType": oci.INDEX, "manifests": [nested]})
    files["oci-layout"] = encoded({"imageLayoutVersion": "1.0.0"})
    with tarfile.open(path, "w") as archive:
        for name, data in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return {"Id": config["digest"], "Os": "linux", "Architecture": architecture,
            "RootFS": {"Type": "layers", "Layers": [diff_id]}}


class ReleaseOciTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.archive = Path(self.temporary.name) / "candidate.oci.tar"

    def test_nested_provenance_index_binds_imported_config_and_layers_for_both_architectures(self):
        for architecture in ("amd64", "arm64"):
            with self.subTest(architecture=architecture):
                image = archive_fixture(self.archive, architecture=architecture)
                before = self.archive.read_bytes()
                binding = oci.inspect_archive(self.archive, architecture)
                checked = oci.verify_import(self.archive, binding, image)
                self.assertTrue(checked["docker_identity_verified"])
                self.assertTrue(checked["provenance"])
                self.assertEqual(checked["config_digest"], image["Id"])
                self.assertEqual(self.archive.read_bytes(), before)

    def test_legacy_attestation_and_uncompressed_layer_supported(self):
        image = archive_fixture(self.archive, legacy=True, compression="none")
        binding = oci.inspect_archive(self.archive, "amd64")
        self.assertTrue(oci.verify_import(self.archive, binding, image)["docker_identity_verified"])

    def test_different_config_or_root_filesystem_cannot_be_accepted(self):
        image = archive_fixture(self.archive)
        binding = oci.inspect_archive(self.archive, "amd64")
        for changed in ({**image, "Id": "sha256:" + "0" * 64},
                        {**image, "RootFS": {"Type": "layers", "Layers": ["sha256:" + "0" * 64]}},
                        {**image, "Architecture": "arm64"}):
            with self.subTest(image=changed), self.assertRaisesRegex(ValueError, "does not match"):
                oci.verify_import(self.archive, binding, changed)

    def test_changed_archive_cannot_be_accepted_after_import(self):
        image = archive_fixture(self.archive)
        binding = oci.inspect_archive(self.archive, "amd64")
        with self.archive.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(ValueError, "archive changed"):
            oci.verify_import(self.archive, binding, image)

    def test_archive_integrity_and_provenance_fail_closed(self):
        cases = [({"bad_blob": True}, "blob digest"),
                 ({"bad_diff": True}, "uncompressed diff ID"),
                 ({"bad_subject": True}, "provenance statement"),
                 ({"provenance": False}, "no matching build provenance"),
                 ({"duplicate": True}, "exactly one matching runtime"),
                 ({"compression": "zstd"}, "unsupported OCI layer compression")]
        for options, message in cases:
            with self.subTest(options=options):
                archive_fixture(self.archive, **options)
                with self.assertRaisesRegex(ValueError, message):
                    oci.inspect_archive(self.archive, "amd64")

    def test_wrong_architecture_is_rejected(self):
        archive_fixture(self.archive, architecture="amd64")
        with self.assertRaisesRegex(ValueError, "exactly one matching runtime"):
            oci.inspect_archive(self.archive, "arm64")

    def test_scanner_directory_preserves_archive_bytes_and_selects_exact_runtime_manifest(self):
        for architecture in ("amd64", "arm64"):
            with self.subTest(architecture=architecture):
                image = archive_fixture(self.archive, architecture=architecture)
                before = self.archive.read_bytes()
                binding = oci.verify_import(self.archive, oci.inspect_archive(self.archive, architecture), image)
                directory = self.archive.parent / architecture
                result = oci.materialise_layout(self.archive, binding, directory)
                self.assertTrue(result["scanner_layout_verified"])
                self.assertEqual(result["scanner_input"], str(directory) + "@" + binding["manifest_digest"])
                with tarfile.open(self.archive) as archive:
                    for member in archive.getmembers():
                        source_bytes = archive.extractfile(member).read()
                        if member.name == "index.json":
                            self.assertEqual(result["scanner_original_index_sha256"], "sha256:" + hashlib.sha256(source_bytes).hexdigest())
                        else:
                            self.assertEqual((directory / member.name).read_bytes(), source_bytes)
                manifest = json.loads((directory / "blobs" / "sha256" / binding["manifest_digest"].split(":")[1]).read_text())
                self.assertEqual(manifest["config"]["digest"], image["Id"])
                self.assertEqual(self.archive.read_bytes(), before)

    def test_scanner_digest_reference_resolves_at_root_for_pinned_trivy(self):
        image = archive_fixture(self.archive)
        binding = oci.verify_import(self.archive, oci.inspect_archive(self.archive, "amd64"), image)
        with tarfile.open(self.archive) as archive:
            original = json.load(archive.extractfile("index.json"))
        # Reproduce the rejected reference: Trivy 0.75 compares the supplied
        # digest only with root descriptors before descending into child indexes.
        self.assertNotIn(binding["manifest_digest"], [item["digest"] for item in original["manifests"]])
        directory = self.archive.parent / "scanner"
        result = oci.materialise_layout(self.archive, binding, directory)
        root = json.loads((directory / "index.json").read_text())
        reference = result["scanner_input"].split("@", 1)[1]
        matches = [item for item in root["manifests"] if item["digest"] == reference]
        self.assertEqual(len(root["manifests"]), 1)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["mediaType"], oci.MANIFEST)
        selected = directory / "blobs" / "sha256" / reference.split(":")[1]
        self.assertEqual(matches[0]["size"], selected.stat().st_size)
        self.assertEqual(json.loads(selected.read_text())["config"]["digest"], image["Id"])
        self.assertEqual(result["scanner_layout_files"]["index.json"], oci.file_digest(directory / "index.json"))

    def test_scanner_directory_refuses_existing_destination_unverified_or_changed_archive(self):
        image = archive_fixture(self.archive)
        binding = oci.inspect_archive(self.archive, "amd64")
        directory = self.archive.parent / "layout"
        with self.assertRaisesRegex(ValueError, "unchanged, verified"):
            oci.materialise_layout(self.archive, binding, directory)
        self.assertFalse(directory.exists())
        binding = oci.verify_import(self.archive, binding, image)
        directory.mkdir()
        sentinel = directory / "keep"
        sentinel.write_text("original")
        with self.assertRaises(FileExistsError):
            oci.materialise_layout(self.archive, binding, directory)
        self.assertEqual(sentinel.read_text(), "original")
        with self.archive.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(ValueError, "unchanged, verified"):
            oci.materialise_layout(self.archive, binding, self.archive.parent / "changed")

    def test_scanner_directory_rejects_unsafe_members_before_writing(self):
        for index, (name, kind) in enumerate((("../escape", tarfile.REGTYPE),
                                             ("/absolute", tarfile.REGTYPE),
                                             ("blobs/linked", tarfile.SYMTYPE),
                                             ("blobs/hardlinked", tarfile.LNKTYPE),
                                             ("blobs//alias", tarfile.REGTYPE),
                                             ("./index.json", tarfile.REGTYPE))):
            with self.subTest(name=name):
                image = archive_fixture(self.archive)
                binding = oci.verify_import(self.archive, oci.inspect_archive(self.archive, "amd64"), image)
                with tarfile.open(self.archive, "a") as archive:
                    member = tarfile.TarInfo(name)
                    member.type = kind
                    member.linkname = "../../escape" if kind != tarfile.REGTYPE else ""
                    archive.addfile(member, io.BytesIO())
                binding["archive_sha256"] = oci.file_digest(self.archive)
                directory = self.archive.parent / f"unsafe-{index}"
                with self.assertRaises(ValueError):
                    oci.materialise_layout(self.archive, binding, directory)
                self.assertFalse(directory.exists())


if __name__ == "__main__":
    unittest.main()
