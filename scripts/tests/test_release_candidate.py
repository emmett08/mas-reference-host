"""Offline checks of the release tag's immutable source binding."""
import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location("release_candidate", Path(__file__).resolve().parents[1] / "release-candidate.py")
candidate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidate)
SOURCE = "a" * 40
OTHER = "b" * 40


class FakeGitHub:
    def __init__(self, existing=None):
        self.existing = existing
        self.mutations = []

    def request(self, method, path, data=None):
        if method == "GET" and path == f"/git/commits/{SOURCE}":
            return {"sha": SOURCE}
        if method == "GET" and path == "/git/ref/tags/v0.1.0":
            if self.existing is None:
                raise candidate.ApiError(404)
            return {"object": {"type": "commit", "sha": self.existing}}
        if method == "POST" and path == "/git/refs":
            self.mutations.append((method, path, data))
            raise AssertionError("release verification must not create tags")
        raise AssertionError((method, path, data))


class ReleaseCandidateTests(unittest.TestCase):
    def test_release_branch_is_rejected_before_api_access(self):
        with self.assertRaisesRegex(ValueError, "owner-created"):
            candidate.prepare_candidate(None, "refs/heads/release/v0.1.0", SOURCE)

    def test_existing_tag_at_same_source_is_read_only(self):
        api = FakeGitHub(SOURCE)
        self.assertEqual(candidate.prepare_candidate(api, "refs/tags/v0.1.0", SOURCE), ("v0.1.0", SOURCE))
        self.assertEqual(api.mutations, [])

    def test_existing_tag_at_other_source_is_never_rewritten(self):
        api = FakeGitHub(OTHER)
        with self.assertRaisesRegex(ValueError, "different commit"):
            candidate.prepare_candidate(api, "refs/tags/v0.1.0", SOURCE)
        self.assertEqual(api.mutations, [])

    def test_manual_tag_execution_does_not_create_missing_tag(self):
        api = FakeGitHub()
        with self.assertRaisesRegex(ValueError, "existing candidate tag"):
            candidate.prepare_candidate(api, "refs/tags/v0.1.0", SOURCE)
        self.assertEqual(api.mutations, [])

    def test_github_client_rejects_mutations_without_network_access(self):
        api = candidate.GitHub("emmett08/mas", "test-token")
        for method in ("POST", "PATCH", "PUT", "DELETE"):
            with self.subTest(method=method), self.assertRaisesRegex(ValueError, "read-only"):
                api.request(method, "/git/refs", {"ref": "refs/tags/v0.1.0"})

    def test_annotated_tag_is_resolved_to_its_commit(self):
        class Annotated(FakeGitHub):
            def request(self, method, path, data=None):
                if path == "/git/ref/tags/v0.1.0":
                    return {"object": {"type": "tag", "sha": OTHER}}
                if path == f"/git/tags/{OTHER}":
                    return {"object": {"type": "commit", "sha": SOURCE}}
                return super().request(method, path, data)
        api = Annotated()
        self.assertEqual(candidate.prepare_candidate(api, "refs/tags/v0.1.0", SOURCE), ("v0.1.0", SOURCE))
        self.assertEqual(api.mutations, [])

    def test_invalid_versions_and_refs_reject_before_api_access(self):
        for ref in ("refs/heads/main", "refs/heads/release/v0.1", "refs/tags/v1.0.0",
                    "refs/tags/v0.1.0-rc1", "refs/tags/v0.01.0", "refs/tags/v0.1.0/extra"):
            with self.subTest(ref=ref), self.assertRaises(ValueError):
                candidate.prepare_candidate(None, ref, SOURCE)


if __name__ == "__main__":
    unittest.main()
