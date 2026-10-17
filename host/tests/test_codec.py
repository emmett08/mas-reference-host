"""Stable descriptor identities and explicit bounded JSON contracts."""
import json
import hashlib
import unittest

from mas_host.codec import JsonSizeError, canonical_json, json_identity
from mas_host.contracts import RunnerError


class CanonicalJsonTests(unittest.TestCase):
    def test_existing_unicode_and_escaping_fingerprint_is_preserved(self):
        value = {"z": 'é\\"', "a": {"雪": [True, None, -7]}}
        source = canonical_json(value)
        self.assertEqual(json.loads(source), value)
        self.assertIn("雪", source)
        self.assertEqual(json_identity(value),
                         "c52cbdd520ff0ce5acc2bc8335ebf4a85765c54cff9d7732a1fbca820f688a61")
        self.assertEqual(json_identity(dict(reversed(list(value.items())))), json_identity(value))

    def test_bounds_count_utf8_and_json_escaping_and_keep_codec_errors(self):
        value = {"text": '雪\\"'}
        size = len(canonical_json(value).encode("utf-8"))
        self.assertEqual(json.loads(canonical_json(value, size)), value)
        self.assertEqual(json_identity(value, size), json_identity(value))
        for operation in (canonical_json, json_identity):
            with self.subTest(operation=operation.__name__):
                with self.assertRaises(JsonSizeError):
                    operation(value, size - 1)
                for invalid in ({"number": 1.0}, {1: "coerced key"}, {"text": "\ud800"}):
                    with self.assertRaises(RunnerError):
                        operation(invalid)

    def test_workflow_store_keeps_existing_encoding_and_error_family(self):
        from mas_host.workflow_store import encode

        value = {"z": 'é\\"', "a": {"雪": [True, None, -7]}}
        source = encode(value)
        self.assertIn("雪", source)
        self.assertEqual(hashlib.sha256(source.encode()).hexdigest(),
                         "c52cbdd520ff0ce5acc2bc8335ebf4a85765c54cff9d7732a1fbca820f688a61")
        with self.assertRaises(JsonSizeError):
            encode({"value": "x" * 65536})
        with self.assertRaises(RunnerError):
            encode({"value": 1.5})


if __name__ == "__main__":
    unittest.main()
