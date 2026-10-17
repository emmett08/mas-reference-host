"""Coverage and failure propagation for the isolated PostgreSQL CI shards."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "run-unittest-shard.py"
SPEC = importlib.util.spec_from_file_location("run_unittest_shard", SOURCE)
SHARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SHARD)


class UnittestShardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def fixture(self, source, name="test_fixture.py"):
        (self.directory / name).write_text(textwrap.dedent(source))

    def run_shard(self, index=0, count=4, *, listing=False):
        return subprocess.run([sys.executable, str(SOURCE), "-s", str(self.directory),
            "--shard-index", str(index), "--shard-count", str(count),
            *(["--list"] if listing else [])], capture_output=True, text=True, timeout=10)

    def test_sorted_partitions_are_disjoint_exhaustive_and_deterministic(self):
        self.fixture("import unittest\nclass Cases(unittest.TestCase):\n" + "\n".join(
            f"    def test_{index:02}(self): pass" for index in reversed(range(13))))
        manifests = []
        for index in range(4):
            result = self.run_shard(index, listing=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            manifest = json.loads(result.stdout)
            self.assertEqual(manifest["discovered"], 13)
            self.assertEqual(manifest["shard_index"], index)
            self.assertEqual(manifest["shard_count"], 4)
            manifests.append(manifest["selected"])
            self.assertEqual(result.stdout, self.run_shard(index, listing=True).stdout)
        expected = [f"test_fixture.Cases.test_{index:02}" for index in range(13)]
        self.assertEqual(manifests, [expected[index::4] for index in range(4)])
        selected = [test for manifest in manifests for test in manifest]
        self.assertEqual(len(selected), len(set(selected)))
        self.assertEqual(sorted(selected), expected)

    def test_invalid_indices_counts_and_empty_shards_are_rejected(self):
        for index, count, tests in [(0, 0, [1]), (-1, 4, list(range(4))),
                (4, 4, list(range(4))), (0, 1, []), (0, 4, [1, 2, 3]), (3, 4, [1, 2, 3])]:
            with self.subTest(index=index, count=count, tests=tests), self.assertRaises(ValueError):
                SHARD.select(tests, index, count)

    def test_import_failure_cannot_disappear_into_an_unselected_shard(self):
        self.fixture("raise ImportError('deliberate discovery failure')")
        for index in range(4):
            with self.subTest(index=index):
                result = self.run_shard(index, listing=True)
                self.assertEqual(result.returncode, 2)
                self.assertIn("test discovery failed", result.stderr)
                self.assertIn("deliberate discovery failure", result.stderr)
                self.assertEqual(result.stdout, "")

    def test_duplicate_ids_from_discovery_hooks_are_rejected(self):
        self.fixture("""
            import unittest
            class Cases(unittest.TestCase):
                def test_one(self): pass
            def load_tests(loader, tests, pattern):
                return unittest.TestSuite([Cases('test_one'), Cases('test_one')])
        """)
        result = self.run_shard(count=1, listing=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("duplicate test IDs", result.stderr)

    def test_load_tests_and_standard_fixtures_keep_result_semantics(self):
        self.fixture("""
            from pathlib import Path
            import unittest
            def record(value):
                with Path(__file__).with_suffix('.events').open('a') as stream:
                    stream.write(value + '\\n')
            def setUpModule(): record('module setup')
            def tearDownModule(): record('module teardown')
            class Cases(unittest.TestCase):
                @classmethod
                def setUpClass(cls): record('class setup')
                @classmethod
                def tearDownClass(cls): record('class teardown')
                def setUp(self):
                    self.addCleanup(record, 'test cleanup')
                @unittest.expectedFailure
                def exercise_expected(self): self.fail('expected')
                def exercise_ok(self): record('test body')
                @unittest.skip('fixture skip')
                def exercise_skip(self): self.fail('must not run')
                def test_not_selected_by_hook(self): self.fail('hook was bypassed')
            def load_tests(loader, tests, pattern):
                return unittest.TestSuite(Cases(name) for name in
                    ['exercise_skip', 'exercise_ok', 'exercise_expected'])
        """)
        first = self.run_shard(0, 2)
        second = self.run_shard(1, 2)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("expected failure", first.stderr)
        self.assertIn("skipped 'fixture skip'", first.stderr)
        self.assertIn("Ran 2 tests", first.stderr)
        self.assertIn("Ran 1 test", second.stderr)
        self.assertEqual((self.directory / 'test_fixture.events').read_text().splitlines(),
            ['module setup', 'class setup', 'test cleanup', 'class teardown', 'module teardown',
             'module setup', 'class setup', 'test body', 'test cleanup', 'class teardown', 'module teardown'])

    def test_list_does_not_run_tests_and_failure_exits_unsuccessfully(self):
        self.fixture("""
            from pathlib import Path
            import unittest
            class Cases(unittest.TestCase):
                def test_failure(self):
                    Path(__file__).with_suffix('.ran').touch()
                    self.fail('deliberate assertion failure')
        """)
        self.assertEqual(self.run_shard(count=1, listing=True).returncode, 0)
        self.assertFalse((self.directory / 'test_fixture.ran').exists())
        result = self.run_shard(count=1)
        self.assertEqual(result.returncode, 1)
        self.assertIn("deliberate assertion failure", result.stderr)
        self.assertTrue((self.directory / 'test_fixture.ran').exists())


if __name__ == "__main__":
    unittest.main()
