"""Resource observations preserve namespace identity and detect owned orphans."""
import os
from pathlib import Path
import tempfile
import unittest

from mas_host.reference_resources import ProcessTreeSampler, distribution, process_stat, storage_sample


def proc_stat(pid, parent, start, rss=3, threads=1):
    fields = ['0'] * 22
    fields[0], fields[1], fields[17], fields[19], fields[21] = 'S', str(parent), str(threads), str(start), str(rss)
    return f'{pid} (name with ) parentheses) ' + ' '.join(fields)


class ReferenceResourcesTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def write_process(self, pid, parent, start, **kwargs):
        path = self.root / str(pid)
        path.mkdir(exist_ok=True)
        (path / 'stat').write_text(proc_stat(pid, parent, start, **kwargs))

    def test_proc_names_with_parentheses_and_negative_rss(self):
        self.assertEqual(process_stat(proc_stat(70, 2, 99, rss=-1, threads=4)),
                         {'pid': 70, 'ppid': 2, 'start_ticks': 99, 'rss_pages': 0, 'threads': 4})

    def test_visible_pid_identity_orphans_and_pid_reuse(self):
        # The /proc-visible root is independent from os.getpid() in a nested namespace.
        root_pid = os.getpid() + 10000
        (self.root / 'self').mkdir()
        (self.root / 'self/stat').write_text(proc_stat(root_pid, 1, 10))
        self.write_process(root_pid, 1, 10)
        self.write_process(80, root_pid, 11, rss=4, threads=2)
        self.write_process(81, 80, 12, rss=5)
        self.write_process(90, 1, 99, rss=100)
        sampler = ProcessTreeSampler(self.root)
        first = sampler.sample()
        self.assertEqual(first['descendant_processes'], 2)
        self.assertEqual(first['descendant_threads'], 3)
        self.assertEqual(first['descendant_rss_bytes'], 9 * sampler.page_size)
        self.write_process(80, 1, 11, rss=4, threads=2)
        self.assertEqual(sampler.sample()['observed_orphans'], 2)
        self.write_process(80, 1, 999, rss=100)
        (self.root / '81/stat').unlink()
        self.assertEqual(sampler.sample()['descendant_processes'], 0)
        self.assertEqual(sampler.sample()['distinct_descendants_observed'], 2)

    def test_observer_identity_change_is_not_silent(self):
        (self.root / 'self').mkdir()
        (self.root / 'self/stat').write_text(proc_stat(70, 1, 10))
        self.write_process(70, 1, 10)
        sampler = ProcessTreeSampler(self.root)
        self.write_process(70, 1, 20)
        with self.assertRaisesRegex(RuntimeError, 'identity unavailable'):
            sampler.sample()

    def test_storage_counts_nested_regular_files_and_rejects_symlink(self):
        (self.root / 'nested').mkdir()
        (self.root / 'one').write_bytes(b'abc')
        (self.root / 'nested/two').write_bytes(b'abcd')
        result = storage_sample(self.root)
        self.assertEqual(result['files'], 2)
        self.assertEqual(result['apparent_bytes'], 7)
        self.assertGreaterEqual(result['allocated_bytes'], 7)
        (self.root / 'link').symlink_to(self.root / 'one')
        with self.assertRaisesRegex(RuntimeError, 'unsafe file'):
            storage_sample(self.root)

    def test_quantiles_are_nearest_rank_and_p99_has_sample_count(self):
        result = distribution(list(range(1, 101)))
        self.assertEqual((result['p50'], result['p95'], result['p99'], result['samples']), (50, 95, 99, 100))
        self.assertIsNone(distribution([])['p99'])
        self.assertEqual(distribution([4])['p99'], 4)


if __name__ == '__main__':
    unittest.main()
