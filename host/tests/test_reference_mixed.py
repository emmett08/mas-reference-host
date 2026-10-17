"""Evidence checks must reject misleading progress/latency observations."""
import unittest

from mas_host.reference_mixed import latency_groups, progress_during_pause


class MixedEvidenceTests(unittest.TestCase):
    def test_late_poll_cannot_make_prior_callback_count_as_progress_during_pause(self):
        jobs = {'paused': {'paused_at': 10, 'resumed_at': 13},
                'short': {'completed': 12}}
        records = [{'kind': 'end', 'key': 'short', 'time_ns': 9_000_000_000}]
        self.assertFalse(progress_during_pause(records, jobs))
        records.append({'kind': 'end', 'key': 'short', 'time_ns': 11_000_000_000})
        self.assertTrue(progress_during_pause(records, jobs))

    def test_pause_without_a_continuation_and_its_own_callback_do_not_count(self):
        records = [{'kind': 'end', 'key': 'paused', 'time_ns': 11_000_000_000}]
        self.assertFalse(progress_during_pause(records, {'paused': {'paused_at': 10}}))
        self.assertFalse(progress_during_pause(records, {'paused': {'paused_at': 10, 'resumed_at': 12}}))

    def test_groups_exclude_unfinished_tasks_and_preserve_empty_class(self):
        jobs = {'one': {'principal': 'alice', 'class': 'short', 'submitted': 1, 'completed': 2},
                'two': {'principal': 'bob', 'class': 'short', 'submitted': 2, 'completed': 6},
                'three': {'principal': 'bob', 'class': 'paused', 'submitted': 3}}
        result = latency_groups(jobs)
        self.assertEqual(result['principal']['alice']['p99'], 1)
        self.assertEqual(result['principal']['bob']['samples'], 1)
        self.assertEqual(result['class']['short']['p99'], 4)
        self.assertEqual(result['class']['paused']['samples'], 0)
        self.assertIsNone(result['class']['paused']['p99'])


if __name__ == '__main__':
    unittest.main()
