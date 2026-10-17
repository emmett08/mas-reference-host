"""Qualification checks exercise the real capacity owner and effect evidence."""
import json
from pathlib import Path
import tempfile
import unittest

from mas_host.reference_load import Profile, capacity_checks, trace_metrics


class ReferenceLoadChecksTests(unittest.TestCase):
    def test_actual_capacity_owner_enforces_both_caps_and_fair_fifo(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertTrue(all(capacity_checks(Path(directory) / 'capacity', Profile()).values()))

    def test_duplicate_callback_cannot_pass_even_with_completed_results(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events'
            records = [{'kind': kind, 'key': 'one', 'principal': 'alice', 'time_ns': index * 1000000000}
                       for index, kind in enumerate(('start', 'end', 'start', 'end'), 1)]
            path.write_text('\n'.join(json.dumps(row) for row in records))
            result = trace_metrics(path, {'one': {'principal': 'alice', 'submitted': 0, 'completed': 5}}, Profile())
            self.assertFalse(result['valid_exact_once_pairs'])

    def test_callback_concurrency_excess_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events'
            records = [{'kind': kind, 'key': str(index), 'principal': 'alice', 'time_ns': stamp}
                       for kind, stamp in (('start', 1), ('end', 2)) for index in range(3)]
            path.write_text('\n'.join(json.dumps(row) for row in records))
            jobs = {str(index): {'principal': 'alice', 'submitted': 0, 'completed': 1} for index in range(3)}
            result = trace_metrics(path, jobs, Profile())
            self.assertTrue(result['valid_exact_once_pairs'])
            self.assertFalse(result['capacity_observation_within_policy'])
            self.assertEqual(result['peak_active_callbacks_by_principal']['alice'], 3)


if __name__ == '__main__':
    unittest.main()
