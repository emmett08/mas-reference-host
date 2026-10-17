"""Observability is bounded, private, lossy and cannot alter an effect result."""
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import patch

from mas_host.contracts import AdapterInterrupted
from mas_host.telemetry import record, snapshot, span, NAME, MAX_LOG_BYTES


class TelemetryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_correlations_fixed_histograms_and_exception_certainty_not_inferred(self):
        record(self.root, 'provider', 2300, outcome='returned', task_id='task-abc',
               run_id='run-abc', invocation_id='call-abc', request_id='request-abc')
        with self.assertRaisesRegex(ValueError, 'sensitive exception'):
            with span(self.root, 'provider', run_id='run-abc', invocation_id='call-second'):
                raise ValueError('sensitive exception')
        report = snapshot(self.root)
        self.assertTrue(report['ok'])
        self.assertEqual(report['records'], 2)
        returned = next(row for row in report['histograms'] if (row['stage'],row['outcome'])==('provider','returned'))
        self.assertEqual(returned['buckets'][:3], [0, 1, 1])
        self.assertEqual(returned['sum_us'], 2300)
        self.assertEqual(report['recent_spans'][0]['trace_id'], report['recent_spans'][1]['trace_id'])
        encoded = json.dumps(report)
        self.assertNotIn('sensitive', encoded)
        self.assertNotIn('certainty', encoded)
        self.assertNotIn('run-abc', json.dumps(report['histograms']))

    def test_rotation_and_concurrent_writers_are_bounded(self):
        def write(index):
            for _ in range(300):
                record(self.root, 'dispatch', 10, task_id='task-' + str(index))
        with patch('mas_host.telemetry.MAX_LOG_BYTES', 8192):
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(write, range(4)))
            report = snapshot(self.root)
        self.assertTrue(report['ok'])
        self.assertGreater(report['records'], 0)
        self.assertLessEqual(sum(p.stat().st_size for p in self.root.glob(NAME+'*')), 16384)
        self.assertLessEqual(len(report['recent_spans']), 20)

    def test_audit_copy_and_failed_sink_do_not_mutate_or_raise(self):
        record(self.root, 'http', 1, request_id='first')
        before = {p.name:p.read_bytes() for p in self.root.iterdir()}
        (self.root / '.restored-audit-only').write_text('audit')
        with span(self.root, 'provider', invocation_id='ignored'):
            result = 17
        self.assertEqual(result, 17)
        self.assertEqual({p.name:p.read_bytes() for p in self.root.iterdir() if p.name != '.restored-audit-only'}, before)
        self.assertEqual(snapshot(self.root)['records'], 1)
        with span(self.root / 'absent', 'provider'):
            pass

    def test_untrusted_label_fields_and_symlinks_never_enter_report(self):
        record(self.root, 'secret-prompt', 1, request_id='x')
        record(self.root, 'provider', 1, password='secret')
        self.assertEqual(snapshot(self.root)['records'], 0)
        outside = self.root / 'outside'
        outside.write_text('unchanged')
        (self.root / NAME).symlink_to(outside)
        record(self.root, 'provider', 1)
        self.assertEqual(outside.read_text(), 'unchanged')
        self.assertFalse(snapshot(self.root)['ok'])

    def test_http_application_result_cannot_inject_correlation_fields(self):
        from test_bridge_server import MemoryService, ServerFixture
        class ResultService(MemoryService):
            def result(self, principal, task_id):
                return {'run_id': 'sensitive-output', 'task_id': 'sensitive-task'}
        service = ResultService()
        service.state_dir = self.root
        with ServerFixture(service, request_log=lambda _: None) as server:
            status, _, body = server.request(path='/service/result', payload={'task_id': 'one'})
            self.assertEqual(status, 200)
            self.assertEqual(body['result']['run_id'], 'sensitive-output')
        report = snapshot(self.root)
        self.assertEqual(report['records'], 1)
        self.assertNotIn('sensitive', json.dumps(report))

    def test_corrupt_observations_are_explicitly_unavailable(self):
        record(self.root, 'http', 1)
        with (self.root / NAME).open('ab') as output:
            output.write(b'{"malformed":true}\n')
        self.assertFalse(snapshot(self.root)['ok'])

    @unittest.skipUnless(os.name == 'posix', 'POSIX shutdown signals')
    def test_shutdown_signal_during_observation_open_is_not_discarded(self):
        for number in (signal.SIGTERM, signal.SIGINT):
            for name, observe in (('record', lambda: record(self.root, 'protocol', 1)),
                                  ('snapshot', lambda: snapshot(self.root))):
                with self.subTest(signal=number, observation=name):
                    delivered = []
                    def interrupted(received, _frame):
                        delivered.append(received)
                        raise AdapterInterrupted('host shutdown')
                    def opening(*_args):
                        os.kill(os.getpid(), number)
                    previous = signal.signal(number, interrupted)
                    try:
                        with patch('mas_host.telemetry._open', side_effect=opening):
                            with self.assertRaises(AdapterInterrupted):
                                observe()
                        self.assertEqual(delivered, [number])
                    finally:
                        signal.signal(number, previous)


if __name__ == '__main__':
    unittest.main()
