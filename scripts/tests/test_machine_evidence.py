"""The machine gate must retain real fixed-size BLOB spool observations."""
from contextlib import closing, redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1]/'qualification/machine_fixture.py'
SPEC = importlib.util.spec_from_file_location('machine_fixture_evidence_test', SOURCE)
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)

JOB_SOURCE = SOURCE.with_name('guest_job.py')
JOB_SPEC = importlib.util.spec_from_file_location('guest_job_evidence_test', JOB_SOURCE)
jobs = importlib.util.module_from_spec(JOB_SPEC)
JOB_SPEC.loader.exec_module(jobs)

SUPERVISOR = '''import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location('isolated_guest_job', sys.argv[1])
jobs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(jobs)
jobs.supervise_job(pathlib.Path(sys.argv[2]), sys.argv[3],
                   [sys.executable, '-I', '-c', sys.argv[4]])
'''

INSPECTOR = '''import importlib.util, json, pathlib, sys
spec = importlib.util.spec_from_file_location('isolated_guest_job', sys.argv[1])
jobs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(jobs)
print(json.dumps(jobs.inspect_job(pathlib.Path(sys.argv[2]), sys.argv[3])))
'''


class MachineEvidenceTests(unittest.TestCase):
    def test_detached_fixture_start_creates_private_worker_spool_before_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            nonce = 'a'*32
            def launch(directory, case, selected_nonce, command):
                self.assertEqual(directory, root/'partition'/'job')
                self.assertEqual(directory.parent.stat().st_mode & 0o777, 0o700)
                self.assertEqual((case, selected_nonce), ('partition', nonce))
                self.assertEqual(command[-5:], ['supervise', '--case', 'partition', '--job', nonce])
                return {'started': True}
            original_umask = os.umask(0o022)
            try:
                output = io.StringIO()
                with patch.object(fixture, 'GUEST_STATE', root), \
                     patch.object(sys, 'argv', ['fixture', 'start', '--case', 'partition', '--job', nonce]), \
                     patch.dict(sys.modules, {'guest_job': jobs}), \
                     patch.object(jobs, 'launch_job', side_effect=launch) as launched, redirect_stdout(output):
                    self.assertEqual(fixture.main(), 0)
                launched.assert_called_once()
                self.assertEqual(json.loads(output.getvalue()), {'started': True})
            finally:
                os.umask(original_umask)

    def test_snapshot_decodes_durable_blob_receipts_without_parsing_unwritten_reservations(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spool = root/'example'
            spool.mkdir(mode=0o700)
            receipt = {'id': 'original-receipt', 'call_id': 'original-call', 'status': 'completed'}
            encoded = json.dumps(receipt).encode()
            padded = encoded+b' '*(131072-len(encoded))
            with closing(fixture.connect(spool/'worker-spool.sqlite', fixture.SPOOL, initialise=lambda _db: None)) as db, db:
                db.execute("INSERT INTO attempts VALUES('original-call',1,'{}','receipt',?,zeroblob(1024))", (padded,))
                db.execute("INSERT INTO attempts VALUES('running-call',1,'{}','executing',zeroblob(131072),zeroblob(1024))")
            output = io.StringIO()
            with patch.object(fixture, 'GUEST_STATE', root), patch.object(sys, 'argv', ['fixture', 'snapshot', '--case', 'example']), redirect_stdout(output):
                self.assertEqual(fixture.main(), 0)
            observations = json.loads(output.getvalue())['attempts']
            self.assertEqual(observations[0]['receipt'], receipt)
            self.assertEqual(observations[0]['state'], 'receipt')
            self.assertIsNone(observations[0]['ack'])
            self.assertIsNone(observations[1]['receipt'])


class GuestJobEvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.directory = self.root/'job'
        self.nonce = 'a'*32
        self.processes = []
        self.addCleanup(self.reap_supervisors)

    def reap_supervisors(self):
        for process in self.processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

    def launch(self, child_source, *, seconds=5):
        real_popen = subprocess.Popen
        def start(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            self.processes.append(process)
            return process
        command = [sys.executable, '-I', '-c', SUPERVISOR, str(JOB_SOURCE),
                   str(self.directory), self.nonce, child_source]
        with patch.object(jobs.subprocess, 'Popen', side_effect=start) as launched:
            result = jobs.launch_job(self.directory, 'partition', self.nonce, command, seconds=seconds)
        self.assertTrue(launched.call_args.kwargs['start_new_session'])
        self.assertTrue(launched.call_args.kwargs['close_fds'])
        self.assertEqual(launched.call_args.kwargs['stdin'], subprocess.DEVNULL)
        self.assertNotIn(launched.call_args.kwargs['stdout'], (None, subprocess.PIPE))
        self.assertNotIn(launched.call_args.kwargs['stderr'], (None, subprocess.PIPE))
        return result

    def completion(self):
        deadline = time.monotonic()+5
        while time.monotonic() < deadline:
            observed = jobs.inspect_job(self.directory, self.nonce)
            if observed['completion'] is not None:
                return observed
            time.sleep(.01)
        self.fail('detached supervisor did not retain its child completion')

    def test_fresh_inspection_requires_the_original_child_completion_and_never_relaunches(self):
        effect = self.root/'effects'
        release = self.root/'release'
        launch = self.launch(
            'import json, pathlib, time\n'
            f'while not pathlib.Path({str(release)!r}).exists(): time.sleep(.01)\n'
            f'pathlib.Path({str(effect)!r}).write_text("once")\n'
            'print(json.dumps({"call_id":"original-call","receipt_stored":True}))')
        pending = jobs.inspect_job(self.directory, self.nonce)
        self.assertIsNone(pending['completion'])
        with self.assertRaises(ValueError):
            jobs.completed_result(pending, launch)
        release.write_text('continue')
        self.completion()
        # A new process has neither the launch SSH stream nor its exit status.
        fresh = subprocess.run([sys.executable, '-I', '-c', INSPECTOR, str(JOB_SOURCE),
            str(self.directory), self.nonce], capture_output=True, text=True, check=True)
        observed = json.loads(fresh.stdout)
        self.assertEqual(jobs.completed_result(observed, launch),
                         {'call_id': 'original-call', 'receipt_stored': True})
        self.assertEqual(observed['completion']['return_code'], 0)
        self.assertFalse(observed['completion']['timed_out'])
        self.assertEqual(effect.read_text(), 'once')
        with patch.object(jobs.subprocess, 'Popen', side_effect=AssertionError('duplicate launch')):
            with self.assertRaises((ValueError, FileExistsError)):
                jobs.launch_job(self.directory, 'partition', self.nonce, ['must-not-execute'])
            with self.assertRaises((ValueError, FileExistsError)):
                jobs.supervise_job(self.directory, self.nonce, ['must-not-execute'])
        self.assertEqual(jobs.inspect_job(self.directory, self.nonce), observed)

    def test_nonzero_child_exit_does_not_inherit_successful_output(self):
        launch = self.launch('import json; print(json.dumps({"receipt_stored": True})); raise SystemExit(7)')
        observed = self.completion()
        self.assertEqual(observed['completion']['return_code'], 7)
        with self.assertRaises(ValueError):
            jobs.completed_result(observed, launch)

    def test_malformed_child_output_cannot_supply_completion(self):
        launch = self.launch('print("not a JSON result")')
        observed = self.completion()
        self.assertEqual(observed['completion']['return_code'], 0)
        self.assertFalse(observed['completion']['stdout_valid'])
        with self.assertRaises(ValueError):
            jobs.completed_result(observed, launch)

    def test_completion_published_after_absent_read_is_observed_on_next_inspection(self):
        with patch.object(jobs.subprocess, 'Popen'):
            launch = jobs.launch_job(self.directory, 'partition', self.nonce, ['fixture-supervisor'])
        completion = {**launch, 'finished_ns': launch['started_ns'], 'return_code': 0,
            'timed_out': False, 'stdout_valid': True, 'worker_result': {'receipt_stored': True},
            'stderr': '', 'error': None}
        read = jobs._read
        def publish_after_absent_read(path, *args):
            try:
                return read(path, *args)
            except FileNotFoundError:
                self.assertEqual(path, self.directory/'result.json')
                jobs._publish(self.directory, 'result.json', completion)
                raise
        with patch.object(jobs, '_read', side_effect=publish_after_absent_read):
            pending = jobs.inspect_job(self.directory, self.nonce)
        self.assertIsNone(pending['completion'])
        self.assertEqual(jobs.inspect_job(self.directory, self.nonce)['completion'], completion)

    def test_child_timeout_is_retained_and_fails_the_gate(self):
        # Leave ample startup time for a loaded architecture runner; the guest
        # production limit remains sixty seconds.
        launch = self.launch('import time; time.sleep(3)', seconds=1)
        observed = self.completion()
        self.assertTrue(observed['completion']['timed_out'])
        self.assertNotEqual(observed['completion']['return_code'], 0)
        with self.assertRaises(ValueError):
            jobs.completed_result(observed, launch)

    def test_child_start_failure_cannot_be_recorded_as_completion(self):
        with patch.object(jobs.subprocess, 'Popen'):
            launch = jobs.launch_job(self.directory, 'partition', self.nonce, ['fixture-supervisor'])
        with patch.object(jobs.subprocess, 'Popen', side_effect=OSError('injected start failure')):
            with self.assertRaisesRegex(RuntimeError, 'child exit could not be confirmed'):
                jobs.supervise_job(self.directory, self.nonce, ['fixture-child'])
        self.assertFalse((self.directory/'result.json').exists())
        observed = jobs.inspect_job(self.directory, self.nonce)
        self.assertIs(observed['supervisor_failure']['child_exit_confirmed'], False)
        with self.assertRaises(ValueError):
            jobs.completed_result(observed, launch)

    def test_failed_reap_never_publishes_a_child_completion(self):
        with patch.object(jobs.subprocess, 'Popen'):
            launch = jobs.launch_job(self.directory, 'partition', self.nonce, ['fixture-supervisor'])
        process = Mock(pid=12345, returncode=None)
        process.wait.side_effect = OSError('injected wait failure')
        with patch.object(jobs.subprocess, 'Popen', return_value=process), \
             patch.object(jobs.os, 'killpg') as kill:
            with self.assertRaisesRegex(RuntimeError, 'child exit could not be confirmed'):
                jobs.supervise_job(self.directory, self.nonce, ['fixture-child'])
        kill.assert_called_once_with(process.pid, jobs.signal.SIGKILL)
        self.assertGreaterEqual(process.wait.call_count, 2)
        self.assertFalse((self.directory/'result.json').exists())
        failure = json.loads((self.directory/'failure.json').read_text())
        self.assertIs(failure['child_exit_confirmed'], False)
        observed = jobs.inspect_job(self.directory, self.nonce)
        self.assertIsNone(observed['completion'])
        self.assertEqual(observed['supervisor_failure'], failure)
        with self.assertRaises(ValueError):
            jobs.completed_result(observed, launch)

    def test_stale_nonce_boot_or_malformed_completion_cannot_pass(self):
        launch = self.launch('print("null")')
        observed = self.completion()
        self.assertIsNone(jobs.completed_result(observed, launch))
        with self.assertRaises(ValueError):
            jobs.inspect_job(self.directory, 'b'*32)
        with patch.object(jobs, 'kernel_boot_id', return_value='different-boot'):
            with self.assertRaises(ValueError):
                jobs.inspect_job(self.directory, self.nonce)
        for field, value in (('nonce', 'b'*32), ('case', 'other'),
                             ('kernel_boot_id', 'different-boot'), ('format', 'wrong-format')):
            with self.subTest(field=field), self.assertRaises(ValueError):
                jobs.completed_result(observed, {**launch, field: value})
        for malformed in ('not JSON', 'null', '[]', '{}',
                          json.dumps({**observed['completion'], 'nonce': 'b'*32})):
            (self.directory/'result.json').write_text(malformed)
            with self.subTest(result=malformed), self.assertRaises(ValueError):
                jobs.inspect_job(self.directory, self.nonce)

    def test_success_after_the_launch_deadline_is_rejected(self):
        launch = self.launch('print("null")')
        observed = self.completion()
        observed['completion']['finished_ns'] = launch['deadline_ns']+1
        with self.assertRaises(ValueError):
            jobs.completed_result(observed, launch)


if __name__ == '__main__':
    unittest.main()
