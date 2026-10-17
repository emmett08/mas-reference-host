"""Durable completion evidence for a single disposable qualification guest job.

This module deliberately depends only on the standard library. A disconnected
SSH session is neither worker completion nor permission to run the effect again.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import time

FORMAT = 'mas-vm-job/1'
LAUNCH_KEYS = {'format', 'case', 'nonce', 'kernel_boot_id', 'started_ns', 'deadline_ns'}
RESULT_KEYS = LAUNCH_KEYS | {'finished_ns', 'return_code', 'timed_out', 'stdout_valid',
                             'worker_result', 'stderr', 'error'}
OUTPUT_LIMIT = 131072


def kernel_boot_id():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def _nonce(nonce):
    if not isinstance(nonce, str) or re.fullmatch('[0-9a-f]{32}', nonce) is None:
        raise ValueError('invalid guest job nonce')


def _launch(value, nonce, *, current_boot=False):
    _nonce(nonce)
    if (not isinstance(value, dict) or set(value) != LAUNCH_KEYS or value['format'] != FORMAT
            or value['nonce'] != nonce or not isinstance(value['case'], str)
            or not value['case'].isalnum() or len(value['case']) > 128
            or not isinstance(value['kernel_boot_id'], str) or not value['kernel_boot_id']
            or type(value['started_ns']) is not int or value['started_ns'] <= 0
            or type(value['deadline_ns']) is not int
            or not 0 < value['deadline_ns'] - value['started_ns'] <= 60_000_000_000):
        raise ValueError('invalid guest job launch')
    if current_boot and value['kernel_boot_id'] != kernel_boot_id():
        raise ValueError('guest job kernel changed')
    return value


def _read(path, limit=OUTPUT_LIMIT):
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError('guest job record exceeds bound')
    return json.loads(data)


def _sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new(path, value):
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    _sync_directory(path.parent)


def _publish(directory, filename, value):
    temporary = directory / (filename + '.pending')
    _write_new(temporary, value)
    os.replace(temporary, directory / filename)
    _sync_directory(directory)


def launch_job(directory, case, nonce, supervisor_command, *, seconds=60):
    """Persist the deadline before detaching; never launch twice in a directory."""
    _nonce(nonce)
    if (type(seconds) not in (int, float) or not math.isfinite(seconds)
            or not 0 < seconds <= 60):
        raise ValueError('invalid guest job deadline')
    started = time.monotonic_ns()
    launch = _launch({'format': FORMAT, 'case': case, 'nonce': nonce,
        'kernel_boot_id': kernel_boot_id(), 'started_ns': started,
        'deadline_ns': started + int(seconds * 1_000_000_000)}, nonce, current_boot=True)
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    _write_new(directory / 'launch.json', launch)
    with (directory / 'supervisor.log').open('xb') as log:
        subprocess.Popen(supervisor_command, stdin=subprocess.DEVNULL,
            stdout=log, stderr=log, start_new_session=True, close_fds=True)
    return launch


def _completion(value, launch):
    if (not isinstance(value, dict) or set(value) != RESULT_KEYS
            or any(value[key] != launch[key] for key in LAUNCH_KEYS)
            or type(value['finished_ns']) is not int or value['finished_ns'] < launch['started_ns']
            or (value['return_code'] is not None and type(value['return_code']) is not int)
            or type(value['timed_out']) is not bool or type(value['stdout_valid']) is not bool
            or not isinstance(value['stderr'], str) or len(value['stderr']) > 4096
            or (value['error'] is not None and
                (not isinstance(value['error'], str) or len(value['error']) > 128))
            or (value['worker_result'] is not None and not isinstance(value['worker_result'], dict))):
        raise ValueError('invalid guest job completion')
    return value


def supervise_job(directory, nonce, command):
    """Run exactly one child and durably record its actual exit within the deadline."""
    directory = Path(directory)
    launch = _launch(_read(directory / 'launch.json'), nonce, current_boot=True)
    # Even a supervisor crash cannot make a second start eligible to run effects.
    _write_new(directory / 'supervisor.claim', {'nonce': nonce, 'pid': os.getpid()})
    if (directory / 'result.json').exists():
        raise ValueError('guest job already completed')
    result = {**launch, 'finished_ns': launch['started_ns'], 'return_code': None,
        'timed_out': False, 'stdout_valid': False, 'worker_result': None,
        'stderr': '', 'error': None}
    process = None
    try:
        with (directory / 'stdout.json').open('xb') as stdout, (directory / 'stderr.log').open('xb') as stderr:
            remaining = (launch['deadline_ns'] - time.monotonic_ns()) / 1_000_000_000
            if remaining <= 0:
                result['timed_out'] = True
            else:
                process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=stdout,
                    stderr=stderr, start_new_session=True, close_fds=True)
                try:
                    process.wait(timeout=max(0, (launch['deadline_ns'] - time.monotonic_ns()) / 1_000_000_000))
                except subprocess.TimeoutExpired:
                    result['timed_out'] = True
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
                result['return_code'] = process.returncode
            result['finished_ns'] = time.monotonic_ns()
            if result['finished_ns'] > launch['deadline_ns']:
                result['timed_out'] = True
            for stream in (stdout, stderr):
                stream.flush()
                os.fsync(stream.fileno())
    except Exception as error:
        result['error'] = type(error).__name__
        if process is not None and process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                pass
            try:
                process.wait(timeout=5)
            except Exception:
                pass
        if process is not None:
            result['return_code'] = process.returncode
        result['finished_ns'] = time.monotonic_ns()
    if process is None or process.returncode is None:
        # A failed wait is diagnostic evidence, never an assertion that the
        # original child has exited. In particular it cannot permit a retry.
        failure = {**launch, 'child_exit_confirmed': False,
                   'error': result['error'] or ('ChildNotStarted' if process is None else 'ChildExitUnconfirmed')}
        _publish(directory, 'failure.json', failure)
        raise RuntimeError('guest job child exit could not be confirmed')
    try:
        value = _read(directory / 'stdout.json')
        if value is not None and not isinstance(value, dict):
            raise ValueError('invalid worker result')
        result.update(stdout_valid=True, worker_result=value)
    except (OSError, ValueError):
        pass
    try:
        with (directory / 'stderr.log').open('rb') as stream:
            result['stderr'] = stream.read(4096).decode(errors='replace')
    except OSError:
        pass
    _completion(result, launch)
    _publish(directory, 'result.json', result)
    return result


def inspect_job(directory, nonce):
    """Read this same job after reconnection; missing completion stays incomplete."""
    directory = Path(directory)
    launch = _launch(_read(directory / 'launch.json'), nonce, current_boot=True)
    try:
        result = _read(directory / 'result.json', OUTPUT_LIMIT + 16384)
    except FileNotFoundError:
        result = None
    else:
        if result is None:
            raise ValueError('invalid guest job completion')
        _completion(result, launch)
    observed = {'launch': launch, 'completion': result}
    if (directory / 'failure.json').exists():
        failure = _read(directory / 'failure.json')
        if (not isinstance(failure, dict) or set(failure) != LAUNCH_KEYS | {'child_exit_confirmed', 'error'}
                or any(failure[key] != launch[key] for key in LAUNCH_KEYS)
                or failure['child_exit_confirmed'] is not False
                or not isinstance(failure['error'], str) or len(failure['error']) > 128):
            raise ValueError('invalid guest job supervisor failure')
        observed['supervisor_failure'] = failure
    return observed


def completed_result(observed, expected_launch):
    """Accept only a matching, timely, successful child, never a transport outcome."""
    if not isinstance(expected_launch, dict):
        raise ValueError('invalid expected guest job launch')
    _launch(expected_launch, expected_launch.get('nonce'))
    if (not isinstance(observed, dict) or set(observed) != {'launch', 'completion'}
            or observed['launch'] != expected_launch):
        raise ValueError('guest job identity changed')
    result = _completion(observed['completion'], expected_launch)
    if (result['return_code'] != 0 or result['timed_out'] or not result['stdout_valid']
            or result['error'] is not None or result['finished_ns'] > expected_launch['deadline_ns']):
        raise ValueError('guest job did not complete successfully within its deadline')
    return result['worker_result']
