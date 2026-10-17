"""Bounded, non-authoritative execution observations with fixed metric labels.

Identifiers correlate spans; neither identifiers nor application values become
metric labels. Missing/corrupt telemetry never changes an admission or receipt.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from .contracts import AdapterInterrupted

STAGES = ('http', 'dispatch', 'queue', 'provider', 'protocol', 'reconciliation')
OUTCOMES = ('returned', 'raised', 'observed')
BUCKETS_MS = (1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000, 300000)
IDENTIFIERS = ('request_id', 'task_id', 'run_id', 'assignment_id', 'invocation_id')
MAX_LOG_BYTES = 1_048_576
NAME = 'telemetry.ndjson'
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:/-]{0,159}\Z')


def _open(path, flags):
    fd = os.open(path, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
        os.close(fd)
        raise ValueError('private observation file required')
    return fd


@contextmanager
def _lock(root, *, write):
    import fcntl
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('observation directory unavailable')
    # Audit copies remain byte-for-byte inspectable. Their historical telemetry
    # is readable, but new observations are discarded rather than persisted.
    if write and any((p / '.restored-audit-only').exists() or (p / '.restored-audit-only').is_symlink()
                     for p in (root, *root.parents)):
        raise ValueError('audit observation copy')
    deployment = None
    if write and (root / '.deployment.lock').exists():
        deployment = _open(root / '.deployment.lock', os.O_RDONLY)
        try:
            fcntl.flock(deployment, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BaseException:
            os.close(deployment)
            raise
    fd = None
    try:
        fd = _open(root / '.telemetry.lock', os.O_RDWR | os.O_CREAT) if write else _open(root / '.telemetry.lock', os.O_RDONLY)
        fcntl.flock(fd, (fcntl.LOCK_EX if write else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        yield root
    finally:
        if fd is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        if deployment is not None:
            fcntl.flock(deployment, fcntl.LOCK_UN)
            os.close(deployment)


def record(root, stage, duration_us, *, outcome='observed', started_us=None, **identifiers):
    """Best-effort bounded append. Application strings and exception text absent."""
    if root is None:
        return
    try:
        if (stage not in STAGES or outcome not in OUTCOMES or type(duration_us) is not int
                or not 0 <= duration_us <= 366*86400*1_000_000 or set(identifiers) - set(IDENTIFIERS)):
            return
        ids = {key: value for key, value in identifiers.items()
               if type(value) is str and _ID.fullmatch(value)}
        anchor = ids.get('run_id') or ids.get('task_id') or ids.get('request_id') or uuid.uuid4().hex
        row = {'format': 'mas-span/1', 'stage': stage, 'outcome': outcome,
               'started_us': started_us if type(started_us) is int else time.time_ns() // 1000,
               'duration_us': duration_us, 'trace_id': hashlib.sha256(anchor.encode()).hexdigest()[:32],
               'span_id': uuid.uuid4().hex[:16], **ids}
        data = (json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n').encode()
        if len(data) > 2048:
            return
        with _lock(root, write=True) as directory:
            path = directory / NAME
            fd = _open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
            try:
                if os.fstat(fd).st_size + len(data) > MAX_LOG_BYTES:
                    os.close(fd)
                    fd = None
                    # Both retained files are bounded. The lock serialises
                    # rotation across controller/worker processes.
                    previous = directory / (NAME + '.1')
                    if previous.exists() or previous.is_symlink():
                        check = _open(previous, os.O_RDONLY)
                        os.close(check)
                        previous.unlink()
                    path.replace(previous)
                    fd = _open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
                os.write(fd, data)
            finally:
                if fd is not None:
                    os.close(fd)
    except AdapterInterrupted:
        # The supervisor raises this from SIGTERM/SIGINT. Losing an observation
        # must never consume the shutdown that stops an admitted invocation.
        raise
    except Exception:
        # This is a lossy observation channel, never an effect ledger.
        return


@contextmanager
def span(root, stage, **identifiers):
    start, wall = time.monotonic_ns(), time.time_ns() // 1000
    outcome = 'raised'
    try:
        yield
        outcome = 'returned'
    finally:
        record(root, stage, max(0, (time.monotonic_ns()-start)//1000),
               outcome=outcome, started_us=wall, **identifiers)


def snapshot(root):
    """Histogram window is the retained observations, never lifetime billing."""
    groups = {(stage, outcome): {'count': 0, 'sum_us': 0, 'buckets': [0]*(len(BUCKETS_MS)+1)}
              for stage in STAGES for outcome in OUTCOMES}
    recent, earliest, latest, records = [], None, None, 0
    try:
        with _lock(root, write=False) as directory:
            for name in (NAME + '.1', NAME):
                path = directory / name
                if not path.exists() and not path.is_symlink():
                    continue
                with os.fdopen(_open(path, os.O_RDONLY), 'rb') as stream:
                    raw = stream.read(MAX_LOG_BYTES+1)
                if len(raw) > MAX_LOG_BYTES:
                    raise ValueError('observation bound exceeded')
                for line in raw.splitlines():
                    if not line or len(line) > 2048:
                        raise ValueError('invalid observation')
                    item = json.loads(line)
                    if (type(item) is not dict or item.get('format') != 'mas-span/1'
                            or set(item) - {'format','stage','outcome','started_us','duration_us','trace_id','span_id',*IDENTIFIERS}
                            or (item.get('stage'), item.get('outcome')) not in groups
                            or type(item.get('duration_us')) is not int or item['duration_us'] < 0
                            or item['duration_us'] > 366*86400*1_000_000
                            or type(item.get('started_us')) is not int
                            or item['started_us'] < 0
                            or type(item.get('trace_id')) is not str or re.fullmatch('[0-9a-f]{32}',item['trace_id']) is None
                            or type(item.get('span_id')) is not str or re.fullmatch('[0-9a-f]{16}',item['span_id']) is None
                            or any(type(item[key]) is not str or not _ID.fullmatch(item[key])
                                   for key in IDENTIFIERS if key in item)):
                        raise ValueError('invalid observation')
                    group = groups[item['stage'], item['outcome']]
                    group['count'] += 1
                    group['sum_us'] += item['duration_us']
                    for index, bound in enumerate(BUCKETS_MS):
                        if item['duration_us'] <= bound*1000:
                            group['buckets'][index] += 1
                    group['buckets'][-1] += 1
                    recent.append(item)
                    recent = recent[-20:]
                    earliest = min(earliest or item['started_us'], item['started_us'])
                    latest = max(latest or item['started_us'], item['started_us'])
                    records += 1
        return {'ok': True, 'format': 'mas-telemetry/1', 'basis': 'retained-observation-window',
                'lossy': True, 'max_retained_bytes': 2*MAX_LOG_BYTES,
                'earliest_started_us': earliest, 'latest_started_us': latest, 'records': records,
                'histograms': [{'stage': stage, 'outcome': outcome, 'bounds_ms': [*BUCKETS_MS, 'inf'], **values}
                               for (stage,outcome),values in groups.items()], 'recent_spans': recent}
    except FileNotFoundError:
        return {'ok': True, 'format': 'mas-telemetry/1', 'records': 0, 'histograms': [], 'recent_spans': [],
                'basis': 'retained-observation-window', 'lossy': True}
    except AdapterInterrupted:
        raise
    except Exception:
        return {'ok': False, 'error': 'telemetry_unavailable'}
