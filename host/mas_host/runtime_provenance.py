"""Read bounded, content-bound build and retained registration version evidence.

Records describe installed or retained bytes. They neither execute code nor
establish availability of an old implementation, provider or remote worker.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

BUILD_RECORD = 'runtime_build.json'
MAX_RECORD_BYTES = 1024 * 1024
MAX_PACKAGE_BYTES = 64 * 1024 * 1024
MAX_PACKAGE_FILES = 4096
MAX_DEFINITIONS = 1000
_VERSION = re.compile(r'[A-Za-z0-9][A-Za-z0-9._+/!-]{0,127}\Z')
_DIGEST = re.compile(r'[0-9a-f]{64}\Z')


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _read(path, maximum):
    safeguards = [getattr(os, name, None) for name in ('O_NOFOLLOW', 'O_NONBLOCK')]
    if any(type(flag) is not int for flag in safeguards):
        # A plain open can follow a replaced symlink or block on a FIFO before
        # fstat. Report unavailable on platforms without these safeguards.
        raise ValueError('safe metadata open unavailable')
    with os.fdopen(os.open(path, os.O_RDONLY | safeguards[0] | safeguards[1]), 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ValueError('bounded regular file required')
        content = stream.read(maximum + 1)
        after = os.fstat(stream.fileno())
        fields = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        if len(content) > maximum or fields(before) != fields(after):
            raise ValueError('file changed or exceeded bound')
        return content


def package_digest(package):
    """Bind executable Python sources and shipped data, independently of paths."""
    root = Path(package)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('package unavailable')
    files, total = {}, 0
    for count, path in enumerate(root.rglob('*')):
        if count >= 100000:
            raise ValueError('package entry bound')
        relative = path.relative_to(root)
        if '__pycache__' in relative.parts or path.name == BUILD_RECORD:
            continue
        if path.is_symlink():
            raise ValueError('package symlink')
        if path.is_dir():
            continue
        # Wheel RECORD and bytecode are installation products, not source assets.
        if path.suffix not in {'.py', '.json', '.mjs'}:
            continue
        if len(files) >= MAX_PACKAGE_FILES:
            raise ValueError('package file bound')
        data = _read(path, MAX_PACKAGE_BYTES - total)
        total += len(data)
        files[relative.as_posix()] = hashlib.sha256(data).hexdigest()
    if not files:
        raise ValueError('empty package')
    return _digest(files)


def _json(data):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate field')
            value[key] = item
        return value
    return json.loads(data, object_pairs_hook=unique)


def _version(value):
    if type(value) is not str or _VERSION.fullmatch(value) is None:
        raise ValueError('invalid version')
    return value


def build_record(package, binaries):
    """Match build metadata to this package and the already observed binaries.

    Trust is inherited from the installed package/release. A matching hash is
    content integrity, not independent authentication of a release signature.
    """
    try:
        raw = _read(Path(package) / BUILD_RECORD, MAX_RECORD_BYTES)
    except FileNotFoundError:
        return {'status': 'not_recorded', 'source_commit': None, 'release_tag': None}
    except (OSError, ValueError):
        return {'status': 'unavailable', 'source_commit': None, 'release_tag': None}
    try:
        record = _json(raw)
        if (type(record) is not dict or set(record) != {'format', 'source_commit', 'release_tag',
                'host_version', 'package_sha256', 'binaries'} or record['format'] != 'mas-runtime-build/1'
                or record['source_commit'] is not None and (
                    type(record['source_commit']) is not str or re.fullmatch('[0-9a-f]{40}', record['source_commit']) is None)
                or record['release_tag'] is not None and (
                    type(record['release_tag']) is not str or re.fullmatch(r'v0\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)', record['release_tag']) is None)
                or record['release_tag'] is not None and record['source_commit'] is None
                or type(record['package_sha256']) is not str or _DIGEST.fullmatch(record['package_sha256']) is None
                or type(record['binaries']) is not dict or set(record['binaries']) != {'mas', 'mas-gateway'}):
            raise ValueError('invalid build record')
        _version(record['host_version'])
        for item in record['binaries'].values():
            if (type(item) is not dict or set(item) != {'version', 'sha256'}
                    or type(item['sha256']) is not str or _DIGEST.fullmatch(item['sha256']) is None):
                raise ValueError('invalid binary binding')
            _version(item['version'])
        if record['package_sha256'] != package_digest(package):
            return {'status': 'package_mismatch', 'source_commit': None, 'release_tag': None}
        matched = {name: binaries.get(name, {}).get('sha256') == item['sha256']
                   for name, item in record['binaries'].items()}
        return {**record, 'status': 'content_bound' if all(matched.values()) else 'binary_mismatch',
                'binary_matches': matched, 'record_sha256': hashlib.sha256(raw).hexdigest(),
                'source_status': 'build_recorded' if record['source_commit'] else 'not_recorded',
                'trust_basis': 'installed-package; release authentication is external'}
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RecursionError):
        # Keep this build helper loadable without the mas_host package. Only
        # evidence failures become unavailable; shutdown control flow escapes.
        return {'status': 'unavailable', 'source_commit': None, 'release_tag': None}


def _components(descriptor):
    workers, providers, pending = {}, {}, [(descriptor, 0)]
    visited = 0
    while pending:
        item, depth = pending.pop()
        visited += 1
        if visited > 10000 or depth > 64 or len(workers) + len(providers) > 1024:
            raise ValueError('descriptor traversal bound')
        if type(item) is list:
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is dict:
            if item.get('format') == 'mas-executable-worker/1':
                identity = _digest(item)
                workers[identity] = {'identity': identity, 'version': _version(item.get('version')),
                                     'contract': 'mas-executable-worker/1'}
            if type(item.get('provider')) is str and type(item.get('implementation')) is str:
                identity = _digest(item)
                providers[identity] = {'identity': identity, 'implementation': _version(item['implementation']),
                    'contract': _version(item['contract']) if 'contract' in item else None}
            pending.extend((child, depth + 1) for child in item.values() if type(child) in {dict, list})
    return list(workers.values()), list(providers.values())


def retained_versions(state_dir):
    """Inspect immutable local registration evidence; omit names and descriptors."""
    try:
        from .diagnostics import _root, _read_database
        from .definitions import DEFINITIONS
        from .operations import OperationsError
        from .schema import SchemaError
    except ImportError:
        return {'status': 'unavailable', 'items': [], 'has_more': False}
    try:
        path = _root(state_dir) / 'definition-registry.sqlite'
        if not path.exists() and not path.is_symlink():
            return {'status': 'not_recorded', 'items': [], 'has_more': False}
        with _read_database(path, DEFINITIONS) as db:
            rows = db.execute('SELECT kind,name,identity,version,implementation_digest,retired '
                'FROM definitions ORDER BY kind,identity,name LIMIT ?', (MAX_DEFINITIONS + 1,)).fetchall()
            items, output_bytes, has_more = [], 0, len(rows) > MAX_DEFINITIONS
            for row in rows[:MAX_DEFINITIONS]:
                if (row['kind'] not in {'workflow', 'plan', 'plan_source'}
                        or type(row['identity']) is not str or _DIGEST.fullmatch(row['identity']) is None
                        or row['implementation_digest'] is not None and (
                            type(row['implementation_digest']) is not str
                            or _DIGEST.fullmatch(row['implementation_digest']) is None)):
                    raise ValueError('invalid retained identity')
                saved = db.execute('SELECT length(CAST(descriptor AS BLOB)),substr(descriptor,1,?) '
                    'FROM definitions WHERE kind=? AND name=? AND identity=?',
                    (MAX_RECORD_BYTES, row['kind'], row['name'], row['identity'])).fetchone()
                if saved[0] is not None and saved[0] > MAX_RECORD_BYTES:
                    raise ValueError('definition bound')
                descriptor = saved[1]
                if descriptor is None:
                    items.append({'kind': row['kind'], 'identity': row['identity'], 'status': 'not_recorded'})
                    continue
                if len(descriptor.encode()) > MAX_RECORD_BYTES:
                    raise ValueError('definition bound')
                value = _json(descriptor)
                if _digest(value) != row['identity']:
                    raise ValueError('definition identity mismatch')
                workers, providers = _components(value)
                item = {'kind': row['kind'], 'identity': row['identity'],
                    'version': _version(row['version']) if row['version'] is not None else None,
                    'implementation_digest': row['implementation_digest'], 'retired': bool(row['retired']),
                    'status': 'retained_definition', 'execution_availability': 'not_checked',
                    'workers': workers, 'providers': providers}
                output_bytes += len(_canonical(item).encode())
                if output_bytes > MAX_RECORD_BYTES:
                    has_more = True
                    break
                items.append(item)
            return {'status': 'retained', 'items': items, 'has_more': has_more,
                    'limit': MAX_DEFINITIONS, 'basis': 'immutable registry; current code is not loaded'}
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RecursionError,
            sqlite3.Error, OperationsError, SchemaError):
        return {'status': 'unavailable', 'items': [], 'has_more': False}
