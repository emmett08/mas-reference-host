"""Read-only deployment preflight and fixed-cardinality operational metrics.

These advisory snapshots neither exercise providers nor establish readiness to
execute a particular workflow. Factory and binary code is never executed.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sqlite3
import stat
import sys
import time

from .operations import DeploymentLedger, OperationsError
from .schema import JOBS, JOURNAL, LEDGER, connect, validate


STATES = ('queued', 'running', 'blocked', 'input_required', 'auth_required',
          'completed', 'failed', 'cancelled', 'unknown', 'expired')
REMOTE_STATES = ('pending', 'claimed', 'dispatched', 'unknown', 'completed', 'failed_known', 'cancelled', 'reconciled')
PACKAGES = ('mas-host', 'jsonschema', 'openai', 'cursor-sdk', 'langgraph', 'temporalio')
_MAX_ENTRIES = 100000
_BINARY_MAX_BYTES = 256 * 1024 * 1024
_COUNT_STATES = {
    'tasks': STATES, 'instances': STATES, 'attempts': STATES,
    'nodes': (*STATES, 'waiting'), 'remote_tasks': STATES, 'remote_jobs': REMOTE_STATES,
    'cursor_jobs': ('prepared', 'launch_unknown', 'active', 'terminal')}
_AGE_SOURCES = ('tasks', 'instances', 'remote_tasks', 'remote_jobs')
_METRIC_FIELDS = {
    'budget': ('runs', 'calls', 'reserved_microusd', 'retained_runs', 'max_runs', 'max_calls',
               'max_reserved_microusd', 'max_state_bytes', 'min_free_bytes', 'admission_open'),
    'scheduler': ('retained_slots',), 'workers': ('registered', 'draining'),
    'circuits': ('open', 'probing', 'not_open', 'unobserved_permits'),
    'storage': ('state_bytes', 'free_bytes', 'entries')}


def _schemas():
    from .circuit_breaker import CIRCUITS
    from .definitions import DEFINITIONS
    from .distributed import REMOTE
    from .distributed_worker import SPOOL
    from .scheduler_capacity import CAPACITY
    from .task_store import SERVICE
    from .workflow_store import COMPOSITION
    from .bridges.remote import REMOTE_TASKS
    from .event_delivery import EVENT_DELIVERY
    from .worker_control import CONTROL
    from .request_namespaces import EPOCHS
    from .peer_registry import PEERS
    from .retention_admin import RETENTION
    return {'deployment.sqlite': LEDGER, 'task-service.sqlite': SERVICE,
            'composition.sqlite': COMPOSITION, 'definition-registry.sqlite': DEFINITIONS,
            'scheduler-capacity.sqlite': CAPACITY, 'provider-circuits.sqlite': CIRCUITS,
            'remote-workers.sqlite': REMOTE, 'remote-tasks.sqlite': REMOTE_TASKS,
            'cursor-jobs.sqlite': JOBS, 'host.sqlite': JOURNAL, 'worker-spool.sqlite': SPOOL,
            'event-delivery.sqlite': EVENT_DELIVERY, 'worker-control.sqlite': CONTROL,
            'request-epochs.sqlite': EPOCHS, 'peer-registry.sqlite': PEERS,
            'operator-retention.sqlite': RETENTION}


def _root(path):
    path = Path(path).absolute()
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise OperationsError('diagnostic storage boundary rejected')
    if path.exists() and (not path.is_dir() or stat.S_IMODE(path.stat().st_mode) & 0o077):
        raise OperationsError('diagnostic storage boundary rejected')
    return path


@contextmanager
def _read_database(path, schema):
    _reject_wal(path)
    db = connect(path, schema, readonly=True)
    try:
        deadline = time.monotonic() + 5
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA busy_timeout=1000')
        db.execute('BEGIN')
        if db.execute('PRAGMA quick_check(1)').fetchone()[0] != 'ok':
            raise OperationsError('diagnostic database integrity failed')
        yield db
    finally:
        db.close()


def _reject_wal(path):
    """SQLite mode=ro may create or update WAL shared memory; never open it.

    Do not substitute immutable=1: it can ignore committed WAL records. MAS's
    repositories use rollback journals; operators can inspect a quiescent
    checkpointed backup instead. Concurrent journal-mode changes are outside
    this advisory snapshot's guarantee, like other concurrent file replacement.
    """
    if any(sidecar.exists() or sidecar.is_symlink()
           for sidecar in (Path(str(path) + '-wal'), Path(str(path) + '-shm'))):
        raise OperationsError('WAL diagnostic observation is unavailable')
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise OperationsError('diagnostic database must be regular')
        header = stream.read(20)
    if len(header) >= 20 and (header[18] == 2 or header[19] == 2):
        raise OperationsError('WAL diagnostic observation is unavailable')


def _count(value):
    if type(value) is not int or value < 0:
        raise OperationsError('diagnostic state is invalid')
    return value


def _storage(root):
    entries = total = 0
    stack = [root] if root.exists() else []
    while stack:
        with os.scandir(stack.pop()) as children:
            for child in children:
                entries += 1
                if entries > _MAX_ENTRIES:
                    raise OperationsError('diagnostic storage entry bound exceeded')
                try:
                    info = child.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue  # A transient SQLite sidecar may have disappeared.
                if stat.S_ISDIR(info.st_mode):
                    stack.append(Path(child.path))
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
                else:
                    raise OperationsError('diagnostic storage boundary rejected')
    parent = root
    while not parent.exists():
        parent = parent.parent
    return {'state_bytes': total, 'free_bytes': shutil.disk_usage(parent).free, 'entries': entries}


def _counts(db, table, states, *, created=None, unit=1, now_ms=None):
    result = dict.fromkeys(states, 0)
    ages = {'queued': 0, 'unknown': 0}
    fields = 'state,count(*)' + (f',min({created})' if created else '')
    for row in db.execute(f'SELECT {fields} FROM {table} GROUP BY state'):
        state, count = row[0], _count(row[1])
        if state not in result:
            raise OperationsError('diagnostic state is invalid')
        result[state] = count
        if created and state in ages:
            stamp = _count(row[2])
            ages[state] = max(0, (now_ms - stamp * unit) // 1000)
    return result, ages


def operational_metrics(state_dir, *, now_ms=None):
    """Return aggregate observations; absent stores are zero, invalid stores fail."""
    try:
        return _operational_metrics(state_dir, now_ms=now_ms)
    except Exception:
        # Never return partial metrics that could be mistaken for zero backlog.
        return {'ok': False, 'error': 'diagnostic_state_unavailable'}


def _operational_metrics(state_dir, *, now_ms=None):
    root = _root(state_dir)
    timestamp = int(time.time() * 1000) if now_ms is None else _count(now_ms)
    schemas = _schemas()
    if root.exists() and any(path.name not in schemas for path in root.glob('*.sqlite')):
        raise OperationsError('unknown root database')
    result = {'ok': True, 'format': 'mas-operational-metrics/1',
        'age_basis': 'oldest_current_state_record_creation',
        'stores': {}, 'counts': {name: dict.fromkeys(states, 0) for name, states in _COUNT_STATES.items()},
        'ages_seconds': {name: {'queued': 0, 'unknown': 0} for name in _AGE_SOURCES},
        'budget': {'runs': 0, 'calls': 0, 'reserved_microusd': 0, 'retained_runs': 0,
                   'max_runs': 0, 'max_calls': 0, 'max_reserved_microusd': 0,
                   'max_state_bytes': 0, 'min_free_bytes': 0, 'admission_open': False},
        'scheduler': {'retained_slots': 0}, 'workers': {'registered': 0, 'draining': 0},
        'circuits': {'open': 0, 'probing': 0, 'not_open': 0, 'unobserved_permits': 0},
        'storage': _storage(root)}
    mappings = {
        'task-service.sqlite': [('tasks', 'tasks', 'created_at', 1)],
        'composition.sqlite': [('instances', 'instances', 'created', 1000), ('attempts', 'attempts', None, 1), ('nodes', 'nodes', None, 1)],
        'remote-tasks.sqlite': [('remote_tasks', 'remote_tasks', 'created', 1000)],
        'remote-workers.sqlite': [('remote_jobs', 'jobs', 'created', 1)],
        'cursor-jobs.sqlite': [('cursor_jobs', 'jobs', None, 1)]}
    for name, schema in schemas.items():
        path = root / name
        if not path.exists() and not path.is_symlink():
            result['stores'][name] = {'present': False, 'version': 0, 'migration_needed': False}
            continue
        with _read_database(path, schema) as db:
            result['stores'][name] = {'present': True,
                'version': db.execute('PRAGMA user_version').fetchone()[0],
                'migration_needed': validate(db, schema)}
            for label, table, created, unit in mappings.get(name, ()):
                counts, ages = _counts(db, table, result['counts'][label], created=created,
                                       unit=unit, now_ms=timestamp)
                result['counts'][label] = counts
                if label in result['ages_seconds']:
                    result['ages_seconds'][label] = ages
            if name == 'scheduler-capacity.sqlite':
                result['scheduler']['retained_slots'] = _count(db.execute('SELECT count(*) FROM slots').fetchone()[0])
            if name == 'remote-workers.sqlite':
                for draining, count in db.execute('SELECT draining,count(*) FROM workers GROUP BY draining'):
                    if draining not in (0, 1):
                        raise OperationsError('invalid worker state')
                    result['workers']['registered'] += _count(count)
                    if draining:
                        result['workers']['draining'] += count
                # Pending remote jobs are the remote queue; no lease-expiry
                # inference changes their retained state during observation.
                earliest = db.execute("SELECT min(created) FROM jobs WHERE state='pending'").fetchone()[0]
                if earliest is not None:
                    result['ages_seconds']['remote_jobs']['queued'] = max(0, (timestamp - _count(earliest)) // 1000)
            if name == 'provider-circuits.sqlite':
                for opened, probe, count in db.execute('SELECT open_until_ms>?,probe_call IS NOT NULL,count(*) FROM circuits GROUP BY 1,2', (timestamp,)):
                    result['circuits']['open' if opened else 'probing' if probe else 'not_open'] += _count(count)
                result['circuits']['unobserved_permits'] = _count(db.execute('SELECT count(*) FROM permits WHERE observation IS NULL').fetchone()[0])
    if result['stores']['deployment.sqlite']['present']:
        health = DeploymentLedger(root, readonly=True).health()
        budget = result['budget']
        for name in ('runs', 'calls', 'reserved_microusd'):
            budget[name] = _count(health['usage'][name])
        for name in ('max_runs', 'max_calls', 'max_reserved_microusd', 'max_state_bytes', 'min_free_bytes'):
            budget[name] = _count(health['policy'][name])
        budget['retained_runs'] = _count(health['retained_runs'])
        budget['admission_open'] = health['admission_open']
    return result


def prometheus_metrics(report):
    """Render only predeclared metric/label sets; raw identities never become labels."""
    try:
        return _prometheus_metrics(report)
    except Exception:
        return _prometheus_metrics({'ok': False})


def _fixed_fields(value, fields):
    if type(value) is not dict or set(value) != set(fields):
        raise OperationsError('invalid operational metrics')
    return value


def _prometheus_metrics(report):
    if type(report) is not dict:
        raise OperationsError('invalid operational metrics')
    lines = ['# HELP mas_diagnostics_ok Whether the complete read-only observation succeeded.',
             '# TYPE mas_diagnostics_ok gauge', f'mas_diagnostics_ok {int(report.get("ok") is True)}']
    if report.get('ok') is not True:
        return '\n'.join(lines) + '\n'
    if (report.get('format') != 'mas-operational-metrics/1'
            or report.get('age_basis') != 'oldest_current_state_record_creation'):
        raise OperationsError('invalid operational metrics')
    counts = _fixed_fields(report['counts'], _COUNT_STATES)
    for source, states in _COUNT_STATES.items():
        values = _fixed_fields(counts[source], states)
        for state in states:
            lines.append(f'mas_records{{source="{source}",state="{state}"}} {_count(values[state])}')
    ages = _fixed_fields(report['ages_seconds'], _AGE_SOURCES)
    for source in _AGE_SOURCES:
        values = _fixed_fields(ages[source], ('queued', 'unknown'))
        for state in ('queued', 'unknown'):
            lines.append(f'mas_oldest_record_age_seconds{{source="{source}",state="{state}"}} {_count(values[state])}')
    for family, names in _METRIC_FIELDS.items():
        values = _fixed_fields(report[family], names)
        for name in names:
            value = values[name]
            if family == 'budget' and name == 'admission_open':
                if type(value) is not bool:
                    raise OperationsError('invalid operational metrics')
                value = int(value)
            lines.append(f'mas_{family}_{name} {_count(value)}')
    names = _schemas()
    stores = _fixed_fields(report['stores'], names)
    for name in names:
        value = _fixed_fields(stores[name], ('present', 'version', 'migration_needed'))
        if type(value['present']) is not bool or type(value['migration_needed']) is not bool:
            raise OperationsError('invalid operational metrics')
        _count(value['version'])
        store = name.removesuffix('.sqlite').replace('-', '_')
        lines.append(f'mas_store_present{{store="{store}"}} {int(value["present"])}')
        lines.append(f'mas_store_migration_needed{{store="{store}"}} {int(value["migration_needed"])}')
    return '\n'.join(lines) + '\n'


def effective_configuration(config_path):
    """Validate declarations and local auth material without loading factories."""
    try:
        from .bridge_config import load_config, build_verifier, build_artifact_transfer
        from .bridges.authentication import _principal, read_client_secret
        from .bridges.service_client import validate_endpoint
        from .scheduler_capacity import SchedulerPolicy
        config = load_config(config_path)
        for principal in config['grants']:
            _principal(principal)
        limits = {name: config.get(name, default) for name, default in
                  (('max_pending', 1000), ('max_tasks', 10000), ('max_instances', 1000))}
        for name, value in limits.items():
            if type(value) is not int or not 1 <= value <= (100000 if name == 'max_instances' else 1000000):
                raise ValueError()
        scheduler = SchedulerPolicy(**config.get('scheduler', {}))
        from .principal_quotas import quota_configuration
        principal_quotas = quota_configuration(config.get('principal_quotas', {}))
        factory = config.get('factory')
        if 'factory' in config and (type(factory) is not str or not re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*', factory)):
            raise ValueError()
        mode = config.get('mode', 'mock')
        if mode not in ('mock', 'openai', 'cursor', 'mixed'):
            raise ValueError()
        auth = config.get('auth')
        if 'auth' in config:
            build_verifier(config)  # Constructors only; never verify a token.
            if auth['type'] == 'oauth':
                read_client_secret(auth['client_secret_file'])
        if 'public_url' in config:
            endpoint = validate_endpoint(config['public_url'], allow_loopback_http=True)
            if endpoint.path not in ('', '/', '/mcp'):
                raise ValueError()
        for key in ('allowed_hosts', 'allowed_origins'):
            values = config.get(key, [])
            if type(values) is not list or len(values) > 256 or any(type(value) is not str or not value for value in values):
                raise ValueError()
            for value in values:
                if key == 'allowed_origins':
                    if validate_endpoint(value, allow_loopback_http=True).path not in ('', '/'):
                        raise ValueError()
                elif any(ord(char) <= 32 for char in value) or '/' in value or '@' in value:
                    raise ValueError()
        # Construction validates this declarative registry, creates no files,
        # and neither opens repositories nor admits transfers.
        transfer = build_artifact_transfer(config, Path('.'))
        summary = {'format': 'mas-effective-configuration/1', 'mode': mode,
            'factory_declared': factory is not None, 'factory_loaded': False,
            'limits': limits, 'scheduler': {'max_workers': scheduler.max_workers,
                'max_per_principal': scheduler.max_per_principal,
                'workflow_limit_count': len(scheduler.workflows), 'resource_limit_count': len(scheduler.resources)},
            'principal_count': len(config['grants']),
            'principal_quota_count': len(principal_quotas),
            'grant_count': sum(len(set(names)) for names in config['grants'].values()),
            'auth_type': auth['type'] if auth is not None else 'not_configured',
            'auth_material_readable': auth is not None,
            'public_url_declared': 'public_url' in config,
            'allowed_host_count': len(config.get('allowed_hosts', [])),
            'allowed_origin_count': len(config.get('allowed_origins', [])),
            'artifact_scope_count': len(transfer.scopes) if transfer else 0}
        return {'ok': True, 'configuration': summary}
    except Exception:
        return {'ok': False, 'error': 'diagnostic_configuration_invalid'}


def _binary(command):
    # Explicit paths are still inspectable on a noexec mount. which() excludes
    # those files, hiding their presence and bypassing bounded hash checks.
    path = str(command) if os.path.dirname(str(command)) and Path(command).is_file() else shutil.which(command)
    if path is None:
        return {'present': False, 'executable': False, 'sha256': None, 'version': None,
                'version_status': 'not_executed'}
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > _BINARY_MAX_BYTES:
                raise ValueError()
            digest = hashlib.sha256()
            observed = 0
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                observed += len(chunk)
                if observed > _BINARY_MAX_BYTES:
                    raise ValueError()
                digest.update(chunk)
        return {'present': True, 'executable': os.access(path, os.X_OK), 'sha256': digest.hexdigest(),
                'version': None, 'version_status': 'not_executed'}
    except Exception:
        return {'present': True, 'executable': False, 'sha256': None,
                'version': None, 'version_status': 'unavailable'}


def doctor(state_dir, config_path=None, *, environ=None):
    environment = os.environ if environ is None else environ
    packages = {}
    for name in PACKAGES:
        try:
            version = metadata.version(name)
            packages[name] = version if re.fullmatch(r'[A-Za-z0-9.+!_-]{1,128}', version) else 'unavailable'
        except metadata.PackageNotFoundError:
            packages[name] = None
    binaries = {name: _binary(environment.get(variable, name)) for name, variable in
                (('mas', 'MAS_BIN'), ('mas-gateway', 'MAS_GATEWAY_BIN'))}
    from .runtime_provenance import build_record, retained_versions
    provenance = build_record(Path(__file__).parent, binaries)
    for name, matched in provenance.get('binary_matches', {}).items():
        if matched:
            binaries[name].update(version=provenance['binaries'][name]['version'],
                                  version_status='content_bound_build_record')
    versions = retained_versions(state_dir)
    metrics = operational_metrics(state_dir)
    configuration = effective_configuration(config_path) if config_path is not None else {'ok': True, 'configuration': None}
    checks = {'python_supported': sys.version_info >= (3, 10),
              'jsonschema_available': packages['jsonschema'] is not None,
              'binaries_available': all(value['executable'] and value['sha256'] for value in binaries.values()),
              'state_readable': metrics['ok'], 'configuration_valid': configuration['ok'],
              'build_binding_valid': provenance['status'] in {'not_recorded', 'content_bound'}}
    return {'ok': all(checks.values()), 'advisory': True, 'checks': checks,
        'runtime': {'python': platform.python_version(), 'sqlite': sqlite3.sqlite_version,
                    'packages': packages, 'binaries': binaries},
        'provenance': provenance, 'retained_versions': versions,
        'configuration': configuration.get('configuration'),
        'state': {'stores': metrics.get('stores', {}), 'budget': metrics.get('budget'),
                  'storage': metrics.get('storage'), 'error': metrics.get('error')},
        'limitations': ['factories_not_loaded', 'providers_not_contacted',
                        'binary_versions_not_executed', 'snapshot_not_an_admission_guarantee']}
