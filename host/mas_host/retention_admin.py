"""Audited one-task retention with recoverable intent across local owner stores."""
from contextlib import closing
import hashlib
import json

from .codec import canonical_json
from .operations import _file_lock
from .schema import Schema, connect
from .service_contracts import ServiceError

RETENTION = Schema('operator retention', 0x4D415349, 1, (), (
    'CREATE TABLE commands(request_key TEXT PRIMARY KEY,digest TEXT NOT NULL,command TEXT NOT NULL,receipt TEXT)',
))


def archive(service, operator, payload):
    from .recovery import _identity
    for key in ('principal', 'task_id', 'request_key'):
        _identity(payload[key])
    reason, age = payload['reason'], payload['older_than_seconds']
    if (type(reason) is not str or not reason.strip() or len(reason) > 2048
            or any(ord(c) < 32 for c in reason) or type(age) is not int or not 0 <= age <= (2**63-1)//1000
            or type(payload['kind']) is not str or payload['kind'] not in {'tasks', 'workflows', 'runs'}):
        raise ServiceError('invalid_retention_request')
    if payload['kind'] != 'runs' and (payload['kind'] == 'workflows') != payload['task_id'].startswith('flow-'):
        raise ServiceError('invalid_archive_kind')
    command = {'operator': operator, **payload}
    if hasattr(service, 'authority'):
        return service.authority.command('operator.archive', command)['receipt']
    if payload['kind'] == 'runs':
        raise ServiceError('unsupported_capability')
    service.inspect(payload['principal'], payload['task_id'])
    operations, root = service.operations, service.state_dir
    encoded = canonical_json(command)
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    key = payload['request_key']
    path = root / 'operator-retention.sqlite'
    with _file_lock(root / '.operator-retention.lock', True):
        with _file_lock(root / '.deployment.lock', False):
            if operations.readonly or not operations.admission_enabled:
                raise ServiceError('audit_only')
            with closing(connect(path, RETENTION, initialise=lambda _: None)) as db, db:
                old = db.execute('SELECT digest,receipt FROM commands WHERE request_key=?', (key,)).fetchone()
                if old:
                    if old['digest'] != digest:
                        raise ServiceError('retention_request_conflict')
                    if old['receipt'] is not None:
                        return json.loads(old['receipt'])
                else:
                    if db.execute('SELECT count(*) FROM commands').fetchone()[0] >= 1_000_000:
                        raise ServiceError('retention_history_full')
                    # This permanent intent identifies an interrupted archive;
                    # retries inspect its original target before any mutation.
                    db.execute('INSERT INTO commands VALUES(?,?,?,NULL)', (key, digest, encoded))
        current = service.inspect(payload['principal'], payload['task_id'])
        if not current.get('archived'):
            if payload['kind'] == 'tasks':
                from .task_history import archive_tasks
                result = archive_tasks(root, age, True, task_id=payload['task_id'])['tasks']
            else:
                from .operations import archive_workflows
                result = archive_workflows(root, age, True, task_id=payload['task_id'])['instances']
            if not result or not result[0]['archived']:
                raise ServiceError('task_not_archivable')
            current = service.inspect(payload['principal'], payload['task_id'])
        receipt = {'format': 'mas-retention/1', **command, 'archived': True,
                   'archived_at': current['archived_at'], 'state': current['state'],
                   'result_digest': current.get('result_digest'), 'allocation_changed': False}
        with _file_lock(root / '.deployment.lock', False):
            if operations.readonly or not operations.admission_enabled:
                raise ServiceError('audit_only')
            with closing(connect(path, RETENTION)) as db, db:
                db.execute('UPDATE commands SET receipt=? WHERE request_key=?', (canonical_json(receipt), key))
        return receipt
