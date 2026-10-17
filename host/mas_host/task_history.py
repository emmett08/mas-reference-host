"""Quiescent task archival with permanent request and accounting identities."""
from pathlib import Path
import time

from .operations import (DeploymentLedger, OperationsError, _file_lock,
                         _private_dir, _retention_run_settled, _retention_pending_delivery)
from .task_store import TaskStore, decoded


def archive_tasks(state_dir: Path, older_than_seconds: int, apply: bool = False, *, task_id=None) -> dict:
    if type(older_than_seconds) is not int or older_than_seconds < 0:
        raise OperationsError('retention age must be a nonnegative integer')
    state_dir = _private_dir(Path(state_dir))
    ledger = DeploymentLedger(state_dir, readonly=True)
    if apply and not ledger.admission_enabled:
        raise OperationsError('restored deployment is audit-only; archival disabled')
    if not (state_dir / 'task-service.sqlite').exists():
        return {'ok': True, 'dry_run': not apply, 'tasks': []}
    results = []
    with _file_lock(state_dir / '.deployment.lock', True, nonblocking=True):
        store = TaskStore(state_dir, deployment_id=ledger.deployment_id, readonly=not apply)
        with store.transaction(readonly=not apply) as db:
            for row in db.execute('SELECT * FROM tasks ORDER BY created_at,id').fetchall():
                task = decoded(row)
                if task_id is not None and task['id'] != task_id:
                    continue
                if task.get('archived_at') is not None:
                    continue
                result = {'task_id': task['id'], 'eligible': False, 'archived': False}
                if task['state'] not in {'completed', 'failed', 'cancelled'}:
                    result['reason'] = 'task is not terminal'
                elif task['updated_at'] / 1000 > time.time() - older_than_seconds:
                    result['reason'] = 'retention age not reached'
                elif _retention_pending_delivery(state_dir, task['id']):
                    result['reason'] = 'unacknowledged event delivery retained'
                else:
                    executions = db.execute('SELECT run_directory,state,revision FROM executions WHERE task_id=?',
                                            (task['id'],)).fetchall()
                    settled = all((item['state'] in {'completed', 'failed', 'cancelled'}
                                   or item['revision'] < task['revision'] and item['state'] in {'input_required', 'auth_required'})
                                  and _retention_run_settled(state_dir, item['run_directory'])
                                  for item in executions)
                    if not settled:
                        result['reason'] = 'unsettled run, remote effect or artefact reference retained'
                    else:
                        result['eligible'] = True
                        if apply:
                            store.archive(db, task)
                            result['archived'] = True
                results.append(result)
    return {'ok': True, 'dry_run': not apply, 'tasks': results}
