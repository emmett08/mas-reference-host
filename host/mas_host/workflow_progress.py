"""Durable completed-prefix checkpoints for sequential workflow controls.

A checkpoint references a completed child in the same journal transaction. It
is an optimisation of retained evidence, never permission to skip an unfinished
child or create a replacement invocation. Existing journals without checkpoints
reconstruct them by replaying their unchanged completed nodes once.
"""
from __future__ import annotations

import json

from .codec import canonical_json
from .service_contracts import ServiceError


CONTROL_PROGRESS = ('CREATE TABLE control_progress(instance TEXT NOT NULL,path TEXT NOT NULL,'
                    'kind TEXT NOT NULL,next_index INTEGER NOT NULL,output TEXT NOT NULL,'
                    'consumed INTEGER NOT NULL,PRIMARY KEY(instance,path))')
_PREFIXES = {'sequence': 's', 'reduce': 'r', 'loop': 'i', 'while': 'i'}


def progress(db, instance, path, kind):
    row = db.execute('SELECT kind,next_index,output,consumed FROM control_progress WHERE instance=? AND path=?',
                     (instance, path)).fetchone()
    if row is None:
        return None
    if row['kind'] != kind or row['next_index'] < 1 or row['consumed'] < 0:
        raise ServiceError('workflow_progress_conflict')
    return {'next_index': row['next_index'], 'output': json.loads(row['output']), 'consumed': row['consumed']}


def checkpoint(db, instance, path, kind, next_index, output, consumed=0):
    if (kind not in _PREFIXES or type(next_index) is not int or next_index < 1
            or type(consumed) is not int or not 0 <= consumed < 2**63):
        raise ServiceError('invalid_workflow_progress')
    saved = progress(db, instance, path, kind)
    supplied = {'next_index': next_index, 'output': output, 'consumed': consumed}
    if saved == supplied:
        return
    if next_index != (saved['next_index'] if saved else 0) + 1 or consumed < (saved['consumed'] if saved else 0):
        raise ServiceError('workflow_progress_conflict')
    parent = db.execute('SELECT kind FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone()
    child = db.execute('SELECT state,output FROM nodes WHERE instance=? AND path=?',
                       (instance, f'{path}/{_PREFIXES[kind]}{next_index-1}')).fetchone()
    encoded = canonical_json(output)
    if parent is None or parent['kind'] != kind or child is None or child['state'] != 'completed' or (child['output'] if child['output'] is not None else 'null') != encoded:
        raise ServiceError('workflow_progress_unverified')
    db.execute('INSERT INTO control_progress VALUES(?,?,?,?,?,?) ON CONFLICT(instance,path) DO UPDATE SET '
               'next_index=excluded.next_index,output=excluded.output,consumed=excluded.consumed',
               (instance, path, kind, next_index, encoded, consumed))
