"""Versioned durable composition records, distinct from Rust task state."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import time
import uuid

from .codec import canonical_json
from .contracts import RunnerError
from .schema import Schema, SchemaError, connect
from .service_contracts import ServiceError
from .deadline_amendments import DEADLINE_AMENDMENTS
from .workflow_progress import CONTROL_PROGRESS
from .workflow_evaluator import FINISHED_STATES
from .request_namespaces import RequestScope, OWNER_EPOCHS


COMPOSITION_V1 = Schema('workflow composition', 0x4D415357, 1, (), (
    'CREATE TABLE instances(id TEXT PRIMARY KEY,principal TEXT NOT NULL,request_key TEXT NOT NULL,plan TEXT NOT NULL,identity TEXT NOT NULL,input TEXT NOT NULL,state TEXT NOT NULL,output TEXT,error TEXT,created INTEGER NOT NULL,deadline INTEGER NOT NULL,cancel_requested INTEGER NOT NULL DEFAULT 0,UNIQUE(principal,request_key))',
    'CREATE TABLE nodes(instance TEXT NOT NULL,path TEXT NOT NULL,kind TEXT NOT NULL,input TEXT NOT NULL,state TEXT NOT NULL,output TEXT,error TEXT,PRIMARY KEY(instance,path))',
    'CREATE TABLE attempts(instance TEXT NOT NULL,path TEXT NOT NULL,revision INTEGER NOT NULL,workflow TEXT NOT NULL,identity TEXT NOT NULL,run_directory TEXT UNIQUE NOT NULL,input TEXT NOT NULL,wall_seconds INTEGER NOT NULL,state TEXT NOT NULL,output TEXT,error TEXT,PRIMARY KEY(instance,path,revision))',
    'CREATE TABLE continuations(instance TEXT NOT NULL,request_key TEXT NOT NULL,path TEXT NOT NULL,input TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(instance,request_key))',
    'CREATE TABLE allocations(instance TEXT NOT NULL,run_id TEXT NOT NULL,call_id TEXT NOT NULL,amount INTEGER NOT NULL,PRIMARY KEY(instance,run_id,call_id))',
    'CREATE TABLE scopes(instance TEXT NOT NULL,path TEXT NOT NULL,limits TEXT NOT NULL,deadline INTEGER NOT NULL,PRIMARY KEY(instance,path))',
    'CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,instance TEXT NOT NULL,path TEXT,event TEXT NOT NULL,state TEXT NOT NULL,created INTEGER NOT NULL)',
))
_ARCHIVES = "CREATE TABLE archives(id TEXT PRIMARY KEY,principal TEXT NOT NULL,request_key TEXT NOT NULL,plan TEXT NOT NULL,identity TEXT NOT NULL,input_digest TEXT NOT NULL,state TEXT NOT NULL,error TEXT,created INTEGER NOT NULL,deadline INTEGER NOT NULL,cancel_requested INTEGER NOT NULL,archived_at INTEGER NOT NULL,usage TEXT NOT NULL,output_digest TEXT,event_floor INTEGER NOT NULL,metadata TEXT NOT NULL DEFAULT '{}',clock TEXT,UNIQUE(principal,request_key))"
_METADATA = "ALTER TABLE instances ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'"
_CLOCK = 'ALTER TABLE instances ADD COLUMN clock TEXT'
_CONTINUATION_TOMBSTONES = 'CREATE TABLE continuation_tombstones(instance TEXT NOT NULL,request_key TEXT NOT NULL,path TEXT NOT NULL,input_digest TEXT NOT NULL,revision INTEGER NOT NULL,metadata_digest TEXT NOT NULL,PRIMARY KEY(instance,request_key))'
_CONTINUATION_METADATA = "ALTER TABLE continuations ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'"
_JOIN_DECISIONS = 'CREATE TABLE join_decisions(instance TEXT NOT NULL,path TEXT NOT NULL,winners TEXT NOT NULL,obsolete TEXT NOT NULL,PRIMARY KEY(instance,path))'
_CANCELLED_SCOPES = 'CREATE TABLE cancelled_scopes(instance TEXT NOT NULL,path TEXT NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(instance,path))'
_SCOPE_CLOCK = 'ALTER TABLE scopes ADD COLUMN clock TEXT'
_EVENT_DATA = 'ALTER TABLE events ADD COLUMN data TEXT'
_V2_MIGRATION = (_METADATA, _CLOCK, _ARCHIVES, _CONTINUATION_TOMBSTONES,
                 _JOIN_DECISIONS, _CANCELLED_SCOPES, _SCOPE_CLOCK, _EVENT_DATA, _CONTINUATION_METADATA)
COMPOSITION_V2 = Schema('workflow composition', 0x4D415357, 2, (),
                     (*COMPOSITION_V1.statements, *_V2_MIGRATION),
                     ((COMPOSITION_V1, _V2_MIGRATION),))

COMPOSITION_V3 = Schema('workflow composition', 0x4D415357, 3, (),
                     (*COMPOSITION_V2.statements, DEADLINE_AMENDMENTS),
                     ((COMPOSITION_V1, (*_V2_MIGRATION, DEADLINE_AMENDMENTS)),
                      (COMPOSITION_V2, (DEADLINE_AMENDMENTS,))))

COMPOSITION_V4 = Schema('workflow composition', 0x4D415357, 4, (),
                     (*COMPOSITION_V3.statements, CONTROL_PROGRESS),
                     ((COMPOSITION_V1, (*_V2_MIGRATION, DEADLINE_AMENDMENTS, CONTROL_PROGRESS)),
                      (COMPOSITION_V2, (DEADLINE_AMENDMENTS, CONTROL_PROGRESS)),
                      (COMPOSITION_V3, (CONTROL_PROGRESS,))))

_INSTANCE_COLUMNS = 'id,principal,request_key,plan,identity,input,state,output,error,created,deadline,cancel_requested,metadata,clock'
_ARCHIVE_FIELDS = 'id,principal,request_key,plan,identity,input_digest,state,error,created,deadline,cancel_requested,archived_at,usage,output_digest,event_floor,metadata,clock'
_NEW_INSTANCES = COMPOSITION_V1.statements[0].replace(',UNIQUE(principal,request_key))',
    ",metadata TEXT NOT NULL DEFAULT '{}',clock TEXT,request_namespace TEXT NOT NULL DEFAULT 'legacy',request_epoch TEXT NOT NULL DEFAULT '',UNIQUE(principal,request_namespace,request_epoch,request_key))")
_NEW_ARCHIVES = _ARCHIVES.replace(',UNIQUE(principal,request_key))',
    ",request_namespace TEXT NOT NULL DEFAULT 'legacy',request_epoch TEXT NOT NULL DEFAULT '',UNIQUE(principal,request_namespace,request_epoch,request_key))")
_NAMESPACE_MIGRATION = (
    'ALTER TABLE instances RENAME TO instances_legacy', _NEW_INSTANCES,
    f'INSERT INTO instances({_INSTANCE_COLUMNS}) SELECT {_INSTANCE_COLUMNS} FROM instances_legacy',
    'DROP TABLE instances_legacy',
    'ALTER TABLE archives RENAME TO archives_legacy', _NEW_ARCHIVES,
    f'INSERT INTO archives({_ARCHIVE_FIELDS}) SELECT {_ARCHIVE_FIELDS} FROM archives_legacy',
    'DROP TABLE archives_legacy', OWNER_EPOCHS,
)
COMPOSITION = Schema('workflow composition', 0x4D415357, 5, (),
                     (*COMPOSITION_V4.statements, *_NAMESPACE_MIGRATION),
                     ((COMPOSITION_V1, (*_V2_MIGRATION, DEADLINE_AMENDMENTS, CONTROL_PROGRESS, *_NAMESPACE_MIGRATION)),
                      (COMPOSITION_V2, (DEADLINE_AMENDMENTS, CONTROL_PROGRESS, *_NAMESPACE_MIGRATION)),
                      (COMPOSITION_V3, (CONTROL_PROGRESS, *_NAMESPACE_MIGRATION)),
                      (COMPOSITION_V4, _NAMESPACE_MIGRATION)))


def encode(value):
    return canonical_json(value)


def decode(row):
    if row is None:
        return None
    result = dict(row)
    for key in ('input', 'output', 'error', 'metadata', 'clock'):
        if key in result and result[key] is not None:
            result[key] = json.loads(result[key])
    return result


def event(db, instance, path, name, state):
    db.execute('INSERT INTO events(instance,path,event,state,created) VALUES(?,?,?,?,?)',
               (instance, path, name, state, int(time.time())))


class WorkflowStore:
    """Composition journal and atomic transitions, without execution authority.

    Callers authorise owners and resolve workflow bindings before writing. Each
    transaction owns its connection, so no SQLite owner crosses a process fork.
    Read transactions retain one snapshot across related observations.
    """
    @staticmethod
    def extend_deadline(db, item, command, timestamp):
        from .deadline_amendments import apply, replay
        previous = replay(db, item, command)
        if previous is not None:
            return previous
        if db.execute("SELECT 1 FROM attempts WHERE instance=? AND state IN ('running','unknown') LIMIT 1",
                      (item['id'],)).fetchone():
            raise ServiceError('deadline_extension_not_available')
        from .execution_policy import clock_status
        # Every ancestor stays authoritative. Extending a child cannot reopen
        # an expired root or another expired scope in the same composition.
        scopes = list(db.execute('SELECT clock,deadline FROM scopes WHERE instance=?', (item['id'],)))
        clocks = [item.get('clock')] + [json.loads(row['clock']) for row in scopes if row['clock']]
        if (item['deadline'] * 1000 <= timestamp
                or any(not row['clock'] and row['deadline'] * 1000 <= timestamp for row in scopes)
                or any(clock and clock['phase'] != 'terminal' and clock_status(clock, timestamp)['expired'] for clock in clocks)):
            raise ServiceError('deadline_extension_not_available')
        scope = None
        if command['path'] is not None:
            scope = decode(db.execute('SELECT * FROM scopes WHERE instance=? AND path=?',
                           (item['id'], command['path'])).fetchone())
            if scope is None:
                raise ServiceError('deadline_scope_not_found')
        clock, receipt = apply(db, item, command, timestamp, scope=scope)
        if clock is not None:
            deadline = (receipt['business_expires_ms'] + 999) // 1000
            if scope is None:
                db.execute('UPDATE instances SET clock=?,deadline=? WHERE id=?',
                           (encode(clock), deadline, item['id']))
            else:
                db.execute('UPDATE scopes SET clock=?,deadline=? WHERE instance=? AND path=?',
                           (encode(clock), deadline, item['id'], command['path']))
            db.execute('INSERT INTO events(instance,path,event,state,created,data) VALUES(?,?,?,?,?,?)',
                       (item['id'], command['path'], 'deadline_extended', item['state'], timestamp // 1000, encode(receipt)))
        return receipt

    def __init__(self, state_dir: Path, *, readonly=False):
        self.path = Path(state_dir) / 'composition.sqlite'
        self.readonly = readonly
        if readonly:
            db = connect(self.path, COMPOSITION, readonly=True)
            db.close()
            return
        from .operations import _file_lock
        with _file_lock(Path(state_dir) / '.composition-init.lock', True):
            db = connect(self.path, COMPOSITION, initialise=lambda _db: None)
            db.close()

    @contextmanager
    def transaction(self, *, readonly=False):
        if self.readonly and not readonly:
            raise SchemaError('workflow composition was opened read-only')
        db = connect(self.path, COMPOSITION, readonly=readonly)
        try:
            db.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
            yield db
            if not readonly:
                db.commit()
        except BaseException:
            if not readonly:
                db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def instance(db, instance, principal=None, *, include_input=False):
        columns = 'id,principal,request_key,plan,identity,state,output,error,created,deadline,cancel_requested'
        available = {row[1] for row in db.execute('PRAGMA table_info(instances)')}
        for column in ('metadata', 'clock', 'request_namespace', 'request_epoch'):
            if column in available:
                columns += ',' + column
        if include_input:
            columns += ',input'
        query, values = f'SELECT {columns} FROM instances WHERE id=?', (instance,)
        if principal is not None:
            query += ' AND principal=?'
            values += (principal,)
        result = decode(db.execute(query, values).fetchone())
        if result is not None:
            result.setdefault('metadata', {})
            result.setdefault('clock', None)
            return result
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='archives'").fetchone():
            return None
        query = 'SELECT * FROM archives WHERE id=?'
        if principal is not None:
            query += ' AND principal=?'
        return WorkflowStore.archived_row(db.execute(query, values).fetchone())

    def read_instance(self, instance):
        with self.transaction(readonly=True) as db:
            return self.instance(db, instance, include_input=True)

    @staticmethod
    def submitted(db, principal, request_key, *, scope=RequestScope()):
        item = decode(db.execute('SELECT * FROM instances WHERE principal=? AND request_key=? '
                                 'AND request_namespace=? AND request_epoch=?',
                                 (principal, request_key, *scope.values)).fetchone())
        if item is not None or not db.execute("SELECT 1 FROM sqlite_master WHERE name='archives'").fetchone():
            return item
        return WorkflowStore.archived_row(db.execute(
            'SELECT * FROM archives WHERE principal=? AND request_key=? AND request_namespace=? AND request_epoch=?',
            (principal, request_key, *scope.values)).fetchone())

    @staticmethod
    def archived_row(row):
        item = decode(row)
        if item is not None:
            item.update(archived=True, output=None, result_available=False, usage=json.loads(item['usage']))
        return item

    @staticmethod
    def submission_matches(item, input):
        if item.get('archived'):
            return item['input_digest'] == hashlib.sha256(encode(input).encode()).hexdigest()
        return encode(item['input']) == encode(input)

    @staticmethod
    def create_instance(db, principal, request_key, plan, identity, input, *, wall_seconds, maximum, metadata=None, clock=None, scope=RequestScope()):
        if db.execute('SELECT count(*) FROM instances').fetchone()[0] >= maximum:
            raise ServiceError('workflow_limit')
        instance, now = 'flow-' + uuid.uuid4().hex, int(time.time())
        db.execute("INSERT INTO instances(id,principal,request_key,plan,identity,input,state,created,deadline,metadata,clock,request_namespace,request_epoch) "
                   "VALUES(?,?,?,?,?,?,'queued',?,?,?,?,?,?)",
                   (instance, principal, request_key, plan, identity, encode(input), now, now + wall_seconds,
                    encode(metadata or {}), encode(clock) if clock is not None else None, *scope.values))
        event(db, instance, None, 'submitted', 'queued')
        return instance

    @staticmethod
    def graph(db, item):
        instance = item['id']
        if item.get('archived'):
            return {'instance_id': instance, 'plan': item['plan'], 'identity': item['identity'],
                    'state': item['state'], 'output': None, 'error': item['error'],
                    'request_namespace': item.get('request_namespace', 'legacy'), 'request_epoch': item.get('request_epoch', ''),
                    'deadline': item['deadline'], 'cancel_requested': bool(item['cancel_requested']),
                    'usage': item['usage'], 'nodes': [], 'attempts': [], 'archived': True,
                    'result_available': False, 'archived_at': item['archived_at'],
                    'metadata': item['metadata'], 'created': item['created'], 'updated': item['archived_at'],
                    **({'clock': item['clock']} if item.get('clock') is not None else {}),
                    'output_digest': item['output_digest']}
        # Project before decoding: retained payloads can be much larger than
        # this graph, especially across many continuation revisions.
        nodes = [decode(row) for row in db.execute(
            'SELECT path,kind,state,error FROM nodes WHERE instance=? ORDER BY path', (instance,))]
        attempts = [decode(row) for row in db.execute(
            'SELECT path,revision,run_directory,state,error FROM attempts WHERE instance=? ORDER BY path,revision',
            (instance,))]
        usage = db.execute('SELECT count(*),coalesce(sum(amount),0) FROM allocations WHERE instance=?',
                           (instance,)).fetchone()
        updated = db.execute('SELECT coalesce(max(created),?) FROM events WHERE instance=?',
                             (item['created'], instance)).fetchone()[0]
        return {'instance_id': instance, 'plan': item['plan'], 'identity': item['identity'],
                'state': item['state'], 'output': item['output'], 'error': item['error'],
                'request_namespace': item.get('request_namespace', 'legacy'), 'request_epoch': item.get('request_epoch', ''),
                'deadline': item['deadline'], 'cancel_requested': bool(item['cancel_requested']),
                'usage': {'steps': len(nodes), 'calls': usage[0], 'reserved_microusd': usage[1]},
                'nodes': nodes, 'attempts': attempts, 'metadata': item.get('metadata', {}),
                'created': item['created'], 'updated': updated,
                **({'clock': item['clock']} if item.get('clock') is not None else {})}

    @staticmethod
    def events(db, instance, after, limit):
        item = WorkflowStore.instance(db, instance)
        if item and item.get('archived') and after < item['event_floor']:
            raise ServiceError('event_cursor_expired')
        result = []
        for row in db.execute('SELECT * FROM events WHERE instance=? AND id>? ORDER BY id LIMIT ?', (instance, after, limit)):
            value = dict(row)
            data = value.pop('data', None)
            if data is not None:
                value['data'] = json.loads(data)
            result.append(value)
        return result

    def protocol_event(self, instance, attempt, item):
        data = {key: item[key] for key in ('sender', 'recipient', 'kind', 'message_id', 'task_id') if key in item}
        if not isinstance(data.get('message_id'), str) or any(type(value) is not str or len(value) > 256 for value in data.values()):
            raise ServiceError('invalid_protocol_event')
        data.update(run_id=attempt['run_directory'], revision=attempt['revision'])
        with self.transaction() as db:
            if db.execute("SELECT 1 FROM events WHERE instance=? AND event='protocol' "
                          "AND json_extract(data,'$.run_id')=? AND json_extract(data,'$.message_id')=? LIMIT 1",
                          (instance, data['run_id'], data['message_id'])).fetchone():
                return
            db.execute('INSERT INTO events(instance,path,event,state,created,data) VALUES(?,?,?,?,?,?)',
                       (instance, attempt['path'], 'protocol', 'running', int(time.time()), encode(data)))

    @classmethod
    def archive(cls, db, item):
        """Compact settled history, preserving request identity indefinitely.

        The caller holds exclusive deployment ownership and has verified all
        referenced runs and remote effects are settled. Lifetime allocations
        remain in this repository and in the deployment ledger.
        """
        if item['state'] not in FINISHED_STATES or item.get('archived'):
            raise ServiceError('workflow_not_archivable')
        graph = cls.graph(db, item)
        floor = db.execute('SELECT coalesce(max(id),0) FROM events WHERE instance=?', (item['id'],)).fetchone()[0]
        digest = lambda value: hashlib.sha256(encode(value).encode()).hexdigest()
        db.execute('INSERT INTO archives VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (item['id'], item['principal'], item['request_key'], item['plan'], item['identity'],
                    digest(item['input']), item['state'], encode({'archived': True, 'sha256': digest(item['error'])}) if item['error'] is not None else None,
                    item['created'], item['deadline'], item['cancel_requested'], int(time.time()),
                    encode(graph['usage']), digest(item['output']) if item['output'] is not None else None, floor,
                    encode(item.get('metadata', {})), encode(item['clock']) if item.get('clock') is not None else None,
                    *RequestScope.retained(item).values))
        for row in db.execute('SELECT * FROM continuations WHERE instance=?', (item['id'],)).fetchall():
            db.execute('INSERT INTO continuation_tombstones VALUES(?,?,?,?,?,?)',
                       (item['id'], row['request_key'], row['path'],
                        digest(json.loads(row['input'])), row['revision'], digest(json.loads(row['metadata']))))
        for table in ('nodes', 'attempts', 'continuations', 'scopes', 'events', 'join_decisions', 'cancelled_scopes', 'control_progress'):
            db.execute(f'DELETE FROM {table} WHERE instance=?', (item['id'],))
        db.execute('DELETE FROM instances WHERE id=?', (item['id'],))
        event(db, item['id'], None, 'archived', item['state'])

    @staticmethod
    def task_detail(db, instance, path, revision=None):
        columns = 'path,revision,workflow,identity,run_directory,state,output,error'
        query, values = f'SELECT {columns} FROM attempts WHERE instance=? AND path=?', (instance, path)
        if revision is not None:
            query += ' AND revision=?'
            values += (revision,)
        return decode(db.execute(query + ' ORDER BY revision DESC LIMIT 1', values).fetchone())

    @staticmethod
    def latest_attempt(db, instance, path):
        return decode(db.execute('SELECT * FROM attempts WHERE instance=? AND path=? '
                                 'ORDER BY revision DESC LIMIT 1', (instance, path)).fetchone())

    @classmethod
    def request_cancellation(cls, db, item):
        if item['state'] in FINISHED_STATES:
            return
        instance, state = item['id'], item['state']
        db.execute('UPDATE instances SET cancel_requested=1 WHERE id=?', (instance,))
        # A running owner must classify and drain its workers first. Unknown
        # effects survive cancellation, even though future local work closes.
        if state in {'queued', 'blocked', 'input_required', 'auth_required', 'unknown'}:
            cls.close_stopped_work(db, instance)
            if state != 'unknown':
                state = 'cancelled'
                db.execute('UPDATE instances SET state=?,output=NULL,error=? WHERE id=?',
                           (state, encode({'code': state}), instance))
                from .service_timing import transition
                transition(db, 'instances', item, 'terminal')
                cls._settle_scope_clocks(db, instance, state)
        if not item['cancel_requested']:
            event(db, instance, None, 'cancellation_requested', state)
        elif state != item['state']:
            event(db, instance, None, 'state_changed', state)

    def block_definition(self, instance, *, uncertain=False):
        """Keep missing code distinct from an unresolved external effect."""
        with self.transaction() as db:
            item = self.instance(db, instance)
            if item is None or item['state'] in FINISHED_STATES:
                return
            uncertain = uncertain or item['state'] == 'unknown'
            state = 'unknown' if uncertain else 'blocked'
            error = {'code': ('definition_unavailable_reconciliation_required'
                              if uncertain else 'definition_unavailable')}
            if item['state'] != state or item['error'] != error:
                db.execute('UPDATE instances SET state=?,error=? WHERE id=?',
                           (state, encode(error), instance))
                event(db, instance, None, 'state_changed', state)

    def progress(self, instance, path, kind):
        from .workflow_progress import progress
        with self.transaction(readonly=True) as db:
            return progress(db, instance, path, kind)

    def checkpoint(self, instance, path, kind, next_index, output, consumed=0):
        from .workflow_progress import checkpoint
        with self.transaction() as db:
            checkpoint(db, instance, path, kind, next_index, output, consumed)

    def node(self, instance, path, kind, input, limits):
        supplied = encode(input)
        def read(db):
            row = decode(db.execute('SELECT * FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone())
            if row is not None and (row['kind'] != kind or encode(row['input']) != supplied):
                raise ServiceError('workflow_replay_conflict')
            return row
        with self.transaction(readonly=True) as db:
            row = read(db)
            if row is not None:
                return row
        with self.transaction() as db:
            row = read(db)
            if row is not None:
                return row
            if db.execute('SELECT count(*) FROM nodes WHERE instance=?', (instance,)).fetchone()[0] >= limits['max_steps']:
                raise ServiceError('workflow_step_limit')
            db.execute("INSERT INTO nodes(instance,path,kind,input,state) VALUES(?,?,?,?,'waiting')",
                       (instance, path, kind, supplied))
            event(db, instance, path, 'node_created', 'waiting')
            return {'state': 'waiting', 'output': None, 'error': None}

    def finish_node(self, instance, path, state, output=None, error=None):
        try:
            source = encode(output) if output is not None else None
        except RunnerError:
            state, source, output = 'failed', None, None
            error = {'code': 'composed_output_not_encodable'}
        failure = encode(error) if error else None
        with self.transaction(readonly=True) as db:
            old = db.execute('SELECT state,output,error FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone()
            if old is not None and tuple(old) == (state, source, failure):
                return state, output
        with self.transaction() as db:
            old = db.execute('SELECT state FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone()
            db.execute('UPDATE nodes SET state=?,output=?,error=? WHERE instance=? AND path=?',
                       (state, source, failure, instance, path))
            if old['state'] != state:
                event(db, instance, path, 'node_state', state)
        return state, output

    def scopes(self, instance, path):
        with self.transaction(readonly=True) as db:
            return [{'path': row['path'], 'limits': json.loads(row['limits']), 'deadline': row['deadline'],
                     'clock': json.loads(row['clock']) if row['clock'] else None}
                    for row in db.execute('SELECT * FROM scopes WHERE instance=? ORDER BY path', (instance,))
                    if path == row['path'] or path.startswith(row['path'] + '/')]

    def scope(self, instance, path, limits, deadline, *, timing_policy=None):
        with self.transaction() as db:
            row = db.execute('SELECT * FROM scopes WHERE instance=? AND path=?', (instance, path)).fetchone()
            if row is None:
                from .execution_policy import timing_policy as policy_from
                policy = policy_from(timing_policy)
                clock = policy.start(int(time.time()*1000)) if policy else None
                bound = min(deadline, int(time.time()) + (policy.business_seconds if policy else limits['wall_seconds']))
                db.execute('INSERT INTO scopes(instance,path,limits,deadline,clock) VALUES(?,?,?,?,?)',
                           (instance, path, encode(limits), bound, encode(clock) if clock else None))
                return bound
            recorded = json.loads(row['clock']) if row['clock'] else None
            if row['limits'] != encode(limits) or (recorded['policy'] if recorded else None) != timing_policy:
                raise ServiceError('workflow_scope_conflict')
            return row['deadline']

    @classmethod
    def assign_attempt(cls, db, instance, path, workflow, identity, revision, input, *, deadline, wall_seconds):
        """Persist either an initial assignment or an explicit continuation."""
        directory = 'run-flow-' + hashlib.sha256(f'{instance}:{path}:{revision}'.encode()).hexdigest()[:40]
        seconds = min(wall_seconds, max(1, math.ceil(deadline - time.time())))
        db.execute('INSERT INTO attempts(instance,path,revision,workflow,identity,run_directory,input,wall_seconds,state) '
                   "VALUES(?,?,?,?,?,?,?,?,'queued')",
                   (instance, path, revision, workflow, identity, directory, encode(input), seconds))
        return cls.latest_attempt(db, instance, path)

    def attempt(self, instance, path, workflow, input, deadline, *, identity, wall_seconds):
        with self.transaction(readonly=True) as db:
            attempt = self.latest_attempt(db, instance, path)
        if attempt is None:
            with self.transaction() as db:
                attempt = self.latest_attempt(db, instance, path)
                if attempt is None:
                    if deadline <= time.time():
                        return None
                    attempt = self.assign_attempt(db, instance, path, workflow, identity, 0, input,
                                                  deadline=deadline, wall_seconds=wall_seconds)
        if attempt['workflow'] != workflow or attempt['identity'] != identity:
            raise ServiceError('workflow_changed')
        return attempt

    def save_attempt(self, attempt, state, output=None, error=None):
        with self.transaction() as db:
            db.execute('UPDATE attempts SET state=?,output=?,error=? WHERE instance=? AND path=? AND revision=? AND identity=?',
                       (state, encode(output) if output is not None else None, encode(error) if error else None,
                        attempt['instance'], attempt['path'], attempt['revision'], attempt['identity']))
            event(db, attempt['instance'], attempt['path'], 'task_state', state)

    def attempt_state(self, attempt):
        with self.transaction(readonly=True) as db:
            return db.execute('SELECT state FROM attempts WHERE instance=? AND path=? AND revision=?',
                              (attempt['instance'], attempt['path'], attempt['revision'])).fetchone()[0]

    def pending_attempts(self, instance):
        """Assignments that need evidence checks before a new owner may run."""
        with self.transaction(readonly=True) as db:
            return [dict(row) for row in db.execute(
                "SELECT instance,path,revision,identity,run_directory,state FROM attempts "
                "WHERE instance=? AND state IN ('running','queued')", (instance,))]

    def start_instance(self, instance, reconciled):
        """Apply the owner's evidence decisions before exposing running state.

        ``reconciled`` contains exact assignments and whether they remain
        uncertain. This journal does not inspect external-effect evidence.
        """
        with self.transaction() as db:
            cancelled = [row['path'] for row in db.execute('SELECT path FROM cancelled_scopes WHERE instance=?', (instance,))]
            for attempt, uncertain in reconciled:
                obsolete = any(attempt['path'] == prefix or attempt['path'].startswith(prefix+'/') for prefix in cancelled)
                state = 'unknown' if uncertain else 'cancelled' if obsolete else 'queued'
                reason = ({'code': 'reconciliation_required'} if uncertain else
                          {'code': 'superseded_effect_reconciled'} if obsolete else None)
                db.execute("UPDATE attempts SET state=?,error=? WHERE instance=? AND path=? AND revision=? "
                           "AND identity=? AND state IN ('running','queued')",
                           (state, encode(reason) if reason else None,
                            instance, attempt['path'], attempt['revision'], attempt['identity']))
                if state != attempt['state']:
                    event(db, instance, attempt['path'], 'task_state', state)
            db.execute("UPDATE instances SET state='running' WHERE id=?", (instance,))
            event(db, instance, None, 'running', 'running')

    def finish_instance(self, instance, state, output, *, expired):
        with self.transaction() as db:
            # Cancellation may commit after the scheduler's last observation.
            cancelled = db.execute('SELECT cancel_requested FROM instances WHERE id=?', (instance,)).fetchone()[0]
            uncertain = db.execute("SELECT 1 FROM attempts WHERE instance=? AND state='unknown' LIMIT 1",
                                   (instance,)).fetchone()
            final = 'unknown' if uncertain else 'expired' if expired else 'cancelled' if cancelled else state
            from .service_timing import status, transition
            item = self.instance(db, instance)
            timing = status(item)
            reason = {'code': final}
            if final == 'expired' and timing and timing['expired']:
                reason = {'code': timing['expired'][0], 'expired': timing['expired']}
            elif final == 'expired' and item.get('error', {}) and item['error'].get('code') == 'scope_timing_expired':
                reason = item['error']
            transition(db, 'instances', item, 'paused' if final in {'unknown', 'input_required', 'auth_required'} else 'terminal')
            db.execute('UPDATE instances SET state=?,output=?,error=? WHERE id=?',
                       (final, encode(output) if final == 'completed' else None,
                        encode(reason) if final != 'completed' else None, instance))
            if cancelled or final in {'failed', 'cancelled', 'expired'}:
                self.close_stopped_work(db, instance)
            self._settle_scope_clocks(db, instance, final)
            event(db, instance, None, 'state_changed', final)

    @staticmethod
    def continuation(db, instance, request_key):
        row = db.execute('SELECT * FROM continuations WHERE instance=? AND request_key=?',
                         (instance, request_key)).fetchone()
        if row is not None:
            return dict(row)
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='continuation_tombstones'").fetchone():
            return None
        row = db.execute('SELECT * FROM continuation_tombstones WHERE instance=? AND request_key=?',
                         (instance, request_key)).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def continuation_matches(row, path, input, metadata=None):
        if row['path'] != path:
            return False
        if 'input_digest' in row:
            return (row['input_digest'] == hashlib.sha256(encode(input).encode()).hexdigest()
                    and row['metadata_digest'] == hashlib.sha256(encode(metadata or {}).encode()).hexdigest())
        return row['input'] == encode(input) and row.get('metadata', '{}') == encode(metadata or {})

    @classmethod
    def continue_attempt(cls, db, item, attempt, request_key, supplied, input, *, wall_seconds, metadata=None):
        from .service_timing import require_open, transition
        require_open(item)
        from .execution_policy import transition_clock
        for row in db.execute('SELECT path,clock FROM scopes WHERE instance=? AND clock IS NOT NULL', (item['id'],)).fetchall():
            if attempt['path'] == row['path'] or attempt['path'].startswith(row['path']+'/'):
                clock = json.loads(row['clock'])
                require_open({'clock': clock})
                clock = transition_clock(clock, int(time.time()*1000), 'queued')
                db.execute('UPDATE scopes SET clock=? WHERE instance=? AND path=?', (encode(clock), item['id'], row['path']))
        transition(db, 'instances', item, 'queued')
        revision = attempt['revision'] + 1
        cls.assign_attempt(db, item['id'], attempt['path'], attempt['workflow'], attempt['identity'],
                           revision, input, deadline=item['deadline'], wall_seconds=wall_seconds)
        db.execute('INSERT INTO continuations VALUES(?,?,?,?,?,?)',
                   (item['id'], request_key, attempt['path'], encode(supplied), revision, encode(metadata or {})))
        db.execute("UPDATE instances SET state='queued',error=NULL WHERE id=?", (item['id'],))
        event(db, item['id'], attempt['path'], 'continued', 'queued')

    @staticmethod
    def unknown_attempts(db, instance):
        return [dict(row) for row in db.execute(
            "SELECT path,revision,run_directory FROM attempts WHERE instance=? AND state='unknown'", (instance,))]

    @staticmethod
    def requeue_unknown(db, instance):
        from .service_timing import status, transition
        from .execution_policy import clock_status
        item = WorkflowStore.instance(db, instance)
        timing = status(item)
        scoped = []
        for row in db.execute('SELECT path,clock FROM scopes WHERE instance=? AND clock IS NOT NULL', (instance,)):
            clock = json.loads(row['clock'])
            reasons = clock_status(clock, int(time.time()*1000))['expired']
            if reasons and clock['phase'] != 'terminal':
                scoped.append({'path': row['path'], 'expired': reasons})
        if timing and timing['expired'] or scoped:
            reason = ({'code': timing['expired'][0], 'expired': timing['expired']} if timing and timing['expired']
                      else {'code': 'scope_timing_expired', 'scopes': scoped})
            reason['effect_reconciliation'] = 'resolved'
            db.execute("UPDATE attempts SET state='cancelled',error=? WHERE instance=? AND state='unknown'",
                       (encode({'code': 'effect_reconciled_after_expiry'}), instance))
            WorkflowStore.close_stopped_work(db, instance)
            transition(db, 'instances', item, 'terminal')
            WorkflowStore._settle_scope_clocks(db, instance, 'expired')
            db.execute("UPDATE instances SET state='expired',error=? WHERE id=?", (encode(reason), instance))
            event(db, instance, None, 'recovery_closed', 'expired')
            return
        transition(db, 'instances', item, 'queued')
        cancelled = [row['path'] for row in db.execute('SELECT path FROM cancelled_scopes WHERE instance=?', (instance,))]
        for row in db.execute("SELECT path,revision FROM attempts WHERE instance=? AND state='unknown'", (instance,)).fetchall():
            obsolete = any(row['path'] == prefix or row['path'].startswith(prefix+'/') for prefix in cancelled)
            state = 'cancelled' if obsolete else 'queued'
            error = encode({'code': 'superseded_effect_reconciled'}) if obsolete else None
            db.execute('UPDATE attempts SET state=?,error=? WHERE instance=? AND path=? AND revision=?',
                       (state, error, instance, row['path'], row['revision']))
            if obsolete:
                db.execute("UPDATE nodes SET state='cancelled',error=? WHERE instance=? AND path=?", (error, instance, row['path']))
        db.execute("UPDATE instances SET state='queued',error=NULL WHERE id=?", (instance,))
        event(db, instance, None, 'recovery_queued', 'queued')

    def clock_activity(self, instance, branches):
        """Charge elapsed wall time once while any local leaf is active."""
        from .service_timing import status, transition
        with self.transaction() as db:
            item = self.instance(db, instance)
            observed = status(item)
            if observed and not observed['expired']:
                transition(db, 'instances', item, 'running' if branches else 'queued', branches=branches)
            return observed

    @staticmethod
    def _scope_phase(db, instance, path, final=None):
        node = db.execute('SELECT state FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone()
        if node and node['state'] in {'completed', 'failed', 'cancelled', 'expired'}:
            return 'terminal'
        if final in {'completed', 'failed', 'cancelled', 'expired'}:
            return 'terminal'
        if final in {'input_required', 'auth_required', 'unknown'}:
            return 'paused'
        queued = db.execute("SELECT 1 FROM attempts WHERE instance=? AND (path=? OR substr(path,1,?)=?) "
                            "AND state='queued' LIMIT 1", (instance, path, len(path)+1, path+'/')).fetchone()
        return 'paused' if node and node['state'] in {'input_required', 'auth_required', 'unknown'} and not queued else 'queued'

    @classmethod
    def _settle_scope_clocks(cls, db, instance, final):
        from .execution_policy import transition_clock
        timestamp = int(time.time()*1000)
        for row in db.execute('SELECT path,clock FROM scopes WHERE instance=? AND clock IS NOT NULL', (instance,)).fetchall():
            clock = json.loads(row['clock'])
            if clock['phase'] == 'terminal':
                continue
            clock = transition_clock(clock, timestamp, cls._scope_phase(db, instance, row['path'], final))
            db.execute('UPDATE scopes SET clock=? WHERE instance=? AND path=?', (encode(clock), instance, row['path']))

    def expired_scopes(self, instance):
        from .execution_policy import clock_status
        timestamp = int(time.time()*1000)
        with self.transaction(readonly=True) as db:
            expired = []
            for row in db.execute('SELECT path,clock FROM scopes WHERE instance=? AND clock IS NOT NULL', (instance,)):
                clock = json.loads(row['clock'])
                reasons = clock_status(clock, timestamp)['expired']
                if reasons and clock['phase'] != 'terminal':
                    expired.append({'path': row['path'], 'expired': reasons})
            return expired

    def scope_clock_activity(self, instance, active_paths):
        from .execution_policy import clock_status, transition_clock
        timestamp = int(time.time()*1000)
        with self.transaction() as db:
            expired = []
            for row in db.execute('SELECT path,clock FROM scopes WHERE instance=? AND clock IS NOT NULL', (instance,)).fetchall():
                clock = json.loads(row['clock'])
                if clock['phase'] == 'terminal':
                    continue
                observed = clock_status(clock, timestamp)
                if observed['expired']:
                    expired.append({'path': row['path'], 'expired': observed['expired']})
                    continue
                branches = sum(path == row['path'] or path.startswith(row['path']+'/') for path in active_paths)
                phase = 'running' if branches else self._scope_phase(db, instance, row['path'])
                clock = transition_clock(clock, timestamp, phase, active_branches=branches)
                db.execute('UPDATE scopes SET clock=? WHERE instance=? AND path=?', (encode(clock), instance, row['path']))
            if expired:
                reason = encode({'code': 'scope_timing_expired', 'scopes': expired})
                db.execute('UPDATE instances SET error=? WHERE id=?', (reason, instance))
                db.execute("UPDATE attempts SET state='expired',error=? WHERE instance=? AND state IN ('queued','input_required','auth_required')",
                           (reason, instance))
            return expired

    def join_decision(self, instance, path):
        with self.transaction(readonly=True) as db:
            row = db.execute('SELECT winners,obsolete FROM join_decisions WHERE instance=? AND path=?',
                             (instance, path)).fetchone()
            return {'winners': json.loads(row['winners']), 'obsolete': json.loads(row['obsolete'])} if row else None

    def decide_join(self, instance, path, winners, obsolete):
        """Persist selection before stopping future local sibling dispatch."""
        decision = {'winners': list(winners), 'obsolete': list(obsolete)}
        with self.transaction() as db:
            previous = db.execute('SELECT winners,obsolete FROM join_decisions WHERE instance=? AND path=?',
                                  (instance, path)).fetchone()
            if previous:
                retained = {'winners': json.loads(previous['winners']), 'obsolete': json.loads(previous['obsolete'])}
                if retained != decision:
                    raise ServiceError('workflow_join_conflict')
                return retained
            db.execute('INSERT INTO join_decisions VALUES(?,?,?,?)',
                       (instance, path, encode(decision['winners']), encode(decision['obsolete'])))
            for prefix in decision['obsolete']:
                db.execute('INSERT OR IGNORE INTO cancelled_scopes VALUES(?,?,?)', (instance, prefix, path))
                match = 'instance=? AND (path=? OR substr(path,1,?)=?)'
                values = (instance, prefix, len(prefix)+1, prefix+'/')
                reason = encode({'code': 'scope_superseded', 'decision': path})
                # Running owners must observe cancellation and publish their
                # own result or uncertainty after supervision drains.
                db.execute("UPDATE attempts SET state='cancelled',error=? WHERE " + match +
                           " AND state IN ('queued','input_required','auth_required')", (reason, *values))
                db.execute("UPDATE nodes SET state='cancelled',error=? WHERE " + match +
                           " AND state IN ('waiting','queued','input_required','auth_required')", (reason, *values))
            event(db, instance, path, 'join_decided', 'waiting')
            return decision

    def join_drain_state(self, instance, path):
        with self.transaction(readonly=True) as db:
            row = db.execute('SELECT obsolete FROM join_decisions WHERE instance=? AND path=?', (instance, path)).fetchone()
            if row is None:
                raise ServiceError('workflow_join_missing')
            prefixes = json.loads(row['obsolete'])
            states = {row['state'] for row in db.execute('SELECT path,state FROM attempts WHERE instance=?', (instance,))
                      if any(row['path'] == prefix or row['path'].startswith(prefix+'/') for prefix in prefixes)}
            if 'running' in states:
                return 'waiting'
            if 'unknown' in states:
                return 'unknown'
            if 'queued' in states:
                raise ServiceError('workflow_join_not_closed')
            return 'drained'

    def scope_cancelled(self, instance, path):
        with self.transaction(readonly=True) as db:
            return any(path == row['path'] or path.startswith(row['path']+'/') for row in db.execute(
                'SELECT path FROM cancelled_scopes WHERE instance=?', (instance,)))

    @staticmethod
    def close_stopped_work(db, instance):
        """Close future local work only after the instance's workers have drained.

        Historical revisions and completed or uncertain effects retain their
        evidence. A continuation's latest revision is the only actionable one.
        """
        latest = ('SELECT max(other.revision) FROM attempts other '
                  'WHERE other.instance=attempts.instance AND other.path=attempts.path')
        for row in db.execute('SELECT path,revision FROM attempts WHERE instance=? '
                              "AND state IN ('queued','input_required','auth_required') "
                              f'AND revision=({latest})', (instance,)).fetchall():
            db.execute("UPDATE attempts SET state='cancelled' WHERE instance=? AND path=? AND revision=?",
                       (instance, row['path'], row['revision']))
            event(db, instance, row['path'], 'task_state', 'cancelled')
        # Reconcile the visible graph with closed attempts. Do not conceal a
        # running or unknown descendant beneath a cancelled control node.
        for row in db.execute("SELECT path,kind FROM nodes WHERE instance=? AND state IN "
                              "('waiting','queued','running','input_required','auth_required','unknown')", (instance,)).fetchall():
            path = row['path']
            if row['kind'] == 'task':
                attempt = db.execute('SELECT state FROM attempts WHERE instance=? AND path=? '
                                     'ORDER BY revision DESC LIMIT 1', (instance, path)).fetchone()
                if attempt is None or attempt['state'] != 'cancelled':
                    continue
            elif db.execute("SELECT 1 FROM attempts WHERE instance=? AND state IN ('running','unknown') "
                            'AND (path=? OR substr(path,1,?)=?) LIMIT 1',
                            (instance, path, len(path) + 1, path + '/')).fetchone():
                continue
            db.execute("UPDATE nodes SET state='cancelled',error=? WHERE instance=? AND path=?",
                       (encode({'code': 'cancelled'}), instance, path))
            event(db, instance, path, 'node_state', 'cancelled')


def holds_run(state_dir: Path, run_directory: str) -> bool:
    """Conservative retention: composition history retains all referenced runs.

    A future explicit workflow-history pruning operation may remove these
    references. Ordinary run pruning must not invalidate resumable or inspectable
    composition evidence, including completed child checkpoints.
    """
    path = Path(state_dir) / 'composition.sqlite'
    if not path.exists():
        return False
    db = connect(path, COMPOSITION, readonly=True)
    try:
        return db.execute('SELECT 1 FROM attempts WHERE run_directory=? LIMIT 1',
                          (run_directory,)).fetchone() is not None
    finally:
        db.close()
