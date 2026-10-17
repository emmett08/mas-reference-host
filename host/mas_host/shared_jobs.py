"""Cursor Cloud jobs whose only admission and lifecycle authority is PostgreSQL."""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import uuid

from .codec import is_actor_identity
from .contracts import RunnerError
from .cursor_cloud import CloudError, CloudJobSpec, identifier, run_snapshot
from .durable_jobs import (DurableJobService, JobStore, binding, job_identifier,
                           receipt, _operator_evidence)
from .journal import canonical
from .service_contracts import ServiceError


def _key(*parts):
    return 'cloud-' + hashlib.sha256(canonical(parts).encode()).hexdigest()


class SharedJobStore:
    """Trusted principal binding with disposable cache paths, no SQLite mirror."""
    def __init__(self, authority, cache_dir, *, principal):
        if not is_actor_identity(principal):
            raise CloudError('invalid shared cloud job principal')
        self.authority, self.principal = authority, principal
        self.state_dir = Path(cache_dir).absolute()

    def _base(self, job_id):
        return {'principal': self.principal, 'job_id': job_identifier(job_id)}

    def _command(self, operation, parameters, *, command_id=None):
        try:
            return self.authority.command('cursor_job.' + operation, parameters,
                                          command_id=command_id)
        except (RunnerError, ServiceError):
            raise CloudError('shared cloud job command rejected; reconcile the original identity') from None

    def _query(self, operation, job_id):
        try:
            return self.authority.query('cursor_job.' + operation, self._base(job_id))
        except (RunnerError, ServiceError):
            raise CloudError('shared cloud job unavailable or principal differs') from None

    def prepare(self, job_id, spec):
        if not isinstance(spec, CloudJobSpec):
            raise CloudError('invalid cloud job specification')
        # Stable across controller cache loss and lost prepare receipts. The
        # persisted binding still detects a changed spec under the same job ID.
        agent = 'bc-' + str(uuid.uuid5(uuid.NAMESPACE_URL, canonical([
            self.authority._binding['deployment'], self.authority.namespace,
            self.principal, job_identifier(job_id)])))
        self._command('prepare', {**self._base(job_id), 'spec': canonical(asdict(spec)),
            'binding': binding(spec), 'agent_id': agent},
            command_id=_key('prepare', self.principal, job_id))
        # The immutable prepare receipt may predate later dispatch/observations.
        return self.status(job_id)

    def status(self, job_id, *, include_result=False):
        return JobStore.public(self._query('row', job_id), include_result=include_result)

    def inspect(self, job_id, *, include_result=False):
        row = self._query('inspect', job_id)
        return JobStore.public(row, include_result=include_result) | {'audit': row['audit']}

    def spec(self, job_id):
        return CloudJobSpec(**json.loads(self._query('row', job_id)['spec']))

    def service(self, client=None):
        return SharedDurableJobService(self, client)


class SharedDurableJobService:
    """Persist effect intent before I/O; never reuse a grant after a lost reply.

    Read observations and explicit cancellation retries retain operation IDs and
    revisions. A response from an older observation cannot overwrite newer state.
    Controller fencing applies at both the begin and completion transactions.
    """
    def __init__(self, store, client=None):
        self.store, self.client = store, client

    def _remote(self):
        if self.client is None:
            raise CloudError('this cloud job operation requires a configured API client')
        return self.client

    def _begin(self, job_id, action, **fields):
        operation_id = 'operation-' + uuid.uuid4().hex
        parameters = {**self.store._base(job_id), 'operation_id': operation_id,
                      'action': action, **fields}
        # A submit identity is bound to the original native call. Other actions
        # are explicit observations or user-requested cancel attempts.
        command = (_key('submit', self.store.principal, job_id, fields['run'], fields['call_id'])
                   if action == 'submit' else _key('begin', operation_id))
        if action == 'submit':
            operation_id = _key('submission-operation', self.store.principal, job_id,
                                fields['run'], fields['call_id'])
            parameters['operation_id'] = operation_id
        result = self.store._command('begin', parameters, command_id=command)
        granted = result['receipt']
        if result['replayed'] and granted['dispatch']:
            # Recover a completed receipt through a read, never dispatch again.
            row = self.store._query('row', job_id)
            if action == 'submit' and row['run_id'] is not None:
                granted = {'dispatch': False, 'row': row}
            else:
                raise CloudError('cloud dispatch receipt was already issued; reconcile original job')
        return operation_id, granted

    def _complete(self, job_id, operation_id, snapshot, evidence):
        return self.store._command('complete', {**self.store._base(job_id),
            'operation_id': operation_id, 'snapshot': snapshot, 'evidence': evidence},
            command_id=_key('complete', self.store.principal, job_id, operation_id))['receipt']

    def submit(self, job_id, spec, *, call_id, host_run_dir):
        identifier(call_id, 'call-')
        run = Path(host_run_dir).name
        if not is_actor_identity(run):
            raise CloudError('shared cloud submission requires its original run identity')
        operation, grant = self._begin(job_id, 'submit', run=run, call_id=call_id, binding=binding(spec))
        row = grant['row']
        if grant['dispatch']:
            response = self._remote().create(spec.payload(row['agent_id']))
            if (type(response) is not dict or type(response.get('agent')) is not dict
                    or response['agent'].get('id') != row['agent_id']):
                raise CloudError('Cursor launch response does not match durable agent identity')
            snapshot = run_snapshot(response.get('run'), row['agent_id'])
            row = self._complete(job_id, operation, snapshot, {'run_id': snapshot['run_id']})
        return DurableJobService._invocation(row, spec)

    def poll(self, job_id):
        retained = self.store._query('row', job_id)
        if retained['state'] == 'terminal':
            return JobStore.public(retained)
        operation, grant = self._begin(job_id, 'poll')
        row = grant['row']
        if grant['dispatch']:
            snapshot = run_snapshot(self._remote().run(row['agent_id'], row['run_id']),
                                    row['agent_id'], row['run_id'])
            row = self._complete(job_id, operation, snapshot, {'remote_status': snapshot['remote_status']})
        return JobStore.public(row)

    def attach(self, job_id, run_id, *, evidence):
        identifier(run_id, 'run-'); _operator_evidence(evidence)
        operation, grant = self._begin(job_id, 'attach', run_id=run_id, evidence=evidence)
        row = grant['row']
        if not grant['dispatch']:
            raise CloudError('only an uncertain cloud launch can be attached')
        spec = CloudJobSpec(**json.loads(row['spec']))
        agent = self._remote().agent(row['agent_id'])
        if (type(agent) is not dict or agent.get('id') != row['agent_id']
                or type(agent.get('repos')) is not list or len(agent['repos']) != 1
                or type(agent['repos'][0]) is not dict
                or agent['repos'][0].get('url') != spec.repository
                or agent['repos'][0].get('startingRef') != spec.ref
                or agent.get('autoCreatePR') is not spec.auto_create_pr
                or agent.get('workOnCurrentBranch') is not False):
            raise CloudError('remote agent configuration does not match durable intent')
        snapshot = run_snapshot(self._remote().run(row['agent_id'], run_id), row['agent_id'], run_id)
        return JobStore.public(self._complete(job_id, operation, snapshot, evidence))

    def cancel(self, job_id, *, retry=False):
        if type(retry) is not bool:
            raise CloudError('cloud cancellation requires explicit retry choice')
        retained = self.store._query('row', job_id)
        if retained['state'] == 'terminal':
            return JobStore.public(retained)
        operation, grant = self._begin(job_id, 'cancel', retry=retry)
        row = grant['row']
        if grant['dispatch']:
            response = self._remote().cancel(row['agent_id'], row['run_id'])
            if type(response) is not dict or response.get('id') != row['run_id']:
                raise CloudError('Cursor cancellation response does not match durable run identity')
            row = self._complete(job_id, operation, None, {'run_id': row['run_id']})
        return JobStore.public(row)

    def resolve_submission(self, job_id, *, evidence):
        _operator_evidence(evidence)
        row = self.store._query('row', job_id)
        output = receipt(row)
        if not row['host_run_dir'] or not row['call_id']:
            raise CloudError('cloud job has no recorded host invocation')
        try:
            return self.store.authority.resolve_invocation(row['host_run_dir'], row['call_id'],
                operator=self.store.principal, decision='completed', output=output,
                evidence={'cloud_job': job_id, 'submission_only': True, 'operator': evidence},
                command_id=_key('resolve-submission', self.store.principal, job_id, evidence))
        except (RunnerError, ServiceError):
            raise CloudError('shared cloud submission resolution rejected') from None
