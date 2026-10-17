"""Task lifecycle and worker dispatch backed entirely by native shared commands."""
from __future__ import annotations

import hashlib
from pathlib import Path
import uuid

from .codec import is_actor_identity
from .contracts import AdapterInterrupted, AdapterTimeout, RecoveryRequired, RunnerError
from .definitions import canonical
from .request_namespaces import RequestScope, VERSIONED
from .service_contracts import ServiceError
from .shared_definitions import SharedDefinitionRegistry
from .task_service import (ExecutionBinding, ExecutionResult, INTERRUPTED, TERMINAL,
                           STATES, Workflow, _bounded_items, _object, _text)


class SharedTaskService:
    """Trusted registration, public principal boundary and native worker queue.

    ``state_dir`` contains replaceable provider workspaces only. All task,
    execution, request, definition, event and admission records are PostgreSQL
    authority records. Reconstructing this object cannot authorise a takeover.
    """
    def __init__(self, authority, cache_dir, workflows, grants, *, workflow_versions=None,
                 max_pending=1000, max_tasks=10000, max_revisions=32, supervised=True,
                 scheduler_policy=None, principal_quotas=None, gateway_bin=None):
        registered = [*workflows.values(), *(value for versions in (workflow_versions or {}).values() for value in versions)]
        if any(getattr(workflow, 'assessment_policies', None) for workflow in registered):
            raise ServiceError('assessment_profile_requires_local_journal')
        self.authority = self.operations = authority
        self.state_dir = Path(cache_dir).absolute()
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.workflows = dict(workflows)
        self.grants = {p: frozenset(names) for p, names in grants.items()}
        if any(not is_actor_identity(p) for p in grants):
            raise ServiceError('invalid_principal')
        self.supervised = supervised
        self.gateway_bin = gateway_bin
        self.definitions = SharedDefinitionRegistry(authority)
        self.identities = {name: workflow.identity for name, workflow in self.workflows.items()}
        self.workflow_versions = {}
        versions = workflow_versions or {}
        for name in set(self.workflows) | set(versions):
            for workflow in (*versions.get(name, ()), *((self.workflows[name],) if name in self.workflows else ())):
                if not isinstance(workflow, Workflow):
                    raise ServiceError('invalid_workflow')
                self.workflow_versions[name, workflow.identity] = workflow
                self.definitions.remember('workflow', name, workflow.identity,
                    descriptor=workflow.descriptor,
                    implementation_digest=workflow.verified_implementation.digest,
                    implementation_manifest=workflow.verified_implementation.manifest,
                    current=self.identities.get(name) == workflow.identity)
        from .shared_scheduler import configure_policy
        self.capacity = configure_policy(authority, self.grants, max_pending=max_pending,
            max_tasks=max_tasks, max_revisions=max_revisions, scheduler_policy=scheduler_policy,
            principal_quotas=principal_quotas)

    def _authorise(self, principal, workflow=None):
        if principal not in self.grants or workflow is not None and workflow not in self.grants[principal]:
            raise ServiceError('forbidden')

    def _command(self, operation, **parameters):
        return self.authority.command(operation, parameters)['receipt']

    def _owned(self, principal, task_id):
        self._authorise(principal)
        _text(task_id, 'task_id')
        item = self.authority.query('task.inspect', {'principal': principal, 'task': task_id})
        self._authorise(principal, item['workflow'])
        return item

    def _workflow(self, name, identity, *, verify=True):
        item = self.workflow_versions.get((name, identity))
        retained = self.definitions.inspect('workflow', name, identity)
        if (item is None or retained is None or retained['retired']
                or retained['implementation_provenance'] != 'verified_source_package'):
            raise ServiceError('definition_unavailable')
        if verify:
            item.verify_implementation()
        return item

    def _receipt(self, item):
        value = {k: v for k, v in item.items() if k not in {'input', 'principal', 'owner', 'run_id', 'clock', 'started', 'queue_wait_ms', 'event_floor'}}
        workflow = self.workflow_versions.get((item['workflow'], item['workflow_identity']))
        value['capabilities'] = {'cancel': True, 'events': True,
            'update': workflow is not None and workflow.continue_input is not None}
        return value

    @staticmethod
    def _digest(kind, task_id, workflow, value, metadata):
        return hashlib.sha256(canonical({'kind': kind, 'task_id': task_id,
            'workflow': workflow, 'input': value, 'metadata': metadata}).encode()).hexdigest()

    def _deployment(self, expected):
        if expected is not None and expected != self.authority._binding['deployment']:
            raise ServiceError('deployment_identity_mismatch')

    def submit(self, principal, request_key, workflow, input, *, metadata=None,
               expected_identity=None, expected_deployment_id=None, request_epoch=None,
               _request_namespace=None):
        self._authorise(principal, workflow)
        self._deployment(expected_deployment_id)
        _text(request_key, 'request_key')
        scope = RequestScope.supplied(_request_namespace or VERSIONED, request_epoch)
        value, metadata = _object(input), _object(metadata or {})
        digest = self._digest('submit', None, workflow, value, metadata)
        # Replays can remain readable when the registered implementation is no
        # longer installed. Admission atomically rechecks this receipt and epoch.
        duplicate = self.authority.query('task.request', {'principal': principal,
            'namespace': scope.namespace, 'epoch': scope.epoch, 'key': request_key})
        if duplicate:
            if duplicate['digest'] != digest or duplicate['kind'] != 'task':
                raise ServiceError('request_conflict')
            saved = self._owned(principal, duplicate['target'])
            if expected_identity is not None and saved['workflow_identity'] != expected_identity:
                raise ServiceError('workflow_identity_mismatch')
            return self._receipt(saved)
        if workflow not in self.workflows:
            raise ServiceError('definition_unavailable')
        selected = self._workflow(workflow, self.identities[workflow])
        if expected_identity is not None and expected_identity != selected.identity:
            raise ServiceError('workflow_identity_mismatch')
        contract = selected.contract_descriptor or {}
        if 'input_schema' in contract:
            from .operation_contracts import JsonObjectSchema, ContractError
            try:
                JsonObjectSchema(contract['input_schema']).validate(value)
            except ContractError:
                raise ServiceError('workflow_input_schema_mismatch') from None
        item = self._command('task.submit', principal=principal, namespace=scope.namespace,
            epoch=scope.epoch, key=request_key, digest=digest, task='task-' + uuid.uuid4().hex,
            workflow=workflow, identity=selected.identity, input=value, metadata=metadata,
            expected_identity=expected_identity)
        return self._receipt(item)

    def inspect(self, principal, task_id):
        return self._receipt(self._owned(principal, task_id))

    def result(self, principal, task_id):
        receipt = self.inspect(principal, task_id)
        if receipt['state'] != 'completed' or receipt.get('archived'):
            raise ServiceError('result_unavailable')
        return receipt['result']

    def cancel(self, principal, task_id):
        self._owned(principal, task_id)
        return self._receipt(self._command('task.cancel', principal=principal, task=task_id))

    def update(self, principal, task_id, request_key, input, *, metadata=None,
               expected_identity=None, expected_deployment_id=None):
        self._deployment(expected_deployment_id)
        _text(request_key, 'request_key')
        saved = self._owned(principal, task_id)
        if expected_identity is not None and expected_identity != saved['workflow_identity']:
            raise ServiceError('workflow_identity_mismatch')
        value, metadata = _object(input), _object(metadata or {})
        digest = self._digest('update', task_id, saved['workflow'], value, metadata)
        scope = RequestScope.retained(saved)
        duplicate = self.authority.query('task.request', {'principal': principal,
            'namespace': scope.namespace, 'epoch': scope.epoch, 'key': request_key})
        if duplicate:
            if duplicate['digest'] != digest or duplicate['target'] != task_id or duplicate['kind'] != 'task':
                raise ServiceError('request_conflict')
            if expected_identity is not None and saved['workflow_identity'] != expected_identity:
                raise ServiceError('workflow_identity_mismatch')
            return self._receipt(saved)
        workflow = self._workflow(saved['workflow'], saved['workflow_identity'])
        if workflow.continue_input is None:
            raise ServiceError('continuation_not_supported')
        try:
            combined = _object(workflow.continue_input(saved['result'] or {}, value))
        except Exception:
            raise ServiceError('invalid_continuation') from None
        return self._receipt(self._command('task.update', principal=principal, task=task_id,
            key=request_key, digest=digest, input=combined, expected_revision=saved['revision'],
            expected_identity=expected_identity))

    def events_page(self, principal, task_id, after=0, limit=100):
        from .observability import event_page_arguments
        event_page_arguments(after, limit)
        self._owned(principal, task_id)
        page = self.authority.query('task.events', {'principal': principal,
            'task': task_id, 'after': after, 'limit': limit})
        selected, more = _bounded_items(page['events'], limit=limit, overhead=128)
        return {'events': selected, 'next_after': selected[-1]['id'] if selected else after,
                'has_more': more or page['has_more']}

    def events(self, principal, task_id, after=0):
        return self.events_page(principal, task_id, after, 1000)['events']

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        self._authorise(principal)
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ServiceError('invalid_pagination')
        if state is not None and state not in STATES:
            raise ServiceError('invalid_task_state')
        if after is not None and (type(after) not in (tuple, list) or len(after) != 2
                or type(after[0]) is not int or after[0] < 0 or type(after[1]) is not str):
            raise ServiceError('invalid_cursor')
        if updated_after is not None and (type(updated_after) is not int or updated_after < 0):
            raise ServiceError('invalid_timestamp')
        filters = _object(metadata or {})
        if len(filters) > 64 or any(type(x) is not str for x in filters.values()):
            raise ServiceError('invalid_metadata_filter')
        page = self.authority.query('task.list', {'principal': principal,
            'workflows': sorted(self.grants[principal]), 'limit': page_size, 'after': after,
            'state': state, 'metadata': filters, 'updated_after': updated_after})
        selected, more = _bounded_items((self._receipt(x) for x in page['tasks']), limit=page_size, overhead=1024)
        return {'tasks': selected, 'total_size': page['total_size'],
            'next': [selected[-1]['updated_at'], selected[-1]['task_id']] if more else page['next']}

    def list_tasks(self, principal, limit=100, offset=0):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ServiceError('invalid_pagination')
        after, remaining = None, offset
        while True:
            page = self.list_task_page(principal, page_size=min(1000, remaining + limit), after=after)
            if remaining < len(page['tasks']):
                return page['tasks'][remaining:remaining + limit]
            remaining -= len(page['tasks'])
            if page['next'] is None:
                return []
            after = page['next']

    def recover(self, principal, task_id):
        self._owned(principal, task_id)
        return self._receipt(self._command('task.recover', principal=principal, task=task_id))

    def recovery_runs(self, principal, task_id):
        self._owned(principal, task_id)
        return self.authority.query('task.executions', {'principal': principal, 'task': task_id})

    def discover(self, principal):
        self._authorise(principal)
        return {'format': 'mas-task-service/1', 'principal': principal,
            'deployment_id': self.authority._binding['deployment'],
            'capabilities': ['submit', 'inspect', 'result', 'cancel', 'update', 'events_page',
                             'recover', 'pinned_identity', 'request_epochs'],
            'registrations': [{'name': name, 'kind': 'workflow',
                'available': name in self.workflows, 'identity': self.identities.get(name),
                'contract': self.workflows[name].contract_descriptor if name in self.workflows else None}
                for name in sorted(self.grants[principal])]}

    def request_epoch_status(self, principal, epoch):
        self._authorise(principal)
        scope = RequestScope.supplied(VERSIONED, epoch)
        return {'principal': principal, 'namespace': scope.namespace, 'epoch': scope.epoch,
            **self.authority.query('epoch.status', {'principal': principal, 'namespace': scope.namespace, 'epoch': scope.epoch})}

    def retire_request_epoch(self, principal, epoch, *, reason, prune=False, authoriser=None):
        self._authorise(principal)
        scope = RequestScope.supplied(VERSIONED, epoch)
        if not scope.epoch:
            raise ServiceError('request_namespace_not_retirable')
        if (type(reason) is not str or not reason or len(reason.encode()) > 1024
                or type(prune) is not bool or authoriser is not None and
                (type(authoriser) is not str or not authoriser or len(authoriser.encode()) > 256)):
            raise ServiceError('invalid_epoch_retirement')
        return self._command('epoch.retire', principal=principal, namespace=scope.namespace,
            epoch=scope.epoch, reason=reason, authoriser=authoriser or principal, prune=prune)

    def definition_status(self, name, identity):
        record = self.definitions.inspect('workflow', name, identity)
        if record is None:
            raise ServiceError('definition_not_found')
        return {**record, 'available': (name, identity) in self.workflow_versions
            and not record['retired'] and record['implementation_provenance'] == 'verified_source_package',
            'current': self.definitions.current('workflow', name) == identity,
            'implementation_reverified_at_dispatch': True}

    def retire_definition(self, name, identity):
        return self.definitions.retire('workflow', name, identity)

    def pending(self):
        page = self.authority.query('task.pending', {'after': getattr(self, '_pending_cursor', None)})
        self._pending_cursor = page['next']
        return self.capacity.fair_order([item for item in page['tasks']
                                        if not self._blocked_unavailable(item)])

    def _blocked_unavailable(self, item):
        if item['state'] != 'blocked' or item['workflow'] not in self.grants.get(item['principal'], ()):
            return False
        try:
            self._workflow(item['workflow'], item['workflow_identity'])
        except ServiceError as error:
            if error.code == 'definition_unavailable' or error.code.startswith('implementation_'):
                return True
            raise
        return False

    def execute_assignment(self, workflow, execution, *, ancestors=(), cancelled=None):
        """Execute a stable task or composition run through shared admission."""
        if workflow.assessment_policies:
            raise ServiceError('assessment_profile_requires_local_journal')
        from .executable_worker import ProfileExecutor
        workflow.verify_implementation()
        run_id = execution['run_id']
        path = self.state_dir / run_id
        adapters = workflow.adapters
        if workflow.adapter_factory is not None:
            binding = ExecutionBinding(execution['principal'], execution['workflow'], workflow.identity,
                execution['task_id'], execution['revision'], path, self.state_dir)
            adapters = dict(workflow.adapter_factory(binding))
            workflow.verify_adapters(adapters)
        cancelled = cancelled or (lambda: False)
        # Idempotent create with the original run ID also handles death before
        # bootstrap. Its immutable native receipt is never a new effect grant.
        executor = ProfileExecutor(supervised=self.supervised, gateway_bin=self.gateway_bin)
        with self.authority.runner(workflow.topology, adapters, self.state_dir,
                principal=execution['principal'], new_run_id=run_id, ancestors=ancestors,
                workflow_version=workflow.version, max_depth=workflow.max_depth,
                supervised=self.supervised, retry_policy=workflow.retry_policy,
                circuit_breakers=dict(workflow.circuit_breakers) if workflow.circuit_breakers else None,
                executor=_SharedCancellableExecutor(executor, cancelled),
                execution_deadline=execution.get('execution_deadline'),
                event_sink=execution.get('event_sink')) as runner:
            try:
                outcome = runner.request(workflow.sender, workflow.recipient, workflow.operation, execution['input'])
                state = workflow.classify(outcome) if workflow.classify else (
                    'completed' if outcome.status == 'completed' else 'failed')
                if state not in TERMINAL | INTERRUPTED:
                    raise ServiceError('invalid_workflow_state')
                return ExecutionResult(state, _object(outcome.output),
                    error={'code': 'application_unresolved'} if state == 'failed' and not workflow.classify else None)
            except (RecoveryRequired, AdapterInterrupted, AdapterTimeout, RunnerError) as error:
                invocations = self.authority.query('invocations', {'run': run_id})
                uncertain = any(x['state'] in {'executing', 'unknown'} for x in invocations)
                state = 'unknown' if uncertain or isinstance(error, RecoveryRequired) else 'cancelled' if cancelled() else 'failed'
                return ExecutionResult(state, error={'code': 'reconciliation_required' if state == 'unknown' else 'workflow_failed'})

    def expire_pending(self, task_id=None):
        page = self.authority.query('task.expiring', {'task': task_id,
            'after': None if task_id else getattr(self, '_expiry_cursor', None)})
        if task_id is None:
            self._expiry_cursor = page['next']
        return self._command('task.expire', tasks=page['tasks']) if page['tasks'] else []

    def run_once(self, task_id=None):
        if task_id is None:
            self.expire_pending()
            for item in self.pending():
                result = self.run_once(item['task_id'])
                if result is not None and result['state'] != 'blocked':
                    return result
            return None
        expired = self.expire_pending(task_id)
        if task_id is not None:
            for item in expired:
                if item['task_id'] == task_id:
                    return self._receipt(self.authority.query('task.inspect',
                        {'principal': item['principal'], 'task': task_id}))
        candidate = self.authority.query('task.peek', {'task': task_id})
        if candidate is None or self._blocked_unavailable(candidate):
            return None
        if not self.authority.query('task.claimable', {'task': task_id}):
            return None
        owner = 'worker-' + uuid.uuid4().hex
        claim = self.authority.command('task.claim', {'owner': owner, 'task': task_id,
            'workflows': sorted({name for names in self.grants.values() for name in names})})
        execution = claim['receipt']
        if execution is None:
            return None
        if claim['replayed']:
            raise ServiceError('reconciliation_required')
        principal, task = execution['principal'], execution['task_id']
        if type(execution.get('queue_wait_ms')) is int:
            from .telemetry import record
            record(getattr(self.authority, 'observation_dir', None), 'queue',
                   execution['queue_wait_ms'] * 1000, task_id=task,
                   run_id=execution['run_id'], assignment_id=owner)
        timing = execution.get('timing')
        if timing:
            import time
            execution['execution_deadline'] = time.monotonic() + max(0, min(
                timing['remaining_active_ms'], timing['business_expires_ms'] - timing['observed_at_ms'])) / 1000
        execution['event_sink'] = lambda item: self._command('task.protocol', principal=principal,
            task=task, owner=owner, run=execution['run_id'], event=item)
        if execution['workflow'] not in self.grants.get(principal, ()):
            outcome = ExecutionResult('unknown' if execution['started'] else 'failed', error={
                'code': 'permission_revoked_reconciliation_required' if execution['started'] else 'permission_revoked'})
        else:
            try:
                workflow = self._workflow(execution['workflow'], execution['workflow_identity'])
                from .telemetry import span
                with span(getattr(self.authority, 'observation_dir', None), 'dispatch',
                          task_id=task, run_id=execution['run_id'], assignment_id=owner):
                    outcome = self.execute_assignment(workflow, execution,
                        cancelled=lambda: self._owned(principal, task)['cancel_requested'])
            except ServiceError as error:
                if error.code != 'definition_unavailable' and not error.code.startswith('implementation_'):
                    raise
                outcome = ExecutionResult('blocked', error={'code': 'definition_unavailable'})
        return self._receipt(self._command('task.complete', principal=principal, task=task,
            owner=owner, state=outcome.state, result=outcome.result, error=outcome.error))


class _SharedCancellableExecutor:
    def __init__(self, executor, cancelled):
        self.executor, self.cancelled = executor, cancelled

    def execute(self, adapter, context, request, *, deadline=None, tick=None):
        def poll():
            if self.cancelled():
                raise AdapterInterrupted('task cancellation requested')
            if tick:
                tick()
        poll()
        return self.executor.execute(adapter, context, request, deadline=deadline, tick=poll)
