"""PostgreSQL composition service over the fenced Rust domain command port.

The cache contains only replaceable run locks/workspaces. Graph, assignments,
completed prefixes, definition snapshots, request receipts and ancestor budgets
are authoritative PostgreSQL records. Parallel workers share the fenced graph owner; each retained attempt has its
own native run and every declared ancestor allocation.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import select
from pathlib import Path
import time
import uuid

from .codec import canonical_json
from .contracts import AdapterInterrupted, AdapterTimeout, RecoveryRequired, RunnerError
from .service_contracts import ServiceError
from .task_service import _object, _text
from .workflow_evaluator import PlanEvaluator, FINISHED_STATES, PAUSED_STATES
from .workflow_plan import WorkflowPlan, bind_plan, validate_registry


def _identity(*values):
    return hashlib.sha256(canonical_json(list(values)).encode()).hexdigest()


class SharedWorkflowStore:
    """Explicit evaluator journal; each command owns one shared transaction."""
    def __init__(self, authority, owner, workflows):
        self.authority, self.owner, self.workflows = authority, owner, workflows

    def command(self, operation, instance, **parameters):
        return self.authority.command('composition.' + operation,
            {'instance': instance, 'owner': self.owner, **parameters})['receipt']

    def query(self, operation, instance, **parameters):
        return self.authority.query('composition.' + operation, {'instance': instance, **parameters})

    def node(self, instance, path, kind, input, limits):
        saved = self.query('node', instance, path=path)
        if saved is not None and saved['kind'] == kind and saved['input'] == input:
            return saved
        return self.command('node', instance, path=path, kind=kind, input=input)

    def finish_node(self, instance, path, state, output=None, error=None):
        saved = self.query('node', instance, path=path)
        if saved is not None and (saved['state'], saved['output'], saved['error']) == (state, output, error):
            # The same state can still need its scope clock settled after the
            # last losing parallel branch stops. Reads never suppress that write.
            clock = self.query('clock', instance, path=path) if state in FINISHED_STATES | PAUSED_STATES else None
            expected = 'terminal' if state in FINISHED_STATES else 'paused'
            if clock is None or clock['phase'] == expected or clock['active_branches'] > 0:
                return saved['state'], saved['output']
        saved = self.command('finish_node', instance, path=path, state=state, output=output, error=error)
        return saved['state'], saved['output']

    def progress(self, instance, path, kind):
        saved = self.query('progress', instance, path=path)
        if saved is not None and saved['kind'] != kind:
            raise ServiceError('workflow_progress_conflict')
        return saved

    def checkpoint(self, instance, path, kind, next_index, output, consumed=0):
        return self.command('checkpoint', instance, path=path, kind=kind,
            next_index=next_index, output=output, consumed=consumed)

    def scope(self, instance, path, limits, deadline, *, timing_policy=None):
        saved = self.query('scope', instance, path=path)
        if saved is not None and saved['limits'] == limits:
            return saved['deadline']
        return self.command('scope', instance, path=path, limits=limits,
            deadline=int(deadline), scope_id='ancestor/' + _identity(instance, path), timing_policy=timing_policy)

    def attempt(self, instance, path, workflow, input, deadline):
        leaf = self.workflows[workflow]
        saved = self.query('attempt', instance, path=path)
        if saved is not None and saved['workflow'] == workflow and saved['identity'] == leaf.identity:
            return saved
        return self.command('attempt', instance, path=path, workflow=workflow,
            identity=leaf.identity, input=input, deadline=int(deadline),
            wall_seconds=leaf.topology.get('wall_seconds', 3600),
            run_id='run-' + _identity(instance, path, 0))

    def join_decision(self, instance, path):
        return self.query('join', instance, path=path)

    def decide_join(self, instance, path, winners, obsolete):
        return self.command('decide_join', instance, path=path, winners=winners, obsolete=obsolete)

    def join_drain_state(self, instance, path):
        decision = self.join_decision(instance, path)
        graph = self.query('frontier', instance)
        for attempt in graph['attempts']:
            if any(attempt['path'] == prefix or attempt['path'].startswith(prefix + '/')
                   for prefix in decision['obsolete']):
                if attempt['state'] in {'running', 'unknown'}:
                    return 'unknown'
                if attempt['state'] == 'queued':
                    self.command('save_attempt', instance, path=attempt['path'],
                        run_id=attempt['run_id'], state='cancelled', output=None, error=None)
        return 'drained'


class SharedCompositionService:
    """Trusted composition worker plus protocol-neutral authenticated lifecycle."""
    def __init__(self, authority, cache_dir, plans, workflows, grants, *,
                 workflow_versions=None, supervised=True, max_instances=1000, gateway_bin=None):
        registered = [*workflows.values(), *(value for versions in (workflow_versions or {}).values() for value in versions)]
        if any(getattr(workflow, 'assessment_policies', None) for workflow in registered):
            raise ServiceError('assessment_profile_requires_local_journal')
        self.authority = self.operations = authority
        self.state_dir = Path(cache_dir).absolute()
        self.plans = {name: value if isinstance(value, WorkflowPlan) else WorkflowPlan(value)
                      for name, value in plans.items()}
        self.workflows = dict(workflows)
        validate_registry(self.plans, self.workflows)
        self.bindings = {name: bind_plan(name, self.plans, self.workflows) for name in self.plans}
        self.grants = {principal: frozenset(names) for principal, names in grants.items()}
        self.workflow_versions = {(name, value.identity): value for name, value in self.workflows.items()}
        for name, versions in (workflow_versions or {}).items():
            self.workflow_versions.update({(name, value.identity): value for value in versions})
        self.supervised = supervised
        self.gateway_bin = gateway_bin
        from .shared_scheduler import configure_policy
        self.capacity = configure_policy(authority, self.grants, preserve_existing=True)
        self.authority.command('composition.configure', {'max_instances': max_instances},
            command_id='composition-policy-' + _identity(max_instances))
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        from .shared_task_service import SharedDefinitionRegistry
        self.definitions = SharedDefinitionRegistry(authority)
        for (name, identity), workflow in self.workflow_versions.items():
            self.definitions.remember('workflow', name, identity, descriptor=workflow.descriptor,
                implementation_digest=workflow.verified_implementation.digest,
                implementation_manifest=workflow.verified_implementation.manifest)
        for name, plan in self.plans.items():
            self.definitions.remember('plan_source', name, plan.identity, descriptor=plan.spec)
            self.definitions.remember('plan', name, self.bindings[name]['identity'],
                descriptor={key: value for key, value in self.bindings[name].items() if key != 'identity'}, current=True)

    def _authorise(self, principal, name=None):
        _text(principal, 'principal')
        if principal not in self.grants or name is not None and name not in self.grants[principal]:
            raise ServiceError('forbidden')

    def _graph(self, principal, instance):
        self._authorise(principal)
        graph = self.authority.query('composition.graph', {'instance': instance, 'principal': principal})
        self._authorise(principal, graph['plan'])
        return graph

    def _frontier(self, principal, instance):
        self._authorise(principal)
        value = self.authority.query('composition.frontier', {'instance': instance, 'principal': principal})
        self._authorise(principal, value['plan'])
        return value

    @staticmethod
    def _receipt(graph):
        paused = [{'path': row['path'], 'revision': row['revision'], 'state': row['state']}
                  for row in graph.get('attempts', ()) if row['state'] in {'input_required', 'auth_required'}]
        state = graph['state']
        return {'task_id': graph['id'], 'kind': 'plan', 'workflow': graph['plan'],
            'workflow_identity': graph['identity'], 'request_key': graph['request_key'],
            'request_namespace': graph['request_namespace'], 'request_epoch': graph['request_epoch'],
            'state': {'expired': 'failed', 'blocked_definition': 'blocked'}.get(state, state),
            'created_at': graph['created'] * 1000, 'updated_at': graph['updated'],
            'cancel_requested': graph['cancel_requested'],
            'result': {'pending_tasks': paused} if paused else graph['output'],
            'error': graph['error'], 'metadata': graph['metadata'],
            'revision': graph.get('revision', max((a['revision'] for a in graph.get('attempts', ())), default=0)),
            'capabilities': {'cancel': True, 'update': bool(paused), 'events': True, 'task_detail': True},
            'progress': graph.get('progress', {'nodes': len(graph.get('nodes', ())),
                         'completed_nodes': sum(n['state'] == 'completed' for n in graph.get('nodes', ()))}),
            **({'timing': graph['timing']} if graph.get('timing') else {}),
            **({'archived': True, 'archived_at': graph['archived_at'], 'result_available': False,
                'event_floor': graph['event_floor']} if graph.get('archived') else {})}

    def submit(self, principal, request_key, workflow, input, *, metadata=None,
               expected_identity=None, expected_deployment_id=None, request_epoch=None,
               _request_namespace='mas-unified/1'):
        self._authorise(principal, workflow)
        _text(request_key, 'request_key')
        if expected_deployment_id is not None and expected_deployment_id != self.authority._binding['deployment']:
            raise ServiceError('deployment_identity_mismatch')
        value, metadata = _object(input), _object(metadata or {})
        namespace = _request_namespace or 'mas-unified/1'
        previous = self.authority.query('task.request', {'principal': principal,
            'namespace': namespace, 'epoch': request_epoch or '', 'key': request_key})
        if previous is not None:
            if previous['kind'] != 'composition':
                raise ServiceError('request_conflict')
            saved = self._graph(principal, previous['target'])
            if saved['plan'] != workflow or previous['digest'] != _identity(workflow, value, metadata):
                raise ServiceError('request_conflict')
            if expected_identity is not None and saved['identity'] != expected_identity:
                raise ServiceError('workflow_identity_mismatch')
            return self._receipt(saved)
        plan = self.plans.get(workflow)
        if plan is None:
            raise ServiceError('definition_unavailable')
        binding = self.bindings[workflow]
        if expected_identity is not None and expected_identity != binding['identity']:
            raise ServiceError('workflow_identity_mismatch')
        if self.definitions.current('plan', workflow) != binding['identity']:
            raise ServiceError('workflow_changed')
        if 'input_schema' in plan.spec:
            from .operation_contracts import JsonObjectSchema
            JsonObjectSchema(plan.spec['input_schema']).validate(value)
        frozen = {'binding': binding, 'plans': {name: value.spec for name, value in self.plans.items()}}
        result = self.authority.command('composition.submit', {'instance': 'flow-' + uuid.uuid4().hex,
            'principal': principal, 'request_namespace': namespace,
            'request_epoch': request_epoch or '', 'request_key': request_key,
            'plan': workflow, 'identity': binding['identity'], 'input': value,
            'metadata': metadata, 'binding': frozen, 'limits': plan.spec['limits'],
            'timing_policy': plan.spec.get('timing_policy')})['receipt']
        return self.inspect(principal, result['id'])

    def inspect(self, principal, task_id):
        return self._receipt(self._frontier(principal, task_id))

    def result(self, principal, task_id):
        result = self.inspect(principal, task_id)
        if result['state'] != 'completed' or result.get('archived'):
            raise ServiceError('result_unavailable')
        return result['result']

    def inspect_task(self, principal, task_id, path, revision=None):
        from .workflow_inspection import validate_task_selector
        validate_task_selector(path, revision)
        graph = self._frontier(principal, task_id)
        if graph.get('archived'):
            raise ServiceError('workflow_task_not_found')
        attempt = self.authority.query('composition.attempt', {'instance': task_id,
            'principal': principal, 'path': path, 'revision': revision})
        if attempt is None:
            raise ServiceError('workflow_task_not_found')
        return {'instance_id': task_id, 'plan': graph['plan'], 'instance_state': graph['state'],
                'deadline': graph['deadline'], **attempt}

    def cancel(self, principal, task_id):
        self._frontier(principal, task_id)
        self.authority.command('composition.cancel', {'principal': principal, 'instance': task_id})
        return self.inspect(principal, task_id)

    def events(self, principal, task_id, after=0):
        self._frontier(principal, task_id)
        return self.authority.query('composition.events', {'instance': task_id, 'principal': principal,
                                                         'after': after, 'limit': 1000})

    def events_page(self, principal, task_id, after=0, limit=100):
        self._frontier(principal, task_id)
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ServiceError('invalid_cursor')
        events = self.authority.query('composition.events', {'instance': task_id, 'principal': principal,
                                                            'after': after, 'limit': limit})
        return {'events': events, 'next_after': events[-1]['id'] if events else after,
                'has_more': len(events) == limit}

    def list_tasks(self, principal, limit=100, offset=0):
        if type(limit) is not int or not 1 <= limit <= 1000 or type(offset) is not int or offset < 0:
            raise ServiceError('invalid_page')
        after, remaining = None, offset
        while True:
            page = self.list_task_page(principal, page_size=min(1000, remaining + limit), after=after)
            if remaining < len(page['tasks']):
                return page['tasks'][remaining:remaining + limit]
            remaining -= len(page['tasks'])
            if page['next'] is None:
                return []
            after = page['next']

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        self._authorise(principal)
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ServiceError('invalid_page')
        values = self.authority.query('composition.list', {'principal': principal, 'limit': page_size,
            'after': list(after) if after is not None else None, 'state': state,
            'metadata': _object(metadata or {}), 'updated_after': updated_after,
            'workflows': sorted(self.grants[principal])})
        return {**values, 'tasks': [self._receipt(row) for row in values['tasks']]}

    def discover(self, principal):
        self._authorise(principal)
        return [{'name': name, 'kind': 'plan', 'identity': self.bindings[name]['identity']}
                for name in sorted(self.grants[principal]) if name in self.bindings]

    def deadline_service(self, *, operators):
        from .shared_deadlines import SharedDeadlineService
        return SharedDeadlineService(self, operators=operators)

    def definition_status(self, name, identity):
        record = self.definitions.inspect('plan', name, identity)
        if record is None:
            raise ServiceError('definition_not_found')
        descriptor = record.get('descriptor')
        available = descriptor is not None and not record['retired']
        dependencies = []
        if descriptor:
            for workflow, version in descriptor['workflows'].items():
                retained = self.definitions.inspect('workflow', workflow, version)
                present = ((workflow, version) in self.workflow_versions and retained is not None
                    and not retained['retired'] and retained['implementation_provenance'] == 'verified_source_package')
                dependencies.append({'kind': 'workflow', 'name': workflow, 'identity': version, 'available': present})
                available = available and present
            for plan, version in descriptor['plans'].items():
                retained = self.definitions.inspect('plan_source', plan, version)
                present = retained is not None and retained['descriptor'] is not None and not retained['retired']
                dependencies.append({'kind': 'plan_source', 'name': plan, 'identity': version, 'available': present})
                available = available and present
        return {'kind': 'plan', 'name': name, 'identity': identity,
            'current': self.definitions.current('plan', name) == identity, 'retired': bool(record['retired']),
            'source_present': descriptor is not None, 'available': bool(available), 'dependencies': dependencies,
            'implementation_reverified_at_dispatch': True}

    def retire_definition(self, name, identity):
        return self.definitions.retire('plan', name, identity)

    def _definitions(self, item):
        record = self.definitions.inspect('plan', item['plan'], item['identity'])
        if not record or record['retired']:
            raise ServiceError('definition_unavailable')
        frozen = item['binding']
        plans = {name: WorkflowPlan(value) for name, value in frozen['plans'].items()}
        workflows = {}
        for name, identity in frozen['binding']['workflows'].items():
            value = self.workflow_versions.get((name, identity))
            record = self.definitions.inspect('workflow', name, identity)
            if value is None or not record or record['retired']:
                raise ServiceError('definition_unavailable')
            value.verify_implementation()
            workflows[name] = value
        if bind_plan(item['plan'], plans, workflows)['identity'] != item['identity']:
            raise ServiceError('definition_unavailable')
        return plans, workflows

    @contextmanager
    def _claim(self, item):
        owner = 'composition-owner-' + uuid.uuid4().hex
        self.authority.command('composition.claim', {'instance': item['id'], 'owner': owner})
        try:
            yield owner
        except BaseException:
            try:
                self.authority.command('composition.release', {'instance': item['id'], 'owner': owner})
            except RunnerError:
                pass
            raise
        else:
            self.authority.command('composition.release', {'instance': item['id'], 'owner': owner})

    def _execute(self, store, item, attempt, lifetime_fd=None, descriptors=()):
        workflow = store.workflows[attempt['workflow']]
        workflow.verify_implementation()
        adapters = workflow.adapters
        if workflow.adapter_factory is not None:
            from .task_service import ExecutionBinding
            binding = ExecutionBinding(item['principal'], attempt['workflow'], workflow.identity,
                item['id'], attempt['revision'], self.state_dir / attempt['run_id'], self.state_dir)
            adapters = dict(workflow.adapter_factory(binding))
            workflow.verify_adapters(adapters)
        topology = {**workflow.topology, 'wall_seconds': attempt['wall_seconds']}
        from .executable_worker import ProfileExecutor
        def stopped():
            saved = store.query('instance', item['id'])
            return (lifetime_fd is not None and bool(select.select([lifetime_fd], [], [], 0)[0])
                or saved['cancel_requested'] or saved['deadline'] <= time.time()
                or bool(saved.get('timing', {}).get('expired') if saved.get('timing') else False)
                or store.query('cancelled', item['id'], path=attempt['path'])
                or any(scope.get('timing') and scope['timing']['expired'] for scope in store.query('scopes', item['id'])
                    if attempt['path'] == scope['path'] or attempt['path'].startswith(scope['path'] + '/')))
        class CancellableExecutor:
            def __init__(self, executor):
                self.executor = executor
            def execute(self, adapter, context, request, *, deadline, tick=None):
                def poll():
                    if stopped():
                        raise AdapterInterrupted('composition cancellation requested')
                    if tick:
                        tick()
                poll()
                if descriptors:
                    from .task_service import _OwnedContext
                    context = _OwnedContext(context, descriptors)
                return self.executor.execute(adapter, context, request, deadline=deadline, tick=poll)
        executor = CancellableExecutor(ProfileExecutor(supervised=self.supervised, gateway_bin=self.gateway_bin))
        scopes = [scope for scope in store.query('scopes', item['id'])
                  if attempt['path'] == scope['path'] or attempt['path'].startswith(scope['path'] + '/')]
        deadline = min([item['deadline'], *[scope['deadline'] for scope in scopes]])
        budgets = [entry['timing']['remaining_active_ms'] / 1000 for entry in [item, *scopes] if entry.get('timing')]
        execution_deadline = time.monotonic() + min([max(0, deadline - time.time()), *budgets])
        from .telemetry import span
        try:
            with span(getattr(self.authority, 'observation_dir', None), 'dispatch',
                      task_id=item['id'], run_id=attempt['run_id'],
                      assignment_id=_identity(item['id'], attempt['path'], attempt['revision'])), self.authority.runner(topology, adapters, self.state_dir,
                    principal=item['principal'], new_run_id=attempt['run_id'],
                    ancestors=attempt['ancestors'], workflow_version=workflow.version,
                    max_depth=workflow.max_depth, retry_policy=workflow.retry_policy,
                    circuit_breakers=workflow.circuit_breakers, executor=executor,
                    event_sink=lambda event: store.command('protocol', item['id'], run_id=attempt['run_id'], event=event),
                    execution_deadline=execution_deadline) as runner:
                result = runner.request(workflow.sender, workflow.recipient, workflow.operation, attempt['input'])
                state = workflow.classify(result) if workflow.classify else ('completed' if result.status == 'completed' else 'failed')
                if state not in FINISHED_STATES | PAUSED_STATES:
                    raise ServiceError('invalid_workflow_state')
                output, error = _object(result.output), None
        except (RecoveryRequired, RunnerError, AdapterInterrupted, AdapterTimeout) as failure:
            invocations = self.authority.query('invocations', {'run': attempt['run_id']})
            uncertain = isinstance(failure, RecoveryRequired) or any(v['state'] in {'executing', 'unknown'} for v in invocations)
            state = 'unknown' if uncertain else 'cancelled' if stopped() else 'failed'
            output, error = None, {'code': 'reconciliation_required' if uncertain else 'workflow_failed'}
        return store.command('save_attempt', item['id'], path=attempt['path'], run_id=attempt['run_id'],
                             state=state, output=output, error=error)

    def run(self, principal, task_id):
        item = self._frontier(principal, task_id)
        scopes = self.authority.query('composition.scopes', {'instance': task_id})
        expired = bool(item.get('timing') and item['timing']['expired']) or any(
            scope.get('state') not in FINISHED_STATES and (scope['deadline'] <= time.time()
                or scope.get('timing') and scope['timing']['expired']) for scope in scopes)
        if item['state'] in FINISHED_STATES or item['state'] in PAUSED_STATES and not expired:
            return self._receipt(item)
        if item['state'] == 'blocked_definition' and not expired:
            try:
                self._definitions(item)
            except ServiceError:
                return self._receipt(item)
        from .workflow_scheduler import SchedulerOwnership
        with self._claim(item) as owner, SchedulerOwnership(self.state_dir, task_id) as ownership:
            if item['cancel_requested'] or expired or item['deadline'] <= time.time():
                store = SharedWorkflowStore(self.authority, owner, {})
                uncertain = any(attempt['state'] in {'running', 'unknown'} for attempt in item['attempts'])
                state = 'unknown' if uncertain else 'cancelled' if item['cancel_requested'] else 'expired'
                store.command('finish', task_id, state=state, output=None,
                    error={'code': 'reconciliation_required' if uncertain else 'deadline_exceeded'} if uncertain or expired else None)
                return self.inspect(principal, task_id)
            try:
                plans, workflows = self._definitions(item)
            except ServiceError:
                store = SharedWorkflowStore(self.authority, owner, {})
                store.command('finish', task_id, state='blocked_definition', output=None,
                              error={'code': 'definition_unavailable'})
                return self.inspect(principal, task_id)
            store = SharedWorkflowStore(self.authority, owner, workflows)
            evaluator = PlanEvaluator(plans, store)
            spec, live = plans[item['plan']].spec, {}
            while True:
                for key in ownership.poll():
                    attempt = live.pop(key)
                    saved = store.query('attempt', task_id, path=attempt['path'])
                    if saved['state'] == 'running':
                        store.command('save_attempt', task_id, path=attempt['path'], run_id=attempt['run_id'],
                            state='unknown', output=None, error={'code': 'worker_interrupted'})
                item = store.query('frontier', task_id)
                scopes = store.query('scopes', task_id)
                expired = (item['deadline'] <= time.time()
                    or bool(item.get('timing') and item['timing']['expired'])
                    or any(scope['deadline'] <= time.time() or scope.get('timing') and scope['timing']['expired']
                           for scope in scopes if scope.get('state') not in FINISHED_STATES))
                if item['cancel_requested'] or expired:
                    if live:
                        ownership.wait(.02)
                        continue
                    uncertain = any(a['state'] == 'unknown' for a in item['attempts'])
                    state = 'unknown' if uncertain else 'cancelled' if item['cancel_requested'] else 'expired'
                    store.command('finish', task_id, state=state, output=None,
                                  error={'code': 'reconciliation_required' if uncertain else 'deadline_exceeded'} if uncertain or expired else None)
                    break
                ready = []
                state, output = evaluator.advance(task_id, 'root', spec['root'], item['input'],
                                                   spec['limits'], item['deadline'], ready)
                if state in FINISHED_STATES | PAUSED_STATES:
                    if live:
                        ownership.wait(.02)
                        continue
                    if state == 'completed' and 'output_schema' in spec:
                        from .operation_contracts import JsonObjectSchema
                        JsonObjectSchema(spec['output_schema']).validate(output)
                    store.command('finish', task_id, state=state, output=output, error=None)
                    break
                for candidate in ready:
                    if len(live) >= spec['limits']['max_parallel']:
                        break
                    if candidate['path'] in live:
                        continue
                    lineage = [scope for scope in scopes if candidate['path'] == scope['path']
                               or candidate['path'].startswith(scope['path'] + '/')]
                    if any(sum(a['path'] == scope['path'] or a['path'].startswith(scope['path'] + '/')
                               for a in live.values()) >= scope['limits']['max_parallel'] for scope in lineage):
                        continue
                    if not store.query('attempt_ready', task_id, path=candidate['path'], owner=owner):
                        continue
                    attempt = store.command('start_attempt', task_id, path=candidate['path'], run_id=candidate['run_id'])
                    if attempt is None:
                        continue
                    from .telemetry import record
                    record(getattr(self.authority, 'observation_dir', None), 'queue',
                        (attempt.get('queue_elapsed_ms') or 0) * 1000, outcome='observed',
                        task_id=task_id, run_id=attempt['run_id'],
                        assignment_id=_identity(task_id, attempt['path'], attempt['revision']))
                    live[attempt['path']] = attempt
                    ownership.spawn(attempt['path'], lambda lifetime, descriptors, attempt=attempt:
                        self._execute(store, item, attempt, lifetime, descriptors))
                if not live:
                    store.command('finish', task_id, state='queued' if ready else state, output=output, error=None)
                    break
                ownership.wait(.02)
        return self.inspect(principal, task_id)

    def pending(self):
        page = self.authority.query('composition.pending', {'after': getattr(self, '_pending_cursor', None)})
        self._pending_cursor = page['next']
        return [{'task_id': item['id'], 'principal': item['principal'], 'created_at': item['created'] * 1000}
                for item in page['items'] if item['plan'] in self.grants.get(item['principal'], ())]

    def expire_pending(self, task_id=None):
        expiry = self.authority.query('composition.expiring', {'instance': task_id,
            'after': None if task_id else getattr(self, '_expiry_cursor', None)})
        if task_id is None:
            self._expiry_cursor = expiry['next']
        if expiry['instances']:
            self.authority.command('composition.expire', {'instances': expiry['instances']})

    def run_once(self, task_id=None):
        self.expire_pending(task_id)
        ids = [task_id] if task_id else [item['task_id'] for item in self.pending()]
        if not ids:
            return None
        for identity in ids:
            item = self.authority.query('composition.instance', {'instance': identity})
            if task_id and item['state'] in FINISHED_STATES | PAUSED_STATES:
                return self.inspect(item['principal'], identity)
            if not self.authority.query('composition.claimable', {'instance': identity}):
                continue
            result = self.run(item['principal'], identity)
            if task_id or result['state'] != 'blocked':
                return result
        return None

    def recovery_runs(self, principal, task_id):
        self._frontier(principal, task_id)
        return self.authority.query('composition.runs', {'instance': task_id, 'principal': principal})

    def recover(self, principal, task_id):
        item = self._frontier(principal, task_id)
        with self._claim(item) as owner:
            SharedWorkflowStore(self.authority, owner, {}).command('recover', task_id)
        return self.inspect(principal, task_id)

    def update(self, principal, task_id, request_key, input, *, metadata=None,
               expected_identity=None, expected_deployment_id=None):
        item = self._frontier(principal, task_id)
        if expected_identity is not None and expected_identity != item['identity']:
            raise ServiceError('workflow_identity_mismatch')
        if expected_deployment_id is not None and expected_deployment_id != self.authority._binding['deployment']:
            raise ServiceError('deployment_identity_mismatch')
        value = _object(input)
        if set(value) != {'path', 'input'} or type(value['path']) is not str:
            raise ServiceError('invalid_continuation')
        digest = _identity(task_id, value, metadata or {})
        previous = self.authority.query('task.request', {'principal': principal,
            'namespace': item['request_namespace'], 'epoch': item['request_epoch'], 'key': request_key})
        if previous is not None:
            if previous['kind'] != 'composition' or previous['target'] != task_id or previous['digest'] != digest:
                raise ServiceError('request_conflict')
            return self._receipt(item)
        if item.get('archived') or item['state'] not in {'input_required', 'auth_required'}:
            raise ServiceError('continuation_unavailable')
        with self._claim(item) as owner:
            _plans, workflows = self._definitions(item)
            store = SharedWorkflowStore(self.authority, owner, workflows)
            attempt = store.query('attempt', task_id, path=value['path'])
            if attempt is None:
                raise ServiceError('workflow_task_not_found')
            leaf = workflows[attempt['workflow']]
            if leaf.continue_input is None:
                raise ServiceError('unsupported_capability')
            combined = _object(leaf.continue_input(attempt['output'] or {}, _object(value['input'])))
            store.command('continue', task_id, path=value['path'], request_key=request_key,
                digest=digest, input=combined,
                run_id='run-' + _identity(task_id, value['path'], attempt['revision'] + 1))
        return self.inspect(principal, task_id)
