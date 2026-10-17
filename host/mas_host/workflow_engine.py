"""Durable bounded composition of independently owned Rust-backed workflows."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import replace
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import time

from .operations import DeploymentLedger, _file_lock
from .service_contracts import ServiceError
from .service_mutation import service_mutation
from .request_namespaces import RequestScope, epoch_guard
from .task_service import RunnerExecutor, _object, _text
from .workflow_allocation import WorkflowAllocation
from .workflow_plan import WorkflowPlan, bind_plan, validate_registry
from .workflow_evaluator import PlanEvaluator, FINISHED_STATES as _FINISHED, PAUSED_STATES as _PAUSED
from .workflow_inspection import encode_task_inspection, validate_task_selector
from .workflow_store import WorkflowStore, encode


class WorkflowEngine:
    """Trusted Linux scheduler; callbacks/providers remain in supervised adapters.

    Each task node gets one stable Rust run per explicit continuation revision.
    Plans sequence and join results; they never bypass Rust message admission.
    A run owns a process lock inherited by all child guardians. A parent lifetime
    pipe stops admission after scheduler death without relying on PID reuse.
    """
    def __init__(self, state_dir, plans, workflows, grants, *, operations=None,
                 max_instances=1000, mas_bin='mas', gateway_bin='mas-gateway', workflow_versions=None,
                 scheduler_policy=None):
        self.state_dir = Path(state_dir).absolute()
        self.operations = operations or DeploymentLedger(self.state_dir)
        if Path(self.operations.state_dir).absolute() != self.state_dir:
            raise ServiceError('deployment_mismatch')
        if type(max_instances) is not int or not 1 <= max_instances <= 100000:
            raise ServiceError('invalid_instance_limit')
        self.max_instances = max_instances
        self.plans = {name: plan if isinstance(plan, WorkflowPlan) else WorkflowPlan(plan)
                      for name, plan in plans.items()}
        # Copy mutable topology mappings; adapter implementations are trusted
        # registrations covered by their explicit implementation version.
        self.workflows = {name: replace(workflow, topology=json.loads(encode(workflow.topology)))
                          for name, workflow in workflows.items()}
        validate_registry(self.plans, self.workflows)
        self.bindings = {name: bind_plan(name, self.plans, self.workflows) for name in self.plans}
        self.workflow_versions = {}
        from .task_service import Workflow
        for name, versions in (workflow_versions or {}).items():
            for workflow in versions:
                if not isinstance(workflow, Workflow):
                    raise ServiceError('invalid_workflow')
                copied = replace(workflow, topology=json.loads(encode(workflow.topology)))
                self.workflow_versions[name, copied.identity] = copied
        self.workflow_versions.update({(name, workflow.identity): workflow for name, workflow in self.workflows.items()})
        self._active_workflows = self.workflows
        self.evaluator = PlanEvaluator(self.plans, self)
        self.grants = {}
        for principal, names in grants.items():
            _text(principal, 'principal')
            if isinstance(names, str):
                raise ServiceError('invalid_grants')
            self.grants[principal] = frozenset(names)
        self.options = {'mas_bin': mas_bin, 'gateway_bin': gateway_bin}
        ownership = (nullcontext() if self.operations.readonly else
                     _file_lock(self.state_dir / '.deployment.lock', False))
        with ownership:
            active = not self.operations.readonly and self.operations.admission_enabled
            if not active and not (self.state_dir / 'composition.sqlite').exists():
                raise ServiceError('audit_only')
            self.store = WorkflowStore(self.state_dir, readonly=not active)
            from .definitions import DefinitionRegistry
            self.definitions = DefinitionRegistry(self.state_dir, readonly=not active)
            with self.store.transaction(readonly=True) as db:
                retained = {(row[0], row[1]) for row in db.execute('SELECT DISTINCT plan,identity FROM instances')}
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='archives'").fetchone():
                    retained.update((row[0], row[1]) for row in db.execute('SELECT DISTINCT plan,identity FROM archives'))
            if any(not names <= set(self.plans) | {name for name, _identity in retained} for names in self.grants.values()):
                raise ServiceError('invalid_grants')
            if active:
                for name, identity in retained:
                    self.definitions.remember('plan', name, identity)
                for (name, identity), workflow in self.workflow_versions.items():
                    self.definitions.remember('workflow', name, identity, descriptor=workflow.descriptor,
                                              implementation_digest=workflow.verified_implementation.digest,
                                              implementation_manifest=workflow.verified_implementation.manifest)
                for name, plan in self.plans.items():
                    self.definitions.remember('plan_source', name, plan.identity, descriptor=plan.spec)
                    binding = self.bindings[name]
                    self.definitions.remember('plan', name, binding['identity'],
                                              descriptor={key: value for key, value in binding.items() if key != 'identity'}, current=True)
            from .scheduler_capacity import SchedulerCapacity
            self.capacity = SchedulerCapacity(self.state_dir, scheduler_policy, readonly=not active)

    def _mutation(self):
        return service_mutation(self.operations, self.store)

    def _owned(self, db, principal, instance):
        item = self.store.instance(db, instance, principal)
        if item is None:
            raise ServiceError('workflow_not_found')
        if item['plan'] not in self.grants.get(principal, ()):
            raise ServiceError('forbidden')
        return item

    def _workflow(self, name, identity, *, verify=True):
        workflow = self.workflow_versions.get((name, identity))
        if workflow is None or workflow.identity != identity:
            raise ServiceError('definition_unavailable')
        record = self.definitions.inspect('workflow', name, identity)
        if not record or record['retired'] or record['implementation_provenance'] != 'verified_source_package':
            raise ServiceError('definition_unavailable')
        if verify:
            try:
                workflow.verify_implementation()
            except ServiceError:
                raise ServiceError('definition_unavailable') from None
        return workflow

    @contextmanager
    def _execution_definition(self, item):
        """Resolve retained declarative plans and exact executable leaf versions."""
        binding = self.definitions.inspect('plan', item['plan'], item['identity'])
        descriptor = binding['descriptor'] if binding else None
        if binding and binding['retired']:
            raise ServiceError('definition_unavailable')
        if descriptor is None:
            # Legacy records have no source snapshot. Only an exactly matching
            # currently supplied binding establishes the missing source.
            current = self.bindings.get(item['plan'])
            if current is None or current['identity'] != item['identity']:
                raise ServiceError('definition_unavailable')
            descriptor = current
        plans = {item['plan']: WorkflowPlan(descriptor['spec'])}
        for name, identity in descriptor['plans'].items():
            record = self.definitions.inspect('plan_source', name, identity)
            if record and record['descriptor'] is not None:
                plans[name] = WorkflowPlan(record['descriptor'])
            elif name in self.plans and self.plans[name].identity == identity:
                plans[name] = self.plans[name]
            else:
                raise ServiceError('definition_unavailable')
        workflows = {name: self._workflow(name, identity) for name, identity in descriptor['workflows'].items()}
        if bind_plan(item['plan'], plans, workflows)['identity'] != item['identity']:
            raise ServiceError('definition_unavailable')
        previous, previous_evaluator = self._active_workflows, self.evaluator
        self._active_workflows, self.evaluator = workflows, PlanEvaluator(plans, self)
        try:
            yield plans[item['plan']].spec
        finally:
            self._active_workflows, self.evaluator = previous, previous_evaluator

    def submit(self, principal, request_key, plan, input, *, metadata=None, expected_identity=None, expected_deployment_id=None,
               request_epoch=None, _request_namespace=None):
        _text(request_key, 'request_key')
        if plan not in self.grants.get(principal, ()):
            raise ServiceError('forbidden')
        value = _object(input)
        metadata = _object(metadata) if metadata is not None else {}
        scope = RequestScope.supplied(_request_namespace, request_epoch)
        from .principal_quotas import submission_mutation, check_submission_quota
        with epoch_guard(self.operations, principal, scope), submission_mutation(self.operations, self.store, principal) as db:
            scope.check_owner(db, principal)
            if expected_deployment_id is not None and expected_deployment_id != self.operations.deployment_id:
                raise ServiceError('deployment_identity_mismatch')
            row = self.store.submitted(db, principal, request_key, scope=scope)
            if row is not None:
                if expected_identity is not None and row['identity'] != expected_identity:
                    raise ServiceError('workflow_identity_mismatch')
                if (row['plan'] != plan or not self.store.submission_matches(row, value)
                        or row.get('metadata', {}) != metadata):
                    raise ServiceError('request_conflict')
                instance = row['id']
            else:
                if plan not in self.plans:
                    raise ServiceError('definition_unavailable')
                if 'input_schema' in self.plans[plan].spec:
                    from .operation_contracts import JsonObjectSchema, ContractError
                    try:
                        JsonObjectSchema(self.plans[plan].spec['input_schema']).validate(value)
                    except ContractError:
                        raise ServiceError('workflow_input_schema_mismatch') from None
                if expected_identity is not None and self.bindings[plan]['identity'] != expected_identity:
                    raise ServiceError('workflow_identity_mismatch')
                if bind_plan(plan, self.plans, self.workflows)['identity'] != self.bindings[plan]['identity']:
                    raise ServiceError('workflow_changed')
                if self.definitions.current('plan', plan) != self.bindings[plan]['identity']:
                    raise ServiceError('workflow_changed')
                self.operations.check_storage_admission()
                check_submission_quota(self.operations, principal)
                from .execution_policy import timing_policy
                policy = timing_policy(self.plans[plan].spec.get('timing_policy'))
                clock = policy.start(int(time.time() * 1000)) if policy else None
                instance = self.store.create_instance(
                    db, principal, request_key, plan, self.bindings[plan]['identity'], value,
                    wall_seconds=policy.business_seconds if policy else self.plans[plan].spec['limits']['wall_seconds'],
                    maximum=self.max_instances, metadata=metadata, clock=clock, scope=scope)
        return self.inspect(principal, instance)

    def inspect(self, principal, instance):
        with self.store.transaction(readonly=True) as db:
            item = self._owned(db, principal, instance)
            return self.store.graph(db, item)

    def events(self, principal, instance, after=0, limit=100):
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ServiceError('invalid_cursor')
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
            return self.store.events(db, instance, after, limit)

    def inspect_task(self, principal, instance, path, revision=None):
        """Return one owned leaf revision; omitted revision selects the latest."""
        validate_task_selector(path, revision)
        with self.store.transaction(readonly=True) as db:
            item = self._owned(db, principal, instance)
            attempt = self.store.task_detail(db, instance, path, revision)
            if attempt is None:
                raise ServiceError('workflow_task_not_found')
            detail = {'instance_id': instance, 'plan': item['plan'],
                      'instance_state': item['state'], 'deadline': item['deadline'], **attempt}
            encode_task_inspection(detail)
            return detail

    def cancel(self, principal, instance):
        with self._mutation() as db:
            item = self._owned(db, principal, instance)
            self.store.request_cancellation(db, item)
        return self.inspect(principal, instance)

    @contextmanager
    def _ownership(self, instance):
        if type(instance) is not str or re.fullmatch(r'flow-[0-9a-f]{32}', instance) is None:
            raise ServiceError('invalid_workflow_identity')
        # Close-only is deliberate: LOCK_UN would release a surviving guardian's
        # inherited ownership after an interrupted scheduler exits.
        fd = os.open(self.state_dir / ('.' + instance + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError('workflow_busy') from None
            yield fd
        finally:
            os.close(fd)

    def _instance(self, instance):
        return self.store.read_instance(instance)

    # The evaluator's public journal contract resolves trusted registration
    # metadata here; the store never receives adapters or execution authority.
    def node(self, instance, path, kind, input, limits):
        return self.store.node(instance, path, kind, input, limits)

    def progress(self, instance, path, kind):
        return self.store.progress(instance, path, kind)

    def checkpoint(self, instance, path, kind, next_index, output, consumed=0):
        return self.store.checkpoint(instance, path, kind, next_index, output, consumed)

    def finish_node(self, instance, path, state, output=None, error=None):
        return self.store.finish_node(instance, path, state, output, error)

    def scope(self, instance, path, limits, deadline, *, timing_policy=None):
        return self.store.scope(instance, path, limits, deadline, timing_policy=timing_policy)

    def join_decision(self, instance, path):
        return self.store.join_decision(instance, path)

    def decide_join(self, instance, path, winners, obsolete):
        return self.store.decide_join(instance, path, winners, obsolete)

    def join_drain_state(self, instance, path):
        return self.store.join_drain_state(instance, path)

    def attempt(self, instance, path, workflow, input, deadline):
        leaf = self._active_workflows[workflow]
        return self.store.attempt(instance, path, workflow, input, deadline,
                                  identity=leaf.identity, wall_seconds=leaf.topology.get('wall_seconds', 3600))

    def _advance(self, instance, path, node, input, limits, deadline, ready):
        return self.evaluator.advance(instance, path, node, input, limits, deadline, ready)

    def _save_attempt(self, attempt, state, output=None, error=None):
        # Keep this execution receipt seam for existing interruption fixtures.
        self.store.save_attempt(attempt, state, output, error)

    def _requires_reconciliation(self, run_directory):
        from .bootstrap import requires_reconciliation
        return requires_reconciliation(self.state_dir / run_directory, run_directory, operations=self.operations)

    def _restart(self, instance):
        # Ownership has drained all predecessors before these evidence reads.
        # Recheck queued assignments too: a predecessor may have died after
        # requeuing an old run but before publishing its observed result.
        reconciled = [(attempt, self._requires_reconciliation(attempt['run_directory']))
                      for attempt in self.store.pending_attempts(instance)]
        self.store.start_instance(instance, reconciled)

    def _worker(self, attempt, item, limits, lock_fd, lifetime_fd, monotonic_deadline):
        def stopped():
            if select.select([lifetime_fd], [], [], 0)[0]:
                return True  # EOF: scheduler lifetime ended; nobody writes bytes.
            current = self._instance(item['id'])
            return (current['cancel_requested'] or time.monotonic() >= monotonic_deadline
                    or self.store.scope_cancelled(item['id'], attempt['path'])
                    or bool(current.get('error') and current['error'].get('code') == 'scope_timing_expired')
                    or current['plan'] not in self.grants.get(current['principal'], ()))
        try:
            scopes = self.store.scopes(item['id'], attempt['path'])
            from .service_timing import execution_deadline
            monotonic_deadline = min([monotonic_deadline, *[
                time.monotonic() + max(0, scope['deadline'] - time.time()) for scope in scopes],
                *[execution_deadline(scope) for scope in scopes if scope.get('clock')]])
            allocation = WorkflowAllocation(self.operations, self.store, item['id'], limits, stopped,
                                            monotonic_deadline, scopes=scopes)
            executor = RunnerExecutor(self.state_dir, operations=allocation, **self.options)
            original = self._active_workflows[attempt['workflow']]
            workflow = replace(original, topology={**original.topology, 'wall_seconds': attempt['wall_seconds']})
            result = executor.execute(workflow, {'run_id': attempt['run_directory'],
                'run_directory': attempt['run_directory'], 'input': attempt['input'],
                'principal': item['principal'], 'workflow': attempt['workflow'],
                'task_id': 'task-flow-' + hashlib.sha256((item['id'] + ':' + attempt['path']).encode()).hexdigest(),
                'revision': attempt['revision'], 'execution_deadline': monotonic_deadline,
                'event_sink': lambda event: self.store.protocol_event(item['id'], attempt, event)},
                cancelled=stopped, ownership_descriptor=lock_fd)
            for event in result.events:
                self.store.protocol_event(item['id'], attempt, event)
            self._save_attempt(attempt, result.state, result.result, result.error)
        except BaseException:
            # Unknown includes infrastructure uncertainty. Recovery reuses the
            # assigned run; it never creates a fresh external-effect identity.
            self._save_attempt(attempt, 'unknown', error={'code': 'worker_interrupted'})

    def run(self, principal, instance):
        with self.store.transaction(readonly=True) as db:
            item = self._owned(db, principal, instance)
        from .service_timing import status
        timing = status(item)
        if item['state'] in _FINISHED or item['state'] in _PAUSED and not (timing and timing['expired']) and not self.store.expired_scopes(instance):
            return self.inspect(principal, instance)
        try:
            with self._execution_definition(item) as plan:
                return self._run_definition(principal, instance, plan)
        except ServiceError as error:
            if error.code != 'definition_unavailable':
                raise
            with self._ownership(instance), _file_lock(self.state_dir / '.deployment.lock', False):
                if self.operations.readonly or not self.operations.admission_enabled:
                    raise ServiceError('audit_only')
                uncertain = any(self._requires_reconciliation(attempt['run_directory'])
                                for attempt in self.store.pending_attempts(instance))
                self.store.block_definition(instance, uncertain=uncertain)
            return self.inspect(principal, instance)

    def definition_status(self, name, identity):
        """Read source and dependency availability without mutating the evaluator."""
        record = self.definitions.inspect('plan', name, identity)
        if record is None:
            raise ServiceError('definition_not_found')
        descriptor = record['descriptor']
        available = descriptor is not None and not record['retired']
        dependencies = []
        if descriptor is not None:
            for leaf, version in descriptor['workflows'].items():
                retained = self.definitions.inspect('workflow', leaf, version)
                present = ((leaf, version) in self.workflow_versions and retained is not None
                    and not retained['retired'] and retained['implementation_provenance'] == 'verified_source_package')
                dependencies.append({'kind': 'workflow', 'name': leaf, 'identity': version, 'available': present})
                available = available and present
            for child, version in descriptor['plans'].items():
                retained = self.definitions.inspect('plan_source', child, version)
                present = retained is not None and retained['descriptor'] is not None and not retained['retired']
                dependencies.append({'kind': 'plan_source', 'name': child, 'identity': version, 'available': present})
                available = available and present
        return {'kind': 'plan', 'name': name, 'identity': identity,
                'current': self.definitions.current('plan', name) == identity,
                'retired': bool(record['retired']), 'source_present': descriptor is not None,
                'available': bool(available), 'dependencies': dependencies,
                'implementation_reverified_at_dispatch': True}

    def retire_definition(self, plan, identity):
        """Trusted management operation; current aliases and live work retain definitions."""
        with _file_lock(self.state_dir / '.deployment.lock', True, nonblocking=True):
            if self.operations.readonly or not self.operations.admission_enabled:
                raise ServiceError('audit_only')
            with self.store.transaction(readonly=True) as db:
                if db.execute("SELECT 1 FROM instances i WHERE plan=? AND identity=? AND (state NOT IN "
                              "('completed','failed','cancelled','expired') OR EXISTS(SELECT 1 FROM attempts a "
                              "WHERE a.instance=i.id AND a.state IN ('running','unknown'))) LIMIT 1", (plan, identity)).fetchone():
                    raise ServiceError('definition_in_use')
            self.definitions.retire('plan', plan, identity)
            return self.definitions.inspect('plan', plan, identity)

    def _run_definition(self, principal, instance, plan):
        from .workflow_scheduler import SchedulerOwnership
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
        with SchedulerOwnership(self.state_dir, instance) as ownership:
            if self.operations.readonly or not self.operations.admission_enabled:
                raise ServiceError('audit_only')
            item = self._instance(instance)
            from .service_timing import status, execution_deadline
            timing = status(item)
            if item['state'] in _FINISHED or item['state'] in _PAUSED and not (timing and timing['expired']) and not self.store.expired_scopes(instance):
                return self.inspect(principal, instance)
            limits = plan['limits']
            deadline = time.monotonic() + max(0, item['deadline'] - time.time())
            live = {}
            # Classify retained effects even when cancellation or expiry has
            # already closed admission. Stopping is not reconciliation.
            self._restart(instance)
            while True:
                for key in ownership.poll():
                    attempt = live.pop(key)
                    state = self.store.attempt_state(attempt)
                    if state == 'running':
                        self._save_attempt(attempt, 'unknown', error={'code': 'worker_lost'})
                current = self._instance(instance)
                timing = self.store.clock_activity(instance, len(live)) if current.get('clock') else None
                scope_expired = self.store.scope_clock_activity(instance, [attempt['path'] for attempt in live.values()])
                if timing is not None:
                    deadline = execution_deadline(self._instance(instance))
                ready = []
                state, output = self._advance(instance, 'root', plan['root'], item['input'], limits, item['deadline'], ready)
                expired = time.monotonic() >= deadline or bool(timing and timing['expired']) or bool(scope_expired)
                stopped = current['cancel_requested'] or expired
                if not live and (stopped or state in _PAUSED | {'expired', 'cancelled'}
                                 or state in _FINISHED and not ready):
                    if state == 'completed' and not stopped and 'output_schema' in plan:
                        from .operation_contracts import JsonObjectSchema, ContractError
                        try:
                            JsonObjectSchema(plan['output_schema']).validate(output)
                        except ContractError:
                            state = 'failed'
                            self.store.finish_node(instance, 'root', 'failed', error={'code': 'workflow_output_schema_mismatch'})
                    self.store.finish_instance(instance, state, output, expired=expired)
                    return self.inspect(principal, instance)
                if not stopped and state not in _PAUSED | {'expired', 'cancelled'}:
                    for attempt in ready:
                        if len(live) >= limits['max_parallel']:
                            break
                        if self.store.scope_cancelled(instance, attempt['path']):
                            continue
                        scopes = self.store.scopes(instance, attempt['path'])
                        if any(sum(other['path'] == scope['path'] or other['path'].startswith(scope['path']+'/')
                                   for other in live.values()) >= scope['limits']['max_parallel'] for scope in scopes):
                            continue
                        workflow = self._active_workflows[attempt['workflow']]
                        slot = self.capacity.acquire(attempt['run_directory'], item['principal'], attempt['workflow'],
                                                     resources=getattr(workflow, 'resource_keys', ()))
                        if slot is None:
                            continue
                        key = (attempt['path'], attempt['revision'])
                        def execute(reader, descriptors, attempt=attempt, slot=slot):
                            try:
                                self._worker(attempt, item, limits, (*descriptors, slot.descriptor), reader, deadline)
                            finally:
                                slot.close()
                        try:
                            if current.get('clock'):
                                observed = self.store.clock_activity(instance, len(live) + 1)
                                if observed and observed['expired']:
                                    continue
                            if self.store.scope_clock_activity(instance, [other['path'] for other in live.values()] + [attempt['path']]):
                                continue
                            self._save_attempt(attempt, 'running')
                            ownership.spawn(key, execute)
                        finally:
                            slot.close()
                        live[key] = attempt
                if not live:
                    if ready and not stopped:
                        time.sleep(0.05)
                        continue
                    raise ServiceError('workflow_no_progress')
                ownership.wait(0.02)

    def continue_task(self, principal, instance, path, request_key, input, *, metadata=None, expected_identity=None, expected_deployment_id=None):
        _text(request_key, 'request_key')
        supplied = _object(input)
        metadata = _object(metadata) if metadata is not None else {}
        with self.store.transaction(readonly=True) as db:
            scope = RequestScope.retained(self._owned(db, principal, instance))
        with epoch_guard(self.operations, principal, scope), self._ownership(instance), self._mutation() as db:
            item = self._owned(db, principal, instance)
            scope.check_owner(db, principal)
            if expected_deployment_id is not None and expected_deployment_id != self.operations.deployment_id:
                raise ServiceError('deployment_identity_mismatch')
            if expected_identity is not None and expected_identity != item['identity']:
                raise ServiceError('workflow_identity_mismatch')
            old = self.store.continuation(db, instance, request_key)
            if old:
                if not self.store.continuation_matches(old, path, supplied, metadata):
                    raise ServiceError('request_conflict')
            else:
                if item['cancel_requested'] or item['state'] not in {'input_required', 'auth_required'} or item['deadline'] <= time.time():
                    raise ServiceError('invalid_workflow_state')
                attempt = self.store.latest_attempt(db, instance, path)
                if not attempt or attempt['state'] not in {'input_required', 'auth_required'}:
                    raise ServiceError('invalid_continuation')
                workflow = self._workflow(attempt['workflow'], attempt['identity'])
                if workflow.continue_input is None:
                    raise ServiceError('unsupported_capability')
                self.operations.check_storage_admission()
                value = _object(workflow.continue_input(attempt['output'] or {}, supplied))
                revision = attempt['revision'] + 1
                if revision >= 32:
                    raise ServiceError('revision_limit')
                self.store.continue_attempt(db, item, attempt, request_key, supplied, value,
                                            wall_seconds=workflow.topology.get('wall_seconds', 3600), metadata=metadata)
        return self.inspect(principal, instance)

    def recover(self, principal, instance):
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
        with self._ownership(instance), self._mutation() as db:
            item = self._owned(db, principal, instance)
            if item['state'] != 'unknown':
                raise ServiceError('invalid_workflow_state')
            for row in self.store.unknown_attempts(db, instance):
                if self._requires_reconciliation(row['run_directory']):
                    raise ServiceError('reconciliation_required')
            self.store.requeue_unknown(db, instance)
        return self.inspect(principal, instance)
