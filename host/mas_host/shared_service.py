"""Construct the shared service from a trusted, already bound controller epoch."""
from __future__ import annotations

from .service_contracts import ServiceError
from .shared_task_service import SharedTaskService
from .task_service import _bounded_items


class SharedTaskFacade:
    def __init__(self, leaves, compositions=None):
        self.leaves, self.compositions = leaves, compositions
        self.authority = self.operations = leaves.authority
        self.state_dir = leaves.state_dir
        self.grants = {p: names | (compositions.grants.get(p, frozenset()) if compositions else frozenset())
                       for p, names in leaves.grants.items()}
        self.plan_names = set(compositions.plans) | leaves.definitions.names('plan') if compositions else set()
        self.capacity = leaves.capacity
        self.definitions = leaves.definitions
        from .shared_remote import SharedPeerRegistry
        self.peer_registry = SharedPeerRegistry(self.authority)

    def _owner(self, task_id):
        if type(task_id) is str and task_id.startswith('task-'):
            return self.leaves
        if type(task_id) is str and task_id.startswith('flow-') and self.compositions is not None:
            return self.compositions
        raise ServiceError('task_not_found')

    def submit(self, principal, request_key, workflow, input, **options):
        owner = self.compositions if self.compositions is not None and workflow in self.plan_names else self.leaves
        return owner.submit(principal, request_key, workflow, input, **options)

    def inspect(self, principal, task_id):
        return self._owner(task_id).inspect(principal, task_id)

    def result(self, principal, task_id):
        return self._owner(task_id).result(principal, task_id)

    def cancel(self, principal, task_id):
        return self._owner(task_id).cancel(principal, task_id)

    def update(self, principal, task_id, request_key, input, **options):
        return self._owner(task_id).update(principal, task_id, request_key, input, **options)

    def recover(self, principal, task_id):
        return self._owner(task_id).recover(principal, task_id)

    def inspect_task(self, principal, task_id, path, revision=None):
        owner = self._owner(task_id)
        if owner is self.leaves:
            raise ServiceError('unsupported_capability')
        return owner.inspect_task(principal, task_id, path, revision)

    def inspect_graph(self, principal, task_id, *, section='nodes', offset=0, limit=20):
        from .graph_inspection import inspect_graph
        return inspect_graph(self, principal, task_id, section=section, offset=offset, limit=limit)

    def events(self, principal, task_id, after=0):
        return self._owner(task_id).events(principal, task_id, after)

    def events_page(self, principal, task_id, after=0, limit=100):
        return self._owner(task_id).events_page(principal, task_id, after, limit)

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        options = dict(page_size=page_size, after=after, state=state,
                       metadata=metadata, updated_after=updated_after)
        pages = [self.leaves.list_task_page(principal, **options)]
        if self.compositions is not None:
            pages.append(self.compositions.list_task_page(principal, **options))
        values = sorted((item for page in pages for item in page['tasks']),
                        key=lambda x: (x['updated_at'], x['task_id']), reverse=True)
        selected, more = _bounded_items(values, limit=page_size, overhead=1024)
        more = more or any(page['next'] is not None for page in pages)
        return {'tasks': selected, 'total_size': sum(page['total_size'] for page in pages),
            'next': [selected[-1]['updated_at'], selected[-1]['task_id']] if more and selected else None}

    def list_tasks(self, principal, limit=100, offset=0):
        if type(limit) is not int or not 1 <= limit <= 1000 or type(offset) is not int or offset < 0:
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

    def discover(self, principal):
        discovery = self.leaves.discover(principal)
        if self.compositions is not None:
            plans = self.compositions.discover(principal)
            names = self.plan_names
            discovery['registrations'] = [r for r in discovery['registrations'] if r['name'] not in names] + plans
            discovery['capabilities'] = sorted(set(discovery['capabilities']) | {'task_detail', 'graph'})
        return discovery

    def pending(self):
        pending = self.leaves.pending()
        if self.compositions is not None:
            pending += self.compositions.pending()
        return self.capacity.fair_order(sorted(pending, key=lambda x: (x.get('created_at', 0), x['task_id'])))

    def run_once(self, task_id=None):
        if task_id is not None:
            return self._owner(task_id).run_once(task_id)
        self.leaves.expire_pending()
        if self.compositions is not None:
            self.compositions.expire_pending()
        for item in self.pending():
            result = self._owner(item['task_id']).run_once(item['task_id'])
            if result is not None and result['state'] != 'blocked':
                return result
        return None

    def recovery_runs(self, principal, task_id):
        return self._owner(task_id).recovery_runs(principal, task_id)

    def request_epoch_status(self, principal, epoch):
        return self.leaves.request_epoch_status(principal, epoch)

    def retire_request_epoch(self, principal, epoch, **options):
        return self.leaves.retire_request_epoch(principal, epoch, **options)

    def recovery_service(self, *, operators, queries=None):
        from .shared_recovery import SharedRecoveryService
        return SharedRecoveryService(self, operators=operators, queries=queries)

    def deadline_service(self, *, operators):
        from .shared_deadlines import SharedDeadlineService
        return SharedDeadlineService(self, operators=operators)

    def close(self):
        self.authority.close()


def create_shared_service(authority, cache_dir, workflows, grants, *, plans=None,
                          workflow_versions=None, allocation_limits=None,
                          max_pending=1000, max_tasks=10000, max_revisions=32, max_instances=1000,
                          supervised=True, scheduler_policy=None, principal_quotas=None, gateway_bin=None):
    """Install additive repositories and renew this explicitly supplied epoch.

    This factory never initialises or takes over a deployment. Provision and
    handover are separate operator actions against the shared authority.
    """
    if type(allocation_limits) is not dict or not {'deployment', *(f'principal/{p}' for p in grants)} <= set(allocation_limits):
        raise ServiceError('invalid_shared_allocations')
    from pathlib import Path
    authority.observation_dir = Path(cache_dir)
    authority.install_host()
    authority.start_renewal()
    try:
        for scope, limits in allocation_limits.items():
            limits = dict(limits)
            if scope.startswith('principal/'):
                quota = (principal_quotas or {}).get(scope.removeprefix('principal/'), {})
                if 'max_calls' in quota:
                    limits['max_calls'] = min(limits['max_calls'], quota['max_calls'])
                if 'max_reserved_microusd' in quota:
                    limits['max_microusd'] = min(limits['max_microusd'], quota['max_reserved_microusd'])
            authority.configure_scope(scope, **limits)
        from .shared_definitions import SharedDefinitionRegistry
        registry = SharedDefinitionRegistry(authority)
        plans = plans or {}
        leaf_names = set(workflows) | registry.names('workflow')
        plan_names = set(plans) | registry.names('plan')
        if leaf_names & plan_names:
            raise ServiceError('ambiguous_workflow_name')
        if any(not set(names) <= leaf_names | plan_names for names in grants.values()):
            raise ServiceError('invalid_grant')
        leaf_grants = {p: set(names) & leaf_names for p, names in grants.items()}
        plan_grants = {p: set(names) & plan_names for p, names in grants.items()}
        leaves = SharedTaskService(authority, cache_dir, workflows, leaf_grants,
            workflow_versions=workflow_versions, max_pending=max_pending,
            max_tasks=max_tasks, max_revisions=max_revisions, supervised=supervised,
            scheduler_policy=scheduler_policy, principal_quotas=principal_quotas, gateway_bin=gateway_bin)
        from .shared_composition_service import SharedCompositionService
        compositions = SharedCompositionService(authority, cache_dir, plans, workflows,
            plan_grants, workflow_versions=workflow_versions, supervised=supervised, max_instances=max_instances,
            gateway_bin=gateway_bin)
        return SharedTaskFacade(leaves, compositions)
    except BaseException:
        authority.close()
        raise
