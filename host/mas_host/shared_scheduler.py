"""Frozen native admission policies and fair ordering of shared intent."""
from collections import deque
from dataclasses import asdict

from .scheduler_capacity import SchedulerPolicy
from .definitions import canonical
from .service_contracts import ServiceError


def configure_policy(authority, grants, *, max_tasks=10000, max_pending=1000,
                     max_revisions=32, scheduler_policy=None, principal_quotas=None,
                     preserve_existing=False):
    policy = authority.query('task.policy', {})
    requested = {'max_tasks': max_tasks, 'max_pending': max_pending, 'max_revisions': max_revisions}
    if policy is None or not preserve_existing and canonical(policy) != canonical(requested):
        authority.command('task.configure', requested)
    else:
        max_tasks, max_pending = policy['max_tasks'], policy['max_pending']
    quotas = principal_quotas or {}
    if type(quotas) is not dict or not set(quotas) <= set(grants):
        raise ServiceError('invalid_principal_quotas')
    for principal in grants:
        value = quotas.get(principal, {})
        if type(value) is not dict or set(value) - {'max_pending', 'max_tasks', 'max_calls', 'max_reserved_microusd'}:
            raise ServiceError('invalid_principal_quotas')
        if any(type(limit) is not int or not 1 <= limit < 2**63 for limit in value.values()):
            raise ServiceError('invalid_principal_quotas')
        saved = authority.query('task.principal_policy', {'principal': principal})
        requested = {'max_tasks': value.get('max_tasks', max_tasks),
                     'max_pending': value.get('max_pending', max_pending)}
        if saved is None or not preserve_existing and canonical(saved) != canonical(requested):
            authority.command('task.configure_principal', {'principal': principal,
                **requested})
    selected = scheduler_policy or SchedulerPolicy()
    saved = authority.query('task.scheduler', {})
    if saved is None or not preserve_existing and canonical(saved['policy']) != canonical(asdict(selected)):
        authority.command('task.configure_scheduler', {'policy': asdict(selected)})
    return SharedSchedulerCapacity(authority)


class SharedSchedulerCapacity:
    def __init__(self, authority):
        self.authority = authority
        saved = authority.query('task.scheduler', {})
        if saved is None:
            raise ServiceError('scheduler_unavailable')
        self.policy = SchedulerPolicy(**saved['policy'])

    def fair_order(self, rows, *, principal='principal'):
        if not rows:
            return rows
        last = self.authority.query('task.scheduler', {})['last_principal']
        owners = sorted({row[principal] for row in rows})
        owners = [p for p in owners if p > last] + [p for p in owners if p <= last]
        queues = {p: deque(row for row in rows if row[principal] == p) for p in owners}
        ordered = []
        while queues:
            for p in tuple(queues):
                ordered.append(queues[p].popleft())
                if not queues[p]:
                    del queues[p]
        return ordered
