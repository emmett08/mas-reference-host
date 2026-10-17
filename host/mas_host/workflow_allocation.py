"""Conservative per-composition call admission above the deployment ledger."""
from __future__ import annotations

import time

from .operations import OperationsError


class WorkflowAllocation:
    """Charge once before deployment admission; denial never refunds a charge."""
    def __init__(self, ledger, store, instance, limits, stopped, deadline, *, scopes=()):
        self.ledger, self.store, self.instance = ledger, store, instance
        self.limits, self.stopped, self.deadline = limits, stopped, deadline
        self.scopes = scopes

    def __getattr__(self, name):
        return getattr(self.ledger, name)

    def _check(self):
        if self.stopped() or time.monotonic() >= self.deadline:
            raise OperationsError('workflow admission closed')

    def admit_run(self, run_id, run_dir, *, principal=None):
        self._check()
        return self.ledger.admit_run(run_id, run_dir, principal=principal)

    def admit_call(self, run_id, call_id, reservation_microusd=None):
        self._check()
        amount = (self.ledger.policy.per_call_reservation_microusd
                  if reservation_microusd is None else reservation_microusd)
        if type(amount) is not int or amount < 0:
            raise OperationsError('invalid workflow allocation')
        with self.store.transaction() as db:
            old = db.execute('SELECT amount FROM allocations WHERE instance=? AND run_id=? AND call_id=?',
                             (self.instance, run_id, call_id)).fetchone()
            if old is not None:
                if old['amount'] != amount:
                    raise OperationsError('workflow allocation identity conflict')
            else:
                usage = db.execute('SELECT count(*),coalesce(sum(amount),0) FROM allocations WHERE instance=?',
                                   (self.instance,)).fetchone()
                if (usage[0] >= self.limits['max_calls']
                        or usage[1] + amount > self.limits['max_reserved_microusd']):
                    raise OperationsError('workflow allocation exhausted')
                for scope in self.scopes:
                    prefix = scope['path']
                    usage = db.execute('SELECT count(*),coalesce(sum(a.amount),0) FROM allocations a JOIN attempts t ON a.run_id=t.run_directory AND a.instance=t.instance WHERE a.instance=? AND (t.path=? OR substr(t.path,1,?)=?)',
                                       (self.instance, prefix, len(prefix)+1, prefix+'/')).fetchone()
                    limits = scope['limits']
                    if (usage[0] >= limits['max_calls'] or usage[1] + amount > limits['max_reserved_microusd']):
                        raise OperationsError('subworkflow allocation exhausted')
                db.execute('INSERT INTO allocations VALUES(?,?,?,?)',
                           (self.instance, run_id, call_id, amount))
        self._check()
        return self.ledger.admit_call(run_id, call_id, amount)
