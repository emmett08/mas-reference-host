"""Operator accounting over the same PostgreSQL authority as call admission."""
from __future__ import annotations

from dataclasses import asdict
import hashlib

from .accounting import PeriodPolicy, _identity, _scope
from .codec import canonical_json
from .contracts import RunnerError
from .operations import OperationsError


def _key(*parts):
    return 'accounting-' + hashlib.sha256(canonical_json(list(parts)).encode()).hexdigest()


class SharedAccountingAdministration:
    """Administration is trusted; authenticated operator grants remain external.

    The database supplies admission timestamps and rejects future observations.
    No client clock can select a cheaper period or reopen a previous bucket.
    """
    def __init__(self, authority):
        self.authority = authority

    def _command(self, operation, parameters, command_id):
        try:
            return self.authority.command('accounting.' + operation, parameters,
                command_id=command_id)['receipt']
        except RunnerError:
            raise OperationsError('shared accounting command unavailable') from None

    def _query(self, operation, parameters):
        try:
            return self.authority.query('accounting.' + operation, parameters)
        except RunnerError:
            raise OperationsError('shared accounting query unavailable') from None

    def configure_period(self, scope, policy, *, request_key, actor, reason):
        _scope(scope); _identity(request_key); _identity(actor)
        if not isinstance(policy, PeriodPolicy) or type(reason) is not str or not 1 <= len(reason) <= 500:
            raise OperationsError('invalid allocation period amendment')
        return self._command('configure_period', {'scope': scope, 'policy': asdict(policy),
            'request_key': request_key, 'actor': actor, 'reason': reason}, _key('policy', request_key))

    def periods(self):
        return self._query('periods', {})

    def record_charge(self, *, provider, charge_id, run_id, call_id, actual_microusd,
                      observed_at, evidence, actor):
        for value in (provider, charge_id, run_id, call_id, evidence, actor):
            _identity(value)
        if (type(actual_microusd) is not int or not 0 <= actual_microusd <= 2**63 - 1
                or type(observed_at) is not int or not 0 <= observed_at <= 2**63 - 1):
            raise OperationsError('invalid provider charge observation')
        return self._command('record_charge', {'provider': provider, 'charge_id': charge_id,
            'run_id': run_id, 'call_id': call_id, 'actual_microusd': actual_microusd,
            'observed_at': observed_at, 'evidence': evidence, 'actor': actor},
            _key('charge', provider, charge_id))

    def reconcile(self, run_id, call_id):
        _identity(run_id); _identity(call_id)
        return self._query('reconcile', {'run_id': run_id, 'call_id': call_id})
