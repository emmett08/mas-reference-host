"""Operator recovery over shared authority, independent of local cache paths."""
from __future__ import annotations

import hashlib

from .contracts import RunnerError
from .journal import canonical
from .recovery import ReconciliationQuery, _copy, _identity, observation, validate_evidence
from .service_contracts import ServiceError


class SharedRecoveryService:
    def __init__(self, service, *, operators, queries=None):
        if not callable(getattr(service, 'recovery_runs', None)):
            raise ServiceError('recovery_not_supported')
        self.service, self.authority = service, service.authority
        self.operators = {_identity(actor): frozenset(_identity(owner) for owner in owners)
                          for actor, owners in operators.items()}
        self.queries = dict(queries or {})
        if any(not isinstance(query, ReconciliationQuery) or name != query.name
               for name, query in self.queries.items()):
            raise ServiceError('invalid_reconciliation_query')

    def _runs(self, operator, principal, task_id):
        _identity(operator)
        _identity(principal)
        if principal not in self.operators.get(operator, ()):
            raise ServiceError('forbidden')
        runs = self.service.recovery_runs(principal, task_id)
        if type(runs) is not list or len(runs) > 10000:
            raise ServiceError('invalid_recovery_runs')
        checked = []
        for binding in runs:
            run_id = _identity(binding['run_id'])
            report = self.authority.query('recovery_snapshot', {'run': run_id, 'principal': principal})
            checked.append((binding, report))
        return checked

    def inbox(self, operator, principal, task_id):
        runs = self._runs(operator, principal, task_id)
        current = self.service.inspect(principal, task_id)
        entries, remote, missing = [], [], False
        for binding, report in runs:
            if report is None:
                missing = missing or binding['state'] not in {'queued', 'prepared', 'cancelled'}
                continue
            remote.extend(report['remote_observations'])
            for call in report['invocations']:
                if call['state'] not in {'executing', 'unknown'}:
                    continue
                entries.append({'run_id': report['run_id'], 'path': binding.get('path'),
                    'revision': binding['revision'], 'call_id': call['call_id'], 'actor': call['actor'],
                    'state': call['state'], 'evidence': call['evidence'],
                    'reserved_microusd': call['reservation_microusd'],
                    'admissible_decisions': ['completed', 'confirmed_no_effect'],
                    'required_evidence': ['source', 'reference', 'observed_at_ms'],
                    'automatic_retry': False})
        cancellation = {'requested': bool(current['cancel_requested']),
            'future_dispatch_stopped': bool(current['cancel_requested']) or current['state'] in
                {'completed', 'failed', 'cancelled', 'unknown', 'blocked', 'input_required', 'auth_required'},
            # An expired controller lease fences mutation, not a provider process.
            'local_execution_stopped': None,
            'remote_cancellation': 'acknowledged' if remote and all(
                item['state'] == 'cancelled' for item in remote) else 'not_established',
            'effect_reconciliation': 'required' if entries or missing or any(
                item['uncertain'] for item in remote) else 'not_required'}
        return _copy({'format': 'mas-recovery-inbox/1', 'task_id': task_id, 'state': current['state'],
            'cancellation': cancellation, 'entries': entries, 'remote_observations': remote,
            'queries': [query.descriptor for query in self.queries.values()]}, 512 * 1024)

    def _invocation(self, operator, principal, task_id, call_id):
        _identity(call_id)
        matches = [(binding, report, call) for binding, report in self._runs(operator, principal, task_id)
                   if report is not None for call in report['invocations'] if call['call_id'] == call_id]
        if len(matches) != 1:
            raise ServiceError('invocation_not_found')
        return matches[0]

    def query(self, operator, principal, task_id, call_id, *, query_name, query_version):
        _binding, report, call = self._invocation(operator, principal, task_id, call_id)
        contract = self.queries.get(query_name)
        if contract is None or contract.version != query_version:
            raise ServiceError('reconciliation_contract_mismatch')
        if call['state'] not in {'executing', 'unknown'}:
            raise ServiceError('invocation_not_uncertain')
        request = {'task_id': task_id, 'run_id': report['run_id'], 'call_id': call_id,
                   'actor': call['actor'], 'evidence': call['evidence']}
        from .telemetry import span
        with span(getattr(self.authority, 'observation_dir', None), 'reconciliation',
                  task_id=task_id, run_id=report['run_id'], invocation_id=call_id):
            observed = observation(contract.query(_copy(request)), call_id,
                                   permits_no_effect=contract.permits_no_effect)
        return {'contract': contract.descriptor, 'observation': observed, 'resolved': False}

    def resolve(self, operator, principal, task_id, call_id, *, request_key,
                decision, evidence, output=None):
        _identity(request_key)
        if decision not in {'completed', 'confirmed_no_effect'}:
            raise ServiceError('invalid_recovery_decision')
        supplied = validate_evidence(evidence)
        if decision == 'completed' and type(output) is not dict or decision != 'completed' and output is not None:
            raise ServiceError('invalid_recovery_output')
        output = _copy(output) if output is not None else None
        binding, report, _call = self._invocation(operator, principal, task_id, call_id)
        audit = {'format': 'mas-recovery-decision/1', 'operator': operator, 'owner': principal,
            'task_id': task_id, 'request_key': request_key, 'observation': supplied}
        command_id = 'resolution-' + hashlib.sha256(canonical(
            [operator, principal, task_id, call_id, request_key]).encode()).hexdigest()[:48]
        try:
            self.authority.resolve_invocation(report['run_id'], call_id, operator=operator,
                decision=decision, evidence=audit, output=output, command_id=command_id)
        except RunnerError:
            raise ServiceError('recovery_not_available') from None
        return {'task_id': task_id, 'path': binding.get('path'), 'revision': binding['revision'],
            'call_id': call_id, 'decision': decision, 'resolved': True, 'execution_resumed': False}
