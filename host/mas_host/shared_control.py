"""Worker/control ports backed only by the epoch-fenced PostgreSQL authority.

No local state mirror participates in placement, dispatch or acknowledgement.
Object verification precedes the receipt transaction; conservative retention may
remain when a controller loses authority between verification and commit.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json

from .codec import canonical_json, integer_json
from .contracts import RunnerError
from .distributed import (WorkerController, ARTIFACT_OUTPUT_PROFILE, OUTPUT_PROFILE,
    _identifier, _integer, _versions, validate_binding, validate_worker_receipt)
from .service_contracts import ServiceError


class SharedWorkerController(WorkerController):
    def __init__(self, authority, worker_grants, *, max_outstanding=100,
                 per_principal=10, lease_ms=30000, artifact_repository=None,
                 artifact_store_id=None):
        self.authority = self.operations = authority
        self.max_outstanding = _integer(max_outstanding, 1, 10000)
        self.per_principal = _integer(per_principal, 1, self.max_outstanding)
        self.lease_ms = _integer(lease_ms, 100, 300000)
        if ((artifact_repository is None) != (artifact_store_id is None)
                or artifact_repository is not None and not callable(artifact_repository)):
            raise ServiceError('invalid_artifact_configuration')
        self.artifact_repository = None
        if artifact_repository is not None:
            def shared_repository(namespace, principal):
                from .shared_artifacts import SharedArtifactStore
                from .artifact_contracts import ArtifactError
                store = artifact_repository(namespace, principal)
                if not isinstance(store, SharedArtifactStore) or store.authority is not authority:
                    raise ArtifactError('shared workers require the same shared artefact authority')
                return store
            self.artifact_repository = shared_repository
        self.artifact_store_id = _identifier(artifact_store_id) if artifact_store_id is not None else None
        if type(worker_grants) is not dict or len(worker_grants) > 10000:
            raise ServiceError('invalid_worker_grants')
        self.grants = json.loads(integer_json(worker_grants, 1_048_576))
        # The native repository independently validates and freezes this policy.
        configuration = {'max_outstanding': self.max_outstanding,
            'per_principal': self.per_principal, 'lease_ms': self.lease_ms,
            'worker_grants': self.grants, 'artifact_store_id': self.artifact_store_id}
        identity = hashlib.sha256(canonical_json(configuration).encode()).hexdigest()
        self._command('configure', configuration, command_id='worker-config-' + identity)

    def _command(self, operation, payload, *, command_id=None):
        try:
            result = self.authority.command('worker.' + operation, payload, command_id=command_id)
        except RunnerError:
            raise ServiceError('shared_worker_unavailable') from None
        # A replayed command can never be interpreted as a fresh effect grant.
        if operation == 'dispatch' and result['replayed']:
            return {'execute': False, 'reason': 'dispatch_already_granted'}
        return result['receipt']

    def _query(self, operation, payload):
        try:
            return self.authority.query('worker.' + operation, payload)
        except RunnerError:
            raise ServiceError('shared_worker_unavailable') from None

    def submit_admitted(self, call_id, binding):
        _identifier(call_id)
        checked = validate_binding(binding)
        if checked['output_profile'] == ARTIFACT_OUTPUT_PROFILE and self.artifact_repository is None:
            raise ServiceError('worker_artifact_storage_unconfigured')
        return self._command('submit', {'call_id': call_id, 'binding': checked})

    def inspect(self, principal, call_id):
        # Inspect current database time without manufacturing a command receipt
        # per poll; admission transitions persist expiry before they act.
        return self._query('job', {'principal': principal, 'call_id': call_id})

    def abandon(self, principal, call_id):
        return self._command('abandon', {'principal': principal, 'call_id': call_id})

    def register(self, principal, *, boot, capabilities, architecture, runtime,
                 trust_zone, capacity=1, protocol=None, output_profiles=(OUTPUT_PROFILE,)):
        grant = self._grant(principal)
        for item in (boot, architecture, runtime, trust_zone):
            _identifier(item)
        _integer(capacity, 1, grant['capacity'])
        _versions(protocol, runtime, output_profiles)
        return self._command('register', {'principal': principal, 'boot': boot,
            'descriptor': {'capabilities': capabilities, 'architecture': architecture,
                'runtime': runtime, 'protocol': protocol, 'trust_zone': trust_zone,
                'capacity': capacity, 'output_profiles': sorted(output_profiles)}})

    def claim(self, principal, *, boot):
        return self._command('claim', {'principal': principal, 'boot': boot})

    def dispatch(self, principal, *, boot, call_id, fence):
        # A deterministic command identity also fences lost-reply replays. Even
        # a different command ID encounters the dispatched state and is denied.
        payload = {'principal': principal, 'boot': boot, 'call_id': call_id, 'fence': fence}
        command_id = 'worker-dispatch-' + hashlib.sha256(canonical_json(payload).encode()).hexdigest()
        return self._command('dispatch', payload, command_id=command_id)

    def heartbeat(self, principal, *, boot, call_id, fence):
        return self._command('heartbeat', {'principal': principal, 'boot': boot,
            'call_id': call_id, 'fence': fence})

    def drain(self, principal, *, boot, draining=True):
        if type(draining) is not bool:
            raise ServiceError('invalid_worker_request')
        return self._command('drain', {'principal': principal, 'boot': boot, 'draining': draining})

    def receipt(self, principal, *, receipt):
        self._grant(principal)
        validate_worker_receipt(receipt)
        context = self._query('receipt_context', {'principal': principal, 'receipt': receipt})
        if context.get('archived'):
            return context['acknowledgement']
        self._verify_artifacts(principal, context['binding'], receipt, retain=not context['prior'],
                               retention_target=context.get('retention_target'))
        return self._command('receipt', {'principal': principal, 'receipt': receipt,
            'artifacts_verified': receipt['output_profile'] == ARTIFACT_OUTPUT_PROFILE})

    def evidence(self, principal, call_id):
        return self._query('evidence', {'principal': principal, 'call_id': call_id})

    def reconcile(self, *, run_id, call_id, operator):
        return self._command('reconcile', {'run_id': run_id, 'call_id': call_id, 'operator': operator})


class SharedWorkerControl:
    def __init__(self, authority):
        self.authority = authority

    def status(self):
        return self.authority.query('operator.status', {})

    @contextmanager
    def admission(self):
        # This read avoids unnecessary local processes. The child's native task
        # claim and effect admission recheck the drain gate in their transaction.
        yield not self.status()['draining']

    def set(self, *, operator, request_key, reason, expected_revision, draining):
        return self.authority.command('operator.drain', {'operator': operator,
            'request_key': request_key, 'reason': reason, 'expected_revision': expected_revision,
            'draining': draining})['receipt']
