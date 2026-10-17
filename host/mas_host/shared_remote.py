"""PostgreSQL-backed peers, provider circuits and admitted remote effects.

The Rust authority port owns persistence and every transition. Local paths are
run cache identities only; none of these repositories creates a SQLite shadow.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
from pathlib import Path

from .codec import canonical_json
from .contracts import Invocation, RunnerError
from .peer_registry import PeerRegistration, identifier
from .service_contracts import ServiceError
from .bridges.remote import bounded, digest, peer_identifier, token


def _command_id(*parts):
    return 'remote-' + hashlib.sha256(canonical_json(list(parts)).encode()).hexdigest()


class SharedPeerRegistry:
    """Same trusted administrative boundary as the local peer registry."""
    def __init__(self, authority):
        self.authority = authority

    def status(self):
        return self.authority.query('peer.status', {})

    def resolve(self, name, caller, *, version=None, identity=None):
        parameters = {'name': identifier(name), 'caller': identifier(caller)}
        if version is not None:
            parameters['version'] = identifier(version)
        if identity is not None:
            parameters['identity'] = identity
        value = self.authority.query('peer.resolve', parameters)
        registration = PeerRegistration.from_dict(value['registration'])
        if registration.identity != value['identity']:
            raise ServiceError('peer_binding_changed')
        return registration

    def _change(self, action, *, operator, request_key, reason, expected_revision, **fields):
        identifier(operator); identifier(request_key)
        if (type(reason) is not str or not reason.strip() or len(reason) > 2048
                or any(ord(character) < 32 for character in reason)
                or type(expected_revision) is not int or expected_revision < 0):
            raise ServiceError('invalid_peer_change')
        parameters = {'operator': operator, 'request_key': request_key, 'reason': reason,
                      'expected_revision': expected_revision, **fields}
        return self.authority.command('peer.' + action, parameters,
            command_id=_command_id('peer-change', request_key))['receipt']

    def register(self, *, registration, **options):
        if not isinstance(registration, PeerRegistration):
            registration = PeerRegistration.from_dict(registration)
        return self._change('register', encoded=registration._encoded, **options)

    def revoke(self, *, name, version, **options):
        return self._change('revoke', name=identifier(name), version=identifier(version), **options)


class SharedCircuitBreaker:
    """Authority-clock cooldowns and single shared half-open probe ownership."""
    def __init__(self, authority, policy):
        from .circuit_breaker import CircuitPolicy
        if not isinstance(policy, CircuitPolicy):
            raise RunnerError('invalid provider circuit policy')
        self.authority, self.policy = authority, policy

    def admit(self, call_id, *, now_ms=None):
        if now_ms is not None:
            raise RunnerError('shared circuit time belongs to the database authority')
        return self.authority.command('circuit.admit', {'policy': self.policy.descriptor,
            'call_id': call_id})['receipt']['admitted']

    def observe(self, call_id, category, *, now_ms=None):
        if now_ms is not None or category not in {'success', 'provider_failure', 'aborted'}:
            raise RunnerError('invalid shared provider circuit observation')
        self.authority.command('circuit.observe', {'policy': self.policy.descriptor,
            'call_id': call_id, 'category': category},
            command_id=_command_id('circuit-observe', call_id, category))

    def status(self):
        return self.authority.query('circuit.status', {'policy': self.policy.descriptor})


class SharedRemoteTaskService:
    """RemoteClient lifecycle with intent-before-I/O and fenced completion.

    Each outward operation executes within AdapterContext.invoke. The authority
    verifies its already charged native invocation inside the intent transaction.
    Replayed command receipts never authorise another outward call. Unknown
    submissions require attachment with evidence; unknown mutations require an
    explicit recorded resolution. Reads can resume after authority handover.
    """
    def __init__(self, authority, client, *, namespace):
        if type(namespace) is not str or not 1 <= len(namespace.encode()) <= 4096:
            raise ServiceError('trusted_namespace_required')
        self.authority, self.client = authority, client
        self.namespace = hashlib.sha256(namespace.encode()).hexdigest()
        self.identity = bounded(client.descriptor(), 8192)

    def _base(self, key):
        if bounded(self.client.descriptor(), 8192) != self.identity:
            raise ServiceError('remote_identity_conflict')
        return {'namespace': self.namespace, 'logical_key': token(key), 'descriptor': self.identity}

    @staticmethod
    def _admission(context, host_run_dir, reservation):
        if type(reservation) is not int or not 0 <= reservation <= 100_000_000:
            raise ServiceError('invalid_reservation')
        return {'run': token(Path(host_run_dir).name), 'call_id': token(context.invocation_id),
                'reservation': reservation}

    def status(self, key):
        return self.authority.query('remote.status', self._base(key))

    def inspect(self, key):
        return self.authority.query('remote.inspect', self._base(key))

    def _finish(self, base, admission, operation_key, snapshot):
        snapshot.validate()
        parameters = {**base, 'run': admission['run'], 'call_id': admission['call_id'],
                      'operation_key': operation_key, 'snapshot': asdict(snapshot)}
        return self.authority.command('remote.complete', parameters,
            command_id=_command_id('complete', base['namespace'], base['logical_key'],
                                   admission['run'], admission['call_id'], operation_key))['receipt']

    @staticmethod
    def _begin(result):
        value = result['receipt']
        if value['dispatch'] is True and result['replayed'] is True:
            raise ServiceError('remote_outcome_unknown')
        return value

    def submit(self, key, payload, *, context, host_run_dir, reservation_microusd):
        base, payload = self._base(key), bounded(payload)
        fingerprint = digest({'descriptor': self.identity, 'input': payload})
        def execute():
            admission = self._admission(context, host_run_dir, reservation_microusd)
            begin = self._begin(self.authority.command('remote.submit_begin',
                {**base, **admission, 'payload': payload}, command_id=_command_id(
                    'submit', self.namespace, key, admission['run'], admission['call_id'])))
            if not begin['dispatch']:
                return Invocation(begin['output'], {'remote_key': key, 'replayed': True})
            snapshot = self.client.submit(payload).validate()
            output = self._finish(base, admission, 'submit', snapshot)
            return Invocation(output, {'remote_key': key, 'action': 'submit'})
        return context.invoke(execute, input={'remote': self.identity, 'namespace': self.namespace,
            'key': key, 'binding': fingerprint, 'action': 'submit'}, reservation_microusd=reservation_microusd)

    def _operate(self, key, action, *, context, host_run_dir, reservation_microusd=0,
                 request_key=None, payload=None, remote_id=None, evidence=None):
        base = self._base(key)
        if action in {'update', 'cancel'}:
            token(request_key)
        payload, evidence = bounded(payload or {}), bounded(evidence or {}, 8192)
        if action == 'attach':
            peer_identifier(remote_id)
            if not evidence:
                raise ServiceError('reconciliation_evidence_required')
        operation_binding = digest({'action': action, 'payload': payload, 'remote_id': remote_id, 'evidence': evidence})
        def execute():
            admission = self._admission(context, host_run_dir, reservation_microusd)
            operation = action + ':' + (request_key or admission['call_id'])
            parameters = {**base, **admission, 'action': action, 'operation_key': operation,
                          'payload': payload, 'evidence': evidence}
            if remote_id is not None:
                parameters['remote_id'] = remote_id
            begin = self._begin(self.authority.command('remote.operation_begin', parameters,
                command_id=_command_id('operation', self.namespace, key, operation,
                                       admission['run'], admission['call_id'])))
            if not begin['dispatch']:
                return Invocation(begin['output'], {'remote_key': key, 'replayed': True})
            selected = begin['remote_id']
            if action in {'poll', 'attach'}:
                snapshot = self.client.poll(selected)
            elif action == 'cancel':
                snapshot = self.client.cancel(selected)
            else:
                snapshot = self.client.update(selected, payload)
            output = self._finish(base, admission, operation, snapshot)
            return Invocation(output, {'remote_key': key, 'action': action})
        return context.invoke(execute, input={'remote': self.identity, 'namespace': self.namespace,
            'key': key, 'action': action, 'request_key': request_key, 'binding': operation_binding},
            reservation_microusd=reservation_microusd)

    def resolve_operation(self, key, operation_key, *, evidence, decision):
        if type(operation_key) is not str or not 1 <= len(operation_key) <= 512:
            raise ServiceError('invalid_identifier')
        evidence = bounded(evidence, 8192)
        if not evidence or decision not in {'applied', 'not_applied', 'observation_unavailable'}:
            raise ServiceError('reconciliation_evidence_required')
        return self.authority.command('remote.resolve', {**self._base(key), 'operation_key': operation_key,
            'evidence': evidence, 'decision': decision}, command_id=_command_id(
                'resolve', self.namespace, key, operation_key))['receipt']

    def poll(self, key, **options):
        return self._operate(key, 'poll', **options)

    def cancel(self, key, *, request_key='cancel', **options):
        return self._operate(key, 'cancel', request_key=request_key, **options)

    def update(self, key, *, request_key, payload, **options):
        return self._operate(key, 'update', request_key=request_key, payload=payload, **options)

    def attach(self, key, remote_id, *, evidence, **options):
        return self._operate(key, 'attach', remote_id=remote_id, evidence=evidence, **options)
