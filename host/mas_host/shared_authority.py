"""Shared PostgreSQL leaf-run profile through the Rust authority command port.

Local directories are replaceable work/guardian caches. Native protocol state,
run identity, checkpoints, effect intent and hierarchical allocations live in
PostgreSQL. Task and composition services use the same epoch-fenced transaction authority.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
import sys

from .codec import integer_json, is_actor_identity
from .contracts import RunnerError, RecoveryRequired
from .journal import JournalError, RunLock, canonical
from .topology import compile_topology


class SharedAuthorityRejected(RunnerError):
    """A sanitised native authority decision, distinct from transport failure.

Existing callers may continue catching RunnerError. The code names only a
fixed authority condition; database, credential and request details stay private.
    """
    CODES = frozenset({'stale_protocol_authority', 'controller_lease_expired'})

    def __init__(self, code):
        if code not in self.CODES:
            raise ValueError('unsupported shared authority rejection')
        self.code = code
        super().__init__(code)


class SharedAuthority:
    """Trusted host configuration; never construct from an actor's payload."""
    def __init__(self, *, binary, dsn, namespace, deployment, controller, epoch,
                 secret_key: bytes, secret_reference: str,
                 default_reservation_microusd=100_000, postgres_ca_file=None):
        executable = shutil.which(str(binary))
        if (not executable or not all(is_actor_identity(x) for x in (deployment, controller))
                or type(epoch) is not int or epoch <= 0 or type(dsn) is not str or not dsn
                or type(namespace) is not str or not namespace
                or type(secret_key) is not bytes or len(secret_key) < 32
                or not is_actor_identity(secret_reference)
                or type(default_reservation_microusd) is not int
                or not 0 <= default_reservation_microusd <= 2**63 - 1):
            raise RunnerError('invalid shared authority configuration')
        self._binary, self._dsn = executable, dsn
        self._postgres_ca_file = None if postgres_ca_file is None else str(Path(postgres_ca_file).resolve(strict=True))
        self._secret_key, self.secret_reference = secret_key, secret_reference
        self._binding = {'deployment': deployment, 'controller': controller, 'epoch': epoch}
        self.namespace = namespace
        self.default_reservation_microusd = default_reservation_microusd
        self._renew_process = None
        self._renew_lifetime = None

    def _request(self, operation, *, timeout=10, **fields):
        source = integer_json({'version': 1, 'schema': self.namespace, 'binding': self._binding,
                               'operation': operation, **fields}, 1_048_576)
        try:
            result = subprocess.run([self._binary], input=source, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=timeout,
                env={'PATH': os.environ.get('PATH', os.defpath), 'MAS_POSTGRES_DSN': self._dsn,
                     **({'MAS_POSTGRES_CA_FILE': self._postgres_ca_file} if self._postgres_ca_file else {})},
                check=False)
            if len(result.stdout.encode('utf-8')) > 16 * 1024 * 1024:
                raise ValueError
            decoded = json.loads(result.stdout)
            if (type(decoded) is not dict or type(decoded.get('version')) is not int
                    or decoded['version'] != 1 or (decoded.get('ok') is False
                    and type(decoded.get('error')) is not dict)):
                raise ValueError
            if (decoded.get('version') == 1 and decoded.get('ok') is False
                    and decoded.get('error', {}).get('code') in SharedAuthorityRejected.CODES):
                raise SharedAuthorityRejected(decoded['error']['code'])
            if (decoded.get('version') == 1 and decoded.get('ok') is False
                    and decoded.get('error', {}).get('code') != 'shared_authority_rejected'
                    and operation in ('command', 'query')):
                from .service_contracts import ServiceError
                raise ServiceError(decoded['error']['code'])
            if result.returncode or type(decoded.get('version')) is not int or decoded['version'] != 1 or decoded.get('ok') is not True:
                raise ValueError
            return decoded['result']
        except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired):
            raise RunnerError('shared authority unavailable or command rejected; reconcile original identities') from None

    def initialise(self, *, command_capacity=None):
        """Provision a new namespace; an existing namespace is never reset."""
        return self._request('initialise', **({'command_capacity': command_capacity} if command_capacity is not None else {}))

    def install_host(self, *, command_capacity=None):
        return self._request('install_host', **({'command_capacity': command_capacity} if command_capacity is not None else {}))

    def command(self, operation, parameters, *, command_id=None):
        return self._request('command', command=operation, parameters=parameters,
                             command_id=command_id or 'cmd-' + uuid.uuid4().hex)

    def query(self, operation, parameters):
        return self._request('query', query=operation, parameters=parameters)

    def configure_scope(self, scope, *, max_runs, max_calls, max_microusd):
        parameters = {'scope': scope, 'max_runs': max_runs,
            'max_calls': max_calls, 'max_microusd': max_microusd}
        command_id = 'scope-' + hashlib.sha256(canonical(parameters).encode()).hexdigest()
        return self.command('configure_scope', parameters, command_id=command_id)['receipt']

    @property
    def admission_enabled(self):
        # A successful query validates the bound controller epoch. A stale or
        # inaccessible authority cannot supply an optimistic local fallback.
        return not self.query('authority.status', {})['draining']

    def circuit_breaker(self, policy):
        from .shared_remote import SharedCircuitBreaker
        return SharedCircuitBreaker(self, policy)

    def authority_status(self):
        return self._request('authority_status')

    def renew(self, *, lease_seconds=60):
        return self._request('renew', lease_seconds=lease_seconds)

    def start_renewal(self, *, lease_seconds=60):
        """Maintain only this epoch in a child process; keep fork owners single-threaded."""
        if type(lease_seconds) is not int or not 3 <= lease_seconds <= 3600:
            raise RunnerError('invalid controller lease interval')
        if self._renew_process is not None and self._renew_process.poll() is None:
            return
        self.close()
        self.renew(lease_seconds=lease_seconds)
        read_fd, write_fd = os.pipe()
        try:
            process = subprocess.Popen([sys.executable, '-m', 'mas_host.shared_lease',
                str(read_fd), str(os.getpid())], stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, pass_fds=(read_fd,),
                env={'PATH': os.environ.get('PATH', os.defpath),
                     'PYTHONPATH': str(Path(__file__).resolve().parents[1]),
                     'PYTHONNOUSERSITE': '1'})
            payload = {'binary': self._binary, 'dsn': self._dsn, 'schema': self.namespace,
                'binding': self._binding, 'lease_seconds': lease_seconds,
                'ca_file': self._postgres_ca_file}
            process.stdin.write(integer_json(payload).encode())
            process.stdin.close()
            self._renew_process, self._renew_lifetime = process, write_fd
            write_fd = None
        except (OSError, BrokenPipeError):
            raise RunnerError('controller lease guardian unavailable') from None
        finally:
            os.close(read_fd)
            if write_fd is not None:
                os.close(write_fd)

    def close(self):
        if self._renew_lifetime is not None:
            os.close(self._renew_lifetime)
            self._renew_lifetime = None
        process, self._renew_process = self._renew_process, None
        if process is not None:
            try:
                process.wait(timeout=11)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def handover(self, next_controller):
        """Explicitly transfer this live database epoch; returns successor binding."""
        result = self._request('handover', next_controller=next_controller)
        self.close()
        return result

    def takeover(self, next_controller, *, lease_seconds=60):
        """Acquire only after the prior bound lease expires; preserves every identity."""
        result = self._request('takeover', next_controller=next_controller, lease_seconds=lease_seconds)
        self.close()
        return result

    def set_draining(self, draining, *, operator, evidence, command_id=None):
        return self.command('authority.drain', {'draining': draining, 'operator': operator,
            'evidence': evidence}, command_id=command_id)['receipt']

    def resolve_invocation(self, run_id, call_id, *, operator, decision, evidence, output=None, command_id=None):
        """Audited recovery only; never calls a provider or clears its reservation."""
        command_id = command_id or 'resolve-' + uuid.uuid4().hex
        owner = 'recovery-' + hashlib.sha256(command_id.encode()).hexdigest()[:48]
        self.command('claim_run', {'run': run_id, 'owner': owner})
        try:
            return self.command('resolve_invocation', {'run': run_id, 'owner': owner,
                'call_id': call_id, 'operator': operator, 'decision': decision,
                'evidence': evidence, 'output': output}, command_id=command_id)['receipt']
        finally:
            self.command('release_run', {'run': run_id, 'owner': owner})

    def runner(self, topology, adapters, cache_dir, *, principal, run_id=None, new_run_id=None,
               ancestors=(), **options):
        from .runner import Runner
        if options.get('assessment_policies'):
            raise RunnerError('assessment journal requires local SQLite persistence')
        if any(key in options for key in ('operations', 'protocol_client', 'session_factory', '_resume_dir')):
            raise RunnerError('shared authority owns runner persistence configuration')
        if (not is_actor_identity(principal) or run_id is not None and not is_actor_identity(run_id)
                or new_run_id is not None and not is_actor_identity(new_run_id)
                or run_id is not None and new_run_id is not None):
            raise RunnerError('invalid shared run principal or identity')
        transport = SharedProtocolTransport(self)
        def session_factory(state_dir, **arguments):
            return SharedRunSession(state_dir, authority=self, transport=transport,
                principal=principal, retained_run_id=run_id, new_run_id=new_run_id,
                ancestors=tuple(ancestors), **arguments)
        return Runner(topology, adapters, Path(cache_dir), operations=self,
            protocol_client=transport, session_factory=session_factory, **options)

    def configuration(self, run_id, topology):
        maximum = min(2**63 - 1, 100 + 6 * topology['max_messages'] + 3 * topology['max_calls'])
        credentials = []
        for index, actor in enumerate(topology['agents']):
            material = canonical([self._binding['deployment'], run_id, actor]).encode()
            token = hmac.new(self._secret_key, material, hashlib.sha256).hexdigest()
            credentials.append({'credential_id': f'agent-{index}', 'token': token,
                'run_id': run_id, 'actor': actor, 'max_attempts': maximum})
        return {'gateway_id': 'host-' + hashlib.sha256(run_id.encode()).hexdigest()[:27],
                'max_attempts': maximum, 'credentials': credentials}


class SharedProtocolTransport:
    mas = 'shared-protocol'
    gateway = 'shared-gateway'

    def __init__(self, authority):
        self.authority = authority
        self.run_id = self.configuration = None

    def command(self, args, source=None, *, timeout=10, empty_response=False):
        if args == [self.mas, 'encode', '-'] and source is not None:
            return self.authority._request('encode', source=source, timeout=timeout)
        for operation in ('status', 'reconcile'):
            if args == [self.mas, operation, self.run_id, self.run_id] and source is None:
                return self.authority._request(operation, run=self.run_id, timeout=timeout)
        if args == [self.gateway, 'serve', self.run_id, self.run_id] and source is not None:
            return self.authority._request('gateway', configuration=self.configuration,
                frame=source.rstrip('\n'), timeout=timeout)
        raise RunnerError('unsupported shared protocol command')

    def validate_gateway(self, _database, _configuration, *, timeout=10):
        self.authority._request('validate_gateway', configuration=self.configuration, timeout=timeout)


class SharedRunSession:
    def __init__(self, state_dir, *, authority, transport, principal, retained_run_id,
                 ancestors, topology, workflow_version, max_depth, actors, new_run_id=None,
                 command_timeout=10, **_local_arguments):
        self.authority = authority
        self.journal = self.lock = None
        self.owner = 'owner-' + uuid.uuid4().hex
        self.run_id = retained_run_id or new_run_id or 'run-' + uuid.uuid4().hex
        self.run_dir = Path(state_dir) / self.run_id
        self.run_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.run_dir.is_symlink():
            raise RunnerError('shared run cache cannot be a symbolic link')
        claimed = False
        try:
            # This lock protects local guardian/cache lifetime. PostgreSQL run
            # ownership and controller epochs supply shared admission authority.
            self.lock = RunLock(self.run_dir)
            if retained_run_id is None:
                frozen = compile_topology(topology)
                metadata = {'run_id': self.run_id, 'workflow_version': workflow_version,
                    'topology': frozen, 'max_depth': max_depth,
                    'secret_reference': authority.secret_reference, 'principal': principal}
                authority.command('create_run', {'run': self.run_id, 'principal': principal,
                    'ancestors': list(ancestors), 'configuration': frozen, 'metadata': metadata},
                    command_id='create-' + self.run_id)
            metadata = authority.query('metadata', {'run': self.run_id})
            if (metadata.get('principal') != principal or metadata['workflow_version'] != workflow_version or metadata['max_depth'] != max_depth
                    or metadata['secret_reference'] != authority.secret_reference
                    or retained_run_id is not None and workflow_version == 'unversioned'
                    or set(metadata['topology']['agents']) != actors):
                raise RunnerError('shared resume requires original implementation, secret reference and actors')
            authority.command('claim_run', {'run': self.run_id, 'owner': self.owner})
            claimed = True
            self.snapshot = authority._request('status', run=self.run_id, timeout=command_timeout)
            if self.snapshot['config'] != metadata['topology']:
                raise RunnerError('shared topology differs from native protocol state')
            private = authority.configuration(self.run_id, metadata['topology'])
            transport.run_id, transport.configuration = self.run_id, private
            transport.validate_gateway(None, None, timeout=command_timeout)
            self.tokens = {item['actor']: item['token'] for item in private['credentials']}
            self.database = self.configuration = self.run_id
            self.journal = SharedJournal(authority, self.run_id, self.owner)
        except BaseException:
            if claimed:
                try:
                    authority.command('release_run', {'run': self.run_id, 'owner': self.owner})
                except Exception:
                    pass
            if self.lock:
                self.lock.close()
            raise

    def admit_invocation(self, call_id):
        reply = self.authority.command('admit_invocation',
            {'run': self.run_id, 'owner': self.owner, 'call_id': call_id},
            command_id='admit-' + call_id)
        if reply['replayed']:
            raise RecoveryRequired('admission receipt was replayed; reconcile the original effect')
        return reply['receipt']

    def close(self):
        try:
            if self.journal is not None:
                self.authority.command('release_run', {'run': self.run_id, 'owner': self.owner})
                self.journal = None
        finally:
            if self.lock is not None:
                self.lock.close()
                self.lock = None


class SharedJournal:
    def __init__(self, authority, run_id, owner):
        self.authority, self.run_id, self.owner = authority, run_id, owner

    def _command(self, operation, **values):
        return self.authority.command(operation, {'run': self.run_id, 'owner': self.owner, **values})['receipt']

    def _query(self, operation, **values):
        return self.authority.query(operation, {'run': self.run_id, **values})

    def metadata(self):
        return self._query('metadata')

    def step(self, key, kind, inputs, initial):
        return self._command('step', key=key, kind=kind, input=inputs, initial=initial)['data']

    def get_step(self, key):
        saved = self._query('step', key=key)
        return saved['data'] if saved is not None else None

    def set_step(self, key, data):
        saved = self._query('step', key=key)
        if saved is None:
            raise JournalError('shared checkpoint is missing')
        self._command('set_step', key=key, expected_revision=saved['revision'], data=data)

    def queue_event(self, event):
        key = 'event:' + event['message_id']
        saved = self._query('step', key=key)
        if saved is not None:
            if saved['kind'] not in ('event_pending', 'event_published') or saved['data'] != event:
                raise JournalError('protocol event identity conflict')
            return
        self.step(key, 'event_pending', {}, event)

    def pending_events(self):
        return [row['data'] for row in self._query('steps') if row['kind'] == 'event_pending']

    def publish_event(self, message_id):
        key = 'event:' + message_id
        saved = self._query('step', key=key)
        if saved and saved['kind'] == 'event_pending':
            self._command('set_step', key=key, expected_revision=saved['revision'],
                          data=saved['data'], kind='event_published')

    def recorded_envelopes(self):
        for row in self._query('steps'):
            if row['kind'] == 'request':
                for key in ('request', 'terminal'):
                    if row['data'].get(key) is not None:
                        yield row['data'][key]

    def child_count(self, request_id):
        return sum(row['key'].startswith(request_id + ':') for row in self._query('steps'))

    def invocation(self, call_id):
        return self._query('invocation', call_id=call_id)

    def create_invocation(self, step_key, actor, reservation_microusd):
        amount = self.authority.default_reservation_microusd if reservation_microusd is None else reservation_microusd
        return self._command('prepare_invocation', step_key=step_key, actor=actor,
            call_id='call-' + uuid.uuid4().hex, reservation_microusd=amount)

    def update_invocation(self, call_id, state, *, output=None, evidence=None):
        return self._command('observe_invocation', call_id=call_id, state=state,
                             output=output, evidence=evidence)

    def record_known_failure(self, call_id, outcome, *, completed_output=None):
        from .provider_outcomes import validate_provider_outcome, validate_completed_output
        checked = validate_provider_outcome(outcome)
        output = validate_completed_output(checked, completed_output)
        evidence = {'outcome': 'not_executed' if checked['certainty'] == 'not_sent' else 'completed',
                    'provider_outcome': checked}
        return self.update_invocation(call_id, 'failed_known', output=output, evidence=evidence)

    def schedule_retry(self, call_id, policy, *, next_step_key, now_ms):
        call = self.invocation(call_id)
        if call is None or call['state'] != 'failed_known':
            return None
        saved = self._query('step', key=call['step_key'])
        inputs, data = saved['input'], saved['data']
        attempt = inputs.get('attempt', {}).get('number', 1)
        if inputs.get('retry_policy') != policy.descriptor or not policy.eligible(call['evidence']['provider_outcome'], attempt):
            return None
        if 'retry_decision' in data:
            plan = data['retry_decision']
            if plan['policy'] != policy.descriptor or plan['step_key'] != next_step_key:
                raise JournalError('retry policy or step differs from durable decision')
            return plan
        retry_after = call['evidence']['provider_outcome'].get('retry_after_seconds', 0)
        plan = {'previous_call_id': call_id,
            'root_call_id': inputs.get('attempt', {}).get('root_call_id', call_id),
            'number': attempt + 1, 'eligible_at_ms': now_ms + max(policy.delay_ms(attempt), retry_after * 1000),
            'step_key': next_step_key, 'policy': policy.descriptor}
        if retry_after:
            plan['retry_after_seconds'] = retry_after
        self._command('set_step', key=call['step_key'], expected_revision=saved['revision'],
                      data={**data, 'retry_decision': plan})
        return plan

    def retry_inputs(self, call_id, *, actor, step_key, inputs, policy, now_ms):
        call = self.invocation(call_id)
        if call is None or call['actor'] != actor or call['state'] != 'failed_known':
            raise JournalError('retry does not name a known failed invocation')
        saved = self._query('step', key=call['step_key'])
        previous, plan = saved['input'], saved['data'].get('retry_decision')
        if (type(plan) is not dict or plan['previous_call_id'] != call_id or plan['step_key'] != step_key
                or plan['policy'] != policy.descriptor or now_ms < plan['eligible_at_ms']
                or not policy.eligible(call['evidence']['provider_outcome'], plan['number'] - 1)
                or any(canonical(previous[name]) != canonical(inputs[name]) for name in ('input', 'reservation_microusd'))):
            raise JournalError('retry is not eligible or its binding changed')
        return {'number': plan['number'], 'root_call_id': plan['root_call_id'], 'previous_call_id': call_id}

    def close(self):
        pass
