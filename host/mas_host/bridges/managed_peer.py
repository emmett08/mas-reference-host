"""Authorised use of immutable, administrator-registered federation peers."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from urllib.parse import urlsplit, urlunsplit

from ..peer_registry import PeerRegistry
from ..service_contracts import ServiceError
from .orchestrator import OrchestratorConnector, PeerContract
from .peer_clients import A2ATaskServiceClient, MCPTaskServiceClient
from .authentication import read_token_file
from .http_policy import HTTPTransportPolicy
from .service_client import TaskServiceClient, http_exchange


def _credential_bound(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        with self._credentials():
            return method(self, *args, **kwargs)
    return call


class ManagedPeerConnector(OrchestratorConnector):
    """Trusted caller identity is never taken from workflow arguments.

    The selected peer version remains fixed for this connector. Every operation
    rechecks local caller grants and revocation, then negotiates remote identity.
    A remote effect already sent at revocation can still complete. The remote
    deployment owns its own admission and budget; no shared budget is implied.
    """
    def __init__(self, registry: PeerRegistry, name, caller, *, token_file,
                 version=None, identity=None, timeout_seconds=30, transport_policy=None):
        registration = registry.resolve(name, caller, version=version, identity=identity)
        binding = registration.descriptor
        if (type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60
                or transport_policy is not None and not isinstance(transport_policy, HTTPTransportPolicy)):
            raise ServiceError('invalid_peer_configuration')
        actual_policy = (transport_policy or HTTPTransportPolicy()).descriptor()
        if (actual_policy != binding.get('transport_policy', HTTPTransportPolicy().descriptor())
                or timeout_seconds != binding.get('timeout_seconds', 30)):
            raise ServiceError('peer_binding_changed')
        self.registry, self.caller, self.registration = registry, caller, registration
        self._token_file = token_file
        self._operation_token = ContextVar('managed_peer_token', default=None)
        options = {'token_file': token_file, 'principal': binding['outbound_principal'],
                   'timeout_seconds': timeout_seconds, 'allow_loopback_http': binding['allow_loopback_http'],
                   'transport_policy': transport_policy}
        clients = {'mas-http/1': TaskServiceClient, 'mas-mcp/1': MCPTaskServiceClient,
                   'mas-a2a/1': A2ATaskServiceClient}
        credential_options = ({'transport': self._transport} if binding['profile'] == 'mas-http/1'
                              else {'token_provider': self._token})
        super().__init__(clients[binding['profile']](binding['endpoint'], **options, **credential_options),
            binding['outbound_principal'], peer_contract=PeerContract(binding['deployment_id'],
                binding['workflows'], tuple(binding['capabilities'])))
        parsed = urlsplit(binding['endpoint'])
        self._artifact_service = TaskServiceClient(urlunsplit(parsed._replace(path='')),
            transport=self._transport, **options)

    def _token(self):
        self._check_transport()
        return self._operation_token.get() or read_token_file(self._token_file)

    @contextmanager
    def _credentials(self):
        self._check_transport()
        # Bind discovery and the subsequent effect to exactly the same bearer.
        # Context-local state prevents concurrent calls sharing rotation state.
        if self._operation_token.get() is not None:
            yield
            return
        marker = self._operation_token.set(read_token_file(self._token_file))
        try:
            yield
        finally:
            self._operation_token.reset(marker)

    def _transport(self, endpoint, **options):
        options['headers'] = {**options.get('headers', {}), 'Authorization': 'Bearer ' + self._token()}
        return http_exchange(endpoint, **options)

    def descriptor(self):
        self._check_transport()
        binding = self.registration.descriptor
        value = {'format': 'mas-managed-peer/1', 'name': binding['name'],
                'version': self.registration.descriptor['version'], 'identity': self.registration.identity,
                'caller': self.caller, 'principal': self.principal}
        # Legacy omitted fields continue to mean direct transport and 30s.
        for key in ('transport_policy', 'timeout_seconds'):
            if key in binding:
                value[key] = binding[key]
        return value

    def _check_transport(self):
        binding = self.registration.descriptor
        expected = binding.get('transport_policy', HTTPTransportPolicy().descriptor())
        artifact_endpoint = urlunsplit(urlsplit(binding['endpoint'])._replace(path=''))
        for client, endpoint in ((self.service, binding['endpoint']), (self._artifact_service, artifact_endpoint)):
            policy = client.transport_policy
            if (client.endpoint != endpoint or client.principal != binding['outbound_principal']
                    or client.allow_loopback_http is not binding['allow_loopback_http']
                    or policy is not None and not isinstance(policy, HTTPTransportPolicy)
                    or (policy or HTTPTransportPolicy()).descriptor() != expected
                    or type(client.timeout_seconds) is not int
                    or client.timeout_seconds != binding.get('timeout_seconds', 30)):
                raise ServiceError('peer_binding_changed')

    def _authorise(self, capability=None):
        self._check_transport()
        binding = self.registration.descriptor
        self.registry.resolve(binding['name'], self.caller,
            version=binding['version'], identity=self.registration.identity)
        if capability is not None and capability not in binding['capabilities']:
            raise ServiceError('peer_capability_forbidden')

    @_credential_bound
    def negotiate(self):
        self._authorise()
        observed = super().negotiate()
        registrations = observed.get('registrations')
        if observed.get('principal') != self.principal or type(registrations) is not list:
            raise ServiceError('peer_identity_mismatch')
        identities = {}
        for item in registrations:
            if (type(item) is not dict or type(item.get('name')) is not str
                    or item['name'] in identities):
                raise ServiceError('peer_contract_mismatch')
            identities[item['name']] = item.get('identity') if item.get('available') is True else None
        if any(identities.get(name) != identity for name, identity in self.peer_contract.workflows.items()):
            raise ServiceError('peer_contract_mismatch')
        return observed

    @_credential_bound
    def submit(self, request_key, workflow, input):
        self._authorise('submit')
        return super().submit(request_key, workflow, input)

    @_credential_bound
    def inspect(self, task_id):
        self._authorise('inspect')
        self.negotiate()
        return super().inspect(task_id)

    @_credential_bound
    def result(self, task_id):
        self._authorise('result')
        return super().result(task_id)

    @_credential_bound
    def cancel(self, task_id):
        self._authorise('cancel')
        self.negotiate()
        # Inspect the pinned workflow before issuing an effect on a remote ID.
        super().inspect(task_id)
        return super().cancel(task_id)

    @_credential_bound
    def update(self, task_id, request_key, input):
        self._authorise('update')
        return super().update(task_id, request_key, input)

    def artifacts(self, **options):
        """Use the same-origin artefact route, with per-chunk local revocation.

        Only sessions opened through this instance are accepted. After losing
        a transfer response, the server's operator surface performs recovery;
        a token alone never bypasses the registered scope allowlist.
        """
        from .artifact_client import ArtifactTransferClient
        sessions = set()
        def exchange(operation, payload):
            with self._credentials():
                return bound_exchange(operation, payload)
        def bound_exchange(operation, payload):
            self._authorise()
            if 'scope' in payload:
                if payload['scope'] not in self.registration.descriptor['artifact_scopes']:
                    raise ServiceError('peer_artifact_forbidden')
                self.negotiate()
            elif payload.get('transfer_id') not in sessions:
                raise ServiceError('peer_artifact_forbidden')
            value = self._artifact_service.artifact(self.principal, operation, payload)
            if operation in {'download_begin', 'upload_begin'}:
                sessions.add(value.get('transfer_id'))
            if operation == 'close':
                sessions.discard(payload.get('transfer_id'))
            return value
        return ArtifactTransferClient(exchange, descriptor=self.descriptor(), **options)
