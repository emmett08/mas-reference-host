"""Fixed MAS federation profiles over MCP and A2A; no discovered code or URLs.

These adapt known MAS lifecycle operations, not arbitrary third-party tools.
Requests have bounded transports and never automatically repeat effects.
"""
from __future__ import annotations

import uuid

from ..codec import integer_json
from ..service_contracts import ServiceError
from .authentication import read_token_file
from .service_client import decode_integer_json, http_exchange, validate_endpoint
from .orchestrator import receipt


class _ProfileClient:
    path = ''

    def __init__(self, endpoint, *, token_file, principal, timeout_seconds=30,
                 allow_loopback_http=False, transport_policy=None, token_provider=None):
        parsed = validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
        if parsed.path != self.path or type(principal) is not str or not principal:
            raise ServiceError('invalid_peer_configuration')
        self.endpoint, self.token_file, self.principal = endpoint, token_file, principal
        self.timeout_seconds, self.allow_loopback_http = timeout_seconds, allow_loopback_http
        self.transport_policy = transport_policy
        if token_provider is not None and not callable(token_provider):
            raise ServiceError('invalid_peer_configuration')
        self.token_provider = token_provider or (lambda: read_token_file(self.token_file))

    def _principal(self, principal):
        if principal != self.principal:
            raise ServiceError('principal_mismatch')


class MCPTaskServiceClient(_ProfileClient):
    """Only frozen MAS tool schemas are callable through this lifecycle port."""
    path = '/mcp'

    def _client(self):
        from .mcp_client import MCPClient
        from .mcp_transport import StreamableHTTPTransport
        return MCPClient(StreamableHTTPTransport(self.endpoint,
            token=self.token_provider(), timeout_seconds=self.timeout_seconds,
            allow_loopback_http=self.allow_loopback_http, transport_policy=self.transport_policy))

    def _call(self, principal, name, arguments):
        from .mcp import TOOLS
        self._principal(principal)
        result = self._client().call_tool(next(tool for tool in TOOLS if tool['name'] == 'mas.' + name), arguments)
        if name == 'submit':
            meta = result.get('_meta', {})
            return receipt({'task_id': result.get('taskId'), 'state': meta.get('org.mas/state'),
                'workflow': meta.get('org.mas/workflow'), 'workflow_identity': meta.get('org.mas/workflowIdentity'),
                'cancel_requested': meta.get('org.mas/cancelRequested', False)})
        value = result.get('structuredContent')
        if result.get('resultType') != 'complete' or type(value) is not dict:
            raise ServiceError('invalid_peer_response')
        return value

    def discover(self, principal):
        from .mcp import SERVICE_PROFILE, TASKS
        self._principal(principal)
        observed = self._client().discover()
        capabilities = observed.get('capabilities')
        extensions = capabilities.get('extensions') if type(capabilities) is dict else None
        if (type(extensions) is not dict or type(extensions.get(SERVICE_PROFILE)) is not dict
                or type(extensions.get(TASKS)) is not dict):
            raise ServiceError('peer_profile_mismatch')
        return self._call(principal, 'discover', {})

    def submit(self, principal, request_key, workflow, input, **pins):
        return self._call(principal, 'submit', {'request_key': request_key, 'workflow': workflow, 'input': input, **pins})

    def inspect(self, principal, task_id):
        return self._call(principal, 'status', {'task_id': task_id})

    def result(self, principal, task_id):
        return self._call(principal, 'result', {'task_id': task_id})

    def cancel(self, principal, task_id):
        return self._call(principal, 'cancel', {'task_id': task_id})

    def update(self, principal, task_id, request_key, input, **pins):
        return self._call(principal, 'update', {'task_id': task_id, 'request_key': request_key, 'input': input, **pins})


class A2ATaskServiceClient(_ProfileClient):
    path = '/a2a'

    def _rpc(self, principal, method, params):
        from .a2a import A2AError, VERSION, SERVICE_PROFILE, BOUNDED_PROFILE
        self._principal(principal)
        request_id = uuid.uuid4().hex
        status, headers, body = http_exchange(self.endpoint,
            body=integer_json({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params}, 65536).encode(),
            headers={'Authorization': 'Bearer ' + self.token_provider(),
                     'Content-Type': 'application/json', 'Accept': 'application/json', 'A2A-Version': VERSION,
                     'A2A-Extensions': SERVICE_PROFILE + ',' + BOUNDED_PROFILE},
            timeout_seconds=self.timeout_seconds, max_response_bytes=65536,
            allow_loopback_http=self.allow_loopback_http, transport_policy=self.transport_policy)
        if status != 200 or headers.get('content-type', '').split(';')[0].strip().lower() != 'application/json':
            raise ServiceError('peer_transport_rejected')
        value = decode_integer_json(body, max_bytes=65536)
        if (type(value) is not dict or value.get('jsonrpc') != '2.0' or value.get('id') != request_id
                or ('result' in value) == ('error' in value)):
            raise ServiceError('invalid_peer_response')
        if 'error' in value:
            error = value['error']
            if type(error) is not dict or type(error.get('code')) is not int:
                raise ServiceError('invalid_peer_response')
            raise A2AError(error['code'], 'peer_operation_rejected')
        if type(value['result']) is not dict:
            raise ServiceError('invalid_peer_response')
        return value['result']

    def discover(self, principal):
        from .a2a import SERVICE_PROFILE, VERSION
        card = self._rpc(principal, 'GetExtendedAgentCard', {})
        interfaces = card.get('supportedInterfaces')
        capabilities = card.get('capabilities')
        extensions = capabilities.get('extensions') if type(capabilities) is dict else None
        if (type(interfaces) is not list or not any(type(item) is dict
                and item.get('protocolBinding') == 'JSONRPC' and item.get('protocolVersion') == VERSION
                and item.get('url') == self.endpoint for item in interfaces)
                or type(extensions) is not list):
            raise ServiceError('peer_profile_mismatch')
        matches = [item for item in extensions if type(item) is dict and item.get('uri') == SERVICE_PROFILE]
        if len(matches) != 1 or type(matches[0].get('params')) is not dict:
            raise ServiceError('peer_profile_mismatch')
        observed = matches[0]['params'].get('service')
        if type(observed) is not dict:
            raise ServiceError('peer_profile_mismatch')
        return observed

    @staticmethod
    def _receipt(task):
        from .a2a import STATE_TO_WIRE
        if type(task) is not dict or type(task.get('metadata')) is not dict:
            raise ServiceError('invalid_peer_response')
        meta = task['metadata'].get('mas', {})
        if (type(meta) is not dict or type(task.get('status')) is not dict
                or type(meta.get('state')) is not str or meta.get('state') not in STATE_TO_WIRE
                or task['status'].get('state') != STATE_TO_WIRE[meta['state']]):
            raise ServiceError('invalid_peer_response')
        return receipt({'task_id': task.get('id'), 'state': meta['state'],
            'workflow': meta.get('workflow'), 'workflow_identity': meta.get('workflowIdentity'),
            'cancel_requested': meta.get('cancelRequested', False)})

    def submit(self, principal, request_key, workflow, input, **pins):
        result = self._rpc(principal, 'SendMessage', {'message': {'messageId': request_key,
            'role': 'ROLE_USER', 'parts': [{'data': {'workflow': workflow, 'input': input, **pins},
                                          'mediaType': 'application/json'}]},
            'configuration': {'returnImmediately': True}})
        return self._receipt(result.get('task'))

    def inspect(self, principal, task_id):
        return self._receipt(self._rpc(principal, 'GetTask', {'id': task_id}))

    def result(self, principal, task_id):
        task = self._rpc(principal, 'GetTask', {'id': task_id})
        if self._receipt(task)['state'] != 'completed':
            raise ServiceError('result_unavailable')
        artifacts = task.get('artifacts', [])
        matches = [item for item in artifacts if type(item) is dict and item.get('name') == 'result']
        if (len(matches) != 1 or type(matches[0].get('parts')) is not list or len(matches[0]['parts']) != 1
                or type(matches[0]['parts'][0].get('data')) is not dict):
            raise ServiceError('invalid_peer_response')
        return matches[0]['parts'][0]['data']

    def cancel(self, principal, task_id):
        from .a2a import A2AError
        try:
            return self._receipt(self._rpc(principal, 'CancelTask', {'id': task_id}))
        except A2AError as error:
            if error.rpc_code != -32002:
                raise
            # A pending cancellation is observed with a separate read, never
            # automatically retried or represented as stopped execution.
            return self.inspect(principal, task_id)

    def update(self, principal, task_id, request_key, input, **pins):
        current = self.inspect(principal, task_id)
        result = self._rpc(principal, 'SendMessage', {'message': {'messageId': request_key,
            'taskId': task_id, 'role': 'ROLE_USER', 'parts': [{'data': {
                'workflow': current['workflow'], 'input': input, **pins}, 'mediaType': 'application/json'}]},
            'configuration': {'returnImmediately': True}})
        return self._receipt(result.get('task'))
