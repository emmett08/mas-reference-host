"""A2A 1.0 JSON-RPC polling boundary over the durable task service.

Wire names and enum values follow the normative 1.0 ProtoJSON contract, not the
incompatible 0.3 binding. The application profile accepts one application/json
DataPart containing {workflow, input}. Unsupported binary/URL parts remain
outside the bounded integer-JSON host contract.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import json
import time
import uuid

from ..contracts import AdapterContext, Outcome, TaskRequest
from ..service_contracts import ServiceError, TaskService
from .remote import RemoteSnapshot, RemoteTaskService, bounded, digest, peer_identifier, token
from .remote_http import BoundedJSONHTTP, endpoint_url

VERSION = '1.0'
SERVICE_PROFILE = 'urn:mas:task-service:1'
LIFECYCLE_PROFILE = 'urn:mas:lifecycle-inspection:1'
BOUNDED_PROFILE = 'urn:mas:bounded-json:1'
LIFECYCLE_METHODS = {
    'mas/ListTaskPage': ('list_task_page', set(), {'page_size', 'after', 'state', 'metadata', 'updated_after'}),
    'mas/InspectTask': ('inspect_task', {'task_id', 'path'}, {'revision'}),
    'mas/InspectGraph': ('inspect_graph', {'task_id'}, {'section', 'offset', 'limit'}),
    'mas/EventsPage': ('events_page', {'task_id'}, {'after', 'limit'}),
    'mas/RecoverTask': ('recover', {'task_id'}, set()),
}
STATE_TO_WIRE = {
    'queued': 'TASK_STATE_SUBMITTED', 'running': 'TASK_STATE_WORKING',
    'input_required': 'TASK_STATE_INPUT_REQUIRED', 'auth_required': 'TASK_STATE_AUTH_REQUIRED',
    'completed': 'TASK_STATE_COMPLETED', 'failed': 'TASK_STATE_FAILED',
    'cancelled': 'TASK_STATE_CANCELED', 'unknown': 'TASK_STATE_UNSPECIFIED',
    'blocked': 'TASK_STATE_SUBMITTED',
}
WIRE_TO_STATE = {value: key for key, value in STATE_TO_WIRE.items() if key != 'blocked'} | {'TASK_STATE_REJECTED': 'failed'}
# An unresolved effect is neither a terminal result nor an interruption. Keep
# blocking callers behind the same bounded wait until recovery settles it.
WAITING = frozenset({'queued', 'running', 'unknown', 'blocked'})


class A2AError(ServiceError):
    def __init__(self, code: int, reason: str):
        self.rpc_code = code
        super().__init__(reason)


def _fields(value, allowed):
    if type(value) is not dict or set(value) - set(allowed):
        raise A2AError(-32602, 'invalid_parameters')


def send_task_id(message: dict) -> str | None:
    """Classify SendMessage identically for scope checks and task dispatch.

    Only omission starts a task. A present identifier must name a local MAS
    task; null or malformed values can never turn a continuation into a submit.
    """
    if type(message) is not dict:
        raise A2AError(-32602, 'invalid_parameters')
    if 'taskId' not in message:
        return None
    try:
        return token(message['taskId'])
    except ServiceError:
        raise A2AError(-32602, 'invalid_identifier') from None


def validate_request(message: dict, metadata=None) -> tuple[str, object]:
    """Validate the wire envelope before interpreting operation scopes."""
    request_id = message.get('id') if type(message) is dict else None
    if (type(message) is not dict or message.get('jsonrpc') != '2.0'
            or 'id' not in message or type(request_id) not in (str, int)
            or type(message.get('method')) is not str or not message['method']
            or set(message) - {'jsonrpc', 'id', 'method', 'params'}):
        raise A2AError(-32600, 'invalid_request')
    bounded(message, 65_536)
    if metadata is not None and {k.lower(): v for k, v in metadata.items()}.get('a2a-version', VERSION) != VERSION:
        raise A2AError(-32009, 'version_not_supported')
    return message['method'], message.get('params', {})


def error_response(request_id, error: ServiceError) -> dict:
    """Keep pre-dispatch validation errors in the A2A JSON-RPC error contract."""
    if type(request_id) not in (str, int):
        request_id = None
    if isinstance(error, A2AError):
        code = error.rpc_code
    elif error.code in ('task_not_found', 'not_found', 'forbidden', 'unauthorised'):
        code = -32001
    elif error.code in ('task_not_continuable', 'task_terminal', 'invalid_transition', 'not_interrupted', 'unsupported_capability', 'invalid_task_state'):
        code = -32004
    elif error.code in ('invalid_identifier', 'invalid_payload', 'idempotency_conflict', 'request_conflict', 'unknown_workflow', 'invalid_input', 'input_too_large', 'input_not_encodable'):
        code = -32602
    else:
        code = -32603
    return {'jsonrpc': '2.0', 'id': request_id, 'error': {'code': code, 'message': error.code,
            'data': [{'@type': 'type.googleapis.com/google.rpc.ErrorInfo', 'reason': error.code.upper(), 'domain': 'mas'}]}}


def _message_key(identifier):
    """Preserve legacy keys; encode opaque wire IDs outside the local alphabet."""
    peer_identifier(identifier)
    try:
        return 'a2a:' + token(identifier)
    except ServiceError:
        return 'a2a-opaque:' + hashlib.sha256(identifier.encode('utf-8')).hexdigest()


def _history(value):
    if value is not None and (type(value) is not int or value < 0 or value > 10_000):
        raise A2AError(-32602, 'invalid_history_length')


def _time(value):
    if type(value) is str:
        return value
    # The service records milliseconds. Integer seconds remain useful for simple
    # test implementations of the TaskService port.
    seconds = value / 1000 if value > 100_000_000_000 else value
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def _parts(value, *, application=False):
    if type(value) is not list or not 1 <= len(value) <= 64:
        raise A2AError(-32602, 'invalid_message_parts')
    for part in value:
        if type(part) is not dict or len(set(part) & {'text', 'data', 'raw', 'url'}) != 1:
            raise A2AError(-32602, 'invalid_message_parts')
        if application and (set(part) - {'data', 'mediaType'} or 'data' not in part
                            or part.get('mediaType', 'application/json') != 'application/json'):
            raise A2AError(-32005, 'content_type_not_supported')
        if any(field in part and type(part[field]) is not str for field in ('text', 'url', 'raw', 'mediaType', 'filename')):
            raise A2AError(-32602, 'invalid_message_parts')
        if 'raw' in part:
            try:
                base64.b64decode(part['raw'], validate=True)
            except Exception:
                raise A2AError(-32602, 'invalid_message_parts') from None
    bounded({'parts': value})
    return value


class A2ABridge:
    def __init__(self, service: TaskService, *, endpoint: str = 'https://localhost/a2a',
                 workflow: str = 'default', name: str = 'MAS', wait_seconds: int = 20,
                 allow_http: bool = False, required_extensions=()):
        self.service, self.endpoint = service, endpoint_url(endpoint, allow_http=allow_http)
        self.workflow, self.name = workflow, name
        if type(wait_seconds) is not int or not 1 <= wait_seconds <= 60:
            raise ServiceError('invalid_wait_timeout')
        self.wait_seconds = wait_seconds
        if (type(required_extensions) not in (tuple, list, set, frozenset)
                or any(type(value) is not str for value in required_extensions)
                or len(set(required_extensions)) != len(required_extensions)
                or not set(required_extensions) <= self._supported_extensions()):
            raise ServiceError('invalid_required_extensions')
        self.required_extensions = frozenset(required_extensions)

    def _supported_extensions(self):
        supported = {BOUNDED_PROFILE}
        if callable(getattr(self.service, 'discover', None)):
            supported.add(SERVICE_PROFILE)
            if any(callable(getattr(self.service, target, None))
                   for target, _, _ in LIFECYCLE_METHODS.values()):
                supported.add(LIFECYCLE_PROFILE)
        return supported

    def _extensions(self, metadata):
        headers = {k.lower(): v for k, v in (metadata or {}).items()}
        raw = headers.get('a2a-extensions', '')
        if type(raw) is not str or len(raw) > 4096:
            raise A2AError(-32602, 'invalid_extensions')
        requested = [value.strip() for value in raw.split(',')] if raw else []
        if (len(requested) > 8 or len(set(requested)) != len(requested)
                or any(not value or len(value) > 512 or
                       any(ord(c) < 33 or ord(c) > 126 for c in value) for value in requested)):
            raise A2AError(-32602, 'invalid_extensions')
        if not set(requested) <= self._supported_extensions():
            raise A2AError(-32004, 'extension_not_supported')
        if not self.required_extensions <= set(requested):
            raise A2AError(-32008, 'extension_support_required')

    def agent_card(self) -> dict:
        return {'name': self.name, 'description': 'Bounded durable MAS workflows. Send one JSON data part with workflow and input; use returnImmediately for polling.',
                'version': '1.0.0',
                'supportedInterfaces': [{'url': self.endpoint, 'protocolBinding': 'JSONRPC', 'protocolVersion': VERSION}],
                'capabilities': {'streaming': False, 'pushNotifications': False,
                                 'extendedAgentCard': callable(getattr(self.service, 'discover', None)),
                                 'extensions': [dict(uri=uri, required=uri in self.required_extensions,
                                     **({'params': {'format': 'mas-bounded-json/1', 'inputParts': 1,
                                         'mediaType': 'application/json', 'numbers': 'exact-integers',
                                         'maxRequestBytes': 65536, 'unknownOutcome': 'reconcile-original-identity'}}
                                        if uri == BOUNDED_PROFILE else {}))
                                     for uri in sorted(self._supported_extensions())]},
                'securitySchemes': {'bearer': {'httpAuthSecurityScheme': {'scheme': 'Bearer'}}},
                'securityRequirements': [{'schemes': {'bearer': {'list': []}}}],
                'defaultInputModes': ['application/json'], 'defaultOutputModes': ['application/json'],
                'skills': [{'id': 'bounded-workflow', 'name': 'Bounded MAS workflow',
                            'description': 'Submit an authorised workflow with a stable message identifier, inspect it, cancel it, or supply additional input.',
                            'tags': ['workflow', 'durable', 'integer-json'],
                            'inputModes': ['application/json'], 'outputModes': ['application/json']}]}

    def extended_agent_card(self, principal):
        discover = getattr(self.service, 'discover', None)
        if not callable(discover):
            raise A2AError(-32004, 'operation_not_supported')
        card = self.agent_card()
        discovered = discover(principal)
        card['skills'].extend({'id': 'registration:' + item['name'], 'name': item['name'],
            'description': 'Authorised registered ' + item.get('kind', 'workflow') +
                           '; submit its name in the workflow field.',
            'tags': ['nehemiah', 'registered-workflow', 'integer-json'],
            'inputModes': ['application/json'], 'outputModes': ['application/json']}
            for item in discovered.get('registrations', []) if item.get('available') is True)
        card['capabilities']['extensions'] = [item for item in card['capabilities']['extensions']
                                              if item['uri'] == BOUNDED_PROFILE]
        card['capabilities']['extensions'].append({'uri': SERVICE_PROFILE,
            'required': SERVICE_PROFILE in self.required_extensions,
            'description': 'MAS pinned durable task-service lifecycle, version 1.',
            'params': {'service': discovered}})
        if LIFECYCLE_PROFILE in self._supported_extensions():
            card['capabilities']['extensions'].append({'uri': LIFECYCLE_PROFILE,
                'required': LIFECYCLE_PROFILE in self.required_extensions,
                'description': 'MAS bounded graph, leaf and event inspection and evidence-resolved recovery.',
                'params': {'methods': sorted(name for name, (target, _, _) in LIFECYCLE_METHODS.items()
                                             if callable(getattr(self.service, target, None)))}})
        return card

    def _lifecycle(self, principal, method, params):
        target, required, optional = LIFECYCLE_METHODS[method]
        _fields(params, required | optional)
        if not required <= set(params):
            raise A2AError(-32602, 'invalid_parameters')
        if 'task_id' in params:
            token(params['task_id'])
        callback = getattr(self.service, target, None)
        if not callable(callback):
            raise A2AError(-32004, 'operation_not_supported')
        return callback(principal, **params)

    def _task(self, receipt: dict, *, include_artifacts=True):
        state = receipt['state']
        if state not in STATE_TO_WIRE:
            raise ServiceError('invalid_task_state')
        context_id = receipt.get('metadata', {}).get('context_id', 'context-' + receipt['task_id'])
        result = {'id': receipt['task_id'], 'contextId': context_id,
                  'status': {'state': STATE_TO_WIRE[state], 'timestamp': _time(receipt['updated_at'])},
                  'metadata': {'mas': {'workflow': receipt['workflow'], 'cancelRequested': receipt['cancel_requested']}}}
        result['metadata']['mas']['state'] = state
        if 'workflow_identity' in receipt:
            result['metadata']['mas']['workflowIdentity'] = receipt['workflow_identity']
        if include_artifacts:
            result['artifacts'] = ([] if receipt.get('result') is None else
                                   [{'artifactId': receipt['task_id'] + '-result', 'name': 'result',
                                     'parts': [{'data': receipt['result'], 'mediaType': 'application/json'}]}])
        if receipt.get('error'):
            result['status']['message'] = {'messageId': receipt['task_id'] + '-status',
                                           'contextId': context_id, 'taskId': receipt['task_id'],
                                           'role': 'ROLE_AGENT', 'parts': [{'text': str(receipt['error'])}]}
        return result

    def _send(self, principal, params):
        _fields(params, ('message', 'configuration', 'metadata'))
        message = params.get('message')
        _fields(message, ('messageId', 'role', 'parts', 'contextId', 'taskId', 'metadata'))
        task_id = send_task_id(message)
        message_id = peer_identifier(message.get('messageId'))
        request_key = _message_key(message_id)
        if message.get('role') != 'ROLE_USER':
            raise A2AError(-32602, 'invalid_message_role')
        parts = _parts(message.get('parts'), application=True)
        if len(parts) != 1 or type(parts[0]['data']) is not dict:
            raise A2AError(-32005, 'one_json_object_part_required')
        data = parts[0]['data']
        _fields(data, ('workflow', 'input', 'request_epoch', 'expected_identity', 'expected_deployment_id'))
        pins = {key: data[key] for key in ('expected_identity', 'expected_deployment_id') if key in data}
        if type(data.get('input')) is not dict:
            raise A2AError(-32602, 'workflow_input_required')
        workflow = data.get('workflow', self.workflow)
        if type(workflow) is not str or not workflow:
            raise A2AError(-32602, 'workflow_required')
        config = params.get('configuration', {})
        _fields(config, ('acceptedOutputModes', 'taskPushNotificationConfig', 'historyLength', 'returnImmediately'))
        _history(config.get('historyLength'))
        if 'taskPushNotificationConfig' in config:
            raise A2AError(-32003, 'push_not_supported')
        if 'returnImmediately' in config and type(config['returnImmediately']) is not bool:
            raise A2AError(-32602, 'invalid_execution_mode')
        modes = config.get('acceptedOutputModes', ['application/json'])
        if type(modes) is not list or any(type(mode) is not str for mode in modes):
            raise A2AError(-32602, 'invalid_output_modes')
        if modes and 'application/json' not in modes:
            raise A2AError(-32005, 'content_type_not_supported')
        if 'metadata' in params:
            bounded(params['metadata'], 4096)
        if 'metadata' in message:
            bounded(message['metadata'], 4096)
        metadata = {'message_digest': digest(message), 'request_metadata': params.get('metadata', {})}
        if task_id is not None:
            if 'request_epoch' in data:
                raise A2AError(-32602, 'continuation_inherits_request_epoch')
            existing = self.service.inspect(principal, task_id)
            context_id = existing.get('metadata', {}).get('context_id', 'context-' + task_id)
            if message.get('contextId', context_id) != context_id or workflow != existing['workflow']:
                raise A2AError(-32602, 'task_context_mismatch')
            receipt = self.service.update(principal, task_id, request_key, data['input'], metadata=metadata, **pins)
        else:
            context_id = message.get('contextId')
            if context_id is None:
                context_id = 'context-' + hashlib.sha256((principal + '\x00' + message_id).encode()).hexdigest()[:40]
            peer_identifier(context_id)
            metadata['context_id'] = context_id
            receipt = self.service.submit(principal, request_key, workflow, data['input'], metadata=metadata,
                                          **pins, **({'request_epoch': data['request_epoch']} if 'request_epoch' in data else {}))
        if not config.get('returnImmediately', False):
            deadline = time.monotonic() + self.wait_seconds
            while receipt['state'] in WAITING:
                if time.monotonic() >= deadline:
                    # The request identity remains durable. Repeating this exact
                    # message obtains the same task, including after a timeout.
                    raise A2AError(-32603, 'response_deadline_reached_repeat_same_message')
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
                receipt = self.service.inspect(principal, receipt['task_id'])
        return {'task': self._task(receipt)}

    def _list(self, principal, params):
        _fields(params, ('contextId', 'status', 'pageSize', 'pageToken', 'historyLength', 'statusTimestampAfter', 'includeArtifacts'))
        _history(params.get('historyLength'))
        size = params.get('pageSize', 50)
        if type(size) is not int or not 1 <= size <= 100:
            raise A2AError(-32602, 'invalid_page_size')
        include = params.get('includeArtifacts', False)
        if type(include) is not bool:
            raise A2AError(-32602, 'invalid_artifact_option')
        state = params.get('status')
        if state is not None and (type(state) is not str or state not in WIRE_TO_STATE):
            raise A2AError(-32602, 'invalid_state')
        context = params.get('contextId')
        if context is not None:
            peer_identifier(context)
        after_time = params.get('statusTimestampAfter')
        if after_time is not None:
            try:
                if type(after_time) is not str or not after_time.endswith('Z'):
                    raise ValueError()
                after_time = int(datetime.fromisoformat(after_time.replace('Z', '+00:00')).timestamp() * 1000)
            except ValueError:
                raise A2AError(-32602, 'invalid_timestamp') from None
        filters = {'principal': principal, 'context': context, 'state': state, 'after_time': after_time}
        cursor = None
        if params.get('pageToken'):
            try:
                encoded = params['pageToken']
                if type(encoded) is not str or len(encoded) > 2048:
                    raise ValueError()
                decoded = json.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
                if decoded['filter'] != digest(filters) or type(decoded['after']) is not list or len(decoded['after']) != 2:
                    raise ValueError()
                cursor = tuple(decoded['after'])
                if type(cursor[0]) is not int:
                    raise ValueError()
                token(cursor[1])
            except Exception:
                raise A2AError(-32602, 'invalid_page_token') from None
        page = self.service.list_task_page(principal, page_size=size, after=cursor,
                 state=WIRE_TO_STATE.get(state), metadata={'context_id': context} if context else None,
                 updated_after=after_time)
        next_token = ''
        if page['next'] is not None:
            next_token = base64.urlsafe_b64encode(json.dumps({'filter': digest(filters), 'after': page['next']}, separators=(',', ':')).encode()).decode().rstrip('=')
        return {'tasks': [self._task(item, include_artifacts=include) for item in page['tasks']],
                'nextPageToken': next_token, 'pageSize': size, 'totalSize': page['total_size']}

    def handle(self, principal: str, message: dict, metadata=None) -> dict:
        request_id = message.get('id') if type(message) is dict else None
        try:
            method, params = validate_request(message, metadata)
            self._extensions(metadata)
            if method in LIFECYCLE_METHODS:
                result = self._lifecycle(principal, method, params)
            elif method == 'SendMessage':
                result = self._send(principal, params)
            elif method == 'GetTask':
                _fields(params, ('id', 'historyLength'))
                _history(params.get('historyLength'))
                result = self._task(self.service.inspect(principal, token(params.get('id'))))
            elif method == 'CancelTask':
                _fields(params, ('id', 'metadata'))
                if 'metadata' in params:
                    bounded(params['metadata'], 4096)
                task_id = token(params.get('id'))
                receipt = self.service.cancel(principal, task_id)
                if receipt['state'] != 'cancelled':
                    # A cancellation request is not a confirmation of stopping.
                    raise A2AError(-32002, 'task_not_cancelable_or_cancellation_pending')
                result = self._task(receipt)
            elif method == 'ListTasks':
                result = self._list(principal, params)
            elif method == 'GetExtendedAgentCard':
                _fields(params, ())
                result = self.extended_agent_card(principal)
            elif method in ('SendStreamingMessage', 'SubscribeToTask'):
                raise A2AError(-32004, 'operation_not_supported')
            elif method in ('CreateTaskPushNotificationConfig', 'GetTaskPushNotificationConfig', 'ListTaskPushNotificationConfigs', 'DeleteTaskPushNotificationConfig'):
                raise A2AError(-32003, 'push_not_supported')
            else:
                raise A2AError(-32601, 'method_not_found')
            return {'jsonrpc': '2.0', 'id': request_id, 'result': result}
        except ServiceError as error:
            return error_response(request_id, error)


def _remote_task(task: dict) -> RemoteSnapshot:
    if type(task) is not dict or type(task.get('status')) is not dict:
        raise ServiceError('invalid_remote_task')
    wire_state = task['status'].get('state')
    state = WIRE_TO_STATE.get(wire_state) if type(wire_state) is str else None
    if state is None:
        raise ServiceError('invalid_remote_task')
    if (state == 'queued' and type(task.get('metadata')) is dict
            and type(task['metadata'].get('mas')) is dict
            and task['metadata']['mas'].get('state') == 'blocked'):
        state = 'blocked'
    peer_identifier(task.get('id'))
    context_id = task.get('contextId')
    if context_id is not None:
        peer_identifier(context_id)
    if 'artifacts' in task:
        if type(task['artifacts']) is not list or len(task['artifacts']) > 64:
            raise ServiceError('invalid_remote_task')
        for artifact in task['artifacts']:
            if type(artifact) is not dict:
                raise ServiceError('invalid_remote_task')
            peer_identifier(artifact.get('artifactId'))
            _parts(artifact.get('parts'))
    return RemoteSnapshot(task['id'], state, bounded({'task': task}), context_id).validate()


class A2AClient:
    def __init__(self, transport: BoundedJSONHTTP, *, client_identity: str, tenant: str | None = None):
        self.transport, self.client_identity = transport, token(client_identity)
        self.tenant = peer_identifier(tenant) if tenant is not None else None

    def descriptor(self):
        return {'bridge': 'a2a', 'protocol_version': VERSION, 'binding': 'JSONRPC',
                'client_identity': self.client_identity, 'tenant': self.tenant,
                'contract': 'bounded-json-polling/2', **self.transport.descriptor()}

    def _rpc(self, method, params):
        identifier = str(uuid.uuid4())
        params = bounded(params)
        if self.tenant:
            params['tenant'] = self.tenant
        response = self.transport.request('POST', {'jsonrpc': '2.0', 'id': identifier, 'method': method, 'params': params},
                                          headers={'A2A-Version': VERSION})
        if (response.get('jsonrpc') != '2.0' or response.get('id') != identifier
                or ('result' in response) == ('error' in response)):
            raise ServiceError('invalid_remote_response')
        if 'error' in response:
            raise ServiceError('remote_protocol_error')
        return bounded(response['result'])

    def submit(self, payload: dict) -> RemoteSnapshot:
        payload = bounded(payload)
        message = payload.get('message')
        if type(message) is not dict or message.get('role') != 'ROLE_USER' or message.get('taskId') is not None:
            raise ServiceError('invalid_remote_message')
        peer_identifier(message.get('messageId'))
        if message.get('contextId') is not None:
            peer_identifier(message['contextId'])
        _parts(message.get('parts'))
        payload['configuration'] = dict(payload.get('configuration', {})) | {'returnImmediately': True}
        response = self._rpc('SendMessage', payload)
        if set(response) == {'task'}:
            snapshot = _remote_task(response['task'])
            if message.get('contextId') is not None and snapshot.context_id != message['contextId']:
                raise ServiceError('remote_identity_conflict')
            return snapshot
        if set(response) == {'message'}:
            answer = response['message']
            if type(answer) is not dict or answer.get('role') != 'ROLE_AGENT':
                raise ServiceError('invalid_remote_message')
            peer_identifier(answer.get('messageId'))
            peer_identifier(answer.get('contextId'))
            if message.get('contextId') is not None and answer['contextId'] != message['contextId']:
                raise ServiceError('remote_identity_conflict')
            _parts(answer.get('parts'))
            return RemoteSnapshot(None, 'completed', response, answer['contextId'])
        raise ServiceError('invalid_remote_response')

    def poll(self, remote_id):
        return _remote_task(self._rpc('GetTask', {'id': peer_identifier(remote_id), 'historyLength': 0}))

    def cancel(self, remote_id):
        return _remote_task(self._rpc('CancelTask', {'id': peer_identifier(remote_id)}))

    def update(self, remote_id, payload):
        payload = bounded(payload)
        message = payload.get('message')
        if type(message) is not dict or message.get('role') != 'ROLE_USER':
            raise ServiceError('invalid_remote_message')
        peer_identifier(message.get('messageId'))
        if message.get('contextId') is not None:
            peer_identifier(message['contextId'])
        _parts(message.get('parts'))
        if message.get('taskId', remote_id) != remote_id:
            raise ServiceError('remote_identity_conflict')
        message['taskId'] = peer_identifier(remote_id)
        payload['configuration'] = dict(payload.get('configuration', {})) | {'returnImmediately': True}
        response = self._rpc('SendMessage', payload)
        if set(response) != {'task'}:
            raise ServiceError('invalid_remote_response')
        snapshot = _remote_task(response['task'])
        if snapshot.remote_id != remote_id or (message.get('contextId') is not None and snapshot.context_id != message['contextId']):
            raise ServiceError('remote_identity_conflict')
        return snapshot


class A2AAdapter:
    """One configured remote peer, usable in any permitted logical topology."""
    def __init__(self, service: RemoteTaskService, *, host_run_dir=None, reservation_microusd=0):
        self.service, self.host_run_dir = service, host_run_dir
        self.reservation_microusd = reservation_microusd

    def _context_id(self, alias):
        return 'mas-a2a-v1-context-' + digest({'namespace': self.service.namespace,
                                             'context': peer_identifier(alias)})

    def _message(self, key, message, *, action, request_key=None):
        message = bounded(message)
        if 'referenceTaskIds' in message:
            raise ServiceError('unsupported_capability')
        message['messageId'] = 'mas-a2a-v1-message-' + digest({
            'namespace': self.service.namespace, 'key': key, 'action': action,
            'message_id': peer_identifier(message.get('messageId')), 'request_key': request_key})
        supplied_context = message.get('contextId')
        if action == 'submit':
            if supplied_context is not None:
                message['contextId'] = self._context_id(supplied_context)
        else:
            # Supplied continuation contexts must resolve to the owned receipt.
            # Preserve omission so learning a context cannot change retry input.
            saved_context = self.service.status(key)['context_id']
            if supplied_context is not None and (peer_identifier(supplied_context) != saved_context
                    and self._context_id(supplied_context) != saved_context):
                raise ServiceError('remote_identity_conflict')
            if supplied_context is not None:
                message['contextId'] = saved_context
        return message

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        if self.host_run_dir is None:
            raise ServiceError('invalid_host_directory')
        args = {'context': context, 'host_run_dir': self.host_run_dir,
                'reservation_microusd': self.reservation_microusd}
        body = bounded(request.input)
        key = token(body.get('key'))
        if request.operation == 'a2a.submit' and set(body) == {'key', 'message'}:
            result = self.service.submit(key, {'message': self._message(key, body['message'], action='submit')}, **args)
        elif request.operation == 'a2a.get' and set(body) == {'key'}:
            result = self.service.poll(key, **args)
        elif request.operation == 'a2a.cancel' and set(body) == {'key', 'request_key'}:
            result = self.service.cancel(key, request_key=body['request_key'], **args)
        elif request.operation == 'a2a.update' and set(body) == {'key', 'message', 'request_key'}:
            request_key = token(body['request_key'])
            message = self._message(key, body['message'], action='update', request_key=request_key)
            result = self.service.update(key, request_key=request_key, payload={'message': message}, **args)
        else:
            raise ServiceError('unsupported_operation')
        return Outcome(result)
