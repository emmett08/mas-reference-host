"""MCP 2026-07-28 boundary, with the negotiated 2026-07-28 Tasks extension.

This module is deliberately transport independent. Authentication produces the
principal before dispatch; an MCP request never chooses its execution identity.
Only bounded integer JSON enters MAS. There is no legacy initialise/session path.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from datetime import datetime, timezone

from ..codec import integer_json
from ..contracts import RunnerError
from ..service_contracts import ServiceError, TaskService

VERSION = "2026-07-28"
PREFIX = "io.modelcontextprotocol/"
TASKS = PREFIX + "tasks"
SERVICE_PROFILE = "org.mas/task-service/1"
MAX_MESSAGE = 65_536


class MCPError(RuntimeError):
    def __init__(self, code: int, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.data = code, data


def bounded(value, limit=MAX_MESSAGE):
    try:
        return json.loads(integer_json(value, limit))
    except (RunnerError, ValueError, TypeError, UnicodeError):
        raise MCPError(-32602, "MCP MAS profile requires bounded integer JSON") from None


def decode_message(source: bytes | str) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result
    try:
        if len(source if isinstance(source, bytes) else source.encode()) > MAX_MESSAGE:
            raise ValueError("size")
        if isinstance(source, bytes):
            source = source.decode("utf-8")
        value = json.loads(source, object_pairs_hook=pairs,
                           parse_float=lambda _: (_ for _ in ()).throw(ValueError("float")),
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError("constant")))
        if type(value) is not dict:
            raise ValueError("object")
        return bounded(value)
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise MCPError(-32700, "Invalid bounded JSON message") from None


def encode_header(value) -> str:
    if type(value) not in (str, int, bool):
        raise MCPError(-32602, "Invalid MCP routing value")
    value = ("true" if value else "false") if type(value) is bool else str(value)
    if (value != value.strip() or any(not (32 <= ord(c) <= 126) for c in value)
            or (value.startswith("=?base64?") and value.endswith("?="))):
        return "=?base64?" + base64.b64encode(value.encode()).decode() + "?="
    return value


def decode_header(value: str) -> str:
    if type(value) is not str or value != value.strip() or any(
            not (32 <= ord(c) <= 126 or c == "\t") for c in value):
        raise MCPError(-32020, "Invalid MCP routing header")
    if value.startswith("=?base64?") and value.endswith("?="):
        try:
            return base64.b64decode(value[9:-2], validate=True).decode("utf-8")
        except (ValueError, UnicodeError):
            raise MCPError(-32020, "Invalid MCP routing header") from None
    return value


def metadata(*, tasks: bool = True, elicitation: bool = False) -> dict:
    capabilities = {"extensions": {TASKS: {}}} if tasks else {}
    if elicitation:
        capabilities["elicitation"] = {"form": {}, "url": {}}
    return {PREFIX + "protocolVersion": VERSION,
            PREFIX + "clientInfo": {"name": "mas", "version": "1"},
            PREFIX + "clientCapabilities": capabilities}


def routing_headers(message: dict) -> dict:
    params = message.get("params", {})
    result = {"MCP-Protocol-Version": params.get("_meta", {}).get(PREFIX + "protocolVersion", VERSION),
              "Mcp-Method": message["method"]}
    key = ("taskId" if message["method"] in ("tasks/get", "tasks/update", "tasks/cancel")
           else "uri" if message["method"] == "resources/read" else "name")
    if key in params:
        result["Mcp-Name"] = encode_header(params[key])
    return result


def validate_request(message: dict, headers: dict | None = None) -> tuple[str, dict, dict]:
    bounded(message)
    if (type(message) is not dict or message.get("jsonrpc") != "2.0"
            or type(message.get("method")) is not str
            or "result" in message or "error" in message
            or ("id" in message and type(message["id"]) not in (str, int))):
        raise MCPError(-32600, "Invalid JSON-RPC request")
    params = message.get("params", {})
    if type(params) is not dict:
        raise MCPError(-32602, "Request parameters must be an object")
    if "id" not in message:
        return message["method"], params, {}
    meta = params.get("_meta")
    if (type(meta) is not dict or type(meta.get(PREFIX + "protocolVersion")) is not str
            or type(meta.get(PREFIX + "clientCapabilities")) is not dict):
        raise MCPError(-32602, "Missing required per-request MCP metadata")
    if headers is not None:
        lowered = {str(k).lower(): v for k, v in headers.items()}
        for name, expected in routing_headers(message).items():
            actual = lowered.get(name.lower())
            matches = (decode_header(actual) == decode_header(expected) if name == "Mcp-Name" and actual is not None
                       else actual == expected)
            if not matches:
                raise MCPError(-32020, "MCP routing headers do not match the request")
    version = meta[PREFIX + "protocolVersion"]
    if version != VERSION:
        raise MCPError(-32022, "Unsupported protocol version",
                       {"supported": [VERSION], "requested": version})
    caps = meta[PREFIX + "clientCapabilities"]
    if "extensions" in caps and type(caps["extensions"]) is not dict:
        raise MCPError(-32602, "Invalid extension capabilities")
    return message["method"], params, caps


def error_response(request_id, error: MCPError) -> dict:
    response = {"jsonrpc": "2.0", "error": {"code": error.code, "message": str(error)}}
    if type(request_id) in (str, int):
        response["id"] = request_id
    if error.data is not None:
        response["error"]["data"] = error.data
    return response


def http_status(response: dict | None) -> int:
    if response is None:
        return 202
    code = response.get("error", {}).get("code")
    return 404 if code == -32601 else 400 if code in (
        -32700, -32600, -32602, -32020, -32021, -32022) else 200


def tool_result(value: dict, *, failed=False) -> dict:
    value = bounded(value)
    text = integer_json(value)
    if len(text.encode()) > 4096:
        text = "The exact result is available in structuredContent."
    return {"resultType": "complete", "content": [{"type": "text", "text": text}],
            "structuredContent": value, "isError": bool(failed)}


def _event_page_for_wire(page, after):
    """Reserve envelope space while keeping the cursor at the last sent event."""
    selected, size = [], 256
    for item in page["events"]:
        try:
            item_size = len(integer_json(item).encode("utf-8")) + 1
        except RunnerError:
            item_size = MAX_MESSAGE
        if size + item_size > MAX_MESSAGE - 4096:
            if not selected:
                raise ServiceError("page_item_too_large")
            break
        selected.append(item)
        size += item_size
    return {"events": selected, "next_after": selected[-1]["id"] if selected else after,
            "has_more": page["has_more"] or len(selected) < len(page["events"])}


_NO_INSTANCE = object()


def validate_schema(schema: dict, arguments=_NO_INSTANCE):
    """Use the 2020-12 implementation with a closed reference registry.

    Configuration is bounded before validator construction. No URI is fetched;
    unresolved remote refs are rejected even if a branch happens not to use it.
    """
    dialect = schema.get("$schema", "https://json-schema.org/draft/2020-12/schema")
    if dialect != "https://json-schema.org/draft/2020-12/schema":
        raise MCPError(-32602, "Unsupported tool schema dialect")
    nodes = 0
    def check(item, depth=0):
        nonlocal nodes
        nodes += 1
        if nodes > 2048 or depth > 24:
            raise MCPError(-32602, "Tool schema exceeds validation bounds")
        if type(item) is dict:
            for key, value in item.items():
                if key in ("$ref", "$dynamicRef") and (type(value) is not str or not value.startswith("#")):
                    raise MCPError(-32602, "External schema references are disabled")
                check(value, depth + 1)
        elif type(item) is list:
            for value in item:
                check(value, depth + 1)
    check(schema)
    try:
        from jsonschema import Draft202012Validator
        from referencing import Registry
    except ImportError:
        raise MCPError(-32602, "MCP tool validation requires the bridges extra") from None
    try:
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, registry=Registry())
        if arguments is not _NO_INSTANCE:
            validator.validate(arguments)
    except Exception:
        raise MCPError(-32602, "Invalid tool schema or arguments") from None


def _text(value) -> str:
    if type(value) is not str or not value or len(value.encode()) > 512:
        raise MCPError(-32602, "Expected a bounded nonempty identifier")
    return value


def _integer(value, maximum=1_000_000) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise MCPError(-32602, "Expected a bounded nonnegative integer")
    return value


def _time(value) -> str:
    if type(value) is str:
        return value
    # The task-service repository stores integer Unix milliseconds.
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat().replace("+00:00", "Z")


def task_result(receipt: dict, *, creation=False) -> dict:
    state = receipt["state"]
    # Application failure is a completed tool call with isError, never a
    # protocol-level failed task. Unknown effects stay non-terminal.
    status = {"queued": "working", "running": "working", "unknown": "working", "blocked": "working",
              "auth_required": "working", "input_required": "input_required",
              "completed": "completed", "failed": "completed", "cancelled": "cancelled"}[state]
    result = {"resultType": "task" if creation else "complete", "taskId": receipt["task_id"],
              "status": status, "createdAt": _time(receipt["created_at"]),
              "lastUpdatedAt": _time(receipt["updated_at"]), "ttlMs": None, "pollIntervalMs": 1000,
              "_meta": {"org.mas/state": state, "org.mas/cancelRequested": bool(receipt["cancel_requested"])}}
    if "workflow_identity" in receipt:
        result["_meta"].update({"org.mas/workflow": receipt["workflow"],
                               "org.mas/workflowIdentity": receipt["workflow_identity"]})
    if status == "completed":
        result["result"] = tool_result({"task_id": receipt["task_id"], "state": state,
                                       "result": receipt.get("result"), "error": receipt.get("error")},
                                      failed=state == "failed")
    elif status == "input_required":
        requests = _input_requests(receipt)
        if requests:
            result["inputRequests"] = requests
        else:
            # A generic service interruption is not automatically an MCP
            # elicitation. Its continuation contract remains mas.update.
            result["status"] = "working"
            result["statusMessage"] = "Additional input is required through mas.update"
    elif state == "blocked":
        result["statusMessage"] = "The pinned workflow implementation is unavailable; deployment action is required"
    elif state in ("unknown", "auth_required"):
        result["statusMessage"] = ("External outcome is unknown; reconciliation is required" if state == "unknown"
                                   else "Additional authorisation is required")
    return bounded(result)


def _input_requests(receipt: dict) -> dict:
    requests = (receipt.get("result") or {}).get("input_requests", {})
    return {"r" + str(receipt.get("revision", 0)) + ":" + key: value for key, value in requests.items()}


def require_input_capabilities(result: dict, capabilities: dict) -> dict:
    requests = result.get("inputRequests", {})
    for request in requests.values():
        if type(request) is not dict or request.get("method") != "elicitation/create":
            raise MCPError(1002, "This bridge supports explicit elicitation continuations only")
        mode = request.get("params", {}).get("mode", "form")
        if mode not in ("form", "url"):
            raise MCPError(1002, "Unsupported elicitation mode")
        support = capabilities.get("elicitation")
        if (type(support) is not dict or (mode not in support and not (mode == "form" and not support))):
            raise MCPError(-32021, "Missing required client capability",
                           {"requiredCapabilities": {"elicitation": {mode: {}}}})
    return result


def _schema(required, properties):
    return {"type": "object", "properties": properties, "required": required, "additionalProperties": False}


_STRING = {"type": "string", "minLength": 1, "maxLength": 512}
_INPUT = {"type": "object", "description": "Bounded integer-only MAS JSON; decimal values are rejected."}
TOOLS = [
    {"name": "mas.submit", "description": "Durably submit a workflow using a stable idempotency key.",
     "inputSchema": _schema(["request_key", "workflow", "input"],
                            {"request_key": _STRING, "workflow": _STRING, "input": _INPUT, "request_epoch": _STRING,
                             "expected_identity": _STRING, "expected_deployment_id": _STRING})},
    *({"name": "mas." + name, "description": description,
       "inputSchema": _schema(["task_id"], {"task_id": _STRING})}
      for name, description in (("status", "Inspect an owned task."), ("result", "Retrieve an owned task result."),
                                ("cancel", "Record cancellation intent; acknowledgement does not prove work stopped."))),
    {"name": "mas.update", "description": "Continue an interrupted task with a stable update key.",
     "inputSchema": _schema(["task_id", "request_key", "input"],
                            {"task_id": _STRING, "request_key": _STRING, "input": _INPUT,
                             "expected_identity": _STRING, "expected_deployment_id": _STRING})},
    {"name": "mas.events", "description": "Read durable task events after a sequence number.",
     "inputSchema": _schema(["task_id"], {"task_id": _STRING, "after": {"type": "integer", "minimum": 0}})},
    {"name": "mas.events_page", "description": "Read a bounded event page with an explicit resume cursor.",
     "inputSchema": _schema(["task_id"], {"task_id": _STRING,
         "after": {"type": "integer", "minimum": 0},
         "limit": {"type": "integer", "minimum": 1, "maximum": 1000}})},
    {"name": "mas.task", "description": "Inspect a task within a composed workflow by its logical path.",
     "inputSchema": _schema(["task_id", "path"], {"task_id": _STRING, "path": _STRING,
         "revision": {"type": "integer", "minimum": 0}})},
    {"name": "mas.graph", "description": "Read a bounded page of graph nodes, attempts or scopes.",
     "inputSchema": _schema(["task_id"], {"task_id": _STRING,
         "section": {"type": "string", "enum": ["nodes", "attempts", "scopes"]},
         "offset": {"type": "integer", "minimum": 0},
         "limit": {"type": "integer", "minimum": 1, "maximum": 100}})},
    {"name": "mas.discover", "description": "Discover workflows and capabilities available to the authenticated principal.",
     "inputSchema": _schema([], {})},
    {"name": "mas.recover", "description": "Resume the same task after an operator has reconciled its uncertain effects.",
     "inputSchema": _schema(["task_id"], {"task_id": _STRING})},
    {"name": "mas.list", "description": "List tasks owned by the authenticated caller.",
     "inputSchema": _schema([], {"limit": {"type": "integer", "minimum": 1, "maximum": 100},
                                 "offset": {"type": "integer", "minimum": 0}})},
    {"name": "mas.page", "description": "Search owned tasks with metadata and state filters and a stable resume cursor.",
     "inputSchema": _schema([], {"page_size": {"type": "integer", "minimum": 1, "maximum": 1000},
         "after": {"type": ["array", "null"]}, "state": {"type": ["string", "null"]},
         "metadata": {"type": ["object", "null"]}, "updated_after": {"type": ["integer", "null"], "minimum": 0}})},
]
TOOLS.sort(key=lambda tool: tool["name"])


class MCPBridge:
    def __init__(self, service: TaskService, *, artifact_transfer=None):
        self.service = service
        self.artifact_transfer = artifact_transfer
        self.tools = list(TOOLS)
        if artifact_transfer is not None:
            from .artifact_client import artifact_tools
            self.tools = sorted(self.tools + artifact_tools(), key=lambda item: item["name"])

    def discover(self) -> dict:
        return {"resultType": "complete", "supportedVersions": [VERSION], "cacheScope": "private", "ttlMs": 0,
                "capabilities": {"tools": {"listChanged": False}, "extensions": {TASKS: {}, SERVICE_PROFILE: {}}},
                "instructions": "Integer JSON only. Preserve request_key across retries. Poll tasks; cancellation records intent."}

    def handle(self, principal: str, message: dict, metadata: dict | None = None) -> dict | None:
        request_id = message.get("id") if type(message) is dict else None
        notification = type(message) is dict and "id" not in message and type(message.get("method")) is str
        try:
            method, params, capabilities = validate_request(message, metadata)
            if notification:
                # All methods are quick durable operations. Request cancellation
                # cannot be reinterpreted as cancellation of persisted work.
                return None
            result = self._dispatch(principal, method, params, capabilities)
            return bounded({"jsonrpc": "2.0", "id": request_id, "result": result})
        except MCPError as error:
            return None if notification else error_response(request_id, error)
        except Exception as error:
            code = getattr(error, "code", None)
            if code in ("not_found", "task_not_found", "forbidden", "invalid_input", "invalid_request", "invalid_workflow"):
                public = MCPError(-32602, "Task or parameters are unavailable")
            elif type(code) is str:
                public = MCPError(1001, "MAS task operation rejected", {"reason": code})
            else:
                public = MCPError(-32603, "Internal task service error")
            return None if notification else error_response(request_id, public)

    @staticmethod
    def _tasks(capabilities):
        if type(capabilities.get("extensions", {}).get(TASKS)) is not dict:
            raise MCPError(-32021, "Missing required client capability",
                           {"requiredCapabilities": {"extensions": {TASKS: {}}}})

    def _dispatch(self, principal, method, params, capabilities):
        if method == "server/discover":
            return self.discover()
        if method == "ping":
            return {"resultType": "complete"}
        if method == "tools/list":
            if params.get("cursor") is not None:
                raise MCPError(-32602, "Unknown tool cursor")
            return {"resultType": "complete", "tools": bounded(self.tools), "cacheScope": "private", "ttlMs": 0}
        if method in ("tasks/get", "tasks/update", "tasks/cancel"):
            self._tasks(capabilities)
            task_id = _text(params.get("taskId"))
            if method == "tasks/get":
                return require_input_capabilities(task_result(self.service.inspect(principal, task_id)), capabilities)
            if method == "tasks/cancel":
                self.service.cancel(principal, task_id)
            else:
                responses = params.get("inputResponses")
                if type(responses) is not dict:
                    raise MCPError(-32602, "inputResponses must be an object")
                receipt = self.service.inspect(principal, task_id)
                outstanding = _input_requests(receipt) if receipt["state"] == "input_required" else {}
                require_input_capabilities({"inputRequests": outstanding}, capabilities)
                prefix = "r" + str(receipt.get("revision", 0)) + ":"
                responses = {k[len(prefix):]: v for k, v in responses.items() if k in outstanding}
                if responses:
                    if len(responses) != len(outstanding):
                        raise MCPError(-32602, "This task requires all outstanding input responses together")
                    for key, response in responses.items():
                        source = outstanding[prefix + key]["params"]
                        if (type(response) is not dict or response.get("action") not in ("accept", "decline", "cancel")):
                            raise MCPError(-32602, "Invalid elicitation response")
                        if response["action"] == "accept" and source.get("mode", "form") == "form":
                            if type(response.get("content")) is not dict:
                                raise MCPError(-32602, "Elicitation acceptance requires content")
                            validate_schema(source.get("requestedSchema", {}), response["content"])
                    identity = {"task_id": task_id, "revision": receipt.get("revision", 0), "responses": responses}
                    key = "mcp:" + hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                    self.service.update(principal, task_id, key, {"input_responses": responses})
            return {"resultType": "complete"}
        if method != "tools/call":
            raise MCPError(-32601, "Method not found")
        name, arguments = params.get("name"), params.get("arguments", {})
        definition = next((tool for tool in self.tools if tool["name"] == name), None)
        if definition is None or type(arguments) is not dict:
            raise MCPError(-32602, "Unknown tool or invalid arguments")
        schema = definition["inputSchema"]
        if not set(schema["required"]) <= arguments.keys() or not arguments.keys() <= schema["properties"].keys():
            raise MCPError(-32602, "Invalid tool arguments")
        for key in ("task_id", "request_key", "workflow"):
            if key in arguments:
                _text(arguments[key])
        if "input" in arguments and type(arguments["input"]) is not dict:
            raise MCPError(-32602, "Input must be an object")
        if name.startswith("mas.artifact."):
            if self.artifact_transfer is None:
                raise ServiceError("unsupported_capability")
            return tool_result(self.artifact_transfer.dispatch(principal,
                name.removeprefix("mas.artifact."), arguments))
        if name == "mas.submit":
            self._tasks(capabilities)  # Reject before creating any durable work.
            return require_input_capabilities(task_result(self.service.submit(principal, arguments["request_key"],
                                                   arguments["workflow"], arguments["input"],
                                                   **{key: arguments[key] for key in ("request_epoch", "expected_identity", "expected_deployment_id")
                                                      if key in arguments}), creation=True), capabilities)
        if name == "mas.status":
            value = self.service.inspect(principal, arguments["task_id"])
        elif name == "mas.result":
            value = self.service.result(principal, arguments["task_id"])
        elif name == "mas.cancel":
            value = self.service.cancel(principal, arguments["task_id"])
        elif name == "mas.update":
            value = self.service.update(principal, arguments["task_id"], arguments["request_key"], arguments["input"],
                **{key: arguments[key] for key in ("expected_identity", "expected_deployment_id") if key in arguments})
        elif name == "mas.events":
            value = {"events": self.service.events(principal, arguments["task_id"], _integer(arguments.get("after", 0)))}
        elif name == "mas.events_page":
            method = getattr(self.service, "events_page", None)
            if not callable(method):
                raise ServiceError("unsupported_capability")
            after = _integer(arguments.get("after", 0))
            value = _event_page_for_wire(method(principal, arguments["task_id"], after=after,
                limit=_integer(arguments.get("limit", 100), 1000)), after)
        elif name == 'mas.page':
            value = self.service.list_task_page(principal, **arguments)
        elif name == 'mas.graph':
            callback = getattr(self.service, 'inspect_graph', None)
            if not callable(callback):
                raise ServiceError('unsupported_capability')
            value = callback(principal, **arguments)
        elif name in {"mas.task", "mas.discover", "mas.recover"}:
            method = getattr(self.service, {"mas.task": "inspect_task", "mas.discover": "discover",
                                           "mas.recover": "recover"}[name], None)
            if not callable(method):
                raise ServiceError("unsupported_capability")
            if name == "mas.task":
                revision = arguments.get("revision")
                if revision is not None:
                    revision = _integer(revision)
                value = method(principal, arguments["task_id"], _text(arguments["path"]), revision=revision)
            elif name == "mas.recover":
                value = method(principal, arguments["task_id"])
            else:
                value = method(principal)
        else:
            limit = _integer(arguments.get("limit", 100), 100)
            if not limit:
                raise MCPError(-32602, "List limit must be positive")
            value = {"tasks": self.service.list_tasks(principal, limit, _integer(arguments.get("offset", 0)))}
        return tool_result(value, failed=value.get("state") == "failed")
