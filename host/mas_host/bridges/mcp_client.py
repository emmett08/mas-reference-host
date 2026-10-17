"""MCP tool Strategy and durable Tasks client; no automatic effect retries."""
from __future__ import annotations

import hashlib
import re
import uuid
from pathlib import Path
from typing import Protocol

from ..codec import integer_json
from ..contracts import AdapterContext, Outcome, TaskRequest
from .mcp import MCPError, VERSION, TASKS, PREFIX, bounded, metadata, require_input_capabilities, validate_schema
from .remote import RemoteSnapshot, RemoteTaskService


class MCPTransport(Protocol):
    def descriptor(self) -> dict: ...
    def exchange(self, message: dict, *, headers: dict | None = None) -> dict: ...


def parameter_headers(schema: dict, arguments: dict) -> dict:
    """Validate every annotation and extract statically reachable properties.

    An annotation hidden under an array/composition/ref is invalid. We examine
    the complete schema so it cannot silently become an omitted routing header.
    """
    from .mcp import encode_header
    output, names = {}, set()

    def walk(node, path=(), reachable=True):
        if isinstance(node, list):
            for child in node:
                walk(child, path, False)
            return
        if type(node) is not dict:
            return
        if "x-mcp-header" in node:
            name, kind = node["x-mcp-header"], node.get("type")
            if (not reachable or not path or type(name) is not str
                    or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name)
                    or kind not in ("integer", "string", "boolean") or name.lower() in names):
                raise MCPError(-32602, "Invalid tool routing annotation")
            names.add(name.lower())
            value = arguments
            for key in path:
                if type(value) is not dict or key not in value:
                    value = None
                    break
                value = value[key]
            if value is not None:
                if ((kind == "integer" and (type(value) is not int or abs(value) > 2**53 - 1))
                        or (kind == "string" and type(value) is not str)
                        or (kind == "boolean" and type(value) is not bool)):
                    raise MCPError(-32602, "Tool routing argument has the wrong type")
                output["Mcp-Param-" + name] = encode_header(value)
        for key, value in node.items():
            if key == "properties" and type(value) is dict:
                for name, child in value.items():
                    walk(child, path + (name,), reachable)
            elif key != "x-mcp-header" and isinstance(value, (dict, list)):
                walk(value, path, False)
    walk(schema)
    return output


class MCPClient:
    """Explicit current-version client. The caller owns credential acquisition.

    Credentials stay in the transport and never enter descriptors or receipts.
    Tool schemas are supplied/frozen by trusted configuration after discovery;
    discovery is not permitted to change a running MAS topology.
    """
    def __init__(self, transport: MCPTransport, *, tasks=True, elicitation=False):
        self.transport, self.tasks, self.elicitation = transport, tasks, elicitation

    def descriptor(self):
        return {"protocol": "mcp", "version": VERSION, "transport": self.transport.descriptor(),
                "tasks": self.tasks, "elicitation": self.elicitation}

    def request(self, method: str, params: dict | None = None, *, headers=None) -> dict:
        params = bounded(params or {})
        if "_meta" in params:
            raise MCPError(-32602, "Caller cannot replace protocol capability metadata")
        params["_meta"] = metadata(tasks=self.tasks, elicitation=self.elicitation)
        request_id = uuid.uuid4().hex
        response = bounded(self.transport.exchange(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}, headers=headers))
        if (response.get("jsonrpc") != "2.0" or response.get("id") != request_id
                or ("result" in response) == ("error" in response)):
            raise MCPError(1002, "Invalid MCP response identity")
        if "error" in response:
            error = response["error"]
            if type(error) is not dict or type(error.get("code")) is not int:
                raise MCPError(1002, "Invalid MCP error")
            # Do not surface remote messages: they may echo secrets or prompts.
            raise MCPError(error["code"], "Remote MCP request failed")
        result = response["result"]
        if type(result) is not dict or result.get("resultType") not in ("complete", "input_required", "task"):
            raise MCPError(1002, "Unsupported MCP result type")
        if result["resultType"] == "task" and (method != "tools/call" or not self.tasks):
            raise MCPError(1002, "Unnegotiated MCP task result")
        if result["resultType"] == "input_required" and not self.elicitation:
            raise MCPError(1002, "Unnegotiated MCP input request")
        return result

    def discover(self):
        result = self.request("server/discover")
        if VERSION not in result.get("supportedVersions", []):
            raise MCPError(1002, "Peer does not advertise the configured protocol version")
        return result

    def list_tools(self):
        result = self.request("tools/list")
        if type(result.get("tools")) is not list:
            raise MCPError(1002, "Invalid MCP tool list")
        tools = []
        for tool in result["tools"]:
            try:
                if type(tool) is not dict or type(tool.get("inputSchema")) is not dict:
                    continue
                validate_schema(tool["inputSchema"])
                parameter_headers(tool["inputSchema"], {})
            except MCPError:
                continue
            tools.append(tool)
        return {**result, "tools": tools}

    def call_tool(self, tool: dict, arguments: dict):
        schema = tool.get("inputSchema", {})
        validate_schema(schema, arguments)
        headers = parameter_headers(schema, arguments)
        return self.request("tools/call", {"name": tool["name"], "arguments": bounded(arguments)}, headers=headers)


def _tool_output(result: dict) -> dict:
    if (result.get("resultType") != "complete" or type(result.get("content")) is not list
            or ("isError" in result and type(result["isError"]) is not bool)):
        raise MCPError(1002, "Invalid MCP tool result")
    # Preserve content, structuredContent and the business failure bit exactly.
    # No resource URLs are followed and no decimals are rounded.
    return bounded(result)


def _snapshot(result: dict) -> RemoteSnapshot:
    task_id, status = result.get("taskId"), result.get("status")
    if (type(task_id) is not str or not task_id or status not in
            ("working", "input_required", "completed", "failed", "cancelled")
            or type(result.get("createdAt")) is not str or type(result.get("lastUpdatedAt")) is not str
            or "ttlMs" not in result
            or (result.get("ttlMs") is not None and (type(result["ttlMs"]) is not int or result["ttlMs"] < 0))):
        raise MCPError(1002, "Invalid MCP task response")
    if result.get("resultType") == "task" and ((status == "completed" and "result" not in result)
                                               or (status == "failed" and "error" not in result)
                                               or (status == "input_required" and "inputRequests" not in result)):
        # CreateTaskResult carries the base Task, so terminal content or
        # pending input requests may be available only through tasks/get.
        # Persist the known identity before observing detailed task state.
        return RemoteSnapshot(task_id, "running", {"mcp_status": status, "result_pending": True,
                              "mcp_task": {k: v for k, v in result.items() if k != "resultType"}})
    if status == "completed":
        tool = _tool_output(result.get("result", {}))
        state = "failed" if tool.get("isError", False) else "completed"
        output = {"tool_result": tool}
    elif status == "failed":
        error = result.get("error")
        if type(error) is not dict or type(error.get("code")) is not int:
            raise MCPError(1002, "Invalid failed MCP task")
        output, state = {"protocol_error": {"code": error["code"]}}, "failed"
    elif status == "input_required":
        if type(result.get("inputRequests")) is not dict or not result["inputRequests"]:
            raise MCPError(1002, "Missing MCP task input requests")
        output, state = {"input_requests": result["inputRequests"]}, "input_required"
    else:
        output, state = {}, {"working": "running", "cancelled": "cancelled"}[status]
    # Keep the protocol observation distinct from application success.
    output["mcp_status"] = status
    output["mcp_task"] = {key: value for key, value in result.items()
                          if key not in ("resultType", "result", "error", "inputRequests")}
    return RemoteSnapshot(task_id, state, output)


class MCPRemoteClient:
    """RemoteTaskService port for one frozen MCP tool and its Tasks lifecycle."""
    def __init__(self, client: MCPClient, tool: dict):
        self.client, self.tool = client, bounded(tool)
        if type(tool.get("name")) is not str or type(tool.get("inputSchema")) is not dict:
            raise MCPError(-32602, "A frozen MCP tool definition is required")
        validate_schema(tool["inputSchema"])
        parameter_headers(tool["inputSchema"], {})
        if "outputSchema" in tool:
            validate_schema(tool["outputSchema"])

    def _snapshot(self, result):
        require_input_capabilities(result, metadata(tasks=self.client.tasks,
            elicitation=self.client.elicitation)[PREFIX + "clientCapabilities"])
        if result.get("status") == "completed" and "result" in result:
            self._validate_output(result.get("result", {}))
        return _snapshot(result)

    def _validate_output(self, result):
        result = _tool_output(result)
        if "outputSchema" in self.tool and not result.get("isError", False):
            if "structuredContent" not in result:
                raise MCPError(1002, "MCP output does not satisfy its configured schema")
            validate_schema(self.tool["outputSchema"], result["structuredContent"])
        return result

    def descriptor(self):
        return {**self.client.descriptor(), "tool": self.tool["name"],
                "schema_sha256": hashlib.sha256(integer_json(self.tool).encode()).hexdigest()}

    def submit(self, payload: dict) -> RemoteSnapshot:
        if type(payload) is not dict or set(payload) != {"arguments"} or type(payload["arguments"]) is not dict:
            raise MCPError(-32602, "MCP submission requires arguments")
        result = self.client.call_tool(self.tool, payload["arguments"])
        if result["resultType"] == "task":
            return self._snapshot(result)
        if result["resultType"] == "input_required":
            # MRTR does not provide a durable task identity. Do not invent a
            # remote ID or retry the original effect with guessed state.
            raise MCPError(1002, "Non-task continuation requires explicit reconciliation")
        result = self._validate_output(result)
        return RemoteSnapshot(None, "failed" if result.get("isError", False) else "completed", result)

    def poll(self, remote_id: str):
        result = self.client.request("tasks/get", {"taskId": remote_id})
        if result.get("resultType") != "complete":
            raise MCPError(1002, "Invalid MCP task observation result")
        if result.get("taskId") != remote_id:
            raise MCPError(1002, "MCP task identity changed")
        return self._snapshot(result)

    def cancel(self, remote_id: str):
        result = self.client.request("tasks/cancel", {"taskId": remote_id})
        if result.get("resultType") != "complete":
            raise MCPError(1002, "Invalid MCP cancellation acknowledgement")
        # An acknowledgement proves intent receipt only. Observation is another
        # admitted action, and might still show successful completion.
        return RemoteSnapshot(remote_id, "running", {"cancel_acknowledged": True})

    def update(self, remote_id: str, payload: dict):
        if set(payload) != {"input_responses"} or type(payload["input_responses"]) is not dict:
            raise MCPError(-32602, "MCP update requires input_responses")
        result = self.client.request("tasks/update", {"taskId": remote_id, "inputResponses": payload["input_responses"]})
        if result.get("resultType") != "complete":
            raise MCPError(1002, "Invalid MCP update acknowledgement")
        return RemoteSnapshot(remote_id, "running", {"update_acknowledged": True})


class MCPToolAdapter:
    """Admit each external call and retain receipt/uncertainty across restarts."""
    def __init__(self, service: RemoteTaskService, host_run_dir: Path, *, reservation_microusd: int):
        if type(reservation_microusd) is not int or not 0 <= reservation_microusd <= 100_000_000:
            raise MCPError(-32602, "MCP adapter requires an explicit bounded reservation")
        self.service, self.host_run_dir, self.reservation = service, host_run_dir, reservation_microusd

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        args = bounded(request.input)
        operation = request.operation
        if operation == "mcp.execute":
            # One authorised workflow can observe its own submissions without
            # changing the trusted principal/workflow/actor namespace. Operator
            # attachment is deliberately absent from this public dispatcher.
            action = args.pop("operation", None)
            if action not in ("call", "poll", "cancel", "update"):
                raise MCPError(-32602, "Unsupported MCP workflow action")
            operation = "mcp." + action
        key = args.get("key")
        common = {"context": context, "host_run_dir": self.host_run_dir,
                  "reservation_microusd": self.reservation}
        if operation == "mcp.call" and set(args) == {"key", "arguments"}:
            output = self.service.submit(key, {"arguments": args["arguments"]}, **common)
        elif operation == "mcp.poll" and set(args) == {"key"}:
            output = self.service.poll(key, **common)
        elif operation == "mcp.cancel" and set(args) == {"key", "request_key"}:
            output = self.service.cancel(key, request_key=args["request_key"], **common)
        elif operation == "mcp.update" and set(args) == {"key", "request_key", "input_responses"}:
            output = self.service.update(key, request_key=args["request_key"],
                                         payload={"input_responses": args["input_responses"]}, **common)
        elif operation == "mcp.attach" and set(args) == {"key", "remote_id", "evidence"}:
            output = self.service.attach(key, args["remote_id"], evidence=args["evidence"], **common)
        else:
            raise MCPError(-32602, "Unsupported MCP operation or parameters")
        return Outcome(output)
