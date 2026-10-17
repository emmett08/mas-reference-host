"""Optional LangGraph nodes; import the SDK only when constructing a node."""
from __future__ import annotations

from ..service_contracts import ServiceError
from .orchestrator import OrchestratorConnector, stable_request_key


def make_langgraph_node(connector: OrchestratorConnector, workflow: str, node_id: str,
                       *, input_key: str = "input", poll_seconds: float = 0,
                       interrupt_pending: bool = True):
    """Create a real StateGraph node with explicit operation identity.

    State must include ``mas_operation_id``. Keep it stable on retry and change
    it for a new logical operation in a graph loop. A durable checkpointer and
    ``configurable.thread_id`` are required for interrupt/resume operation.
    The returned ``mas_receipt`` retains failed and uncertain states verbatim.
    """
    from langgraph.types import interrupt
    from langchain_core.runnables import RunnableConfig

    def mas_node(state: dict, config: dict) -> dict:
        configurable = config.get("configurable", {})
        thread = configurable.get("thread_id")
        operation = state.get("mas_operation_id")
        key = stable_request_key("langgraph", thread, node_id, operation)
        current = connector.submit(key, workflow, state.get(input_key, {}))
        if current["state"] in {"queued", "running"} and poll_seconds:
            current = connector.wait(current["task_id"], timeout_seconds=poll_seconds)
        if interrupt_pending and current["state"] in {
                "queued", "running", "input_required", "auth_required", "unknown", "blocked"}:
            capabilities = current.get("capabilities", {})
            actions = ["poll"]
            if capabilities.get("cancel"):
                actions.append("cancel")
            if capabilities.get("update") and current["state"] in {"input_required", "auth_required"}:
                actions.append("input")
            command = interrupt({"mas_receipt": current,
                                 "actions": actions})
            if type(command) is not dict:
                raise ServiceError("invalid_resume_command")
            action = command.get("action")
            if action not in actions:
                raise ServiceError("unsupported_capability")
            if action == "input":
                # A separate stable key is required for each logical update.
                update_key = stable_request_key(key, "input", command.get("request_key"))
                current = connector.update(current["task_id"], update_key, command.get("input", {}))
            elif action == "cancel":
                current = connector.cancel(current["task_id"])
            elif action == "poll":
                current = connector.inspect(current["task_id"])
            else:
                raise ServiceError("invalid_resume_command")
        return {"mas_receipt": current}

    # LangGraph injects config only when this precise SDK annotation is present.
    # Assign the class object after the lazy import, avoiding an unresolved
    # forward annotation in a module that remains usable without the SDK.
    mas_node.__annotations__["config"] = RunnableConfig
    return mas_node
