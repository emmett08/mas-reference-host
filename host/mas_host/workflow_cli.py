"""Trusted local commands for bounded workflow composition and inspection."""
from __future__ import annotations

import argparse
from collections.abc import Mapping
import importlib
import json
from pathlib import Path
import re
import sys

from .codec import JSON_BYTES, integer_json
from .contracts import RunnerError
from .service_contracts import ServiceError
from .topology import inspect_topology, topology_mermaid
from .workflow_inspection import encode_task_inspection, validate_task_selector


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ServiceError("duplicate_field")
        result[key] = value
    return result


def _read_object(path: Path) -> dict:
    try:
        with path.open("rb") as stream:
            raw = stream.read(JSON_BYTES + 1)
        if len(raw) > JSON_BYTES:
            raise ServiceError("input_too_large")
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
        integer_json(value)
        if type(value) is not dict:
            raise ServiceError("invalid_input")
        return value
    except (OSError, UnicodeError, ValueError, RunnerError):
        raise ServiceError("invalid_input") from None


def load_registry(factory: str) -> dict:
    """Import only an explicitly supplied, trusted local factory argument.

    Plan JSON and application input cannot select imports. This loader is a code
    execution boundary for a deployment operator, not an untrusted extension API.
    """
    if (type(factory) is not str or len(factory) > 512
            or re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", factory) is None):
        raise ServiceError("invalid_workflow_factory")
    module, name = factory.split(":")
    function = getattr(importlib.import_module(module), name)
    if not callable(function):
        raise ServiceError("invalid_workflow_factory")
    result = function()
    if type(result) is not dict or set(result) != {"plans", "workflows", "grants"}:
        raise ServiceError("invalid_workflow_registry")
    return result


def _registry_report(registry: dict) -> dict:
    from .task_service import _text
    from .workflow_plan import bind_plan, validate_registry
    plans, workflows = registry["plans"], registry["workflows"]
    validate_registry(plans, workflows)
    if not isinstance(registry["grants"], Mapping):
        raise ServiceError("invalid_grants")
    for principal, names in registry["grants"].items():
        _text(principal, "principal")
        try:
            if isinstance(names, str) or not set(names) <= set(plans):
                raise ServiceError("invalid_grants")
        except TypeError:
            raise ServiceError("invalid_grants") from None
    return {"plans": {name: bind_plan(name, plans, workflows) for name in sorted(plans)},
            "topologies": {name: inspect_topology(workflow.topology)
                           for name, workflow in sorted(workflows.items())}}


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="validate without starting a workflow")
    source = validate.add_mutually_exclusive_group(required=True)
    source.add_argument("--topology", type=Path)
    source.add_argument("--factory", help="trusted local module:callable returning the registry")
    graph = commands.add_parser("graph", help="emit Mermaid for exact permitted routes")
    graph.add_argument("--topology", type=Path, required=True)
    for name in ("submit", "run", "status", "task", "cancel", "continue", "recover"):
        command = commands.add_parser(name)
        command.add_argument("--factory", required=True)
        command.add_argument("--state-dir", type=Path, required=True)
        command.add_argument("--principal", required=True,
                             help="identity asserted by the trusted local launcher")
        command.add_argument("--mas-bin", default="mas")
        command.add_argument("--gateway-bin", default="mas-gateway")
        if name == "run":
            selection = command.add_mutually_exclusive_group(required=True)
            selection.add_argument("--instance-id")
            selection.add_argument("--plan")
        elif name == "submit":
            command.add_argument("--plan", required=True)
        else:
            command.add_argument("--instance-id", required=True)
        if name in ("submit", "run", "continue"):
            command.add_argument("--request-key", required=name != "run")
            command.add_argument("--input", type=Path, required=name != "run")
        if name in ("submit", "run"):
            command.add_argument("--request-epoch", help="versioned idempotency epoch for new submissions")
        if name == "continue":
            command.add_argument("--path", required=True, help="durable leaf path from status")
        if name == "task":
            command.add_argument("--path", required=True, help="durable leaf path from status")
            command.add_argument("--revision", type=int, help="explicit revision; defaults to the latest")
    return parser


def main(argv=None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "graph":
            print(topology_mermaid(_read_object(args.topology)), end="")
            return 0
        if args.command == "validate":
            result = (inspect_topology(_read_object(args.topology)) if args.topology
                      else _registry_report(load_registry(args.factory)))
        else:
            # Validate all invocation arguments before importing trusted factory
            # code or creating a state directory.
            if args.command == "run":
                if args.plan and (args.request_key is None or args.input is None):
                    raise ServiceError("run_requires_request_key_and_input")
                if args.instance_id and (args.request_key is not None or args.input is not None or args.request_epoch is not None):
                    raise ServiceError("resume_does_not_accept_new_input")
            if args.command == "task":
                validate_task_selector(args.path, args.revision)
            input_value = _read_object(args.input) if getattr(args, "input", None) else None
            registry = load_registry(args.factory)
            from .workflow_engine import WorkflowEngine
            options = {}
            if args.command == "task":
                from .operations import DeploymentLedger
                options['operations'] = DeploymentLedger(args.state_dir, readonly=True)
            engine = WorkflowEngine(args.state_dir, **registry, mas_bin=args.mas_bin,
                                    gateway_bin=args.gateway_bin, **options)
            if args.command == "submit":
                result = engine.submit(args.principal, args.request_key, args.plan, input_value, **({"request_epoch": args.request_epoch} if args.request_epoch is not None else {}))
            elif args.command == "run":
                identifier = args.instance_id
                if args.plan:
                    identifier = engine.submit(args.principal, args.request_key, args.plan,
                                               input_value, **({"request_epoch": args.request_epoch} if args.request_epoch is not None else {}))["instance_id"]
                result = engine.run(args.principal, identifier)
            elif args.command == "continue":
                result = engine.continue_task(args.principal, args.instance_id, args.path,
                                              args.request_key, input_value)
            elif args.command == "task":
                result = engine.inspect_task(args.principal, args.instance_id, args.path, args.revision)
            else:
                method = {"status": engine.inspect, "cancel": engine.cancel,
                          "recover": engine.recover}[args.command]
                result = method(args.principal, args.instance_id)
        if args.command == "task":
            print(encode_task_inspection(result), end="", flush=True)
        else:
            print(json.dumps({"ok": True, "result": result}, sort_keys=True), flush=True)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        code = error.code if isinstance(error, ServiceError) else "workflow_failed"
        print(json.dumps({"ok": False, "error": code}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
