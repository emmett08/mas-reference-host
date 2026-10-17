"""Run a controller RPC endpoint or an authenticated registered leaf worker."""
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
import re
import signal
import sys
import time

from .bridge_config import _pairs, build_verifier
from .codec import integer_json
from .distributed import WorkerController
from .distributed_worker import DurableWorker, RegisteredLeafExecutor, WorkerClient
from .service_contracts import ServiceError


def _configuration(path):
    with path.open(encoding="utf-8") as stream:
        value = json.loads(stream.read(65537), object_pairs_hook=_pairs)
    integer_json(value)
    if type(value) is not dict or value.get("version") != 1:
        raise ServiceError("invalid_configuration")
    return value


def _factory(target):
    if type(target) is not str or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", target):
        raise ServiceError("invalid_factory")
    module, name = target.split(":")
    return getattr(importlib.import_module(module), name)()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    controller = commands.add_parser("controller")
    controller.add_argument("--bind", default="127.0.0.1")
    controller.add_argument("--port", type=int, default=8766)
    reconcile = commands.add_parser("reconcile", help="release a remote obligation after host-journal resolution")
    reconcile.add_argument("--run-id", required=True)
    reconcile.add_argument("--call-id", required=True)
    reconcile.add_argument("--operator", required=True)
    worker = commands.add_parser("run")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--interval", type=float, default=.25)
    for command in (controller, worker, reconcile):
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--state-dir", type=Path, required=True)
        command.add_argument("--allow-loopback-http", action="store_true")
    args = parser.parse_args(argv)
    authority = None
    try:
        config = _configuration(args.config)
        if args.command in {"controller", "reconcile"}:
            if set(config) - {"version", "worker_grants", "auth", "public_url", "allowed_hosts",
                              "max_outstanding", "per_principal", "lease_ms", "artifact_factory", "artifact_store_id", "authority_factory"}:
                raise ServiceError("invalid_configuration")
            authority = _factory(config["authority_factory"]) if "authority_factory" in config else None
            if authority is not None:
                from .shared_control import SharedWorkerController
                controller_type, controller_root = SharedWorkerController, authority
            else:
                controller_type, controller_root = WorkerController, args.state_dir
            controller = controller_type(controller_root, config["worker_grants"],
                max_outstanding=config.get("max_outstanding", 100),
                per_principal=config.get("per_principal", 10), lease_ms=config.get("lease_ms", 30000),
                artifact_repository=_factory(config["artifact_factory"]) if "artifact_factory" in config else None,
                artifact_store_id=config.get("artifact_store_id"))
            if args.command == "reconcile":
                print(json.dumps(controller.reconcile(run_id=args.run_id, call_id=args.call_id, operator=args.operator)))
                return 0
            from .bridges.server import BridgeHTTPServer
            if authority is not None:
                authority.start_renewal()
            with BridgeHTTPServer((args.bind, args.port), service=None, verifier=build_verifier(config),
                    public_url=config["public_url"], allowed_hosts=config.get("allowed_hosts"),
                    allow_loopback_http=args.allow_loopback_http, worker_controller=controller) as server:
                server.serve_forever()
            return 0
        if set(config) - {"version", "endpoint", "token_file", "factory", "trust_zone", "transport"}:
            raise ServiceError("invalid_configuration")
        # Factory paths are operator configuration; they are never task input.
        operations = _factory(config["factory"])
        if type(operations) not in (tuple, list) or any(not isinstance(op.execute, RegisteredLeafExecutor) for op in operations):
            raise ServiceError("registered_leaf_execution_required")
        policy = None
        if "transport" in config:
            from .bridges.http_policy import HTTPTransportPolicy
            policy = HTTPTransportPolicy.from_config(config["transport"])
        client = WorkerClient(config["endpoint"], token_file=Path(config["token_file"]),
            allow_loopback_http=args.allow_loopback_http, transport_policy=policy)
        worker = DurableWorker(client, args.state_dir, operations, trust_zone=config["trust_zone"])
        if not 0.01 <= args.interval <= 30:
            raise ServiceError("invalid_interval")
        stopped = False
        def stop(_signal, _frame):
            nonlocal stopped
            stopped = True
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        while not stopped:
            value = worker.run_once()
            if value is not None:
                print(json.dumps(value), flush=True)
            if args.once:
                return 0
            if value is None:
                time.sleep(args.interval)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print(json.dumps({"ok": False, "error": error.code if isinstance(error, ServiceError) else "worker_failed"}), file=sys.stderr)
        return 1
    finally:
        if authority is not None:
            authority.close()


if __name__ == "__main__":
    raise SystemExit(main())
