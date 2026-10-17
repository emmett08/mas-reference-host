"""Run bridge ingress, a bounded process dispatcher or trusted local MCP."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import stat
import sys
import time

from .bridge_config import load_config, build_service, build_verifier
from .service_contracts import ServiceError


def _private_create(path: Path, data: bytes):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    identity = os.fstat(fd)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        _remove_created(path, identity)
        raise
    return identity


def _remove_created(path: Path, identity):
    """Rollback only the owned regular file created by this invocation."""
    try:
        current = path.lstat()
        if (stat.S_ISREG(current.st_mode) and current.st_uid == os.geteuid()
                and (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino)):
            path.unlink()
    except FileNotFoundError:
        pass


def _auth_destinations(token_file: Path, verifier_file: Path):
    if token_file.resolve() == verifier_file.resolve():
        raise ServiceError("invalid_auth_destination")
    for path in (token_file, verifier_file):
        if (path.exists() or path.is_symlink() or not path.parent.is_dir()
                or not os.access(path.parent, os.W_OK | os.X_OK)):
            raise ServiceError("invalid_auth_destination")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("serve", "worker", "stdio"):
        command = sub.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--state-dir", type=Path, required=True)
        if name == "serve":
            command.add_argument("--bind", default="127.0.0.1")
            command.add_argument("--port", type=int, default=8765)
            command.add_argument("--allow-loopback-http", action="store_true",
                                 help="explicit local development profile; public URL must be loopback")
        elif name == "worker":
            command.add_argument("--once", action="store_true")
            command.add_argument("--interval", type=float, default=0.25)
            command.add_argument("--processes", type=int,
                                 help="concurrent assignment processes, bounded by scheduler max_workers")
            command.add_argument("--drain-seconds", type=float, default=30,
                                 help="graceful shutdown allowance before interrupting active assignments")
        else:
            command.add_argument("--principal", required=True)
    health = sub.add_parser("health", help="trusted local HTTP/state readiness without provider calls")
    health.add_argument("--config", type=Path, required=True)
    health.add_argument("--state-dir", type=Path, required=True)
    health.add_argument("--port", type=int, default=8765)
    worker_health = sub.add_parser("worker-health", help="check the live scheduling loop and its deadline")
    worker_health.add_argument("--config", type=Path, help="required for the shared-authority profile")
    worker_health.add_argument("--state-dir", type=Path, required=True)
    worker_health.add_argument("--max-age-seconds", type=float, default=30)
    auth = sub.add_parser("init-auth", help="create a private bearer secret and hashed verifier file")
    auth.add_argument("--token-file", type=Path, required=True)
    auth.add_argument("--verifier-file", type=Path, required=True)
    auth.add_argument("--principal", required=True)
    auth.add_argument("--scope", action="append", choices=("mas:submit", "mas:read", "mas:cancel", "mas:update", "mas:worker", "mas:admin"),
                      help="repeat for each permitted scope; defaults to ordinary task access")
    from .operator_contracts import OPERATIONS
    operator = sub.add_parser("operator", help="call the authenticated operator API using logical identities")
    operator.add_argument("operation", choices=tuple(OPERATIONS))
    operator.add_argument("--endpoint", required=True)
    operator.add_argument("--token-file", type=Path, required=True)
    operator.add_argument("--operator", required=True, help="expected token principal")
    operator.add_argument("--input-file", type=Path, help="JSON request fields; omitted means an empty object")
    operator.add_argument("--allow-loopback-http", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "operator":
            from .bridges.service_client import TaskServiceClient, decode_integer_json
            payload = {}
            if args.input_file is not None:
                with args.input_file.open("rb") as stream:
                    payload = decode_integer_json(stream.read(65537))
                if type(payload) is not dict:
                    raise ServiceError("invalid_operator_request")
            client = TaskServiceClient(args.endpoint, token_file=args.token_file, principal=args.operator,
                                       allow_loopback_http=args.allow_loopback_http)
            print(json.dumps(client.operator(args.operator, args.operation, payload)), flush=True)
            return 0
        if args.command == "init-auth":
            from .bridges.authentication import _principal
            _principal(args.principal)
            _auth_destinations(args.token_file, args.verifier_file)
            token = secrets.token_urlsafe(48)
            record = {"version": 1, "credentials": [{"sha256": hashlib.sha256(token.encode()).hexdigest(),
                "principal": args.principal, "scopes": sorted(set(args.scope or ["mas:submit", "mas:read", "mas:cancel", "mas:update"]))}]}
            token_identity = _private_create(args.token_file, (token + "\n").encode())
            try:
                _private_create(args.verifier_file, (json.dumps(record) + "\n").encode())
            except BaseException:
                _remove_created(args.token_file, token_identity)
                raise
            print(json.dumps({"ok": True, "token_file": str(args.token_file), "verifier_file": str(args.verifier_file)}))
            return 0
        if args.command in ("health", "worker-health"):
            from .bridge_health import ingress_health, worker_health
            result = (ingress_health(args.state_dir, load_config(args.config), args.port)
                      if args.command == "health" else worker_health(args.state_dir, args.max_age_seconds,
                                                                     config=load_config(args.config) if args.config else None))
            print(json.dumps(result), flush=True)
            return 0
        config = load_config(args.config)
        service = build_service(config, args.state_dir)
        if args.command == "worker":
            if not 0.01 <= args.interval <= 30:
                raise ServiceError("invalid_interval")
            stopping = False
            def stop(_signal, _frame):
                nonlocal stopping
                stopping = True
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
            from .bridge_health import WorkerMonitor
            from .service_worker import ServiceDispatcher
            monitor = WorkerMonitor(args.state_dir)
            dispatcher = ServiceDispatcher(service, args.config, config, processes=args.processes,
                                           interval=args.interval, drain_seconds=args.drain_seconds)
            with monitor.ownership():
                monitor.write("starting")
                try:
                    dispatcher.run(monitor, stopping=lambda: stopping, once=args.once,
                                   on_result=lambda result: print(json.dumps(result), flush=True))
                finally:
                    monitor.write("stopped")
            return 0
        from .bridges.mcp import MCPBridge
        from .bridge_config import build_artifact_transfer
        artifact_transfer = build_artifact_transfer(config, args.state_dir, authority=getattr(service, 'authority', None))
        mcp = MCPBridge(service, artifact_transfer=artifact_transfer)
        if args.command == "stdio":
            from .bridges.server import serve_stdio
            if args.principal not in config["grants"]:
                raise ServiceError("forbidden")
            serve_stdio(mcp, principal=args.principal)
            return 0
        from .bridges.a2a import A2ABridge
        from .bridges.server import BridgeHTTPServer
        public_url = config.get("public_url", "http://127.0.0.1:8765")
        a2a = A2ABridge(service, endpoint=public_url + "/a2a", allow_http=args.allow_loopback_http)
        from .operator_service import OperatorService
        operators = OperatorService(service, operators=config["operators"]) if "operators" in config else None
        with BridgeHTTPServer((args.bind, args.port), service=service, verifier=build_verifier(config),
                              public_url=public_url, mcp_bridge=mcp, a2a_bridge=a2a, artifact_transfer=artifact_transfer,
                              allow_loopback_http=args.allow_loopback_http,
                              allowed_hosts=config.get("allowed_hosts"),
                              allowed_origins=config.get("allowed_origins", ()), operator_service=operators) as server:
            server.serve_forever()
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        code = error.code if isinstance(error, ServiceError) else "bridge_failed"
        print(json.dumps({"ok": False, "error": code}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
