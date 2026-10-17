"""Operator commands for one bounded, private MAS deployment."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import sqlite3
import threading
import time

from .monitor import ReconcilerMonitor, reconciler_health
from .operations import (DeploymentLedger, DeploymentPolicy, OperationsError,
                         archive_workflows, backup_state, compact_history, prune_runs, reconcile_run, reconcile_state,
                         restore_state, retained_runs)


def _object_file(path: str) -> dict:
    source = Path(path).read_text(encoding="utf-8")
    if len(source.encode("utf-8")) > 65_536:
        raise OperationsError("operator JSON file exceeds input limit")
    result = json.loads(source)
    if type(result) is not dict:
        raise OperationsError("operator JSON input must be an object")
    return result


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--state-dir", default=os.environ.get("MAS_STATE_DIR", ".mas-state"))
    commands = root.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="create an immutable deployment policy before the first run")
    init.add_argument("--policy", help="JSON object of DeploymentPolicy integer limits")
    commands.add_parser("health", help="read lifetime quotas and disk thresholds without migration")
    for name in ('doctor', 'preflight', 'effective-config'):
        check = commands.add_parser(name, help='read-only configuration and runtime diagnostics; never load workflow factories')
        check.add_argument('--config', type=Path, required=name in {'preflight', 'effective-config'})
    metrics = commands.add_parser('metrics', help='read bounded aggregate state without identities or request bodies')
    metrics.add_argument('--format', choices=('json', 'prometheus'), default='json')
    ready = commands.add_parser("reconciler-health", help="check the live reconciler independently of admission quotas")
    ready.add_argument("--max-age-seconds", type=float, default=30)
    commands.add_parser("runs", help="list retained run identities and directories")
    commands.add_parser("budget-history", help="read audited lifetime budget amendments")
    amend = commands.add_parser("amend-budget", help="increase lifetime limits with an attributed idempotent receipt")
    amend.add_argument("--request-key", required=True)
    amend.add_argument("--principal", required=True)
    amend.add_argument("--reason", required=True)
    amend.add_argument("--expected-revision", type=int, required=True)
    amend.add_argument("--limits", required=True, help="JSON object of absolute lifetime limits")
    deadline = commands.add_parser('extend-deadline', help='extend an open waiting clock with a durable operator audit')
    deadline.add_argument('--config', type=Path, required=True)
    deadline.add_argument('--operator', required=True, help='trusted local operator attribution')
    deadline.add_argument('--owner', required=True)
    deadline.add_argument('task_id')
    deadline.add_argument('--path', help='exact nested composition scope; omitted for the root clock')
    deadline.add_argument('--request-key', required=True)
    deadline.add_argument('--reason', required=True)
    deadline.add_argument('--expected-revision', type=int, required=True)
    deadline.add_argument('--limits', required=True, help='JSON file of absolute business/queue/pause seconds')
    inspect = commands.add_parser("inspect", help="inspect durable workflow and invocation checkpoints")
    inspect.add_argument("run_dir")
    reconcile = commands.add_parser("reconcile", help="reconcile protocol expiry for one run or all retained runs")
    reconcile.add_argument("--run-dir")
    watch = commands.add_parser("watch", help="independently reconcile expiry until SIGTERM/SIGINT")
    watch.add_argument("--interval", type=float, default=5, help="seconds between scans (0.1 to 60)")
    watch.add_argument("--iterations", type=int, default=0, help="stop after this many scans; zero means until signalled")
    resolution = commands.add_parser("resolve", help="record an operator-supported external invocation outcome")
    resolution.add_argument("run_dir")
    resolution.add_argument("call_id")
    resolution.add_argument("--decision", choices=("completed", "confirmed_no_effect"), required=True)
    resolution.add_argument("--evidence", required=True, help="private JSON object documenting the basis for the decision")
    resolution.add_argument("--output", help="verified JSON object; required for a completed invocation")
    for name in ('recovery-inbox', 'recovery-resolve'):
        recovery = commands.add_parser(name, help='inspect or resolve task recovery through the configured service')
        recovery.add_argument('--config', type=Path, required=True)
        recovery.add_argument('--operator', required=True, help='trusted local operator attribution')
        recovery.add_argument('--owner', required=True, help='task owner whose configured grants are rechecked')
        recovery.add_argument('task_id')
        if name == 'recovery-resolve':
            recovery.add_argument('call_id')
            recovery.add_argument('--request-key', required=True)
            recovery.add_argument('--decision', choices=('completed', 'confirmed_no_effect'), required=True)
            recovery.add_argument('--evidence', required=True)
            recovery.add_argument('--output')
    backup = commands.add_parser("backup", help="snapshot a quiescent deployment into a private empty directory")
    backup.add_argument("destination")
    restore = commands.add_parser("restore", help="verify and restore a snapshot into an empty state directory")
    restore.add_argument("bundle")
    prune = commands.add_parser("prune", help="list terminal runs eligible for retention deletion")
    prune.add_argument("--older-than-seconds", type=int, required=True)
    prune.add_argument("--apply", action="store_true", help="delete eligible files while preserving charged admissions")
    archive = commands.add_parser("archive-workflows", help="compact settled workflow history, keeping deduplication receipts")
    archive.add_argument("--older-than-seconds", type=int, required=True)
    archive.add_argument("--apply", action="store_true", help="archive eligible history before pruning its run files")
    task_archive = commands.add_parser('archive-tasks', help='compact settled task history, keeping request receipts')
    task_archive.add_argument('--older-than-seconds', type=int, required=True)
    task_archive.add_argument('--apply', action='store_true')
    commands.add_parser('compact-history', help='reclaim deleted SQLite payload pages after archival')
    return root


def _watch(state_dir: Path, interval: float, iterations: int) -> dict:
    if not 0.1 <= interval <= 60 or iterations < 0:
        raise OperationsError("watch interval must be 0.1 to 60 seconds; iterations must be nonnegative")
    ledger = DeploymentLedger(state_dir)
    monitor = ReconcilerMonitor(state_dir)
    stopping = threading.Event()
    previous = {}
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, lambda *_: stopping.set())
    count = failures = 0
    try:
        with monitor.ownership():
            monitor.write("starting", scans=0, failures=0)
            try:
                while not stopping.is_set():
                    report = reconcile_state(state_dir, os.environ.get("MAS_BIN", "mas"))
                    try:
                        health = ledger.health()
                    except (OperationsError, sqlite3.Error, OSError):
                        # Durable format/storage failures must keep readiness
                        # closed while leaving the watcher alive to observe a
                        # repair. Never print database or application details.
                        health = {"ok": False, "error": "deployment health cannot be verified"}
                        report = {**report, "ok": False}
                    count += 1
                    failures += int(report.get("ok") is not True)
                    monitor.write("healthy" if report.get("ok") is True else "degraded",
                                  scans=count, failures=failures)
                    print(json.dumps({"event": "reconciliation", "timestamp": int(time.time()),
                                      "health": health, **report}, sort_keys=True), flush=True)
                    if iterations and count >= iterations:
                        break
                    stopping.wait(interval)
            finally:
                monitor.write("stopped", scans=count, failures=failures)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return {"ok": failures == 0, "scans": count, "failed_scans": failures,
            "stopped": stopping.is_set()}



def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    state_dir = Path(args.state_dir)
    try:
        if args.command == "init":
            policy = DeploymentPolicy(**_object_file(args.policy)) if args.policy else None
            result = {"ok": True, "health": DeploymentLedger(state_dir, policy).health()}
        elif args.command == "health":
            result = {"ok": True, "health": DeploymentLedger(state_dir, readonly=True).health()}
        elif args.command in {'doctor', 'preflight', 'effective-config', 'metrics'}:
            from .diagnostics import doctor, effective_configuration, operational_metrics, prometheus_metrics
            if args.command == 'effective-config':
                result = effective_configuration(args.config)
            elif args.command == 'metrics':
                result = operational_metrics(state_dir)
                if args.format == 'prometheus':
                    print(prometheus_metrics(result), end='', flush=True)
                    return 0 if result.get('ok') is True else 1
            else:
                result = doctor(state_dir, args.config)
        elif args.command == "reconciler-health":
            result = reconciler_health(state_dir, args.max_age_seconds)
        elif args.command == "runs":
            result = {"ok": True, "runs": retained_runs(state_dir)}
        elif args.command == "budget-history":
            result = {"ok": True, "amendments": DeploymentLedger(state_dir, readonly=True).budget_history()}
        elif args.command == "amend-budget":
            receipt = DeploymentLedger(state_dir).amend_budget(request_key=args.request_key,
                principal=args.principal, reason=args.reason, expected_revision=args.expected_revision,
                limits=_object_file(args.limits))
            result = {"ok": True, "amendment": receipt}
        elif args.command == 'extend-deadline':
            from .bridge_config import build_service, load_config
            from .deadlines import DeadlineService
            service = build_service(load_config(args.config), state_dir)
            deadline = DeadlineService(service, operators={args.operator: {args.owner}})
            result = {'ok': True, 'amendment': deadline.extend(args.operator, args.owner,
                args.task_id, request_key=args.request_key, reason=args.reason,
                expected_revision=args.expected_revision, limits=_object_file(args.limits), path=args.path)}
        elif args.command == "inspect":
            from .journal import inspect_run
            result = {"ok": True, "run": inspect_run(Path(args.run_dir))}
        elif args.command == "reconcile":
            result = ({"ok": True, **reconcile_run(Path(args.run_dir), os.environ.get("MAS_BIN", "mas"))}
                      if args.run_dir else reconcile_state(state_dir, os.environ.get("MAS_BIN", "mas")))
        elif args.command == "watch":
            result = _watch(state_dir, args.interval, args.iterations)
        elif args.command == "resolve":
            from .journal import resolve_invocation
            output = _object_file(args.output) if args.output else None
            result = {"ok": True, "resolution": resolve_invocation(Path(args.run_dir), args.call_id,
                      decision=args.decision, evidence=_object_file(args.evidence), output=output)}
        elif args.command in {'recovery-inbox', 'recovery-resolve'}:
            from .bridge_config import build_service, load_config
            from .recovery import RecoveryService
            service = build_service(load_config(args.config), state_dir)
            recovery = RecoveryService(service, operators={args.operator: {args.owner}})
            if args.command == 'recovery-inbox':
                result = {'ok': True, 'inbox': recovery.inbox(args.operator, args.owner, args.task_id)}
            else:
                result = {'ok': True, 'resolution': recovery.resolve(args.operator, args.owner,
                    args.task_id, args.call_id, request_key=args.request_key, decision=args.decision,
                    evidence=_object_file(args.evidence), output=_object_file(args.output) if args.output else None)}
        elif args.command == "backup":
            result = backup_state(state_dir, Path(args.destination))
        elif args.command == "restore":
            result = restore_state(Path(args.bundle), state_dir)
        elif args.command == "prune":
            result = prune_runs(state_dir, args.older_than_seconds, args.apply)
        elif args.command == "archive-workflows":
            result = archive_workflows(state_dir, args.older_than_seconds, args.apply)
        elif args.command == 'archive-tasks':
            from .task_history import archive_tasks
            result = archive_tasks(state_dir, args.older_than_seconds, args.apply)
        elif args.command == 'compact-history':
            result = compact_history(state_dir)
        else:
            raise OperationsError("unsupported operator command")
    except OperationsError as error:
        result = {"ok": False, "error": str(error)}
    except Exception:
        # Journal/provider data and filesystem names must not leak through an
        # arbitrary exception traceback on a shared operator console.
        result = {"ok": False, "error": "operator action failed; inspect private state and command arguments"}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
    return 0 if result.get("ok") is True else 1
