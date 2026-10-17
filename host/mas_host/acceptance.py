"""Bounded deployment exercises using real Rust storage and mock inference.

This command measures the declared workload on the machine executing it. Mock
mode never calls a paid provider; --live explicitly enables a bounded smoke test.
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import tempfile
import time

from .demo import DEFAULT_TOPOLOGY, run_demo
from .providers import MockProvider, ProviderError
from .provider_config import (CredentialError, create_providers, load_provider_configs,
                              selected_mode, selected_models)


def _job(options: dict) -> dict:
    started = time.monotonic()
    try:
        report = run_demo(topology=DEFAULT_TOPOLOGY, state_dir=options["state_dir"],
                          planner_provider=MockProvider("planner"),
                          reviewer_provider=MockProvider("reviewer"),
                          mas_bin=options["mas_bin"], gateway_bin=options["gateway_bin"])
        status_available = not report.get("status", {}).get("unavailable", False)
        result = {"ok": report["ok"], "run_id": report["run_id"],
                  "outcome": report["outcome"],
                  "status_available": status_available,
                  "calls": len(report.get("status", {}).get("calls", [])) if status_available else None,
                  "unknown_calls": sum(call.get("state") != "recorded"
                                       or call.get("evidence", {}).get("outcome") == "unknown"
                                       for call in report.get("status", {}).get("calls", []))
                                       if status_available else None}
    except Exception:
        # Public evidence contains fixed diagnostics, never adapter exceptions.
        result = {"ok": False, "outcome": "admission_or_execution_failed",
                  "status_available": False, "calls": None, "unknown_calls": None}
    result["duration_ms"] = round((time.monotonic() - started) * 1000)
    return result


def _binary_identity(binary: str) -> dict:
    resolved = shutil.which(binary)
    if resolved is None:
        return {"available": False}
    return {"available": True, "sha256": hashlib.sha256(Path(resolved).read_bytes()).hexdigest()}


def run_acceptance(*, state_dir: str | Path, runs: int = 12, concurrency: int = 3,
                   soak_seconds: int = 0, mas_bin: str = "mas",
                   gateway_bin: str = "mas-gateway") -> dict:
    """Run at most ``runs`` independent mesh jobs with a fixed worker count.

    A positive soak limit stops submission when the elapsed limit is reached;
    already admitted jobs finish under their configured execution deadlines.
    The shared deployment policy remains authoritative across all workers.
    """
    if type(runs) is not int or not 1 <= runs <= 1000:
        raise ValueError("runs must be an integer from 1 to 1000")
    if type(concurrency) is not int or not 1 <= concurrency <= 32:
        raise ValueError("concurrency must be an integer from 1 to 32")
    if type(soak_seconds) is not int or not 0 <= soak_seconds <= 86_400:
        raise ValueError("soak seconds must be an integer from 0 to 86400")
    from .operations import DeploymentLedger
    directory = Path(state_dir).resolve()
    ledger = DeploymentLedger(directory)
    before = ledger.health()
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    options = {"state_dir": str(directory), "mas_bin": mas_bin, "gateway_bin": gateway_bin}
    results: list[dict] = []
    submitted = 0
    # Spawn independent hosts rather than fork a live thread or share a Runner.
    with ProcessPoolExecutor(max_workers=min(concurrency, runs),
                             mp_context=multiprocessing.get_context("spawn")) as pool:
        pending = set()
        while pending or submitted < runs:
            within_duration = not soak_seconds or time.monotonic() - started < soak_seconds
            while submitted < runs and len(pending) < concurrency and within_duration:
                pending.add(pool.submit(_job, options))
                submitted += 1
            if not pending:
                break
            completed, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                try:
                    results.append(future.result())
                except Exception:
                    results.append({"ok": False, "outcome": "worker_process_failed",
                                    "status_available": False, "calls": None,
                                    "unknown_calls": None, "duration_ms": 0})
            if soak_seconds and time.monotonic() - started >= soak_seconds:
                # Continue draining, without extending the admission window.
                submitted = runs
    elapsed_ms = round((time.monotonic() - started) * 1000)
    durations = sorted(item["duration_ms"] for item in results)
    def percentile(percent: int) -> int:
        return durations[min(len(durations) - 1, (len(durations) * percent + 99) // 100 - 1)] if durations else 0
    return {
        "schema": "mas-deployment-acceptance/1", "ok": bool(results) and all(item["ok"] for item in results),
        "mode": "mock", "paid_provider_calls": 0, "started_at": started_at,
        "workload": {"name": "invoice-mesh", "agents_per_run": 3,
                     "mock_provider_calls_per_successful_run": 2,
                     "maximum_runs": runs, "concurrency": concurrency,
                     "soak_seconds": soak_seconds},
        "run_count": len(results), "success_count": sum(item["ok"] for item in results),
        "error_count": sum(not item["ok"] for item in results), "elapsed_ms": elapsed_ms,
        "latency_ms": {"minimum": min(durations, default=0), "p50": percentile(50),
                       "p95": percentile(95), "maximum": max(durations, default=0)},
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "sqlite": sqlite3.sqlite_version, "cpu_count": os.cpu_count(),
                        "mas": _binary_identity(mas_bin), "mas_gateway": _binary_identity(gateway_bin)},
        "deployment_before": before, "deployment_after": ledger.health(),
        "runs": results,
        "scope": "Measured bounded mock workload only. Fault tests and live-provider acceptance are separate evidence. No production capacity claim.",
    }


def run_live_acceptance(*, state_dir: str | Path, mas_bin: str = "mas",
                        gateway_bin: str = "mas-gateway", mode: str = "openai") -> dict:
    """Explicit two-submission provider smoke test; credentials remain in the host.

    Reservation limits record declared exposure. They do not establish the
    provider's actual invoice or protect against changes to provider prices.
    """
    started = time.monotonic()
    base = {"schema": "mas-deployment-acceptance/1", "mode": mode,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "workload": {"name": "invoice-mesh", "maximum_runs": 1, "concurrency": 1,
                         "maximum_provider_submissions": 2},
            "provider_internal_requests": "not_measured",
            "scope": "One live invoice check; no capacity, general accuracy, invoice ceiling or cancellation claim."}
    try:
        configs = load_provider_configs(mode)
        if any(config.provider == "mock" for config in configs.values()):
            raise ProviderError("live acceptance requires two live providers")
        base["mode"] = selected_mode(configs)
        providers = create_providers(configs)
    except CredentialError:
        return {**base, "ok": False, "status": "blocked_missing_credential",
                "run_count": 0, "paid_provider_calls": 0, "paid_provider_submissions": 0}
    except (ProviderError, ValueError):
        return {**base, "ok": False, "status": "blocked_provider_configuration",
                "run_count": 0, "paid_provider_calls": 0, "paid_provider_submissions": 0}
    from .operations import DeploymentLedger
    ledger = DeploymentLedger(Path(state_dir))
    before = ledger.health()
    models = selected_models(configs)
    base["providers"] = {role: config.provider for role, config in configs.items()}
    try:
        report = run_demo(topology=DEFAULT_TOPOLOGY, state_dir=state_dir,
                          planner_provider=providers["planner"], reviewer_provider=providers["reviewer"],
                          mode=base["mode"], models=models, mas_bin=mas_bin, gateway_bin=gateway_bin)
    except Exception:
        return {**base, "ok": False, "status": "execution_interrupted",
                "outcome": "unknown", "models": models,
                "deployment_before": before, "deployment_after": ledger.health()}
    return {**base, "ok": report["ok"], "status": "passed" if report["ok"] else "failed_or_unknown",
            "run_count": 1, "run_id": report["run_id"], "models": models,
            "outcome": report["outcome"], "result": report["result"],
            "call_records": report.get("status", {}).get("calls", []),
            "elapsed_ms": round((time.monotonic() - started) * 1000),
            "deployment_before": before, "deployment_after": ledger.health()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=12)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--soak-seconds", type=int, default=0)
    parser.add_argument("--live", action="store_true", help="explicitly permit one live two-provider mesh smoke test")
    parser.add_argument("--provider-mode", choices=("openai", "cursor", "mixed"), default="openai",
                        help="live provider defaults; per-role MAS_*_PROVIDER settings override them")
    parser.add_argument("--state-dir", help="persistent deployment directory; required for live mode, optional for mock mode")
    parser.add_argument("--report", type=Path, help="write the public JSON evidence report")
    args = parser.parse_args(argv)
    if args.live and not args.state_dir:
        parser.error("live acceptance requires an explicit persistent --state-dir for recovery and reconciliation")
    scratch = None
    try:
        if args.live and (args.runs != 1 or args.concurrency != 1 or args.soak_seconds):
            raise ValueError("live acceptance requires --runs 1 --concurrency 1 and no soak")
        if not args.state_dir:
            scratch = tempfile.TemporaryDirectory(prefix="mas-acceptance-")
        paths = {"state_dir": args.state_dir or scratch.name,
                 "mas_bin": os.environ.get("MAS_BIN", "mas"),
                 "gateway_bin": os.environ.get("MAS_GATEWAY_BIN", "mas-gateway")}
        if args.live:
            report = run_live_acceptance(**paths, mode=args.provider_mode)
        else:
            report = run_acceptance(**paths, runs=args.runs, concurrency=args.concurrency,
                                    soak_seconds=args.soak_seconds)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(report, sort_keys=True))
        return 0 if report["ok"] else 1
    except Exception:
        print(json.dumps({"ok": False, "error": "acceptance exercise failed; inspect deployment state"}))
        return 1
    finally:
        if scratch is not None:
            scratch.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
