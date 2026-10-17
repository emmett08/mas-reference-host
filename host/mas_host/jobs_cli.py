"""Submit and reconcile durable Cursor Cloud jobs without replaying launches."""
from __future__ import annotations

import argparse
import json
import importlib
import os
from pathlib import Path

from .cursor_cloud import CloudError, CloudJobSpec, CursorCloudClient, _unique
from .durable_jobs import CursorCloudJobAdapter, DurableJobService, JobStore, binding, receipt


def _object_file(path: str, maximum: int = 65_536) -> dict:
    with Path(path).open("rb") as stream:
        source = stream.read(maximum + 1)
    if len(source) > maximum:
        raise CloudError("cloud job JSON file exceeds input limit")
    value = json.loads(source, object_pairs_hook=_unique,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    if type(value) is not dict:
        raise CloudError("cloud job input must be a JSON object")
    return value


def _key() -> str:
    from .provider_config import read_api_key
    return read_api_key("cursor")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--state-dir", default=os.environ.get("MAS_STATE_DIR", ".mas-state"))
    root.add_argument('--factory', help='trusted module:callable returning a bound job repository')
    commands = root.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit", help="admit one cloud job through the MAS runner")
    submit.add_argument("job_id", help="stable caller-selected job ID; reuse it for the same submission")
    submit.add_argument("--spec", required=True, help="private JSON file containing CloudJobSpec fields")
    submit.add_argument("--resume", help="resume original MAS submission run after reconciliation")
    status = commands.add_parser("status", help="inspect local state without contacting Cursor")
    status.add_argument("job_id")
    status.add_argument("--result", action="store_true", help="include untrusted remote result text")
    status.add_argument("--audit", action="store_true", help="include operator reconciliation audit")
    reconcile = commands.add_parser("reconcile", help="poll exact saved run or attach an evidenced run")
    reconcile.add_argument("job_id")
    reconcile.add_argument("--run-id", help="explicit exact remote run for an uncertain launch")
    reconcile.add_argument("--evidence", help="private JSON object supporting attachment or host resolution")
    reconcile.add_argument("--resolve-host", action="store_true", help="resolve uncertain MAS submission with its durable receipt")
    cancel = commands.add_parser("cancel", help="request cancellation; reconcile confirms terminal status")
    cancel.add_argument("job_id")
    cancel.add_argument("--retry", action="store_true", help="explicitly repeat cancellation for this same run")
    return root


class _Coordinator:
    def handle(self, context, request):
        raise CloudError("cloud coordinator does not accept inbound work")


def submit_job(store: JobStore, client, job_id: str, spec: CloudJobSpec, *, resume_dir=None) -> dict:
    """Enter the normal Rust capability, deployment quota and supervised path."""
    from .runner import Runner
    saved = store.prepare(job_id, spec)
    if saved["state"] != "prepared" and not resume_dir:
        return {"ok": True, "submission": receipt(saved), "job": saved, "reused": True}
    service = store.service(client)
    adapter = CursorCloudJobAdapter(service, spec)
    adapters = {"coordinator": _Coordinator(), "cloud": adapter}
    options = {"workflow_version": "cursor-cloud-submission/1:" + binding(spec),
               "mas_bin": os.environ.get("MAS_BIN", "mas"),
               "gateway_bin": os.environ.get("MAS_GATEWAY_BIN", "mas-gateway")}
    topology = {"name": "cursor-cloud-submission", "description": "One durable remote job submission",
                "agents": ["coordinator", "cloud"], "edges": [["coordinator", "cloud"], ["cloud", "coordinator"]],
                "application": "agent-task/1", "mailbox_capacity": 4, "max_messages": 8,
                "max_bytes": 131_072, "max_calls": 1, "wall_seconds": 120}
    from .shared_jobs import SharedJobStore
    if isinstance(store, SharedJobStore):
        runner = store.authority.runner(topology, adapters, store.state_dir,
            principal=store.principal, run_id=Path(resume_dir).name if resume_dir else None,
            workflow_version=options['workflow_version'])
    else:
        runner = (Runner.resume(Path(resume_dir), adapters, **options) if resume_dir else
                  Runner(topology, adapters, store.state_dir, **options))
    with runner:
        adapter.host_run_dir = runner.run_dir
        try:
            outcome = runner.request("coordinator", "cloud", "cloud.submit", {"job_id": job_id})
        except Exception:
            return {"ok": False, "error": "submission incomplete; reconcile the durable job before resuming",
                    "run_dir": str(runner.run_dir), "job": store.status(job_id)}
        return {"ok": outcome.status == "completed", "submission": outcome.output,
                "run_dir": str(runner.run_dir), "job": store.status(job_id)}


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.factory:
            module, separator, name = args.factory.partition(':')
            if not separator or not module or not name or '.' in name:
                raise CloudError('invalid trusted cloud job repository factory')
            store = getattr(importlib.import_module(module), name)()
            from .shared_jobs import SharedJobStore
            if not isinstance(store, (JobStore, SharedJobStore)):
                raise CloudError('cloud job factory must return a supported repository')
        else:
            store = JobStore(Path(args.state_dir))
        if args.command == "status":
            report = {"ok": True, "job": (store.inspect(args.job_id, include_result=args.result) if args.audit else
                                           store.status(args.job_id, include_result=args.result))}
        else:
            local_resolution = args.command == "reconcile" and args.resolve_host and not args.run_id
            client = None if local_resolution else CursorCloudClient(_key())
            service = store.service(client)
            if args.command == "submit":
                spec = CloudJobSpec(**_object_file(args.spec))
                report = submit_job(store, client, args.job_id, spec, resume_dir=args.resume)
            elif args.command == "cancel":
                report = {"ok": True, "job": service.cancel(args.job_id, retry=args.retry)}
            else:
                evidence = _object_file(args.evidence, 8192) if args.evidence else None
                if (args.run_id or args.resolve_host) and not evidence:
                    raise CloudError("attachment and host resolution require --evidence")
                job = (service.attach(args.job_id, args.run_id, evidence=evidence) if args.run_id
                       else store.status(args.job_id) if local_resolution else service.poll(args.job_id))
                report = {"ok": True, "job": job}
                if args.resolve_host:
                    report["host_resolution"] = service.resolve_submission(args.job_id, evidence=evidence)
    except CloudError as error:
        report = {"ok": False, "error": str(error)}
    except Exception:
        report = {"ok": False, "error": "cloud job operation failed; inspect private state and command arguments"}
    print(json.dumps(report, sort_keys=True, separators=(",", ":")), flush=True)
    return 0 if report.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
