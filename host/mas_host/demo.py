"""A logical mesh: model proposal, peer model review and an exact Python checker."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from .providers import (ProviderError, TotalProvider,
                        _reject_number, _unique_object, exact_total, provider_binding,
                        validate_invoice, validate_total)
from .contracts import AdapterContext, Outcome, TaskRequest
from .runner import Runner
from .provider_config import (create_providers, load_provider_configs,
                              selected_mode, selected_models)

DEFAULT_INVOICE = [
    {"item": "Notebook", "quantity": 2, "unit_cents": 1299},
    {"item": "Pen set", "quantity": 3, "unit_cents": 499},
    {"item": "Delivery", "quantity": 1, "unit_cents": 1402},
]
DEFAULT_TOPOLOGY = {
    "name": "compose-mesh",
    "description": "Two replaceable inference agents and an exact Python checker communicate over all six directed edges of a logical mesh.",
    "agents": ["planner", "reviewer", "checker"],
    "edges": [["checker", "planner"], ["planner", "checker"],
              ["planner", "reviewer"], ["reviewer", "planner"],
              ["reviewer", "checker"], ["checker", "reviewer"]],
    "application": "agent-task/1", "mailbox_capacity": 8,
    "max_messages": 30, "max_bytes": 131072, "max_calls": 4, "wall_seconds": 180,
}


def _invoice_input(request: TaskRequest, operation: str, fields: set[str]) -> list[dict[str, Any]]:
    if request.operation != operation or set(request.input) != fields:
        raise ValueError("unsupported demo operation or input fields")
    return validate_invoice(request.input["invoice"])


def _provider_descriptor(provider: TotalProvider) -> dict:
    return provider_binding(provider)[0]


def _infer(context: AdapterContext, provider: TotalProvider, invoice: list,
           proposal: dict | None = None) -> dict:
    descriptor, reservation = provider_binding(provider)
    effect = {"adapter": descriptor, "invoice": invoice, "proposal": proposal}
    return validate_total(context.invoke(
        lambda: provider.total(invoice, proposal=proposal, request_id=context.invocation_id),
        input=effect, reservation_microusd=reservation))


class PlannerAdapter:
    """Demonstrates one inference call followed by direct peer delegation."""

    def __init__(self, provider: TotalProvider):
        self.provider = provider

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        invoice = _invoice_input(request, "invoice.plan", {"invoice"})
        proposal = _infer(context, self.provider, invoice)
        review = context.request("reviewer", "invoice.review", {
            "invoice": invoice, "planner_total_cents": proposal["total_cents"],
        })
        return Outcome(output=review.output, status=review.status)


class ReviewerAdapter:
    """The reviewer consumes the planner's delivered proposal as untrusted data."""

    def __init__(self, provider: TotalProvider):
        self.provider = provider

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        invoice = _invoice_input(request, "invoice.review", {"invoice", "planner_total_cents"})
        proposal = validate_total({"total_cents": request.input["planner_total_cents"]})
        reviewed = _infer(context, self.provider, invoice, proposal)
        checked = context.request("checker", "invoice.check", {
            "invoice": invoice,
            "planner_total_cents": proposal["total_cents"],
            "reviewer_total_cents": reviewed["total_cents"],
        })
        return Outcome(output=checked.output, status=checked.status)


class CheckerAdapter:
    """No model or generated code: integer multiplication and addition only."""

    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome:
        invoice = _invoice_input(request, "invoice.check", {
            "invoice", "planner_total_cents", "reviewer_total_cents",
        })
        planner = validate_total({"total_cents": request.input["planner_total_cents"]})["total_cents"]
        reviewer = validate_total({"total_cents": request.input["reviewer_total_cents"]})["total_cents"]
        actual = exact_total(invoice)
        verified = planner == reviewer == actual
        return Outcome(output={
            "total_cents": actual, "planner_total_cents": planner, "reviewer_total_cents": reviewer,
            "planner_correct": planner == actual, "reviewer_correct": reviewer == actual, "verified": verified,
        }, status="completed" if verified else "unresolved")


def run_demo(*, topology: dict[str, Any], state_dir: str | Path,
             planner_provider: TotalProvider, reviewer_provider: TotalProvider,
             invoice: Any = None, mode: str = "mock", models: dict[str, str] | None = None,
             mas_bin: str = "mas", gateway_bin: str = "mas-gateway",
             resume_dir: str | Path | None = None, supervised: bool = True) -> dict[str, Any]:
    invoice = validate_invoice(DEFAULT_INVOICE if invoice is None else invoice)
    adapters = {"planner": PlannerAdapter(planner_provider), "reviewer": ReviewerAdapter(reviewer_provider),
                "checker": CheckerAdapter()}
    binding = {"planner": _provider_descriptor(planner_provider),
               "reviewer": _provider_descriptor(reviewer_provider), "checker": "exact-invoice/2"}
    workflow_version = "invoice-mesh/2:" + hashlib.sha256(
        json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    options = {"mas_bin": mas_bin, "gateway_bin": gateway_bin,
               "workflow_version": workflow_version, "supervised": supervised}
    instance = (Runner.resume(Path(resume_dir), adapters, **options) if resume_dir
                else Runner(topology, adapters, state_dir, **options))
    with instance as runner:
        outcome = None
        try:
            outcome = runner.request("checker", "planner", "invoice.plan", {"invoice": invoice})
        except Exception:
            # Keep the run identity and durable reservations inspectable even
            # when an adapter fails after an external provider call.
            pass
        try:
            status = runner.status()
        except Exception:
            status = {"unavailable": True, "error": "could not inspect run status"}
        report_path = runner.run_dir / "report.json"
        report = {
            "ok": outcome is not None and outcome.status == "completed" and outcome.output.get("verified") is True,
            "demo": "mixed-agent-mesh", "mode": mode, "run_id": runner.run_id,
            "models": models or {"planner": "mock", "reviewer": "mock", "checker": "python"},
            # A lost command reply can follow a committed Result. Only durable
            # task records can distinguish that case from failure or expiry.
            "outcome": outcome.status if outcome is not None else "unknown",
            "result": outcome.output if outcome is not None else {},
            "status": status, "events": runner.events, "report_path": str(report_path),
            "workflow_version": workflow_version, "supervised": supervised,
            "providers": {role: value["provider"] for role, value in binding.items() if role != "checker"},
            "invocation_unit": "provider_submission; SDK internal requests are not counted",
            "scope": "Logical peer mesh over one Rust/SQLite broker; Python checker validates this invoice only.",
        }
        if outcome is None:
            report["error"] = "exchange incomplete; inspect durable task and invocation records before retrying"
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report_path.chmod(0o600)
        return report


def _load_json(path: str) -> Any:
    with Path(path).open(encoding="utf-8") as stream:
        source = stream.read(65_537)
    if len(source.encode("utf-8")) > 65_536:
        raise ValueError("JSON file exceeds the input limit")
    return json.loads(source, object_pairs_hook=_unique_object,
                      parse_float=_reject_number, parse_constant=_reject_number)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("mock", "openai", "cursor", "mixed"), default=os.environ.get("MAS_MODE", "mock"))
    parser.add_argument("--state-dir", default=os.environ.get("MAS_STATE_DIR", ".mas-state"))
    parser.add_argument("--topology", default=os.environ.get("MAS_TOPOLOGY"), help="topology JSON; default is the bundled three-peer mesh")
    parser.add_argument("--invoice", default=os.environ.get("MAS_INVOICE"), help="JSON array of integer-cent invoice lines")
    parser.add_argument("--resume", help="resume an existing private run directory with the same inputs and model bindings")
    args = parser.parse_args(argv)
    try:
        topology = _load_json(args.topology) if args.topology else DEFAULT_TOPOLOGY
        invoice = _load_json(args.invoice) if args.invoice else DEFAULT_INVOICE
        configs = load_provider_configs(args.mode)
        providers = create_providers(configs)
        models, args.mode = selected_models(configs), selected_mode(configs)
        planner_provider, reviewer_provider = providers["planner"], providers["reviewer"]
        print(f"MAS mesh: planner={models['planner']}, reviewer={models['reviewer']}, checker=python ({args.mode})")
        report = run_demo(topology=topology, state_dir=args.state_dir, planner_provider=planner_provider,
                          reviewer_provider=reviewer_provider, invoice=invoice, mode=args.mode, models=models,
                          mas_bin=os.environ.get("MAS_BIN", "mas"),
                          gateway_bin=os.environ.get("MAS_GATEWAY_BIN", "mas-gateway"), resume_dir=args.resume)
    except ProviderError as error:
        report = {"ok": False, "error": str(error), "mode": args.mode}
    except Exception:
        # Do not print arbitrary adapter/SDK exceptions or tracebacks to a shared
        # console: these may include prompts or provider credentials.
        report = {"ok": False, "error": "mesh run failed; inspect private run state before retrying", "mode": args.mode}
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0 if report["ok"] else 1
