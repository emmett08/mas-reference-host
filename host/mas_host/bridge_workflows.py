"""Construct the bounded invoice workflows exposed by the example service."""
from __future__ import annotations

import hashlib
import json
from typing import Mapping

from .contracts import Outcome
from .demo import (CheckerAdapter, PlannerAdapter, ReviewerAdapter, _infer,
                   _invoice_input, DEFAULT_TOPOLOGY)
from .invoice_contract import exact_total, validate_total
from .provider_config import create_providers, load_provider_configs
from .provider_contract import provider_binding


class _Ingress:
    def handle(self, context, request):
        raise ValueError("ingress actor receives only correlated results")


class _Proposal:
    def __init__(self, provider):
        self.provider = provider

    def handle(self, context, request):
        invoice = _invoice_input(request, "invoice.propose", {"invoice"})
        return Outcome(_infer(context, self.provider, invoice))


class _Hub:
    def handle(self, context, request):
        invoice = _invoice_input(request, "invoice.plan", {"invoice"})
        values = [validate_total(context.request(role, "invoice.propose", {
            "invoice": invoice}).output)["total_cents"]
            for role in ("planner", "reviewer")]
        total = exact_total(invoice)
        verified = values == [total, total]
        return Outcome({"total_cents": total, "planner_total_cents": values[0],
                        "reviewer_total_cents": values[1], "verified": verified},
                       "completed" if verified else "unresolved")


def invoice_workflows(*, mode: str = "mock", environ: Mapping[str, str] | None = None):
    """Bind providers once; no provider call occurs during service construction."""
    from .task_service import Workflow
    providers = create_providers(load_provider_configs(mode, environ=environ), environ=environ)
    descriptors = {role: provider_binding(provider)[0] for role, provider in providers.items()}
    identity = hashlib.sha256(json.dumps(descriptors, sort_keys=True,
                                         separators=(",", ":")).encode()).hexdigest()
    mesh = dict(DEFAULT_TOPOLOGY)
    tree = {**mesh, "name": "bridge-tree", "agents": ["client", "planner", "reviewer", "checker"],
            "edges": [[a, b] for x, y in [("client", "planner"), ("planner", "reviewer"),
                                         ("reviewer", "checker")] for a, b in [(x, y), (y, x)]]}
    star = {**mesh, "name": "bridge-star", "agents": ["client", "hub", "planner", "reviewer"],
            "edges": [[a, b] for leaf in ("client", "planner", "reviewer")
                      for a, b in [("hub", leaf), (leaf, "hub")]]}
    common = {"planner": PlannerAdapter(providers["planner"]),
              "reviewer": ReviewerAdapter(providers["reviewer"]), "checker": CheckerAdapter()}
    definitions = {
        "invoice_mesh": (mesh, common, "checker", "planner"),
        "invoice_tree": (tree, {**common, "client": _Ingress()}, "client", "planner"),
        "invoice_star": (star, {"client": _Ingress(), "hub": _Hub(),
                                **{role: _Proposal(provider) for role, provider in providers.items()}},
                         "client", "hub"),
    }
    return {name: Workflow(version=name + "/1:" + identity, topology=topology, adapters=adapters,
                           sender=sender, recipient=recipient, operation="invoice.plan")
            for name, (topology, adapters, sender, recipient) in definitions.items()}
