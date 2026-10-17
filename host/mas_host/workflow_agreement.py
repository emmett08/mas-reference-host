"""Exact candidate and context agreement for opt-in workflow quorum joins."""
from __future__ import annotations

import re

from .codec import canonical_json, is_actor_identity, json_identity
from .service_contracts import ServiceError


AGREEMENT_VERSION = "mas-candidate-quorum/1"


def candidate_reference(value):
    return (type(value) is dict and set(value) == {"id", "sha256"}
            and is_actor_identity(value["id"]) and type(value["sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", value["sha256"]) is not None)


def agreement_binding(policy, input, minimum):
    """Resolve expected values before any branch allocation or dispatch."""
    from .workflow_plan import select_value
    candidate = select_value(policy["candidate"], input)
    context = select_value(policy["context"], input)
    if not candidate_reference(candidate) or type(context) is not dict or not context:
        raise ServiceError("invalid_workflow_agreement_binding")
    binding = {"version": AGREEMENT_VERSION, "minimum": minimum,
               "candidate": candidate, "context": context}
    return {**binding, "binding_sha256": json_identity(binding)}


def agrees(output, binding):
    """Invalid, stale or foreign votes never contribute to agreement."""
    return (type(output) is dict and output.get("accepted") is True
            and candidate_reference(output.get("candidate"))
            and type(output.get("context")) is dict
            and canonical_json(output["candidate"]) == canonical_json(binding["candidate"])
            and canonical_json(output["context"]) == canonical_json(binding["context"]))
