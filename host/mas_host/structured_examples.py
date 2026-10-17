"""Runnable structured inference, acceptance and artefact example.

Two independent operations classify a greeting and square an integer. Explicit
offline fixtures suit the documented input {"text":"hello","value":7}; different
inputs can produce a rejected decision while execution still completes. Set
MAS_STRUCTURED_CONFIG_FILE to an explicit actor configuration JSON file to select
live providers. Credentials use the existing provider-specific secret settings.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .acceptance_policies import AssessmentSubject, DeterministicPolicy
from .agent_config import create_agent_adapters, load_agent_configs
from .artifacts import ArtifactStore
from .codec import integer_json
from .contracts import Invocation, Outcome, RunnerError
from .operation_contracts import JsonObjectSchema, OperationContract, OperationRegistry, RegisteredOperation
from .task_service import Workflow
from .workflow_plan import WorkflowPlan


def _object(properties):
    return {"type": "object", "properties": properties, "required": sorted(properties),
            "additionalProperties": False}


_TEXT_INPUT = _object({"text": {"type": "string", "maxLength": 4096}})
_NUMBER_INPUT = _object({"value": {"type": "integer", "minimum": -1000, "maximum": 1000}})
_INPUT = _object({**_TEXT_INPUT["properties"], **_NUMBER_INPUT["properties"]})
_CONTRACTS = {
    "greeting/1": OperationContract("greeting", "1", _TEXT_INPUT,
        _object({"label": {"type": "string", "enum": ["greeting", "other"]}}), {"coordinator"}),
    "square/1": OperationContract("square", "1", _NUMBER_INPUT,
        _object({"square": {"type": "integer", "minimum": 0, "maximum": 1_000_000}}), {"coordinator"}),
}
_FIXTURES = {"language-agent": {"label": "greeting"}, "number-agent": {"square": 49}}
_SETTINGS = {
    "language-agent": {"provider": "mock", "model": "greeting-fixture-v1", "reservation_microusd": 0,
        "instructions": "Return greeting if the input text, stripped and case-folded, is hello or hi; otherwise other.",
        "operation": "greeting/1", "role": "language-classifier"},
    "number-agent": {"provider": "mock", "model": "square-fixture-v1", "reservation_microusd": 0,
        "instructions": "Return the exact integer square of the input value.",
        "operation": "square/1", "role": "integer-calculator"},
}


def _greeting(input, output):
    return output["label"] == ("greeting" if input["text"].strip().casefold() in {"hello", "hi"} else "other")


def _square(input, output):
    return output["square"] == input["value"] ** 2


class _Ingress:
    def handle(self, context, request):
        raise RunnerError("example ingress receives only replies")


class _Assess:
    def __init__(self, actor, contract, policy, binding=None):
        self.actor, self.contract, self.policy, self.binding = actor, contract, policy, binding

    def handle(self, context, request):
        if self.binding is None:
            raise RunnerError("example requires an execution binding")
        supplied = JsonObjectSchema(_INPUT).validate(request.input)
        selected = {key: supplied[key] for key in self.contract.input_schema["properties"]}
        result = context.request(self.actor, self.contract.operation, selected)
        subject = AssessmentSubject(selected, result.output)
        decision = self.policy.evaluate(subject)
        record = {"input": selected, "result": result.output, "decision": decision.descriptor}
        source = integer_json(record).encode("utf-8")
        owner = "owner-" + hashlib.sha256(self.binding.principal.encode()).hexdigest()

        def publish():
            store = ArtifactStore(self.binding.run_dir / "artifacts", namespace="evidence", principal="coordinator")
            reference = store.put_bytes(source, media_type="application/json", readers=(owner,),
                                         input_sha256s=(subject.descriptor["input_identity"],))
            store.link("assessment-" + subject.identity, [reference],
                       state="unresolved" if decision.status == "unresolved" else "live")
            return Invocation({"artifact": reference.to_dict()}, {"repository": "local-artifact/1",
                               "sha256": reference.sha256, "subject_identity": subject.identity})

        publication = context.invoke(publish, reservation_microusd=0,
            input={"operation": "publish-assessment/1", "subject": subject.descriptor,
                   "decision": decision.descriptor, "record_sha256": hashlib.sha256(source).hexdigest(),
                   "owner": owner, "namespace": "evidence"})
        # Completion means the requested assessment was produced. A rejected
        # result remains explicitly rejected in the separate decision record.
        return Outcome({"result": result.output, "decision": decision.descriptor,
                        "artifact": publication["artifact"]})


def _settings(environ):
    path = environ.get("MAS_STRUCTURED_CONFIG_FILE")
    if not path:
        return json.loads(json.dumps(_SETTINGS))
    try:
        with Path(path).open("rb") as stream:
            data = stream.read(65_537)
        if len(data) > 65_536:
            raise ValueError("size")
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate")
                result[key] = value
            return result
        return json.loads(data, object_pairs_hook=unique)
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
        raise RunnerError("invalid structured example configuration file") from None


def example_registry(*, settings=None, environ=None):
    """Trusted CLI factory binding configuration without credentials or SDK clients."""
    environment = os.environ if environ is None else environ
    configs = load_agent_configs(_settings(environment) if settings is None else settings, contracts=_CONTRACTS)
    if set(configs) != set(_SETTINGS) or any(
            config.contract.operation != _SETTINGS[actor]["operation"] for actor, config in configs.items()):
        raise RunnerError("example requires its two registered operation actors")
    models = create_agent_adapters(configs, registered_actors=configs,
        mock_outputs={actor: _FIXTURES[actor] for actor in configs if configs[actor].provider == "mock"},
        environ=environment)
    workflows = {}
    for actor, model in models.items():
        contract = configs[actor].contract
        policy = DeterministicPolicy("exact-" + contract.name, "1", _greeting if contract.name == "greeting" else _square)
        assessment_contract = OperationContract("assess", "1", _INPUT,
            _object({"result": contract.output_schema, "decision": {"type": "object"},
                     "artifact": {"type": "object"}}), {"client"})

        def build(binding=None, *, actor=actor, model=model, contract=contract,
                  policy=policy, assessment_contract=assessment_contract):
            return {"client": _Ingress(), actor: OperationRegistry(actor, [RegisteredOperation(contract, model, "1")]),
                    "coordinator": OperationRegistry("coordinator", [RegisteredOperation(assessment_contract,
                        _Assess(actor, contract, policy, binding), "1")])}

        topology = {"agents": ["client", "coordinator", actor],
            "edges": [["client", "coordinator"], ["coordinator", "client"],
                      ["coordinator", actor], [actor, "coordinator"]],
            "application": "agent-task/1", "max_messages": 20, "max_bytes": 131072,
            "max_calls": 2, "wall_seconds": 120, "mailbox_capacity": 8}
        workflows[actor] = Workflow("structured-examples/1", topology, build(),
            "client", "coordinator", "assess/1", adapter_factory=build,
            contract_descriptor={"agent": model.descriptor(), "assessment": assessment_contract.descriptor,
                                 "acceptance": policy.descriptor, "publication": "local-artifact/1"})
    plan = WorkflowPlan({"format": "mas-workflow/1", "name": "structured-evidence", "version": "1",
        "limits": {"max_steps": 3, "max_parallel": 2, "wall_seconds": 180, "max_calls": 4,
                   "max_reserved_microusd": sum(config.reservation_microusd for config in configs.values())},
        "root": {"kind": "parallel", "branches": {actor: {"kind": "task", "workflow": actor} for actor in configs}}})
    return {"plans": {"structured-evidence": plan}, "workflows": workflows,
            "grants": {"demo": {"structured-evidence"}}}
