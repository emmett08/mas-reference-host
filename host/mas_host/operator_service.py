"""Authenticated operator operations with explicit owner and deployment grants.

Task operations accept logical identities. A separate peer-management grant
permits explicit peer registrations. Filesystem locations, executable factories
and provider credentials remain trusted deployment configuration.
"""
from __future__ import annotations

from .codec import JsonSizeError, integer_json
from .contracts import RunnerError
from .deadlines import DeadlineService
from .operations import OperationsError, archive_workflows, prune_runs
from .recovery import RecoveryService, _identity
from .service_contracts import ServiceError

from .operator_contracts import OPERATIONS, OWNER_OPERATIONS

def operator_grants(value):
    if type(value) is not dict or len(value) > 1024:
        raise ServiceError("invalid_operator_grants")
    result = {}
    for actor, grant in value.items():
        _identity(actor)
        if (type(grant) is not dict or not {"owners", "deployment_admin"} <= set(grant)
                or set(grant) - {"owners", "deployment_admin", "manage_peers"}
                or type(grant["owners"]) is not list or len(grant["owners"]) > 1024
                or type(grant["deployment_admin"]) is not bool or type(grant.get("manage_peers", False)) is not bool):
            raise ServiceError("invalid_operator_grants")
        owners = [_identity(owner) for owner in grant["owners"]]
        if len(set(owners)) != len(owners):
            raise ServiceError("invalid_operator_grants")
        result[actor] = {"owners": frozenset(owners), "deployment_admin": grant["deployment_admin"], "manage_peers": grant.get("manage_peers", False)}
    return result


def page(items, offset=0, limit=20):
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 20:
        raise ServiceError("invalid_operator_page")
    selected = []
    for item in items[offset:offset + limit]:
        try:
            integer_json([*selected, item], 24000)
        except JsonSizeError:
            if not selected:
                raise ServiceError("operator_item_too_large") from None
            break
        selected.append(item)
    return {"items": selected, "next_offset": offset + len(selected) if offset + len(selected) < len(items) else None,
            "total": len(items)}


class OperatorService:
    def __init__(self, service, *, operators, reconciliation_queries=None):
        self.service = service
        self.grants = operator_grants(operators)
        owners = {actor: grant["owners"] for actor, grant in self.grants.items()}
        if hasattr(service, "authority"):
            self.recovery = service.recovery_service(operators=owners, queries=reconciliation_queries)
            self.deadlines = service.deadline_service(operators=owners)
        else:
            self.recovery = RecoveryService(service, operators=owners, queries=reconciliation_queries)
            self.deadlines = DeadlineService(service, operators=owners)
        from .worker_control import WorkerControl
        if hasattr(service, "authority"):
            from .shared_control import SharedWorkerControl
            self.workers = SharedWorkerControl(service.authority)
        else:
            self.workers = WorkerControl(service.operations)

    def dispatch(self, operator, operation, payload):
        grant = self.grants.get(operator)
        if grant is None:
            raise ServiceError("forbidden")
        contract = OPERATIONS.get(operation)
        if (contract is None or type(payload) is not dict or not contract[0] <= set(payload)
                or set(payload) - contract[0] - contract[1]):
            raise ServiceError("invalid_operator_request")
        integer_json(payload)
        if operation in {'archive', 'artifact_links', 'artifact_bind', 'artifact_release'} and (not grant['deployment_admin']
                or type(payload['principal']) is not str or payload['principal'] not in grant['owners']):
            raise ServiceError('forbidden')
        if operation in OWNER_OPERATIONS:
            if type(payload["principal"]) is not str or payload["principal"] not in grant["owners"]:
                raise ServiceError("forbidden")
        elif operation in {"peer_register", "peer_revoke"}:
            if not grant["manage_peers"]:
                raise ServiceError("forbidden")
        elif operation == "peer_status":
            if not (grant["deployment_admin"] or grant["manage_peers"]):
                raise ServiceError("forbidden")
        elif operation != "capabilities" and not grant["deployment_admin"]:
            raise ServiceError("forbidden")
        try:
            return self._perform(operator, operation, payload)
        except (OperationsError, RunnerError):
            # Local exception strings can contain storage locations. Never send
            # arbitrary diagnostics through the authenticated network boundary.
            raise ServiceError("operator_action_unavailable") from None

    def _perform(self, operator, operation, payload):
        service = self.service
        if operation == "capabilities":
            grant = self.grants[operator]
            allowed = set(OPERATIONS) if grant["deployment_admin"] else OWNER_OPERATIONS | {"capabilities"}
            if grant["manage_peers"]:
                allowed |= {"peer_status", "peer_register", "peer_revoke"}
            else:
                allowed -= {"peer_register", "peer_revoke"}
            return {"format": "mas-operator/1", "operator": operator, "owners": sorted(grant["owners"]),
                    "deployment_admin": grant["deployment_admin"], "manage_peers": grant["manage_peers"], "operations": sorted(allowed)}
        if operation == "registrations":
            return service.discover(payload["principal"])
        if operation in {"request_epoch_status", "request_epoch_retire"}:
            name = "retire_request_epoch" if operation == "request_epoch_retire" else operation
            callback = getattr(service, name, None)
            if callback is None:
                raise ServiceError("unsupported_capability")
            return callback(**payload, **({"authoriser": operator} if operation == "request_epoch_retire" else {}))
        if operation == "tasks":
            return service.list_task_page(**payload)
        if operation == "task":
            if "path" in payload:
                return service.inspect_task(**payload)
            if "revision" in payload:
                raise ServiceError("invalid_operator_request")
            return service.inspect(**payload)
        if operation == "events":
            return service.events_page(**payload)
        if operation == "graph":
            return service.inspect_graph(**payload)
        if operation == 'archive':
            from .retention_admin import archive
            return archive(service, operator, payload)
        if operation.startswith('artifact_'):
            if not hasattr(service, 'authority'):
                raise ServiceError('unsupported_capability')
            name = operation.removeprefix('artifact_')
            if name in {'namespaces', 'links'}:
                return service.authority.query('artifact.' + name, {'offset': 0, 'limit': 20, **payload})
            command = {**payload, 'operator': operator}
            if name == 'bind':
                service.inspect(payload['principal'], payload['task_id'])
                task = command.pop('task_id')
                command['retention_target'] = {'kind': 'workflow' if task.startswith('flow-') else 'task',
                    'id': task, 'principal': payload['principal']}
            return service.authority.command('artifact.' + name, command)['receipt']
        if operation in {'command_capacity', 'command_capacity_amend'}:
            if not hasattr(service, 'authority'):
                raise ServiceError('unsupported_capability')
            if operation == 'command_capacity':
                return service.authority.query('command_capacity.status', {})
            return service.authority.command('command_capacity.amend', {**payload, 'operator': operator})['receipt']
        if operation == "recovery_inbox":
            inbox = self.recovery.inbox(operator, payload["principal"], payload["task_id"])
            offset, limit = payload.get("offset", 0), payload.get("limit", 20)
            return {**inbox, "entries": page(inbox["entries"], offset, limit),
                    "remote_observations": page(inbox["remote_observations"], offset, limit)}
        if operation in {"recovery_query", "recovery_resolve"}:
            return getattr(self.recovery, operation.removeprefix("recovery_"))(operator, **payload)
        if operation == "deadline_extend":
            return self.deadlines.extend(operator, **payload)
        if hasattr(service, "authority") and operation in {"budget", "storage", "archive_preview"}:
            return service.authority.query("operator." + operation, payload)
        if hasattr(service, "authority") and operation == "budget_amend":
            return service.authority.command("operator.budget_amend", {"principal": operator, **payload})["receipt"]
        if operation == "budget":
            history = service.operations.budget_history()
            return {"health": service.operations.health(), "revision": history[-1]["revision"] if history else 0,
                    "amendments": page(history, **payload)}
        if operation == "budget_amend":
            return service.operations.amend_budget(principal=operator, **payload)
        if operation in {"periods", "period_configure", "charge_record", "charge_reconcile"}:
            from .accounting import AccountingAdministration, PeriodPolicy
            if hasattr(service, "authority"):
                from .shared_accounting import SharedAccountingAdministration
                accounting = SharedAccountingAdministration(service.authority)
            else:
                accounting = AccountingAdministration(service.operations)
            if operation == "periods":
                return page(accounting.periods(), **payload)
            if operation == "period_configure":
                if type(payload["policy"]) is not dict:
                    raise ServiceError("invalid_accounting_policy")
                try:
                    policy = PeriodPolicy(**payload["policy"])
                except TypeError:
                    raise ServiceError("invalid_accounting_policy") from None
                return accounting.configure_period(payload["scope"], policy, request_key=payload["request_key"],
                                                   actor=operator, reason=payload["reason"])
            if operation == "charge_record":
                return accounting.record_charge(**payload, actor=operator)
            result = accounting.reconcile(payload["run_id"], payload["call_id"])
            return {**result, "observations": page(result["observations"], payload.get("offset", 0), payload.get("limit", 20))}
        if operation == "storage":
            return service.operations.health()
        if operation == "telemetry":
            from .telemetry import snapshot
            root = getattr(getattr(service, 'authority', None), 'observation_dir', service.state_dir)
            return snapshot(root)
        if operation == "archive_preview":
            from .task_history import archive_tasks
            functions = {"tasks": (archive_tasks, "tasks"), "workflows": (archive_workflows, "instances"),
                         "runs": (prune_runs, "runs")}
            if type(payload["kind"]) is not str or payload["kind"] not in functions:
                raise ServiceError("invalid_archive_kind")
            callback, key = functions[payload["kind"]]
            result = callback(service.state_dir, payload["older_than_seconds"], apply=False)
            return {"kind": payload["kind"], "dry_run": True,
                    **page(result[key], payload.get("offset", 0), payload.get("limit", 20))}
        if operation in {"definition", "definition_retire"}:
            kind, name, identity = payload["kind"], payload["name"], payload["identity"]
            if type(kind) is not str or kind not in {"workflow", "plan"}:
                raise ServiceError("invalid_definition_kind")
            _identity(name); _identity(identity)
            target = (service.leaves if kind == "workflow" else service.compositions) if hasattr(service, "leaves") else service
            if kind == "plan" and not hasattr(target, "bindings"):
                raise ServiceError("unsupported_capability")
            if operation == "definition_retire":
                target.retire_definition(name, identity)
            return target.definition_status(name, identity)
        if operation in {"peer_status", "peer_register", "peer_revoke"}:
            from .peer_registry import PeerRegistry
            if hasattr(service, "authority"):
                from .shared_remote import SharedPeerRegistry
                registry = SharedPeerRegistry(service.authority)
            else:
                registry = PeerRegistry(service.operations)
            if operation == "peer_status":
                status = registry.status()
                return {**status, "peers": page(status["peers"], **payload)}
            return getattr(registry, operation.removeprefix("peer_"))(operator=operator, **payload)
        if operation == "worker_status":
            return self.workers.status()
        if operation == "worker_drain":
            return self.workers.set(operator=operator, **payload)
        raise ServiceError("invalid_operator_request")
