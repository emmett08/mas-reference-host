"""Load trusted deployment configuration and compose the shared task service."""
from __future__ import annotations

from dataclasses import replace
import importlib
import json
import os
from pathlib import Path
import re

from .codec import integer_json
from .service_contracts import ServiceError


def _pairs(items):
    value = {}
    for key, item in items:
        if key in value:
            raise ServiceError("duplicate_field")
        value[key] = item
    return value


def load_config(path: Path) -> dict:
    try:
        with Path(path).open(encoding="utf-8") as stream:
            raw = stream.read(65537)
        value = json.loads(raw, object_pairs_hook=_pairs)
        integer_json(value)
        if (type(value) is not dict or type(value.get("version")) is not int or value.get("version") != 1
                or set(value) - {"version", "mode", "factory", "grants", "auth", "public_url",
                                 "allowed_hosts", "allowed_origins", "max_pending", "max_tasks", "max_instances", "scheduler", "artifacts", "principal_quotas", "operators", "shared"}
                or type(value.get("grants")) is not dict):
            raise ServiceError("invalid_configuration")
        for principal, workflows in value["grants"].items():
            if (not principal or type(workflows) is not list or not workflows
                    or any(type(name) is not str or not name for name in workflows)):
                raise ServiceError("invalid_grants")
        if "operators" in value:
            from .operator_service import operator_grants
            operator_grants(value["operators"])
        return value
    except (OSError, UnicodeError, ValueError):
        raise ServiceError("invalid_configuration") from None


def build_service(config: dict, state_dir: Path):
    from .task_service import DurableTaskService
    from .bridge_workflows import invoice_workflows
    from .scheduler_capacity import SchedulerPolicy
    from .operations import DeploymentLedger
    try:
        scheduler_policy = SchedulerPolicy(**config['scheduler']) if 'scheduler' in config else None
    except (TypeError, ValueError):
        raise ServiceError('invalid_scheduler_policy') from None
    shared = config.get("shared")
    if "factory" in config:
        target = config["factory"]
        if type(target) is not str or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", target):
            raise ServiceError("invalid_factory")
        module, name = target.split(":")
        # This is local operator configuration, never a remotely supplied name.
        factory = getattr(importlib.import_module(module), name)
        workflows = factory()
        def pinned(workflow):
            return replace(workflow, implementation_sources=(*workflow.implementation_sources, factory))
        if (type(workflows) is dict and 'plans' in workflows and 'workflows' in workflows
                and set(workflows) <= {'plans', 'workflows', 'grants', 'workflow_versions'}):
            if shared is not None:
                return _build_shared_service(config, state_dir,
                    {name: pinned(item) for name, item in workflows['workflows'].items()},
                    plans=workflows['plans'], workflow_versions=workflows.get('workflow_versions'))
            from .unified_service import UnifiedTaskService
            return UnifiedTaskService(state_dir, {name: pinned(item) for name, item in workflows['workflows'].items()}, workflows['plans'],
                {principal: set(names) for principal, names in config['grants'].items()},
                operations=DeploymentLedger(state_dir, principal_quotas=config.get('principal_quotas')),
                workflow_versions=workflows.get('workflow_versions'),
                max_pending=config.get('max_pending', 1000), max_tasks=config.get('max_tasks', 10000),
                max_instances=config.get('max_instances', 1000), scheduler_policy=scheduler_policy,
                mas_bin=os.environ.get('MAS_BIN', 'mas'), gateway_bin=os.environ.get('MAS_GATEWAY_BIN', 'mas-gateway'))
        workflows = {name: pinned(item) for name, item in workflows.items()}
    else:
        workflows = invoice_workflows(mode=config.get("mode", "mock"))
    if shared is not None:
        return _build_shared_service(config, state_dir, workflows)
    return DurableTaskService(state_dir, workflows,
        {principal: set(names) for principal, names in config["grants"].items()},
        operations=DeploymentLedger(state_dir, principal_quotas=config.get('principal_quotas')),
        max_pending=config.get("max_pending", 1000), max_tasks=config.get("max_tasks", 10000),
        scheduler_policy=scheduler_policy,
        mas_bin=os.environ.get("MAS_BIN", "mas"), gateway_bin=os.environ.get("MAS_GATEWAY_BIN", "mas-gateway"))


def build_verifier(config: dict):
    from .bridges.authentication import StaticTokenVerifier, OAuthIntrospectionVerifier
    auth = config.get("auth", {})
    if type(auth) is not dict:
        raise ServiceError("invalid_auth_configuration")
    if auth.get("type") == "static" and set(auth) == {"type", "file"}:
        return StaticTokenVerifier.from_file(Path(auth["file"]))
    required = {"type", "issuer", "resource", "introspection_url", "client_id", "client_secret_file"}
    if auth.get("type") == "oauth" and required <= set(auth) <= required | {'transport'}:
        options = {key: value for key, value in auth.items() if key not in {'type', 'transport'}}
        if 'transport' in auth:
            from .bridges.http_policy import HTTPTransportPolicy
            options['transport_policy'] = HTTPTransportPolicy.from_config(auth['transport'])
        return OAuthIntrospectionVerifier(**options)
    raise ServiceError("invalid_auth_configuration")


def build_artifact_transfer(config: dict, state_dir: Path, *, authority=None):
    """Build only fixed operator-selected repositories; request bodies contain no paths."""
    if 'artifacts' not in config:
        return None
    from .artifacts import ArtifactPolicy
    from .bridges.artifact_transfer import ArtifactScope, ArtifactTransferService
    value = config['artifacts']
    if (type(value) is not dict or 'scopes' not in value
            or set(value) - {'scopes', 'ttl_seconds', 'max_sessions', 'max_staging_bytes'}
            or type(value['scopes']) is not dict):
        raise ServiceError('invalid_artifact_configuration')
    scopes = {}
    for name, item in value['scopes'].items():
        if (type(item) is not dict or not {'namespace', 'principals'} <= set(item)
                or set(item) - {'namespace', 'principals', 'path', 'policy', 'content_backend'}):
            raise ServiceError('invalid_artifact_configuration')
        relative = item.get('path', 'shared-artifacts/' + name)
        if (type(relative) is not str or not relative or Path(relative).is_absolute()
                or any(part in {'', '.', '..'} for part in relative.split('/'))
                or relative.split('/')[0] == 'artifact-transfers'):
            raise ServiceError('invalid_artifact_configuration')
        policy = item.get('policy', {})
        if type(policy) is not dict or set(policy) - set(ArtifactPolicy.__dataclass_fields__):
            raise ServiceError('invalid_artifact_configuration')
        backend = None
        if 'content_backend' in item:
            from .s3_artifacts import S3ArtifactContent
            from .s3_transport import S3ContentConfig
            from .artifacts import ArtifactError
            selected = item['content_backend']
            if type(selected) is not dict or selected.get('type') != 's3':
                raise ServiceError('invalid_artifact_configuration')
            try:
                backend = S3ArtifactContent(S3ContentConfig(**{key: value for key, value in selected.items() if key != 'type'}))
            except (ArtifactError, TypeError):
                raise ServiceError('invalid_artifact_configuration') from None
        if 'shared' in config:
            from .shared_artifact_transfer import SharedArtifactScope
            if authority is None:
                raise ServiceError('shared_authority_required')
            scopes[name] = SharedArtifactScope(authority, Path(state_dir) / relative,
                item['namespace'], item['principals'], ArtifactPolicy(**policy), backend)
        else:
            scopes[name] = ArtifactScope(Path(state_dir) / relative, item['namespace'],
                                        item['principals'], ArtifactPolicy(**policy), content_backend=backend)
    options = {key: item for key, item in value.items() if key != 'scopes'}
    if 'shared' in config:
        from .shared_artifact_transfer import SharedArtifactTransferService
        return SharedArtifactTransferService(Path(state_dir) / 'artifact-transfers', scopes, **options)
    return ArtifactTransferService(Path(state_dir) / 'artifact-transfers', scopes, **options)


def build_shared_authority(config):
    """Bind the configured read/command port without provisioning or renewing it."""
    from .bridges.authentication import read_private_file, AuthenticationError
    from .shared_authority import SharedAuthority
    value = config['shared']
    required = {'binary', 'dsn_file', 'namespace', 'deployment', 'controller', 'epoch',
                'secret_key_file', 'secret_reference', 'allocation_limits'}
    if type(value) is not dict or set(value) != required:
        raise ServiceError('invalid_shared_configuration')
    try:
        dsn = read_private_file(value['dsn_file'], max_bytes=8192).decode('utf-8').rstrip('\n')
        key = read_private_file(value['secret_key_file'], max_bytes=8192)
        authority = SharedAuthority(binary=value['binary'], dsn=dsn, namespace=value['namespace'],
            deployment=value['deployment'], controller=value['controller'], epoch=value['epoch'],
            secret_key=key, secret_reference=value['secret_reference'])
    except (AuthenticationError, UnicodeError, TypeError, ValueError):
        raise ServiceError('invalid_shared_configuration') from None
    return authority


def _build_shared_service(config, state_dir, workflows, *, plans=None, workflow_versions=None):
    from .shared_service import create_shared_service
    from .scheduler_capacity import SchedulerPolicy
    authority = build_shared_authority(config)
    value = config['shared']
    return create_shared_service(authority, state_dir, workflows,
        {principal: set(names) for principal, names in config['grants'].items()}, plans=plans,
        workflow_versions=workflow_versions, allocation_limits=value['allocation_limits'],
        max_pending=config.get('max_pending', 1000), max_tasks=config.get('max_tasks', 10000),
        scheduler_policy=SchedulerPolicy(**config.get('scheduler', {})),
        principal_quotas=config.get('principal_quotas'), max_instances=config.get('max_instances', 1000),
        gateway_bin=os.environ.get('MAS_GATEWAY_BIN', 'mas-gateway'))
