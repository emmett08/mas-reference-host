"""Pinned workspaces for trusted processes and explicitly isolated containers.

Filesystem checks protect host-managed materialisation, publication and cleanup.
Trusted processes are supervised by Rust; the optional container strategy adds
namespace/resource isolation for network-free untrusted application code.
Authentication belongs to the caller that binds a manager principal.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from types import MappingProxyType
import uuid

from .artifacts import ArtifactError, ArtifactRef, ArtifactRepository
from .codec import canonical_json, integer_json, is_actor_identity
from .contracts import Invocation, Outcome, RunnerError
from .executable_worker import ExecutableAdapter, RegisteredProcessExecutor, WorkerSpec
from .operations import DeploymentLedger, OperationsError, _file_lock


class WorkspaceError(RunnerError):
    """Fixed diagnostics containing no workspace content or credentials."""


def _identity(value):
    if not is_actor_identity(value):
        raise WorkspaceError("invalid workspace identity")
    return value


def _parts(value):
    if (type(value) is not str or len(value) > 512 or "\\" in value
            or value.startswith("/") or not 1 <= len(value.split("/")) <= 8
            or any(part in {"", ".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", part)
                   for part in value.split("/"))):
        raise WorkspaceError("invalid workspace relative path")
    return value.split("/")


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _open_directory(path):
    """Create a private managed root without following any symlink component."""
    absolute = Path(path).absolute()
    current = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in absolute.parts[1:]:
            if part in {"", ".", ".."}:
                raise WorkspaceError("unsafe workspace root")
            try:
                os.mkdir(part, 0o700, dir_fd=current)
            except FileExistsError:
                pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            os.close(current)
            current = child
        info = os.fstat(current)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise WorkspaceError("workspace root must be private")
        return current
    except BaseException:
        os.close(current)
        raise


@contextmanager
def _directory(parent, parts, *, create=False):
    current = os.dup(parent)
    try:
        for part in parts:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=current)
                except FileExistsError:
                    pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            os.close(current)
            current = child
        yield current
    finally:
        os.close(current)


def _open_file(directory, name, flags=os.O_RDONLY):
    fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
        os.close(fd)
        raise WorkspaceError("workspace files must be private regular files")
    return fd


def _read(directory, path, limit):
    parts = _parts(path)
    with _directory(directory, parts[:-1]) as parent:
        with os.fdopen(_open_file(parent, parts[-1]), "rb") as stream:
            if os.fstat(stream.fileno()).st_size > limit:
                raise WorkspaceError("workspace byte limit exceeded")
            result = stream.read(limit + 1)
            if len(result) > limit:
                raise WorkspaceError("workspace byte limit exceeded")
            return result


def _write(directory, path, data, *, readonly=False):
    parts = _parts(path)
    with _directory(directory, parts[:-1], create=True) as parent:
        temporary = ".pending-" + uuid.uuid4().hex
        try:
            with os.fdopen(_open_file(parent, temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fchmod(stream.fileno(), 0o400 if readonly else 0o600)
                os.fsync(stream.fileno())
            os.replace(temporary, parts[-1], src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass


@dataclass(frozen=True)
class WorkspacePolicy:
    name: str
    version: str
    tools: tuple[str, ...]
    profile: str = "trusted-code-execution"
    max_input_files: int = 32
    max_input_bytes: int = 16 * 1024 * 1024
    max_output_files: int = 16
    max_output_bytes: int = 16 * 1024 * 1024
    max_workspace_files: int = 256
    max_workspace_bytes: int = 64 * 1024 * 1024
    allow_external_effects: bool = False

    def __post_init__(self):
        _identity(self.name)
        _identity(self.version)
        if (type(self.profile) is not str
                or self.profile not in {"trusted-code-execution", "trusted-repository-analysis", "isolated-container-execution"}
                or type(self.tools) is not tuple or not 1 <= len(self.tools) <= 32
                or any(not is_actor_identity(name) for name in self.tools)
                or len(set(self.tools)) != len(self.tools) or type(self.allow_external_effects) is not bool):
            raise WorkspaceError("unsupported workspace trust profile or tool policy")
        if self.profile == 'isolated-container-execution' and self.allow_external_effects:
            raise WorkspaceError('isolated container tools cannot enable external effects')
        for name, maximum in (("max_input_files", 32), ("max_output_files", 32),
                              ("max_workspace_files", 4096), ("max_input_bytes", 64*1024*1024),
                              ("max_output_bytes", 64*1024*1024), ("max_workspace_bytes", 256*1024*1024)):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise WorkspaceError("invalid workspace resource bound")
        if self.max_input_bytes > self.max_workspace_bytes:
            raise WorkspaceError("workspace input budget exceeds total budget")

    @property
    def descriptor(self):
        if self.profile == 'isolated-container-execution':
            return {**asdict(self), 'tools': list(self.tools), 'containment': 'linux-container-no-network',
                    'aggregate_storage_enforcement': 'bounded-tmpfs-and-checked-publication'}
        return {**asdict(self), "tools": list(self.tools), "containment": "trusted-process-no-sandbox",
                "aggregate_storage_enforcement": "preflight-and-postflight"}


@dataclass(frozen=True)
class WorkspaceInput:
    path: str
    reference: ArtifactRef

    def __post_init__(self):
        _parts(self.path)
        if not isinstance(self.reference, ArtifactRef):
            raise WorkspaceError("workspace input requires an artefact reference")


@dataclass(frozen=True)
class WorkspaceOutput:
    path: str
    media_type: str = "application/octet-stream"

    def __post_init__(self):
        _parts(self.path)
        if (type(self.media_type) is not str or len(self.media_type) > 128
                or re.fullmatch(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", self.media_type) is None):
            raise WorkspaceError("invalid output media type")


@dataclass(frozen=True, init=False)
class WorkspaceRef:
    _source: str

    def __init__(self, descriptor):
        try:
            if (type(descriptor) is not dict or set(descriptor) != {
                    "format", "principal", "run", "attempt", "policy", "inputs", "context"}
                    or descriptor["format"] != "mas-workspace/1"
                    or type(descriptor["policy"]) is not dict or type(descriptor["inputs"]) is not list):
                raise WorkspaceError("invalid workspace reference")
            for name in ("principal", "run", "attempt"):
                _identity(descriptor[name])
            fields = dict(descriptor["policy"])
            fields.pop("containment", None)
            fields.pop("aggregate_storage_enforcement", None)
            if type(fields.get("tools")) is not list:
                raise WorkspaceError("invalid workspace reference")
            fields["tools"] = tuple(fields["tools"])
            policy = WorkspacePolicy(**fields)
            if policy.descriptor != descriptor["policy"]:
                raise WorkspaceError("invalid workspace reference")
            paths, size = set(), 0
            for item in descriptor["inputs"]:
                if type(item) is not dict or set(item) != {"path", "reference"}:
                    raise WorkspaceError("invalid workspace reference")
                _parts(item["path"])
                if item["path"] in paths:
                    raise WorkspaceError("invalid workspace reference")
                paths.add(item["path"])
                size += ArtifactRef.from_dict(item["reference"]).size
            if descriptor["context"] is not None:
                size += ArtifactRef.from_dict(descriptor["context"]).size
            if (len(paths) + (descriptor["context"] is not None) > policy.max_input_files
                    or size > policy.max_input_bytes):
                raise WorkspaceError("invalid workspace reference")
            object.__setattr__(self, "_source", canonical_json(descriptor))
        except (TypeError, KeyError, ValueError, ArtifactError):
            raise WorkspaceError("invalid workspace reference") from None

    @property
    def descriptor(self):
        return json.loads(self._source)

    @property
    def identity(self):
        return _digest(self._source.encode())

    @property
    def workspace_id(self):
        value = self.descriptor
        return "ws-" + _digest(canonical_json({key: value[key]
            for key in ("principal", "run", "attempt")}).encode())


@dataclass(frozen=True)
class WorkspaceTool:
    name: str
    worker: WorkerSpec
    reservation_microusd: int = 0
    external_effects: bool = False

    def __post_init__(self):
        _identity(self.name)
        from .container_execution import ContainerSpec
        if (not isinstance(self.worker, (WorkerSpec, ContainerSpec)) or type(self.external_effects) is not bool
                or type(self.reservation_microusd) is not int
                or not 0 <= self.reservation_microusd <= 100_000_000
                or "workspace" in self.worker.descriptor["configuration"]):
            raise WorkspaceError("invalid trusted workspace tool")
        if isinstance(self.worker, ContainerSpec) and self.external_effects:
            raise WorkspaceError('isolated container tools cannot enable external effects')

    @property
    def descriptor(self):
        return {"name": self.name, "worker": self.worker.descriptor,
                "reservation_microusd": self.reservation_microusd,
                "external_effects": self.external_effects}


@dataclass(frozen=True, init=False)
class WorkspaceTools:
    _tools: object

    def __init__(self, tools):
        registered = {}
        for tool in tools:
            if not isinstance(tool, WorkspaceTool) or tool.name in registered or len(registered) >= 128:
                raise WorkspaceError("invalid or duplicate workspace tool registration")
            registered[tool.name] = tool
        object.__setattr__(self, "_tools", MappingProxyType(registered))

    def resolve(self, name, policy):
        if type(name) is not str or name not in policy.tools or name not in self._tools:
            raise WorkspaceError("workspace tool is not authorised")
        tool = self._tools[name]
        if tool.external_effects and not policy.allow_external_effects:
            raise WorkspaceError("workspace external effects are not authorised")
        return tool


class WorkspaceManager:
    """Principal-bound input/output management, distinct from worker containment."""

    def __init__(self, root: Path, *, principal: str, artifacts: ArtifactRepository):
        self.root, self.principal = Path(root).absolute(), _identity(principal)
        if not isinstance(artifacts, ArtifactRepository) or artifacts.principal != principal:
            raise WorkspaceError("workspace artefact repository principal does not match")
        self.artifacts = artifacts
        with self._mutation():
            try:
                fd = _open_directory(self.root)
            except OSError:
                raise WorkspaceError("workspace storage boundary rejected access") from None
            try:
                for name in ("locks", "retired"):
                    with _directory(fd, [name], create=True):
                        pass
            finally:
                os.close(fd)

    @contextmanager
    def _mutation(self):
        """Workspace mutations share the deployment backup/admission barrier."""
        parents = (self.root, *self.root.parents)

        def writable():
            if any((parent / ".restored-audit-only").exists()
                   or (parent / ".restored-audit-only").is_symlink() for parent in parents):
                raise WorkspaceError("restored workspaces are audit-only")
            if deployment is not None and not DeploymentLedger(deployment, readonly=True).admission_enabled:
                raise WorkspaceError("restored workspaces are audit-only")

        deployment = next((parent for parent in parents if (parent / "deployment.sqlite").exists()), None)
        try:
            writable()
            barrier = _file_lock(deployment / ".deployment.lock", False) if deployment else nullcontext()
            with barrier:
                writable()
                yield
        except OperationsError:
            raise WorkspaceError("workspace deployment is unavailable") from None

    def _authorise(self, reference):
        if not isinstance(reference, WorkspaceRef) or reference.descriptor.get("principal") != self.principal:
            raise WorkspaceError("workspace is not owned by this principal")

    @contextmanager
    def lease(self, reference, *, create=False):
        with self._mutation():
            with self._lease(reference, create=create) as leased:
                yield leased

    @contextmanager
    def _lease(self, reference, *, create=False):
        self._authorise(reference)
        root = _open_directory(self.root)
        lock = None
        try:
            with _directory(root, ["locks"]) as locks:
                lock = _open_file(locks, reference.workspace_id, os.O_RDWR | os.O_CREAT)
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise WorkspaceError("workspace is busy") from None
            if create:
                try:
                    _read(root, "retired/" + reference.workspace_id, 256)
                except FileNotFoundError:
                    pass
                else:
                    raise WorkspaceError("retired workspace attempt cannot be recreated")
            with _directory(root, [reference.workspace_id], create=create) as directory:
                if not create:
                    self._verify_manifest(reference, directory)
                yield directory, lock
        except OSError:
            raise WorkspaceError("workspace storage boundary rejected access") from None
        finally:
            # Never explicitly unlock: a Rust guardian may hold the same open
            # file description until an orphaned worker's descendants are gone.
            if lock is not None:
                os.close(lock)
            os.close(root)

    @staticmethod
    def _verify_manifest(reference, directory):
        if _read(directory, "manifest.json", 65536) != reference._source.encode():
            raise WorkspaceError("workspace immutable manifest changed")

    def prepare(self, run: str, attempt: str, policy: WorkspacePolicy,
                inputs: tuple[WorkspaceInput, ...], *, context: ArtifactRef | None = None):
        with self._mutation():
            return self._prepare(run, attempt, policy, inputs, context=context)

    def _prepare(self, run, attempt, policy, inputs, *, context):
        _identity(run)
        _identity(attempt)
        if (not isinstance(policy, WorkspacePolicy) or type(inputs) is not tuple
                or len(inputs) > policy.max_input_files
                or any(not isinstance(item, WorkspaceInput) for item in inputs)
                or len({item.path for item in inputs}) != len(inputs)
                or context is not None and not isinstance(context, ArtifactRef)):
            raise WorkspaceError("invalid workspace input manifest")
        ordered = sorted(inputs, key=lambda item: item.path)
        refs = [item.reference for item in ordered] + ([context] if context is not None else [])
        if len(refs) > policy.max_input_files or sum(ref.size for ref in refs) > policy.max_input_bytes:
            raise WorkspaceError("workspace input quota exceeded")
        # Check every grant before creating the attempt or materialising a file.
        for ref in refs:
            self.artifacts.inspect(ref)
        reference = WorkspaceRef({"format": "mas-workspace/1", "principal": self.principal,
            "run": run, "attempt": attempt, "policy": policy.descriptor,
            "inputs": [{"path": item.path, "reference": item.reference.to_dict()} for item in ordered],
            "context": context.to_dict() if context else None})
        with self.lease(reference, create=True) as (directory, _):
            try:
                previous = _read(directory, "manifest.json", 65536)
            except FileNotFoundError:
                previous = None
            if previous is not None:
                self._verify_manifest(reference, directory)
                self._verify_inputs(reference, directory)
                return reference
            # The immutable manifest is the publication marker. Interrupted
            # preparation can be retried, copying the same verified snapshots.
            for name in ("inputs", "outputs", "work"):
                with _directory(directory, [name], create=True):
                    pass
            for item in ordered:
                _write(directory, "inputs/" + item.path, self.artifacts.read_bytes(item.reference), readonly=True)
            if context is not None:
                _write(directory, "context.json", self.artifacts.read_bytes(context), readonly=True)
            if refs:
                self.artifacts.link("workspace-input-" + reference.workspace_id[3:], refs)
            _write(directory, "state.json", canonical_json({"state": "prepared"}).encode())
            _write(directory, "manifest.json", reference._source.encode(), readonly=True)
        return reference

    def _verify_inputs(self, reference, directory):
        self._verify_manifest(reference, directory)
        manifest = reference.descriptor
        sources = [("inputs/" + item["path"], item["reference"]) for item in manifest["inputs"]]
        if manifest["context"] is not None:
            sources.append(("context.json", manifest["context"]))
        for path, raw in sources:
            ref = ArtifactRef.from_dict(raw)
            data = _read(directory, path, ref.size)
            if len(data) != ref.size or _digest(data) != ref.sha256:
                raise WorkspaceError("workspace input snapshot changed")
        expected = {item["path"] for item in manifest["inputs"]}
        found = set()

        def visit(parent, prefix="", depth=0):
            if depth > 8:
                raise WorkspaceError("workspace input snapshot changed")
            for name in os.listdir(parent):
                path = prefix + name
                info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    with _directory(parent, [name]) as child:
                        visit(child, path + "/", depth + 1)
                elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and path in expected:
                    found.add(path)
                else:
                    raise WorkspaceError("workspace input snapshot changed")
        with _directory(directory, ["inputs"]) as inputs:
            visit(inputs)
        if found != expected:
            raise WorkspaceError("workspace input snapshot changed")

    def verify(self, reference):
        with self.lease(reference) as (directory, _):
            self._verify_inputs(reference, directory)

    @staticmethod
    def _state(directory, state, **fields):
        _write(directory, "state.json", canonical_json({"state": state, **fields}).encode())

    @staticmethod
    def _scan(directory, policy):
        files = total = 0

        def visit(parent, depth):
            nonlocal files, total
            if depth > 10:
                raise WorkspaceError("workspace directory depth exceeded")
            for name in os.listdir(parent):
                info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    with _directory(parent, [name]) as child:
                        visit(child, depth + 1)
                elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                    files += 1
                    total += info.st_size
                    if files > policy.max_workspace_files or total > policy.max_workspace_bytes:
                        raise WorkspaceError("workspace aggregate storage quota exceeded")
                else:
                    raise WorkspaceError("workspace contains an unsafe filesystem object")
        visit(directory, 0)

    def _publish(self, reference, directory, policy, outputs):
        if (type(outputs) is not tuple or len(outputs) > policy.max_output_files
                or any(not isinstance(output, WorkspaceOutput) for output in outputs)
                or len({output.path for output in outputs}) != len(outputs)):
            raise WorkspaceError("invalid workspace output manifest")
        self._verify_inputs(reference, directory)
        self._scan(directory, policy)
        blobs, total = [], 0
        for output in outputs:
            data = _read(directory, "outputs/" + output.path, policy.max_output_bytes - total)
            total += len(data)
            if total > policy.max_output_bytes:
                raise WorkspaceError("workspace output quota exceeded")
            blobs.append((output, data))
        manifest = reference.descriptor
        inputs = {item["reference"]["sha256"] for item in manifest["inputs"]}
        if manifest["context"]:
            inputs.add(manifest["context"]["sha256"])
        refs = [self.artifacts.put_bytes(data, media_type=output.media_type,
                    input_sha256s=tuple(sorted(inputs))).to_dict() for output, data in blobs]
        # Publish recovery evidence before creating the durable retention link.
        # The process may die after link() commits but before state.json is
        # updated; release must still know the exact immutable reference set.
        _write(directory, "publication.json", canonical_json({
            "workspace": reference.identity, "artifacts": refs}).encode(), readonly=True)
        if refs:
            self.artifacts.link("workspace-output-" + reference.workspace_id[3:], refs)
        return refs

    @staticmethod
    def _published_refs(reference, directory, state):
        try:
            raw = _read(directory, "publication.json", 262144)
        except FileNotFoundError:
            # Compatibility with workspaces published before the independent
            # publication manifest was introduced.
            return state.get("artifacts", [])
        try:
            publication = json.loads(raw)
            if (type(publication) is not dict or set(publication) != {"workspace", "artifacts"}
                    or publication["workspace"] != reference.identity
                    or type(publication["artifacts"]) is not list
                    or len(publication["artifacts"]) > reference.descriptor["policy"]["max_output_files"]):
                raise WorkspaceError("workspace publication manifest changed")
            refs = [ArtifactRef.from_dict(item).to_dict() for item in publication["artifacts"]]
            if state.get("artifacts") and state["artifacts"] != refs:
                raise WorkspaceError("workspace publication manifest changed")
            return refs
        except (TypeError, KeyError, ValueError, ArtifactError):
            raise WorkspaceError("workspace publication manifest changed") from None

    def release(self, reference, *, resolution: ArtifactRef):
        """Operator supplies retained evidence that all associated effects resolved.

        Evidence interpretation belongs to the application; neither elapsed age
        nor worker exit is treated as proof of external-effect completion.
        """
        with self.lease(reference) as (directory, _):
            self.artifacts.verify(resolution)
            state = json.loads(_read(directory, "state.json", 65536))
            output_refs = self._published_refs(reference, directory, state)
            input_refs = [item["reference"] for item in reference.descriptor["inputs"]]
            if reference.descriptor["context"]:
                input_refs.append(reference.descriptor["context"])
            # Closing links is explicit and follows resolution, never worker
            # exit or elapsed age. Publications remain immutable audit records.
            if input_refs:
                self.artifacts.link("workspace-input-" + reference.workspace_id[3:], input_refs, state="released")
            if output_refs:
                self.artifacts.link("workspace-output-" + reference.workspace_id[3:], output_refs, state="released")
            self._state(directory, "released", resolution=resolution.to_dict())

    def cleanup(self, reference):
        with self.lease(reference) as (directory, _):
            state = json.loads(_read(directory, "state.json", 65536))
            if state.get("state") != "released":
                raise WorkspaceError("workspace effects have not been released")
            root = _open_directory(self.root)
            try:
                _write(root, "retired/" + reference.workspace_id, reference.identity.encode(), readonly=True)
            finally:
                os.close(root)

            def remove(parent):
                for name in os.listdir(parent):
                    info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        with _directory(parent, [name]) as child:
                            remove(child)
                        os.rmdir(name, dir_fd=parent)
                    else:
                        # Unlinking a symlink removes only the link, never its target.
                        os.unlink(name, dir_fd=parent)
            remove(directory)
            root = _open_directory(self.root)
            try:
                os.rmdir(reference.workspace_id, dir_fd=root)
            finally:
                os.close(root)


@dataclass(frozen=True)
class WorkspaceAdapter:
    manager: WorkspaceManager
    reference: WorkspaceRef
    policy: WorkspacePolicy
    tool: WorkspaceTool
    outputs: tuple[WorkspaceOutput, ...] = ()

    def __post_init__(self):
        if (not isinstance(self.manager, WorkspaceManager) or not isinstance(self.reference, WorkspaceRef)
                or not isinstance(self.policy, WorkspacePolicy) or not isinstance(self.tool, WorkspaceTool)
                or type(self.outputs) is not tuple or len(self.outputs) > self.policy.max_output_files
                or any(not isinstance(item, WorkspaceOutput) for item in self.outputs)
                or len({item.path for item in self.outputs}) != len(self.outputs)):
            raise WorkspaceError("invalid registered workspace adapter")
        self.manager._authorise(self.reference)
        from .container_execution import ContainerSpec
        if (self.policy.profile == 'isolated-container-execution') != isinstance(self.tool.worker, ContainerSpec):
            raise WorkspaceError('workspace tool containment profile mismatch')
        if (self.reference.descriptor["policy"] != self.policy.descriptor
                or self.tool.name not in self.policy.tools
                or self.tool.external_effects and not self.policy.allow_external_effects):
            raise WorkspaceError("workspace adapter conflicts with registered policy")

    def descriptor(self):
        return {"adapter": "mas-workspace-tool/1", "workspace": self.reference.descriptor,
                "tool": self.tool.descriptor, "outputs": [asdict(item) for item in self.outputs]}

    @property
    def registration_descriptor(self):
        """Static capabilities; assigned workspace/input identity belongs to the run."""
        return {"adapter": "mas-workspace-tool/1", "policy": self.policy.descriptor,
                "tool": self.tool.descriptor, "outputs": [asdict(item) for item in self.outputs]}

    def handle(self, _context, _request):
        raise WorkspaceError("workspace adapter requires WorkspaceExecutor")


class WorkspaceExecutor:
    """One admitted invocation per registered tool execution, including outputs."""

    def __init__(self, executor: RegisteredProcessExecutor, *, container_runtime=None):
        self.executor = executor
        self.container_runtime = container_runtime

    def execute(self, adapter, context, request, *, deadline, tick=None):
        if not isinstance(adapter, WorkspaceAdapter):
            return self.executor.execute(adapter, context, request, deadline=deadline, tick=tick)
        from .container_execution import ContainerSpec, DockerRuntime, execute_workspace
        if isinstance(adapter.tool.worker, ContainerSpec):
            return execute_workspace(self.container_runtime or DockerRuntime(), adapter, context, request,
                                     deadline=deadline, tick=tick)
        manager, reference = adapter.manager, adapter.reference
        with manager.lease(reference) as (directory, lease):
            manager._verify_inputs(reference, directory)
            state = json.loads(_read(directory, "state.json", 65536))
            if state.get("state") == "released":
                raise WorkspaceError("released workspace cannot execute")
            descriptor = adapter.tool.worker.descriptor
            pinned = dict(descriptor.get("pinned_files", {}))
            for item in reference.descriptor["inputs"]:
                pinned[str(manager.root / reference.workspace_id / "inputs" / item["path"])] = item["reference"]["sha256"]
            if reference.descriptor["context"]:
                pinned[str(manager.root / reference.workspace_id / "context.json")] = reference.descriptor["context"]["sha256"]
            configuration = {**descriptor["configuration"], "workspace": {
                "identity": reference.identity, "inputs": "inputs", "outputs": "outputs", "work": "work",
                "context": "context.json" if reference.descriptor["context"] else None}}
            worker = adapter.tool.worker.with_workspace(cwd=manager.root / reference.workspace_id,
                configuration=configuration, pinned_files=pinned)
            worker.verify()

            def execute():
                if state.get("state") != "prepared":
                    raise WorkspaceError("workspace attempt already executed; reconcile or prepare a new attempt")
                manager._state(directory, "running")
                refs = []
                try:
                    outcome = self.executor.execute(ExecutableAdapter(worker), context, request,
                        deadline=deadline, tick=tick, ownership_descriptors=(lease,))
                    if outcome.status != "completed":
                        raise WorkspaceError("workspace tool did not complete")
                    refs = manager._publish(reference, directory, adapter.policy, adapter.outputs)
                    output = {"result": outcome.output, "artifacts": refs, "workspace": reference.identity}
                    integer_json(output)
                    manager._state(directory, "quiescent", artifacts=refs)
                    return Invocation(output, {"workspace": reference.identity,
                        "worker": worker.identity, "tool": adapter.tool.name,
                        "input_sha256s": sorted(set(pinned.values()))})
                except BaseException:
                    manager._state(directory, "unknown", artifacts=refs)
                    raise

            result = context.invoke(execute, input={"adapter": adapter.descriptor(),
                "request": asdict(request)}, reservation_microusd=adapter.tool.reservation_microusd)
            return Outcome(result)
