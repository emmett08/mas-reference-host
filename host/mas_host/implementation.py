"""Reproducible source-package bindings for trusted Python registrations.

This module never imports names or executes deployment-supplied code. It follows
installed Python package files and static imports, hashes package assets, and
checks loaded registration code against a fresh compilation of those bytes.
It is integrity/version resolution, not a sandbox for hostile Python objects.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from functools import cached_property, lru_cache
import hashlib
import inspect
import json
import marshal
from pathlib import Path
import sys
import types

from .service_contracts import ServiceError

_FORMAT = 'mas-python-implementation/1'
_MAX_FILES = 100000
_MAX_BYTES = 1024 * 1024 * 1024
_IGNORED = {'__pycache__', '.git', '.pytest_cache', '.mypy_cache', 'node_modules'}


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _normalise(code):
    # Installation paths are not executable identity, including nested code.
    constants = tuple(_normalise(item) if isinstance(item, types.CodeType) else item
                      for item in code.co_consts)
    return code.replace(co_filename='<package>', co_consts=constants)


def _fingerprint(code):
    return hashlib.sha256(marshal.dumps(_normalise(code))).hexdigest()


def _stat(path):
    value = path.stat()
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


@lru_cache(maxsize=100000)
def _file_content(path, identity):
    # ctime detects writes even when callers preserve size and mtime. A new
    # inode or a concurrent rewrite also invalidates this process-local cache.
    digest = hashlib.sha256()
    source = [] if path.suffix == '.py' else None
    try:
        with path.open('rb') as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                if source is not None:
                    source.append(chunk)
        if _stat(path) != identity:
            raise ServiceError('implementation_changed')
    except OSError:
        raise ServiceError('implementation_source_unavailable') from None
    return digest.hexdigest(), b''.join(source) if source is not None else None


@lru_cache(maxsize=512)
def _source_codes(content):
    try:
        root = compile(content, '<package>', 'exec', dont_inherit=True)
    except (SyntaxError, ValueError):
        raise ServiceError('implementation_source_invalid') from None
    pending, result = [root], set()
    while pending:
        code = pending.pop()
        result.add(_fingerprint(code))
        pending.extend(item for item in code.co_consts if isinstance(item, types.CodeType))
    return frozenset(result)


@lru_cache(maxsize=2048)
def _imports(content):
    try:
        tree = ast.parse(content)
    except (SyntaxError, ValueError):
        raise ServiceError('implementation_source_invalid') from None
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split('.')[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            names.add(node.module.split('.')[0])
    return names


def _installed(name):
    """Find a top-level source package without executing an import hook."""
    loaded = sys.modules.get(name)
    origin = getattr(loaded, '__file__', None)
    if origin and Path(origin).is_file():
        path = Path(origin).absolute()
        return path.parent if path.name == '__init__.py' else path
    for directory in sys.path:
        root = Path(directory or '.').absolute()
        candidate = root / name
        if (candidate / '__init__.py').is_file():
            return candidate
        if candidate.with_suffix('.py').is_file():
            return candidate.with_suffix('.py')
    return None


def _class_functions(cls):
    """Read method descriptors without evaluating user attributes."""
    from dataclasses import is_dataclass
    generated = {'__init__', '__repr__', '__eq__', '__ne__', '__lt__', '__le__',
                 '__gt__', '__ge__', '__hash__', '__setattr__', '__delattr__'}
    for base in cls.__mro__:
        if base.__module__.split('.')[0] in sys.stdlib_module_names:
            continue
        for name, item in vars(base).items():
            functions = ((item,) if inspect.isfunction(item) else
                         (item.__func__,) if isinstance(item, (staticmethod, classmethod)) else
                         (item.fget, item.fset, item.fdel) if isinstance(item, property) else ())
            for function in functions:
                if function is None:
                    continue
                if (function.__code__.co_filename.startswith('<') and is_dataclass(base)
                        and name in generated):
                    # Standard dataclass fields are in the pinned class source;
                    # CPython's generator is bound by the recorded runtime.
                    continue
                yield function


def _entrypoints(values):
    """Capture callbacks carried by adapters as well as their handle methods."""
    pending, seen, functions = list(values), set(), {}
    while pending:
        value = pending.pop()
        if id(value) in seen or value is None:
            continue
        seen.add(id(value))
        if len(seen) > 4096:
            raise ServiceError('implementation_graph_too_large')
        if inspect.ismethod(value):
            value = value.__func__
        if inspect.isfunction(value):
            # Decorators such as dataclasses._recursive_repr copy __module__
            # from the wrapped class. Their actual globals identify stdlib
            # machinery; following its synthetic closure is not app code.
            origin = value.__globals__.get('__name__', '').split('.')[0]
            if origin in sys.stdlib_module_names:
                continue
            functions[id(value)] = value
            for name in value.__code__.co_names:
                referenced = value.__globals__.get(name)
                if inspect.isfunction(referenced) or (isinstance(referenced, type)
                        and referenced.__module__.split('.')[0] not in sys.stdlib_module_names):
                    pending.append(referenced)
                elif isinstance(referenced, types.ModuleType):
                    # Module-qualified helpers are already loaded. Reading
                    # their dictionaries does not execute import hooks.
                    for attribute in value.__code__.co_names:
                        helper = vars(referenced).get(attribute)
                        if inspect.isfunction(helper):
                            pending.append(helper)
            # Callback closures may contain another executable callback. State
            # values remain governed by the workflow's explicit descriptor.
            for cell in value.__closure__ or ():
                try:
                    item = cell.cell_contents
                except ValueError:
                    continue
                if inspect.isfunction(item) or inspect.ismethod(item):
                    pending.append(item)
            continue
        if isinstance(value, type):
            pending.extend(_class_functions(value))
        elif isinstance(value, dict):
            pending.extend(value.values())
        elif isinstance(value, (tuple, list)):
            pending.extend(value)
        elif not isinstance(value, (str, bytes, int, float, bool, Path, types.ModuleType, type)):
            # Do not evaluate properties or invoke user __getattr__ methods.
            pending.extend(_class_functions(type(value)))
            namespace = getattr(value, '__dict__', {})
            pending.extend(item for item in namespace.values()
                           if inspect.isfunction(item) or inspect.ismethod(item)
                           or (hasattr(type(item), 'handle') and not isinstance(item, type)))
    return tuple(functions.values())


@dataclass(frozen=True)
class VerifiedImplementation:
    """Immutable manifest bytes; local paths are deliberately never persisted."""
    encoded_manifest: str

    @property
    def manifest(self):
        return json.loads(self.encoded_manifest)

    @cached_property
    def digest(self):
        return hashlib.sha256(self.encoded_manifest.encode()).hexdigest()

    @classmethod
    def capture(cls, values):
        functions = _entrypoints(values)
        if not functions:
            raise ServiceError('implementation_source_unavailable')
        pending = {'mas_host'}
        entry_modules = set()
        entries = []
        for function in functions:
            module = function.__module__
            if not module or module.split('.')[0] in sys.stdlib_module_names:
                continue
            source = Path(function.__code__.co_filename).absolute()
            try:
                content = source.read_bytes()
            except OSError:
                raise ServiceError('implementation_source_unavailable') from None
            code_digest = _fingerprint(function.__code__)
            if code_digest not in _source_codes(content):
                raise ServiceError('implementation_loaded_code_mismatch')
            pending.add(module.split('.')[0])
            entry_modules.add(source)
            pending.update(_imports(content))
            entries.append({'module': module, 'name': function.__qualname__, 'code': code_digest})
        roots, absent, files, total = {}, set(), {}, 0
        while pending:
            name = min(pending)
            pending.remove(name)
            if name in roots or name in absent or name in sys.stdlib_module_names or name == '__future__':
                continue
            root = _installed(name)
            if root is None:
                absent.add(name)
                continue
            roots[name] = root
            candidates = [root] if root.is_file() else sorted(root.rglob('*'))
            for path in candidates:
                relative = path.name if root.is_file() else path.relative_to(root).as_posix()
                if any(part in _IGNORED or part.endswith('.dist-info') for part in Path(relative).parts):
                    continue
                if path.is_dir() or path.suffix in {'.pyc', '.pyo'}:
                    continue
                if path.is_symlink():
                    raise ServiceError('implementation_symlink_rejected')
                try:
                    metadata = _stat(path)
                    digest, content = _file_content(path, metadata)
                except OSError:
                    raise ServiceError('implementation_source_unavailable') from None
                total += metadata[2]
                if total > _MAX_BYTES or len(files) >= _MAX_FILES:
                    raise ServiceError('implementation_package_too_large')
                files[name + '/' + relative] = digest
                # The host package contains optional integrations. Their
                # mere presence does not make every SDK a runtime dependency
                # of every registration. Entry module imports establish that
                # dependency; third-party packages follow runtime imports.
                if content is not None and (path in entry_modules or
                        (name != 'mas_host' and not name.startswith('test_') and
                         not any(part in {'tests', 'test', 'docs', 'examples'} or part.startswith('test_')
                                 for part in Path(relative).parts))):
                    pending.update(_imports(content))
        manifest = {'format': _FORMAT, 'runtime': {'implementation': sys.implementation.name,
                    'version': list(sys.version_info[:3]), 'cache_tag': sys.implementation.cache_tag,
                    'optimisation': sys.flags.optimize},
                    'entrypoints': sorted(entries, key=_canonical), 'files': files,
                    'unavailable_imports': sorted(absent)}
        return cls(_canonical(manifest))

    def verify_bindings(self, values):
        """Factory-produced adapters must come from the pinned package set."""
        current, original = self.capture(values).manifest, self.manifest
        if current['runtime'] != original['runtime'] or any(
                original['files'].get(name) != digest for name, digest in current['files'].items()):
            raise ServiceError('implementation_unpinned_factory_adapter')

    def verify(self, values):
        if self.capture(values).encoded_manifest != self.encoded_manifest:
            raise ServiceError('implementation_changed')


def implementation_values(workflow):
    return (*workflow.adapters.values(), workflow.classify, workflow.continue_input, workflow.adapter_factory,
            *workflow.implementation_sources)
