"""Reusable, provider-independent host adapters for the Rust MAS protocol."""

__all__ = [
    "Adapter", "AdapterContext", "AdapterError", "AdapterExecutor", "Context", "Invocation", "InvocationTicket", "Outcome", "RecoveryRequired", "Runner", "RunnerError", "TaskRequest",
]


def __getattr__(name):
    # Portable HTTP clients must not import Linux execution ownership (fcntl).
    # Preserve existing package exports when an execution consumer requests them.
    if name not in __all__:
        raise AttributeError(name)
    from importlib import import_module
    module = '.contracts' if name in {'AdapterContext', 'AdapterExecutor'} else '.runner'
    value = getattr(import_module(module, __name__), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
