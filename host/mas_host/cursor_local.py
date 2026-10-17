"""Compatibility imports for the original invoice-only Cursor module.

New MAS applications use ``cursor_provider.CursorLocalProvider.infer`` and the
public ``cursor_session`` APIs. ``CursorLocalProvider`` here is the old invoice
adapter, retained through the next major API release with its original recovery
descriptor. This facade contains no prompt, validation or transport logic.
"""
from .invoice_cursor import InvoiceCursorProvider as CursorLocalProvider
from .cursor_session import (CURSOR_SDK_VERSION, MAX_STREAM_TEXT_BYTES,
    RPC_TIMEOUT_SECONDS, CursorTextSession as RestrictedCursorSession,
    check_runtime, launch_client as _launch_client, _bridge_child, _bridge_files,
    _validate_node, _runtime_identity, _runtime_manifest, _sdk_module,
    _OwnedClient, _close_bridge)


if __name__ == "__main__":
    # Preserve historical bridge entry points during rolling upgrades.
    import runpy
    runpy.run_module("mas_host.cursor_session", run_name="__main__")
