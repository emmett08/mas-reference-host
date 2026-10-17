"""Compatibility name for generic Cursor inference; no separate implementation."""
from .cursor_provider import CursorLocalProvider as CursorStructuredProvider

__all__ = ["CursorStructuredProvider"]
