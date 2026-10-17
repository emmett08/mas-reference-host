"""Provider-specific secret sources, independent of application adapters."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from .provider_contract import ProviderError


class CredentialError(ProviderError):
    """A selected provider has no usable credential; no work was submitted."""


def read_api_key(provider: str, *, environ: Mapping[str, str] | None = None) -> str:
    """Read a bounded provider-specific secret; never fall back from an invalid file."""
    if provider not in {"openai", "cursor"}:
        raise ProviderError("unsupported credential provider")
    environment = os.environ if environ is None else environ
    name = provider.upper() + "_API_KEY"
    key_file = environment.get(name + "_FILE")
    if key_file:
        try:
            with Path(key_file).open(encoding="utf-8") as stream:
                key = stream.read(4097)
        except (OSError, UnicodeError):
            raise CredentialError(f"could not read {name}_FILE") from None
    else:
        key = environment.get(name, "")
    if not isinstance(key, str) or len(key) > 4096:
        raise CredentialError(f"invalid {name}_FILE or {name}")
    key = key.strip()
    if not key or any(character.isspace() for character in key):
        raise CredentialError(f"selected provider requires a valid {name}_FILE or {name}")
    return key
