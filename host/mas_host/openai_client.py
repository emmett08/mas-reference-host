"""Optional OpenAI client ownership, independent of any application contract."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Mapping

from .provider_contract import ProviderError
from .provider_outcomes import ProviderOutcomeFailure, not_sent


def create_client(api_key: str):
    """Construct the fixed-endpoint SDK client with bounded, non-retrying calls."""
    if not api_key:
        raise not_sent("openai", "credentials_unavailable")
    try:
        from openai import OpenAI
    except ImportError:
        raise not_sent("openai", "runtime_unavailable") from None
    try:
        return OpenAI(api_key=api_key, base_url="https://api.openai.com/v1",
                      timeout=45.0, max_retries=0)
    except Exception:
        raise not_sent("openai", "client_initialisation_failed") from None


@contextmanager
def response_client(*, client=None, api_key: str | None = None,
                    environ: Mapping[str, str] | None = None):
    """Resolve secrets on execution; close owned clients and retain injected ones.

    Registration and descriptor inspection never enter this context. An explicit
    environment is a credential source for embedders/tests; the default is read
    from the executing process so secret rotation does not alter the binding.
    """
    if client is not None:
        yield client
        return
    if api_key is None:
        from .provider_credentials import CredentialError, read_api_key
        try:
            api_key = read_api_key("openai", environ=environ)
        except CredentialError:
            raise not_sent("openai", "credentials_unavailable") from None
    owned = create_client(api_key)
    primary = None
    try:
        yield owned
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            owned.close()
        except BaseException:
            if primary is None:
                raise
            if isinstance(primary, ProviderOutcomeFailure):
                raise primary.with_cleanup_failure() from None
