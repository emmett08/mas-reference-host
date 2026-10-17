"""Durable, authorised context selection with explicitly registered tokenisers."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Callable

from .artifacts import ArtifactRef, ArtifactRepository
from .codec import integer_json, is_actor_identity
from .workspaces import WorkspaceError


@dataclass(frozen=True)
class TokenCounter:
    """Trusted application tokeniser; JSON cannot select or import a callable."""
    name: str
    version: str
    count: Callable[[str], int] = field(repr=False, compare=False)

    def __post_init__(self):
        if not is_actor_identity(self.name) or not is_actor_identity(self.version) or not callable(self.count):
            raise WorkspaceError("invalid registered context tokeniser")

    def tokens(self, value):
        try:
            result = self.count(value)
        except Exception:
            raise WorkspaceError("registered context tokeniser failed") from None
        if type(result) is not int or not 0 <= result <= 2**31 - 1:
            raise WorkspaceError("registered tokeniser returned an invalid count")
        return result


@dataclass(frozen=True)
class ContextPolicy:
    max_items: int = 16
    max_bytes: int = 16384
    max_tokens: int = 4096
    selection: str = "ordered-whole-items/1"

    def __post_init__(self):
        if self.selection != "ordered-whole-items/1":
            raise WorkspaceError("unsupported context selection policy")
        for name, maximum in (("max_items", 32), ("max_bytes", 32768), ("max_tokens", 1_000_000)):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise WorkspaceError("invalid context budget")


def build_context(store: ArtifactRepository, references: tuple[ArtifactRef, ...], *,
                  policy: ContextPolicy, tokeniser: TokenCounter) -> ArtifactRef:
    """Select an ordered whole-item prefix and publish the exact resulting input.

    No silent summarisation or partial-document truncation is performed. The
    caller must use a tokeniser appropriate to its model and version its changes.
    Reading the returned artefact on recovery supplies the same text and policy.
    """
    if (not isinstance(store, ArtifactRepository) or not isinstance(policy, ContextPolicy)
            or not isinstance(tokeniser, TokenCounter) or type(references) is not tuple
            or len(references) > 64 or any(not isinstance(ref, ArtifactRef) for ref in references)):
        raise WorkspaceError("invalid context snapshot request")
    for reference in references:
        store.inspect(reference)
    texts, selected, token_count = [], [], tokeniser.tokens("")
    if token_count > policy.max_tokens:
        raise WorkspaceError("empty context exceeds the registered token budget")
    reason = None
    for reference in references:
        if len(selected) >= policy.max_items:
            reason = "item_limit"
            break
        if reference.size > policy.max_bytes:
            reason = "byte_limit"
            break
        try:
            text = store.read_bytes(reference).decode("utf-8")
        except UnicodeError:
            raise WorkspaceError("context artefacts must contain UTF-8 text") from None
        candidate = "\n\n".join([*texts, text])
        if len(candidate.encode("utf-8")) > policy.max_bytes:
            reason = "byte_limit"
            break
        measured = tokeniser.tokens(candidate)
        if measured > policy.max_tokens:
            reason = "token_limit"
            break
        texts.append(text)
        selected.append(reference)
        token_count = measured
    content = "\n\n".join(texts)
    snapshot = {"format": "mas-context/1", "principal": store.principal,
        "policy": asdict(policy), "tokeniser": {"name": tokeniser.name, "version": tokeniser.version},
        "references": [reference.to_dict() for reference in selected], "text": content,
        "bytes": len(content.encode("utf-8")), "tokens": token_count,
        "omitted": [reference.artifact_id for reference in references[len(selected):]],
        "omission_reason": reason}
    reference = store.put_bytes(integer_json(snapshot).encode("utf-8"), media_type="application/json",
        input_sha256s=tuple(sorted({item.sha256 for item in selected})))
    store.link("context-" + reference.artifact_id, [reference, *selected])
    return reference
