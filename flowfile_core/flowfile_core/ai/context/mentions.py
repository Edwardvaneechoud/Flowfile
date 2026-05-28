"""Backend ``@``-mention parsing primitive.

Resolves ``@node:<ref>``, ``@schema:<ref>``, ``@flow``,
``@selection`` references in user messages so
:mod:`flowfile_core.ai.context.builder` can pin the right subgraph.
The frontend autocomplete UI (suggestion list, completion dropdown)
lives in the frontend.

Design notes
------------

The parser is intentionally permissive:

* Mentions with a ``:<ref>`` payload (``@node:filter_3``,
  ``@schema:orders``) match alphanumerics, underscores, hyphens, dots,
  and spaces inside an optional pair of quotes. We stop at whitespace
  outside quotes, at end-of-string, or at common punctuation that
  obviously isn't part of a node name.
* Bare mentions (``@flow``, ``@selection``) match exactly.
* Kind names are case-insensitive; refs are kept verbatim for the
  resolver, which downcases as needed.

The parser does **not** validate that the referenced node exists — that
is :func:`resolve_mentions`'s job (and the caller decides whether to
treat a missing ref as an error or a no-op).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Protocol

MentionKind = Literal["node", "schema", "flow", "selection"]

_KIND_WITH_REF: tuple[MentionKind, ...] = ("node", "schema")
_KIND_BARE: tuple[MentionKind, ...] = ("flow", "selection")

_REF_RE = re.compile(
    r"(?<!\w)@(?P<kind>node|schema|flow|selection)"
    r"(?::(?P<ref>"
    r'"[^"]*"'
    r"|'[^']*'"
    r"|[\w\-]+"
    r"))?"
    r"(?=$|[\s,;.!?)\]}])",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class Mention:
    """A parsed ``@``-mention.

    ``span`` is the half-open ``[start, end)`` range in the source
    text (useful for the frontend autocomplete if it wants to
    highlight or replace). ``ref`` is ``None`` for bare ``@flow`` /
    ``@selection``.
    """

    kind: MentionKind
    ref: str | None
    span: tuple[int, int]


@dataclass(frozen=True, slots=True)
class ResolvedMention:
    """A :class:`Mention` mapped to concrete node ids in a flow."""

    kind: MentionKind
    ref: str | None
    node_ids: tuple[int | str, ...]


class _GraphLike(Protocol):
    """The minimal slice of :class:`FlowGraph` we use to resolve mentions."""

    @property
    def nodes(self) -> list: ...  # noqa: D401  # list[FlowNode]


def parse_mentions(text: str) -> list[Mention]:
    """Parse all ``@``-mentions out of ``text``.

    Returns mentions in source order. ``ref`` strips surrounding quotes
    if present so callers see the literal name. Unknown / malformed
    mentions are simply not emitted — there is no error mode.
    """

    mentions: list[Mention] = []
    for match in _REF_RE.finditer(text):
        kind_raw = match.group("kind")
        ref_raw = match.group("ref")
        kind: MentionKind = kind_raw.lower()  # type: ignore[assignment]
        ref: str | None
        if kind in _KIND_BARE:
            if ref_raw is not None:
                # @flow:foo / @selection:foo — strip the trailing payload.
                continue
            ref = None
        else:
            if ref_raw is None:
                # @node / @schema without a ref — ambiguous, skip.
                continue
            ref = _strip_quotes(ref_raw)
        mentions.append(Mention(kind=kind, ref=ref, span=match.span()))
    return mentions


def resolve_mentions(
    mentions: list[Mention],
    graph: _GraphLike,
    *,
    selection_node_ids: list[int | str] | None = None,
) -> list[ResolvedMention]:
    """Map parsed mentions to concrete node ids in ``graph``.

    * ``@node:<ref>`` and ``@schema:<ref>`` resolve by case-insensitive
      node ``name`` first, then by stringified ``node_id``.
    * ``@flow`` resolves to every node in the graph.
    * ``@selection`` resolves to ``selection_node_ids`` (caller-supplied
      because the canvas selection lives in the frontend).

    A reference that doesn't match anything still emits a
    :class:`ResolvedMention` with an empty ``node_ids`` tuple so the
    caller can decide whether to error or silently drop it.
    """

    selection: tuple[int | str, ...] = tuple(selection_node_ids or ())
    name_index = _build_name_index(graph)
    id_index = _build_id_index(graph)

    resolved: list[ResolvedMention] = []
    for mention in mentions:
        if mention.kind == "flow":
            ids = tuple(node.node_id for node in graph.nodes)
        elif mention.kind == "selection":
            ids = selection
        else:
            ids = _lookup(mention.ref, name_index, id_index)
        resolved.append(
            ResolvedMention(kind=mention.kind, ref=mention.ref, node_ids=ids),
        )
    return resolved


def _strip_quotes(s: str) -> str:
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        return s[1:-1]
    return s


def _build_name_index(graph: _GraphLike) -> dict[str, list[int | str]]:
    index: dict[str, list[int | str]] = {}
    for node in graph.nodes:
        name = getattr(node, "name", None)
        if not name:
            continue
        index.setdefault(name.lower(), []).append(node.node_id)
    return index


def _build_id_index(graph: _GraphLike) -> dict[str, int | str]:
    return {str(node.node_id): node.node_id for node in graph.nodes}


def _lookup(
    ref: str | None,
    name_index: dict[str, list[int | str]],
    id_index: dict[str, int | str],
) -> tuple[int | str, ...]:
    if ref is None:
        return ()
    name_hit = name_index.get(ref.lower())
    if name_hit:
        return tuple(name_hit)
    id_hit = id_index.get(ref)
    if id_hit is not None:
        return (id_hit,)
    return ()


__all__ = [
    "Mention",
    "MentionKind",
    "ResolvedMention",
    "parse_mentions",
    "resolve_mentions",
]
