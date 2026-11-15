"""The global search: one call, transport-free, that the HTTP route and any other client (an MCP server) share.

``search_catalog(db, user, request)`` runs every requested kind's provider under the caller's catalog access
(``catalog.access.AccessResolver``, the same resolver the catalog routes build), scores the candidates, and answers the
best ``limit_per_kind`` of each kind plus how many matched in all.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from flowfile_core.catalog.access import AccessResolver
from flowfile_core.search.matching import match_quality, score_fields, tokenize
from flowfile_core.search.models import SearchHit, SearchKind, SearchMatch, SearchRequest, SearchResponse
from flowfile_core.search.providers import MAX_CANDIDATES, PROVIDERS, SearchContext

KIND_ORDER: dict[SearchKind, int] = {kind: index for index, kind in enumerate(SearchKind)}
_OUTSIDE_THE_TREE = frozenset({SearchKind.KERNEL, SearchKind.CUSTOM_NODE})


def _sort_key(hit: SearchHit) -> tuple:
    return (-hit.score, KIND_ORDER[hit.kind], hit.name.lower(), str(hit.id))


def _name_matches(tokens: list[str], candidate) -> bool:
    """Some token is in the candidate's name, whichever field matched it best."""
    name = candidate.fields[0][1] if candidate.fields and candidate.fields[0][0] == "name" else None
    return bool(name) and any(match_quality(token, name) for token in tokens)


def search_catalog(
    db: Session, user, request: SearchRequest, *, max_candidates: int = MAX_CANDIDATES
) -> SearchResponse:
    """Search everything ``user`` may see for ``request.query``.

    ``user`` is the signed-in user (``id``, ``is_admin``); in multi-user mode a non-admin sees only what the catalog's
    list endpoints would show them, and everyone sees only their own kernels.
    """
    tokens = tokenize(request.query)
    kinds = list(dict.fromkeys(request.kinds)) if request.kinds else list(SearchKind)
    if not tokens:
        return SearchResponse(query=request.query, tokens=[], kinds=kinds, hits=[], counts={k: 0 for k in kinds})
    namespaced = any(kind not in _OUTSIDE_THE_TREE for kind in kinds)
    ctx = SearchContext.build(db, AccessResolver(db, user), user.id, tokens, max_candidates, namespaced=namespaced)
    hits: list[SearchHit] = []
    counts: dict[SearchKind, int] = {}
    for kind in kinds:
        found: list[SearchHit] = []
        for candidate in PROVIDERS[kind](ctx):
            scored = score_fields(tokens, candidate.fields)
            if scored is None:
                continue
            if candidate.require_name_match and not _name_matches(tokens, candidate):
                continue
            found.append(
                SearchHit(
                    **candidate.hit,
                    score=round(scored.score * candidate.weight, 3),
                    matches=[SearchMatch(field=field, value=value) for field, value in scored.matches],
                )
            )
        found.sort(key=_sort_key)
        counts[kind] = len(found)
        hits.extend(found[: request.limit_per_kind])
    hits.sort(key=_sort_key)
    return SearchResponse(
        query=request.query,
        tokens=tokens,
        kinds=kinds,
        hits=hits,
        counts=counts,
        truncated=bool(ctx.truncated),
        truncated_kinds=ctx.truncated,
    )
