"""``GET /search``: the global search over the catalog and workspace.

A thin wrapper over ``flowfile_core.search.search_catalog``, which holds every rule (what matches, how it ranks, who
sees what), so another transport such as an MCP server answers exactly what this route answers. JWT-only, no trailing
slash, plain ``def`` because it reads the database.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.search import SearchKind, SearchRequest, SearchResponse, search_catalog

router = APIRouter(prefix="/search", tags=["search"])


@router.get("", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1, max_length=200, description="The words to search for."),
    kinds: list[SearchKind] | None = Query(None, description="Only these kinds; all kinds when omitted."),
    limit_per_kind: int = Query(10, ge=1, le=100, description="At most this many hits of each kind."),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
) -> SearchResponse:
    """Everything the caller may see whose name, path, description or kind-specific fields match ``q``."""
    try:
        request = SearchRequest(query=q, kinds=kinds, limit_per_kind=limit_per_kind)
    except ValidationError as exc:
        detail = [{"loc": ["query", "q"], "msg": e["msg"]} for e in exc.errors()]
        raise HTTPException(status_code=422, detail=detail) from None
    return search_catalog(db, current_user, request)
