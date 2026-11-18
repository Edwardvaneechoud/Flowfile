"""Global search over the catalog and workspace (``GET /search``; the same call backs a future MCP tool)."""

from flowfile_core.search.models import (
    SearchHit,
    SearchKind,
    SearchMatch,
    SearchRef,
    SearchRequest,
    SearchResponse,
)
from flowfile_core.search.resources import RESOURCE_URI_TEMPLATES, resource_uri
from flowfile_core.search.service import search_catalog
from flowfile_core.search.tool import SEARCH_TOOL_NAME, search_tool_definition

__all__ = [
    "RESOURCE_URI_TEMPLATES",
    "SEARCH_TOOL_NAME",
    "SearchHit",
    "SearchKind",
    "SearchMatch",
    "SearchRef",
    "SearchRequest",
    "SearchResponse",
    "resource_uri",
    "search_catalog",
    "search_tool_definition",
]
