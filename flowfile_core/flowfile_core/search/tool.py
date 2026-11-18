"""The global search described as an MCP tool, so an MCP server can register it without restating the contract.

``search_tool_definition()`` is the ``tools/list`` entry (name, title, description, input and output JSON schemas);
the server's ``tools/call`` handler validates the arguments with ``SearchRequest``, calls
``service.search_catalog`` for the authenticated user, and answers ``SearchResponse.model_dump(mode="json")`` as the
structured content. Each hit's ``uri`` is a ``resources.RESOURCE_URI_TEMPLATES`` URI the server can also expose.
"""

from __future__ import annotations

from typing import Any

from flowfile_core.search.models import SearchRequest, SearchResponse

SEARCH_TOOL_NAME = "flowfile.catalog.search"

SEARCH_TOOL_DESCRIPTION = (
    "Search the Flowfile catalog and workspace by name: catalogs and schemas, tables and their columns, flows, "
    "schedules, notebooks, visualizations, dashboards, global artifacts (stored Python objects), Python kernels and "
    "custom nodes. Every word of the query must match a hit's name, its catalog.schema path, its description or a "
    "kind-specific field (a column name, a kernel package, an artifact's Python type or tags). Results are ranked, "
    "at most limit_per_kind per kind, and only include items the caller may see."
)


def search_tool_definition() -> dict[str, Any]:
    """The MCP ``Tool`` object for the global search."""
    return {
        "name": SEARCH_TOOL_NAME,
        "title": "Search the Flowfile catalog",
        "description": SEARCH_TOOL_DESCRIPTION,
        "inputSchema": SearchRequest.model_json_schema(),
        "outputSchema": SearchResponse.model_json_schema(mode="serialization"),
        "annotations": {"readOnlyHint": True, "idempotentHint": True, "openWorldHint": False},
    }
