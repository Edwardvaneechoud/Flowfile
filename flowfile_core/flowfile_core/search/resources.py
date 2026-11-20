"""Stable resource URIs for the items the search finds.

One URI names one item for every client: the renderer keys its results by it, and an MCP server can expose the same
URIs as resources (``RESOURCE_URI_TEMPLATES`` are those resources' templates).
"""

from __future__ import annotations

from urllib.parse import quote

from flowfile_core.search.models import SearchKind

SCHEME = "flowfile"

RESOURCE_URI_TEMPLATES: dict[SearchKind, str] = {
    SearchKind.NAMESPACE: "flowfile://catalog/namespaces/{id}",
    SearchKind.TABLE: "flowfile://catalog/tables/{id}",
    SearchKind.COLUMN: "flowfile://catalog/tables/{table_id}/columns/{name}",
    SearchKind.FLOW: "flowfile://catalog/flows/{id}",
    SearchKind.SCHEDULE: "flowfile://catalog/schedules/{id}",
    SearchKind.NOTEBOOK: "flowfile://catalog/notebooks/{id}",
    SearchKind.VISUALIZATION: "flowfile://catalog/visualizations/{id}",
    SearchKind.DASHBOARD: "flowfile://catalog/dashboards/{id}",
    SearchKind.ARTIFACT: "flowfile://catalog/artifacts/{id}",
    SearchKind.KERNEL: "flowfile://kernels/{id}",
    SearchKind.CUSTOM_NODE: "flowfile://custom-nodes/{id}",
}


def resource_uri(kind: SearchKind, item_id: int | str, *, table_id: int | None = None, name: str | None = None) -> str:
    """The URI of one item; a column takes its ``table_id`` and ``name``."""
    if kind is SearchKind.COLUMN:
        return RESOURCE_URI_TEMPLATES[kind].format(table_id=table_id, name=quote(name or "", safe=""))
    return RESOURCE_URI_TEMPLATES[kind].format(id=quote(str(item_id), safe=""))
