"""Wire models of the global search: what a caller asks for and what it gets back.

These are the contract of every surface that searches: the ``GET /search`` route, the renderer's palette and results
page, and a future MCP tool (``search.tool``), whose input and output schemas are these models' JSON schemas. Every
field carries a description because those descriptions are what an agent reads.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class SearchKind(str, Enum):
    """The kinds of item the search finds."""

    NAMESPACE = "namespace"
    TABLE = "table"
    COLUMN = "column"
    FLOW = "flow"
    SCHEDULE = "schedule"
    NOTEBOOK = "notebook"
    VISUALIZATION = "visualization"
    DASHBOARD = "dashboard"
    ARTIFACT = "artifact"
    KERNEL = "kernel"
    CUSTOM_NODE = "custom_node"


DetailValue = str | int | float | bool | None

MatchField = Literal[
    "name",
    "id",
    "title",
    "path",
    "description",
    "dtype",
    "flow",
    "table",
    "type",
    "cron",
    "python_type",
    "tag",
    "category",
    "package",
]


class SearchRef(BaseModel):
    """A pointer to another item the search can find, such as the table a column belongs to."""

    kind: SearchKind
    id: int | str = Field(description="The item's id within its kind.")
    name: str
    uri: str = Field(description="The item's resource URI (see SearchHit.uri).")


class SearchMatch(BaseModel):
    """One field of an item that matched the query."""

    field: MatchField = Field(
        description="Which field matched. 'name' is the item's own name (a column's for a column hit), 'path' its "
        "catalog.schema path (a column's includes its table), 'flow' a schedule's flow, 'table' a chart's table, "
        "'type' a schedule's or chart's type, 'package' a kernel package or a custom node's dependency."
    )
    value: str = Field(description="The text of that field, cut to 200 characters.")


class SearchHit(BaseModel):
    """One item that matches the query."""

    kind: SearchKind
    id: int | str = Field(description="The item's id within its kind; a column's is '<table id>:<column name>'.")
    name: str
    qualified_name: str | None = Field(
        default=None, description="The dotted catalog name, e.g. 'catalog.schema.table' or 'catalog.schema.table.col'."
    )
    namespace_id: int | None = Field(default=None, description="The catalog or schema the item lives in.")
    path: list[str] = Field(default_factory=list, description="Names of the catalog and schema the item lives in.")
    description: str | None = None
    subtitle: str | None = Field(default=None, description="A short line describing the item, e.g. its row count.")
    parent: SearchRef | None = Field(
        default=None,
        description="The item this one belongs to: a column's table, a schedule's flow, a visualization's table, an "
        "artifact's source flow.",
    )
    matches: list[SearchMatch] = Field(default_factory=list, description="The fields that matched the query.")
    score: float = Field(description="Relevance, higher is better; comparable across kinds within one response.")
    uri: str = Field(
        description="A stable resource URI for the item, e.g. 'flowfile://catalog/tables/12'. The same URI names "
        "the item for any client, including an MCP server's resources."
    )
    updated_at: datetime | None = None
    details: dict[str, DetailValue] = Field(
        default_factory=dict, description="Kind-specific facts, e.g. a table's row_count or a schedule's cron."
    )

    @field_validator("updated_at")
    @classmethod
    def _as_utc(cls, value: datetime | None) -> datetime | None:
        """The catalog stores naive UTC timestamps; answer them with their offset."""
        return value.replace(tzinfo=timezone.utc) if value is not None and value.tzinfo is None else value


class SearchRequest(BaseModel):
    """What to search for."""

    query: str = Field(
        min_length=1,
        max_length=200,
        description="Free text. Every word must match some field of an item (its name, its catalog.schema path, "
        "its description, a column, a package, a tag...); case is ignored and '-', '_' and spaces are "
        "interchangeable. Dots split words, so 'sales.orders' finds the table orders in the schema sales. Words "
        "after the eighth are ignored.",
    )
    kinds: list[SearchKind] | None = Field(default=None, description="Only these kinds; all kinds when omitted.")
    limit_per_kind: int = Field(default=10, ge=1, le=100, description="At most this many hits of each kind.")

    @field_validator("query")
    @classmethod
    def _strip(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("query must contain a non-space character")
        return stripped


class SearchResponse(BaseModel):
    """The items that match, best first."""

    query: str
    tokens: list[str] = Field(description="The words the query was split into; every one matched each hit.")
    kinds: list[SearchKind] = Field(description="The kinds that were searched.")
    hits: list[SearchHit] = Field(description="The matches, best first, at most limit_per_kind of each kind.")
    counts: dict[SearchKind, int] = Field(
        description="How many items of each searched kind matched, including those cut by limit_per_kind."
    )
    truncated: bool = Field(
        default=False,
        description="True when a kind had more candidates than the search examines, so its count is a lower bound.",
    )
    truncated_kinds: list[SearchKind] = Field(
        default_factory=list, description="The kinds whose candidates were cut, so whose counts are lower bounds."
    )
