"""One provider per searchable kind: which rows can match a query, and what a hit on each says.

A provider narrows its rows in SQL with the scoring rule's own shape (``SearchContext.match_filter``: every token in
one of the row's text columns, or the row living in a namespace whose name holds that token), applies the caller's
visibility with the same rule as the kind's list endpoint, and returns ``Candidate``s. The SQL filter is a superset of
what scores, so it never drops a real match, but rows it keeps that do not score can still take candidate-cap slots;
a kind whose cap was reached is therefore reported as truncated. The service scores them
(``matching.score_fields``); a provider never scores. Every provider reads metadata columns only: no table data, no
file on disk, no Docker.

In multi-user mode a hit carries only what the caller may see: a schedule's flow, an artifact's source flow and a
chart's table are named only when the caller can read them, and a namespace's name appears in a path, or matches a
query, only when the catalog tree would show it to the caller (``SearchContext.known``).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import and_, false, func, or_, select, true
from sqlalchemy.orm import Session

from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.repository import tables_kept_here
from flowfile_core.database.models import (
    CatalogDashboard,
    CatalogNamespace,
    CatalogNotebook,
    CatalogTable,
    CatalogVisualization,
    FlowRegistration,
    FlowSchedule,
    GlobalArtifact,
    Kernel,
)
from flowfile_core.search.matching import has_inner_separator, like_pattern, match_quality, sql_narrowable
from flowfile_core.search.models import SearchKind
from flowfile_core.search.resources import resource_uri

MAX_CANDIDATES = 2000
# Stored-schema JSON keys match common words, so up to this many times the cap is scanned for columns.
COLUMN_SCAN_FACTOR = 10
# A one-letter word matches every stored schema's JSON keys, so columns need a word at least this long.
MIN_COLUMN_TOKEN_LENGTH = 2

# The kinds the catalog tree nests under namespaces, which decide the namespaces it shows a restricted caller.
_TREE_ITEM_MODELS = (
    ("catalog_table", CatalogTable),
    ("flow", FlowRegistration),
    ("visualization", CatalogVisualization),
    ("catalog_notebook", CatalogNotebook),
    ("global_artifact", GlobalArtifact),
)

Fields = list[tuple[str, str | None]]


@dataclass
class Candidate:
    """A row that may match: its fields (name first) and the ``SearchHit`` fields to build when it does."""

    fields: Fields
    hit: dict[str, Any]
    require_name_match: bool = False
    weight: float = 1.0


@dataclass(frozen=True)
class _Namespace:
    id: int
    name: str
    parent_id: int | None
    level: int
    description: str | None
    updated_at: datetime | None


@dataclass
class SearchContext:
    """What every provider of one search shares: the session, the caller's access, the tokens, the namespaces.

    ``known`` holds the namespaces the catalog tree shows a restricted caller (``None``: every namespace): the ones
    they own, were granted or that are public, plus the ancestors of those and of every tree item they may read.
    ``namespace_hits`` are the namespaces a search may return as hits, and ``described`` those whose description
    it may show; an ancestor kept only as a breadcrumb is neither, as in the tree.
    """

    db: Session
    access: AccessResolver
    user_id: int
    tokens: list[str]
    namespaces: dict[int, _Namespace]
    known: set[int] | None = None
    namespace_hits: set[int] | None = None
    described: set[int] | None = None
    token_namespaces: dict[str, set[int]] = field(default_factory=dict)
    max_candidates: int = MAX_CANDIDATES
    truncated: list[SearchKind] = field(default_factory=list)

    @classmethod
    def build(
        cls,
        db: Session,
        access: AccessResolver,
        user_id: int,
        tokens: list[str],
        max_candidates: int,
        namespaced: bool = True,
    ) -> SearchContext:
        """``namespaced`` false (only kinds that live outside the catalog tree are searched) skips namespace work."""
        if not namespaced:
            return cls(db, access, user_id, tokens, {}, max_candidates=max_candidates)
        rows = db.query(
            CatalogNamespace.id,
            CatalogNamespace.name,
            CatalogNamespace.parent_id,
            CatalogNamespace.level,
            CatalogNamespace.description,
            CatalogNamespace.updated_at,
        ).all()
        ctx = cls(
            db, access, user_id, tokens, {row.id: _Namespace(*row) for row in rows}, max_candidates=max_candidates
        )
        if access.restricted:
            ctx._restrict_namespaces()
        ctx.token_namespaces = {
            token: {
                ns_id
                for ns_id in ctx.namespaces
                if ctx.knows(ns_id) and match_quality(token, ".".join(ctx.path(ns_id)))
            }
            for token in tokens
        }
        return ctx

    def _restrict_namespaces(self) -> None:
        visible = self.access.visible_namespace_ids()
        holding: set[int] = set()
        for resource_type, model in _TREE_ITEM_MODELS:
            readable = self.access.accessible_ids(resource_type)
            if not readable:
                continue
            query = self.db.query(model.namespace_id).filter(model.namespace_id.isnot(None), model.id.in_(readable))
            if model is GlobalArtifact:
                query = query.filter(GlobalArtifact.status == "active")
            holding |= {namespace_id for (namespace_id,) in query.distinct()}
        shown = self._with_ancestors(visible | holding)
        self.known = shown
        self.namespace_hits = visible | (set(self.access.namespace_permissions()) & self._with_ancestors(holding))
        self.described = visible

    def _with_ancestors(self, namespace_ids: set[int]) -> set[int]:
        out: set[int] = set()
        for namespace_id in namespace_ids:
            current = namespace_id
            while current is not None and current in self.namespaces and current not in out:
                out.add(current)
                current = self.namespaces[current].parent_id
        return out

    def knows(self, namespace_id: int | None) -> bool:
        return namespace_id is not None and (self.known is None or namespace_id in self.known)

    @property
    def restricted(self) -> bool:
        return self.access.restricted

    def accessible(self, resource_type: str) -> set[int] | None:
        """Ids the caller may read of ``resource_type``; ``None`` when nothing is restricted."""
        return self.access.accessible_ids(resource_type) if self.restricted else None

    def path(self, namespace_id: int | None) -> list[str]:
        """The catalog and schema names of ``namespace_id``, outermost first; empty when the caller may not see
        ``namespace_id`` itself, so a hidden schema never reads as its catalog."""
        names: list[str] = []
        seen: set[int] = set()
        while namespace_id is not None and namespace_id in self.namespaces and namespace_id not in seen:
            if not self.knows(namespace_id):
                return []
            seen.add(namespace_id)
            ns = self.namespaces[namespace_id]
            names.insert(0, ns.name)
            namespace_id = ns.parent_id
        return names

    def match_filter(
        self,
        columns: Iterable[Any],
        namespace_column: Any = None,
        extra: Callable[[str, str], list[Any]] | None = None,
    ) -> Any:
        """Every token in one of ``columns`` (case-insensitive), or the row in a namespace whose path holds it.

        ``extra(token, pattern)`` adds a kind's own ways for a token to match. A token SQL cannot narrow by
        (``matching.sql_narrowable``) adds no condition; scoring still requires it.
        """
        columns = list(columns)
        clauses = []
        for token in self.tokens:
            if not sql_narrowable(token):
                continue
            pattern = like_pattern(token)
            options = [column.ilike(pattern, escape="\\") for column in columns]
            namespace_ids = self.token_namespaces.get(token)
            if namespace_column is not None and namespace_ids:
                options.append(namespace_column.in_(namespace_ids))
            if extra is not None:
                options.extend(extra(token, pattern))
            clauses.append(or_(*options) if options else false())
        return and_(*clauses) if clauses else true()

    def capped(self, kind: SearchKind, query) -> list:
        rows = query.limit(self.max_candidates + 1).all()
        if len(rows) > self.max_candidates:
            self.mark_truncated(kind)
            rows = rows[: self.max_candidates]
        return rows

    def mark_truncated(self, kind: SearchKind) -> None:
        if kind not in self.truncated:
            self.truncated.append(kind)


def _located(ctx: SearchContext, kind: SearchKind, item_id: int | str, name: str, namespace_id: int | None) -> dict:
    """The ``SearchHit`` fields every catalog item shares."""
    path = ctx.path(namespace_id)
    return {
        "kind": kind,
        "id": item_id,
        "name": name,
        "qualified_name": ".".join([*path, name]) if path else name,
        "namespace_id": namespace_id if ctx.knows(namespace_id) else None,
        "path": path,
        "uri": resource_uri(kind, item_id),
    }


def _ref(kind: SearchKind, item_id: int | str, name: str) -> dict:
    return {"kind": kind, "id": item_id, "name": name, "uri": resource_uri(kind, item_id)}


def _json_list(raw: str | None) -> list:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return value if isinstance(value, list) else []


def _count(n: int | None, noun: str) -> str | None:
    if n is None:
        return None
    return f"{n:,} {noun}{'' if n == 1 else 's'}"


def _readable(ids: Iterable[int | None], allowed: set[int] | None) -> set[int]:
    """The non-null ``ids`` the caller may read (``allowed`` is ``None`` when nothing is restricted)."""
    return {i for i in ids if i is not None and (allowed is None or i in allowed)}


def _flow_names(db: Session, ids: set[int]) -> dict[int, str]:
    if not ids:
        return {}
    return dict(db.query(FlowRegistration.id, FlowRegistration.name).filter(FlowRegistration.id.in_(ids)).all())


def _path_text(hit: dict) -> str:
    return ".".join(hit["path"])


def namespaces(ctx: SearchContext) -> list[Candidate]:
    """Namespaces the caller owns, was granted or that are public, and the schemas a catalog's grant covers that the
    tree shows (those holding something the caller may read), as the tree shows them: a covered schema without its
    description. An ancestor the tree keeps only as the breadcrumb of a granted item is no hit, though its name is in
    that item's path.
    """
    out = []
    for ns in ctx.namespaces.values():
        if ctx.namespace_hits is not None and ns.id not in ctx.namespace_hits:
            continue
        description = ns.description if ctx.described is None or ns.id in ctx.described else None
        parent = ctx.namespaces.get(ns.parent_id) if ctx.knows(ns.parent_id) else None
        hit = _located(ctx, SearchKind.NAMESPACE, ns.id, ns.name, ns.parent_id)
        hit.update(
            description=description,
            subtitle="Catalog" if ns.level == 0 else f"Schema in {parent.name}" if parent else "Schema",
            updated_at=ns.updated_at,
            details={"level": ns.level},
        )
        fields: Fields = [("name", ns.name), ("path", parent.name if parent else None), ("description", description)]
        out.append(Candidate(fields, hit))
    return out


def _table_subtitle(row) -> str:
    if row.table_type == "virtual":
        return "Virtual table · SQL query" if row.has_sql else "Virtual table · from a flow"
    parts = [
        f"{(row.storage_format or 'delta').capitalize()} table",
        _count(row.row_count, "row"),
        _count(row.column_count, "column"),
    ]
    return " · ".join(p for p in parts if p)


def tables(ctx: SearchContext) -> list[Candidate]:
    query = ctx.db.query(
        CatalogTable.id,
        CatalogTable.name,
        CatalogTable.namespace_id,
        CatalogTable.description,
        CatalogTable.table_type,
        CatalogTable.storage_format,
        CatalogTable.row_count,
        CatalogTable.column_count,
        CatalogTable.size_bytes,
        CatalogTable.updated_at,
        CatalogTable.sql_query.isnot(None).label("has_sql"),
    ).filter(
        ctx.match_filter([CatalogTable.name, CatalogTable.description], CatalogTable.namespace_id),
        tables_kept_here(),
    )
    allowed = ctx.accessible("catalog_table")
    if allowed is not None:
        query = query.filter(CatalogTable.id.in_(allowed))
    out = []
    for row in ctx.capped(SearchKind.TABLE, query.order_by(CatalogTable.updated_at.desc(), CatalogTable.id.desc())):
        hit = _located(ctx, SearchKind.TABLE, row.id, row.name, row.namespace_id)
        hit.update(
            description=row.description,
            subtitle=_table_subtitle(row),
            updated_at=row.updated_at,
            details={
                "table_type": row.table_type,
                "storage_format": row.storage_format,
                "row_count": row.row_count,
                "column_count": row.column_count,
                "size_bytes": row.size_bytes,
            },
        )
        fields: Fields = [("name", row.name), ("path", _path_text(hit)), ("description", row.description)]
        out.append(Candidate(fields, hit))
    return out


def _tables_spanned_by(ctx: SearchContext) -> Callable[[str, str], list[Any]]:
    """An ``extra`` clause for a token that spans a column's path and its table's name (``sales_orders`` for the
    table ``orders`` in ``main.sales``), which neither the table name nor a namespace path holds alone."""
    names: list[tuple[int, str, int | None]] | None = None

    def extra(token: str, pattern: str) -> list[Any]:
        nonlocal names
        if not has_inner_separator(token):
            return []
        if names is None:
            names = ctx.db.query(CatalogTable.id, CatalogTable.name, CatalogTable.namespace_id).all()
        ids = {
            table_id
            for table_id, name, namespace_id in names
            if match_quality(token, ".".join([*ctx.path(namespace_id), name]))
        }
        return [CatalogTable.id.in_(ids)] if ids else []

    return extra


def columns(ctx: SearchContext) -> list[Candidate]:
    """Columns of the caller's tables, parsed from each table's stored schema; a hit needs a token in its name.

    Tables are read newest first until ``max_candidates`` of them hold a matching column, reading at most
    ``COLUMN_SCAN_FACTOR`` times that many: a stored schema's JSON keys match common words, so the SQL filter alone
    would let tables without a matching column fill the cap. A query whose words are all shorter than
    ``MIN_COLUMN_TOKEN_LENGTH`` finds no columns, since such a word matches every stored schema.
    """
    if all(len(token) < MIN_COLUMN_TOKEN_LENGTH for token in ctx.tokens):
        return []
    query = ctx.db.query(
        CatalogTable.id, CatalogTable.name, CatalogTable.namespace_id, CatalogTable.schema_json, CatalogTable.updated_at
    ).filter(
        CatalogTable.schema_json.isnot(None),
        tables_kept_here(),
        ctx.match_filter(
            [CatalogTable.schema_json, CatalogTable.name], CatalogTable.namespace_id, extra=_tables_spanned_by(ctx)
        ),
    )
    allowed = ctx.accessible("catalog_table")
    if allowed is not None:
        query = query.filter(CatalogTable.id.in_(allowed))
    ordered = query.order_by(CatalogTable.updated_at.desc(), CatalogTable.id.desc())
    scan_limit = ctx.max_candidates * COLUMN_SCAN_FACTOR
    out: list[Candidate] = []
    scanned = matched_tables = 0
    for row in ordered.limit(scan_limit + 1).yield_per(500):
        scanned += 1
        if scanned > scan_limit:
            ctx.mark_truncated(SearchKind.COLUMN)
            break
        named = [
            column
            for column in _json_list(row.schema_json)
            if isinstance(column, dict)
            and isinstance(column.get("name"), str)
            and any(match_quality(token, column["name"]) for token in ctx.tokens)
        ]
        if not named:
            continue
        matched_tables += 1
        if matched_tables > ctx.max_candidates:
            ctx.mark_truncated(SearchKind.COLUMN)
            break
        path = ctx.path(row.namespace_id)
        table_name = ".".join([*path, row.name])
        parent = _ref(SearchKind.TABLE, row.id, row.name)
        for column in named:
            name, dtype = column["name"], str(column.get("dtype") or "")
            column_id = f"{row.id}:{name}"
            hit = {
                "kind": SearchKind.COLUMN,
                "id": column_id,
                "name": name,
                "qualified_name": f"{table_name}.{name}",
                "namespace_id": row.namespace_id if ctx.knows(row.namespace_id) else None,
                "path": [*path, row.name],
                "subtitle": dtype or None,
                "parent": parent,
                "uri": resource_uri(SearchKind.COLUMN, column_id, table_id=row.id, name=name),
                "updated_at": row.updated_at,
                "details": {"dtype": dtype, "table_id": row.id},
            }
            fields: Fields = [("name", name), ("path", table_name), ("dtype", dtype)]
            out.append(Candidate(fields, hit, require_name_match=True, weight=0.9))
    return out


def flows(ctx: SearchContext) -> list[Candidate]:
    query = ctx.db.query(
        FlowRegistration.id,
        FlowRegistration.name,
        FlowRegistration.description,
        FlowRegistration.namespace_id,
        FlowRegistration.flow_path,
        FlowRegistration.is_api_compatible,
        FlowRegistration.updated_at,
    ).filter(ctx.match_filter([FlowRegistration.name, FlowRegistration.description], FlowRegistration.namespace_id))
    allowed = ctx.accessible("flow")
    if allowed is not None:
        query = query.filter(FlowRegistration.id.in_(allowed))
    out = []
    ordered = query.order_by(FlowRegistration.updated_at.desc(), FlowRegistration.id.desc())
    for row in ctx.capped(SearchKind.FLOW, ordered):
        hit = _located(ctx, SearchKind.FLOW, row.id, row.name, row.namespace_id)
        file_name = Path(row.flow_path).name if row.flow_path else None
        hit.update(
            description=row.description,
            subtitle=" · ".join(p for p in ("Flow", file_name, "data API" if row.is_api_compatible else None) if p),
            updated_at=row.updated_at,
            details={"flow_path": row.flow_path, "is_api_compatible": bool(row.is_api_compatible)},
        )
        fields: Fields = [("name", row.name), ("path", _path_text(hit)), ("description", row.description)]
        out.append(Candidate(fields, hit))
    return out


def _interval_text(seconds: int | None) -> str:
    if not seconds:
        return "On an interval"
    for unit, size in (("day", 86400), ("hour", 3600), ("minute", 60)):
        if seconds % size == 0:
            n = seconds // size
            return f"Every {unit}" if n == 1 else f"Every {n} {unit}s"
    return f"Every {seconds} seconds"


def _schedule_subtitle(row) -> str:
    if row.schedule_type == "interval":
        text = _interval_text(row.interval_seconds)
    elif row.schedule_type == "cron":
        text = f"Cron {row.cron_expression}" + (f" ({row.cron_timezone})" if row.cron_timezone else "")
    elif row.schedule_type == "table_trigger":
        text = "When a table is updated"
    elif row.schedule_type == "table_set_trigger":
        text = "When a set of tables is updated"
    else:
        text = row.schedule_type
    return text if row.enabled else f"Paused · {text}"


def schedules(ctx: SearchContext) -> list[Candidate]:
    """Schedules the caller owns or whose flow they may use, as ``GET /catalog/schedules`` lists them.

    The flow's name and place show only when the caller may read the flow: an owner can keep a schedule on a flow
    whose grant was revoked.
    """
    allowed = ctx.accessible("flow")

    def by_flow(token: str, pattern: str) -> list[Any]:
        clause = FlowRegistration.name.ilike(pattern, escape="\\")
        if ctx.token_namespaces.get(token):
            clause = or_(clause, FlowRegistration.namespace_id.in_(ctx.token_namespaces[token]))
        return [clause if allowed is None else and_(FlowSchedule.registration_id.in_(allowed), clause)]

    query = (
        ctx.db.query(
            FlowSchedule.id,
            FlowSchedule.name,
            FlowSchedule.description,
            FlowSchedule.schedule_type,
            FlowSchedule.interval_seconds,
            FlowSchedule.cron_expression,
            FlowSchedule.cron_timezone,
            FlowSchedule.enabled,
            FlowSchedule.registration_id,
            FlowSchedule.updated_at,
            FlowRegistration.name.label("flow_name"),
            FlowRegistration.namespace_id,
        )
        .join(FlowRegistration, FlowRegistration.id == FlowSchedule.registration_id)
        .filter(
            ctx.match_filter(
                [FlowSchedule.name, FlowSchedule.description, FlowSchedule.schedule_type, FlowSchedule.cron_expression],
                extra=by_flow,
            )
        )
    )
    if allowed is not None:
        query = query.filter(or_(FlowSchedule.owner_id == ctx.user_id, FlowSchedule.registration_id.in_(allowed)))
    out = []
    ordered = query.order_by(FlowSchedule.updated_at.desc(), FlowSchedule.id.desc())
    for row in ctx.capped(SearchKind.SCHEDULE, ordered):
        flow_readable = allowed is None or row.registration_id in allowed
        name = row.name or (row.flow_name if flow_readable else "Schedule")
        hit = _located(ctx, SearchKind.SCHEDULE, row.id, name, row.namespace_id if flow_readable else None)
        hit.update(
            qualified_name=None,
            description=row.description,
            subtitle=_schedule_subtitle(row),
            parent=_ref(SearchKind.FLOW, row.registration_id, row.flow_name) if flow_readable else None,
            updated_at=row.updated_at,
            details={
                "enabled": bool(row.enabled),
                "schedule_type": row.schedule_type,
                "interval_seconds": row.interval_seconds,
                "cron_expression": row.cron_expression,
                "cron_timezone": row.cron_timezone,
                "registration_id": row.registration_id,
            },
        )
        fields: Fields = [
            ("name", row.name or (row.flow_name if flow_readable else None)),
            ("flow", row.flow_name if flow_readable and row.name else None),
            ("path", _path_text(hit)),
            ("description", row.description),
            ("type", row.schedule_type),
            ("cron", row.cron_expression),
        ]
        out.append(Candidate(fields, hit))
    return out


def notebooks(ctx: SearchContext) -> list[Candidate]:
    query = ctx.db.query(
        CatalogNotebook.id,
        CatalogNotebook.name,
        CatalogNotebook.description,
        CatalogNotebook.namespace_id,
        CatalogNotebook.default_kernel_id,
        CatalogNotebook.updated_at,
    ).filter(ctx.match_filter([CatalogNotebook.name, CatalogNotebook.description], CatalogNotebook.namespace_id))
    allowed = ctx.accessible("catalog_notebook")
    if allowed is not None:
        query = query.filter(CatalogNotebook.id.in_(allowed))
    out = []
    ordered = query.order_by(CatalogNotebook.updated_at.desc(), CatalogNotebook.id.desc())
    for row in ctx.capped(SearchKind.NOTEBOOK, ordered):
        hit = _located(ctx, SearchKind.NOTEBOOK, row.id, row.name, row.namespace_id)
        hit.update(
            description=row.description,
            subtitle="Notebook" + (f" · kernel {row.default_kernel_id}" if row.default_kernel_id else ""),
            updated_at=row.updated_at,
            details={"default_kernel_id": row.default_kernel_id},
        )
        fields: Fields = [("name", row.name), ("path", _path_text(hit)), ("description", row.description)]
        out.append(Candidate(fields, hit))
    return out


def visualizations(ctx: SearchContext) -> list[Candidate]:
    """Charts the caller may use, or whose table they may read; the table is named only when they may read it.
    A chart that belongs to a dashboard is left out: the dashboard is what a search finds."""
    readable_tables = ctx.accessible("catalog_table")

    def by_table(token: str, pattern: str) -> list[Any]:
        table_clause = CatalogTable.name.ilike(pattern, escape="\\")
        if ctx.token_namespaces.get(token):
            table_clause = or_(table_clause, CatalogTable.namespace_id.in_(ctx.token_namespaces[token]))
        if readable_tables is not None:
            table_clause = and_(CatalogTable.id.in_(readable_tables), table_clause)
        return [CatalogVisualization.catalog_table_id.in_(select(CatalogTable.id).where(table_clause))]

    query = ctx.db.query(
        CatalogVisualization.id,
        CatalogVisualization.name,
        CatalogVisualization.description,
        CatalogVisualization.chart_type,
        CatalogVisualization.source_type,
        CatalogVisualization.catalog_table_id,
        CatalogVisualization.namespace_id,
        CatalogVisualization.updated_at,
    ).filter(
        ctx.match_filter(
            [CatalogVisualization.name, CatalogVisualization.description, CatalogVisualization.chart_type],
            CatalogVisualization.namespace_id,
            extra=by_table,
        ),
        CatalogVisualization.dashboard_id.is_(None),
    )
    if ctx.restricted:
        query = query.filter(
            or_(
                CatalogVisualization.id.in_(ctx.access.accessible_ids("visualization")),
                CatalogVisualization.catalog_table_id.in_(readable_tables),
            )
        )
    ordered = query.order_by(CatalogVisualization.updated_at.desc(), CatalogVisualization.id.desc())
    rows = ctx.capped(SearchKind.VISUALIZATION, ordered)
    table_ids = _readable((row.catalog_table_id for row in rows), readable_tables)
    table_rows = (
        ctx.db.query(CatalogTable.id, CatalogTable.name, CatalogTable.namespace_id)
        .filter(CatalogTable.id.in_(table_ids))
        .all()
        if table_ids
        else []
    )
    source_tables = {t.id: t for t in table_rows}
    out = []
    for row in rows:
        table = source_tables.get(row.catalog_table_id)
        namespace_id = row.namespace_id if row.namespace_id is not None else (table.namespace_id if table else None)
        hit = _located(ctx, SearchKind.VISUALIZATION, row.id, row.name, namespace_id)
        chart = (row.chart_type or "chart").replace("_", " ").capitalize()
        source = f"on {table.name}" if table else "from a SQL query" if row.source_type == "sql" else None
        hit.update(
            description=row.description,
            subtitle=" · ".join(p for p in (chart, source) if p),
            parent=_ref(SearchKind.TABLE, table.id, table.name) if table else None,
            updated_at=row.updated_at,
            details={
                "chart_type": row.chart_type,
                "source_type": row.source_type,
                "table_id": table.id if table else None,
            },
        )
        fields: Fields = [
            ("name", row.name),
            ("path", _path_text(hit)),
            ("description", row.description),
            ("table", table.name if table else None),
            ("type", row.chart_type),
        ]
        out.append(Candidate(fields, hit))
    return out


def dashboards(ctx: SearchContext) -> list[Candidate]:
    query = ctx.db.query(
        CatalogDashboard.id,
        CatalogDashboard.name,
        CatalogDashboard.description,
        CatalogDashboard.namespace_id,
        CatalogDashboard.updated_at,
    ).filter(ctx.match_filter([CatalogDashboard.name, CatalogDashboard.description], CatalogDashboard.namespace_id))
    allowed = ctx.accessible("dashboard")
    if allowed is not None:
        query = query.filter(CatalogDashboard.id.in_(allowed))
    out = []
    ordered = query.order_by(CatalogDashboard.updated_at.desc(), CatalogDashboard.id.desc())
    for row in ctx.capped(SearchKind.DASHBOARD, ordered):
        hit = _located(ctx, SearchKind.DASHBOARD, row.id, row.name, row.namespace_id)
        hit.update(description=row.description, subtitle="Dashboard", updated_at=row.updated_at)
        fields: Fields = [("name", row.name), ("path", _path_text(hit)), ("description", row.description)]
        out.append(Candidate(fields, hit))
    return out


def artifacts(ctx: SearchContext) -> list[Candidate]:
    """The newest active version of each artifact the caller may read, as ``GET /artifacts?latest_only=true``.

    Versions are collapsed in SQL before the cap, and ``versions`` counts every active version the caller may read.
    """
    allowed = ctx.accessible("global_artifact")
    scope = [GlobalArtifact.status == "active"]
    if allowed is not None:
        scope.append(GlobalArtifact.id.in_(allowed))
    newest = (
        select(
            GlobalArtifact.name.label("group_name"),
            GlobalArtifact.namespace_id.label("group_namespace_id"),
            func.max(GlobalArtifact.version).label("group_version"),
            func.count(GlobalArtifact.id).label("group_versions"),
        )
        .where(*scope)
        .group_by(GlobalArtifact.name, GlobalArtifact.namespace_id)
        .subquery()
    )
    query = (
        ctx.db.query(
            GlobalArtifact.id,
            GlobalArtifact.name,
            GlobalArtifact.namespace_id,
            GlobalArtifact.version,
            GlobalArtifact.python_type,
            GlobalArtifact.serialization_format,
            GlobalArtifact.description,
            GlobalArtifact.tags,
            GlobalArtifact.size_bytes,
            GlobalArtifact.source_registration_id,
            GlobalArtifact.updated_at,
            newest.c.group_versions,
        )
        .join(
            newest,
            and_(
                GlobalArtifact.name == newest.c.group_name,
                GlobalArtifact.version == newest.c.group_version,
                or_(
                    GlobalArtifact.namespace_id == newest.c.group_namespace_id,
                    and_(GlobalArtifact.namespace_id.is_(None), newest.c.group_namespace_id.is_(None)),
                ),
            ),
        )
        .filter(
            *scope,
            ctx.match_filter(
                [GlobalArtifact.name, GlobalArtifact.description, GlobalArtifact.python_type, GlobalArtifact.tags],
                GlobalArtifact.namespace_id,
            ),
        )
    )
    ordered = query.order_by(GlobalArtifact.updated_at.desc(), GlobalArtifact.id.desc())
    rows = ctx.capped(SearchKind.ARTIFACT, ordered)
    flow_names = _flow_names(ctx.db, _readable((row.source_registration_id for row in rows), ctx.accessible("flow")))
    out = []
    for row in rows:
        hit = _located(ctx, SearchKind.ARTIFACT, row.id, row.name, row.namespace_id)
        tags = [str(t) for t in _json_list(row.tags)]
        flow_name = flow_names.get(row.source_registration_id)
        hit.update(
            description=row.description,
            subtitle=f"{row.python_type or row.serialization_format} · v{row.version}",
            parent=_ref(SearchKind.FLOW, row.source_registration_id, flow_name) if flow_name else None,
            updated_at=row.updated_at,
            details={
                "version": row.version,
                "versions": row.group_versions,
                "python_type": row.python_type,
                "serialization_format": row.serialization_format,
                "size_bytes": row.size_bytes,
                "tags": ", ".join(tags) or None,
            },
        )
        fields: Fields = [
            ("name", row.name),
            ("path", _path_text(hit)),
            ("description", row.description),
            ("python_type", row.python_type),
            *(("tag", tag) for tag in tags),
        ]
        out.append(Candidate(fields, hit))
    return out


def kernels(ctx: SearchContext) -> list[Candidate]:
    """The caller's own kernels, as ``GET /kernels/`` lists them; read from the database, so Docker is not needed."""
    query = ctx.db.query(
        Kernel.id, Kernel.name, Kernel.packages, Kernel.image_flavour, Kernel.custom_image, Kernel.created_at
    ).filter(Kernel.user_id == ctx.user_id, ctx.match_filter([Kernel.id, Kernel.name, Kernel.packages]))
    out = []
    for row in ctx.capped(SearchKind.KERNEL, query.order_by(Kernel.created_at.desc(), Kernel.id)):
        packages = [str(p) for p in _json_list(row.packages)]
        image = row.custom_image or f"{row.image_flavour} image"
        hit = {
            "kind": SearchKind.KERNEL,
            "id": row.id,
            "name": row.name,
            "subtitle": " · ".join(
                p for p in ("Python kernel", image, _count(len(packages) or None, "extra package")) if p
            ),
            "uri": resource_uri(SearchKind.KERNEL, row.id),
            "updated_at": row.created_at,
            "details": {
                "image_flavour": row.image_flavour,
                "custom_image": row.custom_image,
                "packages": ", ".join(packages) or None,
            },
        }
        fields: Fields = [("name", row.name), ("id", row.id), *(("package", p) for p in packages)]
        out.append(Candidate(fields, hit))
    return out


def _node_file_label(entry) -> str:
    return f"{entry.mount_path.rstrip('/')}/{entry.file_name}" if entry.mount_path else entry.file_name


def custom_nodes(ctx: SearchContext) -> list[Candidate]:
    """Installed custom nodes, from the registry's AST-only scan (no node file is executed); install-wide, like the
    palette. Files starting with ``_`` are left out, as the custom-node list leaves them out. The entry that holds a
    node key is found by that key; another file claiming it (a duplicate, a broken copy) by its file name, under its
    mount folder when mounted (the custom-node lists show both), never by the server's path to the nodes folder."""
    from flowfile_core.flowfile.user_defined.registry import registry

    out = []
    for entry in registry.all(include_broken=True):
        if entry.file_name.startswith("_"):
            continue
        manifest = entry.manifest
        holds_key = bool(entry.node_key) and registry.get(entry.node_key) is entry
        item_id = entry.node_key if holds_key else f"file:{_node_file_label(entry)}"
        name = (manifest.node_name if manifest else None) or entry.node_key or Path(entry.file_name).stem
        category = manifest.node_category if manifest else None
        on_kernel = bool(manifest and manifest.environment.kind == "kernel")
        dependencies = list(manifest.environment.dependencies) if manifest and on_kernel else []
        parts = [
            "Custom node",
            category,
            "runs on a kernel" if on_kernel else None,
            "load error" if entry.load_error else None,
        ]
        hit = {
            "kind": SearchKind.CUSTOM_NODE,
            "id": item_id,
            "name": name,
            "description": (manifest.intro or manifest.title or None) if manifest else None,
            "subtitle": " · ".join(p for p in parts if p),
            "uri": resource_uri(SearchKind.CUSTOM_NODE, item_id),
            "updated_at": datetime.fromtimestamp(entry.mtime, UTC) if entry.mtime else None,
            "details": {
                "node_key": entry.node_key,
                "file_name": entry.file_name,
                "category": category,
                "environment": "kernel" if on_kernel else "local",
                "mounted": entry.mount_path is not None,
                "error": entry.load_error,
            },
        }
        fields: Fields = [
            ("name", name),
            ("id", entry.node_key),
            ("title", manifest.title if manifest else None),
            ("description", manifest.intro if manifest else None),
            ("category", category),
            *(("tag", str(tag)) for tag in (manifest.tags if manifest else [])),
            *(("package", dep) for dep in dependencies),
        ]
        out.append(Candidate(fields, hit))
    return out


PROVIDERS: dict[SearchKind, Callable[[SearchContext], list[Candidate]]] = {
    SearchKind.NAMESPACE: namespaces,
    SearchKind.TABLE: tables,
    SearchKind.COLUMN: columns,
    SearchKind.FLOW: flows,
    SearchKind.SCHEDULE: schedules,
    SearchKind.NOTEBOOK: notebooks,
    SearchKind.VISUALIZATION: visualizations,
    SearchKind.DASHBOARD: dashboards,
    SearchKind.ARTIFACT: artifacts,
    SearchKind.KERNEL: kernels,
    SearchKind.CUSTOM_NODE: custom_nodes,
}
