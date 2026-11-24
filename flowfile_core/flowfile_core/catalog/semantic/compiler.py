"""Compile "metric X by these dimensions with these filters" into one SQL statement.

Pure: reads a ``ModelSnapshot`` (``snapshot.py``) and a query, and returns SQL plus what it used. No database, no
FastAPI and no glosslayer import, so it can be called from anywhere and checked on hand-built snapshots.

The emitted shape is fixed::

    SELECT <dimensions>, <metric expression> AS "<metric>"
    FROM (SELECT "<base>".*, <joined columns AS safe aliases>
          FROM "<base table>" AS "<base>" LEFT JOIN ... WHERE <query filters>) AS "<base>"
    WHERE (<metric filter>) GROUP BY <dimensions> ORDER BY ... LIMIT ...

The inner query keeps every base column under its own name and is aliased with the base table's short name, so a
stored expression that says ``amount`` or ``orders.amount`` resolves without being parsed. Joins are followed only
from their many side (``from``) to their one side (``to``), so each base row meets at most one row of a joined table
and the aggregate never fans out.

Free SQL comes only from the stored metric (its ``expression`` and ``filter``); everything taken from a query is an
identifier checked against the snapshot and double-quoted, or a value rendered by ``sql_literal``.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import math
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Protocol

from flowfile_core.catalog.semantic.snapshot import JoinSnap, MetricSnap, ModelSnapshot, TableInfo, TableSnap

GRAINS = ("day", "week", "month", "quarter", "year")
OPS = ("=", "!=", "<", "<=", ">", ">=", "in", "not in", "between", "is null", "is not null")
DEFAULT_LIMIT = 1000
MAX_LIMIT = 10000
_ALIAS_PREFIX = "__gloss_"


class _Filter(Protocol):
    field: str
    op: str
    value: Any


@dataclass(frozen=True)
class CompiledQuery:
    """The SQL, the dataset keys it reads (base first), the metric's name and version, and the output columns."""

    sql: str
    tables: list[str]
    metric_name: str
    metric_version: int | None
    columns: list[str]
    metric: MetricSnap | None = field(default=None, compare=False, repr=False)


def quote_ident(name: str) -> str:
    """``name`` as one double-quoted SQL identifier, embedded quotes doubled."""
    return '"' + str(name).replace('"', '""') + '"'


def sql_literal(value: Any) -> str:
    """A Python value as a SQL literal: strings escaped, numbers, booleans, dates, datetimes, ``None`` and lists."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float | decimal.Decimal):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{value!r} is not a number a filter can compare with")
        return repr(float(value)) if isinstance(value, float) else str(value)
    if isinstance(value, _dt.datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, _dt.date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, list | tuple):
        if not value:
            raise ValueError("A list filter needs at least one value")
        return "(" + ", ".join(sql_literal(v) for v in value) + ")"
    raise ValueError(f"A filter value must be text, a number, a boolean, a date or a list, not {type(value).__name__}")


def _short(key: str) -> str:
    return key.rsplit(".", 1)[-1]


def _norm(sql: str) -> str:
    return re.sub(r"\s+", "", sql).lower()


@dataclass
class _Hop:
    alias: str
    table: str
    join: JoinSnap
    parent: str


class _Graph:
    """The tables a base table reaches by following joins from ``from`` to ``to``, nearest first."""

    def __init__(self, snapshot: ModelSnapshot, base: str):
        self.snapshot = snapshot
        self.base = base
        self.columns = _column_index(snapshot)
        self.paths: dict[str, list[JoinSnap]] = {base: []}
        queue = deque([base])
        joins = sorted(snapshot.joins, key=lambda j: (j.name, j.key))
        while queue:
            cur = queue.popleft()
            for j in joins:
                if j.from_table == cur and j.to_table not in self.paths:
                    self.paths[j.to_table] = [*self.paths[cur], j]
                    queue.append(j.to_table)

    def reachable_names(self) -> list[str]:
        return [_short(k) for k in self.paths]

    def resolve(self, ref: str, what: str) -> tuple[str, str]:
        """``"table.column"`` or a bare base column as ``(dataset key, column)``; a ``ValueError`` otherwise."""
        if not isinstance(ref, str) or not ref.strip():
            raise ValueError(f"A {what} must name a column")
        base_cols = self.columns.get(self.base, set())
        if ref in base_cols:
            return self.base, ref
        if "." in ref:
            table_name, column = ref.rsplit(".", 1)
            matches = [k for k in self.paths if table_name in (k, _short(k))]
            if not matches:
                known = [k for k in self.columns if table_name in (k, _short(k))]
                reach = ", ".join(self.reachable_names())
                if known:
                    raise ValueError(
                        f"The {what} {ref!r} is on table {table_name!r}, which {_short(self.base)!r} does not reach "
                        f"by following joins from their many side to their one side. Reachable tables: {reach}"
                    )
                raise ValueError(f"Unknown table {table_name!r} in {what} {ref!r}. Reachable tables: {reach}")
            key = matches[0]
            if column not in self.columns.get(key, set()):
                raise ValueError(f"Table {_short(key)!r} has no column {column!r} (in {what} {ref!r})")
            return key, column
        raise ValueError(
            f"Table {_short(self.base)!r} has no column {ref!r}; write a column of a joined table as 'table.column' "
            f"(reachable tables: {', '.join(self.reachable_names())})"
        )


def _column_index(snapshot: ModelSnapshot) -> dict[str, set[str]]:
    index: dict[str, set[str]] = {}
    for t in snapshot.tables:
        index[t.key] = {c.name for c in t.columns}
    for key, info in snapshot.related_tables.items():
        index.setdefault(key, {c for c, _ in info.columns})
    return index


def find_metric(snapshot: ModelSnapshot, name: str, tables: set[str] | None = None) -> MetricSnap:
    """The metric called ``name`` on one of ``tables`` (dataset keys; any table when ``None``)."""
    found = [m for m in snapshot.metrics if m.name == name and (tables is None or m.table in tables)]
    if not found:
        known = sorted({m.name for m in snapshot.metrics if tables is None or m.table in tables})
        hint = f" Known metrics: {', '.join(known)}" if known else " This model has no metrics yet."
        raise ValueError(f"No metric called {name!r} in this model.{hint}")
    if len(found) > 1:
        raise ValueError(f"Metric {name!r} exists on several tables: {', '.join(m.table for m in found)}")
    return found[0]


def compile_metric_query(
    snapshot: ModelSnapshot,
    query: Any,
    *,
    metric_tables: set[str] | None = None,
    table_names: dict[str, str] | None = None,
) -> CompiledQuery:
    """Compile ``query`` (a ``MetricQuery`` or anything with its attributes) against ``snapshot``.

    ``metric_tables`` limits which tables' metrics may be asked for (the requested namespace's, when the snapshot
    covers the whole catalog). Tables are written by their dataset key (``catalog.schema.table``, one quoted
    identifier, which is how the catalog SQL path registers them) unless ``table_names`` maps a key to another name
    (a saved flow's ``input_1``). Raises ``ValueError`` with an actionable message for anything that cannot compile.
    """
    names_for = table_names or {}
    metric = find_metric(snapshot, query.metric, metric_tables)
    expression = (metric.expression or "").strip()
    if not expression:
        raise ValueError(f"Metric {metric.name!r} has no expression")
    base = metric.table
    graph = _Graph(snapshot, base)
    if base not in graph.columns:
        raise ValueError(f"Metric {metric.name!r} belongs to table {base!r}, which is not in this model")

    allowed = [d for d in (metric.value("dimensions") or []) if isinstance(d, str)]
    base_alias = _short(base)
    qb = quote_ident(base_alias)
    needed: dict[str, _Hop] = {}
    joined_cols: dict[tuple[str, str], str] = {}

    def table_alias(key: str) -> str:
        path = graph.paths[key]
        parent = base
        for j in path:
            if j.to_table not in needed:
                needed[j.to_table] = _Hop(alias=f"j{len(needed) + 1}", table=j.to_table, join=j, parent=parent)
            parent = j.to_table
        return needed[key].alias

    def inner_ref(key: str, column: str) -> str:
        """The column as the inner query reads it."""
        if key == base:
            return f"{qb}.{quote_ident(column)}"
        return f"{quote_ident(table_alias(key))}.{quote_ident(column)}"

    def outer_ref(key: str, column: str) -> str:
        """The column as the outer query reads it: a base column by name, a joined one by its safe alias."""
        if key == base:
            return f"{qb}.{quote_ident(column)}"
        if (key, column) not in joined_cols:
            table_alias(key)
            joined_cols[(key, column)] = f"{_ALIAS_PREFIX}d{len(joined_cols) + 1}"
        return f"{qb}.{quote_ident(joined_cols[(key, column)])}"

    def check_allowed(ref: str, key: str, column: str) -> None:
        if not allowed:
            return
        names = {column if key == base else None, f"{_short(key)}.{column}", f"{key}.{column}"}
        if not names & set(allowed):
            raise ValueError(
                f"Metric {metric.name!r} may only be broken down by {', '.join(allowed)}; {ref!r} is not one of them"
            )

    select: list[tuple[str, str]] = []
    grain = getattr(query, "grain", None)
    if grain:
        if grain not in GRAINS:
            raise ValueError(f"Unknown grain {grain!r}; use one of {', '.join(GRAINS)}")
        time_column = metric.value("time_column")
        if not time_column:
            raise ValueError(f"Metric {metric.name!r} has no time column, so it cannot be broken down by {grain}")
        key, column = graph.resolve(time_column, "time column")
        select.append((f"DATE_TRUNC({sql_literal(grain)}, {outer_ref(key, column)})", f"{column}_{grain}"))

    seen: set[str] = set()
    for ref in getattr(query, "dimensions", None) or []:
        key, column = graph.resolve(ref, "dimension")
        check_allowed(ref, key, column)
        if ref in seen:
            continue
        seen.add(ref)
        select.append((outer_ref(key, column), ref))

    where: list[str] = []
    for f in getattr(query, "filters", None) or []:
        where.append(_render_filter(f, graph, inner_ref))

    names = [name for _expr, name in select]
    metric_name = metric.name
    if metric_name in names:
        raise ValueError(f"A dimension may not share the metric's name {metric_name!r}")

    order = getattr(query, "order", None)
    if order is not None:
        if order.field not in (*names, metric_name):
            raise ValueError(f"Order by a dimension or the metric; {order.field!r} is neither")
        direction = "DESC" if str(order.direction).lower() == "desc" else "ASC"
        order_sql = f"{quote_ident(order.field)} {direction}"
    elif names:
        order_sql = f"{quote_ident(names[0])} ASC"
    else:
        order_sql = None

    limit = getattr(query, "limit", None)
    limit = DEFAULT_LIMIT if limit is None else int(limit)
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"The limit must be between 1 and {MAX_LIMIT}")

    inner_select = [f"{qb}.*"] + [inner_ref(k, c) + f" AS {quote_ident(a)}" for (k, c), a in joined_cols.items()]
    inner = [f"SELECT {', '.join(inner_select)}", f"FROM {quote_ident(names_for.get(base, base))} AS {qb}"]
    for hop in needed.values():
        parent = qb if hop.parent == base else quote_ident(needed[hop.parent].alias)
        on = " AND ".join(
            f"{parent}.{quote_ident(fc)} = {quote_ident(hop.alias)}.{quote_ident(tc)}"
            for fc, tc in zip(hop.join.from_columns, hop.join.to_columns, strict=True)
        )
        inner.append(
            f"LEFT JOIN {quote_ident(names_for.get(hop.table, hop.table))} AS {quote_ident(hop.alias)} ON {on}"
        )
    if where:
        inner.append("WHERE " + " AND ".join(where))

    outer_select = [f"{expr} AS {quote_ident(name)}" for expr, name in select]
    outer_select.append(f"{expression} AS {quote_ident(metric_name)}")
    sql = [f"SELECT {', '.join(outer_select)}", "FROM (" + " ".join(inner) + f") AS {qb}"]
    stored_filter = (metric.value("filter") or "").strip()
    if stored_filter and _norm(stored_filter) not in _norm(expression):
        sql.append(f"WHERE ({stored_filter})")
    if select:
        sql.append("GROUP BY " + ", ".join(expr for expr, _ in select))
    if order_sql:
        sql.append(f"ORDER BY {order_sql}")
    sql.append(f"LIMIT {limit}")
    return CompiledQuery(
        sql="\n".join(sql),
        tables=[base, *needed],
        metric_name=metric_name,
        metric_version=metric.version,
        columns=[*names, metric_name],
        metric=metric,
    )


def _render_filter(f: _Filter, graph: _Graph, inner_ref) -> str:
    key, column = graph.resolve(f.field, "filter field")
    ref = inner_ref(key, column)
    op = str(f.op).lower()
    value = f.value
    if op not in OPS:
        raise ValueError(f"Unknown filter operator {f.op!r}; use one of {', '.join(OPS)}")
    if op in ("is null", "is not null"):
        return f"{ref} {op.upper()}"
    if op in ("in", "not in"):
        if not isinstance(value, list | tuple) or not value:
            raise ValueError(f"The {op!r} filter on {f.field!r} needs a non-empty list of values")
        return f"{ref} {op.upper()} {sql_literal(list(value))}"
    if op == "between":
        if not isinstance(value, list | tuple) or len(value) != 2:
            raise ValueError(f"The 'between' filter on {f.field!r} needs a list of two values")
        return f"{ref} BETWEEN {sql_literal(value[0])} AND {sql_literal(value[1])}"
    if isinstance(value, list | tuple) or value is None:
        raise ValueError(f"The {op!r} filter on {f.field!r} needs one value; use 'is null' to match missing values")
    return f"{ref} {'<>' if op == '!=' else op} {sql_literal(value)}"


def candidate_snapshot(table: TableInfo, metric: MetricSnap) -> ModelSnapshot:
    """A one-table snapshot holding only ``metric``, for dry-running a proposed definition."""
    from flowfile_core.catalog.semantic.snapshot import ColumnSnap

    ts = TableSnap(
        table_id=table.table_id,
        key=table.key,
        name=table.name,
        description=table.description,
        columns=[ColumnSnap(name=c, dtype=d) for c, d in table.columns],
        metrics=[metric],
    )
    return ModelSnapshot(namespace_id=0, level=1, path=table.key.rsplit(".", 1)[0], tables=[ts])
