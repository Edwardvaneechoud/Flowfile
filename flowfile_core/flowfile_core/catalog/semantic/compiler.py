"""Compile "metric X by these dimensions with these filters" into one SQL statement.

Pure: reads a ``ModelSnapshot`` (``snapshot.py``) and a query, and returns SQL plus what it used. No database, no
FastAPI and no glosslayer import, so it can be called from anywhere and checked on hand-built snapshots.

The emitted shape is fixed::

    SELECT <dimensions>, <metric expression> AS "<metric>"
    FROM (SELECT "<base>".*, <joined columns AS safe aliases>
          FROM "<base table>" AS "<base>" LEFT JOIN ... WHERE <query filters>) AS "<base>"
    WHERE (<metric filter>) GROUP BY <dimensions> ORDER BY ... LIMIT ...

The inner query keeps every base column under its own name and is aliased with the base table's short name, so a
stored expression that says ``amount`` or ``orders.amount`` resolves as written. Joins are followed only from their
many side (``from``) to their one side (``to``), so each base row meets at most one row of a joined table and the
aggregate never fans out.

The expression is not parsed as SQL. A small tokenizer (``find_column_references``) finds its ``table.column`` and
``schema.table.column`` chains, skipping literals, numbers and comments; a chain naming a joined table is projected by
the inner query under a safe alias and exactly that chain is rewritten to read it (``products.price`` →
``"orders"."__gloss_d1"``). Everything else in the expression, bare identifiers included, is left as written.

Free SQL comes only from the stored metric (its ``expression`` and ``filter``); everything taken from a query is an
identifier checked against the snapshot and double-quoted, or a value rendered by ``sql_literal``.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import math
import re
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from flowfile_core.catalog.semantic.snapshot import JoinSnap, MetricSnap, ModelSnapshot, TableInfo, TableSnap

GRAINS = ("day", "week", "month", "quarter", "year")
OPS = ("=", "!=", "<", "<=", ">", ">=", "in", "not in", "between", "is null", "is not null")
DEFAULT_LIMIT = 1000
MAX_LIMIT = 10000
_ALIAS_PREFIX = "__gloss_"
_IDENT = re.compile(r"[^\W\d]\w*")
_NUMBER = re.compile(r"(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
_KEYWORDS = frozenset(
    "SELECT FROM WHERE AND OR NOT CASE WHEN THEN ELSE END AS IN IS NULL LIKE ILIKE BETWEEN DISTINCT FILTER OVER "
    "PARTITION BY ORDER INTERVAL DATE TIMESTAMP TIME CAST TRUE FALSE".split()
)


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


def _schema_table(key: str) -> str:
    return ".".join(key.split(".")[-2:])


def _one_row_of(name: str) -> str:
    """A naive singular for a message (``customers`` → ``customer``); a name that is not plain English stays."""
    if name.endswith("ies") and len(name) > 4:
        return name[:-3] + "y"
    if name.endswith("s") and not name.endswith("ss") and len(name) > 3:
        return name[:-1]
    return f"row of {name}"


@dataclass(frozen=True)
class ColumnReference:
    """A ``table.column`` or ``schema.table.column`` chain in free SQL: its span in the text and its parts."""

    start: int
    end: int
    parts: tuple[str, ...]
    text: str


def _tokens(sql: str) -> list[tuple[str, int, int, str]]:
    """``(kind, start, end, value)`` for each token; whitespace and comments are dropped, quotes unescaped."""
    out: list[tuple[str, int, int, str]] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
        elif sql.startswith("--", i):
            e = sql.find("\n", i)
            i = n if e < 0 else e + 1
        elif sql.startswith("/*", i):
            e = sql.find("*/", i + 2)
            i = n if e < 0 else e + 2
        elif ch in "'\"":
            j, buf = i + 1, []
            while j < n:
                if sql[j] == ch:
                    if j + 1 < n and sql[j + 1] == ch:
                        buf.append(ch)
                        j += 2
                        continue
                    break
                buf.append(sql[j])
                j += 1
            end = min(j + 1, n)
            out.append(("string" if ch == "'" else "qident", i, end, "".join(buf)))
            i = end
        elif ch.isdigit() or (ch == "." and i + 1 < n and sql[i + 1].isdigit()):
            m = _NUMBER.match(sql, i)
            out.append(("number", i, m.end(), m.group()))
            i = m.end()
        elif m := _IDENT.match(sql, i):
            out.append(("ident", i, m.end(), m.group()))
            i = m.end()
        else:
            out.append(("dot" if ch == "." else "punct", i, i + 1, ch))
            i += 1
    return out


def find_column_references(sql: str) -> list[ColumnReference]:
    """The two- and three-part identifier chains of ``sql``, in order.

    A chain is identifiers (bare or double-quoted) joined by dots. One followed by ``(`` is a function call and one
    starting with an unquoted keyword is SQL, so neither counts; a bare identifier is never a reference, and a chain
    of four or more parts is left alone. Text inside string literals, quoted identifiers and comments is never read
    as a chain, so ``'products.price'`` stays a string.
    """
    tokens = _tokens(sql)
    refs: list[ColumnReference] = []
    i = 0
    while i < len(tokens):
        kind, start, _end, value = tokens[i]
        if kind not in ("ident", "qident"):
            i += 1
            continue
        parts, j = [value], i
        while j + 2 < len(tokens) and tokens[j + 1][0] == "dot" and tokens[j + 2][0] in ("ident", "qident"):
            parts.append(tokens[j + 2][3])
            j += 2
        is_call = j + 1 < len(tokens) and tokens[j + 1][3] == "("
        is_keyword = kind == "ident" and value.upper() in _KEYWORDS
        if 2 <= len(parts) <= 3 and not is_call and not is_keyword:
            end = tokens[j][2]
            refs.append(ColumnReference(start=start, end=end, parts=tuple(parts), text=sql[start:end]))
        i = j + 1
    return refs


def _paths_from(snapshot: ModelSnapshot, start: str) -> dict[str, list[JoinSnap]]:
    """Shortest join path from ``start`` to each table it reaches, joins followed from ``from`` to ``to`` only."""
    paths: dict[str, list[JoinSnap]] = {start: []}
    queue = deque([start])
    joins = sorted(snapshot.joins, key=lambda j: (j.name, j.key))
    while queue:
        cur = queue.popleft()
        for j in joins:
            if j.from_table == cur and j.to_table not in paths:
                paths[j.to_table] = [*paths[cur], j]
                queue.append(j.to_table)
    return paths


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
        self.paths = _paths_from(snapshot, base)

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

    def resolve_reference(self, ref: ColumnReference, where: str) -> tuple[str, str]:
        """A chain of a metric's free SQL as ``(dataset key, column)``; a ``ValueError`` naming ``where`` otherwise.

        ``table`` is a short name and ``schema.table`` a key's last two parts. The base table wins its own short
        name; any other table must be reachable many → one, and two reachable tables that answer the same name are
        an error that names both, so the author writes ``schema.table.column``.
        """
        *table_parts, column = ref.parts
        at = f"{ref.text} in {where}"
        usable = ", ".join(self.reachable_names())

        def named(key: str) -> bool:
            return key.split(".")[-len(table_parts) :] == table_parts

        if len(table_parts) == 1 and table_parts[0] == _short(self.base):
            key = self.base
        else:
            matches = [k for k in self.paths if named(k)]
            if len(matches) > 1:
                both = " and ".join(_schema_table(k) for k in matches)
                raise ValueError(
                    f"{'.'.join(table_parts)!r} could mean {both} ({at}); write schema.table.column to choose one"
                )
            if not matches:
                known = [k for k in self.columns if named(k)]
                if not known:
                    name = ".".join(table_parts)
                    raise ValueError(f"Unknown table {name!r} ({at}). Tables this metric can use: {usable}")
                other, base = _short(known[0]), _short(self.base)
                if self.base in _paths_from(self.snapshot, known[0]):
                    raise ValueError(
                        f"{other} has many rows per {_one_row_of(base)}; a metric on {base} cannot use a column of "
                        f"{other} ({at})"
                    )
                raise ValueError(
                    f"Table {other!r} is not reached from {base} by following joins from their many side to their "
                    f"one side ({at}). Tables this metric can use: {usable}"
                )
            key = matches[0]
        if column not in self.columns.get(key, set()):
            raise ValueError(f"Table {_short(key)!r} has no column {column!r} ({at})")
        return key, column


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

    def rewrite(text: str, where_name: str) -> str:
        """``text`` with each chain naming a joined table read from the inner query's alias for it."""
        out = text
        for ref in reversed(find_column_references(text)):
            key, column = graph.resolve_reference(ref, where_name)
            if key == base and len(ref.parts) == 2:
                continue
            out = out[: ref.start] + outer_ref(key, column) + out[ref.end :]
        return out

    stored_filter = (metric.value("filter") or "").strip()
    apply_filter = bool(stored_filter) and _norm(stored_filter) not in _norm(expression)
    expression_sql = rewrite(expression, "the expression")
    filter_sql = rewrite(stored_filter, "the filter") if apply_filter else ""

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
    outer_select.append(f"{expression_sql} AS {quote_ident(metric_name)}")
    sql = [f"SELECT {', '.join(outer_select)}", "FROM (" + " ".join(inner) + f") AS {qb}"]
    if apply_filter:
        sql.append(f"WHERE ({filter_sql})")
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


def with_candidate(snapshot: ModelSnapshot, table: TableInfo, metric: MetricSnap) -> ModelSnapshot:
    """``snapshot`` with ``metric`` on ``table`` in place of any metric of that name, for dry-running a proposal.

    The wider snapshot carries the joins and the reachable tables' columns, so a formula that reads a joined table
    compiles the way it will once stored. A snapshot without the table falls back to ``candidate_snapshot``.
    """
    if snapshot.table(table.key) is None:
        return candidate_snapshot(table, metric)
    tables = [
        replace(
            t,
            metrics=[m for m in t.metrics if m.name != metric.name] + ([metric] if t.key == table.key else []),
        )
        for t in snapshot.tables
    ]
    return replace(snapshot, tables=tables)


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
