"""Compile "metric X by these dimensions with these filters" into one SQL statement.

Pure: reads a ``ModelSnapshot`` and a ``MetricQuery`` and returns SQL plus what it used; no database, no glosslayer.
The emitted shape is fixed::

    SELECT <dimensions>, <metric expression> AS "<metric>"
    FROM (SELECT "<base>".*, <joined columns AS safe aliases>
          FROM "<base table>" AS "<base>" LEFT JOIN ... WHERE <query filters>) AS "<base>"
    WHERE (<metric filter>) GROUP BY <dimensions> ORDER BY ... LIMIT ...

The inner query keeps every base column under its own name and the base table's short name, so a stored expression
that says ``amount`` or ``orders.amount`` resolves as written. Joins are followed only from their many side to their
one side, so each base row meets at most one row of a joined table and the aggregate never fans out.

The expression is not parsed as SQL: ``find_column_references`` tokenizes out its ``[schema.]table.column`` chains, and
a chain naming a joined table is projected by the inner query under a safe alias and rewritten to read it
(``products.price`` → ``"orders"."__gloss_d1"``). Everything else in it is left as written.

Free SQL comes only from the stored metric (its ``expression`` and ``filter``); everything taken from a query is an
identifier checked against the snapshot and double-quoted, or a value rendered by ``sql_literal``.
"""

from __future__ import annotations

import math
import re
from collections import deque
from dataclasses import dataclass, replace
from typing import Any

from flowfile_core.catalog.semantic.snapshot import ModelSnapshot
from flowfile_core.schemas.semantic_schema import JoinOut, MetricFilter, MetricOut, MetricQuery

_TOKEN = re.compile(
    r"""(?P<skip>\s+|--[^\n]*\n?|/\*.*?(?:\*/|\Z))
    |(?P<string>'(?:[^']|'')*'?)
    |"(?P<qident>(?:[^"]|"")*)"?
    |(?P<number>(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)
    |(?P<ident>[^\W\d]\w*)
    |(?P<punct>.)""",
    re.VERBOSE | re.DOTALL,
)
_KEYWORDS = frozenset(
    "SELECT FROM WHERE AND OR NOT CASE WHEN THEN ELSE END AS IN IS NULL LIKE ILIKE BETWEEN DISTINCT FILTER OVER "
    "PARTITION BY ORDER INTERVAL DATE TIMESTAMP TIME CAST TRUE FALSE".split()
)


@dataclass(frozen=True)
class CompiledQuery:
    """The SQL, the dataset keys it reads (base first) and the metric it computes."""

    sql: str
    tables: list[str]
    metric: MetricOut


def quote_ident(name: str) -> str:
    """``name`` as one double-quoted SQL identifier, embedded quotes doubled."""
    return '"' + str(name).replace('"', '""') + '"'


def sql_literal(value: Any) -> str:
    """A JSON value (or a list of them) as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{value!r} is not a number a filter can compare with")
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, list):
        return "(" + ", ".join(sql_literal(v) for v in value) + ")"
    raise ValueError(f"A filter value must be text, a number, a boolean, a date or a list, not {type(value).__name__}")


def _short(key: str) -> str:
    return key.rsplit(".", 1)[-1]


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


def find_column_references(sql: str) -> list[ColumnReference]:
    """The two- and three-part identifier chains of ``sql``, in order; string literals and comments are never read.

    One followed by ``(`` is a function call and one starting with an unquoted keyword is SQL, so neither counts; a
    bare identifier is never a reference, and a chain of four or more parts is left alone.
    """
    tokens = [m for m in _TOKEN.finditer(sql) if m.lastgroup != "skip"]

    def is_name(at: int) -> bool:
        return at < len(tokens) and tokens[at].lastgroup in ("ident", "qident")

    def is_punct(at: int, char: str) -> bool:
        return at < len(tokens) and tokens[at]["punct"] == char

    refs: list[ColumnReference] = []
    i = 0
    while i < len(tokens):
        first = i
        i += 1
        if not is_name(first):
            continue
        while is_punct(i, ".") and is_name(i + 1):
            i += 2
        names = tokens[first:i:2]
        is_keyword = names[0].lastgroup == "ident" and names[0].group().upper() in _KEYWORDS
        if 2 <= len(names) <= 3 and not is_keyword and not is_punct(i, "("):
            parts = tuple(t["ident"] or t["qident"].replace('""', '"') for t in names)
            start, end = names[0].start(), names[-1].end()
            refs.append(ColumnReference(start, end, parts, sql[start:end]))
    return refs


def _paths_from(snapshot: ModelSnapshot, start: str) -> dict[str, list[JoinOut]]:
    """Shortest join path from ``start`` to each table it reaches, joins followed from ``from`` to ``to`` only."""
    paths: dict[str, list[JoinOut]] = {start: []}
    queue = deque([start])
    joins = sorted(snapshot.joins, key=lambda j: (j.name, j.key))
    while queue:
        cur = queue.popleft()
        for j in joins:
            if j.from_table == cur and j.to_table not in paths:
                paths[j.to_table] = [*paths[cur], j]
                queue.append(j.to_table)
    return paths


def reachable_tables(snapshot: ModelSnapshot, start: str) -> list[str]:
    """The dataset keys a metric on ``start`` can read, ``start`` first, then each table reached many → one."""
    return list(_paths_from(snapshot, start))


def _norm(sql: str) -> str:
    return re.sub(r"\s+", "", sql).lower()


class _Graph:
    """The tables a base table reaches by following joins from ``from`` to ``to``, nearest first."""

    def __init__(self, snapshot: ModelSnapshot, base: str):
        self.snapshot = snapshot
        self.base = base
        self.paths = _paths_from(snapshot, base)
        self.columns = {key: {c for c, _ in info.columns} for key, info in snapshot.related_tables.items()}
        self.columns.update({t.key: {c.name for c in t.fields} for t in snapshot.tables})
        self.reach = ", ".join(_short(k) for k in self.paths)

    def resolve(self, ref: str, what: str) -> tuple[str, str]:
        """``"table.column"`` or a bare base column as ``(dataset key, column)``; a ``ValueError`` otherwise."""
        if not ref.strip():
            raise ValueError(f"A {what} must name a column")
        if ref in self.columns[self.base]:
            return self.base, ref
        table_name, _, column = ref.rpartition(".")
        if not table_name:
            raise ValueError(
                f"Table {_short(self.base)!r} has no column {ref!r}; write a column of a joined table as "
                f"'table.column' (reachable tables: {self.reach})"
            )
        key = next((k for k in self.paths if table_name in (k, _short(k))), None)
        if key is None:
            if any(table_name in (k, _short(k)) for k in self.columns):
                raise ValueError(
                    f"The {what} {ref!r} is on table {table_name!r}, which {_short(self.base)!r} does not reach "
                    f"by following joins from their many side to their one side. Reachable tables: {self.reach}"
                )
            raise ValueError(f"Unknown table {table_name!r} in {what} {ref!r}. Reachable tables: {self.reach}")
        return self._checked(key, column, f"in {what} {ref!r}")

    def resolve_reference(self, ref: ColumnReference, where: str) -> tuple[str, str]:
        """A chain of a metric's free SQL as ``(dataset key, column)``; a ``ValueError`` naming ``where`` otherwise.

        The base table wins its own short name; any other table must be reachable many → one, and two reachable
        tables that answer the same name are an error that names both.
        """
        *table_parts, column = ref.parts
        name = ".".join(table_parts)
        at = f"{ref.text} in {where}"
        base = _short(self.base)
        if name == base:
            return self._checked(self.base, column, at)

        def named(keys) -> list[str]:
            return [k for k in keys if k.split(".")[-len(table_parts) :] == table_parts]

        matches = named(self.paths)
        if len(matches) > 1:
            both = " and ".join(".".join(k.split(".")[-2:]) for k in matches)
            raise ValueError(f"{name!r} could mean {both} ({at}); write schema.table.column to choose one")
        if matches:
            return self._checked(matches[0], column, at)
        known = named(self.columns)
        if not known:
            raise ValueError(f"Unknown table {name!r} ({at}). Tables this metric can use: {self.reach}")
        other = _short(known[0])
        if self.base in _paths_from(self.snapshot, known[0]):
            raise ValueError(
                f"{other} has many rows per {_one_row_of(base)}; a metric on {base} cannot use a column of "
                f"{other} ({at})"
            )
        raise ValueError(
            f"Table {other!r} is not reached from {base} by following joins from their many side to their one side "
            f"({at}). Tables this metric can use: {self.reach}"
        )

    def _checked(self, key: str, column: str, at: str) -> tuple[str, str]:
        if column not in self.columns.get(key, ()):
            raise ValueError(f"Table {_short(key)!r} has no column {column!r} ({at})")
        return key, column


def find_metric(snapshot: ModelSnapshot, name: str, tables: set[str]) -> MetricOut:
    """The metric called ``name`` on one of ``tables`` (dataset keys)."""
    metrics = [m for m in snapshot.metrics if m.table in tables]
    found = [m for m in metrics if m.name == name]
    if not found:
        known = sorted({m.name for m in metrics})
        hint = f" Known metrics: {', '.join(known)}" if known else " This model has no metrics yet."
        raise ValueError(f"No metric called {name!r} in this model.{hint}")
    if len(found) > 1:
        raise ValueError(f"Metric {name!r} exists on several tables: {', '.join(m.table for m in found)}")
    return found[0]


def compile_metric_query(
    snapshot: ModelSnapshot,
    query: MetricQuery,
    *,
    metric_tables: set[str],
    table_names: dict[str, str] | None = None,
) -> CompiledQuery:
    """Compile ``query`` against ``snapshot``; a ``ValueError`` with an actionable message when it cannot compile.

    ``metric_tables`` are the tables whose metrics may be asked for. Tables are written by their dataset key (one
    quoted identifier, as the catalog SQL path registers them) unless ``table_names`` maps a key to another name.
    """
    names_for = table_names or {}
    metric = find_metric(snapshot, query.metric, metric_tables)
    expression = (metric.expression or "").strip()
    if not expression:
        raise ValueError(f"Metric {metric.name!r} has no expression")
    base = metric.table
    graph = _Graph(snapshot, base)
    qb = quote_ident(_short(base))
    joined: dict[str, tuple[str, JoinOut]] = {}
    projected: dict[tuple[str, str], str] = {}

    def table_alias(key: str) -> str:
        for j in graph.paths[key]:
            joined.setdefault(j.to_table, (quote_ident(f"j{len(joined) + 1}"), j))
        return joined[key][0] if key != base else qb

    def inner_ref(key: str, column: str) -> str:
        """The column as the inner query reads it."""
        return f"{table_alias(key)}.{quote_ident(column)}"

    def outer_ref(key: str, column: str) -> str:
        """The column as the outer query reads it: a base column by name, a joined one by its safe alias."""
        if key == base:
            return inner_ref(key, column)
        table_alias(key)
        return f"{qb}.{quote_ident(projected.setdefault((key, column), f'__gloss_d{len(projected) + 1}'))}"

    select: list[tuple[str, str]] = []
    if query.grain:
        if not metric.time_column:
            raise ValueError(f"Metric {metric.name!r} has no time column, so it cannot be broken down by {query.grain}")
        key, column = graph.resolve(metric.time_column, "time column")
        select.append((f"DATE_TRUNC({sql_literal(query.grain)}, {outer_ref(key, column)})", f"{column}_{query.grain}"))
    for ref in dict.fromkeys(query.dimensions):
        key, column = graph.resolve(ref, "dimension")
        written = {column if key == base else None, f"{_short(key)}.{column}", f"{key}.{column}"}
        if metric.dimensions and not written & set(metric.dimensions):
            raise ValueError(
                f"Metric {metric.name!r} may only be broken down by {', '.join(metric.dimensions)}; "
                f"{ref!r} is not one of them"
            )
        select.append((outer_ref(key, column), ref))

    where = [_render_filter(f, inner_ref(*graph.resolve(f.field, "filter field"))) for f in query.filters]

    def rewrite(text: str, where_name: str) -> str:
        """``text`` with each chain naming a joined table read from the inner query's alias for it."""
        for ref in reversed(find_column_references(text)):
            key, column = graph.resolve_reference(ref, where_name)
            if key != base or len(ref.parts) == 3:
                text = text[: ref.start] + outer_ref(key, column) + text[ref.end :]
        return text

    stored_filter = (metric.filter or "").strip()
    expression_sql = rewrite(expression, "the expression")
    filter_sql = ""
    if stored_filter and _norm(stored_filter) not in _norm(expression):
        filter_sql = rewrite(stored_filter, "the filter")

    names = [name for _expr, name in select]
    if metric.name in names:
        raise ValueError(f"A dimension may not share the metric's name {metric.name!r}")
    order_sql = f"{quote_ident(names[0])} ASC" if names else None
    if query.order is not None:
        if query.order.field not in (*names, metric.name):
            raise ValueError(f"Order by a dimension or the metric; {query.order.field!r} is neither")
        order_sql = f"{quote_ident(query.order.field)} {query.order.direction.upper()}"

    inner_select = [f"{qb}.*"] + [f"{inner_ref(k, c)} AS {quote_ident(a)}" for (k, c), a in projected.items()]
    inner = [f"SELECT {', '.join(inner_select)}", f"FROM {quote_ident(names_for.get(base, base))} AS {qb}"]
    for table, (alias, j) in joined.items():
        on = " AND ".join(
            f"{table_alias(j.from_table)}.{quote_ident(fc)} = {alias}.{quote_ident(tc)}"
            for fc, tc in zip(j.from_columns, j.to_columns, strict=True)
        )
        inner.append(f"LEFT JOIN {quote_ident(names_for.get(table, table))} AS {alias} ON {on}")
    if where:
        inner.append("WHERE " + " AND ".join(where))

    outer_select = [f"{expr} AS {quote_ident(name)}" for expr, name in select]
    outer_select.append(f"{expression_sql} AS {quote_ident(metric.name)}")
    sql = [f"SELECT {', '.join(outer_select)}", "FROM (" + " ".join(inner) + f") AS {qb}"]
    if filter_sql:
        sql.append(f"WHERE ({filter_sql})")
    if select:
        sql.append("GROUP BY " + ", ".join(expr for expr, _ in select))
    if order_sql:
        sql.append(f"ORDER BY {order_sql}")
    sql.append(f"LIMIT {query.limit}")
    return CompiledQuery(sql="\n".join(sql), tables=[base, *joined], metric=metric)


def _render_filter(f: MetricFilter, ref: str) -> str:
    value, many = f.value, isinstance(f.value, list)
    if f.op in ("is null", "is not null"):
        return f"{ref} {f.op.upper()}"
    if f.op in ("in", "not in"):
        if not many or not value:
            raise ValueError(f"The {f.op!r} filter on {f.field!r} needs a non-empty list of values")
        return f"{ref} {f.op.upper()} {sql_literal(value)}"
    if f.op == "between":
        if not many or len(value) != 2:
            raise ValueError(f"The 'between' filter on {f.field!r} needs a list of two values")
        return f"{ref} BETWEEN {sql_literal(value[0])} AND {sql_literal(value[1])}"
    if many or value is None:
        raise ValueError(f"The {f.op!r} filter on {f.field!r} needs one value; use 'is null' to match missing values")
    return f"{ref} {'<>' if f.op == '!=' else f.op} {sql_literal(value)}"


def with_candidate(snapshot: ModelSnapshot, metric: MetricOut) -> ModelSnapshot:
    """``snapshot`` with ``metric`` on its table in place of any metric of that name, for dry-running a proposal."""
    tables = [
        t.model_copy(
            update={"metrics": [m for m in t.metrics if m.name != metric.name] + [metric] * (t.key == metric.table)}
        )
        for t in snapshot.tables
    ]
    return replace(snapshot, tables=tables)
