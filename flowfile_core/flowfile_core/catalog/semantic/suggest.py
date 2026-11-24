"""Rule-based suggestions for a semantic model: column roles, joins and metrics, with no model involved.

Everything here is pure except ``collect_stats``, which runs one aggregate SQL statement per table through the
catalog SQL path, so the worker computes and only counts and numeric or temporal bounds come back. When the worker
cannot be reached the rules run on column names and types alone, at a lower confidence, and say so.

The rules are deliberately plain, so a reviewer can read each candidate's evidence and agree or not:

* **key**: named like an id, or unique and non-null (integer or string, not named like an amount);
* **time**: a date or datetime column;
* **amount**: numeric and not a key;
* **category**: string or boolean, or few distinct values;
* **join**: a column of table A that matches by name and type class a key that is unique in table B and repeats in
  A gives A (many) to B (one);
* **metrics** per table: a row count, a sum and an average per amount column, a distinct count per foreign key.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from flowfile_core.catalog.semantic.compiler import quote_ident
from flowfile_core.catalog.semantic.snapshot import TableInfo

RULE_SOURCE = "inferred:flowfile-rules"
CATEGORY_MAX_DISTINCT = 20
_ID_NAME = re.compile(r"(^id$|_id$|^id_|_key$|^key$|_uuid$|^uuid$)", re.IGNORECASE)
_CODE_NAME = re.compile(r"(code|sku|email|number|ref|slug|handle|username)", re.IGNORECASE)
_AMOUNT_NAME = re.compile(
    r"(amount|price|total|cost|revenue|value|qty|quantity|sum|balance|fee|tax|discount|score|weight)", re.IGNORECASE
)


def type_class(dtype: str | None) -> str:
    """``numeric``, ``integer``, ``time``, ``string``, ``boolean`` or ``other`` for a Polars dtype string."""
    d = (dtype or "").lower()
    if d.startswith(("int", "uint")):
        return "integer"
    if d.startswith(("float", "decimal")):
        return "numeric"
    if d.startswith(("date", "datetime")):
        return "time"
    if d.startswith(("str", "utf8", "categorical", "enum")):
        return "string"
    if d.startswith("bool"):
        return "boolean"
    return "other"


def _numeric(tc: str) -> bool:
    return tc in ("integer", "numeric")


def singular(name: str) -> str:
    """A crude English singular, enough for ``orders`` → ``order`` and ``categories`` → ``category``."""
    n = name.lower()
    if n.endswith("ies") and len(n) > 4:
        return n[:-3] + "y"
    if n.endswith(("ses", "xes")) and len(n) > 4:
        return n[:-2]
    if n.endswith("s") and not n.endswith("ss") and len(n) > 2:
        return n[:-1]
    return n


def _words(name: str) -> str:
    return name.replace("_", " ").strip()


@dataclass
class ColumnStats:
    name: str
    dtype: str | None
    distinct: int | None = None
    nulls: int | None = None
    min: Any = None
    max: Any = None

    @property
    def tc(self) -> str:
        return type_class(self.dtype)


@dataclass
class TableStats:
    table: TableInfo
    row_count: int | None = None
    columns: list[ColumnStats] = field(default_factory=list)
    available: bool = False

    def column(self, name: str) -> ColumnStats | None:
        return next((c for c in self.columns if c.name == name), None)

    def unique(self, name: str) -> bool | None:
        """Whether the column is unique and non-null; ``None`` without statistics."""
        c = self.column(name)
        if not self.available or c is None or c.distinct is None or self.row_count is None:
            return None
        return self.row_count > 0 and c.distinct == self.row_count and not c.nulls


def stats_sql(table: TableInfo) -> str:
    """One aggregate statement: the row count and, per column, distinct and null counts and numeric/time bounds."""
    parts = ['COUNT(*) AS "__rows"']
    for i, (name, dtype) in enumerate(table.columns):
        col = quote_ident(name)
        parts.append(f'COUNT(DISTINCT {col}) AS "d{i}"')
        parts.append(f'COUNT(*) - COUNT({col}) AS "n{i}"')
        if _numeric(type_class(dtype)) or type_class(dtype) == "time":
            parts.append(f'MIN({col}) AS "lo{i}"')
            parts.append(f'MAX({col}) AS "hi{i}"')
    return f"SELECT {', '.join(parts)} FROM {quote_ident(table.key)}"


def names_only(table: TableInfo) -> TableStats:
    return TableStats(table=table, columns=[ColumnStats(n, d) for n, d in table.columns])


def parse_stats(table: TableInfo, columns: list[str], row: list[Any]) -> TableStats:
    values = dict(zip(columns, row, strict=False))
    out = TableStats(table=table, row_count=_int(values.get("__rows")), available=True)
    for i, (name, dtype) in enumerate(table.columns):
        out.columns.append(
            ColumnStats(
                name,
                dtype,
                distinct=_int(values.get(f"d{i}")),
                nulls=_int(values.get(f"n{i}")),
                min=values.get(f"lo{i}"),
                max=values.get(f"hi{i}"),
            )
        )
    return out


def _int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def collect_stats(tables: Iterable[TableInfo], execute: Callable[[str], Any]) -> tuple[list[TableStats], str | None]:
    """Statistics per table via ``execute`` (SQL → ``SqlQueryResult``); names and types only where that fails.

    Returns the stats and, when any table fell back, a sentence saying why. A ``ComputeError`` (no worker) stops
    the remaining queries too, since they would fail the same way.
    """
    from flowfile_core.flowfile.execution.errors import ComputeError

    out: list[TableStats] = []
    problem: str | None = None
    unreachable = False
    for t in tables:
        if unreachable or not t.columns:
            out.append(names_only(t))
            continue
        try:
            result = execute(stats_sql(t))
        except ComputeError as exc:
            unreachable = True
            problem = f"Statistics were not available (compute unreachable: {exc}); rules used names and types only."
            out.append(names_only(t))
            continue
        if result.error or not result.rows:
            problem = problem or f"Statistics for {t.name!r} failed ({result.error}); its rules used names and types."
            out.append(names_only(t))
            continue
        out.append(parse_stats(t, result.columns, result.rows[0]))
    return out, problem


@dataclass
class RoleCandidate:
    table_id: int
    column: str
    role: str
    confidence: float
    rationale: str


@dataclass
class JoinCandidate:
    name: str
    from_table_id: int
    to_table_id: int
    from_table: str
    to_table: str
    from_columns: list[str]
    to_columns: list[str]
    description: str
    confidence: float
    rationale: str


@dataclass
class MetricCandidate:
    table_id: int
    table: str
    name: str
    expression: str
    description: str
    aggregate: str
    time_column: str | None
    dimensions: list[str]
    confidence: float
    rationale: str
    column: str | None = None


@dataclass
class Suggestions:
    roles: list[RoleCandidate] = field(default_factory=list)
    joins: list[JoinCandidate] = field(default_factory=list)
    metrics: list[MetricCandidate] = field(default_factory=list)
    stats_available: bool = False
    stats: dict[int, TableStats] = field(default_factory=dict)


def column_role(ts: TableStats, c: ColumnStats) -> tuple[str, float, str] | None:
    """The role, a confidence and the evidence for one column; ``None`` when no rule fits."""
    tc = c.tc
    rows = ts.row_count
    if tc == "time":
        return "time", 0.9, f"{c.name} is a {c.dtype} column"
    unique = ts.unique(c.name)
    if _ID_NAME.search(c.name):
        if unique:
            return "key", 0.9, f"{c.name} is named like an id and is unique and non-null ({c.distinct} of {rows} rows)"
        return "key", 0.7 if ts.available else 0.5, f"{c.name} is named like an id"
    if unique and tc == "string" and not _CODE_NAME.search(c.name):
        return None
    if unique and tc in ("integer", "string") and not _AMOUNT_NAME.search(c.name) and (rows or 0) >= 2:
        return "key", 0.6, f"{c.name} is unique and non-null ({c.distinct} of {rows} rows)"
    if _numeric(tc):
        if ts.available and c.distinct is not None and tc == "integer" and c.distinct <= 2:
            return "category", 0.5, f"{c.name} holds {c.distinct} distinct values"
        return "amount", 0.7 if ts.available else 0.5, f"{c.name} is numeric ({c.dtype}) and not a key"
    if tc in ("string", "boolean"):
        if ts.available and c.distinct is not None:
            if c.distinct <= CATEGORY_MAX_DISTINCT or (rows and c.distinct <= rows / 2):
                return "category", 0.8, f"{c.name} is {tc} with {c.distinct} distinct values in {rows} rows"
            return None
        return "category", 0.5, f"{c.name} is a {tc} column"
    return None


def _primary_key(ts: TableStats, roles: dict[str, str]) -> str | None:
    """The column most likely to identify a row: ``id`` or ``<table>_id``, else the one unique key."""
    single = singular(ts.table.name)
    keys = [n for n, r in roles.items() if r == "key"]
    for preferred in ("id", f"{single}_id", f"{ts.table.name.lower()}_id"):
        hit = next((k for k in keys if k.lower() == preferred), None)
        if hit and ts.unique(hit) is not False:
            return hit
    unique = [k for k in keys if ts.unique(k)]
    return unique[0] if len(unique) == 1 else None


def suggest(tables: list[TableStats]) -> Suggestions:
    """Roles, joins and metrics for ``tables`` (one model's tables, with or without statistics)."""
    out = Suggestions(stats_available=any(t.available for t in tables))
    roles: dict[int, dict[str, str]] = {}
    for ts in tables:
        roles[ts.table.table_id] = {}
        for c in ts.columns:
            found = column_role(ts, c)
            if found is None:
                continue
            role, conf, why = found
            roles[ts.table.table_id][c.name] = role
            out.roles.append(RoleCandidate(ts.table.table_id, c.name, role, conf, f"Rule: {why}."))

    pks = {ts.table.table_id: _primary_key(ts, roles[ts.table.table_id]) for ts in tables}
    fks: dict[int, list[str]] = {ts.table.table_id: [] for ts in tables}
    for a in tables:
        for c in a.columns:
            if roles[a.table.table_id].get(c.name) != "key" or c.name == pks[a.table.table_id]:
                continue
            for b in tables:
                if b is a:
                    continue
                pk = pks[b.table.table_id]
                if pk is None:
                    continue
                target_name = pk if pk.lower() == c.name.lower() else None
                if target_name is None and pk.lower() == "id" and c.name.lower() == f"{singular(b.table.name)}_id":
                    target_name = pk
                if target_name is None:
                    continue
                target = b.column(target_name)
                if target is None or _join_class(target.tc) != _join_class(c.tc):
                    continue
                if a.unique(c.name):
                    continue
                if a.available and b.available:
                    conf = 0.85
                    why = (
                        f"{a.table.name}.{c.name} matches {b.table.name}.{target_name} by name and type; "
                        f"{target_name} is unique in {b.table.name} ({target.distinct} of {b.row_count} rows) and "
                        f"repeats in {a.table.name} ({c.distinct} distinct in {a.row_count} rows)"
                    )
                else:
                    conf = 0.55
                    why = (
                        f"{a.table.name}.{c.name} matches {b.table.name}.{target_name} by name and type "
                        "(no statistics, uniqueness not checked)"
                    )
                out.joins.append(
                    JoinCandidate(
                        name=f"{a.table.name}_{b.table.name}",
                        from_table_id=a.table.table_id,
                        to_table_id=b.table.table_id,
                        from_table=a.table.name,
                        to_table=b.table.name,
                        from_columns=[c.name],
                        to_columns=[target_name],
                        description=f"Each {singular(a.table.name)} belongs to one {singular(b.table.name)}.",
                        confidence=conf,
                        rationale=f"Rule: {why}.",
                    )
                )
                fks[a.table.table_id].append(c.name)
                break

    by_id = {ts.table.table_id: ts for ts in tables}
    for ts in tables:
        tid = ts.table.table_id
        r = roles[tid]
        time_col = next((c.name for c in ts.columns if r.get(c.name) == "time"), None)
        dims = [c.name for c in ts.columns if r.get(c.name) == "category"]
        for j in out.joins:
            if j.from_table_id == tid:
                other = by_id[j.to_table_id]
                dims += [
                    f"{other.table.name}.{c.name}"
                    for c in other.columns
                    if roles[j.to_table_id].get(c.name) == "category"
                ]
        noun = singular(ts.table.name)
        conf = 0.7 if ts.available else 0.5
        evidence = f" ({ts.row_count} rows)" if ts.row_count is not None else ""
        out.metrics.append(
            MetricCandidate(
                tid,
                ts.table.name,
                f"{noun}_count",
                "COUNT(*)",
                f"The number of {_words(ts.table.name)}.",
                "count",
                time_col,
                dims,
                conf,
                f"Rule: every table gets a row count{evidence}.",
            )
        )
        for c in ts.columns:
            if r.get(c.name) == "amount":
                w = _words(c.name)
                out.metrics.append(
                    MetricCandidate(
                        tid,
                        ts.table.name,
                        f"total_{c.name}",
                        f"SUM({quote_ident(c.name)})",
                        f"The sum of {w} over {_words(ts.table.name)}.",
                        "sum",
                        time_col,
                        dims,
                        conf,
                        f"Rule: {c.name} is an amount column, so its sum is a candidate metric.",
                        column=c.name,
                    )
                )
                out.metrics.append(
                    MetricCandidate(
                        tid,
                        ts.table.name,
                        f"average_{c.name}",
                        f"AVG({quote_ident(c.name)})",
                        f"The average {w} per {noun}.",
                        "avg",
                        time_col,
                        dims,
                        conf * 0.9,
                        f"Rule: {c.name} is an amount column, so its average is a candidate metric.",
                        column=c.name,
                    )
                )
        for fk in fks[tid]:
            base = re.sub(r"_?(id|key|uuid)$", "", fk, flags=re.IGNORECASE) or fk
            out.metrics.append(
                MetricCandidate(
                    tid,
                    ts.table.name,
                    f"{base}_count",
                    f"COUNT(DISTINCT {quote_ident(fk)})",
                    f"The number of distinct {_words(base)}s with at least one {noun}.",
                    "count_distinct",
                    time_col,
                    dims,
                    conf,
                    f"Rule: {fk} is a foreign key, so its distinct count is a candidate metric.",
                    column=fk,
                )
            )
    _dedupe_metric_names(out.metrics)
    return out


def _join_class(tc: str) -> str:
    return "number" if _numeric(tc) else tc


def _dedupe_metric_names(metrics: list[MetricCandidate]) -> None:
    """Metric names are unique per namespace: on a clash a table's own row count keeps the name and the others get
    their table's singular name as a prefix (``order_customer_count`` beside the customers' ``customer_count``)."""
    counts: dict[str, int] = {}
    for m in metrics:
        counts[m.name] = counts.get(m.name, 0) + 1
    for m in metrics:
        if counts[m.name] > 1 and m.aggregate != "count":
            m.name = f"{singular(m.table)}_{m.name}"


__all__ = [
    "RULE_SOURCE",
    "ColumnStats",
    "JoinCandidate",
    "MetricCandidate",
    "RoleCandidate",
    "Suggestions",
    "TableStats",
    "collect_stats",
    "column_role",
    "names_only",
    "parse_stats",
    "singular",
    "stats_sql",
    "suggest",
    "type_class",
]
