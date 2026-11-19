"""The small profile of a catalog table that ``POST /ai/generate_chart`` sends to the model.

Columns and types come from the table's stored schema, roles from
``CatalogService.get_visualization_fields`` (Graphic Walker's own measure/dimension split),
and the numbers from the worker through ``CatalogService.compute_ad_hoc_visualization``:
one aggregate query for the row count and each column's non-null count, distinct count,
min and max; one raw query for the first ten rows; one distinct-values query for each of
up to eight text columns with twenty or fewer distinct values. Core collects nothing.

Every value is masked with ``safety.scrub_text_regex`` and cut to 40 characters before it
is kept. The profile is cached in process for five minutes per user and table version
(the Delta version of a local table, else its ``updated_at`` and sources), and is built
under a time budget: past it, or on any failure, the caller gets what was finished
(names and types, roles when the fields call answered) with ``problem`` saying why. A
profile that finishes after the budget still lands in the cache for the next request.
Must not import ``litellm``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

from flowfile_core.ai.chart_request import ColumnInfo, TypeClass
from flowfile_core.ai.safety import scrub_text_regex

logger = logging.getLogger(__name__)

PROFILE_BUDGET_SECONDS: float = 8.0
CACHE_TTL_SECONDS: float = 300.0
MAX_VALUE_CHARS: int = 40
EXAMPLE_ROWS: int = 10
MAX_EXAMPLES: int = 5
VALUE_LIST_COLUMNS: int = 8
VALUE_LIST_MAX_DISTINCT: int = 20

_AGGREGATABLE: frozenset[str] = frozenset({"string", "boolean", "integer", "float", "date", "datetime"})
_WITH_MIN_MAX: frozenset[str] = frozenset({"integer", "float", "date", "datetime"})

_INTEGER_BASES = frozenset(
    {"int8", "int16", "int32", "int64", "int128", "uint8", "uint16", "uint32", "uint64", "uint128"}
)
_FLOAT_BASES = frozenset(
    {"float16", "float32", "float64", "float", "double", "halffloat", "decimal", "decimal128", "decimal256"}
)
_STRING_BASES = frozenset({"string", "str", "utf8", "large_string", "large_utf8", "string_view", "categorical", "enum"})
_BOOLEAN_BASES = frozenset({"boolean", "bool"})
_DATE_BASES = frozenset({"date", "date32", "date64"})
_DATETIME_BASES = frozenset({"datetime", "timestamp"})


def type_class(dtype: str) -> TypeClass:
    """Map a stored dtype string (Polars' ``str(dtype)`` or a legacy pyarrow one) to a type class."""
    low = (dtype or "").strip().lower()
    base = re.split(r"[\[(<]", low, maxsplit=1)[0].strip()
    if base in _INTEGER_BASES:
        return "integer"
    if base in _FLOAT_BASES:
        return "float"
    if base in _STRING_BASES or (base == "dictionary" and "value" in low and "string" in low):
        return "string"
    if base in _BOOLEAN_BASES:
        return "boolean"
    if base in _DATE_BASES:
        return "date"
    if base in _DATETIME_BASES:
        return "datetime"
    return "other"


@dataclass(frozen=True)
class ColumnProfile:
    """One column as the model sees it. Never mutated: a stage replaces the whole list."""

    name: str
    dtype: str
    type: TypeClass
    role: Literal["measure", "dimension"] | None = None
    non_null: int | None = None
    distinct: int | None = None
    min: Any = None
    max: Any = None
    examples: tuple[Any, ...] = ()
    values: tuple[Any, ...] | None = None
    values_exact: bool = False


@dataclass
class TableProfile:
    """``complete`` is True once every stage finished; ``problem`` says why it is not."""

    table_id: int
    table_name: str
    columns: list[ColumnProfile] = field(default_factory=list)
    row_count: int | None = None
    complete: bool = False
    problem: str | None = None

    def column_infos(self) -> list[ColumnInfo]:
        """The columns as the chart request checks see them."""
        return [
            ColumnInfo(
                name=c.name,
                type=c.type,
                role=c.role,
                values=c.values if c.values_exact else None,
            )
            for c in self.columns
        ]


def _shown(value: Any) -> Any:
    """A value as it may leave the box: masked, then cut to ``MAX_VALUE_CHARS``."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        text = str(value)
        masked = scrub_text_regex(text)
        return value if masked == text else masked
    text = scrub_text_regex(str(value))
    if len(text) > MAX_VALUE_CHARS:
        text = text[: MAX_VALUE_CHARS - 1] + "…"
    return text


def _base_profile(table: Any) -> TableProfile:
    columns = [
        ColumnProfile(name=c.name, dtype=c.dtype, type=type_class(c.dtype)) for c in (table.schema_columns or [])
    ]
    return TableProfile(table_id=table.id, table_name=table.name, columns=columns)


def cache_key(table: Any, user_id: int | None) -> tuple:
    """What identifies the table's current data: a write moves the Delta version or ``updated_at``."""
    from flowfile_core.catalog.delta_utils import get_live_delta_version

    stored_in = Path(table.file_path).name if table.file_path else ""
    version: int | None = None
    if table.table_type != "virtual" and table.file_path and not table.is_remote_storage:
        try:
            version = get_live_delta_version(table.file_path)
        except Exception:  # noqa: BLE001
            version = None
    updated = table.updated_at.isoformat() if table.updated_at else ""
    return (user_id, table.id, stored_in, version, updated, table.source_table_versions, table.sql_query)


_cache: dict[tuple, tuple[float, TableProfile]] = {}
_cache_lock = threading.Lock()


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


def _cache_get(key: tuple) -> TableProfile | None:
    now = time.monotonic()
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        expires, profile = entry
        if expires <= now:
            _cache.pop(key, None)
            return None
        return profile


def _cache_put(key: tuple, profile: TableProfile) -> None:
    now = time.monotonic()
    with _cache_lock:
        for stale in [k for k, (expires, _) in _cache.items() if expires <= now]:
            _cache.pop(stale, None)
        _cache[key] = (now + CACHE_TTL_SECONDS, profile)


def stats_payload(columns: list[ColumnProfile]) -> dict:
    """One aggregate over the whole table: row count, then per column non-null, distinct, min, max."""
    measures: list[dict] = [{"field": "*", "agg": "count", "asFieldKey": "__rows"}]
    for i, col in enumerate(columns):
        if col.type not in _AGGREGATABLE:
            continue
        measures.append({"field": col.name, "agg": "count", "asFieldKey": f"c{i}_nonnull"})
        measures.append({"field": col.name, "agg": "distinctCount", "asFieldKey": f"c{i}_distinct"})
        if col.type in _WITH_MIN_MAX:
            measures.append({"field": col.name, "agg": "min", "asFieldKey": f"c{i}_min"})
            measures.append({"field": col.name, "agg": "max", "asFieldKey": f"c{i}_max"})
    return {"workflow": [{"type": "view", "query": [{"op": "aggregate", "groupBy": [], "measures": measures}]}]}


def rows_payload(columns: list[ColumnProfile]) -> dict:
    fields = [c.name for c in columns if c.type in _AGGREGATABLE]
    return {"workflow": [{"type": "view", "query": [{"op": "raw", "fields": fields}]}], "limit": EXAMPLE_ROWS}


def values_payload(column: str) -> dict:
    return {"workflow": [{"type": "view", "query": [{"op": "aggregate", "groupBy": [column], "measures": []}]}]}


def _as_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _rows(service: Any, source: Any, payload: dict, max_rows: int, user_id: int | None) -> list[dict]:
    result = service.compute_ad_hoc_visualization(source, payload, max_rows, user_id)
    if result.error:
        raise RuntimeError(result.error)
    return result.rows


def _fill(table: Any, user: Any, into: TableProfile) -> None:
    """Build the profile stage by stage, replacing ``into.columns`` whole after each one."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.catalog.access import AccessResolver
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.schemas.catalog_schema import VizSourceDescriptor

    user_id = getattr(user, "id", None)
    source = VizSourceDescriptor(source_type="table", table_id=table.id)
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db), access=AccessResolver(db, user))

        fields = service.get_visualization_fields(source, user_id)
        if fields.error:
            raise RuntimeError(fields.error)
        roles = {f.get("fid"): f.get("analyticType") for f in fields.fields}
        into.columns = [
            replace(c, role=roles[c.name]) if roles.get(c.name) in ("measure", "dimension") else c for c in into.columns
        ]

        [row] = _rows(service, source, stats_payload(into.columns), 1, user_id) or [{}]
        with_stats = []
        for i, col in enumerate(into.columns):
            if col.type not in _AGGREGATABLE:
                with_stats.append(col)
                continue
            with_stats.append(
                replace(
                    col,
                    non_null=_as_int(row.get(f"c{i}_nonnull")),
                    distinct=_as_int(row.get(f"c{i}_distinct")),
                    min=_shown(row.get(f"c{i}_min")),
                    max=_shown(row.get(f"c{i}_max")),
                )
            )
        into.row_count = _as_int(row.get("__rows"))
        into.columns = with_stats

        first_rows = _rows(service, source, rows_payload(into.columns), EXAMPLE_ROWS, user_id)
        with_examples = []
        for col in into.columns:
            seen: list[Any] = []
            for r in first_rows:
                shown = _shown(r.get(col.name))
                if shown is not None and shown not in seen:
                    seen.append(shown)
                if len(seen) >= MAX_EXAMPLES:
                    break
            with_examples.append(replace(col, examples=tuple(seen)))
        into.columns = with_examples

        listed = [
            c
            for c in into.columns
            if c.type == "string" and c.distinct is not None and c.distinct <= VALUE_LIST_MAX_DISTINCT
        ][:VALUE_LIST_COLUMNS]
        value_lists: dict[str, tuple[tuple[Any, ...], bool]] = {}
        for col in listed:
            raw = [r.get(col.name) for r in _rows(service, source, values_payload(col.name), 21, user_id)]
            raw = sorted({v for v in raw if v is not None}, key=str)
            shown = [_shown(v) for v in raw]
            value_lists[col.name] = (tuple(shown), shown == raw)
        into.columns = [
            replace(c, values=value_lists[c.name][0], values_exact=value_lists[c.name][1])
            if c.name in value_lists
            else c
            for c in into.columns
        ]
        into.complete = True


def _snapshot(profile: TableProfile, problem: str) -> TableProfile:
    return replace(profile, columns=list(profile.columns), complete=False, problem=problem)


async def profile_table(table: Any, user: Any, *, budget: float = PROFILE_BUDGET_SECONDS) -> TableProfile:
    """The profile of ``table`` (a ``CatalogTableOut`` the caller may read) for ``user``.

    Never raises: past ``budget`` seconds or on a failure the result holds what was finished
    and ``problem`` says why.
    """
    holder = _base_profile(table)
    user_id = getattr(user, "id", None)

    def work() -> TableProfile:
        key = cache_key(table, user_id)
        cached = _cache_get(key)
        if cached is not None:
            return cached
        _fill(table, user, holder)
        _cache_put(key, holder)
        return holder

    try:
        return await asyncio.wait_for(asyncio.to_thread(work), timeout=budget)
    except asyncio.TimeoutError:
        logger.info("chart profile of table %s ran past its %.1f s budget", table.id, budget)
        return _snapshot(holder, f"timed out after {budget:g} s")
    except Exception as exc:  # noqa: BLE001
        logger.warning("chart profile of table %s failed: %s", table.id, exc)
        return _snapshot(holder, "failed")


def _literal(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def render_profile(profile: TableProfile) -> str:
    """The profile as the user turn shows it: one line per column."""
    head = f"Table {_literal(profile.table_name)}"
    if profile.row_count is not None:
        head += f": {profile.row_count} rows, {len(profile.columns)} columns."
    else:
        head += f": {len(profile.columns)} columns."
    lines = [head]
    if not profile.complete:
        lines.append(
            "Statistics and example values are not available for this table; use the column names and types below."
        )
    lines.append("Columns (name: type, role; profile):")
    for col in profile.columns:
        if col.type == "other":
            lines.append(f"- {_literal(col.name)}: other ({col.dtype}), cannot be charted")
            continue
        head = f"- {_literal(col.name)}: {col.type}"
        if col.role:
            head += f", {col.role}"
        parts: list[str] = []
        counts = [
            f"{n} {label}" for n, label in ((col.non_null, "non-null"), (col.distinct, "distinct")) if n is not None
        ]
        if counts:
            parts.append(", ".join(counts))
        if col.min is not None or col.max is not None:
            parts.append(f"min {_literal(col.min)}, max {_literal(col.max)}")
        if col.values is not None:
            parts.append("values: " + ", ".join(_literal(v) for v in col.values))
        elif col.examples:
            parts.append("e.g. " + ", ".join(_literal(v) for v in col.examples))
        lines.append(head + ("; " + "; ".join(parts) if parts else ""))
    return "\n".join(lines)


__all__ = [
    "CACHE_TTL_SECONDS",
    "PROFILE_BUDGET_SECONDS",
    "ColumnProfile",
    "TableProfile",
    "cache_key",
    "clear_cache",
    "profile_table",
    "render_profile",
    "rows_payload",
    "stats_payload",
    "type_class",
    "values_payload",
]
