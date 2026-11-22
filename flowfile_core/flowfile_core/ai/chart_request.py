"""Natural language → chart request, backing ``POST /ai/generate_chart``.

The model never writes a Graphic Walker spec. It writes a small *chart request* (the
contract between core and the browser, ``ChartRequest`` below), which the browser expands
with Graphic Walker's own ``normalize``. Core's job is to make the request one the browser
can draw: repair what is unambiguous, reject the rest with a message the model can correct
its answer from, and send that message back once (the repair round of
``local_model/code_build.py``). The rules are the plan's "Python validation rules" table;
``check_chart`` runs them against the table's columns (name, type class, role).

JSON mode, a rate-limit slot, one deadline for both calls, tolerant parsing. Never raises:
a provider failure, a timeout or an answer that still fails after the retry is a result
with ``degraded=True`` and a stable ``reason``. Must not import ``litellm`` at module load.
"""

from __future__ import annotations

import asyncio
import difflib
import json
import logging
import math
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_serializer

from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.metrics import record_autocomplete_call
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler

logger = logging.getLogger(__name__)

SURFACE: str = "chart"
DEFAULT_TIMEOUT_SECONDS: float = 45.0
MAX_TIMEOUT_SECONDS: float = 90.0
MAX_PROMPT_LEN: int = 2000
MAX_CURRENT_CHART_CHARS: int = 16_000
RETRY_ROUNDS: int = 1
_MAX_TOKENS: int = 1500
_MAX_TITLE: int = 120
_MAX_EXPLANATION: int = 400

REASONS: tuple[str, ...] = (
    "empty_prompt",
    "no_columns",
    "timeout",
    "provider_error",
    "empty_response",
    "parse_error",
    "validation_error",
    "no_chart",
    "invalid_chart",
)
"""Every ``reason`` a degraded result can carry."""

REPAIRS: tuple[str, ...] = (
    "json_extracted",
    "bare_chart",
    "chart_string",
    "shorthand",
    "aggregate_alias",
    "time_unit_alias",
    "list_unwrapped",
    "sort_lifted",
    "column_case",
    "theta_dropped",
    "size_dropped",
    "stack_dropped",
    "scatter_raw",
    "boxplot_raw",
    "decimal_sum",
    "measure_sum",
    "pie_channels",
    "filter_value_coerced",
    "filter_value_case",
    "date_range_normalized",
    "map_raw",
    "map_mark",
    "geo_dropped",
)
"""Every code ``ChartGenerationResponse.repairs`` can carry, one per kind of silent repair."""


def _note(repairs: list[str] | None, code: str) -> None:
    if repairs is not None and code not in repairs:
        repairs.append(code)


Mark = Literal["bar", "line", "area", "point", "arc", "rect", "boxplot", "table", "map"]
Aggregate = Literal["sum", "mean", "median", "min", "max", "count", "distinctCount"]
TimeUnit = Literal["year", "quarter", "month", "week", "day", "hour"]
TypeClass = Literal["string", "boolean", "integer", "float", "date", "datetime", "other"]

MARKS: tuple[str, ...] = ("bar", "line", "area", "point", "arc", "rect", "boxplot", "table", "map")
AGGREGATES: tuple[str, ...] = ("sum", "mean", "median", "min", "max", "count", "distinctCount")
TIME_UNITS: tuple[str, ...] = ("year", "quarter", "month", "week", "day", "hour")


class _Compact(BaseModel):
    """Serialises without the keys that are ``None``, so the browser sees the contract's optional keys absent."""

    @model_serializer(mode="wrap")
    def _drop_none(self, handler: Any) -> dict[str, Any]:
        return {k: v for k, v in handler(self).items() if v is not None}


class FieldRef(_Compact):
    """A column on a channel. With ``aggregate`` it is a measure, without it a group-by dimension."""

    model_config = ConfigDict(extra="forbid")

    field: str = Field(min_length=1)
    aggregate: Aggregate | None = None
    timeUnit: TimeUnit | None = None
    bin: int | None = None


class OneOfFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    oneOf: list[Any]


class NotInFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    notIn: list[Any]


class RangeFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    range: tuple[float | None, float | None]


class DateRangeFilter(BaseModel):
    """Both bounds inclusive, ``YYYY-MM-DD``."""

    model_config = ConfigDict(extra="forbid")

    field: str
    dateRange: tuple[str | None, str | None]


ChartFilter = OneOfFilter | NotInFilter | RangeFilter | DateRangeFilter
_FILTER_MODELS: dict[str, type[BaseModel]] = {
    "oneOf": OneOfFilter,
    "notIn": NotInFilter,
    "range": RangeFilter,
    "dateRange": DateRangeFilter,
}


class ChartRequest(_Compact):
    """The chart request, exactly as the plan's contract states it."""

    model_config = ConfigDict(extra="forbid")

    mark: Mark
    x: FieldRef | list[FieldRef] | None = None
    y: FieldRef | list[FieldRef] | None = None
    color: FieldRef | None = None
    size: FieldRef | None = None
    theta: FieldRef | None = None
    latitude: FieldRef | None = None
    longitude: FieldRef | None = None
    filters: list[ChartFilter] | None = None
    aggregate: bool | None = None
    stack: Literal["stack", "none", "normalize"] | None = None
    sort: Literal["ascending", "descending"] | None = None
    limit: int | None = Field(default=None, ge=1)


class ChartGenerationResponse(BaseModel):
    """``chart`` is set on success; a degraded result carries ``reason`` and a ``message``.

    ``attempts`` is how many model calls this answer took (0, 1, or 2 when the correction
    round ran); ``repairs`` the ``REPAIRS`` codes silently applied to the accepted answer;
    ``provider`` and ``model`` the ones the route resolved for the call.
    """

    model_config = ConfigDict(extra="forbid")

    chart: ChartRequest | None = None
    title: str | None = None
    explanation: str | None = None
    warnings: list[str] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None
    message: str | None = None
    attempts: int = 0
    repairs: list[str] = Field(default_factory=list)
    provider: str | None = None
    model: str | None = None


@dataclass(frozen=True)
class ColumnInfo:
    """A column as the checks see it. ``values`` is the complete distinct list, when known exactly."""

    name: str
    type: TypeClass
    role: Literal["measure", "dimension"] | None = None
    values: tuple[Any, ...] | None = None


CHART_KEYS: tuple[str, ...] = tuple(ChartRequest.model_fields)
FIELD_REF_KEYS: tuple[str, ...] = tuple(FieldRef.model_fields)
_CHANNELS: tuple[str, ...] = ("x", "y", "color", "size", "theta", "latitude", "longitude")
_GEO_CHANNELS: tuple[str, ...] = ("latitude", "longitude")
_NUMERIC = frozenset({"integer", "float"})
_TEMPORAL = frozenset({"date", "datetime"})
_NUMBER_AGGS = frozenset({"sum", "mean", "median", "min", "max"})

_AGGREGATE_ALIASES: dict[str, str] = {
    **{a.lower(): a for a in AGGREGATES},
    "avg": "mean",
    "average": "mean",
    "distinct": "distinctCount",
    "distinct_count": "distinctCount",
    "count_distinct": "distinctCount",
    "countdistinct": "distinctCount",
    "nunique": "distinctCount",
    "n_unique": "distinctCount",
}
_TIME_UNIT_ALIASES: dict[str, str] = {
    **{u: u for u in TIME_UNITS},
    "yearquarter": "quarter",
    "yearmonth": "month",
    "yearweek": "week",
    "yearmonthdate": "day",
    "yearmonthdatehours": "hour",
}
_SHORTHAND_RE = re.compile(r"^\s*([A-Za-z_]+)\s*\((.*)\)\s*$", re.DOTALL)
_MAX_SHORTHAND_LEN: int = 256

STAR_MESSAGE = '"*" means row count: use {"field":"*","aggregate":"count"} on x, y, theta, size or color only.'


def _type_word(t: str) -> str:
    return {
        "string": "text",
        "boolean": "boolean",
        "integer": "a whole number",
        "float": "a decimal number",
        "date": "a date",
        "datetime": "a date with time",
    }.get(t, "not chartable")


# Shape repairs: wrong but unambiguous answer shapes, fixed before parsing.


def _canonical(value: Any, aliases: dict[str, str]) -> Any:
    if isinstance(value, str):
        return aliases.get(value.strip().lower(), value)
    return value


def ref_from_string(text: str, column_names: Sequence[str]) -> dict[str, Any]:
    """``"sum(amount)"`` → ``{"field": "amount", "aggregate": "sum"}``; a column literally named so wins.

    Text longer than ``_MAX_SHORTHAND_LEN`` is never read as a shorthand: no aggregate of a
    column is that long, and the text may come straight from a request body.
    """
    if text in column_names:
        return {"field": text}
    match = _SHORTHAND_RE.match(text) if len(text) <= _MAX_SHORTHAND_LEN else None
    if match:
        agg = _AGGREGATE_ALIASES.get(match.group(1).lower())
        inner = match.group(2).strip().strip("\"'`")
        if agg == "count" and inner in ("", "*"):
            return {"field": "*", "aggregate": "count"}
        if agg is not None and inner:
            return {"field": inner, "aggregate": agg}
    return {"field": text.strip()}


def _normalize_ref(value: Any, column_names: Sequence[str], repairs: list[str] | None = None) -> Any:
    if isinstance(value, str):
        _note(repairs, "shorthand")
        return ref_from_string(value, column_names)
    if not isinstance(value, dict):
        return value
    out = value
    for key, aliases, code in (
        ("aggregate", _AGGREGATE_ALIASES, "aggregate_alias"),
        ("timeUnit", _TIME_UNIT_ALIASES, "time_unit_alias"),
    ):
        if key in value:
            canonical = _canonical(value[key], aliases)
            if canonical != value[key]:
                out = {**out, key: canonical}
                _note(repairs, code)
    return out


def normalize_chart_shape(raw: Any, column_names: Sequence[str], repairs: list[str] | None = None) -> Any:
    """Repair the recoverable shapes of a chart request; returns ``raw`` itself when nothing changes.

    Shorthand strings on a channel become references, aggregate and time-unit spellings
    (``"avg"``, ``"Sum"``, ``"yearmonth"``) their canonical names, a one-element list on
    color/size/theta its element, and a ``sort`` written on a reference moves to the request
    when the request has none (Graphic Walker reads it only there). Each repair made is
    noted in ``repairs`` by its ``REPAIRS`` code.
    """
    if not isinstance(raw, dict):
        return raw
    out = raw
    top_sort = raw.get("sort")
    for key in _CHANNELS:
        if key not in raw:
            continue
        value = raw[key]
        if isinstance(value, list):
            items = [_normalize_ref(v, column_names, repairs) for v in value]
            new: Any = items if any(a is not b for a, b in zip(items, value, strict=True)) else value
            if key not in ("x", "y") and len(new) == 1:
                new = new[0]
                _note(repairs, "list_unwrapped")
        else:
            new = _normalize_ref(value, column_names, repairs)
        refs = new if isinstance(new, list) else [new]
        lifted = []
        for ref in refs:
            if isinstance(ref, dict) and ref.get("sort") in ("ascending", "descending"):
                if top_sort is None or top_sort == ref["sort"]:
                    top_sort = ref["sort"]
                    ref = {k: v for k, v in ref.items() if k != "sort"}
                    _note(repairs, "sort_lifted")
            lifted.append(ref)
        if any(a is not b for a, b in zip(lifted, refs, strict=True)):
            new = lifted if isinstance(new, list) else lifted[0]
        if new is not value:
            out = {**out, key: new}
    if top_sort != raw.get("sort"):
        out = {**out, "sort": top_sort}
    return out


def normalize_answer(payload: Any, repairs: list[str] | None = None) -> Any:
    """The model's answer as ``{"chart", "title", "explanation"}``; returns ``payload`` itself when already so.

    Repairs a bare chart request at the top level and a ``chart`` sent as a JSON string.
    """
    if not isinstance(payload, dict):
        return payload
    if "chart" not in payload and "mark" in payload:
        _note(repairs, "bare_chart")
        return {
            "chart": {k: v for k, v in payload.items() if k not in ("title", "explanation")},
            "title": payload.get("title"),
            "explanation": payload.get("explanation"),
        }
    chart = payload.get("chart")
    if isinstance(chart, str) and chart.strip().startswith("{"):
        try:
            parsed = json.loads(chart)
        except json.JSONDecodeError:
            return payload
        if isinstance(parsed, dict):
            _note(repairs, "chart_string")
            return {**payload, "chart": parsed}
    return payload


# Parsing: the contract's shape, each mistake as a sentence.


def _loc(prefix: str, loc: Sequence[Any]) -> str:
    path = prefix
    for part in loc:
        path += f"[{part}]" if isinstance(part, int) else (f".{part}" if path else str(part))
    return path


def _describe_errors(prefix: str, exc: ValidationError, allowed: Sequence[str]) -> list[str]:
    out: list[str] = []
    for err in exc.errors():
        where = _loc(prefix, err.get("loc", ()))
        kind = err.get("type")
        if kind == "literal_error":
            expected = err.get("ctx", {}).get("expected", "")
            out.append(f"{where}: {json.dumps(err.get('input'), default=str)} is not allowed; use {expected}.")
        elif kind == "extra_forbidden":
            out.append(f'{where}: unknown key. Allowed keys: {", ".join(allowed)}.')
        elif kind == "missing":
            out.append(f"{where} is required.")
        else:
            out.append(f"{where}: {err.get('msg', 'invalid value')}.")
    return out


def _parse_ref(path: str, value: Any, problems: list[str]) -> FieldRef | None:
    if not isinstance(value, dict):
        problems.append(f'{path} must be an object such as {{"field": "region"}}.')
        return None
    try:
        return FieldRef.model_validate(value)
    except ValidationError as exc:
        problems.extend(_describe_errors(path, exc, FIELD_REF_KEYS))
        return None


_FILTER_RULE_MESSAGE = (
    "Each filter has exactly one of oneOf, notIn, range, dateRange; filters apply to rows, not to sums."
)


def _parse_filters(value: Any, problems: list[str]) -> list[BaseModel] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        problems.append('filters must be a list such as [{"field": "region", "oneOf": ["West"]}].')
        return None
    parsed: list[BaseModel] = []
    for i, item in enumerate(value):
        path = f"filters[{i}]"
        if not isinstance(item, dict):
            problems.append(f'{path} must be an object such as {{"field": "region", "oneOf": ["West"]}}.')
            continue
        rules = [k for k in item if k in _FILTER_MODELS]
        if len(rules) != 1 or "aggregate" in item:
            problems.append(f"{path}: {_FILTER_RULE_MESSAGE}")
            continue
        try:
            parsed.append(_FILTER_MODELS[rules[0]].model_validate(item))
        except ValidationError as exc:
            problems.extend(_describe_errors(path, exc, ("field", rules[0])))
    return parsed


def parse_chart(raw: Any) -> tuple[ChartRequest | None, list[str]]:
    """Parse a (normalised) chart request; every shape problem becomes one sentence."""
    if not isinstance(raw, dict):
        return None, ['"chart" must be a JSON object such as {"mark": "bar", "x": {...}, "y": {...}}.']
    problems: list[str] = []
    data: dict[str, Any] = {}
    for key in raw:
        if key not in CHART_KEYS:
            problems.append(f'"{key}" is not a chart key. A chart takes only: {", ".join(CHART_KEYS)}.')
    for key in _CHANNELS:
        value = raw.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            if key not in ("x", "y"):
                problems.append(f"{key} takes one field, not a list.")
                continue
            refs = [_parse_ref(f"{key}[{i}]", item, problems) for i, item in enumerate(value)]
            if value and all(r is not None for r in refs):
                data[key] = refs
        else:
            ref = _parse_ref(key, value, problems)
            if ref is not None:
                data[key] = ref
    filters = _parse_filters(raw.get("filters"), problems)
    if filters:
        data["filters"] = filters
    for key in ("mark", "aggregate", "stack", "sort", "limit"):
        if key in raw:
            data[key] = raw[key]
    try:
        chart = ChartRequest.model_validate(data)
    except ValidationError as exc:
        problems.extend(_describe_errors("", exc, CHART_KEYS))
        return None, problems
    return (None, problems) if problems else (chart, [])


# Validation: the rule table, against the table's columns.


def _column_list(names: Sequence[str]) -> str:
    shown = list(names[:60])
    return ", ".join(shown) + (", …" if len(names) > 60 else "")


def _resolve(name: str, by_name: dict[str, ColumnInfo], path: str) -> tuple[str | None, str | None]:
    if name in by_name:
        return name, None
    key = name.strip().lower()
    matches = [n for n in by_name if n.strip().lower() == key]
    if len(matches) == 1:
        return matches[0], None
    message = f'{path} "{name}" is not a column.'
    closest = difflib.get_close_matches(name, list(by_name), n=1, cutoff=0.5)
    if closest:
        message += f' Closest: "{closest[0]}".'
    return None, message + f" Columns: {_column_list(list(by_name))}"


def _plain(ref: FieldRef) -> bool:
    return ref.aggregate is None and ref.bin is None and ref.timeUnit is None


@dataclass
class _Work:
    """A chart under check: channels always as lists, so the rules read one shape."""

    mark: str
    channels: dict[str, list[FieldRef]]
    single: dict[str, bool]
    filters: list[BaseModel]
    aggregate: bool | None
    stack: str | None
    sort: str | None
    limit: int | None

    @classmethod
    def of(cls, chart: ChartRequest) -> _Work:
        channels: dict[str, list[FieldRef]] = {}
        single: dict[str, bool] = {}
        for key in _CHANNELS:
            value = getattr(chart, key)
            channels[key] = list(value) if isinstance(value, list) else ([value] if value is not None else [])
            single[key] = not isinstance(value, list)
        return cls(
            mark=chart.mark,
            channels=channels,
            single=single,
            filters=list(chart.filters or []),
            aggregate=chart.aggregate,
            stack=chart.stack,
            sort=chart.sort,
            limit=chart.limit,
        )

    def path(self, key: str, i: int) -> str:
        return key if self.single[key] and len(self.channels[key]) == 1 else f"{key}[{i}]"

    def build(self) -> ChartRequest:
        def axis(key: str) -> Any:
            refs = self.channels[key]
            if not refs:
                return None
            return refs[0] if self.single[key] and len(refs) == 1 else refs

        return ChartRequest(
            mark=self.mark,
            x=axis("x"),
            y=axis("y"),
            color=self.channels["color"][0] if self.channels["color"] else None,
            size=self.channels["size"][0] if self.channels["size"] else None,
            theta=self.channels["theta"][0] if self.channels["theta"] else None,
            latitude=self.channels["latitude"][0] if self.channels["latitude"] else None,
            longitude=self.channels["longitude"][0] if self.channels["longitude"] else None,
            filters=self.filters or None,
            aggregate=self.aggregate,
            stack=self.stack,
            sort=self.sort,
            limit=self.limit,
        )


class _Checker:
    def __init__(self, work: _Work, columns: Sequence[ColumnInfo], repairs: list[str] | None = None) -> None:
        self.w = work
        self.by_name = {c.name: c for c in columns}
        self.problems: list[str] = []
        self.repairs = repairs
        # False once a reference names no column: the rules that need its type are then skipped.
        self.typed = True

    def type_of(self, ref: FieldRef) -> str | None:
        info = self.by_name.get(ref.field)
        return info.type if info is not None else None

    def numeric(self, ref: FieldRef) -> bool:
        return self.type_of(ref) in _NUMERIC

    @property
    def aggregated(self) -> bool:
        return self.w.aggregate is not False

    def is_measure(self, key: str, ref: FieldRef) -> bool:
        if ref.aggregate is not None:
            return True
        return not self.aggregated and key in ("x", "y", "size") and _plain(ref) and self.numeric(ref)

    def category(self, ref: FieldRef) -> bool:
        """A group-by reference that is a real category: not an aggregate, not a raw decimal."""
        return ref.aggregate is None and not (_plain(ref) and self.type_of(ref) == "float")

    def refs(self, *keys: str) -> list[tuple[str, int, FieldRef]]:
        return [(k, i, r) for k in keys for i, r in enumerate(self.w.channels[k])]

    def replace_ref(self, key: str, i: int, **update: Any) -> None:
        self.w.channels[key][i] = self.w.channels[key][i].model_copy(update=update)

    def note(self, code: str) -> None:
        _note(self.repairs, code)

    def run(self) -> None:
        self.typed = self.resolve_names()
        self.drop_misplaced()
        self.mode_repairs()
        self.check_refs()
        if not any(self.w.channels[k] for k in _CHANNELS):
            self.problems.append(
                "The chart has no fields. Put a category on x and an aggregated number on y, "
                'e.g. "x": {"field": "region"}, "y": {"field": "*", "aggregate": "count"}.'
            )
        else:
            self.check_structure()
        self.check_filters()

    def resolve_names(self) -> bool:
        ok = True
        for key, i, ref in self.refs(*_CHANNELS):
            path = self.w.path(key, i)
            if ref.field == "*":
                if ref.aggregate != "count" or ref.timeUnit is not None or ref.bin is not None:
                    self.problems.append(STAR_MESSAGE)
                    ok = False
                continue
            name, problem = _resolve(ref.field, self.by_name, f"{path}.field")
            if problem is not None:
                self.problems.append(problem)
                ok = False
                continue
            if name != ref.field:
                self.replace_ref(key, i, field=name)
                self.note("column_case")
            if self.by_name[name].type == "other":
                self.problems.append(f'"{name}" cannot be charted.')
                ok = False
        return ok

    def drop_misplaced(self) -> None:
        ch = self.w.channels
        if self.w.mark != "map" and any(ch[k] for k in _GEO_CHANNELS):
            # Only a map reads coordinates: with nothing else on the chart they mean a map.
            if not (ch["x"] or ch["y"] or ch["theta"]):
                self.w.mark = "map"
                self.note("map_mark")
            else:
                for key in _GEO_CHANNELS:
                    ch[key] = []
                self.note("geo_dropped")
        if self.w.mark != "arc" and self.w.channels["theta"]:
            self.w.channels["theta"] = []
            self.note("theta_dropped")
        if self.w.mark not in ("point", "map") and self.w.channels["size"]:
            self.w.channels["size"] = []
            self.note("size_dropped")
        if self.w.mark not in ("bar", "area") and self.w.stack is not None:
            self.w.stack = None
            self.note("stack_dropped")

    def mode_repairs(self) -> None:
        w = self.w
        if w.mark == "point" and w.aggregate is None and w.channels["x"] and w.channels["y"]:
            if all(_plain(r) and self.numeric(r) for _, _, r in self.refs("x", "y")):
                w.aggregate = False
                self.note("scatter_raw")
        if w.mark in ("boxplot", "map"):
            code = f"{w.mark}_raw"
            if w.aggregate is not False:
                w.aggregate = False
                self.note(code)
            for key, i, ref in self.refs(*_CHANNELS):
                if ref.aggregate is not None and ref.field != "*":
                    self.replace_ref(key, i, aggregate=None)
                    self.note(code)

    def check_refs(self) -> None:
        for _key, _i, ref in self.refs(*_CHANNELS):
            t = self.type_of(ref)
            if t is None or t == "other":
                continue
            name = ref.field
            if ref.aggregate in _NUMBER_AGGS and t not in _NUMERIC:
                self.problems.append(
                    f'"{name}" is {_type_word(t)}. Use "count" or "distinctCount", or pick a numeric column.'
                )
            if ref.timeUnit is not None:
                if t not in _TEMPORAL:
                    dates = [c.name for c in self.by_name.values() if c.type in _TEMPORAL]
                    where = ", ".join(dates) if dates else "this table has none"
                    self.problems.append(f'"{name}" is {_type_word(t)}; timeUnit needs a date column: {where}.')
                elif ref.timeUnit == "hour" and t == "date":
                    self.problems.append(f'"{name}" has no time of day; use "day" or coarser.')
                if ref.aggregate is not None:
                    self.problems.append(
                        f'"{name}" has both timeUnit and aggregate: use timeUnit to group by period, '
                        "or aggregate to count, not both."
                    )
            if ref.bin is not None and (
                t not in _NUMERIC or ref.aggregate is not None or ref.timeUnit is not None or not 2 <= ref.bin <= 50
            ):
                self.problems.append("bin needs a numeric column and a count between 2 and 50, with no aggregate.")

    def check_structure(self) -> None:
        w = self.w
        if self.aggregated and self.typed:
            self.decimals()
        if w.mark in ("bar", "line", "area") and self.typed:
            self.one_number_axis()
        if w.mark != "table":
            for key in ("x", "y"):
                count = sum(1 for r in w.channels[key] if self.is_measure(key, r))
                if count > 1:
                    self.problems.append(
                        f"One measure per axis: {key} has {count}. Keep one; "
                        'only a table (mark "table") shows several.'
                    )
        if self.aggregated:
            self.grouped_and_aggregated()
        if w.mark == "arc":
            self.pie()
        elif w.mark == "rect":
            self.heatmap()
        elif w.mark == "point":
            self.scatter()
        elif w.mark == "boxplot" and self.typed:
            self.boxplot()
        elif w.mark == "map":
            self.geo_map()
        elif w.mark == "table" and w.aggregate is False:
            self.problems.append("A table shows aggregated values; give each number an aggregate.")
        if w.aggregate is False and w.mark not in ("boxplot", "table"):
            for key, i, ref in self.refs(*_CHANNELS):
                if ref.aggregate is not None:
                    self.problems.append(
                        f'"aggregate": false plots raw rows, but {w.path(key, i)} has "{ref.aggregate}". Remove one.'
                    )
        if w.mark not in ("arc", "map"):
            self.sort_and_limit()

    def decimals(self) -> None:
        for key, i, ref in self.refs("x", "y", "color", "size", "theta"):
            if not (_plain(ref) and self.type_of(ref) == "float"):
                continue
            if key in ("theta", "size"):
                self.replace_ref(key, i, aggregate="sum")
                self.note("decimal_sum")
            elif key in ("x", "y") and any(self.category(o) for o in self.w.channels["y" if key == "x" else "x"]):
                self.replace_ref(key, i, aggregate="sum")
                self.note("decimal_sum")
            else:
                self.problems.append(
                    f'"{ref.field}" is a decimal used as a category. Add "bin": 10, or an aggregate, '
                    'or set "aggregate": false for raw rows.'
                )

    def one_number_axis(self) -> None:
        w = self.w
        mark = w.mark
        on_x = any(self.is_measure("x", r) for r in w.channels["x"])
        on_y = any(self.is_measure("y", r) for r in w.channels["y"])
        if on_x and on_y and self.aggregated:
            self.problems.append(f'A {mark} has one number axis. Move one measure to color or use mark "point".')
            return
        if on_x or on_y:
            return
        if self.aggregated:
            candidates = [
                (k, i)
                for k, i, r in self.refs("x", "y")
                if _plain(r)
                and self.numeric(r)
                and any(not (_plain(o) and self.numeric(o)) for o in w.channels["y" if k == "x" else "x"])
            ]
            if len(candidates) == 1:
                key, i = candidates[0]
                self.replace_ref(key, i, aggregate="sum")
                self.note("measure_sum")
                return
        self.problems.append(f"A {mark} needs one number axis: give y an aggregate.")

    def grouped_and_aggregated(self) -> None:
        plain = {r.field for _, _, r in self.refs(*_CHANNELS) if _plain(r) and r.field != "*"}
        summed = {r.field for _, _, r in self.refs(*_CHANNELS) if r.aggregate is not None and r.field != "*"}
        for name in sorted(plain & summed):
            self.problems.append(f'"{name}" is both grouped and aggregated. Use "bin" on one, or drop one.')

    def pie(self) -> None:
        w = self.w
        ch = w.channels
        if (
            not ch["theta"]
            and not ch["color"]
            and len(ch["x"]) == 1
            and ch["x"][0].aggregate is None
            and len(ch["y"]) == 1
            and ch["y"][0].aggregate is not None
        ):
            ch["color"], ch["theta"] = ch["x"], ch["y"]
            ch["x"], ch["y"] = [], []
            self.note("pie_channels")
        if (
            ch["x"]
            or ch["y"]
            or not ch["theta"]
            or ch["theta"][0].aggregate is None
            or not ch["color"]
            or ch["color"][0].aggregate is not None
        ):
            self.problems.append(
                "A pie takes theta (aggregated number) and color (category) only; leave x and y empty."
            )
        if w.sort is not None or w.limit is not None:
            self.problems.append("A pie cannot be sorted or limited; filter with oneOf instead.")

    def heatmap(self) -> None:
        ch = self.w.channels
        if not (
            ch["x"]
            and ch["y"]
            and all(r.aggregate is None for r in ch["x"] + ch["y"])
            and ch["color"]
            and ch["color"][0].aggregate is not None
        ):
            self.problems.append("A heatmap needs a category on x, a category on y and an aggregated number on color.")

    def scatter(self) -> None:
        if not self.aggregated:
            return
        if not any(r.aggregate is None for _, _, r in self.refs("x", "y", "color", "size")):
            self.problems.append(
                "This would draw one point. Add a color dimension, or remove the aggregates "
                'and set "aggregate": false.'
            )

    def boxplot(self) -> None:
        ch = self.w.channels

        def value_axis(key: str) -> bool:
            refs = ch[key]
            return len(refs) == 1 and _plain(refs[0]) and self.numeric(refs[0])

        def category_axis(key: str) -> bool:
            refs = ch[key]
            return bool(refs) and all(r.field != "*" and not self.is_measure(key, r) for r in refs)

        color_ok = not ch["color"] or (ch["color"][0].field != "*" and not self.numeric(ch["color"][0]))
        shaped = (value_axis("y") and category_axis("x")) or (value_axis("x") and category_axis("y"))
        if not (shaped and color_ok):
            self.problems.append("A box plot takes categories on one axis and one plain numeric column on the other.")

    def geo_map(self) -> None:
        w = self.w
        ch = w.channels

        def coordinate(key: str) -> bool:
            refs = ch[key]
            return (
                len(refs) == 1
                and refs[0].field != "*"
                and _plain(refs[0])
                and (not self.typed or self.numeric(refs[0]))
            )

        if not all(coordinate(k) for k in _GEO_CHANNELS) or ch["x"] or ch["y"] or ch["theta"]:
            self.problems.append(
                "A map takes latitude and longitude, each one plain number column (no aggregate, bin or "
                "timeUnit), and no x, y or theta."
            )
        size = ch["size"]
        if size and self.typed and not (size[0].field != "*" and _plain(size[0]) and self.numeric(size[0])):
            self.problems.append("On a map, size takes one plain number column.")
        if w.sort is not None or w.limit is not None:
            self.problems.append("A map shows every row: it cannot be sorted or limited; filter with oneOf or range.")

    def sort_and_limit(self) -> None:
        w = self.w
        if w.sort is not None and self.typed:
            axis = next(
                (k for k in ("y", "x") if w.channels[k] and not any(self.is_measure(k, r) for r in w.channels[k])),
                None,
            )
            if axis is None:
                self.problems.append(
                    "sort orders the category axis by the measure, and this chart has no category axis. Remove sort."
                )
            else:
                last = w.channels[axis][-1]
                if (
                    last.timeUnit is not None
                    or last.bin is not None
                    or self.type_of(last) in _TEMPORAL
                    or (_plain(last) and self.numeric(last))
                ):
                    self.problems.append(
                        "A time or number axis is always in order; remove sort and limit, or use dateRange."
                    )
        if w.limit is not None and self.aggregated:
            if w.sort is None:
                self.problems.append('limit needs sort: "descending" for the top 5, "ascending" for the bottom 5.')
            dims = [r.field for k, _, r in self.refs("x", "y", "color", "size") if not self.is_measure(k, r)]
            if len(dims) > 1:
                cells = ", ".join(dims)
                color = w.channels["color"]
                if color and not self.is_measure("color", color[0]):
                    self.problems.append(
                        f'With color "{color[0].field}" the limit keeps the {w.limit} largest ({cells}) cells, '
                        f'not {w.limit} "{dims[0]}" values. Remove color or limit.'
                    )
                else:
                    self.problems.append(
                        f"The limit keeps the {w.limit} largest ({cells}) cells, not {w.limit} "
                        f'"{dims[0]}" values. Keep one category, or remove limit.'
                    )

    def check_filters(self) -> None:
        kept: list[BaseModel] = []
        for i, flt in enumerate(self.w.filters):
            path = f"filters[{i}]"
            if flt.field == "*":
                self.problems.append(STAR_MESSAGE)
                kept.append(flt)
                continue
            name, problem = _resolve(flt.field, self.by_name, f"{path}.field")
            if problem is not None:
                self.problems.append(problem)
                kept.append(flt)
                continue
            if name != flt.field:
                flt = flt.model_copy(update={"field": name})
                self.note("column_case")
            info = self.by_name[name]
            if info.type == "other":
                self.problems.append(f'"{name}" cannot be charted.')
            elif isinstance(flt, OneOfFilter | NotInFilter):
                flt = self.value_filter(flt, info)
            elif isinstance(flt, RangeFilter):
                self.range_filter(flt, info)
            elif isinstance(flt, DateRangeFilter):
                flt = self.date_range_filter(flt, info)
            kept.append(flt)
        self.w.filters = kept

    def _date_hint(self, name: str) -> str:
        return f'Filter dates with {{"field":"{name}","dateRange":["2024-01-01","2024-12-31"]}}.'

    def value_filter(self, flt: OneOfFilter | NotInFilter, info: ColumnInfo) -> BaseModel:
        key = "oneOf" if isinstance(flt, OneOfFilter) else "notIn"
        values = getattr(flt, key)
        if info.type in _TEMPORAL:
            self.problems.append(self._date_hint(info.name))
            return flt
        if not values or any(v is None for v in values):
            self.problems.append(f"{key} needs at least one value; null is not matched.")
            return flt
        coerced: list[Any] = []
        for value in values:
            ok, new = _coerce(value, info.type)
            if not ok:
                self.problems.append(_coerce_message(info))
                return flt
            coerced.append(new)
        type_changed = coerced != list(values) or any(
            type(a) is not type(b) for a, b in zip(coerced, values, strict=True)
        )
        case_changed = False
        if info.type == "string" and info.values is not None:
            known = [v for v in info.values if isinstance(v, str)]
            matched: list[Any] = []
            for value in coerced:
                if value in known:
                    matched.append(value)
                    continue
                folded = [k for k in known if k.strip().lower() == str(value).strip().lower()]
                if len(folded) == 1:
                    matched.append(folded[0])
                    case_changed = True
                    continue
                listed = ", ".join(json.dumps(v, ensure_ascii=False) for v in known)
                self.problems.append(
                    f'{json.dumps(value, ensure_ascii=False)} is not a value of "{info.name}". Values: {listed}.'
                )
                return flt
            coerced = matched
        if coerced == list(values) and all(type(a) is type(b) for a, b in zip(coerced, values, strict=True)):
            return flt
        if type_changed:
            self.note("filter_value_coerced")
        if case_changed:
            self.note("filter_value_case")
        return flt.model_copy(update={key: coerced})

    def range_filter(self, flt: RangeFilter, info: ColumnInfo) -> None:
        if info.type in _TEMPORAL:
            self.problems.append(self._date_hint(info.name))
            return
        low, high = flt.range
        if (
            info.type not in _NUMERIC
            or (low is None and high is None)
            or (low is not None and high is not None and low > high)
        ):
            self.problems.append("range needs a numeric column and [min,max] with at least one bound.")

    def date_range_filter(self, flt: DateRangeFilter, info: ColumnInfo) -> BaseModel:
        message = 'dateRange takes "YYYY-MM-DD" or null on a date column, with at least one bound and start before end.'
        if info.type not in _TEMPORAL:
            self.problems.append(message)
            return flt
        bounds = [_as_date(v) for v in flt.dateRange]
        if any(b is False for b in bounds) or all(b is None for b in bounds):
            self.problems.append(message)
            return flt
        if bounds[0] is not None and bounds[1] is not None and bounds[0] > bounds[1]:
            self.problems.append(message)
            return flt
        repaired = tuple(b.isoformat() if b is not None else None for b in bounds)
        if repaired == tuple(flt.dateRange):
            return flt
        self.note("date_range_normalized")
        return flt.model_copy(update={"dateRange": repaired})


def _as_date(value: str | None) -> date | None | Literal[False]:
    """A ``YYYY-MM-DD`` bound as a date; a full ISO datetime is cut to its date; ``False`` when unparsable."""
    if value is None:
        return None
    text = value.strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return False


def _finite(value: int | float) -> bool:
    """Whether a number is a finite float; an int too large for one is not."""
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _coerce(value: Any, type_: str) -> tuple[bool, Any]:
    """A filter value as the column's type; ``(False, value)`` when it has no unambiguous form."""
    if type_ == "boolean":
        if isinstance(value, bool):
            return True, value
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return True, value.strip().lower() == "true"
        if isinstance(value, int) and value in (0, 1):
            return True, bool(value)
        return False, value
    if isinstance(value, bool):
        return (True, str(value).lower()) if type_ == "string" else (False, value)
    if type_ == "integer":
        if isinstance(value, int):
            return True, value
        if isinstance(value, float) and value.is_integer():
            return True, int(value)
        if isinstance(value, str):
            text = value.strip()
            try:
                return True, int(text)
            except ValueError:
                pass
            try:
                number = float(text)
            except ValueError:
                return False, value
            return (True, int(number)) if number.is_integer() else (False, value)
        return False, value
    if type_ == "float":
        if isinstance(value, int | float) and _finite(value):
            return True, value
        if isinstance(value, str):
            try:
                number = float(value.strip())
            except ValueError:
                return False, value
            return (True, number) if math.isfinite(number) else (False, value)
        return False, value
    if type_ == "string":
        if isinstance(value, str):
            return True, value
        if isinstance(value, int):
            return True, str(value)
        if isinstance(value, float):
            return True, str(int(value)) if value.is_integer() else str(value)
        return False, value
    return True, value


def _coerce_message(info: ColumnInfo) -> str:
    if info.type == "boolean":
        return f'"{info.name}" is boolean; use true or false.'
    if info.type == "integer":
        return f'"{info.name}" holds whole numbers; use numbers such as 3.'
    if info.type == "float":
        return f'"{info.name}" holds numbers; use numbers such as 9.5.'
    return f'"{info.name}" is text; use strings such as "West".'


def check_chart(
    chart: ChartRequest, columns: Sequence[ColumnInfo], repairs: list[str] | None = None
) -> tuple[ChartRequest, list[str]]:
    """Run the repair and validation rules; returns the repaired request and every problem left.

    The codes of the repairs made are appended to ``repairs`` when it is given.
    """
    work = _Work.of(chart)
    checker = _Checker(work, columns, repairs)
    checker.run()
    return work.build(), checker.problems


def review_chart(
    raw: Any, columns: Sequence[ColumnInfo], repairs: list[str] | None = None
) -> tuple[ChartRequest | None, list[str]]:
    """Shape repair, parse and rules in one: ``(chart, [])`` when the browser can draw it."""
    normalized = normalize_chart_shape(raw, [c.name for c in columns], repairs)
    chart, problems = parse_chart(normalized)
    if chart is None:
        return None, problems
    repaired, problems = check_chart(chart, columns, repairs)
    return (None, problems) if problems else (repaired, [])


def problems_message(problems: Sequence[str]) -> str:
    count = len(problems)
    lines = "\n".join(f"- {p}" for p in problems)
    return f"The chart request has {count} problem{'s' if count != 1 else ''}:\n{lines}"


# The prompt.

_SYSTEM_PROMPT = """\
You build one chart in Flowfile's chart builder from a plain-English request. You do not
write a chart library spec: you write a small CHART REQUEST, which Flowfile turns into the
chart. The user message gives the table: every column with its type (string, boolean,
integer, float, date, datetime, other), its role (measure or dimension) and a profile
(non-null count, distinct count, min, max, example values, and for some text columns the
full list of values).

Output a single JSON object, no prose and no markdown fences:

  {"chart": <chart request>, "title": "<short chart title>", "explanation": "<one short sentence>"}

The chart request:

  {
    "mark": "bar" | "line" | "area" | "point" | "arc" | "rect" | "boxplot" | "table" | "map",
    "x": FieldRef or [FieldRef, ...],
    "y": FieldRef or [FieldRef, ...],
    "color": FieldRef,
    "size": FieldRef,            (scatter and map only)
    "theta": FieldRef,           (pie only, required there)
    "latitude": FieldRef,        (map only, required there)
    "longitude": FieldRef,       (map only, required there)
    "filters": [ {"field": ..., "oneOf": [...]} | {"field": ..., "notIn": [...]}
               | {"field": ..., "range": [min or null, max or null]}
               | {"field": ..., "dateRange": ["YYYY-MM-DD" or null, "YYYY-MM-DD" or null]} ],
    "aggregate": true | false,   (default true; false plots raw rows)
    "stack": "stack" | "none" | "normalize",   (bar and area only)
    "sort": "ascending" | "descending",        (orders the category axis by the measure)
    "limit": N                                  (keep the first N after sort)
  }

  FieldRef = {"field": "<exact column name>",
              "aggregate": "sum" | "mean" | "median" | "min" | "max" | "count" | "distinctCount",
              "timeUnit": "year" | "quarter" | "month" | "week" | "day" | "hour",
              "bin": <number of equal-width bins, 2 to 50>}

Leave out every key you do not need. Do not invent keys.

Rules that matter most:
* A FieldRef WITH "aggregate" is a number (a measure). A FieldRef WITHOUT "aggregate" is a
  group (a category, a dimension). "sum of quantity" is {"field": "quantity", "aggregate": "sum"};
  "per region" is {"field": "region"}.
* Use the EXACT column names from the column list. Never invent a column.
* To count rows use {"field": "*", "aggregate": "count"}. "*" means nothing else.
* sum, mean, median, min and max only work on integer and float columns. For a text,
  boolean or date column use "count" or "distinctCount".
* One measure per axis. A bar, line or area chart has exactly one number axis: the category
  on x and the aggregated number on y (or the other way round for horizontal bars).
* Dates: group a date or datetime column with "timeUnit" (year, quarter, month, week, day;
  "hour" only on a datetime column). Never put a raw date column on an axis without a timeUnit
  when you mean "per month" or "per year".
* Filter dates ONLY with "dateRange" and "YYYY-MM-DD" strings, both ends inclusive:
  "in 2025" is {"field": "order_date", "dateRange": ["2025-01-01", "2025-12-31"]}.
  Never use oneOf or range on a date column.
* oneOf and notIn take values exactly as the profile lists them. Booleans are true or false,
  numbers are numbers, text is a string.
* Sort and limit belong to the request, never inside a FieldRef. "sort" orders the category
  axis by the measure; "limit" keeps the first N after sorting, so "top 5" is
  "sort": "descending", "limit": 5 and "bottom 3" is "sort": "ascending", "limit": 3.
  A limit always needs a sort. Never combine a limit with a color category. A date, timeUnit,
  bin or number axis is already in order: no sort there.
* Pie charts: "mark": "arc", "theta" is the aggregated number, "color" is the category.
  No x, no y, no sort, no limit.
* Scatter plots: "mark": "point", numeric columns on x and y, "aggregate": false.
* Histograms: "mark": "bar", x is a numeric column with "bin", y counts rows.
* Heatmaps: "mark": "rect", a category on x, a category on y, an aggregated number on color.
* Box plots: "mark": "boxplot", a category on one axis, one plain numeric column on the other,
  "aggregate": false.
* Tables: "mark": "table", categories on y, aggregated numbers on x (several are fine here).
* Maps: "mark": "map" draws one point per row at its coordinates. "latitude" and "longitude" are
  the plain number columns that hold them (no aggregate), with "aggregate": false. "color" (a
  category or a number) and "size" (a number) are optional. No x, no y, no theta, no sort, no limit.
  Use a map whenever the request asks for locations, places, a map or where things are, and the
  table has latitude and longitude columns (often named lat, latitude, lon, lng, longitude).
  A map shows ONE set of points: when the table holds two pairs of coordinates (for example a
  customer's and a depot's), map the pair the request is about; the other pair cannot be added.
* When a current chart is given, the request is a CHANGE to it: change only what the request
  asks for, keep every other part of the current chart, and return the whole changed chart.
* If the request cannot be drawn from this table, answer {"chart": null, "title": null,
  "explanation": "<why>"} instead of guessing.

Worked examples, for a table "sales" with columns order_date (date), region (string, values
"East", "North", "South", "West"), category (string), product (string), revenue (float),
quantity (integer), price (float), is_return (boolean), store_lat (float), store_lon (float):

Request: revenue by region
{"chart": {"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}},
 "title": "Revenue by region", "explanation": "Total revenue for each region."}

Request: monthly revenue trend
{"chart": {"mark": "line", "x": {"field": "order_date", "timeUnit": "month"},
 "y": {"field": "revenue", "aggregate": "sum"}},
 "title": "Monthly revenue", "explanation": "Total revenue per month."}

Request: top 5 products by revenue
{"chart": {"mark": "bar", "x": {"field": "product"}, "y": {"field": "revenue", "aggregate": "sum"},
 "sort": "descending", "limit": 5},
 "title": "Top 5 products by revenue", "explanation": "The five products with the most revenue."}

Request: share of revenue per category as a pie
{"chart": {"mark": "arc", "theta": {"field": "revenue", "aggregate": "sum"}, "color": {"field": "category"}},
 "title": "Revenue share by category", "explanation": "Each slice is a category's share of revenue."}

Request: price against quantity
{"chart": {"mark": "point", "x": {"field": "price"}, "y": {"field": "quantity"}, "aggregate": false},
 "title": "Price vs quantity", "explanation": "One point per order."}

Request: histogram of price
{"chart": {"mark": "bar", "x": {"field": "price", "bin": 20}, "y": {"field": "*", "aggregate": "count"}},
 "title": "Price distribution", "explanation": "Number of orders in 20 price bins."}

Request: revenue by category in the West during 2024
{"chart": {"mark": "bar", "x": {"field": "category"}, "y": {"field": "revenue", "aggregate": "sum"},
 "filters": [{"field": "region", "oneOf": ["West"]},
             {"field": "order_date", "dateRange": ["2024-01-01", "2024-12-31"]}]},
 "title": "West revenue by category, 2024", "explanation": "Revenue per category, West region, 2024 only."}

Request: number of orders per region, stacked by category
{"chart": {"mark": "bar", "x": {"field": "region"}, "y": {"field": "*", "aggregate": "count"},
 "color": {"field": "category"}, "stack": "stack"},
 "title": "Orders by region and category", "explanation": "Order count per region, split by category."}

Request: horizontal bars of average price per category
{"chart": {"mark": "bar", "x": {"field": "price", "aggregate": "mean"}, "y": {"field": "category"}},
 "title": "Average price by category", "explanation": "Mean price for each category."}

Request: heatmap of revenue by region and category
{"chart": {"mark": "rect", "x": {"field": "region"}, "y": {"field": "category"},
 "color": {"field": "revenue", "aggregate": "sum"}},
 "title": "Revenue by region and category", "explanation": "Darker cells hold more revenue."}

Request: spread of price per category
{"chart": {"mark": "boxplot", "x": {"field": "category"}, "y": {"field": "price"}, "aggregate": false},
 "title": "Price spread by category", "explanation": "A box plot of price for each category."}

Request: table of revenue and quantity per region
{"chart": {"mark": "table", "y": {"field": "region"},
 "x": [{"field": "revenue", "aggregate": "sum"}, {"field": "quantity", "aggregate": "sum"}]},
 "title": "Revenue and quantity by region", "explanation": "Totals per region."}

Request: where are the orders, on a map sized by revenue
{"chart": {"mark": "map", "latitude": {"field": "store_lat"}, "longitude": {"field": "store_lon"},
 "size": {"field": "revenue"}, "color": {"field": "region"}, "aggregate": false},
 "title": "Orders by store location", "explanation": "One point per order at its store, sized by revenue."}

Request: how many returns are there per month
{"chart": {"mark": "bar", "x": {"field": "order_date", "timeUnit": "month"}, "y": {"field": "*", "aggregate": "count"},
 "filters": [{"field": "is_return", "oneOf": [true]}]},
 "title": "Returns per month", "explanation": "Number of returned orders each month."}

Follow-up examples. The current chart is given in the user message.

Current chart: {"mark": "bar", "x": {"field": "order_date", "timeUnit": "month"},
 "y": {"field": "revenue", "aggregate": "sum"}}
Request: make it a line
{"chart": {"mark": "line", "x": {"field": "order_date", "timeUnit": "month"},
 "y": {"field": "revenue", "aggregate": "sum"}},
 "title": "Monthly revenue", "explanation": "The same chart as a line."}

Current chart: {"mark": "line", "x": {"field": "order_date", "timeUnit": "month"},
 "y": {"field": "revenue", "aggregate": "sum"}}
Request: only 2025
{"chart": {"mark": "line", "x": {"field": "order_date", "timeUnit": "month"},
 "y": {"field": "revenue", "aggregate": "sum"},
 "filters": [{"field": "order_date", "dateRange": ["2025-01-01", "2025-12-31"]}]},
 "title": "Monthly revenue, 2025", "explanation": "The same chart limited to 2025."}

Current chart: {"mark": "line", "x": {"field": "order_date", "timeUnit": "month"},
 "y": {"field": "revenue", "aggregate": "sum"}}
Request: split by region
{"chart": {"mark": "line", "x": {"field": "order_date", "timeUnit": "month"},
 "y": {"field": "revenue", "aggregate": "sum"}, "color": {"field": "region"}},
 "title": "Monthly revenue by region", "explanation": "One line per region."}

Current chart: {"mark": "bar", "x": {"field": "product"}, "y": {"field": "revenue", "aggregate": "sum"},
 "sort": "descending", "limit": 5}
Request: show the top 10 instead
{"chart": {"mark": "bar", "x": {"field": "product"}, "y": {"field": "revenue", "aggregate": "sum"},
 "sort": "descending", "limit": 10},
 "title": "Top 10 products by revenue", "explanation": "The ten products with the most revenue."}

Request: what will the weather be tomorrow
{"chart": null, "title": null, "explanation": "The table holds sales, not weather."}
"""


def system_prompt() -> str:
    return _SYSTEM_PROMPT


def user_message(*, profile_text: str, prompt: str, current_chart: ChartRequest | None) -> str:
    parts = [f"## Table\n\n{profile_text}"]
    if current_chart is not None:
        current = json.dumps(current_chart.model_dump(), ensure_ascii=False)
        parts.append(
            "## Current chart\n\n"
            f"{current}\n\n"
            "This chart is on screen. The request is a change to it: change only what the request asks "
            "for, keep every other part, and return the whole changed chart."
        )
    else:
        parts.append("## Current chart\n\nNone: build a new chart.")
    parts.append(f"## Request\n\n{prompt}")
    parts.append('Answer now with the JSON object {"chart": ..., "title": ..., "explanation": ...}.')
    return "\n\n".join(parts)


def retry_message(problem_text: str) -> str:
    return (
        f"{problem_text}\n\n"
        'Answer again with the whole JSON object {"chart": ..., "title": ..., "explanation": ...}, '
        "with these problems fixed. Change nothing else."
    )


# The model call.


def _text(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:limit] or None


@dataclass
class _Answer:
    response: ChartGenerationResponse | None = None
    reason: str | None = None
    problem: str | None = None


def read_answer(content: str | None, columns: Sequence[ColumnInfo]) -> _Answer:
    """One model answer: a chart, a decline (``no_chart``), or a reason plus the message for the retry.

    An accepted chart carries the codes of the repairs it needed in ``repairs``.
    """
    repairs: list[str] = []
    payload, err = parse_json_payload(
        content, extract_object=True, on_extracted=lambda: _note(repairs, "json_extracted")
    )
    if err == "empty_response":
        return _Answer(reason=err, problem='The answer was empty. Answer with one JSON object {"chart": ...}.')
    if err == "parse_error":
        return _Answer(
            reason=err, problem="The answer was not valid JSON. Answer with one JSON object and nothing else."
        )
    payload = normalize_answer(payload, repairs)
    if not isinstance(payload, dict) or "chart" not in payload:
        return _Answer(
            reason="validation_error",
            problem='The answer must be a JSON object {"chart": {...}, "title": "...", "explanation": "..."}.',
        )
    explanation = _text(payload.get("explanation"), _MAX_EXPLANATION)
    if payload["chart"] is None:
        return _Answer(reason="no_chart", problem=explanation or "The model did not build a chart.")
    chart, problems = review_chart(payload["chart"], columns, repairs)
    if chart is None:
        return _Answer(reason="invalid_chart", problem=problems_message(problems))
    return _Answer(
        response=ChartGenerationResponse(
            chart=chart, title=_text(payload.get("title"), _MAX_TITLE), explanation=explanation, repairs=repairs
        )
    )


async def _call(
    *,
    provider: Provider,
    messages: list[Message],
    timeout: float,
    scheduler: RateLimitScheduler | None,
    user_id: int | None,
    surface: str = SURFACE,
    max_tokens: int = _MAX_TOKENS,
) -> tuple[str | None, str | None, str | None]:
    """One JSON-mode call under ``timeout``: ``(content, reason, message)``."""
    sched = scheduler or default_scheduler()

    async def _do_call() -> str | None:
        async with sched.acquire(provider.name, surface=surface):
            response = await provider.chat(
                messages=messages,
                tools=None,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
                surface=surface,
                user_id=user_id,
            )
        return response.content

    try:
        return await asyncio.wait_for(_do_call(), timeout=timeout), None, None
    except TimeoutError:
        return None, "timeout", "The model did not answer in time."
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s provider call failed: %s", surface, exc, exc_info=False)
        return None, "provider_error", f"The AI provider call failed ({type(exc).__name__})."


def _oversized(current_chart: Any) -> bool:
    """Whether the chart on screen is too large to check and send; one that cannot be measured counts."""
    try:
        return len(json.dumps(current_chart, default=str)) > MAX_CURRENT_CHART_CHARS
    except (TypeError, ValueError, RecursionError):
        return True


def _record(provider: Provider, started: float, reason: str | None, surface: str = SURFACE) -> None:
    record_autocomplete_call(
        surface=surface,
        provider=provider.name,
        latency_ms=int((time.monotonic() - started) * 1000),
        suggestion_count=0 if reason else 1,
        degraded_reason=reason,
    )


async def generate_chart(
    *,
    prompt: str,
    columns: Sequence[ColumnInfo],
    profile_text: str,
    provider: Provider,
    user_id: int | None = None,
    current_chart: Any = None,
    warnings: Sequence[str] = (),
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> ChartGenerationResponse:
    """Turn a plain-English request into a chart request for the table ``columns`` describe.

    ``current_chart`` (the chart on screen, as a chart request) makes the prompt a change to
    it; one that fails the checks is dropped with a warning. ``timeout`` covers both calls.
    Never raises.
    """
    started = time.monotonic()
    notes = list(warnings)
    attempts = 0

    def degraded(reason: str, message: str | None, explanation: str | None = None) -> ChartGenerationResponse:
        _record(provider, started, reason)
        return ChartGenerationResponse(
            degraded=True, reason=reason, message=message, explanation=explanation, warnings=notes, attempts=attempts
        )

    text = (prompt or "").strip()
    if not text:
        return degraded("empty_prompt", "Describe the chart you want.")
    if not columns:
        return degraded("no_columns", "This table has no known columns to chart.")

    current: ChartRequest | None = None
    if current_chart is not None:
        if _oversized(current_chart):
            problems = [f"It is larger than {MAX_CURRENT_CHART_CHARS} characters."]
        else:
            current, problems = review_chart(current_chart, columns)
        if current is None:
            notes.append(
                "The current chart could not be used for a change ("
                + " ".join(problems)
                + "); the chart was built from the prompt alone."
            )

    messages = [
        Message(role="system", content=system_prompt()),
        Message(role="user", content=user_message(profile_text=profile_text, prompt=text, current_chart=current)),
    ]
    deadline = started + timeout
    answer = _Answer()
    for attempt in range(RETRY_ROUNDS + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return degraded("timeout", "The model did not answer in time.")
        attempts += 1
        content, err, err_message = await _call(
            provider=provider, messages=messages, timeout=remaining, scheduler=scheduler, user_id=user_id
        )
        if err is not None:
            if answer.problem:
                err_message = f"{err_message} Its first answer had problems.\n{answer.problem}"
            return degraded(err, err_message)
        answer = read_answer(content, columns)
        if answer.response is not None:
            _record(provider, started, None)
            answer.response.warnings = notes
            answer.response.attempts = attempts
            return answer.response
        if answer.reason == "no_chart":
            return degraded("no_chart", answer.problem, explanation=answer.problem)
        if attempt == RETRY_ROUNDS:
            return degraded(answer.reason or "invalid_chart", answer.problem)
        logger.info("chart answer refused (%s), retrying: %s", answer.reason, answer.problem)
        messages = [
            *messages,
            Message(role="assistant", content=content or ""),
            Message(role="user", content=retry_message(answer.problem or "")),
        ]
    raise AssertionError("unreachable")  # pragma: no cover


__all__ = [
    "AGGREGATES",
    "DEFAULT_TIMEOUT_SECONDS",
    "MARKS",
    "MAX_CURRENT_CHART_CHARS",
    "MAX_PROMPT_LEN",
    "MAX_TIMEOUT_SECONDS",
    "REASONS",
    "REPAIRS",
    "SURFACE",
    "TIME_UNITS",
    "ChartGenerationResponse",
    "ChartRequest",
    "ColumnInfo",
    "DateRangeFilter",
    "FieldRef",
    "NotInFilter",
    "OneOfFilter",
    "RangeFilter",
    "TypeClass",
    "check_chart",
    "generate_chart",
    "normalize_answer",
    "normalize_chart_shape",
    "parse_chart",
    "problems_message",
    "read_answer",
    "review_chart",
    "system_prompt",
    "user_message",
]
