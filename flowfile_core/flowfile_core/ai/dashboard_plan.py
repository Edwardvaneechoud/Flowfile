"""A question over several catalog tables → the plan of a dashboard page (``POST /ai/plan_dashboard``).

The first of three stages. The model reads the profile of every selected source and writes a
*plan*: a title, a few KPIs, an ordered list of *checks* and, when the question asks to filter by
something, dashboard filters. A check names one source, says in
words which chart to build (``brief``, the prompt ``POST /ai/generate_chart`` gets for it) and
what the chart tests (``purpose``); ``relation`` names two number columns whose correlation the
story stage measures. Nothing here claims anything about the data: the claims are written by
``dashboard_story`` from what each chart returned.

The model names a source by its key (the table's name, made distinct when two share one) and
core assigns the ids. ``check_plan`` repairs what is unambiguous, sends every problem back to
the model once, and on the last answer keeps the valid items and warns about the rest; only a
plan with no valid check is refused. JSON mode, one deadline for both calls. Never raises: a
failure is a result with ``degraded=True`` and a ``reason``. Must not import ``litellm``.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from flowfile_core.ai.chart_request import (
    _AGGREGATE_ALIASES,
    _NUMBER_AGGS,
    _NUMERIC,
    ColumnInfo,
    _call,
    _parse_json_payload,
    _record,
    _resolve,
    _text,
    _type_word,
    ref_from_string,
)
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler

logger = logging.getLogger(__name__)

SURFACE: str = "dashboard"
DEFAULT_TIMEOUT_SECONDS: float = 60.0
MAX_TIMEOUT_SECONDS: float = 120.0
MAX_SOURCES: int = 6
MAX_CHECKS: int = 8
MAX_KPIS: int = 4
MAX_FILTERS: int = 3
RETRY_ROUNDS: int = 1
DETAILED_COLUMNS: int = 40
_MAX_TOKENS: int = 2500
_MAX_TITLE: int = 120
_MAX_BRIEF: int = 600
_MAX_PURPOSE: int = 300
_MAX_EXPLANATION: int = 400

REASONS: tuple[str, ...] = (
    "empty_prompt",
    "no_columns",
    "timeout",
    "provider_error",
    "empty_response",
    "parse_error",
    "validation_error",
    "no_plan",
    "invalid_plan",
)
"""Every ``reason`` a degraded plan can carry."""

KpiAgg = Literal["sum", "mean", "median", "min", "max", "count", "distinctCount"]
FilterKind = Literal["categorical", "date_range", "numeric_range"]

_FILTER_KINDS: dict[str, tuple[str, ...]] = {
    "string": ("categorical",),
    "boolean": ("categorical",),
    "integer": ("numeric_range", "categorical"),
    "float": ("numeric_range",),
    "date": ("date_range",),
    "datetime": ("date_range",),
}
"""The filter kinds a column type takes, the default first (the filter dialog's own rule)."""


class PlannedRelation(BaseModel):
    """Two number columns of one source whose correlation the story stage measures."""

    model_config = ConfigDict(extra="forbid")

    x: str
    y: str


class PlannedKpi(BaseModel):
    """One headline number: ``agg`` of ``field`` over the source; no field with ``count`` counts rows."""

    model_config = ConfigDict(extra="forbid")

    id: str
    table_id: int
    label: str
    field: str | None = None
    agg: KpiAgg = "sum"


class PlannedCheck(BaseModel):
    """One chart to build and what it tests. ``brief`` is the prompt the chart model gets."""

    model_config = ConfigDict(extra="forbid")

    id: str
    table_id: int
    title: str
    brief: str
    purpose: str
    relation: PlannedRelation | None = None


class PlannedFilter(BaseModel):
    """A dashboard filter on one column of a source; it reaches the page's tiles that read that source."""

    model_config = ConfigDict(extra="forbid")

    id: str
    table_id: int
    field: str
    kind: FilterKind
    label: str | None = None


class DashboardPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    kpis: list[PlannedKpi] = Field(default_factory=list)
    checks: list[PlannedCheck] = Field(default_factory=list)
    filters: list[PlannedFilter] = Field(default_factory=list)


class DashboardPlanResponse(BaseModel):
    """``plan`` is set on success; a degraded result carries ``reason`` and a ``message``."""

    model_config = ConfigDict(extra="forbid")

    plan: DashboardPlan | None = None
    explanation: str | None = None
    warnings: list[str] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None
    message: str | None = None
    attempts: int = 0


@dataclass(frozen=True)
class PlanSource:
    """A selected table as the plan sees it. ``key`` is the name the model uses for it."""

    key: str
    table_id: int
    columns: tuple[ColumnInfo, ...]
    profile_text: str


def source_keys(names: Sequence[str]) -> list[str]:
    """One distinct key per name, in order: the name, then ``name_2``, ``name_3`` for repeats."""
    taken: set[str] = set()
    keys: list[str] = []
    for name in names:
        key, n = name, 1
        while key.lower() in taken:
            n += 1
            key = f"{name}_{n}"
        taken.add(key.lower())
        keys.append(key)
    return keys


def compact_profile(profile_text: str, detailed: int = DETAILED_COLUMNS) -> str:
    """``render_profile``'s text with the columns past ``detailed`` cut to their name and type."""
    lines = profile_text.split("\n")
    start = next((i for i, line in enumerate(lines) if line.startswith("Columns (")), None)
    if start is None or len(lines) - start - 1 <= detailed:
        return profile_text
    kept = lines[: start + 1 + detailed]
    rest = [line[2:].split(";", 1)[0] for line in lines[start + 1 + detailed :]]
    return "\n".join([*kept, "More columns (name: type, role): " + " | ".join(rest)])


def possible_links(sources: Sequence[PlanSource]) -> list[str]:
    """Columns with one name and type in more than one source, as lines; from the schemas alone."""
    seen: dict[tuple[str, str], list[str]] = {}
    for source in sources:
        for col in source.columns:
            if col.type == "other":
                continue
            seen.setdefault((col.name.strip().lower(), col.type), []).append(source.key)
    lines = []
    for (name, type_), keys in seen.items():
        if len(keys) > 1:
            where = " and ".join(json.dumps(k, ensure_ascii=False) for k in keys)
            lines.append(f'- "{name}" ({_type_word(type_)}) is in {where}.')
    return lines


# Validation: one model answer against the selected sources.


def _source_of(raw: Any, sources: Sequence[PlanSource], path: str) -> tuple[PlanSource | None, str | None]:
    name = raw.get("source", raw.get("table")) if isinstance(raw, dict) else None
    if name is None and len(sources) == 1:
        return sources[0], None
    if isinstance(name, str):
        key = name.strip().lower()
        matches = [s for s in sources if s.key.lower() == key]
        if len(matches) == 1:
            return matches[0], None
    known = ", ".join(json.dumps(s.key, ensure_ascii=False) for s in sources)
    return None, f"{path}.source {json.dumps(name, ensure_ascii=False)} is not a source. Sources: {known}"


def _relation(raw: Any, source: PlanSource, title: str, warnings: list[str]) -> PlannedRelation | None:
    """The two columns of a relation, or None with a warning when it cannot be measured."""
    if raw is None:
        return None
    pair = [raw.get("x"), raw.get("y")] if isinstance(raw, dict) else raw
    if not (isinstance(pair, list | tuple) and len(pair) == 2 and all(isinstance(p, str) for p in pair)):
        warnings.append(f'The relation of "{title}" was not two column names; it is not measured.')
        return None
    by_name = {c.name: c for c in source.columns}
    names = [_resolve(p, by_name, "relation")[0] for p in pair]
    if None in names or names[0] == names[1] or any(by_name[n].type not in _NUMERIC for n in names):
        warnings.append(
            f'The relation of "{title}" ({pair[0]}, {pair[1]}) does not name two different number columns '
            f'of "{source.key}"; it is not measured.'
        )
        return None
    return PlannedRelation(x=names[0], y=names[1])


def _check(raw: Any, i: int, sources: Sequence[PlanSource], warnings: list[str]) -> tuple[dict | None, list[str]]:
    path = f"checks[{i}]"
    if not isinstance(raw, dict):
        return None, [f'{path} must be an object {{"source", "title", "chart", "tests"}}.']
    source, problem = _source_of(raw, sources, path)
    problems = [problem] if problem else []
    title = _text(raw.get("title"), _MAX_TITLE)
    brief = _text(raw.get("chart", raw.get("brief")), _MAX_BRIEF)
    if title is None:
        problems.append(f"{path}.title is missing: give the chart a short title.")
    if brief is None:
        problems.append(f"{path}.chart is missing: say in one sentence which chart to build, with exact column names.")
    if problems or source is None:
        return None, problems
    return {
        "table_id": source.table_id,
        "title": title,
        "brief": brief,
        "purpose": _text(raw.get("tests", raw.get("purpose")), _MAX_PURPOSE) or title,
        "relation": _relation(raw.get("relation"), source, title, warnings),
    }, []


def _kpi(raw: Any, i: int, sources: Sequence[PlanSource]) -> tuple[dict | None, list[str]]:
    path = f"kpis[{i}]"
    if not isinstance(raw, dict):
        return None, [f'{path} must be an object {{"source", "label", "field", "agg"}}.']
    source, problem = _source_of(raw, sources, path)
    if source is None:
        return None, [problem or f"{path}.source is missing."]
    by_name = {c.name: c for c in source.columns}
    field, agg = raw.get("field"), raw.get("agg", raw.get("aggregate"))
    if isinstance(field, str) and field not in by_name and agg is None:
        ref = ref_from_string(field, list(by_name))
        field, agg = ref["field"], ref.get("aggregate")
    agg = _AGGREGATE_ALIASES.get(str(agg).strip().lower()) if agg is not None else None
    if agg is None:
        return None, [f"{path}.agg must be one of sum, mean, median, min, max, count, distinctCount."]
    if field in (None, "", "*"):
        if agg != "count":
            return None, [f'{path}.field is missing: "{agg}" needs a column; only "count" counts rows without one.']
        field = None
    else:
        field, problem = _resolve(str(field), by_name, f"{path}.field")
        if field is None:
            return None, [problem or f"{path}.field is not a column."]
        info = by_name[field]
        if agg in _NUMBER_AGGS and info.type not in _NUMERIC:
            return None, [
                f'{path}: "{agg}" needs a number column, and "{field}" is {_type_word(info.type)}. '
                'Use "count" or "distinctCount" for it, or pick a number column.'
            ]
    label = _text(raw.get("label", raw.get("title")), _MAX_TITLE) or (f"{agg} of {field}" if field else "Rows")
    return {"table_id": source.table_id, "label": label, "field": field, "agg": agg}, []


def _filter(raw: Any, i: int, sources: Sequence[PlanSource]) -> tuple[dict | None, list[str]]:
    path = f"filters[{i}]"
    if isinstance(raw, str):
        raw = {"field": raw}
    if not isinstance(raw, dict):
        return None, [f'{path} must be an object {{"source", "field", "label"}}.']
    source, problem = _source_of(raw, sources, path)
    if source is None:
        return None, [problem or f"{path}.source is missing."]
    by_name = {c.name: c for c in source.columns}
    named = raw.get("field", raw.get("column"))
    if not isinstance(named, str) or not named.strip():
        return None, [f"{path}.field is missing: name the column the reader filters by."]
    field, problem = _resolve(named, by_name, f"{path}.field")
    if field is None:
        return None, [problem or f"{path}.field is not a column."]
    kinds = _FILTER_KINDS.get(by_name[field].type)
    if kinds is None:
        return None, [f'{path}: "{field}" cannot be filtered by; pick a text, number or date column.']
    asked = str(raw.get("kind", "")).strip().lower()
    return {
        "table_id": source.table_id,
        "field": field,
        "kind": asked if asked in kinds else kinds[0],
        "label": _text(raw.get("label", raw.get("title")), _MAX_TITLE),
    }, []


def check_plan(raw: Any, sources: Sequence[PlanSource]) -> tuple[DashboardPlan | None, list[str], list[str]]:
    """``(plan, problems, warnings)``: the valid items of ``raw`` as a plan, and what was wrong.

    ``plan`` is None only when no check is valid. ``problems`` are for the model (one sentence
    each), ``warnings`` for the user (what was dropped or trimmed without asking again).
    """
    warnings: list[str] = []
    if not isinstance(raw, dict):
        return None, ['The answer must be a JSON object {"title", "kpis", "checks"}.'], warnings
    problems: list[str] = []
    raw_checks = raw.get("checks")
    if not isinstance(raw_checks, list) or not raw_checks:
        return None, ['"checks" must be a list with at least one chart to build.'], warnings
    raw_kpis = raw.get("kpis") if isinstance(raw.get("kpis"), list) else []

    checks: list[dict] = []
    for i, item in enumerate(raw_checks):
        check, found = _check(item, i, sources, warnings)
        problems.extend(found)
        if check is not None:
            checks.append(check)
    kpis: list[dict] = []
    for i, item in enumerate(raw_kpis):
        kpi, found = _kpi(item, i, sources)
        problems.extend(found)
        if kpi is not None:
            kpis.append(kpi)

    filters: list[dict] = []
    for i, item in enumerate(raw.get("filters") if isinstance(raw.get("filters"), list) else []):
        found_filter, found = _filter(item, i, sources)
        problems.extend(found)
        if found_filter is not None and not any(
            (f["table_id"], f["field"]) == (found_filter["table_id"], found_filter["field"]) for f in filters
        ):
            filters.append(found_filter)

    if not checks:
        return None, problems, warnings
    if len(checks) > MAX_CHECKS:
        warnings.append(f"The plan had {len(checks)} charts; the first {MAX_CHECKS} were kept.")
    if len(kpis) > MAX_KPIS:
        warnings.append(f"The plan had {len(kpis)} KPIs; the first {MAX_KPIS} were kept.")
    checks, kpis = checks[:MAX_CHECKS], kpis[:MAX_KPIS]
    read = {item["table_id"] for item in (*checks, *kpis)}
    keys = {s.table_id: s.key for s in sources}
    for unused in [f for f in filters if f["table_id"] not in read]:
        warnings.append(
            f'The filter on "{unused["field"]}" was left out: nothing on the page reads "{keys[unused["table_id"]]}".'
        )
    filters = [f for f in filters if f["table_id"] in read]
    if len(filters) > MAX_FILTERS:
        warnings.append(f"The plan had {len(filters)} filters; the first {MAX_FILTERS} were kept.")
    plan = DashboardPlan(
        title=_text(raw.get("title"), _MAX_TITLE) or "Dashboard",
        kpis=[PlannedKpi(id=f"k{n}", **k) for n, k in enumerate(kpis, start=1)],
        checks=[PlannedCheck(id=f"c{n}", **c) for n, c in enumerate(checks, start=1)],
        filters=[PlannedFilter(id=f"f{n}", **f) for n, f in enumerate(filters[:MAX_FILTERS], start=1)],
    )
    return plan, problems, warnings


def problems_message(problems: Sequence[str]) -> str:
    count = len(problems)
    lines = "\n".join(f"- {p}" for p in problems)
    return f"The plan has {count} problem{'s' if count != 1 else ''}:\n{lines}"


# The prompt.

_SYSTEM_PROMPT = """\
You plan one dashboard page that answers a question about the user's data.

You get the selected sources (tables), each with its columns, types and a small profile, and the
question. You do not build the charts yourself. You write a plan: a title, a few KPIs, an
ordered list of checks, and filters when the question asks for them. Each check is one chart
that another model builds from your one-sentence
description, and that tests one thing about the data. After the charts are built, their real
numbers are read and the page's text is written from them. So plan what should be LOOKED AT; do
not state findings, and do not guess what the data will show.

Answer with one JSON object and nothing else:

{"title": "<page title>",
 "kpis": [{"source": "<source>", "label": "<label>", "field": "<column>", "agg": "<agg>"}],
 "checks": [{"source": "<source>", "title": "<chart title>", "chart": "<which chart to build>",
             "tests": "<what this chart tests>", "relation": ["<number column>", "<number column>"]}],
 "filters": [{"source": "<source>", "field": "<column>", "label": "<label>"}]}

Rules that matter most:

- "source" is the exact name of one of the sources. Every KPI and every check reads ONE source.
  You cannot join sources. When the question needs two sources, plan one check on each.
- Use the exact column names of that source, spelled as in its column list.
- "chart" is a full instruction for a chart builder that sees only that source and your sentence.
  Name the chart type, the columns, the aggregate (sum, mean, median, min, max, count, distinct
  count) and, for a date, the time unit (year, quarter, month, week, day). One chart per check.
- "tests" says what the chart is there to find out, as a question or a thing to compare.
- Order the checks as a story: first the overall picture, then the breakdowns that explain it,
  then how it moves over time, then the relations between numbers, then the detail.
- Plan 3 to 6 checks. Each check must test something different. Never two charts of the same thing.
- "kpis": 0 to 4 headline numbers. "agg" is one of sum, mean, median, min, max, count,
  distinctCount. sum, mean, median, min and max need a number column. To count rows use
  {"agg": "count"} with no "field".
- "relation" is optional: add it only to a check that asks whether two NUMBER columns of the same
  source move together (a scatter plot). Give the two column names. Leave it out everywhere else.
- A high-cardinality text column (many distinct values) needs a top-N: say "top 10 ... by ...".
- "filters": 0 to 3 controls the READER uses to narrow the page. Add one only when the question
  asks to filter, select, choose, slice or drill by something ("that I can filter per month",
  "for a region I pick", "by period"). Name the column the reader filters by, in a source that a
  check or KPI reads. A date column gives a period picker: that is how "filter per month", "per
  week" or "by year" is done, so use the date column itself. A text column gives a list of values
  to tick. A number column gives a range. Leave "filters" out when the question does not ask.
- A filter is NOT a chart. "Sales per category that I can filter per month" is one chart of sales
  per category PLUS one filter on the date column. Do not put the month on the chart's axis for it.
- When a "Possible links" line says two sources share a column, you may plan one check on each
  side of it, but still never join.
- If the sources cannot answer the question at all, answer
  {"title": null, "kpis": [], "checks": [], "explanation": "<why, in one sentence>"}.

Example 1. Sources: "orders" (order_date date, region text, category text, revenue number,
quantity number, discount number). Question: "How are sales doing and what drives them?"

{"title": "Sales performance and drivers",
 "kpis": [{"source": "orders", "label": "Total revenue", "field": "revenue", "agg": "sum"},
          {"source": "orders", "label": "Orders", "agg": "count"},
          {"source": "orders", "label": "Average order revenue", "field": "revenue", "agg": "mean"}],
 "checks": [
  {"source": "orders", "title": "Revenue by month",
   "chart": "Line chart of the sum of revenue per month of order_date.",
   "tests": "Whether revenue is growing, flat or falling over time."},
  {"source": "orders", "title": "Revenue by region",
   "chart": "Bar chart of the sum of revenue by region, sorted descending.",
   "tests": "Which regions bring in the most revenue and how uneven the split is."},
  {"source": "orders", "title": "Revenue by category",
   "chart": "Bar chart of the sum of revenue by category, sorted descending.",
   "tests": "Which product categories carry the revenue."},
  {"source": "orders", "title": "Discount against quantity",
   "chart": "Scatter plot of discount on x against quantity on y, one point per row.",
   "tests": "Whether larger discounts go together with larger orders.",
   "relation": ["discount", "quantity"]}]}

Example 2. Sources: "customers" (customer_id number, country text, segment text, signup_date
date) and "tickets" (ticket_id number, customer_id number, opened_at date with time, priority
text, hours_to_resolve number). Question: "Where does support struggle?"

{"title": "Where support struggles",
 "kpis": [{"source": "tickets", "label": "Tickets", "agg": "count"},
          {"source": "tickets", "label": "Median hours to resolve", "field": "hours_to_resolve",
           "agg": "median"}],
 "checks": [
  {"source": "tickets", "title": "Tickets per week",
   "chart": "Line chart of the count of rows per week of opened_at.",
   "tests": "Whether ticket volume is rising."},
  {"source": "tickets", "title": "Resolution time by priority",
   "chart": "Bar chart of the median of hours_to_resolve by priority.",
   "tests": "Whether high-priority tickets are really resolved faster."},
  {"source": "tickets", "title": "Tickets by priority",
   "chart": "Bar chart of the count of rows by priority, sorted descending.",
   "tests": "How the workload splits over the priorities."},
  {"source": "customers", "title": "Customers by segment",
   "chart": "Bar chart of the count of rows by segment, sorted descending.",
   "tests": "How large each customer segment is, as context for the ticket numbers."}]}

Example 3. Sources: "orders" as in example 1. Question: "What will the weather be tomorrow?"

{"title": null, "kpis": [], "checks": [],
 "explanation": "The orders table holds sales, not weather."}

Example 4. Sources: "orders" as in example 1. Question: "A dashboard with sales per category on
which I can filter per month and pick a region."

{"title": "Sales per category",
 "kpis": [{"source": "orders", "label": "Total revenue", "field": "revenue", "agg": "sum"}],
 "checks": [
  {"source": "orders", "title": "Revenue by category",
   "chart": "Bar chart of the sum of revenue by category, sorted descending.",
   "tests": "Which categories sell the most in the period the reader picks."},
  {"source": "orders", "title": "Quantity by category",
   "chart": "Bar chart of the sum of quantity by category, sorted descending.",
   "tests": "Whether the categories rank the same by units as by revenue."}],
 "filters": [{"source": "orders", "field": "order_date", "label": "Order date"},
             {"source": "orders", "field": "region", "label": "Region"}]}
"""


def system_prompt() -> str:
    return _SYSTEM_PROMPT


def user_message(*, sources: Sequence[PlanSource], prompt: str) -> str:
    blocks = [
        f"### Source {json.dumps(s.key, ensure_ascii=False)}\n\n{compact_profile(s.profile_text)}" for s in sources
    ]
    links = possible_links(sources) if len(sources) > 1 else []
    parts = ["## Sources\n\n" + "\n\n".join(blocks)]
    if len(sources) > 1:
        parts.append("## Possible links\n\n" + ("\n".join(links) if links else "None found by column name."))
    parts.append(f"## Question\n\n{prompt}")
    parts.append('Answer now with the JSON object {"title": ..., "kpis": [...], "checks": [...], "filters": [...]}.')
    return "\n\n".join(parts)


def retry_message(problem_text: str) -> str:
    return (
        f"{problem_text}\n\n"
        'Answer again with the whole JSON object {"title": ..., "kpis": [...], "checks": [...], "filters": [...]}, '
        "with these problems fixed. Change nothing else."
    )


# The model loop.


@dataclass
class _Answer:
    plan: DashboardPlan | None = None
    reason: str | None = None
    problem: str | None = None
    problems: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


def read_answer(content: str | None, sources: Sequence[PlanSource]) -> _Answer:
    """One model answer: a plan (with the problems of the items it lost), a decline, or a refusal."""
    payload, err = _parse_json_payload(content)
    if err == "empty_response":
        return _Answer(reason=err, problem='The answer was empty. Answer with one JSON object {"checks": ...}.')
    if err == "parse_error":
        return _Answer(
            reason=err, problem="The answer was not valid JSON. Answer with one JSON object and nothing else."
        )
    if isinstance(payload, dict) and not payload.get("checks") and payload.get("title") is None:
        explanation = _text(payload.get("explanation"), _MAX_EXPLANATION)
        return _Answer(reason="no_plan", problem=explanation or "The model did not plan a dashboard.")
    plan, problems, warnings = check_plan(payload, sources)
    if plan is None:
        reason = "validation_error" if not isinstance(payload, dict) else "invalid_plan"
        return _Answer(reason=reason, problem=problems_message(problems), problems=tuple(problems))
    return _Answer(plan=plan, problems=tuple(problems), warnings=tuple(warnings))


async def plan_dashboard(
    *,
    prompt: str,
    sources: Sequence[PlanSource],
    provider: Provider,
    user_id: int | None = None,
    warnings: Sequence[str] = (),
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> DashboardPlanResponse:
    """Turn a question over ``sources`` into the plan of a dashboard page.

    An answer with problems is sent back once. The last answer is taken with its valid items,
    the lost ones named in ``warnings``. ``timeout`` covers both calls. Never raises.
    """
    started = time.monotonic()
    notes = list(warnings)
    attempts = 0

    def degraded(reason: str, message: str | None, explanation: str | None = None) -> DashboardPlanResponse:
        _record(provider, started, reason, SURFACE)
        return DashboardPlanResponse(
            degraded=True, reason=reason, message=message, explanation=explanation, warnings=notes, attempts=attempts
        )

    text = (prompt or "").strip()
    if not text:
        return degraded("empty_prompt", "Ask a question for the dashboard to answer.")
    if not any(c.type != "other" for s in sources for c in s.columns):
        return degraded("no_columns", "The selected tables have no known columns to chart.")

    messages = [
        Message(role="system", content=system_prompt()),
        Message(role="user", content=user_message(sources=sources, prompt=text)),
    ]
    deadline = started + timeout
    answer = _Answer()
    for attempt in range(RETRY_ROUNDS + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if answer.plan is not None:
                break
            return degraded("timeout", "The model did not answer in time.")
        attempts += 1
        content, err, err_message = await _call(
            provider=provider,
            messages=messages,
            timeout=remaining,
            scheduler=scheduler,
            user_id=user_id,
            surface=SURFACE,
            max_tokens=_MAX_TOKENS,
        )
        if err is not None:
            if answer.plan is not None:
                break
            if answer.problem:
                err_message = f"{err_message} Its first answer had problems.\n{answer.problem}"
            return degraded(err, err_message)
        earlier, answer = answer, read_answer(content, sources)
        if answer.reason == "no_plan":
            return degraded("no_plan", answer.problem, explanation=answer.problem)
        if answer.plan is not None and not answer.problems:
            break
        if attempt == RETRY_ROUNDS:
            if answer.plan is None and earlier.plan is not None:
                answer = earlier
            break
        problem_text = answer.problem or problems_message(answer.problems)
        logger.info("dashboard plan refused (%s), retrying: %s", answer.reason or "items", problem_text)
        messages = [
            *messages,
            Message(role="assistant", content=content or ""),
            Message(role="user", content=retry_message(problem_text)),
        ]

    if answer.plan is None:
        return degraded(answer.reason or "invalid_plan", answer.problem)
    _record(provider, started, None, SURFACE)
    notes.extend(answer.warnings)
    notes.extend(f"Left out of the plan: {p}" for p in answer.problems)
    return DashboardPlanResponse(plan=answer.plan, warnings=notes, attempts=attempts)


__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_CHECKS",
    "MAX_FILTERS",
    "MAX_KPIS",
    "MAX_SOURCES",
    "MAX_TIMEOUT_SECONDS",
    "REASONS",
    "SURFACE",
    "DashboardPlan",
    "DashboardPlanResponse",
    "PlanSource",
    "PlannedCheck",
    "PlannedFilter",
    "PlannedKpi",
    "PlannedRelation",
    "check_plan",
    "compact_profile",
    "plan_dashboard",
    "possible_links",
    "read_answer",
    "source_keys",
    "system_prompt",
    "user_message",
]
