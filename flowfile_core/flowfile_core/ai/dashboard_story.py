"""The built charts and what they returned → the dashboard page's story (``POST /ai/dashboard_story``).

The last of three stages. By now the plan's charts exist as chart requests, and the browser
sends each with the query its tile will run. Core runs those queries on the worker and reads
the *evidence*: the rows of every aggregated chart (at most ``EVIDENCE_ROWS``, with the highest
and lowest group of each measure worked out here), the value of every KPI, and Pearson's r for
every relation between two number columns. The model then writes the page from that evidence:
a summary that answers the question, the order of the charts, one finding per chart, and which
charts to leave out and why.

Only aggregates leave the box: a chart that reads raw rows gets no row evidence. Every value is
masked and cut as in ``chart_profile``. With data values withheld nothing is measured and the
model is told to describe, not claim (``grounded`` False). An evidence query that fails or runs
past the budget marks its item "not measured"; the story is still written.

JSON mode, one deadline for both calls, one correction round. Never raises: a failure is a
result with ``degraded=True`` and a ``reason``. Must not import ``litellm``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from flowfile_core.ai.chart_profile import _shown, type_class
from flowfile_core.ai.chart_request import (
    _NUMBER_AGGS,
    _NUMERIC,
    ChartRequest,
    FieldRef,
    _call,
    _parse_json_payload,
    _record,
    _text,
)
from flowfile_core.ai.dashboard_plan import MAX_CHECKS, MAX_KPIS, SURFACE, PlannedKpi, PlannedRelation
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS: float = 60.0
MAX_TIMEOUT_SECONDS: float = 120.0
EVIDENCE_BUDGET_SECONDS: float = 12.0
EVIDENCE_ROWS: int = 40
EVIDENCE_FETCH_ROWS: int = 400
EVIDENCE_COLUMNS: int = 8
EVIDENCE_CONCURRENCY: int = 3
MAX_PAYLOAD_CHARS: int = 20_000
MIN_RELATION_ROWS: int = 10
RETRY_ROUNDS: int = 1
_MAX_TOKENS: int = 2000
_MAX_TITLE: int = 120
_MAX_SUMMARY: int = 1200
_MAX_FINDING: int = 280
_MAX_REASON: int = 200

REASONS: tuple[str, ...] = (
    "empty_prompt",
    "no_charts",
    "timeout",
    "provider_error",
    "empty_response",
    "parse_error",
    "validation_error",
    "invalid_story",
)
"""Every ``reason`` a degraded story can carry."""

EvidenceStatus = Literal["measured", "not_measured", "withheld"]


class StoryChart(BaseModel):
    """A built chart. ``payload`` is the Graphic Walker query its tile runs; None reads no evidence."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=16)
    table_id: int
    title: str = Field(min_length=1, max_length=_MAX_TITLE)
    purpose: str | None = Field(default=None, max_length=300)
    chart: ChartRequest
    payload: dict[str, Any] | None = None
    relation: PlannedRelation | None = None


class StoryFailure(BaseModel):
    """A planned chart that could not be built, so the story can say what is missing."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=16)
    title: str = Field(min_length=1, max_length=_MAX_TITLE)
    reason: str = Field(default="", max_length=300)


class StoryChartOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    title: str
    finding: str | None = None
    width: Literal["half", "full"] = "half"


class LeftOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    reason: str


class DashboardStory(BaseModel):
    """The page as written: charts in reading order, each with what it shows."""

    model_config = ConfigDict(extra="forbid")

    title: str
    summary_md: str
    charts: list[StoryChartOut] = Field(default_factory=list)
    left_out: list[LeftOut] = Field(default_factory=list)


class EvidenceNote(BaseModel):
    """What was measured for one KPI, chart or relation, for the result view. No data rows."""

    model_config = ConfigDict(extra="forbid")

    id: str
    kind: Literal["kpi", "chart", "relation"]
    status: EvidenceStatus
    detail: str | None = None


class DashboardStoryResponse(BaseModel):
    """``story`` is set on success; a degraded result carries ``reason`` and a ``message``.

    ``grounded`` is True when at least one item was measured, so the text rests on numbers.
    """

    model_config = ConfigDict(extra="forbid")

    story: DashboardStory | None = None
    grounded: bool = False
    evidence: list[EvidenceNote] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None
    message: str | None = None
    attempts: int = 0


@dataclass(frozen=True)
class Evidence:
    """One measured item: ``text`` is what the model reads, ``detail`` what the user is told."""

    id: str
    kind: Literal["kpi", "chart", "relation"]
    status: EvidenceStatus
    text: str = ""
    detail: str | None = None

    def note(self) -> EvidenceNote:
        return EvidenceNote(id=self.id, kind=self.kind, status=self.status, detail=self.detail)


# Evidence: what each query returned, as text.


def _number(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    return value


def _cell(value: Any) -> str:
    shown = _shown(_number(value))
    return "null" if shown is None else str(shown)


def aggregate_query(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """The aggregate step of a Graphic Walker query, or None for one that reads raw rows."""
    if not isinstance(payload, dict):
        return None
    for step in payload.get("workflow") or []:
        if not isinstance(step, dict) or step.get("type") != "view":
            continue
        for query in step.get("query") or []:
            if isinstance(query, dict) and query.get("op") == "aggregate":
                return query
    return None


def _is_sorted(payload: dict[str, Any]) -> bool:
    return any(isinstance(s, dict) and s.get("type") == "sort" for s in payload.get("workflow") or [])


def _order(rows: list[dict], keys: Sequence[str]) -> list[dict]:
    def by_value(row: dict) -> tuple:
        return tuple((row.get(k) is None, row.get(k)) for k in keys)

    def by_text(row: dict) -> tuple:
        return tuple((row.get(k) is None, str(row.get(k))) for k in keys)

    try:
        return sorted(rows, key=by_value)
    except TypeError:
        return sorted(rows, key=by_text)


def _spread(rows: list[dict], count: int) -> list[dict]:
    """``count`` rows evenly spaced over ``rows``, the first and the last included."""
    if len(rows) <= count:
        return rows
    step = (len(rows) - 1) / (count - 1)
    return [rows[round(i * step)] for i in range(count)]


def _extremes(rows: list[dict], groups: Sequence[str], measures: Sequence[dict]) -> list[str]:
    """Per number measure: the group with the highest and the lowest value, and the total of a sum or count."""
    lines = []
    for measure in measures:
        key = measure.get("asFieldKey")
        valued = [r for r in rows if isinstance(r.get(key), int | float) and not isinstance(r.get(key), bool)]
        if len(valued) < 2:
            continue

        def where(row: dict) -> str:
            return ", ".join(f"{g}={_cell(row.get(g))}" for g in groups) or "the only row"

        top, low = max(valued, key=lambda r: r[key]), min(valued, key=lambda r: r[key])
        line = f"{key}: highest {_cell(top[key])} at {where(top)}; lowest {_cell(low[key])} at {where(low)}"
        if measure.get("agg") in ("sum", "count"):
            line += f"; total over these rows {_cell(sum(r[key] for r in valued))}"
        lines.append(line)
    return lines


def render_rows(payload: dict[str, Any], rows: list[dict], total_rows: int) -> str:
    """An aggregated query's result as the model reads it: where each measure peaks, then the rows."""
    query = aggregate_query(payload) or {}
    groups = [g for g in query.get("groupBy") or [] if isinstance(g, str)]
    measures = [m for m in query.get("measures") or [] if isinstance(m, dict) and m.get("asFieldKey")]
    wanted = [*groups, *(m["asFieldKey"] for m in measures)]
    seen = list(rows[0]) if rows else []
    columns = ([c for c in wanted if c in seen] or seen)[:EVIDENCE_COLUMNS]
    if not _is_sorted(payload) and groups:
        rows = _order(rows, groups)
    shown = _spread(rows, EVIDENCE_ROWS)

    if len(shown) < len(rows):
        head = f"{len(shown)} evenly spaced rows of the {len(rows)} read"
    else:
        head = f"all {len(rows)} rows" if total_rows <= len(rows) else f"the first {len(rows)} rows"
    if total_rows > len(rows):
        head += f" (the result has {total_rows})"
    lines = [f"Result, {head}:"]
    lines.extend(_extremes(rows, groups, measures))
    lines.append(" | ".join(columns))
    lines.extend(" | ".join(_cell(row.get(c)) for c in columns) for row in shown)
    return "\n".join(lines)


def kpi_payload(kpi: PlannedKpi) -> dict[str, Any]:
    measure = {"field": kpi.field or "*", "agg": kpi.agg, "asFieldKey": "value"}
    return {"workflow": [{"type": "view", "query": [{"op": "aggregate", "groupBy": [], "measures": [measure]}]}]}


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def relation_sql(table: Any, relation: PlannedRelation) -> str:
    """Pearson's r of the two columns over the rows that hold both, as one catalog SQL query."""
    name = getattr(table, "qualified_name", None) or getattr(table, "full_table_name", None) or table.name
    x, y = _quoted(relation.x), _quoted(relation.y)
    return f"SELECT CORR({x}, {y}) AS r, COUNT(*) AS n FROM {_quoted(name)} WHERE {x} IS NOT NULL AND {y} IS NOT NULL"


def relation_words(r: float | None, n: int | None) -> str:
    """Pearson's r in plain words; the thresholds are the usual rule of thumb, stated to the model as such."""
    if n is None or n < MIN_RELATION_ROWS:
        return f"too few rows with both values ({n or 0}) to say whether they move together"
    if r is None or r != r:
        return f"no correlation could be computed over {n} rows (one of the columns may not vary)"
    size = abs(r)
    strength = "no" if size < 0.1 else "a weak" if size < 0.3 else "a moderate" if size < 0.6 else "a strong"
    direction = "" if size < 0.1 else (" positive" if r > 0 else " negative")
    return f"Pearson r = {round(r, 2)} over {n} rows: {strength}{direction} linear relation"


def scatter_relation(chart: ChartRequest, numeric: frozenset[str]) -> PlannedRelation | None:
    """The two plain number columns of a raw scatter plot, which is a relation to measure."""
    x, y = chart.x, chart.y
    if chart.mark != "point" or not isinstance(x, FieldRef) or not isinstance(y, FieldRef):
        return None
    plain = all(ref.aggregate is None and ref.bin is None and ref.timeUnit is None for ref in (x, y))
    if plain and x.field != y.field and {x.field, y.field} <= numeric:
        return PlannedRelation(x=x.field, y=y.field)
    return None


def _columns(table: Any) -> dict[str, str]:
    return {c.name: type_class(c.dtype) for c in (table.schema_columns or [])}


def _measure_kpi(service: Any, table: Any, kpi: PlannedKpi, user_id: int | None) -> Evidence:
    from flowfile_core.schemas.catalog_schema import VizSourceDescriptor

    columns = _columns(table)
    if kpi.field is not None and kpi.field not in columns:
        return Evidence(kpi.id, "kpi", "not_measured", detail=f'"{kpi.field}" is not a column of "{table.name}"')
    if kpi.agg in _NUMBER_AGGS and columns.get(kpi.field or "") not in _NUMERIC:
        return Evidence(kpi.id, "kpi", "not_measured", detail=f'"{kpi.agg}" needs a number column')
    source = VizSourceDescriptor(source_type="table", table_id=table.id)
    result = service.compute_ad_hoc_visualization(source, kpi_payload(kpi), 1, user_id)
    if result.error or not result.rows:
        return Evidence(kpi.id, "kpi", "not_measured", detail="the query failed")
    return Evidence(kpi.id, "kpi", "measured", text=_cell(result.rows[0].get("value")))


def _measure_chart(service: Any, table: Any, chart: StoryChart, user_id: int | None) -> Evidence:
    from flowfile_core.schemas.catalog_schema import VizSourceDescriptor

    if chart.payload is None or aggregate_query(chart.payload) is None:
        return Evidence(chart.id, "chart", "not_measured", detail="the chart reads raw rows, which are not sent")
    if len(json.dumps(chart.payload, default=str)) > MAX_PAYLOAD_CHARS:
        return Evidence(chart.id, "chart", "not_measured", detail="the chart's query is too large to read")
    source = VizSourceDescriptor(source_type="table", table_id=table.id)
    result = service.compute_ad_hoc_visualization(source, chart.payload, EVIDENCE_FETCH_ROWS, user_id)
    if result.error:
        return Evidence(chart.id, "chart", "not_measured", detail="the chart's query failed")
    if not result.rows:
        return Evidence(chart.id, "chart", "measured", text="Result: no rows.", detail="no rows")
    text = render_rows(chart.payload, result.rows, max(result.total_rows, len(result.rows)))
    return Evidence(chart.id, "chart", "measured", text=text, detail=f"{len(result.rows)} rows read")


def _measure_relation(service: Any, table: Any, chart: StoryChart, user_id: int | None) -> Evidence | None:
    columns = _columns(table)
    numeric = frozenset(name for name, type_ in columns.items() if type_ in _NUMERIC)
    relation = chart.relation or scatter_relation(chart.chart, numeric)
    if relation is None:
        return None
    if relation.x == relation.y or not {relation.x, relation.y} <= numeric:
        return Evidence(chart.id, "relation", "not_measured", detail="the relation does not name two number columns")
    result = service.execute_sql_query(relation_sql(table, relation), 1, user_id)
    if result.error or not result.rows or len(result.rows[0]) < 2:
        return Evidence(chart.id, "relation", "not_measured", detail="the correlation query failed")
    r, n = result.rows[0][0], result.rows[0][1]
    r = float(r) if isinstance(r, int | float) and not isinstance(r, bool) else None
    n = n if isinstance(n, int) and not isinstance(n, bool) else None
    words = relation_words(r, n)
    return Evidence(
        chart.id, "relation", "measured", text=f'Relation of "{relation.x}" and "{relation.y}": {words}.', detail=words
    )


def _with_service(user: Any, work: Callable[[Any], Evidence | None]) -> Evidence | None:
    """Run ``work`` with a catalog service of its own session, scoped to ``user``."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.catalog.access import AccessResolver
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return work(CatalogService(SQLAlchemyCatalogRepository(db), access=AccessResolver(db, user)))


async def gather_evidence(
    *,
    tables: dict[int, Any],
    user: Any,
    kpis: Sequence[PlannedKpi],
    charts: Sequence[StoryChart],
    budget: float = EVIDENCE_BUDGET_SECONDS,
) -> list[Evidence]:
    """Measure every KPI, chart and relation on the worker, a few at a time, within ``budget`` seconds.

    Never raises: an item that fails or is not done in time comes back "not measured".
    """
    user_id = getattr(user, "id", None)
    jobs: list[tuple[str, Literal["kpi", "chart", "relation"], Callable[[Any], Evidence | None]]] = []
    for kpi in kpis:
        jobs.append((kpi.id, "kpi", lambda s, k=kpi: _measure_kpi(s, tables[k.table_id], k, user_id)))
    for chart in charts:
        jobs.append((chart.id, "chart", lambda s, c=chart: _measure_chart(s, tables[c.table_id], c, user_id)))
        jobs.append((chart.id, "relation", lambda s, c=chart: _measure_relation(s, tables[c.table_id], c, user_id)))

    gate = asyncio.Semaphore(EVIDENCE_CONCURRENCY)

    async def run(work: Callable[[Any], Evidence | None]) -> Evidence | None:
        async with gate:
            return await asyncio.to_thread(_with_service, user, work)

    tasks = [asyncio.ensure_future(run(work)) for _, _, work in jobs]
    if tasks:
        await asyncio.wait(tasks, timeout=budget)
    found: list[Evidence] = []
    for (item_id, kind, _), task in zip(jobs, tasks, strict=True):
        if not task.done():
            task.cancel()
            if kind != "relation":
                found.append(Evidence(item_id, kind, "not_measured", detail=f"not read within {budget:g} s"))
            continue
        if task.exception() is not None:
            logger.warning("dashboard evidence for %s %s failed: %s", kind, item_id, task.exception())
            found.append(Evidence(item_id, kind, "not_measured", detail="reading it failed"))
        elif task.result() is not None:
            found.append(task.result())
    return found


def withheld_evidence(kpis: Sequence[PlannedKpi], charts: Sequence[StoryChart]) -> list[Evidence]:
    return [
        *(Evidence(k.id, "kpi", "withheld") for k in kpis),
        *(Evidence(c.id, "chart", "withheld") for c in charts),
    ]


# Validation: one model answer against the built charts.


def check_story(
    raw: Any, charts: Sequence[StoryChart], fallback_title: str
) -> tuple[DashboardStory | None, list[str], list[str]]:
    """``(story, problems, warnings)``. ``story`` is None when the answer has no usable summary.

    Every built chart ends up placed or left out: one the answer forgot is placed last.
    """
    warnings: list[str] = []
    if not isinstance(raw, dict):
        return None, ['The answer must be a JSON object {"title", "summary", "charts", "left_out"}.'], warnings
    problems: list[str] = []
    titles = {c.id: c.title for c in charts}
    known = ", ".join(titles)

    summary = _text(raw.get("summary", raw.get("summary_md")), _MAX_SUMMARY)
    if summary is None:
        problems.append('"summary" is missing: answer the question in two to four sentences.')

    placed: list[StoryChartOut] = []
    for i, item in enumerate(raw.get("charts") if isinstance(raw.get("charts"), list) else []):
        item_id = item.get("id") if isinstance(item, dict) else item
        if item_id not in titles:
            problems.append(f"charts[{i}].id {json.dumps(item_id, default=str)} is not a chart. Charts: {known}")
            continue
        if any(p.id == item_id for p in placed):
            continue
        item = item if isinstance(item, dict) else {}
        placed.append(
            StoryChartOut(
                id=item_id,
                title=_text(item.get("title"), _MAX_TITLE) or titles[item_id],
                finding=_text(item.get("finding"), _MAX_FINDING),
                width="full" if str(item.get("width", "")).strip().lower() in ("full", "wide") else "half",
            )
        )

    left_out: list[LeftOut] = []
    for i, item in enumerate(raw.get("left_out") if isinstance(raw.get("left_out"), list) else []):
        item_id = item.get("id") if isinstance(item, dict) else item
        if item_id not in titles:
            problems.append(f"left_out[{i}].id {json.dumps(item_id, default=str)} is not a chart. Charts: {known}")
            continue
        if any(p.id == item_id for p in placed) or any(o.id == item_id for o in left_out):
            continue
        reason = _text(item.get("reason"), _MAX_REASON) if isinstance(item, dict) else None
        left_out.append(LeftOut(id=item_id, reason=reason or "Left out by the AI."))

    handled = {p.id for p in placed} | {o.id for o in left_out}
    for chart in charts:
        if chart.id not in handled:
            placed.append(StoryChartOut(id=chart.id, title=chart.title))
            warnings.append(f'The story did not mention "{chart.title}"; it was placed last without a finding.')
    if summary is None:
        return None, problems, warnings
    story = DashboardStory(
        title=_text(raw.get("title"), _MAX_TITLE) or fallback_title,
        summary_md=summary,
        charts=placed,
        left_out=left_out,
    )
    return story, problems, warnings


def problems_message(problems: Sequence[str]) -> str:
    count = len(problems)
    lines = "\n".join(f"- {p}" for p in problems)
    return f"The story has {count} problem{'s' if count != 1 else ''}:\n{lines}"


# The prompt.

_SYSTEM_PROMPT = """\
You write one dashboard page that answers a question about the user's data.

The charts are already built. For each chart you get its id, its title, what it was built to
test, the chart itself, and the EVIDENCE: the numbers the chart's query returned, read from the
real data. You also get the KPI values, and for a relation between two number columns Pearson's
r in plain words. You write the page from that evidence: a summary that answers the question,
the order of the charts, one finding per chart, and which charts to leave out.

Answer with one JSON object and nothing else:

{"title": "<page title>",
 "summary": "<two to four sentences that answer the question>",
 "charts": [{"id": "<chart id>", "title": "<chart title>", "finding": "<one sentence>",
             "width": "half" or "full"}],
 "left_out": [{"id": "<chart id>", "reason": "<one sentence>"}]}

Rules that matter most:

- Say only what the evidence shows. Every number you write must be in the evidence. Never
  invent a number, a cause or a trend. When you are not sure, say less.
- "summary" answers the question first, in plain words, then gives the one or two facts that
  matter most. You may put the key number in **bold**. No headings, no lists.
- "finding" is one sentence saying what THIS chart shows, with its key number: the highest and
  lowest group, the direction of a trend, the size of a gap.
- A tested relation that the evidence does not support is a finding too. Say it plainly:
  "Discount and quantity show no linear relation (r = 0.03)." Do not hide it and do not dress
  it up. Leave such a chart out only when it adds nothing to the answer, and give the reason.
- Correlation is not cause. Write "go together" or "move together", never "causes" or "drives".
- Order "charts" as a story: the overall picture first, then the breakdowns that explain it,
  then how it moves over time, then the relations, then the detail.
- "width": "full" for a chart over time or one with many groups, "half" for the others.
- Put every chart id either in "charts" or in "left_out", never in both. Use the ids as given.
- Leave a chart out when its evidence is empty, when it repeats another chart, or when it does
  not bear on the question. Keep a chart whose evidence was "not measured": describe what it
  shows without numbers.
- When the evidence says data values are not shared, nothing was measured. Then describe what
  each chart shows ("Revenue per month, as a line") and make NO claim about the data: no
  numbers, no "highest", no "grows". The summary says what the page lets the reader see.
- Keep each chart's title unless the evidence makes a sharper one obvious.

Example 1. Question: "How are sales doing and what drives them?"
KPIs: k1 "Total revenue" = 48210.5; k2 "Orders" = 1200.
Chart c1 "Revenue by month". Evidence: revenue_sum: highest 5400.0 at order_date=2025-11;
lowest 2950.0 at order_date=2025-02; total over these rows 48210.5; rows from 2025-01 (3100.0)
to 2025-12 (5150.0).
Chart c2 "Revenue by region". Evidence: revenue_sum: highest 19800.0 at region=West; lowest
6100.0 at region=North; total over these rows 48210.5.
Chart c3 "Discount against quantity". Evidence: Relation of "discount" and "quantity": Pearson
r = 0.04 over 1200 rows: no linear relation.
Chart c4 "Revenue by category". Evidence: Result: no rows.

<<EXAMPLE_1>>

Example 2. The same charts, but the evidence says: "Data values are not shared. Nothing was
measured."

<<EXAMPLE_2>>
"""


_EXAMPLE_1 = {
    "title": "Sales performance and drivers",
    "summary": (
        "Sales came to **48,210.5** over 1,200 orders and rose through the year, from 3,100 in January to a "
        "peak of 5,400 in November. The West region brings in the most, 19,800, more than three times the North. "
        "Larger discounts do not go together with larger orders."
    ),
    "charts": [
        {
            "id": "c1",
            "title": "Revenue by month",
            "finding": "Revenue rose from 3,100 in January to 5,150 in December, peaking at 5,400 in November.",
            "width": "full",
        },
        {
            "id": "c2",
            "title": "Revenue by region",
            "finding": "West leads with 19,800; North is lowest with 6,100.",
            "width": "half",
        },
        {
            "id": "c3",
            "title": "Discount against quantity",
            "finding": "Discount and quantity show no linear relation (r = 0.04).",
            "width": "half",
        },
    ],
    "left_out": [{"id": "c4", "reason": "The chart returned no rows."}],
}

_EXAMPLE_2 = {
    "title": "Sales performance and drivers",
    "summary": (
        "This page shows how revenue moves over the year, how it splits over the regions and categories, and "
        "whether discounts go together with larger orders. It was written without seeing the data, so it "
        "describes the charts and draws no conclusions."
    ),
    "charts": [
        {
            "id": "c1",
            "title": "Revenue by month",
            "finding": "Total revenue per month, to read the trend over the year.",
            "width": "full",
        },
        {
            "id": "c2",
            "title": "Revenue by region",
            "finding": "Total revenue per region, to compare the regions.",
            "width": "half",
        },
        {
            "id": "c4",
            "title": "Revenue by category",
            "finding": "Total revenue per category, to compare the categories.",
            "width": "half",
        },
        {
            "id": "c3",
            "title": "Discount against quantity",
            "finding": "Each order's discount against its quantity, to see whether they move together.",
            "width": "half",
        },
    ],
    "left_out": [],
}


def system_prompt() -> str:
    """The rules with both worked answers set in as JSON, so each example is an answer the checks accept."""
    text = _SYSTEM_PROMPT
    for marker, example in (("<<EXAMPLE_1>>", _EXAMPLE_1), ("<<EXAMPLE_2>>", _EXAMPLE_2)):
        text = text.replace(marker, json.dumps(example, ensure_ascii=False, indent=1))
    return text


def _chart_block(chart: StoryChart, source: str, evidence: Sequence[Evidence], grounded: bool) -> str:
    lines = [f"### {chart.id} {json.dumps(chart.title, ensure_ascii=False)} (source {json.dumps(source)})"]
    if chart.purpose:
        lines.append(f"Tests: {chart.purpose}")
    lines.append("Chart: " + json.dumps(chart.chart.model_dump(), ensure_ascii=False))
    if not grounded:
        return "\n".join(lines)
    mine = [e for e in evidence if e.id == chart.id and e.kind in ("chart", "relation")]
    measured = [e.text for e in mine if e.status == "measured" and e.text]
    if measured:
        lines.append("Evidence:\n" + "\n".join(measured))
    else:
        why = next((e.detail for e in mine if e.detail), "nothing was read")
        lines.append(f"Evidence: not measured ({why}).")
    return "\n".join(lines)


def user_message(
    *,
    prompt: str,
    title: str | None,
    kpis: Sequence[PlannedKpi],
    charts: Sequence[StoryChart],
    failed: Sequence[StoryFailure],
    sources: dict[int, str],
    evidence: Sequence[Evidence],
    grounded: bool,
) -> str:
    parts = [f"## Question\n\n{prompt}"]
    if title:
        parts.append(f"## Planned page title\n\n{title}")
    if not grounded:
        withheld = not evidence or any(e.status == "withheld" for e in evidence)
        why = "Data values are not shared." if withheld else "The data could not be read."
        parts.append(
            f"## Evidence\n\n{why} Nothing was measured. Describe what each chart shows "
            "and make no claim about the data."
        )
    if kpis:
        values = {e.id: e for e in evidence if e.kind == "kpi"}
        lines = []
        for kpi in kpis:
            what = f"{kpi.agg} of {kpi.field}" if kpi.field else "row count"
            label, source = json.dumps(kpi.label, ensure_ascii=False), json.dumps(sources[kpi.table_id])
            line = f"- {kpi.id} {label} ({what} on {source})"
            found = values.get(kpi.id)
            if grounded:
                line += f" = {found.text}" if found and found.status == "measured" else ": not measured"
            lines.append(line)
        parts.append("## KPIs\n\n" + "\n".join(lines))
    blocks = [_chart_block(c, sources[c.table_id], evidence, grounded) for c in charts]
    parts.append("## Charts\n\n" + "\n\n".join(blocks))
    if failed:
        lines = [f"- {json.dumps(f.title, ensure_ascii=False)}: {f.reason or 'could not be built'}" for f in failed]
        parts.append("## Planned charts that could not be built (not on the page)\n\n" + "\n".join(lines))
    parts.append('Answer now with the JSON object {"title": ..., "summary": ..., "charts": [...], "left_out": [...]}.')
    return "\n\n".join(parts)


def retry_message(problem_text: str) -> str:
    return (
        f"{problem_text}\n\n"
        'Answer again with the whole JSON object {"title": ..., "summary": ..., "charts": [...], '
        '"left_out": [...]}, with these problems fixed. Change nothing else.'
    )


# The model loop.


async def write_story(
    *,
    prompt: str,
    title: str | None,
    kpis: Sequence[PlannedKpi],
    charts: Sequence[StoryChart],
    failed: Sequence[StoryFailure] = (),
    sources: dict[int, str],
    evidence: Sequence[Evidence],
    provider: Provider,
    user_id: int | None = None,
    warnings: Sequence[str] = (),
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> DashboardStoryResponse:
    """Write the page from the built charts and their evidence.

    An answer with problems is sent back once; the last usable answer is taken, its unknown
    ids dropped. ``timeout`` covers both calls. Never raises.
    """
    started = time.monotonic()
    notes = list(warnings)
    attempts = 0
    grounded = any(e.status == "measured" for e in evidence)
    evidence_notes = [e.note() for e in evidence]

    def degraded(reason: str, message: str | None) -> DashboardStoryResponse:
        _record(provider, started, reason, SURFACE)
        return DashboardStoryResponse(
            degraded=True,
            reason=reason,
            message=message,
            grounded=grounded,
            evidence=evidence_notes,
            warnings=notes,
            attempts=attempts,
        )

    text = (prompt or "").strip()
    if not text:
        return degraded("empty_prompt", "Ask a question for the dashboard to answer.")
    if not charts:
        return degraded("no_charts", "No chart could be built, so there is no page to write.")

    fallback_title = (title or "").strip() or "Dashboard"
    messages = [
        Message(role="system", content=system_prompt()),
        Message(
            role="user",
            content=user_message(
                prompt=text,
                title=title,
                kpis=kpis,
                charts=charts,
                failed=failed,
                sources=sources,
                evidence=evidence,
                grounded=grounded,
            ),
        ),
    ]
    deadline = started + timeout
    best: tuple[DashboardStory, list[str]] | None = None
    last_reason, last_problem = "invalid_story", None
    for attempt in range(RETRY_ROUNDS + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if best is None:
                return degraded("timeout", "The model did not answer in time.")
            break
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
            if best is None:
                return degraded(err, err_message)
            break
        payload, parse_err = _parse_json_payload(content)
        if parse_err is not None:
            last_reason = parse_err
            last_problem = (
                'The answer was empty. Answer with one JSON object {"summary": ...}.'
                if parse_err == "empty_response"
                else "The answer was not valid JSON. Answer with one JSON object and nothing else."
            )
            problems: list[str] = []
        else:
            story, problems, story_warnings = check_story(payload, charts, fallback_title)
            last_reason = "validation_error" if not isinstance(payload, dict) else "invalid_story"
            last_problem = problems_message(problems) if problems else None
            if story is not None:
                best = (story, story_warnings)
                if not problems:
                    break
        if attempt == RETRY_ROUNDS:
            break
        logger.info("dashboard story refused (%s), retrying: %s", last_reason, last_problem)
        messages = [
            *messages,
            Message(role="assistant", content=content or ""),
            Message(role="user", content=retry_message(last_problem or "")),
        ]

    if best is None:
        return degraded(last_reason, last_problem)
    _record(provider, started, None, SURFACE)
    story, story_warnings = best
    notes.extend(story_warnings)
    return DashboardStoryResponse(
        story=story, grounded=grounded, evidence=evidence_notes, warnings=notes, attempts=attempts
    )


__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "EVIDENCE_BUDGET_SECONDS",
    "EVIDENCE_ROWS",
    "MAX_CHECKS",
    "MAX_KPIS",
    "MAX_TIMEOUT_SECONDS",
    "REASONS",
    "DashboardStory",
    "DashboardStoryResponse",
    "Evidence",
    "EvidenceNote",
    "LeftOut",
    "StoryChart",
    "StoryChartOut",
    "StoryFailure",
    "aggregate_query",
    "check_story",
    "gather_evidence",
    "kpi_payload",
    "relation_sql",
    "relation_words",
    "render_rows",
    "scatter_relation",
    "system_prompt",
    "user_message",
    "withheld_evidence",
    "write_story",
]
