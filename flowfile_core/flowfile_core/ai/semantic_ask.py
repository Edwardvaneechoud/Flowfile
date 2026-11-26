"""A question → one query of an accepted metric, backing ``POST /ai/semantic/ask``.

The model sees a short menu of the **accepted** model only (``semantic_routes._menu``), the model's own instructions
and today's date, and answers a ``MetricQuery``-shaped object or ``{"no_metric": true, "why": …}``. The model reads the
language; the model's facts are the only knowledge. Free SQL never comes from the model.

Code checks only what can be checked against the menu and the data: the metric, its dimensions, each filter's column,
operator and (where the column's values are known) value, the grain, the limit. What is dropped is noted, kept as a
``Gap`` for the answer sentence, and makes the answer ``partial``. The question's wording is never judged here. One
repair round sends the refusals back, except dimensions: one sent back would be swapped for another silently. Known
values and the rows a query returns never reach the model. Must not import ``litellm``.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any, get_args

from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.semantic_answer import Gap, Intent, decide_intent, shape_query
from flowfile_core.ai.semantic_draft import call_json, match_name, repair_turn, system_prompt
from flowfile_core.schemas.semantic_schema import FilterOp, Grain, MetricFilter, MetricOrder, MetricQuery

logger = logging.getLogger(__name__)

MAX_MENU = 30
MAX_DIMENSIONS_SHOWN = 12
MAX_FILTER_FIELDS_SHOWN = 16
MAX_TOKENS = 500
MAX_INSTRUCTIONS_CHARS = 1200
OPS = get_args(FilterOp)
GRAINS = get_args(Grain)


@dataclass
class MenuItem:
    """One accepted metric as the model sees it: its meaning, synonyms, time column and the columns it allows."""

    metric: Any
    meaning: str | None
    synonyms: list[str]
    dimensions: list[str]
    columns: list[str]
    dimension_words: dict[str, list[str]]
    known_values: dict[str, list[str]]

    @property
    def name(self) -> str:
        return self.metric.name

    @property
    def filter_fields(self) -> list[str]:
        time_column = [self.metric.time_column] if self.metric.time_column else []
        return list(dict.fromkeys([*self.dimensions, *self.columns, *time_column]))


def menu_text(menu: list[MenuItem]) -> str:
    """The metrics, then the columns to group by and to filter on, each listed once per set of metrics."""
    lines = [f"Today: {date.today().isoformat()}", "Metrics:"]
    for m in menu:
        bits = [m.meaning or "No description."]
        if m.synonyms:
            bits.append("Also called: " + ", ".join(m.synonyms[:4]) + ".")
        if m.metric.time_column:
            bits.append(f"Time column: {m.metric.time_column}.")
        lines.append(f"- {m.name}: " + " ".join(bits))

    def written(m: MenuItem, d: str) -> str:
        also = [w for w in m.dimension_words.get(d, []) if w and w != d][:3]
        return f"{d} (also: {', '.join(also)})" if also else d

    def section(title: str, columns: Callable[[MenuItem], list[str]]) -> None:
        groups: dict[tuple[str, ...], list[str]] = {}
        for m in menu:
            groups.setdefault(tuple(columns(m)), []).append(m.name)
        groups.pop((), None)
        lines.extend(["", title])
        lines.extend([f"- for {', '.join(names)}: {', '.join(cols)}" for cols, names in groups.items()] or ["- none"])

    section(
        "Columns to group by (use one ONLY when the question asks to break the answer down by it):",
        lambda m: [written(m, d) for d in m.dimensions[:MAX_DIMENSIONS_SHOWN]],
    )
    section("Columns to filter on:", lambda m: m.filter_fields[:MAX_FILTER_FIELDS_SHOWN])
    return "\n".join(lines)


def user_message(menu: list[MenuItem], question: str, instructions: str | None) -> str:
    owner = (instructions or "").strip()[:MAX_INSTRUCTIONS_CHARS]
    told = f"Instructions from the model's owner:\n{owner}\n\n" if owner else ""
    return (
        f"{menu_text(menu)}\n\n{told}Question: {question}\n\n"
        'Answer now with the JSON object {"metric": ..., "dimensions": [...], "filters": [...], "grain": ..., '
        '"order": ..., "limit": ..., "intent": ...} or {"no_metric": true, "why": ...}.'
    )


@dataclass
class AskAnswer:
    query: MetricQuery | None = None
    no_metric: bool = False
    why: str | None = None
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    reason: str | None = None
    partial: bool = False
    intent: Intent | None = None
    dropped: dict[str, int] = field(default_factory=dict)
    gaps: list[Gap] = field(default_factory=list)

    def drop(self, gap: Gap, counter: str, problem: str | None = None) -> None:
        """Something the model asked for that code did not apply: noted, a gap, partial, counted for the audit."""
        self.notes.append(gap.note)
        self.gaps.append(gap)
        self.partial = True
        self.dropped[counter] = self.dropped.get(counter, 0) + 1
        if problem:
            self.problems.append(problem)


def _as_list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    return value if isinstance(value, list) else [value]


def _field(name: Any, choices: list[str], notes: list[str]) -> str | None:
    """A menu column as written, else matched (``match_name``); ``orders.status`` also matches a bare ``status``
    and a bare ``country`` the one ``customers.country`` (a table prefix added or dropped)."""
    found = match_name(name, choices, notes)
    if found is None and isinstance(name, str) and "." in name:
        found = match_name(name.rsplit(".", 1)[-1], choices, notes)
    if found is None and isinstance(name, str):
        tails = [c for c in choices if c.rsplit(".", 1)[-1].lower() == name.lower()]
        found = tails[0] if len(tails) == 1 else None
    return found


def _known(item: MenuItem, fld: str, value: Any) -> tuple[Any, bool]:
    """``(value, ok)``: corrected to the stored spelling when it differs only in case; ``ok`` false when the column
    holds no such value. Columns without known values pass as given."""
    values = item.known_values.get(fld)
    if values is None or value is None:
        return value, True
    hit = {v.lower(): v for v in values}.get(str(value).lower())
    return (value, False) if hit is None else (hit, True)


def read_answer(content: str | None, menu: list[MenuItem]) -> AskAnswer:
    """One answer checked against the menu and the known values: matched names kept, the rest dropped and noted."""
    ans = AskAnswer()
    payload, err = parse_json_payload(content, extract_object=True)
    if err:
        ans.reason = err
        ans.problems.append("The answer was not one valid JSON object. Answer with the JSON object only.")
        return ans
    if isinstance(payload, dict) and isinstance(payload.get("query"), dict):
        payload = payload["query"]
    if not isinstance(payload, dict):
        ans.reason = "validation_error"
        ans.problems.append('The answer must be a JSON object {"metric": ..., ...}.')
        return ans
    if payload.get("no_metric") is True or (payload.get("metric") in (None, "") and payload.get("why")):
        ans.no_metric, ans.why = True, str(payload.get("why") or "No approved metric answers this question.")[:300]
        return ans
    names = [m.name for m in menu]
    metric_name = match_name(payload.get("metric"), names, ans.notes)
    if metric_name is None:
        ans.reason = "unknown_metric"
        ans.problems.append(
            f"{payload.get('metric')!r} is not on the menu. Pick one of: {', '.join(names)}, or answer no_metric."
        )
        return ans
    item = next(m for m in menu if m.name == metric_name)
    time_column = item.metric.time_column

    def no_split(what: Any, reason: str, counter: str, problem: str | None = None) -> None:
        ans.drop(Gap("split", str(what), f"Not split by {what}: {reason}.", reason), counter, problem)

    dims: list[str] = []
    for d in _as_list(payload.get("dimensions")):
        found = _field(d, item.dimensions, ans.notes)
        if found is None:
            no_split(d, f"it is not among the columns {metric_name} can be grouped by", "dimensions_dropped")
        elif found not in dims:
            dims.append(found)

    fields = item.filter_fields
    filters: list[MetricFilter] = []

    def refuse(fld: str, reason: str, problem: str) -> None:
        gap = Gap("filter", fld, f"Not filtered on {fld}: {reason}.", reason, fld)
        ans.drop(gap, "filters_dropped", problem)

    raw_filters = payload.get("filters")
    for f in _as_list([raw_filters] if isinstance(raw_filters, dict) else raw_filters):
        if not isinstance(f, dict):
            ans.problems.append(f"Filter {f!r} is not an object.")
            continue
        raw_field = f.get("field") or f.get("column")
        fld = _field(raw_field, fields, ans.notes)
        op = str(f.get("op") or f.get("operator") or "=").strip().lower()
        value = f.get("value")
        if fld is None:
            one_of = ", ".join(fields)
            refuse(
                str(raw_field),
                f"{metric_name} has no such column",
                f"{raw_field!r} is not a column of {metric_name}. Use one of: {one_of}.",
            )
            continue
        if op not in OPS:
            refuse(
                fld,
                f"{op!r} is not an operator",
                f"The operator of the filter on {fld} must be one of: {', '.join(OPS)}.",
            )
            continue
        if op in ("is null", "is not null"):
            value = None
        elif op in ("in", "not in"):
            value = _as_list(value)
        elif op == "between":
            value = _as_list(value)
            if len(value) != 2:
                refuse(fld, "between needs two values", f"The filter on {fld} uses between, which takes [low, high].")
                continue
        elif isinstance(value, list):
            if len(value) != 1:
                refuse(
                    fld, f"{op} takes one value", f"The filter on {fld} with {op} takes one value; use in for a list."
                )
                continue
            value = value[0]
        if op in ("=", "!=", "in", "not in"):
            kept, bad = [], []
            for v in value if isinstance(value, list) else [value]:
                fixed, ok = _known(item, fld, v)
                (kept if ok else bad).append(fixed)
            for v in bad:
                note = f"Not filtered to {v!r}: it is not a value of {fld}."
                ans.drop(Gap("filter", str(v), note, column=fld), "filters_dropped")
            if not kept:
                continue
            value = kept if isinstance(value, list) else kept[0]
        filters.append(MetricFilter(field=fld, op=op, value=value))

    grain = payload.get("grain")
    grain = str(grain).strip().lower() if grain not in (None, "", False) else None
    if grain is not None and grain not in GRAINS:
        one_of = ", ".join(GRAINS)
        no_split(grain, f"a grain is one of {one_of}", "grain_dropped", f"The grain must be one of: {one_of}, or null.")
        grain = None
    elif grain is not None and not time_column:
        no_split(grain, f"{metric_name} has no time column", "grain_dropped")
        grain = None

    order = None
    raw_order = payload.get("order")
    if isinstance(raw_order, str):
        raw_order = {"field": raw_order}
    if isinstance(raw_order, dict):
        outputs = [*dims, metric_name] + ([f"{time_column.rpartition('.')[2]}_{grain}"] if grain else [])
        target = match_name(raw_order.get("field"), outputs, ans.notes)
        direction = str(raw_order.get("direction") or "asc").lower()
        if target is None or direction not in ("asc", "desc"):
            ans.notes.append(f"Not ordered by {raw_order.get('field')!r}: it is not a column of the answer.")
            ans.partial = True
        else:
            order = MetricOrder(field=target, direction=direction)
    try:
        limit = int(payload.get("limit") or 100)
    except (TypeError, ValueError):
        limit = 100
    limit = min(max(limit, 1), 10000)
    ans.query = MetricQuery(metric=metric_name, dimensions=dims, filters=filters, grain=grain, order=order, limit=limit)
    hint = payload.get("intent")
    ans.intent = decide_intent(ans.query, hint.strip().lower() if isinstance(hint, str) else None)
    ans.query = shape_query(ans.query, ans.intent, time_column)
    ans.notes.insert(0, reading(ans.query))
    return ans


def reading(query: MetricQuery) -> str:
    """How the question was read, in one line ("total_amount, grouped by customers.country, by month")."""
    bits = [query.metric]
    if query.dimensions:
        bits.append("grouped by " + ", ".join(query.dimensions))
    if query.grain:
        bits.append(f"by {query.grain}")
    for f in query.filters:
        bits.append(f"where {f.field} {f.op} {f.value!r}" if f.value is not None else f"where {f.field} {f.op}")
    return "Read as: " + ", ".join(bits) + "."


async def ask(
    provider: Provider,
    menu: list[MenuItem],
    question: str,
    *,
    user_id: int | None,
    run: Callable[[MetricQuery], Awaitable[Any]],
    instructions: str | None = None,
) -> tuple[AskAnswer, int, Any]:
    """``(answer, attempts, run_result)``. ``run`` is an async compile-and-run; its ``ValueError`` is fed back once."""
    messages = [
        Message(role="system", content=system_prompt("semantic_ask")),
        Message(role="user", content=user_message(menu, question, instructions)),
    ]
    ans = AskAnswer()
    for attempt in (1, 2):
        content, err, msg = await call_json(
            provider=provider, messages=messages, user_id=user_id, max_tokens=MAX_TOKENS
        )
        if err is not None:
            ans.reason, ans.problems = err, [msg or err]
            return ans, attempt, None
        ans = read_answer(content, menu)
        result = None
        if ans.query is not None and (not ans.problems or attempt == 2):
            # on the last attempt, run what survived the checks; the dropped parts are in the notes
            ans.problems = []
            try:
                result = await run(ans.query)
            except ValueError as exc:
                ans.reason = "invalid_query"
                ans.problems.append(f"That query does not compile: {exc}")
        if ans.no_metric or (ans.query is not None and not ans.problems):
            return ans, attempt, result
        logger.info("semantic ask answer refused: %s", ans.problems)
        messages = repair_turn(
            messages, content, "\n".join(ans.problems) + "\n\nAnswer again with the whole JSON object."
        )
    return ans, attempt, None
