"""A question → one query of an accepted metric, backing ``POST /ai/semantic/ask``.

The model sees a short menu of the **accepted** model only (at most ``MAX_MENU`` metrics, narrowed with
glosslayer's keyword search when there are more): each metric's name, description, synonyms and time column, the
columns it may be grouped by and filtered on, the model's own instructions when it has them, and today's date. It
answers a ``MetricQuery``-shaped object or ``{"no_metric": true, "why": …}``. The model reads the language; the
model's facts are the only knowledge. Free SQL never comes from the model.

Code checks only what can be checked against the menu and the data: the metric is on the menu (exact, else the
closest name, noted); each dimension is one the metric may be grouped by and each filter names a column of the
metric with a valid operator (a table prefix added or dropped is matched); a filter value on a column with known
values is one of them (a case-insensitive exact match corrects it); a grain needs a time column and is one of the
five; the limit is within bounds. What is dropped is noted, kept as a ``Gap`` for the answer sentence, and makes
the answer ``partial``. The question's wording is never judged here. One repair round sends the refusals back,
except dimensions: a dimension sent back would be swapped for another silently. Known values and the rows a query
returns stay in core and never reach the model. Must not import ``litellm``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.semantic_answer import Gap, Intent, decide_intent, shape_query
from flowfile_core.ai.semantic_draft import call_json, match_name
from flowfile_core.schemas.semantic_schema import MetricFilter, MetricOrder, MetricQuery

logger = logging.getLogger(__name__)

MAX_MENU = 30
MAX_DIMENSIONS_SHOWN = 12
MAX_FILTER_FIELDS_SHOWN = 16
MAX_TOKENS = 500
MAX_INSTRUCTIONS_CHARS = 1200
_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "semantic_ask.md"

OPS = ("=", "!=", "<", "<=", ">", ">=", "in", "not in", "between", "is null", "is not null")
GRAINS = ("day", "week", "month", "quarter", "year")


def system_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


@dataclass
class MenuItem:
    """One accepted metric as the model sees it: its meaning, synonyms, time column and the columns it allows."""

    name: str
    meaning: str | None
    time_column: str | None
    dimensions: list[str]
    filter_fields: list[str] = field(default_factory=list)
    aggregate: str | None = None
    column: str | None = None
    table: str | None = None
    synonyms: list[str] = field(default_factory=list)
    dimension_words: dict[str, list[str]] = field(default_factory=dict)
    known_values: dict[str, list[str]] = field(default_factory=dict)
    label: str | None = None
    format: str | None = None
    expression: str | None = None


def _grouped(menu: list[MenuItem], columns: Any) -> dict[tuple[str, ...], list[str]]:
    groups: dict[tuple[str, ...], list[str]] = {}
    for m in menu:
        cols = tuple(columns(m))
        if cols:
            groups.setdefault(cols, []).append(m.name)
    return groups


def menu_text(menu: list[MenuItem], today: date) -> str:
    """The metrics, then the columns to group by and to filter on, each listed once per set of metrics."""
    lines = [f"Today: {today.isoformat()}", "Metrics:"]
    for m in menu:
        bits = [m.meaning or "No description."]
        if m.synonyms:
            bits.append("Also called: " + ", ".join(m.synonyms[:4]) + ".")
        if m.time_column:
            bits.append(f"Time column: {m.time_column}.")
        lines.append(f"- {m.name}: " + " ".join(bits))

    def written(m: MenuItem, d: str) -> str:
        also = [w for w in m.dimension_words.get(d, []) if w and w != d][:3]
        return f"{d} (also: {', '.join(also)})" if also else d

    lines.append("")
    lines.append("Columns to group by (use one ONLY when the question asks to break the answer down by it):")
    groups = _grouped(menu, lambda m: [written(m, d) for d in m.dimensions[:MAX_DIMENSIONS_SHOWN]])
    if not groups:
        lines.append("- none")
    for dims, names in groups.items():
        lines.append(f"- for {', '.join(names)}: {', '.join(dims)}")
    lines.append("")
    lines.append("Columns to filter on:")
    fields = _grouped(menu, lambda m: _filter_fields(m)[:MAX_FILTER_FIELDS_SHOWN])
    if not fields:
        lines.append("- none")
    for cols, names in fields.items():
        lines.append(f"- for {', '.join(names)}: {', '.join(cols)}")
    return "\n".join(lines)


def user_message(menu: list[MenuItem], question: str, today: date, instructions: str | None = None) -> str:
    owner = (instructions or "").strip()[:MAX_INSTRUCTIONS_CHARS]
    told = f"Instructions from the model's owner:\n{owner}\n\n" if owner else ""
    return (
        f"{menu_text(menu, today)}\n\n{told}Question: {question}\n\n"
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
    intent_hint: str | None = None
    intent: Intent | None = None
    dropped: dict[str, int] = field(default_factory=dict)
    gaps: list[Gap] = field(default_factory=list)

    def drop(self, gap: Gap, counter: str, problem: str | None = None) -> None:
        """Something the model asked for that code did not apply: noted, a gap, partial, counted for the audit."""
        if gap.note:
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


def _filter_fields(item: MenuItem) -> list[str]:
    return list(
        dict.fromkeys([*item.dimensions, *item.filter_fields, *([item.time_column] if item.time_column else [])])
    )


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
    """``(value, ok)``: a value of a column with known values, corrected to the stored spelling when it differs
    only in case; ``ok`` false when the column holds no such value. Columns without known values pass as given."""
    values = item.known_values.get(fld)
    if values is None or value is None:
        return value, True
    by_lower = {str(v).lower(): v for v in values}
    hit = by_lower.get(str(value).lower())
    return (hit, True) if hit is not None else (value, False)


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

    dims: list[str] = []
    for d in _as_list(payload.get("dimensions")):
        found = _field(d, item.dimensions, ans.notes)
        if found is None:
            note = f"Not split by {d}: it is not among the columns {metric_name} can be grouped by."
            ans.drop(Gap("group", str(d), f"it is not among the columns {metric_name} can be grouped by", note=note),
                     "dimensions_dropped")  # fmt: skip
        elif found not in dims:
            dims.append(found)

    fields = _filter_fields(item)
    filters: list[MetricFilter] = []

    def refuse(fld: str, reason: str, problem: str) -> None:
        gap = Gap("filter", fld, reason, column=fld, note=f"Not filtered on {fld}: {reason}.")
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
            refuse(str(raw_field), f"{metric_name} has no such column",
                   f"{raw_field!r} is not a column of {metric_name}. Use one of: {', '.join(fields)}.")  # fmt: skip
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
                ans.drop(Gap("filter", str(v), column=fld, note=note), "filters_dropped")
            if not kept:
                continue
            value = kept if isinstance(value, list) else kept[0]
        filters.append(MetricFilter(field=fld, op=op, value=value))

    grain = payload.get("grain")
    grain = str(grain).strip().lower() if grain not in (None, "", False) else None
    if grain is not None and grain not in GRAINS:
        note = f"Not split by {grain}: a grain is one of {', '.join(GRAINS)}."
        ans.drop(Gap("grain", grain, f"a grain is one of {', '.join(GRAINS)}", note=note), "grain_dropped",
                 f"The grain must be one of: {', '.join(GRAINS)}, or null.")  # fmt: skip
        grain = None
    elif grain is not None and not item.time_column:
        note = f"Not split by {grain}: {metric_name} has no time column."
        ans.drop(Gap("grain", grain, f"{metric_name} has no time column", note=note), "grain_dropped")
        grain = None

    order = None
    raw_order = payload.get("order")
    if isinstance(raw_order, str):
        raw_order = {"field": raw_order}
    if isinstance(raw_order, dict):
        outputs = [*dims, metric_name] + ([f"{item.time_column.rpartition('.')[2]}_{grain}"] if grain else [])
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
    ans.intent_hint = hint.strip().lower() if isinstance(hint, str) else None
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


def ground(ans: AskAnswer, menu: list[MenuItem]) -> AskAnswer:
    """The answer's kind and the query ordered as it reads; the "Read as" line first among the notes."""
    if ans.query is None:
        return ans
    item = next(m for m in menu if m.name == ans.query.metric)
    ans.intent = decide_intent(ans.query, ans.intent_hint)
    ans.query = shape_query(ans.query, ans.intent, item.time_column)
    ans.notes.insert(0, reading(ans.query))
    return ans


async def ask(
    provider: Provider,
    menu: list[MenuItem],
    question: str,
    *,
    user_id: int | None,
    today: date | None = None,
    run: Any = None,
    instructions: str | None = None,
) -> tuple[AskAnswer, int, Any]:
    """``(answer, attempts, run_result)``. ``run`` is an async compile-and-run; its ``ValueError`` is fed back once."""
    today = today or date.today()
    messages = [
        Message(role="system", content=system_prompt()),
        Message(role="user", content=user_message(menu, question, today, instructions)),
    ]
    attempts = 0
    ans = AskAnswer()
    for attempt in range(2):
        attempts += 1
        content, err, msg = await call_json(provider, messages, user_id=user_id, max_tokens=MAX_TOKENS)
        if err is not None:
            ans.reason, ans.problems = err, [msg or err]
            return ans, attempts, None
        ans = ground(read_answer(content, menu), menu)
        result = None
        last = attempt == 1
        if ans.query is not None and (not ans.problems or last) and run is not None:
            # on the last attempt, run what survived the checks; the dropped parts are in the notes
            ans.problems = []
            try:
                result = await run(ans.query)
            except ValueError as exc:
                ans.reason = "invalid_query"
                ans.problems.append(f"That query does not compile: {exc}")
        if ans.no_metric or (ans.query is not None and not ans.problems):
            return ans, attempts, result
        if attempt == 1:
            return ans, attempts, None
        logger.info("semantic ask answer refused, retrying: %s", ans.problems)
        messages = [
            *messages,
            Message(role="assistant", content=content or ""),
            Message(role="user", content="\n".join(ans.problems) + "\n\nAnswer again with the whole JSON object."),
        ]
    raise AssertionError("unreachable")  # pragma: no cover


__all__ = [
    "GRAINS",
    "MAX_MENU",
    "OPS",
    "AskAnswer",
    "MenuItem",
    "ask",
    "ground",
    "menu_text",
    "read_answer",
    "reading",
    "system_prompt",
    "user_message",
]
