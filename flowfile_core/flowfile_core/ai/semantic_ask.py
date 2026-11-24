"""A question → one query of an accepted metric, backing ``POST /ai/semantic/ask``.

The model sees a short menu of the **accepted** model only (at most ``MAX_MENU`` metrics, narrowed with
glosslayer's keyword search when there are more) and today's date, and answers a ``MetricQuery``-shaped object or
``{"no_metric": true, "why": …}``. Free SQL never comes from the model: it picks names, and every name is checked
here against the menu it was given (``difflib`` for slips, case and a dropped table prefix), obvious shape slips are
coerced (a bare string where a list is wanted, ``==`` or ``eq`` for ``=``, ``monthly`` for ``month``), and what
cannot be matched is dropped with a note. One repair round sends the remaining problems back as a user turn. The
rows a query returns go to the browser only and are never sent to the model. Must not import ``litellm``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.semantic_draft import call_json, match_name
from flowfile_core.schemas.semantic_schema import MetricFilter, MetricOrder, MetricQuery

logger = logging.getLogger(__name__)

MAX_MENU = 30
MAX_DIMENSIONS_SHOWN = 12
MAX_TOKENS = 500
_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "semantic_ask.md"

_OPS = {
    "=": "=", "==": "=", "eq": "=", "equals": "=", "is": "=",
    "!=": "!=", "<>": "!=", "ne": "!=", "neq": "!=", "not": "!=",
    "<": "<", "lt": "<", "<=": "<=", "le": "<=", "lte": "<=",
    ">": ">", "gt": ">", ">=": ">=", "ge": ">=", "gte": ">=",
    "in": "in", "not in": "not in", "not_in": "not in", "notin": "not in",
    "between": "between", "range": "between",
    "is null": "is null", "isnull": "is null", "is_null": "is null",
    "is not null": "is not null", "notnull": "is not null", "is_not_null": "is not null",
}  # fmt: skip
_GRAINS = {
    "day": "day", "daily": "day", "days": "day", "week": "week", "weekly": "week", "weeks": "week",
    "month": "month", "monthly": "month", "months": "month", "quarter": "quarter", "quarterly": "quarter",
    "year": "year", "yearly": "year", "annual": "year", "years": "year",
}  # fmt: skip


def system_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


@dataclass
class MenuItem:
    """One accepted metric as the model sees it: its meaning, time column and the dimensions it allows."""

    name: str
    meaning: str | None
    time_column: str | None
    dimensions: list[str]
    filter_fields: list[str] = field(default_factory=list)


def menu_text(menu: list[MenuItem], today: date) -> str:
    lines = [f"Today: {today.isoformat()}", "Menu:"]
    for m in menu:
        bits = [m.meaning or "No description."]
        if m.time_column:
            bits.append(f"Time column: {m.time_column}.")
        dims = m.dimensions[:MAX_DIMENSIONS_SHOWN]
        bits.append("Dimensions: " + (", ".join(dims) if dims else "none"))
        lines.append(f"- {m.name}: " + " ".join(bits))
    return "\n".join(lines)


def user_message(menu: list[MenuItem], question: str, today: date) -> str:
    return (
        f"{menu_text(menu, today)}\n\nQuestion: {question}\n\n"
        'Answer now with the JSON object {"metric": ..., "dimensions": [...], "filters": [...], "grain": ..., '
        '"order": ..., "limit": ...} or {"no_metric": true, "why": ...}.'
    )


@dataclass
class AskAnswer:
    query: MetricQuery | None = None
    no_metric: bool = False
    why: str | None = None
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    reason: str | None = None


def _as_list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    return value if isinstance(value, list) else [value]


def _field(name: Any, choices: list[str], notes: list[str]) -> str | None:
    """A dimension or filter field: as written, else matched; ``orders.status`` also matches a bare ``status``."""
    found = match_name(name, choices, notes)
    if found is None and isinstance(name, str) and "." in name:
        found = match_name(name.rsplit(".", 1)[-1], choices, notes)
    if found is None and isinstance(name, str):
        tails = [c for c in choices if c.rsplit(".", 1)[-1].lower() == name.lower()]
        found = tails[0] if len(tails) == 1 else None
    return found


def read_answer(content: str | None, menu: list[MenuItem]) -> AskAnswer:
    """One answer checked against the menu; unmatched names are dropped (noted) or, for the metric, a problem."""
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
    allowed = list(item.dimensions)
    filter_fields = list(
        dict.fromkeys([*allowed, *item.filter_fields, *([item.time_column] if item.time_column else [])])
    )

    dims: list[str] = []
    for d in _as_list(payload.get("dimensions")):
        found = _field(d, allowed, ans.notes)
        if found is None:
            ans.notes.append(f"Dimension {d!r} is not allowed for {metric_name}; dropped.")
            if not allowed:
                continue
            ans.problems.append(f"{d!r} is not a dimension of {metric_name}; use one of: {', '.join(allowed)}.")
        elif found not in dims:
            dims.append(found)

    filters: list[MetricFilter] = []
    raw_filters = payload.get("filters")
    if isinstance(raw_filters, dict):
        raw_filters = [raw_filters]
    for f in _as_list(raw_filters):
        if not isinstance(f, dict):
            ans.notes.append(f"Filter {f!r} is not an object; dropped.")
            continue
        fld = _field(f.get("field") or f.get("column"), filter_fields, ans.notes)
        op = _OPS.get(str(f.get("op") or f.get("operator") or "=").strip().lower())
        value = f.get("value")
        if fld is None or op is None:
            ans.notes.append(f"Filter on {f.get('field')!r} ({f.get('op')!r}) cannot be used; dropped.")
            continue
        if op in ("in", "not in"):
            value = _as_list(value)
        elif op == "between":
            value = _as_list(value)
            if len(value) != 2:
                ans.notes.append(f"Filter on {fld!r}: between needs two values; dropped.")
                continue
        elif op in ("is null", "is not null"):
            value = None
        elif isinstance(value, list):
            if len(value) == 1:
                value = value[0]
            else:
                op = "in" if op == "=" else "not in" if op == "!=" else op
                if op not in ("in", "not in"):
                    ans.notes.append(f"Filter on {fld!r}: one value expected; dropped.")
                    continue
        filters.append(MetricFilter(field=fld, op=op, value=value))

    grain = payload.get("grain")
    grain = _GRAINS.get(str(grain).strip().lower()) if grain not in (None, "", False) else None
    if grain and not item.time_column:
        ans.notes.append(f"{metric_name} has no time column, so the grain was dropped.")
        grain = None

    order = None
    raw_order = payload.get("order")
    if isinstance(raw_order, str):
        raw_order = {"field": raw_order}
    if isinstance(raw_order, dict):
        names_out = [*dims, metric_name] + ([f"{item.time_column}_{grain}"] if grain else [])
        target = match_name(raw_order.get("field"), names_out, ans.notes)
        direction = str(raw_order.get("direction") or "asc").lower()
        direction = "desc" if direction.startswith("desc") else "asc"
        if target:
            order = MetricOrder(field=target, direction=direction)
        else:
            ans.notes.append(f"Order by {raw_order.get('field')!r} is not a dimension or the metric; dropped.")
    try:
        limit = int(payload.get("limit") or 100)
    except (TypeError, ValueError):
        limit = 100
    limit = min(max(limit, 1), 10000)
    ans.query = MetricQuery(metric=metric_name, dimensions=dims, filters=filters, grain=grain, order=order, limit=limit)
    return ans


async def ask(
    provider: Provider,
    menu: list[MenuItem],
    question: str,
    *,
    user_id: int | None,
    today: date | None = None,
    run: Any = None,
) -> tuple[AskAnswer, int, Any]:
    """``(answer, attempts, run_result)``. ``run`` is an async compile-and-run; its ``ValueError`` is fed back once."""
    today = today or date.today()
    messages = [
        Message(role="system", content=system_prompt()),
        Message(role="user", content=user_message(menu, question, today)),
    ]
    attempts = 0
    ans = AskAnswer()
    for attempt in range(2):
        attempts += 1
        content, err, msg = await call_json(provider, messages, user_id=user_id, max_tokens=MAX_TOKENS)
        if err is not None:
            ans.reason, ans.problems = err, [msg or err]
            return ans, attempts, None
        ans = read_answer(content, menu)
        result = None
        last = attempt == 1
        if ans.query is not None and (not ans.problems or last) and run is not None:
            # on the last attempt, run what survived the seam; the dropped parts are in the notes
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


__all__ = ["MAX_MENU", "AskAnswer", "MenuItem", "ask", "menu_text", "read_answer", "system_prompt", "user_message"]
