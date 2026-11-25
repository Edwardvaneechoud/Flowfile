"""The answer sentence of ``POST /ai/semantic/ask``: what kind of answer a question wants, and the sentence.

The kind comes from the shape of the query the model wrote, the model's optional ``intent`` key deciding when it names
a kind that fits; the question's wording is never read here. The sentence is rendered from the rows the query returned,
so every number in it is the query's own and none is written by a model. Every function is pure.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from flowfile_core.catalog.semantic.activity import listed
from flowfile_core.schemas.semantic_schema import (
    MetricOrder,
    MetricQuery,
    MetricQueryResult,
    SemanticAnswerFacts,
    SemanticAnswerItem,
)

TOP_LIMIT = 100
MAX_LISTED = 6
MAX_TIED = 3

_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November",
    "December",
)  # fmt: skip


@dataclass
class Intent:
    """What kind of answer a question wants; ``n`` is how many groups a top or bottom answer names."""

    kind: str = "value"
    n: int = 1


@dataclass
class Rendered:
    """``notes`` are the grounding notes the sentence already says (a gap stated in it), left out of the list."""

    answer: str | None = None
    facts: SemanticAnswerFacts | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class Gap:
    """Part of the question the answer cannot cover, found by the grounding (``semantic_ask``).

    ``kind`` is ``split`` (a breakdown or a grain the metric cannot give) or ``filter`` (on ``column``; without a
    ``reason``, ``what`` is a value the column does not hold). ``note`` is the note it was told in.
    """

    kind: str
    what: str
    note: str
    reason: str = ""
    column: str = ""


def gap_sentences(gaps: list[Gap]) -> str:
    """What a partial answer leaves out, as sentences that follow the answer ("It is not split by station: …")."""
    out = [f"It is not split by {g.what}: {g.reason}." for g in gaps if g.kind == "split"]
    for g in gaps:
        if g.kind == "filter" and g.reason:
            out.append(f"It is not filtered on {_column_words(g.column)}: {g.reason}.")
        elif g.kind == "filter":
            out.append(f"It is not filtered to {g.what!r}: that is not a value of {_column_words(g.column)}.")
    return " ".join(out)


def decide_intent(query: MetricQuery, hint: str | None = None) -> Intent:
    """The kind of answer from the query's shape; the model's ``hint`` decides when it names a kind that fits.

    A grouped query ordered on the metric is ``top`` or ``bottom``, naming as many groups as the limit when that is a
    short list.
    """
    n = query.limit if 1 < query.limit <= MAX_LISTED else 1
    grouped, ranks = bool(query.dimensions), bool(query.dimensions) and not query.grain
    fits = {
        "value": not grouped and not query.grain,
        "trend": bool(query.grain) and not grouped,
        "top": ranks,
        "bottom": ranks,
        "breakdown": bool(grouped or query.grain),
    }
    kind = hint if fits.get(hint) else None
    if kind is None and ranks and query.order is not None and query.order.field == query.metric:
        kind = "top" if query.order.direction == "desc" else "bottom"
    kind = kind or ("breakdown" if grouped else "trend" if query.grain else "value")
    return Intent(kind, n if kind in ("top", "bottom") else 1)


def shape_query(query: MetricQuery, intent: Intent, time_column: str | None) -> MetricQuery:
    """The query ordered as the answer reads: by the metric for top and bottom, by period for a trend.

    A top or bottom question that did not say "top N" keeps a generous limit, since the sentence names the
    neighbours and the number of groups; "top 3" is cut to 3.
    """
    order, limit = query.order, query.limit
    if intent.kind in ("top", "bottom"):
        order = MetricOrder(field=query.metric, direction="desc" if intent.kind == "top" else "asc")
        limit = intent.n if intent.n > 1 else max(limit, TOP_LIMIT)
    elif intent.kind == "trend" and query.grain and time_column:
        order = MetricOrder(field=f"{time_column.rpartition('.')[2]}_{query.grain}", direction="asc")
    return query.model_copy(update={"order": order, "limit": limit})


def format_number(value: float | int, fmt: str | None = None) -> str:
    """The metric's ``format`` (``#,##0.00``, ``$#,##0``, ``0.0%``) when it has one, else thousands separators and
    two decimals for a fractional type."""
    if fmt and re.search(r"[0#]", fmt):
        decimals = len(re.findall(r"[0#]", fmt.split(".", 1)[1])) if "." in fmt else 0
        percent = "%" in fmt
        n = value * 100 if percent else value
        body = f"{n:,.{decimals}f}" if "," in fmt else f"{n:.{decimals}f}"
        first = re.search(r"[0#]", fmt).start()
        prefix = fmt[:first].replace("%", "") if first > 0 else ""
        return f"{prefix}{body}{'%' if percent else ''}"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.2f}"


def _number(v: Any) -> float | int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int | float):
        return None if isinstance(v, float) and math.isnan(v) else v
    if isinstance(v, str):
        try:
            return int(v) if re.fullmatch(r"-?\d+", v.strip()) else float(v)
        except ValueError:
            return None
    return None


def format_period(value: Any, grain: str | None) -> str:
    """``March 2026`` for a month, ``Q1 2026``, ``2026``, ``the week of 2 March 2026``, ``2 March 2026``."""
    text = str(value) if value is not None else ""
    try:
        d = date.fromisoformat(text[:10])
    except ValueError:
        return text or "(empty)"
    if grain == "year":
        return str(d.year)
    if grain == "quarter":
        return f"Q{(d.month - 1) // 3 + 1} {d.year}"
    if grain == "month":
        return f"{_MONTH_NAMES[d.month - 1]} {d.year}"
    day = f"{d.day} {_MONTH_NAMES[d.month - 1]} {d.year}"
    return f"the week of {day}" if grain == "week" else day


def _label(value: Any) -> str:
    if value is None or value == "":
        return "(empty)"
    return format_number(value) if isinstance(value, int | float) and not isinstance(value, bool) else str(value)


def _column_words(field_name: str) -> str:
    return field_name.rpartition(".")[2].replace("_", " ")


_OP_WORDS = {
    "=": "is", "!=": "is not", "<": "is below", "<=": "is at most", ">": "is above", ">=": "is at least",
    "in": "is", "not in": "is not", "is null": "is empty", "is not null": "is not empty", "between": "is between",
}  # fmt: skip


def filter_phrase(query: MetricQuery) -> str:
    """The query's filters in plain words ("where status is paid and country is Germany"), or ``""``."""
    where, years = [], ""
    for f in query.filters:
        vals = f.value if isinstance(f.value, list) else [f.value]
        texts = [_label(x) for x in vals]
        if f.op == "between":
            lo, hi = (str(x)[:10] for x in vals)
            if re.fullmatch(r"\d{4}-01-01", lo) and hi == f"{lo[:4]}-12-31":
                years += f" in {lo[:4]}"
                continue
            said = f" {texts[0]} and {texts[1]}"
        elif f.op in ("in", "not in"):
            said = " " + (", ".join(texts[:-1]) + " or " + texts[-1] if len(texts) > 1 else "".join(texts))
        else:
            said = "" if f.op in ("is null", "is not null") else f" {texts[0]}"
        where.append(f"{_column_words(f.field)} {_OP_WORDS[f.op]}{said}")
    return (" where " + " and ".join(where) if where else "") + years


def _said(pairs: list[tuple[str, float | int]], fmt: str | None) -> str:
    """``Germany (5), France (3) and Italy (1)``."""
    return listed([f"{label} ({format_number(v, fmt)})" for label, v in pairs])


def _item(label: str, value: float | int | None, fmt: str | None) -> SemanticAnswerItem:
    return SemanticAnswerItem(label=label, value=value, text=format_number(value, fmt) if value is not None else "")


def render(query: MetricQuery, result: MetricQueryResult, metric: Any, intent: Intent, gaps: list[Gap]) -> Rendered:
    """The answer sentence and its facts from ``result`` alone; ``answer`` is ``None`` when the rows cannot be read
    as an answer (an error, a missing metric column, a value that is not a number).

    What a partial answer leaves out (``gaps``) follows the answer; the notes of those gaps come back in
    ``Rendered.notes`` so the caller does not list them a second time.
    """
    if result.error or metric.name not in result.columns:
        return Rendered()
    rows = result.rows
    mi = result.columns.index(metric.name)
    key_idx = [i for i, c in enumerate(result.columns) if i != mi]
    truncated = bool(result.truncated) or (len(rows) >= query.limit and intent.n == 1)
    facts = SemanticAnswerFacts(intent=intent.kind, truncated=truncated)
    where = filter_phrase(query)
    if not rows:
        return Rendered(f"No rows match{where}.", facts)
    values = [_number(r[mi]) for r in rows]
    if any(v is None and r[mi] is not None for v, r in zip(values, rows, strict=True)):
        return Rendered()
    grain = query.grain
    grain_col = f"{metric.time_column.rpartition('.')[2]}_{grain}" if grain and metric.time_column else None

    def label(r: list[Any]) -> str:
        parts = [format_period(r[i], grain) if result.columns[i] == grain_col else _label(r[i]) for i in key_idx]
        return ", ".join(parts) or "(all)"

    name, fmt = metric.name.replace("_", " ").strip(), metric.format
    pairs = [(label(r), v) for r, v in zip(rows, values, strict=True)]
    valued = [p for p in pairs if p[1] is not None]
    kind = intent.kind
    if kind == "value" and (len(rows) > 1 or key_idx):
        kind = "breakdown"
    if kind == "value":
        v = values[0]
        facts.value = _item("", v, fmt) if v is not None else None
        text = f"The {name}{where} " + ("has no value." if v is None else f"is {format_number(v, fmt)}.")
    elif kind == "trend":
        text = _trend(valued, name, fmt, grain, facts)
    elif kind in ("top", "bottom"):
        text = _ranked(valued, name, metric, intent, facts)
    else:
        text = _breakdown(pairs, f"{name.capitalize()} by {_by(query)}{where}", fmt, facts)
    nulls = len(values) - len(valued)
    if nulls and kind != "value":
        text += f" {nulls} {'row has' if nulls == 1 else 'rows have'} no value."
    if truncated:
        text += f" This covers the first {len(rows):,} rows only."
    if gaps:
        text += f" {gap_sentences(gaps)}"
    return Rendered(text[:1].upper() + text[1:], facts, [g.note for g in gaps])


def _by(query: MetricQuery) -> str:
    by = " and ".join(_column_words(d) for d in query.dimensions) or "period"
    return f"{query.grain} and {by}" if query.grain and query.dimensions else by


def _ranked(pairs: list[tuple[str, Any]], name: str, metric: Any, intent: Intent, facts: SemanticAnswerFacts) -> str:
    if not pairs:
        return f"No group has a {name}."
    top, fmt = intent.kind == "top", metric.format
    pairs = sorted(pairs, key=lambda p: p[1], reverse=top)
    if intent.n > 1:
        shown = pairs[: intent.n]
        facts.leaders = [_item(lb, v, fmt) for lb, v in shown]
        side, verb = "top" if top else "bottom", "is" if len(shown) == 1 else "are"
        return f"The {side} {len(shown)} by {name} {verb} {_said(shown, fmt)}."
    facts.groups = len(pairs)
    best = pairs[0][1]
    tied, rest = [p for p in pairs if p[1] == best], [p for p in pairs if p[1] != best]
    facts.leaders = [_item(lb, v, fmt) for lb, v in tied]
    if len(pairs) == 1:
        return f"Only {pairs[0][0]} is in the result, with a {name} of {format_number(best, fmt)}."
    facts.value = facts.leaders[0]
    facts.runners_up = [_item(lb, v, fmt) for lb, v in rest[:2]]
    sup = f"the {'highest' if top else 'lowest'} {name}: {format_number(best, fmt)}"
    if len(tied) == 1:
        text = f"{tied[0][0]} has {sup}"
        total = sum(v for _, v in pairs)
        # a sum or a plain count adds up over groups, so a group's share means something; a distinct count does not
        adds_up = metric.aggregate in ("sum", "count") and "distinct" not in (metric.expression or "").lower()
        if top and adds_up and not facts.truncated and total > 0 and all(v >= 0 for _, v in pairs):
            facts.share = best / total
            text += f" ({round(facts.share * 100)}% of the total)"
        text += "."
    else:
        names = [lb for lb, _ in tied[:MAX_TIED]]
        if len(tied) > MAX_TIED:
            names.append(f"{len(tied) - MAX_TIED} more")
        text = f"{listed(names)} are tied for {sup} each."
    if rest:
        text += f" {'Next is' if len(rest) == 1 else 'Next are'} {_said(rest[:2], fmt)}."
    return text


def _breakdown(pairs: list[tuple[str, Any]], head: str, fmt: str | None, facts: SemanticAnswerFacts) -> str:
    facts.groups = len(pairs)
    if len(pairs) <= MAX_LISTED:
        values = [f"{lb} {format_number(v, fmt) if v is not None else 'no value'}" for lb, v in pairs]
        return f"{head}: {', '.join(values)}."
    ranked = sorted([p for p in pairs if p[1] is not None], key=lambda p: p[1], reverse=True)[:3]
    facts.leaders = [_item(lb, v, fmt) for lb, v in ranked]
    return f"{head}, across {len(pairs):,} groups: the highest are {_said(ranked, fmt)}."


def _trend(pairs: list[tuple[str, Any]], name: str, fmt: str | None, grain: str, facts: SemanticAnswerFacts) -> str:
    if not pairs:
        return f"No {grain} has a {name}."
    name = name.capitalize()
    ll, lv = pairs[-1]
    facts.latest = _item(ll, lv, fmt)
    if len(pairs) == 1:
        return f"{name} was {format_number(lv, fmt)} in {ll}, the only {grain} in the result."
    hi, lo = max(pairs, key=lambda p: p[1]), min(pairs, key=lambda p: p[1])
    facts.highest, facts.lowest = _item(*hi, fmt), _item(*lo, fmt)
    pl_, pv = pairs[-2]
    facts.previous = _item(pl_, pv, fmt)
    if pv == 0:
        facts.direction = "flat" if lv == 0 else "up"
        move = f"the same as {pl_}" if lv == 0 else f"up from 0 in {pl_}"
    else:
        facts.change = (lv - pv) / abs(pv)
        pct = round(abs(facts.change) * 100)
        facts.direction = "flat" if pct == 0 else "up" if facts.change > 0 else "down"
        move = f"about the same as {pl_}" if pct == 0 else f"{facts.direction} {pct}% from {pl_}"
    return (
        f"{name} was highest in {hi[0]} ({format_number(hi[1], fmt)}) and lowest in {lo[0]} "
        f"({format_number(lo[1], fmt)}). The latest {grain}, {ll}, is {format_number(lv, fmt)}, {move}."
    )
