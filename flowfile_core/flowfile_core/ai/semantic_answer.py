"""The answer sentence of ``POST /ai/semantic/ask``: what kind of answer a question wants, and the sentence.

The model suggests and the code decides: the intent comes from rules on the question, the model's optional
``intent`` key only breaking a tie, and the sentence is rendered here from the rows the query returned, so every
number in it is the query's own and none is written by a model. Nothing here reads the database or calls a model;
the result rows stay in core. Every function is pure.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from flowfile_core.schemas.semantic_schema import (
    MetricOrder,
    MetricQuery,
    MetricQueryResult,
    SemanticAnswerFacts,
    SemanticAnswerItem,
)

INTENTS = ("value", "top", "bottom", "trend", "breakdown")
TOP_LIMIT = 100
MAX_LISTED = 6
MAX_TIED = 3

_TOP = re.compile(
    r"\b(most|highest|largest|biggest|greatest|best|top|leading|maximum|max|more than any)\b", re.IGNORECASE
)
_BOTTOM = re.compile(r"\b(least|lowest|fewest|smallest|worst|bottom|minimum|min)\b", re.IGNORECASE)
_TOP_N = re.compile(
    r"\b(?:top|bottom|best|worst|highest|lowest|largest|smallest|biggest)\s+(\d{1,3})\b|\b(\d{1,3})\s+"
    r"(?:best|worst|highest|lowest|largest|smallest|biggest|most|least)\b",
    re.IGNORECASE,
)
_WHICH = re.compile(r"\b(which|what)\b", re.IGNORECASE)
_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November",
    "December",
)  # fmt: skip
_NOT_GROUPED = re.compile(r"^'(.+?)' is not something \S+ can be grouped by")
_UNCLEAR_VALUE = re.compile(r"^'(.+?)' is a value of .+; not filtered")
_NOT_A_VALUE = re.compile(r"^Left out \S+ \S+(?: in)? '(.+?)': '.+?' is not a value of")


@dataclass
class Intent:
    """What kind of answer a question wants; ``n`` is how many groups a top or bottom answer names."""

    kind: str = "value"
    n: int = 1
    explicit_n: bool = False


@dataclass
class MetricWords:
    """How a metric is said: its readable name, a plural noun for a row count, the number format, additivity."""

    name: str
    label: str | None = None
    aggregate: str | None = None
    format: str | None = None
    expression: str | None = None
    table: str | None = None
    time_column: str | None = None

    @property
    def readable(self) -> str:
        return (self.label or self.name.replace("_", " ")).strip()

    @property
    def noun(self) -> str | None:
        """``customers`` for ``customer_count`` on the customers table; ``None`` when that is not safe to say."""
        if self.aggregate != "count" or self.label or not self.table:
            return None
        m = re.fullmatch(r"([a-z]+)_count", self.name)
        table = self.table.rpartition(".")[2].lower()
        if m is None or table not in (m.group(1), m.group(1) + "s", m.group(1)[:-1] + "ies"):
            return None
        return table if table.endswith("s") else _plural(table)

    @property
    def singular(self) -> str | None:
        return self.name[: -len("_count")] if self.noun else None

    @property
    def additive(self) -> bool:
        """A sum or a plain count adds up over groups, so a group's share means something; a distinct count does
        not."""
        if self.aggregate not in ("sum", "count"):
            return False
        return "distinct" not in (self.expression or "").lower()


@dataclass
class Rendered:
    answer: str | None = None
    facts: SemanticAnswerFacts | None = None
    notes: list[str] = field(default_factory=list)


def _plural(word: str) -> str:
    if word.endswith("y") and not word.endswith(("ay", "ey", "oy", "uy")):
        return word[:-1] + "ies"
    if word.endswith(("s", "x", "ch", "sh")):
        return word + "es"
    return word + "s"


def decide_intent(question: str, query: MetricQuery | None, hint: Any = None) -> Intent:
    """Rules on the question first; the model's ``hint`` only decides when the rules cannot.

    No dimension and no grain is ``value`` whatever the question says: a superlative over nothing it can group by
    has one number to give (the grounding already marks such a question partial). A grain without dimensions is
    ``trend``. A superlative word over dimensions is ``top`` or ``bottom``; both kinds of word, or a "which …"
    question with neither, take the hint when it is one of those two.
    """
    hint = str(hint).strip().lower() if isinstance(hint, str) else None
    if query is None:
        return Intent()
    if not query.dimensions:
        return Intent("trend") if query.grain else Intent()
    if query.grain:
        return Intent("breakdown")
    top, bottom = _TOP.search(question), _BOTTOM.search(question)
    kind = "breakdown"
    if top and bottom:
        kind = hint if hint in ("top", "bottom") else ("top" if top.start() < bottom.start() else "bottom")
    elif top or bottom:
        kind = "top" if top else "bottom"
    elif _WHICH.search(question) and hint in ("top", "bottom"):
        kind = hint
    if kind == "breakdown":
        return Intent("breakdown")
    m = _TOP_N.search(question)
    n = int(m.group(1) or m.group(2)) if m else 1
    return Intent(kind, max(1, min(n, MAX_LISTED)), explicit_n=m is not None and n > 1)


def shape_query(query: MetricQuery, intent: Intent, time_column: str | None) -> MetricQuery:
    """The query ordered as the answer reads: by the metric for top and bottom, by period for a trend.

    A top or bottom question that did not say "top N" keeps a generous limit, since the sentence names the
    neighbours and the number of groups; "top 3" is cut to 3.
    """
    order, limit = query.order, query.limit
    if intent.kind in ("top", "bottom"):
        order = MetricOrder(field=query.metric, direction="desc" if intent.kind == "top" else "asc")
        limit = intent.n if intent.explicit_n else max(limit, TOP_LIMIT)
    elif intent.kind == "trend" and query.grain and time_column:
        order = MetricOrder(field=f"{time_column.rpartition('.')[2]}_{query.grain}", direction="asc")
    if order == query.order and limit == query.limit:
        return query
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


def _join(parts: list[str]) -> str:
    if len(parts) <= 1:
        return "".join(parts)
    return ", ".join(parts[:-1]) + " and " + parts[-1]


def _column_words(field_name: str) -> str:
    return field_name.rpartition(".")[2].replace("_", " ")


def filter_phrase(query: MetricQuery) -> str:
    """The query's filters in plain words ("where status is paid and country is Germany"), or ``""``."""
    bits = []
    for f in query.filters:
        col = _column_words(f.field)
        v = f.value
        vals = v if isinstance(v, list) else [v]
        texts = [_label(x) for x in vals]
        if f.op == "between" and len(vals) == 2:
            lo, hi = (str(x)[:10] for x in vals)
            if re.fullmatch(r"\d{4}-01-01", lo) and hi == f"{lo[:4]}-12-31":
                bits.append(f"in {lo[:4]}")
            else:
                bits.append(f"{col} is between {texts[0]} and {texts[1]}")
            continue
        words = {
            "=": "is", "!=": "is not", "<": "is below", "<=": "is at most", ">": "is above", ">=": "is at least",
            "in": "is", "not in": "is not",
        }  # fmt: skip
        if f.op == "is null":
            bits.append(f"{col} is empty")
        elif f.op == "is not null":
            bits.append(f"{col} is not empty")
        elif f.op in ("in", "not in"):
            either = ", ".join(texts[:-1]) + " or " + texts[-1] if len(texts) > 1 else "".join(texts)
            bits.append(f"{col} {words[f.op]} {either}")
        elif f.op in words:
            bits.append(f"{col} {words[f.op]} {texts[0]}")
    if not bits:
        return ""
    dated = [b for b in bits if b.startswith("in ")]
    rest = [b for b in bits if not b.startswith("in ")]
    out = (" where " + " and ".join(rest)) if rest else ""
    return out + ("".join(" " + b for b in dated))


def uncovered(notes: list[str]) -> list[str]:
    """What a partial answer leaves out, read from the grounding's notes, as short phrases."""
    out = []
    for n in notes:
        if m := _NOT_GROUPED.match(n):
            out.append(f"the {m.group(1)} grouping you asked for")
        elif m := _UNCLEAR_VALUE.match(n):
            out.append(f"the filter on {m.group(1)!r} (it matches more than one column)")
        elif m := _NOT_A_VALUE.match(n):
            out.append(f"the filter on {m.group(1)!r}")
    return list(dict.fromkeys(out))


def _item(label: str, value: float | int | None, words: MetricWords) -> SemanticAnswerItem:
    text = format_number(value, words.format) if value is not None else ""
    return SemanticAnswerItem(label=label, value=value, text=text)


def _amount(value: float | int, words: MetricWords) -> str:
    """``7 customers`` for a row count with a noun, else ``a total amount of 98,058.50``."""
    text = format_number(value, words.format)
    if words.noun:
        return f"{text} {words.noun if value != 1 else words.singular}"
    return f"a {words.readable} of {text}"


def render(
    question: str,
    query: MetricQuery,
    result: MetricQueryResult,
    words: MetricWords,
    intent: Intent,
    *,
    partial: bool = False,
    notes: list[str] | None = None,
) -> Rendered:
    """The answer sentence and its facts from ``result`` alone; ``answer`` is ``None`` when the rows cannot be read
    as an answer (an error, a missing metric column, a value that is not a number)."""
    if result.error or words.name not in result.columns:
        return Rendered()
    rows = result.rows
    mi = result.columns.index(words.name)
    key_idx = [i for i, c in enumerate(result.columns) if i != mi]
    truncated = bool(result.truncated) or (len(rows) >= query.limit and not intent.explicit_n)
    facts = SemanticAnswerFacts(intent=intent.kind, truncated=truncated)
    if not rows:
        return Rendered("No rows match" + filter_phrase(query) + ".", facts)
    values = [_number(r[mi]) for r in rows]
    if any(v is None and r[mi] is not None for v, r in zip(values, rows, strict=True)):
        return Rendered()
    grain = query.grain
    grain_col = f"{words.time_column.rpartition('.')[2]}_{grain}" if grain and words.time_column else None

    def label(r: list[Any]) -> str:
        parts = []
        for i in key_idx:
            col = result.columns[i]
            parts.append(format_period(r[i], grain) if col == grain_col else _label(r[i]))
        return ", ".join(parts) or "(all)"

    kind = intent.kind if len(rows) > 1 or intent.kind != "value" else "value"
    if kind == "value" and (len(rows) > 1 or key_idx):
        kind = "breakdown"
    if kind == "value":
        v = values[0]
        facts.value = _item("", v, words) if v is not None else None
        if v is None:
            text = f"The {words.readable}{filter_phrase(query)} has no value."
        elif words.noun:
            text = f"There {'is' if v == 1 else 'are'} {_amount(v, words)}{filter_phrase(query)}."
        else:
            text = f"The {words.readable}{filter_phrase(query)} is {format_number(v, words.format)}."
    elif kind == "trend":
        text = _trend(rows, values, label, words, grain, facts)
    elif kind in ("top", "bottom"):
        text = _ranked(rows, values, label, words, intent, query, facts)
    else:
        text = _breakdown(rows, values, label, words, query, facts)
    nulls = sum(1 for v in values if v is None)
    if nulls and kind != "value":
        text += f" {nulls} {'row has' if nulls == 1 else 'rows have'} no value."
    if truncated:
        text += f" This covers the first {len(rows):,} rows only."
    if partial:
        missing = uncovered(notes or [])
        lead = "Without " + _join(missing) if missing else "This answers only part of the question"
        if text.startswith(("The ", "There ")):
            text = text[0].lower() + text[1:]
        text = f"{lead}: {text}"
    return Rendered(text[:1].upper() + text[1:], facts)


def _ranked(rows, values, label, words, intent, query, facts) -> str:
    pairs = [(label(r), v) for r, v in zip(rows, values, strict=True) if v is not None]
    if not pairs:
        return f"No group has a {words.readable}."
    top = intent.kind == "top"
    pairs.sort(key=lambda p: p[1], reverse=top)
    facts.groups = None if intent.explicit_n else len(pairs)
    if words.noun:
        sup = f"the {'most' if top else 'fewest'} {words.noun}"
    else:
        sup = f"the {'highest' if top else 'lowest'} {words.readable}"
    fmt = words.format
    if intent.explicit_n:
        shown = pairs[: intent.n]
        facts.leaders = [_item(lb, v, words) for lb, v in shown]
        listed = _join([f"{lb} ({format_number(v, fmt)})" for lb, v in shown])
        side = "top" if top else "bottom"
        return f"The {side} {len(shown)} by {words.readable} {'is' if len(shown) == 1 else 'are'} {listed}."
    if len(pairs) == 1:
        lb, v = pairs[0]
        facts.leaders = [_item(lb, v, words)]
        return f"Only {lb} is in the result, with {_amount(v, words)}."
    best = pairs[0][1]
    tied = [p for p in pairs if p[1] == best]
    rest = [p for p in pairs if p[1] != best]
    facts.leaders = [_item(lb, v, words) for lb, v in tied]
    facts.value = _item(tied[0][0], best, words)
    facts.runners_up = [_item(lb, v, words) for lb, v in rest[:2]]
    if len(tied) == 1:
        text = f"{tied[0][0]} has {sup}: {format_number(best, fmt)}."
    else:
        names = [lb for lb, _ in tied[:MAX_TIED]]
        if len(tied) > MAX_TIED:
            names.append(f"{len(tied) - MAX_TIED} more")
        text = f"{_join(names)} are tied for {sup}: {format_number(best, fmt)} each."
    total = sum(v for _, v in pairs)
    whole = not facts.truncated and total > 0 and all(v >= 0 for _, v in pairs)
    if top and words.additive and whole and len(tied) == 1:
        facts.share = best / total
        text = text[:-1] + f" ({round(facts.share * 100)}% of the total)."
    if rest[:2]:
        nxt = _join([f"{lb} ({format_number(v, fmt)})" for lb, v in rest[:2]])
        text += f" {'Next is' if len(rest[:2]) == 1 else 'Next are'} {nxt}."
    return text


def _breakdown(rows, values, label, words, query, facts) -> str:
    pairs = [(label(r), v) for r, v in zip(rows, values, strict=True)]
    facts.groups = len(pairs)
    by = " and ".join(_column_words(d) for d in query.dimensions) or "period"
    if query.grain and query.dimensions:
        by = f"{query.grain} and {by}"
    head = f"{(words.noun or words.readable).capitalize()} by {by}{filter_phrase(query)}"
    fmt = words.format
    if len(pairs) <= MAX_LISTED:
        listed = ", ".join(f"{lb} {format_number(v, fmt) if v is not None else 'no value'}" for lb, v in pairs)
        return f"{head}: {listed}."
    ranked = sorted([p for p in pairs if p[1] is not None], key=lambda p: p[1], reverse=True)
    facts.leaders = [_item(lb, v, words) for lb, v in ranked[:3]]
    many = _plural(_column_words(query.dimensions[0])) if len(query.dimensions) == 1 and not query.grain else "groups"
    top3 = _join([f"{lb} ({format_number(v, fmt)})" for lb, v in ranked[:3]])
    return f"{head}, across {len(pairs):,} {many}: the highest are {top3}."


def _trend(rows, values, label, words, grain, facts) -> str:
    pairs = [(label(r), v) for r, v in zip(rows, values, strict=True) if v is not None]
    name = f"The number of {words.noun}" if words.noun else words.readable.capitalize()
    unit = grain or "period"
    fmt = words.format
    if not pairs:
        return f"No {unit} has a {words.readable}."
    if len(pairs) == 1:
        lb, v = pairs[0]
        facts.latest = _item(lb, v, words)
        return f"{name} was {format_number(v, fmt)} in {lb}, the only {unit} in the result."
    hi = max(pairs, key=lambda p: p[1])
    lo = min(pairs, key=lambda p: p[1])
    facts.highest, facts.lowest = _item(*hi, words), _item(*lo, words)
    (pl_, pv), (ll, lv) = pairs[-2], pairs[-1]
    facts.latest, facts.previous = _item(ll, lv, words), _item(pl_, pv, words)
    text = (
        f"{name} was highest in {hi[0]} ({format_number(hi[1], fmt)}) and lowest in {lo[0]} "
        f"({format_number(lo[1], fmt)})."
    )
    if pv == 0:
        facts.direction = "flat" if lv == 0 else "up"
        change = "the same as" if lv == 0 else "up from 0 in"
        return text + f" The latest {unit}, {ll}, is {format_number(lv, fmt)}, {change} {pl_}."
    facts.change = (lv - pv) / abs(pv)
    pct = round(abs(facts.change) * 100)
    if pct == 0:
        facts.direction = "flat"
        move = f"about the same as {pl_}"
    else:
        facts.direction = "up" if facts.change > 0 else "down"
        move = f"{facts.direction} {pct}% from {pl_}"
    return text + f" The latest {unit}, {ll}, is {format_number(lv, fmt)}, {move}."


__all__ = [
    "INTENTS",
    "Intent",
    "MetricWords",
    "Rendered",
    "decide_intent",
    "filter_phrase",
    "format_number",
    "format_period",
    "render",
    "shape_query",
    "uncovered",
]
