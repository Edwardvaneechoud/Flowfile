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

import difflib
import logging
import re
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
    aggregate: str | None = None
    column: str | None = None
    table: str | None = None
    synonyms: list[str] = field(default_factory=list)
    dimension_words: dict[str, list[str]] = field(default_factory=dict)
    known_values: dict[str, list[str]] = field(default_factory=dict)


def menu_text(menu: list[MenuItem], today: date) -> str:
    """The metrics, then the columns one may group by listed once per set of metrics that allow them."""
    lines = [f"Today: {today.isoformat()}", "Metrics:"]
    for m in menu:
        bits = [m.meaning or "No description."]
        if m.synonyms:
            bits.append("Also called: " + ", ".join(m.synonyms[:4]) + ".")
        if m.time_column:
            bits.append(f"Time column: {m.time_column}.")
        lines.append(f"- {m.name}: " + " ".join(bits))
    groups: dict[tuple[str, ...], list[str]] = {}
    for m in menu:
        dims = tuple(m.dimensions[:MAX_DIMENSIONS_SHOWN])
        if dims:
            groups.setdefault(dims, []).append(m.name)
    lines.append("")
    lines.append("Columns to group by (use one ONLY when the question asks to break the answer down by it):")
    if not groups:
        lines.append("- none")
    for dims, names in groups.items():
        lines.append(f"- for {', '.join(names)}: {', '.join(dims)}")
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
    partial: bool = False
    reading_extra: str | None = None


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
    if metric_name is None and isinstance(payload.get("metric"), str):
        by_synonym = {syn.lower(): m.name for m in menu for syn in m.synonyms}
        metric_name = by_synonym.get(payload["metric"].strip().lower().replace("_", " "))
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
            # never sent back for a repair: the model would substitute another dimension silently
            ans.notes.append(f"Left out {d!r}: it is not something {metric_name} can be grouped by.")
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


_WORD = re.compile(r"[a-z0-9]+")
_GENERIC_COLUMNS = {"name", "title", "label", "description", "type", "kind", "code"}
_GRAIN_WORDS = {"day", "week", "month", "quarter", "year", "date", "time"}
_STOP = (
    r"for|in|with|where|that|which|during|from|since|before|after|between|last|this|over|of|on|when|whose|who|"
    r"to|than|excluding|except|without|only|sorted|ordered|order|top|biggest|largest|highest|lowest|first"
)
_REQUEST = re.compile(
    r"\b(?:by|per|for each|for every|each|every|across|split (?:by|into|per)|broken down by|"
    r"break(?:s)? (?:it )?down by|"
    rf"grouped by|group(?:ed)? per|group by)\s+(.+?)(?=$|[?.!,;:]|\s(?:{_STOP})\b)",
    re.IGNORECASE,
)
_ASKED_GRAIN = re.compile(
    r"\b(?:by|per|each|every|a|an)\s+(day|week|month|quarter|year)s?\b|\b(daily|weekly|monthly|quarterly|yearly|"
    r"annually|annual)\b|\b(over time|trend|trends|trending)\b",
    re.IGNORECASE,
)
_NEGATION = re.compile(
    r"\b(not|except|excluding|exclude|without|other than|non|isn't|aren't|wasn't|weren't|no)\b|\bnon-", re.IGNORECASE
)
_YEAR = re.compile(r"\b(19\d\d|20\d\d)\b")
_MONTHS = (
    "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november",
    "december", "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec",
)  # fmt: skip
_RELATIVE_DATE = re.compile(
    r"\b(last|this|since|before|after|between|past|previous|year to date|ytd|yesterday|today|q[1-4]|"
    r"recent|recently|ago|until|through)\b",
    re.IGNORECASE,
)
_NULL_WORDS = re.compile(r"\b(null|missing|empty|blank|unknown|known|without|no|has|have|set)\b", re.IGNORECASE)
_SUM_WORDS = re.compile(r"\b(total|sum|how much|most|overall|in total|altogether|combined)\b", re.IGNORECASE)
_AVG_WORDS = re.compile(r"\b(average|avg|mean|typical|typically|per order on average)\b", re.IGNORECASE)
_HOW_MANY = re.compile(r"\bhow many\s+([a-z_]+(?:\s+[a-z_]+){0,3})", re.IGNORECASE)


def _stem(token: str) -> str:
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


def words(text: str) -> list[str]:
    """Lower-case whole words with ``_`` read as a space and a plural ``s`` dropped."""
    return [_stem(t) for t in _WORD.findall(str(text).lower().replace("_", " "))]


def _contains(haystack: list[str], phrase: list[str]) -> bool:
    n = len(phrase)
    return n > 0 and any(haystack[i : i + n] == phrase for i in range(len(haystack) - n + 1))


def dimension_phrases(dimension: str, extra: list[str] | None = None) -> list[list[str]]:
    """The word sequences that name ``dimension`` in a question; a generic column (``name``) needs its table's word.

    The table name alone never names a column: ``products.category`` is named by "category", not by "product".
    """
    table, _, column = dimension.rpartition(".")
    col = words(column)
    phrases = []
    if len(col) == 1 and col[0] in _GENERIC_COLUMNS:
        if table:
            phrases.append([*words(table), *col])
    elif col:
        phrases.append(col)
    for e in extra or []:
        if words(e):
            phrases.append(words(e))
    return phrases


_WHICH = re.compile(
    r"\b(?:which|what)\s+([a-z_ ]+?)\s+(?:has|have|had|buys?|bought|spends?|spent|sells?|sold|is|are|was|were|"
    r"did|does|gets?|got|makes?|made|brings?|brought|orders?|ordered)\b",
    re.IGNORECASE,
)


def requested_groups(question: str) -> list[str]:
    """The things a question asks to break the answer down by ("by country and segment" → two, "which segment
    buys most" → one)."""
    out = []
    for match in [*_REQUEST.finditer(question), *_WHICH.finditer(question)]:
        for part in re.split(r"\s*(?:,|\band\b|&|\bthen\b)\s*", match.group(1)):
            part = re.sub(r"^(?:the|a|an|each|every)\s+", "", part.strip(), flags=re.IGNORECASE)
            if part and not set(words(part)) <= _GRAIN_WORDS:
                out.append(part)
    return out


def asked_grain(question: str) -> str | None:
    """``month`` for "by month" / "monthly"; ``"any"`` for "over time" / "trend"; ``None`` when no grain is asked."""
    m = _ASKED_GRAIN.search(question)
    if m is None:
        return None
    if m.group(1):
        return m.group(1).lower()
    if m.group(2):
        return _GRAINS.get(m.group(2).lower(), "year")
    return "any"


def question_years(question: str) -> set[str]:
    return set(_YEAR.findall(question))


def has_date(question: str) -> bool:
    q = question.lower()
    return bool(question_years(question) or _RELATIVE_DATE.search(q) or any(_contains(words(q), [m]) for m in _MONTHS))


def negates(question: str) -> bool:
    return bool(_NEGATION.search(question))


def value_in_question(value: Any, question: str) -> bool:
    """A filter value the question states: as words, or as a number or year."""
    if value is None or isinstance(value, bool):
        return False
    q = words(question)
    if isinstance(value, int | float):
        text = str(int(value)) if float(value).is_integer() else str(value)
        return text in re.findall(r"\d+(?:\.\d+)?", question)
    return _contains(q, words(value))


def _is_date_filter(f: MetricFilter, time_column: str | None) -> bool:
    if time_column and f.field.rpartition(".")[2] == time_column.rpartition(".")[2]:
        return True
    values = f.value if isinstance(f.value, list) else [f.value]
    return any(isinstance(v, str) and re.match(r"^\d{4}-\d{2}(-\d{2})?", v) for v in values)


def ground_filters(filters: list[MetricFilter], question: str, item: MenuItem, notes: list[str]) -> list[MetricFilter]:
    """Each filter kept only when the question states it; a negation kept only when the question negates."""
    kept: list[MetricFilter] = []
    negated = negates(question)
    years = question_years(question)
    relative = bool(_RELATIVE_DATE.search(question))
    for f in filters:
        lead = (
            re.match(r"^\s*(?:not|non|except|excluding|no)[\s-]+(.+)$", f.value, re.IGNORECASE)
            if isinstance(f.value, str)
            else None
        )
        if lead and f.op in ("=", "!="):
            notes.append(f"Read {f.field} {f.op} {f.value!r} as {f.field} != {lead.group(1)!r}.")
            f = MetricFilter(field=f.field, op="!=", value=lead.group(1))
        label = f"{f.field} {f.op} {f.value!r}" if f.value is not None else f"{f.field} {f.op}"
        if _is_date_filter(f, item.time_column):
            if not has_date(question):
                notes.append(f"Left out the date filter ({label}): the question names no date or period.")
                continue
            vals = f.value if isinstance(f.value, list) else [f.value]
            stated = {str(v)[:4] for v in vals if isinstance(v, str) and re.match(r"^\d{4}", v)}
            if years and not relative and stated and not stated <= years:
                notes.append(f"Left out the date filter ({label}): the question asks about {', '.join(sorted(years))}.")
                continue
            q = words(question)
            if len(years) == 1 and not relative and not any(_contains(q, [m]) for m in _MONTHS):
                (y,) = years
                whole = [f"{y}-01-01", f"{y}-12-31"]
                if f.op != "between" or [str(v)[:10] for v in vals] != whole:
                    notes.append(f"Read the date filter ({label}) as the whole of {y}: the question names the year.")
                    f = MetricFilter(field=f.field, op="between", value=whole)
            kept.append(f)
            continue
        if f.op in ("is null", "is not null"):
            if _NULL_WORDS.search(question):
                kept.append(f)
            else:
                notes.append(f"Left out the filter {label}: the question does not ask about missing values.")
            continue
        if f.op in ("in", "not in", "between"):
            vals = list(f.value or [])
            stated = [v for v in vals if value_in_question(v, question)]
            if f.op == "between" and len(stated) != 2:
                notes.append(f"Left out the filter {label}: the question does not state that range.")
                continue
            if not stated:
                notes.append(f"Left out the filter {label}: the question does not mention it.")
                continue
            if len(stated) < len(vals):
                notes.append(f"Filter on {f.field} kept only the values the question names: {stated!r}.")
            f = MetricFilter(field=f.field, op=f.op, value=stated)
        elif not value_in_question(f.value, question):
            notes.append(f"Left out the filter {label}: the question does not mention {f.value!r}.")
            continue
        if f.op in ("!=", "not in") and not negated:
            flipped = "=" if f.op == "!=" else "in"
            notes.append(f"Read {f.field} {f.op} {f.value!r} as {flipped}: the question does not exclude anything.")
            f = MetricFilter(field=f.field, op=flipped, value=f.value)
        elif f.op in ("=", "in") and negated and _negated_value(f.value, question):
            flipped = "!=" if f.op == "=" else "not in"
            notes.append(f"Read {f.field} {f.op} {f.value!r} as {flipped}: the question excludes it.")
            f = MetricFilter(field=f.field, op=flipped, value=f.value)
        kept.append(f)
    return kept


def _negated_value(value: Any, question: str) -> bool:
    """The value comes within three words after a negation ("not cancelled", "excluding the cancelled ones")."""
    q = words(question)
    negs = {"not", "except", "excluding", "exclude", "without", "non", "no"}
    for v in value if isinstance(value, list) else [value]:
        vw = words(v)
        for i, t in enumerate(q):
            if t in negs and _contains(q[i + 1 : i + 4 + len(vw)], vw):
                return True
    return False


def _phrases_of(item: MenuItem) -> dict[str, list[list[str]]]:
    return {d: dimension_phrases(d, item.dimension_words.get(d)) for d in item.dimensions}


def ground_dimensions(dims: list[str], question: str, item: MenuItem, notes: list[str]) -> tuple[list[str], bool]:
    """``(dimensions, partial)``: what the question mentions, plus what it plainly asks for and the model forgot.

    A requested breakdown that matches no allowed dimension is never replaced by another: it is named in the notes
    and the answer is marked partial.
    """
    q = words(question)
    phrases = _phrases_of(item)
    kept = []
    for d in dims:
        if any(_contains(q, p) for p in phrases.get(d, dimension_phrases(d))):
            kept.append(d)
        else:
            notes.append(f"Left out {d!r}: the question does not ask for it.")
    partial = False
    for asked in requested_groups(question):
        aw = words(asked)
        hits = [d for d, ps in phrases.items() if any(_contains(aw, p) for p in ps)]
        if not hits:
            partial = True
            notes.append(f"{asked!r} is not something {item.name} can be grouped by, so the answer is not split by it.")
            continue
        best = max(hits, key=lambda d: max(len(p) for p in phrases[d] if _contains(aw, p)))
        if best not in kept:
            kept.append(best)
            notes.append(f"Added {best!r}: the question asks for it.")
    return kept, partial


def ground_grain(grain: str | None, question: str, item: MenuItem, notes: list[str]) -> str | None:
    asked = asked_grain(question)
    if grain and asked is None:
        notes.append(f"Left out the {grain} grain: the question does not ask over time.")
        return None
    if asked in (None, "any"):
        return grain
    if not item.time_column:
        notes.append(f"{item.name} has no time column, so it cannot be split by {asked}.")
        return None
    if grain != asked:
        notes.append(f"Grouped by {asked}: the question asks for it." if grain is None else f"Read {grain} as {asked}.")
    return asked


def _mentioned(item: MenuItem, q: list[str]) -> bool:
    names = [item.name, *(item.synonyms or [])] + ([item.column] if item.column else [])
    return any(_contains(q, words(n)) for n in names if words(n))


def choose_metric(item: MenuItem, question: str, menu: list[MenuItem], notes: list[str]) -> MenuItem:
    """The model's pick, unless the question plainly asks for a sibling: the same column summed or averaged, a row
    count of the table it asks "how many" of, or the one metric of the same kind whose column it names."""
    q = words(question)
    pick = item
    if not _mentioned(pick, q):
        same = [m for m in menu if m.aggregate == pick.aggregate and m is not pick and _mentioned(m, q)]
        if len(same) == 1:
            notes.append(f"Used {same[0].name} instead of {pick.name}: the question names it.")
            pick = same[0]
    if pick.column:
        want = None
        if pick.aggregate == "avg" and _SUM_WORDS.search(question) and not _AVG_WORDS.search(question):
            want = "sum"
        elif pick.aggregate == "sum" and _AVG_WORDS.search(question):
            want = "avg"
        if want:
            twin = next(
                (m for m in menu if m.aggregate == want and m.column == pick.column and m.table == pick.table), None
            )
            if twin is not None:
                notes.append(f"Used {twin.name} instead of {pick.name}: the question asks for the {_AGG_WORD[want]}.")
                pick = twin
    if pick.aggregate == "count" and _WHICH_MOST.search(question):
        sums = [m for m in menu if m.aggregate == "sum" and m.table == pick.table]
        if not sums:
            # the count's table has no amount: a sum that can be split by the asked dimension
            asked = {w for g in requested_groups(question) for w in words(g)}
            sums = [
                m
                for m in menu
                if m.aggregate == "sum" and any(set(words(d.rpartition(".")[2])) <= asked for d in m.dimensions)
            ]
        money = [m for m in sums if not _VOLUME_COLUMN.search(m.column or "")]
        if _BUSINESS_VERB.search(question) and (money or len(sums) == 1):
            better = (money or sums)[0]
            notes.append(f"Used {better.name} instead of {pick.name}: the question asks about business, not a count.")
            pick = better
    how_many = _how_many_thing(question, menu)
    if how_many:
        thing = _stem(how_many.lower())
        counts = [
            m for m in menu if m.aggregate == "count" and m.table and _stem(m.table.rpartition(".")[2].lower()) == thing
        ]
        if counts and pick not in counts:
            notes.append(f"Used {counts[0].name} instead of {pick.name}: the question asks how many {how_many}.")
            pick = counts[0]
    return pick


_AGG_WORD = {"sum": "total", "avg": "average"}
_WHICH_MOST = re.compile(
    r"\b(?:which|what)\b.*\b(most|top|highest|best|largest|biggest|greatest|leading)\b", re.IGNORECASE
)
_BUSINESS_VERB = re.compile(
    r"\b(buys?|bought|buying|spends?|spent|spending|sells?|sold|selling|sales|earns?|earned|earning|generates?|"
    r"generated|brings? in|brought in|revenue|pays?|paid the)\b",
    re.IGNORECASE,
)
_VOLUME_COLUMN = re.compile(r"(qty|quantity|count|items|units|number)", re.IGNORECASE)


def ground_values(
    filters: list[MetricFilter], question: str, item: MenuItem, model_fields: set[str], notes: list[str]
) -> tuple[list[MetricFilter], bool]:
    """``(filters, partial)`` from the column values the data holds (``item.known_values``), in both directions.

    A known value the question names becomes a filter on its column (``=``; ``!=`` when negated; ``in`` for
    several); a word that is a value of several columns goes to the metric's own table, else to the column the
    model chose, else to none (noted, partial). A model filter whose value is not a known value of its column is
    corrected to the one close match, else dropped (noted, partial unless the question gives that column a value).
    """
    known = item.known_values
    if not known:
        return filters, False
    partial = False
    q = words(question)
    found: dict[str, list[str]] = {}
    by_value: dict[str, list[tuple[str, str]]] = {}
    for fld, values in known.items():
        for v in values:
            vw = words(v)
            if len(vw) == 0 or (len(vw) == 1 and (len(vw[0]) < 3 or vw[0].isdigit())):
                continue
            if _contains(q, vw):
                by_value.setdefault(" ".join(vw), []).append((fld, v))
    for key, hits in by_value.items():
        fields = {f for f, _ in hits}
        if len(fields) > 1:
            own = [h for h in hits if "." not in h[0]]
            chosen = [h for h in hits if h[0] in model_fields]
            pick = own if len({f for f, _ in own}) == 1 else chosen if len({f for f, _ in chosen}) == 1 else []
            if not pick:
                partial = True
                notes.append(
                    f"{key!r} is a value of {', '.join(sorted(fields))}; not filtered, as it is unclear which."
                )
                continue
            hits = pick
        for fld, v in hits:
            found.setdefault(fld, []).append(v)

    kept: list[MetricFilter] = []
    for f in filters:
        values = known.get(f.field)
        if values is None or f.op not in ("=", "!=", "in", "not in"):
            kept.append(f)
            continue
        lower = {v.lower(): v for v in values}
        fixed = []
        for v in f.value if isinstance(f.value, list) else [f.value]:
            if str(v).lower() in lower:
                fixed.append(lower[str(v).lower()])
                continue
            close = difflib.get_close_matches(str(v).lower(), list(lower), n=2, cutoff=0.85)
            if len(close) == 1:
                notes.append(f"Read {f.field} value {v!r} as {lower[close[0]]!r}.")
                fixed.append(lower[close[0]])
            else:
                notes.append(f"Left out {f.field} {f.op} {v!r}: {v!r} is not a value of {f.field}.")
                placed = any(str(v).lower() == x.lower() for vals in found.values() for x in vals)
                if f.field not in found and not placed:
                    partial = True
        if fixed:
            single = f.op in ("=", "!=") and len(fixed) == 1
            kept.append(MetricFilter(field=f.field, op=f.op, value=fixed[0] if single else fixed))

    for fld, vals in found.items():
        neg = [v for v in vals if _negated_value(v, question)]
        pos = [v for v in vals if v not in neg]
        for group, negated in ((pos, False), (neg, True)):
            if not group:
                continue
            op = ("!=" if negated else "=") if len(group) == 1 else ("not in" if negated else "in")
            value: Any = group[0] if len(group) == 1 else sorted(group)
            same = [k for k in kept if k.field == fld and (k.op in ("!=", "not in")) == negated]
            have = {str(x) for k in same for x in (k.value if isinstance(k.value, list) else [k.value])}
            if have == {str(x) for x in group}:
                continue
            kept = [k for k in kept if k not in same]
            kept.append(MetricFilter(field=fld, op=op, value=value))
            notes.append(f"Added {fld} {op} {value!r}: the question names it.")
    return kept, partial


def _how_many_thing(question: str, menu: list[MenuItem]) -> str | None:
    """The thing "how many <things>" counts, past words that are known values ("how many paid orders" → orders)."""
    m = _HOW_MANY.search(question)
    if m is None:
        return None
    values = {words(v)[0] for item in menu for vals in item.known_values.values() for v in vals if len(words(v)) == 1}
    for token in m.group(1).split():
        if _stem(token.lower()) not in values:
            return token
    return m.group(1).split()[0]


def _unknown_count_target(question: str, menu: list[MenuItem]) -> str | None:
    """The thing a "how many <things>" question counts when nothing on the menu names it, else ``None``."""
    target = _how_many_thing(question, menu)
    if target is None:
        return None
    thing = _stem(target.lower())
    for item in menu:
        names = [item.name, *(item.synonyms or []), *(item.dimensions or [])]
        if item.table:
            names.append(item.table.rpartition(".")[2])
        if item.column:
            names.append(item.column)
        if any(thing in words(n) for n in names):
            return None
    return target


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


def ground(ans: AskAnswer, question: str, menu: list[MenuItem]) -> AskAnswer:
    """Keep only what the question supports: the model suggests, this decides. Changes are told in ``notes``."""
    if ans.query is None:
        return ans
    query = ans.query
    item = next(m for m in menu if m.name == query.metric)
    unknown = _unknown_count_target(question, menu)
    if unknown:
        ans.query, ans.no_metric = None, True
        ans.why = f"No approved metric counts {unknown}."
        return ans
    picked = choose_metric(item, question, menu, ans.notes)
    dims = [d for d in query.dimensions if d in picked.dimensions]
    allowed_fields = set(picked.dimensions) | set(picked.filter_fields) | ({picked.time_column} - {None})
    filters = [f for f in query.filters if f.field in allowed_fields or picked is item]
    dims, partial = ground_dimensions(dims, question, picked, ans.notes)
    model_fields = {f.field for f in filters}
    filters = ground_filters(filters, question, picked, ans.notes)
    filters, value_partial = ground_values(filters, question, picked, model_fields, ans.notes)
    partial = partial or value_partial
    filters += _year_filter(filters, question, picked, ans.notes)
    grain = ground_grain(query.grain, question, picked, ans.notes)
    order = query.order
    if order is not None:
        target = picked.name if order.field == item.name else order.field
        out_names = [*dims, picked.name] + ([f"{picked.time_column}_{grain}"] if grain else [])
        order = MetricOrder(field=target, direction=order.direction) if target in out_names else None
    ans.query = MetricQuery(
        metric=picked.name, dimensions=dims, filters=filters, grain=grain, order=order, limit=query.limit
    )
    ans.partial = ans.partial or partial
    line = reading(ans.query)
    if picked.aggregate == "count" and _WHICH_MOST.search(question) and picked.table:
        line += f" Counted {picked.table.rpartition('.')[2]}, not an amount."
    ans.notes.insert(0, line)
    return ans


def _year_filter(filters: list[MetricFilter], question: str, item: MenuItem, notes: list[str]) -> list[MetricFilter]:
    """One stated year ("in 2026") and no date filter: the year on the time column, so the year is never ignored."""
    years = question_years(question)
    if len(years) != 1 or not item.time_column or _RELATIVE_DATE.search(question):
        return []
    if any(_is_date_filter(f, item.time_column) for f in filters):
        return []
    (y,) = years
    notes.append(f"Added {item.time_column} in {y}: the question names the year.")
    return [MetricFilter(field=item.time_column, op="between", value=[f"{y}-01-01", f"{y}-12-31"])]


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
        ans = ground(read_answer(content, menu), question, menu)
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


__all__ = [
    "MAX_MENU",
    "AskAnswer",
    "MenuItem",
    "ask",
    "ground",
    "ground_dimensions",
    "ground_filters",
    "ground_grain",
    "ground_values",
    "choose_metric",
    "menu_text",
    "read_answer",
    "requested_groups",
    "system_prompt",
    "user_message",
]
