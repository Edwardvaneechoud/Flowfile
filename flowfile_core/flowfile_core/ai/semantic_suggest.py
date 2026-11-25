"""A person's request for specific suggestions, backing ``POST /ai/semantic/suggest``.

The model reads the request; code checks only what can be checked against the model and the data. One call sees the
model's tables (a column kept from AI by name and type only, no data rows and no statistics), its joins and its metric
names, and answers a short list in one fixed shape (``prompts/semantic_suggest.md``). Each suggestion is then checked:

* table, column and metric names are matched to the model's, unknown ones refused;
* a metric is compiled and dry-run through the service's proposal path, and refused when the model holds its formula;
* a join's columns are of one type and it is measured: ``reversed`` is swapped and stored the right way round with a
  note, ``many_to_many`` and ``unchecked`` are refused;
* a role fits its column's type, descriptions and synonyms are not empty, and nothing is said about a column kept from
  AI.

Everything refused goes back to the model once; what still fails is returned as skipped. What survives is stored
through the AI-bound view, so it waits for review. Must not import ``litellm`` or ``glosslayer`` at module load.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.semantic_draft import (
    MAX_COLUMNS,
    PRIVATE_MARK,
    call_json,
    match_name,
    repair_turn,
    sentence,
    snake,
    system_prompt,
)
from flowfile_core.catalog.exceptions import SemanticConflictError, SemanticValueError
from flowfile_core.catalog.semantic.activity import listed
from flowfile_core.catalog.semantic.compiler import reachable_tables
from flowfile_core.catalog.semantic.confidentiality import Withheld, read_withheld
from flowfile_core.catalog.semantic.snapshot import ACCEPTED, ModelSnapshot, TableInfo
from flowfile_core.catalog.semantic.suggest import type_class
from flowfile_core.schemas import semantic_schema as s

logger = logging.getLogger(__name__)

MAX_SUGGESTIONS = 8
MAX_METRICS_SHOWN = 30
MAX_JOINS_SHOWN = 20
MAX_SYNONYMS = 8
TABLES_CHARS = 4500
_DESCRIPTION_CHARS = 70

ROLE_LABELS = {"key": "Identifier", "time": "Date", "category": "Group by", "amount": "Amount"}
_KIND_NOUNS = {
    "metric": "metrics",
    "description": "descriptions",
    "join": "joins",
    "role": "column roles",
    "synonyms": "synonyms",
}


def kinds_phrase(kinds: list[str]) -> str:
    return listed([_KIND_NOUNS[k] for k in kinds])


def _type_word(dtype: str | None) -> str:
    tc = type_class(dtype)
    return {"numeric": "decimal", "string": "text", "time": "date"}.get(tc, tc)


def _text(value: Any) -> str | None:
    return " ".join(value.split()) or None if isinstance(value, str) else None


def _norm(text: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _names(value: Any) -> list[str]:
    values = value if isinstance(value, list) else [value]
    return [v for v in values if isinstance(v, str)]


class Refuse(Exception):
    """A suggestion the code cannot store: its label, why, and whether the model may try again."""

    def __init__(self, what: str, why: str, retry: bool = True) -> None:
        self.entry = s.SemanticSuggestionSkipped(what=what, why=why, retry=retry)


@dataclass
class Context:
    """The live model (pending facts included) with the tables of other schemas its joins reach (``others``)."""

    snap: ModelSnapshot
    infos: dict[int, TableInfo]
    withheld: Withheld
    others: list[s.SemanticTableOut]

    @classmethod
    def read(cls, service: Any, namespace_id: int) -> Context:
        snap, scope = service.snapshot(namespace_id)
        by_key = {info.key: info for info in scope.all_tables.values()}
        ends = dict.fromkeys(key for j in snap.joins for key in (j.from_table, j.to_table))
        own = {t.key for t in snap.tables}
        others = [
            s.SemanticTableOut(
                table_id=info.table_id,
                key=info.key,
                name=info.name,
                fields=[s.FieldOut(name=n, dtype=d) for n, d in info.columns],
            )
            for key in ends
            if key not in own and (info := by_key.get(key)) is not None
        ]
        return cls(snap, scope.all_tables, read_withheld(service.db, scope.all_tables), others)

    @property
    def tables(self) -> list[s.SemanticTableOut]:
        return [*self.snap.tables, *self.others]

    @property
    def withheld_columns(self) -> int:
        return sum(len(self.withheld.names(t.table_id, [c.name for c in t.fields])) for t in self.tables)

    def private(self, t: s.SemanticTableOut, column: str | None = None) -> bool:
        return self.withheld.whole_table(t.table_id) if column is None else self.withheld.blocks(t.table_id, column)

    def private_metric(self, m: s.MetricOut) -> bool:
        owner = self.snap.table(m.table)
        hidden = {c.name.lower() for t in self.tables for c in t.fields if self.private(t, c.name)}
        sql = re.sub(r"'(?:[^']|'')*'", " ", f"{m.expression or ''} {m.filter or ''}")
        named = {(q or b).lower() for q, b in re.findall(r'"([^"]+)"|\b([A-Za-z_][A-Za-z0-9_]*)\b', sql)}
        return owner is None or self.private(owner) or bool(named & hidden)

    def table_names(self, tables: list[s.SemanticTableOut] | None = None) -> str:
        return ", ".join(t.name for t in (self.snap.tables if tables is None else tables))

    def find_table(self, name: Any, *, anywhere: bool = False) -> s.SemanticTableOut | None:
        """A table named by its short name or key: the model's own tables first, then the others shown."""
        for pool in (self.snap.tables, self.others if anywhere else []):
            for attr in ("name", "key"):
                found = match_name(name, [getattr(t, attr) for t in pool])
                if found is not None:
                    return next(t for t in pool if getattr(t, attr) == found)
        return None

    def need_table(self, item: dict[str, Any], what: str, hint: str = "use one of") -> s.SemanticTableOut:
        t = self.find_table(item.get("table"))
        if t is None:
            raise Refuse(what, f"Unknown table {item.get('table')!r}; {hint} {self.table_names()}.")
        return t

    def reachable_columns(self, start: str) -> list[str]:
        """``table.column`` of every table the joins reach from ``start``; columns kept from AI left out."""
        by_key = {t.key: t for t in self.tables}
        reached = [by_key[key] for key in reachable_tables(self.snap, start)[1:] if key in by_key]
        return [f"{t.name}.{c.name}" for t in reached for c in t.fields if not self.private(t, c.name)]


def need_column(t: s.SemanticTableOut, raw: Any, what: str, end: str = ".") -> s.FieldOut:
    name = match_name(raw, [c.name for c in t.fields])
    if name is None:
        raise Refuse(what, f"Table {t.name!r} has no column {raw!r}{end}")
    return t.column(name)


def _table_block(ctx: Context, t: s.SemanticTableOut, with_descriptions: bool) -> str:
    lines = [f"## {t.name}" + ("" if t in ctx.snap.tables else " (another schema; only as the target of a join)")]
    if ctx.private(t):
        lines.append(f"(this table is {PRIVATE_MARK})")
    elif t.description:
        lines.append(f"Description: {t.description[:_DESCRIPTION_CHARS * 2]}")
    lines.append("Columns:")
    for c in t.fields[:MAX_COLUMNS]:
        private, role, described = ctx.private(t, c.name), c.value("role"), c.value("description")
        mark = f", {PRIVATE_MARK}" if private else f", used as {role}" if role else ""
        text = f"- {c.name} ({_type_word(c.dtype)}{mark})"
        if with_descriptions and not private and isinstance(described, str) and described:
            text += f": {described[:_DESCRIPTION_CHARS]}"
        lines.append(text)
    if len(t.fields) > MAX_COLUMNS:
        lines.append(f"- … {len(t.fields) - MAX_COLUMNS} more columns not shown")
    return "\n".join(lines)


def user_message(ctx: Context, request: str | None, kinds: list[str] | None) -> str:
    """The request, the tables (the model's own first), the joins and the metric names."""
    with_descriptions = not kinds or "description" in kinds or "synonyms" in kinds
    lines = [f"Request: {request or f'more {kinds_phrase(kinds)} this model does not have yet.'}"]
    if kinds:
        lines.append(f"Suggest {kinds_phrase(kinds)} only.")
    lines += ["", "Tables:"]
    used, left_out = 0, []
    for t in ctx.tables:
        block = _table_block(ctx, t, with_descriptions)
        if used and used + len(block) > TABLES_CHARS:
            left_out.append(t.name)
            continue
        lines.append(block)
        used += len(block)
    if left_out:
        lines.append(f"Other tables (columns not shown): {', '.join(left_out)}")
    lines += [
        "",
        "Joins (from the many side to the one side). A metric may use a column of a table a join points to",
        "as table.column:",
    ]
    names = {t.key: t.name for t in ctx.tables}

    def short(key: str) -> str:
        return names.get(key) or key.rsplit(".", 1)[-1]

    for j in ctx.snap.joins[:MAX_JOINS_SHOWN]:
        columns = zip(j.from_columns, j.to_columns, strict=False)
        pairs = ", ".join(f"{short(j.from_table)}.{a} → {short(j.to_table)}.{b}" for a, b in columns)
        lines.append(f"- {pairs}" + ("" if j.status == ACCEPTED else " (waiting for review)"))
    if not ctx.snap.joins:
        lines.append("- none yet")
    for t in ctx.snap.tables:
        reach = ctx.reachable_columns(t.key)
        if reach:
            lines.append(f"A metric on {t.name} can also use: {', '.join(reach[:MAX_COLUMNS])}")
    lines += ["", "Metrics the model already has (never suggest one of these again):"]
    for m in ctx.snap.metrics[:MAX_METRICS_SHOWN]:
        formula = "" if ctx.private_metric(m) else f": {m.expression or ''}"
        lines.append(f"- {m.name} on {short(m.table)}{formula}")
    if not ctx.snap.metrics:
        lines.append("- none yet")
    lines += ["", 'Answer now with the JSON object {"suggestions": [...]} or {"nothing": true, "why": "..."}.']
    return "\n".join(lines)


@dataclass
class Reading:
    items: list[dict[str, Any]] = field(default_factory=list)
    nothing: bool = False
    why: str | None = None
    problem: str | None = None
    extra: int = 0


def read_suggestions(content: str | None) -> Reading:
    """The answer's suggestions (at most ``MAX_SUGGESTIONS``), or the model's reason it has none."""
    payload, err = parse_json_payload(content, extract_object=True)
    if err:
        return Reading(problem="The answer was not one valid JSON object. Answer with the JSON object only.")
    if isinstance(payload, list):
        payload = {"suggestions": payload}
    if not isinstance(payload, dict):
        return Reading(problem='The answer must be a JSON object {"suggestions": [...]}.')
    raw = payload.get("suggestions")
    if raw is None and _kind_of(payload) is not None:
        raw = payload
    if isinstance(raw, dict):
        raw = [raw]
    items = [x for x in raw if isinstance(x, dict)] if isinstance(raw, list) else []
    if items:
        return Reading(items=items[:MAX_SUGGESTIONS], extra=max(0, len(items) - MAX_SUGGESTIONS))
    if payload.get("nothing") or payload.get("why"):
        return Reading(nothing=True, why=sentence(payload.get("why")))
    return Reading(problem='Give "suggestions" as a list of objects, or answer {"nothing": true, "why": "..."}.')


def _kind_of(item: dict[str, Any]) -> str | None:
    kind = str(item.get("kind")).strip().lower()
    return kind if kind in _KIND_NOUNS else None


def describe_item(item: dict[str, Any]) -> str:
    """A short label for a suggestion, for the refusal list and ``skipped``."""
    kind = _kind_of(item)
    if kind == "metric":
        return f"metric {item.get('name') or item.get('expression') or '?'}"
    if kind == "join":
        return f"join {item.get('from_table') or '?'} → {item.get('to_table') or '?'}"
    target = item.get("table") or "?"
    if item.get("column"):
        target = f"{target}.{item.get('column')}"
    elif kind == "synonyms" and item.get("metric"):
        target = f"metric {item.get('metric')}"
    return f"{kind or 'role'} of {target}"


@dataclass
class Checked:
    """A suggestion that passed the code's checks, ready for the service's proposal path; ``signature`` tells two
    suggestions of one answer apart (``what`` alone when empty)."""

    what: str
    summary: str
    proposal: Any
    table: str | None = None
    name: str | None = None
    note: str | None = None
    signature: tuple = ()


def _aggregate_of(expression: str) -> str | None:
    head = expression.strip().upper()
    if re.match(r"^COUNT\s*\(\s*DISTINCT\b", head):
        return "count_distinct"
    m = re.match(r"^(SUM|AVG|COUNT|MIN|MAX)\s*\(([^()]*)\)$", head)
    return m.group(1).lower() if m else None


def check_metric(item: dict[str, Any], ctx: Context, what: str) -> Checked:
    table = ctx.need_table(item, what, "a metric belongs to one of")
    name = snake(item.get("name"))
    if name is None:
        raise Refuse(what, "A metric needs a name in lower snake_case, such as average_order_value.")
    expression = _text(item.get("expression") or item.get("sql") or item.get("formula"))
    if expression is None:
        raise Refuse(what, "A metric needs an expression, such as SUM(amount).")
    expression = expression.rstrip(";").strip()
    raw_filter, raw_time = item.get("filter"), item.get("time_column")
    if raw_filter is not None and not isinstance(raw_filter, str):
        raise Refuse(what, "A metric's filter is SQL text, such as status = 'paid', or null.")
    filt = _text(raw_filter)
    if isinstance(raw_time, str) and raw_time.strip():
        time_column = need_column(table, raw_time, what, " for the time column.").name
    else:
        found = [c.name for c in table.fields if c.value("role") == "time"]
        found = found or [c.name for c in table.fields if _type_word(c.dtype) == "date"]
        time_column = found[0] if len(found) == 1 else None
    formula = (table.key, _norm(expression), _norm(filt))
    for m in ctx.snap.metrics:
        if (m.table, _norm(m.expression), _norm(m.filter)) == formula:
            raise Refuse(what, f"The model already has this formula as {m.name}.", retry=False)
    proposal = s.MetricProposal(
        kind="metric",
        table_id=table.table_id,
        name=name,
        expression=expression,
        description=sentence(item.get("description")),
        aggregate=_aggregate_of(expression),
        filter=filt,
        time_column=time_column,
    )
    summary = f"Metric {name} on {table.name}: {expression}" + (f" where {filt}" if filt else "")
    return Checked(f"metric {name}", summary, proposal, table.name, name, signature=formula)


def _field_proposal(t: s.SemanticTableOut, column: str, attribute: str, value: Any) -> s.FieldProposal:
    return s.FieldProposal(kind="field", table_id=t.table_id, column=column, attribute=attribute, value=value)


def check_description(item: dict[str, Any], ctx: Context, what: str) -> Checked:
    t = ctx.need_table(item, what, "describe one of")
    text = sentence(item.get("text") or item.get("description"))
    if text is None:
        raise Refuse(what, "A description needs one sentence of plain text.")
    if item.get("column") in (None, ""):
        target, noun, held, private = t.name, "table", t.description, ctx.private(t)
        proposal = s.TableProposal(kind="table", table_id=t.table_id, attribute="description", value=text)
    else:
        col = need_column(t, item["column"], what)
        target, noun, held = f"{t.name}.{col.name}", "column", col.value("description")
        private = ctx.private(t, col.name)
        proposal = _field_proposal(t, col.name, "description", text)
    if private:
        raise Refuse(f"description of {target}", f"This {noun} is kept from AI.", retry=False)
    if isinstance(held, str) and held and _norm(held) == _norm(text):
        raise Refuse(what, f"{target} already has this description.", retry=False)
    label = f"description of {target}"
    return Checked(label, f"Description of {target}: {text}", proposal, t.name, signature=(label, t.key))


def check_role(item: dict[str, Any], ctx: Context, what: str) -> Checked:
    t = ctx.need_table(item, what)
    col = need_column(t, item.get("column"), what)
    role = str(item.get("role") or "").strip().lower()
    if role not in ROLE_LABELS:
        raise Refuse(
            what, 'A role is one of "key" (identifier), "time" (date), "category" (group by) or "amount" (a number).'
        )
    word = _type_word(col.dtype)
    if role == "time" and word != "date":
        raise Refuse(what, f"{col.name} is a {word} column, so it cannot be used as a date.")
    if role == "amount" and word not in ("integer", "decimal"):
        raise Refuse(what, f"{col.name} is a {word} column, so it cannot be used as an amount.")
    label = f"role of {t.name}.{col.name}"
    if col.value("role") == role:
        raise Refuse(label, f"It is already used as {ROLE_LABELS[role]}.", retry=False)
    summary = f"{t.name}.{col.name} used as {ROLE_LABELS[role]}"
    return Checked(label, summary, _field_proposal(t, col.name, "role", role), t.name, signature=(label, t.key))


def check_synonyms(item: dict[str, Any], ctx: Context, what: str) -> Checked:
    """Other words for a column or a metric, proposed on the ``synonyms`` attribute so they wait in Review."""
    words = [w for w in map(_text, _names(item.get("words") or item.get("synonyms"))) if w]
    words = list(dict.fromkeys(words))[:MAX_SYNONYMS]
    if not words:
        raise Refuse(what, 'Synonyms need "words": a list of other names, such as ["revenue", "turnover"].')
    also = f"also called: {', '.join(words)}"
    if item.get("metric") not in (None, ""):
        name = match_name(item["metric"], [m.name for m in ctx.snap.metrics])
        if name is None:
            raise Refuse(what, f"Unknown metric {item['metric']!r}; the model's metrics are listed above.")
        label = f"synonyms of metric {name}"
        if ctx.private_metric(next(m for m in ctx.snap.metrics if m.name == name)):
            raise Refuse(label, "This metric reads columns kept from AI.", retry=False)
        proposal = s.MetricSynonymsProposal(kind="metric_synonyms", name=name, synonyms=words)
        return Checked(label, f"Metric {name} {also}", proposal, name=name)
    t = ctx.need_table(item, what)
    col = need_column(t, item.get("column"), what, "; name a column or a metric.")
    label = f"synonyms of {t.name}.{col.name}"
    if ctx.private(t, col.name):
        raise Refuse(label, "This column is kept from AI.", retry=False)
    proposal = _field_proposal(t, col.name, "synonyms", words)
    return Checked(label, f"{t.name}.{col.name} {also}", proposal, t.name, signature=(label, t.key))


def _join_end(item: dict[str, Any], side: str, ctx: Context, what: str) -> tuple[s.SemanticTableOut, list[s.FieldOut]]:
    table = ctx.find_table(item.get(f"{side}_table"), anywhere=True)
    if table is None:
        names = ctx.table_names(ctx.tables)
        raise Refuse(what, f"Unknown table {item.get(f'{side}_table')!r}; a join connects two of {names}.")
    raw = _names(item.get(f"{side}_columns") or item.get(f"{side}_column"))
    if not raw:
        raise Refuse(what, f"Name the {side} side's columns in {side}_columns.")
    return table, [need_column(table, r, what) for r in raw]


def check_join(item: dict[str, Any], ctx: Context, what: str, measure: Callable[..., Any]) -> Checked:
    """A join measured against the data: ok as asked, reversed swapped, anything else refused."""
    (ft, f_cols), (tt, t_cols) = _join_end(item, "from", ctx, what), _join_end(item, "to", ctx, what)
    if ft.key == tt.key:
        raise Refuse(what, "A join connects two different tables.")
    if len(f_cols) != len(t_cols):
        raise Refuse(what, "A join needs the same number of columns on both sides.")
    if ft in ctx.others and tt in ctx.others:
        raise Refuse(what, f"A join in this model must touch one of its tables: {ctx.table_names()}.")
    for a, b in zip(f_cols, t_cols, strict=True):
        if _type_word(a.dtype) != _type_word(b.dtype):
            raise Refuse(
                what,
                f"{ft.name}.{a.name} is {_type_word(a.dtype)} and {tt.name}.{b.name} is {_type_word(b.dtype)}; "
                "joined columns hold the same kind of key.",
            )
    fc, tc = [c.name for c in f_cols], [c.name for c in t_cols]
    held = [(j.from_table, j.to_table, j.from_columns, j.to_columns) for j in ctx.snap.joins]
    if (ft.key, tt.key, fc, tc) in held or (tt.key, ft.key, tc, fc) in held:
        raise Refuse(f"join {ft.name} → {tt.name}", "The model already has this join.", retry=False)
    found = measure(ctx.infos[ft.table_id], fc, ctx.infos[tt.table_id], tc)
    note = None
    if found.verdict == "reversed":
        ft, fc, tt, tc = tt, tc, ft, fc
        note = f"Swapped the join so it goes from {ft.name} to {tt.name} (many to one)."
    elif found.verdict == "many_to_many":
        raise Refuse(what, found.message)
    elif found.verdict != "ok":
        raise Refuse(what, f"Not stored: {found.message}", retry=False)
    base = name = snake(item.get("name")) or f"{ft.name}_{tt.name}"
    taken, n = {j.name for j in ctx.snap.joins}, 2
    while name in taken:
        name, n = f"{base}_{n}", n + 1
    proposal = s.JoinProposal(
        kind="join",
        name=name,
        from_table_id=ft.table_id,
        to_table_id=tt.table_id,
        from_columns=fc,
        to_columns=tc,
        description=sentence(item.get("description")),
    )
    pairs = ", ".join(f"{ft.name}.{a} → {tt.name}.{b}" for a, b in zip(fc, tc, strict=True))
    summary, signature = f"Join {pairs} (many → one)", (ft.key, tt.key, *fc, *tc)
    return Checked(f"join {ft.name} → {tt.name}", summary, proposal, ft.name, name, note, signature)


_CHECKS = {
    "metric": check_metric,
    "description": check_description,
    "role": check_role,
    "synonyms": check_synonyms,
}


def check_item(item: dict[str, Any], ctx: Context, kinds: list[str] | None, measure: Callable[..., Any]) -> Checked:
    kind, what = _kind_of(item), describe_item(item)
    if kind is None:
        raise Refuse(what, 'Each suggestion needs a "kind": one of "' + '", "'.join(_KIND_NOUNS) + '".')
    if kinds and kind not in kinds:
        raise Refuse(what, f"Only {kinds_phrase(kinds)} were asked for.", retry=False)
    return check_join(item, ctx, what, measure) if kind == "join" else _CHECKS[kind](item, ctx, what)


def store_round(
    service: Any,
    namespace_id: int,
    items: list[dict[str, Any]],
    out: s.SemanticSuggestRequestOut,
    *,
    ai_source: str,
) -> list[s.SemanticSuggestionSkipped]:
    """Check every suggestion against a fresh read of the model, store each survivor on its own; the refused ones.

    Each goes through ``service.propose(..., ai_source=...)``, so it is checked exactly as a person's proposal is; a
    refusal there becomes this suggestion's refusal sentence. A suggestion already stored in an earlier round or twice
    in this answer is dropped without a word.
    """
    ctx = Context.read(service, namespace_id)
    done = {p.what for p in out.proposed}
    refused: list[s.SemanticSuggestionSkipped] = []
    seen: set[tuple] = set()
    for item in items:
        try:
            checked = check_item(item, ctx, out.kinds, service.measure_join)
            key = checked.signature or (checked.what,)
            if checked.what in done or key in seen:
                continue
            seen.add(key)
            comment = "Suggested by AI." + (f" {checked.note}" if checked.note else "")
            try:
                facts = service.propose(namespace_id, [checked.proposal], ai_source=ai_source, comment=comment)
            except (SemanticValueError, SemanticConflictError) as exc:
                raise Refuse(checked.what, _first_line(str(exc))) from None
            if facts and all(f.status == ACCEPTED for f in facts):
                raise Refuse(checked.what, "The model already holds exactly this.", retry=False)
        except Refuse as exc:
            if exc.entry.what not in done:
                refused.append(exc.entry)
            continue
        if checked.note:
            out.notes.append(checked.note)
        groups = {f.group for f in facts if f.group}
        out.proposed.append(
            s.SemanticSuggestionProposed(
                kind=_kind_of(item),
                what=checked.what,
                summary=checked.summary,
                table=checked.table,
                name=checked.name,
                fact_ids=[f.id for f in facts],
                group=groups.pop() if len(groups) == 1 else None,
            )
        )
    return refused


def _first_line(text: str, limit: int = 300) -> str:
    """An engine error's first line, cut: the plan Polars prints after it helps no model and no person."""
    line = text.strip().partition("\n")[0]
    return line if len(line) <= limit else line[: limit - 1] + "…"


def repair_message(refused: list[Any], stored: list[Any], problem: str | None) -> str:
    lines = [problem] if problem else []
    if refused:
        lines.append("These suggestions were refused:")
        lines += [f"- {r.what}: {r.why}" for r in refused]
    if stored:
        lines.append("Already stored (do not repeat): " + "; ".join(p.summary for p in stored) + ".")
    lines += [
        "",
        'Answer again with the whole JSON object {"suggestions": [...]}: fix the refused ones or leave them out. '
        'If nothing fits, answer {"nothing": true, "why": "..."}.',
    ]
    return "\n".join(lines)


async def suggest_for_request(
    provider: Provider,
    ctx: Context,
    out: s.SemanticSuggestRequestOut,
    *,
    user_id: int | None,
    apply: Callable[[list[dict[str, Any]]], Awaitable[list[s.SemanticSuggestionSkipped]]],
) -> None:
    """Ask once, check and store what passes (``apply``), send the refusals back once; the outcome goes into ``out``."""
    messages = [
        Message(role="system", content=system_prompt("semantic_suggest")),
        Message(role="user", content=user_message(ctx, out.request, out.kinds)),
    ]
    first_round: list[s.SemanticSuggestionSkipped] = []
    for attempt in (1, 2):
        out.attempts = attempt
        content, err, msg = await call_json(provider=provider, messages=messages, user_id=user_id)
        if err is not None:
            out.reason = err
            out.notes.append(msg)
            out.skipped += first_round
            return
        reading = read_suggestions(content)
        if reading.extra:
            more = "1 more suggestion" if reading.extra == 1 else f"{reading.extra} more suggestions"
            out.notes.append(f"The model gave {more} than are read at once; left out.")
        refused = await apply(reading.items) if reading.items else []
        retry = [r for r in refused if r.retry]
        if attempt == 2 or not (retry or reading.problem):
            break
        out.skipped += [r for r in refused if not r.retry]
        first_round = retry
        logger.info("semantic suggest: sending back %d refusals", len(retry))
        messages = repair_turn(messages, content, repair_message(retry, out.proposed, reading.problem))
    seen = {p.what for p in out.proposed} | {r.what for r in refused}
    out.skipped += refused + [r for r in first_round if r.what not in seen]
    if reading.nothing and not out.proposed:
        out.nothing_to_suggest, out.why = True, reading.why
    elif reading.problem and not out.proposed and not out.skipped:
        out.reason = "invalid_answer"
