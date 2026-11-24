"""A person's request for specific suggestions, backing ``POST /ai/semantic/suggest``.

The model reads the request; code checks only what can be checked against the model and the data. Sized for
Flowfile's small local model (JSON mode only, no grammar, sampling at 0.7): one call that sees the model's tables
(names, types, roles and short descriptions; a column kept from AI by name and type only, no data rows and no
statistics), its joins and its metric names, and answers a short list of suggestions in one fixed shape
(``prompts/semantic_suggest.md``). ``kinds``, when the caller gives them (a tab's button), scope the call. Each
suggestion is then checked:

* table, column and metric names are matched to the model's (``semantic_draft.match_name``), unknown ones refused;
* a metric is compiled and dry-run through the service's proposal path like a hand-made one, and refused when the
  model already holds its formula and filter;
* a join's columns are of one type and it is measured (``join_check``): ``ok`` is stored with the measurement,
  ``reversed`` is swapped by the code and stored the right way round with a note, ``many_to_many`` and
  ``unchecked`` are refused;
* a role is one of the four stored values on a column whose type can hold it, a description is a non-empty sentence,
  synonyms are a non-empty list of words for a column or a metric, and nothing is said about a column kept from AI.

Everything refused goes back to the model once, as one user turn of the refusal sentences; what still fails is
returned as skipped. What survives is stored through the AI-bound view, so it waits for review. Must not import
``litellm`` or ``glosslayer`` at module load.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.semantic_draft import MAX_COLUMNS, MAX_TOKENS, PRIVATE_MARK, call_json, match_name, sentence
from flowfile_core.ai.semantic_draft import snake as snake_name

logger = logging.getLogger(__name__)

MAX_SUGGESTIONS = 8
MAX_METRICS_SHOWN = 30
MAX_JOINS_SHOWN = 20
TABLES_CHARS = 4500
_DESCRIPTION_CHARS = 70
_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "semantic_suggest.md"

ROLE_VALUES = ("key", "time", "category", "amount")
ROLE_LABELS = {"key": "Identifier", "time": "Date", "category": "Group by", "amount": "Amount"}
KINDS = ("metric", "description", "join", "role", "synonyms")
_KIND_NOUNS = {
    "metric": "metrics",
    "description": "descriptions",
    "join": "joins",
    "role": "column roles",
    "synonyms": "synonyms",
}


def system_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


def kinds_phrase(kinds: list[str] | None) -> str:
    names = [_KIND_NOUNS[k] for k in kinds or [] if k in _KIND_NOUNS]
    if not names:
        return ""
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def _type_word(dtype: str | None) -> str:
    from flowfile_core.catalog.semantic.suggest import type_class

    return type_class(dtype).replace("numeric", "decimal").replace("string", "text").replace("time", "date")


@dataclass
class ColumnView:
    name: str
    dtype: str | None
    role: str | None = None
    description: str | None = None
    private: bool = False


@dataclass
class TableView:
    """One table as the prompt and the checks see it."""

    table_id: int
    key: str
    name: str
    description: str | None
    columns: list[ColumnView]
    private: bool = False
    in_model: bool = True

    def column(self, name: str) -> ColumnView | None:
        return next((c for c in self.columns if c.name == name), None)

    def time_columns(self) -> list[str]:
        roles = [c.name for c in self.columns if c.role == "time"]
        return roles or [c.name for c in self.columns if _type_word(c.dtype) == "date"]


@dataclass
class JoinView:
    from_table: str
    to_table: str
    from_columns: list[str]
    to_columns: list[str]
    status: str
    name: str = ""

    def signature(self) -> tuple:
        return (self.from_table, self.to_table, tuple(self.from_columns), tuple(self.to_columns))


@dataclass
class MetricView:
    name: str
    table: str
    expression: str
    status: str
    filter: str | None = None
    private: bool = False

    def same_formula(self, table_key: str, expression: str, filt: str | None) -> bool:
        return (
            self.table == table_key
            and _norm(self.expression) == _norm(expression)
            and _norm(self.filter or "") == _norm(filt or "")
        )


@dataclass
class Context:
    """The model as the request sees it: tables by key, joins and metrics live (pending included)."""

    tables: list[TableView]
    joins: list[JoinView]
    metrics: list[MetricView]
    withheld_columns: int = 0

    def by_key(self, key: str) -> TableView | None:
        return next((t for t in self.tables if t.key == key), None)

    def find_table(self, name: Any, *, model_only: bool = False) -> TableView | None:
        """A table named by its short name or key: the model's own tables first, then the others shown."""
        pools = [[t for t in self.tables if t.in_model]]
        if not model_only:
            pools.append([t for t in self.tables if not t.in_model])
        for pool in pools:
            short = match_name(name, [t.name for t in pool])
            if short is not None:
                return next(t for t in pool if t.name == short)
            key = match_name(name, [t.key for t in pool])
            if key is not None:
                return next(t for t in pool if t.key == key)
        return None

    def table_names(self, *, model_only: bool = False) -> str:
        return ", ".join(t.name for t in self.tables if t.in_model or not model_only)


def build_context(snap: Any, model_tables: dict[int, Any], all_tables: dict[int, Any], withheld: Any) -> Context:
    """The live snapshot as plain views; a table a join reaches outside the model is shown too (no facts)."""
    tables: list[TableView] = []
    for t in snap.tables:
        private_table = withheld.whole_table(t.table_id)
        cols = []
        for c in t.columns:
            private = withheld.blocks(t.table_id, c.name)
            role = c.facts.get("role")
            desc = c.facts.get("description")
            cols.append(
                ColumnView(
                    c.name,
                    c.dtype,
                    role=role.value if role is not None and isinstance(role.value, str) else None,
                    description=None if private or desc is None or not isinstance(desc.value, str) else desc.value,
                    private=private,
                )
            )
        described = t.facts.get("description")
        desc = described.value if described is not None and isinstance(described.value, str) else t.description
        tables.append(TableView(t.table_id, t.key, t.name, None if private_table else desc, cols, private_table))
    shown = {t.key for t in tables}
    by_key = {info.key: info for info in all_tables.values()}
    for j in snap.joins:
        for key in (j.from_table, j.to_table):
            info = by_key.get(key)
            if info is None or key in shown:
                continue
            shown.add(key)
            private_table = withheld.whole_table(info.table_id)
            cols = [ColumnView(n, d, private=withheld.blocks(info.table_id, n)) for n, d in info.columns]
            tables.append(TableView(info.table_id, info.key, info.name, None, cols, private_table, in_model=False))
    joins = [
        JoinView(j.from_table, j.to_table, list(j.from_columns), list(j.to_columns), j.status, j.name)
        for j in snap.joins
    ]
    metrics = [
        MetricView(
            m.name,
            m.table,
            m.expression or "",
            m.status,
            m.value("filter") if isinstance(m.value("filter"), str) else None,
        )
        for m in snap.metrics
    ]
    for mv in metrics:
        owner = next((t for t in tables if t.key == mv.table), None)
        hidden = {c.name.lower() for t in tables for c in t.columns if c.private}
        mv.private = owner is None or owner.private or bool(_identifiers(f"{mv.expression} {mv.filter or ''}") & hidden)
    count = sum(1 for t in tables for c in t.columns if c.private)
    return Context(tables=tables, joins=joins, metrics=metrics, withheld_columns=count)


def _short(ctx: Context, key: str) -> str:
    t = ctx.by_key(key)
    return t.name if t is not None else key.rsplit(".", 1)[-1]


def _table_block(t: TableView, with_descriptions: bool) -> str:
    where = "" if t.in_model else " (another schema; only as the target of a join)"
    lines = [f"## {t.name}{where}"]
    if t.private:
        lines.append(f"(this table is {PRIVATE_MARK})")
    elif t.description:
        lines.append(f"Description: {t.description[:_DESCRIPTION_CHARS * 2]}")
    lines.append("Columns:")
    for c in t.columns[:MAX_COLUMNS]:
        bits = [_type_word(c.dtype)]
        if c.private:
            bits.append(PRIVATE_MARK)
        elif c.role:
            bits.append(f"used as {c.role}")
        text = f"- {c.name} ({', '.join(bits)})"
        if with_descriptions and c.description and not c.private:
            text += f": {c.description[:_DESCRIPTION_CHARS]}"
        lines.append(text)
    if len(t.columns) > MAX_COLUMNS:
        lines.append(f"- … {len(t.columns) - MAX_COLUMNS} more columns not shown")
    return "\n".join(lines)


def user_message(ctx: Context, request: str | None, kinds: list[str] | None) -> str:
    """The request, the tables (the model's own first), the joins and the metric names."""
    with_descriptions = not kinds or "description" in kinds or "synonyms" in kinds
    if request:
        lines = [f"Request: {request}"]
    else:
        lines = [f"Request: more {kinds_phrase(kinds)} this model does not have yet."]
    if kinds:
        lines.append(f"Suggest {kinds_phrase(kinds)} only.")
    lines.append("")
    ordered = sorted(ctx.tables, key=lambda t: not t.in_model)
    blocks, used, left_out = [], 0, []
    for t in ordered:
        block = _table_block(t, with_descriptions)
        if blocks and used + len(block) > TABLES_CHARS:
            left_out.append(t.name)
            continue
        blocks.append(block)
        used += len(block)
    lines.append("Tables:")
    lines += blocks
    if left_out:
        lines.append(f"Other tables (columns not shown): {', '.join(left_out)}")
    lines.append("")
    lines.append("Joins (from the many side to the one side). A metric may use a column of a table a join points to")
    lines.append("as table.column:")
    shown_joins = ctx.joins[:MAX_JOINS_SHOWN]
    for j in shown_joins:
        pairs = ", ".join(f"{_short(ctx, j.from_table)}.{a} → {_short(ctx, j.to_table)}.{b}" for a, b in
                          zip(j.from_columns, j.to_columns, strict=False))  # fmt: skip
        lines.append(f"- {pairs}" + ("" if j.status == "accepted" else " (waiting for review)"))
    if not shown_joins:
        lines.append("- none yet")
    for t in ctx.tables:
        reach = _reachable_columns(ctx, t.key)
        if t.in_model and reach:
            lines.append(f"A metric on {t.name} can also use: {', '.join(reach[:MAX_COLUMNS])}")
    lines.append("")
    lines.append("Metrics the model already has (never suggest one of these again):")
    for m in ctx.metrics[:MAX_METRICS_SHOWN]:
        formula = "" if m.private else f": {m.expression}"
        lines.append(f"- {m.name} on {_short(ctx, m.table)}{formula}")
    if not ctx.metrics:
        lines.append("- none yet")
    lines.append("")
    lines.append('Answer now with the JSON object {"suggestions": [...]} or {"nothing": true, "why": "..."}.')
    return "\n".join(lines)


def _reachable_columns(ctx: Context, start: str) -> list[str]:
    """``table.column`` of every table the joins reach from ``start``, many to one; columns kept from AI left out."""
    seen, queue, out = {start}, [start], []
    while queue:
        key = queue.pop(0)
        for j in ctx.joins:
            if j.from_table == key and j.to_table not in seen:
                seen.add(j.to_table)
                queue.append(j.to_table)
                t = ctx.by_key(j.to_table)
                if t is not None and not t.private:
                    out += [f"{t.name}.{c.name}" for c in t.columns if not c.private]
    return out


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
        raw = [payload]
    if isinstance(raw, dict):
        raw = [raw]
    items = [x for x in raw if isinstance(x, dict)] if isinstance(raw, list) else []
    if not items:
        if payload.get("nothing") or payload.get("why"):
            return Reading(nothing=True, why=sentence(payload.get("why")))
        return Reading(problem='Give "suggestions" as a list of objects, or answer {"nothing": true, "why": "..."}.')
    return Reading(items=items[:MAX_SUGGESTIONS], extra=max(0, len(items) - MAX_SUGGESTIONS))


def _kind_of(item: dict[str, Any]) -> str | None:
    value = item.get("kind")
    kind = str(value).strip().lower() if value is not None else None
    return kind if kind in KINDS else None


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())
    return text or None


def _columns(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str)]
    return []


@dataclass
class Checked:
    """A suggestion that passed the code's checks, ready for the service's proposal path."""

    kind: str
    what: str
    summary: str
    proposal: Any
    table: str | None = None
    name: str | None = None
    note: str | None = None
    signature: tuple = ()


@dataclass
class Refused:
    what: str
    why: str
    retry: bool = True


def describe_item(item: dict[str, Any]) -> str:
    """A short label for a suggestion, for the refusal list and ``skipped``."""
    kind = _kind_of(item) or "suggestion"
    if kind == "metric":
        return f"metric {item.get('name') or item.get('expression') or '?'}"
    if kind == "join":
        return f"join {item.get('from_table') or '?'} → {item.get('to_table') or '?'}"
    target = item.get("table") or "?"
    if item.get("column"):
        target = f"{target}.{item.get('column')}"
    elif kind == "synonyms" and item.get("metric"):
        target = f"metric {item.get('metric')}"
    return f"{kind if kind in ('description', 'synonyms') else 'role'} of {target}"


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _identifiers(text: str) -> set[str]:
    """Every identifier of a SQL fragment, lower case, quoted ones unquoted; ``t.c`` gives ``t`` and ``c``."""
    stripped = re.sub(r"'(?:[^']|'')*'", " ", text)
    return {(q or b).lower() for q, b in re.findall(r'"([^"]+)"|\b([A-Za-z_][A-Za-z0-9_]*)\b', stripped)}


def _aggregate_of(expression: str) -> str | None:
    head = expression.strip().upper()
    if re.match(r"^COUNT\s*\(\s*DISTINCT\b", head):
        return "count_distinct"
    m = re.match(r"^(SUM|AVG|COUNT|MIN|MAX)\s*\(([^()]*)\)$", head)
    if m is None:
        return None
    return {"SUM": "sum", "AVG": "avg", "COUNT": "count", "MIN": "min", "MAX": "max"}[m.group(1)]


def check_metric(item: dict[str, Any], ctx: Context) -> Checked | Refused:
    from flowfile_core.schemas.semantic_schema import MetricProposal

    what = describe_item(item)
    table = ctx.find_table(item.get("table"), model_only=True)
    if table is None:
        return Refused(
            what, f"Unknown table {item.get('table')!r}; a metric belongs to one of {ctx.table_names(model_only=True)}."
        )
    name = snake_name(item.get("name"))
    if name is None:
        return Refused(what, "A metric needs a name in lower snake_case, such as average_order_value.")
    expression = _text(item.get("expression") or item.get("sql") or item.get("formula"))
    if expression is None:
        return Refused(what, "A metric needs an expression, such as SUM(amount).")
    expression = expression.rstrip(";").strip()
    raw_filter = item.get("filter")
    if raw_filter not in (None, "") and not isinstance(raw_filter, str):
        return Refused(what, "A metric's filter is SQL text, such as status = 'paid', or null.")
    filt = _text(raw_filter)
    time_column = None
    raw_time = item.get("time_column")
    if isinstance(raw_time, str) and raw_time.strip():
        time_column = match_name(raw_time, [c.name for c in table.columns])
        if time_column is None:
            return Refused(what, f"Table {table.name!r} has no column {raw_time!r} for the time column.")
    else:
        found = table.time_columns()
        if len(found) == 1:
            time_column = found[0]
    same = next((m for m in ctx.metrics if m.same_formula(table.key, expression, filt)), None)
    if same is not None:
        return Refused(what, f"The model already has this formula as {same.name}.", retry=False)
    proposal = MetricProposal(
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
    signature = ("metric", table.key, _norm(expression), _norm(filt or ""))
    return Checked("metric", f"metric {name}", summary, proposal, table=table.name, name=name, signature=signature)


def check_description(item: dict[str, Any], ctx: Context) -> Checked | Refused:
    from flowfile_core.schemas.semantic_schema import FieldProposal, TableProposal

    what = describe_item(item)
    table = ctx.find_table(item.get("table"), model_only=True)
    if table is None:
        return Refused(
            what, f"Unknown table {item.get('table')!r}; describe one of {ctx.table_names(model_only=True)}."
        )
    text = sentence(item.get("text") or item.get("description"))
    if text is None:
        return Refused(what, "A description needs one sentence of plain text.")
    raw_column = item.get("column")
    if raw_column not in (None, ""):
        column = match_name(raw_column, [c.name for c in table.columns])
        if column is None:
            return Refused(what, f"Table {table.name!r} has no column {raw_column!r}.")
        col = table.column(column)
        if col.private or table.private:
            return Refused(f"description of {table.name}.{column}", "This column is kept from AI.", retry=False)
        if col.description and _norm(col.description) == _norm(text):
            return Refused(what, f"{table.name}.{column} already has this description.", retry=False)
        proposal = FieldProposal(
            kind="field", table_id=table.table_id, column=column, attribute="description", value=text
        )
        target = f"{table.name}.{column}"
    else:
        if table.private:
            return Refused(f"description of {table.name}", "This table is kept from AI.", retry=False)
        if table.description and _norm(table.description) == _norm(text):
            return Refused(what, f"{table.name} already has this description.", retry=False)
        proposal = TableProposal(kind="table", table_id=table.table_id, attribute="description", value=text)
        target = table.name
    return Checked("description", f"description of {target}", f"Description of {target}: {text}", proposal,
                   table=table.name, signature=("description", table.key, target))  # fmt: skip


def check_role(item: dict[str, Any], ctx: Context) -> Checked | Refused:
    from flowfile_core.schemas.semantic_schema import FieldProposal

    what = describe_item(item)
    table = ctx.find_table(item.get("table"), model_only=True)
    if table is None:
        return Refused(what, f"Unknown table {item.get('table')!r}; use one of {ctx.table_names(model_only=True)}.")
    column = match_name(item.get("column"), [c.name for c in table.columns])
    if column is None:
        return Refused(what, f"Table {table.name!r} has no column {item.get('column')!r}.")
    role = str(item.get("role") or "").strip().lower()
    if role not in ROLE_VALUES:
        return Refused(
            what, 'A role is one of "key" (identifier), "time" (date), "category" (group by) or "amount" (a number).'
        )
    col = table.column(column)
    tw = _type_word(col.dtype)
    if role == "time" and tw != "date":
        return Refused(what, f"{column} is a {tw} column, so it cannot be used as a date.")
    if role == "amount" and tw not in ("integer", "decimal"):
        return Refused(what, f"{column} is a {tw} column, so it cannot be used as an amount.")
    if col.role == role:
        return Refused(f"role of {table.name}.{column}", f"It is already used as {ROLE_LABELS[role]}.", retry=False)
    proposal = FieldProposal(kind="field", table_id=table.table_id, column=column, attribute="role", value=role)
    summary = f"{table.name}.{column} used as {ROLE_LABELS[role]}"
    return Checked("role", f"role of {table.name}.{column}", summary, proposal, table=table.name,
                   signature=("role", table.key, column))  # fmt: skip


def check_join_shape(item: dict[str, Any], ctx: Context) -> tuple[TableView, list[str], TableView, list[str]] | Refused:
    """The join's two ends matched to real tables and columns; the measurement comes after."""
    what = describe_item(item)
    ends = []
    for side in ("from", "to"):
        raw_table = item.get(f"{side}_table")
        table = ctx.find_table(raw_table)
        if table is None:
            return Refused(what, f"Unknown table {raw_table!r}; a join connects two of {ctx.table_names()}.")
        raw_cols = _columns(item.get(f"{side}_columns") or item.get(f"{side}_column"))
        if not raw_cols:
            return Refused(what, f"Name the {side} side's columns in {side}_columns.")
        cols = []
        for raw in raw_cols:
            col = match_name(raw, [c.name for c in table.columns])
            if col is None:
                return Refused(what, f"Table {table.name!r} has no column {raw!r}.")
            cols.append(col)
        ends.append((table, cols))
    (ft, fc), (tt, tc) = ends
    if ft.key == tt.key:
        return Refused(what, "A join connects two different tables.")
    if len(fc) != len(tc):
        return Refused(what, "A join needs the same number of columns on both sides.")
    if not (ft.in_model or tt.in_model):
        return Refused(what, f"A join in this model must touch one of its tables: {ctx.table_names(model_only=True)}.")
    for a, b in zip(fc, tc, strict=True):
        ca, cb = ft.column(a), tt.column(b)
        if _type_word(ca.dtype) != _type_word(cb.dtype):
            return Refused(
                what,
                f"{ft.name}.{a} is {_type_word(ca.dtype)} and {tt.name}.{b} is {_type_word(cb.dtype)}; "
                "joined columns hold the same kind of key.",
            )
    return ft, fc, tt, tc


def _join_signature(ft: TableView, fc: list[str], tt: TableView, tc: list[str]) -> tuple:
    return (ft.key, tt.key, tuple(fc), tuple(tc))


def check_join(item: dict[str, Any], ctx: Context, measure: Callable[..., Any]) -> Checked | Refused:
    """A join measured against the data: ok as asked, reversed swapped, anything else refused."""
    from flowfile_core.catalog.semantic.snapshot import TableInfo
    from flowfile_core.schemas.semantic_schema import JoinProposal

    shape = check_join_shape(item, ctx)
    if isinstance(shape, Refused):
        return shape
    ft, fc, tt, tc = shape
    what = describe_item(item)
    existing = {j.signature() for j in ctx.joins}
    if _join_signature(ft, fc, tt, tc) in existing or _join_signature(tt, tc, ft, fc) in existing:
        return Refused(f"join {ft.name} → {tt.name}", "The model already has this join.", retry=False)

    def info(t: TableView) -> TableInfo:
        return TableInfo(t.table_id, t.key, t.name, None, tuple((c.name, c.dtype) for c in t.columns))

    found = measure(info(ft), fc, info(tt), tc)
    note = None
    if found.verdict == "reversed":
        ft, fc, tt, tc = tt, tc, ft, fc
        note = f"Swapped the join so it goes from {ft.name} to {tt.name} (many to one)."
        if _join_signature(ft, fc, tt, tc) in existing:
            return Refused(f"join {ft.name} → {tt.name}", "The model already has this join (the right way round).",
                           retry=False)  # fmt: skip
    elif found.verdict == "many_to_many":
        return Refused(what, found.message)
    elif found.verdict != "ok":
        return Refused(what, f"Not stored: {found.message}", retry=False)
    if note and _join_signature(ft, fc, tt, tc) in existing:
        return Refused(f"join {ft.name} → {tt.name}", "The model already has this join.", retry=False)
    name = snake_name(item.get("name")) or f"{ft.name}_{tt.name}"
    taken = {j.name for j in ctx.joins}
    base, n = name, 2
    while name in taken:
        name, n = f"{base}_{n}", n + 1
    proposal = JoinProposal(
        kind="join",
        name=name,
        from_table_id=ft.table_id,
        to_table_id=tt.table_id,
        from_columns=fc,
        to_columns=tc,
        description=sentence(item.get("description")),
    )
    pairs = ", ".join(f"{ft.name}.{a} → {tt.name}.{b}" for a, b in zip(fc, tc, strict=True))
    return Checked("join", f"join {ft.name} → {tt.name}", f"Join {pairs} (many → one)", proposal,
                   table=ft.name, name=name, note=note, signature=_join_signature(ft, fc, tt, tc))  # fmt: skip


def check_item(
    item: dict[str, Any], ctx: Context, kinds: list[str] | None, measure: Callable[..., Any]
) -> Checked | Refused:
    kind = _kind_of(item)
    what = describe_item(item)
    if kind is None:
        return Refused(what, 'Each suggestion needs a "kind": one of "' + '", "'.join(KINDS) + '".')
    if kinds and kind not in kinds:
        return Refused(what, f"Only {kinds_phrase(kinds)} were asked for.", retry=False)
    if kind == "metric":
        return check_metric(item, ctx)
    if kind == "description":
        return check_description(item, ctx)
    if kind == "role":
        return check_role(item, ctx)
    if kind == "synonyms":
        return check_synonyms(item, ctx)
    return check_join(item, ctx, measure)


MAX_SYNONYMS = 8


def check_synonyms(item: dict[str, Any], ctx: Context) -> Checked | Refused:
    """Other words for a column or a metric, proposed on the ``synonyms`` attribute so they wait in Review."""
    from flowfile_core.schemas.semantic_schema import FieldProposal, MetricSynonymsProposal

    what = describe_item(item)
    raw = item.get("words") or item.get("synonyms")
    words = list(dict.fromkeys(w for w in (_text(x) for x in _columns(raw)) if w))[:MAX_SYNONYMS]
    if not words:
        return Refused(what, 'Synonyms need "words": a list of other names, such as ["revenue", "turnover"].')
    metric_name = item.get("metric")
    if metric_name not in (None, ""):
        name = match_name(metric_name, [m.name for m in ctx.metrics])
        if name is None:
            return Refused(what, f"Unknown metric {metric_name!r}; the model's metrics are listed above.")
        metric = next(m for m in ctx.metrics if m.name == name)
        if metric.private:
            return Refused(f"synonyms of metric {name}", "This metric reads columns kept from AI.", retry=False)
        proposal = MetricSynonymsProposal(kind="metric_synonyms", name=name, synonyms=words)
        return Checked("synonyms", f"synonyms of metric {name}", f"Metric {name} also called: {', '.join(words)}",
                       proposal, name=name, signature=("synonyms", "metric", name))  # fmt: skip
    table = ctx.find_table(item.get("table"), model_only=True)
    if table is None:
        return Refused(what, f"Unknown table {item.get('table')!r}; use one of {ctx.table_names(model_only=True)}.")
    column = match_name(item.get("column"), [c.name for c in table.columns])
    if column is None:
        return Refused(what, f"Table {table.name!r} has no column {item.get('column')!r}; name a column or a metric.")
    if table.private or table.column(column).private:
        return Refused(f"synonyms of {table.name}.{column}", "This column is kept from AI.", retry=False)
    proposal = FieldProposal(kind="field", table_id=table.table_id, column=column, attribute="synonyms", value=words)
    return Checked("synonyms", f"synonyms of {table.name}.{column}",
                   f"{table.name}.{column} also called: {', '.join(words)}", proposal, table=table.name,
                   signature=("synonyms", table.key, column))  # fmt: skip


@dataclass
class Stored:
    kind: str
    what: str
    summary: str
    table: str | None
    name: str | None
    fact_ids: list[str]
    group: str | None


@dataclass
class RoundResult:
    """One round's outcome: what was stored, what was refused (with whether to send it back), the notes."""

    stored: list[Stored] = field(default_factory=list)
    refused: list[Refused] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class Outcome:
    stored: list[Stored] = field(default_factory=list)
    skipped: list[Refused] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    nothing: bool = False
    why: str | None = None
    reason: str | None = None
    attempts: int = 0


def store_round(
    service: Any,
    namespace_id: int,
    items: list[dict[str, Any]],
    done: set[str],
    kinds: list[str] | None,
    *,
    ai_source: str,
    withheld_for: Callable[[list[int]], Any],
) -> RoundResult:
    """Check every suggestion against a fresh read of the model, then store each survivor on its own.

    Each one goes through ``service.propose(..., ai_source=...)``, so a metric is compiled and dry-run and a join's
    structure checked exactly as a person's proposal is; a refusal there becomes this suggestion's refusal sentence.
    A suggestion already stored in an earlier round (``done``) or twice in this answer is dropped without a word.
    """
    from flowfile_core.catalog.exceptions import SemanticConflictError, SemanticValueError

    snap, model_tables, all_tables = service.suggest_scope(namespace_id)
    ctx = build_context(snap, model_tables, all_tables, withheld_for([t.table_id for t in all_tables.values()]))
    out = RoundResult()
    seen: set[tuple] = set()
    for item in items:
        checked = check_item(item, ctx, kinds, service.measure_join)
        if isinstance(checked, Refused):
            if checked.what not in done:
                out.refused.append(checked)
            continue
        key = checked.signature or (checked.what,)
        if checked.what in done or key in seen:
            continue
        seen.add(key)
        comment = "Suggested by AI." + (f" {checked.note}" if checked.note else "")
        try:
            facts = service.propose(namespace_id, [checked.proposal], ai_source=ai_source, comment=comment)
        except (SemanticValueError, SemanticConflictError) as exc:
            out.refused.append(Refused(checked.what, _first_line(str(exc))))
            continue
        if facts and all(f.status == "accepted" for f in facts):
            out.refused.append(Refused(checked.what, "The model already holds exactly this.", retry=False))
            continue
        if checked.note:
            out.notes.append(checked.note)
        groups = {f.group for f in facts if f.group}
        out.stored.append(
            Stored(
                kind=checked.kind,
                what=checked.what,
                summary=checked.summary,
                table=checked.table,
                name=checked.name,
                fact_ids=[f.id for f in facts],
                group=next(iter(groups)) if len(groups) == 1 else None,
            )
        )
    return out


def _first_line(text: str, limit: int = 300) -> str:
    """An engine error's first line, cut: the plan Polars prints after it helps no model and no person."""
    line = text.strip().splitlines()[0] if text.strip() else text
    return line if len(line) <= limit else line[: limit - 1] + "…"


def repair_message(refused: list[Refused], stored: list[Stored], problem: str | None) -> str:
    lines = []
    if problem:
        lines.append(problem)
    if refused:
        lines.append("These suggestions were refused:")
        lines += [f"- {r.what}: {r.why}" for r in refused]
    if stored:
        lines.append("Already stored (do not repeat): " + "; ".join(s.summary for s in stored) + ".")
    lines.append("")
    lines.append(
        'Answer again with the whole JSON object {"suggestions": [...]}: fix the refused ones or leave them out. '
        'If nothing fits, answer {"nothing": true, "why": "..."}.'
    )
    return "\n".join(lines)


async def suggest_for_request(
    provider: Provider,
    ctx: Context,
    request: str | None,
    kinds: list[str] | None,
    *,
    user_id: int | None,
    apply: Callable[[list[dict[str, Any]], set[str]], Awaitable[RoundResult]],
) -> Outcome:
    """Ask once, check and store what passes (``apply``), send the refusals back once, and report the rest."""
    messages = [
        Message(role="system", content=system_prompt()),
        Message(role="user", content=user_message(ctx, request, kinds)),
    ]
    out = Outcome()
    done: set[str] = set()
    first_round: list[Refused] = []
    for attempt in range(2):
        out.attempts += 1
        content, err, msg = await call_json(provider, messages, user_id=user_id, max_tokens=MAX_TOKENS)
        if err is not None:
            out.reason = err
            out.notes.append(msg or "The AI call failed.")
            out.skipped += first_round
            return out
        reading = read_suggestions(content)
        if reading.extra:
            more = "1 more suggestion" if reading.extra == 1 else f"{reading.extra} more suggestions"
            out.notes.append(f"The model gave {more} than are read at once; left out.")
        refused: list[Refused] = []
        if reading.items:
            result = await apply(reading.items, done)
            out.stored += result.stored
            done |= {s.what for s in result.stored}
            out.notes += result.notes
            refused = result.refused
        retry = [r for r in refused if r.retry]
        final = [r for r in refused if not r.retry]
        if attempt == 0 and (retry or reading.problem is not None):
            out.skipped += final
            first_round = retry
            logger.info("semantic suggest: sending back %d refusals", len(retry))
            messages = [
                *messages,
                Message(role="assistant", content=content or ""),
                Message(role="user", content=repair_message(retry, out.stored, reading.problem)),
            ]
            continue
        seen = done | {r.what for r in refused}
        out.skipped += refused + [r for r in first_round if r.what not in seen]
        if reading.nothing and not out.stored:
            out.nothing, out.why = True, reading.why
        elif reading.problem is not None and not out.stored and not out.skipped:
            out.reason = "invalid_answer"
        return out
    return out  # pragma: no cover


__all__ = [
    "MAX_SUGGESTIONS",
    "ROLE_LABELS",
    "ROLE_VALUES",
    "Checked",
    "Context",
    "Outcome",
    "Reading",
    "Refused",
    "RoundResult",
    "Stored",
    "build_context",
    "check_item",
    "describe_item",
    "kinds_phrase",
    "read_suggestions",
    "repair_message",
    "suggest_for_request",
    "system_prompt",
    "user_message",
]
