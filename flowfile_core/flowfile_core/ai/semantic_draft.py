"""Words for a semantic model's rule candidates, backing ``POST /ai/semantic/draft``, and what the three semantic AI
surfaces share (the JSON call, the repair turn, matching names).

The rules (``catalog/semantic/suggest.py``) decide what the model holds; the model only writes words: one sentence per
table, one per column and a better name and meaning per metric candidate. Sized for Flowfile's small local model (JSON
mode only, no grammar, sampling at 0.7): one small call per table, wider tables split, one repair round. Names are
matched to the ones asked for at the seam, and a call that stays unusable leaves that table with its rule-based
candidates only. Must not import ``litellm`` at module load.
"""

from __future__ import annotations

import difflib
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

from pydantic import Field

from flowfile_core.ai.chart_request import _call
from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.catalog.semantic.suggest import type_class
from flowfile_core.schemas.semantic_schema import SemanticDraftTable

logger = logging.getLogger(__name__)

SURFACE = "semantic"
MAX_COLUMNS = 16
MAX_TOKENS = 800
CALL_TIMEOUT_SECONDS = 60.0
PRIVATE_MARK = "kept private: do not describe"
_MAX_SENTENCE = 240


def system_prompt(name: str) -> str:
    return (Path(__file__).resolve().parent / "prompts" / f"{name}.md").read_text(encoding="utf-8")


call_json = partial(_call, timeout=CALL_TIMEOUT_SECONDS, scheduler=None, surface=SURFACE, max_tokens=MAX_TOKENS)


def repair_turn(messages: list[Message], answer: str | None, problems: str) -> list[Message]:
    """The conversation with the model's own answer and what was wrong with it added."""
    return [*messages, Message(role="assistant", content=answer or ""), Message(role="user", content=problems)]


def snake(name: Any) -> str | None:
    text = re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower()).strip("_")
    return text[:48] if text and text[0].isalpha() else None


def sentence(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("description") or value.get("meaning") or value.get("text")
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())[:_MAX_SENTENCE]
    return text + "." if text and text[-1] not in ".!?" else text or None


def match_name(name: Any, choices: list[str], notes: list[str] | None = None) -> str | None:
    """``name`` as one of ``choices``: exact, then case- and underscore-insensitive, then the closest by ``difflib``."""
    if not isinstance(name, str) or not choices:
        return None
    if name in choices:
        return name

    def fold(text: str) -> str:
        return text.lower().replace("_", "").replace(" ", "")

    folded = {fold(c): c for c in choices}
    if fold(name) in folded:
        return folded[fold(name)]
    close = difflib.get_close_matches(name, choices, n=1, cutoff=0.75)
    if close and notes is not None:
        notes.append(f"{name!r} read as {close[0]!r}")
    return close[0] if close else None


def _as_mapping(value: Any, key_fields: tuple[str, ...]) -> dict[str, Any]:
    """A list of ``{"name": …, …}`` objects turned into the ``{name: …}`` object the prompt asks for."""
    if isinstance(value, dict):
        return value
    out: dict[str, Any] = {}
    for item in value if isinstance(value, list) else ():
        if isinstance(item, dict):
            key = next((item.get(f) for f in key_fields if isinstance(item.get(f), str)), None)
            if key:
                out[key] = item
    return out


@dataclass
class Chunk:
    """One call's worth of a table: its columns (with role and statistics text) and, in the first chunk, metrics."""

    table_id: int
    table: str
    part: int
    parts: int
    rows: int | None
    columns: list[tuple[str, str]]
    metrics: list[tuple[str, str]]
    private: list[str]


@dataclass
class DraftWords:
    description: str | None = None
    columns: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, tuple[str | None, str | None]] = field(default_factory=dict)
    dropped: set[str] = field(default_factory=set)

    def empty(self) -> bool:
        return not (self.description or self.columns or self.metrics)

    def add(self, other: DraftWords) -> None:
        """Take what this one does not have yet."""
        self.description = self.description or other.description
        self.columns = other.columns | self.columns
        self.metrics = other.metrics | self.metrics
        self.dropped |= other.dropped


class TableOutcome(SemanticDraftTable):
    words: Any = Field(default_factory=DraftWords, exclude=True)


def chunks_for(sugg: Any, table_ids: list[int], *, share_values: bool, withheld: Any) -> list[Chunk]:
    """The calls for ``table_ids``: columns described by type and role, values only when ``share_values``.

    A column ``withheld`` keeps from AI is listed by name and type with ``PRIVATE_MARK`` and nothing else.
    """
    roles = {(r.table_id, r.column): r.role for r in sugg.roles}
    out: list[Chunk] = []
    for tid in table_ids:
        ts = sugg.stats.get(tid)
        if ts is None:
            continue
        lines = []
        for c in ts.columns:
            word = type_class(c.dtype).replace("numeric", "decimal").replace("string", "text")
            role = roles.get((tid, c.name))
            if withheld.blocks(tid, c.name):
                text = f"{c.name} ({word}, {PRIVATE_MARK})"
            else:
                text = f"{c.name} ({word}, role {role})" if role else f"{c.name} ({word})"
                if ts.available and c.distinct is not None and role == "category":
                    text += f" {c.distinct} distinct values"
                if share_values and c.min is not None and c.max is not None:
                    text += f" values from {str(c.min)[:24]} to {str(c.max)[:24]}"
            lines.append((c.name, text))
        metrics = [(m.name, m.expression) for m in sugg.metrics if m.table_id == tid]
        groups = [lines[i : i + MAX_COLUMNS] for i in range(0, len(lines), MAX_COLUMNS)] or [[]]
        for i, group in enumerate(groups):
            private = withheld.names(tid, [n for n, _ in group])
            first = metrics if i == 0 else []
            out.append(Chunk(tid, ts.table.name, i + 1, len(groups), ts.row_count, group, first, private))
    return out


def user_message(chunk: Chunk) -> str:
    part = f"part {chunk.part} of {chunk.parts}, " if chunk.parts > 1 else ""
    rows = f"{chunk.rows} rows" if chunk.rows is not None else "row count unknown"
    lines = [f"Table: {chunk.table} ({part}{rows})", "Columns:"]
    lines += [f"- {text}" for _name, text in chunk.columns]
    lines.append("Metric ids:" if chunk.metrics else "Metric ids: none")
    lines += [f"- {name}: {expr}" for name, expr in chunk.metrics]
    lines += ["", 'Answer now with the JSON object {"table": ..., "columns": {...}, "metrics": {...}}.']
    return "\n".join(lines)


def read_words(content: str | None, chunk: Chunk) -> tuple[DraftWords, list[str]]:
    """The usable words of one answer and the problems to send back (empty when the answer is good enough)."""
    words = DraftWords()
    payload, err = parse_json_payload(content, extract_object=True)
    if err:
        return words, ["The answer was not one valid JSON object. Answer with the JSON object only."]
    if not isinstance(payload, dict):
        return words, ['The answer must be a JSON object {"table": ..., "columns": {...}, "metrics": {...}}.']
    problems: list[str] = []
    words.description = sentence(payload.get("table") or payload.get("description"))
    if words.description is None:
        problems.append('"table" must be one sentence saying what one row of the table is.')
    names = [n for n, _ in chunk.columns]
    for key, value in _as_mapping(payload.get("columns"), ("name", "column")).items():
        col, text = match_name(key, names), sentence(value)
        if col and text and col in chunk.private:
            words.dropped.add(col)
        elif col and text:
            words.columns[col] = text
    missing = [n for n in names if n not in words.columns and n not in chunk.private]
    if missing:
        problems.append("Describe these columns too: " + ", ".join(missing) + ".")
    ids = [n for n, _ in chunk.metrics]
    for key, value in _as_mapping(payload.get("metrics"), ("id", "metric", "name")).items():
        metric = match_name(key, ids)
        if isinstance(value, str):
            value = {"meaning": value}
        if metric is not None and isinstance(value, dict):
            meaning = sentence(value.get("meaning") or value.get("description"))
            words.metrics[metric] = (snake(value.get("name")), meaning)
    missing = [m for m in ids if m not in words.metrics]
    if missing:
        problems.append("Name and explain these metric ids too: " + ", ".join(missing) + ".")
    return words, problems


async def draft_chunk(
    provider: Provider, chunk: Chunk, *, user_id: int | None
) -> tuple[DraftWords, int, str | None, str | None]:
    """One table part: ``(words, calls, reason, message)``; a reason means the part fell back (partly or wholly)."""
    messages = [
        Message(role="system", content=system_prompt("semantic_draft")),
        Message(role="user", content=user_message(chunk)),
    ]
    best = DraftWords()
    for calls in (1, 2):
        content, err, msg = await call_json(provider=provider, messages=messages, user_id=user_id)
        if err is not None:
            return best, calls, err, msg
        words, problems = read_words(content, chunk)
        best.add(words)
        if not problems:
            return best, calls, None, None
        messages = repair_turn(
            messages,
            content,
            "\n".join(problems) + "\n\nAnswer again with the whole JSON object, these problems fixed.",
        )
    return best, calls, "invalid_answer" if best.empty() else "partial_answer", " ".join(problems)


async def draft_words(
    provider: Provider,
    sugg: Any,
    table_ids: list[int],
    *,
    user_id: int | None,
    share_values: bool,
    withheld: Any,
    progress: Callable[[int, int], None],
) -> list[TableOutcome]:
    """Every table's words, one table at a time (the local model serves one request at a time anyway).

    A table ``withheld`` keeps from AI as a whole is not sent; a column it keeps is sent by name and type, and any
    description the model writes for it is dropped here. ``progress(done, total)`` is called as each table starts.
    """
    outcomes: dict[int, TableOutcome] = {}
    chunks = chunks_for(sugg, table_ids, share_values=share_values, withheld=withheld)
    total = len({c.table_id for c in chunks})
    for chunk in chunks:
        if chunk.table_id not in outcomes:
            progress(len(outcomes) + 1, total)
            outcomes[chunk.table_id] = TableOutcome(table_id=chunk.table_id, table=chunk.table)
        out = outcomes[chunk.table_id]
        out.withheld_columns += chunk.private
        if withheld.whole_table(chunk.table_id):
            continue
        words, calls, reason, message = await draft_chunk(provider, chunk, user_id=user_id)
        out.calls += calls
        out.words.add(words)
        if reason and out.reason is None:
            out.reason, out.message = reason, message
    for out in outcomes.values():
        if withheld.whole_table(out.table_id):
            out.ok, out.reason = True, "kept_private"
            out.message = (
                "This table is kept from AI: it was not sent to the model and keeps its rule-based candidates only."
            )
            continue
        out.ok = not out.words.empty()
        if not out.ok and out.reason is None:
            out.reason = "invalid_answer"
        if out.withheld_columns:
            note = f"Kept from AI (sent by name and type only): {', '.join(out.withheld_columns)}."
            if out.words.dropped:
                dropped = ", ".join(sorted(out.words.dropped))
                note += f" The model described {dropped} anyway; that was dropped, not stored."
            out.message = f"{out.message} {note}" if out.message else note
    return list(outcomes.values())


def words_for_store(outcomes: list[TableOutcome]) -> dict[int, dict[str, Any]]:
    """The outcomes as ``SemanticModelService.store_suggestions(words=)`` reads them."""
    return {
        o.table_id: {"description": o.words.description, "columns": o.words.columns, "metrics": o.words.metrics}
        for o in outcomes
        if not o.words.empty()
    }
