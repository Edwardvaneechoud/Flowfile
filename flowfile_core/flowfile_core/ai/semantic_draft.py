"""Words for a semantic model's rule candidates, backing ``POST /ai/semantic/draft``.

The rules (``catalog/semantic/suggest.py``) decide what the model holds; the model only writes words: one sentence
per table, one per column and a better name and meaning per metric candidate. Sized for Flowfile's small local
model (JSON mode only, no grammar, sampling at 0.7): one small call per table, at most ``MAX_COLUMNS`` columns per
call (wider tables are split), ``max_tokens`` 800, one repair round. The answer is read with the shared tolerant
parser and repaired at the seam: column and metric ids are matched to the ones asked for with ``difflib`` (case,
a dropped underscore), a list where an object was asked for is turned into one, names are made snake_case, and what
cannot be matched is dropped. A call that fails, or still has no usable words after the repair, leaves that table
with its rule-based candidates only. Must not import ``litellm`` at module load.
"""

from __future__ import annotations

import asyncio
import difflib
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowfile_core.ai.json_payload import parse_json_payload
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler

logger = logging.getLogger(__name__)

SURFACE = "semantic"
MAX_COLUMNS = 16
MAX_TOKENS = 800
RETRY_ROUNDS = 1
CALL_TIMEOUT_SECONDS = 60.0
_MAX_SENTENCE = 240
_NAME = re.compile(r"[^a-z0-9]+")
_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "semantic_draft.md"


def system_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


async def call_json(
    provider: Provider,
    messages: list[Message],
    *,
    user_id: int | None,
    timeout: float = CALL_TIMEOUT_SECONDS,
    scheduler: RateLimitScheduler | None = None,
    max_tokens: int = MAX_TOKENS,
) -> tuple[str | None, str | None, str | None]:
    """One JSON-mode call under a rate-limit slot: ``(content, reason, message)``; never raises."""
    sched = scheduler or default_scheduler()

    async def _do() -> str | None:
        async with sched.acquire(provider.name, surface=SURFACE):
            response = await provider.chat(
                messages=messages,
                tools=None,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
                surface=SURFACE,
                user_id=user_id,
            )
        return response.content

    try:
        return await asyncio.wait_for(_do(), timeout=timeout), None, None
    except TimeoutError:
        return None, "timeout", "The model did not answer in time."
    except Exception as exc:  # noqa: BLE001
        logger.warning("semantic provider call failed: %s", exc, exc_info=False)
        return None, "provider_error", f"The AI provider call failed ({type(exc).__name__})."


def snake(name: Any) -> str | None:
    text = _NAME.sub("_", str(name or "").strip().lower()).strip("_")
    if not text or not text[0].isalpha():
        return None
    return text[:48]


def sentence(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("description") or value.get("meaning") or value.get("text")
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())[:_MAX_SENTENCE]
    if not text:
        return None
    return text if text[-1] in ".!?" else text + "."


def match_name(name: Any, choices: list[str], notes: list[str] | None = None) -> str | None:
    """``name`` as one of ``choices``: exact, then case- and underscore-insensitive, then the closest by ``difflib``."""
    if not isinstance(name, str) or not choices:
        return None
    if name in choices:
        return name
    folded = {c.lower().replace("_", "").replace(" ", ""): c for c in choices}
    key = name.lower().replace("_", "").replace(" ", "")
    if key in folded:
        return folded[key]
    close = difflib.get_close_matches(name, choices, n=1, cutoff=0.75)
    if close and notes is not None:
        notes.append(f"{name!r} read as {close[0]!r}")
    return close[0] if close else None


def _as_mapping(value: Any, key_fields: tuple[str, ...]) -> dict[str, Any]:
    """A list of ``{"name": …, …}`` objects turned into the ``{name: …}`` object the prompt asks for."""
    if isinstance(value, dict):
        return value
    out: dict[str, Any] = {}
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                k = next((item.get(f) for f in key_fields if isinstance(item.get(f), str)), None)
                if k:
                    out[k] = item
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
    private: tuple[str, ...] = ()
    private_table: bool = False


@dataclass
class DraftWords:
    description: str | None = None
    columns: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, tuple[str | None, str | None]] = field(default_factory=dict)
    dropped: set[str] = field(default_factory=set)

    def empty(self) -> bool:
        return not (self.description or self.columns or self.metrics)


PRIVATE_MARK = "kept private: do not describe"


def chunks_for(sugg: Any, table_ids: list[int], *, share_values: bool, withheld: Any = None) -> list[Chunk]:
    """The calls for ``table_ids``: columns described by type and role, values only when ``share_values``.

    A column ``withheld`` keeps from AI (``catalog/semantic/confidentiality.Withheld``) is listed by name and type
    with ``PRIVATE_MARK`` and nothing else: no role, count or value.
    """
    from flowfile_core.catalog.semantic.suggest import type_class

    roles = {(r.table_id, r.column): r.role for r in sugg.roles}
    out: list[Chunk] = []
    for tid in table_ids:
        ts = sugg.stats.get(tid)
        if ts is None:
            continue
        lines = []
        private: list[str] = []
        for c in ts.columns:
            bits = [type_class(c.dtype).replace("numeric", "decimal").replace("string", "text")]
            if withheld is not None and withheld.blocks(tid, c.name):
                private.append(c.name)
                lines.append((c.name, f"{c.name} ({bits[0]}, {PRIVATE_MARK})"))
                continue
            role = roles.get((tid, c.name))
            if role:
                bits.append(f"role {role}")
            text = f"{c.name} ({', '.join(bits)})"
            if ts.available and c.distinct is not None and role == "category":
                text += f" {c.distinct} distinct values"
            if share_values and c.min is not None and c.max is not None:
                text += f" values from {str(c.min)[:24]} to {str(c.max)[:24]}"
            lines.append((c.name, text))
        metrics = [(m.name, m.expression) for m in sugg.metrics if m.table_id == tid]
        groups = [lines[i : i + MAX_COLUMNS] for i in range(0, len(lines), MAX_COLUMNS)] or [[]]
        for i, group in enumerate(groups):
            names = {n for n, _ in group}
            out.append(
                Chunk(
                    tid,
                    ts.table.name,
                    i + 1,
                    len(groups),
                    ts.row_count,
                    group,
                    metrics if i == 0 else [],
                    private=tuple(n for n in private if n in names),
                    private_table=withheld is not None and withheld.whole_table(tid),
                )
            )
    return out


def user_message(chunk: Chunk) -> str:
    part = f"part {chunk.part} of {chunk.parts}, " if chunk.parts > 1 else ""
    rows = f"{chunk.rows} rows" if chunk.rows is not None else "row count unknown"
    lines = [f"Table: {chunk.table} ({part}{rows})", "Columns:"]
    lines += [f"- {text}" for _name, text in chunk.columns]
    if chunk.metrics:
        lines.append("Metric ids:")
        lines += [f"- {name}: {expr}" for name, expr in chunk.metrics]
    else:
        lines.append("Metric ids: none")
    lines.append("")
    lines.append('Answer now with the JSON object {"table": ..., "columns": {...}, "metrics": {...}}.')
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
        col = match_name(key, names)
        text = sentence(value)
        if col and text:
            if col in chunk.private:
                words.dropped.add(col)
                continue
            words.columns[col] = text
    missing = [n for n in names if n not in words.columns and n not in chunk.private]
    if missing:
        problems.append("Describe these columns too: " + ", ".join(missing) + ".")
    ids = [n for n, _ in chunk.metrics]
    for key, value in _as_mapping(payload.get("metrics"), ("id", "metric", "name")).items():
        mid = match_name(key, ids)
        if mid is None:
            continue
        if isinstance(value, str):
            value = {"meaning": value}
        if not isinstance(value, dict):
            continue
        words.metrics[mid] = (snake(value.get("name")), sentence(value.get("meaning") or value.get("description")))
    missing_m = [m for m in ids if m not in words.metrics]
    if missing_m:
        problems.append("Name and explain these metric ids too: " + ", ".join(missing_m) + ".")
    return words, problems


def _merge(into: DraftWords, words: DraftWords) -> None:
    into.description = into.description or words.description
    for k, v in words.columns.items():
        into.columns.setdefault(k, v)
    for k, v in words.metrics.items():
        into.metrics.setdefault(k, v)
    into.dropped |= words.dropped


@dataclass
class TableOutcome:
    table_id: int
    table: str
    calls: int = 0
    ok: bool = False
    reason: str | None = None
    message: str | None = None
    words: DraftWords = field(default_factory=DraftWords)
    withheld: list[str] = field(default_factory=list)


async def draft_chunk(
    provider: Provider, chunk: Chunk, *, user_id: int | None
) -> tuple[DraftWords, int, str | None, str | None]:
    """One table part: ``(words, calls, reason, message)``; a reason means the part fell back (partly or wholly)."""
    messages = [Message(role="system", content=system_prompt()), Message(role="user", content=user_message(chunk))]
    best = DraftWords()
    calls = 0
    for attempt in range(RETRY_ROUNDS + 1):
        calls += 1
        content, err, msg = await call_json(provider, messages, user_id=user_id)
        if err is not None:
            return best, calls, err, msg
        words, problems = read_words(content, chunk)
        _merge(best, words)
        if not problems:
            return best, calls, None, None
        if attempt == RETRY_ROUNDS:
            reason = "invalid_answer" if best.empty() else "partial_answer"
            return best, calls, reason, " ".join(problems)
        logger.info("semantic draft answer for %s refused, retrying: %s", chunk.table, problems)
        messages = [
            *messages,
            Message(role="assistant", content=content or ""),
            Message(
                role="user",
                content="\n".join(problems) + "\n\nAnswer again with the whole JSON object, these problems fixed.",
            ),
        ]
    raise AssertionError("unreachable")  # pragma: no cover


def _private_note(out: TableOutcome, private_table: bool) -> str:
    if private_table:
        return "This table is kept from AI: it was not sent to the model and keeps its rule-based candidates only."
    note = f"Kept from AI (sent by name and type only): {', '.join(out.withheld)}."
    if out.words.dropped:
        note += f" The model described {', '.join(sorted(out.words.dropped))} anyway; that was dropped, not stored."
    return note


async def draft_words(
    provider: Provider,
    sugg: Any,
    table_ids: list[int],
    *,
    user_id: int | None,
    share_values: bool,
    withheld: Any = None,
) -> list[TableOutcome]:
    """Every table's words, one table at a time (the local model serves one request at a time anyway).

    A table ``withheld`` keeps from AI as a whole is not sent; a column it keeps is sent by name and type, and any
    description the model writes for it is dropped here, so none is ever stored.
    """
    started = time.monotonic()
    outcomes: dict[int, TableOutcome] = {}
    private_tables: set[int] = set()
    for chunk in chunks_for(sugg, table_ids, share_values=share_values, withheld=withheld):
        out = outcomes.setdefault(chunk.table_id, TableOutcome(chunk.table_id, chunk.table))
        out.withheld += [n for n in chunk.private if n not in out.withheld]
        if chunk.private_table:
            private_tables.add(chunk.table_id)
            continue
        words, calls, reason, message = await draft_chunk(provider, chunk, user_id=user_id)
        out.calls += calls
        _merge(out.words, words)
        if reason and out.reason is None:
            out.reason, out.message = reason, message
    for out in outcomes.values():
        for col in out.withheld:
            out.words.columns.pop(col, None)
        if out.table_id in private_tables:
            out.ok, out.reason, out.message = True, "kept_private", _private_note(out, True)
            continue
        out.ok = not out.words.empty()
        if out.words.empty() and out.reason is None:
            out.reason = "invalid_answer"
        if out.withheld:
            out.message = " ".join(m for m in (out.message, _private_note(out, False)) if m)
    logger.info("semantic draft: %d tables in %.1fs", len(outcomes), time.monotonic() - started)
    return list(outcomes.values())


def words_for_store(outcomes: list[TableOutcome]) -> dict[int, dict[str, Any]]:
    """The outcomes as ``SemanticModelService.store_suggestions(words=)`` reads them."""
    return {
        o.table_id: {
            "description": o.words.description,
            "columns": {c: d for c, d in o.words.columns.items() if c not in o.withheld},
            "metrics": o.words.metrics,
        }
        for o in outcomes
        if not o.words.empty() and o.reason != "kept_private"
    }


__all__ = [
    "MAX_COLUMNS",
    "MAX_TOKENS",
    "PRIVATE_MARK",
    "SURFACE",
    "Chunk",
    "DraftWords",
    "TableOutcome",
    "call_json",
    "chunks_for",
    "draft_words",
    "match_name",
    "read_words",
    "snake",
    "system_prompt",
    "user_message",
    "words_for_store",
]
