"""Per-call LLM prompt log — the dev-mode "what did the model actually see?" hatch.

Sits under :func:`is_logging_enabled` so production stays silent by
default; dev / debug runs flip ``FLOWFILE_AI_LOG_PROMPTS=true`` in
the env and get a daily-rotated JSONL file with one line per LLM
round-trip.

Design notes:

* **One file per day, one line per call.**
  ``{base}/ai_prompts/YYYY-MM-DD.jsonl`` where ``{base}`` is the
  same dir that owns ``master_key.txt`` / ``temp/`` / ``system_logs/``
  (``shared.storage_config.storage.base_directory``).
  ``FLOWFILE_USER_DATA_DIR`` resolves to ``Path.home()`` in local
  mode and writing transcripts to a user's HOME would be intrusive
  — ``base_directory`` is the precedent and the right granularity.
* **Single seam.** All six per-vendor providers share the
  ``LiteLLMProvider`` base, so wrapping ``chat`` / ``stream`` there
  is enough. The wrapper builds a :class:`PromptLogEntry` and calls
  :func:`log_prompt`.
* **Streaming aggregation.** One line per LLM call (not per chunk).
  The wrapper accumulates content + tool-call deltas as the iterator
  yields and emits the entry at stream-end. If the stream errors
  mid-flight the entry still lands (with whatever was accumulated)
  and ``error`` is set.
* **Truncation.** Individual entries past 256 KiB get the older
  user/assistant message bodies replaced with
  ``[...truncated, len=N chars]`` markers, preserving the system
  prompt + most-recent ``KEEP_RECENT_TURNS`` messages. Keeps each
  line parseable by ``jq`` regardless of agent-loop depth.
* **Optional PII scrub.** ``FLOWFILE_AI_LOG_PROMPTS_SCRUB=true``
  runs user / tool message bodies through the safety regex scrubber
  before writing. System and assistant content are left untouched —
  those are the things you actually need to debug. Off by default;
  the whole point of the log is to see what the model saw.
* **Failure isolation.** A logging error never crashes the LLM call.
  The wrapper swallows + warns on file-IO / serialisation failures.

Lazy-litellm contract: this module imports nothing from ``litellm``
at module level. The ``safety`` import is regex-only and pulls
neither litellm nor presidio.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flowfile_core.ai.providers.base import (
    ChatResponse,
    Message,
    ToolCall,
    ToolSpec,
    Usage,
)

logger = logging.getLogger(__name__)

#: Soft cap on the JSON-serialised ``messages`` payload per entry. Picked so a
#: typical agent run with a moderate history fits unscathed but a runaway loop
#: doesn't bloat a single line beyond what ``jq`` / line-readers cope with.
MAX_MESSAGES_BYTES: int = 256 * 1024

#: Number of most-recent non-system messages preserved in full when truncation
#: kicks in. The system prompt is always preserved.
KEEP_RECENT_TURNS: int = 5

#: Subdirectory (under ``storage.base_directory``) that holds the daily files.
LOG_SUBDIR: str = "ai_prompts"


# ---------------------------------------------------------------------------
# Entry shape
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PromptLogEntry:
    """A single LLM round-trip captured for offline inspection.

    Built by the wrapper in ``providers/_litellm_base.py`` from the call's
    inputs + outputs. Kept dataclass-not-Pydantic to avoid pulling validation
    into a path that runs on every LLM call.
    """

    timestamp: str
    provider: str
    model: str
    surface: str | None = None
    user_id: int | None = None
    session_id: str | None = None
    tool_call_id: str | None = None
    messages: list[dict[str, Any]] = field(default_factory=list)
    tools: list[dict[str, Any]] | None = None
    max_tokens: int | None = None
    temperature: float | None = None
    latency_ms: int | None = None
    response: str | None = None
    response_tool_calls: list[dict[str, Any]] = field(default_factory=list)
    finish_reason: str | None = None
    usage: dict[str, int] | None = None
    error: str | None = None
    truncated: bool = False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def is_logging_enabled() -> bool:
    """Live read of ``FLOWFILE_AI_LOG_PROMPTS``.

    Reads via ``getattr`` against the settings module so test fixtures
    and the admin path can flip the underlying ``MutableBool`` without
    re-importing this module. Mirrors ``feature_flag.is_ai_enabled``.
    """
    from flowfile_core.configs import settings as _settings

    return bool(getattr(_settings, "FLOWFILE_AI_LOG_PROMPTS", False))


def is_scrubbing_enabled() -> bool:
    """Live read of ``FLOWFILE_AI_LOG_PROMPTS_SCRUB``."""
    from flowfile_core.configs import settings as _settings

    return bool(getattr(_settings, "FLOWFILE_AI_LOG_PROMPTS_SCRUB", False))


def build_entry_from_chat(
    *,
    provider: str,
    model: str,
    messages: list[Message],
    tools: list[ToolSpec] | None,
    max_tokens: int | None,
    response: ChatResponse | None,
    latency_ms: int,
    error: str | None,
    surface: str | None = None,
    session_id: str | None = None,
    user_id: int | None = None,
    tool_call_id: str | None = None,
    temperature: float | None = None,
) -> PromptLogEntry:
    """Build an entry for a non-streaming :meth:`Provider.chat` call."""
    serialised_messages = [_message_to_dict(m) for m in messages]
    serialised_tools = [_tool_to_dict(t) for t in tools] if tools else None
    response_text = response.content if response is not None else None
    response_tool_calls = [_tool_call_to_dict(tc) for tc in response.tool_calls] if response is not None else []
    finish_reason = response.finish_reason if response is not None else None
    usage = _usage_to_dict(response.usage) if response is not None else None
    return PromptLogEntry(
        timestamp=_now_iso(),
        provider=provider,
        model=model,
        surface=surface,
        user_id=user_id,
        session_id=session_id,
        tool_call_id=tool_call_id,
        messages=serialised_messages,
        tools=serialised_tools,
        max_tokens=max_tokens,
        temperature=temperature,
        latency_ms=latency_ms,
        response=response_text,
        response_tool_calls=response_tool_calls,
        finish_reason=finish_reason,
        usage=usage,
        error=error,
    )


def build_entry_from_stream(
    *,
    provider: str,
    model: str,
    messages: list[Message],
    tools: list[ToolSpec] | None,
    max_tokens: int | None,
    response_text: str | None,
    response_tool_calls: list[ToolCall],
    finish_reason: str | None,
    latency_ms: int,
    error: str | None,
    surface: str | None = None,
    session_id: str | None = None,
    user_id: int | None = None,
    tool_call_id: str | None = None,
    temperature: float | None = None,
) -> PromptLogEntry:
    """Build an entry for a streaming :meth:`Provider.stream` call.

    Streaming responses don't carry a ``Usage`` object inline today;
    tokens will be backfilled by the metric layer once that lands.
    ``usage`` stays ``None`` here.
    """
    serialised_messages = [_message_to_dict(m) for m in messages]
    serialised_tools = [_tool_to_dict(t) for t in tools] if tools else None
    return PromptLogEntry(
        timestamp=_now_iso(),
        provider=provider,
        model=model,
        surface=surface,
        user_id=user_id,
        session_id=session_id,
        tool_call_id=tool_call_id,
        messages=serialised_messages,
        tools=serialised_tools,
        max_tokens=max_tokens,
        temperature=temperature,
        latency_ms=latency_ms,
        response=response_text,
        response_tool_calls=[_tool_call_to_dict(tc) for tc in response_tool_calls],
        finish_reason=finish_reason,
        usage=None,
        error=error,
    )


def log_prompt(entry: PromptLogEntry) -> None:
    """Append ``entry`` to today's JSONL file.

    Idempotent on caller error: file-IO / serialisation failures are logged at
    WARNING and swallowed. The LLM call must never crash because the logger
    couldn't write.
    """
    try:
        prepared = _prepare_entry(entry)
        path = _log_path_for(_now_utc().date().isoformat())
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(asdict(prepared), default=str, ensure_ascii=False)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)
            fh.write("\n")
    except Exception:  # noqa: BLE001 — dev hatch must never crash the LLM path
        logger.warning("prompt_log: failed to write entry", exc_info=True)


def tail(n: int = 10, *, date: str | None = None) -> list[PromptLogEntry]:
    """Read the last ``n`` entries from the day's JSONL file.

    ``date`` is ``YYYY-MM-DD``; defaults to today (UTC). Returns empty when
    the file doesn't exist. Useful from the REPL or a one-shot script when
    diagnosing the most recent agent failure.
    """
    target = date or _now_utc().date().isoformat()
    path = _log_path_for(target)
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[PromptLogEntry] = []
    for line in lines[-n:]:
        line = line.strip()
        if not line:
            continue
        out.append(_entry_from_dict(json.loads(line)))
    return out


def grep(
    pattern: str,
    *,
    surface: str | None = None,
    date: str | None = None,
) -> Iterator[PromptLogEntry]:
    """Yield entries from the day's file whose JSON text matches ``pattern``.

    Quick-and-dirty regex over the serialised entry — good enough for "find
    every call where the model emitted ``flowfile.graph.xyz``". Filter by
    ``surface`` to scope to a single AI surface.
    """
    target = date or _now_utc().date().isoformat()
    path = _log_path_for(target)
    if not path.exists():
        return
    rx = re.compile(pattern)
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        if not rx.search(line):
            continue
        entry = _entry_from_dict(json.loads(line))
        if surface is not None and entry.surface != surface:
            continue
        yield entry


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _log_dir() -> Path:
    """Resolve the prompt-log directory at call time.

    Computed lazily so test fixtures that swap ``storage.base_directory``
    (or ``FLOWFILE_STORAGE_DIR``) take effect. Mirrors the lazy
    settings reads used elsewhere in this package.
    """
    from shared.storage_config import storage

    return Path(storage.base_directory) / LOG_SUBDIR


def _log_path_for(date_str: str) -> Path:
    return _log_dir() / f"{date_str}.jsonl"


def _message_to_dict(msg: Message) -> dict[str, Any]:
    """Pydantic ``Message`` → plain dict, dropping ``None`` fields."""
    return msg.model_dump(mode="json", exclude_none=True)


def _tool_to_dict(tool: ToolSpec) -> dict[str, Any]:
    return tool.model_dump(mode="json", exclude_none=True)


def _tool_call_to_dict(tc: ToolCall) -> dict[str, Any]:
    return tc.model_dump(mode="json", exclude_none=True)


def _usage_to_dict(usage: Usage) -> dict[str, int]:
    return {
        "prompt_tokens": usage.prompt_tokens,
        "completion_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
    }


def _prepare_entry(entry: PromptLogEntry) -> PromptLogEntry:
    """Apply optional scrubbing + size truncation before write."""
    messages = entry.messages
    if is_scrubbing_enabled():
        messages = _scrub_messages(messages)
    truncated = False
    if _messages_byte_size(messages) > MAX_MESSAGES_BYTES:
        messages = _truncate_messages(messages)
        truncated = True
    if messages is entry.messages and not truncated:
        return entry
    # PromptLogEntry has slots; rebuild via asdict + reconstruction.
    payload = asdict(entry)
    payload["messages"] = messages
    payload["truncated"] = truncated or entry.truncated
    return PromptLogEntry(**payload)


def _messages_byte_size(messages: list[dict[str, Any]]) -> int:
    return len(json.dumps(messages, default=str, ensure_ascii=False).encode("utf-8"))


def _truncate_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replace older non-system / non-recent message bodies with markers.

    Preserves message order and roles; only ``content`` and ``tool_calls`` of
    older messages are replaced. The system prompt and the most-recent
    ``KEEP_RECENT_TURNS`` messages are kept intact.
    """
    if not messages:
        return messages
    n = len(messages)
    keep_from = max(0, n - KEEP_RECENT_TURNS)
    out: list[dict[str, Any]] = []
    for i, msg in enumerate(messages):
        is_system = msg.get("role") == "system"
        is_recent = i >= keep_from
        if is_system or is_recent:
            out.append(msg)
            continue
        truncated = dict(msg)
        content = truncated.get("content")
        if isinstance(content, str) and content:
            truncated["content"] = f"[...truncated, len={len(content)} chars]"
        if "tool_calls" in truncated:
            tc_count = len(truncated.get("tool_calls") or [])
            truncated["tool_calls"] = [{"__truncated__": True, "count": tc_count}] if tc_count else None
        out.append(truncated)
    return out


def _scrub_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply the safety regex scrubber to user / tool content only.

    System and assistant content are explicitly left untouched —
    those are the things you need to read verbatim when debugging a
    model behaviour. Lazy-imports the safety module so callers that
    never enable scrub stay light.
    """
    from flowfile_core.ai.safety import scrub_value_regex

    out: list[dict[str, Any]] = []
    for msg in messages:
        role = msg.get("role")
        if role not in ("user", "tool"):
            out.append(msg)
            continue
        scrubbed = dict(msg)
        if "content" in scrubbed and scrubbed["content"] is not None:
            scrubbed["content"] = scrub_value_regex(scrubbed["content"])
        out.append(scrubbed)
    return out


def _entry_from_dict(payload: dict[str, Any]) -> PromptLogEntry:
    """Inverse of ``asdict(entry)`` — used by ``tail`` / ``grep``."""
    fields = {
        "timestamp",
        "provider",
        "model",
        "surface",
        "user_id",
        "session_id",
        "tool_call_id",
        "messages",
        "tools",
        "max_tokens",
        "temperature",
        "latency_ms",
        "response",
        "response_tool_calls",
        "finish_reason",
        "usage",
        "error",
        "truncated",
    }
    filtered = {k: v for k, v in payload.items() if k in fields}
    return PromptLogEntry(**filtered)


# ---------------------------------------------------------------------------
# CLI: `python -m flowfile_core.ai.prompt_log tail [N]`
# ---------------------------------------------------------------------------


def _cli_main(argv: list[str]) -> int:
    if not argv:
        argv = ["tail"]
    cmd = argv[0]
    if cmd == "tail":
        n = int(argv[1]) if len(argv) > 1 else 10
        for entry in tail(n):
            print(json.dumps(asdict(entry), default=str, ensure_ascii=False))
        return 0
    if cmd == "grep":
        if len(argv) < 2:
            print("usage: grep PATTERN [SURFACE]", file=sys.stderr)
            return 2
        pattern = argv[1]
        surface = argv[2] if len(argv) > 2 else None
        for entry in grep(pattern, surface=surface):
            print(json.dumps(asdict(entry), default=str, ensure_ascii=False))
        return 0
    print(f"unknown command: {cmd!r} (try: tail, grep)", file=sys.stderr)
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli_main(sys.argv[1:]))


__all__ = [
    "KEEP_RECENT_TURNS",
    "LOG_SUBDIR",
    "MAX_MESSAGES_BYTES",
    "PromptLogEntry",
    "build_entry_from_chat",
    "build_entry_from_stream",
    "grep",
    "is_logging_enabled",
    "is_scrubbing_enabled",
    "log_prompt",
    "tail",
]


# Type stub used by the wrapper to satisfy lints that complain about the
# unused Iterable import. Kept here so external callers can import it
# directly if they want to type a generator over the log.
EntryStream = Iterable[PromptLogEntry]
