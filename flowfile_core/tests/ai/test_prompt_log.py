"""Prompt logging for debugging.

Cases:

* ``test_module_lazy_imports_litellm`` — importing ``flowfile_core.ai.prompt_log``
  does not transitively import ``litellm``.
* ``test_log_prompt_writes_one_line_per_call`` — basic JSONL append.
* ``test_log_prompt_filename_is_today_iso_date`` — daily-rotation filename.
* ``test_log_prompt_creates_directory_lazily`` — first write makes the dir.
* ``test_log_prompt_no_op_when_disabled`` — ``FLOWFILE_AI_LOG_PROMPTS=false``
  short-circuits at the wrapper layer, so the helper itself still writes.
* ``test_truncation_kicks_in_past_threshold`` — entry past 256 KiB compacts
  older non-system / non-recent message bodies and sets ``truncated=True``.
* ``test_truncation_preserves_system_and_recent`` — system + last 5 turns
  retain their original ``content``.
* ``test_scrub_runs_on_user_and_tool_only`` — ``FLOWFILE_AI_LOG_PROMPTS_SCRUB``
  scrubs user / tool messages but leaves system + assistant intact.
* ``test_build_entry_from_chat_round_trip`` — Pydantic Message / ToolSpec /
  ChatResponse → dict shape that survives ``json.dumps``.
* ``test_build_entry_from_stream_aggregates_response`` — stream entry rolls
  up per-chunk text + tool calls.
* ``test_log_prompt_swallows_io_error`` — a write error doesn't raise.
* ``test_tail_returns_last_n_entries`` — ``tail`` reads JSONL back.
* ``test_grep_filters_by_pattern_and_surface`` — ``grep`` matches text and
  scopes by surface.
"""

from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flowfile_core.ai.prompt_log import (
    KEEP_RECENT_TURNS,
    LOG_SUBDIR,
    MAX_MESSAGES_BYTES,
    PromptLogEntry,
    build_entry_from_chat,
    build_entry_from_stream,
    grep,
    is_logging_enabled,
    is_scrubbing_enabled,
    log_prompt,
    tail,
)
from flowfile_core.ai.providers.base import (
    ChatResponse,
    Message,
    ToolCall,
    ToolSpec,
    Usage,
)
from flowfile_core.configs import settings as core_settings


# Fixtures


@pytest.fixture
def isolated_log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point ``storage.base_directory`` at a tmp dir for the duration of the test."""
    from shared.storage_config import storage

    original = storage._base_dir
    storage._base_dir = tmp_path
    try:
        yield tmp_path / LOG_SUBDIR
    finally:
        storage._base_dir = original


@pytest.fixture
def logging_on(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    original = core_settings.FLOWFILE_AI_LOG_PROMPTS.value
    core_settings.FLOWFILE_AI_LOG_PROMPTS.set(True)
    try:
        yield
    finally:
        core_settings.FLOWFILE_AI_LOG_PROMPTS.set(original)


@pytest.fixture
def logging_off(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    original = core_settings.FLOWFILE_AI_LOG_PROMPTS.value
    core_settings.FLOWFILE_AI_LOG_PROMPTS.set(False)
    try:
        yield
    finally:
        core_settings.FLOWFILE_AI_LOG_PROMPTS.set(original)


@pytest.fixture
def scrub_on(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    original = core_settings.FLOWFILE_AI_LOG_PROMPTS_SCRUB.value
    core_settings.FLOWFILE_AI_LOG_PROMPTS_SCRUB.set(True)
    try:
        yield
    finally:
        core_settings.FLOWFILE_AI_LOG_PROMPTS_SCRUB.set(original)


def _make_entry(**overrides: Any) -> PromptLogEntry:
    base: dict[str, Any] = {
        "timestamp": "2026-05-05T10:00:00+00:00",
        "provider": "anthropic",
        "model": "anthropic/claude-haiku-4-5",
        "surface": "agent",
        "messages": [
            {"role": "system", "content": "you are flowfile"},
            {"role": "user", "content": "hi"},
        ],
        "tools": [
            {
                "name": "flowfile.graph.add_filter",
                "description": "Add a filter",
                "parameters": {"type": "object"},
            }
        ],
        "max_tokens": 200,
        "latency_ms": 123,
        "response": "hi back",
    }
    base.update(overrides)
    return PromptLogEntry(**base)


# Lazy contract


def test_module_lazy_imports_litellm() -> None:
    """Importing ``flowfile_core.ai.prompt_log`` must not pull in litellm."""
    cleared = {}
    for name in list(sys.modules):
        if name == "litellm" or name.startswith("litellm."):
            cleared[name] = sys.modules.pop(name)
        elif name == "flowfile_core.ai.prompt_log":
            cleared[name] = sys.modules.pop(name)
    try:
        importlib.import_module("flowfile_core.ai.prompt_log")
    finally:
        for name, mod in cleared.items():
            if name not in sys.modules:
                sys.modules[name] = mod
    assert "litellm" not in sys.modules, (
        "Importing flowfile_core.ai.prompt_log must not eagerly import litellm"
    )


# is_logging_enabled / is_scrubbing_enabled


def test_is_logging_enabled_reads_live_value(logging_on: None) -> None:
    assert is_logging_enabled() is True
    core_settings.FLOWFILE_AI_LOG_PROMPTS.set(False)
    assert is_logging_enabled() is False


def test_is_scrubbing_enabled_reads_live_value(scrub_on: None) -> None:
    assert is_scrubbing_enabled() is True
    core_settings.FLOWFILE_AI_LOG_PROMPTS_SCRUB.set(False)
    assert is_scrubbing_enabled() is False


# Write path


def test_log_prompt_writes_one_line_per_call(
    isolated_log_dir: Path, logging_on: None
) -> None:
    log_prompt(_make_entry())
    log_prompt(_make_entry(response="second"))
    files = list(isolated_log_dir.iterdir())
    assert len(files) == 1, files
    lines = files[0].read_text().splitlines()
    assert len(lines) == 2
    payload = json.loads(lines[0])
    assert payload["provider"] == "anthropic"
    assert payload["surface"] == "agent"
    assert payload["messages"][0]["role"] == "system"
    assert payload["response"] == "hi back"
    assert payload["truncated"] is False


def test_log_prompt_filename_is_today_iso_date(
    isolated_log_dir: Path, logging_on: None
) -> None:
    from datetime import datetime, timezone

    today = datetime.now(tz=timezone.utc).date().isoformat()
    log_prompt(_make_entry())
    expected = isolated_log_dir / f"{today}.jsonl"
    assert expected.exists()


def test_log_prompt_creates_directory_lazily(
    isolated_log_dir: Path, logging_on: None
) -> None:
    assert not isolated_log_dir.exists()
    log_prompt(_make_entry())
    assert isolated_log_dir.is_dir()


def test_log_prompt_helper_is_unconditional(
    isolated_log_dir: Path, logging_off: None
) -> None:
    """``log_prompt`` itself writes regardless of the flag — gating lives at the
    wrapper. This decouples the helper from the env-var so callers that want to
    force a write (manual scripts, tests) don't have to flip the global flag.
    """
    log_prompt(_make_entry())
    files = list(isolated_log_dir.iterdir())
    assert len(files) == 1


def test_log_prompt_swallows_io_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing write must not raise — the LLM call must keep returning."""
    from flowfile_core.ai import prompt_log

    def _boom(*_args: Any, **_kw: Any) -> Path:
        raise RuntimeError("disk full")

    monkeypatch.setattr(prompt_log, "_log_path_for", _boom)
    # Should not raise.
    log_prompt(_make_entry())


# Truncation


def _big_message(role: str, kb: int) -> dict[str, Any]:
    return {"role": role, "content": "x" * (kb * 1024)}


def test_truncation_kicks_in_past_threshold(
    isolated_log_dir: Path, logging_on: None
) -> None:
    bulky = [
        {"role": "system", "content": "you are flowfile"},
        _big_message("user", 80),
        _big_message("assistant", 80),
        _big_message("user", 80),
        _big_message("assistant", 10),
        _big_message("user", 10),
        _big_message("assistant", 10),
        _big_message("user", 10),
    ]
    log_prompt(_make_entry(messages=bulky))
    files = list(isolated_log_dir.iterdir())
    payload = json.loads(files[0].read_text().splitlines()[0])
    assert payload["truncated"] is True
    # The first big user/assistant pair should be marker-replaced.
    assert payload["messages"][1]["content"].startswith("[...truncated, len=")
    # Total bytes after truncation should be under the cap with breathing room.
    assert (
        len(json.dumps(payload["messages"]).encode("utf-8")) < MAX_MESSAGES_BYTES
    )


def test_truncation_preserves_system_and_recent(
    isolated_log_dir: Path, logging_on: None
) -> None:
    n_recent = KEEP_RECENT_TURNS
    # Push well past MAX_MESSAGES_BYTES so truncation triggers.
    bulky = [
        {"role": "system", "content": "system body kept"},
        _big_message("user", 150),  # OLD — should be truncated
        _big_message("assistant", 150),  # OLD — should be truncated
    ]
    # Append exactly KEEP_RECENT_TURNS recent messages with distinctive content.
    for i in range(n_recent):
        bulky.append({"role": "user", "content": f"recent-{i}"})
    log_prompt(_make_entry(messages=bulky))
    payload = json.loads(
        list(isolated_log_dir.iterdir())[0].read_text().splitlines()[0]
    )
    msgs = payload["messages"]
    # system kept verbatim
    assert msgs[0]["content"] == "system body kept"
    # last KEEP_RECENT_TURNS kept verbatim
    for i in range(n_recent):
        assert msgs[-(n_recent - i)]["content"] == f"recent-{i}"
    # the two large old messages got marker-replaced
    assert msgs[1]["content"].startswith("[...truncated, len=")
    assert msgs[2]["content"].startswith("[...truncated, len=")


def test_truncation_skipped_when_under_threshold(
    isolated_log_dir: Path, logging_on: None
) -> None:
    log_prompt(_make_entry())
    payload = json.loads(
        list(isolated_log_dir.iterdir())[0].read_text().splitlines()[0]
    )
    assert payload["truncated"] is False


# Scrubbing


def test_scrub_runs_on_user_and_tool_only(
    isolated_log_dir: Path, logging_on: None, scrub_on: None
) -> None:
    msgs = [
        {"role": "system", "content": "contact admin@example.com if stuck"},
        {"role": "user", "content": "my email is alice@example.com"},
        {"role": "assistant", "content": "I see alice@example.com — got it"},
        {
            "role": "tool",
            "content": "lookup result: bob@example.com",
            "tool_call_id": "x",
        },
    ]
    log_prompt(_make_entry(messages=msgs))
    payload = json.loads(
        list(isolated_log_dir.iterdir())[0].read_text().splitlines()[0]
    )
    out = payload["messages"]
    # System: untouched (need to read system prompts verbatim when debugging)
    assert "admin@example.com" in out[0]["content"]
    # User: scrubbed
    assert "alice@example.com" not in out[1]["content"]
    # Assistant: untouched (model output is part of what we're debugging)
    assert "alice@example.com" in out[2]["content"]
    # Tool: scrubbed
    assert "bob@example.com" not in out[3]["content"]


def test_scrub_off_by_default(isolated_log_dir: Path, logging_on: None) -> None:
    msgs = [
        {"role": "user", "content": "my email is alice@example.com"},
    ]
    log_prompt(_make_entry(messages=msgs))
    payload = json.loads(
        list(isolated_log_dir.iterdir())[0].read_text().splitlines()[0]
    )
    assert "alice@example.com" in payload["messages"][0]["content"]


# build_entry_from_chat / build_entry_from_stream


def test_build_entry_from_chat_round_trip() -> None:
    messages = [
        Message(role="system", content="you are flowfile"),
        Message(role="user", content="hi"),
    ]
    tools = [
        ToolSpec(
            name="flowfile.graph.add_filter",
            description="Add a filter",
            parameters={"type": "object"},
        )
    ]
    response = ChatResponse(
        model="anthropic/claude-haiku-4-5",
        content="hi back",
        tool_calls=[ToolCall(id="c1", name="x", arguments={"a": 1})],
        finish_reason="tool_calls",
        usage=Usage(prompt_tokens=10, completion_tokens=5, total_tokens=15),
    )
    entry = build_entry_from_chat(
        provider="anthropic",
        model="anthropic/claude-haiku-4-5",
        messages=messages,
        tools=tools,
        max_tokens=200,
        response=response,
        latency_ms=123,
        error=None,
        surface="agent",
        session_id="sess-1",
        user_id=42,
    )
    # Survives JSON round-trip — that's the contract the file format requires.
    from dataclasses import asdict

    blob = json.dumps(asdict(entry), default=str)
    parsed = json.loads(blob)
    assert parsed["provider"] == "anthropic"
    assert parsed["surface"] == "agent"
    assert parsed["session_id"] == "sess-1"
    assert parsed["user_id"] == 42
    assert parsed["messages"][0]["role"] == "system"
    assert parsed["tools"][0]["name"] == "flowfile.graph.add_filter"
    assert parsed["response"] == "hi back"
    assert parsed["response_tool_calls"][0]["id"] == "c1"
    assert parsed["finish_reason"] == "tool_calls"
    assert parsed["usage"]["total_tokens"] == 15
    assert parsed["error"] is None


def test_build_entry_from_chat_handles_error() -> None:
    entry = build_entry_from_chat(
        provider="anthropic",
        model="m",
        messages=[Message(role="user", content="hi")],
        tools=None,
        max_tokens=None,
        response=None,
        latency_ms=42,
        error="RuntimeError('boom')",
        surface="agent",
    )
    assert entry.response is None
    assert entry.response_tool_calls == []
    assert entry.usage is None
    assert entry.error == "RuntimeError('boom')"
    assert entry.latency_ms == 42


def test_build_entry_from_stream_aggregates_response() -> None:
    entry = build_entry_from_stream(
        provider="anthropic",
        model="m",
        messages=[Message(role="user", content="hi")],
        tools=None,
        max_tokens=None,
        response_text="streamed answer",
        response_tool_calls=[ToolCall(id="c1", name="x", arguments={})],
        finish_reason="tool_calls",
        latency_ms=99,
        error=None,
        surface="chat",
    )
    assert entry.response == "streamed answer"
    assert entry.response_tool_calls[0]["id"] == "c1"
    assert entry.finish_reason == "tool_calls"
    assert entry.usage is None  # not populated for stream path in v0


# tail / grep


def test_tail_returns_last_n_entries(
    isolated_log_dir: Path, logging_on: None
) -> None:
    for i in range(5):
        log_prompt(_make_entry(response=f"r{i}"))
    out = tail(2)
    assert len(out) == 2
    assert out[0].response == "r3"
    assert out[1].response == "r4"


def test_tail_handles_missing_file() -> None:
    # No isolated_log_dir — points at a fresh storage which has no prior log.
    # Just exercise the no-file branch directly.
    from flowfile_core.ai.prompt_log import tail as _tail

    assert _tail(date="1970-01-01") == []


def test_grep_filters_by_pattern_and_surface(
    isolated_log_dir: Path, logging_on: None
) -> None:
    log_prompt(_make_entry(surface="agent", response="apple"))
    log_prompt(_make_entry(surface="chat", response="banana"))
    log_prompt(_make_entry(surface="agent", response="apricot"))

    matches_apple = list(grep("apple"))
    assert len(matches_apple) == 1
    assert matches_apple[0].response == "apple"

    matches_a_in_agent = list(grep(r"^.*", surface="agent"))
    assert {m.response for m in matches_a_in_agent} == {"apple", "apricot"}
