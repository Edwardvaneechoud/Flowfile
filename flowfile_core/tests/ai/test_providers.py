"""Provider abstraction tests.

Cases:

* ``test_provider_protocol_runtime_check`` — every adapter satisfies the
  ``Provider`` runtime-checkable Protocol.
* ``test_factory_returns_correct_class`` — ``provider_factory(name)`` returns
  the matching adapter for each name in ``PROVIDERS``.
* ``test_factory_unknown_provider_raises`` — bad name → ``UnknownProviderError``.
* ``test_factory_resolves_surface_to_model`` — surface→model mapping for
  each provider.
* ``test_explicit_model_overrides_surface`` — explicit ``model=`` wins over surface.
* ``test_factory_applies_litellm_prefix`` — model normalisation prepends prefix.
* ``test_list_supported_providers`` — all six vendors enumerated.
* ``test_lazy_litellm_import`` — importing the providers package doesn't pull in
  litellm itself.
* ``test_chat_calls_litellm_with_correct_kwargs`` — ``provider.chat()`` passes
  through model, messages, tools, max_tokens, api_key, api_base.
* ``test_chat_translates_tool_calls_in_response`` — model-emitted tool calls
  surface as ``ChatResponse.tool_calls`` with parsed JSON arguments.
* ``test_chat_handles_response_without_tool_calls`` — content-only path returns
  empty ``tool_calls``.
* ``test_chat_translates_message_with_tool_role`` — Pydantic ``Message(role="tool")``
  renders to litellm shape.
* ``test_stream_yields_streamchunks`` — async iterator yields content deltas,
  buffered tool-call delta, and a finish-reason marker in order.
* ``test_provider_unknown_arg_resolution`` — unknown surface falls back to default.
"""

from __future__ import annotations

import json
import sys
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from flowfile_core.ai.providers import (
    AnthropicProvider,
    ChatResponse,
    GoogleProvider,
    GroqProvider,
    Message,
    OllamaProvider,
    OpenAIProvider,
    OpenRouterProvider,
    Provider,
    StreamChunk,
    ToolCall,
    ToolSpec,
    UnknownProviderError,
    list_supported_providers,
    provider_factory,
)
from flowfile_core.ai.providers._litellm_base import (
    _chunks_from_litellm,
    _message_to_litellm,
    _PartialToolCall,
    _response_from_litellm,
    _tool_to_litellm,
)


PROVIDER_NAMES = ["anthropic", "openai", "google", "groq", "openrouter", "ollama"]
PROVIDER_CLASSES = {
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
    "google": GoogleProvider,
    "groq": GroqProvider,
    "openrouter": OpenRouterProvider,
    "ollama": OllamaProvider,
}


class _Obj:
    """Minimal attribute-bag used to mimic litellm's response shape in tests."""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def _make_chat_response(
    *,
    content: str | None = "hello",
    tool_calls: list[dict[str, Any]] | None = None,
    finish_reason: str = "stop",
    usage: dict[str, int] | None = None,
) -> _Obj:
    """Build a fake litellm chat completion response."""
    raw_tool_calls = []
    for tc in tool_calls or []:
        raw_tool_calls.append(
            _Obj(
                id=tc["id"],
                type="function",
                function=_Obj(name=tc["name"], arguments=tc["arguments"]),
            )
        )
    message = _Obj(content=content, tool_calls=raw_tool_calls or None)
    choice = _Obj(message=message, finish_reason=finish_reason)
    usage_obj = _Obj(
        prompt_tokens=(usage or {}).get("prompt", 10),
        completion_tokens=(usage or {}).get("completion", 5),
        total_tokens=(usage or {}).get("total", 15),
    )
    return _Obj(choices=[choice], usage=usage_obj)


# -------- Protocol & registry --------


@pytest.mark.parametrize("name", PROVIDER_NAMES)
def test_provider_protocol_runtime_check(name: str) -> None:
    p = provider_factory(name)
    assert isinstance(p, Provider), f"{type(p).__name__} should satisfy Provider Protocol"
    assert p.name == name
    assert isinstance(p.model, str) and p.model
    assert isinstance(p.supports_tools, bool)
    assert isinstance(p.supports_streaming, bool)


@pytest.mark.parametrize("name", PROVIDER_NAMES)
def test_factory_returns_correct_class(name: str) -> None:
    p = provider_factory(name)
    assert isinstance(p, PROVIDER_CLASSES[name])


def test_factory_unknown_provider_raises() -> None:
    with pytest.raises(UnknownProviderError) as exc_info:
        provider_factory("nope")
    assert exc_info.value.name == "nope"
    assert "anthropic" in exc_info.value.supported


def test_list_supported_providers() -> None:
    names = list_supported_providers()
    assert sorted(names) == sorted(PROVIDER_NAMES)


# -------- surface → model resolution --------


@pytest.mark.parametrize(
    ("name", "surface", "model_substr"),
    [
        ("anthropic", "cmd_k", "haiku"),
        # — legacy ``"agent"`` surface removed; the staged
        # surface routes to haiku for anthropic, agent_complex stays on
        # opus.
        ("anthropic", "agent_staged", "haiku"),
        ("anthropic", "agent_complex", "opus"),
        ("google", "cmd_k", "flash"),
        ("google", "agent_complex", "pro"),
        ("groq", "cmd_k", "qwen"),
        ("ollama", "agent_complex", "70b"),
        ("openrouter", "cmd_k", "haiku"),
    ],
)
def test_factory_resolves_surface_to_model(name: str, surface: str, model_substr: str) -> None:
    p = provider_factory(name, surface=surface)
    assert model_substr in p.model.lower(), f"{p.model} should contain {model_substr!r}"


def test_explicit_model_overrides_surface() -> None:
    p = provider_factory("anthropic", model="claude-opus-4-7", surface="cmd_k")
    # Explicit model wins; cmd_k surface would have picked Haiku.
    assert "opus" in p.model.lower()


def test_unknown_surface_falls_back_to_default() -> None:
    p = provider_factory("anthropic", surface="unrecognised_surface")
    # Default model for Anthropic is Sonnet.
    assert "sonnet" in p.model.lower()


# -------- Model prefix normalisation --------


@pytest.mark.parametrize(
    ("name", "raw_model", "expected_prefix"),
    [
        ("anthropic", "claude-haiku-4-5", "anthropic/"),
        ("google", "gemini-2.5-flash", "gemini/"),
        ("groq", "llama-3.3-70b-versatile", "groq/"),
        ("ollama", "llama3.1:8b", "ollama_chat/"),
        ("openrouter", "anthropic/claude-haiku-4.5", "openrouter/"),
    ],
)
def test_factory_applies_litellm_prefix(name: str, raw_model: str, expected_prefix: str) -> None:
    p = provider_factory(name, model=raw_model)
    assert p.model.startswith(expected_prefix), f"{p.model} missing {expected_prefix!r}"


def test_prefix_not_double_applied() -> None:
    p = provider_factory("anthropic", model="anthropic/claude-haiku-4-5")
    assert p.model == "anthropic/claude-haiku-4-5"
    assert p.model.count("anthropic/") == 1


def test_openai_no_prefix() -> None:
    # OpenAI models in litellm are routed by bare name (no "openai/" prefix).
    p = provider_factory("openai", model="gpt-4.1")
    assert p.model == "gpt-4.1"


# -------- api_base + api_key --------


def test_ollama_default_api_base() -> None:
    p = provider_factory("ollama")
    assert p.api_base == "http://localhost:11434"


def test_api_base_override() -> None:
    p = provider_factory("ollama", api_base="http://gpu-host:11434")
    assert p.api_base == "http://gpu-host:11434"


def test_non_ollama_no_default_api_base() -> None:
    p = provider_factory("anthropic")
    assert p.api_base is None


def test_api_key_passthrough() -> None:
    p = provider_factory("anthropic", api_key="sk-ant-test")
    assert p.api_key == "sk-ant-test"


# -------- Lazy litellm import --------


def test_lazy_litellm_import() -> None:
    """Importing the providers package must not pull in litellm.

    Re-imports are no-ops thanks to module caching, so we exercise the cold
    path by clearing both modules from ``sys.modules`` first. This guards the
    cheap-startup invariant for tests / module-walk tooling.
    """
    cleared = {}
    for mod_name in list(sys.modules):
        if mod_name == "litellm" or mod_name.startswith("litellm."):
            cleared[mod_name] = sys.modules.pop(mod_name)
        elif mod_name == "flowfile_core.ai.providers" or mod_name.startswith(
            "flowfile_core.ai.providers."
        ):
            cleared[mod_name] = sys.modules.pop(mod_name)
    try:
        import flowfile_core.ai.providers  # noqa: F401
    finally:
        # Restore any cleared litellm modules so subsequent tests can patch them.
        for mod_name, mod in cleared.items():
            if mod_name not in sys.modules:
                sys.modules[mod_name] = mod
    assert "litellm" not in sys.modules, (
        "Importing flowfile_core.ai.providers must not eagerly import litellm"
    )


# -------- chat() translation --------


@pytest.mark.asyncio
async def test_chat_calls_litellm_with_correct_kwargs() -> None:
    p = provider_factory("anthropic", model="claude-haiku-4-5", api_key="sk-test")
    messages = [
        Message(role="system", content="You are Flowfile's AI."),
        Message(role="user", content="Hi"),
    ]
    tools = [
        ToolSpec(
            name="flowfile.graph.add_filter",
            description="Add a filter node",
            parameters={"type": "object", "properties": {"predicate": {"type": "string"}}},
        )
    ]

    fake_response = _make_chat_response(content="hello world")
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)) as mock_acomp:
        result = await p.chat(messages, tools=tools, max_tokens=200)

    mock_acomp.assert_awaited_once()
    kwargs = mock_acomp.await_args.kwargs
    assert kwargs["model"] == "anthropic/claude-haiku-4-5"
    assert kwargs["api_key"] == "sk-test"
    assert kwargs["max_tokens"] == 200
    assert kwargs["messages"][0] == {"role": "system", "content": "You are Flowfile's AI."}
    assert kwargs["messages"][1] == {"role": "user", "content": "Hi"}
    assert kwargs["tools"][0]["type"] == "function"
    assert kwargs["tools"][0]["function"]["name"] == "flowfile.graph.add_filter"
    assert kwargs["tools"][0]["function"]["description"] == "Add a filter node"
    assert "stream" not in kwargs

    assert isinstance(result, ChatResponse)
    assert result.content == "hello world"
    assert result.model == "anthropic/claude-haiku-4-5"
    assert result.finish_reason == "stop"
    assert result.usage.total_tokens == 15


@pytest.mark.asyncio
async def test_chat_translates_tool_calls_in_response() -> None:
    p = provider_factory("anthropic")
    fake_response = _make_chat_response(
        content=None,
        tool_calls=[
            {
                "id": "call_abc",
                "name": "flowfile.graph.add_filter",
                "arguments": json.dumps({"predicate": "amount > 100"}),
            }
        ],
        finish_reason="tool_calls",
    )

    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)):
        result = await p.chat([Message(role="user", content="filter big rows")])

    assert len(result.tool_calls) == 1
    tc = result.tool_calls[0]
    assert tc.id == "call_abc"
    assert tc.name == "flowfile.graph.add_filter"
    assert tc.arguments == {"predicate": "amount > 100"}
    assert result.finish_reason == "tool_calls"


@pytest.mark.asyncio
async def test_chat_handles_response_without_tool_calls() -> None:
    p = provider_factory("anthropic")
    fake_response = _make_chat_response(content="no tools today")

    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)):
        result = await p.chat([Message(role="user", content="explain")])

    assert result.tool_calls == []
    assert result.content == "no tools today"


@pytest.mark.asyncio
async def test_chat_passes_api_base_for_ollama() -> None:
    p = provider_factory("ollama", api_base="http://gpu-host:11434")
    fake_response = _make_chat_response()
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)) as mock_acomp:
        await p.chat([Message(role="user", content="hi")])

    kwargs = mock_acomp.await_args.kwargs
    assert kwargs["api_base"] == "http://gpu-host:11434"
    # Ollama gets no api_key by default.
    assert "api_key" not in kwargs


@pytest.mark.asyncio
async def test_chat_omits_optional_fields() -> None:
    p = provider_factory("anthropic")  # no api_key, no api_base, no max_tokens
    fake_response = _make_chat_response()
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)) as mock_acomp:
        await p.chat([Message(role="user", content="hi")])

    kwargs = mock_acomp.await_args.kwargs
    assert "api_key" not in kwargs
    assert "api_base" not in kwargs
    assert "max_tokens" not in kwargs
    assert "tools" not in kwargs


# -------- Message translation helpers (unit-level) --------


def test_message_to_litellm_minimal() -> None:
    out = _message_to_litellm(Message(role="user", content="hi"))
    assert out == {"role": "user", "content": "hi"}


def test_message_to_litellm_tool_call_round_trip() -> None:
    msg = Message(
        role="assistant",
        content=None,
        tool_calls=[
            ToolCall(id="call_1", name="flowfile.graph.add_filter", arguments={"x": 1})
        ],
    )
    out = _message_to_litellm(msg)
    assert out["role"] == "assistant"
    assert "content" not in out  # no None content emitted
    assert out["tool_calls"][0]["id"] == "call_1"
    assert out["tool_calls"][0]["function"]["name"] == "flowfile.graph.add_filter"
    assert json.loads(out["tool_calls"][0]["function"]["arguments"]) == {"x": 1}


def test_message_to_litellm_tool_result() -> None:
    msg = Message(
        role="tool",
        content='{"ok": true}',
        tool_call_id="call_1",
        name="flowfile.graph.add_filter",
    )
    out = _message_to_litellm(msg)
    assert out["role"] == "tool"
    assert out["tool_call_id"] == "call_1"
    assert out["name"] == "flowfile.graph.add_filter"


def test_tool_to_litellm() -> None:
    spec = ToolSpec(
        name="flowfile.schema.read_node_schema",
        description="Read a node's predicted output schema.",
        parameters={"type": "object", "properties": {"node_id": {"type": "integer"}}},
    )
    out = _tool_to_litellm(spec)
    assert out == {
        "type": "function",
        "function": {
            "name": "flowfile.schema.read_node_schema",
            "description": "Read a node's predicted output schema.",
            "parameters": {"type": "object", "properties": {"node_id": {"type": "integer"}}},
        },
    }


def test_response_from_litellm_handles_missing_usage() -> None:
    raw = _Obj(
        choices=[_Obj(message=_Obj(content="hi", tool_calls=None), finish_reason="stop")],
        usage=None,
    )
    out = _response_from_litellm(raw, model="anthropic/claude-haiku-4-5")
    assert out.usage.total_tokens == 0


def test_response_from_litellm_drops_invalid_tool_call_args() -> None:
    raw = _Obj(
        choices=[
            _Obj(
                message=_Obj(
                    content=None,
                    tool_calls=[
                        _Obj(
                            id="call_x",
                            type="function",
                            function=_Obj(name="x.y", arguments="not-valid-json"),
                        )
                    ],
                ),
                finish_reason="tool_calls",
            )
        ],
        usage=_Obj(prompt_tokens=1, completion_tokens=1, total_tokens=2),
    )
    out = _response_from_litellm(raw, model="m")
    # Bad JSON arguments fall back to empty dict — no crash.
    assert out.tool_calls[0].arguments == {}


# -------- stream() chunk translation --------


def test_partial_tool_call_buffering() -> None:
    partial = _PartialToolCall()
    partial.update(_Obj(id="call_1", function=_Obj(name="flowfile.graph.add_filter", arguments='{"a"')))
    assert not partial.is_complete()
    partial.update(_Obj(id=None, function=_Obj(name=None, arguments=':1}')))
    assert partial.is_complete()
    tc = partial.to_tool_call()
    assert tc.id == "call_1"
    assert tc.name == "flowfile.graph.add_filter"
    assert tc.arguments == {"a": 1}


def test_chunks_from_litellm_content_delta() -> None:
    raw = _Obj(choices=[_Obj(delta=_Obj(content="hel"), finish_reason=None)])
    out = _chunks_from_litellm(raw, {})
    assert len(out) == 1
    assert out[0].content_delta == "hel"
    assert out[0].finish_reason is None


def test_chunks_from_litellm_finish_reason_only() -> None:
    raw = _Obj(choices=[_Obj(delta=_Obj(content=None), finish_reason="stop")])
    out = _chunks_from_litellm(raw, {})
    assert len(out) == 1
    assert out[0].finish_reason == "stop"


def test_chunks_from_litellm_empty_choices() -> None:
    raw = _Obj(choices=[])
    assert _chunks_from_litellm(raw, {}) == []


@pytest.mark.asyncio
async def test_stream_yields_streamchunks_in_order() -> None:
    """End-to-end: stream() returns content deltas, then a buffered tool-call delta, then finish.

    Mocks litellm.acompletion to yield three chunks: a content fragment,
    two tool-call fragments (split JSON arguments), then a finish marker.
    """

    async def fake_stream() -> Any:
        yield _Obj(choices=[_Obj(delta=_Obj(content="thinking..."), finish_reason=None)])
        yield _Obj(
            choices=[
                _Obj(
                    delta=_Obj(
                        content=None,
                        tool_calls=[
                            _Obj(
                                index=0,
                                id="call_z",
                                function=_Obj(
                                    name="flowfile.graph.add_filter",
                                    arguments='{"predicate": "',
                                ),
                            )
                        ],
                    ),
                    finish_reason=None,
                )
            ]
        )
        yield _Obj(
            choices=[
                _Obj(
                    delta=_Obj(
                        content=None,
                        tool_calls=[
                            _Obj(
                                index=0,
                                id=None,
                                function=_Obj(name=None, arguments='amount > 100"}'),
                            )
                        ],
                    ),
                    finish_reason=None,
                )
            ]
        )
        yield _Obj(choices=[_Obj(delta=_Obj(content=None), finish_reason="tool_calls")])

    p = provider_factory("anthropic")
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_stream())):
        chunks: list[StreamChunk] = []
        async for chunk in p.stream([Message(role="user", content="filter big rows")]):
            chunks.append(chunk)

    content_deltas = [c.content_delta for c in chunks if c.content_delta is not None]
    tool_deltas = [c.tool_call_delta for c in chunks if c.tool_call_delta is not None]
    finish_reasons = [c.finish_reason for c in chunks if c.finish_reason is not None]

    assert content_deltas == ["thinking..."]
    assert len(tool_deltas) == 1
    assert tool_deltas[0].id == "call_z"
    assert tool_deltas[0].name == "flowfile.graph.add_filter"
    assert tool_deltas[0].arguments == {"predicate": "amount > 100"}
    assert finish_reasons == ["tool_calls"]


# --------: prompt logging + provider-call counter --------


@pytest.fixture
def _isolated_log_dir(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    """Point ``storage.base_directory`` at tmp_path for the duration of the test."""
    from shared.storage_config import storage

    original = storage._base_dir
    storage._base_dir = tmp_path
    try:
        yield tmp_path / "ai_prompts"
    finally:
        storage._base_dir = original


@pytest.fixture
def _logging_on(monkeypatch):  # type: ignore[no-untyped-def]
    from flowfile_core.configs import settings as _settings

    original = _settings.FLOWFILE_AI_LOG_PROMPTS.value
    _settings.FLOWFILE_AI_LOG_PROMPTS.set(True)
    try:
        yield
    finally:
        _settings.FLOWFILE_AI_LOG_PROMPTS.set(original)


@pytest.fixture
def _logging_off(monkeypatch):  # type: ignore[no-untyped-def]
    from flowfile_core.configs import settings as _settings

    original = _settings.FLOWFILE_AI_LOG_PROMPTS.value
    _settings.FLOWFILE_AI_LOG_PROMPTS.set(False)
    try:
        yield
    finally:
        _settings.FLOWFILE_AI_LOG_PROMPTS.set(original)


@pytest.fixture
def _reset_provider_counter():  # type: ignore[no-untyped-def]
    from flowfile_core.ai.metrics import reset_provider_call_counts

    reset_provider_call_counts()
    yield
    reset_provider_call_counts()


@pytest.mark.asyncio
async def test_chat_writes_prompt_log_when_enabled(
    _isolated_log_dir, _logging_on, _reset_provider_counter
) -> None:
    """acceptance #1: chat call writes one JSONL line with full payload."""
    p = provider_factory("anthropic", model="claude-haiku-4-5", api_key="sk-test")
    fake_response = _make_chat_response(content="hello world")
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)):
        await p.chat(
            [Message(role="user", content="filter big rows")],
            tools=[
                ToolSpec(
                    name="flowfile.graph.add_filter",
                    description="Add a filter",
                    parameters={"type": "object"},
                )
            ],
            max_tokens=200,
            surface="agent_complex",
            session_id="sess-1",
        )

    files = list(_isolated_log_dir.iterdir())
    assert len(files) == 1, files
    lines = files[0].read_text().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["provider"] == "anthropic"
    assert payload["surface"] == "agent_complex"
    assert payload["session_id"] == "sess-1"
    assert payload["max_tokens"] == 200
    assert payload["response"] == "hello world"
    assert payload["messages"][0]["content"] == "filter big rows"
    assert payload["tools"][0]["name"] == "flowfile.graph.add_filter"
    assert payload["error"] is None
    assert isinstance(payload["latency_ms"], int)


@pytest.mark.asyncio
async def test_chat_no_log_when_disabled(
    _isolated_log_dir, _logging_off, _reset_provider_counter
) -> None:
    """acceptance #2: with the flag off, no file is written."""
    p = provider_factory("anthropic")
    fake_response = _make_chat_response(content="hi")
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)):
        await p.chat([Message(role="user", content="hi")], surface="agent_complex")

    assert not _isolated_log_dir.exists() or not list(_isolated_log_dir.iterdir())


@pytest.mark.asyncio
async def test_stream_writes_one_log_entry_on_end(
    _isolated_log_dir, _logging_on, _reset_provider_counter
) -> None:
    """acceptance #3: streaming path emits ONE entry on stream-end with
    aggregated text + tool calls. Per-chunk lines are not emitted."""

    async def fake_stream() -> Any:
        yield _Obj(choices=[_Obj(delta=_Obj(content="hello "), finish_reason=None)])
        yield _Obj(choices=[_Obj(delta=_Obj(content="world"), finish_reason=None)])
        yield _Obj(
            choices=[
                _Obj(
                    delta=_Obj(
                        content=None,
                        tool_calls=[
                            _Obj(
                                index=0,
                                id="call_z",
                                function=_Obj(
                                    name="flowfile.graph.add_filter",
                                    arguments='{"predicate": "amount > 100"}',
                                ),
                            )
                        ],
                    ),
                    finish_reason=None,
                )
            ]
        )
        yield _Obj(choices=[_Obj(delta=_Obj(content=None), finish_reason="tool_calls")])

    p = provider_factory("anthropic")
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_stream())):
        async for _ in p.stream(
            [Message(role="user", content="hi")],
            surface="chat",
            session_id="sess-2",
        ):
            pass

    files = list(_isolated_log_dir.iterdir())
    assert len(files) == 1
    lines = files[0].read_text().splitlines()
    assert len(lines) == 1, "stream must emit one entry per call, not per chunk"
    payload = json.loads(lines[0])
    assert payload["surface"] == "chat"
    assert payload["session_id"] == "sess-2"
    assert payload["response"] == "hello world"
    assert payload["finish_reason"] == "tool_calls"
    assert len(payload["response_tool_calls"]) == 1
    assert payload["response_tool_calls"][0]["id"] == "call_z"


@pytest.mark.asyncio
async def test_chat_log_records_error(
    _isolated_log_dir, _logging_on, _reset_provider_counter
) -> None:
    """An LLM-call error still writes an entry (with ``error`` populated) and
    re-raises. The wrapper must not swallow the underlying exception."""
    p = provider_factory("anthropic")
    with patch(
        "litellm.acompletion",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await p.chat([Message(role="user", content="hi")], surface="agent_complex")

    files = list(_isolated_log_dir.iterdir())
    assert len(files) == 1
    payload = json.loads(files[0].read_text().splitlines()[0])
    assert payload["error"] is not None
    assert "boom" in payload["error"]
    assert payload["response"] is None


@pytest.mark.asyncio
async def test_provider_counter_increments_regardless_of_log_flag(
    _logging_off, _reset_provider_counter
) -> None:
    """acceptance #7: counter increments even when prompt log is off."""
    from flowfile_core.ai.metrics import get_provider_call_counts

    p = provider_factory("anthropic")
    fake_response = _make_chat_response()
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)):
        await p.chat([Message(role="user", content="hi")], surface="cmd_k")

    counts = get_provider_call_counts()
    assert counts.get(("anthropic", "cmd_k", p.model, "success")) == 1


@pytest.mark.asyncio
async def test_provider_counter_increments_on_error(
    _logging_off, _reset_provider_counter
) -> None:
    from flowfile_core.ai.metrics import get_provider_call_counts

    p = provider_factory("anthropic")
    with patch(
        "litellm.acompletion",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        with pytest.raises(RuntimeError):
            await p.chat([Message(role="user", content="hi")], surface="agent_complex")

    counts = get_provider_call_counts()
    assert counts.get(("anthropic", "agent_complex", p.model, "error")) == 1


@pytest.mark.asyncio
async def test_chat_surface_defaults_to_unknown_in_counter(
    _logging_off, _reset_provider_counter
) -> None:
    """Calling without ``surface`` keys the counter under ``"unknown"`` so the
    label tuple stays a 4-tuple."""
    from flowfile_core.ai.metrics import get_provider_call_counts

    p = provider_factory("anthropic")
    fake_response = _make_chat_response()
    with patch("litellm.acompletion", new=AsyncMock(return_value=fake_response)):
        await p.chat([Message(role="user", content="hi")])

    counts = get_provider_call_counts()
    assert counts.get(("anthropic", "unknown", p.model, "success")) == 1
