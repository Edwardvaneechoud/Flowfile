"""Shared litellm-backed ``LiteLLMProvider`` concrete class.

Owned by W11. Per plan §6.2: ``litellm`` is the default backing library so
all six vendors (Anthropic / OpenAI / Google / Groq / OpenRouter / Ollama)
share one async dispatch path. Per-vendor subclasses only carry config —
``name``, ``default_model``, capability flags, optional ``api_base`` for
self-hosted, and the D010 surface→model map.

The litellm import is **lazy** (inside ``chat`` / ``stream``) so importing
``flowfile_core.ai.providers`` stays cheap. Tests monkeypatch ``litellm.acompletion``.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any, ClassVar, TypedDict

from flowfile_core.ai.providers.base import (
    ChatResponse,
    Message,
    StreamChunk,
    ToolCall,
    ToolSpec,
    Usage,
)

logger = logging.getLogger(__name__)


class LiteLLMKwargs(TypedDict, total=False):
    """The litellm.acompletion kwargs `_build_kwargs` produces.

    `total=False` so all keys are individually optional — `model` and
    `messages` are always set in practice; the rest are conditional on
    constructor / call args.
    """

    model: str
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]]
    max_tokens: int
    api_key: str
    api_base: str
    stream: bool
    response_format: dict[str, Any]


class LiteLLMProvider:
    """Concrete provider using litellm for the actual LLM dispatch.

    Subclasses set ``name``, ``default_model``, ``model_prefix`` (litellm's
    vendor route, e.g. ``"anthropic/"``), capability flags, and the D010
    ``surface_models`` map. They typically don't override ``chat()`` /
    ``stream()`` — the Pydantic ↔ litellm shape translation is identical
    across vendors thanks to litellm's standardisation.
    """

    name: ClassVar[str] = "_litellm_base"
    default_model: ClassVar[str] = ""
    model_prefix: ClassVar[str] = ""
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {}
    default_api_base: ClassVar[str | None] = None

    def __init__(
        self,
        model: str | None = None,
        *,
        api_key: str | None = None,
        api_base: str | None = None,
    ) -> None:
        self.model = self._normalise_model(model or self.default_model)
        self.api_key = api_key
        self.api_base = api_base or self.default_api_base

    @classmethod
    def _normalise_model(cls, model: str) -> str:
        """Apply the litellm vendor prefix if it isn't already present."""
        if not cls.model_prefix:
            return model
        if model.startswith(cls.model_prefix):
            return model
        return f"{cls.model_prefix}{model}"

    @classmethod
    def resolve_model_for_surface(cls, surface: str | None) -> str:
        if surface and surface in cls.surface_models:
            return cls.surface_models[surface]
        return cls.default_model

    def _build_kwargs(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None,
        max_tokens: int | None,
        *,
        stream: bool,
        response_format: dict[str, Any] | None = None,
    ) -> LiteLLMKwargs:
        # TODO REMOVE WHEN GOING LIVE — dev-only verbose prompt dump.
        # Logs the full assembled messages right before they're sent to the
        # provider so the user can see exactly what the LLM sees. Covers
        # every AI route (chat / planner / run-failure / autocomplete /
        # docgen / lineage / inline-actions / command-palette / suggest /
        # intent-router) because every one of them goes through this seam.
        # Strip before release — content is unredacted (samples may leak PII)
        # and the volume is high.
        _dev_log_prompt(provider=self.name, model=self.model, messages=messages, tools=tools, stream=stream)
        kwargs: LiteLLMKwargs = {
            "model": self.model,
            "messages": [_message_to_litellm(m) for m in messages],
        }
        if tools:
            kwargs["tools"] = [_tool_to_litellm(t) for t in tools]
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        if self.api_key:
            kwargs["api_key"] = self.api_key
        if self.api_base:
            kwargs["api_base"] = self.api_base
        if stream:
            kwargs["stream"] = True
        if response_format is not None:
            kwargs["response_format"] = response_format
        return kwargs

    async def chat(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
        max_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
        *,
        surface: str | None = None,
        session_id: str | None = None,
        user_id: int | None = None,
    ) -> ChatResponse:
        import litellm  # lazy: keeps module import cheap

        kwargs = self._build_kwargs(
            messages,
            tools,
            max_tokens,
            stream=False,
            response_format=response_format,
        )
        started_at = time.perf_counter()
        error: str | None = None
        result: ChatResponse | None = None
        try:
            response = await litellm.acompletion(**kwargs)
            result = _response_from_litellm(response, model=self.model)
            return result
        except Exception as exc:
            error = repr(exc)
            raise
        finally:
            latency_ms = int((time.perf_counter() - started_at) * 1000)
            _record_call_telemetry(
                provider=self.name,
                surface=surface,
                model=self.model,
                error=error,
            )
            _record_chat_log(
                provider=self.name,
                model=self.model,
                surface=surface,
                session_id=session_id,
                user_id=user_id,
                messages=messages,
                tools=tools,
                max_tokens=max_tokens,
                response=result,
                latency_ms=latency_ms,
                error=error,
            )

    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
        max_tokens: int | None = None,
        *,
        surface: str | None = None,
        session_id: str | None = None,
        user_id: int | None = None,
    ) -> AsyncIterator[StreamChunk]:
        import litellm

        kwargs = self._build_kwargs(messages, tools, max_tokens, stream=True)
        started_at = time.perf_counter()
        error: str | None = None
        aggregated_text: list[str] = []
        aggregated_tool_calls: list[ToolCall] = []
        finish_reason: str | None = None
        try:
            response = await litellm.acompletion(**kwargs)
            tool_buffer: dict[int, _PartialToolCall] = {}
            async for raw_chunk in response:
                for chunk in _chunks_from_litellm(raw_chunk, tool_buffer):
                    if chunk.content_delta:
                        aggregated_text.append(chunk.content_delta)
                    if chunk.tool_call_delta is not None:
                        aggregated_tool_calls.append(chunk.tool_call_delta)
                    if chunk.finish_reason:
                        finish_reason = chunk.finish_reason
                    yield chunk
        except Exception as exc:
            error = repr(exc)
            raise
        finally:
            latency_ms = int((time.perf_counter() - started_at) * 1000)
            _record_call_telemetry(
                provider=self.name,
                surface=surface,
                model=self.model,
                error=error,
            )
            _record_stream_log(
                provider=self.name,
                model=self.model,
                surface=surface,
                session_id=session_id,
                user_id=user_id,
                messages=messages,
                tools=tools,
                max_tokens=max_tokens,
                response_text="".join(aggregated_text) if aggregated_text else None,
                response_tool_calls=aggregated_tool_calls,
                finish_reason=finish_reason,
                latency_ms=latency_ms,
                error=error,
            )


def _dev_log_prompt(
    *,
    provider: str,
    model: str,
    messages: list[Message],
    tools: list[ToolSpec] | None,
    stream: bool,
) -> None:
    """TODO REMOVE WHEN GOING LIVE — dump every AI prompt to the core log.

    Uses ``print(flush=True)`` rather than ``logger.info`` because the
    project's runtime logging is highly customised: ``PipelineHandler``
    has ``propagate=False`` and its own handler, while module-named
    loggers inherit the root logger's config — which in Electron / IDE
    runs is not always wired to a visible stream. ``print`` is the
    dumbest thing that works; the user explicitly asked for visibility.
    Strip before release — content is unredacted (samples may leak PII),
    volume is high, and ``print`` bypasses log routing / structured
    logging entirely.
    """
    import sys as _sys

    out = _sys.stderr  # uvicorn's INFO access lines also go to stderr
    total_chars = sum(len(m.content or "") for m in messages)
    tool_count = len(tools) if tools else 0
    print(
        f"\n[DEV-PROMPT-DUMP — REMOVE WHEN GOING LIVE] provider={provider} "
        f"model={model} mode={'stream' if stream else 'chat'} "
        f"messages={len(messages)} total_chars={total_chars} "
        f"total_tokens~{total_chars // 4} tools={tool_count}",
        file=out,
        flush=True,
    )
    for i, m in enumerate(messages):
        content = m.content or ""
        print(
            f"[DEV-PROMPT-DUMP] [{i + 1}/{len(messages)}] role={m.role} "
            f"chars={len(content)}\n"
            f"----- BEGIN {m.role.upper()} MESSAGE -----\n"
            f"{content}\n"
            f"----- END {m.role.upper()} MESSAGE -----",
            file=out,
            flush=True,
        )
    if tools:
        for i, t in enumerate(tools):
            desc = (t.description or "")[:200]
            print(
                f"[DEV-PROMPT-DUMP] tool [{i + 1}/{len(tools)}] "
                f"name={t.name} description={desc}",
                file=out,
                flush=True,
            )


def _message_to_litellm(msg: Message) -> dict[str, Any]:
    """Translate a Pydantic ``Message`` to litellm's dict shape."""
    out: dict[str, Any] = {"role": msg.role}
    if msg.content is not None:
        out["content"] = msg.content
    if msg.tool_calls:
        out["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.name,
                    "arguments": json.dumps(tc.arguments),
                },
            }
            for tc in msg.tool_calls
        ]
    if msg.tool_call_id:
        out["tool_call_id"] = msg.tool_call_id
    if msg.name:
        out["name"] = msg.name
    return out


def _tool_to_litellm(tool: ToolSpec) -> dict[str, Any]:
    """Translate an MCP-shaped ``ToolSpec`` to litellm's tools[] entry."""
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        },
    }


def _response_from_litellm(response: Any, *, model: str) -> ChatResponse:
    """Translate a litellm completion response to a ``ChatResponse``."""
    choice = response.choices[0]
    message = choice.message
    content = getattr(message, "content", None)
    finish_reason = getattr(choice, "finish_reason", None)
    tool_calls: list[ToolCall] = []
    raw_calls = getattr(message, "tool_calls", None) or []
    for raw in raw_calls:
        function = getattr(raw, "function", None)
        if function is None:
            continue
        name = getattr(function, "name", None)
        if not name:
            continue
        args_raw = getattr(function, "arguments", "") or ""
        arguments = _parse_arguments(args_raw)
        tool_calls.append(
            ToolCall(
                id=getattr(raw, "id", "") or "",
                name=name,
                arguments=arguments,
            )
        )

    usage_obj = getattr(response, "usage", None)
    usage = Usage(
        prompt_tokens=getattr(usage_obj, "prompt_tokens", 0) or 0,
        completion_tokens=getattr(usage_obj, "completion_tokens", 0) or 0,
        total_tokens=getattr(usage_obj, "total_tokens", 0) or 0,
    )

    return ChatResponse(
        model=model,
        content=content,
        tool_calls=tool_calls,
        finish_reason=finish_reason,
        usage=usage,
    )


class _PartialToolCall:
    """Mutable buffer for an in-flight streamed tool call.

    Providers stream tool calls as a series of (index, name?, arguments-fragment)
    deltas. We accumulate fragments until ``finish_reason`` flips to
    ``"tool_calls"`` (or the call is otherwise structurally complete) and then
    surface a single ``StreamChunk.tool_call_delta``.
    """

    __slots__ = ("id", "name", "arguments")

    def __init__(self) -> None:
        self.id: str = ""
        self.name: str = ""
        self.arguments: str = ""

    def update(self, delta: Any) -> None:
        if getattr(delta, "id", None):
            self.id = delta.id
        function = getattr(delta, "function", None)
        if function is None:
            return
        if getattr(function, "name", None):
            self.name = function.name
        fragment = getattr(function, "arguments", None)
        if fragment:
            self.arguments += fragment

    def is_complete(self) -> bool:
        if not (self.id and self.name):
            return False
        if not self.arguments:
            return True
        try:
            json.loads(self.arguments)
        except json.JSONDecodeError:
            return False
        return True

    def to_tool_call(self) -> ToolCall:
        return ToolCall(id=self.id, name=self.name, arguments=_parse_arguments(self.arguments))


def _chunks_from_litellm(
    raw_chunk: Any,
    tool_buffer: dict[int, _PartialToolCall],
) -> list[StreamChunk]:
    """Translate a single litellm streaming chunk into ``StreamChunk`` deltas.

    May produce 0, 1, or 2 chunks (a content delta + a finish marker, etc.).
    """
    out: list[StreamChunk] = []
    if not getattr(raw_chunk, "choices", None):
        return out

    choice = raw_chunk.choices[0]
    delta = getattr(choice, "delta", None)
    finish_reason = getattr(choice, "finish_reason", None)

    if delta is not None:
        content = getattr(delta, "content", None)
        if content:
            out.append(StreamChunk(content_delta=content))

        for tc_delta in getattr(delta, "tool_calls", None) or []:
            index = getattr(tc_delta, "index", 0) or 0
            partial = tool_buffer.setdefault(index, _PartialToolCall())
            partial.update(tc_delta)

    if finish_reason:
        for partial in tool_buffer.values():
            if partial.is_complete():
                out.append(StreamChunk(tool_call_delta=partial.to_tool_call()))
        tool_buffer.clear()
        out.append(StreamChunk(finish_reason=finish_reason))

    return out


def _parse_arguments(raw: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Provider emitted non-JSON tool arguments: %r", raw[:200])
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


# ---------------------------------------------------------------------------
# Telemetry + prompt-log hooks (W59)
# ---------------------------------------------------------------------------
#
# Imports below are kept inside the helper functions for the same reason as
# ``litellm`` — keeping ``providers/_litellm_base`` cheap to import. The
# wrappers also swallow logging errors so a misconfigured log dir / disk
# pressure / serialisation hiccup never crashes the actual LLM call.


def _record_call_telemetry(
    *,
    provider: str,
    surface: str | None,
    model: str,
    error: str | None,
) -> None:
    """Bump the W15 ``flowfile_ai_provider_call_total`` counter.

    Independent of ``FLOWFILE_AI_LOG_PROMPTS`` — the counter is cheap and
    always-on so dashboards see traffic regardless of whether prompt
    transcripts are being captured. Failures are swallowed.
    """
    try:
        from flowfile_core.ai.metrics import record_provider_call

        record_provider_call(
            provider=provider,
            surface=surface,
            model=model,
            status="error" if error else "success",
        )
    except Exception:  # noqa: BLE001 — telemetry must not crash the call path
        logger.warning("provider call telemetry failed", exc_info=True)


def _record_chat_log(
    *,
    provider: str,
    model: str,
    surface: str | None,
    session_id: str | None,
    user_id: int | None,
    messages: list[Message],
    tools: list[ToolSpec] | None,
    max_tokens: int | None,
    response: ChatResponse | None,
    latency_ms: int,
    error: str | None,
) -> None:
    """Append one prompt-log entry for a non-streaming chat call (W59)."""
    try:
        from flowfile_core.ai.prompt_log import (
            build_entry_from_chat,
            is_logging_enabled,
            log_prompt,
        )

        if not is_logging_enabled():
            return
        entry = build_entry_from_chat(
            provider=provider,
            model=model,
            messages=messages,
            tools=tools,
            max_tokens=max_tokens,
            response=response,
            latency_ms=latency_ms,
            error=error,
            surface=surface,
            session_id=session_id,
            user_id=user_id,
        )
        log_prompt(entry)
    except Exception:  # noqa: BLE001 — prompt-log must not crash the call path
        logger.warning("prompt_log: chat-side write failed", exc_info=True)


def _record_stream_log(
    *,
    provider: str,
    model: str,
    surface: str | None,
    session_id: str | None,
    user_id: int | None,
    messages: list[Message],
    tools: list[ToolSpec] | None,
    max_tokens: int | None,
    response_text: str | None,
    response_tool_calls: list[ToolCall],
    finish_reason: str | None,
    latency_ms: int,
    error: str | None,
) -> None:
    """Append one prompt-log entry for a streaming call (W59).

    Aggregates the per-chunk deltas the streaming wrapper accumulates;
    one line per LLM call, not per chunk.
    """
    try:
        from flowfile_core.ai.prompt_log import (
            build_entry_from_stream,
            is_logging_enabled,
            log_prompt,
        )

        if not is_logging_enabled():
            return
        entry = build_entry_from_stream(
            provider=provider,
            model=model,
            messages=messages,
            tools=tools,
            max_tokens=max_tokens,
            response_text=response_text,
            response_tool_calls=response_tool_calls,
            finish_reason=finish_reason,
            latency_ms=latency_ms,
            error=error,
            surface=surface,
            session_id=session_id,
            user_id=user_id,
        )
        log_prompt(entry)
    except Exception:  # noqa: BLE001 — prompt-log must not crash the call path
        logger.warning("prompt_log: stream-side write failed", exc_info=True)
