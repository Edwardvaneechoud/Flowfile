"""``Provider`` Protocol and supporting MCP-shaped (D004) message / tool types.

Owned by W11. Per plan §6.2 — every per-vendor adapter conforms to this
Protocol so retry / scheduler (W14), audit / metrics (W15), and SSE encoding
(W13) can wrap a single seam without provider-specific branches.

The dotted ``ToolSpec.name`` (e.g. ``flowfile.graph.add_filter``) is the MCP
naming convention from D004. ``ToolSpec.parameters`` is JSON-Schema-2020-12
and is passed through to litellm's standard ``tools[].function`` shape.
"""

from collections.abc import AsyncIterator
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

Role = Literal["system", "user", "assistant", "tool"]


class ToolCall(BaseModel):
    """A model-emitted tool call.

    ``id`` is the provider-issued correlation token used to thread the tool
    result back via a follow-up message with ``role="tool"``.
    """

    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class Message(BaseModel):
    """A single chat message in the canonical (provider-agnostic) shape."""

    role: Role
    content: str | None = None
    tool_calls: list[ToolCall] | None = None
    tool_call_id: str | None = None
    name: str | None = None


class ToolSpec(BaseModel):
    """An MCP-shaped tool the model may call."""

    name: str
    description: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class Usage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChatResponse(BaseModel):
    """Result of a single non-streaming chat call."""

    model: str
    content: str | None = None
    tool_calls: list[ToolCall] = Field(default_factory=list)
    finish_reason: str | None = None
    usage: Usage = Field(default_factory=Usage)


class StreamChunk(BaseModel):
    """A delta from a streaming response.

    Tool-call deltas arrive incrementally from most providers; we surface a
    ``tool_call_delta`` only once the call is structurally complete (id +
    name + parsable arguments) so downstream consumers don't have to
    re-implement the buffering. ``content_delta`` is plain-text streaming.
    """

    model_config = ConfigDict(extra="forbid")

    content_delta: str | None = None
    tool_call_delta: ToolCall | None = None
    finish_reason: str | None = None


@runtime_checkable
class Provider(Protocol):
    """Common interface for LLM backends.

    Implementations live in ``providers/{name}.py`` and share the litellm
    backing via ``providers/_litellm_base.py``. The seam is intentionally
    thin so W14 (rate-limit scheduler) and W15 (audit log) can wrap the
    Provider without provider-specific awareness.
    """

    name: str
    model: str
    supports_tools: bool
    supports_streaming: bool
    # TODO(W11): Unresolved attribute reference 'default_model' for class 'Provider'.
    # LiteLLMProvider subclasses expose `default_model: ClassVar[str]` but the
    # Protocol doesn't declare it, so static analysers warn at call sites like
    # `p.default_model`. Resolution: either add `default_model: str` to the
    # Protocol surface, or migrate call sites to `.model` (the resolved model
    # after surface routing per D010).

    async def chat(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
        max_tokens: int | None = None,
    ) -> ChatResponse: ...

    def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
        max_tokens: int | None = None,
    ) -> AsyncIterator[StreamChunk]: ...
