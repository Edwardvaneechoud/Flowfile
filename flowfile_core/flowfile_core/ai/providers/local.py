"""Local model adapter — on-demand llama.cpp server, fully offline.

The Flowfile-managed ``llama-server`` (see
:mod:`flowfile_core.ai.local_model.manager`) exposes an OpenAI-compatible
API on ``http://127.0.0.1:<port>/v1``, so litellm's ``openai/`` route
drives it with a per-call ``api_base``. No BYOK, no tools — this provider
backs read-only / text surfaces (chat, fix-error, docgen, lineage, inline
actions, …) where a small CPU model is viable. The tool-calling agent
rejects it explicitly (see ``agent_routes``).

It is **not** registered in the BYOK ``PROVIDERS`` map — instead
``byok.get_configured_provider`` special-cases ``LOCAL_PROVIDER_ID`` and
returns a ``LocalProvider`` directly. The server's base URL isn't known
until the process is booted, so ``chat`` / ``stream`` lazy-boot it on first
call (idempotent) rather than requiring the caller to pass ``api_base``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider
from flowfile_core.ai.providers.base import ChatResponse, Message, StreamChunk, ToolSpec

# Stable wire id the frontend + every read-only route use to mean "the local
# model". Kept here (not in registry's PROVIDERS) so it's importable without
# pulling local into the BYOK provider set.
LOCAL_PROVIDER_ID = "local"

# llama-server ignores the key; litellm's openai route requires a non-empty one.
_LOCAL_API_KEY = "sk-local"


class LocalProvider(LiteLLMProvider):
    name: ClassVar[str] = LOCAL_PROVIDER_ID
    # Cosmetic — llama-server serves whichever GGUF is loaded regardless of the
    # model name in the request. Kept aligned with the catalog default.
    default_model: ClassVar[str] = "qwen2.5-coder-3b"
    # litellm routes ``openai/<name>`` + ``api_base`` to any OpenAI-compatible
    # server; llama-server speaks exactly that shape.
    model_prefix: ClassVar[str] = "openai/"
    supports_tools: ClassVar[bool] = False
    supports_streaming: ClassVar[bool] = True
    # Resolved lazily from the running server's port on first chat/stream call.
    default_api_base: ClassVar[str | None] = None

    async def _ensure_ready(self) -> None:
        """Boot the local server (if needed) and point this provider at it.

        ``manager.ensure_running`` is blocking (downloads nothing, but polls
        ``/health`` on cold boot) so it's offloaded to a worker thread. The
        ``manager`` import stays in-method to keep the providers package cheap
        to import and free of the manager's subprocess machinery at load time.
        Raises ``LocalModelNotInstalled`` (a ``LocalModelError``) if the files
        aren't present — that surfaces to the SSE layer as ``event: error``.
        """
        from flowfile_core.ai.local_model import manager

        base_url = await asyncio.to_thread(manager.ensure_running)
        self.api_base = base_url
        self.api_key = _LOCAL_API_KEY

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
        await self._ensure_ready()
        return await super().chat(
            messages,
            tools=tools,
            max_tokens=max_tokens,
            response_format=response_format,
            surface=surface,
            session_id=session_id,
            user_id=user_id,
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
        await self._ensure_ready()
        async for chunk in super().stream(
            messages,
            tools=tools,
            max_tokens=max_tokens,
            surface=surface,
            session_id=session_id,
            user_id=user_id,
        ):
            yield chunk
