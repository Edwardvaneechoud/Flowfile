"""HTTP routes for the optional on-demand local LLM (llama.cpp + small GGUF).

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`, so the
``FEATURE_FLAG_AI`` gate + auth apply. Nothing here downloads or runs a model
until the user explicitly calls ``/local-model/install`` then
``/local-model/start`` (or first chat / generate triggers a lazy boot) —
matching the "install only when wanted" contract.

The server is a single shared process (not per-user); install / start / stop
are global. Chat + one-shot generation route through
:class:`~flowfile_core.ai.providers.local.LocalProvider` directly (the local
model isn't a BYOK provider, so it never touches ``get_configured_provider``).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from flowfile_core import flow_file_handler
from flowfile_core.ai.local_model import manager, oneshot
from flowfile_core.ai.providers import Message
from flowfile_core.ai.providers.local import LocalProvider
from flowfile_core.ai.streaming import (
    SSEEvent,
    format_sse_error,
    make_streaming_response,
    sse_stream,
)
from flowfile_core.auth.jwt import get_current_active_user

logger = logging.getLogger(__name__)

router = APIRouter()

# llama-server ignores the key; litellm's openai route requires a non-empty string.
_LOCAL_API_KEY = "sk-local"

_CHAT_SYSTEM_PROMPT = (
    "You are Flowfile's built-in local assistant, running fully offline on the "
    "user's machine. Answer questions about building data pipelines in Flowfile "
    "concisely and accurately. You cannot change the flow from chat; to build a "
    "flow, the user uses the 'Generate flow' action."
)


@router.get("/local-model/status", tags=["ai"])
def local_model_status(current_user=Depends(get_current_active_user)) -> dict:
    """Install / run state for the local model card in AI settings."""
    return manager.status()


@router.post("/local-model/install", tags=["ai"])
async def local_model_install(current_user=Depends(get_current_active_user)):
    """Download + install the binary and model, streaming progress as SSE.

    Events: ``event: progress`` (``{phase, received, total}``) per chunk,
    then a terminal progress event ``{phase: "done"}``; ``event: error`` on
    failure. The blocking install runs in a worker thread; a thread-safe queue
    bridges its progress callback to this async generator.
    """
    if not manager.is_available():
        raise HTTPException(status_code=422, detail=manager.unsupported_platform_detail())

    async def gen():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        sentinel = object()

        def on_progress(ev: dict) -> None:
            loop.call_soon_threadsafe(queue.put_nowait, ev)

        def run() -> None:
            try:
                manager.install(on_progress=on_progress)
            except Exception as exc:  # noqa: BLE001 — surfaced to the client as event:error
                logger.warning("local model install failed: %s", exc)
                loop.call_soon_threadsafe(queue.put_nowait, {"phase": "error", "message": str(exc)})
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, sentinel)

        fut = loop.run_in_executor(None, run)
        try:
            while True:
                ev = await queue.get()
                if ev is sentinel:
                    break
                if ev.get("phase") == "error":
                    yield format_sse_error(ev.get("message", "install failed"))
                else:
                    yield SSEEvent(event="progress", data=json.dumps(ev)).format()
        finally:
            await fut

    return make_streaming_response(gen())


@router.post("/local-model/start", tags=["ai"])
async def local_model_start(current_user=Depends(get_current_active_user)) -> dict:
    """Boot the local server (idempotent). Returns the updated status."""
    try:
        await asyncio.to_thread(manager.ensure_running)
    except manager.LocalModelNotInstalled as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except manager.LocalModelError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return manager.status()


@router.post("/local-model/stop", tags=["ai"])
async def local_model_stop(current_user=Depends(get_current_active_user)) -> dict:
    """Stop the local server (idempotent). Returns the updated status."""
    await asyncio.to_thread(manager.stop)
    return manager.status()


@router.delete("/local-model", tags=["ai"])
async def local_model_delete(current_user=Depends(get_current_active_user)) -> dict:
    """Stop the server and remove the binary + model from disk."""
    await asyncio.to_thread(manager.uninstall)
    return manager.status()


class LocalChatMessage(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1)


class LocalChatRequest(BaseModel):
    messages: list[LocalChatMessage] = Field(min_length=1)
    max_tokens: int | None = Field(default=None, gt=0)


@router.post("/local-model/chat", tags=["ai"])
async def local_model_chat(body: LocalChatRequest, current_user=Depends(get_current_active_user)):
    """Stream a plain (tool-free) chat completion from the local model as SSE.

    Lazy-boots the server if needed. Same SSE wire format as
    ``/ai/chat/stream`` (``event: chunk`` / ``event: done`` / ``event: error``).
    """
    try:
        base_url = await asyncio.to_thread(manager.ensure_running)
    except manager.LocalModelNotInstalled as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except manager.LocalModelError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    provider = LocalProvider(api_base=base_url, api_key=_LOCAL_API_KEY)
    messages = [Message(role="system", content=_CHAT_SYSTEM_PROMPT)]
    messages += [Message(role=m.role, content=m.content) for m in body.messages]
    stream = provider.stream(messages=messages, tools=None, max_tokens=body.max_tokens)
    return make_streaming_response(sse_stream(stream))


class LocalGenerateRequest(BaseModel):
    user_request: str = Field(min_length=1)
    flow_id: int
    max_tokens: int | None = Field(default=None, gt=0)


@router.post("/local-model/generate", tags=["ai"])
async def local_model_generate(
    body: LocalGenerateRequest,
    current_user=Depends(get_current_active_user),
) -> dict:
    """Generate a whole flow from one prompt (non-agentic) and stage it as a diff.

    Returns ``{diff_id, op_count, created, warnings, rationale}``; the existing
    diff-accept UI inserts the staged nodes onto the canvas. Writer / sink nodes
    are never created (the executor blocks them) — the user attaches the
    destination after inserting.
    """
    flow = flow_file_handler.get_flow(body.flow_id)
    if flow is None:
        raise HTTPException(status_code=422, detail=f"Flow {body.flow_id} not found")

    try:
        base_url = await asyncio.to_thread(manager.ensure_running)
    except manager.LocalModelNotInstalled as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except manager.LocalModelError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    provider = LocalProvider(api_base=base_url, api_key=_LOCAL_API_KEY)
    try:
        return await oneshot.generate_flow(
            provider=provider,
            flow=flow,
            flow_id=body.flow_id,
            user_id=current_user.id,
            user_request=body.user_request,
            max_tokens=body.max_tokens,
        )
    except oneshot.OneShotError as exc:
        raise HTTPException(status_code=422, detail=f"Could not generate a flow: {exc}") from exc


__all__ = ["router"]
