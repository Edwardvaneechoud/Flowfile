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

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from flowfile_core.ai.local_model import manager
from flowfile_core.ai.streaming import (
    SSEEvent,
    format_sse_error,
    make_streaming_response,
)
from flowfile_core.auth.jwt import get_current_active_user

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/local-model/status", tags=["ai"])
def local_model_status(current_user=Depends(get_current_active_user)) -> dict:
    """Install / run state for the local model card in AI settings."""
    return manager.status()


@router.post("/local-model/install", tags=["ai"])
async def local_model_install(
    model_id: str | None = None,
    current_user=Depends(get_current_active_user),
):
    """Download + install the shared binary and one catalog model, streaming SSE.

    ``model_id`` (query param) selects the model; omitted = catalog default.
    The installed model becomes the active selection. Events: ``event: progress``
    (``{phase, received, total, model_id}``) per chunk, terminal
    ``{phase: "done"}``; ``event: error`` on failure. The blocking install runs
    in a worker thread; a thread-safe queue bridges its progress callback here.
    """
    if not manager.is_available():
        raise HTTPException(status_code=422, detail=manager.unsupported_platform_detail())
    if model_id is not None and model_id not in manager.MODELS:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown model id {model_id!r}. Known: {sorted(manager.MODELS)}.",
        )

    async def gen():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        sentinel = object()

        def on_progress(ev: dict) -> None:
            loop.call_soon_threadsafe(queue.put_nowait, ev)

        def run() -> None:
            try:
                manager.install(model_id=model_id, on_progress=on_progress)
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


class LocalSelectRequest(BaseModel):
    model_id: str = Field(min_length=1)


@router.post("/local-model/select", tags=["ai"])
async def local_model_select(
    body: LocalSelectRequest,
    current_user=Depends(get_current_active_user),
) -> dict:
    """Make an already-installed model the active one (recycles a running
    server onto it). 409 if the model isn't installed yet."""
    try:
        await asyncio.to_thread(manager.set_active_model, body.model_id)
    except manager.LocalModelNotInstalled as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except manager.LocalModelError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return manager.status()


class LocalCtxSizeRequest(BaseModel):
    ctx_size: int = Field(ge=1)


@router.post("/local-model/ctx-size", tags=["ai"])
async def local_model_set_ctx_size(
    body: LocalCtxSizeRequest,
    current_user=Depends(get_current_active_user),
) -> dict:
    """Set the llama-server context window (clamped to the allowed range). If a
    server is running, it's stopped so the next use boots with the new size."""
    await asyncio.to_thread(manager.set_ctx_size, body.ctx_size)
    # Recycle so the change takes effect — the server only reads ctx-size at boot.
    await asyncio.to_thread(manager.stop)
    return manager.status()


@router.post("/local-model/start", tags=["ai"])
async def local_model_start(current_user=Depends(get_current_active_user)) -> dict:
    """Boot the selected model's server (idempotent). Returns the updated status."""
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
async def local_model_delete(
    model_id: str | None = None,
    current_user=Depends(get_current_active_user),
) -> dict:
    """Remove one model's GGUF (``model_id`` query param) or, when omitted, the
    whole runtime (binary + every model)."""
    if model_id is not None and model_id not in manager.MODELS:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown model id {model_id!r}. Known: {sorted(manager.MODELS)}.",
        )
    await asyncio.to_thread(manager.uninstall, model_id)
    return manager.status()


__all__ = ["router"]
