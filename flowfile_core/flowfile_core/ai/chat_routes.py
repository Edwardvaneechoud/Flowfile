"""HTTP route for the read-only chat surface (W20).

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; W17's feature-flag gate covers this
through the parent ``ai_router``.

This route is intentionally read-only — no tools are passed to the
provider's ``stream()``, no tool-call branch ever fires. Tool execution
lands in W31 / W40 against the same provider seam.

Provider resolution flows through :func:`flowfile_core.ai.byok.get_configured_provider`
so BYOK rows + env-var fallback + surface-keyed model defaults are honoured
identically to the BYOK test ping in W12.

W26 wires :func:`flowfile_core.ai.context.assemble_system_prompt` in: every
request prepends a server-issued ``system`` message built from
``body.surface`` (defaulting to ``"explain"`` → the ``assist`` level per
W22's ``SURFACE_TO_LEVEL``) so the LLM is always grounded in Flowfile's
contract before it sees the client's first turn. Client-supplied ``system``
messages remain accepted and follow the server prompt in order.

The SSE wire format mirrors W13 exactly — ``event: chunk`` deltas,
``event: done`` finish, keepalive comments every 15s. W42 will add
``Last-Event-ID`` resumption — that won't change today's contract.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.context import SURFACE_TO_LEVEL, assemble_system_prompt
from flowfile_core.ai.providers import (
    PROVIDERS,
    Message,
    UnknownProviderError,
    list_supported_providers,
)
from flowfile_core.ai.streaming import make_streaming_response, sse_stream
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


_ALLOWED_ROLES: tuple[str, ...] = ("system", "user", "assistant")

# Vanilla chat has no inherent "surface" of its own — it's the general-purpose
# drawer. Default to ``"explain"`` so we land on W22's ``assist``-level suffix
# (concise, read-only, no graph-mutation talk), which matches what the chat
# drawer can actually do today.
_DEFAULT_SURFACE: str = "explain"


class ChatMessageInput(BaseModel):
    """A single message in the chat request payload.

    The ``tool`` role is intentionally excluded at this surface — W20 is
    read-only by construction, so the only roles a client can send are
    ``system`` (reserved for future W22 layered-prompt injection at the
    route layer, but accepted from the client today) and ``user`` /
    ``assistant`` for the back-and-forth.
    """

    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1)


class ChatStreamRequest(BaseModel):
    """Body for ``POST /ai/chat/stream``."""

    provider: str = Field(min_length=1)
    model: str | None = None
    surface: str | None = None
    messages: list[ChatMessageInput] = Field(min_length=1)
    max_tokens: int | None = Field(default=None, gt=0)


def _ensure_known_provider(name: str) -> None:
    if name not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {list_supported_providers()}",
        )


def _to_provider_messages(payload: list[ChatMessageInput]) -> list[Message]:
    """Map the API payload onto the provider-layer ``Message`` shape.

    The route doesn't accept ``tool_calls`` or ``tool_call_id`` from the
    client — those only flow back from the provider in non-streaming
    responses, and W20 is read-only anyway.
    """
    return [Message(role=msg.role, content=msg.content) for msg in payload]


def _resolve_prompt_surface(requested: str | None) -> str:
    """Pick the surface used for system-prompt assembly.

    A request may set ``surface`` to anything for model selection (W12 /
    D010), but only known surfaces participate in the W22 layered prompt.
    Unknown or missing values silently fall back to :data:`_DEFAULT_SURFACE`
    rather than 422 — the chat drawer is general-purpose and we'd rather
    ground every call than reject one.
    """
    if requested and requested in SURFACE_TO_LEVEL:
        return requested
    return _DEFAULT_SURFACE


@router.post("/chat/stream", tags=["ai"])
async def chat_stream(
    body: ChatStreamRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Stream a single chat completion as SSE.

    Read-only by construction — ``tools=None`` is passed to the provider so
    no ``tool_call_delta`` is ever produced and no graph mutation can
    happen via this surface.

    Errors before the stream opens:

    * ``404`` — provider name is not in :data:`PROVIDERS`.
    * ``409`` — no BYOK row, no env-var fallback, not Ollama
      (:class:`ProviderNotConfiguredError`).
    * ``503`` — ``FEATURE_FLAG_AI`` is off (inherited from the parent
      router-level dependency).
    * ``422`` — Pydantic validation (empty ``messages``, missing ``provider``,
      etc.).

    Once the stream opens, transient provider errors surface as
    ``event: error`` payloads from :func:`sse_stream` and the response
    closes.
    """
    _ensure_known_provider(body.provider)

    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            body.provider,
            surface=body.surface,
            model=body.model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        # Defence-in-depth — _ensure_known_provider already covers the
        # PROVIDERS-side mapping, but provider_factory has its own check.
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    prompt_surface = _resolve_prompt_surface(body.surface)
    system_prompt = assemble_system_prompt(prompt_surface)
    messages = [
        Message(role="system", content=system_prompt),
        *_to_provider_messages(body.messages),
    ]

    provider_stream = provider.stream(
        messages=messages,
        tools=None,
        max_tokens=body.max_tokens,
    )

    return make_streaming_response(sse_stream(provider_stream))


__all__ = ["router"]
