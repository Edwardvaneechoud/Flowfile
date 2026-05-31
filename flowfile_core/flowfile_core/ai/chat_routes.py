"""HTTP route for the read-only chat surface.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
this through the parent ``ai_router``.

This route is intentionally read-only — no tools are passed to the
provider's ``stream()``, no tool-call branch ever fires. Tool
execution lands in the executor / planner against the same provider
seam.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` so BYOK rows +
env-var fallback + surface-keyed model defaults are honoured
identically to the BYOK test ping.

Every request prepends a server-issued ``system`` message built from
``body.surface`` (defaulting to ``"explain"`` → the ``assist`` level
per ``SURFACE_TO_LEVEL``) via
:func:`flowfile_core.ai.context.assemble_system_prompt` so the LLM is
always grounded in Flowfile's contract before it sees the client's
first turn. Client-supplied ``system`` messages remain accepted and
follow the server prompt in order.

The SSE wire format: ``event: chunk`` deltas, ``event: done`` finish,
keepalive comments every 15s.
"""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.context import (
    SURFACE_TO_LEVEL,
    assemble_system_prompt,
    render_prompt_context,
)
from flowfile_core.ai.providers import (
    LOCAL_PROVIDER_ID,
    Message,
    UnknownProviderError,
    is_resolvable_provider,
    resolvable_provider_names,
)
from flowfile_core.ai.streaming import make_streaming_response, sse_stream
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


_ALLOWED_ROLES: tuple[str, ...] = ("system", "user", "assistant")

# Vanilla chat has no inherent "surface" of its own — it's the
# general-purpose drawer. Default to ``"explain"`` so we land on the
# ``assist``-level suffix (concise, read-only, no graph-mutation
# talk), which matches what the chat drawer can actually do today.
_DEFAULT_SURFACE: str = "explain"


class ChatMessageInput(BaseModel):
    """A single message in the chat request payload.

    The ``tool`` role is intentionally excluded at this surface — chat
    is read-only by construction, so the only roles a client can send
    are ``system`` (also accepted from the client today) and ``user``
    / ``assistant`` for the back-and-forth.
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
    # When ``flow_id`` is set, the route resolves the live
    # ``FlowGraph`` via :mod:`flowfile_core.flow_file_handler` and
    # pipes it through :func:`render_prompt_context` so the model sees
    # the actual subgraph + schemas instead of an identity-only prompt.
    # ``selected_node_ids`` narrows the pinned set. ``mentions``
    # carries the parsed mention strings (e.g. ``["@flow",
    # "@node:filter_3"]``) when the client passes them; when nothing is
    # pinned and no mentions are given, we auto-expand to ``"@flow"``
    # so the model can still answer "what is this flow about?". When
    # ``flow_id`` is omitted, behaviour falls back to identity-only —
    # keeps the contract backwards-compatible for any caller that
    # doesn't have a flow context to share.
    flow_id: int | None = None
    selected_node_ids: list[int] | None = None
    mentions: list[str] | None = None
    chat_mode: Literal["chat", "auto_agent"] | None = None
    """Selects which escalation footer the chat agent uses. ``"chat"``
    swaps the assist.md default for an agent-toggle-only footer (since
    auto-promotion is off in that mode)."""


def _ensure_known_provider(name: str) -> None:
    if not is_resolvable_provider(name):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {resolvable_provider_names()}",
        )


def _to_provider_messages(payload: list[ChatMessageInput]) -> list[Message]:
    """Map the API payload onto the provider-layer ``Message`` shape.

    The route doesn't accept ``tool_calls`` or ``tool_call_id`` from
    the client — those only flow back from the provider in
    non-streaming responses, and chat is read-only anyway.
    """
    return [Message(role=msg.role, content=msg.content) for msg in payload]


def _chat_mode_footer_override(
    chat_mode: Literal["chat", "auto_agent"] | None,
) -> str | None:
    if chat_mode != "chat":
        return None
    return (
        "MODE OVERRIDE — user is in CHAT-ONLY mode. The assist.md "
        "footer mentions 'do it' / 'implement' as auto-switch words; "
        "those WILL NOT WORK in chat-only mode (no auto-promotion is "
        "running). When you reply to any 'add this node' / 'do X' "
        "request, REPLACE the footer with this exact 2-line block:\n"
        "> \"Chat mode can't change the flow. Switch the chat to "
        "**agent** mode (toggle at the bottom of the drawer) and ask "
        "again — I'll stage the steps for you there.\""
    )


def _resolve_prompt_surface(requested: str | None) -> str:
    """Pick the surface used for system-prompt assembly.

    A request may set ``surface`` to anything for model selection, but
    only known surfaces participate in the layered prompt. Unknown or
    missing values silently fall back to :data:`_DEFAULT_SURFACE`
    rather than 422 — the chat drawer is general-purpose and we'd
    rather ground every call than reject one.
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

    # When the client sends a ``flow_id``, build a context-rich prompt
    # (subgraph snapshot + per-node schemas + optional samples).
    # Otherwise fall back to the identity-only prompt so the contract
    # stays backwards-compatible for callers without a flow.
    if body.flow_id is not None:
        flow = flow_file_handler.get_flow(body.flow_id)
        if flow is None:
            raise HTTPException(status_code=422, detail=f"Flow {body.flow_id} not found")
        # If the client passed parsed mentions, forward them as a
        # single space-joined string — ``render_prompt_context`` parses
        # raw text via ``_coerce_mentions``. When neither mentions nor
        # a selection are given, default to ``@flow`` so the chat is
        # still context-grounded by default. Explicit selections +
        # explicit mentions both win over the auto-expand default.
        mention_text: str
        if body.mentions:
            mention_text = " ".join(body.mentions)
        elif not body.selected_node_ids:
            mention_text = "@flow"
        else:
            mention_text = ""
        logger.debug(
            "chat_stream render_prompt_context flow_id=%s surface=%s selection=%s mentions=%r samples_mode=off",
            body.flow_id,
            prompt_surface,
            body.selected_node_ids,
            mention_text,
        )
        ctx = render_prompt_context(
            flow,
            body.selected_node_ids or [],
            surface=prompt_surface,
            mentions=mention_text or None,
            samples_mode="off",
            # Local model has a small context window; shrink verbose node
            # settings (column/field lists, code bodies) and cap columns so a
            # wide source node can't overflow the window.
            compact_settings=body.provider == LOCAL_PROVIDER_ID,
            max_columns_per_node=12 if body.provider == LOCAL_PROVIDER_ID else None,
        )
        messages = [
            Message(role="system", content=ctx.system),
            Message(role="user", content=ctx.user),
            *_to_provider_messages(body.messages),
        ]
    else:
        system_prompt = assemble_system_prompt(prompt_surface)
        messages = [
            Message(role="system", content=system_prompt),
            *_to_provider_messages(body.messages),
        ]

    footer_override = _chat_mode_footer_override(body.chat_mode)
    if footer_override is not None:
        messages.append(Message(role="system", content=footer_override))

    provider_stream = provider.stream(
        messages=messages,
        tools=None,
        max_tokens=body.max_tokens,
    )

    return make_streaming_response(sse_stream(provider_stream))


class ChatPreviewMessage(BaseModel):
    """One message in a preview response."""

    role: str
    content: str
    char_count: int


class ChatPreviewResponse(BaseModel):
    """The assembled prompt that would be sent to the LLM, as JSON.

    Inspection-only — no provider call is made. Useful when debugging
    "why is the model saying X?" without burning provider quota.
    """

    provider: str
    model: str
    surface: str
    prompt_surface: str
    messages: list[ChatPreviewMessage]
    total_chars: int
    estimated_tokens: int


@router.post(
    "/chat/preview",
    tags=["ai"],
    response_model=ChatPreviewResponse,
)
async def chat_preview(
    body: ChatStreamRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> ChatPreviewResponse:
    """Return the exact messages that ``POST /ai/chat/stream`` would send,
    as JSON, without calling the provider.

    Same body schema as ``/chat/stream``. Same error mapping for the
    "before stream" cases (404 / 409 / 422 / 503). Useful for diagnosing
    "the chat said X — what context did it actually see?".

    The response is plain JSON (not SSE) so any HTTP client can read it.
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
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    prompt_surface = _resolve_prompt_surface(body.surface)

    if body.flow_id is not None:
        flow = flow_file_handler.get_flow(body.flow_id)
        if flow is None:
            raise HTTPException(status_code=422, detail=f"Flow {body.flow_id} not found")
        mention_text: str
        if body.mentions:
            mention_text = " ".join(body.mentions)
        elif not body.selected_node_ids:
            mention_text = "@flow"
        else:
            mention_text = ""
        ctx = render_prompt_context(
            flow,
            body.selected_node_ids or [],
            surface=prompt_surface,
            mentions=mention_text or None,
            compact_settings=body.provider == LOCAL_PROVIDER_ID,
            max_columns_per_node=12 if body.provider == LOCAL_PROVIDER_ID else None,
        )
        messages = [
            Message(role="system", content=ctx.system),
            Message(role="user", content=ctx.user),
            *_to_provider_messages(body.messages),
        ]
    else:
        system_prompt = assemble_system_prompt(prompt_surface)
        messages = [
            Message(role="system", content=system_prompt),
            *_to_provider_messages(body.messages),
        ]

    preview_messages: list[ChatPreviewMessage] = []
    total_chars = 0
    for msg in messages:
        content = msg.content or ""
        char_count = len(content)
        total_chars += char_count
        preview_messages.append(ChatPreviewMessage(role=msg.role, content=content, char_count=char_count))

    return ChatPreviewResponse(
        provider=body.provider,
        # ``provider.model`` is the resolved model after BYOK / surface-keyed
        # default lookup — what the actual stream call would use.
        model=getattr(provider, "model", body.model or ""),
        surface=body.surface or "",
        prompt_surface=prompt_surface,
        messages=preview_messages,
        total_chars=total_chars,
        # Same chars/4 estimator the budget module uses (no tiktoken pull).
        estimated_tokens=total_chars // 4,
    )


__all__ = ["router"]
