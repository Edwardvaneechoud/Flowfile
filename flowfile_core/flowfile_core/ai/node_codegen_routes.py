"""``POST /ai/generate_node_code`` — natural language → node code snippet.

Mounted under ``/ai``; auth + feature-flag gating come from the parent router.
Non-streaming JSON. This is the thin route surface: it resolves the
``FlowGraph`` + node, checks the node is code-bearing, resolves a BYOK provider,
and delegates the work to :mod:`flowfile_core.ai.node_codegen`.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` (surface ``"explain"``)
so BYOK rows + env-var fallback + surface-keyed model defaults are honoured the
same way the inline-action / cron routes do.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.node_codegen import (
    CODE_BEARING_NODE_TYPES,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_PROMPT_LEN,
    NodeCodeGenerationResponse,
    generate_node_code,
)
from flowfile_core.ai.providers import (
    PROVIDERS,
    UnknownProviderError,
    is_resolvable_provider,
    list_supported_providers,
    resolvable_provider_names,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


class GenerateNodeCodeRequest(BaseModel):
    """Body for ``POST /ai/generate_node_code``.

    ``provider`` is optional — ``None`` resolves to the first configured
    provider, mirroring ``/ai/generate_cron``.
    """

    flow_id: int
    node_id: int
    prompt: str = Field(min_length=1, max_length=MAX_PROMPT_LEN)
    provider: str | None = Field(default=None, min_length=1)
    model: str | None = None
    timeout: float = Field(default=DEFAULT_TIMEOUT_SECONDS, gt=0.0, le=60.0)


def _ensure_known_provider(name: str | None) -> None:
    if name is not None and not is_resolvable_provider(name):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {resolvable_provider_names()}",
        )


def _resolve_provider(db: Session, user_id: int, name: str | None, *, model: str | None):
    """Resolve a configured provider, preferring ``name`` if given.

    The generate-code button has no provider picker, so it must not dead-end when
    the caller's preferred provider (e.g. the chat-selected one) has no key. We
    try the preferred provider first, then fall back to any other configured
    provider; the per-call ``model`` only applies to the preferred one. Only when
    nothing at all is configured do we 409.
    """
    candidates: list[str] = []
    if name is not None:
        candidates.append(name)
    candidates.extend(p for p in PROVIDERS if p != name)

    for candidate in candidates:
        try:
            return get_configured_provider(
                db,
                user_id,
                candidate,
                surface="explain",
                model=model if candidate == name else None,
            )
        except (ProviderNotConfiguredError, UnknownProviderError):
            continue

    raise HTTPException(
        status_code=409,
        detail=(
            "No AI provider configured. Save an API key via "
            "POST /ai/providers/{name} or set the relevant env var. "
            f"Supported: {list_supported_providers()}"
        ),
    )


@router.post("/generate_node_code", response_model=NodeCodeGenerationResponse, tags=["ai"])
async def generate_node_code_route(
    body: GenerateNodeCodeRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> NodeCodeGenerationResponse:
    """404 unknown provider · 409 not configured · 422 bad flow/node/non-code-bearing
    · 503 AI off. Soft failures (timeout, parse error, declined) return 200 + degraded."""
    _ensure_known_provider(body.provider)

    flow = flow_file_handler.get_flow(body.flow_id)
    if flow is None:
        raise HTTPException(status_code=422, detail=f"Flow {body.flow_id} not found")

    node = flow.get_node(body.node_id)
    if node is None:
        raise HTTPException(
            status_code=422,
            detail=f"Node {body.node_id} not found in flow {body.flow_id}",
        )

    node_type = getattr(node, "node_type", None) or ""
    if node_type not in CODE_BEARING_NODE_TYPES:
        allowed = ", ".join(sorted(CODE_BEARING_NODE_TYPES))
        raise HTTPException(
            status_code=422,
            detail=(
                f"Code generation requires a code-bearing node (one of: {allowed}); "
                f"node {node_type!r} is not code-bearing."
            ),
        )

    provider = _resolve_provider(db, current_user.id, body.provider, model=body.model)

    return await generate_node_code(
        flow=flow,
        node_id=body.node_id,
        node_type=node_type,
        prompt=body.prompt,
        provider=provider,
        timeout=body.timeout,
    )


__all__ = ["router", "GenerateNodeCodeRequest"]
