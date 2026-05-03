"""HTTP route for the Cmd+K command palette (W33).

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; W17's feature-flag gate covers this
through the parent ``ai_router``.

The route resolves the flow + provider identically to W34's autocomplete
routes, then calls :func:`flowfile_core.ai.command_palette.run_command_palette`.
That function never raises — soft failures become a 200 response with
``degraded=true``. The route still maps the obvious 4xx cases up front:

* ``404`` — provider name is not in :data:`PROVIDERS`.
* ``409`` — :class:`ProviderNotConfiguredError`.
* ``422`` — flow not found; Pydantic validation; missing-required-fields.
* ``503`` — ``FEATURE_FLAG_AI`` off (inherited router-level).

Once the LLM call starts, every soft failure surfaces inside the
:class:`CommandPaletteResponse` so the frontend renders a single error
state. The diff (if any) is registered with W41's ``DiffStore`` before
the response is returned, so the frontend's call to
``useAiDiffStore.setCurrentDiff(response.diff)`` is enough to wire up
accept/reject without an extra round-trip.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.command_palette import (
    SURFACE,
    CommandPaletteRequest,
    CommandPaletteResponse,
    run_command_palette,
)
from flowfile_core.ai.providers import (
    PROVIDERS,
    UnknownProviderError,
    list_supported_providers,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


def _ensure_known_provider(name: str) -> None:
    if name not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {list_supported_providers()}",
        )


def _resolve_flow(flow_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(status_code=422, detail=f"Flow {flow_id} not found")
    return flow


@router.post(
    "/command_palette",
    response_model=CommandPaletteResponse,
    tags=["ai"],
)
async def submit_command_palette(
    body: CommandPaletteRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> CommandPaletteResponse:
    """Run one Cmd+K command-palette request and return the staged diff.

    The route is thin — provider resolution + flow resolution + thin
    error mapping. The actual LLM call + W31 staging + W41 register all
    happen inside :func:`run_command_palette`.

    Errors before the LLM call:

    * ``404`` — unknown provider.
    * ``409`` — :class:`ProviderNotConfiguredError`.
    * ``422`` — flow not found; Pydantic validation.
    * ``503`` — ``FEATURE_FLAG_AI`` off (inherited).

    After the LLM call: soft failures are returned as 200 with
    ``degraded=true``. See :class:`CommandPaletteResponse`.
    """
    _ensure_known_provider(body.provider)
    flow = _resolve_flow(body.flow_id)

    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            body.provider,
            surface=SURFACE,
            model=body.model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        # Defence-in-depth — _ensure_known_provider already covers the
        # PROVIDERS-side mapping, but provider_factory has its own check.
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    session_id = body.session_id or f"cmdk-{uuid.uuid4().hex[:12]}"

    return await run_command_palette(
        flow,
        prompt=body.prompt,
        provider=provider,
        session_id=session_id,
        user_id=int(current_user.id),
        selected_node_ids=body.selected_node_ids,
        insertion_context=body.insertion_context,
        max_tokens=body.max_tokens,
        timeout=body.timeout,
    )


__all__ = ["router"]
