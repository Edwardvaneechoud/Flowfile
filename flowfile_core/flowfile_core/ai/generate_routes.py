"""Provider-agnostic whole-flow generation (``POST /ai/generate``).

The "Simple build" + "one-shot" surfaces: the model emits a whole
``{nodes, edges}`` flow in one response, which we register as a
:class:`~flowfile_core.ai.diff.GraphDiff` for the frontend's "Add to canvas"
button. Works for ANY resolvable provider (local or cloud) via
:func:`get_configured_provider`; the local model boots lazily inside
``LocalProvider.stream``/``chat`` on first use.

``mode``:
  * ``"simple"`` — build the diff directly, no validation until apply (cheap;
    good for small local models).
  * ``"one_shot"`` — validate each node through the executor (schema
    prediction + dry-run); for bigger models.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`; the feature-flag
gate + auth apply via the parent router.
"""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.local_model import manager, oneshot
from flowfile_core.ai.providers import UnknownProviderError, is_resolvable_provider, resolvable_provider_names
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter()

# Generation surface uses the local-model-tuned budget; cloud providers ignore
# the per-surface model routing here and use the credential default.
SURFACE = "local_oneshot"


class GenerateFlowRequest(BaseModel):
    """Body for ``POST /ai/generate``."""

    flow_id: int = Field(ge=0)
    user_request: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    model: str | None = None
    mode: Literal["simple", "one_shot"] = "simple"
    max_tokens: int | None = Field(default=None, gt=0)


@router.post("/generate", tags=["ai"])
async def generate_flow_route(
    body: GenerateFlowRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> dict:
    """Generate a whole flow from one prompt and register it as a GraphDiff.

    Returns ``{diff_id, op_count, created, warnings, rationale, diff_payload}``;
    the frontend's "Add to canvas" applies it via the existing diff-accept
    route. Writer / sink nodes are never created — the user attaches the
    destination after inserting.

    Errors: 404 unknown provider · 409 provider not configured / local model
    not installed · 422 flow not found / unparseable model output · 503 AI off
    or local server failed to boot.
    """
    if not is_resolvable_provider(body.provider):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {body.provider!r}; supported: {resolvable_provider_names()}",
        )

    flow = flow_file_handler.get_flow(body.flow_id)
    if flow is None:
        raise HTTPException(status_code=422, detail=f"Flow {body.flow_id} not found")

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
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        return await oneshot.generate_flow(
            provider=provider,
            flow=flow,
            flow_id=body.flow_id,
            user_id=current_user.id,
            user_request=body.user_request,
            max_tokens=body.max_tokens,
            mode=body.mode,
        )
    except oneshot.OneShotError as exc:
        raise HTTPException(status_code=422, detail=f"Could not generate a flow: {exc}") from exc
    except manager.LocalModelNotInstalled as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except manager.LocalModelError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


__all__ = ["router"]
