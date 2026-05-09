"""HTTP route for the "Fix with AI" run-failure surface.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
this through the parent ``ai_router``.

Like ``chat_routes``, this surface is read-only by construction —
``tools=None`` is passed to the provider's ``stream()`` so no
``tool_call_delta`` is ever emitted. The Assist surface ships text
only; the executor / diff layer owns the tool-driven apply path.

The single endpoint takes ``{flow_id, node_id, ...}``, looks up the
``FlowGraph`` via the existing ``flow_file_handler`` singleton,
builds a schema-grounded :class:`PromptContext` via
:func:`flowfile_core.ai.context.render_prompt_context` (surface
``"explain"``), then appends a structured ``## Failure`` block
carrying the error string + a "diagnose and suggest a fix"
instruction. The composed ``[system, user]`` message pair is
streamed through the SSE primitives exactly so the wire format
matches ``/ai/chat/stream``.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` so BYOK rows +
env-var fallback + surface-keyed model defaults are honoured
identically to the chat route.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.context import render_prompt_context
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


SamplesMode = Literal["off", "regex"]


class ExplainRunFailureRequest(BaseModel):
    """Body for ``POST /ai/explain_run_failure``.

    ``error_message`` is optional — when omitted, the server reads the most
    recent matching ``NodeResult`` from ``flow.latest_run_info``. Allowing
    the client to override is what makes this useable when the user has
    edited the node since the last run (the cached error stays useful as
    a hint for the AI even though the settings have moved on).

    ``samples_mode`` defaults to ``"off"``. The frontend doesn't
    expose a UI toggle today; a per-flow safety config workstream
    will surface it.
    """

    flow_id: int
    node_id: int
    provider: str = Field(min_length=1)
    model: str | None = None
    error_message: str | None = None
    samples_mode: SamplesMode = "off"
    max_tokens: int | None = Field(default=None, gt=0)


def _ensure_known_provider(name: str) -> None:
    if name not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {list_supported_providers()}",
        )


def _resolve_error_message(flow, node_id: int, override: str | None) -> str:
    """Return the error string to surface to the LLM.

    Priority: client-supplied ``override`` first (so the frontend can
    pass a fresh string when a stale ``latest_run_info`` would mislead),
    then the latest matching failed ``NodeResult`` from the flow's last
    run. Returns an empty string if neither yields content; the route
    raises 422 in that case.
    """

    if override is not None and override.strip():
        return override.strip()

    run_info = getattr(flow, "latest_run_info", None)
    if run_info is None:
        return ""

    # Walk in reverse so the most recent matching failure wins when a
    # node has been retried within a single run.
    for result in reversed(run_info.node_step_result):
        if result.node_id == node_id and result.success is False and result.error:
            return str(result.error).strip()
    return ""


def _compose_failure_user_message(*, rendered_user: str, node_label: str, node_id: int, error_text: str) -> str:
    """Build the user-message body the LLM sees.

    Re-uses the deterministic ``ctx.user`` (subgraph + schemas +
    settings) verbatim so its prompt-cache hash stays stable, then
    appends a single ``## Failure`` block with the error string and a
    read-only Assist instruction.
    """

    return (
        f"{rendered_user}\n\n"
        f"## Failure\n\n"
        f"Node `{node_label}` (id `{node_id}`) failed during the most recent run "
        f"with this error:\n\n"
        f"```\n{error_text}\n```\n\n"
        "Explain what's wrong in plain language and suggest a concrete fix. "
        "Do not propose graph mutations — this is read-only assist; the user "
        "will apply any change manually."
    )


@router.post("/explain_run_failure", tags=["ai"])
async def explain_run_failure(
    body: ExplainRunFailureRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Stream an Assist-level explanation + suggested fix for a failed node.

    Errors before the stream opens:

    * ``404`` — provider name is not in :data:`PROVIDERS`.
    * ``409`` — no BYOK row, no env-var fallback, not Ollama
      (:class:`ProviderNotConfiguredError`).
    * ``422`` — flow not found, node not found, no recorded failure for
      the node, or Pydantic validation.
    * ``503`` — ``FEATURE_FLAG_AI`` is off (inherited from the parent
      router-level dependency).

    Once the stream opens, transient provider errors surface as
    ``event: error`` payloads from :func:`sse_stream` and the response
    closes — same posture as :mod:`flowfile_core.ai.chat_routes`.
    """

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

    error_text = _resolve_error_message(flow, body.node_id, body.error_message)
    if not error_text:
        raise HTTPException(
            status_code=422,
            detail=(
                f"No recorded failure for node {body.node_id} in flow {body.flow_id}. "
                "Run the flow first or pass error_message explicitly."
            ),
        )

    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            body.provider,
            surface="explain",
            model=body.model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        # Defence-in-depth — _ensure_known_provider already covers the
        # PROVIDERS-side mapping, but provider_factory has its own check.
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    ctx = render_prompt_context(
        flow,
        [body.node_id],
        surface="explain",
        samples_mode=body.samples_mode,
    )

    node_label = getattr(node, "name", None) or getattr(node, "node_type", None) or f"node-{body.node_id}"
    failure_user = _compose_failure_user_message(
        rendered_user=ctx.user,
        node_label=node_label,
        node_id=body.node_id,
        error_text=error_text,
    )
    messages = [
        Message(role="system", content=ctx.system),
        Message(role="user", content=failure_user),
    ]

    provider_stream = provider.stream(
        messages=messages,
        tools=None,
        max_tokens=body.max_tokens,
    )

    return make_streaming_response(sse_stream(provider_stream))


__all__ = ["router"]
