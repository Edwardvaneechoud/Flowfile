"""HTTP route for the "Generate documentation" surface (W50).

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; W17's feature-flag gate covers this
through the parent ``ai_router``.

Like W20's ``chat_routes`` and W23's ``run_failure_routes``, this surface is
read-only by construction — ``tools=None`` is passed to the provider's
``stream()`` so no ``tool_call_delta`` is ever emitted. The route streams an
Assist-level markdown document describing every node in the flow.

The single endpoint takes ``{flow_id, ...}``, looks up the ``FlowGraph`` via
the existing ``flow_file_handler`` singleton, pins **all** ``flow.nodes``
ids into W22's :func:`flowfile_core.ai.context.render_prompt_context`
(surface ``"docgen"``), then appends a structured ``## Documentation
request`` block to ``ctx.user`` carrying the flow-name title hint, the
markdown shape contract, and a "do not propose graph mutations — read-only
assist" instruction. The composed ``[system, user]`` message pair is
streamed through W13's SSE primitives exactly so the wire format matches
``/ai/chat/stream`` and ``/ai/explain_run_failure``.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` (W12) so BYOK rows +
env-var fallback + surface-keyed model defaults are honoured identically
to the chat route.
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


class GenerateDocumentationRequest(BaseModel):
    """Body for ``POST /ai/generate_documentation``.

    ``samples_mode`` defaults to D009's ``"off"``. The frontend in v0
    doesn't expose a UI toggle; W25 + a future per-flow safety config
    workstream will surface it.
    """

    flow_id: int
    provider: str = Field(min_length=1)
    model: str | None = None
    samples_mode: SamplesMode = "off"
    max_tokens: int | None = Field(default=None, gt=0)


def _ensure_known_provider(name: str) -> None:
    if name not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {list_supported_providers()}",
        )


def _resolve_flow_name(flow, flow_id: int) -> str:
    """Return the flow name the LLM should use as the markdown title.

    Falls back to ``f"flow-{flow_id}"`` when ``flow_settings.name`` is
    missing or empty (e.g. an unsaved flow).
    """
    settings = getattr(flow, "flow_settings", None)
    name = getattr(settings, "name", None) if settings is not None else None
    if isinstance(name, str) and name.strip():
        return name.strip()
    return f"flow-{flow_id}"


def _compose_documentation_user_message(*, rendered_user: str, flow_name: str) -> str:
    """Build the user-message body the LLM sees.

    Re-uses W22's deterministic ``ctx.user`` (subgraph + schemas +
    settings) verbatim so its prompt-cache hash stays stable, then
    appends a structured ``## Documentation request`` block with the
    flow-name hint and the markdown shape contract.
    """

    return (
        f"{rendered_user}\n\n"
        "## Documentation request\n\n"
        f"Flow name: `{flow_name}`\n\n"
        "Generate concise markdown documentation for this flow. Use this structure:\n\n"
        f"1. Top-level title: `# Flow: {flow_name}`.\n"
        "2. One-paragraph **Overview** describing what the flow does end-to-end, "
        "inferred from the node settings + lineage above.\n"
        "3. **Nodes** section. For each node in the subgraph, in topological order:\n"
        "   - `## {node.name} (id={node.id}, type={node.node_type})`\n"
        "   - one short paragraph: purpose / what it does.\n"
        "   - `**Key settings:**` bulleted list of the most relevant `settings` "
        "fields (skip empty / default / verbose ones).\n"
        "   - `**Input columns:**` bulleted list (only if upstream schema is known).\n"
        "   - `**Output columns:**` bulleted list of `schema_columns`. If "
        '`schema_status` is `unknown`, write "schema unknown — node has not '
        'been run".\n'
        "4. **Lineage** section: one paragraph tracing how data flows from the "
        "source nodes to the sinks, naming the nodes by id.\n\n"
        "Rules:\n"
        "- Cite only column names that appear in the schemas above.\n"
        "- Do not invent columns, settings keys, or node types.\n"
        "- Do not propose graph mutations — this is read-only assist.\n"
        "- Output the markdown body only. Do not wrap in code fences.\n"
    )


@router.post("/generate_documentation", tags=["ai"])
async def generate_documentation(
    body: GenerateDocumentationRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Stream an Assist-level markdown document describing the whole flow.

    Errors before the stream opens:

    * ``404`` — provider name is not in :data:`PROVIDERS`.
    * ``409`` — no BYOK row, no env-var fallback, not Ollama
      (:class:`ProviderNotConfiguredError`).
    * ``422`` — flow not found, flow has no nodes, or Pydantic validation.
    * ``503`` — ``FEATURE_FLAG_AI`` is off (inherited from the parent
      router-level dependency).

    Once the stream opens, transient provider errors surface as
    ``event: error`` payloads from :func:`sse_stream` and the response
    closes — same posture as :mod:`flowfile_core.ai.chat_routes` and
    :mod:`flowfile_core.ai.run_failure_routes`.
    """

    _ensure_known_provider(body.provider)

    flow = flow_file_handler.get_flow(body.flow_id)
    if flow is None:
        raise HTTPException(status_code=422, detail=f"Flow {body.flow_id} not found")

    pinned_node_ids = [node.node_id for node in flow.nodes]
    if not pinned_node_ids:
        raise HTTPException(
            status_code=422,
            detail=f"Flow {body.flow_id} has no nodes",
        )

    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            body.provider,
            surface="docgen",
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
        pinned_node_ids,
        surface="docgen",
        samples_mode=body.samples_mode,
    )

    flow_name = _resolve_flow_name(flow, body.flow_id)
    user_text = _compose_documentation_user_message(
        rendered_user=ctx.user,
        flow_name=flow_name,
    )
    messages = [
        Message(role="system", content=ctx.system),
        Message(role="user", content=user_text),
    ]

    provider_stream = provider.stream(
        messages=messages,
        tools=None,
        max_tokens=body.max_tokens,
    )

    return make_streaming_response(sse_stream(provider_stream))


__all__ = ["router"]
