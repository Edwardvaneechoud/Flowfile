"""HTTP route for the inline ``✨`` menu surface (W21).

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; W17's feature-flag gate covers this
through the parent ``ai_router``.

Like W20's ``chat_routes``, W23's ``run_failure_routes``, and W50's
``docgen_routes``, this surface is read-only by construction —
``tools=None`` is passed to the provider's ``stream()`` so no
``tool_call_delta`` is ever emitted. Phase 1 (Assist) ships text only;
W31 / W41 / W35 own the tool-driven apply path. The "Regenerate code"
action emits a *suggested* snippet to the chat drawer; the user copy-
pastes it manually until that landing happens.

The single endpoint takes ``{flow_id, node_id, action, ...}``, looks up
the ``FlowGraph`` via ``flow_file_handler``, builds a schema-grounded
:class:`PromptContext` via W22's
:func:`flowfile_core.ai.context.render_prompt_context` (surface
``"explain"`` — fits D010's Sonnet-4.6 budget for "Inline ✨ Explain"),
then appends a structured ``## Action: {action}`` block with an action-
specific instruction. The composed ``[system, user]`` message pair is
streamed through W13's SSE primitives so the wire format matches the
sibling routes.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` (W12) so BYOK rows
+ env-var fallback + surface-keyed model defaults are honoured the same
way the chat / run-failure / docgen routes do.
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
InlineActionType = Literal[
    "explain",
    "optimise",
    "document",
    "regenerate_code",
    "suggest_filters",
]

# Node types whose primary configuration is a code snippet — only these
# accept ``regenerate_code``. Kept narrow on purpose; ``formula``'s
# advanced expression and the SQL transform's text are technically
# editable too, but their settings shapes don't fit a "rewrite this code"
# instruction the same way and would need their own action variants.
_CODE_BEARING_NODE_TYPES: frozenset[str] = frozenset(
    {
        "polars_code",
        "python_script",
        "sql_query",
    }
)


class InlineActionRequest(BaseModel):
    """Body for ``POST /ai/inline_action``.

    ``action`` is a ``Literal`` so Pydantic rejects unknown action names
    at the boundary — keeps the executor's dispatch table tight.

    ``samples_mode`` defaults to D009's ``"off"``. The frontend in v0
    doesn't expose a UI toggle; W25 + a future per-flow safety config
    workstream will surface it.
    """

    flow_id: int
    node_id: int
    action: InlineActionType
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


def _ensure_action_compatible(action: InlineActionType, node_type: str) -> None:
    """Block ``regenerate_code`` on non-code-bearing nodes.

    The other actions accept any node type — Explain / Optimise /
    Document / Suggest filters all make sense for a filter, a join, a
    source reader, etc.
    """

    if action == "regenerate_code" and node_type not in _CODE_BEARING_NODE_TYPES:
        allowed = ", ".join(sorted(_CODE_BEARING_NODE_TYPES))
        raise HTTPException(
            status_code=422,
            detail=(
                f"Action 'regenerate_code' requires a code-bearing node "
                f"(one of: {allowed}); node {node_type!r} is not code-bearing."
            ),
        )


def _action_instruction(action: InlineActionType) -> str:
    """Per-action instruction block appended to the W22 ``ctx.user``.

    Each block is a single paragraph + a "do not propose graph
    mutations" reminder. Kept short on purpose so the W22 deterministic
    body stays the bulk of the user message and the prompt cache can
    still hit on the shared subgraph snapshot.
    """

    if action == "explain":
        return (
            "Explain in plain language what this node does and how it transforms the input. "
            "Reference the actual column names and settings from the snapshot above. "
            "Keep it concise — one or two paragraphs."
        )
    if action == "optimise":
        return (
            "Suggest concrete optimisations for this node — performance, readability, "
            "or correctness. Reference actual column names and settings from the snapshot above. "
            "Each suggestion should be specific enough that a developer can act on it; "
            "if the node is already in good shape, say so explicitly."
        )
    if action == "document":
        return (
            "Write a short, user-facing description for this node suitable for the "
            "node's `description` field. One to three sentences explaining what the "
            "node does and why someone reading the flow should care. Reference real "
            "column names and settings from the snapshot above. Output the description "
            "text only — no headings, no fences, no preamble."
        )
    if action == "regenerate_code":
        return (
            "Rewrite the code in this node so it does the same thing more clearly. "
            "Preserve the input/output schema exactly. Use the upstream column names "
            "and types from the snapshot above. Output the rewritten code in a single "
            "fenced code block matching the node's language; one short paragraph above "
            "the block can describe the change. The user will copy-paste the snippet "
            "into the editor manually — do not propose graph mutations."
        )
    if action == "suggest_filters":
        return (
            "Suggest 3 to 5 useful filter conditions for the upstream data. Reference "
            "actual column names and types from the snapshot above. For each suggestion, "
            "give: a one-line description of what it removes/keeps, and the corresponding "
            'Polars expression in a fenced code block (e.g. `pl.col("X") > 0`). Do not '
            "propose graph mutations; the user will paste the chosen expression into the "
            "filter node's settings manually."
        )
    # Unreachable — Pydantic rejects unknown literals at the boundary.
    raise HTTPException(status_code=422, detail=f"Unknown action {action!r}")


def _compose_action_user_message(
    *,
    rendered_user: str,
    action: InlineActionType,
    node_label: str,
    node_id: int,
    node_type: str,
) -> str:
    """Build the user-message body the LLM sees.

    Re-uses W22's deterministic ``ctx.user`` (subgraph + schemas +
    settings) verbatim so its prompt-cache hash stays stable, then
    appends a single ``## Action: {action}`` block carrying the action-
    specific instruction.
    """

    instruction = _action_instruction(action)
    return (
        f"{rendered_user}\n\n"
        f"## Action: {action}\n\n"
        f"Target node: `{node_label}` (id `{node_id}`, type `{node_type}`).\n\n"
        f"{instruction}"
    )


@router.post("/inline_action", tags=["ai"])
async def inline_action(
    body: InlineActionRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Stream an Assist-level response for a per-action inline ✨ menu pick.

    Errors before the stream opens:

    * ``404`` — provider name is not in :data:`PROVIDERS`.
    * ``409`` — no BYOK row, no env-var fallback, not Ollama
      (:class:`ProviderNotConfiguredError`).
    * ``422`` — flow not found, node not found, ``regenerate_code`` on a
      non-code-bearing node, or Pydantic validation.
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

    node_type = getattr(node, "node_type", None) or ""
    _ensure_action_compatible(body.action, node_type)

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

    node_label = getattr(node, "name", None) or node_type or f"node-{body.node_id}"
    user_text = _compose_action_user_message(
        rendered_user=ctx.user,
        action=body.action,
        node_label=node_label,
        node_id=body.node_id,
        node_type=node_type or "unknown",
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


__all__ = ["router", "InlineActionRequest", "InlineActionType"]
