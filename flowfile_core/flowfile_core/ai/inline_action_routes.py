"""HTTP route for the inline ``✨`` menu surface.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
this through the parent ``ai_router``.

Like ``chat_routes``, ``run_failure_routes``, and ``docgen_routes``,
this surface is read-only by construction — ``tools=None`` is passed
to the provider's ``stream()`` so no ``tool_call_delta`` is ever
emitted. The Assist surface ships text only; the executor / diff /
inline-diff modules own the tool-driven apply path. The "Regenerate
code" action emits a *suggested* snippet to the chat drawer; the user
copy-pastes it manually.

The single endpoint takes ``{flow_id, node_id, action, ...}``, looks
up the ``FlowGraph`` via ``flow_file_handler``, builds a
schema-grounded :class:`PromptContext` via
:func:`flowfile_core.ai.context.render_prompt_context` (surface
``"explain"``), then appends a structured ``## Action: {action}``
block with an action-specific instruction. The composed ``[system,
user]`` message pair is streamed through the SSE primitives so the
wire format matches the sibling routes.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` so BYOK rows +
env-var fallback + surface-keyed model defaults are honoured the
same way the chat / run-failure / docgen routes do.
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
    Message,
    UnknownProviderError,
    is_resolvable_provider,
    resolvable_provider_names,
)
from flowfile_core.ai.streaming import make_streaming_response, sse_stream
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


SamplesMode = Literal["off", "regex"]
InlineActionType = Literal[
    "explain",
    "add_description",
    "regenerate_code",
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

    ``samples_mode`` defaults to ``"off"``. The frontend doesn't
    expose a UI toggle today; a per-flow safety config workstream
    will surface it.
    """

    flow_id: int
    node_id: int
    action: InlineActionType
    provider: str = Field(min_length=1)
    model: str | None = None
    samples_mode: SamplesMode = "off"
    max_tokens: int | None = Field(default=None, gt=0)


def _ensure_known_provider(name: str) -> None:
    if not is_resolvable_provider(name):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {resolvable_provider_names()}",
        )


def _ensure_action_compatible(action: InlineActionType, node_type: str) -> None:
    """Block ``regenerate_code`` on non-code-bearing nodes.

    The other actions (``explain`` and ``add_description``) accept any
    node type — both make sense for a filter, a join, a source reader, etc.
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
    """Per-action instruction block appended to ``ctx.user``.

    Each block is a single paragraph + a "do not propose graph
    mutations" reminder. Kept short on purpose so the deterministic
    context body stays the bulk of the user message and the prompt
    cache can still hit on the shared subgraph snapshot.
    """

    if action == "explain":
        return (
            "Explain in plain language what this node does and how it transforms the input. "
            "Reference the actual column names and settings from the snapshot above. "
            "Keep it concise — one or two paragraphs."
        )
    if action == "add_description":
        return (
            "Write a single sentence for this node's `description` field. "
            "Use imperative voice (verb-first), reference the key column "
            "names and settings from the snapshot above, and aim for 5–15 "
            "words. Do not start with \"This node\", do not explain why "
            "the node exists, do not add commentary or surrounding quotes.\n\n"
            "Good examples:\n"
            "- Filter orders to status='shipped' and region='EU'.\n"
            "- Join customers with orders on customer_id.\n"
            "- Aggregate revenue by month and product_category.\n"
            "- Read sales.csv from the data lake.\n\n"
            "Bad (do not produce):\n"
            "- This node filters the dataset to focus on shipped EU orders, "
            "which helps analysts… (too long, explains why)\n"
            "- A filter operation. (too vague — name the columns and condition)\n\n"
            "Output the description sentence only — no headings, no fences, "
            "no preamble, no surrounding quotes."
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

    Re-uses the deterministic ``ctx.user`` (subgraph + schemas +
    settings) verbatim so its prompt-cache hash stays stable, then
    appends a single ``## Action: {action}`` block carrying the
    action-specific instruction.
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
