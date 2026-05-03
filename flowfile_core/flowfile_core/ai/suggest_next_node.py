"""Edge ghost-node suggestions — owned by W32.

Backs ``POST /ai/suggest_next_node`` (plan §3.3 + §6.4). On hover over an
outgoing edge stub the frontend asks for top-3 schema-grounded next-node
suggestions; this module produces them.

Design (mirrors W34's ``autocomplete`` posture):

* The function is pure-async and never raises — every failure mode (cold
  upstream, timeout, parse error, validation error) becomes a
  ``degraded=True`` response with a stable ``reason`` string. The frontend
  hides the popover when ``degraded`` is set.
* The LLM emits a JSON document. Each candidate is parsed into Pydantic
  via the matching ``NODE_TYPE_TO_SETTINGS_CLASS`` entry; failures are
  dropped silently (the LLM occasionally hallucinates settings shapes).
* Column refs in the candidate's settings are walked via W31's
  ``collect_column_refs`` and validated against the upstream
  ``predicted_schema`` via W25's ``validate_column_references``. Candidates
  that miss columns are dropped.
* Predicted output schema is produced via W31's ``predict_schema_via_mirror``
  for static / source / passthrough nodes. Dynamic nodes
  (``polars_code`` / ``python_script`` / ``sql_query``) are NOT in the
  ``ghost_node`` ``SURFACE_PRESETS`` (per W30's curation) — if the LLM
  ignores the preset hint and proposes one anyway, it survives only if
  Pydantic + column-ref validation pass; we don't run a kernel dry-run on
  the ghost-node hot path (latency budget per D010 is <2s TTFB).
* D012 — only ``predicted_schema`` cache reads, no ``LazyFrame.collect()``.
* D011 — when upstream schema is ``None``, ``degraded=True`` (the ghost
  surface should never block a casual hover; users wanting to populate
  schema for cold flows can run the upstream first, like W34).

Wire-layer note: ``node_type`` is the un-namespaced flowfile name
(``"filter"``), not the MCP-shaped tool name. W30 uses
``flowfile.graph.add_filter`` for tool catalog purposes; the ghost-node
endpoint deals in node types directly because the frontend renders these
suggestions as ghost VueFlow nodes (each ghost has a node type, not a
tool call).

The W11 lazy-litellm contract is preserved — this module must not import
``litellm`` at load time.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from flowfile_core.ai import safety
from flowfile_core.ai.metrics import record_autocomplete_call
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler
from flowfile_core.ai.tools.classification import is_predictable_via_mirror
from flowfile_core.ai.tools.predictor import (
    collect_column_refs,
    predict_schema_via_mirror,
    schema_to_dict_list,
)
from flowfile_core.ai.tools.registry import SURFACE_PRESETS, mcp_tool_name
from flowfile_core.schemas.schemas import get_settings_class_for_node_type

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import (
        FlowfileColumn,
    )
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

logger = logging.getLogger(__name__)


SURFACE: str = "ghost_node"
"""The W30 ``SurfaceLiteral`` value W32 emits — kept as a constant so call
sites and test assertions stay in lock-step."""

DEFAULT_TIMEOUT_SECONDS: float = 3.5
"""Hard timeout per call. Picked under D010 (TTFB <2s, total <4s for the
ghost surface) — leaves ~500 ms of headroom for the route/scheduler/serdes
overhead before the user perceives the hover as stale."""

MAX_SUGGESTIONS: int = 3
"""§3.3 spec: top-3 by upstream-schema fit."""

MAX_TOKENS: int = 1024
"""Generous-but-bounded — three candidates with full settings JSON
typically fit well under 700 tokens; we add slack for the rationale field."""


# --------------------------------------------------------------------------- #
# Wire types                                                                   #
# --------------------------------------------------------------------------- #


class NextNodeSuggestion(BaseModel):
    """A single ghost-node candidate.

    The ``settings`` field is the validated Pydantic settings dict for the
    suggested node type — frontend forwards it verbatim to
    ``POST /editor/add_node/`` when the user clicks accept.
    """

    model_config = ConfigDict(extra="forbid")

    node_type: str
    """Un-namespaced flowfile node type (e.g. ``"filter"``).
    Always one of the entries in the ``ghost_node`` ``SURFACE_PRESETS``."""
    settings: dict[str, Any]
    """Validated settings payload — already model_dump'd through the matching
    Pydantic class so it round-trips back to the real-graph add path."""
    label: str
    """Short human label rendered in the ghost popover (e.g. ``"Filter rows
    where region == 'EU'"``)."""
    description: str | None = None
    """One-line elaboration; optional — cheap providers tend to skip it."""
    predicted_output_schema: list[dict[str, Any]] | None = None
    """Result of ``predict_schema_via_mirror`` if available; ``None`` when
    the node is dynamic / source-without-callback / mirror-prediction failed.
    The frontend can show downstream-friendly schema previews when present."""
    rationale: str | None = None
    """The LLM's reasoning for why this node fits. Optional."""


class NextNodeSuggestionsResponse(BaseModel):
    suggestions: list[NextNodeSuggestion] = Field(default_factory=list)
    degraded: bool = False
    """``True`` when no real suggestions were produced (cold upstream,
    timeout, parse error, all candidates filtered, etc.) — frontend hides
    the popover. ``reason`` carries a stable string for analytics."""
    reason: str | None = None


# Internal: shape the LLM is asked to return.
class _LLMSuggestion(BaseModel):
    """Subset of ``NextNodeSuggestion`` the LLM emits.

    ``settings`` is forwarded as-is to the per-node-type Pydantic class for
    validation; ``predicted_output_schema`` is server-computed (never trusted
    from the model) so it isn't part of the LLM-output shape.
    """

    model_config = ConfigDict(extra="ignore")

    node_type: str
    settings: dict[str, Any] = Field(default_factory=dict)
    label: str = ""
    description: str | None = None
    rationale: str | None = None


class _LLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    suggestions: list[_LLMSuggestion] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Surface preset → candidate node-type set                                     #
# --------------------------------------------------------------------------- #


def _allowed_node_types() -> frozenset[str]:
    """The un-namespaced node types the LLM is allowed to suggest.

    Derived from W30's ``SURFACE_PRESETS["ghost_node"]`` — each entry is an
    MCP-shaped tool name like ``flowfile.graph.add_filter``; we strip the
    ``flowfile.graph.add_`` prefix to get the flowfile node type.

    ``read_node_schema`` is in the preset for the LLM's introspection use
    but isn't a node type, so we filter to ``flowfile.graph.add_*`` only.
    """
    preset = SURFACE_PRESETS["ghost_node"]
    add_prefix = mcp_tool_name("graph", "add_")  # "flowfile.graph.add_"
    types: set[str] = set()
    for tool_name in preset:
        if tool_name.startswith(add_prefix):
            types.add(tool_name[len(add_prefix) :])
    return frozenset(types)


# Computed once at import — the SURFACE_PRESETS table is itself a module-level
# constant so a freshly imported module sees a stable allowed set.
_ALLOWED_NODE_TYPES: frozenset[str] = _allowed_node_types()


# --------------------------------------------------------------------------- #
# Schema lookup                                                                #
# --------------------------------------------------------------------------- #


def _column_names_for_node(node: FlowNode | None) -> list[str] | None:
    """Return predicted column names, or ``None`` if unknown / missing.

    Mirrors W34 — schema-only path, no force-recompute (D012). Cold upstreams
    propagate as ``None`` to the caller's degraded branch.
    """
    if node is None:
        return None
    schema: list[FlowfileColumn] | None = node.node_schema.predicted_schema
    if not schema:
        return None
    return [col.column_name for col in schema]


def _columns_for_node(node: FlowNode | None) -> list[FlowfileColumn] | None:
    if node is None:
        return None
    schema: list[FlowfileColumn] | None = node.node_schema.predicted_schema
    if not schema:
        return None
    return list(schema)


# --------------------------------------------------------------------------- #
# Prompt construction                                                          #
# --------------------------------------------------------------------------- #


_SYSTEM_PROMPT = """\
You suggest the next node to add downstream of a Flowfile node, given the
upstream node's predicted output schema.

Output a single JSON object — no prose, no code blocks. Shape:

  {"suggestions": [
     {"node_type": "<one of the allowed types>",
      "settings": { ... pydantic-valid settings for that node type ... },
      "label": "<short imperative label, max 50 chars>",
      "description": "<one-line elaboration, optional>",
      "rationale": "<why this fits, optional>"}
  ]}

Hard rules:

* `node_type` MUST be one of: {{ALLOWED_TYPES}}. Reject any other value;
  the user has scoped this surface to fast in-flow editing.
* `settings` MUST validate against the Pydantic class for that node type.
  When unsure of a field, omit it and let the default apply.
* You MAY only reference columns from the upstream schema you're given.
  Do not invent column names. Match capitalisation exactly.
* Keep `label` short (<50 chars). Order suggestions by relevance —
  most-likely-fit first.
* Return at most {{MAX_SUGGESTIONS}} suggestions. If no plausible
  suggestion exists, return `{"suggestions": []}`.
"""


def _format_columns_for_prompt(columns: list[str]) -> str:
    if not columns:
        return "(empty)"
    return ", ".join(columns)


def _build_messages(
    *,
    upstream_columns: list[str],
    upstream_node_type: str,
    intent: str | None,
    max_suggestions: int,
) -> list[Message]:
    user_lines = [
        f"Upstream node type: {upstream_node_type}",
        f"Upstream schema columns: {_format_columns_for_prompt(upstream_columns)}",
    ]
    if intent:
        user_lines.append(f"User intent: {intent}")
    user_lines.append(f"Return at most {max_suggestions} suggestions.")
    # ``.replace`` not ``.format`` — the JSON shape examples in the prompt
    # use literal ``{`` / ``}``, which would be interpreted as format
    # placeholders. Same posture as W34's autocomplete.
    allowed = ", ".join(sorted(_ALLOWED_NODE_TYPES))
    system = _SYSTEM_PROMPT.replace("{{ALLOWED_TYPES}}", allowed).replace("{{MAX_SUGGESTIONS}}", str(max_suggestions))
    return [
        Message(role="system", content=system),
        Message(role="user", content="\n".join(user_lines)),
    ]


# --------------------------------------------------------------------------- #
# Provider call wrapper                                                        #
# --------------------------------------------------------------------------- #


async def _call_provider_for_json(
    *,
    provider: Provider,
    messages: list[Message],
    timeout: float,
    scheduler: RateLimitScheduler | None,
    max_tokens: int,
) -> tuple[str | None, str | None]:
    """Invoke ``provider.chat`` with JSON-mode and a hard timeout.

    Returns ``(content, error_reason)`` — same contract W34 follows. Retry/
    backoff is intentionally bypassed; the W14 scheduler is consulted only
    for RPM tracking via ``acquire``.
    """
    sched = scheduler or default_scheduler()

    async def _do_call() -> str | None:
        async with sched.acquire(provider.name, surface=SURFACE):
            response = await provider.chat(
                messages=messages,
                tools=None,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
        return response.content

    try:
        content = await asyncio.wait_for(_do_call(), timeout=timeout)
    except asyncio.TimeoutError:
        return None, "timeout"
    except Exception as exc:  # noqa: BLE001 — collapse to a single reason
        logger.warning("suggest_next_node provider call failed: %s", exc, exc_info=False)
        return None, "provider_error"
    return content, None


def _parse_json_payload(content: str | None) -> tuple[Any, str | None]:
    """Strict JSON parse with a Markdown-fence fallback (matches W34)."""
    if content is None or not content.strip():
        return None, "empty_response"
    try:
        return json.loads(content), None
    except json.JSONDecodeError:
        pass
    stripped = content.strip()
    if stripped.startswith("```"):
        without_open = stripped.split("\n", 1)[-1]
        if without_open.rstrip().endswith("```"):
            without_open = without_open.rstrip()[: -len("```")].rstrip()
        try:
            return json.loads(without_open), None
        except json.JSONDecodeError:
            pass
    return None, "parse_error"


# --------------------------------------------------------------------------- #
# Candidate validation                                                         #
# --------------------------------------------------------------------------- #


def _validate_candidate(
    candidate: _LLMSuggestion,
    *,
    upstream_columns: list[str],
    upstream_columns_full: list[FlowfileColumn] | None,
    upstream_node_id: int,
) -> NextNodeSuggestion | None:
    """Run the candidate through the four-stage filter:

    1. ``node_type`` is in the ``ghost_node`` allowed set.
    2. Settings validate via the matching Pydantic class.
    3. All settings column refs exist in the upstream schema.
    4. ``predict_schema_via_mirror`` for static/source/passthrough.

    Returns the wire-shape ``NextNodeSuggestion`` on success, ``None``
    (with a debug log) when any filter rejects it. Rejections are silent
    by design — the user shouldn't see hallucinated suggestions in the
    popover, and a degraded popover is preferable to a noisy one.
    """
    # 1. Allowed node type.
    if candidate.node_type not in _ALLOWED_NODE_TYPES:
        logger.debug(
            "ghost_node candidate rejected: %s not in ghost_node preset",
            candidate.node_type,
        )
        return None

    # 2. Pydantic settings validation.
    settings_cls = get_settings_class_for_node_type(candidate.node_type)
    if settings_cls is None:
        # Defensive — _ALLOWED_NODE_TYPES is derived from the preset which is
        # validated against the catalog at import time, so this branch is
        # effectively unreachable. Log if it fires.
        logger.debug("ghost_node candidate rejected: no settings class for %s", candidate.node_type)
        return None
    try:
        settings_obj = settings_cls.model_validate(candidate.settings)
    except ValidationError as exc:
        logger.debug(
            "ghost_node candidate rejected: settings validation failed for %s: %s",
            candidate.node_type,
            exc,
        )
        return None

    # 3. Column-ref validation against upstream.
    if upstream_columns:
        refs = collect_column_refs(candidate.node_type, settings_obj)
        if refs:
            missing = safety.validate_column_references(refs, upstream_columns)
            if missing:
                logger.debug(
                    "ghost_node candidate rejected: %s references missing columns %s",
                    candidate.node_type,
                    missing,
                )
                return None

    # 4. Predicted schema via mirror (best-effort).
    predicted_dicts: list[dict[str, Any]] | None = None
    if is_predictable_via_mirror(candidate.node_type) and upstream_columns_full is not None:
        try:
            upstream_map = {upstream_node_id: upstream_columns_full}
            predicted = predict_schema_via_mirror(
                candidate.node_type,
                settings_obj,
                upstream_map,
            )
            if predicted:
                predicted_dicts = schema_to_dict_list(predicted)
        except Exception as exc:  # noqa: BLE001 — predicted_schema is best-effort
            logger.debug(
                "ghost_node mirror prediction failed for %s: %s",
                candidate.node_type,
                exc,
            )
            predicted_dicts = None

    label = candidate.label.strip() or candidate.node_type
    return NextNodeSuggestion(
        node_type=candidate.node_type,
        settings=settings_obj.model_dump(mode="json"),
        label=label,
        description=candidate.description,
        predicted_output_schema=predicted_dicts,
        rationale=candidate.rationale,
    )


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


async def suggest_next_node(
    graph: FlowGraph,
    upstream_node_id: int | str,
    *,
    provider: Provider,
    intent: str | None = None,
    max_suggestions: int = MAX_SUGGESTIONS,
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> NextNodeSuggestionsResponse:
    """Top-N schema-grounded next-node suggestions for an outgoing edge.

    See module docstring for invariants. Never raises — every failure mode
    becomes a ``degraded=True`` response with a stable ``reason`` string so
    the frontend can hide the popover gracefully.
    """
    started = time.monotonic()

    upstream_node = graph.get_node(upstream_node_id)
    upstream_columns = _column_names_for_node(upstream_node)

    if upstream_node is None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="missing_upstream",
        )
        return NextNodeSuggestionsResponse(
            suggestions=[],
            degraded=True,
            reason="Upstream node not found.",
        )

    if upstream_columns is None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="upstream_schema_unknown",
        )
        return NextNodeSuggestionsResponse(
            suggestions=[],
            degraded=True,
            reason="Upstream schema unknown — run the upstream node first.",
        )

    upstream_columns_full = _columns_for_node(upstream_node)

    # Cap on input + treat values <=0 as the default.
    effective_max = max(1, min(max_suggestions, MAX_SUGGESTIONS * 2))

    messages = _build_messages(
        upstream_columns=upstream_columns,
        upstream_node_type=upstream_node.node_type,
        intent=intent,
        max_suggestions=effective_max,
    )

    content, err = await _call_provider_for_json(
        provider=provider,
        messages=messages,
        timeout=timeout,
        scheduler=scheduler,
        max_tokens=MAX_TOKENS,
    )
    if err is not None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason=err,
        )
        return NextNodeSuggestionsResponse(suggestions=[], degraded=True, reason=err)

    payload, parse_err = _parse_json_payload(content)
    if parse_err is not None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason=parse_err,
        )
        return NextNodeSuggestionsResponse(suggestions=[], degraded=True, reason=parse_err)

    try:
        llm_output = _LLMOutput.model_validate(payload)
    except ValidationError as exc:
        logger.debug("ghost_node llm output failed top-level validation: %s", exc)
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="parse_error",
        )
        return NextNodeSuggestionsResponse(suggestions=[], degraded=True, reason="parse_error")

    validated: list[NextNodeSuggestion] = []
    for raw in llm_output.suggestions:
        result = _validate_candidate(
            raw,
            upstream_columns=upstream_columns,
            upstream_columns_full=upstream_columns_full,
            upstream_node_id=upstream_node.node_id,
        )
        if result is not None:
            validated.append(result)
        if len(validated) >= max_suggestions:
            break

    latency_ms = int((time.monotonic() - started) * 1000)

    if not validated:
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="no_valid_suggestions",
        )
        return NextNodeSuggestionsResponse(
            suggestions=[],
            degraded=True,
            reason="no_valid_suggestions",
        )

    record_autocomplete_call(
        surface=SURFACE,
        provider=provider.name,
        latency_ms=latency_ms,
        suggestion_count=len(validated),
        degraded_reason=None,
    )
    return NextNodeSuggestionsResponse(suggestions=validated, degraded=False)


__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_SUGGESTIONS",
    "NextNodeSuggestion",
    "NextNodeSuggestionsResponse",
    "SURFACE",
    "suggest_next_node",
]
