"""Level 3 — Planner (multi-turn, plan-then-execute). Owned by W40.

Per plan §2 / §6.4 / §5.6, the planner:

* opens a session via ``POST /ai/agent/start``;
* surfaces a narrowed tool catalog using D002's two-stage pattern (default
  ``surface="agent"`` calls ``flowfile.meta.pick_category`` first, then the
  matching ``CATEGORY_PRESETS`` for the rest of the loop; ``surface=
  "agent_complex"`` exposes the full catalog in one shot);
* dispatches each LLM tool call through W31's :func:`execute_tool_call` with
  ``mode="stage"`` — the live graph is never mutated mid-run (per §9.2
  "Level 3 agent never auto-applies");
* runs D006's snapshot+warn-and-pause check before every dispatch — if the
  user mutated the canvas mid-run, the loop yields ``drift_detected`` +
  ``paused`` and exits cleanly so the route can return JSON / SSE-close
  while the session waits for ``POST /ai/agent/{session_id}/resume``;
* retries a rejected step up to ``max_retries_per_step`` times by feeding
  the executor's ``refusal_detail`` back as a ``role="tool"`` message and
  asking the LLM to correct;
* on completion, bundles the per-step :class:`StagedToolEntry` list into a
  single :class:`flowfile_core.ai.diff.GraphDiff` via
  :func:`flowfile_core.ai.diff.bundle_staged_results` and registers it via
  W41's :func:`flowfile_core.ai.diff.register_diff` — the user reviews the
  diff via the W35 ``AiDiffPreview`` and accepts atomically.

The function is a **pure async generator** that never raises — every
failure mode becomes a :class:`PlannerEvent` of type ``"error"`` /
``"tool_call_rejected"`` / ``"drift_detected"`` / ``"abort"`` so the SSE
wrapper can stream the failure to the client without a structured
exception escaping the generator boundary.

W42 swaps the in-memory session store for a disk-backed sidecar; W40 only
needs the in-memory shape. The ``PlannerEvent`` ``id:`` headers carry
``f"{session_id}.{step_count}"`` so EventSource clients *can* re-attach via
``Last-Event-ID`` once W42 lands the replay buffer — W40's resume route is
exclusively for D006 drift-pause, not connection drop.

System prompt: ``prompts/base.md`` + ``prompts/planner.md`` (D008) — the
``copilot`` level wouldn't fit; planner-level prompt language is
intentionally distinct.

The W11 lazy-litellm contract is preserved — this module must not import
``litellm`` at load time. The provider call goes through the W11 seam,
which lazy-loads litellm in its own subclass.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from flowfile_core.ai import audit as audit_module
from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import safety, sessions
from flowfile_core.ai.context.builder import render_prompt_context
from flowfile_core.ai.providers.base import Message, Provider, ToolCall
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler
from flowfile_core.ai.tools.dry_run import DryRunCache
from flowfile_core.ai.tools.executor import (
    InsertionContext,
    ToolExecutionResult,
    execute_tool_call,
)
from flowfile_core.ai.tools.meta_ops import (
    CLASSIFY_INTENT_TOOL_NAME,
    PICK_NODE_TYPE_TOOL_NAME,
    PICK_UPSTREAM_TOOL_NAME,
    build_pick_upstream_spec,
)
from flowfile_core.ai.tools.registry import (
    build_staged_fill_tool_spec,
    build_tool_catalog,
    get_staged_fill_inner_field_name,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


DEFAULT_MAX_STEPS: int = 32
"""Per-session planner-loop budget. Bumped from 12 for W71 because the
``agent_staged`` surface fans each node-add into 4 LLM rounds (classify
→ pick_type → pick_upstream → fill_settings); a 3-node user turn now
needs at least 12 rounds. Legacy ``agent`` / ``agent_complex`` surfaces
rarely hit even half of this — wasted budget is free, missed budget
truncates the user's plan."""
DEFAULT_MAX_RETRIES_PER_STEP: int = 3
DEFAULT_MAX_TOKENS: int = 2_048
RATIONALE_MAX_LEN: int = 500


PlannerEventName = Literal[
    "thinking",
    "tool_call_proposed",
    "tool_call_staged",
    "tool_call_warned",
    "tool_call_rejected",
    "drift_detected",
    "paused",
    "retry",
    "abort",
    "complete",
    "awaiting_user_input",
    "stage_advanced",
    "error",
    "info",
]
"""W49 — ``awaiting_user_input`` is emitted *instead of* ``complete`` when
the planner loop ends with no tool calls AND no staged results AND the last
assistant message looks like a clarifying question. The session flips to
``awaiting_user_input`` (not ``completed``) so the frontend can surface
*"Agent waiting for your reply…"* and the followup endpoint accepts the
user's answer for re-entry."""


class PlannerEvent(BaseModel):
    """One event yielded by :func:`run_planner_session`.

    The ``payload`` shape is event-specific — see the caller-side handler
    contract in ``stores/ai-agent-store.ts``. Keeping it a dict rather than
    a discriminated union keeps wire-shape evolution cheap; consumers
    branch on ``event``.
    """

    model_config = ConfigDict(frozen=True)

    event: PlannerEventName
    payload: dict[str, Any] = Field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


_ADD_PREFIX = "flowfile.graph.add_"

# W71 — staged-flow tool name lookups. Mirrored from ``meta_ops`` so the
# planner can branch on tool name without re-importing the constants in
# the loop hot path.
_CLASSIFY_INTENT_NAME = CLASSIFY_INTENT_TOOL_NAME
_PICK_NODE_TYPE_NAME = PICK_NODE_TYPE_TOOL_NAME

# W71 v1.6 / v1.8 — recovery is now applied at all agent_staged stages
# (the "exactly one match" + "name must be in expected_tool_names"
# rules in ``_recover_textual_tool_call`` defend against the after-add
# summary pattern). This frozenset stays in place to identify the
# **mandatory-tool-call** stages where an empty ``tool_calls`` —
# even after recovery — should route through the retry path rather
# than terminating the loop (W71 v1.7). At ``classify`` an empty
# ``tool_calls`` is still a valid termination signal: the LLM
# decided op_kind="other" (silently) or the user got their answer.
_MANDATORY_TOOL_CALL_STAGES: frozenset[str] = frozenset(
    {"pick_type", "pick_upstream", "fill_settings", "single_stage_op"}
)
_PICK_UPSTREAM_NAME = PICK_UPSTREAM_TOOL_NAME

# W71 — non-add ops that the staged surface dispatches via ``single_stage_op``.
# The planner uses this set to decide when to call ``reset_stage_state`` after
# a successful single-stage op so the next round restarts at ``classify``.
_STAGED_SINGLE_OP_TOOL_NAMES: frozenset[str] = frozenset(
    {
        "flowfile.graph.update_node_settings",
        "flowfile.graph.delete_node",
        "flowfile.graph.connect",
        "flowfile.graph.delete_connection",
    }
)

# W57 — settings-field names that express primary upstream dependency for
# single-input nodes. The catalog generator (``tools/registry.py``) auto-derives
# JSON Schema from the per-node Pydantic settings classes, so the LLM sees
# both ``upstream_node_ids`` (planner-injected) AND these legacy connection-state
# fields. When the LLM emits the settings field but omits ``upstream_node_ids``,
# the resolver canonicalises the two views.
#
# Audit (2026-05-06): ``depending_on_id`` (NodeSingleInput base) is the
# canonical primary-upstream field. ``depending_on_ids`` (NodeMultiInput) is
# excluded — multi-upstream insertion context is out of v0 scope. Other
# upstream-shaped fields (``ApplyModelSettings.upstream_node_id``,
# ``EvaluateModelSettings.upstream_train_node_id``) are nested in ML
# sub-settings and reference auxiliary cross-flow links, not the primary
# data input — also excluded.
_SETTINGS_DEPENDENCY_FIELDS: tuple[str, ...] = ("depending_on_id",)

# W38 — single short sentence per step, ≤ 20 words per the prompt instruction;
# we capture up to ~280 chars (a generous cap that tolerates the model running
# slightly long) and trim trailing whitespace. Rationale longer than this is
# almost always the model writing a paragraph — clipping keeps the chat scannable.
_RATIONALE_MAX_CHARS: int = 280


OpKind = Literal["meta", "graph", "schema", "codegen", "unknown"]


def _classify_op_kind(tool_name: str) -> OpKind:
    """Map a fully-qualified tool name to its op_kind for the W38 UI gating.

    ``flowfile.meta.*`` are LLM-internal routing decisions (D002 two-stage
    selection) and the frontend hides them. ``flowfile.graph.*`` are the
    user-facing canvas mutations that need a rationale. ``flowfile.schema.*``
    are read-only introspection calls — we still show them so the user
    knows the agent is "looking at" something. ``flowfile.codegen.*`` are
    code generation helpers that only show up under ``agent_complex``.
    """
    if tool_name.startswith("flowfile.meta."):
        return "meta"
    if tool_name.startswith("flowfile.graph."):
        return "graph"
    if tool_name.startswith("flowfile.schema."):
        return "schema"
    if tool_name.startswith("flowfile.codegen."):
        return "codegen"
    return "unknown"


def _capture_rationale(text: str | None) -> str | None:
    """Trim and bound the model's preamble for use as a tool_step rationale.

    The planner prompt instructs the model to emit a single short
    natural-language sentence immediately before each tool call. We capture
    that assistant ``content`` and clip it; if the model didn't produce a
    preamble (or emitted only whitespace), we return ``None`` so the
    frontend / executor falls back to the server-generated arg_summary.
    """
    if not isinstance(text, str):
        return None
    trimmed = text.strip()
    if not trimmed:
        return None
    if len(trimmed) > _RATIONALE_MAX_CHARS:
        # Cut on the last sentence boundary inside the cap if we can find one,
        # else hard-truncate. Keeps the UI from showing a half-sentence with
        # an awkward dangling "and".
        head = trimmed[:_RATIONALE_MAX_CHARS]
        for boundary in (". ", "! ", "? ", "\n"):
            idx = head.rfind(boundary)
            if idx > _RATIONALE_MAX_CHARS // 2:
                return head[: idx + 1].strip()
        return head.rstrip() + "…"
    return trimmed


_QUESTION_TOKENS: tuple[str, ...] = (
    "which",
    "what",
    "where",
    "when",
    "who",
    "why",
    "how",
    "do you",
    "would you",
    "should i",
    "should we",
    "can you",
    "could you",
    "shall i",
    "shall we",
    "did you mean",
)
"""W49 — interrogative tokens used by :func:`_looks_like_question` when an
assistant message is missing a trailing ``?`` (the model occasionally drops
it, especially on shorter clarifying turns). Lower-cased substring match —
``"do you want"`` and ``"do you have"`` are both covered by ``"do you"``."""


def _looks_like_question(text: str | None) -> bool:
    """W49 — true if ``text`` reads like a clarifying question to the user.

    Two cheap signals that catch the live transcripts the spec captured:

    * **Ends with ``?``** — wins regardless of length / wording.
    * **Lower-cased substring contains an interrogative token** from
      :data:`_QUESTION_TOKENS`. Catches *"Should I drop nulls first?"*-style
      preambles even when the trailing ``?`` is missing, and *"Do you want
      me to ..."* when the model gets verbose.

    Whitespace-only or ``None`` returns ``False``. No NLP — a regex / token
    list keeps the planner free of model-driven classification cost on a
    path that fires on every loop exit. False positives on declarative
    sentences containing ``"how"`` or ``"which"`` (e.g. *"This is how the
    join works."*) are tolerable: the only consequence is the frontend
    renders *"Agent waiting for your reply…"* instead of *"Agent finished
    — nothing to stage"*, both still resumable via ``/followup``.
    """
    if not isinstance(text, str):
        return False
    trimmed = text.strip()
    if not trimmed:
        return False
    if trimmed.rstrip().endswith("?"):
        return True
    lowered = trimmed.lower()
    return any(token in lowered for token in _QUESTION_TOKENS)


def _format_columns(value: Any) -> str | None:
    """Render a list-of-strings / list-of-dicts as a comma-separated column list."""
    if isinstance(value, list):
        names: list[str] = []
        for item in value:
            if isinstance(item, str) and item:
                names.append(item)
            elif isinstance(item, dict):
                nm = item.get("name") or item.get("column")
                if isinstance(nm, str) and nm:
                    names.append(nm)
        if names:
            return ", ".join(names)
    return None


def _arg_summary_for_add(node_type: str, settings: dict[str, Any]) -> str:
    """Render a one-line natural-language summary for an ``add_<node_type>`` call.

    Used by the planner as the frontend's secondary line (raw args reference)
    and by the frontend as the headline when the model failed to emit a
    preamble. Each branch covers the load-bearing fields for the most common
    node types; everything else falls back to the generic ``"Adding <type>"``
    shape so we never crash on a settings shape we didn't anticipate.

    The LLM's tool call ``arguments`` follow the per-node Pydantic settings
    schema directly (e.g. ``NodeFilter`` has ``filter_input`` at the top
    level), so the load-bearing fields live at the root of ``settings``.
    Some surfaces (e.g. ``add_node`` with explicit envelope) wrap them
    under ``settings_input`` — we handle both shapes.
    """
    if not isinstance(settings, dict):
        settings = {}
    nested = settings.get("settings_input")
    settings_dict: dict[str, Any] = nested if isinstance(nested, dict) else settings

    pretty_type = node_type.replace("_", " ")

    if node_type == "filter":
        predicate = settings_dict.get("filter_input", {})
        if isinstance(predicate, dict):
            expr = predicate.get("advanced_filter") or predicate.get("basic_filter")
            if isinstance(expr, str) and expr.strip():
                return f"Filter on `{expr.strip()}`"
        return "Adding filter"

    if node_type == "sort":
        cols = settings_dict.get("sort_by")
        if isinstance(cols, list) and cols:
            names = []
            for item in cols:
                if isinstance(item, dict):
                    nm = item.get("column")
                    direction = item.get("how") or item.get("direction") or "asc"
                    if isinstance(nm, str) and nm:
                        names.append(f"{nm} {direction}")
            if names:
                return f"Sort by {', '.join(names)}"
        return "Adding sort"

    if node_type == "join":
        join_input = settings_dict.get("join_input", {})
        if isinstance(join_input, dict):
            keys = join_input.get("join_mapping") or join_input.get("join_keys")
            how = join_input.get("how") or "inner"
            if isinstance(keys, list) and keys:
                key_strs: list[str] = []
                for k in keys:
                    if isinstance(k, dict):
                        left = k.get("left_col") or k.get("left")
                        right = k.get("right_col") or k.get("right")
                        if isinstance(left, str) and isinstance(right, str):
                            key_strs.append(f"{left}={right}")
                    elif isinstance(k, str):
                        key_strs.append(k)
                if key_strs:
                    return f"{how.capitalize()} join on {', '.join(key_strs)}"
        return "Adding join"

    if node_type == "select":
        cols = _format_columns(settings_dict.get("select_input"))
        if cols:
            return f"Select columns: {cols}"
        return "Adding select"

    if node_type in {"formula", "polars_code", "python_script", "sql_query"}:
        # These either have a code/expression body or a target column; show
        # the target name when present so the user sees "Adding amount_usd"
        # rather than the raw expression.
        target = settings_dict.get("function") or settings_dict.get("output_column")
        if isinstance(target, dict):
            field = target.get("field") or target.get("column")
            if isinstance(field, str) and field:
                return f"Adding {pretty_type} → `{field}`"
        return f"Adding {pretty_type}"

    if node_type == "group_by":
        group_cols = _format_columns(settings_dict.get("group_by_input"))
        if group_cols:
            return f"Group by {group_cols}"
        return "Adding group_by"

    if node_type == "unique":
        cols = _format_columns(settings_dict.get("unique_input"))
        if cols:
            return f"Unique on {cols}"
        return "Adding unique"

    if node_type == "union":
        return "Adding union"

    if node_type.startswith("read_") or node_type.endswith("_source") or node_type in {"manual_input"}:
        # Sources don't have an upstream; a path / table is the right hint.
        path = settings_dict.get("path") or settings_dict.get("file_path")
        table = settings_dict.get("table_name")
        if isinstance(path, str) and path:
            return f"Reading from `{path}`"
        if isinstance(table, str) and table:
            return f"Reading from `{table}`"
        return f"Adding {pretty_type}"

    return f"Adding {pretty_type}"


def _arg_summary(tool_name: str, tool_args: dict[str, Any]) -> str | None:
    """Server-side fallback summary for a tool call.

    Used when the model didn't emit a rationale preamble. Returns ``None``
    for meta ops (the UI hides them anyway). For ``flowfile.graph.add_*``
    we render a settings-aware one-liner; for ``connect`` / ``delete_*`` /
    ``schema.*`` we render a generic but still informative line.
    """
    if not tool_args:
        tool_args = {}

    if tool_name.startswith(_ADD_PREFIX):
        node_type = tool_name.removeprefix(_ADD_PREFIX)
        return _arg_summary_for_add(node_type, tool_args)

    if tool_name == "flowfile.graph.connect":
        upstream = tool_args.get("upstream_node_id") or tool_args.get("from_node_id")
        downstream = tool_args.get("downstream_node_id") or tool_args.get("to_node_id")
        if isinstance(upstream, int) and isinstance(downstream, int):
            return f"Connecting node {upstream} → node {downstream}"
        return "Connecting nodes"

    if tool_name == "flowfile.graph.delete_node":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Removing node {nid}"
        return "Removing a node"

    if tool_name == "flowfile.graph.update_node_settings":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Updating settings on node {nid}"
        return "Updating node settings"

    if tool_name == "flowfile.graph.delete_connection":
        upstream = tool_args.get("upstream_node_id")
        downstream = tool_args.get("downstream_node_id")
        if isinstance(upstream, int) and isinstance(downstream, int):
            return f"Disconnecting node {upstream} ↛ node {downstream}"
        return "Removing a connection"

    if tool_name == "flowfile.schema.read_node_schema":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Reading schema for node {nid}"
        return "Reading node schema"

    if tool_name == "flowfile.schema.read_node_preview":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Reading preview for node {nid}"
        return "Reading node preview"

    if tool_name.startswith("flowfile.codegen."):
        return f"Generating code ({tool_name.removeprefix('flowfile.codegen.')})"

    if tool_name.startswith("flowfile.meta."):
        return None

    return None


def _allocate_node_id(flow: FlowGraph, session: sessions.AgentSession) -> int:
    """Pick the next free node_id, considering live nodes + in-batch additions.

    The flow's ``_node_db`` is keyed by id; every add_* tool dispatch
    reserves a slot in the staged session as well. Allocating here keeps
    the LLM out of the id-management business — it just emits settings.
    """
    used: set[int] = set()
    for node in flow.nodes:
        try:
            used.add(int(node.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    for entry in session.staged_results:
        if not isinstance(entry.staged_node_payload, dict):
            continue
        settings = entry.staged_node_payload.get("settings")
        if isinstance(settings, dict):
            nid = settings.get("node_id")
            if isinstance(nid, int):
                used.add(nid)
    return (max(used) + 1) if used else 1


def _read_settings_dependency_field(args: dict[str, Any]) -> int | None:
    """Read a primary-upstream settings field out of ``args``.

    The LLM's tool ``arguments`` for ``add_<node_type>`` follow the per-node
    Pydantic settings schema. Most surfaces emit fields at the root of
    ``args`` (e.g. ``{"depending_on_id": 2, "filter_input": {...}, ...}``);
    a few wrap them under ``settings_input``. Mirrors the dual-lookup
    pattern in :func:`_arg_summary_for_add`.

    Returns the int id when a known dependency field is present and parseable,
    else ``None``. See :data:`_SETTINGS_DEPENDENCY_FIELDS` for the canonical
    set.
    """
    nested = args.get("settings_input")
    sources: list[dict[str, Any]] = [args]
    if isinstance(nested, dict):
        sources.append(nested)
    for source in sources:
        for field in _SETTINGS_DEPENDENCY_FIELDS:
            raw = source.get(field)
            if isinstance(raw, int) and raw >= 0:
                # ``-1`` is the NodeSingleInput default sentinel meaning
                # "no dependency configured"; treat as absent.
                return raw
    return None


def _format_ambiguous_insertion_detail(flow: FlowGraph) -> str:
    """Build a refusal_detail string listing the live candidates.

    Used by the W57 ambiguity refusal so the LLM can retry with explicit
    ``upstream_node_ids``. Walks ``flow.nodes`` once; defensive against
    a node missing ``node_id`` / ``node_type``.
    """
    parts: list[str] = []
    for node in flow.nodes:
        try:
            nid = int(node.node_id)
        except (TypeError, ValueError, AttributeError):
            continue
        node_type = getattr(node, "node_type", None) or "unknown"
        parts.append(f"{nid} ({node_type})")
    candidates = ", ".join(parts) if parts else "(none)"
    return (
        "multiple live nodes and no user selection / pin / explicit "
        f"upstream_node_ids — choose one and retry. Candidates: {candidates}"
    )


def _resolve_insertion_context(
    session: sessions.AgentSession,
    tc: ToolCall,
    flow: FlowGraph,
) -> tuple[InsertionContext, str | None]:
    """Build an :class:`InsertionContext` for a tool call.

    Returns ``(insertion_context, ambiguous_detail_or_none)``. The second
    element is always ``None`` post-2026-05-07 — the W57 ambiguity-refusal
    tier was reverted after live UX showed it too aggressive. Tuple shape
    preserved so existing destructuring at call sites keeps working.

    Tier order (W57 + 2026-05-07 revert; 7 tiers):

    1. LLM-provided ``args["upstream_node_ids"]`` (explicit override always
       wins).
    2. Settings-field dependency hint in ``args`` — see
       :data:`_SETTINGS_DEPENDENCY_FIELDS`. The catalog generator exposes
       legacy connection-state fields (``depending_on_id``) AND the planner
       param (``upstream_node_ids``) to the LLM; when only the legacy field
       is set, the resolver canonicalises.
    3. Most-recent in-batch staged ``add_*`` from ``session.staged_results``
       (chained transformations: add_filter → add_sort).
    4. ``session.selected_node_ids`` (W57) — the user's canvas selection at
       start time.
    5. ``session.pinned_node_ids`` (W57) — ``@``-mention targets at start
       time.
    6. Most-recently-added live node whose ``NodeTemplate.output > 0``
       — i.e. skip terminal / sink types (``explore_data``, ``output``,
       ``database_writer``, ``cloud_storage_writer``, ``catalog_writer``)
       in reverse order. (Was ``flow.nodes[-1]`` blindly until 2026-05-07
       afternoon; the original W57 revert flagged this as the residual
       risk and dogfood on the customer_deduplication template hit it
       immediately — the template ends in ``explore_data`` and the
       fallback was attaching the new node downstream of a sink.)
    7. Empty (truly cold flow — no nodes, OR all live nodes are terminals).
       The executor handles this (sources don't need an upstream;
       most node types refuse).
    """
    args: dict[str, Any] = tc.arguments or {}

    upstream_ids: list[int] = []
    ambiguous_detail: str | None = None

    # Tier 0 (W71) — agent_staged stage 3: session state is canonical.
    # The LLM at ``fill_settings`` doesn't see the upstream fields in its
    # tool schema (they're stripped by ``build_staged_fill_tool_spec``);
    # the upstream picker at stage 2 already resolved the choice and
    # stored it on the session. Skip every other tier — these picks
    # cannot be overridden by an LLM that didn't see the schema.
    if session.surface == "agent_staged" and session.stage == "fill_settings":
        upstream_ids = [uid for uid in (session.picked_upstream_ids or []) if isinstance(uid, int)]

    # Tier 1 — explicit planner param.
    if not upstream_ids:
        raw_upstream = args.get("upstream_node_ids")
        if isinstance(raw_upstream, list):
            for uid in raw_upstream:
                if isinstance(uid, int):
                    upstream_ids.append(uid)

    # Tier 2 — settings-field dependency hint.
    if not upstream_ids:
        settings_dep = _read_settings_dependency_field(args)
        if settings_dep is not None:
            upstream_ids = [settings_dep]

    # Tier 3 — most-recent in-batch staged add_*.
    if not upstream_ids:
        for entry in reversed(session.staged_results):
            if not entry.tool_name.startswith(_ADD_PREFIX):
                continue
            payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
            settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
            nid = settings.get("node_id") if isinstance(settings, dict) else None
            if isinstance(nid, int):
                upstream_ids = [nid]
                break

    # Tier 4 — session selection.
    if not upstream_ids and session.selected_node_ids:
        upstream_ids = [uid for uid in session.selected_node_ids if isinstance(uid, int)]

    # Tier 5 — session pinned (@-mention).
    if not upstream_ids and session.pinned_node_ids:
        upstream_ids = [uid for uid in session.pinned_node_ids if isinstance(uid, int)]

    # Tiers 6-7 — live-graph fallback. Tier 6 walks live nodes in reverse
    # and falls back to the most-recently-added node whose template advertises
    # an output port (``NodeTemplate.output > 0``). Sink types (``output=0``
    # — explore_data, output, database_writer, cloud_storage_writer,
    # catalog_writer) are skipped: attaching a downstream node to a sink is
    # always wrong (the sink consumes data, doesn't produce it). Tier 7
    # is the truly-empty case (cold flow OR all live nodes are sinks).
    # Selection / pin tiers above still take precedence when set; LLM override
    # + settings-field hint always win.
    if not upstream_ids:
        live_nodes = flow.nodes
        if live_nodes:
            for candidate in reversed(live_nodes):
                template = getattr(candidate, "node_template", None)
                # ``NodeTemplate.output`` is an int port count. Default to 1
                # when the template is missing or the attribute isn't set —
                # treating an unknown node type as non-terminal preserves the
                # pre-fix behaviour for any node missing a registered template.
                if template is not None and getattr(template, "output", 1) == 0:
                    continue
                try:
                    upstream_ids = [int(candidate.node_id)]
                    break
                except (TypeError, ValueError, AttributeError):
                    continue
        # else / no non-sink found: Tier 7 — leave upstream_ids empty.

    # W71 — at fill_settings, right input is also session-canonical.
    if session.surface == "agent_staged" and session.stage == "fill_settings":
        right_input_node_id = session.picked_right_input_id
    else:
        raw_right = args.get("right_input_node_id")
        right_input_node_id = raw_right if isinstance(raw_right, int) else None

    pos_x = args.get("pos_x")
    pos_y = args.get("pos_y")
    # W62 — leave pos_x / pos_y as ``None`` when the LLM didn't supply
    # numbers so the executor's auto-layout resolver kicks in. The LLM
    # never invents screen coordinates in practice.
    pos_x_val = float(pos_x) if isinstance(pos_x, int | float) else None
    pos_y_val = float(pos_y) if isinstance(pos_y, int | float) else None

    ctx = InsertionContext(
        upstream_node_ids=upstream_ids,
        right_input_node_id=right_input_node_id,
        pos_x=pos_x_val,
        pos_y=pos_y_val,
    )
    return ctx, ambiguous_detail


def _count_prior_staged_with_same_upstream(
    session: sessions.AgentSession,
    upstream_node_ids: list[int],
) -> int:
    """W62 — count prior in-batch staged ``add_*`` entries anchored at the
    same upstream as ``upstream_node_ids``. Threaded into the executor as
    ``staged_offset_index`` so fan-outs from one upstream stack vertically
    instead of overlapping. Chained adds (each with a different upstream)
    naturally see 0 here and lay out as a straight horizontal chain.
    """
    target = list(upstream_node_ids)
    count = 0
    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            continue
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
        ic = payload.get("insertion_context") if isinstance(payload, dict) else None
        if not isinstance(ic, dict):
            continue
        prior_upstream = ic.get("upstream_node_ids")
        if isinstance(prior_upstream, list) and list(prior_upstream) == target:
            count += 1
    return count


def _collect_staged_upstream_positions(
    session: sessions.AgentSession,
) -> dict[int, tuple[float, float]]:
    """W62 — build ``{node_id: (pos_x, pos_y)}`` for every prior in-batch
    staged ``add_*`` so the executor's auto-layout resolver can anchor
    chained adds onto staged-but-unapplied upstreams. Without this the
    second add in a multi-step plan can't find its upstream in
    ``flow.nodes`` (the prior add is only staged, not applied), and the
    resolver falls back to the cold-flow seed at (50, 50).
    """
    out: dict[int, tuple[float, float]] = {}
    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            continue
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
        if not isinstance(payload, dict):
            continue
        settings = payload.get("settings")
        if not isinstance(settings, dict):
            continue
        nid = settings.get("node_id")
        sx = settings.get("pos_x")
        sy = settings.get("pos_y")
        if not isinstance(nid, int):
            continue
        if not (isinstance(sx, int | float) and isinstance(sy, int | float)):
            # Fall back to the staged insertion_context if settings didn't
            # carry coords (defence-in-depth — the executor stamps both).
            ic = payload.get("insertion_context")
            if isinstance(ic, dict):
                sx = ic.get("pos_x")
                sy = ic.get("pos_y")
        if isinstance(sx, int | float) and isinstance(sy, int | float):
            out[int(nid)] = (float(sx), float(sy))
    return out


def _staged_dict_to_flowfile_column(col: dict[str, Any]) -> Any:
    """W71 v1.11 — reconstruct a ``FlowfileColumn``-shaped object from a
    serialized predicted_output_schema dict.

    ``predictor.schema_to_dict_list`` projects a ``FlowfileColumn`` to
    ``{"name": ..., "data_type": ..., "nullable": True}`` for the wire
    payload (and disk persistence). To feed that back into the
    predictor's mirror-graph as an upstream ``predicted_schema``, we
    need objects that quack like ``FlowfileColumn`` — at minimum
    ``column_name`` and ``data_type`` attributes. The mirror's
    schema_callback closures only read those two; the rest of the
    dataclass fields are populated with safe defaults so reflection /
    repr / private attributes don't trip.

    Uses ``__new__`` to bypass ``FlowfileColumn.__init__`` (which
    requires a heavy ``PlType`` argument we don't have on the staging
    path).
    """
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

    obj = FlowfileColumn.__new__(FlowfileColumn)
    obj.column_name = str(col.get("name") or "")
    obj.data_type = str(col.get("data_type") or "Unknown")
    obj.size = 0
    obj.max_value = ""
    obj.min_value = ""
    obj.col_index = 0
    obj.number_of_empty_values = 0
    obj.number_of_unique_values = 0
    obj.example_values = ""
    obj.average_value = None
    # Name-mangled private dataclass fields. Setting them to ``None``
    # matches what the regular ``__init__`` does so any access path
    # (e.g. ``__str__``) doesn't AttributeError.
    obj._FlowfileColumn__has_values = None
    obj._FlowfileColumn__nullable = None
    obj._FlowfileColumn__is_unique = None
    obj._FlowfileColumn__sql_type = None
    obj._FlowfileColumn__perc_unique = None
    try:
        obj.data_type_group = obj.get_readable_datatype_group()
    except Exception:
        obj.data_type_group = None
    return obj


def _collect_staged_upstream_schemas(
    session: sessions.AgentSession,
) -> dict[int, list[Any]]:
    """W71 v1.11 — build ``{node_id: list[FlowfileColumn-like]}`` for every
    prior in-batch staged ``add_*`` whose predicted output schema is
    known.

    Mirrors :func:`_collect_staged_upstream_positions`'s shape (same
    walk over ``session.staged_results``) so the executor receives a
    parallel ``extra_upstream_schemas`` lookup. The predictor consults
    this BEFORE the live-graph ``flow.get_node(uid)`` lookup at Tier
    0a (see ``predictor._resolve_upstream_schemas``). Without it,
    chained add_* calls in a single agent turn produce *"upstream node
    N not found in flow"* warnings on every step — which the LLM
    reads as a failure signal and tries to "fix" by re-staging with
    new ids, leading to a 14-node runaway loop on smaller models
    (Qwen 32B / agent_complex, 2026-05-08 dogfood).
    """
    out: dict[int, list[Any]] = {}
    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            continue
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else None
        if payload is None:
            continue
        settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
        nid = settings.get("node_id") if isinstance(settings, dict) else None
        if not isinstance(nid, int):
            continue
        preds = payload.get("predicted_output_schema")
        if not isinstance(preds, list):
            continue
        cols = [_staged_dict_to_flowfile_column(c) for c in preds if isinstance(c, dict)]
        if cols:
            out[int(nid)] = cols
    return out


_SETTINGS_REPLY_MAX_CHARS = 2000
"""Cap on the JSON-serialised settings echo in the LLM tool reply.

Empirically, simple node settings (filter, group_by, select) serialise to
200-500 chars; complex ones (multi-column joins, formulas with long
expressions) can exceed 1500. The cap keeps the tool reply bounded so a
pathological staging doesn't blow the context budget. When clipped, we
append ``... [truncated]`` so the LLM knows the echo is partial and can
``read_node_schema`` if needed.
"""


def _extract_staged_settings_for_reply(result: ToolExecutionResult) -> str | None:
    """Return a compact JSON of the just-staged settings, for the LLM tool reply.

    2026-05-07 — without this, the only post-staging signal the LLM gets back
    is ``status: staged | predicted columns: ...``. When the user later asks
    to modify the same node, the LLM has to invent the full settings dict
    (W47 contract: pass the *full* settings) — including fields it can't
    know like ``pos_x`` / ``pos_y`` / ``user_id`` / ``depending_on_id``,
    which it then hallucinates with random-looking values that overwrite
    the real ones. Echoing the staged settings closes that loop.

    Handles both ``add_*`` payloads (settings under ``"settings"``) and
    W47 modification payloads (settings under ``"new_settings"``). Returns
    ``None`` for op shapes without a settings dict (connect / delete_node
    / delete_connection).
    """
    payload = result.staged_node_payload
    if not isinstance(payload, dict):
        return None
    settings = payload.get("settings")
    if not isinstance(settings, dict):
        settings = payload.get("new_settings")
    if not isinstance(settings, dict):
        return None
    try:
        rendered = json.dumps(settings, separators=(",", ":"))
    except (TypeError, ValueError):
        return None
    if len(rendered) > _SETTINGS_REPLY_MAX_CHARS:
        rendered = rendered[:_SETTINGS_REPLY_MAX_CHARS] + "... [truncated]"
    return rendered


def _summarise_result_for_llm(result: ToolExecutionResult) -> str:
    """Compact summary of a :class:`ToolExecutionResult` for the LLM tool message.

    The LLM sees this as the ``role="tool"`` reply that closes the loop on
    its prior tool call. Keep it terse but include enough signal for the
    LLM to correct on retry: refusal reason / detail, predicted columns,
    warnings, and (2026-05-07) the just-staged settings dict so subsequent
    ``update_node_settings`` calls can copy current values rather than
    hallucinate them.
    """
    parts: list[str] = [f"status: {result.status}"]
    if result.status == "rejected":
        if result.refusal_reason:
            parts.append(f"refusal: {result.refusal_reason}")
        if result.refusal_detail:
            parts.append(f"detail: {result.refusal_detail}")
    if result.warnings:
        parts.append("warnings: " + "; ".join(result.warnings))
    if result.predicted_output_schema:
        cols = ", ".join(
            str(col.get("name", ""))
            for col in result.predicted_output_schema
            if isinstance(col, dict) and col.get("name")
        )
        if cols:
            parts.append(f"predicted columns: {cols}")
    if result.status in {"staged", "applied", "warned"}:
        settings_json = _extract_staged_settings_for_reply(result)
        if settings_json is not None:
            parts.append(f"settings: {settings_json}")
    if result.extra:
        parts.append(f"extra: {result.extra}")
    return " | ".join(parts)


def _payload_node_id(payload: dict[str, Any] | None) -> int | None:
    """Extract the node id a staged tool call targets.

    For ``add_*`` payloads the id lives on the validated settings dict
    (``payload["settings"]["node_id"]``); the planner emits this for
    ``staged_node_ids`` tracking and for the ``tool_call_staged`` event.
    For W47 ``update_node_settings`` modification payloads, the id is
    a top-level field — modifications target an existing node, so the
    inner ``new_settings.node_id`` is not authoritative (and might be
    omitted by the LLM). We return the top-level ``node_id`` in that
    case for the event payload, but the planner does NOT add modification
    targets to ``staged_node_ids`` (that list is for net-new nodes; the
    W45 drift detector treats modification targets as unchanged).
    """
    if not isinstance(payload, dict):
        return None
    if payload.get("kind") == "modification":
        nid = payload.get("node_id")
        return nid if isinstance(nid, int) else None
    settings = payload.get("settings")
    if not isinstance(settings, dict):
        return None
    nid = settings.get("node_id")
    return nid if isinstance(nid, int) else None


def _collect_live_node_ids(flow: FlowGraph) -> list[int]:
    out: list[int] = []
    for node in flow.nodes:
        try:
            out.append(int(node.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    return out


def _check_self_loop(
    proposed_node_id: int,
    insertion_context: InsertionContext,
) -> str | None:
    """Return a refusal_detail string if ``proposed_node_id`` would self-loop, else None.

    W54 universal invariant: a new node's ``node_id`` may never equal any of
    its own ``upstream_node_ids`` or its ``right_input_node_id``. Catches all
    three plausible upstream causes (LLM-provided collision, stale
    staged_results post-resume, live-graph drift) regardless of which one
    fired — the apply_diff cycle error is the same.
    """
    upstream = list(insertion_context.upstream_node_ids or [])
    right_input = insertion_context.right_input_node_id
    if proposed_node_id in upstream:
        return (
            f"proposed node_id {proposed_node_id} collides with upstream_node_ids "
            f"{upstream} — would create a self-loop on apply"
        )
    if right_input is not None and proposed_node_id == right_input:
        return (
            f"proposed node_id {proposed_node_id} equals right_input_node_id "
            f"{right_input} — would create a self-loop on apply"
        )
    return None


def _op_count(graph_diff: diff_module.GraphDiff) -> int:
    return (
        len(graph_diff.additions)
        + len(graph_diff.connections_added)
        + len(graph_diff.deletions)
        + len(graph_diff.connections_removed)
    )


_AGENT_STAGED_OP_TO_SURFACE: dict[str, str] = {
    "modify": "staged_modify",
    "delete": "staged_delete",
    "connect": "staged_connect",
    "disconnect": "staged_disconnect",
}


def _resolve_current_surface(session: sessions.AgentSession) -> str:
    """Map the session's surface + state to a tool-catalog surface key.

    * ``surface="agent_complex"`` — one-shot full catalog (single round
      with every tool exposed). Big-model power-user path.
    * ``surface="agent_staged"`` — W71 multi-stage state machine. The
      tool catalog is per-stage:
      - ``classify`` → ``"staged_classify"``.
      - ``pick_type`` → ``"staged_pick_type"``.
      - ``pick_upstream`` → ``"staged_pick_upstream"`` (planner overrides
        with a per-turn dynamic spec via
        ``build_pick_upstream_spec``).
      - ``fill_settings`` → telemetry-only key (planner overrides with
        ``build_staged_fill_tool_spec(picked_node_type)``).
      - ``single_stage_op`` → ``"staged_modify"`` / ``"staged_delete"``
        / ``"staged_connect"`` / ``"staged_disconnect"`` per
        ``picked_op_kind``.

    W71 v1.10 — legacy ``surface="agent"`` (two-stage ``pick_category``)
    was removed; the literal type rejects it at request validation, so
    it can't reach this function.
    """
    if session.surface == "agent_complex":
        return "agent_complex"
    # session.surface == "agent_staged" — only remaining alternative.
    if session.stage == "single_stage_op" and session.picked_op_kind is not None:
        mapped = _AGENT_STAGED_OP_TO_SURFACE.get(session.picked_op_kind)
        if mapped is not None:
            return mapped
        # Defensive — picked_op_kind in {add, other} never reaches this
        # branch (add advances through pick_type stages; other terminates
        # the loop). Fall through to classify so the loop self-heals.
        return "staged_classify"
    if session.stage == "pick_type":
        return "staged_pick_type"
    if session.stage == "pick_upstream":
        return "staged_pick_upstream"
    if session.stage == "fill_settings":
        # Telemetry-only: the planner overrides the catalog with a
        # per-turn ``build_staged_fill_tool_spec`` call before
        # dispatch.
        return "staged_pick_upstream"
    return "staged_classify"


def _build_initial_messages(flow: FlowGraph, session: sessions.AgentSession) -> list[Message]:
    """Build ``[system, user]`` from W22 + the user's goal.

    The system block comes from ``assemble_system_prompt(surface)`` (via
    ``render_prompt_context``) — D008's ``base.md`` + ``planner.md`` for
    both ``agent`` and ``agent_complex``. The user block is W22's
    deterministic subgraph snapshot followed by a ``## Goal`` block.

    **Context bug fix (D1 from W40 diagnostic 2026-05-04):** previously
    called with ``pinned_node_ids=[]`` and no ``mentions``, so the user
    saw ``## Subgraph (empty)`` regardless of canvas state — the agent
    was context-blind and refused every cold-flow request even when nodes
    existed. Pass ``mentions="@flow"`` so the resolver expands to all
    current nodes (mirrors W28's chat-route fix and W23's "Fix with AI"
    pattern).
    """
    ctx = render_prompt_context(
        flow,
        [],
        surface=session.surface,
        samples_mode=session.samples_mode,
        mentions="@flow",
        stage=session.stage if session.surface == "agent_staged" else None,
        picked_node_type=session.picked_node_type if session.surface == "agent_staged" else None,
    )
    user_text = f"{ctx.user}\n\n## Goal\n\n{session.user_prompt}".strip()
    return [
        Message(role="system", content=ctx.system),
        Message(role="user", content=user_text),
    ]


def _log_stage_transition(
    session: sessions.AgentSession,
    *,
    from_stage: str,
    to_stage: str,
    tool_name: str,
    op_kind: str | None = None,
    node_type: str | None = None,
    upstream_node_ids: list[int] | None = None,
    completed_op: str | None = None,
) -> None:
    """W71 — emit a structured INFO line per stage transition.

    Pairs with the prompt log (``FLOWFILE_AI_LOG_PROMPTS=true``) to give
    a complete debugging picture without enabling DEBUG: the prompt log
    captures the raw LLM input/output per stage; this line captures the
    state-machine transition itself, with the session id so multiple
    concurrent sessions stay disambiguated.

    Format: ``planner.staged session=<sid> from=<from> to=<to>
    tool=<name> op_kind=<ok> node_type=<nt> upstream=<ids>
    completed_op=<co>``. Tail with::

        tail -F ~/.flowfile/logs/flowfile_core.log | grep planner.staged
    """
    parts: list[str] = [
        f"session={session.session_id[:8]}",
        f"from={from_stage}",
        f"to={to_stage}",
        f"tool={tool_name}",
    ]
    if op_kind:
        parts.append(f"op_kind={op_kind}")
    if node_type:
        parts.append(f"node_type={node_type}")
    if upstream_node_ids is not None:
        parts.append(f"upstream={upstream_node_ids}")
    if completed_op:
        parts.append(f"completed_op={completed_op}")
    logger.info("planner.staged %s", " ".join(parts))


def _walk_balanced_braces(content: str, start_idx: int) -> int:
    """Return the index just after the closing ``}`` that pairs with
    the ``{`` at ``start_idx``, or ``-1`` if no balanced pair exists.

    String-context-aware: open/close braces inside JSON strings are
    ignored so the depth counter doesn't get fooled by a value like
    ``"text {with} braces"``. Used by the JSON-object-shape recovery
    path to find a candidate object's bounds before passing the
    substring to ``json.loads``.
    """
    if start_idx >= len(content) or content[start_idx] != "{":
        return -1
    depth = 1
    in_str = False
    esc = False
    for i in range(start_idx + 1, len(content)):
        c = content[i]
        if esc:
            esc = False
            continue
        if in_str and c == "\\":
            esc = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
    return -1


def _try_parse_function_call_shape(
    content: str, expected_tool_names: set[str]
) -> list[tuple[str, dict[str, Any]]]:
    """W71 v1.6 shape — find ``<tool_name>(<json>)`` invocations.

    Walks balanced parens (string-aware) for each expected tool name.
    Returns a list of ``(name, args)`` matches. The caller decides
    whether to dispatch (single match) or decline (multiple distinct
    names → summary).
    """
    found: list[tuple[str, dict[str, Any]]] = []
    for tool_name in expected_tool_names:
        marker = f"{tool_name}("
        idx = content.find(marker)
        if idx < 0:
            continue
        start = idx + len(marker)
        depth = 1
        end = -1
        in_str = False
        esc = False
        for i in range(start, len(content)):
            c = content[i]
            if esc:
                esc = False
                continue
            if in_str and c == "\\":
                esc = True
                continue
            if c == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end < 0:
            continue
        try:
            args = json.loads(content[start:end])
        except (TypeError, ValueError):
            continue
        if isinstance(args, dict):
            found.append((tool_name, args))
    return found


def _try_parse_json_object_shape(
    content: str, expected_tool_names: set[str]
) -> list[tuple[str, dict[str, Any]]]:
    """W71 v1.8 shape — find ``{"name": "...", "parameters": {...}}``
    or the ``"arguments"`` alias.

    Llama-3.3-8b (and other small open-weights models) often emit the
    OpenAI-style function-call envelope as text content rather than via
    the function-calling API. This helper walks the content for
    balanced ``{...}`` blocks, attempts ``json.loads`` on each, and
    accepts when the parsed object has ``name``/``tool`` plus
    ``parameters``/``arguments`` AND the name is in
    ``expected_tool_names``.
    """
    found: list[tuple[str, dict[str, Any]]] = []
    i = 0
    while i < len(content):
        if content[i] != "{":
            i += 1
            continue
        end = _walk_balanced_braces(content, i)
        if end < 0:
            break
        candidate = content[i:end]
        try:
            obj = json.loads(candidate)
        except (TypeError, ValueError):
            obj = None
        i = end
        if not isinstance(obj, dict):
            continue
        name = obj.get("name") or obj.get("tool")
        args = obj.get("parameters")
        if args is None:
            args = obj.get("arguments")
        if not isinstance(name, str) or name not in expected_tool_names or not isinstance(args, dict):
            continue
        found.append((name, args))
    return found


def _recover_textual_tool_call(
    content: str,
    expected_tool_names: set[str],
) -> ToolCall | None:
    """W71 v1.6 + v1.8 — last-resort parse for LLMs that emit a
    function-call invocation as **text content** rather than via the
    function-calling API.

    Two LLM-emitted shapes are recognised:

    * **v1.6 shape** — ``flowfile.graph.add_sort({"sort_input": ...})``.
      Llama-3.3-70b's typical fallback when it almost-but-not-quite
      uses the function-calling API.
    * **v1.8 shape** — ``{"name": "flowfile.meta.classify_intent",
      "parameters": {"op_kind": "add", ...}}`` (OpenAI-envelope form).
      Llama-3.3-8b's typical fallback even at the simplest stage 0.
      The ``"arguments"`` alias is also accepted because some models
      use that key name instead of ``"parameters"``.

    Robustness rules:

    * Only matches against ``expected_tool_names`` (the catalog the
      current stage exposed). Defends against the model emitting an
      unrelated tool name in its prose.
    * Returns ``None`` when **multiple distinct** expected names
      match across both shapes — that's almost always a "summary of
      past calls" style message after a successful add. Re-firing
      those would double-stage. The loop's normal "no tool calls →
      terminate" path handles the summary case correctly.
    """
    if not content or not expected_tool_names:
        return None

    found = _try_parse_function_call_shape(content, expected_tool_names)
    found.extend(_try_parse_json_object_shape(content, expected_tool_names))

    # Dedupe by tool name — a single call can match both shapes if
    # the model emits both forms; that's still one logical call.
    distinct_names = {name for name, _ in found}
    if len(distinct_names) != 1 or not found:
        return None
    tool_name = next(iter(distinct_names))
    # Pick the args from the first match for that name.
    args = next(args for name, args in found if name == tool_name)

    # Synthetic id for the tool message correlation. Stable hash of the
    # call string keeps it deterministic for tests; the planner's
    # ``role="tool"`` reply uses this id so the LLM can correlate on
    # the next turn.
    sha = abs(hash((tool_name, json.dumps(args, sort_keys=True)))) % 10_000_000
    return ToolCall(
        id=f"recovered_{tool_name.replace('.', '_')}_{sha}",
        name=tool_name,
        arguments=args,
    )


def _build_fill_settings_user_message(
    session: sessions.AgentSession, flow: FlowGraph
) -> str | None:
    """W71 v1.5 — focused user message for the ``fill_settings`` stage.

    By the time we reach stage 3, the only context the LLM needs is:

    1. **The user's actual ask** (their original prompt; for auto-promote
       sessions this includes the chat transcript that produced the
       intent).
    2. **The picked upstream's column schema** so the LLM can reference
       valid column names in the new node's settings. Reading the rest
       of the subgraph at this point is a distraction.

    Without this slim, ~4.5k chars of irrelevant subgraph + every other
    node's settings dict ride into stage 3 and small models like
    llama-3.3-70b end up writing a rationale instead of calling the
    only-tool-in-its-array (dogfood 2026-05-08 PM).

    Returns ``None`` when there's no picked upstream (i.e. the helper
    was called outside fill_settings or stage 2 didn't produce one).
    """
    from flowfile_core.ai.context.builder import _safe_get_predicted_schema

    upstream_ids = list(session.picked_upstream_ids or [])
    if session.picked_right_input_id is not None:
        upstream_ids.append(session.picked_right_input_id)
    if not upstream_ids:
        return None

    lines: list[str] = ["## Your task", "", session.user_prompt.strip(), ""]

    # Helper to render one upstream's column block from staged_results
    # when the node isn't in flow.nodes yet (chained add within one
    # user turn — the prior add hasn't been applied because this
    # session's diff is still being assembled).
    def _staged_schema_for(node_id: int) -> list[dict[str, Any]] | None:
        for entry in session.staged_results:
            payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else None
            if payload is None:
                continue
            settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
            staged_id = settings.get("node_id") if isinstance(settings, dict) else None
            if staged_id != node_id:
                continue
            preds = payload.get("predicted_output_schema")
            if isinstance(preds, list):
                return preds
            return None
        return None

    for uid in upstream_ids:
        node = flow.get_node(uid)
        cols_text: list[str] = []
        if node is not None:
            predicted = _safe_get_predicted_schema(node)
            if predicted:
                for col in predicted:
                    name = getattr(col, "column_name", "?")
                    dtype = getattr(col, "data_type", "Unknown")
                    cols_text.append(f"- {name}: {dtype}")
        else:
            staged_preds = _staged_schema_for(uid)
            if staged_preds is not None:
                for col in staged_preds:
                    if isinstance(col, dict):
                        cols_text.append(
                            f"- {col.get('name', '?')}: {col.get('data_type', 'Unknown')}"
                        )

        label_kind = (
            "Right input"
            if session.picked_right_input_id is not None and uid == session.picked_right_input_id
            else "Upstream"
        )
        if cols_text:
            lines.append(f"## {label_kind} node {uid} columns")
            lines.append("")
            lines.extend(cols_text)
            lines.append("")
        else:
            # Schema-unknown path. Emit the marker so the LLM knows to
            # refuse rather than hallucinate column names. Mirrors the
            # ``schema: unknown`` posture of ``render_user_message``.
            lines.append(f"## {label_kind} node {uid} columns")
            lines.append("")
            lines.append("schema: unknown — upstream has no predicted schema")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _refresh_system_prompt_for_stage(
    session: sessions.AgentSession, flow: FlowGraph | None = None
) -> None:
    """W71 — replace ``session.messages[0]`` with a freshly-rendered system
    prompt for the current stage.

    No-op when the surface is not ``agent_staged`` or when the session
    doesn't have a system message yet (the initial system prompt is built
    by :func:`_build_initial_messages` on first entry to the loop).

    Stages have different suffix files and different catalog scopes, so
    the system prompt must be re-assembled when the stage changes.
    Updating ``messages[0]`` in place keeps the rest of the conversation
    history intact (assistant turns, tool replies). The prompt cache is
    invalidated by the change but per-stage prompts are smaller, so the
    re-keyed cache is cheap to fill.

    W71 v1.5 — when ``flow`` is provided AND the stage is
    ``fill_settings``, also rewrite ``session.messages[1]`` (the user
    message) with a focused mini-prompt that contains only the user's
    goal + the picked upstream's column schema. The full subgraph
    embedded by :func:`_build_initial_messages` is irrelevant once the
    upstream is locked in; keeping it bloats the prompt and confuses
    smaller models like llama-3.3-70b.
    """
    if session.surface != "agent_staged":
        return
    if not session.messages or session.messages[0].role != "system":
        return
    from flowfile_core.ai.context.builder import assemble_system_prompt

    new_system = assemble_system_prompt(
        session.surface,
        stage=session.stage,
        picked_node_type=session.picked_node_type,
    )
    session.messages[0] = Message(role="system", content=new_system)

    if (
        session.stage == "fill_settings"
        and flow is not None
        and len(session.messages) >= 2
        and session.messages[1].role == "user"
    ):
        slim_user = _build_fill_settings_user_message(session, flow)
        if slim_user is not None:
            session.messages[1] = Message(role="user", content=slim_user)


def _build_staged_tool_catalog(
    session: sessions.AgentSession,
    flow: FlowGraph,
) -> tuple[list, str | None]:
    """W71 — build the per-turn tool catalog for ``agent_staged``.

    Returns ``(tools, error_detail)``. Most stages dispatch through the
    static ``SURFACE_PRESETS`` lookup; two stages need per-turn dynamic
    specs because their schemas depend on session state:

    * ``pick_upstream`` — the upstream-id enum is the union of currently-
      live node ids and ids the agent has staged this session. Building
      per-turn means a multi-node turn (filter → sort) sees the prior
      add's id in the picker enum without history pruning.
    * ``fill_settings`` — the single tool exposed is the
      ``flowfile.graph.add_<picked_type>`` Pydantic schema with planner-
      injected fields stripped. ``picked_node_type`` is set by the prior
      stage; defensive ``None`` fallback returns an error string instead
      of an empty catalog so the loop refuses cleanly.
    """
    if session.stage == "fill_settings":
        if not session.picked_node_type:
            return [], "agent_staged: stage=fill_settings without picked_node_type"
        spec = build_staged_fill_tool_spec(session.picked_node_type)
        if spec is None:
            return [], (
                f"agent_staged: picked_node_type {session.picked_node_type!r} "
                "has no registered settings class"
            )
        return [spec], None

    if session.stage == "pick_upstream":
        live_ids = _collect_live_node_ids(flow)
        spec = build_pick_upstream_spec(live_ids, list(session.staged_node_ids))
        return [spec], None

    # All other stages map to a static surface preset via _resolve_current_surface.
    return [], None


# --------------------------------------------------------------------------- #
# Main loop                                                                    #
# --------------------------------------------------------------------------- #


FollowupAction = Literal["rejected_diff", "user_message"]


_REJECTED_DIFF_DEFAULT_NOTE: str = "no specific reason provided"
"""Generic rejection reason used by :func:`inject_followup_message` when the
user clicks Reject without typing an explanation. Surfaces in the synthetic
followup turn so the model sees *something* signalling the rejection rather
than just an empty user message."""


def inject_followup_message(
    session: sessions.AgentSession,
    *,
    action: FollowupAction,
    message: str | None = None,
    rejected_diff_id: str | None = None,
) -> Message:
    """W49 — append the synthetic followup turn to ``session.messages``.

    Two action shapes:

    * ``"rejected_diff"`` — the user clicked Reject on a staged diff. The
      synthesised content carries the optional user-supplied note (or a
      generic *"no specific reason provided"* fallback) plus the rejected
      ``diff_id`` for diagnostics so the model sees an explicit "course
      correct" signal rather than re-emitting the same plan.
    * ``"user_message"`` — the user typed a new instruction after a
      ``complete`` / ``awaiting_user_input``. The text is appended verbatim
      as the next user turn.

    **Why ``role="user"`` instead of ``role="tool"``** — a ``role="tool"``
    message must be paired with a preceding assistant turn whose
    ``tool_calls`` carries the matching ``tool_call_id``. By the time the
    planner has completed, the last assistant turn has no tool calls (that
    is what triggered the completion); injecting an unpaired ``tool``
    message would be rejected by litellm / Anthropic / OpenAI on the next
    chat call. ``role="user"`` is semantically equivalent to "the human
    sent feedback" and works across every provider.

    The function mutates ``session.messages`` in place and returns the
    appended :class:`Message` for the caller's bookkeeping. Callers
    (``agent_routes.followup``) typically follow this with
    :func:`run_planner_session` so the planner's followup-resume entry path
    re-snapshots the graph + drops staged bookkeeping before the next chat
    call.
    """
    if action == "rejected_diff":
        note = (message or "").strip() or _REJECTED_DIFF_DEFAULT_NOTE
        diff_ref = rejected_diff_id or session.diff_id or "unknown"
        content = (
            "[The user rejected the previously staged diff "
            f"(diff_id={diff_ref}).]\n"
            f"User's reason: {note}\n\n"
            "Treat this as authoritative feedback: do not re-emit the same plan. "
            "Re-plan based on the user's reason; if the reason names a different "
            "upstream node or a different transformation, follow that lead."
        )
    elif action == "user_message":
        content = (message or "").strip()
        if not content:
            raise ValueError("user_message followup requires a non-empty message")
    else:
        raise ValueError(f"unknown followup action: {action!r}")

    msg = Message(role="user", content=content)
    session.messages.append(msg)
    return msg


async def run_planner_session(
    *,
    session: sessions.AgentSession,
    flow: FlowGraph,
    provider: Provider,
    scheduler: RateLimitScheduler | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    max_retries_per_step: int = DEFAULT_MAX_RETRIES_PER_STEP,
) -> AsyncIterator[PlannerEvent]:
    """Drive a planner session to completion / pause / failure.

    Pure async generator. **Never raises** — every failure becomes an
    ``error`` / ``tool_call_rejected`` / ``drift_detected`` / ``abort``
    event with a stable shape. The caller (``agent_routes.py``) wraps
    the generator in SSE; tests consume the raw events.

    Mutations to the in-memory ``session`` (status, step_count,
    staged_results, messages, drift_detail, diff_id, rationale) are
    in-place — callers can inspect the session after the generator
    exhausts. W42 disk-persists these fields verbatim.
    """
    try:
        async for event in _run_planner_loop(
            session=session,
            flow=flow,
            provider=provider,
            scheduler=scheduler,
            max_tokens=max_tokens,
            max_retries_per_step=max_retries_per_step,
        ):
            yield event
    except Exception as exc:  # noqa: BLE001 — last-resort safety net
        logger.exception("run_planner_session crashed unexpectedly")
        try:
            session.status = "failed"
            session.touch()
        except Exception:
            pass
        yield PlannerEvent(event="error", payload={"message": f"planner crashed: {exc}"})


async def _run_planner_loop(
    *,
    session: sessions.AgentSession,
    flow: FlowGraph,
    provider: Provider,
    scheduler: RateLimitScheduler | None,
    max_tokens: int,
    max_retries_per_step: int,
) -> AsyncIterator[PlannerEvent]:
    # ``aborted`` is honored as an early-exit (the abort route flips status
    # to ``aborted`` between iterations). Same for ``running`` (normal start),
    # ``paused_drift`` (resume after D006 drift), ``paused_user_action``
    # (W42 cold-start re-attach), and ``completed`` / ``awaiting_user_input``
    # (W49 post-completion followup re-entry).
    if session.status == "aborted":
        yield PlannerEvent(event="abort", payload={"session_id": session.session_id})
        return
    if session.status not in (
        "running",
        "paused_drift",
        "paused_user_action",
        "completed",
        "awaiting_user_input",
    ):
        yield PlannerEvent(
            event="error",
            payload={"message": f"cannot run session in status {session.status!r}"},
        )
        return

    if session.status in ("completed", "awaiting_user_input"):
        # W49 — post-completion followup re-entry. The route already
        # appended the synthetic ``user`` / ``tool`` message that drives
        # the next planner turn; everything we do here is housekeeping so
        # the resumed run starts from a clean slate:
        #
        # * **Re-snapshot the graph (D006).** The user may have mutated
        #   the canvas between completion and the followup, so we capture
        #   a fresh baseline. The very next ``detect_drift`` round
        #   compares against this; if drift fires, the loop pauses
        #   exactly like a mid-run mutation would.
        # * **Drop the prior diff bookkeeping.** ``staged_results`` /
        #   ``staged_node_ids`` / ``diff_id`` reflect the *previous* round
        #   (rejected, abandoned, or never-bundled). Keeping them would
        #   double-count node ids in ``_allocate_node_id`` and re-bundle
        #   already-rejected ops on the next ``complete``.
        # * **Reset retry / drift bookkeeping.** ``rationale`` is rebuilt
        #   from the next assistant turn; ``last_assistant_text`` was
        #   used by W49's question-detection on the prior completion and
        #   is no longer authoritative.
        was_completion = session.status
        session.snapshot = sessions.capture_graph_snapshot(flow)
        session.staged_results = []
        session.staged_node_ids = []
        session.diff_id = None
        session.rationale = None
        session.drift_detail = None
        session.pause_reason = None
        session.last_assistant_text = None
        session.status = "running"
        session.touch()
        yield PlannerEvent(
            event="info",
            payload={
                "message": "followup; re-snapshotted graph",
                "previous_status": was_completion,
            },
        )

    if session.status == "paused_user_action":
        # W42 — cold-start resume. The previous SSE stream is dead and an
        # arbitrary amount of time has passed. Re-snapshot, clear the
        # pause reason, flip to running. We don't revalidate staged_results
        # eagerly here — the subsequent drift_detect run will surface any
        # missing upstream as a paused_drift event (and that path already
        # owns ``revalidate_staged_results_against_live``).
        session.snapshot = sessions.capture_graph_snapshot(flow)
        session.pause_reason = None
        session.status = "running"

    if session.status == "paused_drift":
        # Resume after drift — re-snapshot so subsequent drift_detect compares against fresh state.
        session.snapshot = sessions.capture_graph_snapshot(flow)
        session.drift_detail = None
        session.pause_reason = None
        session.status = "running"

        # W54 — staged_results hygiene. Drop entries whose node_id now
        # collides with a live node (user manually added one mid-pause)
        # or whose upstream references an id that no longer exists (user
        # deleted the upstream). One audit row per drop so the cause is
        # diagnosable post-hoc. ``awaiting_user`` is reserved-but-unused
        # today, so we only thread hygiene through the paused_drift path.
        _, dropped = sessions.revalidate_staged_results_against_live(session, flow)
        for entry, reason in dropped:
            try:
                audit_module.record_event(
                    audit_module.AuditEvent(
                        session_id=session.session_id,
                        user_id=session.user_id,
                        tool_name="internal.staged_drop_on_resume",
                        flow_id=session.flow_id,
                        result_status="error",
                        error=f"staged_drop_on_resume:{reason}",
                        tool_args={
                            "__planner_meta__": {
                                "dropped_tool_name": entry.tool_name,
                                "dropped_payload": entry.staged_node_payload,
                                "dropped_audit_id": entry.audit_id,
                                "reason": reason,
                                "live_node_ids_at_resume": sorted(_collect_live_node_ids(flow)),
                            }
                        },
                    )
                )
            except Exception:  # noqa: BLE001 — audit-side errors must not crash the loop
                logger.warning("audit.record_event failed for staged_drop_on_resume", exc_info=False)
        if dropped:
            yield PlannerEvent(
                event="info",
                payload={
                    "message": "resumed; dropped stale staged entries",
                    "dropped_count": len(dropped),
                    "drop_reasons": [reason for _, reason in dropped],
                },
            )

        session.touch()
        yield PlannerEvent(
            event="info",
            payload={"message": "resumed; re-snapshotted graph"},
        )

    sched = scheduler or default_scheduler()
    dry_run_cache = DryRunCache()
    retries_for_step = 0

    if not session.messages:
        session.messages = _build_initial_messages(flow, session)

    while True:
        if session.step_count >= session.max_steps:
            session.status = "failed"
            session.touch()
            yield PlannerEvent(
                event="error",
                payload={"message": "max_steps reached", "max_steps": session.max_steps},
            )
            return

        if session.status == "aborted":
            yield PlannerEvent(
                event="abort",
                payload={"session_id": session.session_id},
            )
            return

        # D006 — drift check before every dispatch. ``staged_node_ids``
        # excludes the agent's own staged additions from the external-added
        # bucket so the planner doesn't self-pause on its own work (W45 Q1).
        drift = sessions.detect_drift(
            flow,
            session.snapshot,
            agent_staged_node_ids=set(session.staged_node_ids),
        )
        if drift is not None:
            session.status = "paused_drift"
            session.drift_detail = drift
            session.pause_reason = "graph_changed"
            session.touch()
            yield PlannerEvent(
                event="drift_detected",
                payload={"drift": drift.model_dump(), "session_id": session.session_id},
            )
            yield PlannerEvent(
                event="paused",
                payload={"reason": "graph_changed", "session_id": session.session_id},
            )
            return

        current_surface = _resolve_current_surface(session)

        # W71 — refresh the system prompt for the current stage. The first
        # iteration's system prompt was set in ``_build_initial_messages``;
        # subsequent stage advances need a re-render so the LLM sees the
        # right per-stage suffix and (at fill_settings) the picked type's
        # single-node block. v1.5 also slims the user message at
        # fill_settings to drop the full subgraph noise — passing
        # ``flow`` lets the helper resolve the picked upstream's
        # predicted schema for the focused mini-prompt.
        _refresh_system_prompt_for_stage(session, flow)

        try:
            if session.surface == "agent_staged" and session.stage in ("pick_upstream", "fill_settings"):
                tool_catalog, dyn_err = _build_staged_tool_catalog(session, flow)
                if dyn_err is not None:
                    session.status = "failed"
                    session.touch()
                    yield PlannerEvent(event="error", payload={"message": dyn_err})
                    return
            else:
                tool_catalog = build_tool_catalog(surface=current_surface)
        except KeyError as exc:
            session.status = "failed"
            session.touch()
            yield PlannerEvent(event="error", payload={"message": f"unknown surface: {exc}"})
            return

        # --- Provider call ---
        # W71 — pass surface/session_id/user_id through so the prompt log
        # (FLOWFILE_AI_LOG_PROMPTS=true) tags each entry with the stage
        # the call came from. Without this, every planner entry lands as
        # ``surface=null`` and you can't grep by stage post-hoc.
        try:
            async with sched.acquire(provider.name, surface=current_surface):
                response = await provider.chat(
                    messages=session.messages,
                    tools=tool_catalog,
                    max_tokens=max_tokens,
                    surface=current_surface,
                    session_id=session.session_id,
                    user_id=session.user_id,
                )
        except Exception as exc:  # noqa: BLE001 — collapse to a stable reason
            logger.warning("planner provider call failed: %s", exc)
            session.status = "failed"
            session.touch()
            yield PlannerEvent(event="error", payload={"message": f"provider error: {exc}"})
            return

        assistant_text = response.content or ""
        if assistant_text:
            session.last_assistant_text = assistant_text
        assistant_msg = Message(
            role="assistant",
            content=assistant_text or None,
            tool_calls=list(response.tool_calls) if response.tool_calls else None,
        )
        session.messages.append(assistant_msg)

        tool_calls = list(response.tool_calls or [])

        # W71 v1.6 + v1.8 — last-resort text-JSON recovery for small
        # models that emit the function call as content prose instead
        # of via the function-calling API. Applied at ALL agent_staged
        # stages now (v1.8): llama-3.3-8b fails this way at stage 0
        # too, not just stage 3. The "exactly one match" + "name must
        # be in expected_tool_names" rules in
        # ``_recover_textual_tool_call`` are sufficient to defend
        # against the after-add summary pattern (multiple distinct
        # tool names → declines).
        if (
            not tool_calls
            and assistant_text
            and session.surface == "agent_staged"
        ):
            expected_names = {t.name for t in tool_catalog}
            recovered = _recover_textual_tool_call(assistant_text, expected_names)
            if recovered is not None:
                tool_calls = [recovered]
                # Patch the assistant message we just appended so the
                # provider sees a coherent history on subsequent rounds
                # — the recovered tool_call gets paired with a
                # ``role="tool"`` reply downstream, which requires the
                # corresponding tool_calls to have appeared on the
                # prior assistant turn.
                session.messages[-1] = Message(
                    role="assistant",
                    content=assistant_text or None,
                    tool_calls=[recovered],
                )
                logger.info(
                    "planner.staged session=%s recovered_text_json_tool_call tool=%s stage=%s",
                    session.session_id[:8],
                    recovered.name,
                    session.stage,
                )

        # W38 — when the assistant turn is pure prose (no tool calls), surface
        # it as a ``thinking`` event so the user sees what the model said.
        # When tool calls follow, the same text rides on each ``tool_call_*``
        # event as ``rationale`` (the W38 contract), so emitting both would
        # render the same sentence twice in the chat trail.
        if assistant_text and not tool_calls:
            yield PlannerEvent(event="thinking", payload={"text": assistant_text})

        if not tool_calls:
            # W71 v1.7 — at mandatory-tool-call stages, an empty
            # ``tool_calls`` (even after v1.6/v1.8 text-JSON recovery
            # tried) means the LLM emitted unparseable prose — e.g.
            # llama-70b's token-corruption case
            # ("altimoreFiltering to rows ..."). Treat it as a
            # no-progress retry instead of a terminal "nothing to
            # stage": append a synthetic reminder so the next round
            # explicitly tells the LLM what's expected, and route
            # through the existing ``max_retries_per_step`` budget.
            # Classify stage stays on the legacy break path —
            # there an empty ``tool_calls`` is a valid termination
            # signal (op_kind="other" handled, or LLM has nothing
            # more to add).
            if (
                session.surface == "agent_staged"
                and session.stage in _MANDATORY_TOOL_CALL_STAGES
            ):
                retries_for_step += 1
                if retries_for_step >= max_retries_per_step:
                    session.status = "failed"
                    session.touch()
                    yield PlannerEvent(
                        event="error",
                        payload={
                            "message": (
                                f"agent_staged stage={session.stage}: LLM emitted "
                                f"prose with no tool_call across "
                                f"{max_retries_per_step} consecutive rounds; check "
                                "the prompt log for the model's response shape"
                            ),
                        },
                    )
                    return
                expected_tool = next(
                    iter(t.name for t in tool_catalog), "the available tool"
                )
                session.messages.append(
                    Message(
                        role="user",
                        content=(
                            f"Your previous response was prose only — no function "
                            f"call. You MUST call ``{expected_tool}`` via the "
                            f"function-calling API to advance. Do not write the "
                            f"call as text in your response; emit a real tool "
                            f"call with the correct arguments."
                        ),
                    )
                )
                yield PlannerEvent(
                    event="retry",
                    payload={
                        "attempt": retries_for_step,
                        "max": max_retries_per_step,
                    },
                )
                continue
            # All other stages / surfaces — preserve the existing
            # termination path so legacy ``agent`` / ``agent_complex``
            # and the ``classify`` stage of ``agent_staged`` keep
            # their current "LLM is done" semantics.
            break

        any_succeeded_this_round = False

        # W38 — capture the assistant preamble that landed alongside this turn's
        # tool calls; it's the natural-language "what this step does" that the
        # planner.md prompt asks the model to emit. Shared across every tool
        # call in this round (the model writes one preamble per turn, even when
        # it ends up emitting multiple calls). Falls back to ``None`` when the
        # model skipped the preamble — the per-call ``arg_summary`` covers the
        # rendering gap.
        rationale_for_round = _capture_rationale(assistant_text)

        for tc in tool_calls:
            op_kind = _classify_op_kind(tc.name)
            # Rationale only attaches to user-facing ops. Meta ops are LLM-internal
            # routing and the UI hides them; if the model wrote a preamble for a
            # round whose only call is meta, that preamble belongs to whatever
            # *next* round of work the meta call is selecting for, not to the
            # meta call itself.
            rationale = rationale_for_round if op_kind != "meta" else None
            arg_summary = _arg_summary(tc.name, tc.arguments or {})

            yield PlannerEvent(
                event="tool_call_proposed",
                payload={
                    "id": tc.id,
                    "name": tc.name,
                    "arguments": tc.arguments,
                    "op_kind": op_kind,
                    "rationale": rationale,
                    "arg_summary": arg_summary,
                },
            )

            # Inject planner-managed args for add_* dispatches
            tool_args: dict[str, Any] = dict(tc.arguments) if tc.arguments else {}
            # W71 v1.2 — at fill_settings on agent_staged, the LLM-facing
            # tool spec exposes only the inner-input shape (e.g.
            # ``GroupByInput``: top-level ``agg_cols``) for single-input
            # node types. The executor's settings validation expects the
            # full Pydantic envelope (``NodeGroupBy.groupby_input.agg_cols``),
            # so wrap the LLM's args under the resolved field name before
            # the existing flow_id / node_id injection runs. Multi-field
            # types fall back to the flat-stripped spec (no wrap needed).
            if (
                session.surface == "agent_staged"
                and session.stage == "fill_settings"
                and tc.name.startswith(_ADD_PREFIX)
            ):
                picked_type = tc.name.removeprefix(_ADD_PREFIX)
                inner_field = get_staged_fill_inner_field_name(picked_type)
                if inner_field is not None and inner_field not in tool_args:
                    # Wrap every key in tool_args under inner_field, except
                    # planner-injected fields (flow_id / node_id) which
                    # belong at the envelope level. The LLM never sees
                    # those at fill_settings, so in practice tool_args
                    # contains only inner-shape keys at this point — but
                    # the conditional defends against future schema drift.
                    inner_args = {
                        k: v
                        for k, v in tool_args.items()
                        if k not in {"flow_id", "node_id", "upstream_node_ids", "right_input_node_id"}
                    }
                    tool_args = {
                        k: v
                        for k, v in tool_args.items()
                        if k in {"flow_id", "node_id", "upstream_node_ids", "right_input_node_id"}
                    }
                    tool_args[inner_field] = inner_args
            # W54 — capture provenance: did the LLM emit ``node_id`` itself,
            # or did the planner allocate? Both values flow into audit_meta
            # so the audit row alone shows whether a self-loop traced back
            # to an LLM hallucination or a planner allocation collision.
            llm_provided_node_id: int | None = None
            allocated_node_id: int | None = None
            if tc.name.startswith(_ADD_PREFIX):
                raw_llm_id = tool_args.get("node_id")
                if isinstance(raw_llm_id, int):
                    llm_provided_node_id = raw_llm_id
                tool_args.setdefault("flow_id", session.flow_id)
                if "node_id" not in tool_args:
                    tool_args["node_id"] = _allocate_node_id(flow, session)
                    raw_allocated = tool_args.get("node_id")
                    if isinstance(raw_allocated, int):
                        allocated_node_id = raw_allocated

            insertion_context, ambiguous_detail = _resolve_insertion_context(session, tc, flow)

            # W54 — build audit_meta for instrumentation. Rides on
            # tool_args["__planner_meta__"] in the persisted audit row.
            # Always populated for add_* calls (success or rejected) so
            # any future self-loop is diagnosable from the audit row alone.
            audit_meta: dict[str, Any] | None = None
            if tc.name.startswith(_ADD_PREFIX):
                audit_meta = {
                    "allocated_node_id": allocated_node_id,
                    "llm_provided_node_id": llm_provided_node_id,
                    "resolved_upstream_node_ids": list(insertion_context.upstream_node_ids or []),
                    "right_input_node_id": insertion_context.right_input_node_id,
                    "live_node_ids_at_stage": sorted(_collect_live_node_ids(flow)),
                    "staged_node_ids_at_stage": list(session.staged_node_ids),
                }

            # W57 ambiguity-refusal block removed 2026-05-07 — the resolver
            # now falls back to ``live_nodes[-1]`` for the multi-live-node
            # case rather than refusing. ``ambiguous_detail`` is always
            # ``None`` post-revert; preserved in the tuple shape for call-
            # site destructuring stability.

            # W54 — universal self-loop invariant guard. Catches all three
            # plausible upstream causes (LLM-provided collision, stale
            # staged_results post-resume, live-graph drift). When it fires,
            # treat as ``tool_call_rejected`` with refusal_reason
            # ``self_loop_prevented``; counts toward W53's retry budget;
            # writes its own audit row (we never reach execute_tool_call,
            # which is what would otherwise persist the audit).
            if tc.name.startswith(_ADD_PREFIX):
                proposed = tool_args.get("node_id")
                if isinstance(proposed, int):
                    self_loop_detail = _check_self_loop(proposed, insertion_context)
                    if self_loop_detail is not None:
                        audit_id_for_event: int | None = None
                        try:
                            audit_row = audit_module.record_event(
                                audit_module.AuditEvent(
                                    session_id=session.session_id,
                                    user_id=session.user_id,
                                    tool_name=tc.name,
                                    flow_id=session.flow_id,
                                    result_status="rejected",
                                    error=self_loop_detail,
                                    tool_args={
                                        **(safety.redact_secrets(tool_args) if tool_args else {}),
                                        "__planner_meta__": audit_meta,
                                    },
                                )
                            )
                            audit_id_for_event = audit_row.id if audit_row is not None else None
                        except Exception:  # noqa: BLE001 — audit must not crash the loop
                            logger.warning("audit.record_event failed for self_loop_prevented", exc_info=False)

                        # Feed the rejection back to the LLM so it can correct on retry.
                        session.messages.append(
                            Message(
                                role="tool",
                                tool_call_id=tc.id,
                                name=tc.name,
                                content=f"status: rejected | refusal: self_loop_prevented | detail: {self_loop_detail}",
                            )
                        )
                        yield PlannerEvent(
                            event="tool_call_rejected",
                            payload={
                                "id": tc.id,
                                "name": tc.name,
                                "reason": "self_loop_prevented",
                                "detail": self_loop_detail,
                                "op_kind": op_kind,
                                "rationale": rationale,
                                "arg_summary": arg_summary,
                                "audit_id": audit_id_for_event,
                            },
                        )
                        continue

            # W62 — count prior staged adds anchored at the same upstream so
            # fan-outs stack vertically rather than overlap. ``add_*`` calls
            # only; non-add ops aren't laid out. Also build a position map for
            # in-batch staged-but-unapplied upstreams so the executor's
            # resolver can anchor chained adds (filter → sort) onto the
            # prior staged add — which by definition isn't in ``flow.nodes``
            # yet.
            if tc.name.startswith(_ADD_PREFIX):
                staged_offset_index = _count_prior_staged_with_same_upstream(
                    session, insertion_context.upstream_node_ids
                )
                extra_upstream_positions: dict[int, tuple[float, float]] | None = (
                    _collect_staged_upstream_positions(session) or None
                )
            else:
                staged_offset_index = 0
                extra_upstream_positions = None
            # W71 v1.11 — staged-but-not-yet-applied upstream schemas.
            # Threaded through every dispatch (add_* AND
            # update_node_settings) so the predictor's Tier 0a sees them
            # and stops emitting *"upstream not found"* warnings for
            # nodes the agent staged earlier in the same session.
            extra_upstream_schemas: dict[int, Any] | None = (
                _collect_staged_upstream_schemas(session) or None
            )

            # Dispatch — execute_tool_call is meant to never raise (returns rejected
            # result instead) but we wrap defensively.
            try:
                result = execute_tool_call(
                    flow_id=session.flow_id,
                    tool_name=tc.name,
                    tool_args=tool_args,
                    insertion_context=insertion_context,
                    session_id=session.session_id,
                    user_id=session.user_id,
                    mode="stage",
                    flow=flow,
                    dry_run_cache=dry_run_cache,
                    llm_provided_node_id=llm_provided_node_id,
                    audit_meta=audit_meta,
                    staged_offset_index=staged_offset_index,
                    extra_upstream_positions=extra_upstream_positions,
                    extra_upstream_schemas=extra_upstream_schemas,
                )
            except Exception as exc:  # noqa: BLE001 — defence in depth
                logger.exception("tool dispatch raised; treating as rejected")
                tool_msg = Message(
                    role="tool",
                    tool_call_id=tc.id,
                    name=tc.name,
                    content=f"dispatch raised: {exc}",
                )
                session.messages.append(tool_msg)
                yield PlannerEvent(
                    event="tool_call_rejected",
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "reason": "exception",
                        "detail": str(exc),
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "arg_summary": arg_summary,
                    },
                )
                continue

            # Feed the result back into the conversation so the LLM can correct on retry
            tool_msg = Message(
                role="tool",
                tool_call_id=tc.id,
                name=tc.name,
                content=_summarise_result_for_llm(result),
            )
            session.messages.append(tool_msg)

            if result.status == "rejected":
                yield PlannerEvent(
                    event="tool_call_rejected",
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "reason": result.refusal_reason or "rejected",
                        "detail": result.refusal_detail or "",
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "arg_summary": arg_summary,
                    },
                )
                continue

            # W71 — agent_staged stage transitions on meta tool success.
            # Each meta tool sets a piece of session state and advances the
            # stage; the next loop iteration re-renders the system prompt
            # and exposes the next stage's tool. ``stage_advanced`` is
            # surfaced to the frontend so the agent panel can render the
            # current step ("Step 2/4: picking upstream").
            if (
                session.surface == "agent_staged"
                and tc.name == _CLASSIFY_INTENT_NAME
                and isinstance(result.extra, dict)
            ):
                op_kind = result.extra.get("op_kind")
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                if isinstance(op_kind, str):
                    session.picked_op_kind = op_kind  # type: ignore[assignment]
                    if op_kind == "add":
                        session.stage = "pick_type"
                    elif op_kind in ("modify", "delete", "connect", "disconnect"):
                        session.stage = "single_stage_op"
                    # ``other`` leaves stage at ``classify`` — the loop will
                    # call the LLM again, which sees the rationale already
                    # in history and will most likely emit no tool call,
                    # ending the loop with the rationale as the final
                    # assistant message (W49 question detection still
                    # routes to ``awaiting_user_input`` if it ends in a
                    # question).
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                    op_kind=op_kind if isinstance(op_kind, str) else None,
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "meta",
                    },
                )
                any_succeeded_this_round = True
                continue

            if (
                session.surface == "agent_staged"
                and tc.name == _PICK_NODE_TYPE_NAME
                and isinstance(result.extra, dict)
            ):
                node_type = result.extra.get("node_type")
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                if isinstance(node_type, str):
                    session.picked_node_type = node_type
                    session.stage = "pick_upstream"
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                    node_type=node_type if isinstance(node_type, str) else None,
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "picked_node_type": node_type,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "meta",
                    },
                )
                any_succeeded_this_round = True
                continue

            if (
                session.surface == "agent_staged"
                and tc.name == _PICK_UPSTREAM_NAME
                and isinstance(result.extra, dict)
            ):
                upstream_ids_raw = result.extra.get("upstream_node_ids")
                right_input_raw = result.extra.get("right_input_node_id")
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                if isinstance(upstream_ids_raw, list):
                    session.picked_upstream_ids = [
                        u for u in upstream_ids_raw if isinstance(u, int)
                    ]
                    session.picked_right_input_id = (
                        right_input_raw if isinstance(right_input_raw, int) else None
                    )
                    session.stage = "fill_settings"
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                    upstream_node_ids=list(session.picked_upstream_ids),
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "picked_upstream_ids": session.picked_upstream_ids,
                        "right_input_node_id": session.picked_right_input_id,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "meta",
                    },
                )
                any_succeeded_this_round = True
                continue

            # Real staging path
            if result.status in ("staged", "warned", "applied"):
                if result.staged_node_payload is not None:
                    session.staged_results.append(
                        diff_module.StagedToolEntry(
                            tool_name=tc.name,
                            audit_id=result.audit_id,
                            staged_node_payload=result.staged_node_payload,
                        )
                    )
                    # Track ids the agent has staged so subsequent drift
                    # checks can exclude them from external-added detection
                    # (W45 Q1). Only ``add_<node_type>`` calls produce a
                    # node_id — connection / delete payloads don't.
                    if tc.name.startswith(_ADD_PREFIX):
                        staged_id = _payload_node_id(result.staged_node_payload)
                        if staged_id is not None and staged_id not in session.staged_node_ids:
                            session.staged_node_ids.append(staged_id)
                event_name: PlannerEventName = "tool_call_warned" if result.status == "warned" else "tool_call_staged"
                # W42 — re-persist the session after each staging so a
                # process crash mid-loop can resume against the up-to-date
                # ``staged_results`` / ``staged_node_ids``. Best-effort: a
                # checkpoint failure must not stall the planner.
                try:
                    session.touch()
                    sessions.register_session(session)
                except Exception:
                    logger.exception(
                        "planner: session checkpoint after tool_call_staged failed (session=%s)",
                        session.session_id,
                    )
                yield PlannerEvent(
                    event=event_name,
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "node_id": _payload_node_id(result.staged_node_payload),
                        "predicted_output_schema": result.predicted_output_schema,
                        "warnings": list(result.warnings),
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "arg_summary": arg_summary,
                    },
                )
                # W71 — on agent_staged, reset the state machine after each
                # successful add_* (stage 3) or single-stage non-add op so
                # the next round starts a fresh classify→pick→fill cycle.
                # Multi-node turns serialize naturally as N×4 rounds without
                # any history pruning.
                if session.surface == "agent_staged" and (
                    tc.name.startswith(_ADD_PREFIX)
                    or tc.name in _STAGED_SINGLE_OP_TOOL_NAMES
                ):
                    prev_stage = session.stage
                    sessions.reset_stage_state(session)
                    _log_stage_transition(
                        session,
                        from_stage=prev_stage,
                        to_stage=session.stage,
                        tool_name=tc.name,
                        completed_op=tc.name,
                    )
                    yield PlannerEvent(
                        event="stage_advanced",
                        payload={
                            "from": prev_stage,
                            "to": session.stage,
                            "session_id": session.session_id,
                            "completed_op": tc.name,
                            "op_kind_meta": "meta",
                        },
                    )
                any_succeeded_this_round = True

        # End of per-tool-call loop.
        session.step_count += 1
        session.touch()

        if any_succeeded_this_round:
            retries_for_step = 0
        else:
            retries_for_step += 1
            if retries_for_step >= max_retries_per_step:
                session.status = "failed"
                session.touch()
                yield PlannerEvent(
                    event="error",
                    payload={
                        "message": (
                            f"all {max_retries_per_step} consecutive attempts at step "
                            f"{session.step_count} were rejected"
                        ),
                    },
                )
                return
            yield PlannerEvent(
                event="retry",
                payload={"attempt": retries_for_step, "max": max_retries_per_step},
            )
        # Loop continues — accumulated tool messages are in session.messages.

    # --- Loop ended (no more tool calls) ---
    if not session.staged_results:
        # W49 — if the agent's last assistant message reads like a clarifying
        # question, this is *not* "agent finished — nothing to stage"; the
        # agent is waiting on the user. Flip to ``awaiting_user_input`` and
        # emit a distinct SSE event so the frontend renders the right state.
        # Both ``awaiting_user_input`` and ``completed`` are resumable via
        # the W49 ``/followup`` endpoint, so the only difference is UX
        # framing and the SSE event name.
        if _looks_like_question(session.last_assistant_text):
            session.status = "awaiting_user_input"
            session.touch()
            yield PlannerEvent(
                event="awaiting_user_input",
                payload={
                    "session_id": session.session_id,
                    "question": session.last_assistant_text,
                },
            )
            return

        session.status = "completed"
        session.touch()
        yield PlannerEvent(
            event="complete",
            payload={
                "session_id": session.session_id,
                "diff_id": None,
                "op_count": 0,
                "rationale": session.last_assistant_text,
                "diff_payload": None,
            },
        )
        return

    try:
        graph_diff = diff_module.bundle_staged_results(session.staged_results)
    except ValueError as exc:
        session.status = "failed"
        session.touch()
        yield PlannerEvent(event="error", payload={"message": f"bundle failed: {exc}"})
        return

    rationale = (session.last_assistant_text or "")[:RATIONALE_MAX_LEN] or None
    graph_diff = graph_diff.model_copy(
        update={
            "session_id": session.session_id,
            "flow_id": session.flow_id,
            "rationale": rationale,
        }
    )
    diff_id = diff_module.register_diff(graph_diff)
    session.diff_id = diff_id
    session.rationale = rationale
    session.status = "completed"
    session.touch()
    yield PlannerEvent(
        event="complete",
        payload={
            "session_id": session.session_id,
            "diff_id": diff_id,
            "op_count": _op_count(graph_diff),
            "rationale": rationale,
            "diff_payload": graph_diff.model_dump(mode="json"),
        },
    )


__all__ = [
    "DEFAULT_MAX_RETRIES_PER_STEP",
    "DEFAULT_MAX_STEPS",
    "DEFAULT_MAX_TOKENS",
    "FollowupAction",
    "OpKind",
    "PlannerEvent",
    "PlannerEventName",
    "RATIONALE_MAX_LEN",
    "inject_followup_message",
    "run_planner_session",
]
