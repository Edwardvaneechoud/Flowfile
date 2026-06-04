"""Constants, enums, event model, and small helpers used across the planner package.

Kept in a single internal module (no other package imports) so every other
file in the package can ``from ._internal import ...`` without risk of
cycles. Moving any of these into a higher-level module would force the
leaf modules to import the loop, which is the wrong direction.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger("flowfile_core.ai.agents.planner")


DEFAULT_MAX_STEPS: int = 32
DEFAULT_MAX_RETRIES_PER_STEP: int = 3
DEFAULT_MAX_TOKENS: int = 2_048
RATIONALE_MAX_LEN: int = 500
DEFAULT_MAX_DB_READER_OPS: int = 8


PlannerEventName = Literal[
    "thinking",
    "tool_call_proposed",
    "tool_call_staged",
    "tool_call_warned",
    "tool_call_rejected",
    "tool_call_applied",
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


_ADD_PREFIX = "flowfile.graph.add_"

# Surfaces that share the staged state machine
# (classify → pick_type → pick_upstream → fill_settings). ``agent_live``
# differs only in post-apply behaviour (lives in flow.nodes immediately +
# observation), so every gate on ``agent_staged`` must also include
# ``agent_live``.
_STAGED_STATE_MACHINE_SURFACES: frozenset[str] = frozenset({"agent_staged", "agent_live"})

# Stages where an empty ``tool_calls`` — even after textual-tool-call recovery
# — should route through the retry path rather than terminating the loop.
# At ``classify`` an empty ``tool_calls`` is a valid termination signal:
# the LLM decided op_kind="other" or the user got their answer.
_MANDATORY_TOOL_CALL_STAGES: frozenset[str] = frozenset(
    {"pick_type", "pick_upstream", "fill_settings", "single_stage_op", "verify_completion"}
)

# Non-add ops that the staged surface dispatches via ``single_stage_op``.
# The planner uses this set to decide when to call ``reset_stage_state``
# after a successful single-stage op so the next round restarts at
# ``classify``.
_STAGED_SINGLE_OP_TOOL_NAMES: frozenset[str] = frozenset(
    {
        "flowfile.graph.update_node_settings",
        "flowfile.graph.delete_node",
        "flowfile.graph.connect",
        "flowfile.graph.delete_connection",
    }
)

# Settings-field names that express primary upstream dependency for
# single-input nodes. The catalog generator (``tools/registry.py``)
# auto-derives JSON Schema from the per-node Pydantic settings classes,
# so the LLM sees both ``upstream_node_ids`` (planner-injected) AND
# these legacy connection-state fields. When the LLM emits the settings
# field but omits ``upstream_node_ids``, the resolver canonicalises the
# two views.
#
# Only ``depending_on_id`` (NodeSingleInput base) is canonical primary
# upstream. ``depending_on_ids`` (NodeMultiInput) is excluded —
# multi-upstream insertion context is out of v0 scope. Other
# upstream-shaped fields (``ApplyModelSettings.upstream_node_id``,
# ``EvaluateModelSettings.upstream_train_node_id``) are nested in ML
# sub-settings and reference auxiliary cross-flow links, not the
# primary data input — also excluded.
_SETTINGS_DEPENDENCY_FIELDS: tuple[str, ...] = ("depending_on_id",)


_AGENT_STAGED_OP_TO_SURFACE: dict[str, str] = {
    "modify": "staged_modify",
    "delete": "staged_delete",
    "connect": "staged_connect",
    "disconnect": "staged_disconnect",
}


OpKind = Literal["meta", "graph", "schema", "codegen", "unknown"]


def _classify_op_kind(tool_name: str) -> OpKind:
    """Map a fully-qualified tool name to its op_kind for UI gating.

    ``flowfile.meta.*`` are LLM-internal routing decisions and the
    frontend hides them. ``flowfile.graph.*`` are the user-facing canvas
    mutations that need a rationale. ``flowfile.schema.*`` are read-only
    introspection calls — we still show them so the user knows the agent
    is "looking at" something. ``flowfile.codegen.*`` are code generation
    helpers that only show up under ``agent_complex``.
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
    insertion_context,
) -> str | None:
    """Return a refusal_detail string if ``proposed_node_id`` would self-loop, else None.

    Universal invariant: a new node's ``node_id`` may never equal any of
    its own ``upstream_node_ids`` or its ``right_input_node_id``. Catches
    all three plausible upstream causes (LLM-provided collision, stale
    staged_results post-resume, live-graph drift) regardless of which
    one fired — the apply_diff cycle error is the same.
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


def _op_count(graph_diff) -> int:
    return (
        len(graph_diff.additions)
        + len(graph_diff.connections_added)
        + len(graph_diff.deletions)
        + len(graph_diff.connections_removed)
    )
