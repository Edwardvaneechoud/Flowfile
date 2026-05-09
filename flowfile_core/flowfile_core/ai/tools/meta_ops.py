"""Meta-op tool surface — owned by W30.

W71 ``agent_staged`` exposes three meta tools — one per state-machine
stage — each producing exactly one tool call so small open-weights
models comply with the function-calling API rather than emitting
text-JSON in content:

* ``classify_intent`` — stage 0; LLM picks an op kind.
* ``pick_node_type`` — stage 1 (add path); LLM picks the node type.
* ``pick_upstream`` — stage 2 (add path); LLM picks upstream node ids
  from a per-turn enum union of live + session-staged ids.

Stage 3 (``fill_settings``) does NOT live in this module — it's a
per-turn variant of the existing ``flowfile.graph.add_<type>`` tool with
planner-injected fields stripped, built by ``registry.build_staged_fill_tool_spec``.

W71 v1.10 — the legacy ``flowfile.meta.pick_category`` (and its
``CATEGORY_NAMES`` enum) was removed alongside the two-stage
``surface=agent`` flow it powered. Small open-weights models silently
fell back to text-JSON-in-content on it; ``classify_intent`` /
``pick_node_type`` are the staged replacements.

MCP-shaped names per D004: ``flowfile.meta.<op>``.
"""

from __future__ import annotations

from typing import Final

from flowfile_core.ai.providers.base import ToolSpec
from flowfile_core.ai.safety import AGENT_BLOCKED_NODE_TYPES
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"


# W71 — op kinds chosen by the stage-0 ``classify_intent`` tool. Mirrors
# the values in ``flowfile_core.ai.sessions.PlannerOpKind`` (kept here as a
# tuple so it can be inlined into the JSON Schema enum without a pydantic
# round-trip).
OP_KIND_NAMES: Final[tuple[str, ...]] = (
    "add",
    "modify",
    "delete",
    "connect",
    "disconnect",
    "other",
)


EMIT_PLAN_TOOL_NAME: Final[str] = "flowfile.meta.emit_plan"
CLASSIFY_INTENT_TOOL_NAME: Final[str] = "flowfile.meta.classify_intent"
PICK_NODE_TYPE_TOOL_NAME: Final[str] = "flowfile.meta.pick_node_type"
PICK_UPSTREAM_TOOL_NAME: Final[str] = "flowfile.meta.pick_upstream"


# W71 v1.14B — node types that take TWO upstream inputs (a primary
# ``upstream_node_ids`` list AND a separate ``right_input_node_id``).
# Pick_upstream upgrades ``right_input_node_id`` to *required* when the
# picked type is in this set so cross-join / join / fuzzy-match never
# stage with only one wire connected (2026-05-08 dogfood: agent
# attached cross_join to node 2 alone, missing input-1).
JOIN_SHAPED_NODE_TYPES: Final[frozenset[str]] = frozenset(
    {"join", "cross_join", "fuzzy_match"}
)


def _pick_node_type_disambiguation_text() -> str:
    """W71 v1.14A.1 — render the do/don't list of palette-label vs
    node_type confusions for inlining into the ``pick_node_type``
    tool's description AND its ``node_type`` parameter description.

    The same data v1.12B already surfaces in the catalog
    disambiguation block, but at the *spec* level: the
    function-calling decoder attends to tool spec text far more
    than to system-prompt prose buried 30k tokens deep. This catches
    the bypass case (LLM emits ``pick_node_type`` directly at the
    classify stage, where the catalog disambiguation isn't
    rendered).

    Caches at module load so repeated session starts are cheap.
    Returns ``""`` when the helpers can't be loaded (defensive — the
    spec still works, just without the disambiguation note).
    """
    try:
        from flowfile_core.ai.tools.node_docs import palette_label_for
    except Exception:
        return ""

    pairs: list[tuple[str, str]] = []
    for nt in sorted(NODE_TYPE_TO_SETTINGS_CLASS.keys()):
        label = palette_label_for(nt)
        if not label or label == nt:
            continue
        snake = label.lower().replace(" ", "_")
        if snake != nt and snake.replace("-", "_") != nt:
            pairs.append((nt, label))
    if not pairs:
        return ""

    lines = [
        "Common confusions (these are the palette LABEL snake-cased — NOT valid enum values):",
    ]
    for nt, label in pairs:
        bad = label.lower().replace(" ", "_")
        lines.append(f"  - use `{nt}` (NOT `{bad}`, palette label \"{label}\")")
    return "\n".join(lines)


_DISAMBIGUATION_TEXT: Final[str] = _pick_node_type_disambiguation_text()


META_OPS_TOOLS: Final[list[ToolSpec]] = [
    ToolSpec(
        name=EMIT_PLAN_TOOL_NAME,
        description=(
            "W71 v2.4 stage-0' plan emitter for agent_staged / agent_live. "
            "Before any classify→pick_type→pick_upstream→fill_settings cycle, "
            "the LLM articulates a brief multi-step plan for the user's "
            "request. The plan is shown in the chat trail; then the agent "
            "starts executing the plan one node at a time. Fires ONCE at "
            "session start; multi-node turns don't re-plan after each add."
        ),
        long_description=(
            "Stage-0' (pre-classify) of the agent_staged / agent_live state "
            "machine. Read the user's goal, the live subgraph, and the "
            "conversation history above. Then call this tool with a SHORT "
            "markdown plan (numbered list, ≤6 steps) that names the node "
            "types you intend to add and what each one does. After this "
            "round, the host advances to stage 1 (classify) and you start "
            "executing the plan one step at a time.\n\n"
            "Plan format guidance:\n"
            "- Numbered list of concrete steps, each naming a Flowfile "
            "  node_type (e.g. ``group_by``, ``cross_join``, ``formula``).\n"
            "- 1 sentence per step describing what it does.\n"
            "- Don't include settings details — those land at fill_settings.\n"
            "- Don't propose writer nodes (output / database_writer / "
            "  cloud_storage_writer / catalog_writer); you can suggest the "
            "  user adds them but the agent isn't authorized to stage them.\n\n"
            "If the user's request is a question or doesn't require canvas "
            "changes, emit a one-line plan that says so (e.g. *\"User "
            "asked a question; will respond without staging changes.\"*) — "
            "the next stage will classify ``op_kind=other`` and the run "
            "ends with the assistant's reply."
        ),
        parameters={
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "plan": {
                    "type": "string",
                    "description": (
                        "Multi-step plan as markdown. Numbered list, ≤6 "
                        "steps; each step names a node_type and a one-"
                        "sentence description. Surfaced to the user in "
                        "the chat trail before the agent starts executing."
                    ),
                },
                "rationale": {
                    "type": "string",
                    "description": (
                        "One short sentence summarizing the overall "
                        "approach (e.g. *\"Group by city, broadcast the "
                        "global total via cross_join, compute percentage "
                        "with formula\"*). Surfaced as the run-start "
                        "headline."
                    ),
                },
            },
            "required": ["plan", "rationale"],
        },
    ),
    ToolSpec(
        name=CLASSIFY_INTENT_TOOL_NAME,
        description=(
            "W71 stage-0 intent classifier for the agent_staged surface. "
            "Given the user's request and the conversation history, pick the single "
            "op_kind that best describes what the user wants to do next. The host "
            "uses your choice to route to the next stage."
        ),
        long_description=(
            "Stage 0 of the agent_staged state machine. Pick exactly one op_kind:\n"
            "* 'add' — the user wants to add a new node (filter, group_by, join, "
            "etc.). Advances to stage 1 (pick_node_type) so you choose the type, "
            "then upstream(s), then settings.\n"
            "* 'modify' — the user wants to change settings on an existing node "
            "(e.g. *'show top 5 rows in node 9'*, *'change the join key to "
            "customer_id'*). Advances to a single-stage update_node_settings call.\n"
            "* 'delete' — the user wants to remove a node from the canvas.\n"
            "* 'connect' — the user wants to wire two existing nodes together.\n"
            "* 'disconnect' — the user wants to remove a connection between two "
            "existing nodes.\n"
            "* 'other' — the user is asking a question, requesting an explanation, "
            "or the request doesn't fit any of the above. Terminates the loop; "
            "use the rationale to write your answer to the user.\n\n"
            "Pick conservatively. If multiple intents fit (*'filter then group by'*) "
            "pick the FIRST one — the loop will return to stage 0 after each node "
            "is staged, so you can classify the next intent on the next round."
        ),
        parameters={
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "op_kind": {
                    "type": "string",
                    "enum": list(OP_KIND_NAMES),
                    "description": "The chosen op kind. Must be one of the enumerated values.",
                },
                "rationale": {
                    "type": "string",
                    "description": (
                        "One short sentence explaining the choice. Surfaced to "
                        "the user as the agent's reasoning. For 'other', this is "
                        "your answer to the user — write it as a complete reply."
                    ),
                },
            },
            "required": ["op_kind", "rationale"],
        },
    ),
    # W71 — static placeholder for ``pick_upstream``. The planner builds a
    # per-turn dynamic spec via :func:`build_pick_upstream_spec` with the
    # live + agent-staged enum populated; this static entry exists so
    # ``SURFACE_PRESETS["staged_pick_upstream"]`` resolves to a non-empty
    # tool list when callers go through ``build_tool_catalog`` (defensive
    # for tests / introspection — never reached on the planner hot path).
    ToolSpec(
        name=PICK_UPSTREAM_TOOL_NAME,
        description=(
            "W71 stage-2 upstream picker for the agent_staged surface (add path). "
            "Pick the node id(s) the new node should attach to."
        ),
        long_description=(
            "Stage 2 placeholder: the planner overrides this spec at provider-call "
            "time with a fresh enum drawn from live + agent-staged ids."
        ),
        parameters={
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "upstream_node_ids": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Primary input upstream node ids.",
                },
                "right_input_node_id": {
                    "type": ["integer", "null"],
                    "description": "Right input for join-shaped node types.",
                },
                "rationale": {
                    "type": "string",
                    "description": "One-sentence justification.",
                },
            },
            "required": ["upstream_node_ids", "rationale"],
        },
    ),
    ToolSpec(
        name=PICK_NODE_TYPE_TOOL_NAME,
        description=(
            "W71 stage-1 node-type picker for the agent_staged surface (add path). "
            "Given the user's request and the per-node guidance in the system prompt's "
            "tool catalog, pick the single node_type to add. "
            "IMPORTANT: ``node_type`` is the registered snake_case identifier (e.g. "
            "``sort``, ``select``, ``unique``) — DO NOT snake-case the palette label "
            "(e.g. ``sort_data``, ``select_data``, ``drop_duplicates`` will all be "
            "rejected). See the enum and the disambiguation list on the node_type "
            "parameter."
        ),
        long_description=(
            "Stage 1 of the agent_staged state machine, reached after classify_intent "
            "returned op_kind='add'. Pick exactly one node_type from the catalog "
            "in your system prompt — match the user's intent to the closest "
            "*when to use* description there. The next stage exposes a typed "
            "upstream picker; the stage after that exposes the chosen node type's "
            "settings shape, so don't worry about settings here, just the type."
        ),
        parameters={
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "node_type": {
                    "type": "string",
                    # W71 v2.1 — drop writer-shaped node types (output,
                    # database_writer, cloud_storage_writer,
                    # catalog_writer) so the AI agent surfaces can
                    # never stage one. Writers go to external
                    # destinations (files, DBs, cloud) and the user
                    # always adds them manually for safety.
                    "enum": sorted(
                        nt
                        for nt in NODE_TYPE_TO_SETTINGS_CLASS.keys()
                        if nt not in AGENT_BLOCKED_NODE_TYPES
                    ),
                    "description": (
                        "The chosen node type. Must be one of Flowfile's registered "
                        "types — exactly as it appears in the enum (snake_case "
                        "node_type identifier, NOT the snake-cased palette label). "
                        "Writer-shaped node types (output, database_writer, "
                        "cloud_storage_writer, catalog_writer) are intentionally "
                        "absent from the enum — those write to external destinations "
                        "and must be added manually by the user.\n\n"
                        + _DISAMBIGUATION_TEXT
                    ),
                },
                "rationale": {
                    "type": "string",
                    "description": (
                        "One short sentence explaining why this node type fits "
                        "the user's request. Surfaced to the user."
                    ),
                },
            },
            "required": ["node_type", "rationale"],
        },
    ),
]


def build_pick_upstream_spec(
    live_node_ids: list[int],
    staged_node_ids: list[int] | None = None,
    *,
    picked_node_type: str | None = None,
) -> ToolSpec:
    """W71 — build the stage-2 upstream picker with a fresh enum.

    The enum is the union of currently-live node ids and any nodes the
    agent has already staged this session (multi-node turns chain
    *filter → sort* by attaching the second add to the first's
    not-yet-applied node id). Build per-turn — the static spec would go
    stale between LLM rounds.

    W71 v1.14B — when ``picked_node_type`` is in
    :data:`JOIN_SHAPED_NODE_TYPES` the spec marks
    ``right_input_node_id`` as required (drops the ``null`` option,
    adds the field to ``required[]``). Otherwise — including when
    ``picked_node_type`` is unknown — the field stays optional with a
    nullable type. Catches the cross_join / join / fuzzy_match
    failure mode where the LLM stages with only ``upstream_node_ids``
    populated and leaves the right input dangling.

    Returns an empty-enum spec when no upstreams exist (truly cold flow);
    callers handle this case as a refusal — the only valid downstream
    action on a cold flow is adding a source node, which doesn't need an
    upstream.
    """
    seen: set[int] = set()
    ordered: list[int] = []
    for nid in live_node_ids:
        if isinstance(nid, int) and nid not in seen:
            seen.add(nid)
            ordered.append(nid)
    for nid in staged_node_ids or ():
        if isinstance(nid, int) and nid not in seen:
            seen.add(nid)
            ordered.append(nid)

    enum_values: list[int] = sorted(ordered)
    is_join_shaped = (
        picked_node_type is not None and picked_node_type in JOIN_SHAPED_NODE_TYPES
    )
    rationale_field: dict = {
        "type": "string",
        "description": (
            "One short sentence explaining why these upstreams are "
            "the right input for the new node."
        ),
    }

    if is_join_shaped:
        # W71 v1.15A — for join-shaped node types the spec uses TWO
        # SCALAR fields (``left_input_node_id`` + ``right_input_node_id``),
        # both required, mirroring the UI's L/R port labels and the
        # user mental model. The asymmetric ``upstream_node_ids`` (list)
        # + ``right_input_node_id`` (scalar) shape that pre-v1.15
        # spec exposed forced the LLM to learn two unrelated field
        # shapes for what it intuitively models as two equivalent
        # inputs. Symmetric scalar pair removes that asymmetry.
        # Translation back to the legacy ``upstream_node_ids`` /
        # ``right_input_node_id`` representation happens in
        # ``executor._handle_meta`` so downstream consumers (planner
        # session state, _handle_add_node) are unchanged.
        return ToolSpec(
            name=PICK_UPSTREAM_TOOL_NAME,
            description=(
                f"W71 stage-2 upstream picker for ``{picked_node_type}`` "
                "(join-shaped: two distinct inputs — LEFT and RIGHT). "
                "Both inputs are REQUIRED. The picker exposes the union "
                "of live + agent-staged node ids as the enum."
            ),
            long_description=(
                f"Stage 2 of the agent_staged state machine for the "
                f"join-shaped node type ``{picked_node_type}``. Pick "
                "TWO distinct upstream node ids — one for the LEFT "
                "input, one for the RIGHT input.\n\n"
                "**Convention**: the LEFT side's columns appear FIRST "
                "in the output schema, then the RIGHT side's columns. "
                f"For ``join`` / ``fuzzy_match`` (asymmetric), the LEFT "
                "is the *preserved* / *driving* table for left-join "
                "semantics. For ``cross_join`` (commutative — A×B has "
                "the same rows as B×A), the choice is arbitrary; "
                "pick whichever side you want first in the output.\n\n"
                "Picking the same id for both LEFT and RIGHT is "
                "invalid (a node cannot join to itself); pick two "
                "different ids."
            ),
            parameters={
                "$schema": JSON_SCHEMA_DIALECT,
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "left_input_node_id": {
                        "type": "integer",
                        "enum": enum_values,
                        "description": (
                            "The LEFT input node id. Its columns appear "
                            "first in the output schema. For asymmetric "
                            "joins (join / fuzzy_match) this is the "
                            "preserved / driving side."
                        ),
                    },
                    "right_input_node_id": {
                        "type": "integer",
                        "enum": enum_values,
                        "description": (
                            "The RIGHT input node id. Its columns appear "
                            "second in the output schema. Must be a "
                            "DIFFERENT id from ``left_input_node_id``."
                        ),
                    },
                    "rationale": rationale_field,
                },
                "required": [
                    "left_input_node_id",
                    "right_input_node_id",
                    "rationale",
                ],
            },
        )

    return ToolSpec(
        name=PICK_UPSTREAM_TOOL_NAME,
        description=(
            "W71 stage-2 upstream picker for the agent_staged surface (add path). "
            "Pick the node id(s) from the live graph that the new node should "
            "attach to."
        ),
        long_description=(
            "Stage 2 of the agent_staged state machine, reached after "
            "pick_node_type. Pick the upstream node id(s) from the subgraph "
            "in your system prompt — these are the nodes whose schema the new "
            "node will read from.\n\n"
            "Most node types take one upstream (filter, sort, group_by, etc.) "
            "— pass a single-element list ``[node_id]``. Union takes multiple "
            "upstreams in ``upstream_node_ids``. Source-only node types (read, "
            "manual_input, database_reader, etc.) get an empty upstream list — "
            "the host accepts that. Join-shaped types (join / cross_join / "
            "fuzzy_match) use a different spec shape (LEFT + RIGHT scalars) "
            "exposed when those types are picked.\n\n"
            "The id values are constrained by the enum to live + agent-staged "
            "node ids, so you cannot pick an invalid id. Picking the right one "
            "is your job: match the schema columns shown for each node to what "
            "your new node needs as input."
        ),
        parameters={
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "upstream_node_ids": {
                    "type": "array",
                    "items": {
                        "type": "integer",
                        "enum": enum_values,
                    },
                    "description": (
                        "Upstream node ids. Empty array for source-only "
                        "node types; one-element list for the typical "
                        "single-input case; multiple for ``union``. The "
                        "integers must be drawn from the enum above (live "
                        "or agent-staged ids)."
                    ),
                },
                "right_input_node_id": {
                    "type": ["integer", "null"],
                    "enum": [*enum_values, None],
                    "description": (
                        "Legacy field — leave null for non-join types. "
                        "Join-shaped types now use a different spec "
                        "(left_input_node_id + right_input_node_id)."
                    ),
                },
                "rationale": rationale_field,
            },
            "required": ["upstream_node_ids", "rationale"],
        },
    )


__all__ = [
    "CLASSIFY_INTENT_TOOL_NAME",
    "EMIT_PLAN_TOOL_NAME",
    "JOIN_SHAPED_NODE_TYPES",
    "META_OPS_TOOLS",
    "OP_KIND_NAMES",
    "PICK_NODE_TYPE_TOOL_NAME",
    "PICK_UPSTREAM_TOOL_NAME",
    "build_pick_upstream_spec",
]
