"""Meta-op tool surface — owned by W30.

The two-stage agent flow (D002) routes the model's intent through a single
``flowfile.meta.pick_category`` call before exposing the per-category
catalog. W30 declares the spec; the model invocation lives in W40's planner.

The accompanying heuristic fallback in ``registry.pick_category`` lets W31's
executor exercise the two-stage path in tests and degrade gracefully when no
provider is configured.

W71 adds three more meta tools to power the multi-stage ``agent_staged``
state machine. Each stage exposes exactly one tool to the function-calling
API so smaller models comply rather than emitting text-JSON:

* ``classify_intent`` — stage 0; LLM picks an op kind.
* ``pick_node_type`` — stage 1 (add path); LLM picks the node type.
* ``pick_upstream`` — stage 2 (add path); LLM picks upstream node ids
  from a per-turn enum union of live + session-staged ids.

Stage 3 (``fill_settings``) does NOT live in this module — it's a
per-turn variant of the existing ``flowfile.graph.add_<type>`` tool with
planner-injected fields stripped, built by ``registry.build_staged_fill_tool_spec``.

MCP-shaped names per D004: ``flowfile.meta.<op>``.
"""

from __future__ import annotations

from typing import Final

from flowfile_core.ai.providers.base import ToolSpec
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"

CATEGORY_NAMES: Final[tuple[str, ...]] = (
    "transformations",
    "joins",
    "aggregations",
    "io",
    "code",
    "ml",
    "meta",
    "graph",
)


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


CLASSIFY_INTENT_TOOL_NAME: Final[str] = "flowfile.meta.classify_intent"
PICK_NODE_TYPE_TOOL_NAME: Final[str] = "flowfile.meta.pick_node_type"
PICK_UPSTREAM_TOOL_NAME: Final[str] = "flowfile.meta.pick_upstream"


META_OPS_TOOLS: Final[list[ToolSpec]] = [
    ToolSpec(
        name="flowfile.meta.pick_category",
        description=(
            "First-stage categoriser for the two-stage agent flow (D002). "
            "Given the user's intent, return the single category whose tool surface "
            "best fits the next step. The next call will be issued with only that "
            "category's tools available — pick conservatively. Use 'meta' when you "
            "need more information from the user before acting."
        ),
        long_description=(
            "Internal routing call for the two-stage agent (D002). The host expands "
            "the chosen category into a narrowed tool surface for your next turn. "
            "Categories: 'transformations' (filter/select/sort/formula/etc.), "
            "'joins' (join/cross_join/fuzzy_match/union), 'aggregations' "
            "(group_by/pivot/unpivot/record_count), 'io' (read/output/database/cloud), "
            "'code' (polars_code/python_script/sql_query), 'ml' "
            "(train/apply/evaluate_model), 'meta' (clarify with the user; nothing "
            "is staged), 'graph' (delete/connect — graph mutation outside add_*). "
            "Pick the category that fits the *immediate next step*; you can pick "
            "again on the following turn if the work spans categories. Don't "
            "narrate this call to the user — it's a routing primitive."
        ),
        parameters={
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "intent": {
                    "type": "string",
                    "description": "Short summary of what the user is trying to accomplish next.",
                },
                "rationale": {
                    "type": "string",
                    "description": (
                        "One sentence explaining why this category is the right fit. "
                        "Surfaced to the user when the agent shows its reasoning."
                    ),
                },
                "category": {
                    "type": "string",
                    "enum": list(CATEGORY_NAMES),
                    "description": "The chosen category. Must be one of the enumerated values.",
                },
            },
            "required": ["intent", "category", "rationale"],
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
            "tool catalog, pick the single node_type to add."
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
                    "enum": sorted(NODE_TYPE_TO_SETTINGS_CLASS.keys()),
                    "description": (
                        "The chosen node type. Must be one of Flowfile's registered "
                        "types listed in the catalog above."
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
) -> ToolSpec:
    """W71 — build the stage-2 upstream picker with a fresh enum.

    The enum is the union of currently-live node ids and any nodes the
    agent has already staged this session (multi-node turns chain
    *filter → sort* by attaching the second add to the first's
    not-yet-applied node id). Build per-turn — the static spec would go
    stale between LLM rounds.

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
    return ToolSpec(
        name=PICK_UPSTREAM_TOOL_NAME,
        description=(
            "W71 stage-2 upstream picker for the agent_staged surface (add path). "
            "Pick the node id(s) from the live graph that the new node should "
            "attach to. Use right_input_node_id for join-shaped node types."
        ),
        long_description=(
            "Stage 2 of the agent_staged state machine, reached after "
            "pick_node_type. Pick the upstream node id(s) from the subgraph "
            "in your system prompt — these are the nodes whose schema the new "
            "node will read from.\n\n"
            "Most node types take one upstream (filter, sort, group_by, etc.). "
            "Joins take two: the left input goes in upstream_node_ids[0]; the "
            "right input goes in right_input_node_id. Union takes multiple "
            "upstreams in upstream_node_ids. Source-only node types (read, "
            "manual_input, database_reader, etc.) get an empty upstream list — "
            "the host accepts that.\n\n"
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
                        "Primary input upstream node ids. Empty array for "
                        "source-only node types. The integers must be drawn "
                        "from the enum above (live or agent-staged ids)."
                    ),
                },
                "right_input_node_id": {
                    "type": ["integer", "null"],
                    "enum": [*enum_values, None],
                    "description": (
                        "Right input for join-shaped node types (join, "
                        "cross_join, fuzzy_match). Null for everything else."
                    ),
                },
                "rationale": {
                    "type": "string",
                    "description": (
                        "One short sentence explaining why these upstreams are "
                        "the right input for the new node."
                    ),
                },
            },
            "required": ["upstream_node_ids", "rationale"],
        },
    )


__all__ = [
    "CATEGORY_NAMES",
    "CLASSIFY_INTENT_TOOL_NAME",
    "META_OPS_TOOLS",
    "OP_KIND_NAMES",
    "PICK_NODE_TYPE_TOOL_NAME",
    "PICK_UPSTREAM_TOOL_NAME",
    "build_pick_upstream_spec",
]
