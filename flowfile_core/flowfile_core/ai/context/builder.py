"""Subgraph + schema + sample serialiser.

Composes the per-call prompt context used by the AI surfaces:

* walks the :class:`flowfile_core.flowfile.flow_graph.FlowGraph` upstream
  from one or more "pinned" nodes;
* projects each visited node into a JSON-safe :class:`NodeSnapshot`;
* renders the snapshot as the user message;
* assembles the layered system prompt (``prompts/base.md`` concatenated
  with ``prompts/{level}.md``);
* hands the rendered context to :mod:`flowfile_core.ai.context.budget`
  for token-budget truncation when needed.

Sample rows are gated by ``samples_mode`` (default ``"off"``). PII
scrubbing is **not** performed here — that is the safety module's seam.

When ``resolve_schemas=True`` (the default) and an upstream node's
``predicted_schema`` is ``None``, the builder applies the tier-1
resolver for predictable upstreams (``static`` / ``source`` /
``passthrough`` per
:func:`flowfile_core.ai.tools.classification.is_predictable_via_mirror`)
by calling :meth:`FlowNode.get_predicted_schema(force=True)`, which
fires the registered ``schema_callback`` (or the
``_predicted_data_getter`` fallback). Dynamic node types
(``polars_code`` / ``python_script`` / ``sql_query`` / ``pivot`` /
etc.) stay ``schema_status="unknown"`` from this surface — kernel
dry-run is reserved for the executor path. Callers that need the
cache-only behaviour pass ``resolve_schemas=False``
(``settings_autocomplete`` keeps its own resolver and does not opt in
here).
"""

from __future__ import annotations

import functools
import logging
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field

from flowfile_core.ai.context.budget import BudgetReport, apply_budget, estimate_tokens
from flowfile_core.ai.context.mentions import (
    Mention,
    ResolvedMention,
    parse_mentions,
    resolve_mentions,
)
from flowfile_core.ai.providers.base import Message
from flowfile_core.ai.safety import FlowSafetyConfig, prepare_samples
from flowfile_core.ai.tools.classification import is_predictable_via_mirror

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import (
        FlowfileColumn,
    )
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode


SurfaceLiteral = Literal[
    "cmd_k",
    "ghost_node",
    "explain",
    "agent_complex",
    "agent_staged",
    "agent_live",
    "docgen",
    "settings_autocomplete",
    "lineage",
    "intent_classifier",
]

SamplesMode = Literal["off", "regex"]

PromptLevel = Literal["assist", "copilot", "planner"]

SURFACE_TO_LEVEL: dict[str, PromptLevel] = {
    "cmd_k": "copilot",
    "ghost_node": "copilot",
    "explain": "assist",
    "agent_complex": "planner",
    # ``agent_staged`` falls back to ``planner`` only when no stage is
    # supplied (defensive). When a stage IS supplied,
    # :func:`assemble_system_prompt` routes to a stage-specific suffix
    # via :data:`_STAGE_TO_PROMPT` instead of using this level mapping.
    "agent_staged": "planner",
    # ``agent_live`` shares the same per-stage suffix routing as
    # ``agent_staged`` (its state machine is identical through
    # fill_settings); ``planner`` is the no-stage fallback.
    "agent_live": "planner",
    "docgen": "assist",
    # Settings autocomplete maps to copilot — short-context
    # suggestion-shaped output. The strict-JSON-only instruction is
    # added inline by the autocomplete module's per-call system prompt
    # rather than living in a fourth level file.
    "settings_autocomplete": "copilot",
    # Lineage Q&A is a read-only Assist surface — same level mapping
    # as ``docgen`` and ``explain``. The lineage-specific shape
    # contract lives in the appended user-message block rather than a
    # fourth level file.
    "lineage": "assist",
    # Intent classifier — read-only single-shot judgement; level
    # mapping is nominal (the classifier injects its own tight system
    # prompt rather than using the layered prompt). Mapped to
    # ``copilot`` only because SURFACE_TO_LEVEL is exhaustive over
    # SurfaceLiteral; ``assemble_system_prompt`` is never called for
    # this surface.
    "intent_classifier": "copilot",
}


_STAGE_TO_PROMPT: dict[str, str] = {
    "plan": "stage_plan",
    "classify": "stage_classify",
    "pick_type": "stage_pick_type",
    "pick_upstream": "stage_pick_upstream",
    "fill_settings": "stage_fill_settings",
    "single_stage_op": "planner",
    "verify_completion": "stage_verify_completion",
}
"""Per-stage suffix file map for the ``agent_staged`` surface.

Each stage gets its own short suffix prompt so the LLM only sees the
guidance relevant to the choice it's about to make. ``single_stage_op``
re-uses ``planner.md`` because the modify/delete/connect/disconnect
ops follow the same staging discipline as the legacy agent surfaces
(they emit one ``flowfile.graph.<op>`` call directly, no multi-stage
cycle)."""


_PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

_HTML_COMMENT_OPEN = "<!--"
_HTML_COMMENT_CLOSE = "-->"

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Snapshot models                                                              #
# --------------------------------------------------------------------------- #


class ColumnSnapshot(BaseModel):
    """A single column in the prompt context.

    Carries only what the model actually needs — name and dtype always,
    sample values only when ``samples_mode != "off"``.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    data_type: str
    nullable: bool | None = None
    sample: list[str] | None = None


class NodeSnapshot(BaseModel):
    """Projection of a :class:`FlowNode` for prompt rendering."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    node_id: int | str
    name: str
    node_type: str
    settings: dict[str, Any] = Field(default_factory=dict)
    schema_columns: list[ColumnSnapshot] | None = None
    schema_status: Literal["known", "unknown"] = "unknown"
    is_pinned: bool = False


class SubgraphSnapshot(BaseModel):
    """A pinned-plus-upstream slice of a :class:`FlowGraph`."""

    pinned_node_ids: list[int | str]
    nodes: list[NodeSnapshot]
    edges: list[tuple[int | str, int | str]] = Field(default_factory=list)
    samples_mode: SamplesMode = "off"


class PromptContext(BaseModel):
    """The full per-call prompt — system + user + message list."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    surface: SurfaceLiteral
    system: str
    user: str
    messages: list[Message]
    snapshot: SubgraphSnapshot
    report: BudgetReport
    resolved_mentions: list[ResolvedMention] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Public entry points                                                          #
# --------------------------------------------------------------------------- #


def render_prompt_context(
    graph: FlowGraph,
    pinned_node_ids: list[int | str] | int | str,
    *,
    surface: SurfaceLiteral,
    samples_mode: SamplesMode = "off",
    sample_rows: int = 5,
    mentions: list[Mention] | str | None = None,
    selection_node_ids: list[int | str] | None = None,
    max_columns_per_node: int | None = None,
    depth: int | None = None,
    resolve_schemas: bool = True,
    stage: str | None = None,
    picked_node_type: str | None = None,
) -> PromptContext:
    """Build a budget-bounded :class:`PromptContext` for ``surface``.

    ``pinned_node_ids`` may be a single id or a list. ``mentions`` may be
    raw user text (parsed here) or a pre-parsed list of :class:`Mention`s.
    Any ``@flow`` / ``@selection`` / ``@node`` / ``@schema`` mention
    contributes its resolved ``node_ids`` to the pinned set so the
    subgraph walk includes them.

    ``resolve_schemas`` (default ``True``) applies the tier-1
    prospective-schema resolution for predictable upstreams (see
    module docstring). Pass ``False`` to fall back to the cache-only
    behaviour.
    """

    pinned_list = _coerce_to_list(pinned_node_ids)
    mention_list, raw_text = _coerce_mentions(mentions)
    resolved = resolve_mentions(mention_list, graph, selection_node_ids=selection_node_ids)
    extra_pinned = [node_id for r in resolved for node_id in r.node_ids if node_id not in pinned_list]
    pinned_list = pinned_list + extra_pinned

    logger.debug(
        "render_prompt_context surface=%s samples_mode=%s sample_rows=%d pinned=%s",
        surface,
        samples_mode,
        sample_rows,
        pinned_list,
    )

    snapshot = extract_subgraph(graph, pinned_list, depth=depth, resolve_schemas=resolve_schemas)
    snapshot = _attach_samples(snapshot, graph, samples_mode=samples_mode, sample_rows=sample_rows)
    snapshot.samples_mode = samples_mode

    rendered_user = render_user_message(snapshot, user_text=raw_text)
    snapshot, report = apply_budget(
        snapshot,
        surface,
        samples_mode=samples_mode,
        rendered_size_hint=estimate_tokens(rendered_user),
        max_columns_per_node=max_columns_per_node,
    )
    rendered_user = render_user_message(snapshot, user_text=raw_text)
    report.estimated_input_tokens = estimate_tokens(rendered_user)

    system = assemble_system_prompt(surface, stage=stage, picked_node_type=picked_node_type)
    messages = [
        Message(role="system", content=system),
        Message(role="user", content=rendered_user),
    ]

    return PromptContext(
        surface=surface,
        system=system,
        user=rendered_user,
        messages=messages,
        snapshot=snapshot,
        report=report,
        resolved_mentions=resolved,
    )


def extract_subgraph(
    graph: FlowGraph,
    pinned_node_ids: list[int | str] | int | str,
    *,
    depth: int | None = None,
    resolve_schemas: bool = True,
) -> SubgraphSnapshot:
    """Walk upstream from ``pinned_node_ids``, returning a snapshot.

    ``depth=None`` walks the full transitive upstream; ``depth=0`` is
    the pinned set only; ``depth=N`` is the pinned set plus ``N``
    levels of parents. Nodes are returned in topological upstream-first
    order so :mod:`budget` can drop the furthest-upstream entries first.

    ``resolve_schemas`` (default ``True``) is forwarded to
    :func:`snapshot_node` — see the module docstring for the
    schema-resolution semantics.
    """

    pinned_list = _coerce_to_list(pinned_node_ids)
    pinned_nodes: list[FlowNode] = [
        node for node in (graph.get_node(node_id) for node_id in pinned_list) if node is not None
    ]
    pinned_resolved_ids = {node.node_id for node in pinned_nodes}

    visited: dict[int | str, FlowNode] = {node.node_id: node for node in pinned_nodes}
    order: list[FlowNode] = list(pinned_nodes)
    edges: set[tuple[int | str, int | str]] = set()

    queue: deque[tuple[FlowNode, int]] = deque((node, 0) for node in pinned_nodes)
    while queue:
        current, level = queue.popleft()
        if depth is not None and level >= depth:
            continue
        for parent in current.all_inputs:
            edges.add((parent.node_id, current.node_id))
            if parent.node_id in visited:
                continue
            visited[parent.node_id] = parent
            order.append(parent)
            queue.append((parent, level + 1))

    topo: list[FlowNode] = list(reversed(order))

    snapshots = [
        snapshot_node(
            node,
            samples_mode="off",
            sample_rows=0,
            is_pinned=node.node_id in pinned_resolved_ids,
            resolve_schemas=resolve_schemas,
        )
        for node in topo
    ]

    return SubgraphSnapshot(
        pinned_node_ids=list(pinned_resolved_ids),
        nodes=snapshots,
        edges=sorted(edges),
        samples_mode="off",
    )


def snapshot_node(
    node: FlowNode,
    *,
    samples_mode: SamplesMode = "off",
    sample_rows: int = 0,
    is_pinned: bool = False,
    resolve_schemas: bool = True,
) -> NodeSnapshot:
    """Project ``node`` into a JSON-safe :class:`NodeSnapshot`.

    Sample rows are only attached when ``samples_mode != "off"`` and
    the node has cached predicted data with ``data`` populated. PII
    scrubbing is **not** applied here — the safety module wraps this
    seam.

    When ``resolve_schemas=True`` (default) and the cached predicted
    schema is empty, this fires the tier-1 resolver for predictable
    upstreams; see the module docstring.
    """

    settings_dict = _settings_to_dict(node.setting_input)
    columns: list[ColumnSnapshot] | None = None
    status: Literal["known", "unknown"] = "unknown"

    predicted = _safe_get_predicted_schema(node)
    if predicted is None and resolve_schemas:
        predicted = _resolve_predicted_schema_if_predictable(node)
    if predicted is not None:
        columns = [_column_snapshot(col) for col in predicted]
        status = "known"

    if samples_mode != "off" and sample_rows > 0 and columns is not None:
        sample_rows_data = _safe_get_sample_data(node, sample_rows)
        if sample_rows_data:
            columns = _attach_samples_to_columns(columns, sample_rows_data)

    return NodeSnapshot(
        node_id=node.node_id,
        name=getattr(node, "name", None) or str(node.node_id),
        node_type=node.node_type,
        settings=settings_dict,
        schema_columns=columns,
        schema_status=status,
        is_pinned=is_pinned,
    )


def assemble_system_prompt(
    surface: SurfaceLiteral,
    *,
    stage: str | None = None,
    picked_node_type: str | None = None,
) -> str:
    """Compose the layered system prompt for ``surface``.

    Reads ``prompts/base.md`` plus ``prompts/{level}.md`` (where
    ``level`` is mapped from ``surface`` via :data:`SURFACE_TO_LEVEL`),
    strips HTML comment markers, and joins them with a blank line.

    Tool-calling surfaces (``agent_complex``, ``cmd_k``, ``ghost_node``,
    and the staged surfaces) get a ``Tool catalog`` block — agent-shaped
    "when to call this tool" prose used to disambiguate which tool to
    dispatch. Read-only / advisory surfaces (``explain``, ``lineage``,
    ``docgen``) get a separate ``Flowfile node reference`` block built
    from per-node ``user_instructions`` (palette label, sidebar
    section, key settings, worked example, pitfalls) so the chat can
    answer "how do I X" with the user's actual UI vocabulary instead of
    inventing names like "transform node". ``settings_autocomplete``
    is a constrained-JSON surface and skips both blocks.

    For ``surface="agent_staged"`` the suffix is selected from
    :data:`_STAGE_TO_PROMPT` per the supplied ``stage``. The catalog
    block also varies by stage: the full catalog at ``"pick_type"``, a
    single-node block at ``"fill_settings"`` (using
    ``picked_node_type``), and an empty catalog at the other stages.

    Raises ``ValueError`` for unknown surfaces.
    """

    if surface not in SURFACE_TO_LEVEL:
        raise ValueError(f"Unknown surface {surface!r}. Expected one of {sorted(get_args(SurfaceLiteral))}.")

    base = _load_prompt("base")
    # ``agent_live`` reuses the same per-stage suffix routing as
    # ``agent_staged``: identical state machine through fill_settings,
    # only the post-apply behaviour differs.
    if surface in ("agent_staged", "agent_live") and stage:
        suffix_name = _STAGE_TO_PROMPT.get(stage, SURFACE_TO_LEVEL[surface])
    else:
        suffix_name = SURFACE_TO_LEVEL[surface]
    suffix = _load_prompt(suffix_name)
    catalog = _build_catalog_block(surface, stage=stage, picked_node_type=picked_node_type)
    blocks = [block for block in (base, suffix, catalog) if block]
    return "\n\n".join(blocks)


_CATALOG_HEADER = "## Tool catalog"
_NODE_REFERENCE_HEADER = "## Flowfile node reference"

# Surface that gets the full catalog block. ``agent_complex`` is the
# one-shot full-catalog surface; ``agent_staged`` renders the catalog
# only at the ``pick_type`` stage via the per-stage branch in
# :func:`_build_catalog_block`.
_FULL_CATALOG_SURFACES: frozenset[str] = frozenset({"agent_complex"})

# Surfaces that get a narrowed catalog filtered to their preset.
_PRESET_CATALOG_SURFACES: frozenset[str] = frozenset({"cmd_k", "ghost_node"})

# Read-only / advisory ("assist-level") surfaces — they don't call tools
# but they advise the user about Flowfile, so they need user-facing UI
# vocabulary. We give them a per-node-type reference rendered from
# ``user_instructions`` (palette label, sidebar, settings labels, worked
# example, pitfalls) so "how do I X" answers cite real UI elements
# instead of hallucinating names like "transform node" or "expression
# editor".
_ASSIST_CATALOG_SURFACES: frozenset[str] = frozenset({"explain", "lineage", "docgen"})

_NODE_TYPE_TOOL_PREFIX = "flowfile.graph.add_"


def _build_catalog_block(
    surface: str,
    *,
    stage: str | None = None,
    picked_node_type: str | None = None,
) -> str:
    """Build the narrative block for ``surface``.

    Two views over one source-of-truth:

    * **Tool-calling surfaces** (``agent_complex`` / ``cmd_k`` /
      ``ghost_node``) — emit ``## Tool catalog`` with each tool's
      ``long_description`` plus, where set, a fenced
      ``agent_payload_example`` showing the literal JSON the executor
      accepts. Agent-shaped: *"when to call this tool"*.
    * **Assist-level surfaces** (``explain`` / ``lineage`` / ``docgen``)
      — emit ``## Flowfile node reference`` with each node-type tool's
      ``user_instructions`` only (palette label / sidebar / settings
      labels / worked example / pitfalls). User-shaped: *"how does the
      user do this in the UI"*. Agent specs and payload examples are
      omitted: the chat surface can't call tools, surfacing them would
      only confuse the model into mixing UI advice with tool-call talk.
    * ``settings_autocomplete`` — returns ``""`` (constrained-JSON
      output, no narrative grounding needed).

    ``agent_staged`` is per-stage:

    * ``stage="pick_type"`` — full ``## Tool catalog`` with every node
      type's narrative grounding so the LLM picks the right one.
    * ``stage="fill_settings"`` — a single-node block scoped to
      ``picked_node_type`` (description + payload example). Other
      stages get ``""`` — they don't need catalog content because
      their decision space is already constrained by the
      function-calling enum.

    Lazy-imports ``tools.registry`` so the prompts package stays
    independently importable in tests that mock the catalog.
    """

    from flowfile_core.ai.tools.registry import build_tool_catalog

    full_catalog = build_tool_catalog()

    if surface in ("agent_staged", "agent_live"):
        if stage == "pick_type":
            tools = build_tool_catalog(surface="agent_complex")
            catalog_block = _render_tool_catalog(tools)
            # Append palette-label vs node_type disambiguation. Only
            # at pick_type (other stages don't pick types and don't
            # need the warning).
            disambig = _build_palette_label_disambiguation_block()
            if disambig:
                return f"{catalog_block}\n\n{disambig}"
            return catalog_block
        if stage == "classify":
            # Some models bypass classify_intent and emit
            # pick_node_type directly at this stage. Surface the
            # disambiguation block at classify too so the warning
            # reaches the LLM regardless of bypass. Cheap (~600 chars)
            # and only runs at the classify round, which has the
            # smallest budget anyway.
            disambig = _build_palette_label_disambiguation_block()
            return disambig or ""
        if stage == "fill_settings" and picked_node_type:
            return _build_single_node_block(picked_node_type, full_catalog)
        return ""

    if surface in _FULL_CATALOG_SURFACES:
        tools = build_tool_catalog(surface="agent_complex")
        return _render_tool_catalog(tools)
    if surface in _PRESET_CATALOG_SURFACES:
        tools = build_tool_catalog(surface=surface)
        return _render_tool_catalog(tools)
    if surface in _ASSIST_CATALOG_SURFACES:
        node_tools = [tool for tool in full_catalog if tool.name.startswith(_NODE_TYPE_TOOL_PREFIX)]
        return _render_node_reference(node_tools)
    return ""


@functools.lru_cache(maxsize=1)
def _palette_label_disambiguation_pairs() -> tuple[tuple[str, str], ...]:
    """Return ``(node_type, palette_label)`` pairs where snake-casing
    the palette label produces a different identifier from the
    registered ``node_type``.

    These are the exact mismatches that confuse small LLMs into
    emitting the snake-cased palette label as a ``node_type``. The
    disambiguation block (rendered at ``stage="pick_type"``) lists
    these so the LLM sees the explicit do/don't pairing.

    Cached at module level — palette labels are static at runtime.
    """
    try:
        from flowfile_core.ai.tools.node_docs import palette_label_for
        from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS
    except Exception:
        return ()

    pairs: list[tuple[str, str]] = []
    for nt in sorted(NODE_TYPE_TO_SETTINGS_CLASS.keys()):
        label = palette_label_for(nt)
        if not label or label == nt:
            continue
        # Snake-case the palette label and compare to the node_type.
        snake = label.lower().replace(" ", "_")
        if snake != nt and snake.replace("-", "_") != nt:
            pairs.append((nt, label))
    return tuple(pairs)


def _build_palette_label_disambiguation_block() -> str:
    """Render the do/don't section warning the LLM that the
    function-calling enum is the snake_case ``node_type``, not the
    snake-cased palette label. Rendered at the ``pick_type`` stage of
    ``agent_staged`` (where the LLM is about to pick a node_type).
    """
    pairs = _palette_label_disambiguation_pairs()
    if not pairs:
        return ""

    lines: list[str] = [
        "## Important: enum is `node_type`, NOT the palette label",
        "",
        (
            "The function-calling enum on ``flowfile.meta.pick_node_type`` "
            "is the registered snake_case ``node_type``. The catalog above "
            "leads with the palette label (in *italics* or quotes). DO NOT "
            "snake-case the palette label and submit it — the enum will "
            "reject every value not on this list. Common confusions:"
        ),
        "",
    ]
    for nt, label in pairs:
        lines.append(f"- ✅ ``{nt}``  ❌ ``{label.lower().replace(' ', '_')}`` (palette label *\"{label}\"*)")
    return "\n".join(lines)


@functools.lru_cache(maxsize=1)
def _formula_function_names() -> tuple[str, ...]:
    """Fetch the canonical Flowfile expression-function list.

    Sourced from ``polars_expr_transformer.function_overview``, the same
    surface the chat-side ``GET /editor/expressions`` route uses. Cached
    via ``lru_cache`` so the polars_expr_transformer import is paid once
    per process — and only when stage-3 fill_settings actually renders
    a formula prompt (NOT at module import or in unrelated paths).
    Falls back to a hard-coded short list if the import fails (e.g.
    test environments without the package installed) so the catalog
    stays renderable.
    """
    try:
        from polars_expr_transformer.function_overview import get_all_expressions
    except Exception:
        return ()
    try:
        names = get_all_expressions()
    except Exception:
        return ()
    return tuple(sorted({str(n) for n in names if isinstance(n, str) and n}))


def _build_sql_query_caveat_block() -> str:
    """Conditional sql_query-specific guidance. Renders ONLY at stage
    ``fill_settings`` when ``picked_node_type == "sql_query"`` (gated
    by the caller in ``_build_single_node_block``). Other node types
    (including ``polars_code``, which has its own predictor path that
    works fine) don't see this block. Pick_type / agent_complex
    full-catalog rounds also don't render it — keeping the caveat
    scoped to the one prompt where the LLM is about to stage a
    sql_query, exactly when the guidance is actionable.

    Two pieces:

    * **Table-name convention.** Upstream inputs are registered
      positionally as ``input_1``, ``input_2``, ... by
      :func:`flowfile_core.flowfile.flow_data_engine.flow_data_engine.execute_sql_query`
      (``ctx.register(f"input_{i + 1}", ...)``). The chat LLM
      previously hallucinated ``join_<node_id>``-shaped table names
      from the prior worked example; the agent inherited the
      hallucination and produced ``FROM join_5`` SQL that hit
      ``relation 'join_5' was not found`` at runtime.
    * **Development-mode auto-undo.** ``add_sql_query`` does not
      register a ``schema_callback`` (see ``flow_graph.py``
      ``add_sql_query``), so ``_observe_development``'s
      ``get_predicted_schema(force=True)`` returns ``None`` and the
      host fails the observation with ``UnpredictableSchema``,
      auto-undoing the just-added node. Without this pre-warning the
      LLM hits the failure, hallucinates a cause, claims false
      success (*"I added a sql_query node"* — but the node was
      deleted), or burns its retry budget on identical re-attempts.
    """
    return (
        "## sql_query-specific guidance\n"
        "\n"
        "**Upstream table names — positional, NEVER node-id-based.** "
        "When you stage this node, the host registers each upstream "
        "input as ``input_1``, ``input_2``, ... (in connect order). "
        "Always write ``FROM input_1`` (single upstream) or "
        "``FROM input_1 a JOIN input_2 b ON ...`` (multiple "
        "upstreams). Do **NOT** invent table names from the upstream "
        "node's id, type, or display name (e.g. ``FROM join_5`` is "
        "wrong — it will fail at runtime with ``relation 'join_5' "
        "was not found``). This is true regardless of what the chat "
        "trail above may have suggested.\n"
        "\n"
        "**Development-mode caveat.** Polars' embedded SQL engine "
        "cannot introspect a SELECT's column list without running "
        "it, and the sql_query node currently has no "
        "``schema_callback`` to work around that, so "
        "``get_predicted_schema`` returns ``None`` for this node "
        "type. On ``surface=agent_live`` runs in **Development "
        "mode**, the host's post-apply observation will fail with "
        "``UnpredictableSchema`` and **auto-undo your just-added "
        "node** — the canvas reverts to the prior state and you only "
        "see the failure on the next round's tool reply.\n"
        "\n"
        "Before staging this node, check the user message for the "
        "flow's execution mode (the host surfaces it). If the flow "
        "is in Development mode, **do not stage** — instead, write a "
        "short assistant message refusing the operation and ask the "
        "user to switch the top-right mode toggle to **Performance** "
        "(the schema is predictable once the query actually runs). "
        "If the flow is already in Performance mode, proceed "
        "normally and use ``input_1`` / ``input_2`` / ... as the "
        "table names.\n"
        "\n"
        "If you've already attempted the add and the previous tool "
        "reply contains ``UnpredictableSchema``: do **not** retry "
        "the same payload — the failure mode is intrinsic to the "
        "run mode, not the settings. Quote the error verbatim and "
        "surface the Performance-mode fix."
    )


def _build_formula_function_reference_block() -> str:
    """Append a compact alphabetical list of available Flowfile-expression
    functions. Renders ONLY at stage-3 ``fill_settings`` when the picked
    node type is ``formula`` (gated by the caller in
    ``_build_single_node_block``) so smaller catalog surfaces
    (pick_type, agent_complex full catalog) don't carry the
    function-name dump on every round.
    """
    names = _formula_function_names()
    if not names:
        # Hard-coded fallback when polars_expr_transformer is unavailable
        # (test environments). Short list of widely-used functions.
        names = (
            "concat",
            "contains",
            "lowercase",
            "round",
            "to_date",
            "to_int",
            "to_string",
            "trim",
            "uppercase",
        )
    body = ", ".join(f"``{n}``" for n in names)
    return (
        "## Formula functions (Flowfile expression library)\n"
        "\n"
        "Available functions you can use inside the ``function`` field. "
        "Call them like ``uppercase([col])`` or ``concat([first], ' ', "
        "[last])``. Column references use SQL-style ``[column_name]`` "
        "(square brackets), NOT ``pl.col(...)``.\n"
        "\n"
        f"{body}"
    )


def _build_single_node_block(node_type: str, full_catalog: list[Any]) -> str:
    """W71 — render one node's narrative + payload example for stage 3.

    Used by ``agent_staged`` at stage ``fill_settings``: the LLM has
    already picked the node type at stage 1, so the system prompt only
    needs that one type's settings reference (not the full 40-type
    catalog). Mirrors :func:`_render_tool_catalog` for the single-tool
    case, including the fenced ``Example payload`` block when one
    exists in ``NODE_AGENT_PAYLOAD_EXAMPLES``.

    v1.2 — when the stage-3 tool exposes the inner-input shape (e.g.
    ``GroupByInput`` for ``group_by``), the rendered example is trimmed
    to the inner shape too. Without this trim the LLM would see the
    full envelope (``{"flow_id": 1, "node_id": 99, "groupby_input":
    {"agg_cols": [...]}}``) but a tool spec that expects only
    ``{"agg_cols": [...]}`` — confusing for any model and especially
    for smaller ones.

    Returns ``""`` when no matching tool is found in the catalog
    (defensive: stage 1 enforces a registered node type via the
    ``pick_node_type`` enum).
    """
    add_name = f"{_NODE_TYPE_TOOL_PREFIX}{node_type}"
    tool = next((t for t in full_catalog if t.name == add_name), None)
    if tool is None:
        return ""

    from flowfile_core.ai.tools.registry import get_staged_fill_inner_field_name

    inner_field = get_staged_fill_inner_field_name(node_type)
    example = tool.agent_payload_example.strip() if tool.agent_payload_example else ""
    if example and inner_field is not None:
        example = _trim_example_to_inner_shape(example, inner_field)

    lines: list[str] = ["## Node settings reference", ""]
    description = (tool.long_description or tool.description or "").strip()
    if description:
        lines.append(description)
        lines.append("")
    if example:
        lines.append("Example payload (use this exact shape):")
        lines.append("```json")
        lines.append(example)
        lines.append("```")
        lines.append("")

    # W71 v1.12C — when the picked type is ``formula``, append the
    # canonical Flowfile-expression function list. Gated to this exact
    # case so the catalog at pick_type / agent_complex never carries
    # the (potentially long) function-name dump on every round.
    if node_type == "formula":
        lines.append(_build_formula_function_reference_block())

    # sql_query has two issues that need pre-warning the LLM about
    # at the moment of staging: (a) upstream tables are registered
    # positionally as ``input_1``/``input_2``/... (the chat LLM
    # previously hallucinated ``join_<node_id>`` table names — see
    # the 2026-05-09 transcript), and (b) Development mode trips
    # ``UnpredictableSchema`` because the node has no
    # ``schema_callback``. polars_code has its own predictor path
    # that works fine, so it's NOT included in this gate.
    if node_type == "sql_query":
        lines.append(_build_sql_query_caveat_block())

    return "\n".join(lines).rstrip()


def _trim_example_to_inner_shape(example_json: str, inner_field: str) -> str:
    """W71 v1.2 — strip the wrapper envelope from a payload example.

    ``NODE_AGENT_PAYLOAD_EXAMPLES`` carries the full validated payload
    (``{"flow_id": 1, "node_id": 99, "<inner_field>": {...}}``). The
    inner-input stage-3 tool spec only accepts ``{...}`` (the inner
    object directly), so the example must be trimmed to match. Returns
    the original ``example_json`` unchanged when parsing fails or the
    inner field is missing — defensive: a stale example surface still
    renders, and the LLM has the JSON-schema spec to fall back on.
    """
    import json as _json

    try:
        data = _json.loads(example_json)
    except (TypeError, ValueError):
        return example_json
    if not isinstance(data, dict):
        return example_json
    inner_value = data.get(inner_field)
    if inner_value is None:
        return example_json
    try:
        return _json.dumps(inner_value, indent=2)
    except (TypeError, ValueError):
        return example_json


def _render_tool_catalog(tools: list[Any]) -> str:
    """Render the agent-shaped ``## Tool catalog`` block.

    Includes ``long_description`` for every documented tool, plus a
    fenced JSON ``Example payload`` block for the seven node types
    whose Pydantic shape diverges from the natural LLM guess
    (group_by / pivot / join / fuzzy_match / select / unpivot /
    text_to_rows — see ``NODE_AGENT_PAYLOAD_EXAMPLES``).
    """

    documented = [tool for tool in tools if tool.long_description]
    if not documented:
        return ""
    documented.sort(key=lambda tool: tool.name)
    lines: list[str] = [
        _CATALOG_HEADER,
        "",
        (
            "Detailed guidance per tool below — use this to choose which tool "
            "fits the user's request. The JSON Schema parameters are sent "
            "separately on each call; this section is the *when to use* "
            "narrative. Match the user's intent to the closest match here "
            "before emitting a tool call. For a handful of node types whose "
            "Pydantic shape diverges from the natural guess, an `Example "
            "payload` block follows the description — copy that exact shape, "
            "don't re-derive it from the JSON Schema."
        ),
        "",
    ]
    for tool in documented:
        # W71 v1.14A.2 — for ``flowfile.graph.add_<type>`` entries, render
        # the snake_case node_type beside the heading so the LLM sees the
        # enum value at every catalog entry, not only in the trailing
        # disambiguation block. Catches the common confusion where the
        # LLM snake-cases the palette label (*"Sort data"* → ``sort_data``)
        # — the heading now reads ``add_sort  (node_type: `sort`)``.
        if tool.name.startswith(_NODE_TYPE_TOOL_PREFIX):
            node_type = tool.name.removeprefix(_NODE_TYPE_TOOL_PREFIX)
            lines.append(f"### {tool.name}  (node_type: `{node_type}`)")
        else:
            lines.append(f"### {tool.name}")
        lines.append(tool.long_description.strip())
        if tool.agent_payload_example:
            lines.append("")
            lines.append("Example payload:")
            lines.append("```json")
            lines.append(tool.agent_payload_example.strip())
            lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip()


def _render_node_reference(tools: list[Any]) -> str:
    """Render the user-shaped ``## Flowfile node reference`` block.

    Pulled from each node-type tool's :attr:`ToolSpec.user_instructions`.
    Empty entries are dropped (caught at test time — every node type
    must populate them).
    """

    documented = [tool for tool in tools if tool.user_instructions]
    if not documented:
        return ""
    documented.sort(key=lambda tool: tool.name)
    lines: list[str] = [
        _NODE_REFERENCE_HEADER,
        "",
        (
            "Reference of every Flowfile node type and how the user creates "
            "and configures it in the canvas UI. When advising the user on "
            "how to accomplish a task, cite the actual palette labels and "
            "settings field names from this reference — never invent UI "
            "elements (no 'transform node', no 'aggregator', no "
            "'expression editor'; only what appears here exists)."
        ),
        "",
    ]
    for tool in documented:
        node_type = tool.name.removeprefix(_NODE_TYPE_TOOL_PREFIX)
        lines.append(f"### {node_type}")
        lines.append(tool.user_instructions.strip())
        lines.append("")
    return "\n".join(lines).rstrip()


def render_user_message(snapshot: SubgraphSnapshot, *, user_text: str | None = None) -> str:
    """Render the user prompt body from a :class:`SubgraphSnapshot`.

    The format is markdown-flavoured but stable enough for the prompt
    cache to hash. Pinned nodes are flagged inline; unknown schemas
    surface a clear marker so the model knows to refuse column refs
    (see plan §4.3 + D011).
    """

    lines: list[str] = []

    if user_text is not None and user_text.strip():
        lines.append("## User request")
        lines.append(user_text.strip())
        lines.append("")

    lines.append("## Subgraph")
    if not snapshot.nodes:
        lines.append("(empty)")
        return "\n".join(lines)

    lines.append(f"Pinned: {sorted(str(n) for n in snapshot.pinned_node_ids)}")
    lines.append(f"Samples mode: {snapshot.samples_mode}")
    lines.append("")

    for node in snapshot.nodes:
        marker = " (pinned)" if node.is_pinned else ""
        lines.append(f"### {node.name} (id={node.node_id}, type={node.node_type}){marker}")
        if node.settings:
            lines.append(f"settings: {_compact_json(node.settings)}")
        if node.schema_status == "unknown" or node.schema_columns is None:
            lines.append("schema: unknown — upstream node has no predicted schema")
        else:
            lines.append("schema:")
            for col in node.schema_columns:
                line = f"  - {col.name}: {col.data_type}"
                if col.nullable is False:
                    line += " (not null)"
                lines.append(line)
                if col.sample:
                    lines.append(f"      sample: {col.sample}")
        lines.append("")

    if snapshot.edges:
        lines.append("## Edges (upstream → downstream)")
        for src, dst in snapshot.edges:
            lines.append(f"- {src} → {dst}")

    return "\n".join(lines).rstrip() + "\n"


# --------------------------------------------------------------------------- #
# Internals                                                                    #
# --------------------------------------------------------------------------- #


def _coerce_to_list(node_ids: list[int | str] | int | str) -> list[int | str]:
    if isinstance(node_ids, list):
        return list(node_ids)
    return [node_ids]


def _coerce_mentions(
    mentions: list[Mention] | str | None,
) -> tuple[list[Mention], str | None]:
    if mentions is None:
        return [], None
    if isinstance(mentions, str):
        return parse_mentions(mentions), mentions
    return list(mentions), None


def _attach_samples(
    snapshot: SubgraphSnapshot,
    graph: FlowGraph,
    *,
    samples_mode: SamplesMode,
    sample_rows: int,
) -> SubgraphSnapshot:
    """Attach cached sample rows to each snapshot node, scrubbed per W25.

    Reads from :func:`_safe_get_sample_data`, which calls
    :meth:`FlowNode.get_table_example` (the same surface as
    ``GET /node/data``). No node is ever re-executed by this path —
    nodes that haven't run yield ``None`` and ship without samples.
    """

    if samples_mode == "off" or sample_rows <= 0:
        return snapshot

    safety_config = FlowSafetyConfig(sample_mode=samples_mode, sample_row_count=sample_rows)
    enriched: list[NodeSnapshot] = []
    for node_snapshot in snapshot.nodes:
        node = graph.get_node(node_snapshot.node_id)
        if node is None or node_snapshot.schema_columns is None:
            enriched.append(node_snapshot)
            continue
        raw_rows = _safe_get_sample_data(node, sample_rows)
        if not raw_rows:
            enriched.append(node_snapshot)
            continue
        scrubbed_rows = prepare_samples(raw_rows, safety_config)
        if not scrubbed_rows:
            enriched.append(node_snapshot)
            continue
        new_cols = _attach_samples_to_columns(node_snapshot.schema_columns, scrubbed_rows)
        enriched.append(node_snapshot.model_copy(update={"schema_columns": new_cols}))
    return snapshot.model_copy(update={"nodes": enriched})


def _settings_to_dict(setting_input: Any) -> dict[str, Any]:
    if setting_input is None:
        return {}
    dump = getattr(setting_input, "model_dump", None)
    if callable(dump):
        try:
            return dump(mode="json", exclude_none=True)
        except TypeError:
            return dump()
    if hasattr(setting_input, "__dict__"):
        return {k: v for k, v in vars(setting_input).items() if not k.startswith("_")}
    return {}


def _safe_get_predicted_schema(node: FlowNode) -> list[FlowfileColumn] | None:
    """Read predicted schema without forcing recomputation.

    Prefers the cached :attr:`NodeSchemaInformation.predicted_schema`
    attribute so the cache-only tier (D011 tier 0) stays cheap. The
    optional D011 tier-1 step lives in
    :func:`_resolve_predicted_schema_if_predictable` and is gated by the
    ``resolve_schemas`` kwarg in :func:`snapshot_node`.
    """

    node_schema = getattr(node, "node_schema", None)
    if node_schema is None:
        return None
    cached = getattr(node_schema, "predicted_schema", None)
    return cached or None


def _resolve_predicted_schema_if_predictable(node: FlowNode) -> list[FlowfileColumn] | None:
    """Apply **D011** tier-1 resolution for predictable upstreams.

    Mirrors the pattern in :func:`flowfile_core.ai.tools.predictor._resolve_upstream_schemas`
    (predictor.py:251-256). For ``static`` / ``source`` / ``passthrough``
    node types — i.e. anything for which
    :func:`flowfile_core.ai.tools.classification.is_predictable_via_mirror`
    returns ``True`` — we call :meth:`FlowNode.get_predicted_schema(force=True)`,
    which fires the registered ``schema_callback`` (or the
    ``_predicted_data_getter`` fallback) without invoking
    ``LazyFrame.collect`` — D012 stays clean.

    Dynamic node types (``polars_code`` / ``python_script`` /
    ``sql_query`` / ``pivot`` / etc.) need kernel dry-run, which stays
    W31-only per D003 + D012; this helper returns ``None`` for them so
    the snapshot keeps ``schema_status="unknown"``.

    Any exception falls back to ``None`` for parity with
    ``predictor.py:251-253`` so a malformed flow can't crash the prompt
    build.
    """

    if not is_predictable_via_mirror(node.node_type):
        return None
    try:
        forced = node.get_predicted_schema(force=True)
    except Exception:
        return None
    if not forced:
        return None
    # Mirror predictor.py:255 — be explicit about the in-place mutation
    # so same-session repeat reads (next render_prompt_context call)
    # hit the cache via _safe_get_predicted_schema.
    node.node_schema.predicted_schema = list(forced)
    return list(forced)


def _safe_get_sample_data(node: FlowNode, n: int) -> list[dict[str, Any]] | None:
    """Pull cached sample rows from a node's existing data-preview endpoint.

    Reads the same surface the frontend's data-preview tab uses
    (:meth:`FlowNode.get_table_example` with ``include_data=True``, wired to
    ``GET /node/data``). That returns rows from
    :attr:`NodeResults.example_data_generator` — cached PyArrow / parquet
    populated during a real node run, zero compute on read.

    Returns ``None`` when the node has not been run with its current
    setup (``has_run_with_current_setup=False``) or when the cached
    sample is empty. The chat preamble must never trigger node
    execution — pre-W65 this function called
    ``node._predicted_data_getter()`` which ran the node's ``_function``
    on materialized upstream data and could hang on any node whose
    ``_function`` calls ``.collect()`` (e.g. ``random_split``) or trains
    a model (``train_model``). The new contract is "cached only".
    """

    getter = getattr(node, "get_table_example", None)
    has_run = bool(getattr(getattr(node, "node_stats", None), "has_run_with_current_setup", False))
    if not callable(getter) or not has_run:
        return None
    try:
        example = getter(True)
    except Exception:
        logger.exception("sample-fetch error node=%s", getattr(node, "node_id", "?"))
        return None
    if example is None:
        return None
    rows = getattr(example, "data", None)
    if not rows:
        return None
    return list(rows)[:n] if n > 0 else []


def _column_snapshot(column: FlowfileColumn) -> ColumnSnapshot:
    nullable = None
    empty = getattr(column, "number_of_empty_values", None)
    if isinstance(empty, int) and empty == 0:
        nullable = False
    return ColumnSnapshot(
        name=getattr(column, "column_name", "?"),
        data_type=getattr(column, "data_type", "Unknown"),
        nullable=nullable,
    )


def _attach_samples_to_columns(columns: list[ColumnSnapshot], rows: list[dict[str, Any]]) -> list[ColumnSnapshot]:
    out: list[ColumnSnapshot] = []
    for col in columns:
        values = [_stringify(row.get(col.name)) for row in rows if col.name in row]
        out.append(col.model_copy(update={"sample": values or None}))
    return out


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if len(text) > 64:
        text = text[:61] + "..."
    return text


def _compact_json(value: Any) -> str:
    import json

    try:
        return json.dumps(value, default=str, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError):
        return repr(value)


def _load_prompt(name: str) -> str:
    path = _PROMPTS_DIR / f"{name}.md"
    if not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8")
    return _strip_html_comments(text).strip()


def _strip_html_comments(text: str) -> str:
    out = []
    i = 0
    while i < len(text):
        start = text.find(_HTML_COMMENT_OPEN, i)
        if start == -1:
            out.append(text[i:])
            break
        out.append(text[i:start])
        end = text.find(_HTML_COMMENT_CLOSE, start + len(_HTML_COMMENT_OPEN))
        if end == -1:
            break
        i = end + len(_HTML_COMMENT_CLOSE)
    return "".join(out)


__all__ = [
    "ColumnSnapshot",
    "NodeSnapshot",
    "PromptContext",
    "SamplesMode",
    "SubgraphSnapshot",
    "SURFACE_TO_LEVEL",
    "SurfaceLiteral",
    "PromptLevel",
    "Mention",
    "ResolvedMention",
    "BudgetReport",
    "assemble_system_prompt",
    "extract_subgraph",
    "parse_mentions",
    "render_prompt_context",
    "render_user_message",
    "resolve_mentions",
    "snapshot_node",
]
