"""Subgraph + schema + sample serialiser — owned by W22.

Composes the per-call prompt context used by the AI surfaces:

* walks the :class:`flowfile_core.flowfile.flow_graph.FlowGraph` upstream
  from one or more "pinned" nodes;
* projects each visited node into a JSON-safe :class:`NodeSnapshot`;
* renders the snapshot as the user message;
* assembles the layered system prompt per **D008** (``prompts/base.md``
  concatenated with ``prompts/{level}.md``);
* hands the rendered context to :mod:`flowfile_core.ai.context.budget`
  for token-budget truncation when needed.

Sample rows are gated by ``samples_mode`` (default ``"off"`` per **D009**).
PII regex scrubbing is **not** performed here — that is W25's seam.

When an upstream node's ``predicted_schema`` is ``None`` (cold flow,
schema callback returned empty, source not yet accessible) the builder
emits ``schema_status="unknown"`` and propagates a ``None``
``schema_columns``. **W31 owns the D011 degraded-mode policy** — this
module is intentionally agnostic.
"""

from __future__ import annotations

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
    "agent",
    "agent_complex",
    "docgen",
    "settings_autocomplete",
    "lineage",
]

SamplesMode = Literal["off", "regex"]

PromptLevel = Literal["assist", "copilot", "planner"]

SURFACE_TO_LEVEL: dict[str, PromptLevel] = {
    "cmd_k": "copilot",
    "ghost_node": "copilot",
    "explain": "assist",
    "agent": "planner",
    "agent_complex": "planner",
    "docgen": "assist",
    # Settings autocomplete (W34) maps to copilot — short-context suggestion-shaped
    # output. The strict-JSON-only instruction is added inline by the autocomplete
    # module's per-call system prompt rather than living in a fourth level file
    # (D008 anchors level vocabulary to the three depth levels).
    "settings_autocomplete": "copilot",
    # Lineage Q&A (W51) is a read-only Assist surface — same level mapping as
    # ``docgen`` and ``explain``. The lineage-specific shape contract lives in
    # the appended user-message block rather than a fourth level file.
    "lineage": "assist",
}


_PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

_HTML_COMMENT_OPEN = "<!--"
_HTML_COMMENT_CLOSE = "-->"


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
    """The full per-call prompt — system + user + W11 message list."""

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
) -> PromptContext:
    """Build a budget-bounded :class:`PromptContext` for ``surface``.

    ``pinned_node_ids`` may be a single id or a list. ``mentions`` may be
    raw user text (parsed here) or a pre-parsed list of :class:`Mention`s.
    Any ``@flow`` / ``@selection`` / ``@node`` / ``@schema`` mention
    contributes its resolved ``node_ids`` to the pinned set so the
    subgraph walk includes them.
    """

    pinned_list = _coerce_to_list(pinned_node_ids)
    mention_list, raw_text = _coerce_mentions(mentions)
    resolved = resolve_mentions(mention_list, graph, selection_node_ids=selection_node_ids)
    extra_pinned = [node_id for r in resolved for node_id in r.node_ids if node_id not in pinned_list]
    pinned_list = pinned_list + extra_pinned

    snapshot = extract_subgraph(graph, pinned_list, depth=depth)
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

    system = assemble_system_prompt(surface)
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
) -> SubgraphSnapshot:
    """Walk upstream from ``pinned_node_ids``, returning a snapshot.

    ``depth=None`` walks the full transitive upstream; ``depth=0`` is
    the pinned set only; ``depth=N`` is the pinned set plus ``N``
    levels of parents. Nodes are returned in topological upstream-first
    order so :mod:`budget` can drop the furthest-upstream entries first.
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
) -> NodeSnapshot:
    """Project ``node`` into a JSON-safe :class:`NodeSnapshot`.

    Sample rows are only attached when ``samples_mode != "off"`` and the
    node has cached predicted data with ``data`` populated. PII scrubbing
    is **not** applied here — W25 wraps this seam.
    """

    settings_dict = _settings_to_dict(node.setting_input)
    columns: list[ColumnSnapshot] | None = None
    status: Literal["known", "unknown"] = "unknown"

    predicted = _safe_get_predicted_schema(node)
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


def assemble_system_prompt(surface: SurfaceLiteral) -> str:
    """Compose the layered system prompt for ``surface`` per **D008**.

    Reads ``prompts/base.md`` plus ``prompts/{level}.md`` (where
    ``level`` is mapped from ``surface`` via :data:`SURFACE_TO_LEVEL`),
    strips HTML comment markers used in the W10 stubs, and joins them
    with a blank line.

    Raises ``ValueError`` for unknown surfaces.
    """

    if surface not in SURFACE_TO_LEVEL:
        raise ValueError(f"Unknown surface {surface!r}. Expected one of {sorted(get_args(SurfaceLiteral))}.")

    base = _load_prompt("base")
    suffix = _load_prompt(SURFACE_TO_LEVEL[surface])
    blocks = [block for block in (base, suffix) if block]
    return "\n\n".join(blocks)


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
    if samples_mode == "off" or sample_rows <= 0:
        return snapshot
    enriched: list[NodeSnapshot] = []
    for node_snapshot in snapshot.nodes:
        node = graph.get_node(node_snapshot.node_id)
        if node is None or node_snapshot.schema_columns is None:
            enriched.append(node_snapshot)
            continue
        sample_data = _safe_get_sample_data(node, sample_rows)
        if not sample_data:
            enriched.append(node_snapshot)
            continue
        new_cols = _attach_samples_to_columns(node_snapshot.schema_columns, sample_data)
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
    attribute so we don't accidentally trigger expensive schema
    callbacks during prompt building. If the cache is empty we leave it
    empty and signal ``schema_status="unknown"``.
    """

    node_schema = getattr(node, "node_schema", None)
    if node_schema is None:
        return None
    cached = getattr(node_schema, "predicted_schema", None)
    return cached or None


def _safe_get_sample_data(node: FlowNode, n: int) -> list[dict[str, Any]] | None:
    """Pull cached sample rows from the node's predicted-data getter.

    Returns ``None`` if anything is missing — sample rows are an opt-in
    enrichment, never required for the snapshot to be valid.
    """

    getter = getattr(node, "_predicted_data_getter", None)
    if not callable(getter):
        return None
    try:
        predicted_data = getter()
    except Exception:
        return None
    if predicted_data is None:
        return None
    rows: list[dict[str, Any]] | None = None
    for attr in ("data_for_ai", "sample_rows", "data"):
        candidate = getattr(predicted_data, attr, None)
        if candidate:
            rows = list(candidate)
            break
    if rows is None:
        return None
    return rows[:n] if n > 0 else []


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
