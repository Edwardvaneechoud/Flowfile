"""Token-budgeted context selection — owned by W22.

When a flow's full subgraph + schemas exceed the per-surface budget, this
module decides which nodes / schemas / samples to drop. Budgets are
sourced from D010's per-surface placeholder targets and will be tightened
once W11 publishes real cost-per-flow numbers.

Design notes
------------

The estimator is intentionally crude (chars / 4) so the context module
doesn't pull in ``tiktoken`` or vendor SDKs. Real token counts arrive at
the provider layer — we accept some over-truncation in exchange for a
zero-dependency, deterministic budget pass.

Truncation order is fixed and audit-rendered in :class:`BudgetReport`:

1. Drop sample rows on the *furthest-upstream* nodes first — those are
   least likely to be the user's focus, and samples are the largest
   per-node payload.
2. Drop sample rows everywhere else (still preserves schema + settings).
3. Drop nodes furthest from the pinned set (still preserves the pinned
   node and its immediate parents).
4. Per-node column-list truncation — keep the first ``N`` columns and
   add a ``__truncated__`` ``ColumnSnapshot`` carrying the dropped count
   so the model knows the surface was capped.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flowfile_core.ai.context.builder import (
        ColumnSnapshot,
        NodeSnapshot,
        SamplesMode,
        SubgraphSnapshot,
        SurfaceLiteral,
    )


_CHARS_PER_TOKEN = 4


SurfaceBudget = tuple[int, int]


_SURFACE_BUDGETS: dict[str, SurfaceBudget] = {
    "cmd_k": (4_000, 1_500),
    "ghost_node": (4_000, 1_500),
    "explain": (16_000, 4_000),
    "agent": (32_000, 4_000),
    "agent_complex": (96_000, 4_000),
    "docgen": (32_000, 8_000),
    # W34 settings autocomplete: tiny prompt (schema column list + partial text)
    # and tiny response (≤5 short suggestions). Smaller than cmd_k to preserve
    # the sub-1s TTFB target on every keystroke.
    "settings_autocomplete": (2_000, 1_000),
    # W51 lineage Q&A: input mirrors ``agent`` because the run-history block
    # can be sizeable (10+ runs × per-node aggregates). Output stays modest
    # because lineage answers are summary-shaped, not generative.
    "lineage": (32_000, 4_000),
    # W58 intent classifier: tiny prompt (system + last 4 turns × ≤1 K chars
    # + current message) and a strict-JSON response capped at 96 tokens.
    # Smaller than ``settings_autocomplete`` because there's no schema
    # column dump in the prompt.
    "intent_classifier": (2_000, 200),
}


@dataclass(slots=True)
class BudgetReport:
    """Summary of what :func:`apply_budget` kept and dropped.

    ``estimated_input_tokens`` is the post-truncation estimate against
    which downstream layers can sanity-check before issuing the actual
    provider call.
    """

    surface: str
    prompt_budget: int
    response_budget: int
    estimated_input_tokens: int
    samples_dropped: int = 0
    nodes_dropped: int = 0
    columns_truncated: int = 0
    truncation_steps: list[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.truncation_steps is None:
            self.truncation_steps = []


def estimate_tokens(text: str) -> int:
    """Estimate token count from character count.

    Uses the well-known ``chars / 4`` heuristic — accurate to ±20% on
    English ASCII; closer for code-like text. Returns 0 on empty input.
    """

    if not text:
        return 0
    return math.ceil(len(text) / _CHARS_PER_TOKEN)


def surface_budget(surface: str) -> SurfaceBudget:
    """Return ``(prompt_budget, response_budget)`` for ``surface``.

    Falls back to the ``agent`` budget for unknown surfaces — the broad
    default lets callers keep working when adding a new surface without
    immediately editing this table.
    """

    return _SURFACE_BUDGETS.get(surface, _SURFACE_BUDGETS["agent"])


def apply_budget(
    snapshot: SubgraphSnapshot,
    surface: SurfaceLiteral,
    *,
    samples_mode: SamplesMode = "off",
    rendered_size_hint: int | None = None,
    max_columns_per_node: int | None = None,
) -> tuple[SubgraphSnapshot, BudgetReport]:
    """Truncate ``snapshot`` to fit ``surface``'s prompt budget.

    The truncation is iterative: each step computes a fresh size
    estimate from a serialised representation of the snapshot. Callers
    that already have a cheap way to measure rendered size can pass it
    via ``rendered_size_hint``; otherwise we recompute.

    Returns the (possibly modified) snapshot and a :class:`BudgetReport`.
    """

    prompt_budget, response_budget = surface_budget(surface)

    nodes = list(snapshot.nodes)
    edges = list(snapshot.edges)
    pinned_ids = set(snapshot.pinned_node_ids)

    report = BudgetReport(
        surface=surface,
        prompt_budget=prompt_budget,
        response_budget=response_budget,
        estimated_input_tokens=0,
    )

    if max_columns_per_node is not None:
        nodes, columns_truncated = _truncate_columns(nodes, max_columns_per_node)
        if columns_truncated:
            report.columns_truncated += columns_truncated
            report.truncation_steps.append(
                f"truncated columns: kept {max_columns_per_node}/node, " f"dropped {columns_truncated}",
            )

    estimated = rendered_size_hint if rendered_size_hint is not None else _estimate_snapshot(nodes, edges)
    if estimated <= prompt_budget:
        report.estimated_input_tokens = estimated
        return _rebuild(snapshot, nodes, edges), report

    # Step 1+2 — drop samples upstream-first.
    upstream_first_order = _upstream_first(nodes, pinned_ids)
    samples_dropped = 0
    for node in upstream_first_order:
        if not _has_samples(node):
            continue
        node = _strip_samples(node)
        nodes = _replace(nodes, node)
        samples_dropped += 1
        estimated = _estimate_snapshot(nodes, edges)
        if estimated <= prompt_budget:
            break
    if samples_dropped:
        report.samples_dropped = samples_dropped
        report.truncation_steps.append(
            f"dropped samples on {samples_dropped} node(s)",
        )

    if estimated <= prompt_budget:
        report.estimated_input_tokens = estimated
        return _rebuild(snapshot, nodes, edges), report

    # Step 3 — drop furthest-upstream nodes (excluding pinned).
    nodes_dropped = 0
    while estimated > prompt_budget:
        candidate = _next_drop_candidate(nodes, pinned_ids)
        if candidate is None:
            break
        nodes = [n for n in nodes if n.node_id != candidate.node_id]
        edges = [e for e in edges if candidate.node_id not in e]
        nodes_dropped += 1
        estimated = _estimate_snapshot(nodes, edges)
    if nodes_dropped:
        report.nodes_dropped = nodes_dropped
        report.truncation_steps.append(
            f"dropped {nodes_dropped} upstream node(s)",
        )

    # Step 4 — column truncation as a last resort if the caller didn't
    # pass an explicit cap. We default to keeping 20 columns per node.
    if estimated > prompt_budget and max_columns_per_node is None:
        nodes, columns_truncated = _truncate_columns(nodes, _DEFAULT_COL_CAP)
        if columns_truncated:
            report.columns_truncated += columns_truncated
            report.truncation_steps.append(
                f"truncated columns to {_DEFAULT_COL_CAP}/node, " f"dropped {columns_truncated}",
            )
        estimated = _estimate_snapshot(nodes, edges)

    report.estimated_input_tokens = estimated
    return _rebuild(snapshot, nodes, edges), report


_DEFAULT_COL_CAP = 20


def _rebuild(
    original: SubgraphSnapshot,
    nodes: list[NodeSnapshot],
    edges: list[tuple],
) -> SubgraphSnapshot:
    return original.model_copy(update={"nodes": nodes, "edges": edges})


def _has_samples(node: NodeSnapshot) -> bool:
    if node.schema_columns is None:
        return False
    return any(col.sample for col in node.schema_columns)


def _strip_samples(node: NodeSnapshot) -> NodeSnapshot:
    if node.schema_columns is None:
        return node
    new_cols: list[ColumnSnapshot] = [  # noqa: F821
        col.model_copy(update={"sample": None}) for col in node.schema_columns
    ]
    return node.model_copy(update={"schema_columns": new_cols})


def _replace(nodes: list[NodeSnapshot], replacement: NodeSnapshot) -> list[NodeSnapshot]:
    return [replacement if n.node_id == replacement.node_id else n for n in nodes]


def _truncate_columns(nodes: list[NodeSnapshot], cap: int) -> tuple[list[NodeSnapshot], int]:
    from flowfile_core.ai.context.builder import ColumnSnapshot

    truncated_total = 0
    out: list[NodeSnapshot] = []  # noqa: F821
    for node in nodes:
        if node.schema_columns is None or len(node.schema_columns) <= cap:
            out.append(node)
            continue
        kept = list(node.schema_columns[:cap])
        dropped = len(node.schema_columns) - cap
        marker = ColumnSnapshot(
            name="__truncated__",
            data_type=f"+{dropped} more columns",
            sample=None,
        )
        kept.append(marker)
        truncated_total += dropped
        out.append(node.model_copy(update={"schema_columns": kept}))
    return out, truncated_total


def _next_drop_candidate(nodes: list[NodeSnapshot], pinned_ids: set) -> NodeSnapshot | None:
    for node in _upstream_first(nodes, pinned_ids):
        if node.node_id in pinned_ids:
            continue
        return node
    return None


def _upstream_first(nodes: list[NodeSnapshot], pinned_ids: set) -> list[NodeSnapshot]:
    """Order nodes furthest-upstream first, with pinned nodes always last.

    The :class:`SubgraphSnapshot` already lays nodes out in topological
    upstream-to-downstream order; we simply respect that and stably move
    pinned nodes to the tail so they are dropped last.
    """

    upstream = [n for n in nodes if n.node_id not in pinned_ids]
    pinned = [n for n in nodes if n.node_id in pinned_ids]
    return upstream + pinned


def _estimate_snapshot(nodes: list[NodeSnapshot], edges: list[tuple]) -> int:
    chars = 0
    for node in nodes:
        chars += _estimate_node_chars(node)
    chars += 8 * len(edges)
    return math.ceil(chars / _CHARS_PER_TOKEN)


def _estimate_node_chars(node: NodeSnapshot) -> int:
    chars = len(str(node.node_id)) + len(node.name) + len(node.node_type) + 32
    chars += len(repr(node.settings))
    if node.schema_columns is not None:
        for col in node.schema_columns:
            chars += len(col.name) + len(col.data_type) + 8
            if col.sample is not None:
                chars += sum(len(s) for s in col.sample) + 4 * len(col.sample)
    return chars


__all__ = [
    "BudgetReport",
    "SurfaceBudget",
    "apply_budget",
    "estimate_tokens",
    "surface_budget",
]
