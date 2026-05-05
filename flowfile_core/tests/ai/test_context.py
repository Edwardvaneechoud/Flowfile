"""W22 — Context builder unit tests.

Covers the public surface of :mod:`flowfile_core.ai.context`:

* subgraph extraction (``extract_subgraph``)
* node projection (``snapshot_node``)
* layered system-prompt assembly (``assemble_system_prompt``)
* token-budget truncation priority (``apply_budget``)
* mention parsing + resolution (``parse_mentions``, ``resolve_mentions``)
* end-to-end ``render_prompt_context`` over a real :class:`FlowGraph`

The lazy-import contract follows W11/W13's pattern: importing
``flowfile_core.ai.context.builder`` must not pull ``litellm`` itself.
"""

from __future__ import annotations

import sys
from typing import Any, get_args

import pytest

from flowfile_core.ai.context import (
    SURFACE_TO_LEVEL,
    BudgetReport,
    ColumnSnapshot,
    Mention,
    NodeSnapshot,
    PromptContext,
    SubgraphSnapshot,
    SurfaceLiteral,
    apply_budget,
    assemble_system_prompt,
    estimate_tokens,
    extract_subgraph,
    parse_mentions,
    render_prompt_context,
    render_user_message,
    resolve_mentions,
    snapshot_node,
    surface_budget,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

# --------------------------------------------------------------------------- #
# Test fixtures                                                                #
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_ai_context",
    )


def _basic_flow() -> FlowGraph:
    return FlowGraph(flow_settings=_flow_settings(), name="ctx_test")


def _add_orders_input(flow: FlowGraph, node_id: int = 1) -> FlowGraph:
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="customer_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="amount", data_type="Double"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[
                [1, 2, 3, 4],
                [10, 20, 30, 40],
                [100.0, 200.0, 50.0, 75.0],
                ["EU", "US", "EU", "US"],
            ],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(node_id).name = "orders"
    return flow


def _add_filter(flow: FlowGraph, node_id: int = 2, parent_id: int = 1) -> FlowGraph:
    filter_node = input_schema.NodeFilter(
        flow_id=flow.flow_id,
        node_id=node_id,
        depending_on_id=parent_id,
        filter_input=transform_schema.FilterInput(
            filter_type="advanced",
            advanced_filter="[region]=='EU'",
        ),
    )
    flow.add_filter(filter_node)
    add_connection(
        flow,
        node_connection=input_schema.NodeConnection.create_from_simple_input(parent_id, node_id),
    )
    flow.get_node(node_id).name = "filter_eu"
    return flow


def _add_select(flow: FlowGraph, node_id: int = 3, parent_id: int = 2) -> FlowGraph:
    select_node = input_schema.NodeSelect(
        flow_id=flow.flow_id,
        node_id=node_id,
        depending_on_id=parent_id,
        select_input=[
            transform_schema.SelectInput("order_id", "order_id", keep=True),
            transform_schema.SelectInput("amount", "amount", keep=True),
            transform_schema.SelectInput("region", "region", keep=False),
            transform_schema.SelectInput("customer_id", "customer_id", keep=False),
        ],
    )
    flow.add_select(select_node)
    add_connection(
        flow,
        node_connection=input_schema.NodeConnection.create_from_simple_input(parent_id, node_id),
    )
    flow.get_node(node_id).name = "select_only_eu_orders"
    return flow


@pytest.fixture
def linear_flow() -> FlowGraph:
    """orders (1) → filter_eu (2) → select_only_eu_orders (3)."""

    flow = _basic_flow()
    _add_orders_input(flow)
    _add_filter(flow)
    _add_select(flow)
    # Force schema prediction for every node so snapshots get a known schema.
    for node in flow.nodes:
        node.get_predicted_schema()
    return flow


@pytest.fixture
def cold_static_flow() -> FlowGraph:
    """orders (1) → filter_eu (2). predicted_schema NOT pre-warmed.

    Used by the W48 tier-1 prospective-schema tests. ``filter`` is
    classified ``static`` (predictable via the mirror-graph path), so
    the W48 helper should resolve its schema via the production
    schema-prediction path on the next ``render_prompt_context`` call.
    """

    flow = _basic_flow()
    _add_orders_input(flow)
    _add_filter(flow)
    # Be explicit: ensure the filter's cache is empty so the W48 helper
    # is the only thing that can populate it during the test.
    flow.get_node(2).node_schema.predicted_schema = None
    return flow


@pytest.fixture
def cold_dynamic_flow() -> FlowGraph:
    """orders (1) → polars_passthrough (2). polars_code is dynamic.

    The W48 helper short-circuits via ``is_predictable_via_mirror`` for
    dynamic node types — chat / lineage / Assist must not trigger
    kernel dry-run (D003 + D012). The snapshot stays
    ``schema_status="unknown"``.
    """

    flow = _basic_flow()
    _add_orders_input(flow)
    promise = input_schema.NodePromise(flow_id=flow.flow_id, node_id=2, node_type="polars_code")
    flow.add_node_promise(promise)
    flow.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=flow.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df",
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.get_node(2).name = "polars_passthrough"
    flow.get_node(2).node_schema.predicted_schema = None
    return flow


# --------------------------------------------------------------------------- #
# 1. Subgraph extraction                                                       #
# --------------------------------------------------------------------------- #


def test_extract_subgraph_pinned_only(linear_flow: FlowGraph) -> None:
    snapshot = extract_subgraph(linear_flow, [3], depth=0)
    assert [n.node_id for n in snapshot.nodes] == [3]
    assert snapshot.edges == []
    assert snapshot.pinned_node_ids == [3]
    assert snapshot.nodes[0].is_pinned


def test_extract_subgraph_pinned_plus_one(linear_flow: FlowGraph) -> None:
    snapshot = extract_subgraph(linear_flow, [3], depth=1)
    ids = [n.node_id for n in snapshot.nodes]
    assert ids == [2, 3]
    assert (2, 3) in snapshot.edges


def test_extract_subgraph_full_transitive(linear_flow: FlowGraph) -> None:
    snapshot = extract_subgraph(linear_flow, [3])  # depth=None → full upstream
    ids = [n.node_id for n in snapshot.nodes]
    assert ids == [1, 2, 3], "expected upstream-first topological order"
    assert (1, 2) in snapshot.edges
    assert (2, 3) in snapshot.edges


def test_extract_subgraph_accepts_scalar_pin(linear_flow: FlowGraph) -> None:
    scalar = extract_subgraph(linear_flow, 3, depth=0)
    listed = extract_subgraph(linear_flow, [3], depth=0)
    assert scalar.model_dump() == listed.model_dump()


def test_extract_subgraph_unknown_pin_returns_empty(linear_flow: FlowGraph) -> None:
    snapshot = extract_subgraph(linear_flow, [9999])
    assert snapshot.nodes == []
    assert snapshot.edges == []


# --------------------------------------------------------------------------- #
# 2. Node projection                                                            #
# --------------------------------------------------------------------------- #


def test_snapshot_node_known_schema(linear_flow: FlowGraph) -> None:
    node = linear_flow.get_node(1)
    snap = snapshot_node(node)
    assert snap.schema_status == "known"
    assert snap.schema_columns is not None
    names = [c.name for c in snap.schema_columns]
    assert names == ["order_id", "customer_id", "amount", "region"]


def test_snapshot_node_unknown_schema(linear_flow: FlowGraph) -> None:
    node = linear_flow.get_node(2)
    # Wipe predicted_schema to simulate a cold flow upstream.
    node.node_schema.predicted_schema = None
    # W48: pass resolve_schemas=False to keep this test on the legacy
    # cache-only path. Default-on would auto-resolve this static-schema
    # filter via FlowNode.get_predicted_schema(force=True).
    snap = snapshot_node(node, resolve_schemas=False)
    assert snap.schema_status == "unknown"
    assert snap.schema_columns is None


def test_snapshot_node_serialises_settings(linear_flow: FlowGraph) -> None:
    snap = snapshot_node(linear_flow.get_node(2))
    assert "filter_input" in snap.settings
    assert snap.node_type == "filter"


def test_snapshot_node_samples_off_by_default(linear_flow: FlowGraph) -> None:
    snap = snapshot_node(linear_flow.get_node(1))
    assert all(c.sample is None for c in (snap.schema_columns or []))


# --------------------------------------------------------------------------- #
# 3. Layered system-prompt assembly (D008)                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("surface", list(get_args(SurfaceLiteral)))
def test_assemble_system_prompt_per_surface(surface: SurfaceLiteral) -> None:
    text = assemble_system_prompt(surface)
    assert text, f"expected non-empty system prompt for surface={surface}"
    assert "<!--" not in text and "-->" not in text, f"HTML comment markers leaked into prompt for surface={surface}"


def test_assemble_system_prompt_concatenates_base_and_suffix() -> None:
    text = assemble_system_prompt("explain")
    # Base claims should appear plus the assist suffix.
    assert "Schema-grounded" in text
    assert "Assist mode" in text


def test_assemble_system_prompt_unknown_surface_raises() -> None:
    with pytest.raises(ValueError):
        assemble_system_prompt("not_a_surface")  # type: ignore[arg-type]


def test_surface_to_level_covers_every_surface() -> None:
    """Every advertised surface must map to a valid prompt level."""

    levels = {"assist", "copilot", "planner"}
    for surface in get_args(SurfaceLiteral):
        assert SURFACE_TO_LEVEL[surface] in levels, surface


# --------------------------------------------------------------------------- #
# 4. Token estimator + budget                                                   #
# --------------------------------------------------------------------------- #


def test_estimate_tokens_within_tolerance() -> None:
    text = "a" * 1000
    estimate = estimate_tokens(text)
    assert 200 <= estimate <= 300, "chars/4 heuristic lives near 250 for 1000 chars"


def test_estimate_tokens_empty_returns_zero() -> None:
    assert estimate_tokens("") == 0


def test_surface_budget_known_and_fallback() -> None:
    cmd_k_prompt, cmd_k_response = surface_budget("cmd_k")
    assert cmd_k_prompt > 0 and cmd_k_response > 0
    fallback_prompt, _ = surface_budget("not-a-surface")
    agent_prompt, _ = surface_budget("agent")
    assert fallback_prompt == agent_prompt


def _bigsnap(samples: bool = True) -> SubgraphSnapshot:
    """Snapshot deliberately large enough to overflow the cmd_k budget."""

    cols_per_node = 30
    rows_per_col = 4
    pinned_id = 99
    nodes: list[NodeSnapshot] = []
    edges: list[tuple[int, int]] = []
    for i in range(6):
        nid = i
        edges.append((nid, nid + 1))
        cols = [
            ColumnSnapshot(
                name=f"col_{i}_{j}",
                data_type="String",
                sample=[f"value_{i}_{j}_{k}" for k in range(rows_per_col)] if samples else None,
            )
            for j in range(cols_per_node)
        ]
        nodes.append(
            NodeSnapshot(
                node_id=nid,
                name=f"node_{i}",
                node_type="filter",
                settings={"x": "y"},
                schema_columns=cols,
                schema_status="known",
                is_pinned=False,
            )
        )
    nodes.append(
        NodeSnapshot(
            node_id=pinned_id,
            name="pinned",
            node_type="select",
            settings={"x": "y"},
            schema_columns=[
                ColumnSnapshot(
                    name=f"pin_col_{j}",
                    data_type="String",
                    sample=[f"pin_{j}_{k}" for k in range(rows_per_col)] if samples else None,
                )
                for j in range(cols_per_node)
            ],
            schema_status="known",
            is_pinned=True,
        )
    )
    edges.append((5, pinned_id))
    return SubgraphSnapshot(
        pinned_node_ids=[pinned_id],
        nodes=nodes,
        edges=edges,
        samples_mode="regex",
    )


def test_apply_budget_under_budget_no_change() -> None:
    snapshot = SubgraphSnapshot(
        pinned_node_ids=[1],
        nodes=[
            NodeSnapshot(
                node_id=1,
                name="n",
                node_type="filter",
                settings={},
                schema_columns=[
                    ColumnSnapshot(name="a", data_type="Int"),
                ],
                schema_status="known",
                is_pinned=True,
            )
        ],
        edges=[],
    )
    out, report = apply_budget(snapshot, "agent_complex")
    assert out.model_dump() == snapshot.model_dump()
    assert report.samples_dropped == 0
    assert report.nodes_dropped == 0
    assert report.columns_truncated == 0


def test_apply_budget_drops_samples_first() -> None:
    snapshot = _bigsnap(samples=True)
    out, report = apply_budget(snapshot, "cmd_k")
    # Priority 1: at least one node had its samples stripped before any
    # other truncation kicked in. We don't insist *all* samples are gone
    # because the loop stops as soon as the budget fits — that's the
    # correct behaviour, not over-truncation.
    assert report.samples_dropped > 0
    assert report.truncation_steps[0].startswith("dropped samples")
    # The first node in the upstream-first order should be the first to
    # lose its samples.
    upstream_first = [n for n in out.nodes if not n.is_pinned]
    assert any(all(c.sample is None for c in (n.schema_columns or [])) for n in upstream_first)


def test_apply_budget_drops_upstream_nodes_when_samples_alone_dont_fit() -> None:
    """Push past sample-stripping into node-dropping territory.

    50 upstream nodes × 30 cols guarantees that even after every node's
    samples are gone, the pinned + 50 upstream is still over the cmd_k
    budget — so the node-drop path must trigger.
    """

    cols_per_node = 30
    rows_per_col = 4
    pinned_id = 999
    nodes: list[NodeSnapshot] = [
        NodeSnapshot(
            node_id=i,
            name=f"node_{i}",
            node_type="filter",
            settings={"x": "y"},
            schema_columns=[
                ColumnSnapshot(
                    name=f"col_{i}_{j}",
                    data_type="String",
                    sample=[f"value_{i}_{j}_{k}" for k in range(rows_per_col)],
                )
                for j in range(cols_per_node)
            ],
            schema_status="known",
            is_pinned=False,
        )
        for i in range(50)
    ]
    nodes.append(
        NodeSnapshot(
            node_id=pinned_id,
            name="pinned",
            node_type="select",
            settings={"x": "y"},
            schema_columns=[ColumnSnapshot(name=f"pin_col_{j}", data_type="String") for j in range(cols_per_node)],
            schema_status="known",
            is_pinned=True,
        )
    )
    big = SubgraphSnapshot(
        pinned_node_ids=[pinned_id],
        nodes=nodes,
        edges=[(i, i + 1) for i in range(49)] + [(49, pinned_id)],
        samples_mode="regex",
    )
    out, report = apply_budget(big, "cmd_k")

    assert report.samples_dropped > 0
    assert report.nodes_dropped > 0
    # The pinned node must always survive.
    surviving_pinned = {n.node_id for n in out.nodes if n.is_pinned}
    assert surviving_pinned == {pinned_id}
    # Priority order: samples first, then nodes.
    sample_step = next(
        (i for i, s in enumerate(report.truncation_steps) if s.startswith("dropped samples")),
        None,
    )
    node_step = next(
        (i for i, s in enumerate(report.truncation_steps) if "upstream" in s),
        None,
    )
    assert sample_step is not None and node_step is not None
    assert sample_step < node_step


def test_apply_budget_caps_columns_when_requested() -> None:
    snapshot = _bigsnap(samples=False)
    out, report = apply_budget(snapshot, "agent_complex", max_columns_per_node=5)
    for node in out.nodes:
        # 5 kept + 1 __truncated__ marker
        assert len(node.schema_columns or []) <= 6
        last = (node.schema_columns or [])[-1]
        if len(node.schema_columns or []) == 6:
            assert last.name == "__truncated__"
    assert report.columns_truncated > 0


def test_apply_budget_truncation_step_order() -> None:
    """Tight cmd_k budget → samples → upstream nodes → column-cap fallback."""

    snapshot = _bigsnap(samples=True)
    out, report = apply_budget(snapshot, "cmd_k")
    # The report's truncation_steps list is appended in priority order.
    if len(report.truncation_steps) >= 2:
        assert report.truncation_steps[0].startswith("dropped samples")
    # Column truncation only fires as a last resort when no explicit cap was passed.
    if any("truncated columns" in step for step in report.truncation_steps):
        assert report.columns_truncated > 0


# --------------------------------------------------------------------------- #
# 5. Mentions                                                                   #
# --------------------------------------------------------------------------- #


def test_parse_mentions_single_node() -> None:
    [m] = parse_mentions("look at @node:filter_3 please")
    assert m == Mention(kind="node", ref="filter_3", span=(8, 22))


def test_parse_mentions_multiple_kinds() -> None:
    text = "@flow then @node:orders, also @schema:filter_eu and @selection."
    parsed = parse_mentions(text)
    kinds = [m.kind for m in parsed]
    assert kinds == ["flow", "node", "schema", "selection"]


def test_parse_mentions_quoted_ref_with_space() -> None:
    [m] = parse_mentions('the @node:"my filter" works')
    assert m.kind == "node"
    assert m.ref == "my filter"


def test_parse_mentions_skips_bare_kind_with_payload() -> None:
    """`@flow:foo` and `@selection:foo` are invalid — those are bare kinds."""

    parsed = parse_mentions("@flow:bad and @selection:nope")
    assert parsed == []


def test_parse_mentions_skips_kind_without_ref() -> None:
    parsed = parse_mentions("look at @node and @schema only")
    assert parsed == []


def test_parse_mentions_ignores_email_like_at() -> None:
    parsed = parse_mentions("ping me at user@node:foo for context")
    # `user@node:foo` — `@node` is preceded by a word char, so no mention.
    assert parsed == []


def test_parse_mentions_case_insensitive_kind() -> None:
    [m] = parse_mentions("hey @NODE:Filter_3")
    assert m.kind == "node"
    assert m.ref == "Filter_3"


def test_resolve_mentions_by_name(linear_flow: FlowGraph) -> None:
    parsed = parse_mentions("see @node:filter_eu and @schema:orders")
    resolved = resolve_mentions(parsed, linear_flow)
    by_kind = {r.kind: r for r in resolved}
    assert by_kind["node"].node_ids == (2,)
    assert by_kind["schema"].node_ids == (1,)


def test_resolve_mentions_flow_returns_every_node(linear_flow: FlowGraph) -> None:
    parsed = parse_mentions("@flow")
    [resolved] = resolve_mentions(parsed, linear_flow)
    assert resolved.kind == "flow"
    assert set(resolved.node_ids) == {1, 2, 3}


def test_resolve_mentions_selection_uses_caller_set(linear_flow: FlowGraph) -> None:
    parsed = parse_mentions("@selection")
    [resolved] = resolve_mentions(parsed, linear_flow, selection_node_ids=[2, 3])
    assert resolved.node_ids == (2, 3)


def test_resolve_mentions_unknown_ref_returns_empty(linear_flow: FlowGraph) -> None:
    parsed = parse_mentions("@node:does_not_exist")
    [resolved] = resolve_mentions(parsed, linear_flow)
    assert resolved.node_ids == ()


def test_resolve_mentions_by_id_string(linear_flow: FlowGraph) -> None:
    parsed = parse_mentions("@node:2")
    [resolved] = resolve_mentions(parsed, linear_flow)
    assert resolved.node_ids == (2,)


# --------------------------------------------------------------------------- #
# 6. End-to-end render_prompt_context                                           #
# --------------------------------------------------------------------------- #


def test_render_prompt_context_produces_messages(linear_flow: FlowGraph) -> None:
    ctx = render_prompt_context(linear_flow, [3], surface="explain")
    assert isinstance(ctx, PromptContext)
    assert ctx.surface == "explain"
    assert len(ctx.messages) == 2
    assert ctx.messages[0].role == "system"
    assert ctx.messages[1].role == "user"
    assert "Schema-grounded" in ctx.messages[0].content
    assert "filter_eu" in ctx.messages[1].content
    assert "orders" in ctx.messages[1].content


def test_render_prompt_context_with_string_mentions(linear_flow: FlowGraph) -> None:
    ctx = render_prompt_context(
        linear_flow,
        pinned_node_ids=[],
        surface="agent",
        mentions="please describe @flow",
    )
    assert isinstance(ctx, PromptContext)
    snapshot_ids = sorted(n.node_id for n in ctx.snapshot.nodes)
    assert snapshot_ids == [1, 2, 3]
    # `@flow` brought every node in.
    assert "User request" in ctx.user


def test_render_prompt_context_samples_off_default(linear_flow: FlowGraph) -> None:
    ctx = render_prompt_context(linear_flow, [3], surface="explain")
    for node in ctx.snapshot.nodes:
        for col in node.schema_columns or []:
            assert col.sample is None


def test_render_prompt_context_unknown_schema_propagates(linear_flow: FlowGraph) -> None:
    linear_flow.get_node(1).node_schema.predicted_schema = None
    # W48: pass resolve_schemas=False to keep this test on the legacy
    # cache-only path. Default-on would auto-resolve this manual_input
    # source node and the assertion would flip to "known".
    ctx = render_prompt_context(linear_flow, [1], surface="explain", resolve_schemas=False)
    [snap] = ctx.snapshot.nodes
    assert snap.schema_status == "unknown"
    assert "schema: unknown" in ctx.user


def test_render_user_message_handles_empty_subgraph() -> None:
    snapshot = SubgraphSnapshot(pinned_node_ids=[], nodes=[], edges=[])
    text = render_user_message(snapshot)
    assert "Subgraph" in text
    assert "(empty)" in text


# --------------------------------------------------------------------------- #
# 6b. W48 — prospective schema resolution in W22                                #
# --------------------------------------------------------------------------- #


def test_render_prompt_context_resolves_static_upstream_via_callback(
    cold_static_flow: FlowGraph,
) -> None:
    """W48: a cold static-schema upstream (filter) gets force-resolved
    when render_prompt_context runs with the default resolve_schemas=True."""

    filter_node = cold_static_flow.get_node(2)
    assert filter_node.node_schema.predicted_schema is None  # fixture invariant

    ctx = render_prompt_context(cold_static_flow, [2], surface="explain")

    [filter_snap] = [n for n in ctx.snapshot.nodes if n.node_id == 2]
    assert filter_snap.schema_status == "known"
    assert filter_snap.schema_columns is not None
    column_names = [c.name for c in filter_snap.schema_columns]
    # Filter passes the upstream schema through; expect the manual_input columns.
    assert "order_id" in column_names
    assert "region" in column_names
    # In-place mutation: the filter's predicted_schema cache is now populated
    # so same-session repeat reads hit tier 0 (mirrors predictor.py:255).
    assert filter_node.node_schema.predicted_schema, (
        "expected the W48 helper to populate the filter's predicted_schema cache in place"
    )
    # The rendered user message names the actual columns instead of the
    # "schema: unknown" sentinel.
    assert "schema: unknown" not in ctx.user
    assert "order_id" in ctx.user


def test_render_prompt_context_leaves_dynamic_upstream_unknown(
    cold_dynamic_flow: FlowGraph,
) -> None:
    """W48: dynamic node types (polars_code) stay schema_status='unknown'
    from the chat surface — kernel dry-run is W31-only per D003 + D012."""

    polars_node = cold_dynamic_flow.get_node(2)
    assert polars_node.node_type == "polars_code"

    ctx = render_prompt_context(cold_dynamic_flow, [2], surface="explain")

    [polars_snap] = [n for n in ctx.snapshot.nodes if n.node_id == 2]
    assert polars_snap.schema_status == "unknown"
    assert polars_snap.schema_columns is None
    assert "schema: unknown" in ctx.user
    # The chat-surface helper must not have populated the cache for
    # dynamic types — that path lives in W31's executor.
    assert polars_node.node_schema.predicted_schema in (None, [])


def test_render_prompt_context_in_place_mutation_persists_for_session(
    cold_static_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """W48: the helper fires once per cold node; subsequent
    render_prompt_context calls hit the populated cache (tier 0)."""

    from flowfile_core.ai.context import builder

    call_count = {"n": 0}
    real_helper = builder._resolve_predicted_schema_if_predictable

    def counting_helper(node: Any) -> Any:
        call_count["n"] += 1
        return real_helper(node)

    monkeypatch.setattr(builder, "_resolve_predicted_schema_if_predictable", counting_helper)

    render_prompt_context(cold_static_flow, [2], surface="explain")
    first_count = call_count["n"]
    assert first_count >= 1, "expected the helper to fire at least once on a cold flow"

    render_prompt_context(cold_static_flow, [2], surface="explain")
    assert call_count["n"] == first_count, (
        f"expected zero further helper invocations after the cache is populated, "
        f"got {call_count['n'] - first_count} extra"
    )


def test_render_prompt_context_resolve_schemas_false_preserves_legacy_behaviour(
    cold_static_flow: FlowGraph,
) -> None:
    """W48: opt-out matches today's cache-only semantics — no callbacks
    fired, no in-place mutation, status stays 'unknown' for un-warmed nodes."""

    filter_node = cold_static_flow.get_node(2)
    assert filter_node.node_schema.predicted_schema is None  # fixture invariant

    ctx = render_prompt_context(
        cold_static_flow, [2], surface="explain", resolve_schemas=False,
    )

    [filter_snap] = [n for n in ctx.snapshot.nodes if n.node_id == 2]
    assert filter_snap.schema_status == "unknown"
    assert filter_snap.schema_columns is None
    assert "schema: unknown" in ctx.user
    # No in-place mutation when opted out.
    assert filter_node.node_schema.predicted_schema is None


def test_render_prompt_context_d012_clean(
    cold_static_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """W48: the chat / lineage / Assist context build never invokes
    LazyFrame.collect — D012's worker-only-materialization invariant.

    Patches polars.LazyFrame.collect to raise; render_prompt_context must
    still complete on a cold static flow.
    """

    import polars as pl

    def _refuse_collect(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "D012 violation — render_prompt_context invoked LazyFrame.collect"
        )

    monkeypatch.setattr(pl.LazyFrame, "collect", _refuse_collect)

    ctx = render_prompt_context(cold_static_flow, [2], surface="explain")
    assert isinstance(ctx, PromptContext)
    [filter_snap] = [n for n in ctx.snapshot.nodes if n.node_id == 2]
    # Sanity: the resolution still succeeded — the test is meaningful only
    # when the static node's schema lands as "known" without .collect().
    assert filter_snap.schema_status == "known"


# --------------------------------------------------------------------------- #
# 7. Lazy-import contract                                                       #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_import_for_context() -> None:
    """Importing ``flowfile_core.ai.context.builder`` must not pull litellm.

    Mirrors the W11 / W13 / W15 pattern.
    """

    cleared: dict[str, Any] = {}
    for mod_name in list(sys.modules):
        if mod_name == "litellm" or mod_name.startswith("litellm."):
            cleared[mod_name] = sys.modules.pop(mod_name)
        elif mod_name in (
            "flowfile_core.ai.context",
            "flowfile_core.ai.context.builder",
            "flowfile_core.ai.context.budget",
            "flowfile_core.ai.context.mentions",
        ):
            cleared[mod_name] = sys.modules.pop(mod_name)
    try:
        import flowfile_core.ai.context.builder  # noqa: F401

        assert (
            "litellm" not in sys.modules
        ), "Importing flowfile_core.ai.context.builder must not eagerly import litellm"
    finally:
        for mod_name, mod in cleared.items():
            sys.modules[mod_name] = mod


# --------------------------------------------------------------------------- #
# 8. BudgetReport dataclass defaults                                            #
# --------------------------------------------------------------------------- #


def test_budget_report_dataclass_defaults() -> None:
    report = BudgetReport(
        surface="cmd_k",
        prompt_budget=100,
        response_budget=50,
        estimated_input_tokens=20,
    )
    assert report.truncation_steps == []
    assert report.samples_dropped == 0
    assert report.nodes_dropped == 0
    assert report.columns_truncated == 0
