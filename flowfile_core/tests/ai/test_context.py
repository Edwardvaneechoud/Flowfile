"""Context builder unit tests.

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

# Test fixtures


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

    Used by the tier-1 prospective-schema tests. ``filter`` is
    classified ``static`` (predictable via the mirror-graph path), so
    the helper should resolve its schema via the production
    schema-prediction path on the next ``render_prompt_context`` call.
    """

    flow = _basic_flow()
    _add_orders_input(flow)
    _add_filter(flow)
    # Be explicit: ensure the filter's cache is empty so the helper
    # is the only thing that can populate it during the test.
    flow.get_node(2).node_schema.predicted_schema = None
    return flow


@pytest.fixture
def cold_dynamic_flow() -> FlowGraph:
    """orders (1) → polars_passthrough (2). polars_code is dynamic.

    The helper short-circuits via ``is_predictable_via_mirror`` for
    dynamic node types — chat / lineage / Assist must not trigger
    kernel dry-run. The snapshot stays
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


# 1. Subgraph extraction


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


# 2. Node projection


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
    # pass resolve_schemas=False to keep this test on the legacy
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


# 3. Layered system-prompt assembly


@pytest.mark.parametrize("surface", list(get_args(SurfaceLiteral)))
def test_assemble_system_prompt_per_surface(surface: SurfaceLiteral) -> None:
    text = assemble_system_prompt(surface)
    assert text, f"expected non-empty system prompt for surface={surface}"
    assert "<!--" not in text and "-->" not in text, f"HTML comment markers leaked into prompt for surface={surface}"


def test_assemble_system_prompt_concatenates_base_and_suffix() -> None:
    text = assemble_system_prompt("explain")
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


# 4. Token estimator + budget


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


# 5. Mentions


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


# 6. End-to-end render_prompt_context


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
        surface="agent_complex",
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
    # pass resolve_schemas=False to keep this test on the legacy
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


# 6b. — prospective schema resolution in


def test_render_prompt_context_resolves_static_upstream_via_callback(
    cold_static_flow: FlowGraph,
) -> None:
    """a cold static-schema upstream (filter) gets force-resolved
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
    assert (
        filter_node.node_schema.predicted_schema
    ), "expected the helper to populate the filter's predicted_schema cache in place"
    # The rendered user message names the actual columns instead of the
    # "schema: unknown" sentinel.
    assert "schema: unknown" not in ctx.user
    assert "order_id" in ctx.user


def test_render_prompt_context_leaves_dynamic_upstream_unknown(
    cold_dynamic_flow: FlowGraph,
) -> None:
    """dynamic node types (polars_code) stay schema_status='unknown'
    from the chat surface — kernel dry-run is-only per +."""

    polars_node = cold_dynamic_flow.get_node(2)
    assert polars_node.node_type == "polars_code"

    ctx = render_prompt_context(cold_dynamic_flow, [2], surface="explain")

    [polars_snap] = [n for n in ctx.snapshot.nodes if n.node_id == 2]
    assert polars_snap.schema_status == "unknown"
    assert polars_snap.schema_columns is None
    assert "schema: unknown" in ctx.user
    # The chat-surface helper must not have populated the cache for
    # dynamic types — that path lives in's executor.
    assert polars_node.node_schema.predicted_schema in (None, [])


def test_render_prompt_context_in_place_mutation_persists_for_session(
    cold_static_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """the helper fires once per cold node; subsequent
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
    """opt-out matches today's cache-only semantics — no callbacks
    fired, no in-place mutation, status stays 'unknown' for un-warmed nodes."""

    filter_node = cold_static_flow.get_node(2)
    assert filter_node.node_schema.predicted_schema is None  # fixture invariant

    ctx = render_prompt_context(
        cold_static_flow,
        [2],
        surface="explain",
        resolve_schemas=False,
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
    """the chat / lineage / Assist context build never invokes
    LazyFrame.collect's worker-only-materialization invariant.

    Patches polars.LazyFrame.collect to raise; render_prompt_context must
    still complete on a cold static flow.
    """

    import polars as pl

    def _refuse_collect(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("D012 violation — render_prompt_context invoked LazyFrame.collect")

    monkeypatch.setattr(pl.LazyFrame, "collect", _refuse_collect)

    ctx = render_prompt_context(cold_static_flow, [2], surface="explain")
    assert isinstance(ctx, PromptContext)
    [filter_snap] = [n for n in ctx.snapshot.nodes if n.node_id == 2]
    # Sanity: the resolution still succeeded — the test is meaningful only
    # when the static node's schema lands as "known" without .collect().
    assert filter_snap.schema_status == "known"


# 7. Lazy-import contract


def test_lazy_litellm_import_for_context() -> None:
    """Importing ``flowfile_core.ai.context.builder`` must not pull litellm.

    Mirrors the / / pattern.
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


# 8. BudgetReport dataclass defaults


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


# per-node-type catalog block in assemble_system_prompt


_W56_CATALOG_HEADER = "## Tool catalog"

# Three representative node-type tool headings the agent surface must show
# (AC2). Picked across categories so a regression in one category doesn't
# silently pass.
_W56_REPRESENTATIVE_NODE_TOOLS = (
    "### flowfile.graph.add_filter",
    "### flowfile.graph.add_join",
    "### flowfile.graph.add_group_by",
)


@pytest.mark.parametrize("surface", ["agent_complex"])
def test_w56_agent_surfaces_include_catalog_block(surface: SurfaceLiteral) -> None:
    """AC2 — agent_complex prompt includes the catalog header plus
    several representative node-type sections so the model sees
    narrative grounding for each tool. (W71 — legacy ``"agent"``
    surface removed; ``agent_staged`` only renders the catalog at the
    ``pick_type`` stage, which is exercised by ``test_planner_staged.py``.)"""
    text = assemble_system_prompt(surface)
    assert _W56_CATALOG_HEADER in text, f"catalog header missing for {surface}"
    for heading in _W56_REPRESENTATIVE_NODE_TOOLS:
        assert heading in text, f"{heading} missing from {surface} prompt"


def test_w56_cmd_k_only_includes_preset_tools_in_catalog() -> None:
    """AC3 — cmd_k narrows the catalog to its preset's tools."""
    from flowfile_core.ai.tools.registry import SURFACE_PRESETS

    text = assemble_system_prompt("cmd_k")
    assert _W56_CATALOG_HEADER in text

    cmd_k_preset = SURFACE_PRESETS["cmd_k"]
    # Tools in the preset must appear; tools outside it must not.
    for tool_name in cmd_k_preset:
        assert f"### {tool_name}" in text, f"cmd_k preset tool {tool_name} missing from catalog block"
    # Spot-check a few tools that are *not* in cmd_k's preset:
    assert "### flowfile.graph.add_join" not in text
    assert "### flowfile.graph.add_group_by" not in text
    assert "### flowfile.codegen.generate_python_script" not in text


def test_w56_ghost_node_only_includes_preset_tools_in_catalog() -> None:
    """AC3 — ghost_node narrows the catalog to its preset's tools."""
    from flowfile_core.ai.tools.registry import SURFACE_PRESETS

    text = assemble_system_prompt("ghost_node")
    assert _W56_CATALOG_HEADER in text

    ghost_preset = SURFACE_PRESETS["ghost_node"]
    for tool_name in ghost_preset:
        assert f"### {tool_name}" in text, f"ghost_node preset tool {tool_name} missing"
    # ghost_node carries common transforms, so add_join *is* in its preset;
    # use tools that genuinely aren't.
    assert "### flowfile.graph.add_pivot" not in text
    assert "### flowfile.graph.add_train_model" not in text


@pytest.mark.parametrize("surface", ["explain", "lineage", "docgen", "settings_autocomplete"])
def test_w56_read_only_surfaces_do_not_get_tool_catalog(surface: SurfaceLiteral) -> None:
    """AC4 — read-only surfaces never see the agent-shaped Tool catalog
    block (it'd be misleading — they can't call tools)."""
    text = assemble_system_prompt(surface)
    assert _W56_CATALOG_HEADER not in text, f"{surface} should not include the agent-shaped Tool catalog block"


# v2 — node-reference block on read-only / advisory surfaces
#
# Motivated by the live transcript where the chat surface (uses
# surface="explain" → assist level) hallucinated a non-existent
# "transform node" because it had no Flowfile vocabulary in the prompt.
# Read-only surfaces don't get the agent-shaped Tool catalog (above), but
# they DO get a separate user-shaped "Flowfile node reference" block built
# from each ToolSpec.user_instructions so the chat answer cites real
# palette labels and settings field names.

_W56_NODE_REFERENCE_HEADER = "## Flowfile node reference"


@pytest.mark.parametrize("surface", ["explain", "lineage", "docgen"])
def test_w56_advisory_surfaces_include_node_reference_block(surface: SurfaceLiteral) -> None:
    """v2 — explain / lineage / docgen carry a node-reference block.

    The chat surface (surface="explain") was hallucinating UI elements
    because it had zero Flowfile vocabulary in its prompt. The node
    reference block fixes this with real palette labels + settings
    field names so the model can answer "how do I X" correctly.
    """
    text = assemble_system_prompt(surface)
    assert _W56_NODE_REFERENCE_HEADER in text, f"{surface} should include the user-shaped node reference block"


@pytest.mark.parametrize("surface", ["explain", "lineage", "docgen"])
def test_w56_node_reference_cites_real_palette_labels(surface: SurfaceLiteral) -> None:
    """v2 — node-reference block cites real palette labels from nodes.py.

    Specifically asserts the user-transcript-relevant labels appear:
    'Group by' (the actual palette name for group_by), 'Filter data',
    'Aggregations' (the sidebar section). Without these, chat answers
    invent names like 'transform node'.
    """
    text = assemble_system_prompt(surface)
    # Real palette labels the chat answer should cite:
    assert "'Group by'" in text or '"Group by"' in text, "real 'Group by' palette label missing"
    assert "'Filter data'" in text or '"Filter data"' in text, "real 'Filter data' palette label missing"
    # Real sidebar section the chat answer should cite:
    assert "'Aggregations'" in text or '"Aggregations"' in text, "real 'Aggregations' sidebar label missing"
    # The bug from the live transcript was the model saying "transform
    # node" — a name that doesn't exist. The prompt's `_render_node_reference`
    # explicitly tells the model NOT to say "transform node" (that's good —
    # warning text shouldn't be removed). But the prompt should NOT positively
    # describe a "transform node" anywhere as if it existed. We allow at most
    # one occurrence (the explicit don't-say-this warning).
    matches = text.lower().count("transform node")
    assert matches <= 1, (
        f"'transform node' appears {matches} times in the prompt — at most "
        f"one occurrence is allowed (the explicit warning); more suggests a "
        f"node-doc entry is positively describing a phantom node type."
    )


def test_w56_node_reference_includes_user_transcript_repro() -> None:
    """v2 — the customers-per-city worked example from the user
    transcript is in the prompt verbatim (the example that motivated
    the v2 widening).
    """
    text = assemble_system_prompt("explain")
    # The exact phrase from group_by's user_instructions.
    assert "customers per city" in text.lower(), "the customers-per-city worked example is missing"
    # The settings field labels the chat answer should cite when
    # describing how to configure group_by:
    assert "Field" in text and "Action" in text and "Output Field Name" in text


def test_w56_node_reference_omits_ops_tools() -> None:
    """v2 — read-only surfaces only see node-type tools, not ops.

    The user can't call graph_ops / schema_ops / codegen_ops / meta from
    the canvas; surfacing them in the chat-facing reference would only
    confuse the model into describing tool calls in chat answers.
    """
    text = assemble_system_prompt("explain")
    assert "flowfile.graph.connect" not in text
    assert "flowfile.graph.delete_node" not in text
    assert "flowfile.schema.read_node_schema" not in text
    assert "flowfile.codegen.generate_polars_code" not in text
    assert "flowfile.meta.pick_category" not in text


def test_w56_settings_autocomplete_skips_both_blocks() -> None:
    """v2 — settings_autocomplete is constrained-JSON output only;
    it doesn't need either narrative block."""
    text = assemble_system_prompt("settings_autocomplete")
    assert _W56_CATALOG_HEADER not in text
    assert _W56_NODE_REFERENCE_HEADER not in text


def test_w56_node_reference_sorted_for_cache_stability() -> None:
    """v2 — node-reference headings are alphabetical (cache hygiene)."""
    text = assemble_system_prompt("explain")
    block_start = text.find(_W56_NODE_REFERENCE_HEADER)
    assert block_start >= 0
    block = text[block_start:]
    # Headings are bare node_type names (e.g. "### group_by"), not the
    # dotted MCP names the agent surface uses.
    headings = [line for line in block.splitlines() if line.startswith("### ") and not line.startswith("### flowfile.")]
    assert len(headings) >= 30, f"expected most node types in the reference, got {len(headings)}"
    assert headings == sorted(headings), "node-reference headings out of order — prompt cache will thrash"


def test_w56_explain_surface_token_budget_reasonable() -> None:
    """v2 — the chat / advisory prompt stays within a sensible bound.

    The chat surface combines base.md + assist.md + the node reference;
    expect ~5-8 K tokens total. We cap at 12 K (chars/4) to flag any
    bloat regression early — if a node's user_instructions balloons,
    this test fires.
    """
    text = assemble_system_prompt("explain")
    estimated_tokens = len(text) // 4
    assert estimated_tokens <= 12_000, (
        f"explain prompt is {estimated_tokens} tokens; cap is 12,000. "
        "Investigate which user_instructions entry grew."
    )


def test_w56_agent_surface_token_budget_under_70_pct_of_agent_budget() -> None:
    """AC6 — full-catalog agent prompt fits inside the per-call budget. — legacy ``"agent"`` surface removed; the equivalent
    full-catalog prompt now lives on ``agent_complex`` (96K budget).
    The static prompt has to carry the catalog plus the layered base +
    planner suffix and still leave room for the per-call user message
    and the model's reply on the wire — assert it's under 70% of the
    96K complex budget.
    """
    text = assemble_system_prompt("agent_complex")
    # Same chars/4 estimator the budget module uses (no tiktoken pull).
    estimated_tokens = len(text) // 4
    cap = int(96_000 * 0.7)
    assert estimated_tokens <= cap, (
        f"agent_complex system prompt is {estimated_tokens} tokens (chars/4); "
        f"AC6 caps it at {cap} (70% of 96K)"
    )


def test_w56_catalog_block_does_not_leak_html_comments() -> None:
    """the catalog block uses plain markdown only; no HTML comment markers."""
    text = assemble_system_prompt("agent_complex")
    # The HTML-comment guard from test_assemble_system_prompt_per_surface already
    # covers the layered prompts, but the catalog block is generated fresh —
    # repeat the assertion here so a regression in node_docs.py is caught.
    assert "<!--" not in text
    assert "-->" not in text


def test_w56_catalog_block_renders_descriptions_in_stable_order() -> None:
    """catalog ordering is stable (alphabetical by tool name).

    Stable ordering means the prompt-cache hash is deterministic across
    process restarts, which matters for any caching layer downstream.
    """
    text_a = assemble_system_prompt("agent_complex")
    text_b = assemble_system_prompt("agent_complex")
    assert text_a == text_b

    # Walk the headings in document order and verify they are sorted.
    headings = [line for line in text_a.splitlines() if line.startswith("### flowfile.")]
    assert headings == sorted(headings), "catalog headings out of order — prompt cache will thrash"


# sample fetch must use cached endpoint, not _predicted_data_getter


def test_render_prompt_context_default_samples_mode_is_off(linear_flow: FlowGraph) -> None:
    """calling ``render_prompt_context`` without an explicit
    ``samples_mode`` must produce snapshots whose columns carry no ``sample``
    values. Pins the dev-default flip from ``"regex"`` to ``"off"`` per.
    """

    ctx = render_prompt_context(linear_flow, [3], surface="explain")
    assert ctx.snapshot.samples_mode == "off"
    for node in ctx.snapshot.nodes:
        for col in node.schema_columns or []:
            assert col.sample is None, f"unexpected sample on {node.node_id}.{col.name}: {col.sample!r}"


def test_render_prompt_context_never_invokes_predicted_data_getter(
    linear_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """even with samples_mode="regex" (opt-in), the sample-fetch path
    must read from the cached :meth:`FlowNode.get_table_example` endpoint
    and never call ``_predicted_data_getter``. previously, ``_safe_get_sample_data``
    invoked the getter unconditionally — running the node's ``_function``
    on materialized upstream data and triggering ``.collect()`` for any
    node whose function does so (random_split, train_model).
    """

    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

    invocations: list[int | str] = []
    original_getter = FlowNode._predicted_data_getter

    def _spy_getter(self: FlowNode):
        invocations.append(self.node_id)
        return original_getter(self)

    monkeypatch.setattr(FlowNode, "_predicted_data_getter", _spy_getter)
    render_prompt_context(linear_flow, [3], surface="explain", samples_mode="regex", sample_rows=5)
    assert invocations == [], f"_predicted_data_getter must not fire from chat preamble; got {invocations}"


def test_safe_get_sample_data_returns_none_when_node_not_run(linear_flow: FlowGraph) -> None:
    """:func:`_safe_get_sample_data` returns ``None`` for nodes that
    haven't been run with their current setup. Cached samples only —
    no node execution from the chat path.
    """

    from flowfile_core.ai.context.builder import _safe_get_sample_data

    node = linear_flow.get_node(3)
    # Linear-flow fixture only runs schema prediction; has_run_with_current_setup is False.
    assert not getattr(node.node_stats, "has_run_with_current_setup", False)
    assert _safe_get_sample_data(node, 5) is None


def test_safe_get_sample_data_reads_from_example_data_generator(
    linear_flow: FlowGraph,
) -> None:
    """when ``node.results.example_data_generator`` is populated (the
    post-run cache surface), :func:`_safe_get_sample_data` returns the
    cached rows up to ``n``. Mirrors what ``GET /node/data`` does.
    """

    from flowfile_core.ai.context.builder import _safe_get_sample_data

    node = linear_flow.get_node(1)

    class _FakeArrowTable:
        def __init__(self, rows: list[dict[str, Any]]) -> None:
            self._rows = rows

        def to_pylist(self) -> list[dict[str, Any]]:
            return self._rows

    fake_rows = [
        {"order_id": 1, "customer_id": 10, "amount": 100.0, "region": "EU"},
        {"order_id": 2, "customer_id": 20, "amount": 200.0, "region": "US"},
        {"order_id": 3, "customer_id": 30, "amount": 50.0, "region": "EU"},
    ]
    node.results.example_data_generator = lambda: _FakeArrowTable(fake_rows)
    node.node_stats.has_run_with_current_setup = True
    node.node_stats.has_completed_last_run = True

    rows = _safe_get_sample_data(node, 2)
    assert rows is not None
    assert len(rows) == 2
    assert rows[0]["order_id"] == 1
    assert rows[1]["order_id"] == 2


def test_render_prompt_context_no_lazyframe_collect_on_random_split_upstream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """extends ``test_render_prompt_context_d012_clean`` to the actual
    hang flow. Build a ``read → random_split`` graph; monkeypatch
    ``pl.LazyFrame.collect`` to raise; run ``render_prompt_context``. previously
    the sample-fetch path called ``random_split._function(...)`` which
    invoked ``.collect()`` directly — this test would fail.
    """

    import polars as pl

    flow = _basic_flow()
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="customer_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="churned", data_type="String"),
            ],
            data=[[1, 2, 3, 4], ["yes", "no", "yes", "no"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(1).name = "customers"

    split_settings = input_schema.NodeRandomSplit(
        flow_id=flow.flow_id,
        node_id=2,
        depending_on_id=1,
        splits=[
            input_schema.RandomSplitGroup(name="train", percentage=80.0),
            input_schema.RandomSplitGroup(name="test", percentage=20.0),
        ],
        seed=42,
    )
    flow.add_random_split(split_settings)
    add_connection(
        flow,
        node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2),
    )
    flow.get_node(2).name = "split_train_test"
    for node in flow.nodes:
        node.get_predicted_schema()

    def _refuse_collect(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("D012 violation — render_prompt_context invoked LazyFrame.collect")

    monkeypatch.setattr(pl.LazyFrame, "collect", _refuse_collect)

    ctx = render_prompt_context(flow, [2], surface="explain")
    assert isinstance(ctx, PromptContext)
    # Sanity: the snapshot still rendered both nodes.
    snapshot_ids = sorted(n.node_id for n in ctx.snapshot.nodes)
    assert snapshot_ids == [1, 2]


# — palette label vs node_type disambiguation


def test_pick_type_prompt_warns_against_palette_labels() -> None:
    """at the ``pick_type`` stage of agent_staged the
    catalog block must be followed by a do/don't section listing the
    palette-label vs node_type confusions that have caused real
    failures (sort_data, select_data, etc.). Without it small models
    snake-case the palette label and submit it as the enum value,
    burning a retry on every type-pick.
    """
    text = assemble_system_prompt("agent_staged", stage="pick_type")
    assert "## Important: enum is" in text, (
        "v1.12B disambiguation block missing from pick_type system prompt"
    )
    # Two of the failure modes the user dogfooded — must be listed.
    assert "sort_data" in text and "``sort``" in text
    assert "select_data" in text and "``select``" in text


def test_classify_stage_includes_multi_step_discipline_section() -> None:
    """the classify-stage prompt has a *"Multi-step
    discipline"* section that tells the LLM not to terminate with
    plan steps remaining. Specifically calls out the *"insert a
    node mid-flow without rewiring"* failure mode that bit the
    an earlier dogfood (agent added unique but stopped before
    reconnecting downstream nodes).
    """
    text = assemble_system_prompt("agent_staged", stage="classify")
    assert "## Multi-step discipline" in text, (
        "v2.9B: classify prompt missing the multi-step discipline section"
    )
    text_lower = text.lower()
    # The "don't pick `other` until all steps done" rule.
    assert 'op_kind="other"' in text or "op_kind=\"other\"" in text
    assert "all steps" in text_lower or "until they're all" in text_lower
    # The mid-flow rewire warning explicitly.
    assert "rewir" in text_lower
    assert "dangling" in text_lower or "dangl" in text_lower


def test_classify_stage_includes_re_add_prevention_section() -> None:
    """classify prompt warns against re-adding a
    same-type node already staged in the session. Direct response
    to the 2026-05-09 cross_join cascade dogfood (LLM re-staged
    cross_join after misreading a predictor warning).
    """
    text = assemble_system_prompt("agent_staged", stage="classify")
    text_lower = text.lower()
    # Sub-block heading anchors the insertion.
    assert "re-adding a node that's already staged" in text_lower
    # Cross_join worked example — specific to this dogfood.
    assert "cross_join cascade" in text_lower
    assert "add_cross_join" in text_lower
    # The "modify on the existing node" rule.
    assert 'op_kind="modify"' in text
    # Anchor for the predictor-warning-misread paragraph.
    assert "re-staging duplicates" in text_lower


def test_classify_stage_now_includes_palette_disambiguation() -> None:
    """supersedes the *"only at pick_type"*
    posture. Some models (qwen3-vl-32b on 2026-05-08) bypass
    classify_intent and emit pick_node_type directly at the classify
    stage. The classify-stage prompt must therefore carry the
    palette-label disambiguation too so the warning reaches the LLM
    on bypass paths.
    """
    text = assemble_system_prompt("agent_staged", stage="classify")
    assert "## Important: enum is" in text, (
        "v1.14A.3: classify stage system prompt must include the "
        "palette-label disambiguation block (bypass-path defense)"
    )
    assert "sort_data" in text and "``sort``" in text


# — formula description + on-demand function list


def test_formula_node_docs_uses_flowfile_expression_syntax() -> None:
    """the formula long description and chat-mode
    instructions must reflect the *actual* Flowfile expression
    syntax (SQL-style ``[column_name]``) instead of the previous
    Polars-Python pseudo-code (``pl.col('first')``). Mentions of
    ``pl.col`` are tolerated only when used as a *negative* example
    (i.e. *"NOT ``pl.col``"*); the docstring must point at
    ``polars_code`` as the alternative for raw Polars work.
    """
    from flowfile_core.ai.tools.node_docs import (
        NODE_LONG_DESCRIPTIONS,
        NODE_USER_INSTRUCTIONS,
    )

    long_desc = NODE_LONG_DESCRIPTIONS["formula"]
    user_inst = NODE_USER_INSTRUCTIONS["formula"]
    for body, label in ((long_desc, "long_description"), (user_inst, "user_instruction")):
        assert "[" in body and "]" in body, (
            f"formula {label} should reference SQL-style [column] syntax; got: {body!r}"
        )
        # If pl.col appears it must be contrasted with the SQL-style
        # alternative (negation context). Bare advocacy is the failure
        # we're guarding against.
        if "pl.col" in body:
            assert "not" in body.lower() or "instead of" in body.lower(), (
                f"formula {label} mentions pl.col without contrasting "
                f"against the actual [column_name] syntax; got: {body!r}"
            )
        # The cross-reference to polars_code is the user-facing escape
        # hatch for raw Polars work — must be present so the LLM has
        # somewhere to route requests this tool can't satisfy. Chat-mode
        # instructions may use the friendly palette label ("Polars
        # code"); agent docs reference the registered node_type id.
        polars_ref_present = "polars_code" in body or "Polars code" in body
        assert polars_ref_present, (
            f"formula {label} must cross-reference polars_code as the "
            f"raw-Polars alternative; got: {body!r}"
        )


def test_formula_long_description_leads_with_row_wise_constraint() -> None:
    """the formula long_description must lead with the
    *"ROW-WISE ONLY — CANNOT aggregate"* assertion. Mid-paragraph
    guidance is the wrong place for a hard constraint; small models
    read past it and pick formula for count/sum/avg requests.
    """
    from flowfile_core.ai.tools.node_docs import (
        NODE_LONG_DESCRIPTIONS,
        NODE_USER_INSTRUCTIONS,
    )

    long_desc = NODE_LONG_DESCRIPTIONS["formula"]
    user_inst = NODE_USER_INSTRUCTIONS["formula"]
    # Anchor the constraint near the start so it's the first thing
    # the LLM reads — both surfaces (catalog + chat-mode) get it.
    for body, label in ((long_desc, "long_description"), (user_inst, "user_instruction")):
        head = body[:200]  # generous lead window
        head_lower = head.lower()
        assert "row-wise" in head_lower, (
            f"formula {label} must lead with the row-wise constraint; "
            f"got head: {head!r}"
        )
        assert "cannot" in head_lower, (
            f"formula {label} lead must explicitly say 'CANNOT'; got head: {head!r}"
        )
        assert "aggregat" in head_lower, (
            f"formula {label} lead must mention aggregation; got head: {head!r}"
        )


def test_filter_long_description_teaches_string_typed_value() -> None:
    """the filter agent docstring must spell out that
    ``basic_filter.value`` is a JSON string (even for numeric
    comparisons) and that ``operator`` names are snake_case, not
    symbols. Both points were missing and led to the "got int"
    rejection loop on ``email_count > 1``: the LLM emitted ``"value":
    "1"`` correctly, but the executor's unwrap heuristic was eagerly
    parsing digit-strings into ints. Even with the unwrap fix in place,
    the prompt-side nudge prevents the FIRST attempt (which previously
    sent a raw int) from being wrong.
    """
    from flowfile_core.ai.tools.node_docs import NODE_LONG_DESCRIPTIONS

    desc = NODE_LONG_DESCRIPTIONS["filter"]

    # Operator vocabulary nudge — explicit snake_case + negative
    # example. ``BasicFilter.operator`` accepts ``FilterOperator | str``
    # so the symbol form passes type-check, but ``from_symbol`` is a
    # separate conversion path; smaller models drift toward symbols
    # (the failing chat reply suggested "Operator: >").
    assert "greater_than" in desc, (
        "filter long_description must list snake_case operator names "
        "so the agent doesn't drift to '>' / '<' / '==' shapes."
    )
    assert "do NOT" in desc, (
        "filter long_description must include an explicit negative "
        "example warning against symbol-form operators."
    )

    # String-typing imperative — JSON Schema's ``"type": "string"`` is
    # not enough on its own; smaller models attend prose more reliably.
    assert "ALWAYS a JSON string" in desc, (
        "filter long_description must explicitly say `value` is a "
        "JSON string, even for numeric comparisons."
    )

    # Basic-mode worked example present (the existing entry only had
    # an advanced-mode example, which left basic-mode shape implicit).
    assert '"mode": "basic"' in desc, (
        "filter long_description must include a basic-mode worked "
        "example, not only the advanced-mode one."
    )
    assert '"value": "1"' in desc, (
        "filter long_description's basic-mode example must show the "
        "numeric-as-string shape (`\"value\": \"1\"`)."
    )


def test_pick_type_prompt_includes_tool_selection_rules() -> None:
    """the agent_staged ``pick_type`` system prompt
    carries the explicit do/don't list naming the four commonly-
    confused choices (``record_count`` / ``group_by`` / ``formula`` /
    ``polars_code``) so small models can read the rules instead of
    inferring them from prose.
    """
    text = assemble_system_prompt("agent_staged", stage="pick_type")
    assert "## Tool selection rules" in text, (
        "v1.13A: pick_type system prompt missing the tool-selection-rules block"
    )
    for label in ("record_count", "group_by", "formula", "polars_code"):
        assert label in text, f"v1.13A rules block missing reference to {label!r}"
    # The "row-wise only" assertion must appear AGAIN here (the rules
    # block reinforces the long_description's lead constraint).
    assert "row-wise" in text.lower(), (
        "v1.13A rules block must reiterate that formula is row-wise only"
    )


def test_pick_node_type_spec_description_carries_disambiguation() -> None:
    """the function-calling spec for
    ``flowfile.meta.pick_node_type`` (description AND the
    ``node_type`` parameter description) carries the palette-label
    vs node_type confusion list. Decoders attend to tool-spec text
    far more than to prompt prose, so this catches the bypass case
    (LLM emits ``pick_node_type`` at the classify stage, where the
    catalog-side disambiguation isn't rendered).
    """
    from flowfile_core.ai.tools.meta_ops import (
        META_OPS_TOOLS,
        PICK_NODE_TYPE_TOOL_NAME,
    )

    spec = next(s for s in META_OPS_TOOLS if s.name == PICK_NODE_TYPE_TOOL_NAME)
    # Headline disambiguation note in the tool description.
    assert "snake-case the palette label" in spec.description.lower() or "palette label" in spec.description.lower(), (
        f"v1.14A.1: pick_node_type description missing palette-label warning; got: {spec.description!r}"
    )
    # Detailed do/don't list inside the node_type parameter description.
    nt_desc = spec.parameters["properties"]["node_type"]["description"]
    assert "Common confusions" in nt_desc, nt_desc
    # Two of the cases the user dogfooded.
    assert "sort_data" in nt_desc and "`sort`" in nt_desc, nt_desc
    assert "select_data" in nt_desc and "`select`" in nt_desc, nt_desc


def test_catalog_headers_inline_node_type_for_add_tools() -> None:
    """every ``flowfile.graph.add_<type>`` catalog entry
    renders its snake_case node_type beside the heading so the LLM
    sees the enum value at every entry, not only in the trailing
    disambiguation block.
    """
    text = assemble_system_prompt("agent_staged", stage="pick_type")
    # Spot-check four common confusions: sort, select, unique, sample.
    for nt in ("sort", "select", "unique", "sample"):
        marker = f"### flowfile.graph.add_{nt}  (node_type: `{nt}`)"
        assert marker in text, (
            f"v1.14A.2: catalog header for {nt!r} missing inline node_type marker. "
            f"Expected to find: {marker!r}"
        )


def test_pick_upstream_spec_requires_right_input_for_join_shaped_types() -> None:
    """for join-shaped node types (join, cross_join,
    fuzzy_match) the ``right_input_node_id`` field on
    ``pick_upstream`` is REQUIRED, not optional. Stops the LLM from
    staging cross_join with only one upstream wire connected.
    """
    from flowfile_core.ai.tools.meta_ops import build_pick_upstream_spec

    # Non-join: stays optional.
    spec_filter = build_pick_upstream_spec(
        live_node_ids=[1, 2, 3], picked_node_type="filter"
    )
    required_filter = spec_filter.parameters.get("required", [])
    assert "right_input_node_id" not in required_filter, required_filter
    # The type still allows null.
    rtype = spec_filter.parameters["properties"]["right_input_node_id"]["type"]
    assert "null" in rtype, rtype

    # Each join-shaped type: required AND no null in the type union.
    for nt in ("join", "cross_join", "fuzzy_match"):
        spec = build_pick_upstream_spec(
            live_node_ids=[1, 2, 3], picked_node_type=nt
        )
        required = spec.parameters.get("required", [])
        assert "right_input_node_id" in required, (
            f"v1.14B: right_input_node_id must be required for {nt!r}; "
            f"got required={required}"
        )
        rfield = spec.parameters["properties"]["right_input_node_id"]
        assert rfield["type"] == "integer", (
            f"v1.14B: right_input_node_id type for {nt!r} must be plain "
            f"integer (no null); got {rfield['type']!r}"
        )

    # Unknown / None picked_node_type: backward-compatible (optional).
    spec_unknown = build_pick_upstream_spec(live_node_ids=[1, 2])
    required_unknown = spec_unknown.parameters.get("required", [])
    assert "right_input_node_id" not in required_unknown


def test_pick_upstream_spec_join_shaped_uses_left_right_scalars() -> None:
    """for join-shaped node types the pick_upstream spec
    exposes a SYMMETRIC scalar pair (``left_input_node_id`` +
    ``right_input_node_id``), both required, and drops the
    ``upstream_node_ids`` list field entirely. This removes the
    asymmetric-shape confusion that previously forced the LLM to
    learn (list+scalar for what it intuitively models as two
    equivalent inputs).
    """
    from flowfile_core.ai.tools.meta_ops import build_pick_upstream_spec

    for nt in ("join", "cross_join", "fuzzy_match"):
        spec = build_pick_upstream_spec(
            live_node_ids=[1, 2, 3], picked_node_type=nt
        )
        props = spec.parameters["properties"]
        # Both LEFT and RIGHT are top-level scalar integer fields.
        assert "left_input_node_id" in props, f"{nt}: missing left_input_node_id"
        assert "right_input_node_id" in props, f"{nt}: missing right_input_node_id"
        assert props["left_input_node_id"]["type"] == "integer", props["left_input_node_id"]
        assert props["right_input_node_id"]["type"] == "integer", props["right_input_node_id"]
        # The legacy list field is GONE for join-shaped types.
        assert "upstream_node_ids" not in props, (
            f"{nt}: upstream_node_ids list still in spec — should be replaced by "
            "left_input_node_id + right_input_node_id"
        )
        # Both scalars are required; rationale also required.
        required = spec.parameters.get("required", [])
        assert "left_input_node_id" in required, f"{nt}: left_input_node_id must be required"
        assert "right_input_node_id" in required, f"{nt}: right_input_node_id must be required"


def test_pick_upstream_spec_non_join_keeps_list_shape() -> None:
    """non-join types (and the no-picked-type fallback)
    keep the legacy ``upstream_node_ids`` list + nullable
    ``right_input_node_id``. Only join-shaped types get the
    symmetric-pair shape.
    """
    from flowfile_core.ai.tools.meta_ops import build_pick_upstream_spec

    for nt in ("filter", "sort", "group_by", "union", None):
        kwargs = {"live_node_ids": [1, 2, 3]}
        if nt is not None:
            kwargs["picked_node_type"] = nt
        spec = build_pick_upstream_spec(**kwargs)
        props = spec.parameters["properties"]
        assert "upstream_node_ids" in props, f"{nt}: missing list field"
        assert props["upstream_node_ids"]["type"] == "array"
        # No left_input_node_id on non-join specs.
        assert "left_input_node_id" not in props, (
            f"{nt}: spec should not carry left_input_node_id"
        )
        rtype = props["right_input_node_id"]["type"]
        # Nullable scalar for non-join; the field is documented as
        # legacy and should be left null.
        assert "null" in rtype, f"{nt}: right_input_node_id should still allow null; got {rtype!r}"


def test_pick_upstream_dispatch_translates_left_right_to_legacy() -> None:
    """when the LLM emits the new symmetric shape
    (``left_input_node_id`` + ``right_input_node_id``) for a
    join-shaped node, the executor's ``_handle_meta`` ``pick_upstream``
    branch translates it to the legacy ``upstream_node_ids`` /
    ``right_input_node_id`` representation so downstream consumers
    (planner session state, ``_handle_add_node``) are unchanged.
    """
    from flowfile_core.ai.tools import executor as executor_module
    from flowfile_core.ai.tools.executor import InsertionContext
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.schemas import schemas
    from flowfile_core.schemas import input_schema

    flow = FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=1,
            execution_mode="Performance",
            execution_location="local",
            path="/tmp/test_v15a_translation",
        ),
        name="v15a_translation_test",
    )
    raw = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="x", data_type="Integer")],
            data=[[1]],
        ),
    )
    flow.add_manual_input(raw)

    result = executor_module.execute_tool_call(
        tool_name="flowfile.meta.pick_upstream",
        tool_args={
            "left_input_node_id": 2,
            "right_input_node_id": 5,
            "rationale": "left=per-city counts, right=global total",
        },
        insertion_context=InsertionContext(
            upstream_node_ids=[], right_input_node_id=None
        ),
        flow=flow,
        flow_id=1,
        session_id="test-v15a",
        user_id=1,
    )
    assert result.status == "applied", result
    # The handler's extra dict carries the legacy fields, with the
    # left scalar wrapped into upstream_node_ids and right preserved
    # as the scalar.
    assert result.extra["upstream_node_ids"] == [2], result.extra
    assert result.extra["right_input_node_id"] == 5, result.extra


def test_join_user_instruction_warns_against_cross_in_chat_mode() -> None:
    """chat-mode reads ``NODE_USER_INSTRUCTIONS``, not the
    agent-side ``NODE_LONG_DESCRIPTIONS``. An earlier dogfood
    showed the chat-mode plan offered ``join`` for a no-key
    broadcast task, then got confused when it realised there was no
    key column. Strengthen the chat-mode prose so the LLM never
    suggests ``join`` for a no-key combination — point it at
    ``cross_join`` directly.
    """
    from flowfile_core.ai.tools.node_docs import NODE_USER_INSTRUCTIONS

    body = NODE_USER_INSTRUCTIONS["join"]
    body_lower = body.lower()
    # Lead with the key-based constraint.
    assert "key-based" in body_lower or "key based" in body_lower or "requires" in body_lower
    # Cross-reference cross_join as the no-key alternative.
    assert "cross_join" in body, body
    # `how` field doesn't include "cross".
    assert "no `cross`" in body_lower or "not include" in body_lower or "no \"cross\"" in body_lower


def test_cross_join_user_instruction_names_broadcast_pattern() -> None:
    """chat-mode cross_join entry must explicitly name the
    *broadcast a single-row value* pattern (the percentage-of-total
    use case from an earlier dogfood) so the LLM proposes
    cross_join for those tasks instead of confusing itself with
    join + missing key.
    """
    from flowfile_core.ai.tools.node_docs import NODE_USER_INSTRUCTIONS

    body = NODE_USER_INSTRUCTIONS["cross_join"]
    body_lower = body.lower()
    # The lead frames it as the no-key tool.
    assert "no-key" in body_lower or "no key" in body_lower or "without a key" in body_lower
    # Names the broadcast pattern explicitly.
    assert "broadcast" in body_lower
    # The percentage example is the canonical motivator.
    assert "percentage" in body_lower or "total" in body_lower


def test_explore_data_doc_signals_no_settings_required() -> None:
    """the agent should add ``explore_data`` without
    fabricating settings; the user's framing was *"the agent can
    just say, and now you can explore the data with an explore data!
    I added it to the canvas."* Both the agent-side and chat-mode
    descriptions must lead with the no-settings signal so the LLM
    emits an empty inner object at fill_settings.
    """
    from flowfile_core.ai.tools.node_docs import (
        NODE_AGENT_PAYLOAD_EXAMPLES,
        NODE_LONG_DESCRIPTIONS,
        NODE_USER_INSTRUCTIONS,
    )

    long_desc = NODE_LONG_DESCRIPTIONS["explore_data"]
    user_inst = NODE_USER_INSTRUCTIONS["explore_data"]
    for body, label in ((long_desc, "long_description"), (user_inst, "user_instruction")):
        body_lower = body.lower()
        assert "no settings" in body_lower or "no configuration" in body_lower, (
            f"explore_data {label} must lead with the no-settings signal; "
            f"got: {body!r}"
        )

    # The payload example must show the empty-shape envelope so the
    # LLM at fill_settings sees an unambiguous template.
    example = NODE_AGENT_PAYLOAD_EXAMPLES["explore_data"]
    assert "graphic_walker_input" in example
    assert "{}" in example  # the inner is empty


def test_pick_type_prompt_includes_join_vs_cross_join_section() -> None:
    """the agent_staged ``pick_type`` system prompt has a
    dedicated *"Join vs cross_join"* section that names both nodes
    and the key-required vs no-key distinction so the LLM picks the
    right one when the user describes a broadcast pattern.
    """
    text = assemble_system_prompt("agent_staged", stage="pick_type")
    assert "## Join vs cross_join" in text, (
        "v2.2: pick_type prompt missing the dedicated join-vs-cross_join section"
    )
    # Both node types named.
    assert "`join`" in text and "`cross_join`" in text
    # The broadcast / no-key trigger words are present.
    text_lower = text.lower()
    assert "broadcast" in text_lower
    assert "percentage" in text_lower
    # The decision rule is unambiguous.
    assert "no `cross` option" in text_lower or "no \"cross\"" in text_lower or "not `join`" in text_lower


def test_join_long_description_uses_left_right_only() -> None:
    """the join / cross_join / fuzzy_match long
    descriptions must use the LEFT/RIGHT vocabulary exclusively.
    Drop the confusing *"main"* / *"input-0"* / *"input-1"* aliases
    that previously leaked into the prose.
    """
    from flowfile_core.ai.tools.node_docs import NODE_LONG_DESCRIPTIONS

    for nt in ("join", "cross_join", "fuzzy_match"):
        body = NODE_LONG_DESCRIPTIONS[nt]
        body_lower = body.lower()
        assert "left" in body_lower, f"{nt}: must mention left"
        assert "right" in body_lower, f"{nt}: must mention right"
        # The triple-naming aliases are gone.
        assert "input-0" not in body_lower, f"{nt}: drop the input-0 alias; got {body!r}"
        assert "input-1" not in body_lower, f"{nt}: drop the input-1 alias; got {body!r}"
        # "main" appears only in inline contexts unrelated to input
        # naming. Specifically forbid the previously phrase ``main /
        # left`` or ``= main``.
        assert "main / left" not in body_lower, body
        assert "= main" not in body_lower, body


def test_pick_upstream_prompt_includes_worked_example_for_joins() -> None:
    """the pick_upstream stage prompt carries worked
    examples for join-shaped types so the LLM has a concrete shape
    to copy. The stage_pick_upstream.md content is loaded and joined
    into the system prompt at the agent_staged pick_upstream stage.
    """
    text = assemble_system_prompt("agent_staged", stage="pick_upstream")
    assert "## Worked examples for join-shaped types" in text, (
        "v1.15C: pick_upstream prompt missing the worked-examples section"
    )
    # Both the asymmetric (join) and symmetric (cross_join) examples
    # must be present and use the new field names.
    assert "left_input_node_id" in text and "right_input_node_id" in text
    # The cross_join output-column-order convention is the
    # disambiguation rule — test it's reiterated in the example.
    assert "first in the output" in text.lower() or "columns first" in text.lower(), text


def test_formula_fill_settings_prompt_includes_function_reference() -> None:
    """at stage 3 ``fill_settings`` for ``formula`` ONLY,
    the system prompt must carry a ``## Formula functions`` block
    listing the Flowfile expression library so the LLM can pick valid
    function names. Other node types must NOT carry this block — the
    function list is long and irrelevant outside the formula tool.
    """
    formula_prompt = assemble_system_prompt(
        "agent_staged", stage="fill_settings", picked_node_type="formula"
    )
    assert "Formula functions" in formula_prompt, (
        "v1.12C: formula fill_settings prompt missing the function reference block"
    )

    group_by_prompt = assemble_system_prompt(
        "agent_staged", stage="fill_settings", picked_node_type="group_by"
    )
    assert "Formula functions" not in group_by_prompt, (
        "v1.12C: function reference block leaked into a non-formula fill_settings prompt"
    )

    pick_type_prompt = assemble_system_prompt("agent_staged", stage="pick_type")
    assert "Formula functions" not in pick_type_prompt, (
        "v1.12C: function reference must not appear in the pick_type catalog "
        "(it would cost tokens on every pick_type round)"
    )


def test_polars_code_doc_says_pl_is_already_available() -> None:
    """short standing-prompt nudge on the polars_code docs:
    ``pl`` is already in scope, so the LLM should NOT prefix the body
    with ``import polars as pl``. Pairs with the
    ``polars_code_import_forbidden`` refusal in executor.py — the
    refusal catches misses on retry; this nudge prevents most misses
    upfront. Both the agent-facing long_description and the chat-mode
    user_instruction carry the rule.
    """
    from flowfile_core.ai.tools.node_docs import (
        NODE_LONG_DESCRIPTIONS,
        NODE_USER_INSTRUCTIONS,
    )

    for body, label in (
        (NODE_LONG_DESCRIPTIONS["polars_code"], "long_description"),
        (NODE_USER_INSTRUCTIONS["polars_code"], "user_instruction"),
    ):
        body_lower = body.lower()
        # Cite ``pl`` directly so the LLM has a concrete name to map
        # the rule to.
        assert "pl" in body, (
            f"polars_code {label} should reference `pl`; got: {body!r}"
        )
        # Tell the LLM `pl` is already there.
        assert "already available" in body_lower or "already imported" in body_lower, (
            f"polars_code {label} should say `pl` is already available; got: {body!r}"
        )
        # And explicitly forbid the import line so the LLM matches the
        # surface form it would otherwise emit.
        assert "import polars as pl" in body, (
            f"polars_code {label} should explicitly mention "
            f"``import polars as pl``; got: {body!r}"
        )


def test_verify_completion_stage_prompt_renders() -> None:
    """the verify_completion stage suffix is wired up:
    ``assemble_system_prompt(surface="agent_staged",
    stage="verify_completion")`` returns the new
    ``stage_verify_completion.md`` content with the LLM-facing anchor
    phrases. The prompt must NOT bleed into other staged stages.
    """
    text = assemble_system_prompt("agent_staged", stage="verify_completion")
    text_lower = text.lower()
    # Heading + tool name.
    assert "verify plan completion" in text_lower
    assert "flowfile.meta.verify_completion" in text
    # Both branches the LLM must pick between.
    assert "is_complete=true" in text
    assert "is_complete=false" in text
    # Inserted-node-mid-flow guidance — direct response to the
    # an earlier dogfood that motivated (chat-mode plan with
    # add-then-rewire steps where the agent terminated after step 1).
    assert "inserted-node-mid-flow" in text_lower or "inserted node mid-flow" in text_lower
    # The one-shot guard reminder so the LLM knows it can't ping-pong.
    assert "at most once per loop" in text_lower

    # Bleed-through check — verify_completion content is per-stage and
    # must not appear at classify (which would defeat the per-stage
    # tool-catalog separation).
    classify_prompt = assemble_system_prompt("agent_staged", stage="classify")
    assert "verify plan completion" not in classify_prompt.lower(), (
        "v2.12: verify_completion prompt content leaked into the classify "
        "stage system prompt"
    )


# Honest runtime-failure reporting — direct response to the 2026-05-09
# dogfood where the agent hit ``UnpredictableSchema`` on sql_query,
# hallucinated *"the kernel couldn't be found"* (not in the error text),
# and falsely told the user *"I added a sql_query node"* after the host had
# already auto-undone it. The fix has two pieces: (1) generic guidance in
# stage_fill_settings.md teaching the LLM to read the ``✗`` observation
# block honestly, and (2) a conditional Development-mode caveat for
# sql_query / polars_code that pre-warns the LLM before the failure.


def test_fill_settings_prompt_includes_runtime_feedback_section() -> None:
    """Generic guidance loaded at every fill_settings round so the LLM
    can interpret the ``✗`` observation block honestly on retries.
    Does NOT depend on picked_node_type — applies to all node types
    because the failure path is type-agnostic.
    """
    text = assemble_system_prompt("agent_live", stage="fill_settings")
    assert "## Reading runtime feedback" in text, (
        "fill_settings prompt missing the runtime-feedback section"
    )
    # Anchor phrases the LLM keys on when reading the appended observation.
    assert "✗ Step on node" in text
    text_lower = text.lower()
    assert "quote the error message verbatim" in text_lower, (
        "missing the verbatim-quoting imperative — without this the LLM "
        "paraphrases or invents causes"
    )
    assert "rolled back" in text_lower, (
        "missing the auto-undo acknowledgment language"
    )
    # The negative example is explicitly there so the LLM sees the exact
    # hallucination pattern it must avoid (transcript 2026-05-09).
    assert "kernel couldn't be found" in text_lower, (
        "missing the negative example — the worked-example contrast is "
        "what trains the LLM not to fabricate plausible causes"
    )


def test_fill_settings_renders_sql_query_caveat_when_picked() -> None:
    """The sql_query caveat block is gated like the formula function
    reference: it renders ONLY at fill_settings + when the picked
    node type is sql_query. Carries two pieces the LLM needs at the
    moment of staging — (a) upstream table-name convention
    (``input_1``/``input_2``/..., NEVER node-id-based) so the agent
    doesn't write ``FROM join_5`` and hit ``relation '...' was not
    found``, and (b) the Development-mode auto-undo warning.
    """
    text = assemble_system_prompt(
        "agent_live", stage="fill_settings", picked_node_type="sql_query"
    )
    assert "## sql_query-specific guidance" in text, (
        "sql_query fill_settings prompt missing the sql_query-specific "
        "guidance section"
    )

    # Table-name convention — the actual reason the retry
    # failed with ``relation 'join_5' was not found``.
    assert "input_1" in text, (
        "missing the ``input_1`` table-name reminder — without this "
        "the LLM keeps hallucinating ``join_<node_id>`` table names"
    )
    assert "input_2" in text, "missing the multi-input ``input_2`` example"
    # Anti-pattern call-out: explicitly name the failure mode so the
    # LLM doesn't repeat the join_5 hallucination.
    assert "join_5" in text or "join_<node_id>" in text, (
        "the caveat must name the wrong-table-name anti-pattern "
        "explicitly (otherwise the LLM repeats the chat-mode "
        "hallucination)"
    )

    # Development-mode auto-undo guidance.
    assert "UnpredictableSchema" in text
    assert "Performance" in text, (
        "missing the Performance-mode remediation — without it the "
        "LLM can't tell the user what to do"
    )
    text_lower = text.lower()
    assert "auto-undo" in text_lower or "auto undo" in text_lower


def test_pick_type_does_NOT_render_sql_query_caveat() -> None:
    """Guardrail — the caveat must NOT appear in the pick_type stage
    output. The gate is at ``_build_single_node_block`` (fill_settings
    only); a regression would re-bloat every pick_type round with
    sql_query-specific guidance that isn't actionable until the type
    is actually picked.
    """
    text = assemble_system_prompt("agent_live", stage="pick_type")
    assert "## sql_query-specific guidance" not in text, (
        "sql_query caveat leaked into pick_type catalog — this defeats "
        "the conditional-rendering gate"
    )


def test_fill_settings_other_types_do_NOT_render_sql_query_caveat() -> None:
    """Guardrail — other types' fill_settings rounds must not see the
    caveat. Only sql_query triggers it; polars_code (which works
    fine in Development mode — it has its own predictor path) is
    explicitly excluded.
    """
    for nt in ("group_by", "filter", "join", "select", "formula", "polars_code"):
        text = assemble_system_prompt(
            "agent_live", stage="fill_settings", picked_node_type=nt
        )
        assert "## sql_query-specific guidance" not in text, (
            f"sql_query caveat leaked into fill_settings for {nt!r}"
        )


def test_sql_query_long_description_names_input_1_table_convention() -> None:
    """The agent's pick_type catalog has to surface the
    ``input_1``/``input_2`` table-name convention or the LLM has no
    way to write correct SQL. Direct response to an earlier dogfood where the agent wrote ``FROM join_5`` and got ``relation
    'join_5' was not found``.
    """
    from flowfile_core.ai.tools.node_docs import NODE_LONG_DESCRIPTIONS

    desc = NODE_LONG_DESCRIPTIONS["sql_query"]
    assert "input_1" in desc
    assert "input_2" in desc
    desc_lower = desc.lower()
    assert "positional" in desc_lower, (
        "the description must call out the positional naming so the "
        "LLM doesn't infer node-id-based names from upstream context"
    )


def test_sql_query_user_instruction_uses_input_1_in_example() -> None:
    """The chat-mode prose (``NODE_USER_INSTRUCTIONS``) that the auto-
    promote path embeds as agent context must show ``FROM input_1``
    in its worked example, not ``FROM orders``/``FROM customers``.
    The previous text was the source of the chat LLM's ``join_5``
    hallucination."""
    from flowfile_core.ai.tools.node_docs import NODE_USER_INSTRUCTIONS

    instruction = NODE_USER_INSTRUCTIONS["sql_query"]
    assert "input_1" in instruction, (
        "chat-mode worked example doesn't use ``input_1`` — chat will "
        "keep hallucinating table names from node display names"
    )
    assert "input_2" in instruction
    # The old broken example used ``FROM orders`` / ``FROM customers``
    # as table names. Make sure those don't reappear.
    assert "FROM orders" not in instruction
    assert "FROM customers" not in instruction
