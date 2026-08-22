"""Tests for the phase-2 conditional-execution trigger rules.

Three layers:

A. ``classify_from_inputs`` — the pure local rule (ALL vs ANY) as a unit table.
B. ``classify_graph`` — full-graph Kahn classification over REAL FlowGraphs,
   cross-checked against ``determine_nodes_to_skip``.
C. The runtime seam in ``FlowGraph._execute_stages`` — deliberate skips are
   injected directly (white-box) until the gate node exists, so the gated-branch
   drop, the ANY-input liveness invalidation, and the run-flag clearing can be
   exercised against real data.

Run with an isolated DB:
    FLOWFILE_DB_PATH=/tmp/ff_p2_tests.db SKIP_WORKER_TESTS=1 \
        poetry run pytest flowfile_core/tests/flowfile/test_skip_rules.py -q
"""

from typing import Literal

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
from flowfile_core.flowfile.util.node_skipper import determine_nodes_to_skip
from flowfile_core.flowfile.util.skip_rules import (
    ANY_INPUT_NODE_TYPES,
    NodeRunStatus,
    classify_from_inputs,
    classify_graph,
    dead_gate_handles,
    effective_input_status,
)
from flowfile_core.schemas import input_schema, schemas, transform_schema

RUN = NodeRunStatus.RUN
FAILED = NodeRunStatus.FAILED
SKIPPED_ERROR = NodeRunStatus.SKIPPED_ERROR
SKIPPED_DELIBERATE = NodeRunStatus.SKIPPED_DELIBERATE


# Graph building helpers


def create_graph(
    flow_id: int = 1,
    execution_mode: Literal["Development", "Performance"] = "Development",
) -> FlowGraph:
    """A local-execution FlowGraph, so no worker is involved."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="skip_rules_test",
            path=".",
            execution_mode=execution_mode,
            execution_location="local",
        )
    )
    return handler.get_flow(flow_id)


def add_promise(graph: FlowGraph, node_type: str, node_id: int) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type)
    )


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int) -> None:
    add_promise(graph, "manual_input", node_id)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(data),
        )
    )


def connect(graph: FlowGraph, from_id: int, to_id: int, output_handle: str = "output-0") -> None:
    add_connection(
        graph,
        input_schema.NodeConnection.create_from_simple_input(from_id, to_id, output_handle=output_handle),
    )


def add_passthrough_select(graph: FlowGraph, node_id: int, depending_on_id: int) -> None:
    """A select that keeps every column — a healthy, cheap ALL-rule node."""
    add_promise(graph, "select", node_id)
    connect(graph, depending_on_id, node_id)
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            select_input=[],
            keep_missing=True,
        )
    )


def add_union(graph: FlowGraph, node_id: int, depending_on_ids: list[int]) -> None:
    add_promise(graph, "union", node_id)
    for from_id in depending_on_ids:
        connect(graph, from_id, node_id)
    graph.add_union(
        input_schema.NodeUnion(flow_id=graph.flow_id, node_id=node_id, depending_on_ids=depending_on_ids)
    )


def add_formula(graph: FlowGraph, node_id: int, depending_on_id: int, field: str, expression: str) -> None:
    add_promise(graph, "formula", node_id)
    connect(graph, depending_on_id, node_id)
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name=field),
                function=expression,
            ),
        )
    )


def add_csv_read(graph: FlowGraph, node_id: int, path: str) -> None:
    add_promise(graph, "read", node_id)
    graph.add_read(
        input_schema.NodeRead(
            flow_id=graph.flow_id,
            node_id=node_id,
            received_file=input_schema.ReceivedTable.create_from_path(path, file_type="csv"),
        )
    )


def statuses_of(graph: FlowGraph, deliberate_seed_ids: set | None = None) -> dict:
    return classify_graph(graph.nodes, deliberate_seed_ids=deliberate_seed_ids)


def run_stages(graph: FlowGraph, deliberate: set) -> set:
    """Drive ``_execute_stages`` directly with an injected deliberate-skip set."""
    plan = compute_execution_plan(
        nodes=graph.nodes, flow_starts=graph._flow_starts + graph.get_implicit_starter_nodes()
    )
    graph.latest_run_info = graph.create_initial_run_information(plan.node_count, "full_run")
    return graph._execute_stages(plan, False, {}, set(), set(deliberate))


def collect_node(graph: FlowGraph, node_id: int) -> pl.DataFrame:
    return graph.get_node(node_id).results.resulting_data.data_frame.lazy().collect()


# A. classify_from_inputs unit table


class _StubNode:
    """``classify_from_inputs`` only reads ``node_type``."""

    def __init__(self, node_type: str):
        self.node_type = node_type


@pytest.mark.parametrize(
    "input_statuses, expected",
    [
        ([], RUN),
        ([RUN], RUN),
        ([FAILED], SKIPPED_ERROR),
        ([SKIPPED_ERROR], SKIPPED_ERROR),
        ([SKIPPED_DELIBERATE], SKIPPED_DELIBERATE),
        ([SKIPPED_DELIBERATE, SKIPPED_ERROR], SKIPPED_ERROR),
        ([RUN, SKIPPED_DELIBERATE], SKIPPED_DELIBERATE),
    ],
)
def test_classify_from_inputs_all_rule(input_statuses, expected):
    """Default (ALL) rule: any error-ish input wins, then any deliberate skip."""
    assert classify_from_inputs(_StubNode("select"), input_statuses) is expected


@pytest.mark.parametrize(
    "input_statuses, expected",
    [
        ([RUN, SKIPPED_DELIBERATE], RUN),
        ([SKIPPED_DELIBERATE, SKIPPED_DELIBERATE], SKIPPED_DELIBERATE),
        ([RUN, FAILED], SKIPPED_ERROR),
        ([RUN, SKIPPED_ERROR], SKIPPED_ERROR),
        ([RUN, RUN], RUN),
    ],
)
def test_classify_from_inputs_any_rule(input_statuses, expected):
    """ANY rule (union): one surviving input suffices, but failure never inherits."""
    assert classify_from_inputs(_StubNode("union"), input_statuses) is expected


def test_union_is_the_only_any_rule_node_type():
    assert ANY_INPUT_NODE_TYPES == frozenset({"union"})


# B. classify_graph on real graphs


def test_classify_graph_linear_with_misconfigured_middle():
    """manual_input -> unconfigured promise -> select: the promise and its downstream error-skip."""
    graph = create_graph()
    add_manual_input(graph, [{"a": 1}, {"a": 2}], node_id=1)
    add_promise(graph, "select", 2)
    connect(graph, 1, 2)
    add_passthrough_select(graph, node_id=3, depending_on_id=2)

    assert graph.get_node(2).is_correct is False

    status = statuses_of(graph)
    assert status[1] is RUN
    assert status[2] is SKIPPED_ERROR
    assert status[3] is SKIPPED_ERROR


def test_classify_graph_diamond_into_union_error_blocks_any():
    """An error-ish input blocks a union: ANY survives deliberate skips, never failures."""
    graph = create_graph()
    add_manual_input(graph, [{"a": 1}, {"a": 2}], node_id=1)
    add_passthrough_select(graph, node_id=2, depending_on_id=1)
    add_promise(graph, "select", 3)
    connect(graph, 1, 3)
    add_union(graph, node_id=4, depending_on_ids=[2, 3])
    add_passthrough_select(graph, node_id=5, depending_on_id=4)

    status = statuses_of(graph)
    assert status[1] is RUN
    assert status[2] is RUN
    assert status[3] is SKIPPED_ERROR
    assert status[4] is SKIPPED_ERROR
    assert status[5] is SKIPPED_ERROR

    skipped_ids = {node.node_id for node in determine_nodes_to_skip(graph.nodes)}
    assert skipped_ids == {3, 4, 5}


def test_classify_graph_deliberate_skip_survives_union_but_not_select():
    """A deliberately-skipped branch keeps a union alive; an ALL node off it is skipped too."""
    graph = create_graph()
    add_manual_input(graph, [{"a": 1}, {"a": 2}], node_id=1)
    add_passthrough_select(graph, node_id=2, depending_on_id=1)
    add_passthrough_select(graph, node_id=3, depending_on_id=1)
    add_union(graph, node_id=4, depending_on_ids=[2, 3])
    add_passthrough_select(graph, node_id=5, depending_on_id=4)
    add_passthrough_select(graph, node_id=6, depending_on_id=3)

    status = statuses_of(graph, deliberate_seed_ids={3})
    assert status[1] is RUN
    assert status[2] is RUN
    assert status[3] is SKIPPED_DELIBERATE
    assert status[4] is RUN
    assert status[5] is RUN
    assert status[6] is SKIPPED_DELIBERATE

    # A deliberate skip is never an error skip.
    assert determine_nodes_to_skip(graph.nodes) == []


# C. Runtime seam — deliberate skips injected into _execute_stages


def build_gated_diamond(flow_id: int = 1) -> FlowGraph:
    """1 manual_input -> 2 select (branch A) and -> 3 formula (branch B, adds b_only); 2+3 -> 4 union -> 5 select."""
    graph = create_graph(flow_id=flow_id)
    add_manual_input(graph, [{"id": 1}, {"id": 2}, {"id": 3}], node_id=1)
    add_passthrough_select(graph, node_id=2, depending_on_id=1)
    add_formula(graph, node_id=3, depending_on_id=1, field="b_only", expression="[id] * 10")
    add_union(graph, node_id=4, depending_on_ids=[2, 3])
    add_passthrough_select(graph, node_id=5, depending_on_id=4)
    return graph


def test_deliberate_skip_keeps_run_green_and_records_a_skipped_result():
    graph = build_gated_diamond()

    failed = run_stages(graph, deliberate={3})

    assert failed == set()
    skipped_results = [nr for nr in graph.latest_run_info.node_step_result if nr.skipped]
    assert [nr.node_id for nr in skipped_results] == [3]
    assert skipped_results[0].success is True

    run_info = graph.get_run_info()
    assert run_info.success is True
    # The gated node counts as completed, and is not double-counted in the denominator.
    assert run_info.number_of_nodes == 5
    assert run_info.nodes_completed == 5


def test_union_drops_the_gated_branch_and_runs_with_survivors():
    """The union survives the gate and outputs branch A alone — branch B's column is gone."""
    graph = build_gated_diamond()
    run_stages(graph, deliberate={3})

    union_df = collect_node(graph, 4)
    branch_a_df = collect_node(graph, 2)

    assert union_df.height == branch_a_df.height == 3
    assert "b_only" not in union_df.columns


def test_deliberately_skipped_node_run_flags_cleared():
    """Both state layers must be cleared, else the dev-mode cache skips the node forever."""
    graph = build_gated_diamond()
    run_stages(graph, deliberate={3})

    node_b = graph.get_node(3)
    assert node_b.node_stats.has_run_with_current_setup is False
    assert node_b._execution_state.has_run_with_current_setup is False


def test_any_node_skipped_input_ids_reset_after_run():
    graph = build_gated_diamond()
    run_stages(graph, deliberate={3})

    assert graph.get_node(4)._skipped_input_ids_this_run == frozenset()


def test_any_input_liveness_invalidates_union_across_gate_flips():
    """Gate closed -> open -> closed: the union's cached partial result must never be served."""
    graph = build_gated_diamond()

    run_stages(graph, deliberate={3})
    gated_df = collect_node(graph, 4)
    assert gated_df.height == 3
    assert "b_only" not in gated_df.columns

    # Both branches survive here, so the diagonal concat null-fills across them.
    run_stages(graph, deliberate=set())
    open_df = collect_node(graph, 4)
    assert open_df.height == 6
    assert open_df["b_only"].null_count() == 3
    assert sorted(v for v in open_df["b_only"].to_list() if v is not None) == [10, 20, 30]

    run_stages(graph, deliberate={3})
    regated_df = collect_node(graph, 4)
    assert regated_df.height == 3
    assert "b_only" not in regated_df.columns


def test_failed_branch_blocks_the_union_and_is_not_reported_as_skipped(tmp_path):
    """A failed input never inherits ANY: the union must not silently write half the data."""
    missing_csv = tmp_path / "does_not_exist.csv"
    graph = create_graph(flow_id=2)
    add_manual_input(graph, [{"id": 1}, {"id": 2}], node_id=1)
    add_passthrough_select(graph, node_id=2, depending_on_id=1)
    add_csv_read(graph, node_id=3, path=str(missing_csv))
    add_union(graph, node_id=4, depending_on_ids=[2, 3])

    run_info = graph.run_graph()

    assert run_info.success is False
    results_by_id = {nr.node_id: nr for nr in run_info.node_step_result}
    assert results_by_id[3].success is False

    union = graph.get_node(4)
    assert union.node_stats.has_run_with_current_setup is False
    assert union._execution_state.has_run_with_current_setup is False
    assert union.results.resulting_data is None
    # A failure-driven skip produces no report row at all — never a green "skipped" one.
    assert 4 not in results_by_id
    assert 4 not in {nr.node_id for nr in run_info.node_step_result if nr.skipped}


def test_skipped_node_recovers_once_the_missing_source_appears(tmp_path):
    """Pins the run-flag clearing: an error-skipped node must re-execute on the next run."""
    csv_path = tmp_path / "late.csv"
    graph = create_graph(flow_id=3)
    add_csv_read(graph, node_id=1, path=str(csv_path))
    add_passthrough_select(graph, node_id=2, depending_on_id=1)

    first_run = graph.run_graph()
    assert first_run.success is False
    select_node = graph.get_node(2)
    assert select_node.node_stats.has_run_with_current_setup is False
    assert select_node._execution_state.has_run_with_current_setup is False

    csv_path.write_text("name,value\nalice,1\nbob,2\n")

    second_run = graph.run_graph()
    assert second_run.success is True, [
        (nr.node_id, nr.error) for nr in second_run.node_step_result if not nr.success
    ]
    recovered = collect_node(graph, 2)
    assert recovered.height == 2
    assert recovered["name"].to_list() == ["alice", "bob"]


# D. Per-handle gate routing (pure rules; graph-level coverage in test_gate_node.py)


class TestDeadGateHandles:
    @pytest.mark.parametrize(
        "is_open, has_else, expected",
        [
            (True, False, None),
            (True, True, frozenset({"output-1"})),
            (False, True, frozenset({"output-0"})),
            # A closed single-output gate kills BOTH handles: a stale else edge
            # left behind by toggling the option off must never leak data.
            (False, False, frozenset({"output-0", "output-1"})),
        ],
    )
    def test_routing_table(self, is_open, has_else, expected):
        assert dead_gate_handles(is_open, has_else) == expected


class TestEffectiveInputStatusPerHandle:
    def test_dead_handle_skips_only_its_consumers(self):
        closed = {2: frozenset({"output-0"})}

        then_side = effective_input_status({2: RUN}, 2, "output-0", closed)
        else_side = effective_input_status({2: RUN}, 2, "output-1", closed)

        assert then_side == SKIPPED_DELIBERATE
        assert else_side == RUN

    def test_failed_gate_beats_routing_on_every_handle(self):
        closed = {2: frozenset({"output-0"})}

        assert effective_input_status({2: FAILED}, 2, "output-0", closed) == FAILED
        assert effective_input_status({2: FAILED}, 2, "output-1", closed) == FAILED

    def test_gate_without_entry_is_fully_open(self):
        assert effective_input_status({2: RUN}, 2, "output-1", {}) == RUN
        assert effective_input_status({2: RUN}, 2, "output-1", None) == RUN
