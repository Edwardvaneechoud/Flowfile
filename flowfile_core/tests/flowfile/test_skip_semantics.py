"""Tests for the "skipped != failed" foundation (phase 1 of conditional execution).

Covers:
- NodeResult.skipped wire compatibility (old run-history rows, round-trip)
- _run_post_execution_callbacks: deliberate skips must not block source callbacks
  (the Kafka offset-commit regression this phase exists to prevent)
- _record_deliberate_skips: green zero-work rows + cleared run flags
- Zero behavior change for a normal, fully-executed run

Run with:
    FLOWFILE_DB_PATH=/tmp/ff_p1_tests.db SKIP_WORKER_TESTS=1 \
        pytest flowfile_core/tests/flowfile/test_skip_semantics.py -q
"""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.schemas.output_model import NodeResult
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

SAMPLE_DATA = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
]


def _add_formula(graph: FlowGraph, node_id: int, depends_on: int, output_field: str, function: str) -> None:
    """Add a formula node downstream of ``depends_on``."""
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="formula")
    )
    add_connection(
        graph, input_schema.NodeConnection.create_from_simple_input(from_id=depends_on, to_id=node_id)
    )
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=node_id,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name=output_field),
                function=function,
            ),
        )
    )


def _linear_graph(flow_id: int = 1) -> FlowGraph:
    """manual_input(1) -> formula(2) -> formula(3)."""
    graph = create_test_graph(flow_id=flow_id, execution_location="local")
    add_test_manual_input(graph, SAMPLE_DATA, node_id=1)
    _add_formula(graph, node_id=2, depends_on=1, output_field="age_plus_one", function="[age] + 1")
    _add_formula(graph, node_id=3, depends_on=2, output_field="age_plus_two", function="[age] + 2")
    return graph


def _branching_graph(flow_id: int = 1) -> FlowGraph:
    """manual_input(1) -> formula(2) and manual_input(1) -> formula(3)."""
    graph = create_test_graph(flow_id=flow_id, execution_location="local")
    add_test_manual_input(graph, SAMPLE_DATA, node_id=1)
    _add_formula(graph, node_id=2, depends_on=1, output_field="age_plus_one", function="[age] + 1")
    _add_formula(graph, node_id=3, depends_on=1, output_field="age_plus_two", function="[age] + 2")
    return graph


def _record_callback(graph: FlowGraph, node_id: int) -> list[bool]:
    """Register a recording _on_flow_complete callback and return its call log."""
    calls: list[bool] = []
    graph.get_node(node_id)._on_flow_complete = calls.append
    return calls


@pytest.fixture()
def linear_flow() -> FlowGraph:
    return _linear_graph()


# A) NodeResult wire compatibility


class TestNodeResultWireCompatibility:
    """The new ``skipped`` field must not break existing run-history payloads."""

    def test_legacy_row_without_skipped_defaults_to_false(self):
        legacy_row = {"node_id": 1, "success": True, "error": "", "run_time": 5, "is_running": False}
        result = NodeResult.model_validate(legacy_row)
        assert result.skipped is False
        assert result.success is True
        assert result.run_time_ms == 5

    def test_run_time_ms_key_still_accepted(self):
        result = NodeResult.model_validate({"node_id": 1, "run_time_ms": 12, "is_running": False})
        assert result.run_time_ms == 12
        assert result.skipped is False

    def test_defaults_when_only_node_id_given(self):
        result = NodeResult(node_id=1)
        assert result.skipped is False
        assert result.run_time_ms == -1

    def test_skipped_round_trips_through_dump_and_validate(self):
        original = NodeResult(
            node_id=7, node_name="formula", success=True, skipped=True, run_time_ms=0, is_running=False
        )
        dumped = original.model_dump()
        assert dumped["skipped"] is True

        restored = NodeResult.model_validate(dumped)
        assert restored.skipped is True
        assert restored.success is True
        assert restored.run_time_ms == 0
        assert restored.is_running is False

    def test_skipped_round_trips_through_json(self):
        original = NodeResult(node_id=7, success=True, skipped=True, run_time_ms=0, is_running=False)
        restored = NodeResult.model_validate_json(original.model_dump_json())
        assert restored.skipped is True
        assert restored.success is True


# B) Post-execution callback semantics


class TestPostExecutionCallbacks:
    """A deliberately-gated branch must not block a source's completion callback.

    Kafka sources commit their offsets from ``_on_flow_complete``; a closed gate
    downstream would otherwise make every run re-read the same messages.
    """

    def test_clean_run_reports_success(self, linear_flow):
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), set(), set())
        assert calls == [True]

    def test_error_driven_skip_downstream_blocks_callback(self, linear_flow):
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), {3}, set())
        assert calls == [False]

    def test_deliberate_skip_downstream_does_not_block_callback(self, linear_flow):
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), {3}, {3})
        assert calls == [True]

    def test_failed_source_itself_blocks_callback(self, linear_flow):
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks({1}, set(), set())
        assert calls == [False]

    def test_deliberate_skip_of_source_itself_does_not_block_callback(self, linear_flow):
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), {1}, {1})
        assert calls == [True]

    def test_error_skip_alongside_deliberate_skip_still_blocks(self):
        graph = _branching_graph()
        calls = _record_callback(graph, 1)
        graph._run_post_execution_callbacks(set(), {2, 3}, {3})
        assert calls == [False]

    def test_failure_alongside_deliberate_skip_still_blocks(self):
        graph = _branching_graph()
        calls = _record_callback(graph, 1)
        graph._run_post_execution_callbacks({2}, {3}, {3})
        assert calls == [False]

    def test_callback_is_cleared_and_can_be_re_registered(self, linear_flow):
        source = linear_flow.get_node(1)
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), set(), set())
        assert calls == [True]
        assert source._on_flow_complete is None

        # A second pass with the callback cleared must not re-invoke it.
        linear_flow._run_post_execution_callbacks(set(), {3}, set())
        assert calls == [True]

        second_calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), {3}, set())
        assert second_calls == [False]
        assert source._on_flow_complete is None

    def test_raising_callback_is_swallowed_and_cleared(self, linear_flow):
        source = linear_flow.get_node(1)
        invoked: list[bool] = []

        def boom(success: bool) -> None:
            invoked.append(success)
            raise RuntimeError("offset commit exploded")

        source._on_flow_complete = boom
        linear_flow._run_post_execution_callbacks(set(), set(), set())

        assert invoked == [True]
        assert source._on_flow_complete is None

    def test_deliberate_argument_is_optional(self, linear_flow):
        """Two-argument calls keep the pre-phase-1 behaviour: a skip blocks."""
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), {3})
        assert calls == [False]

    def test_two_arg_clean_run_still_succeeds(self, linear_flow):
        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), set())
        assert calls == [True]

    def test_gated_output_node_logs_a_warning_but_still_commits(self, monkeypatch):
        graph = create_test_graph(flow_id=1, execution_location="local")
        add_test_manual_input(graph, SAMPLE_DATA, node_id=1)
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="output")
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        assert graph.get_node(2).node_template.node_group == "output"

        warnings: list[str] = []
        monkeypatch.setattr(graph.flow_logger, "warning", lambda msg, *a, **kw: warnings.append(msg))

        calls = _record_callback(graph, 1)
        graph._run_post_execution_callbacks(set(), {2}, {2})

        assert calls == [True]
        assert len(warnings) == 1
        assert "deliberately skipped" in warnings[0]

    def test_no_warning_when_gated_node_is_not_an_output(self, linear_flow, monkeypatch):
        warnings: list[str] = []
        monkeypatch.setattr(linear_flow.flow_logger, "warning", lambda msg, *a, **kw: warnings.append(msg))

        calls = _record_callback(linear_flow, 1)
        linear_flow._run_post_execution_callbacks(set(), {3}, {3})

        assert calls == [True]
        assert warnings == []

    def test_nodes_without_callbacks_are_untouched(self, linear_flow):
        linear_flow._run_post_execution_callbacks(set(), {2, 3}, set())
        assert all(node._on_flow_complete is None for node in linear_flow.nodes)


# C) Recording deliberate skips on the run report


class TestRecordDeliberateSkips:
    def test_skip_row_is_green_and_counted(self):
        graph = _linear_graph()
        run_info = graph.run_graph()
        assert run_info.success

        before_completed = graph.latest_run_info.nodes_completed
        before_total = graph.latest_run_info.number_of_nodes
        before_rows = len(graph.latest_run_info.node_step_result)

        graph._record_deliberate_skips({3})

        info = graph.latest_run_info
        assert len(info.node_step_result) == before_rows + 1
        assert info.nodes_completed == before_completed + 1
        assert info.number_of_nodes == before_total + 1

        skipped_rows = [r for r in info.node_step_result if r.skipped]
        assert len(skipped_rows) == 1
        row = skipped_rows[0]
        assert row.node_id == 3
        assert row.success is True
        assert row.is_running is False
        assert row.run_time_ms == 0
        assert row.error == ""

    def test_run_flags_are_cleared_so_the_node_re_runs_next_time(self):
        graph = _linear_graph()
        graph.run_graph()
        node = graph.get_node(3)
        assert node.node_stats.has_run_with_current_setup
        assert node.node_stats.has_completed_last_run

        graph._record_deliberate_skips({3})

        assert node.node_stats.has_run_with_current_setup is False
        assert node.node_stats.has_completed_last_run is False
        assert node._execution_state.has_run_with_current_setup is False
        assert node._execution_state.has_completed_last_run is False

    def test_skipped_rows_keep_the_run_green(self):
        graph = _linear_graph()
        graph.run_graph()
        graph._record_deliberate_skips({3})
        assert graph.get_run_info().success is True

    def test_multiple_skips_are_all_recorded(self):
        graph = _branching_graph()
        graph.run_graph()
        before_completed = graph.latest_run_info.nodes_completed
        before_total = graph.latest_run_info.number_of_nodes

        graph._record_deliberate_skips({2, 3})

        info = graph.latest_run_info
        assert {r.node_id for r in info.node_step_result if r.skipped} == {2, 3}
        assert info.nodes_completed == before_completed + 2
        assert info.number_of_nodes == before_total + 2
        assert graph.get_run_info().success is True

    def test_empty_set_changes_nothing(self):
        graph = _linear_graph()
        graph.run_graph()
        before_rows = len(graph.latest_run_info.node_step_result)
        before_completed = graph.latest_run_info.nodes_completed
        before_total = graph.latest_run_info.number_of_nodes

        graph._record_deliberate_skips(set())

        assert len(graph.latest_run_info.node_step_result) == before_rows
        assert graph.latest_run_info.nodes_completed == before_completed
        assert graph.latest_run_info.number_of_nodes == before_total

    def test_unknown_node_id_is_ignored(self):
        graph = _linear_graph()
        graph.run_graph()
        before_rows = len(graph.latest_run_info.node_step_result)
        before_completed = graph.latest_run_info.nodes_completed
        before_total = graph.latest_run_info.number_of_nodes

        graph._record_deliberate_skips({99999})

        assert len(graph.latest_run_info.node_step_result) == before_rows
        assert graph.latest_run_info.nodes_completed == before_completed
        assert graph.latest_run_info.number_of_nodes == before_total

    def test_known_and_unknown_ids_mixed(self):
        graph = _linear_graph()
        graph.run_graph()
        graph._record_deliberate_skips({3, 99999})
        assert [r.node_id for r in graph.latest_run_info.node_step_result if r.skipped] == [3]

    def test_no_op_when_the_flow_never_ran(self):
        graph = _linear_graph()
        assert graph.latest_run_info is None
        graph._record_deliberate_skips({3})
        assert graph.latest_run_info is None


# D) Zero behaviour change for a normal run


class TestNormalRunUnaffected:
    def test_full_run_stays_green_with_no_skipped_rows(self):
        graph = _linear_graph()
        run_info = graph.run_graph()

        assert run_info.success is True
        assert run_info.node_step_result, "expected a result row per executed node"
        assert all(r.skipped is False for r in run_info.node_step_result)
        assert all(r.success for r in run_info.node_step_result)
        assert run_info.nodes_completed == run_info.number_of_nodes
        assert {r.node_id for r in run_info.node_step_result} == {1, 2, 3}

    def test_run_produces_expected_data(self):
        graph = _linear_graph()
        graph.run_graph()
        result = graph.get_node(3).get_resulting_data().to_pylist()
        assert [row["age_plus_one"] for row in result] == [31, 26, 36]
        assert [row["age_plus_two"] for row in result] == [32, 27, 37]
