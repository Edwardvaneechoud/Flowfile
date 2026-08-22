"""End-to-end tests for the ``gate`` node (conditional execution).

A gate is a passthrough with a condition. When the condition holds its input
flows through unchanged; when it does not, the gate itself still *executes*
(it "ran and decided") while everything downstream of it is deliberately
skipped — reported green, never failed.

Layers covered here:

A. The parameter-mode diamond — the target user story: two complementary gates
   feeding one union, flipped by a flow parameter, including the dev-mode
   cache invalidation when the parameter flips on a live graph.
B. Union semantics under gates: all-branches-off, and error-beats-deliberate.
C. Control-input gates: routing on the first value of a second input, decided
   at execution time and re-decided on every run.
D. Failure modes: an unknown parameter must fail the gate loudly.
E. Gate-blind callers (single-node fetch, code export) must plan as if every
   gate were open.
F. Code export parity across the Polars and FlowFrame exports.
G. YAML round-trip of the gate's settings.

Run with an isolated DB:
    FLOWFILE_DB_PATH=/tmp/ff_gate_tests.db SKIP_WORKER_TESTS=1 \
        poetry run pytest flowfile_core/tests/flowfile/test_gate_node.py -q
"""

import os
import platform
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
import requests
from fastapi import HTTPException

from flowfile_core.flowfile.code_generator.code_generator import (
    FlowGraphToFlowFrameConverter,
    FlowGraphToPolarsConverter,
    UnsupportedNodeError,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, delete_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.transform_schema import BasicFilter, FilterInput, FilterOperator

SOURCE_ROWS = [{"a": 1}, {"a": 2}, {"a": 3}]


# Graph building helpers (same shape as test_skip_rules.py)


def create_graph(
    flow_id: int = 1,
    execution_mode: Literal["Development", "Performance"] = "Development",
    execution_location: Literal["local", "remote"] = "local",
) -> FlowGraph:
    """A FlowGraph pinned to local execution by default, so no worker is involved."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="gate_node_test",
            path=".",
            execution_mode=execution_mode,
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def add_promise(graph: FlowGraph, node_type: str, node_id: int) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type)
    )


def connect(
    graph: FlowGraph,
    from_id: int,
    to_id: int,
    input_type: str = "input-0",
    output_handle: str = "output-0",
) -> None:
    add_connection(
        graph,
        input_schema.NodeConnection.create_from_simple_input(
            from_id, to_id, input_type=input_type, output_handle=output_handle
        ),
    )


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int) -> None:
    add_promise(graph, "manual_input", node_id)
    set_manual_input(graph, data, node_id)


def set_manual_input(graph: FlowGraph, data: list[dict], node_id: int) -> None:
    """(Re)configure an existing manual_input node — used to mutate control data."""
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(data),
        )
    )


def add_gate(
    graph: FlowGraph,
    node_id: int,
    depending_on_id: int,
    control_id: int | None = None,
    else_output: bool = False,
    **gate_kwargs,
) -> None:
    add_promise(graph, "gate", node_id)
    connect(graph, depending_on_id, node_id)
    if control_id is not None:
        connect(graph, control_id, node_id, input_type="right")
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            is_setup=True,
            else_output=else_output,
            gate_input=transform_schema.GateInput(**gate_kwargs),
        )
    )


def add_passthrough_select(graph: FlowGraph, node_id: int, depending_on_id: int) -> None:
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


def add_rename_select(
    graph: FlowGraph,
    node_id: int,
    depending_on_id: int,
    old: str,
    new: str,
    output_handle: str = "output-0",
) -> None:
    add_promise(graph, "select", node_id)
    connect(graph, depending_on_id, node_id, output_handle=output_handle)
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            select_input=[transform_schema.SelectInput(old_name=old, new_name=new)],
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


def add_csv_read(graph: FlowGraph, node_id: int, path: str) -> None:
    add_promise(graph, "read", node_id)
    graph.add_read(
        input_schema.NodeRead(
            flow_id=graph.flow_id,
            node_id=node_id,
            received_file=input_schema.ReceivedTable.create_from_path(path, file_type="csv"),
        )
    )


def add_filter(
    graph: FlowGraph,
    node_id: int,
    depending_on_id: int,
    field: str,
    value: str,
    split_mode: bool = False,
) -> None:
    """An equals filter — used to build an empty control frame or a pass/fail split."""
    add_promise(graph, "filter", node_id)
    connect(graph, depending_on_id, node_id)
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            split_mode=split_mode,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field=field, operator=FilterOperator.EQUALS, value=value),
            ),
        )
    )


def collect_node(graph: FlowGraph, node_id: int) -> pl.DataFrame:
    return graph.get_node(node_id).results.resulting_data.data_frame.lazy().collect()


def would_re_execute(graph: FlowGraph, node_id: int) -> bool:
    """Whether the executor would run this node again as the graph now stands."""
    node = graph.get_node(node_id)
    state = node.executor.state_provider.get_state(node.node_id, node.parent_uuid)
    return node.executor._decide_execution(state, "local", False, False).should_run


def results_by_id(run_info) -> dict:
    return {nr.node_id: nr for nr in run_info.node_step_result}


def build_diamond(
    env: str = "prod",
    flow_id: int = 1,
    execution_mode: Literal["Development", "Performance"] = "Development",
    execution_location: Literal["local", "remote"] = "local",
) -> FlowGraph:
    """The target user story.

    1 manual_input -> 2 gate(env == prod) -> 3 select(a -> a_prod) -> 6 union
    1 manual_input -> 4 gate(env != prod, the complement) -> 5 select(a -> a_dev) -> 6
    6 union -> 7 select (terminal, record-preserving)
    """
    graph = create_graph(flow_id=flow_id, execution_mode=execution_mode, execution_location=execution_location)
    graph.flow_settings.parameters.append(
        FlowParameter(name="env", default_value=env, type="string")
    )
    add_manual_input(graph, SOURCE_ROWS, node_id=1)
    add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="prod")
    add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_prod")
    add_gate(
        graph,
        node_id=4,
        depending_on_id=1,
        parameter="env",
        operator="not_equals",
        value="prod",
    )
    add_rename_select(graph, node_id=5, depending_on_id=4, old="a", new="a_dev")
    add_union(graph, node_id=6, depending_on_ids=[3, 5])
    add_passthrough_select(graph, node_id=7, depending_on_id=6)
    return graph


def build_split_diamond(
    env: str = "prod",
    flow_id: int = 1,
    execution_mode: Literal["Development", "Performance"] = "Development",
) -> FlowGraph:
    """The one-gate if/else: a single else_output gate replacing the two-gate diamond.

    1 manual_input -> 2 gate(env == prod, else_output)
    then (output-0) -> 3 select(a -> a_prod) -> 6 union
    else (output-1) -> 5 select(a -> a_dev) -> 6
    6 union -> 7 select (terminal, record-preserving)
    """
    graph = create_graph(flow_id=flow_id, execution_mode=execution_mode)
    graph.flow_settings.parameters.append(
        FlowParameter(name="env", default_value=env, type="string")
    )
    add_manual_input(graph, SOURCE_ROWS, node_id=1)
    add_gate(
        graph,
        node_id=2,
        depending_on_id=1,
        else_output=True,
        parameter="env",
        operator="equals",
        value="prod",
    )
    add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_prod")
    add_rename_select(graph, node_id=5, depending_on_id=2, old="a", new="a_dev", output_handle="output-1")
    add_union(graph, node_id=6, depending_on_ids=[3, 5])
    add_passthrough_select(graph, node_id=7, depending_on_id=6)
    return graph


def set_parameter(graph: FlowGraph, name: str, value: str) -> None:
    for param in graph.flow_settings.parameters:
        if param.name == name:
            param.default_value = value
            return
    raise AssertionError(f"parameter {name} not on the flow")


def assert_branch(df: pl.DataFrame, live: str, gated: str, values: list[int]) -> None:
    """The union outputs only the branch that ran; the gated column is absent."""
    assert set(df.columns) == {live}
    assert gated not in df.columns
    assert df[live].to_list() == values


# A. The parameter diamond


class TestParameterGateDiamond:
    def test_run_is_green_and_only_the_gated_branch_is_skipped(self):
        graph = build_diamond(env="prod")

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[5].skipped is True
        assert by_id[5].success is True
        assert [nr.node_id for nr in run_info.node_step_result if nr.skipped] == [5]

    def test_open_and_closed_gates_both_execute_normally(self):
        """A closed gate runs — it decided; only its *downstream* is skipped."""
        graph = build_diamond(env="prod")
        run_info = graph.run_graph()

        by_id = results_by_id(run_info)
        for gate_id in (2, 4):
            assert by_id[gate_id].success is True
            assert by_id[gate_id].skipped is False

    def test_progress_reaches_the_denominator(self):
        graph = build_diamond(env="prod")
        run_info = graph.run_graph()

        assert run_info.number_of_nodes == len(graph.nodes) == 7
        assert run_info.nodes_completed == run_info.number_of_nodes

    def test_union_outputs_only_the_live_branch_columns(self):
        graph = build_diamond(env="prod")
        graph.run_graph()

        union_df = collect_node(graph, 6)
        assert union_df.height == 3
        assert_branch(union_df, live="a_prod", gated="a_dev", values=[1, 2, 3])

    def test_terminal_node_sees_the_union_output(self):
        graph = build_diamond(env="prod")
        graph.run_graph()

        assert_branch(collect_node(graph, 7), live="a_prod", gated="a_dev", values=[1, 2, 3])

    def test_flipping_the_parameter_reroutes_the_live_graph(self):
        """Dev-mode liveness: the same graph instance must not serve the previous branch."""
        graph = build_diamond(env="prod")
        graph.run_graph()
        assert_branch(collect_node(graph, 6), live="a_prod", gated="a_dev", values=[1, 2, 3])

        set_parameter(graph, "env", "dev")
        second_run = graph.run_graph()

        assert second_run.success is True
        assert_branch(collect_node(graph, 6), live="a_dev", gated="a_prod", values=[1, 2, 3])
        # The reset must cascade through the tail, not stop at the union.
        assert_branch(collect_node(graph, 7), live="a_dev", gated="a_prod", values=[1, 2, 3])

        by_id = results_by_id(second_run)
        assert by_id[3].skipped is True
        assert by_id[5].skipped is False

    def test_flipping_back_restores_the_first_branch(self):
        graph = build_diamond(env="prod")
        graph.run_graph()
        set_parameter(graph, "env", "dev")
        graph.run_graph()
        set_parameter(graph, "env", "prod")
        third_run = graph.run_graph()

        assert third_run.success is True
        assert_branch(collect_node(graph, 7), live="a_prod", gated="a_dev", values=[1, 2, 3])

    def test_gated_branch_run_flags_are_cleared_so_it_can_run_again(self):
        graph = build_diamond(env="prod")
        graph.run_graph()

        gated = graph.get_node(5)
        assert gated.node_stats.has_run_with_current_setup is False
        assert gated._execution_state.has_run_with_current_setup is False

    def test_gates_pass_their_data_through_unchanged(self):
        graph = build_diamond(env="prod")
        graph.run_graph()

        source_df = collect_node(graph, 1)
        for gate_id in (2, 4):
            assert collect_node(graph, gate_id).to_dicts() == source_df.to_dicts()


class TestParameterOperators:
    """Every operator decides a real run, not just a unit-level boolean."""

    @staticmethod
    def _run_single_gate(flow_id: int, param: FlowParameter, **gate_kwargs) -> bool:
        """Build source -> gate -> select and return whether the downstream ran."""
        graph = create_graph(flow_id=flow_id)
        graph.flow_settings.parameters.append(param)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, **gate_kwargs)
        add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_kept")

        run_info = graph.run_graph()
        assert run_info.success is True
        return results_by_id(run_info)[3].skipped is False

    @pytest.mark.parametrize(
        "flow_id, param, gate_kwargs, expect_open",
        [
            (
                30,
                FlowParameter(name="env", default_value="prod"),
                {"parameter": "env", "operator": "not_equals", "value": "dev"},
                True,
            ),
            (
                31,
                FlowParameter(name="env", default_value="dev"),
                {"parameter": "env", "operator": "in", "value": "prod, staging"},
                False,
            ),
            (
                32,
                FlowParameter(name="env", default_value="staging"),
                {"parameter": "env", "operator": "in", "value": "prod, staging"},
                True,
            ),
            (
                33,
                FlowParameter(name="env", default_value="dev"),
                {"parameter": "env", "operator": "not_in", "value": "prod, staging"},
                True,
            ),
            (
                34,
                FlowParameter(name="full", default_value="true", type="boolean"),
                {"parameter": "full", "operator": "is_true"},
                True,
            ),
            (
                35,
                FlowParameter(name="full", default_value="false", type="boolean"),
                {"parameter": "full", "operator": "is_true"},
                False,
            ),
            (
                36,
                FlowParameter(name="full", default_value="false", type="boolean"),
                {"parameter": "full", "operator": "is_false"},
                True,
            ),
            (
                37,
                FlowParameter(name="who", default_value=""),
                {"parameter": "who", "operator": "is_set"},
                False,
            ),
            (
                38,
                FlowParameter(name="who", default_value="alice"),
                {"parameter": "who", "operator": "is_set"},
                True,
            ),
            (
                39,
                FlowParameter(name="rows", default_value="10", type="integer"),
                {"parameter": "rows", "operator": "equals", "value": "10"},
                True,
            ),
            (
                40,
                # Legacy shape: equals + the removed invert flag migrates to not_equals.
                FlowParameter(name="rows", default_value="10", type="integer"),
                {"parameter": "rows", "operator": "equals", "value": "10", "invert": True},
                False,
            ),
        ],
    )
    def test_operator_decides_the_downstream(self, flow_id, param, gate_kwargs, expect_open):
        assert self._run_single_gate(flow_id, param, **gate_kwargs) is expect_open


# B. Union semantics under gates


class TestUnionUnderGates:
    def test_all_inputs_gated_off_skips_the_union_and_stays_green(self):
        graph = create_graph(flow_id=2)
        graph.flow_settings.parameters.append(
            FlowParameter(name="env", default_value="other", type="string")
        )
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="value_x")
        add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_x")
        add_gate(graph, node_id=4, depending_on_id=1, parameter="env", operator="equals", value="value_y")
        add_rename_select(graph, node_id=5, depending_on_id=4, old="a", new="a_y")
        add_union(graph, node_id=6, depending_on_ids=[3, 5])
        add_passthrough_select(graph, node_id=7, depending_on_id=6)

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert {nr.node_id for nr in run_info.node_step_result if nr.skipped} == {3, 5, 6, 7}
        assert all(by_id[node_id].success is True for node_id in (3, 5, 6, 7))
        assert run_info.nodes_completed == run_info.number_of_nodes

    def test_failed_branch_blocks_the_union_and_never_reports_it_as_skipped(self, tmp_path):
        graph = create_graph(flow_id=3)
        graph.flow_settings.parameters.append(
            FlowParameter(name="env", default_value="prod", type="string")
        )
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="prod")
        add_passthrough_select(graph, node_id=3, depending_on_id=2)
        add_csv_read(graph, node_id=4, path=str(tmp_path / "does_not_exist.csv"))
        add_union(graph, node_id=5, depending_on_ids=[3, 4])

        run_info = graph.run_graph()

        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[4].success is False
        union = graph.get_node(5)
        assert union.results.resulting_data is None
        assert union.node_stats.has_run_with_current_setup is False
        assert 5 not in by_id
        assert 5 not in {nr.node_id for nr in run_info.node_step_result if nr.skipped}

    def test_error_beats_deliberate_even_when_a_live_branch_survives(self, tmp_path):
        """Healthy + gated-off + failing: the failure must still block the union."""
        graph = create_graph(flow_id=4)
        graph.flow_settings.parameters.append(
            FlowParameter(name="env", default_value="prod", type="string")
        )
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="prod")
        add_passthrough_select(graph, node_id=3, depending_on_id=2)
        add_gate(
            graph,
            node_id=4,
            depending_on_id=1,
            parameter="env",
            operator="not_equals",
            value="prod",
        )
        add_passthrough_select(graph, node_id=5, depending_on_id=4)
        add_csv_read(graph, node_id=6, path=str(tmp_path / "missing.csv"))
        add_union(graph, node_id=7, depending_on_ids=[3, 5, 6])

        run_info = graph.run_graph()

        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[5].skipped is True
        assert 7 not in by_id
        assert graph.get_node(7).results.resulting_data is None


# C. Formula gates


def build_formula_gate_graph(
    control_rows: list[dict],
    flow_id: int = 5,
    formula: str = "[flag]",
) -> FlowGraph:
    """1 data -> 2 gate (formula over control node 10) -> 3 select(a -> a_kept).

    Node 3 renames rather than passing through so it emits real code on export.
    """
    graph = create_graph(flow_id=flow_id)
    add_manual_input(graph, SOURCE_ROWS, node_id=1)
    add_manual_input(graph, control_rows, node_id=10)
    add_gate(
        graph,
        node_id=2,
        depending_on_id=1,
        control_id=10,
        condition_source="formula",
        formula=formula,
    )
    add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_kept")
    return graph


def build_self_gate_graph(flow_id: int, formula: str) -> FlowGraph:
    """1 data -> 2 gate (formula over its own data input) -> 3 select(a -> a_kept)."""
    graph = create_graph(flow_id=flow_id)
    add_manual_input(graph, SOURCE_ROWS, node_id=1)
    add_gate(graph, node_id=2, depending_on_id=1, condition_source="formula", formula=formula)
    add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_kept")
    return graph


class TestFormulaGate:
    def test_matching_formula_runs_the_downstream(self):
        graph = build_formula_gate_graph([{"flag": True}])

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[3].skipped is False
        assert collect_node(graph, 3).height == 3

    def test_non_matching_formula_skips_the_downstream_and_stays_green(self):
        graph = build_formula_gate_graph([{"flag": False}])

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[3].skipped is True
        assert by_id[3].success is True
        assert by_id[2].success is True and by_id[2].skipped is False

    def test_formula_checks_the_data_input_when_no_control_is_connected(self):
        open_graph = build_self_gate_graph(flow_id=6, formula="[a] > 2")
        closed_graph = build_self_gate_graph(flow_id=7, formula="[a] > 100")

        assert open_graph.run_graph().success is True
        assert results_by_id(open_graph.get_run_info())[3].skipped is False

        assert closed_graph.run_graph().success is True
        assert results_by_id(closed_graph.get_run_info())[3].skipped is True

    def test_any_row_matching_opens_the_gate(self):
        """Existence semantics: one matching row out of many is enough."""
        graph = build_formula_gate_graph(
            [{"flag": False}, {"flag": False}, {"flag": True}], flow_id=8
        )

        run_info = graph.run_graph()

        assert run_info.success is True
        assert results_by_id(run_info)[3].skipped is False

    @pytest.mark.parametrize(
        "formula, expected_open",
        [("[flag] = 'go'", True), ("[flag] = 'stop'", False)],
    )
    def test_string_comparison_formulas(self, formula, expected_open):
        graph = build_formula_gate_graph([{"flag": "go"}], flow_id=9, formula=formula)

        run_info = graph.run_graph()

        assert run_info.success is True
        assert results_by_id(run_info)[3].skipped is (not expected_open)

    def test_null_predicate_rows_never_match(self):
        """Polars filter semantics: rows where the predicate is null don't match.

        Unit-level: manual_input stringifies columns containing nulls, so the
        typed-null case is built directly on the matcher.
        """
        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        from flowfile_core.flowfile.flow_graph import _gate_formula_matches

        source = FlowDataEngine(pl.DataFrame({"n": [5, None]}))
        assert _gate_formula_matches(source, "[n] > 10") is False
        assert _gate_formula_matches(source, "[n] > 4") is True

    def test_control_input_takes_precedence_over_the_data_input(self):
        """With a control wired, the formula must see the control frame, not the data."""
        graph = build_formula_gate_graph([{"flag": False}], flow_id=12, formula="[flag]")

        run_info = graph.run_graph()

        # The data input has rows; only the control's flag=False can close this.
        assert run_info.success is True
        assert results_by_id(run_info)[3].skipped is True

    def test_parameter_ref_resolves_inside_the_formula(self):
        graph = build_self_gate_graph(flow_id=13, formula="[a] > ${threshold}")
        graph.flow_settings.parameters.append(
            FlowParameter(name="threshold", default_value="2", type="integer")
        )

        first = graph.run_graph()
        assert first.success is True
        assert results_by_id(first)[3].skipped is False

        set_parameter(graph, "threshold", "100")
        second = graph.run_graph()
        assert second.success is True
        assert results_by_id(second)[3].skipped is True

    def test_empty_control_frame_closes_the_gate(self):
        """No rows means no match — don't run the branch."""
        graph = create_graph(flow_id=14)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_manual_input(graph, [{"flag": True, "k": 1}], node_id=10)
        add_filter(graph, node_id=11, depending_on_id=10, field="k", value="999")
        add_gate(
            graph,
            node_id=2,
            depending_on_id=1,
            control_id=11,
            condition_source="formula",
            formula="[flag]",
        )
        add_passthrough_select(graph, node_id=3, depending_on_id=2)

        run_info = graph.run_graph()

        assert run_info.success is True
        assert collect_node(graph, 11).height == 0
        assert results_by_id(run_info)[3].skipped is True

    def test_formula_gate_re_evaluates_on_every_run(self):
        """The source data can change with no hash changing, so the gate must always run."""
        graph = build_formula_gate_graph([{"flag": True}], flow_id=15)

        first = graph.run_graph()
        assert first.success is True
        assert results_by_id(first)[3].skipped is False
        assert 2 in results_by_id(first)

        set_manual_input(graph, [{"flag": False}], node_id=10)
        second = graph.run_graph()
        assert second.success is True
        assert results_by_id(second)[3].skipped is True
        assert 2 in results_by_id(second), "the gate must execute again to decide"

        # Nothing changed: the decision must be re-made, not cached away.
        third = graph.run_graph()
        assert third.success is True
        assert results_by_id(third)[3].skipped is True
        assert 2 in results_by_id(third), "the gate must re-execute even with no settings change"
        assert would_re_execute(graph, 2) is True

    def test_only_formula_gates_bypass_the_development_cache(self):
        """Parameter gates keep the normal caching rule; formula gates never do."""
        parameter_graph = build_diamond(env="prod", flow_id=27)
        parameter_graph.run_graph()
        formula_graph = build_formula_gate_graph([{"flag": True}], flow_id=28)
        formula_graph.run_graph()

        assert would_re_execute(parameter_graph, 2) is False
        assert would_re_execute(formula_graph, 2) is True

    def test_formula_gate_reopens_when_the_control_data_flips_back(self):
        graph = build_formula_gate_graph([{"flag": False}], flow_id=16)
        graph.run_graph()
        assert results_by_id(graph.get_run_info())[3].skipped is True

        set_manual_input(graph, [{"flag": True}], node_id=10)
        run_info = graph.run_graph()

        assert run_info.success is True
        assert results_by_id(run_info)[3].skipped is False
        assert collect_node(graph, 3).height == 3

    @pytest.mark.parametrize(
        "formula, expect_open",
        [("[flag] = 'reject'", True), ("[flag] = 'keep'", False)],
    )
    def test_control_wired_to_a_secondary_output_probes_that_frame(self, formula, expect_open):
        """A split filter's fail side feeds the control pip: the formula must
        run against output-1's rows, not the source's default (pass) output —
        'reject' rows exist only on the fail side, 'keep' rows only on pass."""
        graph = create_graph(flow_id=76)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_manual_input(graph, [{"flag": "keep"}, {"flag": "reject"}], node_id=10)
        add_filter(graph, node_id=11, depending_on_id=10, field="flag", value="keep", split_mode=True)
        add_promise(graph, "gate", 2)
        connect(graph, 1, 2)
        connect(graph, 11, 2, input_type="right", output_handle="output-1")
        graph.add_gate(
            input_schema.NodeGate(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                is_setup=True,
                gate_input=transform_schema.GateInput(condition_source="formula", formula=formula),
            )
        )
        add_passthrough_select(graph, node_id=3, depending_on_id=2)

        run_info = graph.run_graph()

        assert run_info.success is True
        assert results_by_id(run_info)[3].skipped is (not expect_open)

    def test_broken_formula_fails_the_gate_loudly(self):
        graph = build_formula_gate_graph([{"flag": True}], flow_id=29, formula="[no_such_col] = 1")

        run_info = graph.run_graph()

        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[2].success is False
        assert "formula" in by_id[2].error.lower()
        # Error-driven skip: no green skipped rows downstream.
        assert 3 not in by_id
        assert [nr.node_id for nr in run_info.node_step_result if nr.skipped] == []

    def test_empty_formula_fails_the_gate_loudly(self):
        graph = build_formula_gate_graph([{"flag": True}], flow_id=30, formula="   ")

        run_info = graph.run_graph()

        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[2].success is False
        assert "formula" in by_id[2].error.lower()


# D. Failure modes


class TestGateFailureModes:
    def test_is_true_coercion_failure_raises_at_the_schema_level(self):
        gate = transform_schema.GateInput(parameter="full", operator="is_true")
        with pytest.raises(ValueError, match="not a valid boolean"):
            gate.evaluate({"full": FlowParameter(name="full", default_value="banana")})

    @pytest.mark.parametrize("operator", ["is_true", "is_false"])
    def test_non_boolean_value_fails_a_truthiness_gate_loudly(self, operator):
        """No silent coercion: 'banana' must fail the gate, never route a branch."""
        graph = create_graph(flow_id=70)
        graph.flow_settings.parameters.append(FlowParameter(name="full", default_value="banana"))
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="full", operator=operator)
        add_passthrough_select(graph, node_id=3, depending_on_id=2)

        run_info = graph.run_graph()

        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[2].success is False
        assert "boolean" in by_id[2].error.lower()
        # Error-driven skip: no green skipped rows downstream.
        assert 3 not in by_id
        assert [nr.node_id for nr in run_info.node_step_result if nr.skipped] == []

    def test_unknown_parameter_fails_the_gate_and_error_skips_downstream(self):
        graph = create_graph(flow_id=17)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="missing", operator="equals", value="x")
        add_passthrough_select(graph, node_id=3, depending_on_id=2)

        run_info = graph.run_graph()

        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[2].success is False
        assert "missing" in by_id[2].error
        # An error-driven skip produces no row at all — never a green skipped one.
        assert 3 not in by_id
        assert [nr.node_id for nr in run_info.node_step_result if nr.skipped] == []


# E. Gate-blind callers


class TestGateBlindCallers:
    def test_single_node_fetch_is_allowed_downstream_of_a_closed_gate(self):
        graph = build_diamond(env="prod")

        # node 5 sits behind the closed (complementary) gate 4
        graph.validate_if_node_can_be_fetched(5)

    def test_plan_without_closed_gate_handles_has_no_deliberate_skips(self):
        graph = build_diamond(env="prod")

        plan = compute_execution_plan(
            nodes=graph.nodes,
            flow_starts=graph._flow_starts + graph.get_implicit_starter_nodes(),
        )

        assert plan.deliberate_skip_nodes == []
        assert plan.skip_nodes == []
        assert plan.node_count == len(graph.nodes)

    def test_plan_with_closed_gate_handles_marks_the_branch_deliberate(self):
        graph = build_diamond(env="prod")

        plan = compute_execution_plan(
            nodes=graph.nodes,
            flow_starts=graph._flow_starts + graph.get_implicit_starter_nodes(),
            closed_gate_handles={4: frozenset({"output-0", "output-1"})},
        )

        assert [node.node_id for node in plan.deliberate_skip_nodes] == [5]
        assert plan.skip_nodes == []


# F. Code export


def run_generated(code: str, **kwargs) -> pl.DataFrame:
    namespace: dict = {}
    exec(code, namespace)
    result = namespace["run_etl_pipeline"](**kwargs)
    # Duck-typed: a Polars module returns a pl.LazyFrame, a FlowFrame module a
    # FlowFrame — both collect to a pl.DataFrame.
    if hasattr(result, "collect"):
        return result.collect()
    return result


class TestPolarsExport:
    def test_diamond_export_emits_real_if_blocks(self):
        graph = build_diamond(env="prod", flow_id=18)

        code = FlowGraphToPolarsConverter(graph).convert()

        assert "if env == 'prod':" in code
        assert "if env != 'prod':" in code
        assert "def run_etl_pipeline(*, env" in code
        # Gated union inputs append to a list under their guards — no stand-in
        # substitution, no None-filtering listcomp.
        assert "df_6_frames = []" in code
        assert "df_6_frames.append(df_3)" in code
        assert "df_6_frames.append(df_5)" in code
        assert "if df is not None" not in code
        # The terminal select 7 is ungated, so no branch var pre-inits at all.
        assert "df_3 = pl.LazyFrame(schema=" not in code
        assert "df_5 = pl.LazyFrame(schema=" not in code

    @pytest.mark.parametrize("env", ["prod", "dev"])
    def test_generated_code_matches_the_engine_for_both_parameter_values(self, env):
        graph = build_diamond(env=env, flow_id=19)
        code = FlowGraphToPolarsConverter(graph).convert()
        graph.run_graph()
        engine_df = collect_node(graph, 7)

        generated_df = run_generated(code, env=env)

        assert set(generated_df.columns) == set(engine_df.columns)
        generated_df = generated_df.select(engine_df.columns)
        assert generated_df.to_dicts() == engine_df.to_dicts()

    def test_one_exported_module_serves_both_parameter_values(self):
        """The condition is a real ``if`` on the kwarg, not a baked-in branch."""
        graph = build_diamond(env="prod", flow_id=20)
        code = FlowGraphToPolarsConverter(graph).convert()

        prod_df = run_generated(code, env="prod")
        dev_df = run_generated(code, env="dev")

        assert prod_df["a_prod"].to_list() == [1, 2, 3]
        assert "a_dev" not in prod_df.columns
        assert dev_df["a_dev"].to_list() == [1, 2, 3]
        assert "a_prod" not in dev_df.columns

    @pytest.mark.parametrize("flag, expect_rows", [(True, 3), (False, 0)])
    def test_formula_gate_export_uses_the_formula_helper(self, flag, expect_rows):
        graph = build_formula_gate_graph([{"flag": flag}], flow_id=21)

        code = FlowGraphToPolarsConverter(graph).convert()

        assert "_flowfile_gate_formula_matches" in code
        assert "simple_function_to_expr" in code
        assert "if _gate_2_open:" in code
        result = run_generated(code)
        assert result.columns == ["a_kept"]
        assert result.height == expect_rows

    def test_parameter_ref_inside_a_formula_survives_export(self):
        """${refs} in the formula become live references to the function's kwargs."""
        graph = build_self_gate_graph(flow_id=32, formula="[a] > ${threshold}")
        graph.flow_settings.parameters.append(
            FlowParameter(name="threshold", default_value="2", type="integer")
        )

        code = FlowGraphToPolarsConverter(graph).convert()

        assert run_generated(code, threshold=2).height == 3
        assert run_generated(code, threshold=100).height == 0

    @pytest.mark.parametrize("formula, expect_rows", [("[a] > 2", 3), ("[a] > 100", 0)])
    def test_self_gate_export_checks_the_data_input(self, formula, expect_rows):
        graph = build_self_gate_graph(flow_id=31, formula=formula)

        code = FlowGraphToPolarsConverter(graph).convert()

        graph.run_graph()
        result = run_generated(code)
        assert result.height == expect_rows
        engine_result = graph.get_node(3).results.resulting_data
        engine_height = engine_result.data_frame.lazy().collect().height if engine_result else 0
        assert result.height == engine_height

    def test_gated_passthrough_terminal_node_matches_the_engine(self):
        """A no-op passthrough behind a gate must not be elided from the export.

        Eliding it would resolve the return to the ungated upstream variable —
        data leaking past a closed gate. The generator now emits an explicit
        alias inside the gate's block, so the closed-gate export returns the
        empty schema-typed frame instead.
        """
        graph = create_graph(flow_id=26)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_manual_input(graph, [{"flag": False}], node_id=10)
        add_gate(
            graph,
            node_id=2,
            depending_on_id=1,
            control_id=10,
            condition_source="formula",
            formula="[flag]",
        )
        add_passthrough_select(graph, node_id=3, depending_on_id=2)
        code = FlowGraphToPolarsConverter(graph).convert()

        run_info = graph.run_graph()

        assert results_by_id(run_info)[3].skipped is True
        assert graph.get_node(3).results.resulting_data is None
        assert run_generated(code).height == 0

class TestFlowFrameExport:
    """The FlowFrame export emits the same real if-blocks as the Polars export.

    A ``df.gate(...)`` emission would be wrong here: the generated module
    returns a FlowFrame whose ``collect()`` deliberately ignores gates, so the
    condition has to live in the exported Python itself. Gated union inputs
    append to a list under their guards (same list-append shape as the Polars
    export) and the formula helper probes the FlowFrame's underlying LazyFrame
    via ``.data``.
    """

    def test_diamond_export_emits_real_if_blocks(self):
        graph = build_diamond(env="prod", flow_id=40)

        code = FlowGraphToFlowFrameConverter(graph).convert()

        assert "if env == 'prod':" in code
        assert "if env != 'prod':" in code
        assert "def run_etl_pipeline(*, env" in code
        assert "import flowfile as ff" in code
        assert "import polars as pl" in code
        # Gated union inputs append to a list under their guards.
        assert "df_6_frames = []" in code
        assert "df_6_frames.append(df_3)" in code
        assert "df_6_frames.append(df_5)" in code
        assert "if df is not None" not in code
        assert "ff.concat(df_6_frames" in code
        # The terminal select 7 is ungated, so no branch var pre-inits at all.
        assert "df_3 = ff.FlowFrame(" not in code
        assert "df_5 = ff.FlowFrame(" not in code

    @pytest.mark.parametrize("env", ["prod", "dev"])
    def test_generated_code_matches_the_engine_for_both_parameter_values(self, env):
        graph = build_diamond(env=env, flow_id=41)
        code = FlowGraphToFlowFrameConverter(graph).convert()
        graph.run_graph()
        engine_df = collect_node(graph, 7)

        generated_df = run_generated(code, env=env)

        assert set(generated_df.columns) == set(engine_df.columns)
        generated_df = generated_df.select(engine_df.columns)
        assert generated_df.to_dicts() == engine_df.to_dicts()

    def test_one_exported_module_serves_both_parameter_values(self):
        graph = build_diamond(env="prod", flow_id=42)
        code = FlowGraphToFlowFrameConverter(graph).convert()

        prod_df = run_generated(code, env="prod")
        dev_df = run_generated(code, env="dev")

        assert prod_df["a_prod"].to_list() == [1, 2, 3]
        assert "a_dev" not in prod_df.columns
        assert dev_df["a_dev"].to_list() == [1, 2, 3]
        assert "a_prod" not in dev_df.columns

    @pytest.mark.parametrize("flag, expect_rows", [(True, 3), (False, 0)])
    def test_formula_gate_export_probes_the_lazyframe(self, flag, expect_rows):
        graph = build_formula_gate_graph([{"flag": flag}], flow_id=43)

        code = FlowGraphToFlowFrameConverter(graph).convert()

        assert "_flowfile_gate_formula_matches" in code
        assert ".data, simple_function_to_expr" in code
        assert "if _gate_2_open:" in code
        result = run_generated(code)
        assert result.columns == ["a_kept"]
        assert result.height == expect_rows

    def test_parameter_ref_inside_a_formula_survives_export(self):
        graph = build_self_gate_graph(flow_id=44, formula="[a] > ${threshold}")
        graph.flow_settings.parameters.append(
            FlowParameter(name="threshold", default_value="2", type="integer")
        )

        code = FlowGraphToFlowFrameConverter(graph).convert()

        assert run_generated(code, threshold=2).height == 3
        assert run_generated(code, threshold=100).height == 0

    @pytest.mark.parametrize("formula, expect_rows", [("[a] > 2", 3), ("[a] > 100", 0)])
    def test_self_gate_export_checks_the_data_input(self, formula, expect_rows):
        graph = build_self_gate_graph(flow_id=45, formula=formula)

        code = FlowGraphToFlowFrameConverter(graph).convert()

        assert run_generated(code).height == expect_rows

    def test_gated_passthrough_terminal_returns_the_empty_stand_in(self):
        graph = create_graph(flow_id=46)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_manual_input(graph, [{"flag": False}], node_id=10)
        add_gate(
            graph,
            node_id=2,
            depending_on_id=1,
            control_id=10,
            condition_source="formula",
            formula="[flag]",
        )
        add_passthrough_select(graph, node_id=3, depending_on_id=2)

        code = FlowGraphToFlowFrameConverter(graph).convert()

        assert run_generated(code).height == 0

    def test_empty_formula_still_refuses(self):
        graph = build_self_gate_graph(flow_id=47, formula="")

        with pytest.raises(UnsupportedNodeError) as exc_info:
            FlowGraphToFlowFrameConverter(graph).convert()

        assert "formula" in str(exc_info.value).lower()

    def test_unknown_parameter_still_refuses(self):
        graph = create_graph(flow_id=48)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="ghost", operator="equals", value="x")
        add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_kept")

        with pytest.raises(UnsupportedNodeError) as exc_info:
            FlowGraphToFlowFrameConverter(graph).convert()

        assert "ghost" in str(exc_info.value)


class TestWholeFieldParameterFormulaExport:
    """A formula that is exactly ``${param}`` must reach the parser stringified.

    The sentinel post-pass rewrites a whole-field ref to the bare typed kwarg,
    which would hand ``simple_function_to_expr`` a raw bool/int (TypeError at
    run time). The export mirrors the engine's ``resolve_parameters`` instead:
    bools render ``'true'``/``'false'``, everything else through ``str(...)``.
    """

    @staticmethod
    def _build(flow_id: int, param: FlowParameter) -> FlowGraph:
        graph = build_self_gate_graph(flow_id=flow_id, formula=f"${{{param.name}}}")
        graph.flow_settings.parameters.append(param)
        return graph

    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    @pytest.mark.parametrize("flag", [True, False])
    def test_boolean_parameter_routes_like_the_engine(self, converter, flag):
        param = FlowParameter(name="flag", default_value="true" if flag else "false", type="boolean")
        graph = self._build(flow_id=71, param=param)

        code = converter(graph).convert()

        assert "simple_function_to_expr(('true' if flag else 'false'))" in code
        graph.run_graph()
        engine_result = graph.get_node(3).results.resulting_data
        engine_height = engine_result.data_frame.lazy().collect().height if engine_result else 0
        generated_df = run_generated(code, flag=flag)
        assert generated_df.height == engine_height == (3 if flag else 0)

    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    def test_integer_parameter_fails_like_the_engine(self, converter):
        """A bare integer is not a boolean predicate, so the engine fails the
        gate loudly — the generated module must fail at the same point with
        the same stringified formula, not TypeError inside the parser."""
        param = FlowParameter(name="n", default_value="2", type="integer")
        graph = self._build(flow_id=72, param=param)

        code = converter(graph).convert()

        assert "simple_function_to_expr(str(n))" in code
        run_info = graph.run_graph()
        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[2].success is False
        assert "boolean" in by_id[2].error.lower()
        with pytest.raises(pl.exceptions.ComputeError, match="Boolean"):
            run_generated(code, n=2)


class TestTypedParamRefComparisonExport:
    """A typed gate value holding ``${ref}`` runs in the engine but not in export."""

    @staticmethod
    def _build(flow_id: int) -> FlowGraph:
        graph = create_graph(flow_id=flow_id)
        graph.flow_settings.parameters.append(FlowParameter(name="rows", default_value="10", type="integer"))
        graph.flow_settings.parameters.append(FlowParameter(name="limit", default_value="10", type="integer"))
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="rows", operator="equals", value="${limit}")
        add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_kept")
        return graph

    def test_engine_resolves_the_ref_and_routes(self):
        graph = self._build(flow_id=73)

        first = graph.run_graph()
        assert first.success is True
        assert results_by_id(first)[3].skipped is False

        set_parameter(graph, "limit", "99")
        second = graph.run_graph()
        assert second.success is True
        assert results_by_id(second)[3].skipped is True

    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    def test_export_refuses_with_an_accurate_message(self, converter):
        graph = self._build(flow_id=74)

        with pytest.raises(UnsupportedNodeError) as exc_info:
            converter(graph).convert()

        message = str(exc_info.value)
        assert "typed parameter-to-parameter comparison is not supported" in message
        assert "${limit}" in message
        assert "does not exist" not in message


class TestAllGatedOffUnionExport:
    """Pins the deliberate engine/export divergence when every union input closes.

    Engine: the union and its downstream are deliberately skipped (green
    skipped rows, no result). Export: the guarded append list stays empty, the
    generator's fallback appends an empty schema-typed frame, and downstream
    runs to a zero-row result. Accepted behavior — documented next to the
    ``if not df_6_frames`` explanation in
    docs/users/visual-editor/tutorials/code-generator.md. Built with two
    INDEPENDENT gates (build_diamond-style, not one gate's then/else pair) so
    the list+fallback emission form is what this pins.
    """

    @staticmethod
    def _build(flow_id: int) -> FlowGraph:
        graph = create_graph(flow_id=flow_id)
        graph.flow_settings.parameters.append(
            FlowParameter(name="env", default_value="other", type="string")
        )
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="value_x")
        add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_x")
        add_gate(graph, node_id=4, depending_on_id=1, parameter="env", operator="equals", value="value_y")
        add_rename_select(graph, node_id=5, depending_on_id=4, old="a", new="a_y")
        add_union(graph, node_id=6, depending_on_ids=[3, 5])
        add_passthrough_select(graph, node_id=7, depending_on_id=6)
        return graph

    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    def test_all_closed_export_returns_the_empty_stand_in_while_the_engine_skips(self, converter):
        graph = self._build(flow_id=75)

        code = converter(graph).convert()

        assert "df_6_frames = []" in code
        assert "if not df_6_frames:" in code

        run_info = graph.run_graph()
        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[6].skipped is True
        assert by_id[7].skipped is True
        assert graph.get_node(6).results.resulting_data is None

        generated_df = run_generated(code, env="other")
        assert generated_df.height == 0
        assert set(generated_df.columns) == {"a_x", "a_y"}


# G. YAML round-trip


class TestGateYamlRoundTrip:
    def test_gate_settings_survive_save_and_open(self, tmp_path):
        graph = build_diamond(env="prod", flow_id=23)
        # Make every gate field non-default so a dropped field is visible.
        gate = graph.get_node(4).setting_input.gate_input
        gate.operator = "in"
        gate.value = "prod,staging"
        path = tmp_path / "gate_flow.yaml"
        graph.save_flow(str(path))

        reopened = open_flow(Path(path))

        original = graph.get_node(4).setting_input.gate_input
        restored = reopened.get_node(4).setting_input.gate_input
        assert restored.condition_source == original.condition_source == "parameter"
        assert restored.parameter == "env"
        assert restored.operator == "in"
        assert restored.value == "prod,staging"

    def test_formula_gate_settings_survive_save_and_open(self, tmp_path):
        graph = build_formula_gate_graph([{"flag": True}], flow_id=24, formula="[flag] = 'go'")
        path = tmp_path / "formula_gate_flow.yaml"
        graph.save_flow(str(path))

        reopened = open_flow(Path(path))

        restored = reopened.get_node(2).setting_input.gate_input
        assert restored.condition_source == "formula"
        assert restored.formula == "[flag] = 'go'"

    def test_reopened_flow_produces_the_same_result(self, tmp_path):
        graph = build_diamond(env="prod", flow_id=25)
        path = tmp_path / "gate_roundtrip.yaml"
        graph.save_flow(str(path))
        graph.run_graph()
        expected = collect_node(graph, 7)

        reopened = open_flow(Path(path))
        reopened.flow_settings.execution_location = "local"
        run_info = reopened.run_graph()

        assert run_info.success is True
        assert [nr.node_id for nr in run_info.node_step_result if nr.skipped] == [5]
        assert collect_node(reopened, 7).to_dicts() == expected.to_dicts()


class TestGateLegacyMigration:
    """Pre-formula gate settings (control_input source, invert flag) migrate on load."""

    def test_control_input_source_becomes_formula(self):
        restored = transform_schema.GateInput.model_validate(
            {"condition_source": "control_input", "control_column": "flag", "invert": True}
        )
        assert restored.condition_source == "formula"
        assert restored.formula == ""

    @pytest.mark.parametrize(
        "operator, flipped",
        [
            ("equals", "not_equals"),
            ("not_equals", "equals"),
            ("in", "not_in"),
            ("not_in", "in"),
            ("is_true", "is_false"),
            ("is_false", "is_true"),
        ],
    )
    def test_inverted_parameter_gate_flips_to_the_complementary_operator(self, operator, flipped):
        restored = transform_schema.GateInput.model_validate(
            {"condition_source": "parameter", "parameter": "env", "operator": operator, "invert": True}
        )
        assert restored.operator == flipped

    def test_uninverted_parameter_gate_keeps_its_operator(self):
        restored = transform_schema.GateInput.model_validate(
            {"condition_source": "parameter", "parameter": "env", "operator": "equals", "invert": False}
        )
        assert restored.operator == "equals"


class TestAdversarialReviewRegressions:
    """Pins for defects found (and fixed) by the adversarial review pass."""

    def test_gate_wired_directly_into_union_is_guarded_in_export(self):
        graph = create_graph(flow_id=40)
        graph.flow_settings.parameters.append(FlowParameter(name="env", default_value="prod", type="string"))
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="prod")
        add_gate(
            graph, node_id=4, depending_on_id=1, parameter="env", operator="not_equals", value="prod"
        )
        add_union(graph, node_id=6, depending_on_ids=[2, 4])

        code = FlowGraphToPolarsConverter(graph).convert()
        # Both union inputs come straight off a gate, so both appends are guarded
        # and the list needs the empty-frame fallback for the all-closed case.
        assert "df_6_frames = []" in code
        assert "if env == 'prod':\n        df_6_frames.append(df_1)" in code
        assert "if env != 'prod':\n        df_6_frames.append(df_1)" in code
        assert "\n    df_6_frames.append(" not in code
        assert "if not df_6_frames:\n        df_6_frames.append(pl.LazyFrame(schema=" in code
        assert "df_6 = pl.concat(df_6_frames, how='diagonal_relaxed')" in code
        for env_value in ("prod", "dev"):
            generated = run_generated(code, env=env_value)
            graph.flow_settings.parameters[0].default_value = env_value
            run_info = graph.run_graph()
            engine = collect_node(graph, 6)
            assert run_info.success is True
            assert generated.height == engine.height == 3, (env_value, generated.height, engine.height)
            assert generated.columns == engine.columns == ["a"]

    def test_gate_with_only_control_connected_is_not_runnable(self):
        graph = create_graph(flow_id=41)
        add_manual_input(graph, [{"flag": True}], node_id=10)
        add_promise(graph, "gate", 2)
        connect(graph, 10, 2, input_type="right")
        graph.add_gate(
            input_schema.NodeGate(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=-1,
                is_setup=True,
                gate_input=transform_schema.GateInput(condition_source="formula", formula="[flag]"),
            )
        )
        gate_node = graph.get_node(2)
        assert gate_node.is_correct is False

        run_info = graph.run_graph()
        assert all(nr.node_id != 2 for nr in run_info.node_step_result)

    def test_deleted_parameter_after_green_run_fails_loudly(self):
        graph = build_diamond(env="prod", flow_id=42)
        first = graph.run_graph()
        assert first.success is True

        graph.flow_settings.parameters.clear()
        second = graph.run_graph()

        assert second.success is False
        gate_results = [nr for nr in second.node_step_result if nr.node_id in (2, 4) and nr.success is False]
        assert gate_results, "at least one gate must fail visibly when its parameter disappears"
        assert any("parameter" in nr.error.lower() for nr in gate_results)

    def test_nested_dtype_rendering_is_importable_python(self):
        from flowfile_core.flowfile.code_generator.code_generator import _render_polars_dtype

        for dtype in (
            pl.List(pl.Int64),
            pl.Struct({"a": pl.String, "b": pl.List(pl.Float64)}),
            pl.Datetime("us", "Europe/Amsterdam"),
            pl.Array(pl.Int32, 3),
        ):
            rendered = _render_polars_dtype(dtype)
            assert eval(rendered, {"pl": pl}) == dtype

    def test_empty_frame_schema_expr_falls_back_on_lossy_schema_strings(self):
        """A schema string that can't be cast back (bare 'List') degrades to None, never an invalid literal."""

        class LossyColumn:
            name = "a"
            data_type = "List"

        class StubNode:
            schema = [LossyColumn()]

        assert FlowGraphToPolarsConverter._empty_frame_schema_expr(StubNode()) is None


# H. Else output (one-gate if/else)


class TestElseOutputDiamond:
    @pytest.mark.parametrize(
        "env, live, gated, live_node, gated_node",
        [("prod", "a_prod", "a_dev", 3, 5), ("dev", "a_dev", "a_prod", 5, 3)],
    )
    def test_exactly_one_branch_runs_per_parameter_value(self, env, live, gated, live_node, gated_node):
        graph = build_split_diamond(env=env, flow_id=50)

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[2].success is True and by_id[2].skipped is False
        assert by_id[gated_node].skipped is True and by_id[gated_node].success is True
        assert by_id[live_node].skipped is False
        assert run_info.nodes_completed == run_info.number_of_nodes
        assert_branch(collect_node(graph, 6), live, gated, [1, 2, 3])

    def test_flipping_the_parameter_reroutes_the_live_graph(self):
        graph = build_split_diamond(env="prod", flow_id=51)
        assert graph.run_graph().success is True
        assert_branch(collect_node(graph, 6), "a_prod", "a_dev", [1, 2, 3])

        set_parameter(graph, "env", "dev")

        assert graph.run_graph().success is True
        assert_branch(collect_node(graph, 6), "a_dev", "a_prod", [1, 2, 3])

    def test_closed_unpaired_gate_kills_a_stale_else_edge(self):
        """Toggling else_output off leaves a dangling output-1 edge; a closed
        gate must not leak data through it."""
        graph = build_split_diamond(env="dev", flow_id=52)
        graph.get_node(2).setting_input.else_output = False

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[3].skipped is True
        assert by_id[5].skipped is True
        assert by_id[6].skipped is True

    def test_open_unpaired_gate_serves_a_stale_else_edge_like_then(self):
        graph = build_split_diamond(env="prod", flow_id=53)
        graph.get_node(2).setting_input.else_output = False

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[3].skipped is False
        assert by_id[5].skipped is False


class TestElseOutputFormulaGate:
    def build(self, control_rows: list[dict], flow_id: int) -> FlowGraph:
        """1 data + 10 control -> 2 gate([flag], else_output) -> then 3 / else 5 -> 6 union."""
        graph = create_graph(flow_id=flow_id)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_manual_input(graph, control_rows, node_id=10)
        add_gate(
            graph,
            node_id=2,
            depending_on_id=1,
            control_id=10,
            else_output=True,
            condition_source="formula",
            formula="[flag]",
        )
        add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_then")
        add_rename_select(graph, node_id=5, depending_on_id=2, old="a", new="a_else", output_handle="output-1")
        add_union(graph, node_id=6, depending_on_ids=[3, 5])
        return graph

    @pytest.mark.parametrize(
        "flag, live, gated, live_node, gated_node",
        [(True, "a_then", "a_else", 3, 5), (False, "a_else", "a_then", 5, 3)],
    )
    def test_formula_routes_exactly_one_side(self, flag, live, gated, live_node, gated_node):
        graph = self.build([{"flag": flag}], flow_id=54)

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[gated_node].skipped is True
        assert by_id[live_node].skipped is False
        assert_branch(collect_node(graph, 6), live, gated, [1, 2, 3])

    def test_routing_re_evaluates_when_the_control_data_flips(self):
        graph = self.build([{"flag": True}], flow_id=55)
        assert graph.run_graph().success is True
        assert results_by_id(graph.get_run_info())[5].skipped is True

        set_manual_input(graph, [{"flag": False}], node_id=10)

        assert graph.run_graph().success is True
        by_id = results_by_id(graph.get_run_info())
        assert by_id[3].skipped is True
        assert by_id[5].skipped is False

    def test_broken_formula_fails_the_gate_and_error_skips_both_sides(self):
        graph = self.build([{"flag": True}], flow_id=56)
        graph.get_node(2).setting_input.gate_input.formula = "[no_such_column] > 1"

        run_info = graph.run_graph()

        assert run_info.success is False
        by_id = results_by_id(run_info)
        assert by_id[2].success is False
        assert 3 not in by_id
        assert 5 not in by_id
        assert 6 not in by_id


class TestElseOutputYamlRoundTrip:
    def test_else_output_and_edge_survive_save_and_open(self, tmp_path):
        graph = build_split_diamond(env="prod", flow_id=57)
        path = tmp_path / "split_gate_flow.yaml"
        graph.save_flow(str(path))

        reopened = open_flow(Path(path))

        restored = reopened.get_node(2).setting_input
        assert restored.else_output is True
        assert reopened.get_node(5)._input_output_handles[2] == "output-1"
        assert reopened.get_node(3)._input_output_handles[2] == "output-0"

        run_info = reopened.run_graph()
        assert run_info.success is True
        assert_branch(collect_node(reopened, 6), "a_prod", "a_dev", [1, 2, 3])


class TestDoubleWiredGateExits:
    """One target may read a single output handle per source node.

    ``_input_output_handles`` keys one handle per source, so a second edge from
    the same gate re-keys the first: both exits would read one side, silently
    dropping the live branch or duplicating it.
    """

    @staticmethod
    def _build(flow_id: int) -> FlowGraph:
        """1 manual_input -> 2 gate(env == prod, else_output), with union 6 waiting."""
        graph = create_graph(flow_id=flow_id)
        graph.flow_settings.parameters.append(FlowParameter(name="env", default_value="prod", type="string"))
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(
            graph,
            node_id=2,
            depending_on_id=1,
            else_output=True,
            parameter="env",
            operator="equals",
            value="prod",
        )
        add_promise(graph, "union", 6)
        return graph

    def test_wiring_both_exits_into_one_union_is_rejected(self):
        graph = self._build(flow_id=61)
        connect(graph, 2, 6)

        with pytest.raises(HTTPException) as exc_info:
            connect(graph, 2, 6, output_handle="output-1")

        assert exc_info.value.status_code == 422
        assert "output-0" in exc_info.value.detail
        union = graph.get_node(6)
        assert [node.node_id for node in union.all_inputs] == [2]
        assert union._input_output_handles[2] == "output-0"

    def test_disconnecting_first_allows_rewiring_the_other_exit(self):
        graph = self._build(flow_id=62)
        connect(graph, 2, 6)

        delete_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 6))
        connect(graph, 2, 6, output_handle="output-1")

        union = graph.get_node(6)
        assert [node.node_id for node in union.all_inputs] == [2]
        assert union._input_output_handles[2] == "output-1"

    def test_same_handle_double_edge_still_connects(self):
        """A self-join reads one source twice — same handle, so nothing is re-keyed."""
        graph = create_graph(flow_id=63)
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_passthrough_select(graph, node_id=2, depending_on_id=1)
        add_promise(graph, "join", 3)

        connect(graph, 2, 3)
        connect(graph, 2, 3, input_type="right")

        join = graph.get_node(3)
        assert [node.node_id for node in join.all_inputs] == [2, 2]
        assert join._input_output_handles[2] == "output-0"

    def test_the_same_exit_pair_may_feed_two_different_targets(self):
        """The normal then/else split: two targets, one handle each."""
        graph = build_split_diamond(env="prod", flow_id=64)

        assert graph.get_node(3)._input_output_handles[2] == "output-0"
        assert graph.get_node(5)._input_output_handles[2] == "output-1"


class TestElseOutputExport:
    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    def test_split_gate_emits_if_else_blocks(self, converter):
        graph = build_split_diamond(env="prod", flow_id=58)

        code = converter(graph).convert()

        assert "if env == 'prod':" in code
        assert "else:" in code
        assert "if not (env == 'prod'):" not in code

    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    @pytest.mark.parametrize("env", ["prod", "dev"])
    def test_split_gate_engine_parity_both_values(self, converter, env):
        graph = build_split_diamond(env=env, flow_id=59)
        code = converter(graph).convert()
        graph.run_graph()
        engine_df = collect_node(graph, 7)

        generated_df = run_generated(code, env=env)

        assert set(generated_df.columns) == set(engine_df.columns)
        generated_df = generated_df.select(engine_df.columns)
        assert generated_df.to_dicts() == engine_df.to_dicts()

    @pytest.mark.parametrize(
        "flag, live, gated",
        [(True, "a_then", "a_else"), (False, "a_else", "a_then")],
    )
    def test_formula_split_gate_emits_if_else_flag_blocks(self, flag, live, gated):
        graph = TestElseOutputFormulaGate().build([{"flag": flag}], flow_id=60)

        code = FlowGraphToPolarsConverter(graph).convert()

        assert "if _gate_2_open:" in code
        assert "else:" in code
        assert "if not (_gate_2_open):" not in code
        assert "_frames" not in code
        assert_branch(run_generated(code), live, gated, [1, 2, 3])

    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    @pytest.mark.parametrize("env", ["prod", "dev"])
    def test_union_renders_as_if_else_assignment_and_executes(self, converter, env):
        """The complementary-pair union is a plain if/else assignment — no list, no concat."""
        graph = build_split_diamond(env=env, flow_id=61)
        code = converter(graph).convert()

        assert "df_6 = df_3" in code
        assert "df_6 = df_5" in code
        assert "_frames" not in code
        assert "concat" not in code

        graph.run_graph()
        engine_df = collect_node(graph, 7)
        generated_df = run_generated(code, env=env)
        assert set(generated_df.columns) == set(engine_df.columns)
        assert generated_df.select(engine_df.columns).to_dicts() == engine_df.to_dicts()


class TestComplementaryPairNegativeSpace:
    """Only one gate's exactly-complementary singleton pair fuses to if/else.

    Different gates and shared-prefix conjunctions keep today's rendering:
    separate if blocks, list-append unions, and the empty-frame fallback
    whenever the guard set isn't provably exhaustive.
    """

    def test_independent_gate_diamond_keeps_list_form_and_fallback(self):
        graph = build_diamond(env="prod", flow_id=62)

        code = FlowGraphToPolarsConverter(graph).convert()

        assert "else:" not in code
        assert "df_6_frames = []" in code
        assert "if not df_6_frames:" in code
        assert "df_6 = pl.concat(df_6_frames, how='diagonal_relaxed')" in code

    @staticmethod
    def _build_nested(flow_id: int) -> FlowGraph:
        """An else_output split nested under an outer gate: P∧g / P∧¬g branch blocks."""
        graph = create_graph(flow_id=flow_id)
        graph.flow_settings.parameters.append(FlowParameter(name="env", default_value="prod", type="string"))
        graph.flow_settings.parameters.append(FlowParameter(name="mode", default_value="x", type="string"))
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="prod")
        add_gate(
            graph, node_id=4, depending_on_id=2, else_output=True, parameter="mode", operator="equals", value="x"
        )
        add_rename_select(graph, node_id=5, depending_on_id=4, old="a", new="a_then")
        add_rename_select(graph, node_id=6, depending_on_id=4, old="a", new="a_else", output_handle="output-1")
        add_union(graph, node_id=7, depending_on_ids=[5, 6])
        return graph

    @pytest.mark.parametrize("mode", ["x", "y"])
    def test_nested_split_keeps_conjunction_blocks(self, mode):
        """The else of ``if P and g`` is not ``P and not g`` — no fusion under a prefix."""
        graph = self._build_nested(flow_id=63)
        set_parameter(graph, "mode", mode)

        code = FlowGraphToPolarsConverter(graph).convert()

        assert "if (env == 'prod') and (mode == 'x'):" in code
        assert "if (env == 'prod') and (not (mode == 'x')):" in code

        graph.run_graph()
        engine_df = collect_node(graph, 7)
        generated_df = run_generated(code, env="prod", mode=mode)
        assert set(generated_df.columns) == set(engine_df.columns)
        assert generated_df.select(engine_df.columns).to_dicts() == engine_df.to_dicts()

    def test_three_input_union_with_complementary_pair_drops_the_fallback(self):
        """A then/else pair proves the list non-empty even with a third, independent branch."""
        graph = create_graph(flow_id=64)
        graph.flow_settings.parameters.append(FlowParameter(name="env", default_value="prod", type="string"))
        graph.flow_settings.parameters.append(FlowParameter(name="flag", default_value="yes", type="string"))
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(
            graph, node_id=2, depending_on_id=1, else_output=True, parameter="env", operator="equals", value="prod"
        )
        add_rename_select(graph, node_id=3, depending_on_id=2, old="a", new="a_then")
        add_rename_select(graph, node_id=5, depending_on_id=2, old="a", new="a_else", output_handle="output-1")
        add_gate(graph, node_id=8, depending_on_id=1, parameter="flag", operator="equals", value="yes")
        add_rename_select(graph, node_id=9, depending_on_id=8, old="a", new="a_extra")
        add_union(graph, node_id=6, depending_on_ids=[3, 5, 9])

        code = FlowGraphToPolarsConverter(graph).convert()

        assert "df_6_frames = []" in code
        assert "if not df_6_frames:" not in code
        result = run_generated(code, env="dev", flag="no")
        assert result.columns == ["a_else"]
        assert result.height == 3


# I. Performance execution mode


class TestPerformanceModeGates:
    """Gate routing is execution-mode independent.

    Performance runs must route, deliberately skip, and report N/N progress
    exactly like the Development runs pinned in sections A and H.
    """

    def test_parameter_diamond_routes_and_skips_like_development(self):
        graph = build_diamond(env="prod", flow_id=80, execution_mode="Performance")

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert [nr.node_id for nr in run_info.node_step_result if nr.skipped] == [5]
        assert by_id[5].success is True
        for gate_id in (2, 4):
            assert by_id[gate_id].success is True and by_id[gate_id].skipped is False
        assert run_info.number_of_nodes == run_info.nodes_completed == 7
        assert_branch(collect_node(graph, 7), live="a_prod", gated="a_dev", values=[1, 2, 3])

    def test_parameter_flip_reroutes_in_performance_mode(self):
        graph = build_diamond(env="prod", flow_id=81, execution_mode="Performance")
        assert graph.run_graph().success is True
        assert_branch(collect_node(graph, 7), live="a_prod", gated="a_dev", values=[1, 2, 3])

        set_parameter(graph, "env", "dev")
        second = graph.run_graph()

        assert second.success is True
        by_id = results_by_id(second)
        assert by_id[3].skipped is True
        assert by_id[5].skipped is False
        assert_branch(collect_node(graph, 7), live="a_dev", gated="a_prod", values=[1, 2, 3])

    @pytest.mark.parametrize(
        "env, live, gated, live_node, gated_node",
        [("prod", "a_prod", "a_dev", 3, 5), ("dev", "a_dev", "a_prod", 5, 3)],
    )
    def test_else_output_split_routes_and_skips_like_development(self, env, live, gated, live_node, gated_node):
        graph = build_split_diamond(env=env, flow_id=82, execution_mode="Performance")

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert by_id[2].success is True and by_id[2].skipped is False
        assert by_id[gated_node].skipped is True and by_id[gated_node].success is True
        assert by_id[live_node].skipped is False
        assert run_info.nodes_completed == run_info.number_of_nodes
        assert_branch(collect_node(graph, 6), live, gated, [1, 2, 3])


# J. Worker offload


WORKER_HOST = os.environ.get(
    "FLOWFILE_WORKER_HOST", "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1"
)
WORKER_PORT = int(os.environ.get("FLOWFILE_WORKER_PORT", 63579))


def _worker_available() -> bool:
    if os.environ.get("SKIP_WORKER_TESTS") == "1":
        return False
    try:
        return requests.get(f"http://{WORKER_HOST}:{WORKER_PORT}/docs", timeout=5).ok
    except requests.exceptions.RequestException:
        return False


class TestWorkerOffloadDiamond:
    """The default offloaded path (execution_location="remote") keeps gate semantics.

    The rest of this suite pins execution_location="local"; this one diamond run
    proves the deliberate-skip semantics survive the worker offload hop. Skips
    cleanly when no worker is reachable (e.g. SKIP_WORKER_TESTS=1).
    """

    def test_offloaded_diamond_routes_and_skips_like_local(self):
        if not _worker_available():
            pytest.skip("flowfile_worker is not reachable")
        graph = build_diamond(env="prod", flow_id=83, execution_location="remote")

        run_info = graph.run_graph()

        assert run_info.success is True
        by_id = results_by_id(run_info)
        assert [nr.node_id for nr in run_info.node_step_result if nr.skipped] == [5]
        assert by_id[5].success is True
        for gate_id in (2, 4):
            assert by_id[gate_id].success is True and by_id[gate_id].skipped is False
        assert run_info.nodes_completed == run_info.number_of_nodes == 7
        assert_branch(collect_node(graph, 7), live="a_prod", gated="a_dev", values=[1, 2, 3])


# K. Nested parameter gates


class TestNestedParameterGates:
    """A gate behind a gate, both parameter-mode: an AND of the two conditions.

    Engine: the doubly-gated branch runs only when BOTH gates are open, and the
    inner gate itself only executes (decides) when the outer gate lets data
    reach it. Export: the inner node's guard renders the two-atom conjunction —
    never flat-merged into an else, which would invert the outer condition.
    """

    _COMBOS = [("prod", "x"), ("prod", "y"), ("qa", "x"), ("qa", "y")]

    @staticmethod
    def _build(flow_id: int, env: str, mode: str) -> FlowGraph:
        """1 manual_input -> 2 gate(env == prod) -> 3 gate(mode == x) -> 4 select(a -> a_kept)."""
        graph = create_graph(flow_id=flow_id)
        graph.flow_settings.parameters.append(FlowParameter(name="env", default_value=env, type="string"))
        graph.flow_settings.parameters.append(FlowParameter(name="mode", default_value=mode, type="string"))
        add_manual_input(graph, SOURCE_ROWS, node_id=1)
        add_gate(graph, node_id=2, depending_on_id=1, parameter="env", operator="equals", value="prod")
        add_gate(graph, node_id=3, depending_on_id=2, parameter="mode", operator="equals", value="x")
        add_rename_select(graph, node_id=4, depending_on_id=3, old="a", new="a_kept")
        return graph

    @pytest.mark.parametrize("env, mode", _COMBOS)
    def test_only_the_doubly_open_path_runs(self, env, mode):
        graph = self._build(flow_id=84, env=env, mode=mode)

        run_info = graph.run_graph()

        assert run_info.success is True
        assert run_info.nodes_completed == run_info.number_of_nodes == 4
        by_id = results_by_id(run_info)
        outer_open = env == "prod"
        both_open = outer_open and mode == "x"
        assert by_id[2].success is True and by_id[2].skipped is False
        assert by_id[3].success is True
        assert by_id[3].skipped is (not outer_open)
        assert by_id[4].success is True
        assert by_id[4].skipped is (not both_open)
        if both_open:
            assert collect_node(graph, 4)["a_kept"].to_list() == [1, 2, 3]
        else:
            assert graph.get_node(4).results.resulting_data is None

    @pytest.mark.parametrize("converter", [FlowGraphToPolarsConverter, FlowGraphToFlowFrameConverter])
    @pytest.mark.parametrize("env, mode", _COMBOS)
    def test_export_renders_the_conjunction_and_matches_the_engine(self, converter, env, mode):
        graph = self._build(flow_id=85, env=env, mode=mode)

        code = converter(graph).convert()

        assert "if (env == 'prod') and (mode == 'x'):" in code
        assert "else:" not in code

        graph.run_graph()
        engine_result = graph.get_node(4).results.resulting_data
        engine_height = engine_result.data_frame.lazy().collect().height if engine_result else 0
        generated_df = run_generated(code, env=env, mode=mode)
        assert generated_df.columns == ["a_kept"]
        expected_height = 3 if env == "prod" and mode == "x" else 0
        assert generated_df.height == engine_height == expected_height
