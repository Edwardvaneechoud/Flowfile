"""Integration tests: flow parameters are substituted correctly at execution time.

These tests verify the full pipeline:
  define parameter → reference ${name} in node settings → run_graph() → correct output
"""

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.schemas import FlowParameter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_graph(flow_id: int = 99) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="param_test",
            path=".",
            execution_mode="Development",
            execution_location="local",
        )
    )
    return handler.get_flow(flow_id)


def run_and_assert_ok(graph: FlowGraph) -> None:
    result = graph.run_graph()
    assert result is not None, "run_graph returned None"
    if not result.success:
        errors = "\n".join(
            f"  node {r.node_id}: {r.error}"
            for r in result.node_step_result
            if not r.success
        )
        raise AssertionError(f"Flow failed:\n{errors}")


# ---------------------------------------------------------------------------
# polars_code node
# ---------------------------------------------------------------------------


def test_polars_code_parameter_substituted():
    """${col_name} inside polars code is replaced with the parameter value."""
    graph = make_graph(101)
    graph.flow_settings.parameters = [FlowParameter(name="col_name", default_value="city")]

    # Manual input: [{name: "Alice", city: "Amsterdam"}]
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{"name": "Alice", "city": "Amsterdam"}]
            ),
        )
    )

    # polars_code node that selects a column by the parameter name
    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.select(pl.col('${col_name}'))"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result_df = graph.get_node(2).get_resulting_data()
    assert result_df is not None
    assert list(result_df.columns) == ["city"]


def test_polars_code_original_code_preserved_after_run():
    """After run_graph, the ${...} reference must still be in setting_input (not resolved)."""
    graph = make_graph(102)
    graph.flow_settings.parameters = [FlowParameter(name="col", default_value="name")]

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Bob"}]),
        )
    )

    original_code = "output_df = input_df.select(pl.col('${col}'))"
    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=original_code),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    stored_code = graph.get_node(2).setting_input.polars_code_input.polars_code
    assert stored_code == original_code, (
        f"Original ${'{col}'} reference should be preserved after run, got: {stored_code!r}"
    )


def test_polars_code_filter_with_numeric_parameter():
    """A numeric parameter replaces ${threshold} so a filter expression evaluates correctly."""
    graph = make_graph(103)
    graph.flow_settings.parameters = [FlowParameter(name="threshold", default_value="30")]

    rows = [{"age": 25}, {"age": 35}, {"age": 40}]
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(rows),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('age') > ${threshold})"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result = graph.get_node(2).get_resulting_data()
    ages = result.to_dict()["age"]
    assert all(a > 30 for a in ages), f"Expected only ages > 30, got {ages}"
    assert len(ages) == 2


def test_polars_code_multiple_parameters():
    """Two parameters in the same code string are both resolved."""
    graph = make_graph(104)
    graph.flow_settings.parameters = [
        FlowParameter(name="src_col", default_value="name"),
        FlowParameter(name="new_col", default_value="upper_name"),
    ]

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "alice"}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code=(
                    "output_df = input_df.with_columns("
                    "pl.col('${src_col}').str.to_uppercase().alias('${new_col}'))"
                )
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result = graph.get_node(2).get_resulting_data()
    assert "upper_name" in result.columns
    assert result.to_dict()["upper_name"][0] == "ALICE"


def test_no_parameters_flow_unchanged():
    """A flow with no parameters executes exactly as before (no regression)."""
    graph = make_graph(105)
    assert graph.flow_settings.parameters == []

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"x": 1}, {"x": 2}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.with_columns(pl.col('x') * 2)"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result = graph.get_node(2).get_resulting_data()
    assert result.to_dict()["x"] == [2, 4]


def test_unresolved_parameter_fails_node():
    """A ${missing} reference that has no matching parameter causes the node to fail."""
    graph = make_graph(106)
    graph.flow_settings.parameters = []  # no parameters defined

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"x": 1}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.select(pl.col('${undefined_param}'))"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    result = graph.run_graph()
    node_result = next(r for r in result.node_step_result if r.node_id == 2)
    assert not node_result.success, "Node with unresolved parameter should fail"
    assert "undefined_param" in (node_result.error or "")
