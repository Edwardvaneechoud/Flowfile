"""Integration tests for the Run-flow node.

A Run-flow node runs a saved sub-flow once per input row, mapping input columns to
the sub-flow's ${param} references. These tests build a hermetic sub-flow
(manual_input -> polars_code echoing ${x} -> api_response), save it, then drive it
from a parent flow and assert the unioned per-row output plus the __param_value__
correlation column.
"""

import logging

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.schemas import FlowParameter


def _make_graph(flow_id: int, execution_location: str = "local"):
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="run_flow_test",
            path=".",
            execution_mode="Development",
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def _build_and_save_echo_subflow(path, flow_id: int = 7011) -> None:
    """manual_input(1 row) -> polars_code echoing ${x} -> api_response, saved to *path*.

    The sub-flow has a single flow parameter ``x``; each run emits one row whose
    ``echo`` column equals the value injected for ``x``.
    """
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="echo_subflow",
            path=str(path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(flow_id)
    graph.flow_settings.parameters = [FlowParameter(name="x", default_value="")]

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"seed": 1}]),
        )
    )

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.with_columns(pl.lit('${x}').alias('echo')).select('echo')"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type="api_response"))
    graph.add_api_response(input_schema.NodeApiResponse(flow_id=flow_id, node_id=3, depending_on_id=2))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

    graph.save_flow(str(path))


def _run(graph):
    result = graph.run_graph()
    assert result is not None, "run_graph returned None"
    return result


def test_run_flow_runs_subflow_per_row(execution_location, tmp_path):
    """The sub-flow runs once per input row; outputs union with a __param_value__ column."""
    sub_path = tmp_path / "echo.yaml"
    _build_and_save_echo_subflow(sub_path)

    graph = _make_graph(7000, execution_location=execution_location)
    tickers = ["AAPL", "MSFT", "GOOG"]
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"ticker": t} for t in tickers]),
        )
    )

    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="run_flow"))
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            flow_reference=str(sub_path),
            parameter_mappings=[input_schema.ParameterMapping(param_name="x", input_column="ticker")],
            max_rows=100,
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    result = _run(graph)
    assert result.success, "\n".join(f"node {r.node_id}: {r.error}" for r in result.node_step_result if not r.success)

    out = graph.get_node(2).get_resulting_data().data_frame
    df = out.collect() if isinstance(out, pl.LazyFrame) else out
    df = df.sort("__param_value__")
    assert df.height == len(tickers)
    assert df["echo"].to_list() == sorted(tickers)
    assert df["__param_value__"].to_list() == sorted(tickers)


def test_run_flow_max_rows_cap(execution_location, tmp_path, caplog):
    """Input larger than max_rows is truncated to max_rows with a logged warning."""
    sub_path = tmp_path / "echo.yaml"
    _build_and_save_echo_subflow(sub_path)

    graph = _make_graph(7001, execution_location=execution_location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"ticker": f"T{i}"} for i in range(5)]),
        )
    )

    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="run_flow"))
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            flow_reference=str(sub_path),
            parameter_mappings=[input_schema.ParameterMapping(param_name="x", input_column="ticker")],
            max_rows=2,
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    with caplog.at_level(logging.WARNING):
        result = _run(graph)
    assert result.success

    out = graph.get_node(2).get_resulting_data().data_frame
    df = out.collect() if isinstance(out, pl.LazyFrame) else out
    assert df.height == 2
    assert any("max_rows" in rec.message for rec in caplog.records)


def test_run_flow_recursion_guard(tmp_path):
    """A flow whose Run-flow node references itself fails with a recursion error."""
    self_path = tmp_path / "self.yaml"

    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=7002,
            name="self_ref",
            path=str(self_path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(7002)

    graph.add_node_promise(input_schema.NodePromise(flow_id=7002, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=7002,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"v": 1}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=7002, node_id=2, node_type="run_flow"))
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=7002,
            node_id=2,
            depending_on_id=1,
            flow_reference=str(self_path),
            max_rows=5,
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_node_promise(input_schema.NodePromise(flow_id=7002, node_id=3, node_type="api_response"))
    graph.add_api_response(input_schema.NodeApiResponse(flow_id=7002, node_id=3, depending_on_id=2))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    graph.save_flow(str(self_path))

    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    reopened = open_flow(self_path, user_id=1)
    result = reopened.run_graph()
    node2 = next(r for r in result.node_step_result if r.node_id == 2)
    assert node2.success is False
    assert "recursive" in (node2.error or "").lower()


def test_run_flow_no_subflow_selected_errors(tmp_path):
    """A Run-flow node with no sub-flow reference fails clearly."""
    graph = _make_graph(7003, execution_location="local")
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"ticker": "AAPL"}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="run_flow"))
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            parameter_mappings=[input_schema.ParameterMapping(param_name="x", input_column="ticker")],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    result = graph.run_graph()
    node2 = next(r for r in result.node_step_result if r.node_id == 2)
    assert node2.success is False
    assert "sub-flow" in (node2.error or "").lower()


def _build_and_save_filter_subflow(path, flow_id: int = 7401) -> None:
    """manual_input -> polars_code filtering on ${x} -> api_response.

    Only the row matching ``${x}`` survives, so an unmatched value yields 0 rows.
    """
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="filter_subflow",
            path=str(path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(flow_id)
    graph.flow_settings.parameters = [FlowParameter(name="x", default_value="")]

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"sym": "AAA"}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('sym') == '${x}')"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type="api_response"))
    graph.add_api_response(input_schema.NodeApiResponse(flow_id=flow_id, node_id=3, depending_on_id=2))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    graph.save_flow(str(path))


def test_run_flow_empty_rows_tolerated(execution_location, tmp_path):
    """A sub-flow run that yields 0 rows for an input contributes nothing (no failure)."""
    sub_path = tmp_path / "filter.yaml"
    _build_and_save_filter_subflow(sub_path)

    graph = _make_graph(7400, execution_location=execution_location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            # "AAA" matches (1 row); "ZZZ" matches nothing (0 rows).
            raw_data_format=input_schema.RawData.from_pylist([{"ticker": "AAA"}, {"ticker": "ZZZ"}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="run_flow"))
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            flow_reference=str(sub_path),
            parameter_mappings=[input_schema.ParameterMapping(param_name="x", input_column="ticker")],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    result = _run(graph)
    assert result.success, "\n".join(f"node {r.node_id}: {r.error}" for r in result.node_step_result if not r.success)

    out = graph.get_node(2).get_resulting_data().data_frame
    df = out.collect() if isinstance(out, pl.LazyFrame) else out
    # Only the matching input contributes a row; the empty run is silently dropped.
    assert df.height == 1
    assert df["__param_value__"].to_list() == ["AAA"]


def _build_and_save_collision_subflow(path, flow_id: int = 7501) -> None:
    """A sub-flow whose api_response already emits a ``__param_value__`` column.

    The column comes straight from a data source (manual_input) rather than from
    polars_code, which forbids ``__`` dunder patterns. Used to verify the Run-flow
    node refuses to overwrite it with the parameter-value column.
    """
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="collision_subflow",
            path=str(path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(flow_id)
    graph.flow_settings.parameters = [FlowParameter(name="x", default_value="")]

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"__param_value__": "preexisting"}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="api_response"))
    graph.add_api_response(input_schema.NodeApiResponse(flow_id=flow_id, node_id=2, depending_on_id=1))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.save_flow(str(path))


def test_run_flow_output_column_collision_errors(tmp_path):
    """A sub-flow that already emits the param output column fails with a clear error."""
    sub_path = tmp_path / "collision.yaml"
    _build_and_save_collision_subflow(sub_path)

    graph = _make_graph(7500, execution_location="local")
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"ticker": "AAA"}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="run_flow"))
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            flow_reference=str(sub_path),
            parameter_mappings=[input_schema.ParameterMapping(param_name="x", input_column="ticker")],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    result = graph.run_graph()
    node2 = next(r for r in result.node_step_result if r.node_id == 2)
    assert node2.success is False
    assert "collide" in (node2.error or "").lower()


def test_run_flow_empty_input_returns_predicted_schema(tmp_path):
    """Zero input rows yields an empty result that still advertises the output columns."""
    sub_path = tmp_path / "echo.yaml"
    _build_and_save_echo_subflow(sub_path)

    graph = _make_graph(7600, execution_location="local")
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"ticker": "AAA"}]),
        )
    )
    # Empty the input (keep its schema) before the Run-flow node.
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="output_df = input_df.head(0)"),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type="run_flow"))
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            flow_reference=str(sub_path),
            parameter_mappings=[input_schema.ParameterMapping(param_name="x", input_column="ticker")],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

    result = _run(graph)
    assert result.success, "\n".join(f"node {r.node_id}: {r.error}" for r in result.node_step_result if not r.success)

    out = graph.get_node(3).get_resulting_data().data_frame
    df = out.collect() if isinstance(out, pl.LazyFrame) else out
    assert df.height == 0
    assert set(df.columns) == {"echo", "__param_value__"}
