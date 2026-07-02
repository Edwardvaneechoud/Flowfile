"""Tests for FlowFrame.run_flow — running a saved sub-flow once per input row."""

from flowfile_frame.flow_frame_methods import from_dict


def _build_and_save_echo_subflow(path) -> None:
    """manual_input -> polars_code echoing ${x} -> api_response, saved to *path*."""
    from flowfile_core.flowfile.flow_graph import add_connection
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas import input_schema, schemas, transform_schema
    from flowfile_core.schemas.schemas import FlowParameter

    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=8101,
            name="echo_subflow",
            path=str(path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(8101)
    graph.flow_settings.parameters = [FlowParameter(name="x", default_value="")]

    graph.add_node_promise(input_schema.NodePromise(flow_id=8101, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=8101,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"seed": 1}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=8101, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=8101,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.with_columns(pl.lit('${x}').alias('echo')).select('echo')"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_node_promise(input_schema.NodePromise(flow_id=8101, node_id=3, node_type="api_response"))
    graph.add_api_response(input_schema.NodeApiResponse(flow_id=8101, node_id=3, depending_on_id=2))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    graph.save_flow(str(path))


def test_run_flow_per_row(tmp_path):
    sub_path = tmp_path / "echo.yaml"
    _build_and_save_echo_subflow(sub_path)

    frame = from_dict({"ticker": ["AAPL", "MSFT"]})
    out = frame.run_flow(str(sub_path), parameter_mappings={"x": "ticker"}).collect().sort("__param_value__")

    assert out["echo"].to_list() == ["AAPL", "MSFT"]
    assert out["__param_value__"].to_list() == ["AAPL", "MSFT"]
