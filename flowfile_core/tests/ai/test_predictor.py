"""Predictor + tier handler tests."""

from __future__ import annotations

from flowfile_core.ai.tools.predictor import (
    _resolve_upstream_schemas,
    collect_column_refs,
    predict_schema_via_mirror,
    schema_to_dict_list,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema


def _flow_settings(flow_id: int = 100) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_predictor",
    )


def _flow_with_orders(flow_id: int = 100) -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="predictor_test")
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="amount", data_type="Double"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[[1, 2], [10.0, 20.0], ["EU", "US"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(1).get_predicted_schema()
    return flow


# --------------------------------------------------------------------------- #
# Mirror-graph predict #
# --------------------------------------------------------------------------- #


def test_mirror_predicts_filter_passthrough_schema() -> None:
    """A filter applied to an upstream of (order_id, amount, region) should
    predict the same three columns."""
    upstream_schema = [
        FlowfileColumn.from_input("order_id", "Integer"),
        FlowfileColumn.from_input("amount", "Double"),
        FlowfileColumn.from_input("region", "String"),
    ]
    settings = input_schema.NodeFilter(
        flow_id=999,
        node_id=42,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    predicted = predict_schema_via_mirror("filter", settings, {1: upstream_schema})
    assert predicted is not None
    names = {col.column_name for col in predicted}
    assert names == {"order_id", "amount", "region"}


def test_mirror_predicts_manual_input_from_settings() -> None:
    """Source nodes predict directly from settings — no upstream needed."""
    settings = input_schema.NodeManualInput(
        flow_id=999,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="label", data_type="String"),
            ],
            data=[[1, 2], ["a", "b"]],
        ),
    )
    predicted = predict_schema_via_mirror("manual_input", settings, {})
    assert predicted is not None
    assert {col.column_name for col in predicted} == {"id", "label"}


def test_mirror_returns_none_for_dynamic_nodes() -> None:
    """Dynamic nodes (polars_code/python_script/sql_query) cannot be predicted
    via the mirror — they need the kernel dry-run path instead."""
    settings = input_schema.NodePolarsCode(
        flow_id=999,
        node_id=10,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(polars_code="main"),
    )
    predicted = predict_schema_via_mirror("polars_code", settings, {1: []})
    assert predicted is None


def test_mirror_isolation_does_not_mutate_real_graph() -> None:
    """Predicting a static schema must not add nodes to the caller's flow."""
    flow = _flow_with_orders()
    upstream_schema = flow.get_node(1).node_schema.predicted_schema
    settings = input_schema.NodeFilter(
        flow_id=flow.flow_id,
        node_id=999,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    nodes_before = list(flow.nodes)
    predicted = predict_schema_via_mirror("filter", settings, {1: list(upstream_schema)})
    assert predicted is not None
    nodes_after = list(flow.nodes)
    assert [n.node_id for n in nodes_after] == [n.node_id for n in nodes_before]


# --------------------------------------------------------------------------- #
# tier handler #
# --------------------------------------------------------------------------- #


def test_resolve_upstream_tier_0_cached() -> None:
    flow = _flow_with_orders()
    resolved, warnings = _resolve_upstream_schemas(flow, [1])
    assert 1 in resolved
    assert warnings == []


def test_resolve_upstream_tier_1_callback_force() -> None:
    """Static upstream with no cached schema gets predicted via its callback."""
    flow = _flow_with_orders()
    filter_settings = input_schema.NodeFilter(
        flow_id=flow.flow_id,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    flow.add_filter(filter_settings)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.get_node(2).node_schema.predicted_schema = None  # force tier 1 path
    resolved, warnings = _resolve_upstream_schemas(flow, [2])
    assert 2 in resolved
    assert warnings == []
    # Schema populated in place.
    assert flow.get_node(2).node_schema.predicted_schema is not None


def test_resolve_upstream_missing_node_warns() -> None:
    flow = _flow_with_orders()
    resolved, warnings = _resolve_upstream_schemas(flow, [9999])
    assert resolved == {}
    assert any("not found" in w for w in warnings)


def test_resolve_upstream_no_callback_warns() -> None:
    """If a node has neither cached schema nor callback, tier 2 fires."""
    flow = _flow_with_orders()
    py_settings = input_schema.NodePythonScript(
        flow_id=flow.flow_id,
        node_id=5,
        depending_on_ids=[1],
        python_script_input=input_schema.PythonScriptInput(code="output_df = input_df_1"),
    )
    flow.add_python_script(py_settings)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 5))
    py_node = flow.get_node(5)
    py_node.node_schema.predicted_schema = None
    py_node._schema_callback = None
    py_node.user_provided_schema_callback = None  # property re-instantiates from this otherwise

    def _broken(*_args, **_kwargs):
        raise RuntimeError("forced tier-2 path for test")

    py_node._function = _broken
    resolved, warnings = _resolve_upstream_schemas(flow, [5])
    assert resolved == {}
    assert any("schema unknown" in w for w in warnings)


# --------------------------------------------------------------------------- #
# Column-ref collection #
# --------------------------------------------------------------------------- #


def test_collect_column_refs_filter_basic() -> None:
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            filter_type="basic",
            basic_filter=transform_schema.BasicFilter(field="amount", operator="greater_than", value="100"),
        ),
    )
    refs = collect_column_refs("filter", settings)
    assert refs == ["amount"]


def test_collect_column_refs_filter_advanced_returns_empty() -> None:
    """Advanced (Polars expression) filters aren't statically parsed — refs
    are validated by the dry-run / mirror callback instead."""
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[amount] > 100"),
    )
    refs = collect_column_refs("filter", settings)
    assert refs == []


def test_collect_column_refs_select() -> None:
    settings = input_schema.NodeSelect(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        select_input=[
            transform_schema.SelectInput("order_id", "order_id", keep=True),
            transform_schema.SelectInput("amount", "net", keep=True),
        ],
    )
    refs = collect_column_refs("select", settings)
    assert refs == ["order_id", "amount"]


def test_collect_column_refs_unknown_node_returns_empty() -> None:
    """Conservative fallback — never raise on unfamiliar node types."""
    refs = collect_column_refs(
        "polars_code",
        input_schema.NodePolarsCode(
            flow_id=1,
            node_id=1,
            depending_on_ids=[],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="main"),
        ),
    )
    assert refs == []


# --------------------------------------------------------------------------- #
# schema_to_dict_list #
# --------------------------------------------------------------------------- #


def test_schema_to_dict_list_shape() -> None:
    schema = [
        FlowfileColumn.from_input("a", "Integer"),
        FlowfileColumn.from_input("b", "String"),
    ]
    out = schema_to_dict_list(schema)
    assert out == [
        {"name": "a", "data_type": "Int64", "nullable": True},
        {"name": "b", "data_type": "String", "nullable": True},
    ]


def test_schema_to_dict_list_none_passes_through() -> None:
    assert schema_to_dict_list(None) is None
