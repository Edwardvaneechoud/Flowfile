"""``ff.Node``: place any built-in node type from its type string and settings."""

import time

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import GraphicWalkerInput

ORDERS = {"order_id": [1, 2, 3], "customer_id": [10, 20, 10], "amount": [5.0, 7.5, 2.5]}
CUSTOMERS = {"customer_id": [10, 20], "name": ["Ann", "Bob"]}
TEN_X = "output_df = input_df.with_columns((pl.col('amount') * 10).alias('amount10'))"


# node types through ff.Node


def test_sql_query_over_two_inputs_on_different_graphs():
    orders = ff.from_dict(ORDERS)
    customers = ff.from_dict(CUSTOMERS)
    sql = (
        "select o.order_id, c.name, o.amount from input_1 o join input_2 c on o.customer_id = c.customer_id "
        "order by o.order_id"
    )

    node = ff.Node(ff.NodeTypes.SQL_QUERY, orders, customers, settings={"sql_query_input": {"sql_code": sql}})

    assert orders.flow_graph is customers.flow_graph is node.flow_graph
    core = node.node
    assert core.node_type == "sql_query"
    assert core.setting_input.sql_query_input.sql_code == sql
    assert core.setting_input.depending_on_ids == [orders.node_id, customers.node_id]
    assert [n.node_id for n in core.node_inputs.main_inputs] == [orders.node_id, customers.node_id]
    assert node.outputs == ["main"] and node["main"] is node.output
    assert node.output._deferred is False
    expected = pl.DataFrame({"order_id": [1, 2, 3], "name": ["Ann", "Bob", "Ann"], "amount": [5.0, 7.5, 2.5]})
    assert_frame_equal(node.output.collect(), expected)


def test_api_response_is_a_pass_through_sink():
    orders = ff.from_dict(ORDERS)
    node = ff.Node("api_response", orders, settings={"max_rows": 100}, description="orders endpoint")
    settings = node.node.setting_input
    assert node.node.node_type == "api_response"
    assert (settings.max_rows, settings.orientation, settings.description) == (100, "records", "orders endpoint")
    assert settings.depending_on_id == orders.node_id
    assert_frame_equal(node.output.collect(), pl.DataFrame(ORDERS))


def test_explore_data_gets_a_graphic_walker_input():
    orders = ff.from_dict(ORDERS)
    node = ff.Node("explore_data", orders)
    assert node.node.node_type == "explore_data"
    assert isinstance(node.node.setting_input.graphic_walker_input, GraphicWalkerInput)
    assert node.output.columns == list(ORDERS)


def test_flow_output_via_node_and_duplicate_name():
    orders = ff.from_dict(ORDERS)
    graph = orders.flow_graph
    node = ff.Node("flow_output", orders, settings={"output_name": "clean_orders"})
    assert node.node.node_type == "flow_output"
    assert node.node.setting_input.output_name == "clean_orders"
    node_count = len(graph.nodes)

    with pytest.raises(ff.NativeNodeError, match="already used"):
        ff.Node("flow_output", orders, settings={"output_name": "clean_orders"})
    assert len(graph.nodes) == node_count


def test_settings_as_a_model_instance():
    orders = ff.from_dict(ORDERS)
    model = input_schema.NodeApiResponse(flow_id=0, node_id=0, max_rows=5, orientation="columns")

    node = ff.Node("api_response", orders, settings=model)

    settings = node.node.setting_input
    assert (settings.max_rows, settings.orientation) == (5, "columns")
    assert (settings.flow_id, settings.node_id) == (orders.flow_graph.flow_id, node.node_id)
    assert settings.depending_on_id == orders.node_id
    assert (model.flow_id, model.node_id) == (0, 0)


# deferral


def test_deferred_override_seeds_instead_of_running():
    orders = ff.from_dict(ORDERS)
    node = ff.Node("polars_code", orders, settings={"polars_code_input": {"polars_code": TEN_X}}, deferred=True)

    assert node.deferred is True and node.output._deferred is True
    assert node.node.deferred_until_run is True
    assert node.output.columns == [*ORDERS, "amount10"]
    assert node.output.data.collect().height == 0
    assert node.output.collect()["amount10"].to_list() == [50.0, 75.0, 25.0]
    assert node.node.deferred_until_run is False


def test_deferred_false_for_a_writer_below_a_deferred_frame_raises():
    orders = ff.from_dict(ORDERS)
    deferred = ff.Node("polars_code", orders, settings={"polars_code_input": {"polars_code": TEN_X}}, deferred=True)
    with pytest.raises(ff.NativeNodeError, match="leave deferred unset"):
        ff.Node("api_response", deferred.output, deferred=False)


@pytest.mark.parametrize(
    "fields, schema",
    [
        (None, {}),  # no declared schema: an undeferred placement would prefetch by running the source
        (
            [{"name": "id", "data_type": "Int64"}, {"name": "email", "data_type": "String"}],
            {"id": pl.Int64, "email": pl.String},
        ),
    ],
)
def test_zero_input_deferred_node_never_runs_at_placement(monkeypatch, fields, schema):
    calls: list[str] = []

    def counting(*args, **kwargs):
        calls.append("run")
        raise AssertionError("external source ran at placement")

    monkeypatch.setattr(FlowDataEngine, "create_from_external_source", counting)
    settings = {"identifier": "sample_users", "source_settings": {"SAMPLE_USERS": True, "fields": fields}}

    node = ff.Node("external_source", settings=settings)
    time.sleep(0.3)  # let any orphaned background prefetch surface

    assert calls == []
    assert node.node.deferred_until_run is True
    assert node.output._deferred is True
    assert node.output.collect_schema() == pl.Schema(schema)
    assert node.output.data.collect().height == 0


# errors


class _GenericCustomNode(CustomNodeBase):
    node_name: str = "Generic Node Custom"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(main_section=Section(title="Main", note=TextInput(label="Note")))

    def process(self, *inputs):
        return inputs[0]


@pytest.fixture
def custom_node_type():
    saved_store = dict(node_store.CUSTOM_NODE_STORE)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    node_store.add_to_custom_node_store(_GenericCustomNode)
    yield _GenericCustomNode().item
    node_store.CUSTOM_NODE_STORE.clear()
    node_store.CUSTOM_NODE_STORE.update(saved_store)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_dict)
    node_store.nodes_list[:] = saved_list


@pytest.mark.parametrize(
    "node_type, match",
    [
        ("not_a_node", "Unknown node type 'not_a_node'"),
        ("promise", "canvas placeholder"),
        ("polars_lazy_frame", "fl.FlowFrame"),
        ("user_defined", "fl.CustomNode"),
    ],
)
def test_refused_node_types(node_type, match):
    with pytest.raises(ff.NativeNodeError, match=match):
        ff.Node(node_type, ff.from_dict(ORDERS))


def test_custom_node_type_points_at_custom_node(custom_node_type):
    with pytest.raises(ff.NativeNodeError, match="fl.CustomNode"):
        ff.Node(custom_node_type, ff.from_dict(ORDERS))


def test_unknown_settings_key_lists_the_valid_keys():
    with pytest.raises(ff.NativeNodeError, match=r"Unknown settings for api_response: \['max_row'\].*'max_rows'"):
        ff.Node("api_response", ff.from_dict(ORDERS), settings={"max_row": 5})


@pytest.mark.parametrize("field", ["node_id", "flow_id", "pos_x", "is_setup", "user_id", "depending_on_id"])
def test_base_managed_settings_are_refused(field):
    with pytest.raises(ff.NativeNodeError, match="set by the node itself"):
        ff.Node("api_response", ff.from_dict(ORDERS), settings={field: 1})


def test_settings_of_the_wrong_model_are_refused():
    model = input_schema.NodeFlowOutput(flow_id=0, node_id=0)
    with pytest.raises(ff.NativeNodeError, match="must be a dict or a NodeApiResponse"):
        ff.Node("api_response", ff.from_dict(ORDERS), settings=model)


def test_invalid_settings_values_raise_native_error():
    with pytest.raises(ff.NativeNodeError, match="Invalid settings for api_response"):
        ff.Node("api_response", ff.from_dict(ORDERS), settings={"orientation": "rows"})


@pytest.mark.parametrize(
    "node_type, count, match",
    [
        ("join", 1, "join takes 2 input frame"),
        ("api_response", 2, "api_response takes 1 input frame"),
        ("api_response", 0, "api_response takes 1 input frame"),
        ("union", 0, "union needs at least one input frame"),
    ],
)
def test_wrong_arity_raises_before_placing(node_type, count, match):
    frames = [ff.from_dict(ORDERS) for _ in range(count)]
    graphs = [f.flow_graph for f in frames]
    with pytest.raises(ff.NativeNodeError, match=match):
        ff.Node(node_type, *frames)
    assert [f.flow_graph for f in frames] == graphs  # refused before any merge
    assert all(len(g.nodes) == 1 for g in graphs)


def test_run_flow_is_refused_in_favour_of_the_dedicated_class():
    import pytest

    import flowfile_frame as ff
    from flowfile_frame.native import NativeNodeError, Node

    orders = ff.from_dict({"id": [1]})
    with pytest.raises(NativeNodeError, match="fl.RunFlow"):
        Node("run_flow", orders)
