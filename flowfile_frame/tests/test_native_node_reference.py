"""``node_reference`` on frames and native nodes: the designer's reference edit, from Python."""

import pytest

import flowfile_frame as ff
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars
from flowfile_core.flowfile.flow_graph import FlowGraph

from .native_helpers import core_node, round_trip

ORDERS = {"order_id": [1, 2, 3], "amount": [5.0, 7.5, 2.5]}
CUSTOMERS = {"customer_id": [10, 20], "name": ["Ann", "Bob"]}


def _saved_reference(graph: FlowGraph, node_id: int) -> str | None:
    saved = {node.id: node for node in graph.get_flowfile_data().nodes}
    return saved[node_id].setting_input.node_reference


def test_frame_reference_is_stored_on_its_node_and_saved():
    orders = ff.from_dict(ORDERS)
    assert orders.node_reference is None
    orders.node_reference = "orders"
    assert orders.node_reference == "orders"
    assert core_node(orders).setting_input.node_reference == "orders"
    assert _saved_reference(orders.flow_graph, orders.node_id) == "orders"


def test_native_node_reference_is_stored_on_its_node_and_saved():
    script = ff.PythonScript(ff.from_dict(ORDERS), code="x = 1", kernel="ml-kernel")
    assert script.node_reference is None
    script.node_reference = "scored"
    assert script.node_reference == "scored"
    assert script.output.node_reference == "scored"
    assert _saved_reference(script.flow_graph, script.node_id) == "scored"


def test_every_output_frame_of_a_node_shares_its_reference():
    script = ff.PythonScript(ff.from_dict(ORDERS), code="x = 1", outputs=["main", "metrics"])
    script["metrics"].node_reference = "scoring"
    assert script.node_reference == script["main"].node_reference == "scoring"


def test_round_trip_keeps_the_references():
    orders = ff.from_dict(ORDERS)
    orders.node_reference = "orders"
    script = ff.PythonScript(orders, code="x = 1", kernel="ml-kernel")
    script.node_reference = "scored"
    out = script.output.select("amount")
    out.node_reference = "amounts"

    reopened, first_doc = round_trip(out, "node_reference_roundtrip.yaml")
    assert reopened.get_node(orders.node_id).setting_input.node_reference == "orders"
    assert reopened.get_node(script.node_id).setting_input.node_reference == "scored"
    assert reopened.get_node(out.node_id).setting_input.node_reference == "amounts"
    saved = {node["id"]: node["node_reference"] for node in first_doc["nodes"]}
    assert saved == {orders.node_id: "orders", script.node_id: "scored", out.node_id: "amounts"}


def test_exported_code_names_the_variable_after_the_reference():
    orders = ff.from_dict(ORDERS)
    orders.node_reference = "orders"
    out = orders.filter(ff.col("amount") > 3)
    out.node_reference = "big_orders"

    code = export_flow_to_polars(out.flow_graph)
    assert "orders = " in code
    assert "big_orders = orders" in code
    assert f"df_{orders.node_id}" not in code
    assert f"df_{out.node_id}" not in code


def test_kernel_input_names_use_the_upstream_references():
    orders, customers = ff.from_dict(ORDERS), ff.from_dict(CUSTOMERS)
    orders.node_reference = "orders"
    script = ff.PythonScript(orders, customers, code="x = 1")
    assert FlowGraph._resolve_input_names(script.node, 2) == ["orders", f"df_{customers.node_id}"]
    customers.node_reference = "customers"
    assert FlowGraph._resolve_input_names(script.node, 2) == ["orders", "customers"]


@pytest.mark.parametrize("value", ["Orders", "my ref", "1abc", "a-b", "_orders", 42, ["orders"]])
def test_an_invalid_reference_raises_and_leaves_the_node_unchanged(value):
    orders = ff.from_dict(ORDERS)
    orders.node_reference = "orders"
    with pytest.raises(ff.NativeNodeError, match="Invalid node_reference") as info:
        orders.node_reference = value
    assert isinstance(info.value, ValueError)
    assert orders.node_reference == "orders"


def test_a_reference_used_by_another_node_in_the_graph_raises():
    orders = ff.from_dict(ORDERS)
    orders.node_reference = "orders"
    script = ff.PythonScript(orders, code="x = 1")
    with pytest.raises(ff.NativeNodeError, match=f"already used by node {orders.node_id}"):
        script.node_reference = "orders"
    assert script.node_reference is None
    orders.node_reference = "orders"
    assert orders.node_reference == "orders"


def test_none_and_empty_clear_back_to_the_default_name():
    orders = ff.from_dict(ORDERS)
    out = orders.filter(ff.col("amount") > 3)
    for cleared in (None, ""):
        orders.node_reference = "orders"
        out.node_reference = "big_orders"
        assert "big_orders = orders" in export_flow_to_polars(out.flow_graph)
        orders.node_reference = cleared
        out.node_reference = cleared
        assert orders.node_reference is None and out.node_reference is None
        assert _saved_reference(orders.flow_graph, orders.node_id) is None
        assert "orders" not in export_flow_to_polars(out.flow_graph)


def test_setting_it_clears_the_nodes_hash_so_the_change_is_seen():
    orders = ff.from_dict(ORDERS)
    before = core_node(orders).hash
    orders.node_reference = "orders"
    assert core_node(orders).hash != before


def test_setting_it_on_a_deferred_output_keeps_the_placeholder():
    orders = ff.from_dict(ORDERS)
    script = ff.PythonScript(orders, code="x = 1", kernel="ml-kernel")
    expected = orders.data.collect_schema()
    script.output.node_reference = "scored"

    core = script.node
    assert core.deferred_until_run is True
    assert script.output.data.collect_schema() == expected
    downstream = script.output.select("amount")
    assert downstream._deferred is True
    assert downstream.data.collect_schema().names() == ["amount"]
    assert downstream.data.collect().height == 0
    assert core.results.errors is None
