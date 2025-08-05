import pytest
from pathlib import Path
from copy import deepcopy

from flowfile_core.schemas import input_schema, transform_schema, schema_helpers
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_graph_utils import (
    combine_flow_graphs,
    _validate_input,
    _create_node_id_mapping
)


# Helper functions from your existing test suite
def create_flowfile_handler():
    from flowfile_core.flowfile.handler import FlowfileHandler
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


def create_graph(flow_id: int = 1, execution_mode: str = 'Development') -> FlowGraph:
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=f'flow_{flow_id}', path='.', execution_mode=execution_mode))
    graph = handler.get_flow(flow_id)
    return graph


def add_manual_input(graph: FlowGraph, data, node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=node_id,
                                              raw_data_format=input_schema.RawData.from_pylist(data))
    graph.add_manual_input(input_file)
    return graph


def add_node_promise_on_type(graph: FlowGraph, node_type: str, node_id: int, flow_id: int = None):
    if flow_id is None:
        flow_id = graph.flow_id
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type)
    graph.add_node_promise(node_promise)


def handle_run_info(run_info):
    from flowfile_core.flowfile.flow_graph import RunInformation
    if not run_info.success:
        errors = 'errors:'
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f'\n node_id:{node_step.node_id}, error: {node_step.error}'
        raise ValueError(f'Graph should run successfully:\n{errors}')


# Test functions for the combine_flow_graphs functionality
def test_validate_unique_flow_ids():
    """Test that _validate_unique_flow_ids works correctly."""
    # Create a set of unique graphs
    graphs = tuple(create_graph(flow_id=i) for i in range(3))

    # Should work with unique IDs
    _validate_input(graphs)

    # Should fail with duplicate IDs
    graphs_with_duplicates = (create_graph(flow_id=1), create_graph(flow_id=1))
    with pytest.raises(ValueError, match="Cannot combine"):
        _validate_input(graphs_with_duplicates)


def test_create_node_id_mapping():
    """Test that _create_node_id_mapping generates correct mappings."""
    # Create two graphs with different nodes
    graph1 = create_graph(flow_id=1)
    add_manual_input(graph1, [{"name": "john"}], node_id=101)
    add_node_promise_on_type(graph1, "filter", 102)

    graph2 = create_graph(flow_id=2)
    add_manual_input(graph2, [{"name": "jane"}], node_id=201)
    add_node_promise_on_type(graph2, "record_id", 202)

    # Create the mapping
    mapping = _create_node_id_mapping((graph1, graph2))

    # Verify the mapping structure
    assert (1, 101) in mapping
    assert (1, 102) in mapping
    assert (2, 201) in mapping
    assert (2, 202) in mapping

    # Verify all IDs are unique and higher than original
    new_ids = list(mapping.values())
    assert len(new_ids) == len(set(new_ids)), "New IDs should be unique"
    assert min(new_ids) > max(102, 202), "New IDs should be higher than original IDs"


def test_combine_flow_graphs_basic():
    """Test combining two simple flow graphs."""
    # Create two simple graphs with manual input nodes
    graph1 = create_graph(flow_id=1)
    add_manual_input(graph1, [{"name": "john"}], node_id=1)

    graph2 = create_graph(flow_id=2)
    add_manual_input(graph2, [{"name": "jane"}], node_id=1)
    # Combine the graphs
    combined = combine_flow_graphs(graph1, graph2)

    # Verify the combined graph properties
    assert combined.flow_id != graph1.flow_id
    assert combined.flow_id != graph2.flow_id
    assert len(combined.nodes) == 2

    # Run the combined graph
    run_info = combined.run_graph()
    handle_run_info(run_info)

    # Get node data and verify it's preserved
    for node in combined.nodes:
        data = node.get_resulting_data().collect().to_dicts()
        assert len(data) == 1
        assert "name" in data[0]
        assert data[0]["name"] in ["john", "jane"]


def test_combine_flow_graphs_with_filter():
    """Test combining graphs with both manual input and filter nodes."""
    # Create first graph with manual input
    graph1 = create_graph(flow_id=1)
    add_manual_input(graph1, [{"name": "john", "age": 30}], node_id=1)

    # Add filter node to first graph
    add_node_promise_on_type(graph1, "filter", 2)
    filter_input = transform_schema.FilterInput(
        advanced_filter="[age] > 25",
        filter_type='advanced'
    )
    filter_settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=filter_input
    )
    graph1.add_filter(filter_settings)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph1, connection)

    # Create second graph with manual input
    graph2 = create_graph(flow_id=2)
    add_manual_input(graph2, [{"name": "jane", "age": 20}], node_id=1)

    # Add filter node to second graph
    add_node_promise_on_type(graph2, "filter", 2)
    filter_input2 = transform_schema.FilterInput(
        advanced_filter="[age] < 25",
        filter_type='advanced'
    )
    filter_settings2 = input_schema.NodeFilter(
        flow_id=2,
        node_id=2,
        depending_on_id=1,
        filter_input=filter_input2
    )
    graph2.add_filter(filter_settings2)
    connection2 = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph2, connection2)
    # Combine the graphs
    combined = combine_flow_graphs(graph1, graph2)

    # Verify the combined graph properties
    assert len(combined.nodes) == 4
    assert len(combined.node_connections) == 2

    # Run the combined graph
    run_info = combined.run_graph()
    handle_run_info(run_info)

    # Check that filters have correct dependencies
    filter_nodes = [n for n in combined.nodes if n.node_type == "filter"]
    assert len(filter_nodes) == 2

    # Each filter should depend on a correct input node
    for filter_node in filter_nodes:
        # Check filter has one input node
        assert len(filter_node.node_inputs.main_inputs) == 1
        input_node = filter_node.node_inputs.main_inputs[0]

        # Verify input node exists and is a manual_input node
        assert input_node is not None
        assert input_node.node_type == "manual_input"

        # Verify filter works as expected
        filter_data = filter_node.get_resulting_data().collect().to_dicts()
        if "[age] > 25" in filter_node.setting_input.filter_input.advanced_filter:
            assert filter_data[0]["name"] == "john"
            assert filter_data[0]["age"] == 30
        else:
            assert filter_data[0]["name"] == "jane"
            assert filter_data[0]["age"] == 20


def test_combine_flow_graphs_with_join():
    """Test combining graphs with join operations."""
    # Create first graph with customer data
    graph1 = create_graph(flow_id=1)
    customers = [
        {"customer_id": 1, "name": "John"},
        {"customer_id": 2, "name": "Jane"}
    ]
    add_manual_input(graph1, customers, node_id=1)

    # Create second graph with order data
    graph2 = create_graph(flow_id=2)
    orders = [
        {"order_id": 101, "customer_id": 1, "amount": 100},
        {"order_id": 102, "customer_id": 1, "amount": 200},
        {"order_id": 103, "customer_id": 2, "amount": 300}
    ]
    add_manual_input(graph2, orders, node_id=1)

    # Create third graph with a join
    graph3 = create_graph(flow_id=3)
    add_manual_input(graph3, customers, node_id=1)
    add_manual_input(graph3, orders, node_id=2)

    # Add join node
    add_node_promise_on_type(graph3, "join", 3)

    # Create join settings
    join_input = transform_schema.JoinInput(
        join_mapping=[transform_schema.JoinMap(left_col='customer_id', right_col='customer_id')],
        left_select=[
            transform_schema.SelectInput(old_name='customer_id', new_name='customer_id', keep=True),
            transform_schema.SelectInput(old_name='name', new_name='name', keep=True),

        ],
        right_select=[
            transform_schema.SelectInput(old_name='customer_id', new_name='customer_id', keep=True),
            transform_schema.SelectInput(old_name='order_id', new_name='order_id', keep=True),
            transform_schema.SelectInput(old_name='amount', new_name='amount', keep=True)
        ],
        how='inner'
    )

    join_settings = input_schema.NodeJoin(
        flow_id=3,
        node_id=3,
        depending_on_ids=[1, 2],
        join_input=join_input,
        auto_generate_selection=True,
        verify_integrity=True,
        auto_keep_all=True,
        auto_keep_left=True,
        auto_keep_right=True
    )
    graph3.add_join(join_settings)

    # Add connections
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, input_type="main")
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type="right")
    add_connection(graph3, left_connection)
    add_connection(graph3, right_connection)

    # Combine the graphs
    combined = combine_flow_graphs(graph1, graph2, graph3)

    # Verify the combined graph properties
    assert len(combined.nodes) == 5  # 2 data inputs + 1 joined data input + 1 join node

    run_info = combined.run_graph()
    handle_run_info(run_info)

    # Find the join node in the combined graph
    join_nodes = [n for n in combined.nodes if n.node_type == "join"]
    assert len(join_nodes) == 1
    join_node = join_nodes[0]

    # Verify join node has correct inputs
    assert join_node.node_inputs.main_inputs is not None
    assert join_node.node_inputs.right_input is not None

    # Verify join operation produces correct results
    join_data = join_node.get_resulting_data().collect().to_dicts()
    assert len(join_data) == 3
    assert any(row["name"] == "John" and row["order_id"] == 101 and row["amount"] == 100 for row in join_data)
    assert any(row["name"] == "John" and row["order_id"] == 102 and row["amount"] == 200 for row in join_data)
    assert any(row["name"] == "Jane" and row["order_id"] == 103 and row["amount"] == 300 for row in join_data)


def test_combine_flow_graphs_with_specific_target_id():
    """Test combining graphs with a specific target flow ID."""
    # Create two simple graphs
    graph1 = create_graph(flow_id=1)
    add_manual_input(graph1, [{"name": "john"}], node_id=1)

    graph2 = create_graph(flow_id=2)
    add_manual_input(graph2, [{"name": "jane"}], node_id=1)

    # Combine with a specific target ID
    target_id = 999
    combined = combine_flow_graphs(graph1, graph2, target_flow_id=target_id)

    # Verify the combined graph has the correct ID
    assert combined.flow_id == target_id

    # Verify all nodes have the correct flow ID
    for node in combined.nodes:
        assert node.setting_input.flow_id == target_id

    # Run the combined graph
    run_info = combined.run_graph()
    handle_run_info(run_info)


def test_combine_flow_graphs_preserves_node_properties():
    """Test that combining graphs preserves node properties like positions."""
    # Create first graph with positioned nodes
    graph1 = create_graph(flow_id=1)
    node_promise = input_schema.NodePromise(
        flow_id=1,
        node_id=1,
        node_type='manual_input',
        pos_x=100,
        pos_y=200
    )
    graph1.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData.from_pylist([{"name": "john"}]),
        pos_x=100,
        pos_y=200
    )
    graph1.add_manual_input(input_file)

    # Combine the graph with itself
    combined = combine_flow_graphs(graph1)

    # Verify node properties are preserved
    for node in combined.nodes:
        # Position might be adjusted by layout algorithm, but should exist
        assert hasattr(node.setting_input, 'pos_x')
        assert hasattr(node.setting_input, 'pos_y')

        # Make sure node runs correctly
        data = node.get_resulting_data().collect().to_dicts()
        assert len(data) == 1
        assert data[0]["name"] == "john"


def test_combine_complex_flow_graphs():
    """Test combining complex graphs with multiple node types and connections."""
    # Create graph with a chain of operations
    graph1 = create_graph(flow_id=1)

    # Add input data
    data = [
        {"name": "John", "age": 30, "city": "New York"},
        {"name": "Jane", "age": 25, "city": "Boston"},
        {"name": "Mike", "age": 40, "city": "Chicago"},
        {"name": "Sarah", "age": 35, "city": "New York"}
    ]
    add_manual_input(graph1, data, node_id=1)

    # Add filter for age > 30
    add_node_promise_on_type(graph1, "filter", 2)
    filter_input = transform_schema.FilterInput(
        advanced_filter="[age] > 30",
        filter_type='advanced'
    )
    filter_settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=filter_input
    )
    graph1.add_filter(filter_settings)
    connection1 = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph1, connection1)

    # Add record ID node
    add_node_promise_on_type(graph1, "record_id", 3)
    record_id_settings = input_schema.NodeRecordId(
        flow_id=1,
        node_id=3,
        depending_on_id=2,
        record_id_input=transform_schema.RecordIdInput()
    )
    graph1.add_record_id(record_id_settings)
    connection2 = input_schema.NodeConnection.create_from_simple_input(2, 3)
    add_connection(graph1, connection2)

    # Create a second graph with a different chain
    graph2 = create_graph(flow_id=2)

    # Add input data
    data2 = [
        {"name": "Alex", "age": 22, "city": "Seattle"},
        {"name": "Lisa", "age": 45, "city": "Portland"},
        {"name": "Dave", "age": 33, "city": "Denver"}
    ]
    add_manual_input(graph2, data2, node_id=1)

    # Add filter for city = "Portland"
    add_node_promise_on_type(graph2, "filter", 2)
    filter_input2 = transform_schema.FilterInput(
        advanced_filter="[city] = 'Portland'",
        filter_type='advanced'
    )
    filter_settings2 = input_schema.NodeFilter(
        flow_id=2,
        node_id=2,
        depending_on_id=1,
        filter_input=filter_input2
    )
    graph2.add_filter(filter_settings2)
    connection3 = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph2, connection3)

    # Combine the graphs
    combined = combine_flow_graphs(graph1, graph2)

    # Verify the combined graph structure
    assert len(combined.nodes) == 5  # 2 inputs + 2 filters + 1 record_id

    # Verify the connections
    connections = combined.node_connections
    assert len(connections) == 3  # input-filter, filter-record_id, input-filter

    # Run the combined graph
    run_info = combined.run_graph()
    handle_run_info(run_info)

    # Verify each node produces correct results
    for node in combined.nodes:
        if node.node_type == "filter":
            data = node.get_resulting_data().collect().to_dicts()
            if "[age] > 30" in node.setting_input.filter_input.advanced_filter:
                # Filter from graph1 - should return 2 rows (Mike and Sarah)
                assert len(data) == 2
                assert all(row["age"] > 30 for row in data)
            elif "[city] = 'Portland'" in node.setting_input.filter_input.advanced_filter:
                # Filter from graph2 - should return 1 row (Lisa)
                assert len(data) == 1
                assert data[0]["city"] == "Portland"

        elif node.node_type == "record_id":
            # Should contain records from filter1 (Mike and Sarah) with record IDs
            data = node.get_resulting_data().collect().to_dicts()
            assert len(data) == 2
            assert "record_id" in data[0]
            assert all(row["age"] > 30 for row in data)
            assert set([row["record_id"] for row in data]) == {1, 2}
