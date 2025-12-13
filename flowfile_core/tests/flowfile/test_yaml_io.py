"""
Tests for YAML save/load functionality.

Run with:
    pytest flowfile_core/tests/flowfile/test_yaml_io.py -v

These tests use real file I/O - NO MOCKING.
"""
import tempfile
from pathlib import Path
from typing import Literal

import pytest
import yaml

from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, transform_schema, schemas
from flowfile_core.schemas.output_model import RunInformation


# =============================================================================
# HELPERS
# =============================================================================

def find_parent_directory(target_dir_name: str) -> Path:
    """Navigate up directories until finding the target directory."""
    current_path = Path(__file__)
    while current_path.parent != current_path:
        current_path = current_path.parent
        if current_path.name == target_dir_name:
            return current_path
    raise FileNotFoundError(f"Could not find directory: {target_dir_name}")


def create_graph(flow_id: int = 1, execution_mode: Literal['Development', 'Performance'] = 'Development') -> FlowGraph:
    """Create a new FlowGraph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=flow_id,
        name='test_flow',
        path='.',
        execution_mode=execution_mode
    ))
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data, node_id: int = 1):
    """Add a manual input node with data."""
    node_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data)
    )
    graph.add_manual_input(input_file)
    return graph


def add_node_promise(graph: FlowGraph, node_type: str, node_id: int):
    """Add a node promise."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type=node_type
    )
    graph.add_node_promise(node_promise)


def handle_run_info(run_info: RunInformation):
    """Check run info for errors."""
    if run_info is None:
        raise ValueError("Run info is None")
    if not run_info.success:
        errors = 'errors:'
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f'\n node_id:{node_step.node_id}, error: {node_step.error}'
        raise ValueError(f'Graph should run successfully:\n{errors}')


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def simple_graph() -> FlowGraph:
    """Create a simple graph with manual input and select."""
    graph = create_graph(flow_id=100)

    # Add manual input
    data = [
        {'name': 'Alice', 'age': 30, 'city': 'NYC'},
        {'name': 'Bob', 'age': 25, 'city': 'LA'},
        {'name': 'Charlie', 'age': 35, 'city': 'Chicago'},
    ]
    add_manual_input(graph, data, node_id=1)

    # Add select node
    add_node_promise(graph, 'select', node_id=2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)

    select_inputs = [
        transform_schema.SelectInput(old_name='name', new_name='full_name', keep=True),
        transform_schema.SelectInput(old_name='age', keep=True),
        transform_schema.SelectInput(old_name='city', keep=False),  # Drop city
    ]
    node_select = input_schema.NodeSelect(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        select_input=select_inputs,
    )
    graph.add_select(node_select)

    return graph


@pytest.fixture
def graph_with_filter() -> FlowGraph:
    """Create a graph with filter node."""
    graph = create_graph(flow_id=101)

    data = [
        {'product': 'Apple', 'price': 1.50, 'quantity': 100},
        {'product': 'Banana', 'price': 0.75, 'quantity': 150},
        {'product': 'Orange', 'price': 2.00, 'quantity': 80},
    ]
    add_manual_input(graph, data, node_id=1)

    # Add filter node
    add_node_promise(graph, 'filter', node_id=2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)

    filter_input = transform_schema.FilterInput(
        filter_type='basic',
        basic_filter=transform_schema.BasicFilter(
            field='price',
            filter_type='>',
            filter_value='1.0'
        )
    )
    node_filter = input_schema.NodeFilter(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        filter_input=filter_input,
    )
    graph.add_filter(node_filter)

    return graph


@pytest.fixture
def graph_with_group_by() -> FlowGraph:
    """Create a graph with group by node."""
    graph = create_graph(flow_id=102)

    input_data = (
        FlowDataEngine.create_random(100)
        .apply_flowfile_formula('random_int(0, 4)', 'groups')
        .select_columns(['groups', 'Country', 'sales_data'])
    )
    add_manual_input(graph, data=input_data.to_pylist(), node_id=1)

    # Add group by
    add_node_promise(graph, 'group_by', node_id=2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)

    group_by_input = transform_schema.GroupByInput([
        transform_schema.AggColl('groups', 'groupby'),
        transform_schema.AggColl('sales_data', 'sum', 'total_sales')
    ])
    node_group_by = input_schema.NodeGroupBy(
        flow_id=graph.flow_id,
        node_id=2,
        groupby_input=group_by_input,
    )
    graph.add_group_by(node_group_by)

    return graph


@pytest.fixture
def graph_with_join() -> FlowGraph:
    """Create a graph with join node."""
    graph = create_graph(flow_id=103)

    # Left table
    left_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'},
        {'id': 3, 'name': 'Charlie'},
    ]
    add_manual_input(graph, left_data, node_id=1)

    # Right table
    right_data = [
        {'id': 1, 'department': 'Sales'},
        {'id': 2, 'department': 'Engineering'},
        {'id': 4, 'department': 'Marketing'},
    ]
    add_node_promise(graph, 'manual_input', node_id=2)
    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=2,
        raw_data_format=input_schema.RawData.from_pylist(right_data)
    )
    graph.add_manual_input(input_file)

    # Add join node
    add_node_promise(graph, 'join', node_id=3)

    # Left connection
    left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
    add_connection(graph, left_conn)

    # Right connection
    right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type='right')
    add_connection(graph, right_conn)

    join_input = transform_schema.JoinInput(
        join_mapping=[transform_schema.JoinMap(left_col='id', right_col='id')],
        how='inner',
        left_select=transform_schema.JoinInputs(renames=[
            transform_schema.SelectInput(old_name='id', keep=True),
            transform_schema.SelectInput(old_name='name', keep=True),
        ]),
        right_select=transform_schema.JoinInputs(renames=[
            transform_schema.SelectInput(old_name='id', keep=False),  # Drop duplicate key
            transform_schema.SelectInput(old_name='department', keep=True),
        ]),
    )
    node_join = input_schema.NodeJoin(
        flow_id=graph.flow_id,
        node_id=3,
        join_input=join_input,
        auto_generate_selection=False,
    )
    graph.add_join(node_join)

    return graph


# =============================================================================
# BASIC SAVE/LOAD TESTS
# =============================================================================

class TestYamlSave:
    """Test saving flows to YAML."""

    def test_save_simple_graph_to_yaml(self, simple_graph: FlowGraph, temp_dir: Path):
        """Test saving a simple graph to YAML."""
        yaml_path = temp_dir / "simple_flow.yaml"
        # Save
        simple_graph.save_flow(str(yaml_path))
        # Verify file exists
        assert yaml_path.exists(), f"YAML file should exist at {yaml_path}"

        # Verify it's valid YAML
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        assert data is not None, "YAML should parse successfully"
        assert 'flowfile_version' in data, "Should have version"
        assert 'nodes' in data, "Should have nodes"
        assert len(data['nodes']) == 2, "Should have 2 nodes"

        print(f"\n=== Saved YAML ===")
        print(yaml.dump(data, default_flow_style=False))

    def test_save_graph_with_filter(self, graph_with_filter: FlowGraph, temp_dir: Path):
        """Test saving a graph with filter node."""
        yaml_path = temp_dir / "filter_flow.yaml"

        graph_with_filter.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        # Find filter node
        filter_node = next((n for n in data['nodes'] if n['type'] == 'filter'), None)
        assert filter_node is not None, "Should have filter node"
        assert 'setting_input' in filter_node, "Filter node should have settings"

        print(f"\n=== Filter Node ===")
        print(yaml.dump(filter_node, default_flow_style=False))

    def test_save_graph_with_group_by(self, graph_with_group_by: FlowGraph, temp_dir: Path):
        """Test saving a graph with group by node."""
        yaml_path = temp_dir / "groupby_flow.yaml"

        graph_with_group_by.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        # Find group_by node
        groupby_node = next((n for n in data['nodes'] if n['type'] == 'group_by'), None)
        assert groupby_node is not None, "Should have group_by node"

        print(f"\n=== Group By Node ===")
        print(yaml.dump(groupby_node, default_flow_style=False))

    def test_save_graph_with_join(self, graph_with_join: FlowGraph, temp_dir: Path):
        """Test saving a graph with join node."""
        yaml_path = temp_dir / "join_flow.yaml"
        graph_with_join.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        assert len(data['nodes']) == 3, "Should have 3 nodes (2 inputs + join)"

        # Find join node
        join_node = next((n for n in data['nodes'] if n['type'] == 'join'), None)
        assert join_node is not None, "Should have join node"
        assert 'setting_input' in join_node, "Join node should have settings"

        print(f"\n=== Join Node ===")
        print(yaml.dump(join_node, default_flow_style=False))


class TestYamlLoad:
    """Test loading flows from YAML."""

    def test_load_simple_graph_from_yaml(self, simple_graph: FlowGraph, temp_dir: Path):
        """Test loading a simple graph from YAML."""
        yaml_path = temp_dir / "simple_flow.yaml"

        # Save first
        simple_graph.save_flow(str(yaml_path))

        # Load
        loaded_flow = open_flow(yaml_path)

        assert loaded_flow is not None, "Should load flow"
        assert len(loaded_flow.nodes) == 2, "Should have 2 nodes"

        # Verify node types
        node_types = {n.node_type for n in loaded_flow.nodes}
        assert 'manual_input' in node_types, "Should have manual_input"
        assert 'select' in node_types, "Should have select"

        print(f"\n=== Loaded Flow ===")
        print(f"Flow ID: {loaded_flow.flow_id}")
        print(f"Nodes: {[n.node_id for n in loaded_flow.nodes]}")

    def test_load_graph_with_join(self, graph_with_join: FlowGraph, temp_dir: Path):
        """Test loading a graph with join from YAML."""
        yaml_path = temp_dir / "join_flow.yaml"

        # Save first
        graph_with_join.save_flow(str(yaml_path))
        # Load
        loaded_flow = open_flow(yaml_path)

        assert len(loaded_flow.nodes) == 3, "Should have 3 nodes"

        # Find join node
        join_node = next((n for n in loaded_flow.nodes if n.node_type == 'join'), None)
        assert join_node is not None, "Should have join node"
        assert join_node.setting_input is not None, "Join should have settings"

        # Verify join settings
        join_settings = join_node.setting_input
        assert hasattr(join_settings, 'join_input'), "Should have join_input"


class TestRoundTrip:
    """Test save -> load -> verify cycle."""

    def test_roundtrip_simple_graph(self, simple_graph: FlowGraph, temp_dir: Path):
        """Test full round trip for simple graph."""
        yaml_path = temp_dir / "roundtrip.yaml"

        # Run original
        original_result = simple_graph.run_graph()
        handle_run_info(original_result)
        original_data = simple_graph.get_node(2).get_resulting_data().to_pylist()

        # Save
        simple_graph.save_flow(str(yaml_path))

        # Load
        loaded_flow = open_flow(yaml_path)
        # Run loaded
        loaded_result = loaded_flow.run_graph()
        handle_run_info(loaded_result)
        loaded_data = loaded_flow.get_node(2).get_resulting_data().to_pylist()

        # Compare results
        assert len(original_data) == len(loaded_data), "Should have same number of rows"
        assert original_data == loaded_data, "Data should match"

        print(f"\n=== Round Trip Success ===")
        print(f"Original rows: {len(original_data)}")
        print(f"Loaded rows: {len(loaded_data)}")

    def test_roundtrip_preserves_connections(self, graph_with_join: FlowGraph, temp_dir: Path):
        """Test that connections are preserved in round trip."""
        yaml_path = temp_dir / "connections.yaml"

        original_connections = set(graph_with_join.node_connections)

        # Save and load
        graph_with_join.save_flow(str(yaml_path))
        loaded_flow = open_flow(yaml_path)

        loaded_connections = set(loaded_flow.node_connections)

        assert original_connections == loaded_connections, "Connections should match"

        print(f"\n=== Connections ===")
        print(f"Original: {original_connections}")
        print(f"Loaded: {loaded_connections}")

    def test_roundtrip_with_execution(self, graph_with_group_by: FlowGraph, temp_dir: Path):
        """Test that loaded graph executes correctly."""
        yaml_path = temp_dir / "executable.yaml"

        # Save
        graph_with_group_by.save_flow(str(yaml_path))

        # Load
        loaded_flow = open_flow(yaml_path)

        # Execute
        result = loaded_flow.run_graph()
        handle_run_info(result)

        # Verify results
        output_node = loaded_flow.get_node(2)
        output_data = output_node.get_resulting_data()

        assert output_data is not None, "Should have output data"
        assert output_data.number_of_records > 0, "Should have records"

        print(f"\n=== Execution Result ===")
        print(f"Records: {output_data.number_of_records}")


class TestLegacyPickleLoad:
    """Test loading legacy pickle files."""

    def test_load_existing_pickle_file(self):
        """Test loading an existing pickle file."""
        flowfile_path = (
                find_parent_directory("Flowfile")
                / "flowfile_core/tests/support_files/flows/read_csv.flowfile"
        )

        if not flowfile_path.exists():
            pytest.skip(f"Test file not found: {flowfile_path}")

        # Load pickle
        flow = open_flow(flowfile_path)

        assert flow is not None, "Should load pickle file"
        assert len(flow.nodes) > 0, "Should have nodes"

        print(f"\n=== Loaded Pickle ===")
        print(f"Flow ID: {flow.flow_id}")
        print(f"Nodes: {len(flow.nodes)}")
        for node in flow.nodes:
            print(f"  - {node.node_id}: {node.node_type}")

    def test_convert_pickle_to_yaml(self, temp_dir: Path):
        """Test converting a pickle file to YAML."""
        flowfile_path = (
                find_parent_directory("Flowfile")
                / "flowfile_core/tests/support_files/flows/read_csv.flowfile"
        )

        if not flowfile_path.exists():
            pytest.skip(f"Test file not found: {flowfile_path}")

        yaml_path = temp_dir / "converted.yaml"

        # Load pickle
        flow = open_flow(flowfile_path)

        # Save as YAML
        flow.save_flow(str(yaml_path))

        assert yaml_path.exists(), "YAML file should exist"

        # Verify YAML is valid
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        assert data is not None, "Should parse YAML"
        assert 'nodes' in data, "Should have nodes"

        print(f"\n=== Converted YAML ===")
        print(f"Nodes: {len(data['nodes'])}")
        print(yaml.dump(data, default_flow_style=False)[:1000] + "...")


class TestFlowfileDataModel:
    """Test FlowfileData Pydantic model."""

    def test_flowfile_data_model_validation(self, simple_graph: FlowGraph):
        """Test that FlowfileData validates correctly."""
        flowfile_data = simple_graph.get_flowfile_data()

        assert flowfile_data is not None, "Should create FlowfileData"
        assert flowfile_data.flowfile_version is not None, "Should have version"
        assert len(flowfile_data.nodes) == 2, "Should have 2 nodes"

        # Verify round-trip through model_dump/model_validate
        data = flowfile_data.model_dump(mode='json')
        restored = schemas.FlowfileData.model_validate(data)

        assert restored.flowfile_id == flowfile_data.flowfile_id
        assert len(restored.nodes) == len(flowfile_data.nodes)

        print(f"\n=== FlowfileData ===")
        print(f"Version: {flowfile_data.flowfile_version}")
        print(f"ID: {flowfile_data.flowfile_id}")
        print(f"Nodes: {len(flowfile_data.nodes)}")

    def test_node_settings_validation(self, graph_with_join: FlowGraph):
        """Test that node settings are properly validated."""
        flowfile_data = graph_with_join.get_flowfile_data()

        # Get the join node
        join_node = next((n for n in flowfile_data.nodes if n.type == 'join'), None)
        assert join_node is not None, "Should have join node"

        # Settings should be the correct type
        settings = join_node.setting_input
        assert isinstance(settings, input_schema.NodeJoin), f"Expected NodeJoin, got {type(settings)}"

        print(f"\n=== Join Settings ===")
        print(f"Type: {type(settings).__name__}")
        print(f"Has join_input: {hasattr(settings, 'join_input')}")


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_save_empty_graph(self, temp_dir: Path):
        """Test saving an empty graph."""
        graph = create_graph(flow_id=999)
        yaml_path = temp_dir / "empty.yaml"

        graph.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        assert data['nodes'] == [], "Empty graph should have no nodes"

    def test_load_nonexistent_file(self, temp_dir: Path):
        """Test loading a file that doesn't exist."""
        fake_path = temp_dir / "nonexistent.yaml"

        with pytest.raises(FileNotFoundError):
            open_flow(fake_path)

    def test_json_format(self, simple_graph: FlowGraph, temp_dir: Path):
        """Test saving/loading JSON format."""
        json_path = temp_dir / "flow.json"

        # Save as JSON
        simple_graph.save_flow(str(json_path))

        assert json_path.exists(), "JSON file should exist"

        # Load
        loaded = open_flow(json_path)
        assert len(loaded.nodes) == 2, "Should have 2 nodes"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])