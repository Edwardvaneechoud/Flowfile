"""
Tests for YAML serialization cleanup - verifying internal fields are removed
and properly reconstructed on load.

Run with:
    pytest flowfile_core/tests/flowfile/test_yaml_serialization.py -v
"""
import tempfile
from pathlib import Path

import pytest
import yaml

from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import schemas, input_schema, transform_schema
from flowfile_core.flowfile.manage.io_flowfile import open_flow


def create_graph(flow_id: int = 1) -> FlowGraph:
    """Create a new FlowGraph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=flow_id,
        name='test_flow',
        path='.',
        execution_mode='Performance'
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


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# =============================================================================
# SELECT INPUT SERIALIZATION TESTS
# =============================================================================

class TestSelectInputSerialization:
    """Test that SelectInput internal fields are removed from YAML and reconstructed on load."""

    # Fields that should NOT appear in YAML output
    INTERNAL_FIELDS = [
        'original_position',
        'data_type_change',
        'is_altered',
        'position',
        'is_available',
        'join_key',
    ]

    def test_select_input_yaml_excludes_internal_fields(self, temp_dir: Path):
        """Verify that internal fields are not present in YAML output."""
        flow = create_graph(flow_id=300)
        add_manual_input(flow, data=[{'name': 'John', 'age': 30, 'city': 'NYC'}], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        # Create select with all internal fields set
        select_input = transform_schema.SelectInput(
            old_name='name',
            new_name='full_name',
            keep=True,
            original_position=0,
            data_type_change=False,
            is_altered=True,
            position=5,
            is_available=True,
            join_key=False,
        )
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[select_input],
        )
        flow.add_select(node_select)

        # Save to YAML
        yaml_path = temp_dir / "select_test.yaml"
        flow.save_flow(str(yaml_path))

        # Parse YAML and check fields
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        select_node = next(n for n in data['nodes'] if n['type'] == 'select')
        select_items = select_node['setting_input']['select_input']

        assert len(select_items) == 1
        item = select_items[0]

        # Verify internal fields are NOT present
        for field in self.INTERNAL_FIELDS:
            assert field not in item, f"Internal field '{field}' should not be in YAML"

        # Verify user-relevant fields ARE present
        assert item['old_name'] == 'name'
        assert item['new_name'] == 'full_name'

        print(f"\n=== SelectInput in YAML ===")
        print(yaml.dump(item, default_flow_style=False))

    def test_select_input_omits_new_name_when_same(self, temp_dir: Path):
        """Verify new_name is omitted when it equals old_name."""
        flow = create_graph(flow_id=301)
        add_manual_input(flow, data=[{'name': 'John'}], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        # new_name defaults to old_name
        select_input = transform_schema.SelectInput(old_name='name', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[select_input],
        )
        flow.add_select(node_select)

        yaml_path = temp_dir / "select_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        select_node = next(n for n in data['nodes'] if n['type'] == 'select')
        item = select_node['setting_input']['select_input'][0]

        # new_name should be omitted when same as old_name
        assert 'new_name' not in item, "new_name should be omitted when equal to old_name"
        assert item['old_name'] == 'name'

    def test_select_input_omits_keep_when_true(self, temp_dir: Path):
        """Verify keep is omitted when True (default)."""
        flow = create_graph(flow_id=302)
        add_manual_input(flow, data=[{'name': 'John'}], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        select_input = transform_schema.SelectInput(old_name='name', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[select_input],
        )
        flow.add_select(node_select)

        yaml_path = temp_dir / "select_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        select_node = next(n for n in data['nodes'] if n['type'] == 'select')
        item = select_node['setting_input']['select_input'][0]

        # keep should be omitted when True
        assert 'keep' not in item, "keep should be omitted when True"

    def test_select_input_includes_keep_when_false(self, temp_dir: Path):
        """Verify keep is included when False."""
        flow = create_graph(flow_id=303)
        add_manual_input(flow, data=[{'name': 'John', 'secret': 'xyz'}], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        select_inputs = [
            transform_schema.SelectInput(old_name='name', keep=True),
            transform_schema.SelectInput(old_name='secret', keep=False),  # Drop this column
        ]
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=select_inputs,
        )
        flow.add_select(node_select)

        yaml_path = temp_dir / "select_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        select_node = next(n for n in data['nodes'] if n['type'] == 'select')
        items = select_node['setting_input']['select_input']

        dropped_item = next(i for i in items if i['old_name'] == 'secret')
        assert dropped_item['keep'] == False, "keep: false should be present for dropped columns"

    def test_select_input_roundtrip_reconstructs_internal_fields(self, temp_dir: Path):
        """Verify internal fields are reconstructed after round-trip."""
        flow = create_graph(flow_id=304)
        add_manual_input(flow, data=[{'name': 'John', 'age': 30}], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        select_inputs = [
            transform_schema.SelectInput(old_name='name', new_name='full_name', keep=True),
            transform_schema.SelectInput(old_name='age', keep=False),
        ]
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=select_inputs,
        )
        flow.add_select(node_select)

        # Save and reload
        yaml_path = temp_dir / "select_test.yaml"
        flow.save_flow(str(yaml_path))
        loaded_flow = open_flow(yaml_path)

        # Verify loaded select inputs
        loaded_node = loaded_flow.get_node(2)
        loaded_inputs = loaded_node.setting_input.select_input

        assert len(loaded_inputs) == 2

        # Check first input (renamed)
        name_input = next(i for i in loaded_inputs if i.old_name == 'name')
        assert name_input.new_name == 'full_name'
        assert name_input.keep == True
        assert name_input.is_altered == True  # Reconstructed because old_name != new_name

        # Check second input (dropped)
        age_input = next(i for i in loaded_inputs if i.old_name == 'age')
        assert age_input.keep == False
        assert age_input.new_name == 'age'

    def test_select_input_with_data_type_change(self, temp_dir: Path):
        """Verify data_type is included when there's a type change."""
        flow = create_graph(flow_id=305)
        add_manual_input(flow, data=[{'value': '123'}], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        select_input = transform_schema.SelectInput(
            old_name='value',
            data_type='Integer',
            data_type_change=True,
        )
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[select_input],
        )
        flow.add_select(node_select)

        yaml_path = temp_dir / "select_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        select_node = next(n for n in data['nodes'] if n['type'] == 'select')
        item = select_node['setting_input']['select_input'][0]

        assert item['data_type'] == 'Integer', "data_type should be present when type change occurs"
        assert 'data_type_change' not in item, "data_type_change is internal and should not be in YAML"


# =============================================================================
# JOIN INPUT SERIALIZATION TESTS
# =============================================================================

class TestJoinInputSerialization:
    """Test that JoinInput uses 'select' key instead of 'renames' in YAML."""

    def test_join_input_uses_select_key(self, temp_dir: Path):
        """Verify join uses 'select' key instead of 'renames'."""
        flow = create_graph(flow_id=400)

        # Left table
        add_manual_input(flow, data=[{'id': 1, 'name': 'John'}], node_id=1)

        # Right table
        add_node_promise(flow, 'manual_input', node_id=2)
        right_input = input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{'id': 1, 'city': 'NYC'}])
        )
        flow.add_manual_input(right_input)

        # Add join
        add_node_promise(flow, 'join', node_id=3)
        left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
        add_connection(flow, left_conn)
        right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type='right')
        add_connection(flow, right_conn)

        join_settings = input_schema.NodeJoin(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            join_input=transform_schema.JoinInput(
                how='inner',
                join_mapping=[transform_schema.JoinMap(left_col='id', right_col='id')],
                left_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='id', keep=True),
                    transform_schema.SelectInput(old_name='name', keep=True),
                ]),
                right_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='id', keep=False),
                    transform_schema.SelectInput(old_name='city', keep=True),
                ]),
            ),
            auto_generate_selection=False,
        )
        flow.add_join(join_settings)

        yaml_path = temp_dir / "join_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        join_node = next(n for n in data['nodes'] if n['type'] == 'join')
        join_input = join_node['setting_input']['join_input']

        # Verify 'select' key is used, not 'renames'
        assert 'select' in join_input['left_select'], "Should use 'select' key"
        assert 'renames' not in join_input['left_select'], "Should not have 'renames' key"
        assert 'select' in join_input['right_select'], "Should use 'select' key"
        assert 'renames' not in join_input['right_select'], "Should not have 'renames' key"

        # Verify internal fields are excluded from select items
        for item in join_input['left_select']['select']:
            assert 'join_key' not in item
            assert 'is_available' not in item

        print(f"\n=== Join Input in YAML ===")
        print(yaml.dump(join_input, default_flow_style=False))

    def test_join_input_roundtrip(self, temp_dir: Path):
        """Verify join node works correctly after round-trip."""
        flow = create_graph(flow_id=401)

        add_manual_input(flow, data=[{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}], node_id=1)

        add_node_promise(flow, 'manual_input', node_id=2)
        right_input = input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([
                {'id': 1, 'dept': 'Sales'},
                {'id': 2, 'dept': 'Engineering'}
            ])
        )
        flow.add_manual_input(right_input)

        add_node_promise(flow, 'join', node_id=3)
        left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
        add_connection(flow, left_conn)
        right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type='right')
        add_connection(flow, right_conn)

        join_settings = input_schema.NodeJoin(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            join_input=transform_schema.JoinInput(
                how='left',
                join_mapping=[transform_schema.JoinMap(left_col='id', right_col='id')],
                left_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='id', new_name='employee_id', keep=True),
                    transform_schema.SelectInput(old_name='name', keep=True),
                ]),
                right_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='id', keep=False),
                    transform_schema.SelectInput(old_name='dept', new_name='department', keep=True),
                ]),
            ),
            auto_generate_selection=False,
        )
        flow.add_join(join_settings)

        # Save and reload
        yaml_path = temp_dir / "join_test.yaml"
        flow.save_flow(str(yaml_path))
        loaded_flow = open_flow(yaml_path)

        # Verify join settings
        loaded_join = loaded_flow.get_node(3)
        assert loaded_join.setting_input.join_input.how == 'left'

        left_select = loaded_join.setting_input.join_input.left_select.renames
        assert len(left_select) == 2
        id_input = next(i for i in left_select if i.old_name == 'id')
        assert id_input.new_name == 'employee_id'

        right_select = loaded_join.setting_input.join_input.right_select.renames
        dept_input = next(i for i in right_select if i.old_name == 'dept')
        assert dept_input.new_name == 'department'
        assert dept_input.keep == True


# =============================================================================
# OUTPUT SETTINGS SERIALIZATION TESTS
# =============================================================================

class TestOutputSettingsSerialization:
    """Test OutputSettings YAML serialization."""

    def test_output_settings_no_duplicate_file_type(self, temp_dir: Path):
        """Verify file_type only appears once (not in table_settings)."""
        flow = create_graph(flow_id=500)
        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        add_node_promise(flow, 'output', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        output_settings = input_schema.NodeOutput(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            output_settings=input_schema.OutputSettings(
                name='output.csv',
                directory='/tmp',
                file_type='csv',
                write_mode='overwrite',
                table_settings=input_schema.OutputCsvTable(delimiter=',')
            )
        )
        flow.add_output(output_settings)

        yaml_path = temp_dir / "output_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        output_node = next(n for n in data['nodes'] if n['type'] == 'output')
        output_settings = output_node['setting_input']['output_settings']

        # file_type should be at top level
        assert output_settings['file_type'] == 'csv'

        # table_settings should NOT have file_type (if present)
        if 'table_settings' in output_settings:
            assert 'file_type' not in output_settings['table_settings'], \
                "file_type should not be duplicated in table_settings"

        print(f"\n=== Output Settings in YAML ===")
        print(yaml.dump(output_settings, default_flow_style=False))

    def test_output_settings_parquet_no_table_settings(self, temp_dir: Path):
        """Verify parquet output omits table_settings when empty."""
        flow = create_graph(flow_id=501)
        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        add_node_promise(flow, 'output', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        output_settings = input_schema.NodeOutput(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            output_settings=input_schema.OutputSettings(
                name='output.parquet',
                directory='/tmp',
                file_type='parquet',
                write_mode='overwrite',
                table_settings=input_schema.OutputParquetTable()
            )
        )
        flow.add_output(output_settings)

        yaml_path = temp_dir / "output_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        output_node = next(n for n in data['nodes'] if n['type'] == 'output')
        output_settings = output_node['setting_input']['output_settings']

        # Parquet has no settings beyond file_type, so table_settings should be omitted
        assert 'table_settings' not in output_settings, \
            "table_settings should be omitted for parquet (no meaningful settings)"

        print(f"\n=== Parquet Output Settings in YAML ===")
        print(yaml.dump(output_settings, default_flow_style=False))

    def test_output_settings_csv_includes_table_settings(self, temp_dir: Path):
        """Verify CSV output includes table_settings with delimiter."""
        flow = create_graph(flow_id=502)
        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        add_node_promise(flow, 'output', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        output_settings = input_schema.NodeOutput(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            output_settings=input_schema.OutputSettings(
                name='output.csv',
                directory='/tmp',
                file_type='csv',
                write_mode='overwrite',
                table_settings=input_schema.OutputCsvTable(delimiter=';', encoding='latin-1')
            )
        )
        flow.add_output(output_settings)

        yaml_path = temp_dir / "output_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        output_node = next(n for n in data['nodes'] if n['type'] == 'output')
        output_settings = output_node['setting_input']['output_settings']

        # CSV with non-default delimiter should have table_settings
        assert 'table_settings' in output_settings
        assert output_settings['table_settings']['delimiter'] == ';'
        assert output_settings['table_settings']['encoding'] == 'latin-1'

    def test_output_settings_roundtrip_parquet(self, temp_dir: Path):
        """Verify parquet output works after round-trip (no table_settings in YAML)."""
        flow = create_graph(flow_id=503)
        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        add_node_promise(flow, 'output', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        output_settings = input_schema.NodeOutput(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            output_settings=input_schema.OutputSettings(
                name='output.parquet',
                directory='/tmp',
                file_type='parquet',
                write_mode='overwrite',
                table_settings=input_schema.OutputParquetTable()
            )
        )
        flow.add_output(output_settings)

        # Save and reload
        yaml_path = temp_dir / "output_test.yaml"
        flow.save_flow(str(yaml_path))
        loaded_flow = open_flow(yaml_path)

        # Verify output settings
        loaded_output = loaded_flow.get_node(2)
        assert loaded_output.setting_input.output_settings.file_type == 'parquet'
        assert loaded_output.setting_input.output_settings.name == 'output.parquet'
        # table_settings should be reconstructed with correct type
        assert isinstance(
            loaded_output.setting_input.output_settings.table_settings,
            input_schema.OutputParquetTable
        )


# =============================================================================
# CROSS JOIN SERIALIZATION TESTS
# =============================================================================

class TestCrossJoinSerialization:
    """Test CrossJoinInput YAML serialization."""

    def test_cross_join_uses_select_key(self, temp_dir: Path):
        """Verify cross join uses 'select' key."""
        flow = create_graph(flow_id=600)

        add_manual_input(flow, data=[{'a': 1}], node_id=1)

        add_node_promise(flow, 'manual_input', node_id=2)
        right_input = input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{'b': 2}])
        )
        flow.add_manual_input(right_input)

        add_node_promise(flow, 'cross_join', node_id=3)
        left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
        add_connection(flow, left_conn)
        right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type='right')
        add_connection(flow, right_conn)

        cross_join_settings = input_schema.NodeCrossJoin(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            cross_join_input=transform_schema.CrossJoinInput(
                left_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='a', keep=True),
                ]),
                right_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='b', keep=True),
                ]),
            ),
            auto_generate_selection=False,
        )
        flow.add_cross_join(cross_join_settings)

        yaml_path = temp_dir / "cross_join_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        cross_join_node = next(n for n in data['nodes'] if n['type'] == 'cross_join')
        cross_join_input = cross_join_node['setting_input']['cross_join_input']

        assert 'select' in cross_join_input['left_select']
        assert 'renames' not in cross_join_input['left_select']

        print(f"\n=== Cross Join Input in YAML ===")
        print(yaml.dump(cross_join_input, default_flow_style=False))


# =============================================================================
# FUZZY MATCH SERIALIZATION TESTS
# =============================================================================

class TestFuzzyMatchSerialization:
    """Test FuzzyMatchInput YAML serialization."""

    def test_fuzzy_match_serialization(self, temp_dir: Path):
        """Verify fuzzy match serializes correctly."""
        flow = create_graph(flow_id=700)

        add_manual_input(flow, data=[{'name': 'John', 'id': 1}], node_id=1)

        add_node_promise(flow, 'manual_input', node_id=2)
        right_input = input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{'name': 'Jon', 'city': 'NYC'}])
        )
        flow.add_manual_input(right_input)

        add_node_promise(flow, 'fuzzy_match', node_id=3)
        left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
        add_connection(flow, left_conn)
        right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type='right')
        add_connection(flow, right_conn)

        from pl_fuzzy_frame_match.models import FuzzyMapping

        fuzzy_settings = input_schema.NodeFuzzyMatch(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            join_input=transform_schema.FuzzyMatchInput(
                join_mapping=[FuzzyMapping(
                    left_col='name',
                    right_col='name',
                    threshold_score=80.0,
                    fuzzy_type='levenshtein'
                )],
                left_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='name', new_name='left_name', keep=True),
                    transform_schema.SelectInput(old_name='id', keep=True),
                ]),
                right_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='name', new_name='right_name', keep=True),
                    transform_schema.SelectInput(old_name='city', keep=True),
                ]),
                how='inner',
                aggregate_output=False,
            ),
            auto_generate_selection=False,
        )
        flow.add_fuzzy_match(fuzzy_settings)

        yaml_path = temp_dir / "fuzzy_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        fuzzy_node = next(n for n in data['nodes'] if n['type'] == 'fuzzy_match')
        fuzzy_input = fuzzy_node['setting_input']['join_input']

        # Verify structure
        assert 'select' in fuzzy_input['left_select']
        assert 'select' in fuzzy_input['right_select']
        assert fuzzy_input['how'] == 'inner'
        assert fuzzy_input['aggregate_output'] == False

        # Verify join_mapping has fuzzy-specific fields
        mapping = fuzzy_input['join_mapping'][0]
        assert mapping['left_col'] == 'name'
        assert mapping['right_col'] == 'name'
        assert mapping['threshold_score'] == 80.0
        assert mapping['fuzzy_type'] == 'levenshtein'

        print(f"\n=== Fuzzy Match Input in YAML ===")
        print(yaml.dump(fuzzy_input, default_flow_style=False))

    def test_fuzzy_match_roundtrip(self, temp_dir: Path):
        """Verify fuzzy match works after round-trip."""
        flow = create_graph(flow_id=701)

        add_manual_input(flow, data=[{'name': 'John'}], node_id=1)

        add_node_promise(flow, 'manual_input', node_id=2)
        right_input = input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{'name': 'Jon'}])
        )
        flow.add_manual_input(right_input)

        add_node_promise(flow, 'fuzzy_match', node_id=3)
        left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
        add_connection(flow, left_conn)
        right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type='right')
        add_connection(flow, right_conn)

        from pl_fuzzy_frame_match.models import FuzzyMapping

        fuzzy_settings = input_schema.NodeFuzzyMatch(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            join_input=transform_schema.FuzzyMatchInput(
                join_mapping=[FuzzyMapping(
                    left_col='name',
                    right_col='name',
                    threshold_score=75.0,
                    fuzzy_type='jaro_winkler'
                )],
                left_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='name', keep=True),
                ]),
                right_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='name', new_name='matched_name', keep=True),
                ]),
                how='left',
                aggregate_output=True,
            ),
            auto_generate_selection=False,
        )
        flow.add_fuzzy_match(fuzzy_settings)

        # Save and reload
        yaml_path = temp_dir / "fuzzy_test.yaml"
        flow.save_flow(str(yaml_path))
        loaded_flow = open_flow(yaml_path)

        # Verify
        loaded_fuzzy = loaded_flow.get_node(3)
        join_input = loaded_fuzzy.setting_input.join_input

        assert join_input.how == 'left'
        assert join_input.aggregate_output == True
        assert len(join_input.join_mapping) == 1
        assert join_input.join_mapping[0].left_col == 'name'
        assert join_input.join_mapping[0].threshold_score == 75.0
        assert join_input.join_mapping[0].fuzzy_type == 'jaro_winkler'


# =============================================================================
# COMPREHENSIVE YAML STRUCTURE TESTS
# =============================================================================

class TestYamlStructure:
    """Test overall YAML structure and cleanliness."""

    def test_yaml_is_human_readable(self, temp_dir: Path):
        """Verify YAML output is clean and human-readable."""
        flow = create_graph(flow_id=800)

        add_manual_input(flow, data=[
            {'customer_id': 'C001', 'name': 'John Doe', 'amount': 100.50}
        ], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        select_inputs = [
            transform_schema.SelectInput(old_name='customer_id', new_name='id'),
            transform_schema.SelectInput(old_name='name', new_name='customer_name'),
            transform_schema.SelectInput(old_name='amount', keep=False),
        ]
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=select_inputs,
        )
        flow.add_select(node_select)

        yaml_path = temp_dir / "readable_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            content = f.read()

        print(f"\n=== Full YAML Output ===\n{content}")

        # Verify no internal fields leaked
        assert 'original_position' not in content
        assert 'is_available' not in content
        assert 'join_key' not in content
        assert 'data_type_change' not in content
        assert 'is_altered' not in content

        # Verify clean structure
        assert 'select_input:' in content
        assert 'old_name:' in content

    def test_minimal_yaml_for_simple_select(self, temp_dir: Path):
        """Verify simple select produces minimal YAML."""
        flow = create_graph(flow_id=801)

        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        add_node_promise(flow, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        # Simple select - just keeping column with same name
        select_input = transform_schema.SelectInput(old_name='x')
        node_select = input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[select_input],
        )
        flow.add_select(node_select)

        yaml_path = temp_dir / "minimal_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        select_node = next(n for n in data['nodes'] if n['type'] == 'select')
        item = select_node['setting_input']['select_input'][0]

        # Should only have old_name (minimal)
        assert item == {'old_name': 'x'}, f"Expected minimal dict, got: {item}"

        print(f"\n=== Minimal Select Item ===")
        print(yaml.dump(item, default_flow_style=False))


class TestNodePromiseSerialization:
    """Test that NodePromise (unconfigured nodes) serialize correctly."""

    def test_node_promise_serializes_as_null(self, temp_dir: Path):
        """Verify NodePromise becomes setting_input: null in YAML."""
        flow = create_graph(flow_id=900)

        # Add a configured input node
        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        # Add an unconfigured filter node (just the promise)
        add_node_promise(flow, 'filter', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        yaml_path = temp_dir / "promise_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            content = f.read()

        print(f"\n=== Full YAML with NodePromise ===\n{content}")

        data = yaml.safe_load(content)

        # Find the filter node
        filter_node = next(n for n in data['nodes'] if n['type'] == 'filter')

        # Verify node exists but setting_input is null
        assert filter_node['id'] == 2
        assert filter_node['type'] == 'filter'
        assert filter_node['setting_input'] is None, "NodePromise should serialize as null"
        assert filter_node['input_ids'] == [1], "Connections should be preserved"

        print(f"\n=== Filter Node (NodePromise) ===")
        print(yaml.dump(filter_node, default_flow_style=False))

    def test_record_count_vs_node_promise(self, temp_dir: Path):
        """Compare record_count (configured, minimal) vs NodePromise (unconfigured)."""
        flow = create_graph(flow_id=903)

        # Input node
        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        # Configured record_count node (has settings but minimal)
        add_node_promise(flow, 'record_count', node_id=2)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
        flow.add_record_count(input_schema.NodeRecordCount(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
        ))

        # Unconfigured filter node (NodePromise)
        add_node_promise(flow, 'filter', node_id=3)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        # Note: NOT calling flow.add_filter() - leaving as promise

        yaml_path = temp_dir / "record_count_vs_promise.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            content = f.read()

        print(f"\n=== Full YAML: record_count vs NodePromise ===\n{content}")

        data = yaml.safe_load(content)

        # Find nodes
        record_count_node = next(n for n in data['nodes'] if n['type'] == 'record_count')
        filter_node = next(n for n in data['nodes'] if n['type'] == 'filter')

        print(f"\n=== record_count node (configured, minimal settings) ===")
        print(yaml.dump(record_count_node, default_flow_style=False))

        print(f"\n=== filter node (unconfigured NodePromise) ===")
        print(yaml.dump(filter_node, default_flow_style=False))

        # record_count should have setting_input (even if minimal/empty)
        # filter (NodePromise) should have setting_input: null
        print(f"\nrecord_count setting_input: {record_count_node['setting_input']}")
        print(f"filter setting_input: {filter_node['setting_input']}")

    def test_node_promise_roundtrip_preserves_structure(self, temp_dir: Path):
        """Verify unconfigured node survives round-trip with is_setup=False."""
        flow = create_graph(flow_id=901)

        add_manual_input(flow, data=[{'x': 1}], node_id=1)

        # Add unconfigured nodes
        add_node_promise(flow, 'filter', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(flow, connection)

        add_node_promise(flow, 'select', node_id=3)
        connection2 = input_schema.NodeConnection.create_from_simple_input(2, 3)
        add_connection(flow, connection2)

        # Save and reload
        yaml_path = temp_dir / "promise_roundtrip.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            content = f.read()
        print(f"\n=== YAML before roundtrip ===\n{content}")

        loaded_flow = open_flow(yaml_path)

        # Verify unconfigured nodes loaded with is_setup=False
        filter_node = loaded_flow.get_node(2)
        assert filter_node is not None, "Filter node should exist"
        assert filter_node.setting_input.is_setup == False, "Unconfigured node should have is_setup=False"
        assert filter_node.node_type == 'filter'

        select_node = loaded_flow.get_node(3)
        assert select_node is not None, "Select node should exist"
        assert select_node.setting_input.is_setup == False

        # Verify connections preserved
        assert filter_node.main_input is not None
        assert filter_node.main_input[0].node_id == 1

        print(f"\n=== After roundtrip ===")
        print(f"filter_node.is_setup: {filter_node.is_setup}")
        print(f"filter_node.node_type: {filter_node.node_type}")
        print(f"filter_node.main_input.node_id: {filter_node.main_input[0].node_id}")

    def test_mixed_configured_and_unconfigured_nodes(self, temp_dir: Path):
        """Verify flow with both configured and unconfigured nodes works."""
        flow = create_graph(flow_id=902)

        # Configured input
        add_manual_input(flow, data=[{'name': 'John', 'age': 30}], node_id=1)

        # Configured select
        add_node_promise(flow, 'select', node_id=2)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
        flow.add_select(input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[transform_schema.SelectInput(old_name='name', new_name='customer_name')],
        ))

        # Unconfigured filter (user hasn't set it up yet)
        add_node_promise(flow, 'filter', node_id=3)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))
        # Configured record_count (minimal settings)
        add_node_promise(flow, 'record_count', node_id=4)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(3, 4))
        flow.add_record_count(input_schema.NodeRecordCount(
            flow_id=flow.flow_id,
            node_id=4,
            depending_on_id=3,
        ))

        # Unconfigured output
        add_node_promise(flow, 'output', node_id=5)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(4, 5))

        yaml_path = temp_dir / "mixed_test.yaml"
        flow.save_flow(str(yaml_path))

        with open(yaml_path, 'r') as f:
            content = f.read()

        print(f"\n=== Mixed Flow YAML ===\n{content}")

        data = yaml.safe_load(content)

        # Summary
        print(f"\n=== Node Summary ===")
        for node in data['nodes']:
            has_settings = node['setting_input'] is not None
            print(f"Node {node['id']} ({node['type']}): setting_input={'present' if has_settings else 'null'}")

        # Check configured nodes have settings
        select_node = next(n for n in data['nodes'] if n['type'] == 'select')
        assert select_node['setting_input'] is not None
        assert 'select_input' in select_node['setting_input']

        record_count_node = next(n for n in data['nodes'] if n['type'] == 'record_count')
        assert record_count_node['setting_input'] is not None

        # Check unconfigured nodes have null settings
        filter_node = next(n for n in data['nodes'] if n['type'] == 'filter')
        assert filter_node['setting_input'] is None

        output_node = next(n for n in data['nodes'] if n['type'] == 'output')
        assert output_node['setting_input'] is None

        # Verify round-trip
        loaded_flow = open_flow(yaml_path)

        print(f"\n=== After roundtrip ===")
        for node_id in [1, 2, 3, 4, 5]:
            node = loaded_flow.get_node(node_id)
            print(f"Node {node_id} ({node.node_type}): is_setup={node.is_setup}")

        assert loaded_flow.get_node(1).setting_input.is_setup == True  # manual_input
        assert loaded_flow.get_node(2).setting_input.is_setup == True  # select (configured)
        assert loaded_flow.get_node(3).setting_input.is_setup == False  # filter (NodePromise)
        assert loaded_flow.get_node(4).setting_input.is_setup == True  # record_count (configured)
        assert loaded_flow.get_node(5).setting_input.is_setup == False  # output (NodePromise)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
