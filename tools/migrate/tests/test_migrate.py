"""
Tests for the Flowfile Migration Tool.

Run with:
    pytest tools/migrate/tests/test_migrate.py -v

Or from repo root:
    python -m pytest tools/migrate/tests/ -v
"""

import pickle
import tempfile
from pathlib import Path
from dataclasses import asdict
import pytest

from tools.migrate.legacy_schemas import (
    # Transform schemas
    SelectInput,
    FieldInput,
    FunctionInput,
    FilterInput,
    BasicFilter,
    SelectInputs,
    JoinMap,
    JoinInput,
    JoinInputs,
    PolarsCodeInput,
    GroupByInput,
    AggColl,
    GraphSolverInput,
    SortByInput,
    UnionInput,
    UniqueInput,
    PivotInput,
    UnpivotInput,
    RecordIdInput,
    TextToRowsInput,
    CrossJoinInput,
    FuzzyMatchInput,
    FuzzyMapping,

    # Input/Output schemas
    ReceivedTable,
    OutputSettings,
    OutputCsvTable,
    OutputExcelTable,

    # Node schemas
    NodeRead,
    NodeSelect,
    NodeFilter,
    NodeFormula,
    NodeJoin,
    NodePolarsCode,
    NodeOutput,
    NodeGroupBy,
    NodeSort,
    NodeUnion,
    NodeUnique,

    # Flow schemas
    FlowSettings,
    NodeInformation,
    FlowInformation,

    LEGACY_CLASS_MAP,
)

from tools.migrate.migrate import (
    LegacyUnpickler,
    load_legacy_flowfile,
    convert_to_dict,
    transform_to_new_schema,
    migrate_flowfile,
    migrate_directory,
    _transform_read_settings,
    _transform_output_settings,
    _transform_polars_code_settings,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_flow_info():
    """Create a sample FlowInformation with various node types."""
    # Create a read node
    received_table = ReceivedTable(
        name='test_data.csv',
        path='/path/to/test_data.csv',
        file_type='csv',
        delimiter=',',
        encoding='utf-8',
        has_headers=True,
        starting_from_line=0,
        infer_schema_length=10000,
    )

    node_read = NodeRead(
        flow_id=1,
        node_id=1,
        received_file=received_table,
        is_setup=True,
    )

    # Create a select node
    select_inputs = [
        SelectInput(old_name='col_a', new_name='column_a', keep=True),
        SelectInput(old_name='col_b', new_name='column_b', keep=True),
        SelectInput(old_name='col_c', keep=False),
    ]

    node_select = NodeSelect(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        select_input=select_inputs,
        is_setup=True,
    )

    # Create a polars code node
    polars_code = PolarsCodeInput(
        polars_code='output_df = input_df.with_columns(pl.col("x") * 2)'
    )

    node_polars = NodePolarsCode(
        flow_id=1,
        node_id=3,
        polars_code_input=polars_code,
        depending_on_ids=[2],
        is_setup=True,
    )

    # Create an output node
    output_settings = OutputSettings(
        name='output.csv',
        directory='/output',
        file_type='csv',
        write_mode='overwrite',
        output_csv_table=OutputCsvTable(delimiter=';', encoding='utf-8'),
    )

    node_output = NodeOutput(
        flow_id=1,
        node_id=4,
        depending_on_id=3,
        output_settings=output_settings,
        is_setup=True,
    )

    # Build flow
    flow_settings = FlowSettings(
        flow_id=1,
        name='test_flow',
        description='A test flow',
        path='/flows/test_flow.flowfile',
    )

    flow_info = FlowInformation(
        flow_id=1,
        flow_name='test_flow',
        flow_settings=flow_settings,
        data={
            1: NodeInformation(id=1, type='read', is_setup=True, outputs=[2], setting_input=node_read),
            2: NodeInformation(id=2, type='select', is_setup=True, input_ids=[1], outputs=[3], setting_input=node_select),
            3: NodeInformation(id=3, type='polars_code', is_setup=True, input_ids=[2], outputs=[4], setting_input=node_polars),
            4: NodeInformation(id=4, type='output', is_setup=True, input_ids=[3], outputs=[], setting_input=node_output),
        },
        node_starts=[1],
        node_connections=[(1, 2), (2, 3), (3, 4)],
    )

    return flow_info


@pytest.fixture
def sample_join_flow():
    """Create a sample flow with join nodes."""
    # Left input
    left_table = ReceivedTable(
        name='left.csv',
        path='/path/to/left.csv',
        file_type='csv',
    )
    node_left = NodeRead(flow_id=1, node_id=1, received_file=left_table, is_setup=True)

    # Right input
    right_table = ReceivedTable(
        name='right.csv',
        path='/path/to/right.csv',
        file_type='csv',
    )
    node_right = NodeRead(flow_id=1, node_id=2, received_file=right_table, is_setup=True)

    # Join node
    join_input = JoinInput(
        join_mapping=[
            JoinMap(left_col='id', right_col='id'),
            JoinMap(left_col='date', right_col='date'),
        ],
        left_select=JoinInputs(renames=[
            SelectInput(old_name='id', new_name='id', keep=True, join_key=True),
            SelectInput(old_name='value', new_name='left_value', keep=True),
        ]),
        right_select=JoinInputs(renames=[
            SelectInput(old_name='id', new_name='id', keep=False, join_key=True),
            SelectInput(old_name='value', new_name='right_value', keep=True),
        ]),
        how='left',
    )

    node_join = NodeJoin(
        flow_id=1,
        node_id=3,
        join_input=join_input,
        depending_on_ids=[1, 2],
        is_setup=True,
    )

    flow_info = FlowInformation(
        flow_id=1,
        flow_name='join_flow',
        flow_settings=FlowSettings(flow_id=1, name='join_flow'),
        data={
            1: NodeInformation(id=1, type='read', outputs=[3], setting_input=node_left),
            2: NodeInformation(id=2, type='read', outputs=[3], setting_input=node_right),
            3: NodeInformation(id=3, type='join', left_input_id=1, right_input_id=2, outputs=[], setting_input=node_join),
        },
        node_starts=[1, 2],
        node_connections=[(1, 3), (2, 3)],
    )

    return flow_info


@pytest.fixture
def sample_excel_flow():
    """Create a sample flow reading Excel file."""
    received_table = ReceivedTable(
        name='data.xlsx',
        path='/path/to/data.xlsx',
        file_type='excel',
        sheet_name='Sheet1',
        start_row=1,
        start_column=0,
        end_row=100,
        end_column=5,
        has_headers=True,
        type_inference=True,
    )

    node_read = NodeRead(
        flow_id=1,
        node_id=1,
        received_file=received_table,
        is_setup=True,
    )

    flow_info = FlowInformation(
        flow_id=1,
        flow_name='excel_flow',
        flow_settings=FlowSettings(flow_id=1, name='excel_flow'),
        data={
            1: NodeInformation(id=1, type='read', outputs=[], setting_input=node_read),
        },
        node_starts=[1],
        node_connections=[],
    )

    return flow_info


# =============================================================================
# LEGACY SCHEMA TESTS
# =============================================================================

class TestLegacySchemas:
    """Test that legacy schemas can be instantiated and serialized."""

    def test_select_input_defaults(self):
        """Test SelectInput default values."""
        si = SelectInput(old_name='test')
        assert si.old_name == 'test'
        assert si.new_name == 'test'  # Should default to old_name
        assert si.keep == True
        assert si.join_key == False

    def test_select_input_with_rename(self):
        """Test SelectInput with rename."""
        si = SelectInput(old_name='old', new_name='new', keep=True)
        assert si.old_name == 'old'
        assert si.new_name == 'new'

    def test_polars_code_input(self):
        """Test PolarsCodeInput."""
        code = 'output_df = input_df.filter(pl.col("x") > 0)'
        pci = PolarsCodeInput(polars_code=code)
        assert pci.polars_code == code

    def test_join_map(self):
        """Test JoinMap."""
        jm = JoinMap(left_col='a', right_col='b')
        assert jm.left_col == 'a'
        assert jm.right_col == 'b'

    def test_received_table_csv(self):
        """Test ReceivedTable for CSV."""
        rt = ReceivedTable(
            name='test.csv',
            path='/path/test.csv',
            file_type='csv',
            delimiter=';',
        )
        assert rt.file_type == 'csv'
        assert rt.delimiter == ';'

    def test_received_table_excel(self):
        """Test ReceivedTable for Excel."""
        rt = ReceivedTable(
            name='test.xlsx',
            path='/path/test.xlsx',
            file_type='excel',
            sheet_name='Data',
            start_row=5,
        )
        assert rt.file_type == 'excel'
        assert rt.sheet_name == 'Data'
        assert rt.start_row == 5

    def test_output_settings(self):
        """Test OutputSettings with CSV table."""
        csv_table = OutputCsvTable(delimiter='|', encoding='latin-1')
        os = OutputSettings(
            name='out.csv',
            directory='/out',
            file_type='csv',
            output_csv_table=csv_table,
        )
        assert os.output_csv_table.delimiter == '|'
        assert os.output_csv_table.encoding == 'latin-1'

    def test_flow_information(self):
        """Test FlowInformation structure."""
        fi = FlowInformation(
            flow_id=1,
            flow_name='test',
            node_starts=[1],
            node_connections=[(1, 2)],
        )
        assert fi.flow_id == 1
        assert fi.flow_name == 'test'
        assert fi.node_connections == [(1, 2)]

    def test_legacy_class_map_completeness(self):
        """Test that LEGACY_CLASS_MAP contains all expected classes."""
        expected_classes = [
            'SelectInput', 'PolarsCodeInput', 'JoinMap', 'JoinInput',
            'ReceivedTable', 'OutputSettings', 'NodeRead', 'NodePolarsCode',
            'FlowInformation', 'FlowSettings', 'NodeInformation',
        ]
        for cls_name in expected_classes:
            assert cls_name in LEGACY_CLASS_MAP, f"Missing {cls_name} in LEGACY_CLASS_MAP"


# =============================================================================
# PICKLE LOADING TESTS
# =============================================================================

class TestLegacyUnpickler:
    """Test the custom unpickler for legacy files."""

    def test_pickle_roundtrip_simple(self, temp_dir):
        """Test pickle save/load with simple data."""
        pci = PolarsCodeInput(polars_code='test code')

        pickle_path = temp_dir / 'test.pickle'
        with open(pickle_path, 'wb') as f:
            pickle.dump(pci, f)

        loaded = load_legacy_flowfile(pickle_path)
        assert loaded.polars_code == 'test code'

    def test_pickle_roundtrip_flow(self, temp_dir, sample_flow_info):
        """Test pickle save/load with full flow."""
        pickle_path = temp_dir / 'flow.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_flow_info, f)

        loaded = load_legacy_flowfile(pickle_path)
        assert loaded.flow_id == 1
        assert loaded.flow_name == 'test_flow'
        assert len(loaded.data) == 4
        assert loaded.node_connections == [(1, 2), (2, 3), (3, 4)]

    def test_pickle_preserves_nested_structures(self, temp_dir, sample_join_flow):
        """Test that nested structures are preserved through pickle."""
        pickle_path = temp_dir / 'join.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_join_flow, f)

        loaded = load_legacy_flowfile(pickle_path)

        # Check join node
        join_node = loaded.data[3].setting_input
        assert len(join_node.join_input.join_mapping) == 2
        assert join_node.join_input.join_mapping[0].left_col == 'id'
        assert join_node.join_input.how == 'left'


# =============================================================================
# CONVERSION TESTS
# =============================================================================

class TestConvertToDict:
    """Test the convert_to_dict function."""

    def test_convert_simple_dataclass(self):
        """Test converting simple dataclass."""
        si = SelectInput(old_name='test', new_name='test2')
        result = convert_to_dict(si)

        assert isinstance(result, dict)
        assert result['old_name'] == 'test'
        assert result['new_name'] == 'test2'

    def test_convert_nested_dataclass(self):
        """Test converting nested dataclasses."""
        join_input = JoinInput(
            join_mapping=[JoinMap(left_col='a', right_col='b')],
            how='inner',
        )

        result = convert_to_dict(join_input)

        assert isinstance(result, dict)
        assert isinstance(result['join_mapping'], list)
        assert result['join_mapping'][0]['left_col'] == 'a'
        assert result['how'] == 'inner'

    def test_convert_flow_info(self, sample_flow_info):
        """Test converting full FlowInformation."""
        result = convert_to_dict(sample_flow_info)

        assert isinstance(result, dict)
        assert result['flow_id'] == 1
        assert result['flow_name'] == 'test_flow'
        assert isinstance(result['data'], dict)
        assert 1 in result['data']

    def test_convert_handles_none(self):
        """Test that None values are preserved."""
        si = SelectInput(old_name='test', data_type=None)
        result = convert_to_dict(si)

        assert result['data_type'] is None

    def test_convert_handles_lists(self):
        """Test that lists are converted properly."""
        result = convert_to_dict([1, 2, 3])
        assert result == [1, 2, 3]

        result = convert_to_dict([SelectInput(old_name='a'), SelectInput(old_name='b')])
        assert len(result) == 2
        assert result[0]['old_name'] == 'a'

    def test_convert_handles_primitives(self):
        """Test that primitives pass through unchanged."""
        assert convert_to_dict('test') == 'test'
        assert convert_to_dict(123) == 123
        assert convert_to_dict(1.5) == 1.5
        assert convert_to_dict(True) == True
        assert convert_to_dict(None) is None


# =============================================================================
# TRANSFORMATION TESTS
# =============================================================================

class TestTransformToNewSchema:
    """Test the schema transformation functions."""

    def test_transform_adds_version(self, sample_flow_info):
        """Test that transformation adds version info."""
        data_dict = convert_to_dict(sample_flow_info)
        result = transform_to_new_schema(data_dict)

        assert result['_version'] == '2.0'
        assert result['_migrated_from'] == 'pickle'

    def test_transform_preserves_flow_info(self, sample_flow_info):
        """Test that basic flow info is preserved."""
        data_dict = convert_to_dict(sample_flow_info)
        result = transform_to_new_schema(data_dict)

        assert result['flow_id'] == 1
        assert result['flow_name'] == 'test_flow'
        assert result['node_starts'] == [1]
        assert result['connections'] == [(1, 2), (2, 3), (3, 4)]

    def test_transform_nodes_to_list(self, sample_flow_info):
        """Test that nodes dict is converted to list."""
        data_dict = convert_to_dict(sample_flow_info)
        result = transform_to_new_schema(data_dict)

        assert isinstance(result['nodes'], list)
        assert len(result['nodes']) == 4

    def test_transform_node_has_position(self, sample_flow_info):
        """Test that nodes have position structure."""
        data_dict = convert_to_dict(sample_flow_info)
        result = transform_to_new_schema(data_dict)

        node = result['nodes'][0]
        assert 'position' in node
        assert 'x' in node['position']
        assert 'y' in node['position']


class TestTransformReadSettings:
    """Test _transform_read_settings function."""

    def test_csv_settings(self):
        """Test CSV table settings transformation."""
        settings = {
            'received_file': {
                'name': 'test.csv',
                'path': '/path/test.csv',
                'file_type': 'csv',
                'delimiter': ';',
                'encoding': 'latin-1',
                'has_headers': True,
                'starting_from_line': 5,
            }
        }

        result = _transform_read_settings(settings)

        assert result['received_file']['file_type'] == 'csv'
        assert 'table_settings' in result['received_file']
        assert result['received_file']['table_settings']['delimiter'] == ';'
        assert result['received_file']['table_settings']['encoding'] == 'latin-1'
        assert result['received_file']['table_settings']['starting_from_line'] == 5

    def test_excel_settings(self):
        """Test Excel table settings transformation."""
        settings = {
            'received_file': {
                'name': 'test.xlsx',
                'path': '/path/test.xlsx',
                'file_type': 'excel',
                'sheet_name': 'Data',
                'start_row': 10,
                'end_row': 100,
            }
        }

        result = _transform_read_settings(settings)

        assert result['received_file']['table_settings']['file_type'] == 'excel'
        assert result['received_file']['table_settings']['sheet_name'] == 'Data'
        assert result['received_file']['table_settings']['start_row'] == 10

    def test_parquet_settings(self):
        """Test Parquet table settings transformation."""
        settings = {
            'received_file': {
                'name': 'test.parquet',
                'path': '/path/test.parquet',
                'file_type': 'parquet',
            }
        }

        result = _transform_read_settings(settings)

        assert result['received_file']['table_settings']['file_type'] == 'parquet'


class TestTransformOutputSettings:
    """Test _transform_output_settings function."""

    def test_csv_output(self):
        """Test CSV output settings transformation."""
        settings = {
            'output_settings': {
                'name': 'out.csv',
                'directory': '/out',
                'file_type': 'csv',
                'write_mode': 'overwrite',
                'output_csv_table': {
                    'delimiter': '|',
                    'encoding': 'utf-16',
                },
            }
        }

        result = _transform_output_settings(settings)

        assert result['output_settings']['table_settings']['file_type'] == 'csv'
        assert result['output_settings']['table_settings']['delimiter'] == '|'
        assert result['output_settings']['table_settings']['encoding'] == 'utf-16'

    def test_excel_output(self):
        """Test Excel output settings transformation."""
        settings = {
            'output_settings': {
                'name': 'out.xlsx',
                'directory': '/out',
                'file_type': 'excel',
                'output_excel_table': {
                    'sheet_name': 'Results',
                },
            }
        }

        result = _transform_output_settings(settings)

        assert result['output_settings']['table_settings']['file_type'] == 'excel'
        assert result['output_settings']['table_settings']['sheet_name'] == 'Results'


class TestTransformPolarsCodeSettings:
    """Test _transform_polars_code_settings function."""

    def test_extracts_code(self):
        """Test that polars code is extracted."""
        settings = {
            'polars_code_input': {
                'polars_code': 'output_df = input_df.select("a")',
            },
            'depending_on_ids': [1, 2],
        }

        result = _transform_polars_code_settings(settings)

        assert result['polars_code'] == 'output_df = input_df.select("a")'
        assert result['depending_on_ids'] == [1, 2]


# =============================================================================
# FULL MIGRATION TESTS
# =============================================================================

class TestMigrateFlowfile:
    """Test the full migration process."""

    def test_migrate_creates_yaml(self, temp_dir, sample_flow_info):
        """Test that migration creates a YAML file."""
        pytest.importorskip('yaml')

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_flow_info, f)

        output_path = migrate_flowfile(pickle_path)

        assert output_path.exists()
        assert output_path.suffix == '.yaml'

    def test_migrate_creates_json(self, temp_dir, sample_flow_info):
        """Test that migration can create JSON."""
        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_flow_info, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        assert output_path.exists()
        assert output_path.suffix == '.json'

        # Verify JSON is valid
        import json
        with open(output_path) as f:
            data = json.load(f)
        assert data['flow_name'] == 'test_flow'

    def test_migrate_custom_output_path(self, temp_dir, sample_flow_info):
        """Test migration with custom output path."""
        pytest.importorskip('yaml')

        pickle_path = temp_dir / 'test.flowfile'
        custom_output = temp_dir / 'custom' / 'output.yaml'
        custom_output.parent.mkdir(parents=True)

        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_flow_info, f)

        output_path = migrate_flowfile(pickle_path, custom_output)

        assert output_path == custom_output
        assert output_path.exists()

    def test_migrate_preserves_polars_code(self, temp_dir, sample_flow_info):
        """Test that polars code is preserved correctly."""
        import json

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_flow_info, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        # Find polars_code node
        polars_node = next(n for n in data['nodes'] if n['type'] == 'polars_code')
        assert 'output_df = input_df.with_columns' in polars_node['settings']['polars_code']

    def test_migrate_preserves_join_structure(self, temp_dir, sample_join_flow):
        """Test that join structures are preserved."""
        import json

        pickle_path = temp_dir / 'join.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_join_flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        # Find join node
        join_node = next(n for n in data['nodes'] if n['type'] == 'join')
        assert 'join_input' in join_node['settings']


class TestMigrateDirectory:
    """Test directory migration."""

    def test_migrate_multiple_files(self, temp_dir, sample_flow_info):
        """Test migrating multiple files in a directory."""
        pytest.importorskip('yaml')

        # Create multiple flowfiles
        for i in range(3):
            flow = FlowInformation(
                flow_id=i,
                flow_name=f'flow_{i}',
                flow_settings=FlowSettings(flow_id=i, name=f'flow_{i}'),
                data={},
                node_starts=[],
                node_connections=[],
            )
            with open(temp_dir / f'flow_{i}.flowfile', 'wb') as f:
                pickle.dump(flow, f)

        migrated = migrate_directory(temp_dir)

        assert len(migrated) == 3
        for path in migrated:
            assert path.exists()
            assert path.suffix == '.yaml'

    def test_migrate_preserves_subdirectories(self, temp_dir, sample_flow_info):
        """Test that subdirectory structure is preserved."""
        pytest.importorskip('yaml')

        # Create subdirectory structure
        subdir = temp_dir / 'subdir' / 'nested'
        subdir.mkdir(parents=True)

        with open(temp_dir / 'root.flowfile', 'wb') as f:
            pickle.dump(sample_flow_info, f)
        with open(subdir / 'nested.flowfile', 'wb') as f:
            pickle.dump(sample_flow_info, f)

        migrated = migrate_directory(temp_dir)

        assert len(migrated) == 2
        # Check paths preserved
        paths = [str(p.relative_to(temp_dir)) for p in migrated]
        assert 'root.yaml' in paths
        assert 'subdir/nested/nested.yaml' in paths or str(Path('subdir/nested/nested.yaml')) in paths

    def test_migrate_empty_directory(self, temp_dir):
        """Test migrating empty directory."""
        migrated = migrate_directory(temp_dir)
        assert migrated == []

    def test_migrate_to_different_output_dir(self, temp_dir, sample_flow_info):
        """Test migrating to a different output directory."""
        pytest.importorskip('yaml')

        input_dir = temp_dir / 'input'
        output_dir = temp_dir / 'output'
        input_dir.mkdir()

        with open(input_dir / 'test.flowfile', 'wb') as f:
            pickle.dump(sample_flow_info, f)

        migrated = migrate_directory(input_dir, output_dir)

        assert len(migrated) == 1
        assert migrated[0].parent == output_dir


# =============================================================================
# EDGE CASE TESTS
# =============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_flow(self, temp_dir):
        """Test migrating an empty flow."""
        import json

        flow = FlowInformation(
            flow_id=1,
            flow_name='empty',
            flow_settings=FlowSettings(flow_id=1, name='empty'),
            data={},
            node_starts=[],
            node_connections=[],
        )

        pickle_path = temp_dir / 'empty.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        assert data['flow_name'] == 'empty'
        assert data['nodes'] == []

    def test_none_values_in_settings(self, temp_dir):
        """Test handling of None values in settings."""
        import json

        node_read = NodeRead(
            flow_id=1,
            node_id=1,
            received_file=ReceivedTable(
                name='test.csv',
                path='/path/test.csv',
                file_type='csv',
                sheet_name=None,  # None value
            ),
        )

        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type='read', setting_input=node_read)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        # Should not raise
        output_path = migrate_flowfile(pickle_path, format='json')
        assert output_path.exists()

    def test_special_characters_in_code(self, temp_dir):
        """Test handling of special characters in polars code."""
        import json

        polars_code = PolarsCodeInput(
            polars_code='''
# Comment with "quotes" and 'apostrophes'
output_df = input_df.filter(
    pl.col("name").str.contains(r"[a-z]+")  # regex
)
'''
        )

        node = NodePolarsCode(
            flow_id=1,
            node_id=1,
            polars_code_input=polars_code,
        )

        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type='polars_code', setting_input=node)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        node_settings = data['nodes'][0]['settings']
        assert 'quotes' in node_settings['polars_code']
        assert 'regex' in node_settings['polars_code']

    def test_unicode_in_paths(self, temp_dir):
        """Test handling of unicode in file paths."""
        import json

        node_read = NodeRead(
            flow_id=1,
            node_id=1,
            received_file=ReceivedTable(
                name='données.csv',  # French
                path='/путь/données.csv',  # Russian + French
                file_type='csv',
            ),
        )

        flow = FlowInformation(
            flow_id=1,
            flow_name='тест',  # Russian
            flow_settings=FlowSettings(flow_id=1, name='тест'),
            data={1: NodeInformation(id=1, type='read', setting_input=node_read)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path, encoding='utf-8') as f:
            data = json.load(f)

        assert data['flow_name'] == 'тест'


# =============================================================================
# YAML OUTPUT TESTS
# =============================================================================

class TestYamlOutput:
    """Test YAML-specific output features."""

    def test_yaml_multiline_code(self, temp_dir, sample_flow_info):
        """Test that multiline code is formatted nicely in YAML."""
        yaml = pytest.importorskip('yaml')

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_flow_info, f)

        output_path = migrate_flowfile(pickle_path, format='yaml')

        with open(output_path) as f:
            content = f.read()

        # YAML should be readable
        data = yaml.safe_load(content)
        assert data['flow_name'] == 'test_flow'

    def test_yaml_is_valid(self, temp_dir, sample_flow_info):
        """Test that output YAML is valid and can be loaded."""
        yaml = pytest.importorskip('yaml')

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_flow_info, f)

        output_path = migrate_flowfile(pickle_path, format='yaml')

        # Should not raise
        with open(output_path) as f:
            data = yaml.safe_load(f)

        assert isinstance(data, dict)
        assert 'nodes' in data


if __name__ == '__main__':
    pytest.main([__file__, '-v'])