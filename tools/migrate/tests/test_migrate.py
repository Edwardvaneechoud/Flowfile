"""
Tests for schema compatibility and migration validation.

These tests verify that:
1. Old flat ReceivedTable can be migrated to new nested table_settings
2. Old separate OutputSettings tables can be migrated to unified table_settings
3. All node types are handled correctly in migration
"""

import pickle
import tempfile
import json
from pathlib import Path
from typing import Dict, Any
import pytest


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# =============================================================================
# OLD -> NEW SCHEMA TRANSFORMATION TESTS
# =============================================================================

class TestReceivedTableTransformation:
    """Test transformation of OLD flat ReceivedTable to NEW nested table_settings."""

    def test_csv_flat_to_nested(self, temp_dir):
        """Test that flat CSV fields become nested in table_settings."""
        from tools.migrate.legacy_schemas import (
            FlowInformation, FlowSettings, NodeInformation,
            NodeRead, ReceivedTable
        )
        from tools.migrate.migrate import migrate_flowfile

        # OLD format: flat fields
        received = ReceivedTable(
            name='data.csv',
            path='/path/to/data.csv',
            file_type='csv',
            delimiter=';',
            encoding='latin-1',
            has_headers=True,
            starting_from_line=1,
            infer_schema_length=5000,
            quote_char="'",
            row_delimiter='\n',
            truncate_ragged_lines=True,
            ignore_errors=True,
        )

        node = NodeRead(flow_id=1, node_id=1, received_file=received)
        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type='read', setting_input=node)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        read_node = data['nodes'][0]
        received_file = read_node['setting_input']['received_file']

        # Verify NEW structure has table_settings
        assert 'table_settings' in received_file
        ts = received_file['table_settings']

        # Verify values migrated correctly
        assert ts['file_type'] == 'csv'
        assert ts['delimiter'] == ';'
        assert ts['encoding'] == 'latin-1'
        assert ts['has_headers'] == True
        assert ts['starting_from_line'] == 1
        assert ts['infer_schema_length'] == 5000
        assert ts['quote_char'] == "'"
        assert ts['truncate_ragged_lines'] == True
        assert ts['ignore_errors'] == True

    def test_excel_flat_to_nested(self, temp_dir):
        """Test that flat Excel fields become nested in table_settings."""
        from tools.migrate.legacy_schemas import (
            FlowInformation, FlowSettings, NodeInformation,
            NodeRead, ReceivedTable
        )
        from tools.migrate.migrate import migrate_flowfile

        # OLD format: flat fields
        received = ReceivedTable(
            name='data.xlsx',
            path='/path/to/data.xlsx',
            file_type='excel',
            sheet_name='Sales Data',
            start_row=2,
            start_column=1,
            end_row=100,
            end_column=10,
            has_headers=True,
            type_inference=True,
        )

        node = NodeRead(flow_id=1, node_id=1, received_file=received)
        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type='read', setting_input=node)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        read_node = data['nodes'][0]
        received_file = read_node['setting_input']['received_file']

        # Verify NEW structure
        assert received_file['file_type'] == 'excel'
        assert 'table_settings' in received_file
        ts = received_file['table_settings']

        assert ts['file_type'] == 'excel'
        assert ts['sheet_name'] == 'Sales Data'
        assert ts['start_row'] == 2
        assert ts['start_column'] == 1
        assert ts['end_row'] == 100
        assert ts['end_column'] == 10
        assert ts['has_headers'] == True
        assert ts['type_inference'] == True

    def test_parquet_flat_to_nested(self, temp_dir):
        """Test that parquet file type gets table_settings."""
        from tools.migrate.legacy_schemas import (
            FlowInformation, FlowSettings, NodeInformation,
            NodeRead, ReceivedTable
        )
        from tools.migrate.migrate import migrate_flowfile

        received = ReceivedTable(
            name='data.parquet',
            path='/path/to/data.parquet',
            file_type='parquet',
        )

        node = NodeRead(flow_id=1, node_id=1, received_file=received)
        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type='read', setting_input=node)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        read_node = data['nodes'][0]
        received_file = read_node['setting_input']['received_file']

        assert received_file['file_type'] == 'parquet'
        assert 'table_settings' in received_file
        assert received_file['table_settings']['file_type'] == 'parquet'


class TestOutputSettingsTransformation:
    """Test transformation of OLD separate output tables to NEW unified table_settings."""

    def test_csv_output_consolidation(self, temp_dir):
        """Test that separate output_csv_table becomes table_settings."""
        from tools.migrate.legacy_schemas import (
            FlowInformation, FlowSettings, NodeInformation,
            NodeOutput, OutputSettings, OutputCsvTable
        )
        from tools.migrate.migrate import migrate_flowfile

        # OLD format: separate table objects
        output_settings = OutputSettings(
            name='result.csv',
            directory='/output',
            file_type='csv',
            write_mode='overwrite',
            output_csv_table=OutputCsvTable(delimiter='|', encoding='utf-16'),
        )

        node = NodeOutput(flow_id=1, node_id=1, output_settings=output_settings)
        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type='output', setting_input=node)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        output_node = data['nodes'][0]
        os = output_node['setting_input']['output_settings']

        # Verify NEW structure
        assert 'table_settings' in os
        assert os['table_settings']['file_type'] == 'csv'
        assert os['table_settings']['delimiter'] == '|'
        assert os['table_settings']['encoding'] == 'utf-16'

        # Verify OLD fields removed
        assert 'output_csv_table' not in os
        assert 'output_parquet_table' not in os
        assert 'output_excel_table' not in os

    def test_excel_output_consolidation(self, temp_dir):
        """Test that separate output_excel_table becomes table_settings."""
        from tools.migrate.legacy_schemas import (
            FlowInformation, FlowSettings, NodeInformation,
            NodeOutput, OutputSettings, OutputExcelTable
        )
        from tools.migrate.migrate import migrate_flowfile

        output_settings = OutputSettings(
            name='result.xlsx',
            directory='/output',
            file_type='excel',
            output_excel_table=OutputExcelTable(sheet_name='Results'),
        )

        node = NodeOutput(flow_id=1, node_id=1, output_settings=output_settings)
        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type='output', setting_input=node)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            data = json.load(f)

        output_node = data['nodes'][0]
        os = output_node['setting_input']['output_settings']

        assert os['table_settings']['file_type'] == 'excel'
        assert os['table_settings']['sheet_name'] == 'Results'


# =============================================================================
# NODE TYPE MIGRATION TESTS
# =============================================================================

class TestNodeTypeMigration:
    """Test that all node types can be migrated correctly."""

    def _create_and_migrate(self, temp_dir, node_type: str, setting_input) -> Dict:
        """Helper to create a flow with one node and migrate it."""
        from tools.migrate.legacy_schemas import (
            FlowInformation, FlowSettings, NodeInformation
        )
        from tools.migrate.migrate import migrate_flowfile

        flow = FlowInformation(
            flow_id=1,
            flow_name='test',
            flow_settings=FlowSettings(flow_id=1, name='test'),
            data={1: NodeInformation(id=1, type=node_type, setting_input=setting_input)},
            node_starts=[1],
            node_connections=[],
        )

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='json')

        with open(output_path) as f:
            return json.load(f)

    def test_migrate_select_node(self, temp_dir):
        """Test select node migration."""
        from tools.migrate.legacy_schemas import NodeSelect, SelectInput

        node = NodeSelect(
            flow_id=1,
            node_id=1,
            select_input=[
                SelectInput(old_name='a', new_name='b', keep=True),
                SelectInput(old_name='c', keep=False),
            ]
        )

        data = self._create_and_migrate(temp_dir, 'select', node)
        assert data['nodes'][0]['type'] == 'select'
        assert 'select_input' in data['nodes'][0]['setting_input']

    def test_migrate_filter_node(self, temp_dir):
        """Test filter node migration."""
        from tools.migrate.legacy_schemas import NodeFilter, FilterInput, BasicFilter

        node = NodeFilter(
            flow_id=1,
            node_id=1,
            filter_input=FilterInput(
                filter_type='basic',
                basic_filter=BasicFilter(field='x', filter_type='>', filter_value='5')
            )
        )

        data = self._create_and_migrate(temp_dir, 'filter', node)
        assert data['nodes'][0]['type'] == 'filter'
        assert 'filter_input' in data['nodes'][0]['setting_input']

    def test_migrate_formula_node(self, temp_dir):
        """Test formula node migration."""
        from tools.migrate.legacy_schemas import NodeFormula, FunctionInput, FieldInput

        node = NodeFormula(
            flow_id=1,
            node_id=1,
            function=FunctionInput(
                field=FieldInput(name='result'),
                function='[x] + [y]'
            )
        )

        data = self._create_and_migrate(temp_dir, 'formula', node)
        assert data['nodes'][0]['type'] == 'formula'
        assert 'function' in data['nodes'][0]['setting_input']

    def test_migrate_join_node(self, temp_dir):
        """Test join node migration."""
        from tools.migrate.legacy_schemas import (
            NodeJoin, JoinInput, JoinMap, JoinInputs, SelectInput
        )

        node = NodeJoin(
            flow_id=1,
            node_id=1,
            join_input=JoinInput(
                join_mapping=[JoinMap(left_col='id', right_col='id')],
                left_select=JoinInputs(renames=[SelectInput(old_name='id')]),
                right_select=JoinInputs(renames=[SelectInput(old_name='id')]),
                how='left'
            )
        )

        data = self._create_and_migrate(temp_dir, 'join', node)
        assert data['nodes'][0]['type'] == 'join'
        assert 'join_input' in data['nodes'][0]['setting_input']

    def test_migrate_join_node_with_none_selects(self, temp_dir):
        """Test join node migration when left_select/right_select are None (old format)."""
        from tools.migrate.legacy_schemas import (
            NodeJoin, JoinInput, JoinMap
        )

        # OLD format: left_select and right_select could be None
        node = NodeJoin(
            flow_id=1,
            node_id=1,
            join_input=JoinInput(
                join_mapping=[JoinMap(left_col='id', right_col='id')],
                left_select=None,
                right_select=None,
                how='inner'
            )
        )

        data = self._create_and_migrate(temp_dir, 'join', node)
        join_input = data['nodes'][0]['setting_input']['join_input']

        # Verify migration added empty renames lists
        assert join_input['left_select'] == {'renames': []}
        assert join_input['right_select'] == {'renames': []}

    def test_migrate_groupby_node(self, temp_dir):
        """Test group by node migration."""
        from tools.migrate.legacy_schemas import (
            NodeGroupBy, GroupByInput, AggColl
        )

        node = NodeGroupBy(
            flow_id=1,
            node_id=1,
            groupby_input=GroupByInput(
                agg_cols=[
                    AggColl(old_name='category', agg='groupby'),
                    AggColl(old_name='amount', agg='sum', new_name='total'),
                ]
            )
        )

        data = self._create_and_migrate(temp_dir, 'group_by', node)
        assert data['nodes'][0]['type'] == 'group_by'
        assert 'groupby_input' in data['nodes'][0]['setting_input']

    def test_migrate_polars_code_node(self, temp_dir):
        """Test polars code node migration."""
        from tools.migrate.legacy_schemas import NodePolarsCode, PolarsCodeInput

        node = NodePolarsCode(
            flow_id=1,
            node_id=1,
            polars_code_input=PolarsCodeInput(
                polars_code='output_df = input_df.with_columns(pl.col("x") * 2)'
            ),
            depending_on_ids=[0]
        )

        data = self._create_and_migrate(temp_dir, 'polars_code', node)

        polars_node = data['nodes'][0]
        assert polars_node['type'] == 'polars_code'
        assert 'output_df' in polars_node['setting_input']['polars_code_input']['polars_code']


# =============================================================================
# LEGACY SCHEMA VALIDATION TESTS
# =============================================================================

class TestLegacySchemas:
    """Test that legacy schemas can be instantiated correctly."""

    def test_received_table_has_flat_fields(self):
        """Verify OLD ReceivedTable has flat structure."""
        from tools.migrate.legacy_schemas import ReceivedTable

        rt = ReceivedTable(
            name='test.csv',
            path='/path/test.csv',
            file_type='csv',
            delimiter=';',
            encoding='latin-1',
            sheet_name='Sheet1',  # Excel field at top level (OLD style)
        )

        # OLD style: all fields at top level
        assert rt.delimiter == ';'
        assert rt.encoding == 'latin-1'
        assert rt.sheet_name == 'Sheet1'

        # Verify no table_settings (OLD style)
        assert not hasattr(rt, 'table_settings')

    def test_output_settings_has_separate_tables(self):
        """Verify OLD OutputSettings has separate table fields."""
        from tools.migrate.legacy_schemas import (
            OutputSettings, OutputCsvTable, OutputExcelTable
        )

        os = OutputSettings(
            name='out.csv',
            directory='/out',
            file_type='csv',
            output_csv_table=OutputCsvTable(delimiter='|'),
            output_excel_table=OutputExcelTable(sheet_name='Data'),
        )

        # OLD style: separate table objects
        assert os.output_csv_table.delimiter == '|'
        assert os.output_excel_table.sheet_name == 'Data'

        # Verify no unified table_settings (OLD style)
        assert not hasattr(os, 'table_settings')

    def test_legacy_class_map_completeness(self):
        """Test that LEGACY_CLASS_MAP has all needed classes."""
        from tools.migrate.legacy_schemas import LEGACY_CLASS_MAP

        required_classes = [
            # Transform schemas
            'SelectInput', 'JoinInput', 'JoinMap', 'PolarsCodeInput',
            'GroupByInput', 'AggColl', 'FilterInput', 'BasicFilter',

            # Input/Output schemas
            'ReceivedTable', 'OutputSettings', 'OutputCsvTable',

            # Node schemas
            'NodeRead', 'NodeSelect', 'NodeOutput', 'NodeJoin',
            'NodePolarsCode', 'NodeGroupBy',

            # Flow schemas
            'FlowInformation', 'FlowSettings', 'NodeInformation',
        ]

        for cls_name in required_classes:
            assert cls_name in LEGACY_CLASS_MAP, f"Missing {cls_name}"


# =============================================================================
# ROUND TRIP TESTS
# =============================================================================

class TestRoundTrip:
    """Test complete pickle -> YAML -> validation round trips."""

    def test_complex_flow_roundtrip(self, temp_dir):
        """Test migration of a flow with multiple node types."""
        yaml = pytest.importorskip('yaml')

        from tools.migrate.legacy_schemas import (
            FlowInformation, FlowSettings, NodeInformation,
            NodeRead, ReceivedTable, NodeSelect, SelectInput,
            NodeOutput, OutputSettings, OutputCsvTable
        )
        from tools.migrate.migrate import migrate_flowfile

        flow = FlowInformation(
            flow_id=1,
            flow_name='complex_flow',
            flow_settings=FlowSettings(
                flow_id=1,
                name='complex_flow',
                description='A complex flow for testing'
            ),
            data={
                1: NodeInformation(
                    id=1, type='read',
                    setting_input=NodeRead(
                        flow_id=1, node_id=1,
                        received_file=ReceivedTable(
                            name='input.csv',
                            path='/data/input.csv',
                            file_type='csv',
                            delimiter=','
                        )
                    )
                ),
                2: NodeInformation(
                    id=2, type='select',
                    setting_input=NodeSelect(
                        flow_id=1, node_id=2,
                        select_input=[SelectInput(old_name='a')]
                    )
                ),
                3: NodeInformation(
                    id=3, type='output',
                    setting_input=NodeOutput(
                        flow_id=1, node_id=3,
                        output_settings=OutputSettings(
                            name='output.csv',
                            directory='/out',
                            file_type='csv',
                            output_csv_table=OutputCsvTable(delimiter=';')
                        )
                    )
                ),
            },
            node_starts=[1],
            node_connections=[(1, 2), (2, 3)],
        )

        pickle_path = temp_dir / 'complex.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='yaml')

        # Load and validate YAML
        with open(output_path) as f:
            data = yaml.safe_load(f)

        # Verify FlowfileData format
        assert data['flowfile_version'] == '2.0'
        assert data['flowfile_name'] == 'complex_flow'
        assert data['flowfile_id'] == 1
        assert len(data['nodes']) == 3

        # Verify transformations applied
        read_node = next(n for n in data['nodes'] if n['type'] == 'read')
        assert 'table_settings' in read_node['setting_input']['received_file']

        output_node = next(n for n in data['nodes'] if n['type'] == 'output')
        assert 'table_settings' in output_node['setting_input']['output_settings']
        assert output_node['setting_input']['output_settings']['table_settings']['delimiter'] == ';'

        # Verify start node is marked
        start_nodes = [n for n in data['nodes'] if n.get('is_start_node')]
        assert len(start_nodes) == 1
        assert start_nodes[0]['id'] == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])