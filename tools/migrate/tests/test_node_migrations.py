"""
Tests for migration tool - verifies all node types migrate correctly.

Run with:
    pytest tools/migrate/tests/test_node_migrations.py -v
"""

import pickle
import tempfile
import json
from pathlib import Path
import pytest

from tools.migrate.legacy_schemas import (
    # Flow schemas
    FlowInformation, FlowSettings, NodeInformation,

    # Node schemas
    NodeRead, NodeSelect, NodeFilter, NodeFormula, NodeOutput,
    NodeJoin, NodeCrossJoin, NodeFuzzyMatch, NodePolarsCode,
    NodeGroupBy, NodeSort, NodeUnion, NodeUnique,
    NodePivot, NodeUnpivot, NodeRecordId, NodeTextToRows,
    NodeGraphSolver, NodeSample,

    # Input/Output schemas
    ReceivedTable, OutputSettings, OutputCsvTable, OutputExcelTable,

    # Transform schemas
    SelectInput, SelectInputs, JoinInputs, JoinInput, JoinMap,
    CrossJoinInput, FuzzyMatchInput, FuzzyMapping,
    FilterInput, BasicFilter, FunctionInput, FieldInput,
    GroupByInput, AggColl, SortByInput, UnionInput, UniqueInput,
    PivotInput, UnpivotInput, RecordIdInput, TextToRowsInput,
    GraphSolverInput, PolarsCodeInput,
)

from tools.migrate.migrate import migrate_flowfile


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def create_flow_with_node(node_type: str, node_setting) -> FlowInformation:
    """Helper to create a flow with a single node."""
    return FlowInformation(
        flow_id=1,
        flow_name='test',
        flow_settings=FlowSettings(flow_id=1, name='test'),
        data={1: NodeInformation(id=1, type=node_type, setting_input=node_setting)},
        node_starts=[1],
        node_connections=[],
    )


def pickle_and_migrate(temp_dir: Path, flow: FlowInformation) -> dict:
    """Pickle a flow, migrate it, return JSON result."""
    pickle_path = temp_dir / 'test.flowfile'
    with open(pickle_path, 'wb') as f:
        pickle.dump(flow, f)

    output_path = migrate_flowfile(pickle_path, format='json')

    with open(output_path) as f:
        return json.load(f)


# =============================================================================
# INPUT NODE TESTS
# =============================================================================

class TestReadNodeMigration:
    """Test NodeRead migrations with different file types."""

    def test_csv_read_migration(self, temp_dir):
        """CSV read with custom settings."""
        node = NodeRead(
            flow_id=1, node_id=1,
            received_file=ReceivedTable(
                name='data.csv',
                path='/data/data.csv',
                file_type='csv',
                delimiter=';',
                encoding='latin-1',
                has_headers=True,
                starting_from_line=1,
                infer_schema_length=5000,
                quote_char="'",
                truncate_ragged_lines=True,
                ignore_errors=True,
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('read', node))

        rf = data['nodes'][0]['setting_input']['received_file']
        assert rf['file_type'] == 'csv'
        assert 'table_settings' in rf
        assert rf['table_settings']['delimiter'] == ';'
        assert rf['table_settings']['encoding'] == 'latin-1'
        assert rf['table_settings']['starting_from_line'] == 1

    def test_excel_read_migration(self, temp_dir):
        """Excel read with sheet and range settings."""
        node = NodeRead(
            flow_id=1, node_id=1,
            received_file=ReceivedTable(
                name='data.xlsx',
                path='/data/data.xlsx',
                file_type='excel',
                sheet_name='Sales',
                start_row=2,
                start_column=1,
                end_row=100,
                end_column=10,
                has_headers=True,
                type_inference=True,
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('read', node))

        rf = data['nodes'][0]['setting_input']['received_file']
        assert rf['file_type'] == 'excel'
        assert rf['table_settings']['sheet_name'] == 'Sales'
        assert rf['table_settings']['start_row'] == 2
        assert rf['table_settings']['type_inference'] == True

    def test_parquet_read_migration(self, temp_dir):
        """Parquet read."""
        node = NodeRead(
            flow_id=1, node_id=1,
            received_file=ReceivedTable(
                name='data.parquet',
                path='/data/data.parquet',
                file_type='parquet',
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('read', node))

        rf = data['nodes'][0]['setting_input']['received_file']
        assert rf['file_type'] == 'parquet'
        assert rf['table_settings']['file_type'] == 'parquet'


# =============================================================================
# OUTPUT NODE TESTS
# =============================================================================

class TestOutputNodeMigration:
    """Test NodeOutput migrations."""

    def test_csv_output_migration(self, temp_dir):
        """CSV output with custom delimiter."""
        node = NodeOutput(
            flow_id=1, node_id=1,
            output_settings=OutputSettings(
                name='result.csv',
                directory='/output',
                file_type='csv',
                write_mode='overwrite',
                output_csv_table=OutputCsvTable(delimiter='|', encoding='utf-16'),
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('output', node))

        os = data['nodes'][0]['setting_input']['output_settings']
        assert os['file_type'] == 'csv'
        assert 'table_settings' in os
        assert os['table_settings']['delimiter'] == '|'
        assert os['table_settings']['encoding'] == 'utf-16'
        # Old fields should be removed
        assert 'output_csv_table' not in os

    def test_excel_output_migration(self, temp_dir):
        """Excel output with sheet name."""
        node = NodeOutput(
            flow_id=1, node_id=1,
            output_settings=OutputSettings(
                name='result.xlsx',
                directory='/output',
                file_type='excel',
                output_excel_table=OutputExcelTable(sheet_name='Results'),
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('output', node))

        os = data['nodes'][0]['setting_input']['output_settings']
        assert os['file_type'] == 'excel'
        assert os['table_settings']['sheet_name'] == 'Results'


# =============================================================================
# TRANSFORM NODE TESTS
# =============================================================================

class TestSelectNodeMigration:
    """Test NodeSelect migrations."""

    def test_select_with_renames(self, temp_dir):
        """Select with column renames and drops."""
        node = NodeSelect(
            flow_id=1, node_id=1,
            select_input=[
                SelectInput(old_name='col_a', new_name='column_a', keep=True),
                SelectInput(old_name='col_b', new_name='column_b', keep=True, data_type='String'),
                SelectInput(old_name='col_c', keep=False),
            ],
            sorted_by='asc',
            keep_missing=True,
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('select', node))

        setting_input = data['nodes'][0]['setting_input']
        assert 'select_input' in setting_input
        assert len(setting_input['select_input']) == 3
        assert setting_input['select_input'][0]['old_name'] == 'col_a'
        assert setting_input['select_input'][0]['new_name'] == 'column_a'

    def test_select_adds_position(self, temp_dir):
        """Verify positions are added to select inputs."""
        node = NodeSelect(
            flow_id=1, node_id=1,
            select_input=[
                SelectInput(old_name='a'),
                SelectInput(old_name='b'),
            ]
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('select', node))

        inputs = data['nodes'][0]['setting_input']['select_input']
        assert inputs[0].get('position') == 0
        assert inputs[1].get('position') == 1


class TestFilterNodeMigration:
    """Test NodeFilter migrations."""

    def test_basic_filter(self, temp_dir):
        """Basic filter with single condition."""
        node = NodeFilter(
            flow_id=1, node_id=1,
            filter_input=FilterInput(
                filter_type='basic',
                basic_filter=BasicFilter(
                    field='amount',
                    filter_type='>',
                    filter_value='100'
                )
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('filter', node))

        fi = data['nodes'][0]['setting_input']['filter_input']
        assert fi['filter_type'] == 'basic'
        assert fi['basic_filter']['field'] == 'amount'
        assert fi['basic_filter']['filter_type'] == '>'

    def test_advanced_filter(self, temp_dir):
        """Advanced filter with expression."""
        node = NodeFilter(
            flow_id=1, node_id=1,
            filter_input=FilterInput(
                filter_type='advanced',
                advanced_filter='[amount] > 100 and [status] == "active"'
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('filter', node))

        fi = data['nodes'][0]['setting_input']['filter_input']
        assert fi['filter_type'] == 'advanced'
        assert '[amount] > 100' in fi['advanced_filter']


class TestFormulaNodeMigration:
    """Test NodeFormula migrations."""

    def test_formula_with_expression(self, temp_dir):
        """Formula creating new column."""
        node = NodeFormula(
            flow_id=1, node_id=1,
            function=FunctionInput(
                field=FieldInput(name='total', data_type='Float64'),
                function='[price] * [quantity]'
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('formula', node))

        func = data['nodes'][0]['setting_input']['function']
        assert func['field']['name'] == 'total'
        assert '[price] * [quantity]' in func['function']


class TestJoinNodeMigration:
    """Test NodeJoin migrations."""

    def test_inner_join(self, temp_dir):
        """Inner join with single key."""
        node = NodeJoin(
            flow_id=1, node_id=1,
            join_input=JoinInput(
                join_mapping=[JoinMap(left_col='id', right_col='id')],
                how='inner',
                left_select=JoinInputs(renames=[SelectInput(old_name='id')]),
                right_select=JoinInputs(renames=[SelectInput(old_name='value')]),
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('join', node))

        ji = data['nodes'][0]['setting_input']['join_input']
        assert ji['how'] == 'inner'
        assert ji['join_mapping'][0]['left_col'] == 'id'
        assert ji['join_mapping'][0]['right_col'] == 'id'

    def test_left_join_multi_key(self, temp_dir):
        """Left join with multiple keys."""
        node = NodeJoin(
            flow_id=1, node_id=1,
            join_input=JoinInput(
                join_mapping=[
                    JoinMap(left_col='date', right_col='date'),
                    JoinMap(left_col='product_id', right_col='prod_id'),
                ],
                how='left',
                left_select=JoinInputs(renames=[]),
                right_select=JoinInputs(renames=[]),
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('join', node))

        ji = data['nodes'][0]['setting_input']['join_input']
        assert ji['how'] == 'left'
        assert len(ji['join_mapping']) == 2
        assert ji['join_mapping'][1]['left_col'] == 'product_id'
        assert ji['join_mapping'][1]['right_col'] == 'prod_id'

    def test_join_with_none_selects(self, temp_dir):
        """Join with None left_select/right_select (old format) gets default empty renames."""
        node = NodeJoin(
            flow_id=1, node_id=1,
            join_input=JoinInput(
                join_mapping=[JoinMap(left_col='id', right_col='id')],
                how='inner',
                left_select=None,
                right_select=None,
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('join', node))

        ji = data['nodes'][0]['setting_input']['join_input']
        assert ji['left_select'] == {'renames': []}
        assert ji['right_select'] == {'renames': []}


class TestCrossJoinNodeMigration:
    """Test NodeCrossJoin migrations."""

    def test_cross_join(self, temp_dir):
        """Cross join with selects."""
        node = NodeCrossJoin(
            flow_id=1, node_id=1,
            cross_join_input=CrossJoinInput(
                left_select=JoinInputs(renames=[SelectInput(old_name='a')]),
                right_select=JoinInputs(renames=[SelectInput(old_name='b')]),
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('cross_join', node))

        cji = data['nodes'][0]['setting_input']['cross_join_input']
        assert 'left_select' in cji
        assert 'right_select' in cji


class TestFuzzyMatchNodeMigration:
    """Test NodeFuzzyMatch migrations."""

    def test_fuzzy_match(self, temp_dir):
        """Fuzzy match with threshold."""
        node = NodeFuzzyMatch(
            flow_id=1, node_id=1,
            join_input=FuzzyMatchInput(
                join_mapping=[
                    FuzzyMapping(
                        left_col='name',
                        right_col='company_name',
                        threshold_score=80,
                        fuzzy_type='levenshtein'
                    )
                ],
                how='inner',
                left_select=JoinInputs(renames=[]),
                right_select=JoinInputs(renames=[]),
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('fuzzy_match', node))

        ji = data['nodes'][0]['setting_input']['join_input']
        assert ji['join_mapping'][0]['threshold_score'] == 80
        assert ji['join_mapping'][0]['fuzzy_type'] == 'levenshtein'


class TestGroupByNodeMigration:
    """Test NodeGroupBy migrations."""

    def test_groupby_with_aggregations(self, temp_dir):
        """Group by with multiple aggregations."""
        node = NodeGroupBy(
            flow_id=1, node_id=1,
            groupby_input=GroupByInput(
                agg_cols=[
                    AggColl(old_name='category', agg='groupby'),
                    AggColl(old_name='amount', agg='sum', new_name='total_amount'),
                    AggColl(old_name='amount', agg='mean', new_name='avg_amount'),
                    AggColl(old_name='id', agg='count', new_name='record_count'),
                ]
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('group_by', node))

        agg_cols = data['nodes'][0]['setting_input']['groupby_input']['agg_cols']
        assert len(agg_cols) == 4
        assert agg_cols[0]['agg'] == 'groupby'
        assert agg_cols[1]['new_name'] == 'total_amount'


class TestSortNodeMigration:
    """Test NodeSort migrations."""

    def test_multi_column_sort(self, temp_dir):
        """Sort by multiple columns."""
        node = NodeSort(
            flow_id=1, node_id=1,
            sort_input=[
                SortByInput(column='date', how='desc'),
                SortByInput(column='name', how='asc'),
            ]
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('sort', node))

        sort_input = data['nodes'][0]['setting_input']['sort_input']
        assert len(sort_input) == 2
        assert sort_input[0]['column'] == 'date'
        assert sort_input[0]['how'] == 'desc'


class TestUnionNodeMigration:
    """Test NodeUnion migrations."""

    def test_union_relaxed(self, temp_dir):
        """Union with relaxed mode."""
        node = NodeUnion(
            flow_id=1, node_id=1,
            union_input=UnionInput(mode='relaxed')
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('union', node))

        assert data['nodes'][0]['setting_input']['union_input']['mode'] == 'relaxed'

    def test_union_selective(self, temp_dir):
        """Union with selective mode."""
        node = NodeUnion(
            flow_id=1, node_id=1,
            union_input=UnionInput(mode='selective')
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('union', node))

        assert data['nodes'][0]['setting_input']['union_input']['mode'] == 'selective'


class TestUniqueNodeMigration:
    """Test NodeUnique migrations."""

    def test_unique_first_strategy(self, temp_dir):
        """Unique with first strategy."""
        node = NodeUnique(
            flow_id=1, node_id=1,
            unique_input=UniqueInput(
                columns=['id', 'date'],
                strategy='first'
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('unique', node))

        ui = data['nodes'][0]['setting_input']['unique_input']
        assert ui['columns'] == ['id', 'date']
        assert ui['strategy'] == 'first'


class TestPivotNodeMigration:
    """Test NodePivot migrations."""

    def test_pivot(self, temp_dir):
        """Pivot with aggregations."""
        node = NodePivot(
            flow_id=1, node_id=1,
            pivot_input=PivotInput(
                index_columns=['date'],
                pivot_column='category',
                value_col='amount',
                aggregations=['sum', 'mean']
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('pivot', node))

        pi = data['nodes'][0]['setting_input']['pivot_input']
        assert pi['index_columns'] == ['date']
        assert pi['pivot_column'] == 'category'
        assert 'sum' in pi['aggregations']


class TestUnpivotNodeMigration:
    """Test NodeUnpivot migrations."""

    def test_unpivot(self, temp_dir):
        """Unpivot with column selection."""
        node = NodeUnpivot(
            flow_id=1, node_id=1,
            unpivot_input=UnpivotInput(
                index_columns=['id', 'date'],
                value_columns=['jan', 'feb', 'mar'],
                data_type_selector_mode='column'
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('unpivot', node))

        ui = data['nodes'][0]['setting_input']['unpivot_input']
        assert ui['index_columns'] == ['id', 'date']
        assert ui['value_columns'] == ['jan', 'feb', 'mar']


class TestRecordIdNodeMigration:
    """Test NodeRecordId migrations."""

    def test_record_id(self, temp_dir):
        """Record ID with offset."""
        node = NodeRecordId(
            flow_id=1, node_id=1,
            record_id_input=RecordIdInput(
                output_column_name='row_number',
                offset=1,
                group_by=True,
                group_by_columns=['category']
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('record_id', node))

        ri = data['nodes'][0]['setting_input']['record_id_input']
        assert ri['output_column_name'] == 'row_number'
        assert ri['offset'] == 1
        assert ri['group_by'] == True
        assert ri['group_by_columns'] == ['category']


class TestTextToRowsNodeMigration:
    """Test NodeTextToRows migrations."""

    def test_text_to_rows(self, temp_dir):
        """Text to rows with delimiter."""
        node = NodeTextToRows(
            flow_id=1, node_id=1,
            text_to_rows_input=TextToRowsInput(
                column_to_split='tags',
                output_column_name='tag',
                split_by_fixed_value=True,
                split_fixed_value=','
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('text_to_rows', node))

        ti = data['nodes'][0]['setting_input']['text_to_rows_input']
        assert ti['column_to_split'] == 'tags'
        assert ti['split_fixed_value'] == ','


class TestGraphSolverNodeMigration:
    """Test NodeGraphSolver migrations."""

    def test_graph_solver(self, temp_dir):
        """Graph solver for connected components."""
        node = NodeGraphSolver(
            flow_id=1, node_id=1,
            graph_solver_input=GraphSolverInput(
                col_from='source_id',
                col_to='target_id',
                output_column_name='component_id'
            )
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('graph_solver', node))

        gi = data['nodes'][0]['setting_input']['graph_solver_input']
        assert gi['col_from'] == 'source_id'
        assert gi['col_to'] == 'target_id'
        assert gi['output_column_name'] == 'component_id'


class TestPolarsCodeNodeMigration:
    """Test NodePolarsCode migrations."""

    def test_polars_code(self, temp_dir):
        """Polars code with multi-line script."""
        code = '''# Transform data
output_df = input_df.with_columns([
    pl.col("amount") * 2,
    pl.col("name").str.to_uppercase()
])
'''
        node = NodePolarsCode(
            flow_id=1, node_id=1,
            polars_code_input=PolarsCodeInput(polars_code=code),
            depending_on_ids=[0]
        )

        data = pickle_and_migrate(temp_dir, create_flow_with_node('polars_code', node))

        pci = data['nodes'][0]['setting_input']['polars_code_input']
        assert 'output_df' in pci['polars_code']
        assert 'to_uppercase' in pci['polars_code']


# =============================================================================
# COMPLEX FLOW TESTS
# =============================================================================

class TestComplexFlowMigration:
    """Test migration of flows with multiple connected nodes."""

    def test_pipeline_flow(self, temp_dir):
        """Test a complete pipeline: read -> select -> filter -> output."""
        flow = FlowInformation(
            flow_id=1,
            flow_name='pipeline_flow',
            flow_settings=FlowSettings(
                flow_id=1,
                name='pipeline_flow',
                description='Test pipeline'
            ),
            data={
                1: NodeInformation(
                    id=1, type='read',
                    setting_input=NodeRead(
                        flow_id=1, node_id=1,
                        received_file=ReceivedTable(
                            name='input.csv', path='/data/input.csv',
                            file_type='csv', delimiter=','
                        )
                    )
                ),
                2: NodeInformation(
                    id=2, type='select',
                    setting_input=NodeSelect(
                        flow_id=1, node_id=2,
                        select_input=[SelectInput(old_name='a', new_name='col_a')]
                    )
                ),
                3: NodeInformation(
                    id=3, type='filter',
                    setting_input=NodeFilter(
                        flow_id=1, node_id=3,
                        filter_input=FilterInput(
                            filter_type='advanced',
                            advanced_filter='[col_a] > 0'
                        )
                    )
                ),
                4: NodeInformation(
                    id=4, type='output',
                    setting_input=NodeOutput(
                        flow_id=1, node_id=4,
                        output_settings=OutputSettings(
                            name='output.csv', directory='/out',
                            file_type='csv',
                            output_csv_table=OutputCsvTable(delimiter=';')
                        )
                    )
                ),
            },
            node_starts=[1],
            node_connections=[(1, 2), (2, 3), (3, 4)],
        )

        data = pickle_and_migrate(temp_dir, flow)

        # Verify structure (FlowfileData format)
        assert data['flowfile_name'] == 'pipeline_flow'
        assert len(data['nodes']) == 4

        # Verify each node migrated correctly
        read_node = next(n for n in data['nodes'] if n['type'] == 'read')
        assert 'table_settings' in read_node['setting_input']['received_file']

        output_node = next(n for n in data['nodes'] if n['type'] == 'output')
        assert 'table_settings' in output_node['setting_input']['output_settings']
        assert output_node['setting_input']['output_settings']['table_settings']['delimiter'] == ';'

    def test_join_flow(self, temp_dir):
        """Test flow with join: two inputs -> join -> output."""
        flow = FlowInformation(
            flow_id=1,
            flow_name='join_flow',
            flow_settings=FlowSettings(flow_id=1, name='join_flow'),
            data={
                1: NodeInformation(
                    id=1, type='read',
                    setting_input=NodeRead(
                        flow_id=1, node_id=1,
                        received_file=ReceivedTable(
                            name='left.csv', path='/data/left.csv', file_type='csv'
                        )
                    )
                ),
                2: NodeInformation(
                    id=2, type='read',
                    setting_input=NodeRead(
                        flow_id=1, node_id=2,
                        received_file=ReceivedTable(
                            name='right.csv', path='/data/right.csv', file_type='csv'
                        )
                    )
                ),
                3: NodeInformation(
                    id=3, type='join',
                    left_input_id=1, right_input_id=2,
                    setting_input=NodeJoin(
                        flow_id=1, node_id=3,
                        join_input=JoinInput(
                            join_mapping=[JoinMap(left_col='id', right_col='id')],
                            how='left',
                            left_select=None,
                            right_select=None,
                        )
                    )
                ),
            },
            node_starts=[1, 2],
            node_connections=[(1, 3), (2, 3)],
        )

        data = pickle_and_migrate(temp_dir, flow)

        assert len(data['nodes']) == 3

        # Verify start nodes are marked
        start_nodes = [n for n in data['nodes'] if n.get('is_start_node')]
        assert len(start_nodes) == 2

        join_node = next(n for n in data['nodes'] if n['type'] == 'join')
        assert join_node['setting_input']['join_input']['how'] == 'left'


# =============================================================================
# YAML OUTPUT TESTS
# =============================================================================

class TestYamlMigration:
    """Test YAML format output."""

    def test_yaml_format(self, temp_dir):
        """Verify YAML output is valid and readable."""
        yaml = pytest.importorskip('yaml')

        node = NodeSelect(
            flow_id=1, node_id=1,
            select_input=[SelectInput(old_name='test')]
        )

        flow = create_flow_with_node('select', node)

        pickle_path = temp_dir / 'test.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(flow, f)

        output_path = migrate_flowfile(pickle_path, format='yaml')

        with open(output_path) as f:
            data = yaml.safe_load(f)

        # Verify FlowfileData format
        assert data['flowfile_version'] == '2.0'
        assert data['flowfile_id'] == 1
        assert len(data['nodes']) == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])