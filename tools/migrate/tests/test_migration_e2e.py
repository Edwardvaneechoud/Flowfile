"""
End-to-end tests for migration tool.

These tests verify that migrated flows can actually be loaded and executed
by the flowfile system - not just that the YAML/JSON structure is correct.

Run with:
    pytest tools/migrate/tests/test_migration_e2e.py -v
"""

import pickle
import tempfile
from pathlib import Path
from typing import List, Dict, Literal

import pytest

from tools.migrate.legacy_schemas import (
    # Flow schemas
    FlowInformation, FlowSettings, NodeInformation,

    # Node schemas
    NodeSelect, NodeFilter, NodeFormula, NodeOutput,
    NodeJoin, NodeGroupBy, NodeSort, NodeUnique,
    NodePivot, NodeUnpivot, NodeRecordId, NodeTextToRows,
    NodeGraphSolver, NodeUnion,

    # Input/Output schemas
    OutputSettings, OutputCsvTable,

    # Transform schemas
    SelectInput, SelectInputs, JoinInputs, JoinInput, JoinMap,
    FilterInput, BasicFilter, FunctionInput, FieldInput,
    GroupByInput, AggColl, SortByInput, UniqueInput,
    PivotInput, UnpivotInput, RecordIdInput, TextToRowsInput,
    GraphSolverInput, UnionInput,
)

from tools.migrate.migrate import migrate_flowfile

# Import actual flowfile system for verification
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, transform_schema, schemas
from flowfile_core.schemas.output_model import RunInformation
from flowfile_core.flowfile.manage.io_flowfile import open_flow


# =============================================================================
# HELPERS
# =============================================================================

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


def add_manual_input(graph: FlowGraph, data: List[Dict], node_id: int = 1):
    """Add a manual input node with data."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type='manual_input'
    )
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
    """Check run info for errors and raise if failed."""
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
def sample_data() -> List[Dict]:
    """Basic sample data for testing."""
    return [
        {'name': 'Alice', 'age': 30, 'city': 'NYC', 'sales': 100},
        {'name': 'Bob', 'age': 25, 'city': 'LA', 'sales': 150},
        {'name': 'Charlie', 'age': 35, 'city': 'NYC', 'sales': 200},
        {'name': 'Diana', 'age': 28, 'city': 'Chicago', 'sales': 120},
    ]


# =============================================================================
# BASELINE TEST - Verify flowfile system works without migration
# =============================================================================

class TestFlowfileBaseline:
    """Verify the flowfile system works before testing migration."""

    def test_manual_input_runs(self, sample_data):
        """Basic test that manual input works."""
        graph = create_graph(flow_id=1)
        add_manual_input(graph, sample_data, node_id=1)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        node = graph.get_node(1)
        assert node is not None

    def test_select_node_runs(self, sample_data):
        """Test select node works."""
        graph = create_graph(flow_id=2)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        select_input = [
            transform_schema.SelectInput(old_name='name', new_name='full_name', keep=True),
            transform_schema.SelectInput(old_name='age', keep=True),
        ]
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=select_input,
        )
        graph.add_select(node_select)

        run_info = graph.run_graph()
        handle_run_info(run_info)

    def test_filter_node_runs(self, sample_data):
        """Test filter node works."""
        graph = create_graph(flow_id=3)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'filter', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_input = transform_schema.FilterInput(
            filter_type='advanced',
            advanced_filter='[age] > 25'
        )
        node_filter = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=filter_input,
        )
        graph.add_filter(node_filter)

        run_info = graph.run_graph()
        handle_run_info(run_info)


# =============================================================================
# YAML ROUND-TRIP TESTS - Save and reload flows (using NEW API)
# =============================================================================

class TestYamlRoundTrip:
    """Test that flows survive YAML save/load cycle."""

    def test_select_roundtrip(self, temp_dir, sample_data):
        """Save and reload a flow with select node."""
        graph = create_graph(flow_id=100)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'select', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        select_input = [
            transform_schema.SelectInput(old_name='name', new_name='person_name'),
            transform_schema.SelectInput(old_name='sales', keep=True),
        ]
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=select_input,
        )
        graph.add_select(node_select)

        # Save as YAML
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))

        # Reload and run
        loaded_flow = open_flow(path)
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

        # Verify node preserved
        loaded_select = loaded_flow.get_node(2)
        assert loaded_select is not None
        assert loaded_select.setting_input.select_input[0].new_name == 'person_name'

    def test_filter_roundtrip(self, temp_dir, sample_data):
        """Save and reload a flow with filter node."""
        graph = create_graph(flow_id=101)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'filter', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_input = transform_schema.FilterInput(
            filter_type='basic',
            basic_filter=transform_schema.BasicFilter(
                field='age',
                filter_type='>',
                filter_value='25'
            )
        )
        node_filter = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=filter_input,
        )
        graph.add_filter(node_filter)

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_groupby_roundtrip(self, temp_dir, sample_data):
        """Save and reload a flow with groupby node."""
        graph = create_graph(flow_id=102)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'group_by', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        groupby_input = transform_schema.GroupByInput([
            transform_schema.AggColl('city', 'groupby'),
            transform_schema.AggColl('sales', 'sum', 'total_sales'),
            transform_schema.AggColl('age', 'mean', 'avg_age'),
        ])
        node_groupby = input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            groupby_input=groupby_input,
        )
        graph.add_group_by(node_groupby)

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

        # Verify aggregations preserved
        loaded_gb = loaded_flow.get_node(2)
        agg_cols = loaded_gb.setting_input.groupby_input.agg_cols
        assert len(agg_cols) == 3
        assert agg_cols[1].new_name == 'total_sales'

    def test_join_roundtrip(self, temp_dir):
        """Save and reload a flow with join node (using NEW API with required selects)."""
        graph = create_graph(flow_id=103)

        # Left table
        left_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
        ]
        add_manual_input(graph, left_data, node_id=1)

        # Right table
        right_data = [
            {'id': 1, 'dept': 'Sales'},
            {'id': 2, 'dept': 'Engineering'},
        ]
        add_node_promise(graph, 'manual_input', node_id=2)
        input_file = input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist(right_data)
        )
        graph.add_manual_input(input_file)

        # Join node - NEW API requires left_select and right_select
        add_node_promise(graph, 'join', node_id=3)
        left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
        right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3, input_type='right')
        add_connection(graph, left_conn)
        add_connection(graph, right_conn)

        join_input = transform_schema.JoinInput(
            join_mapping=[transform_schema.JoinMap(left_col='id', right_col='id')],
            how='inner',
            left_select=transform_schema.JoinInputs(renames=[]),   # Required in new API
            right_select=transform_schema.JoinInputs(renames=[]),  # Required in new API
        )
        node_join = input_schema.NodeJoin(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            join_input=join_input,
        )
        graph.add_join(node_join)

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_formula_roundtrip(self, temp_dir, sample_data):
        """Save and reload a flow with formula node."""
        graph = create_graph(flow_id=104)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'formula', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        node_formula = input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name='double_sales'),
                function='[sales] * 2'
            )
        )
        graph.add_formula(node_formula)

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_sort_roundtrip(self, temp_dir, sample_data):
        """Save and reload a flow with sort node."""
        graph = create_graph(flow_id=105)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'sort', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        node_sort = input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[
                transform_schema.SortByInput(column='age', how='desc'),
                transform_schema.SortByInput(column='name', how='asc'),
            ]
        )
        graph.add_sort(node_sort)

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_unique_roundtrip(self, temp_dir, sample_data):
        """Save and reload a flow with unique node."""
        graph = create_graph(flow_id=106)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'unique', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        node_unique = input_schema.NodeUnique(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            unique_input=transform_schema.UniqueInput(
                columns=['city'],
                strategy='first'
            )
        )
        graph.add_unique(node_unique)

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_record_id_roundtrip(self, temp_dir, sample_data):
        """Save and reload a flow with record_id node."""
        graph = create_graph(flow_id=107)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'record_id', node_id=2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        node_record_id = input_schema.NodeRecordId(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            record_id_input=transform_schema.RecordIdInput(
                output_column_name='row_num',
                offset=1
            )
        )
        graph.add_record_id(node_record_id)

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)


# =============================================================================
# LEGACY MIGRATION TESTS - Test OLD pickle format → migrate → run
# =============================================================================

class TestLegacyMigration:
    """Test migration of OLD pickle format to new YAML format.

    These tests:
    1. Create flows using LEGACY schemas (simulating old .flowfile pickles)
    2. Pickle them
    3. Run migrate_flowfile()
    4. Load with open_flow()
    5. Add runtime data and verify execution
    """

    def test_join_migration_with_none_selects(self, temp_dir):
        """Migrate old pickle with join node where left_select/right_select are None."""
        # 1. Build legacy flow with OLD JoinInput (left_select=None, right_select=None)
        legacy_join_input = JoinInput(
            join_mapping=[JoinMap(left_col='id', right_col='id')],
            how='inner',
            left_select=None,   # OLD: was allowed to be None
            right_select=None,  # OLD: was allowed to be None
        )

        legacy_flow = FlowInformation(
            flow_id=103,
            flow_name='join_migration_test',
            flow_settings=FlowSettings(
                flow_id=103,
                name='join_migration_test',
                path='.',
                execution_mode='Development',
            ),
            data={
                1: NodeInformation(
                    id=1,
                    type='manual_input',
                    is_setup=True,
                    setting_input=None,
                ),
                2: NodeInformation(
                    id=2,
                    type='manual_input',
                    is_setup=True,
                    setting_input=None,
                ),
                3: NodeInformation(
                    id=3,
                    type='join',
                    is_setup=True,
                    left_input_id=1,
                    right_input_id=2,
                    setting_input=NodeJoin(
                        flow_id=103,
                        node_id=3,
                        depending_on_ids=[1, 2],
                        join_input=legacy_join_input,
                    ),
                ),
            },
            node_connections=[(1, 3), (2, 3)],
            node_starts=[1, 2],
        )

        # 2. Pickle it (simulating old .flowfile)
        pickle_path = temp_dir / 'old_join.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(legacy_flow, f)

        # 3. Migrate to YAML
        yaml_path = temp_dir / 'migrated.yaml'
        migrate_flowfile(pickle_path, yaml_path, 'yaml')

        # 4. Load with current system
        loaded_flow = open_flow(yaml_path)

        # 5. Add manual input data (not stored in pickle, added at runtime)
        left_data = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        right_data = [{'id': 1, 'dept': 'Sales'}, {'id': 2, 'dept': 'Engineering'}]

        loaded_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=103, node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(left_data)
        ))
        loaded_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=103, node_id=2,
            raw_data_format=input_schema.RawData.from_pylist(right_data)
        ))

        # 6. Run and verify
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_filter_migration(self, temp_dir):
        """Migrate old pickle with filter node."""
        legacy_filter = FilterInput(
            filter_type='advanced',
            advanced_filter='[age] > 25',
            basic_filter=None,
        )

        legacy_flow = FlowInformation(
            flow_id=201,
            flow_name='filter_migration_test',
            flow_settings=FlowSettings(
                flow_id=201,
                name='filter_migration_test',
                path='.',
                execution_mode='Development',
            ),
            data={
                1: NodeInformation(
                    id=1,
                    type='manual_input',
                    is_setup=True,
                    setting_input=None,
                ),
                2: NodeInformation(
                    id=2,
                    type='filter',
                    is_setup=True,
                    setting_input=NodeFilter(
                        flow_id=201,
                        node_id=2,
                        depending_on_id=1,
                        filter_input=legacy_filter,
                    ),
                ),
            },
            node_connections=[(1, 2)],
            node_starts=[1],
        )

        # Pickle
        pickle_path = temp_dir / 'old_filter.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(legacy_flow, f)

        # Migrate
        yaml_path = temp_dir / 'migrated_filter.yaml'
        migrate_flowfile(pickle_path, yaml_path, 'yaml')

        # Load and add data
        loaded_flow = open_flow(yaml_path)
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 20},
            {'name': 'Charlie', 'age': 35},
        ]
        loaded_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=201, node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(data)
        ))

        # Run and verify
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_groupby_migration(self, temp_dir):
        """Migrate old pickle with groupby node."""
        legacy_groupby = GroupByInput(
            agg_cols=[
                AggColl(old_name='city', agg='groupby'),
                AggColl(old_name='sales', agg='sum', new_name='total_sales'),
            ]
        )

        legacy_flow = FlowInformation(
            flow_id=202,
            flow_name='groupby_migration_test',
            flow_settings=FlowSettings(
                flow_id=202,
                name='groupby_migration_test',
                path='.',
                execution_mode='Development',
            ),
            data={
                1: NodeInformation(
                    id=1,
                    type='manual_input',
                    is_setup=True,
                    setting_input=None,
                ),
                2: NodeInformation(
                    id=2,
                    type='group_by',
                    is_setup=True,
                    setting_input=NodeGroupBy(
                        flow_id=202,
                        node_id=2,
                        depending_on_id=1,
                        groupby_input=legacy_groupby,
                    ),
                ),
            },
            node_connections=[(1, 2)],
            node_starts=[1],
        )

        # Pickle
        pickle_path = temp_dir / 'old_groupby.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(legacy_flow, f)

        # Migrate
        yaml_path = temp_dir / 'migrated_groupby.yaml'
        migrate_flowfile(pickle_path, yaml_path, 'yaml')

        # Load and add data
        loaded_flow = open_flow(yaml_path)
        data = [
            {'city': 'NYC', 'sales': 100},
            {'city': 'NYC', 'sales': 150},
            {'city': 'LA', 'sales': 200},
        ]
        loaded_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=202, node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(data)
        ))

        # Run and verify
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_select_migration(self, temp_dir):
        """Migrate old pickle with select node."""
        legacy_select_input = [
            SelectInput(old_name='name', new_name='person_name', keep=True),
            SelectInput(old_name='age', keep=True),
        ]

        legacy_flow = FlowInformation(
            flow_id=203,
            flow_name='select_migration_test',
            flow_settings=FlowSettings(
                flow_id=203,
                name='select_migration_test',
                path='.',
                execution_mode='Development',
            ),
            data={
                1: NodeInformation(
                    id=1,
                    type='manual_input',
                    is_setup=True,
                    setting_input=None,
                ),
                2: NodeInformation(
                    id=2,
                    type='select',
                    is_setup=True,
                    setting_input=NodeSelect(
                        flow_id=203,
                        node_id=2,
                        depending_on_id=1,
                        select_input=legacy_select_input,
                    ),
                ),
            },
            node_connections=[(1, 2)],
            node_starts=[1],
        )

        # Pickle
        pickle_path = temp_dir / 'old_select.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(legacy_flow, f)

        # Migrate
        yaml_path = temp_dir / 'migrated_select.yaml'
        migrate_flowfile(pickle_path, yaml_path, 'yaml')

        # Load and add data
        loaded_flow = open_flow(yaml_path)
        data = [
            {'name': 'Alice', 'age': 30, 'city': 'NYC'},
            {'name': 'Bob', 'age': 25, 'city': 'LA'},
        ]
        loaded_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=203, node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(data)
        ))

        # Run and verify
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_formula_migration(self, temp_dir):
        """Migrate old pickle with formula node."""
        legacy_function = FunctionInput(
            field=FieldInput(name='double_sales'),
            function='[sales] * 2'
        )

        legacy_flow = FlowInformation(
            flow_id=204,
            flow_name='formula_migration_test',
            flow_settings=FlowSettings(
                flow_id=204,
                name='formula_migration_test',
                path='.',
                execution_mode='Development',
            ),
            data={
                1: NodeInformation(
                    id=1,
                    type='manual_input',
                    is_setup=True,
                    setting_input=None,
                ),
                2: NodeInformation(
                    id=2,
                    type='formula',
                    is_setup=True,
                    setting_input=NodeFormula(
                        flow_id=204,
                        node_id=2,
                        depending_on_id=1,
                        function=legacy_function,
                    ),
                ),
            },
            node_connections=[(1, 2)],
            node_starts=[1],
        )

        # Pickle
        pickle_path = temp_dir / 'old_formula.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(legacy_flow, f)

        # Migrate
        yaml_path = temp_dir / 'migrated_formula.yaml'
        migrate_flowfile(pickle_path, yaml_path, 'yaml')

        # Load and add data
        loaded_flow = open_flow(yaml_path)
        data = [
            {'name': 'Alice', 'sales': 100},
            {'name': 'Bob', 'sales': 150},
        ]
        loaded_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=204, node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(data)
        ))

        # Run and verify
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

    def test_sort_migration(self, temp_dir):
        """Migrate old pickle with sort node."""
        legacy_sort = [
            SortByInput(column='age', how='desc'),
            SortByInput(column='name', how='asc'),
        ]

        legacy_flow = FlowInformation(
            flow_id=205,
            flow_name='sort_migration_test',
            flow_settings=FlowSettings(
                flow_id=205,
                name='sort_migration_test',
                path='.',
                execution_mode='Development',
            ),
            data={
                1: NodeInformation(
                    id=1,
                    type='manual_input',
                    is_setup=True,
                    setting_input=None,
                ),
                2: NodeInformation(
                    id=2,
                    type='sort',
                    is_setup=True,
                    setting_input=NodeSort(
                        flow_id=205,
                        node_id=2,
                        depending_on_id=1,
                        sort_input=legacy_sort,
                    ),
                ),
            },
            node_connections=[(1, 2)],
            node_starts=[1],
        )

        # Pickle
        pickle_path = temp_dir / 'old_sort.flowfile'
        with open(pickle_path, 'wb') as f:
            pickle.dump(legacy_flow, f)

        # Migrate
        yaml_path = temp_dir / 'migrated_sort.yaml'
        migrate_flowfile(pickle_path, yaml_path, 'yaml')

        # Load and add data
        loaded_flow = open_flow(yaml_path)
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35},
        ]
        loaded_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=205, node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(data)
        ))

        # Run and verify
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)


# =============================================================================
# COMPLEX PIPELINE TESTS
# =============================================================================

class TestComplexPipelines:
    """Test complex multi-node pipelines."""

    def test_etl_pipeline(self, temp_dir):
        """Test a typical ETL pipeline: input -> filter -> formula -> groupby."""
        graph = create_graph(flow_id=200)

        data = [
            {'region': 'East', 'product': 'A', 'sales': 100, 'active': True},
            {'region': 'East', 'product': 'B', 'sales': 150, 'active': True},
            {'region': 'West', 'product': 'A', 'sales': 200, 'active': False},
            {'region': 'West', 'product': 'B', 'sales': 120, 'active': True},
            {'region': 'East', 'product': 'A', 'sales': 80, 'active': True},
        ]
        add_manual_input(graph, data, node_id=1)

        # Filter: only active
        add_node_promise(graph, 'filter', node_id=2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        graph.add_filter(input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type='advanced',
                advanced_filter='[active] == true'
            )
        ))

        # Formula: double sales
        add_node_promise(graph, 'formula', node_id=3)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
        graph.add_formula(input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name='adjusted_sales'),
                function='[sales] * 1.1'
            )
        ))

        # GroupBy: sum by region
        add_node_promise(graph, 'group_by', node_id=4)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(3, 4))
        graph.add_group_by(input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=4,
            depending_on_id=3,
            groupby_input=transform_schema.GroupByInput([
                transform_schema.AggColl('region', 'groupby'),
                transform_schema.AggColl('adjusted_sales', 'sum', 'total_adjusted'),
            ])
        ))

        # Run original
        run_info = graph.run_graph()
        handle_run_info(run_info)

        # Save and reload
        path = temp_dir / 'etl_pipeline.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        # Run reloaded
        run_info = loaded_flow.run_graph()
        handle_run_info(run_info)

        # Verify all nodes present
        assert loaded_flow.get_node(1) is not None
        assert loaded_flow.get_node(2) is not None
        assert loaded_flow.get_node(3) is not None
        assert loaded_flow.get_node(4) is not None


# =============================================================================
# OUTPUT NODE TESTS
# =============================================================================

class TestOutputNode:
    """Test output node with different file types."""

    def test_csv_output_roundtrip(self, temp_dir, sample_data):
        """Test CSV output node roundtrip."""
        graph = create_graph(flow_id=300)
        add_manual_input(graph, sample_data, node_id=1)

        add_node_promise(graph, 'output', node_id=2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        output_dir = temp_dir / 'output'
        output_dir.mkdir()

        graph.add_output(input_schema.NodeOutput(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            output_settings=input_schema.OutputSettings(
                name='result.csv',
                directory=str(output_dir),
                file_type='csv',
                write_mode='overwrite',
                table_settings=input_schema.OutputCsvTable(
                    delimiter=';',
                    encoding='utf-8'
                )
            )
        ))

        # Save and reload
        path = temp_dir / 'test.yaml'
        graph.save_flow(str(path))
        loaded_flow = open_flow(path)

        # Verify output settings preserved
        output_node = loaded_flow.get_node(2)
        assert output_node.setting_input.output_settings.table_settings.delimiter == ';'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
