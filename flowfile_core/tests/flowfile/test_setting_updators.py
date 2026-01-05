"""Tests for setting generator and updator functionality.

These tests verify that:
1. Setting generators correctly initialize settings from input schemas
2. Setting updators properly update settings when input columns change
3. Columns are correctly added/removed when inputs change
4. No duplicate columns appear when both sides have overlapping column names
"""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema


@pytest.fixture
def flow_handler() -> FlowfileHandler:
    """Create a fresh FlowfileHandler for each test."""
    return FlowfileHandler()


@pytest.fixture
def basic_flow(flow_handler: FlowfileHandler) -> FlowGraph:
    """Create a basic flow with id 1."""
    flow_handler.register_flow(schemas.FlowSettings(
        flow_id=1,
        name='test_flow',
        path='.',
        execution_mode='Development'
    ))
    return flow_handler.get_flow(1)


def create_manual_input_node(graph: FlowGraph, node_id: int, data: list[dict]) -> None:
    """Helper to create a manual input node."""
    graph.add_node_promise(input_schema.NodePromise(
        flow_id=graph.flow_id, node_id=node_id, node_type='manual_input'
    ))
    graph.add_manual_input(input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data)
    ))


class TestCrossJoinSettingGenerator:
    """Tests for cross join setting generator."""

    def test_cross_join_generator_creates_settings(self, basic_flow: FlowGraph):
        """Test that cross join generator creates initial settings from inputs."""
        # Create left input with columns: id, name
        create_manual_input_node(basic_flow, 1, [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
        ])

        # Create right input with columns: code, value
        create_manual_input_node(basic_flow, 2, [
            {'code': 'A', 'value': 100},
            {'code': 'B', 'value': 200},
        ])

        # Create cross join node
        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        # Get the node and verify settings were generated
        node = basic_flow.get_node(3)
        node_data = node.get_node_data(basic_flow.flow_id)
        assert node_data.setting_input is not None
        assert isinstance(node_data.setting_input, input_schema.NodeCrossJoin)

        cross_join_input = node_data.setting_input.cross_join_input
        left_cols = [r.old_name for r in cross_join_input.left_select.renames]
        right_cols = [r.old_name for r in cross_join_input.right_select.renames]

        assert set(left_cols) == {'id', 'name'}
        assert set(right_cols) == {'code', 'value'}

    def test_cross_join_generator_renames_overlapping_columns(self, basic_flow: FlowGraph):
        """Test that overlapping columns are automatically renamed."""
        # Create left and right inputs with same column name
        create_manual_input_node(basic_flow, 1, [{'col': 1}])
        create_manual_input_node(basic_flow, 2, [{'col': 2}])

        # Create cross join node
        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        node = basic_flow.get_node(3)
        node_data = node.get_node_data(basic_flow.flow_id)

        cross_join_input = node_data.setting_input.cross_join_input
        left_new_names = [r.new_name for r in cross_join_input.left_select.renames]
        right_new_names = [r.new_name for r in cross_join_input.right_select.renames]

        # Left should keep original name, right should be renamed to avoid conflict
        assert 'col' in left_new_names
        assert 'col' not in right_new_names
        assert 'right_col' in right_new_names


class TestCrossJoinSettingUpdator:
    """Tests for cross join setting updator."""

    def test_cross_join_updator_marks_missing_columns_unavailable(self, basic_flow: FlowGraph):
        """Test that columns no longer in input are marked as unavailable (not removed)."""
        # Initial setup with multiple columns
        create_manual_input_node(basic_flow, 1, [{'id': 1, 'name': 'Alice', 'extra': 'X'}])
        create_manual_input_node(basic_flow, 2, [{'code': 'A', 'value': 100}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        # Get initial node data
        node = basic_flow.get_node(3)
        initial_data = node.get_node_data(basic_flow.flow_id)

        # Verify initial left columns include 'extra'
        left_cols = {r.old_name: r for r in initial_data.setting_input.cross_join_input.left_select.renames}
        assert 'extra' in left_cols
        assert left_cols['extra'].is_available is True

        # Update left input to remove 'extra' column
        basic_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{'id': 1, 'name': 'Alice'}])
        ))

        # Get updated node data - this should trigger the updator
        updated_data = node.get_node_data(basic_flow.flow_id)

        # Verify 'extra' column is still there but marked as unavailable
        left_cols_after = {r.old_name: r for r in updated_data.setting_input.cross_join_input.left_select.renames}
        assert 'extra' in left_cols_after
        assert left_cols_after['extra'].is_available is False
        assert 'id' in left_cols_after
        assert left_cols_after['id'].is_available is True
        assert 'name' in left_cols_after
        assert left_cols_after['name'].is_available is True

    def test_cross_join_updator_adds_new_columns(self, basic_flow: FlowGraph):
        """Test that new columns in input are added to settings."""
        # Initial setup
        create_manual_input_node(basic_flow, 1, [{'id': 1}])
        create_manual_input_node(basic_flow, 2, [{'code': 'A'}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        # Get initial data
        node = basic_flow.get_node(3)
        node.get_node_data(basic_flow.flow_id)

        # Update left input to add 'new_col' column
        basic_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{'id': 1, 'new_col': 'value'}])
        ))

        # Get updated node data
        updated_data = node.get_node_data(basic_flow.flow_id)

        left_cols = [r.old_name for r in updated_data.setting_input.cross_join_input.left_select.renames]
        assert 'new_col' in left_cols
        assert 'id' in left_cols

    def test_cross_join_updator_no_duplicates_with_overlapping_columns(self, basic_flow: FlowGraph):
        """Test that no duplicate columns appear when both inputs have same column name."""
        # Create inputs with same column name
        create_manual_input_node(basic_flow, 1, [{'Column 1': 'left_val'}])
        create_manual_input_node(basic_flow, 2, [{'Column 1': 'right_val'}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        # Get node data multiple times to trigger updator
        node = basic_flow.get_node(3)
        node_data = node.get_node_data(basic_flow.flow_id)

        # Verify no duplicates in left select
        left_old_names = [r.old_name for r in node_data.setting_input.cross_join_input.left_select.renames]
        assert len(left_old_names) == len(set(left_old_names)), f"Duplicate columns in left_select: {left_old_names}"

        # Verify no duplicates in right select
        right_old_names = [r.old_name for r in node_data.setting_input.cross_join_input.right_select.renames]
        assert len(right_old_names) == len(set(right_old_names)), f"Duplicate columns in right_select: {right_old_names}"

        # Verify exactly one Column 1 on each side
        assert left_old_names.count('Column 1') == 1
        assert right_old_names.count('Column 1') == 1

    def test_cross_join_updator_repeated_calls_no_duplicates(self, basic_flow: FlowGraph):
        """Test that repeated get_node_data calls don't create duplicates."""
        create_manual_input_node(basic_flow, 1, [{'col': 'left'}])
        create_manual_input_node(basic_flow, 2, [{'col': 'right'}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        node = basic_flow.get_node(3)

        # Call get_node_data multiple times
        for _ in range(5):
            node_data = node.get_node_data(basic_flow.flow_id)

        left_old_names = [r.old_name for r in node_data.setting_input.cross_join_input.left_select.renames]
        right_old_names = [r.old_name for r in node_data.setting_input.cross_join_input.right_select.renames]

        assert len(left_old_names) == 1, f"Expected 1 left column, got {len(left_old_names)}: {left_old_names}"
        assert len(right_old_names) == 1, f"Expected 1 right column, got {len(right_old_names)}: {right_old_names}"


class TestJoinSettingUpdator:
    """Tests for regular join setting updator."""

    def test_join_updator_marks_missing_left_columns_unavailable(self, basic_flow: FlowGraph):
        """Test that columns removed from left input are marked as unavailable."""
        # Initial setup
        create_manual_input_node(basic_flow, 1, [{'id': 1, 'name': 'Alice', 'extra': 'X'}])
        create_manual_input_node(basic_flow, 2, [{'id': 1, 'value': 100}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        node = basic_flow.get_node(3)
        node.get_node_data(basic_flow.flow_id)

        # Update left input to remove 'extra'
        basic_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{'id': 1, 'name': 'Alice'}])
        ))

        updated_data = node.get_node_data(basic_flow.flow_id)

        left_cols = {r.old_name: r for r in updated_data.setting_input.join_input.left_select.renames}
        assert 'extra' in left_cols
        assert left_cols['extra'].is_available is False
        assert 'id' in left_cols
        assert left_cols['id'].is_available is True
        assert 'name' in left_cols
        assert left_cols['name'].is_available is True

    def test_join_updator_marks_missing_right_columns_unavailable(self, basic_flow: FlowGraph):
        """Test that columns removed from right input are marked as unavailable."""
        create_manual_input_node(basic_flow, 1, [{'id': 1, 'name': 'Alice'}])
        create_manual_input_node(basic_flow, 2, [{'id': 1, 'value': 100, 'extra': 'Y'}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        node = basic_flow.get_node(3)
        node.get_node_data(basic_flow.flow_id)

        # Update right input to remove 'extra'
        basic_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=1,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{'id': 1, 'value': 100}])
        ))

        updated_data = node.get_node_data(basic_flow.flow_id)

        right_cols = {r.old_name: r for r in updated_data.setting_input.join_input.right_select.renames}
        assert 'extra' in right_cols
        assert right_cols['extra'].is_available is False
        assert 'id' in right_cols
        assert right_cols['id'].is_available is True
        assert 'value' in right_cols
        assert right_cols['value'].is_available is True

    def test_join_updator_no_duplicates_with_overlapping_columns(self, basic_flow: FlowGraph):
        """Test that no duplicate columns appear when both join inputs have same column name."""
        create_manual_input_node(basic_flow, 1, [{'shared_col': 'left', 'id': 1}])
        create_manual_input_node(basic_flow, 2, [{'shared_col': 'right', 'id': 1}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        node = basic_flow.get_node(3)

        # Call multiple times to ensure updator doesn't create duplicates
        for _ in range(3):
            node_data = node.get_node_data(basic_flow.flow_id)

        left_old_names = [r.old_name for r in node_data.setting_input.join_input.left_select.renames]
        right_old_names = [r.old_name for r in node_data.setting_input.join_input.right_select.renames]

        assert len(left_old_names) == len(set(left_old_names)), f"Duplicates in left: {left_old_names}"
        assert len(right_old_names) == len(set(right_old_names)), f"Duplicates in right: {right_old_names}"


class TestSelectInputsRemoveMethod:
    """Tests for the remove_select_input method on SelectInputs."""

    def test_remove_select_input_removes_column(self):
        """Test that remove_select_input correctly removes a column."""
        select_inputs = transform_schema.SelectInputs(renames=[
            transform_schema.SelectInput(old_name='a'),
            transform_schema.SelectInput(old_name='b'),
            transform_schema.SelectInput(old_name='c'),
        ])

        select_inputs.remove_select_input('b')

        remaining = [r.old_name for r in select_inputs.renames]
        assert remaining == ['a', 'c']

    def test_remove_select_input_nonexistent_column(self):
        """Test that removing a non-existent column doesn't raise error."""
        select_inputs = transform_schema.SelectInputs(renames=[
            transform_schema.SelectInput(old_name='a'),
        ])

        # Should not raise
        select_inputs.remove_select_input('nonexistent')

        remaining = [r.old_name for r in select_inputs.renames]
        assert remaining == ['a']

    def test_join_inputs_inherits_remove_method(self):
        """Test that JoinInputs also has the remove_select_input method."""
        join_inputs = transform_schema.JoinInputs(renames=[
            transform_schema.SelectInput(old_name='x'),
            transform_schema.SelectInput(old_name='y'),
        ])

        join_inputs.remove_select_input('x')

        remaining = [r.old_name for r in join_inputs.renames]
        assert remaining == ['y']


class TestSettingUpdatorWithIsAvailableFalse:
    """Tests for setting updator when existing settings have is_available=False.

    This covers the case where settings are loaded from frontend/storage with
    is_available=False, which previously caused duplicate columns.
    """

    def test_cross_join_updator_no_duplicates_when_is_available_false(self):
        """Test that no duplicates are created when existing columns have is_available=False."""
        from flowfile_core.flowfile.setting_generator.settings import cross_join as cross_join_updator
        from flowfile_core.schemas.output_model import NodeData, TableExample

        # Create a NodeData with existing settings where is_available=False
        existing_cross_join = transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='Column 1', new_name='Column 1', is_available=False),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='Column 1', new_name='right_Column 1', is_available=False),
            ]),
        )

        node_data = NodeData(
            flow_id=1,
            node_id=1,
            flow_type='cross_join',
            setting_input=input_schema.NodeCrossJoin(
                flow_id=1,
                node_id=1,
                cross_join_input=existing_cross_join,
            ),
            main_input=TableExample(node_id=1, number_of_records=1, number_of_columns=1, name='left', table_schema=[], columns=['Column 1']),
            right_input=TableExample(node_id=2, number_of_records=1, number_of_columns=1, name='right', table_schema=[], columns=['Column 1']),
        )

        # Run the updator
        cross_join_updator(node_data)

        # Verify no duplicates
        left_cols = [r.old_name for r in node_data.setting_input.cross_join_input.left_select.renames]
        right_cols = [r.old_name for r in node_data.setting_input.cross_join_input.right_select.renames]

        assert len(left_cols) == 1, f"Expected 1 left column, got {len(left_cols)}: {left_cols}"
        assert len(right_cols) == 1, f"Expected 1 right column, got {len(right_cols)}: {right_cols}"

        # Verify is_available is now True
        assert node_data.setting_input.cross_join_input.left_select.renames[0].is_available is True
        assert node_data.setting_input.cross_join_input.right_select.renames[0].is_available is True

    def test_join_updator_no_duplicates_when_is_available_false(self):
        """Test that join updator doesn't create duplicates when is_available=False."""
        from flowfile_core.flowfile.setting_generator.settings import join as join_updator
        from flowfile_core.schemas.output_model import NodeData, TableExample

        existing_join = transform_schema.JoinInput(
            join_mapping='id',
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='id', new_name='id', is_available=False),
                transform_schema.SelectInput(old_name='name', new_name='name', is_available=False),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='id', new_name='id', is_available=False),
                transform_schema.SelectInput(old_name='value', new_name='value', is_available=False),
            ]),
        )

        node_data = NodeData(
            flow_id=1,
            node_id=1,
            flow_type='join',
            setting_input=input_schema.NodeJoin(
                flow_id=1,
                node_id=1,
                join_input=existing_join,
            ),
            main_input=TableExample(node_id=1, number_of_records=1, number_of_columns=2, name='left', table_schema=[], columns=['id', 'name']),
            right_input=TableExample(node_id=2, number_of_records=1, number_of_columns=2, name='right', table_schema=[], columns=['id', 'value']),
        )

        # Run the updator
        join_updator(node_data)

        # Verify no duplicates
        left_cols = [r.old_name for r in node_data.setting_input.join_input.left_select.renames]
        right_cols = [r.old_name for r in node_data.setting_input.join_input.right_select.renames]

        assert len(left_cols) == 2, f"Expected 2 left columns, got {len(left_cols)}: {left_cols}"
        assert len(right_cols) == 2, f"Expected 2 right columns, got {len(right_cols)}: {right_cols}"

        # All should now be available
        for r in node_data.setting_input.join_input.left_select.renames:
            assert r.is_available is True
        for r in node_data.setting_input.join_input.right_select.renames:
            assert r.is_available is True

    def test_updator_sets_is_available_correctly(self):
        """Test that is_available is set based on incoming dataframe columns."""
        from flowfile_core.flowfile.setting_generator.settings import cross_join as cross_join_updator
        from flowfile_core.schemas.output_model import NodeData, TableExample

        # Settings have columns that may or may not exist in input
        existing_cross_join = transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='exists', new_name='exists', is_available=False),
                transform_schema.SelectInput(old_name='removed', new_name='removed', is_available=True),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='right_exists', new_name='right_exists', is_available=False),
            ]),
        )

        node_data = NodeData(
            flow_id=1,
            node_id=1,
            flow_type='cross_join',
            setting_input=input_schema.NodeCrossJoin(
                flow_id=1,
                node_id=1,
                cross_join_input=existing_cross_join,
            ),
            main_input=TableExample(node_id=1, number_of_records=1, number_of_columns=2, name='left', table_schema=[], columns=['exists', 'new_col']),  # 'removed' is no longer in input
            right_input=TableExample(node_id=2, number_of_records=1, number_of_columns=1, name='right', table_schema=[], columns=['right_exists']),
        )

        cross_join_updator(node_data)

        left_cols = {r.old_name: r for r in node_data.setting_input.cross_join_input.left_select.renames}

        assert 'exists' in left_cols
        assert left_cols['exists'].is_available is True

        assert 'removed' in left_cols
        assert left_cols['removed'].is_available is False

        assert 'new_col' in left_cols
        assert left_cols['new_col'].is_available is True

    def test_column_renamed_in_input_keeps_old_column_unavailable(self):
        """Test that when input column is renamed, the old column stays but is marked unavailable."""
        from flowfile_core.flowfile.setting_generator.settings import cross_join as cross_join_updator
        from flowfile_core.schemas.output_model import NodeData, TableExample

        # Initial state: settings have "Column 1" selected
        existing_cross_join = transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='Column 1', new_name='Column 1', is_available=True),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='other', new_name='other', is_available=True),
            ]),
        )

        # Now input has "Column 2" instead of "Column 1" (column was renamed)
        node_data = NodeData(
            flow_id=1,
            node_id=1,
            flow_type='cross_join',
            setting_input=input_schema.NodeCrossJoin(
                flow_id=1,
                node_id=1,
                cross_join_input=existing_cross_join,
            ),
            main_input=TableExample(node_id=1, number_of_records=1, number_of_columns=1, name='left', table_schema=[], columns=['Column 2']),  # Column 1 renamed to Column 2
            right_input=TableExample(node_id=2, number_of_records=1, number_of_columns=1, name='right', table_schema=[], columns=['other']),
        )

        cross_join_updator(node_data)

        left_cols = {r.old_name: r for r in node_data.setting_input.cross_join_input.left_select.renames}

        assert 'Column 1' in left_cols
        assert left_cols['Column 1'].is_available is False

        assert 'Column 2' in left_cols
        assert left_cols['Column 2'].is_available is True


class TestAddNewSelectColumn:
    """Tests for the add_new_select_column method."""

    def test_cross_join_input_add_column_left(self):
        """Test adding a column to left side of cross join."""
        cross_join = transform_schema.CrossJoinInput(
            left_select=[transform_schema.SelectInput(old_name='existing')],
            right_select=[transform_schema.SelectInput(old_name='other')],
        )

        new_col = transform_schema.SelectInput(old_name='new_col')
        cross_join.add_new_select_column(new_col, 'left')

        left_cols = [r.old_name for r in cross_join.left_select.renames]
        assert 'new_col' in left_cols
        assert 'existing' in left_cols

    def test_cross_join_input_add_column_right(self):
        """Test adding a column to right side of cross join."""
        cross_join = transform_schema.CrossJoinInput(
            left_select=[transform_schema.SelectInput(old_name='existing')],
            right_select=[transform_schema.SelectInput(old_name='other')],
        )

        new_col = transform_schema.SelectInput(old_name='new_col')
        cross_join.add_new_select_column(new_col, 'right')

        right_cols = [r.old_name for r in cross_join.right_select.renames]
        assert 'new_col' in right_cols
        assert 'other' in right_cols

    def test_join_input_add_column(self):
        """Test adding a column to JoinInput."""
        join_input = transform_schema.JoinInput(
            join_mapping='id',
            left_select=[transform_schema.SelectInput(old_name='id')],
            right_select=[transform_schema.SelectInput(old_name='id')],
        )

        new_col = transform_schema.SelectInput(old_name='new_col')
        join_input.add_new_select_column(new_col, 'left')

        left_cols = [r.old_name for r in join_input.left_select.renames]
        assert 'new_col' in left_cols

    def test_add_column_sets_new_name_if_none(self):
        """Test that add_new_select_column sets new_name to old_name if not provided."""
        cross_join = transform_schema.CrossJoinInput(
            left_select=[],
            right_select=[],
        )

        new_col = transform_schema.SelectInput(old_name='test_col')
        new_col.new_name = None  # Explicitly set to None
        cross_join.add_new_select_column(new_col, 'left')

        added = cross_join.left_select.renames[0]
        assert added.new_name == 'test_col'


class TestCrossJoinExecution:
    """Integration tests for cross join execution with setting updators."""

    def test_cross_join_executes_with_overlapping_columns(self, basic_flow: FlowGraph):
        """Test that cross join actually executes correctly with overlapping columns."""
        create_manual_input_node(basic_flow, 1, [
            {'id': 1, 'value': 'left1'},
            {'id': 2, 'value': 'left2'},
        ])
        create_manual_input_node(basic_flow, 2, [
            {'id': 10, 'value': 'right1'},
            {'id': 20, 'value': 'right2'},
        ])
        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        # Add cross_join settings (the setting generator doesn't run automatically in tests)
        cross_join_input = transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='id', new_name='id'),
                transform_schema.SelectInput(old_name='value', new_name='value'),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='id', new_name='right_id'),
                transform_schema.SelectInput(old_name='value', new_name='right_value'),
            ]),
        )
        basic_flow.add_cross_join(input_schema.NodeCrossJoin(
            flow_id=1, node_id=3, cross_join_input=cross_join_input
        ))

        node = basic_flow.get_node(3)
        basic_flow.run_graph()
        result = node.get_resulting_data()

        # Cross join of 2 rows x 2 rows = 4 rows
        assert result.count() == 4

        # Verify columns don't have duplicates in the result
        assert len(result.columns) == len(set(result.columns))

    def test_cross_join_executes_after_input_change(self, basic_flow: FlowGraph):
        """Test that cross join executes correctly after input columns change."""
        create_manual_input_node(basic_flow, 1, [{'a': 1}])
        create_manual_input_node(basic_flow, 2, [{'b': 2}])

        basic_flow.add_node_promise(input_schema.NodePromise(
            flow_id=1, node_id=3, node_type='cross_join'
        ))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(basic_flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))

        # Add cross_join settings
        cross_join_input = transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='a', new_name='a'),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='b', new_name='b'),
            ]),
        )
        basic_flow.add_cross_join(input_schema.NodeCrossJoin(
            flow_id=1, node_id=3, cross_join_input=cross_join_input
        ))

        node = basic_flow.get_node(3)
        basic_flow.run_graph()
        result1 = node.get_resulting_data()
        assert set(result1.columns) == {'a', 'b'}

        # Update left input with new column
        basic_flow.add_manual_input(input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{'a': 1, 'c': 3}])
        ))

        # Trigger updator to add new column to settings
        node.get_node_data(basic_flow.flow_id)

        node.reset()
        basic_flow.run_graph()
        result2 = node.get_resulting_data()

        assert 'c' in result2.columns
