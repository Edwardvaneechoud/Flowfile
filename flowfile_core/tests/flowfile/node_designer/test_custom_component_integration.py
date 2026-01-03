"""
End-to-end tests for custom nodes in flow graphs.
Tests the full lifecycle: definition, registration, graph integration, and execution.
"""
import pytest
import polars as pl
from typing import List, Dict

from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, nodes_list, add_to_custom_node_store
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import (
    CustomNodeBase,
    NodeSettings,
    Section,
    TextInput,
    NumericInput,
    ToggleSwitch,
    SingleSelect,
    MultiSelect,
    ColumnSelector,
    Types,
    IncomingColumns,
)
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.types import DataType


# =============================================================================
# Test Utilities
# =============================================================================

def create_flowfile_handler() -> FlowfileHandler:
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


def create_graph(flow_id: int = 1, execution_mode: str = 'Development') -> FlowGraph:
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=flow_id,
        name=f'flow_{flow_id}',
        path='.',
        execution_mode=execution_mode
    ))
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: List[Dict], node_id: int = 1) -> FlowGraph:
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


def add_custom_node_to_graph(
        graph: FlowGraph,
        custom_node_class: type,
        node_id: int,
        settings: Dict
) -> FlowGraph:
    """Helper to add a custom node to a graph with settings."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type=custom_node_class().item,
        is_user_defined=True
    )
    graph.add_node_promise(node_promise)
    user_defined_node = custom_node_class.from_settings(settings)
    node_settings = input_schema.UserDefinedNode(
        flow_id=graph.flow_id,
        node_id=node_id,
        settings=settings,
        is_user_defined=True
    )
    graph.add_user_defined_node(
        custom_node=user_defined_node,
        user_defined_node_settings=node_settings
    )
    return graph


def handle_run_info(run_info):
    """Helper to handle run results and raise on failure."""
    if not run_info.success:
        errors = 'errors:'
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f'\n node_id:{node_step.node_id}, error: {node_step.error}'
        raise ValueError(f'Graph should run successfully:\n{errors}')


# =============================================================================
# Custom Node Fixtures
# =============================================================================

@pytest.fixture
def AddFixedColumnNode():
    """A simple custom node that adds a column with a fixed value."""

    class AddFixedColumn(CustomNodeBase):
        node_name: str = "Add Fixed Column"
        node_category: str = "Transform"
        title: str = "Add Fixed Column"
        intro: str = "Adds a new column with a fixed value"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Configuration",
                column_name=TextInput(
                    label="New Column Name",
                    default="new_column",
                    placeholder="Enter column name"
                ),
                fixed_value=TextInput(
                    label="Fixed Value",
                    default="default_value",
                    placeholder="Enter value"
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            col_name = self.settings_schema.config.column_name.value
            value = self.settings_schema.config.fixed_value.value

            if not col_name:
                return lf

            return lf.with_columns(pl.lit(value).alias(col_name))

    return AddFixedColumn


@pytest.fixture
def NumericMultiplierNode():
    """A custom node that multiplies a numeric column by a factor."""

    class NumericMultiplier(CustomNodeBase):
        node_name: str = "Numeric Multiplier"
        node_category: str = "Math"
        title: str = "Multiply Column"
        intro: str = "Multiplies a numeric column by a given factor"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Configuration",
                column=SingleSelect(
                    label="Column to Multiply",
                    options=IncomingColumns
                ),
                factor=NumericInput(
                    label="Multiplication Factor",
                    default=2.0,
                    min_value=0.0
                ),
                output_column=TextInput(
                    label="Output Column Name",
                    default="",
                    placeholder="Leave empty to overwrite"
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            col = self.settings_schema.config.column.value
            factor = self.settings_schema.config.factor.value or 1.0
            output_col = self.settings_schema.config.output_column.value or col

            if not col:
                return lf

            return lf.with_columns(
                (pl.col(col) * factor).alias(output_col)
            )

    return NumericMultiplier


@pytest.fixture
def ColumnsToStringNode():
    """A custom node that converts selected numeric columns to string."""

    class ColumnsToString(CustomNodeBase):
        node_name: str = "Columns to String"
        node_category: str = "Convert"
        title: str = "Convert Columns to String"
        intro: str = "Select numeric columns and convert them to string"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            input_section=Section(
                title="Input",
                numeric_columns=ColumnSelector(
                    label="Select numeric columns",
                    required=False,
                    multiple=True,
                    data_types=Types.Numeric,
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            selected_cols = self.settings_schema.input_section.numeric_columns.value or []

            if not selected_cols:
                return lf

            schema = lf.collect_schema()
            expr = [
                pl.col(col).cast(pl.Utf8) if col in selected_cols else pl.col(col)
                for col in schema.names()
            ]
            return lf.select(expr)

    return ColumnsToString


@pytest.fixture
def FilterByConditionNode():
    """A custom node with toggle and conditional filtering."""

    class FilterByCondition(CustomNodeBase):
        node_name: str = "Conditional Filter"
        node_category: str = "Filter"
        title: str = "Filter by Condition"
        intro: str = "Filter rows based on configurable conditions"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Filter Configuration",
                column=SingleSelect(
                    label="Column to Filter",
                    options=IncomingColumns
                ),
                filter_type=SingleSelect(
                    label="Filter Type",
                    options=["equals", "contains", "greater_than", "less_than"],
                    default="equals"
                ),
                filter_value=TextInput(
                    label="Filter Value",
                    default=""
                ),
                case_sensitive=ToggleSwitch(
                    label="Case Sensitive",
                    default=True
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            col = self.settings_schema.config.column.value
            filter_type = self.settings_schema.config.filter_type.value
            filter_value = self.settings_schema.config.filter_value.value
            case_sensitive = self.settings_schema.config.case_sensitive.value

            if not col or not filter_value:
                return lf

            if filter_type == "equals":
                if case_sensitive:
                    return lf.filter(pl.col(col) == filter_value)
                else:
                    return lf.filter(pl.col(col).str.to_lowercase() == filter_value.lower())
            elif filter_type == "contains":
                if case_sensitive:
                    return lf.filter(pl.col(col).str.contains(filter_value))
                else:
                    return lf.filter(pl.col(col).str.to_lowercase().str.contains(filter_value.lower()))
            elif filter_type == "greater_than":
                return lf.filter(pl.col(col) > float(filter_value))
            elif filter_type == "less_than":
                return lf.filter(pl.col(col) < float(filter_value))

            return lf

    return FilterByCondition


@pytest.fixture
def MultiColumnAggregatorNode():
    """A custom node that aggregates multiple columns."""

    class MultiColumnAggregator(CustomNodeBase):
        node_name: str = "Multi Column Aggregator"
        node_category: str = "Aggregate"
        title: str = "Aggregate Multiple Columns"
        intro: str = "Sum, average, or concatenate multiple columns"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Aggregation Settings",
                columns=MultiSelect(
                    label="Columns to Aggregate",
                    options=IncomingColumns
                ),
                aggregation_type=SingleSelect(
                    label="Aggregation Type",
                    options=["sum", "mean", "min", "max", "concat"],
                    default="sum"
                ),
                output_column=TextInput(
                    label="Output Column Name",
                    default="aggregated"
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            columns = self.settings_schema.config.columns.value or []
            agg_type = self.settings_schema.config.aggregation_type.value
            output_col = self.settings_schema.config.output_column.value or "aggregated"

            if not columns:
                return lf

            if agg_type == "sum":
                expr = sum(pl.col(c) for c in columns)
            elif agg_type == "mean":
                expr = sum(pl.col(c) for c in columns) / len(columns)
            elif agg_type == "min":
                expr = pl.min_horizontal(*[pl.col(c) for c in columns])
            elif agg_type == "max":
                expr = pl.max_horizontal(*[pl.col(c) for c in columns])
            elif agg_type == "concat":
                expr = pl.concat_str([pl.col(c).cast(pl.Utf8) for c in columns], separator="_")
            else:
                return lf

            return lf.with_columns(expr.alias(output_col))

    return MultiColumnAggregator


# =============================================================================
# E2E Tests: Registration and Basic Graph Integration
# =============================================================================

class TestCustomNodeRegistration:
    """Tests for custom node registration in the node store."""

    def test_register_custom_node(self, AddFixedColumnNode):
        """Test that a custom node can be registered."""
        add_to_custom_node_store(AddFixedColumnNode)

        instance = AddFixedColumnNode()
        assert instance.item in CUSTOM_NODE_STORE
        assert any(n.name == instance.node_name for n in nodes_list)

    def test_register_multiple_custom_nodes(
            self, AddFixedColumnNode, NumericMultiplierNode, ColumnsToStringNode
    ):
        """Test registering multiple custom nodes."""
        add_to_custom_node_store(AddFixedColumnNode)
        add_to_custom_node_store(NumericMultiplierNode)
        add_to_custom_node_store(ColumnsToStringNode)

        assert AddFixedColumnNode().item in CUSTOM_NODE_STORE
        assert NumericMultiplierNode().item in CUSTOM_NODE_STORE
        assert ColumnsToStringNode().item in CUSTOM_NODE_STORE


class TestCustomNodeInGraph:
    """Tests for custom nodes integrated into flow graphs."""

    def test_add_fixed_column_in_graph(self, AddFixedColumnNode):
        """Test AddFixedColumn node in a graph."""
        add_to_custom_node_store(AddFixedColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"A": 1}, {"A": 2}, {"A": 3}], node_id=1)

        settings = {
            "config": {
                "column_name": "status",
                "fixed_value": "active"
            }
        }
        add_custom_node_to_graph(graph, AddFixedColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        expected = FlowDataEngine({
            "A": [1, 2, 3],
            "status": ["active", "active", "active"]
        })
        result.assert_equal(expected)

    def test_numeric_multiplier_in_graph(self, NumericMultiplierNode):
        """Test NumericMultiplier node in a graph."""
        add_to_custom_node_store(NumericMultiplierNode)

        graph = create_graph()
        add_manual_input(graph, [{"value": 10}, {"value": 20}, {"value": 30}], node_id=1)

        settings = {
            "config": {
                "column": "value",
                "factor": 2.5,
                "output_column": "multiplied"
            }
        }
        add_custom_node_to_graph(graph, NumericMultiplierNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        expected = FlowDataEngine({
            "value": [10, 20, 30],
            "multiplied": [25.0, 50.0, 75.0]
        })
        result.assert_equal(expected)

    def test_columns_to_string_in_graph(self, ColumnsToStringNode):
        """Test ColumnsToString node with ColumnSelector in a graph."""
        add_to_custom_node_store(ColumnsToStringNode)

        graph = create_graph()
        add_manual_input(graph, [
            {"id": 1, "score": 95.5, "name": "Alice"},
            {"id": 2, "score": 87.3, "name": "Bob"},
            {"id": 3, "score": 92.1, "name": "Charlie"}
        ], node_id=1)

        settings = {
            "input_section": {
                "numeric_columns": ["id", "score"]
            }
        }
        add_custom_node_to_graph(graph, ColumnsToStringNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        result_dict = result.to_dict()

        # Verify conversions
        assert result_dict["id"] == ["1", "2", "3"]
        assert result_dict["score"] == ["95.5", "87.3", "92.1"]
        assert result_dict["name"] == ["Alice", "Bob", "Charlie"]

    def test_filter_by_condition_in_graph(self, FilterByConditionNode):
        """Test FilterByCondition node with various settings."""
        add_to_custom_node_store(FilterByConditionNode)

        graph = create_graph()
        add_manual_input(graph, [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
            {"name": "alice", "age": 28}
        ], node_id=1)

        # Test case-insensitive equals filter
        settings = {
            "config": {
                "column": "name",
                "filter_type": "equals",
                "filter_value": "alice",
                "case_sensitive": False
            }
        }
        add_custom_node_to_graph(graph, FilterByConditionNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        assert result.count() == 2  # Both "Alice" and "alice"

    def test_multi_column_aggregator_in_graph(self, MultiColumnAggregatorNode):
        """Test MultiColumnAggregator node with sum aggregation."""
        add_to_custom_node_store(MultiColumnAggregatorNode)

        graph = create_graph()
        add_manual_input(graph, [
            {"a": 1, "b": 2, "c": 3},
            {"a": 4, "b": 5, "c": 6},
            {"a": 7, "b": 8, "c": 9}
        ], node_id=1)

        settings = {
            "config": {
                "columns": ["a", "b", "c"],
                "aggregation_type": "sum",
                "output_column": "total"
            }
        }
        add_custom_node_to_graph(graph, MultiColumnAggregatorNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        result_dict = result.to_dict()

        assert result_dict["total"] == [6, 15, 24]


class TestCustomNodeChaining:
    """Tests for chaining multiple custom nodes together."""

    def test_chain_two_custom_nodes(self, AddFixedColumnNode, NumericMultiplierNode):
        """Test chaining AddFixedColumn -> NumericMultiplier."""
        add_to_custom_node_store(AddFixedColumnNode)
        add_to_custom_node_store(NumericMultiplierNode)

        graph = create_graph()
        add_manual_input(graph, [{"value": 10}, {"value": 20}], node_id=1)

        # First custom node: add a multiplier column
        settings_1 = {
            "config": {
                "column_name": "factor",
                "fixed_value": "2"
            }
        }
        add_custom_node_to_graph(graph, AddFixedColumnNode, node_id=2, settings=settings_1)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        # Second custom node: multiply the value
        settings_2 = {
            "config": {
                "column": "value",
                "factor": 3.0,
                "output_column": "tripled"
            }
        }
        add_custom_node_to_graph(graph, NumericMultiplierNode, node_id=3, settings=settings_2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(3).get_resulting_data()
        result_dict = result.to_dict()

        assert "factor" in result_dict
        assert result_dict["tripled"] == [30.0, 60.0]

    def test_chain_three_custom_nodes(
            self, AddFixedColumnNode, NumericMultiplierNode, ColumnsToStringNode
    ):
        """Test chaining three custom nodes."""
        add_to_custom_node_store(AddFixedColumnNode)
        add_to_custom_node_store(NumericMultiplierNode)
        add_to_custom_node_store(ColumnsToStringNode)

        graph = create_graph()
        add_manual_input(graph, [{"value": 5}, {"value": 10}], node_id=1)

        # Node 2: Add fixed column
        add_custom_node_to_graph(graph, AddFixedColumnNode, node_id=2, settings={
            "config": {"column_name": "status", "fixed_value": "processed"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        # Node 3: Multiply
        add_custom_node_to_graph(graph, NumericMultiplierNode, node_id=3, settings={
            "config": {"column": "value", "factor": 2.0, "output_column": "doubled"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

        # Node 4: Convert to string
        add_custom_node_to_graph(graph, ColumnsToStringNode, node_id=4, settings={
            "input_section": {"numeric_columns": ["value", "doubled"]}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(3, 4))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(4).get_resulting_data()
        result_dict = result.to_dict()

        assert result_dict["value"] == ["5", "10"]
        assert result_dict["doubled"] == ["10.0", "20.0"]
        assert result_dict["status"] == ["processed", "processed"]


class TestCustomNodeWithBuiltinNodes:
    """Tests for mixing custom nodes with built-in nodes."""

    def test_custom_node_after_builtin_filter(self, AddFixedColumnNode):
        """Test custom node after a built-in filter node."""
        add_to_custom_node_store(AddFixedColumnNode)

        graph = create_graph()
        add_manual_input(graph, [
            {"name": "Alice", "score": 90},
            {"name": "Bob", "score": 70},
            {"name": "Charlie", "score": 85}
        ], node_id=1)

        # Add built-in filter node
        from flowfile_core.schemas import transform_schema
        filter_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type='filter')
        graph.add_node_promise(filter_promise)
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            filter_input=transform_schema.FilterInput(
                advanced_filter="[score] >= 80",
                filter_type='advanced'
            )
        )
        graph.add_filter(filter_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        # Add custom node after filter
        add_custom_node_to_graph(graph, AddFixedColumnNode, node_id=3, settings={
            "config": {"column_name": "passed", "fixed_value": "yes"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(3).get_resulting_data()
        assert result.count() == 2  # Only Alice and Charlie
        result_dict = result.to_dict()
        assert all(v == "yes" for v in result_dict["passed"])

    def test_builtin_node_after_custom_node(self, NumericMultiplierNode):
        """Test built-in node after a custom node."""
        add_to_custom_node_store(NumericMultiplierNode)

        graph = create_graph()
        add_manual_input(graph, [
            {"value": 10, "category": "A"},
            {"value": 20, "category": "A"},
            {"value": 30, "category": "B"}
        ], node_id=1)

        # Add custom multiplier node
        add_custom_node_to_graph(graph, NumericMultiplierNode, node_id=2, settings={
            "config": {"column": "value", "factor": 2.0, "output_column": "doubled"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        # Add built-in group by node
        from flowfile_core.schemas import transform_schema
        groupby_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type='group_by')
        graph.add_node_promise(groupby_promise)
        groupby_input = transform_schema.GroupByInput([
            transform_schema.AggColl('category', 'groupby'),
            transform_schema.AggColl('doubled', 'sum', 'total_doubled')
        ])
        graph.add_group_by(input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=3,
            groupby_input=groupby_input
        ))
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(3).get_resulting_data()
        result_dict = result.to_dict()

        # Category A: 10*2 + 20*2 = 60, Category B: 30*2 = 60
        assert set(result_dict["category"]) == {"A", "B"}


class TestCustomNodeEdgeCases:
    """Tests for edge cases and error handling."""

    def test_custom_node_with_empty_input(self, AddFixedColumnNode):
        """Test custom node with empty input data."""
        add_to_custom_node_store(AddFixedColumnNode)

        graph = create_graph()
        add_manual_input(graph, [], node_id=1)

        add_custom_node_to_graph(graph, AddFixedColumnNode, node_id=2, settings={
            "config": {"column_name": "new_col", "fixed_value": "value"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        assert result.count() == 0

    def test_custom_node_with_missing_settings(self, ColumnsToStringNode):
        """Test custom node when no columns are selected."""
        add_to_custom_node_store(ColumnsToStringNode)

        graph = create_graph()
        add_manual_input(graph, [{"a": 1}, {"a": 2}], node_id=1)

        # No columns selected - should pass through unchanged
        add_custom_node_to_graph(graph, ColumnsToStringNode, node_id=2, settings={
            "input_section": {"numeric_columns": []}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        # Column should remain as integer since no conversion was requested
        assert result.to_dict()["a"] == [1, 2]

    def test_custom_node_update_settings_and_rerun(self, NumericMultiplierNode):
        """Test updating custom node settings and re-running."""
        add_to_custom_node_store(NumericMultiplierNode)

        graph = create_graph()
        add_manual_input(graph, [{"value": 10}], node_id=1)

        # First run with factor=2
        add_custom_node_to_graph(graph, NumericMultiplierNode, node_id=2, settings={
            "config": {"column": "value", "factor": 2.0, "output_column": "result"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result1 = graph.get_node(2).get_resulting_data().to_dict()
        assert result1["result"] == [20.0]

        # Update settings and re-run with factor=5
        new_node = NumericMultiplierNode.from_settings({
            "config": {"column": "value", "factor": 5.0, "output_column": "result"}
        })
        new_settings = input_schema.UserDefinedNode(
            flow_id=graph.flow_id,
            node_id=2,
            settings={"config": {"column": "value", "factor": 5.0, "output_column": "result"}},
            is_user_defined=True
        )
        graph.add_user_defined_node(custom_node=new_node, user_defined_node_settings=new_settings)

        run_result2 = graph.run_graph()
        handle_run_info(run_result2)

        result2 = graph.get_node(2).get_resulting_data().to_dict()
        assert result2["result"] == [50.0]


class TestCustomNodeSchema:
    """Tests for schema prediction and validation."""

    def test_custom_node_schema_prediction(self, AddFixedColumnNode):
        """Test that schema is correctly predicted before running."""
        add_to_custom_node_store(AddFixedColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"existing": 1}], node_id=1)

        add_custom_node_to_graph(graph, AddFixedColumnNode, node_id=2, settings={
            "config": {"column_name": "new_column", "fixed_value": "test"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        # Get predicted schema before running
        node = graph.get_node(2)
        predicted_schema = node.get_predicted_schema()

        # Should have both original and new column
        column_names = [col.column_name for col in predicted_schema]
        assert "existing" in column_names
        assert "new_column" in column_names

    def test_column_selector_filters_by_type(self, ColumnsToStringNode):
        """Test that ColumnSelector correctly filters columns by data type."""
        add_to_custom_node_store(ColumnsToStringNode)

        node = ColumnsToStringNode()
        selector = node.settings_schema.input_section.numeric_columns

        # Verify the selector is configured for numeric types
        expected_numeric_types = {
            DataType.Int8, DataType.Int16, DataType.Int32, DataType.Int64,
            DataType.UInt8, DataType.UInt16, DataType.UInt32, DataType.UInt64,
            DataType.Float32, DataType.Float64, DataType.Decimal
        }
        assert set(selector.data_types_filter) == expected_numeric_types


# =============================================================================
# Source Node Fixtures (No Inputs, Only Outputs)
# =============================================================================

@pytest.fixture
def DataGeneratorNode():
    """A custom source node that generates test data."""

    class DataGenerator(CustomNodeBase):
        node_name: str = "Data Generator"
        node_category: str = "Input"
        title: str = "Generate Test Data"
        intro: str = "Generates a dataset with configurable rows"
        number_of_inputs: int = 0
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Generator Settings",
                num_rows=NumericInput(
                    label="Number of Rows",
                    default=10,
                    min_value=1,
                    max_value=10000
                ),
                start_id=NumericInput(
                    label="Starting ID",
                    default=1,
                    min_value=0
                ),
                include_names=ToggleSwitch(
                    label="Include Names Column",
                    default=True
                ),
                prefix=TextInput(
                    label="Name Prefix",
                    default="Item",
                    placeholder="Prefix for generated names"
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            num_rows = int(self.settings_schema.config.num_rows.value or 10)
            start_id = int(self.settings_schema.config.start_id.value or 1)
            include_names = self.settings_schema.config.include_names.value
            prefix = self.settings_schema.config.prefix.value or "Item"

            ids = list(range(start_id, start_id + num_rows))
            data = {"id": ids}

            if include_names:
                data["name"] = [f"{prefix}_{i}" for i in ids]

            return pl.LazyFrame(data)

    return DataGenerator


@pytest.fixture
def DateRangeGeneratorNode():
    """A custom source node that generates a date range."""

    class DateRangeGenerator(CustomNodeBase):
        node_name: str = "Date Range Generator"
        node_category: str = "Input"
        title: str = "Generate Date Range"
        intro: str = "Generates a sequence of dates"
        number_of_inputs: int = 0
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Date Range Settings",
                start_date=TextInput(
                    label="Start Date (YYYY-MM-DD)",
                    default="2024-01-01",
                    placeholder="2024-01-01"
                ),
                num_days=NumericInput(
                    label="Number of Days",
                    default=7,
                    min_value=1,
                    max_value=365
                ),
                include_weekday=ToggleSwitch(
                    label="Include Weekday Name",
                    default=True
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            from datetime import datetime, timedelta

            start_str = self.settings_schema.config.start_date.value or "2024-01-01"
            num_days = int(self.settings_schema.config.num_days.value or 7)
            include_weekday = self.settings_schema.config.include_weekday.value

            try:
                start_date = datetime.strptime(start_str, "%Y-%m-%d")
            except ValueError:
                start_date = datetime(2024, 1, 1)

            dates = [start_date + timedelta(days=i) for i in range(num_days)]

            data = {
                "date": [d.date() for d in dates],
                "day_number": list(range(1, num_days + 1))
            }

            if include_weekday:
                data["weekday"] = [d.strftime("%A") for d in dates]

            return pl.LazyFrame(data)

    return DateRangeGenerator


@pytest.fixture
def LookupTableNode():
    """A custom source node that provides a static lookup/reference table."""

    class LookupTable(CustomNodeBase):
        node_name: str = "Lookup Table"
        node_category: str = "Input"
        title: str = "Static Lookup Table"
        intro: str = "Provides a configurable reference table"
        number_of_inputs: int = 0
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Lookup Configuration",
                table_type=SingleSelect(
                    label="Table Type",
                    options=["status_codes", "priorities", "regions", "custom"],
                    default="status_codes"
                ),
                custom_values=TextInput(
                    label="Custom Values (comma-separated)",
                    default="",
                    placeholder="value1,value2,value3"
                ),
            ),
        )

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            table_type = self.settings_schema.config.table_type.value
            custom_values = self.settings_schema.config.custom_values.value or ""

            if table_type == "status_codes":
                data = {
                    "code": ["NEW", "PROG", "DONE", "CANC"],
                    "description": ["New", "In Progress", "Completed", "Cancelled"],
                    "is_active": [True, True, False, False]
                }
            elif table_type == "priorities":
                data = {
                    "priority": [1, 2, 3, 4, 5],
                    "label": ["Critical", "High", "Medium", "Low", "Trivial"],
                    "sla_hours": [4, 8, 24, 72, 168]
                }
            elif table_type == "regions":
                data = {
                    "region_code": ["NA", "EU", "APAC", "LATAM"],
                    "region_name": ["North America", "Europe", "Asia Pacific", "Latin America"],
                    "timezone": ["UTC-5", "UTC+1", "UTC+8", "UTC-3"]
                }
            elif table_type == "custom" and custom_values:
                values = [v.strip() for v in custom_values.split(",") if v.strip()]
                data = {
                    "id": list(range(1, len(values) + 1)),
                    "value": values
                }
            else:
                data = {"id": [1], "value": ["default"]}

            return pl.LazyFrame(data)

    return LookupTable


# =============================================================================
# E2E Tests: Source Nodes (No Inputs)
# =============================================================================

class TestSourceNodes:
    """Tests for custom nodes with no inputs (source/generator nodes)."""

    def test_data_generator_basic(self, DataGeneratorNode):
        """Test basic data generation."""
        add_to_custom_node_store(DataGeneratorNode)

        graph = create_graph()

        settings = {
            "config": {
                "num_rows": 5,
                "start_id": 100,
                "include_names": True,
                "prefix": "Record"
            }
        }
        add_custom_node_to_graph(graph, DataGeneratorNode, node_id=1, settings=settings)
        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        result_dict = result.to_dict()

        assert result.count() == 5
        assert result_dict["id"] == [100, 101, 102, 103, 104]
        assert result_dict["name"] == ["Record_100", "Record_101", "Record_102", "Record_103", "Record_104"]

    def test_data_generator_without_names(self, DataGeneratorNode):
        """Test data generation without names column."""
        add_to_custom_node_store(DataGeneratorNode)

        graph = create_graph()

        settings = {
            "config": {
                "num_rows": 3,
                "start_id": 1,
                "include_names": False,
                "prefix": "Ignored"
            }
        }
        add_custom_node_to_graph(graph, DataGeneratorNode, node_id=1, settings=settings)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()

        assert result.count() == 3
        assert result.columns == ["id"]
        assert "name" not in result.columns

    def test_date_range_generator(self, DateRangeGeneratorNode):
        """Test date range generation."""
        add_to_custom_node_store(DateRangeGeneratorNode)

        graph = create_graph()

        settings = {
            "config": {
                "start_date": "2024-01-01",
                "num_days": 7,
                "include_weekday": True
            }
        }
        add_custom_node_to_graph(graph, DateRangeGeneratorNode, node_id=1, settings=settings)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        result_dict = result.to_dict()

        assert result.count() == 7
        assert result_dict["day_number"] == [1, 2, 3, 4, 5, 6, 7]
        assert "weekday" in result_dict
        assert result_dict["weekday"][0] == "Monday"  # 2024-01-01 is Monday

    def test_lookup_table_status_codes(self, LookupTableNode):
        """Test lookup table with status codes."""
        add_to_custom_node_store(LookupTableNode)

        graph = create_graph()

        settings = {
            "config": {
                "table_type": "status_codes",
                "custom_values": ""
            }
        }
        add_custom_node_to_graph(graph, LookupTableNode, node_id=1, settings=settings)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        result_dict = result.to_dict()

        assert result.count() == 4
        assert result_dict["code"] == ["NEW", "PROG", "DONE", "CANC"]
        assert "description" in result_dict
        assert "is_active" in result_dict

    def test_lookup_table_custom_values(self, LookupTableNode):
        """Test lookup table with custom values."""
        add_to_custom_node_store(LookupTableNode)

        graph = create_graph()

        settings = {
            "config": {
                "table_type": "custom",
                "custom_values": "apple, banana, cherry"
            }
        }
        add_custom_node_to_graph(graph, LookupTableNode, node_id=1, settings=settings)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        result_dict = result.to_dict()

        assert result.count() == 3
        assert result_dict["value"] == ["apple", "banana", "cherry"]
        assert result_dict["id"] == [1, 2, 3]

    def test_source_node_connected_to_transform(self, DataGeneratorNode, NumericMultiplierNode):
        """Test source node feeding into a transform node."""
        add_to_custom_node_store(DataGeneratorNode)
        add_to_custom_node_store(NumericMultiplierNode)

        graph = create_graph()

        # Source node generates data
        add_custom_node_to_graph(graph, DataGeneratorNode, node_id=1, settings={
            "config": {
                "num_rows": 3,
                "start_id": 10,
                "include_names": False,
                "prefix": ""
            }
        })

        # Transform node multiplies the generated IDs
        add_custom_node_to_graph(graph, NumericMultiplierNode, node_id=2, settings={
            "config": {
                "column": "id",
                "factor": 10.0,
                "output_column": "id_times_ten"
            }
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        result_dict = result.to_dict()

        assert result_dict["id"] == [10, 11, 12]
        assert result_dict["id_times_ten"] == [100.0, 110.0, 120.0]

    def test_source_node_with_builtin_join(self, LookupTableNode):
        """Test source node (lookup) joined with manual input data."""
        add_to_custom_node_store(LookupTableNode)

        graph = create_graph()

        # Manual input with status codes to look up
        add_manual_input(graph, [
            {"task_id": 1, "status": "NEW"},
            {"task_id": 2, "status": "PROG"},
            {"task_id": 3, "status": "DONE"},
        ], node_id=1)

        # Lookup table source node
        add_custom_node_to_graph(graph, LookupTableNode, node_id=2, settings={
            "config": {"table_type": "status_codes", "custom_values": ""}
        })

        # Join node
        join_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type='join')
        graph.add_node_promise(join_promise)

        join_data = {
            'flow_id': graph.flow_id,
            'node_id': 3,
            'join_input': {
                'join_mapping': [{'left_col': 'status', 'right_col': 'code'}],
                'left_select': {'renames': [
                    {'old_name': 'task_id', 'new_name': 'task_id', 'keep': True},
                    {'old_name': 'status', 'new_name': 'status', 'keep': True},
                ]},
                'right_select': {'renames': [
                    {'old_name': 'code', 'new_name': 'code', 'keep': False},
                    {'old_name': 'description', 'new_name': 'status_description', 'keep': True},
                    {'old_name': 'is_active', 'new_name': 'is_active', 'keep': True},
                ]},
                'how': 'left'
            },
            'auto_keep_all': False,
        }
        graph.add_join(input_schema.NodeJoin(**join_data))

        # Connect: manual_input (left) + lookup (right) -> join
        left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3)
        right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3)
        right_connection.input_connection.connection_class = 'input-1'
        add_connection(graph, left_connection)
        add_connection(graph, right_connection)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(3).get_resulting_data()
        result_dict = result.to_dict()

        assert result.count() == 3
        assert "status_description" in result_dict
        assert "is_active" in result_dict

    def test_multiple_source_nodes_in_graph(self, DataGeneratorNode, LookupTableNode):
        """Test multiple source nodes feeding into a cross join."""
        add_to_custom_node_store(DataGeneratorNode)
        add_to_custom_node_store(LookupTableNode)

        graph = create_graph()

        # First source: generate IDs
        add_custom_node_to_graph(graph, DataGeneratorNode, node_id=1, settings={
            "config": {"num_rows": 2, "start_id": 1, "include_names": False, "prefix": ""}
        })

        # Second source: lookup table
        add_custom_node_to_graph(graph, LookupTableNode, node_id=2, settings={
            "config": {"table_type": "priorities", "custom_values": ""}
        })

        # Cross join them
        cross_join_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type='cross_join')
        graph.add_node_promise(cross_join_promise)

        cross_join_data = {
            'flow_id': graph.flow_id,
            'node_id': 3,
            'cross_join_input': {
                'left_select': {'renames': [
                    {'old_name': 'id', 'new_name': 'id', 'keep': True}
                ]},
                'right_select': {'renames': [
                    {'old_name': 'priority', 'new_name': 'priority', 'keep': True},
                    {'old_name': 'label', 'new_name': 'priority_label', 'keep': True},
                    {'old_name': 'sla_hours', 'new_name': 'sla_hours', 'keep': False},
                ]}
            },
            'auto_keep_all': False,
        }
        graph.add_cross_join(input_schema.NodeCrossJoin(**cross_join_data))

        left_conn = input_schema.NodeConnection.create_from_simple_input(1, 3)
        right_conn = input_schema.NodeConnection.create_from_simple_input(2, 3)
        right_conn.input_connection.connection_class = 'input-1'
        add_connection(graph, left_conn)
        add_connection(graph, right_conn)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(3).get_resulting_data()

        # 2 IDs × 5 priorities = 10 rows
        assert result.count() == 10

    def test_source_node_schema_prediction(self, DataGeneratorNode):
        """Test that source nodes correctly predict their schema."""
        add_to_custom_node_store(DataGeneratorNode)

        graph = create_graph()

        add_custom_node_to_graph(graph, DataGeneratorNode, node_id=1, settings={
            "config": {
                "num_rows": 5,
                "start_id": 1,
                "include_names": True,
                "prefix": "Test"
            }
        })

        # Get predicted schema without running
        node = graph.get_node(1)
        predicted_schema = node.get_predicted_schema()

        column_names = [col.column_name for col in predicted_schema]
        assert "id" in column_names
        assert "name" in column_names


class TestSourceNodeEdgeCases:
    """Edge case tests for source nodes."""

    def test_source_node_with_zero_rows(self, DataGeneratorNode):
        """Test generator configured for minimum rows."""
        add_to_custom_node_store(DataGeneratorNode)
        graph = create_graph()

        settings = {
            "config": {
                "num_rows": 1,  # Minimum allowed
                "start_id": 0,
                "include_names": True,
                "prefix": "Only"
            }
        }
        add_custom_node_to_graph(graph, DataGeneratorNode, node_id=1, settings=settings)
        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        assert result.count() == 1
        assert result.to_dict()["name"] == ["Only_1"]

    def test_source_node_invalid_date_fallback(self, DateRangeGeneratorNode):
        """Test date generator with invalid date string falls back gracefully."""
        add_to_custom_node_store(DateRangeGeneratorNode)

        graph = create_graph()

        settings = {
            "config": {
                "start_date": "not-a-date",
                "num_days": 3,
                "include_weekday": False
            }
        }
        add_custom_node_to_graph(graph, DateRangeGeneratorNode, node_id=1, settings=settings)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        # Should fall back to default date
        assert result.count() == 3

    def test_source_node_empty_custom_values(self, LookupTableNode):
        """Test lookup table with empty custom values falls back to default."""
        add_to_custom_node_store(LookupTableNode)

        graph = create_graph()

        settings = {
            "config": {
                "table_type": "custom",
                "custom_values": ""  # Empty
            }
        }
        add_custom_node_to_graph(graph, LookupTableNode, node_id=1, settings=settings)

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        # Should return default fallback
        assert result.count() == 1
        assert result.to_dict()["value"] == ["default"]


@pytest.fixture
def WrongNoInputNode():
    """A buggy node that declares no inputs but tries to access inputs[0]."""

    class TestNodeNoSectionsSettings(NodeSettings):
        pass

    class TestNodeNoSections(CustomNodeBase):
        node_name: str = "test_node_no_sections"
        node_category: str = "Custom"
        title: str = "test_node_no_sections"
        intro: str = "A custom node for data processing"
        number_of_inputs: int = 0
        number_of_outputs: int = 1
        settings_schema: TestNodeNoSectionsSettings = TestNodeNoSectionsSettings()

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            # BUG: This node declares 0 inputs but tries to access inputs[0]
            lf = inputs[0]
            return lf

    return TestNodeNoSections  # Return class, not instance


class TestMisconfiguredNodes:
    """Tests for nodes with configuration errors or bugs."""

    def test_no_input_node_accessing_inputs_fails(self, WrongNoInputNode):
        """Test that a source node incorrectly accessing inputs raises an error."""
        add_to_custom_node_store(WrongNoInputNode)

        graph = create_graph()

        add_custom_node_to_graph(graph, WrongNoInputNode, node_id=1, settings={})

        run_result = graph.run_graph()

        # The run should fail because process() tries to access inputs[0]
        # but no inputs are provided to a source node
        assert not run_result.success, "Node should fail when accessing non-existent inputs"

        # Verify the error is related to the index/input access
        node_errors = [
            step for step in run_result.node_step_result
            if not step.success and step.node_id == 1
        ]
        assert len(node_errors) > 0, "Should have an error for node 1"

    def test_no_input_node_error_message_is_descriptive(self, WrongNoInputNode):
        """Test that the error from misconfigured node is descriptive."""
        add_to_custom_node_store(WrongNoInputNode)

        graph = create_graph()

        add_custom_node_to_graph(graph, WrongNoInputNode, node_id=1, settings={})

        run_result = graph.run_graph()

        assert not run_result.success

        # Find the error message
        failed_step = next(
            (step for step in run_result.node_step_result if not step.success),
            None
        )
        assert failed_step is not None
        assert failed_step.error is not None
        # Should mention index error or tuple index out of range
        assert "index" in failed_step.error.lower() or "IndexError" in failed_step.error

    def test_no_input_node_does_not_block_other_nodes(self, WrongNoInputNode, AddFixedColumnNode):
        """Test that a failing source node doesn't prevent independent nodes from running."""
        add_to_custom_node_store(WrongNoInputNode)
        add_to_custom_node_store(AddFixedColumnNode)

        graph = create_graph()

        # Add the broken source node
        add_custom_node_to_graph(graph, WrongNoInputNode, node_id=1, settings={})

        # Add an independent working branch
        add_manual_input(graph, [{"a": 1}], node_id=2)
        add_custom_node_to_graph(graph, AddFixedColumnNode, node_id=3, settings={
            "config": {"column_name": "new", "fixed_value": "works"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

        run_result = graph.run_graph()

        # Node 1 should fail
        node_1_result = next(
            (step for step in run_result.node_step_result if step.node_id == 1),
            None
        )
        assert node_1_result is not None
        assert not node_1_result.success

        # Node 3 (independent branch) should succeed
        node_3_result = next(
            (step for step in run_result.node_step_result if step.node_id == 3),
            None
        )
        assert node_3_result is not None
        assert node_3_result.success

        # Verify node 3 has correct data
        result = graph.get_node(3).get_resulting_data()
        assert result.to_dict()["new"] == ["works"]

    def test_no_input_node_blocks_dependent_nodes(self, WrongNoInputNode, NumericMultiplierNode):
        """Test that nodes depending on a failing source node also fail."""
        add_to_custom_node_store(WrongNoInputNode)
        add_to_custom_node_store(NumericMultiplierNode)

        graph = create_graph()

        # Broken source node
        add_custom_node_to_graph(graph, WrongNoInputNode, node_id=1, settings={})

        # Dependent transform node
        add_custom_node_to_graph(graph, NumericMultiplierNode, node_id=2, settings={
            "config": {"column": "value", "factor": 2.0, "output_column": "doubled"}
        })
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()

        # Both nodes should fail (node 1 directly, node 2 because of dependency)
        assert not run_result.success

        node_1_result = next(
            (step for step in run_result.node_step_result if step.node_id == 1),
            None
        )
        assert not node_1_result.success

    def test_empty_settings_node_registration(self, WrongNoInputNode):
        """Test that a node with empty settings can still be registered."""
        add_to_custom_node_store(WrongNoInputNode)

        instance = WrongNoInputNode()
        assert instance.item in CUSTOM_NODE_STORE
        assert instance.number_of_inputs == 0
        assert instance.number_of_outputs == 1

    def test_empty_settings_schema_structure(self, WrongNoInputNode):
        """Test the frontend schema of a node with no settings."""
        instance = WrongNoInputNode()
        schema = instance.get_frontend_schema()

        assert "settings_schema" in schema
        # Empty settings should still have the structure, just no sections
        assert schema["settings_schema"] == {} or isinstance(schema["settings_schema"], dict)


class TestCorrectNoInputNode:
    """Tests for properly implemented source nodes (for comparison)."""

    @pytest.fixture
    def CorrectNoInputNode(self):
        """A correctly implemented source node that doesn't access inputs."""

        class CorrectSourceSettings(NodeSettings):
            config: Section = Section(
                title="Configuration",
                value=TextInput(label="Output Value", default="generated")
            )

        class CorrectSource(CustomNodeBase):
            node_name: str = "correct_source_node"
            node_category: str = "Custom"
            title: str = "Correct Source Node"
            intro: str = "A properly implemented source node"
            number_of_inputs: int = 0
            number_of_outputs: int = 1
            settings_schema: CorrectSourceSettings = CorrectSourceSettings()

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                # Correctly ignores inputs since number_of_inputs is 0
                value = self.settings_schema.config.value.value
                return pl.LazyFrame({"output": [value]})

        return CorrectSource

    def test_correct_no_input_node_succeeds(self, CorrectNoInputNode):
        """Test that a properly implemented source node works."""
        add_to_custom_node_store(CorrectNoInputNode)

        graph = create_graph()

        add_custom_node_to_graph(graph, CorrectNoInputNode, node_id=1, settings={
            "config": {"value": "hello"}
        })

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(1).get_resulting_data()
        assert result.to_dict()["output"] == ["hello"]

    def test_comparison_wrong_vs_correct_source_node(self, WrongNoInputNode, CorrectNoInputNode):
        """Compare behavior of wrong vs correct source node implementations."""
        add_to_custom_node_store(WrongNoInputNode)
        add_to_custom_node_store(CorrectNoInputNode)

        # Test wrong node
        graph1 = create_graph(flow_id=1)
        add_custom_node_to_graph(graph1, WrongNoInputNode, node_id=1, settings={})
        wrong_result = graph1.run_graph()

        # Test correct node
        graph2 = create_graph(flow_id=2)
        add_custom_node_to_graph(graph2, CorrectNoInputNode, node_id=1, settings={
            "config": {"value": "test"}
        })
        correct_result = graph2.run_graph()
        assert not wrong_result.success, "Wrong implementation should fail"
        assert correct_result.success, "Correct implementation should succeed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])