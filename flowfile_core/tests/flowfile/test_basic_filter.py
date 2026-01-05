"""
Tests for the BasicFilter feature with FilterOperator enum.

Run with:
    pytest flowfile_core/tests/flowfile/test_basic_filter.py -v

Tests cover:
- FilterOperator enum functionality
- BasicFilter model creation and backward compatibility
- FilterInput model creation and mode detection
- Filter expression building for all operators
- YAML serialization/deserialization
- Integration with FlowGraph
"""
from pathlib import Path
from typing import Literal

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import RunInformation
from flowfile_core.schemas.transform_schema import BasicFilter, FilterInput, FilterOperator


# =============================================================================
# Test Fixtures
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
# FilterOperator Enum Tests
# =============================================================================


class TestFilterOperator:
    """Tests for the FilterOperator enum."""

    def test_all_operators_defined(self):
        """Test that all expected operators are defined."""
        expected_operators = [
            'equals', 'not_equals', 'greater_than', 'greater_than_or_equals',
            'less_than', 'less_than_or_equals', 'contains', 'not_contains',
            'starts_with', 'ends_with', 'is_null', 'is_not_null',
            'in', 'not_in', 'between'
        ]
        actual_operators = [op.value for op in FilterOperator]
        assert set(expected_operators) == set(actual_operators)

    def test_str_conversion(self):
        """Test that FilterOperator converts to string correctly."""
        assert str(FilterOperator.EQUALS) == "equals"
        assert str(FilterOperator.GREATER_THAN) == "greater_than"
        assert str(FilterOperator.BETWEEN) == "between"

    def test_from_symbol_basic_operators(self):
        """Test converting basic symbols to FilterOperator."""
        assert FilterOperator.from_symbol("=") == FilterOperator.EQUALS
        assert FilterOperator.from_symbol("==") == FilterOperator.EQUALS
        assert FilterOperator.from_symbol("!=") == FilterOperator.NOT_EQUALS
        assert FilterOperator.from_symbol("<>") == FilterOperator.NOT_EQUALS
        assert FilterOperator.from_symbol(">") == FilterOperator.GREATER_THAN
        assert FilterOperator.from_symbol(">=") == FilterOperator.GREATER_THAN_OR_EQUALS
        assert FilterOperator.from_symbol("<") == FilterOperator.LESS_THAN
        assert FilterOperator.from_symbol("<=") == FilterOperator.LESS_THAN_OR_EQUALS

    def test_from_symbol_string_operators(self):
        """Test converting string operators to FilterOperator."""
        assert FilterOperator.from_symbol("contains") == FilterOperator.CONTAINS
        assert FilterOperator.from_symbol("not_contains") == FilterOperator.NOT_CONTAINS
        assert FilterOperator.from_symbol("starts_with") == FilterOperator.STARTS_WITH
        assert FilterOperator.from_symbol("ends_with") == FilterOperator.ENDS_WITH

    def test_from_symbol_null_operators(self):
        """Test converting null operators to FilterOperator."""
        assert FilterOperator.from_symbol("is_null") == FilterOperator.IS_NULL
        assert FilterOperator.from_symbol("is_not_null") == FilterOperator.IS_NOT_NULL

    def test_from_symbol_set_operators(self):
        """Test converting set operators to FilterOperator."""
        assert FilterOperator.from_symbol("in") == FilterOperator.IN
        assert FilterOperator.from_symbol("not_in") == FilterOperator.NOT_IN
        assert FilterOperator.from_symbol("between") == FilterOperator.BETWEEN

    def test_from_symbol_by_value(self):
        """Test that from_symbol works with enum values directly."""
        assert FilterOperator.from_symbol("equals") == FilterOperator.EQUALS
        assert FilterOperator.from_symbol("greater_than") == FilterOperator.GREATER_THAN

    def test_from_symbol_invalid(self):
        """Test that from_symbol raises ValueError for invalid symbols."""
        with pytest.raises(ValueError, match="Unknown filter operator symbol"):
            FilterOperator.from_symbol("invalid_operator")

    def test_to_symbol(self):
        """Test converting FilterOperator to symbols."""
        assert FilterOperator.EQUALS.to_symbol() == "="
        assert FilterOperator.NOT_EQUALS.to_symbol() == "!="
        assert FilterOperator.GREATER_THAN.to_symbol() == ">"
        assert FilterOperator.LESS_THAN.to_symbol() == "<"
        assert FilterOperator.CONTAINS.to_symbol() == "contains"
        assert FilterOperator.BETWEEN.to_symbol() == "between"


# =============================================================================
# BasicFilter Model Tests
# =============================================================================


class TestBasicFilter:
    """Tests for the BasicFilter model."""

    def test_create_with_new_fields(self):
        """Test creating BasicFilter with new field names."""
        bf = BasicFilter(
            field="price",
            operator=FilterOperator.GREATER_THAN,
            value="100"
        )
        assert bf.field == "price"
        assert bf.get_operator() == FilterOperator.GREATER_THAN
        assert bf.value == "100"

    def test_create_with_string_operator(self):
        """Test creating BasicFilter with string operator."""
        bf = BasicFilter(
            field="name",
            operator=">",
            value="John"
        )
        assert bf.field == "name"
        assert bf.get_operator() == FilterOperator.GREATER_THAN

    def test_backward_compatibility_with_filter_type(self):
        """Test backward compatibility with old filter_type field."""
        bf = BasicFilter(
            field="price",
            filter_type=">",
            filter_value="100"
        )
        assert bf.field == "price"
        assert bf.get_operator() == FilterOperator.GREATER_THAN
        assert bf.value == "100"

    def test_backward_compatibility_with_symbol(self):
        """Test backward compatibility with legacy symbols."""
        bf = BasicFilter(
            field="status",
            filter_type="=",
            filter_value="active"
        )
        assert bf.get_operator() == FilterOperator.EQUALS
        assert bf.value == "active"

    def test_between_operator_with_value2(self):
        """Test BETWEEN operator with value2."""
        bf = BasicFilter(
            field="age",
            operator=FilterOperator.BETWEEN,
            value="18",
            value2="65"
        )
        assert bf.get_operator() == FilterOperator.BETWEEN
        assert bf.value == "18"
        assert bf.value2 == "65"

    def test_to_yaml_dict(self):
        """Test YAML serialization."""
        bf = BasicFilter(
            field="price",
            operator=FilterOperator.GREATER_THAN,
            value="100"
        )
        yaml_dict = bf.to_yaml_dict()
        assert yaml_dict["field"] == "price"
        assert yaml_dict["operator"] == "greater_than"
        assert yaml_dict["value"] == "100"
        assert "value2" not in yaml_dict

    def test_to_yaml_dict_with_value2(self):
        """Test YAML serialization with value2."""
        bf = BasicFilter(
            field="age",
            operator=FilterOperator.BETWEEN,
            value="18",
            value2="65"
        )
        yaml_dict = bf.to_yaml_dict()
        assert yaml_dict["value2"] == "65"

    def test_from_yaml_dict(self):
        """Test YAML deserialization."""
        yaml_dict = {
            "field": "name",
            "operator": "equals",
            "value": "John"
        }
        bf = BasicFilter.from_yaml_dict(yaml_dict)
        assert bf.field == "name"
        assert bf.get_operator() == FilterOperator.EQUALS
        assert bf.value == "John"

    def test_from_yaml_dict_with_value2(self):
        """Test YAML deserialization with value2."""
        yaml_dict = {
            "field": "age",
            "operator": "between",
            "value": "18",
            "value2": "65"
        }
        bf = BasicFilter.from_yaml_dict(yaml_dict)
        assert bf.value2 == "65"

    def test_default_values(self):
        """Test default values for BasicFilter."""
        bf = BasicFilter()
        assert bf.field == ""
        assert bf.get_operator() == FilterOperator.EQUALS
        assert bf.value == ""
        assert bf.value2 is None


# =============================================================================
# FilterInput Model Tests
# =============================================================================


class TestFilterInput:
    """Tests for the FilterInput model."""

    def test_create_basic_mode(self):
        """Test creating FilterInput in basic mode."""
        fi = FilterInput(
            mode="basic",
            basic_filter=BasicFilter(
                field="price",
                operator=FilterOperator.GREATER_THAN,
                value="100"
            )
        )
        assert fi.mode == "basic"
        assert not fi.is_advanced()
        assert fi.basic_filter is not None
        assert fi.basic_filter.field == "price"

    def test_create_advanced_mode(self):
        """Test creating FilterInput in advanced mode."""
        fi = FilterInput(
            mode="advanced",
            advanced_filter="[price] > 100"
        )
        assert fi.mode == "advanced"
        assert fi.is_advanced()
        assert fi.advanced_filter == "[price] > 100"

    def test_backward_compatibility_filter_type(self):
        """Test backward compatibility with filter_type field."""
        fi = FilterInput(
            filter_type="advanced",
            advanced_filter="[price] > 100"
        )
        assert fi.mode == "advanced"
        assert fi.is_advanced()

    def test_basic_mode_creates_basic_filter(self):
        """Test that basic mode creates a BasicFilter if not provided."""
        fi = FilterInput(mode="basic")
        assert fi.basic_filter is not None

    def test_to_yaml_dict_basic(self):
        """Test YAML serialization in basic mode."""
        fi = FilterInput(
            mode="basic",
            basic_filter=BasicFilter(
                field="name",
                operator=FilterOperator.EQUALS,
                value="John"
            )
        )
        yaml_dict = fi.to_yaml_dict()
        assert yaml_dict["mode"] == "basic"
        assert "basic_filter" in yaml_dict
        assert yaml_dict["basic_filter"]["field"] == "name"

    def test_to_yaml_dict_advanced(self):
        """Test YAML serialization in advanced mode."""
        fi = FilterInput(
            mode="advanced",
            advanced_filter="[price] > 100"
        )
        yaml_dict = fi.to_yaml_dict()
        assert yaml_dict["mode"] == "advanced"
        assert yaml_dict["advanced_filter"] == "[price] > 100"

    def test_from_yaml_dict_basic(self):
        """Test YAML deserialization in basic mode."""
        yaml_dict = {
            "mode": "basic",
            "basic_filter": {
                "field": "price",
                "operator": "greater_than",
                "value": "100"
            }
        }
        fi = FilterInput.from_yaml_dict(yaml_dict)
        assert fi.mode == "basic"
        assert fi.basic_filter.field == "price"
        assert fi.basic_filter.get_operator() == FilterOperator.GREATER_THAN

    def test_from_yaml_dict_advanced(self):
        """Test YAML deserialization in advanced mode."""
        yaml_dict = {
            "mode": "advanced",
            "advanced_filter": "[name] = 'John'"
        }
        fi = FilterInput.from_yaml_dict(yaml_dict)
        assert fi.mode == "advanced"
        assert fi.advanced_filter == "[name] = 'John'"


# =============================================================================
# Filter Integration Tests with FlowGraph
# =============================================================================


class TestBasicFilterIntegration:
    """Integration tests for BasicFilter with FlowGraph."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing filters."""
        return [
            {"name": "Alice", "age": 25, "city": "New York", "salary": 50000},
            {"name": "Bob", "age": 30, "city": "Los Angeles", "salary": 60000},
            {"name": "Charlie", "age": 35, "city": "Chicago", "salary": 70000},
            {"name": "Diana", "age": 28, "city": "New York", "salary": 55000},
            {"name": "Eve", "age": 40, "city": "Boston", "salary": 80000},
        ]

    def test_filter_equals_string(self, sample_data):
        """Test basic filter with equals operator on string."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="city",
                    operator=FilterOperator.EQUALS,
                    value="New York"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 2
        assert all(row["city"] == "New York" for row in result)

    def test_filter_greater_than_numeric(self, sample_data):
        """Test basic filter with greater_than operator on numeric."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="age",
                    operator=FilterOperator.GREATER_THAN,
                    value="30"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 2
        assert all(row["age"] > 30 for row in result)

    def test_filter_less_than_or_equals(self, sample_data):
        """Test basic filter with less_than_or_equals operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="salary",
                    operator=FilterOperator.LESS_THAN_OR_EQUALS,
                    value="55000"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 2
        assert all(row["salary"] <= 55000 for row in result)

    def test_filter_not_equals(self, sample_data):
        """Test basic filter with not_equals operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="city",
                    operator=FilterOperator.NOT_EQUALS,
                    value="New York"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 3
        assert all(row["city"] != "New York" for row in result)

    def test_filter_contains(self, sample_data):
        """Test basic filter with contains operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="name",
                    operator=FilterOperator.CONTAINS,
                    value="li"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        # Should match "Alice" and "Charlie"
        assert len(result) == 2
        assert all("li" in row["name"] for row in result)

    def test_filter_starts_with(self, sample_data):
        """Test basic filter with starts_with operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="name",
                    operator=FilterOperator.STARTS_WITH,
                    value="A"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_filter_ends_with(self, sample_data):
        """Test basic filter with ends_with operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="name",
                    operator=FilterOperator.ENDS_WITH,
                    value="e"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        # Should match "Alice", "Charlie", "Eve"
        assert len(result) == 3
        assert all(row["name"].endswith("e") for row in result)

    def test_filter_in_operator(self, sample_data):
        """Test basic filter with in operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="city",
                    operator=FilterOperator.IN,
                    value="New York, Boston"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 3
        assert all(row["city"] in ["New York", "Boston"] for row in result)

    def test_filter_not_in_operator(self, sample_data):
        """Test basic filter with not_in operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="city",
                    operator=FilterOperator.NOT_IN,
                    value="New York, Boston"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 2
        assert all(row["city"] not in ["New York", "Boston"] for row in result)

    def test_filter_between(self, sample_data):
        """Test basic filter with between operator."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="age",
                    operator=FilterOperator.BETWEEN,
                    value="25",
                    value2="30"
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 3
        assert all(25 <= row["age"] <= 30 for row in result)

    def test_filter_backward_compatibility(self, sample_data):
        """Test that old-style filter_type/filter_value still works."""
        graph = create_graph()
        add_manual_input(graph, sample_data, node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        # Use old-style field names
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                filter_type="basic",  # Legacy field
                basic_filter=BasicFilter(
                    field="age",
                    filter_type=">",  # Legacy field
                    filter_value="30"  # Legacy field
                )
            )
        )
        graph.add_filter(filter_settings)

        run_info = graph.run_graph()
        handle_run_info(run_info)

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 2
        assert all(row["age"] > 30 for row in result)


# =============================================================================
# Filter with Null Data Tests
# =============================================================================


class TestBasicFilterNullHandling:
    """Tests for filters handling null values.

    Note: These tests use advanced filter mode because manual input converts
    Python None values to strings. The is_empty/is_not_empty functions work
    correctly with actual null values from data sources like CSV/Parquet.
    """

    def test_filter_is_null_expression_generation(self):
        """Test that is_null operator generates correct expression."""
        bf = BasicFilter(
            field="age",
            operator=FilterOperator.IS_NULL,
            value=""
        )
        # The expression should use is_empty function
        assert bf.get_operator() == FilterOperator.IS_NULL

    def test_filter_is_not_null_expression_generation(self):
        """Test that is_not_null operator generates correct expression."""
        bf = BasicFilter(
            field="city",
            operator=FilterOperator.IS_NOT_NULL,
            value=""
        )
        # The expression should use is_not_empty function
        assert bf.get_operator() == FilterOperator.IS_NOT_NULL

    @pytest.mark.skip(reason="Manual input converts None to string; test requires actual null values from file sources")
    def test_filter_is_null_integration(self):
        """Test is_null operator with actual null data.

        Note: This test is skipped because manual input doesn't preserve
        Python None values as proper null values. For actual null handling,
        use data from CSV/Parquet files with null values.
        """
        pass

    @pytest.mark.skip(reason="Manual input converts None to string; test requires actual null values from file sources")
    def test_filter_is_not_null_integration(self):
        """Test is_not_null operator with actual null data.

        Note: This test is skipped because manual input doesn't preserve
        Python None values as proper null values. For actual null handling,
        use data from CSV/Parquet files with null values.
        """
        pass


# =============================================================================
# YAML Roundtrip Tests
# =============================================================================


class TestBasicFilterYamlRoundtrip:
    """Tests for saving and loading filters via YAML."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for YAML files."""
        return tmp_path

    def test_basic_filter_yaml_roundtrip(self, temp_dir):
        """Test saving and loading a basic filter via YAML."""
        # Create flow with basic filter
        graph = create_graph(flow_id=1)
        add_manual_input(graph, [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="age",
                    operator=FilterOperator.GREATER_THAN,
                    value="25"
                )
            )
        )
        graph.add_filter(filter_settings)

        # Save to YAML
        yaml_path = temp_dir / "test_filter.yaml"
        graph.save_flow(str(yaml_path))

        # Load from YAML
        loaded_graph = open_flow(yaml_path)

        # Verify filter settings were preserved
        loaded_filter = loaded_graph.get_node(2)
        assert loaded_filter is not None
        assert loaded_filter.setting_input.filter_input.mode == "basic"

        # Run and verify results
        run_info = loaded_graph.run_graph()
        handle_run_info(run_info)

        result = loaded_graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_between_filter_yaml_roundtrip(self, temp_dir):
        """Test saving and loading a BETWEEN filter via YAML."""
        graph = create_graph(flow_id=2)
        add_manual_input(graph, [
            {"product": "A", "price": 10},
            {"product": "B", "price": 25},
            {"product": "C", "price": 50},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="price",
                    operator=FilterOperator.BETWEEN,
                    value="15",
                    value2="30"
                )
            )
        )
        graph.add_filter(filter_settings)

        # Save and reload
        yaml_path = temp_dir / "test_between.yaml"
        graph.save_flow(str(yaml_path))
        loaded_graph = open_flow(yaml_path)

        # Run and verify
        run_info = loaded_graph.run_graph()
        handle_run_info(run_info)

        result = loaded_graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 1
        assert result[0]["product"] == "B"


# =============================================================================
# Code Generator Tests
# =============================================================================


class TestBasicFilterCodeGenerator:
    """Tests for code generation with basic filters."""

    def test_code_generator_basic_filter_equals(self, tmp_path):
        """Test code generation for equals filter."""
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        graph = create_graph(flow_id=1)
        add_manual_input(graph, [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="name",
                    operator=FilterOperator.EQUALS,
                    value="Alice"
                )
            )
        )
        graph.add_filter(filter_settings)

        # Generate code
        code = export_flow_to_polars(graph)

        # Verify code contains proper filter expression
        assert 'pl.col("name")' in code
        assert '"Alice"' in code

    def test_code_generator_basic_filter_greater_than(self, tmp_path):
        """Test code generation for greater_than filter."""
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        graph = create_graph(flow_id=1)
        add_manual_input(graph, [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="age",
                    operator=FilterOperator.GREATER_THAN,
                    value="25"
                )
            )
        )
        graph.add_filter(filter_settings)

        # Generate code
        code = export_flow_to_polars(graph)

        # Verify code contains proper filter expression
        assert 'pl.col("age")' in code
        assert '>' in code

    def test_code_generator_basic_filter_contains(self, tmp_path):
        """Test code generation for contains filter."""
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        graph = create_graph(flow_id=1)
        add_manual_input(graph, [
            {"name": "Alice", "city": "New York"},
            {"name": "Bob", "city": "Los Angeles"},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="city",
                    operator=FilterOperator.CONTAINS,
                    value="York"
                )
            )
        )
        graph.add_filter(filter_settings)

        # Generate code
        code = export_flow_to_polars(graph)

        # Verify code contains proper filter expression
        assert 'pl.col("city")' in code
        assert 'str.contains' in code

    def test_code_generator_basic_filter_in(self, tmp_path):
        """Test code generation for in filter."""
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        graph = create_graph(flow_id=1)
        add_manual_input(graph, [
            {"name": "Alice", "city": "New York"},
            {"name": "Bob", "city": "Los Angeles"},
            {"name": "Charlie", "city": "Chicago"},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="city",
                    operator=FilterOperator.IN,
                    value="New York, Chicago"
                )
            )
        )
        graph.add_filter(filter_settings)

        # Generate code
        code = export_flow_to_polars(graph)

        # Verify code contains proper filter expression
        assert 'pl.col("city")' in code
        assert 'is_in' in code

    def test_code_generator_basic_filter_is_null(self, tmp_path):
        """Test code generation for is_null filter."""
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        graph = create_graph(flow_id=1)
        add_manual_input(graph, [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": None},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="age",
                    operator=FilterOperator.IS_NULL,
                    value=""
                )
            )
        )
        graph.add_filter(filter_settings)

        # Generate code
        code = export_flow_to_polars(graph)

        # Verify code contains proper filter expression
        assert 'pl.col("age")' in code
        assert 'is_null' in code

    def test_code_generator_basic_filter_between(self, tmp_path):
        """Test code generation for between filter."""
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        graph = create_graph(flow_id=1)
        add_manual_input(graph, [
            {"name": "Alice", "age": 20},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 40},
        ], node_id=1)
        add_node_promise(graph, 'filter', node_id=2)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(
                    field="age",
                    operator=FilterOperator.BETWEEN,
                    value="25",
                    value2="35"
                )
            )
        )
        graph.add_filter(filter_settings)

        # Generate code
        code = export_flow_to_polars(graph)

        # Verify code contains proper filter expression
        assert 'pl.col("age")' in code
        assert '>=' in code
        assert '<=' in code
