"""
Tests for the OutputFieldConfig feature.

Run with:
    pytest flowfile_core/tests/flowfile/flow_node/test_output_field_config.py -v

Tests cover:
- Helper functions (_parse_default_value, _select_columns_in_order, etc.)
- OutputFieldConfig model creation
- Validation mode behaviors (select_only, add_missing, raise_on_missing)
- Data type validation
- Integration with flow nodes
- Error handling for missing/extra columns
"""
import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.output_field_config_applier import (
    _apply_add_missing,
    _apply_raise_on_missing,
    _parse_default_value,
    _select_columns_in_order,
    _validate_data_types,
    apply_output_field_config,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine


# =============================================================================
# Test Fixtures
# =============================================================================


def create_graph(flow_id: int = 1) -> FlowGraph:
    """Create a new FlowGraph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="test_flow", path=".", execution_mode="Development", execution_location="local"
        )
    )
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: dict, node_id: int = 1):
    """Add a manual input node with data."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"
    )
    graph.add_node_promise(node_promise)

    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        is_setup=True,
        raw_data_format=input_schema.RawData.from_pydict(data),
        cache_results=False,
    )
    graph.add_manual_input(input_file)
    return graph


# =============================================================================
# Unit Tests: Helper Functions
# =============================================================================


class TestParseDefaultValue:
    """Tests for _parse_default_value function."""

    def test_none_default(self):
        """Test parsing None default value."""
        field = input_schema.OutputFieldInfo(name="test", data_type="String", default_value=None)
        expr = _parse_default_value(field)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.with_columns(expr.alias("test"))
        assert result["test"].to_list() == [None, None]

    def test_literal_string(self):
        """Test parsing literal string default value."""
        field = input_schema.OutputFieldInfo(name="test", data_type="String", default_value="hello")
        expr = _parse_default_value(field)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.with_columns(expr.alias("test"))
        assert result["test"].to_list() == ["hello", "hello"]

    def test_literal_number(self):
        """Test parsing literal number default value."""
        field = input_schema.OutputFieldInfo(name="test", data_type="Int64", default_value="42")
        expr = _parse_default_value(field)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.with_columns(expr.alias("test"))
        assert result["test"].to_list() == [42, 42]


class TestSelectColumnsInOrder:
    """Tests for _select_columns_in_order function."""

    def test_reorder_columns(self):
        """Test selecting columns in specified order."""
        df = pl.DataFrame({"c": [7, 8], "a": [1, 2], "b": [4, 5]})
        fields = [
            input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value=None),
        ]

        result = _select_columns_in_order(df, fields)
        assert result.columns == ["b", "a", "c"]


class TestApplyRaiseOnMissing:
    """Tests for _apply_raise_on_missing function."""

    def test_success_all_columns_present(self):
        """Test success when all required columns are present."""
        engine = FlowDataEngine(pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]}))
        fields = [
            input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
        ]
        result = _apply_raise_on_missing(engine, fields)
        assert result.columns == ["b", "a"]

    def test_error_when_columns_missing(self):
        """Test raises ValueError when required columns are missing."""
        df = FlowDataEngine(pl.DataFrame({"a": [1, 2]}))
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
        ]
        with pytest.raises(ValueError, match="Missing required columns"):
            _apply_raise_on_missing(df, fields)


class TestApplyAddMissing:
    """Tests for _apply_add_missing function."""

    def test_add_columns_with_defaults(self):
        """Test adding missing columns with default values."""
        df = FlowDataEngine({"a": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value="default"),
            input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value="99"),
        ]
        result = _apply_add_missing(df, fields)
        assert result.columns == ["a", "b", "c"]
        assert result.to_dict()["b"] == ["default", "default"]
        assert result.to_dict()["c"] == [99, 99]

    def test_add_columns_with_null_defaults(self):
        """Test adding missing columns with null defaults when no default specified."""
        df = FlowDataEngine(pl.LazyFrame({"a": [1, 2]}))
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
        ]

        result = _apply_add_missing(df, fields)
        assert result.columns == ["a", "b"]
        assert result.to_dict()["b"] == [None, None]


class TestValidateDataTypes:
    """Tests for _validate_data_types function."""

    def test_validation_passes_when_types_match(self):
        """Test validation passes when data types match expected types."""
        df = FlowDataEngine({"a": [1, 2], "b": ["x", "y"]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
        ]

        # Should not raise
        _validate_data_types(df, fields)

    def test_validation_fails_on_type_mismatch(self):
        """Test validation raises ValueError when types don't match."""
        df = FlowDataEngine({"a": ["not", "int"], "b": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
        ]

        with pytest.raises(ValueError, match="Data type validation failed"):
            _validate_data_types(df, fields)

    def test_skip_missing_columns(self):
        """Test validation skips columns not present in dataframe."""
        df = FlowDataEngine({"a": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),  # Not in df
        ]
        _validate_data_types(df, fields)


# =============================================================================
# Integration Tests: Validation Mode Behaviors
# =============================================================================


class TestSelectOnlyMode:
    """Tests for select_only validation mode."""

    def test_basic_column_selection(self):
        """Test select_only mode keeps only specified columns in order."""
        df = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            ],
            validate_data_types=True,
        )
        engine = FlowDataEngine(raw_data=df)
        result_engine = apply_output_field_config(engine, config)
        # Should only have columns c and a in that order
        assert result_engine.data_frame.columns == ["c", "a"]
        result_engine.assert_equal(FlowDataEngine(
            input_schema.RawData(columns=[input_schema.MinimalFieldInfo(name='c', data_type='Int64'),
                                          input_schema.MinimalFieldInfo(name='a', data_type='Int64')],
                                 data=[[7, 8, 9], [1, 2, 3]])))


    def test_missing_column_silently_skipped(self):
        """Test select_only mode silently skips missing columns."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value=None),  # Missing
            ],
            validate_data_types=False,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(df)
        result_engine = apply_output_field_config(engine, config)

        # Should only have column a (c is missing and skipped)
        assert result_engine.data_frame.columns == ["a"]


class TestAddMissingMode:
    """Tests for add_missing validation mode."""

    def test_add_missing_with_defaults(self):
        """Test add_missing mode adds columns with default values."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="add_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="String", default_value="default"),
                input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value="0"),
            ],
            validate_data_types=False,
        )
        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(df)
        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b", "c"]
        assert result_engine.to_dict()["a"] == [1, 2, 3]
        assert result_engine.to_dict()["b"] == ["default", "default", "default"]
        assert result_engine.to_dict()["c"] == [0, 0, 0]

    def test_add_missing_with_null_defaults(self):
        """Test add_missing mode adds null columns when default_value is null."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="add_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
            ],
            validate_data_types=False,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(df)
        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b"]
        assert result_engine.to_dict()["b"] == [None, None, None]

    def test_add_missing_removes_extra_columns(self):
        """Test add_missing mode removes columns not in config."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="add_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="d", data_type="Int64", default_value="99"),
            ],
            validate_data_types=False,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(df)
        result_engine = apply_output_field_config(engine, config)

        # Should only have a and d, not b or c
        assert result_engine.columns == ["a", "d"]
        assert result_engine.to_dict()["d"] == [99, 99, 99]


class TestRaiseOnMissingMode:
    """Tests for raise_on_missing validation mode."""

    def test_success_when_all_columns_present(self):
        """Test raise_on_missing mode succeeds when all columns present."""
        engine = FlowDataEngine({"a": [1, 2, 3], "b": [4, 5, 6]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="raise_on_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )
        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b"]

    def test_error_when_column_missing(self):
        """Test raise_on_missing mode raises error when column missing."""
        engine = FlowDataEngine({"a": [1, 2, 3]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="raise_on_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            apply_output_field_config(engine, config)


class TestDataTypeValidation:
    """Tests for data type validation feature."""

    def test_validation_passes_when_types_match(self):
        """Test data type validation passes when types match."""
        engine = FlowDataEngine({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
            ],
            validate_data_types=True,
        )

        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b"]

    def test_validation_raises_error_on_mismatch(self):
        """Test data type validation raises error when types don't match."""
        engine = FlowDataEngine({"a": ["not", "an", "int"], "b": [1, 2, 3]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
            ],
            validate_data_types=True,
        )

        with pytest.raises(ValueError, match="Data type validation failed"):
            apply_output_field_config(engine, config)

    def test_validation_skipped_when_disabled(self):
        """Test that validation is skipped when validate_data_types is False."""
        engine = FlowDataEngine({"a": ["not", "an", "int"]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        # Should not raise error even though type doesn't match
        result_engine = apply_output_field_config(engine, config)
        assert result_engine.data_frame.columns == ["a"]


class TestFlowIntegration:
    """Tests for output_field_config integration with flow nodes."""

    def test_polars_code_node_integration(self):
        """Test output_field_config integration with PolarsCode node."""
        graph = create_graph()
        breakpoint()
        # Add manual input
        data = {"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]}
        add_manual_input(graph, data, node_id=1)

        # Add PolarsCode node promise
        polars_code_promise = input_schema.NodePromise(
            flow_id=graph.flow_id, node_id=2, node_type="polars_code"
        )
        graph.add_node_promise(polars_code_promise)

        # Create connection
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        # Configure output field config
        output_config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="y", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="x", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        # Add PolarsCode node with settings
        polars_code = input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            is_setup=True,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="output_df = input_df"),
            cache_results=False,
            output_field_config=output_config,
        )
        graph.add_polars_code(polars_code)

        # Run the flow
        run_info = graph.run_graph()

        # Check that output has correct columns in correct order
        output = graph.get_node(2).get_resulting_data()
        expected = FlowDataEngine({'y': [4, 5, 6], 'x': [1, 2, 3]})
        assert expected.columns == ["y", "x"]
        output.assert_equal(expected)

    def test_add_missing_mode_in_flow(self):
        """Test output_field_config add_missing mode in flow."""
        graph = create_graph()

        # Add manual input with only some columns
        data = {"a": [1, 2, 3]}
        add_manual_input(graph, data, node_id=1)
        breakpoint()
        # Add PolarsCode node promise
        polars_code_promise = input_schema.NodePromise(
            flow_id=graph.flow_id, node_id=2, node_type="polars_code"
        )
        graph.add_node_promise(polars_code_promise)

        # Create connection
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        # Configure output to add missing columns
        output_config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="add_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="String", default_value="new"),
                input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value="100"),
            ],
            validate_data_types=False,
        )

        # Add PolarsCode node
        polars_code = input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            is_setup=True,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="input_df"),
            cache_results=False,
            output_field_config=output_config,
        )
        graph.add_polars_code(polars_code)

        # Run the flow
        run_info = graph.run_graph()

        # Check that missing columns were added with defaults
        output = graph.get_node(2).get_resulting_data()
        expected = FlowDataEngine({
            'a': [1, 2, 3],
            'b': ['new', 'new', 'new'],
            'c': [100, 100, 100]
        })
        assert output.columns == ["a", "b", "c"]
        output.assert_equal(expected)


if __name__ == "__main__":
    pytest.main([__file__])
