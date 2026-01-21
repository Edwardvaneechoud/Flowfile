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

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.output_field_config_applier import (
    _apply_add_missing,
    _apply_raise_on_missing,
    _apply_select_only,
    _parse_default_value,
    _select_columns_in_order,
    _validate_data_types,
    apply_output_field_config,
    polars_dtype_to_data_type_str,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


# =============================================================================
# Test Fixtures
# =============================================================================


def create_graph(flow_id: int = 1) -> FlowGraph:
    """Create a new FlowGraph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="test_flow", path=".", execution_mode="Development"
        )
    )
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: dict, node_id: int = 1):
    """Add a manual input node with data."""
    df = pl.DataFrame(data)
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"
    )
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type="manual_input",
        is_setup=True,
        raw_data=data,
        cache_results=False,
    )
    graph.update_node_settings(node_promise, input_file)
    return df


# =============================================================================
# Unit Tests: Helper Functions
# =============================================================================


class TestPolarsTypeConversion:
    """Tests for polars_dtype_to_data_type_str function."""

    def test_string_types(self):
        """Test conversion of string types."""
        # Test with actual dtype objects from a dataframe
        df = pl.DataFrame({"a": ["x"], "b": ["y"]})
        assert polars_dtype_to_data_type_str(df["a"].dtype) == "String"

    def test_integer_types(self):
        """Test conversion of integer types."""
        df = pl.DataFrame({"a": [1], "b": [2]})
        assert polars_dtype_to_data_type_str(df["a"].dtype) == "Int64"

        df32 = pl.DataFrame({"a": pl.Series([1], dtype=pl.Int32)})
        assert polars_dtype_to_data_type_str(df32["a"].dtype) == "Int32"

    def test_float_types(self):
        """Test conversion of float types."""
        df = pl.DataFrame({"a": [1.0], "b": [2.0]})
        assert polars_dtype_to_data_type_str(df["a"].dtype) == "Float64"

        df32 = pl.DataFrame({"a": pl.Series([1.0], dtype=pl.Float32)})
        assert polars_dtype_to_data_type_str(df32["a"].dtype) == "Float32"

    def test_other_types(self):
        """Test conversion of other common types."""
        df = pl.DataFrame({
            "bool_col": [True, False],
            "date_col": pl.Series([pl.date(2024, 1, 1)], dtype=pl.Date),
        })
        assert polars_dtype_to_data_type_str(df["bool_col"].dtype) == "Boolean"
        assert polars_dtype_to_data_type_str(df["date_col"].dtype) == "Date"


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

    def test_polars_expression(self):
        """Test parsing Polars expression as default value."""
        field = input_schema.OutputFieldInfo(name="test", data_type="Int64", default_value="pl.lit(100)")
        expr = _parse_default_value(field)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.with_columns(expr.alias("test"))
        assert result["test"].to_list() == [100, 100]

    def test_invalid_expression_fallback(self):
        """Test parsing invalid Polars expression falls back to None."""
        field = input_schema.OutputFieldInfo(name="test", data_type="String", default_value="pl.invalid_function()")
        expr = _parse_default_value(field)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.with_columns(expr.alias("test"))
        assert result["test"].to_list() == [None, None]


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
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        fields = [
            input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
        ]
        current_columns = set(df.columns)
        expected_columns = {f.name for f in fields}

        result = _apply_raise_on_missing(df, fields, current_columns, expected_columns)
        assert result.columns == ["b", "a"]

    def test_error_when_columns_missing(self):
        """Test raises ValueError when required columns are missing."""
        df = pl.DataFrame({"a": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
        ]
        current_columns = set(df.columns)
        expected_columns = {f.name for f in fields}

        with pytest.raises(ValueError, match="Missing required columns"):
            _apply_raise_on_missing(df, fields, current_columns, expected_columns)


class TestApplyAddMissing:
    """Tests for _apply_add_missing function."""

    def test_add_columns_with_defaults(self):
        """Test adding missing columns with default values."""
        df = pl.DataFrame({"a": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value="default"),
            input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value="99"),
        ]
        current_columns = set(df.columns)

        result = _apply_add_missing(df, fields, current_columns)
        assert result.columns == ["a", "b", "c"]
        assert result["b"].to_list() == ["default", "default"]
        assert result["c"].to_list() == [99, 99]

    def test_add_columns_with_null_defaults(self):
        """Test adding missing columns with null defaults when no default specified."""
        df = pl.DataFrame({"a": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
        ]
        current_columns = set(df.columns)

        result = _apply_add_missing(df, fields, current_columns)
        assert result.columns == ["a", "b"]
        assert result["b"].to_list() == [None, None]


class TestApplySelectOnly:
    """Tests for _apply_select_only function."""

    def test_select_only_existing_columns(self):
        """Test selecting only columns that exist in the dataframe."""
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        fields = [
            input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="d", data_type="Int64", default_value=None),  # Missing
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
        ]
        current_columns = set(df.columns)

        result = _apply_select_only(df, fields, current_columns)
        # Should only select b and a (d is missing)
        assert result.columns == ["b", "a"]

    def test_no_columns_match(self):
        """Test behavior when no specified columns exist in dataframe."""
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        fields = [
            input_schema.OutputFieldInfo(name="x", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="y", data_type="Int64", default_value=None),
        ]
        current_columns = set(df.columns)

        result = _apply_select_only(df, fields, current_columns)
        # Should return original dataframe when no columns match
        assert result.columns == ["a", "b"]


class TestValidateDataTypes:
    """Tests for _validate_data_types function."""

    def test_validation_passes_when_types_match(self):
        """Test validation passes when data types match expected types."""
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
        ]

        # Should not raise
        _validate_data_types(df, fields)

    def test_validation_fails_on_type_mismatch(self):
        """Test validation raises ValueError when types don't match."""
        df = pl.DataFrame({"a": ["not", "int"], "b": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
        ]

        with pytest.raises(ValueError, match="Data type validation failed"):
            _validate_data_types(df, fields)

    def test_case_insensitive_comparison(self):
        """Test validation is case insensitive."""
        df = pl.DataFrame({"a": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="int64", default_value=None),  # lowercase
        ]

        # Should not raise even though case doesn't match
        _validate_data_types(df, fields)

    def test_skip_missing_columns(self):
        """Test validation skips columns not present in dataframe."""
        df = pl.DataFrame({"a": [1, 2]})
        fields = [
            input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),  # Not in df
        ]

        # Should not raise - only validates columns present in dataframe
        _validate_data_types(df, fields)


# =============================================================================
# Integration Tests: Validation Mode Behaviors
# =============================================================================


class TestSelectOnlyMode:
    """Tests for select_only validation mode."""

    def test_basic_column_selection(self):
        """Test select_only mode keeps only specified columns in order."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="c", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(data_frame=df)
        result_engine = apply_output_field_config(engine, config)

        # Should only have columns c and a in that order
        assert result_engine.data_frame.columns == ["c", "a"]
        assert result_engine.data_frame["c"].to_list() == [7, 8, 9]
        assert result_engine.data_frame["a"].to_list() == [1, 2, 3]

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
        engine = FlowDataEngine(data_frame=df)
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
        engine = FlowDataEngine(data_frame=df)
        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b", "c"]
        assert result_engine.data_frame["a"].to_list() == [1, 2, 3]
        assert result_engine.data_frame["b"].to_list() == ["default", "default", "default"]
        assert result_engine.data_frame["c"].to_list() == [0, 0, 0]

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
        engine = FlowDataEngine(data_frame=df)
        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b"]
        assert result_engine.data_frame["b"].to_list() == [None, None, None]

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
        engine = FlowDataEngine(data_frame=df)
        result_engine = apply_output_field_config(engine, config)

        # Should only have a and d, not b or c
        assert result_engine.data_frame.columns == ["a", "d"]
        assert result_engine.data_frame["d"].to_list() == [99, 99, 99]


class TestRaiseOnMissingMode:
    """Tests for raise_on_missing validation mode."""

    def test_success_when_all_columns_present(self):
        """Test raise_on_missing mode succeeds when all columns present."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="raise_on_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(data_frame=df)
        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b"]

    def test_error_when_column_missing(self):
        """Test raise_on_missing mode raises error when column missing."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="raise_on_missing",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(data_frame=df)

        with pytest.raises(ValueError, match="Missing required columns"):
            apply_output_field_config(engine, config)


class TestDataTypeValidation:
    """Tests for data type validation feature."""

    def test_validation_passes_when_types_match(self):
        """Test data type validation passes when types match."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
            ],
            validate_data_types=True,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(data_frame=df)
        result_engine = apply_output_field_config(engine, config)

        assert result_engine.data_frame.columns == ["a", "b"]

    def test_validation_raises_error_on_mismatch(self):
        """Test data type validation raises error when types don't match."""
        df = pl.DataFrame({"a": ["not", "an", "int"], "b": [1, 2, 3]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="b", data_type="String", default_value=None),
            ],
            validate_data_types=True,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(data_frame=df)

        with pytest.raises(ValueError, match="Data type validation failed"):
            apply_output_field_config(engine, config)

    def test_validation_skipped_when_disabled(self):
        """Test that validation is skipped when validate_data_types is False."""
        df = pl.DataFrame({"a": ["not", "an", "int"]})

        config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="a", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        engine = FlowDataEngine(data_frame=df)

        # Should not raise error even though type doesn't match
        result_engine = apply_output_field_config(engine, config)
        assert result_engine.data_frame.columns == ["a"]


# =============================================================================
# Integration Tests: Full Flow
# =============================================================================


class TestFlowIntegration:
    """Tests for output_field_config integration with flow nodes."""

    def test_polars_code_node_integration(self):
        """Test output_field_config integration with PolarsCode node."""
        graph = create_graph()

        # Add manual input
        data = {"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]}
        add_manual_input(graph, data, node_id=1)

        # Add PolarsCode node with output_field_config
        polars_code_promise = input_schema.NodePromise(
            flow_id=graph.flow_id, node_id=2, node_type="polars_code"
        )
        graph.add_node_promise(polars_code_promise)

        output_config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="y", data_type="Int64", default_value=None),
                input_schema.OutputFieldInfo(name="x", data_type="Int64", default_value=None),
            ],
            validate_data_types=False,
        )

        polars_code = input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            node_type="polars_code",
            is_setup=True,
            polars_code_input=input_schema.PolarsCodeInput(polars_code="df"),
            cache_results=False,
            output_field_config=output_config,
        )
        graph.update_node_settings(polars_code_promise, polars_code)

        # Connect nodes
        graph.add_connection(1, 2)

        # Run the flow
        result = graph.run(run_id=1)

        # Check that output has correct columns in correct order
        output_df = result[2]
        assert output_df.columns == ["y", "x"]
        assert output_df["y"].to_list() == [4, 5, 6]
        assert output_df["x"].to_list() == [1, 2, 3]

    def test_add_missing_mode_in_flow(self):
        """Test output_field_config add_missing mode in flow."""
        graph = create_graph()

        # Add manual input with only some columns
        data = {"a": [1, 2, 3]}
        add_manual_input(graph, data, node_id=1)

        # Add PolarsCode node that adds columns with defaults
        polars_code_promise = input_schema.NodePromise(
            flow_id=graph.flow_id, node_id=2, node_type="polars_code"
        )
        graph.add_node_promise(polars_code_promise)

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

        polars_code = input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            node_type="polars_code",
            is_setup=True,
            polars_code_input=input_schema.PolarsCodeInput(polars_code="df"),
            cache_results=False,
            output_field_config=output_config,
        )
        graph.update_node_settings(polars_code_promise, polars_code)

        # Connect nodes
        graph.add_connection(1, 2)

        # Run the flow
        result = graph.run(run_id=1)

        # Check that missing columns were added with defaults
        output_df = result[2]
        assert output_df.columns == ["a", "b", "c"]
        assert output_df["b"].to_list() == ["new", "new", "new"]
        assert output_df["c"].to_list() == [100, 100, 100]
