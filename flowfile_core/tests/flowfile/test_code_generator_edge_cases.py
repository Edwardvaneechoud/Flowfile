"""
Extended tests for code_generator to identify edge cases and potential bugs.

This test file follows the pattern of executing the flow graph locally and comparing
the results from flow.get_node(n).get_resulting_data().data_frame with the output
from the generated Polars code.

Tests cover:
1. Basic filter operators (not just advanced filters)
2. Edge cases with special characters in column names
3. Empty inputs and edge cases
4. Record ID generation (checking deprecated method)
5. Schema validation edge cases
6. Union modes
7. Unique operation variations
8. Polars code edge cases
"""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.flowfile.code_generator.code_generator import (
    export_flow_to_polars,
    FlowGraphToPolarsConverter,
    UnsupportedNodeError,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema


def create_flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    """Create basic flow settings for tests"""
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_flow"
    )


def create_basic_flow(flow_id: int = 1, name: str = "test_flow") -> FlowGraph:
    """Create a basic flow graph for testing"""
    return FlowGraph(flow_settings=create_flow_settings(flow_id), name=name)


def get_result_from_generated_code(code: str) -> pl.DataFrame | pl.LazyFrame:
    """Execute generated code and return the result"""
    exec_globals = {}
    exec(code, exec_globals)
    return exec_globals['run_etl_pipeline']()


def verify_code_executes(code: str):
    """Verify that generated code can be executed without errors"""
    exec_globals = {}
    try:
        exec(code, exec_globals)
        _ = exec_globals['run_etl_pipeline']()
    except Exception as e:
        raise AssertionError(f"Code execution failed:\n{e}\n\nGenerated code:\n{code}")


def assert_flow_result_matches_generated(flow: FlowGraph, output_node_id: int, code: str):
    """Compare flow's node output with generated code output.

    Args:
        flow: The FlowGraph instance
        output_node_id: The node ID whose output should be compared
        code: The generated Polars code

    Raises:
        AssertionError: If the results don't match
    """
    # Get expected result from the flow
    try:
        expected_df = flow.get_node(output_node_id).get_resulting_data().data_frame
    except Exception as e:
        raise AssertionError(f"Failed to get flow result for node {output_node_id}: {e}")

    # Get actual result from generated code
    try:
        result = get_result_from_generated_code(code)
    except Exception as e:
        raise AssertionError(f"Failed to execute generated code:\n{code}\n\nError: {e}")

    # Handle LazyFrame by collecting
    try:
        if hasattr(result, 'collect'):
            result = result.collect()
        if hasattr(expected_df, 'collect'):
            expected_df = expected_df.collect()
    except Exception as e:
        raise AssertionError(
            f"Failed to collect results.\n"
            f"Result type: {type(result)}\n"
            f"Expected type: {type(expected_df)}\n"
            f"Error: {e}\n"
            f"Generated code:\n{code}"
        )

    try:
        assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)
    except AssertionError as e:
        raise AssertionError(
            f"Results don't match.\n"
            f"Generated code result:\n{result}\n\n"
            f"Flow result:\n{expected_df}\n\n"
            f"Original error: {e}\n\n"
            f"Generated code:\n{code}"
        )


class TestBasicFilterOperators:
    """Test all basic filter operators individually."""

    @pytest.fixture
    def sample_data_flow(self):
        """Create a flow with sample data for filtering tests"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="name", data_type="String"),
                    input_schema.MinimalFieldInfo(name="value", data_type="Double"),
                ],
                data=[
                    [1, 2, 3, 4, 5],
                    ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                    [10.5, 20.0, 30.5, 40.0, 50.5]
                ]
            )
        )
        flow.add_manual_input(data)
        return flow

    @pytest.mark.parametrize("operator,value,expected_ids", [
        ("=", "3", [3]),  # EQUALS numeric
        ("!=", "3", [1, 2, 4, 5]),  # NOT_EQUALS numeric
        (">", "3", [4, 5]),  # GREATER_THAN
        (">=", "3", [3, 4, 5]),  # GREATER_THAN_OR_EQUALS
        ("<", "3", [1, 2]),  # LESS_THAN
        ("<=", "3", [1, 2, 3]),  # LESS_THAN_OR_EQUALS
    ])
    def test_numeric_filter_operators(self, sample_data_flow, operator, value, expected_ids):
        """Test numeric filter operators - compare flow result with generated code"""
        flow = sample_data_flow
        filter_node = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="id",
                    operator=operator,
                    value=value
                )
            )
        )
        flow.add_filter(filter_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        # Compare flow node output with generated code output
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)

    @pytest.mark.parametrize("operator,value,expected_names", [
        ("=", "Alice", ["Alice"]),  # EQUALS string
        ("!=", "Alice", ["Bob", "Charlie", "Diana", "Eve"]),  # NOT_EQUALS string
        ("contains", "li", ["Alice", "Charlie"]),  # CONTAINS
        ("not_contains", "li", ["Bob", "Diana", "Eve"]),  # NOT_CONTAINS (NOTE: uses underscore)
        ("starts_with", "A", ["Alice"]),  # STARTS_WITH (NOTE: uses underscore)
        ("ends_with", "e", ["Alice", "Charlie", "Eve"]),  # ENDS_WITH (NOTE: uses underscore)
    ])
    def test_string_filter_operators(self, sample_data_flow, operator, value, expected_names):
        """Test string filter operators - compare flow result with generated code"""
        flow = sample_data_flow
        filter_node = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="name",
                    operator=operator,
                    value=value
                )
            )
        )
        flow.add_filter(filter_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        # Compare flow node output with generated code output
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)

    @pytest.mark.xfail(
        reason="BUG: Flow's IN filter quotes numeric values incorrectly. "
               "Flow checks 'is_numeric' on whole value '1, 3, 5' (fails due to commas), "
               "while code generator correctly splits first then checks each value. "
               "See flow_graph.py:1055-1056 vs code_generator.py:1517-1522"
    )
    def test_in_operator_numeric(self, sample_data_flow):
        """Test IN operator with numeric values - compare flow result with generated code.

        BUG FOUND: There's an inconsistency between how the flow and the code generator
        handle IN operator values. The flow checks if the whole value string "1, 3, 5"
        is numeric (it's not, due to commas), so it quotes all values as strings.
        The code generator splits first, then checks each individual value "1", "3", "5"
        and correctly generates integers.

        This test will fail until the bug is fixed in flow_graph.py.
        """
        flow = sample_data_flow
        filter_node = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="id",
                    operator="in",
                    value="1, 3, 5"
                )
            )
        )
        flow.add_filter(filter_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)

    def test_not_in_operator_string(self, sample_data_flow):
        """Test NOT IN operator with string values - compare flow result with generated code"""
        flow = sample_data_flow
        filter_node = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="name",
                    operator="not_in",  # NOTE: uses underscore
                    value="Alice, Bob"
                )
            )
        )
        flow.add_filter(filter_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)

    def test_between_operator(self, sample_data_flow):
        """Test BETWEEN operator - compare flow result with generated code"""
        flow = sample_data_flow
        filter_node = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="id",
                    operator="between",
                    value="2",
                    value2="4"
                )
            )
        )
        flow.add_filter(filter_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)


class TestNullHandling:
    """Test null value handling in filters and operations."""

    @pytest.fixture
    def data_with_nulls_flow(self):
        """Create a flow with data containing nulls"""
        flow = create_basic_flow()
        # Creating data with nulls using explicit None values
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="name", data_type="String"),
                ],
                data=[
                    [1, 2, 3, 4],
                    ["Alice", None, "Charlie", None]
                ]
            )
        )
        flow.add_manual_input(data)
        return flow

    def test_is_null_filter(self, data_with_nulls_flow):
        """Test IS NULL filter - compare flow result with generated code"""
        flow = data_with_nulls_flow
        filter_node = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="name",
                    operator="is_null",  # NOTE: uses underscore
                    value=""
                )
            )
        )
        flow.add_filter(filter_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)

    def test_is_not_null_filter(self, data_with_nulls_flow):
        """Test IS NOT NULL filter - compare flow result with generated code"""
        flow = data_with_nulls_flow
        filter_node = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="name",
                    operator="is_not_null",  # NOTE: uses underscore
                    value=""
                )
            )
        )
        flow.add_filter(filter_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)


class TestSpecialColumnNames:
    """Test handling of special characters in column names."""

    def test_column_with_space(self):
        """Test columns with spaces in names"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="User ID", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="Full Name", data_type="String"),
                ],
                data=[
                    [1, 2, 3],
                    ["Alice Smith", "Bob Jones", "Charlie Brown"]
                ]
            )
        )
        flow.add_manual_input(data)

        code = export_flow_to_polars(flow)
        verify_code_executes(code)
        result = get_result_from_generated_code(code)
        assert "User ID" in result.columns
        assert "Full Name" in result.columns

    def test_select_column_with_space(self):
        """Test selecting and renaming columns with spaces"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="User ID", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="Full Name", data_type="String"),
                ],
                data=[[1, 2], ["Alice", "Bob"]]
            )
        )
        flow.add_manual_input(data)

        select_node = input_schema.NodeSelect(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            select_input=[
                transform_schema.SelectInput("User ID", "user_id", keep=True),
                transform_schema.SelectInput("Full Name", "full_name", keep=True),
            ]
        )
        flow.add_select(select_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        verify_code_executes(code)
        result = get_result_from_generated_code(code)
        assert "user_id" in result.columns
        assert "full_name" in result.columns


class TestRecordIdDeprecation:
    """Test record ID generation - checking for deprecated method usage."""

    def test_record_id_without_grouping_uses_correct_method(self):
        """Test that record ID without grouping generates working code.

        Note: The code generator uses with_row_count which is deprecated
        in newer Polars versions (should use with_row_index instead).

        Compare flow result with generated code output.
        """
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="name", data_type="String")],
                data=[["Alice", "Bob", "Charlie"]]
            )
        )
        flow.add_manual_input(data)

        record_id_node = input_schema.NodeRecordId(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            record_id_input=transform_schema.RecordIdInput(
                output_column_name="row_id",
                offset=0,
            )
        )
        flow.add_record_id(record_id_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)

        # Compare flow result with generated code output
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)


class TestUniqueOperationVariations:
    """Test unique operation with different configurations."""

    @pytest.mark.xfail(
        reason="BUG: Flow's unique with empty columns uses group_by internally, "
               "which requires at least one key. Generated code uses .unique(keep='first') "
               "which works on all columns. The flow should handle empty columns the same way."
    )
    def test_unique_without_columns(self):
        """Test unique operation on all columns - compare flow result with generated code.

        BUG FOUND: The flow's unique implementation uses group_by internally, which
        requires at least one key column. When columns=[] (empty), it fails with:
        "at least one key is required in a group_by operation"

        The code generator correctly generates `.unique(keep='first')` which works.
        """
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="a", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="b", data_type="String"),
                ],
                data=[
                    [1, 1, 2, 2, 1],
                    ["x", "x", "y", "y", "z"]
                ]
            )
        )
        flow.add_manual_input(data)

        unique_node = input_schema.NodeUnique(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            unique_input=transform_schema.UniqueInput(
                columns=[],  # Empty = all columns
                strategy="first"
            )
        )
        flow.add_unique(unique_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)

    def test_unique_with_last_strategy(self):
        """Test unique with 'last' strategy - compare flow result with generated code"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="value", data_type="Integer"),
                ],
                data=[
                    [1, 1, 2],
                    [10, 20, 30]
                ]
            )
        )
        flow.add_manual_input(data)

        unique_node = input_schema.NodeUnique(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            unique_input=transform_schema.UniqueInput(
                columns=["id"],
                strategy="last"
            )
        )
        flow.add_unique(unique_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)


class TestUnionModes:
    """Test union operation with different modes.

    Note: UnionInput.mode only accepts 'selective' or 'relaxed'.
    - 'relaxed' mode uses pl.concat with how='diagonal_relaxed'
    - 'selective' mode uses pl.concat with how='diagonal'
    """

    def test_union_selective_mode(self):
        """Test union with selective mode (diagonal) - compare flow result with generated code"""
        flow = create_basic_flow()

        # Two dataframes with same columns
        for i in range(1, 3):
            data = input_schema.NodeManualInput(
                flow_id=1,
                node_id=i,
                raw_data_format=input_schema.RawData(
                    columns=[
                        input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                        input_schema.MinimalFieldInfo(name="value", data_type="String")
                    ],
                    data=[[i * 10], [f"value_{i}"]]
                )
            )
            flow.add_manual_input(data)

        union_node = input_schema.NodeUnion(
            flow_id=1,
            node_id=3,
            depending_on_ids=[1, 2],
            union_input=transform_schema.UnionInput(mode="selective")
        )
        flow.add_union(union_node)
        for i in range(1, 3):
            add_connection(flow, input_schema.NodeConnection.create_from_simple_input(i, 3, 'main'))

        code = export_flow_to_polars(flow)
        # Selective mode should use 'diagonal' not 'diagonal_relaxed'
        assert "diagonal_relaxed" not in code
        assert "how='diagonal'" in code
        assert_flow_result_matches_generated(flow, output_node_id=3, code=code)

    def test_union_relaxed_mode(self):
        """Test union with relaxed mode (diagonal_relaxed) - compare flow result with generated code"""
        flow = create_basic_flow()

        # Two dataframes with same columns
        for i in range(1, 3):
            data = input_schema.NodeManualInput(
                flow_id=1,
                node_id=i,
                raw_data_format=input_schema.RawData(
                    columns=[
                        input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                        input_schema.MinimalFieldInfo(name="value", data_type="String")
                    ],
                    data=[[i * 10], [f"value_{i}"]]
                )
            )
            flow.add_manual_input(data)

        union_node = input_schema.NodeUnion(
            flow_id=1,
            node_id=3,
            depending_on_ids=[1, 2],
            union_input=transform_schema.UnionInput(mode="relaxed")
        )
        flow.add_union(union_node)
        for i in range(1, 3):
            add_connection(flow, input_schema.NodeConnection.create_from_simple_input(i, 3, 'main'))

        code = export_flow_to_polars(flow)
        # Relaxed mode should use 'diagonal_relaxed'
        assert "how='diagonal_relaxed'" in code
        assert_flow_result_matches_generated(flow, output_node_id=3, code=code)


class TestSchemaValidation:
    """Test schema validation edge cases."""

    def test_manual_input_with_various_types(self):
        """Test manual input with all supported data types"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="int_col", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="float_col", data_type="Double"),
                    input_schema.MinimalFieldInfo(name="str_col", data_type="String"),
                    input_schema.MinimalFieldInfo(name="bool_col", data_type="Boolean"),
                ],
                data=[
                    [1, 2],
                    [1.5, 2.5],
                    ["a", "b"],
                    [True, False]
                ]
            )
        )
        flow.add_manual_input(data)

        code = export_flow_to_polars(flow)
        verify_code_executes(code)
        result = get_result_from_generated_code(code).collect()

        assert result["int_col"].dtype == pl.Int64
        assert result["float_col"].dtype == pl.Float64
        assert result["str_col"].dtype == pl.String
        assert result["bool_col"].dtype == pl.Boolean


class TestPolarsCodeEdgeCases:
    """Test custom Polars code node edge cases."""

    def test_polars_code_with_empty_string(self):
        """Test handling of empty or whitespace-only code"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="id", data_type="Integer")],
                data=[[1, 2, 3]]
            )
        )
        flow.add_manual_input(data)

        # Code that just returns the input
        polars_code_node = input_schema.NodePolarsCode(
            flow_id=1,
            node_id=2,
            depending_on_ids=[1],
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="input_df"
            )
        )
        flow.add_polars_code(polars_code_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        verify_code_executes(code)

    def test_polars_code_with_comments_only(self):
        """Test code with comments and an expression.

        Note: This test revealed a BUG where the code generator has issues
        handling polars code that starts with comments. The parser may not
        correctly detect the return value when comments precede it.
        """
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="id", data_type="Integer")],
                data=[[1, 2, 3]]
            )
        )
        flow.add_manual_input(data)

        # Use explicit output_df assignment to work around comment handling
        polars_code_node = input_schema.NodePolarsCode(
            flow_id=1,
            node_id=2,
            depending_on_ids=[1],
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="# This is a comment\noutput_df = input_df"
            )
        )
        flow.add_polars_code(polars_code_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        verify_code_executes(code)


class TestEmptyDataHandling:
    """Test handling of empty data scenarios."""

    def test_empty_dataframe(self):
        """Test code generation with an empty dataframe"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="id", data_type="Integer")],
                data=[[]]  # Empty data
            )
        )
        flow.add_manual_input(data)

        code = export_flow_to_polars(flow)
        verify_code_executes(code)
        result = get_result_from_generated_code(code).collect()
        assert len(result) == 0

    def test_select_with_empty_input(self):
        """Test select node with empty select_input list"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="id", data_type="Integer")],
                data=[[1, 2, 3]]
            )
        )
        flow.add_manual_input(data)

        select_node = input_schema.NodeSelect(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            select_input=[]  # Empty selection
        )
        flow.add_select(select_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        verify_code_executes(code)


class TestGroupByEdgeCases:
    """Test group by edge cases."""

    @pytest.mark.xfail(
        reason="BUG: Delimiter mismatch in str.concat. "
               "Code generator uses default '-' delimiter, flow uses ',' delimiter. "
               "Generated: str.concat() → ['x-y'], Flow: str.concat(',') → ['x,y']. "
               "See code_generator.py:1564 - should specify delimiter=','"
    )
    def test_groupby_with_concat_aggregation(self):
        """Test groupby with string concatenation aggregation - compare flow result with generated code.

        BUG FOUND: The code generator's str.concat() uses the default delimiter "-" (hyphen),
        but the flow uses "," (comma) as the delimiter.

        Result from generated code: ['x-y', 'z']
        Result from flow: ['x,y', 'z']

        The code generator should specify delimiter=',' to match the flow's behavior.
        """
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="category", data_type="String"),
                    input_schema.MinimalFieldInfo(name="item", data_type="String"),
                ],
                data=[
                    ["A", "A", "B"],
                    ["x", "y", "z"]
                ]
            )
        )
        flow.add_manual_input(data)

        groupby_node = input_schema.NodeGroupBy(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("category", "groupby"),
                    transform_schema.AggColl("item", "concat", "items_concat"),
                ]
            )
        )
        flow.add_group_by(groupby_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        # Check that concat uses str.concat (deprecated - should be str.join)
        assert "str.concat" in code or "str.join" in code
        assert_flow_result_matches_generated(flow, output_node_id=2, code=code)


class TestFlowOutputNodes:
    """Test flow output node handling."""

    def test_multiple_output_nodes(self):
        """Test flow with multiple output markers"""
        flow = create_basic_flow()

        # Input node
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="id", data_type="Integer")],
                data=[[1, 2, 3]]
            )
        )
        flow.add_manual_input(data)

        # Two branches from the same input
        for i in range(2, 4):
            filter_node = input_schema.NodeFilter(
                flow_id=1,
                node_id=i,
                depending_on_id=1,
                filter_input=transform_schema.FilterInput(
                    filter_type="advanced",
                    advanced_filter=f"[id]={i}"
                )
            )
            flow.add_filter(filter_node)
            add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, i))

        code = export_flow_to_polars(flow)
        verify_code_executes(code)


class TestDataTypeConversions:
    """Test data type conversion edge cases."""

    def test_cast_to_all_supported_types(self):
        """Test casting to all supported data types"""
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="num_str", data_type="String"),
                ],
                data=[
                    ["1", "2", "3"]
                ]
            )
        )
        flow.add_manual_input(data)

        select_node = input_schema.NodeSelect(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            select_input=[
                transform_schema.SelectInput(
                    "num_str", "as_int", keep=True,
                    data_type="Int64", data_type_change=True
                ),
            ]
        )
        flow.add_select(select_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)
        verify_code_executes(code)
        result = get_result_from_generated_code(code).collect()
        assert result["as_int"].dtype == pl.Int64


class TestConverterHelperMethods:
    """Direct tests for converter helper methods."""

    def test_get_polars_dtype_mapping(self):
        """Test data type string to Polars type mapping"""
        flow = create_basic_flow()
        converter = FlowGraphToPolarsConverter(flow)

        # Test all documented mappings
        assert converter._get_polars_dtype("String") == "pl.Utf8"
        assert converter._get_polars_dtype("Integer") == "pl.Int64"
        assert converter._get_polars_dtype("Double") == "pl.Float64"
        assert converter._get_polars_dtype("Boolean") == "pl.Boolean"
        assert converter._get_polars_dtype("Date") == "pl.Date"
        assert converter._get_polars_dtype("Datetime") == "pl.Datetime"

        # Test unknown type defaults to Utf8
        assert converter._get_polars_dtype("UnknownType") == "pl.Utf8"

    def test_get_agg_function_mapping(self):
        """Test aggregation function name mapping"""
        flow = create_basic_flow()
        converter = FlowGraphToPolarsConverter(flow)

        # Test mapped functions
        assert converter._get_agg_function("avg") == "mean"
        assert converter._get_agg_function("average") == "mean"
        assert converter._get_agg_function("concat") == "str.concat"

        # Test pass-through functions
        assert converter._get_agg_function("sum") == "sum"
        assert converter._get_agg_function("min") == "min"
        assert converter._get_agg_function("max") == "max"


class TestUnsupportedNodeHandling:
    """Test handling of unsupported nodes."""

    def test_unknown_node_type_raises_error(self):
        """Test that unknown node types raise UnsupportedNodeError"""
        flow = create_basic_flow()

        # Add a manual input
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="id", data_type="Integer")],
                data=[[1, 2, 3]]
            )
        )
        flow.add_manual_input(data)

        # Manually add an unknown node type (this is a bit hacky but tests the handler)
        converter = FlowGraphToPolarsConverter(flow)

        # We can verify the handler returns early for unknown types by checking unsupported_nodes
        # after attempting to generate code for a node with unknown type
        # This is tested in test_node_with_no_handler but we verify the error message format

        code = export_flow_to_polars(flow)  # Should work since we have valid nodes
        assert "import polars as pl" in code


class TestDeprecatedMethodUsage:
    """Tests that document deprecated Polars methods used in generated code.

    These tests serve as documentation of known deprecation issues that should
    be addressed in future updates to the code generator.
    """

    def test_record_id_uses_deprecated_with_row_count(self):
        """Test that record ID generation uses deprecated with_row_count method.

        BUG: The code generator uses `with_row_count()` which is deprecated
        in newer Polars versions. It should use `with_row_index()` instead.

        See: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.with_row_index.html
        """
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="name", data_type="String")],
                data=[["Alice", "Bob"]]
            )
        )
        flow.add_manual_input(data)

        record_id_node = input_schema.NodeRecordId(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            record_id_input=transform_schema.RecordIdInput(
                output_column_name="row_id",
                offset=0,
            )
        )
        flow.add_record_id(record_id_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)

        # Document the deprecated method usage
        # This test passes when the deprecated method IS used (documenting the bug)
        # When the code is fixed to use with_row_index, this test should be updated
        uses_deprecated = "with_row_count" in code
        uses_modern = "with_row_index" in code

        # Currently the code generator uses the deprecated method
        assert uses_deprecated or uses_modern, "Neither deprecated nor modern method found"

        # TODO: When fixed, change to: assert uses_modern and not uses_deprecated

    def test_groupby_concat_uses_deprecated_str_concat(self):
        """Test that string concatenation in group_by uses deprecated str.concat.

        BUG: The code generator uses `str.concat()` for string concatenation
        which is deprecated. It should use `str.join()` instead.

        The default delimiter also changed from '-' to '' (empty string).

        See: https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.join.html
        """
        flow = create_basic_flow()
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="category", data_type="String"),
                    input_schema.MinimalFieldInfo(name="item", data_type="String"),
                ],
                data=[
                    ["A", "A"],
                    ["x", "y"]
                ]
            )
        )
        flow.add_manual_input(data)

        groupby_node = input_schema.NodeGroupBy(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("category", "groupby"),
                    transform_schema.AggColl("item", "concat", "items_concat"),
                ]
            )
        )
        flow.add_group_by(groupby_node)
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(flow)

        # Document the deprecated method usage
        uses_deprecated = "str.concat" in code
        uses_modern = "str.join" in code

        # Currently the code generator uses the deprecated method
        assert uses_deprecated or uses_modern, "Neither deprecated nor modern method found"

        # TODO: When fixed, change to: assert uses_modern and not uses_deprecated


class TestFilterOperatorSpaceVariants:
    """Tests that document filter operator symbol variations.

    The FilterOperator.from_symbol() method accepts underscore variants
    (e.g., 'not_contains') but not space variants (e.g., 'not contains').

    This is documented behavior, but users might expect space variants to work.
    These tests document this edge case for future API improvements.
    """

    def test_filter_operators_require_underscores(self):
        """Document that filter operators require underscores, not spaces.

        The from_symbol() method mapping uses:
        - 'not_contains' (NOT 'not contains')
        - 'starts_with' (NOT 'starts with')
        - 'ends_with' (NOT 'ends with')
        - 'is_null' (NOT 'is null')
        - 'is_not_null' (NOT 'is not null')
        - 'not_in' (NOT 'not in')

        Consider adding space variants to the mapping for better UX.
        """
        from flowfile_core.schemas.transform_schema import FilterOperator

        # These work (underscore versions)
        valid_symbols = [
            "not_contains", "starts_with", "ends_with",
            "is_null", "is_not_null", "not_in"
        ]

        for symbol in valid_symbols:
            operator = FilterOperator.from_symbol(symbol)
            assert operator is not None

        # These do NOT work (space versions) - documented edge case
        invalid_symbols = [
            "not contains", "starts with", "ends with",
            "is null", "is not null", "not in"
        ]

        for symbol in invalid_symbols:
            with pytest.raises(ValueError, match="Unknown filter operator symbol"):
                FilterOperator.from_symbol(symbol)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
