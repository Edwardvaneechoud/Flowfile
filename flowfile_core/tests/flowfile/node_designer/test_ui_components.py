from typing import Any, Literal

import polars as pl
import pytest

# Note: For these imports to work, your project's source directory
# (e.g., the directory containing `flowfile_core`) should be in your PYTHONPATH.
from flowfile_core.flowfile.node_designer.ui_components import ColumnSelector
from flowfile_core.types import DataType, TypeGroup, Types

# Helper lists for expected sorted outputs from TypeGroups
NUMERIC_TYPES_SORTED = sorted([
    DataType.Int8, DataType.Int16, DataType.Int32, DataType.Int64,
    DataType.UInt8, DataType.UInt16, DataType.UInt32, DataType.UInt64,
    DataType.Float32, DataType.Float64, DataType.Decimal
], key=lambda x: x.value)

STRING_TYPES_SORTED = sorted([
    DataType.String, DataType.Categorical
], key=lambda x: x.value)

DATE_TYPES_SORTED = sorted([
    DataType.Date, DataType.Datetime, DataType.Time, DataType.Duration
], key=lambda x: x.value)


def test_column_selector_default_initialization():
    """Tests that the default data type filter is 'ALL' when no input is given."""
    selector = ColumnSelector()
    assert selector.data_types_filter == "ALL"


@pytest.mark.parametrize("input_spec, expected_output", [
    # Case 1: "ALL" string literal
    ("ALL", "ALL"),

    # Case 2: Single DataType as a string
    ("Int64", [DataType.Int64]),

    # Case 3: Single DataType alias string
    ("int", [DataType.Int64]),
    ("str", [DataType.String]),
    ("float", [DataType.Float64]),

    # Case 4: Single TypeGroup as a string
    ("Numeric", NUMERIC_TYPES_SORTED),

    # Case 5: Single DataType as an enum
    (DataType.Boolean, [DataType.Boolean]),

    # Case 6: Single TypeGroup as an enum
    (TypeGroup.String, STRING_TYPES_SORTED),

    # Case 7: Polars data type class
    (pl.Int32, [DataType.Int32]),
    (pl.Utf8, [DataType.String]), # pl.String is an alias for pl.Utf8

    # Case 8: Polars data type instance
    (pl.Datetime(time_unit="ms"), [DataType.Datetime]),

    # Case 9: List of strings (should be sorted in output)
    (["String", "Int32"], [DataType.Categorical, DataType.Int32, DataType.String]),

    # Case 10: List of enums (unsorted, should be sorted in output)
    ([DataType.String, DataType.Boolean], [DataType.Boolean, DataType.String]),

    # Case 11: Mixed list of aliases, enums, and groups (should be flattened and sorted)
    (["int", DataType.Boolean, TypeGroup.Date], sorted(
        [DataType.Int64, DataType.Boolean] + DATE_TYPES_SORTED,
        key=lambda x: x.value
    )),

    # Case 12: List of Polars types (should be sorted in output)
    ([pl.Float32, pl.Int16], [DataType.Float32, DataType.Int16]),

])
def test_column_selector_data_type_normalization(input_spec: Any, expected_output: Literal["ALL"] | list[DataType]):
    """
    Tests various inputs for the `data_types` field and verifies
    the computed `data_types_filter` is correctly normalized and sorted.
    """
    selector = ColumnSelector(data_types=input_spec)
    assert set(selector.data_types_filter) == set(expected_output)


def test_model_dump_when_filter_is_all():
    """
    Tests that `model_dump` does not include the 'data_types' key
    when the filter is 'ALL'.
    """
    selector = ColumnSelector(data_types="ALL")
    dumped_data = selector.model_dump()
    assert "data_types" not in dumped_data
    # Ensure other essential fields are still present
    assert dumped_data["component_type"] == "ColumnSelector"


def test_model_dump_when_filter_is_date():
    selector = ColumnSelector(data_types=Types.Date)
    dumped_data = selector.model_dump()
    assert "data_types" in dumped_data
    # Ensure other essential fields are still present
    assert dumped_data["data_types"] == ["Date"]


def test_model_dump_when_filter_is_specific():
    """
    Tests that `model_dump` correctly serializes a list of specific
    DataType enums into their string values, maintaining the sorted order.
    """
    # Use an unsorted list to ensure the output is based on the sorted property
    selector = ColumnSelector(data_types=[DataType.String, DataType.Boolean])
    dumped_data = selector.model_dump()
    # The internal `data_types_filter` property is sorted, so the dump should reflect that
    expected_values = ["Boolean", "String"]

    assert "data_types" in dumped_data
    assert dumped_data["data_types"] == expected_values
    assert dumped_data["component_type"] == "ColumnSelector"


if __name__ == "__main__":
    pytest.main([__file__])
