from typing import Any, Literal

import polars as pl
import pytest

# Note: For these imports to work, your project's source directory
# (e.g., the directory containing `flowfile_core`) should be in your PYTHONPATH.
from flowfile_core.flowfile.node_designer.ui_components import (
    ActionOption,
    ColumnActionInput,
    ColumnSelector,
)
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


# =============================================================================
# ActionOption Tests
# =============================================================================


def test_action_option_creation():
    """Tests that ActionOption can be created with value and label."""
    opt = ActionOption("sum", "Sum")
    assert opt.value == "sum"
    assert opt.label == "Sum"


def test_action_option_is_tuple():
    """Tests that ActionOption is a NamedTuple and can be unpacked."""
    opt = ActionOption("mean", "Average")
    value, label = opt
    assert value == "mean"
    assert label == "Average"


def test_action_option_indexing():
    """Tests that ActionOption supports indexing like a tuple."""
    opt = ActionOption("max", "Maximum")
    assert opt[0] == "max"
    assert opt[1] == "Maximum"


# =============================================================================
# ColumnActionInput Tests
# =============================================================================


def test_column_action_input_default_initialization():
    """Tests default initialization of ColumnActionInput."""
    comp = ColumnActionInput(label="Test")
    assert comp.component_type == "ColumnActionInput"
    assert comp.actions == []
    assert comp.output_name_template == "{column}_{action}"
    assert comp.show_group_by is False
    assert comp.show_order_by is False
    assert comp.data_types_filter == "ALL"


def test_column_action_input_with_string_actions():
    """Tests ColumnActionInput with string actions."""
    comp = ColumnActionInput(
        label="Aggregations",
        actions=["sum", "mean", "max"],
    )
    assert comp.actions == ["sum", "mean", "max"]


def test_column_action_input_with_action_options():
    """Tests ColumnActionInput with ActionOption tuples."""
    comp = ColumnActionInput(
        label="Aggregations",
        actions=[
            ActionOption("sum", "Sum"),
            ActionOption("avg", "Average"),
        ],
    )
    assert len(comp.actions) == 2
    assert comp.actions[0].value == "sum"
    assert comp.actions[0].label == "Sum"


def test_column_action_input_with_mixed_actions():
    """Tests ColumnActionInput with mixed string and ActionOption actions."""
    comp = ColumnActionInput(
        label="Operations",
        actions=[
            "sum",
            ActionOption("avg", "Average"),
            "max",
        ],
    )
    assert len(comp.actions) == 3
    assert comp.actions[0] == "sum"
    assert comp.actions[1].value == "avg"
    assert comp.actions[2] == "max"


def test_column_action_input_with_options():
    """Tests ColumnActionInput with all options configured."""
    comp = ColumnActionInput(
        label="Rolling Window",
        actions=["sum", "mean"],
        output_name_template="{column}_rolling_{action}",
        show_group_by=True,
        show_order_by=True,
        data_types="Numeric",
    )
    assert comp.output_name_template == "{column}_rolling_{action}"
    assert comp.show_group_by is True
    assert comp.show_order_by is True
    # data_types_filter should normalize "Numeric" to list of numeric types
    assert comp.data_types_filter != "ALL"


def test_column_action_input_default_value():
    """Tests that ColumnActionInput initializes with correct default value structure."""
    comp = ColumnActionInput(label="Test")
    assert comp.value == {
        "rows": [],
        "group_by_columns": [],
        "order_by_column": None,
    }


def test_column_action_input_set_value():
    """Tests setting value from frontend data."""
    comp = ColumnActionInput(label="Test", actions=["sum", "mean"])

    frontend_value = {
        "rows": [
            {"column": "sales", "action": "sum", "output_name": "sales_sum"},
            {"column": "price", "action": "mean", "output_name": "price_mean"},
        ],
        "group_by_columns": ["category"],
        "order_by_column": "date",
    }

    comp.set_value(frontend_value)
    assert comp.value == frontend_value


def test_column_action_input_model_dump_string_actions():
    """Tests model_dump serializes string actions correctly."""
    comp = ColumnActionInput(
        label="Test",
        actions=["sum", "mean", "max"],
    )

    dumped = comp.model_dump()
    assert dumped["component_type"] == "ColumnActionInput"
    assert dumped["actions"] == [
        {"value": "sum", "label": "sum"},
        {"value": "mean", "label": "mean"},
        {"value": "max", "label": "max"},
    ]


def test_column_action_input_model_dump_action_options():
    """Tests model_dump serializes ActionOption correctly."""
    comp = ColumnActionInput(
        label="Test",
        actions=[
            ActionOption("sum", "Sum Total"),
            ActionOption("avg", "Average"),
        ],
    )

    dumped = comp.model_dump()
    assert dumped["actions"] == [
        {"value": "sum", "label": "Sum Total"},
        {"value": "avg", "label": "Average"},
    ]


def test_column_action_input_model_dump_with_data_types():
    """Tests model_dump includes data_types when not ALL."""
    comp = ColumnActionInput(
        label="Test",
        actions=["sum"],
        data_types="Numeric",
    )

    dumped = comp.model_dump()
    assert "data_types" in dumped
    assert isinstance(dumped["data_types"], list)
    # Should contain numeric type strings
    assert "Int64" in dumped["data_types"] or "Float64" in dumped["data_types"]


def test_column_action_input_model_dump_all_options():
    """Tests model_dump includes all configured options."""
    comp = ColumnActionInput(
        label="Full Config",
        actions=["sum"],
        output_name_template="{column}_custom_{action}",
        show_group_by=True,
        show_order_by=True,
    )

    dumped = comp.model_dump()
    assert dumped["output_name_template"] == "{column}_custom_{action}"
    assert dumped["show_group_by"] is True
    assert dumped["show_order_by"] is True


if __name__ == "__main__":
    pytest.main([__file__])
