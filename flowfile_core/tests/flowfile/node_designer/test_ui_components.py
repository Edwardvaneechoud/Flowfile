from typing import Any, Literal

import polars as pl
import pytest

from flowfile_core.flowfile.node_designer.ui_components import (
    ActionOption,
    ColumnActionInput,
    ColumnActionRow,
    ColumnActionValue,
    ColumnSelector,
)
from flowfile_core.types import DataType, TypeGroup, Types

def test_column_selector_default_initialization():
    """Tests that the default data type filter is 'ALL' when no input is given."""
    selector = ColumnSelector()
    assert selector.data_types_filter == "ALL"


@pytest.mark.parametrize("input_spec, expected_output", [
    # "ALL" sentinel
    ("ALL", "ALL"),

    # Specific type as a canonical string / alias -> canonical DataType name
    ("Int64", ["Int64"]),
    ("int", ["Int64"]),
    ("str", ["String"]),
    ("float", ["Float64"]),

    # TypeGroups are preserved as their canonical group name (NOT expanded to members)
    ("Numeric", ["Numeric"]),
    (TypeGroup.String, ["String"]),

    # Specific DataType enum -> canonical name
    (DataType.Boolean, ["Boolean"]),

    # Polars type class / instance -> canonical specific name
    (pl.Int32, ["Int32"]),
    (pl.Utf8, ["String"]),  # pl.String is an alias for pl.Utf8
    (pl.Datetime(time_unit="ms"), ["Datetime"]),

    # A bare "String" string resolves to the group; "Int32" stays specific
    (["String", "Int32"], ["Int32", "String"]),

    # List of specific enums, sorted
    ([DataType.String, DataType.Boolean], ["Boolean", "String"]),

    # Mixed: alias -> specific, enum -> specific, group -> canonical group name
    (["int", DataType.Boolean, TypeGroup.Date], ["Boolean", "Date", "Int64"]),

    # List of polars types -> specific names
    ([pl.Float32, pl.Int16], ["Float32", "Int16"]),
])
def test_column_selector_data_type_normalization(input_spec, expected_output):
    """
    Tests various inputs for the `data_types` field and verifies the computed
    `data_types_filter` is normalized to canonical tokens with groups preserved.
    """
    selector = ColumnSelector(data_types=input_spec)
    if expected_output == "ALL":
        assert selector.data_types_filter == "ALL"
    else:
        assert set(selector.data_types_filter) == set(expected_output)


def test_model_dump_when_filter_is_all():
    """
    Tests that `model_dump` does not include the 'data_types' key
    when the filter is 'ALL'.
    """
    selector = ColumnSelector(data_types="ALL")
    dumped_data = selector.model_dump()
    assert "data_types" not in dumped_data
    assert dumped_data["component_type"] == "ColumnSelector"


def test_model_dump_when_filter_is_date():
    selector = ColumnSelector(data_types=Types.Date)
    dumped_data = selector.model_dump()
    assert "data_types" in dumped_data
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


# ActionOption Tests


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


# ColumnActionInput Tests


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
    assert comp.data_types_filter != "ALL"


def test_column_action_input_default_value():
    """Tests that ColumnActionInput initializes with a typed, empty ColumnActionValue."""
    comp = ColumnActionInput(label="Test")
    assert isinstance(comp.value, ColumnActionValue)
    assert comp.value.rows == []
    assert comp.value.group_by_columns == []
    assert comp.value.order_by_column is None
    assert comp.value.model_dump() == {
        "rows": [],
        "group_by_columns": [],
        "order_by_column": None,
    }


def test_column_action_input_set_value():
    """Tests that the raw frontend dict is coerced into a typed ColumnActionValue."""
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

    # Attribute access — the whole point of the typed value.
    assert isinstance(comp.value, ColumnActionValue)
    assert isinstance(comp.value.rows[0], ColumnActionRow)
    assert comp.value.rows[0].column == "sales"
    assert comp.value.rows[0].action == "sum"
    assert comp.value.rows[0].output_name == "sales_sum"
    assert comp.value.group_by_columns == ["category"]
    assert comp.value.order_by_column == "date"
    # And it round-trips back to the exact wire dict.
    assert comp.value.model_dump() == frontend_value


def test_column_action_value_exported_from_nd():
    """The value models are reachable through the canonical `nd` namespace."""
    from flowfile import node_designer as nd

    assert nd.ColumnActionValue is ColumnActionValue
    assert nd.ColumnActionRow is ColumnActionRow


def test_column_action_input_model_dump_value_is_plain_dict():
    """Frontend/flow-save paths go through model_dump — value must serialize to a plain dict."""
    comp = ColumnActionInput(label="Test", actions=["sum"])
    comp.set_value(
        {
            "rows": [{"column": "a", "action": "sum", "output_name": "a_sum"}],
            "group_by_columns": [],
            "order_by_column": None,
        }
    )
    dumped = comp.model_dump()
    assert isinstance(dumped["value"], dict)
    assert dumped["value"]["rows"] == [{"column": "a", "action": "sum", "output_name": "a_sum"}]
    # order_by_column stays present (not stripped) even when None.
    assert dumped["value"]["order_by_column"] is None


def test_extract_settings_values_is_json_safe():
    """Guards the kernel/worker/save seams: extracted settings are plain, JSON-serializable dicts."""
    import json

    from flowfile import node_designer as nd

    class _Settings(nd.NodeSettings):
        agg: nd.Section = nd.Section(
            title="Agg",
            column_actions=nd.ColumnActionInput(label="Aggregations", actions=["sum"]),
        )

    class _Node(nd.CustomNodeBase):
        node_name: str = "T"
        settings_schema: _Settings = _Settings()

    node = _Node()
    node.settings_schema.populate_values(
        {
            "agg": {
                "column_actions": {
                    "rows": [{"column": "a", "action": "sum", "output_name": "a_sum"}],
                    "group_by_columns": [],
                    "order_by_column": None,
                }
            }
        }
    )

    extracted = node._extract_settings_values()
    ca = extracted["agg"]["column_actions"]
    assert isinstance(ca, dict)
    assert ca["rows"][0]["column"] == "a"
    json.dumps(extracted)  # must not raise


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
    # Groups are preserved as their canonical name, not expanded to member types.
    assert dumped["data_types"] == ["Numeric"]


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
