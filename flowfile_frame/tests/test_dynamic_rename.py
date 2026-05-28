import os

os.environ["TESTING"] = "True"

import pytest

from flowfile_core.schemas.input_schema import NodeDynamicRename
from flowfile_frame.flow_frame import FlowFrame


@pytest.fixture
def df():
    return FlowFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "city": ["New York", "Los Angeles", "Chicago"],
        }
    )


def test_dynamic_rename_prefix_all(df):
    result = df.dynamic_rename(mode="prefix", prefix="src_")
    assert result.collect().columns == ["src_id", "src_name", "src_age", "src_city"]


def test_dynamic_rename_suffix_with_columns_list(df):
    result = df.dynamic_rename(mode="suffix", suffix="_raw", columns=["name"])
    assert result.collect().columns == ["id", "name_raw", "age", "city"]


def test_dynamic_rename_data_type_numeric(df):
    result = df.dynamic_rename(mode="prefix", prefix="num_", data_type="Numeric")
    cols = result.collect().columns
    assert "num_id" in cols and "num_age" in cols
    assert "name" in cols and "city" in cols
    assert "num_name" not in cols and "num_city" not in cols


def test_dynamic_rename_formula(df):
    result = df.dynamic_rename(mode="formula", formula="uppercase([column_name])")
    assert result.collect().columns == ["ID", "NAME", "AGE", "CITY"]


def test_dynamic_rename_first_row_promotes_and_drops(df):
    result = df.dynamic_rename(mode="first_row")
    out = result.collect()
    assert out.columns == ["1", "Alice", "25", "New York"]
    assert len(out) == 2


def test_dynamic_rename_rejects_columns_and_data_type_together(df):
    with pytest.raises(ValueError, match="at most one"):
        df.dynamic_rename(mode="prefix", prefix="x_", columns=["id"], data_type="Numeric")


def test_dynamic_rename_rejects_empty_prefix_in_prefix_mode(df):
    with pytest.raises(ValueError, match="'prefix' is required"):
        df.dynamic_rename(mode="prefix")


def test_dynamic_rename_rejects_empty_suffix_in_suffix_mode(df):
    with pytest.raises(ValueError, match="'suffix' is required"):
        df.dynamic_rename(mode="suffix")


def test_dynamic_rename_rejects_empty_formula_in_formula_mode(df):
    with pytest.raises(ValueError, match="'formula' is required"):
        df.dynamic_rename(mode="formula", formula="   ")


def test_dynamic_rename_rejects_extras_in_first_row_mode(df):
    with pytest.raises(ValueError, match="must be empty"):
        df.dynamic_rename(mode="first_row", prefix="x_")


def test_dynamic_rename_rejects_mismatched_param_for_mode(df):
    with pytest.raises(ValueError, match="only 'prefix' may be set"):
        df.dynamic_rename(mode="prefix", prefix="x_", suffix="_y")


def test_dynamic_rename_node_type_and_settings(df):
    result = df.dynamic_rename(mode="prefix", prefix="src_", columns=["name"])
    settings_node = result.get_node_settings()
    assert settings_node.node_type == "dynamic_rename"
    assert isinstance(settings_node.setting_input, NodeDynamicRename)
    rename_input = settings_node.setting_input.dynamic_rename_input
    assert rename_input.rename_mode == "prefix"
    assert rename_input.prefix == "src_"
    assert rename_input.selection_mode == "list"
    assert rename_input.selected_columns == ["name"]
    assert rename_input.selected_data_type is None


def test_dynamic_rename_data_type_selection_wires_settings(df):
    result = df.dynamic_rename(mode="suffix", suffix="_n", data_type="Numeric")
    rename_input = result.get_node_settings().setting_input.dynamic_rename_input
    assert rename_input.selection_mode == "data_type"
    assert rename_input.selected_data_type == "Numeric"
    assert rename_input.selected_columns == []


def test_dynamic_rename_default_selection_is_all(df):
    result = df.dynamic_rename(mode="prefix", prefix="src_")
    rename_input = result.get_node_settings().setting_input.dynamic_rename_input
    assert rename_input.selection_mode == "all"
