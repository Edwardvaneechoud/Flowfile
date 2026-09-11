import os

os.environ["TESTING"] = "True"

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.schemas.input_schema import NodeMultiFieldFormula
from flowfile_frame.flow_frame import FlowFrame

RAW = {
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
    "jan": [5.0, 15.0, 30.0],
    "feb": [5.0, 5.0, 10.0],
    "total": [10.0, 20.0, 40.0],
}


@pytest.fixture
def df():
    return FlowFrame(dict(RAW))


@pytest.fixture
def pl_df():
    return pl.DataFrame(dict(RAW))


def test_multi_field_formula_replaces_listed_columns(df, pl_df):
    result = df.multi_field_formula("uppercase([_CurrentField_])", columns=["name"]).collect()
    assert_frame_equal(result, pl_df.with_columns(pl.col("name").str.to_uppercase()))


def test_multi_field_formula_over_all_columns(df, pl_df):
    result = df.multi_field_formula("to_string([_CurrentField_])").collect()
    assert_frame_equal(result, pl_df.select(pl.all().cast(pl.String)))


def test_multi_field_formula_new_columns_with_prefix(df, pl_df):
    result = df.multi_field_formula("[_CurrentField_] * 2", columns=["jan", "feb"], prefix="dbl_").collect()
    expected = pl_df.with_columns(
        (pl.col("jan") * 2).alias("dbl_jan"),
        (pl.col("feb") * 2).alias("dbl_feb"),
    )
    assert_frame_equal(result, expected)


def test_multi_field_formula_suffix_keeps_a_leading_space(df, pl_df):
    result = df.multi_field_formula("[_CurrentField_] + 1", columns=["jan"], suffix=" plus").collect()
    assert_frame_equal(result, pl_df.with_columns((pl.col("jan") + 1).alias("jan plus")))


def test_multi_field_formula_by_data_type(df, pl_df):
    result = df.multi_field_formula("[_CurrentField_] * 10", data_type="Numeric").collect()
    assert_frame_equal(result, pl_df.with_columns(pl.col("id", "jan", "feb", "total") * 10))


def test_multi_field_formula_reads_the_original_values_of_other_targets(df, pl_df):
    """One `with_columns` means a target referencing another target sees the untouched input."""
    # `total` is listed FIRST so a sequential per-column implementation would diverge here.
    result = df.multi_field_formula("[_CurrentField_] / [total] * 100", columns=["total", "jan", "feb"]).collect()
    expected = pl_df.with_columns(
        (pl.col("jan") / pl.col("total") * 100).alias("jan"),
        (pl.col("feb") / pl.col("total") * 100).alias("feb"),
        (pl.col("total") / pl.col("total") * 100).alias("total"),
    )
    assert_frame_equal(result, expected)


def test_multi_field_formula_binds_the_column_name_and_type(df):
    result = df.multi_field_formula(
        '[_CurrentFieldName_] + ":" + [_CurrentFieldType_]', columns=["name", "jan"], prefix="meta_"
    ).collect()
    assert result["meta_name"].to_list() == ["name:String"] * 3
    assert result["meta_jan"].to_list() == ["jan:Float64"] * 3


def test_multi_field_formula_casts_the_output(df):
    result = df.multi_field_formula("[_CurrentField_] * 2", columns=["jan"], output_data_type="Int64").collect()
    assert result.schema["jan"] == pl.Int64
    assert result["jan"].to_list() == [10, 30, 60]


def test_multi_field_formula_skips_unknown_columns(df, pl_df):
    result = df.multi_field_formula("[_CurrentField_] * 2", columns=["jan", "nope"]).collect()
    assert_frame_equal(result, pl_df.with_columns(pl.col("jan") * 2))


def test_multi_field_formula_without_targets_is_a_no_op(df, pl_df):
    result = df.multi_field_formula("[_CurrentField_] * 2", columns=[]).collect()
    assert_frame_equal(result, pl_df)


def test_multi_field_formula_node_type_and_settings(df):
    result = df.multi_field_formula("[_CurrentField_] * 2", columns=["jan"], prefix="p_", suffix="_s")
    node = result.get_node_settings()
    assert node.node_type == "multi_field_formula"
    assert isinstance(node.setting_input, NodeMultiFieldFormula)
    settings = node.setting_input.multi_field_formula_input
    assert settings.formula == "[_CurrentField_] * 2"
    assert settings.selection_mode == "list"
    assert settings.selected_columns == ["jan"]
    assert settings.selected_data_type is None
    assert settings.output_mode == "new"
    assert (settings.output_prefix, settings.output_suffix) == ("p_", "_s")
    assert settings.output_data_type == "Auto"


def test_multi_field_formula_data_type_selection_wires_settings(df):
    result = df.multi_field_formula("[_CurrentField_] * 2", data_type="Numeric", output_data_type="Float64")
    settings = result.get_node_settings().setting_input.multi_field_formula_input
    assert settings.selection_mode == "data_type"
    assert settings.selected_data_type == "Numeric"
    assert settings.selected_columns == []
    assert settings.output_mode == "replace"
    assert settings.output_data_type == "Float64"


def test_multi_field_formula_default_selection_is_all(df):
    settings = df.multi_field_formula("to_string([_CurrentField_])").get_node_settings()
    assert settings.setting_input.multi_field_formula_input.selection_mode == "all"


def test_multi_field_formula_rejects_a_blank_formula(df):
    with pytest.raises(ValueError, match="'formula' is required"):
        df.multi_field_formula("   ")


def test_multi_field_formula_rejects_columns_and_data_type_together(df):
    with pytest.raises(ValueError, match="at most one"):
        df.multi_field_formula("[_CurrentField_] * 2", columns=["jan"], data_type="Numeric")
