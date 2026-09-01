import os

os.environ["TESTING"] = "True"

import datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.schemas.input_schema import NodeDataCleansing
from flowfile_core.schemas.transform_schema import CLEANSING_PUNCTUATION_REGEX
from flowfile_frame.flow_frame import FlowFrame

MESSY_DATA = {
    "id": [1, 2, None],
    "name": ["  Alice  ", "b0b!", None],
    "score": [10.5, None, 3.0],
    "active": [True, False, None],
}


@pytest.fixture
def df():
    return FlowFrame(MESSY_DATA)


def _reference() -> pl.DataFrame:
    return pl.DataFrame(MESSY_DATA)


def test_data_cleansing_emits_native_node(df):
    result = df.data_cleansing()
    node = result.get_node_settings()
    assert node.node_type == "data_cleansing"
    assert isinstance(node.setting_input, NodeDataCleansing)


def test_data_cleansing_defaults_land_on_settings(df):
    settings = df.data_cleansing().get_node_settings().setting_input.cleansing_input
    assert settings.model_dump() == {
        "remove_null_rows": False,
        "remove_null_columns": False,
        "selection_mode": "all",
        "selected_columns": [],
        "replace_nulls_with_blank": True,
        "replace_nulls_with_zero": True,
        "trim_whitespace": True,
        "normalize_whitespace": False,
        "remove_all_whitespace": False,
        "remove_letters": False,
        "remove_numbers": False,
        "remove_punctuation": False,
        "case_mode": "none",
    }


def test_data_cleansing_every_kwarg_reaches_the_settings_model(df):
    """The codegen FlowFrame handler emits these exact keywords; they must all land."""
    settings = (
        df.data_cleansing(
            columns=["name"],
            remove_null_rows=True,
            remove_null_columns=True,
            replace_nulls_with_blank=False,
            replace_nulls_with_zero=False,
            trim_whitespace=False,
            normalize_whitespace=True,
            remove_all_whitespace=True,
            remove_letters=True,
            remove_numbers=True,
            remove_punctuation=True,
            case_mode="titlecase",
        )
        .get_node_settings()
        .setting_input.cleansing_input
    )
    assert settings.model_dump() == {
        "remove_null_rows": True,
        "remove_null_columns": True,
        "selection_mode": "list",
        "selected_columns": ["name"],
        "replace_nulls_with_blank": False,
        "replace_nulls_with_zero": False,
        "trim_whitespace": False,
        "normalize_whitespace": True,
        "remove_all_whitespace": True,
        "remove_letters": True,
        "remove_numbers": True,
        "remove_punctuation": True,
        "case_mode": "titlecase",
    }


def test_data_cleansing_defaults_match_polars(df):
    result = df.data_cleansing().collect()
    expected = _reference().with_columns(
        pl.col("id").fill_null(0),
        pl.col("score").fill_null(0),
        pl.col("name").fill_null("").str.strip_chars(),
    )
    assert_frame_equal(result, expected)


def test_data_cleansing_string_ops_combo_matches_polars(df):
    result = df.data_cleansing(
        replace_nulls_with_zero=False,
        normalize_whitespace=True,
        remove_numbers=True,
        remove_punctuation=True,
        case_mode="uppercase",
    ).collect()
    expected = _reference().with_columns(
        pl.col("name")
        .fill_null("")
        .str.replace_all(r"\d", "")
        .str.replace_all(CLEANSING_PUNCTUATION_REGEX, "")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_uppercase()
    )
    assert_frame_equal(result, expected)
    assert result["name"].to_list() == ["ALICE", "BB", ""]


def test_data_cleansing_remove_null_rows_and_columns_match_polars():
    data = {"a": [None, None, 1], "b": [None, None, None], "c": ["", None, "x"]}
    result = (
        FlowFrame(data)
        .data_cleansing(remove_null_rows=True, remove_null_columns=True, replace_nulls_with_blank=False)
        .collect()
    )
    expected = (
        pl.DataFrame(data)
        .filter(~pl.all_horizontal(pl.all().is_null()))
        .drop("b")
        .with_columns(pl.col("a").fill_null(0))
    )
    assert_frame_equal(result, expected)
    assert result.columns == ["a", "c"]


def test_data_cleansing_remove_null_rows_keeps_row_with_empty_string():
    """An empty string is not a null, so its row survives."""
    frame = pl.DataFrame({"a": [None, None], "b": ["", None]}, schema={"a": pl.Int64, "b": pl.String})
    result = FlowFrame(frame).data_cleansing(remove_null_rows=True, replace_nulls_with_blank=False).collect()
    assert result.to_dicts() == [{"a": 0, "b": ""}]


def test_data_cleansing_column_selection_leaves_others_untouched(df):
    result = df.data_cleansing(["name"], case_mode="uppercase").collect()
    assert result["name"].to_list() == ["ALICE", "B0B!", ""]
    assert result["id"].to_list() == [1, 2, None]
    assert result["score"].to_list() == [10.5, None, 3.0]


def test_data_cleansing_accepts_a_single_column_name(df):
    result = df.data_cleansing("name", case_mode="lowercase")
    assert result.get_node_settings().setting_input.cleansing_input.selected_columns == ["name"]
    assert result.collect()["name"].to_list() == ["alice", "b0b!", ""]


def test_data_cleansing_ignores_missing_selected_columns(df):
    result = df.data_cleansing(["name", "does_not_exist"], case_mode="uppercase").collect()
    assert result.columns == ["id", "name", "score", "active"]
    assert result["name"].to_list() == ["ALICE", "B0B!", ""]


def test_data_cleansing_remove_all_whitespace_supersedes_trim_and_normalize():
    result = (
        FlowFrame({"txt": ["  a\tb\nc  "]})
        .data_cleansing(remove_all_whitespace=True, normalize_whitespace=True, trim_whitespace=True)
        .collect()
    )
    assert result["txt"].to_list() == ["abc"]


def test_data_cleansing_removes_unicode_letters():
    result = FlowFrame({"txt": ["Àbé ö9"]}).data_cleansing(remove_letters=True, trim_whitespace=False).collect()
    assert result["txt"].to_list() == [" 9"]


def test_data_cleansing_leaves_boolean_and_date_columns_alone():
    data = {"flag": [True, None], "day": [datetime.date(2024, 1, 1), None]}
    result = FlowFrame(data).data_cleansing().collect()
    assert_frame_equal(result, pl.DataFrame(data))


def test_data_cleansing_leaves_null_dtype_columns_alone():
    """A Null-typed column is neither String nor Numeric, so no rule touches it."""
    result = FlowFrame({"a": [None, None], "b": ["x", None]}).data_cleansing().collect()
    assert result["a"].to_list() == [None, None]
    assert result["b"].to_list() == ["x", ""]


def test_data_cleansing_description_defaults_to_a_rule_summary(df):
    auto = df.data_cleansing(case_mode="uppercase").get_node_settings().get_node_information().description
    assert "uppercase" in auto
    custom = df.data_cleansing(description="clean it up").get_node_settings().get_node_information().description
    assert custom == "clean it up"
