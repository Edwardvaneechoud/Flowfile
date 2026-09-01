import string
from datetime import date
from decimal import Decimal

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import transform_schema


def _engine(data: dict, schema: dict | None = None) -> FlowDataEngine:
    return FlowDataEngine(pl.DataFrame(data, schema=schema))


def _clean(engine: FlowDataEngine, **kwargs) -> pl.DataFrame:
    settings = transform_schema.DataCleansingInput(**kwargs)
    return engine.apply_data_cleansing(settings).collect()


def _text(value: str, **kwargs) -> str:
    return _clean(_engine({"txt": [value]}), **kwargs)["txt"][0]


# Stage A - remove null rows


def test_remove_null_rows_drops_only_fully_null_rows():
    engine = _engine({"a": ["x", None, None], "b": [1, 2, None]})
    result = _clean(
        engine,
        remove_null_rows=True,
        replace_nulls_with_blank=False,
        replace_nulls_with_zero=False,
    )
    assert result.to_dicts() == [{"a": "x", "b": 1}, {"a": None, "b": 2}]


def test_remove_null_rows_keeps_row_with_empty_string():
    engine = _engine({"a": ["", None], "b": [None, None]})
    result = _clean(
        engine,
        remove_null_rows=True,
        replace_nulls_with_blank=False,
        replace_nulls_with_zero=False,
    )
    assert result.to_dicts() == [{"a": "", "b": None}]


def test_remove_null_rows_off_by_default():
    engine = _engine({"a": [None, None], "b": [None, None]})
    result = _clean(engine, replace_nulls_with_blank=False, replace_nulls_with_zero=False)
    assert result.height == 2


# Stage B - remove null columns


def test_remove_null_columns_drops_all_null_column():
    engine = _engine({"keep": ["x", "y"], "drop": [None, None]}, schema={"keep": pl.String, "drop": pl.String})
    result = _clean(engine, remove_null_columns=True)
    assert result.columns == ["keep"]


def test_remove_null_columns_keeps_empty_string_column():
    engine = _engine({"keep": ["", ""], "drop": [None, None]}, schema={"keep": pl.String, "drop": pl.String})
    result = _clean(engine, remove_null_columns=True)
    assert result.columns == ["keep"]


def test_remove_null_columns_keeps_partially_null_column():
    engine = _engine({"a": ["x", None]})
    result = _clean(engine, remove_null_columns=True, replace_nulls_with_blank=False)
    assert result.columns == ["a"]


def test_remove_null_columns_drops_nothing_on_zero_record_frame():
    engine = _engine({"a": [], "b": []}, schema={"a": pl.String, "b": pl.String})
    result = _clean(engine, remove_null_columns=True)
    assert result.columns == ["a", "b"]


def test_remove_null_columns_drops_nothing_on_zero_height_lazy_frame():
    engine = FlowDataEngine(pl.LazyFrame(schema={"a": pl.String, "b": pl.Int64}))
    result = _clean(engine, remove_null_columns=True)
    assert result.columns == ["a", "b"]


def test_remove_null_columns_ignores_column_selection():
    engine = _engine({"sel": ["x", "y"], "other": [None, None]}, schema={"sel": pl.String, "other": pl.String})
    result = _clean(engine, remove_null_columns=True, selection_mode="list", selected_columns=["sel"])
    assert result.columns == ["sel"]


def test_remove_null_rows_and_columns_together():
    engine = _engine(
        {"a": ["x", None, None], "b": [None, None, None], "c": [1, None, 2]},
        schema={"a": pl.String, "b": pl.String, "c": pl.Int64},
    )
    result = _clean(engine, remove_null_rows=True, remove_null_columns=True)
    assert result.to_dicts() == [{"a": "x", "c": 1}, {"a": "", "c": 2}]


# Null replacement


def test_blank_fill_applies_to_strings_only():
    engine = _engine({"txt": [None], "num": [None]}, schema={"txt": pl.String, "num": pl.Int64})
    result = _clean(engine, replace_nulls_with_blank=True, replace_nulls_with_zero=False)
    assert result.to_dicts() == [{"txt": "", "num": None}]


def test_zero_fill_applies_to_numerics_only():
    engine = _engine({"txt": [None], "num": [None]}, schema={"txt": pl.String, "num": pl.Int64})
    result = _clean(engine, replace_nulls_with_blank=False, replace_nulls_with_zero=True)
    assert result.to_dicts() == [{"txt": None, "num": 0}]


def test_zero_fill_keeps_float_dtype():
    engine = _engine({"num": [None, 1.5]}, schema={"num": pl.Float64})
    result = _clean(engine, replace_nulls_with_zero=True)
    assert result.schema["num"] == pl.Float64
    assert result["num"].to_list() == [0.0, 1.5]


def test_zero_fill_preserves_every_numeric_dtype():
    """A literal `fill_null(0)` widens Decimal(p, s) to Decimal(38, s); the zero strategy does not."""
    schema = {
        "money": pl.Decimal(10, 2),
        "whole": pl.Decimal(5, 0),
        "i8": pl.Int8,
        "u32": pl.UInt32,
        "f32": pl.Float32,
    }
    engine = _engine({name: [1, None] for name in schema}, schema=schema)
    result = _clean(engine, replace_nulls_with_zero=True)
    assert dict(result.schema) == schema
    assert result["money"].to_list() == [Decimal("1.00"), Decimal("0.00")]
    assert result["i8"].to_list() == [1, 0]


# Column selection


def test_selection_all_touches_every_column():
    engine = _engine({"a": ["  x  "], "b": ["  y  "]})
    result = _clean(engine, trim_whitespace=True)
    assert result.to_dicts() == [{"a": "x", "b": "y"}]


def test_selection_list_leaves_other_columns_untouched():
    engine = _engine({"a": ["  x  "], "b": ["  y  "]})
    result = _clean(engine, trim_whitespace=True, selection_mode="list", selected_columns=["a"])
    assert result.to_dicts() == [{"a": "x", "b": "  y  "}]


def test_selection_list_ignores_missing_column():
    engine = _engine({"a": ["  x  "]})
    result = _clean(
        engine,
        trim_whitespace=True,
        selection_mode="list",
        selected_columns=["a", "does_not_exist"],
    )
    assert result.to_dicts() == [{"a": "x"}]


def test_selection_list_empty_selects_nothing():
    engine = _engine({"a": ["  x  "]})
    result = _clean(engine, trim_whitespace=True, selection_mode="list", selected_columns=[])
    assert result.to_dicts() == [{"a": "  x  "}]


# Whitespace


def test_trim_strips_leading_and_trailing_only():
    assert _text("  a  b  ", trim_whitespace=True) == "a  b"


def test_normalize_collapses_tabs_newlines_and_runs():
    assert _text("a\tb\nc   d", trim_whitespace=False, normalize_whitespace=True) == "a b c d"


def test_normalize_without_trim_collapses_padding_to_single_space():
    assert _text("  a  ", trim_whitespace=False, normalize_whitespace=True) == " a "


def test_normalize_with_trim_removes_padding():
    assert _text("  a\t\tb  ", trim_whitespace=True, normalize_whitespace=True) == "a b"


def test_remove_all_whitespace():
    assert _text(" a\tb\nc ", remove_all_whitespace=True) == "abc"


def test_remove_all_whitespace_supersedes_trim_and_normalize():
    assert (
        _text(" a\tb ", remove_all_whitespace=True, trim_whitespace=True, normalize_whitespace=True) == "ab"
    )


def test_no_whitespace_rule_leaves_value_untouched():
    assert _text("  a  b  ", trim_whitespace=False) == "  a  b  "


# Character removal


def test_remove_letters_covers_non_latin_characters():
    assert _text("AbZ À é ö 123", remove_letters=True, trim_whitespace=False) == "    123"


def test_remove_numbers():
    assert _text("a1b23c", remove_numbers=True) == "abc"


def test_remove_punctuation_removes_every_ascii_punctuation_char():
    for char in string.punctuation:
        assert _text(f"a{char}b", remove_punctuation=True) == "ab", char


def test_remove_punctuation_leaves_alphanumerics_and_space():
    keep = string.ascii_letters + string.digits
    assert _text(string.punctuation + keep, remove_punctuation=True) == keep


# Case


def test_uppercase():
    assert _text("aB c", case_mode="uppercase") == "AB C"


def test_lowercase():
    assert _text("aB C", case_mode="lowercase") == "ab c"


def test_titlecase():
    assert _text("hello wide world", case_mode="titlecase") == "Hello Wide World"


# Operation order


def test_character_removal_runs_before_whitespace_cleanup():
    result = _text(
        "abc 123 def",
        remove_letters=True,
        normalize_whitespace=True,
        trim_whitespace=True,
    )
    assert result == "123"


def test_case_applies_after_character_removal():
    assert _text("a-b", remove_punctuation=True, case_mode="uppercase") == "AB"


# Data-type scoping


def test_boolean_and_date_columns_are_untouched():
    engine = _engine(
        {"flag": [True, None], "when": [date(2024, 1, 1), None], "txt": ["  x  ", None]},
        schema={"flag": pl.Boolean, "when": pl.Date, "txt": pl.String},
    )
    result = _clean(
        engine,
        trim_whitespace=True,
        replace_nulls_with_blank=True,
        replace_nulls_with_zero=True,
        case_mode="uppercase",
    )
    assert result.to_dicts() == [
        {"flag": True, "when": date(2024, 1, 1), "txt": "X"},
        {"flag": None, "when": None, "txt": ""},
    ]


def test_dtypes_are_never_changed():
    engine = _engine(
        {"txt": ["a"], "num": [1], "flt": [1.5], "flag": [True]},
        schema={"txt": pl.String, "num": pl.Int32, "flt": pl.Float32, "flag": pl.Boolean},
    )
    result = _clean(engine, case_mode="uppercase")
    assert dict(result.schema) == {
        "txt": pl.String,
        "num": pl.Int32,
        "flt": pl.Float32,
        "flag": pl.Boolean,
    }


# Schema-only prediction placeholder


def test_schema_only_placeholder_keeps_full_schema():
    source = _engine(
        {"a": ["x"], "b": [1], "c": [None]},
        schema={"a": pl.String, "b": pl.Int64, "c": pl.String},
    )
    placeholder = FlowDataEngine.create_from_schema(source.schema)
    settings = transform_schema.DataCleansingInput(
        remove_null_rows=True,
        remove_null_columns=True,
        remove_punctuation=True,
        case_mode="uppercase",
    )
    result = placeholder.apply_data_cleansing(settings)
    assert [c.column_name for c in result.schema] == ["a", "b", "c"]
    assert result.collect().height == 0


# Pure expression builder


def test_expression_builder_skips_untargeted_and_unaffected_columns():
    columns = [("txt", "String"), ("num", "Numeric"), ("flag", "Boolean")]
    settings = transform_schema.DataCleansingInput(
        replace_nulls_with_blank=False,
        replace_nulls_with_zero=False,
        trim_whitespace=False,
    )
    assert FlowDataEngine.build_data_cleansing_expressions(columns, settings) == []


def test_expression_builder_emits_one_expression_per_affected_column():
    columns = [("txt", "String"), ("num", "Numeric"), ("flag", "Boolean")]
    settings = transform_schema.DataCleansingInput()
    assert len(FlowDataEngine.build_data_cleansing_expressions(columns, settings)) == 2
