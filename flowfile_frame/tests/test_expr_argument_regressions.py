"""Expressions whose arguments are functions, frame expressions or Polars' own defaults."""

import polars as pl
import pytest

import flowfile as ff


def summ(series):
    return series.sum()


def big(value):
    return value > 1


def test_a_named_function_argument_is_written_by_name():
    df = ff.from_dict({"a": [1, 2, 3]})

    expr = ff.col("a").rolling_map(summ, window_size=2)

    assert "<function" not in str(expr)
    assert "summ" in str(expr)
    assert df.with_columns(expr.alias("r")).collect()["r"].to_list() == [None, 3, 5]


def test_a_filter_constraint_may_be_a_frame_expression():
    df = ff.from_dict({"a": [1, 2, 3], "c": [1, 0, 3]})

    expr = ff.col("a").filter(c=ff.col("a"))

    assert df.select(expr).collect()["a"].to_list() == [1, 3]


def test_a_filter_predicate_carries_its_function_sources():
    df = ff.from_dict({"a": [1, 2, 3]})
    predicate = ff.col("a").map_elements(big, return_dtype=pl.Boolean)

    expr = ff.col("a").filter(predicate)

    assert expr._function_sources == predicate._function_sources != []
    assert df.select(expr.sum()).collect().to_dicts() == [{"a": 5}]


def test_list_to_struct_takes_polars_defaults():
    df = ff.from_dict({"l": [[1, 2], [3, 4]]})

    with pytest.raises(Exception, match="upper_bound"):
        df.select(ff.col("l").list.to_struct()).collect()
    widened = df.select(ff.col("l").list.to_struct(upper_bound=2)).collect()

    assert widened.schema["l"] == pl.Struct({"field_0": pl.Int64, "field_1": pl.Int64})
    assert "first_non_null" not in str(ff.col("l").list.to_struct(upper_bound=2))
