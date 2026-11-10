"""Expressions whose arguments are functions, frame expressions or Polars' own defaults."""

import polars as pl

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


def test_list_to_struct_names_its_fields():
    df = ff.from_dict({"l": [[1, 2], [3, 4]]})

    result = df.select(ff.col("l").list.to_struct(["a", "b"])).collect()

    assert result.schema["l"] == pl.Struct({"a": pl.Int64, "b": pl.Int64})
    assert result["l"].to_list() == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
