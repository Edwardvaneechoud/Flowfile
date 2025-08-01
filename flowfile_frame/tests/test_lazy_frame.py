from __future__ import annotations

import sys

from datetime import date, datetime
from functools import reduce
from operator import add
from string import ascii_letters
from typing import TYPE_CHECKING, Any, Callable, NoReturn, cast
import numpy as np
import polars as pl
import pytest
from polars import NUMERIC_DTYPES, FLOAT_DTYPES
from polars.exceptions import (
    InvalidOperationError,
    PolarsInefficientMapWarning,
)
from polars.testing import assert_frame_equal, assert_series_equal

import flowfile_frame as fl
from flowfile_frame.flow_frame import FlowFrame


@pytest.fixture
def fruits_cars() -> FlowFrame:
    return FlowFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        },
        schema_overrides={"A": pl.Int64, "B": pl.Int64},
    )

@pytest.fixture
def df() -> FlowFrame:
    df = pl.DataFrame(
        {
            "bools": [False, True, False],
            "bools_nulls": [None, True, False],
            "int": [1, 2, 3],
            "int_nulls": [1, None, 3],
            "floats": [1.0, 2.0, 3.0],
            "floats_nulls": [1.0, None, 3.0],
            "strings": ["foo", "bar", "ham"],
            "strings_nulls": ["foo", None, "ham"],
            "date": [1324, 123, 1234],
            "datetime": [13241324, 12341256, 12341234],
            "time": [13241324, 12341256, 12341234],
            "list_str": [["a", "b", None], ["a"], []],
            "list_bool": [[True, False, None], [None], []],
            "list_int": [[1, None, 3], [None], []],
            "list_flt": [[1.0, None, 3.0], [None], []],
        }
    )
    return FlowFrame(df)


if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture

    from polars._typing import PolarsDataType


def test_lazy_misc() -> None:
    ldf = FlowFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
    _ = ldf.with_columns(fl.lit(1).alias("foo")).select([fl.col("a"), fl.col("foo")])

    # test if it executes
    _ = ldf.with_columns(
        fl.when(fl.col("a") > fl.lit(2)).then(fl.lit(10)).otherwise(fl.lit(1)).alias("new")
    ).collect()


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Skipping on Windows")
def test_implode() -> None:
    ldf = FlowFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
    eager = (
        ldf.group_by(fl.col("a").alias("grp"), maintain_order=True)
        .agg(fl.implode("a", "b").name.suffix("_imp"))
        .collect()
    )

    assert_frame_equal(
        eager,
        pl.DataFrame(
            {
                "grp": [1, 2, 3],
                "a_imp": [[1], [2], [3]],
                "b_imp": [[1.0], [2.0], [3.0]],
            }
        ),
    )


def test_lazyframe_membership_operator() -> None:
    ldf = FlowFrame({"name": ["Jane", "John"], "age": [20, 30]})
    assert "name" in ldf
    assert "phone" not in ldf

    # note: cannot use lazyframe in boolean context
    with pytest.raises(TypeError, match="ambiguous"):
        not ldf


def test_apply() -> None:
    ldf = FlowFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
    new = ldf.with_columns(
        fl.col("a").map_batches(lambda s: s * 2, return_dtype=pl.Int64).alias("foo"),
    )
    expected = ldf.data.clone().with_columns((pl.col("a") * 2).alias("foo"))
    assert_frame_equal(new.data, expected)
    assert_frame_equal(new.collect(), expected.collect())
    with pytest.warns(PolarsInefficientMapWarning, match="with this one instead"):
        for strategy in ["thread_local", "threading"]:
            ldf = FlowFrame({"a": [1, 2, 3] * 20, "b": [1.0, 2.0, 3.0] * 20})
            new = ldf.with_columns(
                fl.col("a")
                .map_elements(lambda s: s * 2, strategy=strategy, return_dtype=pl.Int64)  # type: ignore[arg-type]
                .alias("foo")
            )
            expected = ldf.data.clone().with_columns((pl.col("a") * 2).alias("foo"))
            assert_frame_equal(new.collect(), expected.collect())


def test_add_eager_column() -> None:
    lf = FlowFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
    assert lf.collect_schema().len() == 2

    out = lf.with_columns(fl.lit(fl.Series("c", [1, 2, 3]))).collect()
    assert out["c"].sum() == 6
    assert out.collect_schema().len() == 3


def test_set_null() -> None:
    ldf = FlowFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
    out = ldf.with_columns(
        fl.when(fl.col("a") > 1).then(fl.lit(None)).otherwise(100).alias("foo")
    ).collect()
    s = out["foo"]
    assert s[0] == 100
    assert s[1] is None
    assert s[2] is None


def test_gather_every() -> None:
    ldf = FlowFrame({"a": [1, 2, 3, 4], "b": ["w", "x", "y", "z"]})
    expected_df = pl.DataFrame({"a": [1, 3], "b": ["w", "y"]})
    assert_frame_equal(expected_df, ldf.gather_every(2).collect())
    expected_df = pl.DataFrame({"a": [2, 4], "b": ["x", "z"]})
    assert_frame_equal(expected_df, ldf.gather_every(2, offset=1).collect())


def test_agg() -> None:
    df = FlowFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
    ldf = df.lazy().min()
    res = ldf.collect()
    assert res.shape == (1, 2)
    assert res.row(0) == (1, 1.0)


def test_count_suffix_10783() -> None:
    df = FlowFrame(
        {
            "a": [["a", "c", "b"], ["a", "b", "c"], ["a", "d", "c"], ["c", "a", "b"]],
            "b": [["a", "c", "b"], ["a", "b", "c"], ["a", "d", "c"], ["c", "a", "b"]],
        }
    )
    df_with_cnt = df.with_columns(
        fl.len()
        .over(fl.col("a").list.sort().list.join("").hash())
        .name.suffix("_suffix")
    )
    df_expect = df.with_columns(fl.Series("len_suffix", [3, 3, 1, 3]))
    assert_frame_equal(df_with_cnt.data, df_expect.data, check_dtypes=False)


def test_or() -> None:
    ldf = FlowFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
    out = ldf.filter((fl.col("a") == 1) | (fl.col("b") > 2)).collect()
    assert out.rows() == [(1, 1.0), (3, 3.0)]


def test_filter_str() -> None:
    # use a str instead of a column expr
    ldf = FlowFrame(
        {
            "time": ["11:11:00", "11:12:00", "11:13:00", "11:14:00"],
            "bools": [True, False, True, False],
        }
    )
    result = ldf.filter(fl.col("bools")).select_seq(fl.last("*")).collect()
    expected = pl.DataFrame({"time": ["11:13:00"], "bools": [True]})
    assert_frame_equal(result, expected)

    # last row based on a filter
    result = ldf.filter("bools").select(fl.last("*")).collect()
    assert_frame_equal(result, expected)


def test_filter_multiple_predicates() -> None:
    ldf = FlowFrame(
        {
            "a": [1, 1, 1, 2, 2],
            "b": [1, 1, 2, 2, 2],
            "c": [1, 1, 2, 3, 4],
        }
    )

    # multiple predicates
    expected = pl.DataFrame({"a": [1, 1, 1], "b": [1, 1, 2], "c": [1, 1, 2]})
    for out in (
        ldf.filter(fl.col("a") == 1, fl.col("b") <= 2),  # positional/splat
        ldf.filter([fl.col("a") == 1, fl.col("b") <= 2]),  # as list
    ):
        assert_frame_equal(out.collect(), expected)

    # multiple kwargs
    assert_frame_equal(
        ldf.filter(a=1, b=2).collect(),
        pl.DataFrame({"a": [1], "b": [2], "c": [2]}),
    )

    # both positional and keyword args
    assert_frame_equal(
        ldf.filter(fl.col("c") < 4, a=2, b=2).collect(),
        pl.DataFrame({"a": [2], "b": [2], "c": [3]}),
    )
    ldf = FlowFrame(
        {
            "description": ["eq", "gt", "ge"],
            "predicate": ["==", ">", ">="],
        },
    )
    assert ldf.filter(predicate="==").select("description").collect().item() == "eq"


@pytest.mark.parametrize(
    "predicate",
    [
        [fl.lit(True)],
        iter([fl.lit(True)]),
        [True, True, True],
        iter([True, True, True]),
        (p for p in (fl.col("c") < 9,)),
        (p for p in (fl.col("a") > 0, fl.col("b") > 0)),
    ],
)

def test_filter_seq_iterable_all_true(predicate: Any) -> None:
    ldf = FlowFrame(
        {
            "a": [1, 1, 1],
            "b": [1, 1, 2],
            "c": [3, 1, 2],
        }
    )
    assert_frame_equal(ldf.data, ldf.filter(predicate).data)


def test_apply_custom_function() -> None:
    ldf = FlowFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    )
    # two ways to determine the length groups.
    df = (
        ldf.group_by("fruits")
        .agg(
            [
                fl.col("cars")
                .map_elements(lambda groups: groups.len(), return_dtype=pl.Int64)
                .alias("custom_1"),
                fl.col("cars")
                .map_elements(lambda groups: groups.len(), return_dtype=pl.Int64)
                .alias("custom_2"),
                fl.count("cars").alias("cars_count"),
            ]

        )
        .sort("custom_1", descending=True)
    ).collect()

    expected = pl.DataFrame(
        {
            "fruits": ["banana", "apple"],
            "custom_1": [3, 2],
            "custom_2": [3, 2],
            "cars_count": [3, 2],
        }
    )
    expected = expected.with_columns(pl.col("cars_count").cast(pl.UInt32))
    assert_frame_equal(df, expected)


def test_group_by() -> None:
    ldf = FlowFrame(
        {
            "a": [1.0, None, 3.0, 4.0],
            "b": [5.0, 2.5, -3.0, 2.0],
            "grp": ["a", "a", "b", "b"],
        }
    )
    expected_a = pl.DataFrame({"grp": ["a", "b"], "a": [1.0, 3.5]})
    expected_a_b = pl.DataFrame({"grp": ["a", "b"], "a": [1.0, 3.5], "b": [3.75, -0.5]})
    #
    for out in (
        ldf.group_by("grp").agg(fl.mean("a")).collect(),
        ldf.group_by(fl.col("grp")).agg(fl.mean("a")).collect(),
    ):
        assert_frame_equal(out.sort(by="grp"), expected_a)

    out = ldf.group_by("grp").agg(fl.mean("a", "b")).collect()
    assert_frame_equal(out.sort(by="grp"), expected_a_b)


def test_arg_unique() -> None:
    ldf = FlowFrame({"a": [4, 1, 4]})
    col_a_unique = ldf.select(fl.col("a").arg_unique()).collect()["a"]
    assert_series_equal(col_a_unique, pl.Series("a", [0, 1]).cast(pl.UInt32))


def test_arg_sort() -> None:
    ldf = FlowFrame({"a": [4, 1, 3]}).select(fl.col("a").arg_sort(nulls_last=True))
    assert ldf.collect()["a"].to_list() == [1, 2, 0]


def test_window_function() -> None:
    lf = FlowFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    )
    assert lf.collect_schema().len() == 4
    q = lf.with_columns(
        fl.sum("A").over("fruits").alias("fruit_sum_A"),
        fl.first("B").over("fruits").alias("fruit_first_B"),
        fl.max("B").over("cars").alias("cars_max_B"),
    )
    assert q.collect_schema().len() == 7

    assert q.collect()["cars_max_B"].to_list() == [5, 4, 5, 5, 5]

    out = lf.select([fl.first("B").over(["fruits", "cars"]).alias("B_first")])
    assert out.collect()["B_first"].to_list() == [5, 4, 3, 3, 5]


def test_when_then_flatten() -> None:
    ldf = FlowFrame({"foo": [1, 2, 3], "bar": [3, 4, 5]})

    assert ldf.select(
        fl.when(fl.col("foo") > 1).then(fl.col("bar")).when(fl.col("bar") < 3).then(10).otherwise(30)
    ).collect()["bar"].to_list() == [30, 4, 5]


def test_describe_plan() -> None:
    assert isinstance(FlowFrame({"a": [1]}).explain(optimized=True), str)
    assert isinstance(FlowFrame({"a": [1]}).explain(optimized=False), str)


def test_inspect(capsys: CaptureFixture[str]) -> None:
    ldf = FlowFrame({"a": [1]})
    ldf.inspect().collect()
    captured = capsys.readouterr()
    assert len(captured.out) > 0

    ldf.select(fl.col("a").cum_sum().inspect().alias("bar")).collect()
    res = capsys.readouterr()
    assert len(res.out) > 0


@pytest.mark.may_fail_auto_streaming
def test_fetch(fruits_cars: FlowFrame) -> None:
    res = fruits_cars.lazy().select("*").fetch(2)
    assert_frame_equal(res, res[:2])


def test_fold_filter() -> None:

    def f(a, b):
        return a&b
    lf = FlowFrame({"a": [1, 2, 3], "b": [0, 1, 2]})
    out = lf.filter(
        fl.fold(
            acc=fl.lit(True),
            function=f,
            exprs=[fl.col(c) > 1 for c in lf.collect_schema()],
        )
    ).collect()
    assert out.shape == (1, 2)
    assert out.rows() == [(3, 2)]

    def or_func(a, b):
        return a | b

    out = lf.filter(
        fl.fold(
            acc=fl.lit(True),
            function=or_func,
            exprs=[fl.col(c) > 1 for c in lf.collect_schema()],
        )
    ).collect()
    assert out.rows() == [(1, 0), (2, 1), (3, 2)]


def test_head_group_by() -> None:
    commodity_prices = {
        "commodity": [
            "Wheat",
            "Wheat",
            "Wheat",
            "Wheat",
            "Corn",
            "Corn",
            "Corn",
            "Corn",
            "Corn",
        ],
        "location": [
            "StPaul",
            "StPaul",
            "StPaul",
            "Chicago",
            "Chicago",
            "Chicago",
            "Chicago",
            "Chicago",
            "Chicago",
        ],
        "seller": [
            "Bob",
            "Charlie",
            "Susan",
            "Paul",
            "Ed",
            "Mary",
            "Paul",
            "Charlie",
            "Norman",
        ],
        "price": [1.0, 0.7, 0.8, 0.55, 2.0, 3.0, 2.4, 1.8, 2.1],
    }
    ldf = FlowFrame(commodity_prices)
    # this query flexes the wildcard exclusion quite a bit.
    keys = ["commodity", "location"]
    out = (
        ldf.sort(by="price", descending=True)
        .group_by(keys, maintain_order=True)
        .agg([fl.col("*").exclude(keys).head(2).name.keep()])
        .explode(fl.col("*").exclude(keys))
    )
    assert out.collect().rows() == [
        ("Corn", "Chicago", "Mary", 3.0),
        ("Corn", "Chicago", "Paul", 2.4),
        ("Wheat", "StPaul", "Bob", 1.0),
        ("Wheat", "StPaul", "Susan", 0.8),
        ("Wheat", "Chicago", "Paul", 0.55),
    ]

    ldf = FlowFrame(
        {"letters": ["c", "c", "a", "c", "a", "b"], "nrs": [1, 2, 3, 4, 5, 6]}
    )
    out = ldf.group_by("letters").tail(2).sort("letters")
    assert_frame_equal(
        out.collect(),
        pl.DataFrame({"letters": ["a", "a", "b", "c", "c"], "nrs": [3, 5, 6, 2, 4]}),
    )
    out = ldf.group_by("letters").head(2).sort("letters")
    assert_frame_equal(
        out.collect(),
        pl.DataFrame({"letters": ["a", "a", "b", "c", "c"], "nrs": [3, 5, 6, 1, 2]}),
    )


def test_is_null_is_not_null() -> None:
    ldf = FlowFrame({"nrs": [1, 2, None]}).select(
        fl.col("nrs").is_null().alias("is_null"),
        fl.col("nrs").is_not_null().alias("not_null"),
    )
    assert ldf.collect()["is_null"].to_list() == [False, False, True]
    assert ldf.collect()["not_null"].to_list() == [True, True, False]


def test_is_nan_is_not_nan() -> None:
    ldf = FlowFrame({"nrs": np.array([1, 2, np.nan])}).select(
        fl.col("nrs").is_nan().alias("is_nan"),
        fl.col("nrs").is_not_nan().alias("not_nan"),
    )
    assert ldf.collect()["is_nan"].to_list() == [False, False, True]
    assert ldf.collect()["not_nan"].to_list() == [True, True, False]


def test_is_finite_is_infinite() -> None:
    ldf = FlowFrame({"nrs": np.array([1, 2, np.inf])}).select(
        fl.col("nrs").is_infinite().alias("is_inf"),
        fl.col("nrs").is_finite().alias("not_inf"),
    )
    assert ldf.collect()["is_inf"].to_list() == [False, False, True]
    assert ldf.collect()["not_inf"].to_list() == [True, True, False]


def test_len() -> None:
    ldf = FlowFrame({"nrs": [1, 2, 3]})
    assert cast(int, ldf.select(fl.col("nrs").len()).collect().item()) == 3


@pytest.mark.parametrize("dtype", NUMERIC_DTYPES)
def test_cum_agg(dtype: PolarsDataType) -> None:
    ldf = FlowFrame({"a": [1, 2, 3, 2]}, schema={"a": dtype})
    assert_series_equal(
        ldf.select(fl.col("a").cum_min()).collect()["a"],
        pl.Series("a", [1, 1, 1, 1], dtype=dtype),
    )
    assert_series_equal(
        ldf.select(fl.col("a").cum_max()).collect()["a"],
        pl.Series("a", [1, 2, 3, 3], dtype=dtype),
    )

    expected_dtype = (
        pl.Int64 if dtype in [pl.Int8, pl.Int16, pl.UInt8, pl.UInt16] else dtype
    )
    assert_series_equal(
        ldf.select(fl.col("a").cum_sum()).collect()["a"],
        pl.Series("a", [1, 3, 6, 8], dtype=expected_dtype),
    )

    expected_dtype = (
        pl.Int64
        if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.UInt8, pl.UInt16, pl.UInt32]
        else dtype
    )
    if not expected_dtype == pl.Decimal:
        assert_series_equal(
            ldf.select(fl.col("a").cum_prod()).collect()["a"],
            pl.Series("a", [1, 2, 6, 12], dtype=expected_dtype),
        )


def test_ceil() -> None:
    ldf = FlowFrame({"a": [1.8, 1.2, 3.0]})
    result = ldf.select(fl.col("a").ceil()).collect()
    assert_frame_equal(result, pl.DataFrame({"a": [2.0, 2.0, 3.0]}))

    ldf = FlowFrame({"a": [1, 2, 3]})
    result = ldf.select(fl.col("a").ceil()).collect()
    assert_frame_equal(ldf.collect(), result)


def test_floor() -> None:
    ldf = FlowFrame({"a": [1.8, 1.2, 3.0]})
    result = ldf.select(fl.col("a").floor()).collect()
    assert_frame_equal(result, pl.DataFrame({"a": [1.0, 1.0, 3.0]}))

    ldf = FlowFrame({"a": [1, 2, 3]})
    result = ldf.select(fl.col("a").floor()).collect()
    assert_frame_equal(ldf.collect(), result)


@pytest.mark.parametrize(
    ("n", "ndigits", "expected"),
    [
        (1.005, 2, 1.0),
        (1234.00000254495, 10, 1234.000002545),
        (1835.665, 2, 1835.67),
        (-1835.665, 2, -1835.67),
        (1.27499, 2, 1.27),
        (123.45678, 2, 123.46),
        (1254, 2, 1254.0),
        (1254, 0, 1254.0),
        (123.55, 0, 124.0),
        (123.55, 1, 123.6),
        (-1.23456789, 6, -1.234568),
        (1.0e-5, 5, 0.00001),
        (1.0e-20, 20, 1e-20),
        (1.0e20, 2, 100000000000000000000.0),
    ],
)
@pytest.mark.parametrize("dtype", FLOAT_DTYPES)
def test_round(n: float, ndigits: int, expected: float, dtype: pl.DataType) -> None:
    ldf = FlowFrame({"value": [n]}, schema_overrides={"value": dtype})
    assert_series_equal(
        ldf.select(fl.col("value").round(decimals=ndigits)).collect().to_series(),
        pl.Series("value", [expected], dtype=dtype),
    )


def test_dot() -> None:
    ldf = FlowFrame({"a": [1.8, 1.2, 3.0], "b": [3.2, 1, 2]}).select(
        fl.col("a").dot(fl.col("b"))
    )
    assert cast(float, ldf.collect().item()) == 12.96


def test_sort() -> None:
    ldf = FlowFrame({"a": [1, 2, 3, 2]}).select(fl.col("a").sort())
    assert_series_equal(ldf.collect()["a"], pl.Series("a", [1, 2, 2, 3]))


def test_custom_group_by() -> None:
    ldf = FlowFrame({"a": [1, 2, 1, 1], "b": ["a", "b", "c", "c"]})
    out = (
        ldf.group_by("b", maintain_order=True)
        .agg([fl.col("a").map_elements(lambda x: x.sum(), return_dtype=pl.Int64)])
        .collect()
    )
    assert out.rows() == [("a", 1), ("b", 2), ("c", 2)]


def test_lazy_columns() -> None:
    lf = FlowFrame(
        {
            "a": [1],
            "b": [1],
            "c": [1],
        }
    )
    assert lf.select("a", "c").collect_schema().names() == ["a", "c"]


def test_cast_frame() -> None:
    lf = FlowFrame(
        {
            "a": [1.0, 2.5, 3.0],
            "b": [4, 5, None],
            "c": [True, False, True],
            "d": [date(2020, 1, 2), date(2021, 3, 4), date(2022, 5, 6)],
        }
    )
    # cast via col:dtype map
    assert lf.cast(
        dtypes={"b": pl.Float32, "c": pl.String, "d": pl.Datetime("ms")}
    ).collect_schema() == {
        "a": pl.Float64,
        "b": pl.Float32,
        "c": pl.String,
        "d": pl.Datetime("ms"),
    }

    # cast via selector:dtype map
    lfc = lf.cast(
        {
            fl.selectors.float_(): pl.UInt8,
            fl.selectors.integer(): pl.Int32,
            fl.selectors.temporal(): pl.String,
        }
    )
    assert lfc.collect_schema() == {
        "a": pl.UInt8,
        "b": pl.Int32,
        "c": pl.Boolean,
        "d": pl.String,
    }
    assert lfc.collect().rows() == [
        (1, 4, True, "2020-01-02"),
        (2, 5, False, "2021-03-04"),
        (3, None, True, "2022-05-06"),
    ]

    # cast all fields to a single type
    result = lf.cast(pl.String)
    expected = FlowFrame(
        {
            "a": ["1.0", "2.5", "3.0"],
            "b": ["4", "5", None],
            "c": ["true", "false", "true"],
            "d": ["2020-01-02", "2021-03-04", "2022-05-06"],
        }
    )
    assert_frame_equal(result.collect(), expected.collect())

    # test 'strict' mode
    lf = FlowFrame({"a": [1000, 2000, 3000]})

    with pytest.raises(InvalidOperationError, match="conversion .* failed"):
        lf.cast(pl.UInt8).collect()

    assert lf.cast(pl.UInt8, strict=False).collect().rows() == [
        (None,),
        (None,),
        (None,),
    ]


def test_interpolate() -> None:
    df = pl.DataFrame({"a": [1, None, 3]})
    assert df.select(pl.col("a").interpolate())["a"].to_list() == [1, 2, 3]
    assert df["a"].interpolate().to_list() == [1, 2, 3]
    assert df.interpolate()["a"].to_list() == [1, 2, 3]
    assert df.lazy().interpolate().collect()["a"].to_list() == [1, 2, 3]


def test_fill_nan() -> None:
    df = pl.DataFrame({"a": [1.0, np.nan, 3.0]})
    assert_series_equal(df.fill_nan(2.0)["a"], pl.Series("a", [1.0, 2.0, 3.0]))
    assert_series_equal(
        df.lazy().fill_nan(2.0).collect()["a"], pl.Series("a", [1.0, 2.0, 3.0])
    )
    assert_series_equal(
        df.lazy().fill_nan(None).collect()["a"], pl.Series("a", [1.0, None, 3.0])
    )
    assert_series_equal(
        df.select(pl.col("a").fill_nan(2))["a"], pl.Series("a", [1.0, 2.0, 3.0])
    )
    # nearest
    assert pl.Series([None, 1, None, None, None, -8, None, None, 10]).interpolate(
        method="nearest"
    ).to_list() == [None, 1, 1, -8, -8, -8, -8, 10, 10]


def test_fill_null() -> None:
    df = pl.DataFrame({"a": [1.0, None, 3.0]})

    assert df.select([pl.col("a").fill_null(strategy="min")])["a"][1] == 1.0
    assert df.lazy().fill_null(2).collect()["a"].to_list() == [1.0, 2.0, 3.0]

    with pytest.raises(ValueError, match="must specify either"):
        df.fill_null()
    with pytest.raises(ValueError, match="cannot specify both"):
        df.fill_null(value=3.0, strategy="max")
    with pytest.raises(ValueError, match="can only specify `limit`"):
        df.fill_null(strategy="max", limit=2)



@pytest.mark.skip(reason="Not implemented yet")
def test_backward_fill() -> None:
    ldf = FlowFrame({"a": [1.0, None, 3.0]})
    col_a_backward_fill = ldf.select(
        [pl.col("a").fill_null(strategy="backward")]
    ).collect()["a"]
    assert_series_equal(col_a_backward_fill, pl.Series("a", [1, 3, 3]).cast(pl.Float64))


def test_rolling(fruits_cars: FlowFrame) -> None:
    ldf = fruits_cars.lazy()
    out = ldf.select(
        fl.col("A").rolling_min(3, min_samples=1).alias("1"),
        fl.col("A").rolling_min(3).alias("1b"),
        fl.col("A").rolling_mean(3, min_samples=1).alias("2"),
        fl.col("A").rolling_mean(3).alias("2b"),
        fl.col("A").rolling_max(3, min_samples=1).alias("3"),
        fl.col("A").rolling_max(3).alias("3b"),
        fl.col("A").rolling_sum(3, min_samples=1).alias("4"),
        fl.col("A").rolling_sum(3).alias("4b"),
        # below we use .round purely for the ability to do assert frame equality
        fl.col("A").rolling_std(3).round(1).alias("std"),
        fl.col("A").rolling_var(3).round(1).alias("var"),
    )

    assert_frame_equal(
        out.collect(),
        pl.DataFrame(
            {
                "1": [1, 1, 1, 2, 3],
                "1b": [None, None, 1, 2, 3],
                "2": [1.0, 1.5, 2.0, 3.0, 4.0],
                "2b": [None, None, 2.0, 3.0, 4.0],
                "3": [1, 2, 3, 4, 5],
                "3b": [None, None, 3, 4, 5],
                "4": [1, 3, 6, 9, 12],
                "4b": [None, None, 6, 9, 12],
                "std": [None, None, 1.0, 1.0, 1.0],
                "var": [None, None, 1.0, 1.0, 1.0],
            }
        ),
    )

    out_single_val_variance = ldf.select(
        fl.col("A").rolling_std(3, min_samples=1).round(decimals=4).alias("std"),
        fl.col("A").rolling_var(3, min_samples=1).round(decimals=1).alias("var"),
    ).collect()

    assert cast(float, out_single_val_variance[0, "std"]) is None
    assert cast(float, out_single_val_variance[0, "var"]) is None


def test_arr_namespace(fruits_cars: FlowFrame) -> None:
    ldf = fruits_cars.lazy()
    out = ldf.select(
        "fruits",
        fl.col("B")
        .over("fruits", mapping_strategy="join")
        .list.min()
        .alias("B_by_fruits_min1"),
        fl.col("B")
        .min()
        .over("fruits", mapping_strategy="join")
        .alias("B_by_fruits_min2"),
        fl.col("B")
        .over("fruits", mapping_strategy="join")
        .list.max()
        .alias("B_by_fruits_max1"),
        fl.col("B")
        .max()
        .over("fruits", mapping_strategy="join")
        .alias("B_by_fruits_max2"),
        fl.col("B")
        .over("fruits", mapping_strategy="join")
        .list.sum()
        .alias("B_by_fruits_sum1"),
        fl.col("B")
        .sum()
        .over("fruits", mapping_strategy="join")
        .alias("B_by_fruits_sum2"),
        fl.col("B")
        .over("fruits", mapping_strategy="join")
        .list.mean()
        .alias("B_by_fruits_mean1"),
        fl.col("B")
        .mean()
        .over("fruits", mapping_strategy="join")
        .alias("B_by_fruits_mean2"),
    )
    expected = pl.DataFrame(
        {
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B_by_fruits_min1": [1, 1, 2, 2, 1],
            "B_by_fruits_min2": [1, 1, 2, 2, 1],
            "B_by_fruits_max1": [5, 5, 3, 3, 5],
            "B_by_fruits_max2": [5, 5, 3, 3, 5],
            "B_by_fruits_sum1": [10, 10, 5, 5, 10],
            "B_by_fruits_sum2": [10, 10, 5, 5, 10],
            "B_by_fruits_mean1": [
                3.3333333333333335,
                3.3333333333333335,
                2.5,
                2.5,
                3.3333333333333335,
            ],
            "B_by_fruits_mean2": [
                3.3333333333333335,
                3.3333333333333335,
                2.5,
                2.5,
                3.3333333333333335,
            ],
        }
    )
    assert_frame_equal(out.collect(), expected)


def test_arithmetic() -> None:
    ldf = FlowFrame({"a": [1, 2, 3]})
    out = ldf.select(
        (fl.col("a") % 2).alias("1"),
        (2 % fl.col("a")).alias("2"),
        (1 // fl.col("a")).alias("3"),
        (1 * fl.col("a")).alias("4"),
        (1 + fl.col("a")).alias("5"),
        (1 - fl.col("a")).alias("6"),
        (fl.col("a") // 2).alias("7"),
        (fl.col("a") * 2).alias("8"),
        (fl.col("a") + 2).alias("9"),
        (fl.col("a") - 2).alias("10"),
        (-fl.col("a")).alias("11"),
    )
    expected = pl.DataFrame(
        {
            "1": [1, 0, 1],
            "2": [0, 0, 2],
            "3": [1, 0, 0],
            "4": [1, 2, 3],
            "5": [2, 3, 4],
            "6": [0, -1, -2],
            "7": [0, 1, 1],
            "8": [2, 4, 6],
            "9": [3, 4, 5],
            "10": [-1, 0, 1],
            "11": [-1, -2, -3],
        }
    )
    assert_frame_equal(out.collect(), expected)


def test_float_floor_divide() -> None:
    x = 10.4
    step = 0.5
    ldf = FlowFrame({"x": [x]})
    ldf_res = ldf.with_columns(fl.col("x") // step).collect().item()
    assert ldf_res == x // step


def test_argminmax() -> None:
    ldf = FlowFrame({"a": [1, 2, 3, 4, 5], "b": [1, 1, 2, 2, 2]})
    out = ldf.select(
        fl.col("a").arg_min().alias("min"),
        fl.col("a").arg_max().alias("max"),
    ).collect()
    assert out["max"][0] == 4
    assert out["min"][0] == 0

    out = (
        ldf.group_by("b", maintain_order=True)
        .agg([fl.col("a").arg_min().alias("min"), fl.col("a").arg_max().alias("max")])
        .collect()
    )
    assert out["max"][0] == 1
    assert out["min"][0] == 0


def test_limit(fruits_cars: FlowFrame) -> None:
    assert_frame_equal(fruits_cars.lazy().limit(1).collect(), fruits_cars.data.collect()[0, :])


def test_head(fruits_cars: FlowFrame) -> None:
    assert_frame_equal(fruits_cars.lazy().head(2).collect(), fruits_cars.data.collect()[:2, :])


def test_tail(fruits_cars: FlowFrame) -> None:
    assert_frame_equal(fruits_cars.lazy().tail(2).collect(), fruits_cars.data.collect()[3:, :])


def test_last(fruits_cars: FlowFrame) -> None:
    result = fruits_cars.lazy().last().collect()
    expected = fruits_cars.data.collect()[(len(fruits_cars.data.collect()) - 1):, :]
    assert_frame_equal(result, expected)


def test_first(fruits_cars: FlowFrame) -> None:
    assert_frame_equal(fruits_cars.lazy().first().collect(), fruits_cars.data.collect()[0, :])


def test_join_suffix() -> None:
    df_left = pl.DataFrame(
        {
            "a": ["a", "b", "a", "z"],
            "b": [1, 2, 3, 4],
            "c": [6, 5, 4, 3],
        }
    )
    df_right = pl.DataFrame(
        {
            "a": ["b", "c", "b", "a"],
            "b": [0, 3, 9, 6],
            "c": [1, 0, 2, 1],
        }
    )
    out = df_left.join(df_right, on="a", suffix="_bar")
    assert out.columns == ["a", "b", "c", "b_bar", "c_bar"]
    out = df_left.lazy().join(df_right.lazy(), on="a", suffix="_bar").collect()
    assert out.columns == ["a", "b", "c", "b_bar", "c_bar"]


def test_collect_unexpected_kwargs(df: FlowFrame) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        df.collect(common_subexpr_elim=False)  # type: ignore[call-overload]


def test_spearman_corr() -> None:
    ldf = FlowFrame(
        {
            "era": [1, 1, 1, 2, 2, 2],
            "prediction": [2, 4, 5, 190, 1, 4],
            "target": [1, 3, 2, 1, 43, 3],
        }
    )

    out = (
        ldf.group_by("era", maintain_order=True).agg(
            fl.corr(fl.col("prediction"), fl.col("target"), method="spearman").alias(
                "c"
            ),
        )
    ).collect()["c"]
    assert np.isclose(out[0], 0.5)
    assert np.isclose(out[1], -1.0)

    # we can also pass in column names directly
    out = (
        ldf.group_by("era", maintain_order=True).agg(
            fl.corr("prediction", "target", method="spearman").alias("c"),
        )
    ).collect()["c"]
    assert np.isclose(out[0], 0.5)
    assert np.isclose(out[1], -1.0)


def test_spearman_corr_ties() -> None:
    """In Spearman correlation, ranks are computed using the average method ."""
    df = pl.DataFrame({"a": [1, 1, 1, 2, 3, 7, 4], "b": [4, 3, 2, 2, 4, 3, 1]})

    result = df.select(
        pl.corr("a", "b", method="spearman").alias("a1"),
        pl.corr(pl.col("a").rank("min"), pl.col("b").rank("min")).alias("a2"),
        pl.corr(pl.col("a").rank(), pl.col("b").rank()).alias("a3"),
    )
    expected = pl.DataFrame(
        [
            pl.Series("a1", [-0.19048482943986483], dtype=pl.Float64),
            pl.Series("a2", [-0.17223653586587362], dtype=pl.Float64),
            pl.Series("a3", [-0.19048482943986483], dtype=pl.Float64),
        ]
    )
    assert_frame_equal(result, expected)


def test_pearson_corr() -> None:
    ldf = FlowFrame(
        {
            "era": [1, 1, 1, 2, 2, 2],
            "prediction": [2, 4, 5, 190, 1, 4],
            "target": [1, 3, 2, 1, 43, 3],
        }
    )

    out = (
        ldf.group_by("era", maintain_order=True).agg(
            fl.corr(
                fl.col("prediction"),
                fl.col("target"),
                method="pearson",
            ).alias("c"),
        )
    ).collect()["c"]
    assert out.to_list() == pytest.approx([0.6546536707079772, -5.477514993831792e-1])

    # we can also pass in column names directly
    out = (
        ldf.group_by("era", maintain_order=True).agg(
            fl.corr("prediction", "target", method="pearson").alias("c"),
        )
    ).collect()["c"]
    assert out.to_list() == pytest.approx([0.6546536707079772, -5.477514993831792e-1])


def test_null_count() -> None:
    lf = FlowFrame({"a": [1, 2, None, 2], "b": [None, 3, None, 3]})
    assert lf.null_count().collect().rows() == [(1, 2)]


def test_lazy_concat(df: FlowFrame) -> None:

    shape = df.data.collect().shape
    shape = (shape[0] * 2, shape[1])
    out = fl.concat([df, df]).collect()
    assert out.shape == shape
    assert_frame_equal(out, df.data.collect().vstack(df.data.collect()))


def test_self_join() -> None:
    # 2720
    ldf = FlowFrame(
        data={
            "employee_id": [100, 101, 102],
            "employee_name": ["James", "Alice", "Bob"],
            "manager_id": [None, 100, 101],
        }
    )

    out = (
        ldf.join(other=ldf, left_on="manager_id", right_on="employee_id", how="left")
        .select(
            fl.col("employee_id"),
            fl.col("employee_name"),
            fl.col("employee_name_right").alias("manager_name"),
        )
        .collect()
    )
    assert set(out.rows()) == {
        (100, "James", None),
        (101, "Alice", "James"),
        (102, "Bob", "Alice"),
    }


def test_group_lengths() -> None:
    ldf = FlowFrame(
        {
            "group": ["A", "A", "A", "B", "B", "B", "B"],
            "id": ["1", "1", "2", "3", "4", "3", "5"],
        }
    )

    result = ldf.group_by(["group"], maintain_order=True).agg(
        [
            (fl.col("id").unique_counts() / fl.col("id").len())
            .sum()
            .alias("unique_counts_sum"),
            fl.col("id").unique().len().alias("unique_len"),
        ]
    )
    expected = pl.DataFrame(
        {
            "group": ["A", "B"],
            "unique_counts_sum": [1.0, 1.0],
            "unique_len": [2, 3],
        },
        schema_overrides={"unique_len": pl.UInt32},
    )
    assert_frame_equal(result.collect(), expected)


@pytest.mark.skip(reason="Not implemented yet")
def test_quantile_filtered_agg() -> None:
    assert (
        FlowFrame(
            {
                "group": [0, 0, 0, 0, 1, 1, 1, 1],
                "value": [1, 2, 3, 4, 1, 2, 3, 4],
            }
        )
        .group_by("group")
        .agg(fl.col("value").filter(fl.col("value") < 2).quantile(0.5))
        .collect()["value"]
        .to_list()
    ) == [1.0, 1.0]


def test_predicate_count_vstack() -> None:
    l1 = FlowFrame(
        {
            "k": ["x", "y"],
            "v": [3, 2],
        }
    )
    l2 = FlowFrame(
        {
            "k": ["x", "y"],
            "v": [5, 7],
        }
    )
    assert fl.concat([l1, l2]).filter(fl.len().over("k") == 2).collect()["v"].to_list() == [3, 2, 5, 7]


def test_lazy_method() -> None:
    # We want to support `.lazy()` on a Lazy DataFrame to allow more generic user code.
    df = FlowFrame(pl.DataFrame({"a": [1, 1, 2, 2, 3, 3], "b": [1, 2, 3, 4, 5, 6]}))
    assert_frame_equal(df.lazy().collect(), df.lazy().lazy().collect())


def test_update_schema_after_projection_pd_t4157() -> None:
    ldf = FlowFrame({"c0": [], "c1": [], "c2": []}).rename({"c2": "c2_"})
    assert ldf.drop("c2_").select(fl.col("c0")).collect().columns == ["c0"]


def test_type_coercion_unknown_4190() -> None:
    df = (
        FlowFrame({"a": [1, 2, 3], "b": [1, 2, 3]}).with_columns(
            fl.col("a") & fl.col("a").fill_null(True)
        )
    ).collect()
    assert df.shape == (3, 2)
    assert df.rows() == [(1, 1), (2, 2), (3, 3)]


def test_lazy_cache_same_key() -> None:
    ldf = FlowFrame({"a": [1, 2, 3], "b": [3, 4, 5], "c": ["x", "y", "z"]})

    # these have the same schema, but should not be used by cache as they are different
    add_node = ldf.select([(fl.col("a") + fl.col("b")).alias("a"), fl.col("c")]).cache()
    mult_node = ldf.select((fl.col("a") * fl.col("b")).alias("a"), fl.col("c")).cache()

    result = mult_node.join(add_node, on="c", suffix="_mult").select(
        (fl.col("a") - fl.col("a_mult")).alias("a"), fl.col("c")
    )
    expected = FlowFrame({"a": [-1, 2, 7], "c": ["x", "y", "z"]})
    assert_frame_equal(result.data, expected.data, check_row_order=False)


@pytest.mark.skip(reason="Not implemented yet")
def test_quadratic_behavior_4736() -> None:
    # no assert; if this function does not stall our tests it has passed!
    lf = FlowFrame(schema=list(ascii_letters))
    lf.select(reduce(add, (pl.col(c) for c in lf.collect_schema())))


@pytest.mark.skip(reason="Not implemented yet")
@pytest.mark.parametrize("input_dtype", [pl.Int64, pl.Float64])
def test_from_epoch(input_dtype: PolarsDataType) -> None:
    ldf = FlowFrame(
        [
            pl.Series("timestamp_d", [13285]).cast(input_dtype),
            pl.Series("timestamp_s", [1147880044]).cast(input_dtype),
            pl.Series("timestamp_ms", [1147880044 * 1_000]).cast(input_dtype),
            pl.Series("timestamp_us", [1147880044 * 1_000_000]).cast(input_dtype),
            pl.Series("timestamp_ns", [1147880044 * 1_000_000_000]).cast(input_dtype),
        ]
    )

    exp_dt = datetime(2006, 5, 17, 15, 34, 4)
    expected = pl.DataFrame(
        [
            pl.Series("timestamp_d", [date(2006, 5, 17)]),
            pl.Series("timestamp_s", [exp_dt]),  # s is no Polars dtype, defaults to us
            pl.Series("timestamp_ms", [exp_dt]).cast(pl.Datetime("ms")),
            pl.Series("timestamp_us", [exp_dt]),  # us is Polars Datetime default
            pl.Series("timestamp_ns", [exp_dt]).cast(pl.Datetime("ns")),
        ]
    )

    ldf_result = ldf.select(
        pl.from_epoch(pl.col("timestamp_d"), time_unit="d"),
        pl.from_epoch(pl.col("timestamp_s"), time_unit="s"),
        pl.from_epoch(pl.col("timestamp_ms"), time_unit="ms"),
        pl.from_epoch(pl.col("timestamp_us"), time_unit="us"),
        pl.from_epoch(pl.col("timestamp_ns"), time_unit="ns"),
    ).collect()

    assert_frame_equal(ldf_result, expected)

    ts_col = pl.col("timestamp_s")
    with pytest.raises(ValueError):
        _ = ldf.select(pl.from_epoch(ts_col, time_unit="s2"))  # type: ignore[call-overload]


@pytest.mark.skip(reason="Not implemented yet")
def test_from_epoch_str() -> None:
    ldf = FlowFrame(
        [
            pl.Series("timestamp_ms", [1147880044 * 1_000]).cast(pl.String),
            pl.Series("timestamp_us", [1147880044 * 1_000_000]).cast(pl.String),
        ]
    )

    with pytest.raises(InvalidOperationError):
        ldf.select(
            pl.from_epoch(pl.col("timestamp_ms"), time_unit="ms"),
            pl.from_epoch(pl.col("timestamp_us"), time_unit="us"),
        ).collect()


@pytest.mark.skip(reason="Not implemented yet")
def test_cum_agg_types() -> None:
    ldf = FlowFrame({"a": [1, 2], "b": [True, False], "c": [1.3, 2.4]})
    cum_sum_lf = ldf.select(
        fl.col("a").cum_sum(),
        fl.col("b").cum_sum(),
        fl.col("c").cum_sum(),
    )
    assert cum_sum_lf.collect_schema()["a"] == pl.Int64
    assert cum_sum_lf.collect_schema()["b"] == pl.UInt32
    assert cum_sum_lf.collect_schema()["c"] == pl.Float64
    collected_cumsum_lf = cum_sum_lf.collect()
    assert collected_cumsum_lf.schema == cum_sum_lf.collect_schema()

    cum_prod_lf = ldf.select(
        fl.col("a").cast(pl.UInt64).cum_prod(),
        fl.col("b").cum_prod(),
        fl.col("c").cum_prod(),
    )
    assert cum_prod_lf.collect_schema()["a"] == pl.UInt64
    assert cum_prod_lf.collect_schema()["b"] == pl.Int64
    assert cum_prod_lf.collect_schema()["c"] == pl.Float64
    collected_cum_prod_lf = cum_prod_lf.collect()
    assert collected_cum_prod_lf.schema == cum_prod_lf.collect_schema()


def test_compare_schema_between_lazy_and_eager_6904() -> None:
    float32_df = pl.DataFrame({"x": pl.Series(values=[], dtype=pl.Float32)})
    eager_result = float32_df.select(pl.col("x").sqrt()).select(pl.col(pl.Float32))
    lazy_result = (
        float32_df.lazy()
        .select(pl.col("x").sqrt())
        .select(pl.col(pl.Float32))
        .collect()
    )
    assert eager_result.shape == lazy_result.shape

    eager_result = float32_df.select(pl.col("x").pow(2)).select(pl.col(pl.Float32))
    lazy_result = (
        float32_df.lazy()
        .select(pl.col("x").pow(2))
        .select(pl.col(pl.Float32))
        .collect()
    )
    assert eager_result.shape == lazy_result.shape

    int32_df = pl.DataFrame({"x": pl.Series(values=[], dtype=pl.Int32)})
    eager_result = int32_df.select(pl.col("x").pow(2)).select(pl.col(pl.Float64))
    lazy_result = (
        int32_df.lazy().select(pl.col("x").pow(2)).select(pl.col(pl.Float64)).collect()
    )
    assert eager_result.shape == lazy_result.shape

    int8_df = pl.DataFrame({"x": pl.Series(values=[], dtype=pl.Int8)})
    eager_result = int8_df.select(pl.col("x").diff()).select(pl.col(pl.Int16))
    lazy_result = (
        int8_df.lazy().select(pl.col("x").diff()).select(pl.col(pl.Int16)).collect()
    )
    assert eager_result.shape == lazy_result.shape


@pytest.mark.slow
@pytest.mark.parametrize(
    "dtype",
    [
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.Float32,
        pl.Float64,
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        pl.col("x").arg_max(),
        pl.col("x").arg_min(),
        pl.col("x").max(),
        pl.col("x").mean(),
        pl.col("x").median(),
        pl.col("x").min(),
        pl.col("x").nan_max(),
        pl.col("x").nan_min(),
        pl.col("x").product(),
        pl.col("x").quantile(0.5),
        pl.col("x").std(),
        pl.col("x").sum(),
        pl.col("x").var(),
    ],
)
def test_compare_aggregation_between_lazy_and_eager_6904(
    dtype: PolarsDataType, func: pl.Expr
) -> None:
    df = pl.DataFrame(
        {
            "x": pl.Series(values=[1, 2, 3] * 2, dtype=dtype),
            "y": pl.Series(values=["a"] * 3 + ["b"] * 3),
        }
    )
    result_eager = df.select(func.over("y")).select("x")
    dtype_eager = result_eager["x"].dtype
    result_lazy = df.lazy().select(func.over("y")).select(pl.col(dtype_eager)).collect()
    assert_frame_equal(result_eager, result_lazy)


@pytest.mark.parametrize(
    "comparators",
    [
        ("==", fl.FlowFrame.__eq__),
        ("!=", fl.FlowFrame.__ne__),
        (">", fl.FlowFrame.__gt__),
        ("<", fl.FlowFrame.__lt__),
        (">=", fl.FlowFrame.__ge__),
        ("<=", fl.FlowFrame.__le__),
    ],
)
def test_lazy_comparison_operators(
    comparators: tuple[str, Callable[[pl.LazyFrame, Any], NoReturn]],
) -> None:
    # we cannot compare lazy frames, so all should raise a TypeError
    with pytest.raises(
        TypeError,
        match=f'"{comparators[0]!r}" comparison not supported for LazyFrame objects',
    ):
        comparators[1](FlowFrame(), FlowFrame())


def test_lf_unnest() -> None:
    lf = pl.DataFrame(
        [
            pl.Series(
                "a",
                [{"ab": [1, 2, 3], "ac": [3, 4, 5]}],
                dtype=pl.Struct({"ab": pl.List(pl.Int64), "ac": pl.List(pl.Int64)}),
            ),
            pl.Series(
                "b",
                [{"ba": [5, 6, 7], "bb": [7, 8, 9]}],
                dtype=pl.Struct({"ba": pl.List(pl.Int64), "bb": pl.List(pl.Int64)}),
            ),
        ]
    ).lazy()

    expected = pl.DataFrame(
        [
            pl.Series("ab", [[1, 2, 3]], dtype=pl.List(pl.Int64)),
            pl.Series("ac", [[3, 4, 5]], dtype=pl.List(pl.Int64)),
            pl.Series("ba", [[5, 6, 7]], dtype=pl.List(pl.Int64)),
            pl.Series("bb", [[7, 8, 9]], dtype=pl.List(pl.Int64)),
        ]
    )
    assert_frame_equal(lf.unnest("a", "b").collect(), expected)


@pytest.mark.skip(reason="Not implemented yet")
def test_type_coercion_cast_boolean_after_comparison() -> None:
    import operator

    lf = FlowFrame({"a": 1, "b": 2})

    for op in [
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        pl.Expr.eq_missing,
        pl.Expr.ne_missing,
    ]:
        e = op(pl.col("a"), pl.col("b")).cast(pl.Boolean).alias("o")
        assert "cast" not in lf.with_columns(e).explain()

        e = op(pl.col("a"), pl.col("b")).cast(pl.Boolean).cast(pl.Boolean).alias("o")
        assert "cast" not in lf.with_columns(e).explain()

    for op in [operator.and_, operator.or_, operator.xor]:
        e = op(pl.col("a"), pl.col("b")).cast(pl.Boolean)
        assert "cast" in lf.with_columns(e).explain()


def test_unique_length_multiple_columns() -> None:
    lf = FlowFrame(
        {
            "a": [1, 1, 1, 2, 3],
            "b": [100, 100, 200, 100, 300],
        }
    )
    assert lf.unique().select(fl.len()).collect().item() == 4


@pytest.mark.skip(reason="Not implemented yet")
def test_asof_cross_join() -> None:
    left = FlowFrame({"a": [-10, 5, 10], "left_val": ["a", "b", "c"]}).with_columns(
        fl.col("a").set_sorted()
    )
    right = FlowFrame(
        {"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]}
    ).with_columns(fl.col("a").set_sorted())

    out = left.join_asof(right, on="a").collect()
    assert out.shape == (3, 3)


@pytest.mark.skip(reason="Not implemented yet")
def test_join_bad_input_type() -> None:
    left = FlowFrame({"a": [1, 2, 3]})
    right = FlowFrame({"a": [1, 2, 3]})

    with pytest.raises(
        TypeError,
        match="expected `other` .*to be a 'LazyFrame'.* not 'DataFrame'",
    ):
        left.join(right.collect(), on="a")  # type: ignore[arg-type]

    with pytest.raises(
        TypeError,
        match="expected `other` .*to be a 'LazyFrame'.* not 'Series'",
    ):
        left.join(pl.Series([1, 2, 3]), on="a")  # type: ignore[arg-type]


@pytest.mark.skip(reason="Not implemented yet")
def test_join_where() -> None:
    east = FlowFrame(
        {
            "id": [100, 101, 102],
            "dur": [120, 140, 160],
            "rev": [12, 14, 16],
            "cores": [2, 8, 4],
        }
    )
    west = FlowFrame(
        {
            "t_id": [404, 498, 676, 742],
            "time": [90, 130, 150, 170],
            "cost": [9, 13, 15, 16],
            "cores": [4, 2, 1, 4],
        }
    )
    out = east.join_where(
        west,
        pl.col("dur") < pl.col("time"),
        pl.col("rev") < pl.col("cost"),
    ).collect()

    expected = pl.DataFrame(
        {
            "id": [100, 100, 100, 101, 101],
            "dur": [120, 120, 120, 140, 140],
            "rev": [12, 12, 12, 14, 14],
            "cores": [2, 2, 2, 8, 8],
            "t_id": [498, 676, 742, 676, 742],
            "time": [130, 150, 170, 150, 170],
            "cost": [13, 15, 16, 15, 16],
            "cores_right": [2, 1, 4, 1, 4],
        }
    )

    assert_frame_equal(out, expected)

@pytest.mark.skip(reason="Not implemented yet")
def test_join_where_bad_input_type() -> None:
    east = FlowFrame(
        {
            "id": [100, 101, 102],
            "dur": [120, 140, 160],
            "rev": [12, 14, 16],
            "cores": [2, 8, 4],
        }
    )
    west = FlowFrame(
        {
            "t_id": [404, 498, 676, 742],
            "time": [90, 130, 150, 170],
            "cost": [9, 13, 15, 16],
            "cores": [4, 2, 1, 4],
        }
    )
    with pytest.raises(
        TypeError,
        match="expected `other` .*to be a 'LazyFrame'.* not 'DataFrame'",
    ):
        east.join_where(
            west.collect(),  # type: ignore[arg-type]
            pl.col("dur") < pl.col("time"),
            pl.col("rev") < pl.col("cost"),
        )

    with pytest.raises(
        TypeError,
        match="expected `other` .*to be a 'LazyFrame'.* not 'Series'",
    ):
        east.join_where(
            pl.Series(west.collect()),  # type: ignore[arg-type]
            pl.col("dur") < pl.col("time"),
            pl.col("rev") < pl.col("cost"),
        )

