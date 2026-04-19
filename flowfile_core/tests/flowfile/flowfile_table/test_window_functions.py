"""Tests for the window-functions FlowDataEngine operation."""

import polars as pl
import pytest
from pydantic import ValidationError

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import transform_schema


def _collect(fde: FlowDataEngine) -> pl.DataFrame:
    df = fde.data_frame
    return df.collect() if isinstance(df, pl.LazyFrame) else df


def test_rolling_mean_with_partition_and_order() -> None:
    fde = FlowDataEngine(
        pl.DataFrame(
            {
                "group": ["a", "a", "a", "b", "b", "b"],
                "t": [1, 2, 3, 1, 2, 3],
                "value": [10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
            }
        )
    )
    settings = transform_schema.WindowFunctionsInput(
        partition_by=["group"],
        order_by=[transform_schema.SortByInput(column="t", how="asc")],
        window_functions=[
            transform_schema.WindowFunctionInput(
                column="value",
                function="rolling_mean",
                new_column_name="value_ma2",
                window_size=2,
                min_periods=1,
            )
        ],
    )
    out = _collect(fde.do_window_functions(settings))
    # Sorted by group,t; rolling mean of window 2 with min_samples=1
    expected = pl.DataFrame(
        {
            "group": ["a", "a", "a", "b", "b", "b"],
            "t": [1, 2, 3, 1, 2, 3],
            "value": [10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
            "value_ma2": [10.0, 15.0, 25.0, 100.0, 150.0, 250.0],
        }
    )
    assert out.sort(["group", "t"]).equals(expected)


def test_cumulative_sum_partitioned() -> None:
    fde = FlowDataEngine(
        pl.DataFrame(
            {
                "group": ["a", "a", "b", "a", "b"],
                "t": [1, 2, 1, 3, 2],
                "value": [1, 2, 10, 3, 20],
            }
        )
    )
    settings = transform_schema.WindowFunctionsInput(
        partition_by=["group"],
        order_by=[transform_schema.SortByInput(column="t", how="asc")],
        window_functions=[
            transform_schema.WindowFunctionInput(
                column="value", function="cum_sum", new_column_name="value_cum"
            )
        ],
    )
    out = _collect(fde.do_window_functions(settings)).sort(["group", "t"])
    assert out["value_cum"].to_list() == [1, 3, 6, 10, 30]


def test_tile_equal_groups_sql_ntile_semantics() -> None:
    # 10 rows, 4 tiles -> SQL NTILE sizes: 3, 3, 2, 2
    fde = FlowDataEngine(pl.DataFrame({"t": list(range(10))}))
    settings = transform_schema.WindowFunctionsInput(
        partition_by=[],
        order_by=[transform_schema.SortByInput(column="t", how="asc")],
        window_functions=[
            transform_schema.WindowFunctionInput(
                function="tile", new_column_name="quartile", number_of_groups=4
            )
        ],
    )
    out = _collect(fde.do_window_functions(settings)).sort("t")
    tiles = out["quartile"].to_list()
    assert tiles == [1, 1, 1, 2, 2, 2, 3, 3, 4, 4]


def test_tile_per_partition() -> None:
    fde = FlowDataEngine(
        pl.DataFrame(
            {
                "group": ["a"] * 5 + ["b"] * 5,
                "t": list(range(5)) + list(range(5)),
            }
        )
    )
    settings = transform_schema.WindowFunctionsInput(
        partition_by=["group"],
        order_by=[transform_schema.SortByInput(column="t", how="asc")],
        window_functions=[
            transform_schema.WindowFunctionInput(
                function="tile", new_column_name="half", number_of_groups=2
            )
        ],
    )
    out = _collect(fde.do_window_functions(settings)).sort(["group", "t"])
    # 5 rows into 2 tiles: sizes 3, 2
    assert out.filter(pl.col("group") == "a")["half"].to_list() == [1, 1, 1, 2, 2]
    assert out.filter(pl.col("group") == "b")["half"].to_list() == [1, 1, 1, 2, 2]


def test_rank_ordinal_per_partition() -> None:
    fde = FlowDataEngine(
        pl.DataFrame(
            {
                "group": ["a", "a", "a", "b", "b"],
                "value": [30, 10, 20, 500, 100],
            }
        )
    )
    settings = transform_schema.WindowFunctionsInput(
        partition_by=["group"],
        window_functions=[
            transform_schema.WindowFunctionInput(
                column="value",
                function="rank",
                new_column_name="value_rank",
                rank_method="ordinal",
            )
        ],
    )
    out = _collect(fde.do_window_functions(settings))
    # Ordinal rank within group: a -> 10:1, 20:2, 30:3; b -> 100:1, 500:2
    a = out.filter(pl.col("group") == "a").sort("value")["value_rank"].to_list()
    b = out.filter(pl.col("group") == "b").sort("value")["value_rank"].to_list()
    assert a == [1, 2, 3]
    assert b == [1, 2]


def test_rolling_requires_window_size_validation() -> None:
    with pytest.raises(ValidationError):
        transform_schema.WindowFunctionInput(
            column="value",
            function="rolling_mean",
            new_column_name="x",
        )


def test_tile_requires_number_of_groups_validation() -> None:
    with pytest.raises(ValidationError):
        transform_schema.WindowFunctionInput(
            function="tile",
            new_column_name="x",
        )


def test_rolling_requires_order_by_at_input_level() -> None:
    with pytest.raises(ValidationError):
        transform_schema.WindowFunctionsInput(
            partition_by=["g"],
            order_by=[],
            window_functions=[
                transform_schema.WindowFunctionInput(
                    column="v", function="rolling_sum", new_column_name="x", window_size=3
                )
            ],
        )


def test_duplicate_output_names_rejected() -> None:
    with pytest.raises(ValidationError):
        transform_schema.WindowFunctionsInput(
            order_by=[transform_schema.SortByInput(column="t", how="asc")],
            window_functions=[
                transform_schema.WindowFunctionInput(
                    column="v", function="rolling_sum", new_column_name="dup", window_size=3
                ),
                transform_schema.WindowFunctionInput(
                    column="v", function="rolling_mean", new_column_name="dup", window_size=3
                ),
            ],
        )
