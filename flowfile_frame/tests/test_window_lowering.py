"""``col(x).<agg>().over(g)`` in ``with_columns`` lowers onto a native Window Functions node."""

import os
from pathlib import Path
import tempfile

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_frame import col


def _make_frame() -> ff.FlowFrame:
    return ff.from_dict({"a": [1, 2, 3, 4], "g": ["x", "x", "y", "y"], "h": ["p", "q", "p", "q"], "t": [4, 3, 2, 1]})


def _pl() -> pl.LazyFrame:
    return pl.LazyFrame({"a": [1, 2, 3, 4], "g": ["x", "x", "y", "y"], "h": ["p", "q", "p", "q"], "t": [4, 3, 2, 1]})


def _node_type(frame: ff.FlowFrame) -> str:
    return frame.flow_graph.get_node(frame.node_id).node_type


@pytest.mark.parametrize("func", ["mean", "sum", "min", "max", "count", "std", "median"])
def test_partition_aggregate_renders_window_node(func):
    out = _make_frame().with_columns([getattr(col("a"), func)().over("g").alias("m")])
    assert _node_type(out) == "window_functions"
    settings = out.flow_graph.get_node(out.node_id).setting_input.window_input
    assert settings.partition_by == ["g"]
    assert settings.order_by == []
    assert [(w.column, w.function, w.new_column_name) for w in settings.window_functions] == [("a", func, "m")]
    expected = _pl().with_columns(getattr(pl.col("a"), func)().over("g").alias("m")).collect()
    assert_frame_equal(out.collect(), expected)


def test_multiple_aggregates_share_one_window_node():
    out = _make_frame().with_columns(
        [col("a").mean().over("g").alias("m"), col("a").sum().over("g").alias("s")], description="per-group stats"
    )
    assert _node_type(out) == "window_functions"
    node = out.flow_graph.get_node(out.node_id)
    assert node.setting_input.description == "per-group stats"
    assert [w.new_column_name for w in node.setting_input.window_input.window_functions] == ["m", "s"]
    schema = {c.name: c.data_type for c in node.schema}
    assert schema["m"] == "Float64"
    assert schema["s"] == "Int64"
    expected = _pl().with_columns(pl.col("a").mean().over("g").alias("m"), pl.col("a").sum().over("g").alias("s"))
    assert_frame_equal(out.collect(), expected.collect())


def test_multi_column_partition_lowers():
    for exprs in (
        [col("a").max().over(["g", "h"]).alias("m")],
        [col("a").max().over("g", "h").alias("m")],
        [col("a").max().over(col("g"), col("h")).alias("m")],
    ):
        out = _make_frame().with_columns(exprs)
        assert _node_type(out) == "window_functions"
        assert out.flow_graph.get_node(out.node_id).setting_input.window_input.partition_by == ["g", "h"]
        expected = _pl().with_columns(pl.col("a").max().over(["g", "h"]).alias("m")).collect()
        assert_frame_equal(out.collect(), expected)


@pytest.mark.parametrize(
    "exprs",
    [
        pytest.param([col("a").sum().over("g", order_by="t").alias("s")], id="order_by"),
        pytest.param([col("a").sum().over("g", mapping_strategy="join").alias("s")], id="mapping_strategy"),
        pytest.param([col("a").mean().over("g")], id="unaliased_overwrites_source"),
        pytest.param([col("a").mean().over("g").alias("m"), col("a").mean().over("h").alias("n")], id="mixed_partitions"),
        pytest.param([(col("a") * 2).mean().over("g").alias("m")], id="arithmetic_base"),
        pytest.param([col("a").std(ddof=0).over("g").alias("m")], id="std_ddof0"),
        pytest.param([col("a").cum_sum().over("g").alias("m")], id="cumulative_not_lowered"),
        pytest.param([col("a").mean().over("g").alias("m"), (col("a") + 1).alias("b")], id="mixed_with_plain_expr"),
        pytest.param([col("a").max().over(col("g").alias("gg")).alias("m")], id="altered_partition_column"),
    ],
)
def test_unsupported_shapes_fall_back_to_polars_code(exprs):
    out = _make_frame().with_columns(exprs)
    assert _node_type(out) == "polars_code"
    pl_exprs = [e.expr for e in exprs]
    assert_frame_equal(out.collect(), _pl().with_columns(pl_exprs).collect())


def test_window_node_round_trips_through_save():
    out = _make_frame().with_columns([col("a").mean().over("g").alias("m")])
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "window.yaml")
        out.save_graph(path)
        reopened = open_flow(Path(path))
    node = reopened.get_node(out.node_id)
    assert node.node_type == "window_functions"
    w = node.setting_input.window_input
    assert w.partition_by == ["g"]
    assert [(x.column, x.function, x.new_column_name) for x in w.window_functions] == [("a", "mean", "m")]
    reopened.run_graph()
    result = node.get_resulting_data().data_frame
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    assert result["m"].to_list() == [1.5, 1.5, 3.5, 3.5]
