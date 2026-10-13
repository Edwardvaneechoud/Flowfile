"""``fill_null`` / ``fill_nan`` take what Polars takes: a literal, a frame expression, a Polars
expression, or (``fill_null``) a ``strategy``.

A fill with a formula form lowers onto a Formula node as ``coalesce(...)``; anything else (a
window, a raw Polars expression, a strategy, ``fill_nan``) is a Polars-code node. Each case is
built twice, once with ``ff`` and once with ``pl``, and the collected frames must be equal.
"""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe, export_flow_to_polars

from .utils import run_exported

DATA = {
    "a": [1.0, None, float("nan"), None],
    "b": [10.0, 20.0, 30.0, 40.0],
    "i": [1, None, 3, None],
    "s": ["x", None, "z", None],
}
TITANIC = {
    "Name": [
        "Braund, Mr. Owen",
        "Cumings, Mrs. John",
        "Heikkinen, Mr. Laina",
        "Futrelle, Miss. Lily",
        "Allen, Mrs. Ann",
        "Moran, Mr. James",
        "McCarthy, Miss. Ellen",
    ],
    "Age": [22.0, 38.0, None, 26.0, None, 54.0, None],
}
TITLE = r",\s*([^.]+)\."


def _node(frame: ff.FlowFrame):
    return frame.flow_graph.get_node(frame.node_id)


def _check(build, node_type: str, data: dict = DATA) -> ff.FlowFrame:
    out = ff.from_dict(data).with_columns(build(ff).alias("out"))
    assert _node(out).node_type == node_type
    assert_frame_equal(out.collect(), pl.LazyFrame(data).with_columns(build(pl).alias("out")).collect())
    return out


def _double(x):
    return x * 2


@pytest.mark.parametrize(
    ("build", "node_type"),
    [
        (lambda m: m.col("a").fill_null(0.0), "formula"),
        (lambda m: m.col("a").fill_null(m.col("b")), "formula"),
        (lambda m: m.col("i").fill_null(m.col("b")), "formula"),
        (lambda m: m.col("s").fill_null(m.col("i")), "formula"),
        (lambda m: m.col("i").fill_null(m.lit(0.5)), "formula"),
        (lambda m: m.col("a").fill_null(pl.col("b")), "polars_code"),
        (lambda m: m.col("a").fill_null(m.col("b").mean().over("s")), "polars_code"),
        (lambda m: m.col("a").fill_null(pl.col("b").mean().over("s")), "polars_code"),
        (lambda m: m.col("a").fill_null(strategy="forward"), "polars_code"),
        (lambda m: m.col("i").fill_null(strategy="backward", limit=1), "polars_code"),
        (lambda m: m.col("a").fill_null(strategy="mean"), "polars_code"),
        (lambda m: m.col("a").fill_nan(m.col("b")), "polars_code"),
        (lambda m: m.col("a").fill_nan(pl.col("b")), "polars_code"),
        (lambda m: m.col("a").fill_nan(0.0), "polars_code"),
        (lambda m: m.col("a").fill_nan(None), "polars_code"),
    ],
    ids=[
        "literal",
        "column-keeps-nan",
        "int-from-float-column",
        "string-from-int-column",
        "int-from-float-lit",
        "polars-column",
        "window",
        "polars-window",
        "strategy-forward",
        "strategy-backward-limit",
        "strategy-mean",
        "nan-from-column",
        "nan-from-polars-column",
        "nan-literal",
        "nan-to-null",
    ],
)
def test_fill_matches_polars(build, node_type):
    _check(build, node_type)


def test_a_column_fill_lowers_to_coalesce():
    out = _check(lambda m: m.col("a").fill_null(m.col("b")), "formula")
    assert _node(out).setting_input.function.function == "coalesce([a], [b])"


def test_a_polars_expression_renders_as_its_polars_code():
    out = _check(lambda m: m.col("a").fill_null(pl.col("b")), "polars_code")
    assert "pl.col('a').fill_null(pl.col('b'))" in _node(out).setting_input.polars_code_input.polars_code


def test_a_fill_value_brings_its_function_definition():
    out = _check(lambda m: m.col("a").fill_null(m.col("b").map_elements(_double, return_dtype=pl.Float64)),
                 "polars_code")
    assert "def _double(x):" in _node(out).setting_input.polars_code_input.polars_code


def test_fill_null_refuses_what_polars_refuses():
    with pytest.raises(ValueError, match="must specify either"):
        ff.col("a").fill_null()
    with pytest.raises(ValueError, match="cannot specify both"):
        ff.col("a").fill_null(0.0, strategy="forward")


def test_is_in_takes_a_column():
    data = {"n": [1, 5, 3], "l": [[1, 2], [3], [3, 4]]}
    _check(lambda m: m.col("n").is_in(m.col("l")), "polars_code", data)
    _check(lambda m: m.col("n").is_in(pl.col("l")), "polars_code", data)


def _titanic(m):
    frame = ff.from_dict(TITANIC) if m is ff else pl.LazyFrame(TITANIC)
    return frame.with_columns(m.col("Name").str.extract(TITLE, 1).alias("Title")).with_columns(
        m.col("Age").fill_null(m.col("Age").median().over("Title"))
    )


def test_titanic_age_imputed_by_title_median():
    out = _titanic(ff)
    assert _node(out).node_type == "polars_code"
    assert_frame_equal(out.collect(), _titanic(pl).collect())
    assert out.collect()["Age"].to_list() == [22.0, 38.0, 38.0, 26.0, 38.0, 54.0, 26.0]


def _fills(m):
    frame = ff.from_dict(DATA) if m is ff else pl.LazyFrame(DATA)
    return (
        frame.with_columns(m.col("a").fill_null(m.col("b")).alias("from_column"))
        .with_columns(m.col("i").fill_null(m.col("b")).alias("supercast"))
        .with_columns(m.col("a").fill_null(m.col("b").mean().over("s")).alias("window"))
        .with_columns(m.col("a").fill_null(pl.col("b")).alias("from_polars"))
        .with_columns(m.col("a").fill_null(pl.col("b").mean().over("s")).alias("polars_window"))
        .with_columns(m.col("a").fill_null(strategy="forward").alias("forward"))
        .with_columns(m.col("a").fill_nan(m.col("b")).alias("nan_filled"))
    )


@pytest.mark.parametrize("build", [_fills, _titanic], ids=["fills", "titanic"])
@pytest.mark.parametrize("export", [export_flow_to_flowframe, export_flow_to_polars], ids=["flowframe", "polars"])
def test_exported_code_reruns_to_the_same_result(build, export):
    out = build(ff)
    assert_frame_equal(out.collect(), build(pl).collect())
    assert_frame_equal(run_exported(export(out.flow_graph)), out.collect())
