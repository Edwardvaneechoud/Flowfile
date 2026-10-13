"""Wherever Polars takes an expression, a frame method takes a frame expression (``ff.col``) or a Polars
expression (``pl.col``) too: ``Expr`` itself and its ``.str`` / ``.dt`` / ``.list`` namespaces.

Each case is built on an ``ff`` receiver, once with ``ff`` arguments and once with ``pl`` arguments, and
collected next to plain Polars. The result keeps its Polars expression, so a later method still applies.
"""

from datetime import date, time

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe, export_flow_to_polars

from .utils import run_exported

DATA = {
    "x": [1.0, 5.0, None, 9.0],
    "lo": [2.0, 2.0, 2.0, 2.0],
    "hi": [6.0, 6.0, 6.0, 6.0],
    "srt": [1.0, 2.0, 3.0, 4.0],
    "n": [1, 2, 1, 0],
    "p": [True, False, True, False],
    "q": [True, True, False, False],
    "s": ["apple", "banana", None, "cherry"],
    "pat": ["app", "nan", "x", "rr"],
    "rep": ["A", "B", "C", "D"],
    "dig": ["10", "11", "12", "13"],
    "l": [[1, 2], [3], [4, 5, 6], [7]],
    "l2": [[2], [3, 4], [6], [8]],
    "ls": [["a", "b"], ["c"], ["d", "e"], ["f"]],
    "sep": ["-", "+", "/", "_"],
    "d": [date(2024, 1, 1), date(2024, 1, 5), date(2024, 2, 1), date(2024, 3, 1)],
    "off": ["1d", "2d", "1mo", "1y"],
}

# receiver namespace ``b``, argument namespace ``m``
CASES = {
    "fill_null": lambda b, m: b.col("x").fill_null(m.col("lo")),
    "fill_nan": lambda b, m: b.col("x").fill_nan(m.col("lo")),
    "clip": lambda b, m: b.col("x").clip(m.col("lo"), m.col("hi")),
    "is_between": lambda b, m: b.col("x").is_between(m.col("lo"), m.col("hi")),
    "replace": lambda b, m: b.col("x").replace(m.lit(5.0), m.col("hi").max()),
    "replace_strict-default": lambda b, m: b.col("n").replace_strict({1: 10.0}, default=m.col("lo")),
    "shift-fill_value": lambda b, m: b.col("x").shift(1, fill_value=m.col("lo").first()),
    "shift-n": lambda b, m: b.col("x").shift(m.lit(1)),
    "is_in": lambda b, m: b.col("n").is_in(m.col("l")),
    "pow": lambda b, m: b.col("x").pow(m.col("lo")),
    "log": lambda b, m: b.col("hi").log(m.col("lo")),
    "add": lambda b, m: b.col("x").add(m.col("lo")),
    "lt": lambda b, m: b.col("x").lt(m.col("hi")),
    "eq_missing": lambda b, m: b.col("x").eq_missing(m.col("lo")),
    "and_": lambda b, m: b.col("p").and_(m.col("q")),
    "append": lambda b, m: b.col("x").append(m.col("lo")),
    "dot": lambda b, m: b.col("x").dot(m.col("lo")),
    "extend_constant": lambda b, m: b.col("x").extend_constant(m.lit(0.0), 2),
    "gather": lambda b, m: b.col("x").gather(m.col("n")),
    "get": lambda b, m: b.col("x").get(m.lit(1)),
    "head": lambda b, m: b.col("x").head(m.lit(2)),
    "slice": lambda b, m: b.col("x").slice(m.lit(1), m.lit(2)),
    "top_k": lambda b, m: b.col("x").top_k(m.lit(2)),
    "top_k_by": lambda b, m: b.col("x").top_k_by(m.col("n"), 2),
    "sort_by": lambda b, m: b.col("x").sort_by(m.col("n")),
    "sort_by-list": lambda b, m: b.col("x").sort_by([m.col("n"), m.col("srt")]),
    "max_by": lambda b, m: b.col("x").max_by(m.col("n")),
    "repeat_by": lambda b, m: b.col("x").repeat_by(m.col("n")),
    "index_of": lambda b, m: b.col("x").index_of(m.lit(5.0)),
    "search_sorted": lambda b, m: b.col("srt").search_sorted(m.col("lo")),
    "is_close": lambda b, m: b.col("x").is_close(m.col("lo")),
    "interpolate_by": lambda b, m: b.col("x").interpolate_by(m.col("srt")),
    "diff": lambda b, m: b.col("x").diff(m.lit(1)),
    "quantile": lambda b, m: b.col("x").quantile(m.lit(0.5)),
    "rolling_sum_by": lambda b, m: b.col("x").rolling_sum_by(m.col("d"), "2d"),
    "ewm_mean_by": lambda b, m: b.col("srt").ewm_mean_by(m.col("d"), half_life="1d"),
    "filter": lambda b, m: b.col("x").filter(m.col("p")),
    "over": lambda b, m: b.col("x").sum().over(m.col("p")),
    "str.contains": lambda b, m: b.col("s").str.contains(m.col("pat")),
    "str.starts_with": lambda b, m: b.col("s").str.starts_with(m.col("pat")),
    "str.ends_with": lambda b, m: b.col("s").str.ends_with(m.col("pat")),
    "str.replace": lambda b, m: b.col("s").str.replace(m.lit("a"), m.col("rep")),
    "str.replace_all": lambda b, m: b.col("s").str.replace_all(m.lit("a"), m.col("rep")),
    "str.strip_chars": lambda b, m: b.col("s").str.strip_chars(m.col("pat")),
    "str.strip_chars_start": lambda b, m: b.col("s").str.strip_chars_start(m.col("pat")),
    "str.strip_chars_start-literal": lambda b, m: b.col("s").str.strip_chars_start("ap"),
    "str.strip_chars_end-literal": lambda b, m: b.col("s").str.strip_chars_end("ey"),
    "str.strip_prefix": lambda b, m: b.col("s").str.strip_prefix(m.col("pat")),
    "str.count_matches": lambda b, m: b.col("s").str.count_matches(m.col("pat")),
    "str.find": lambda b, m: b.col("s").str.find(m.col("pat")),
    "str.extract": lambda b, m: b.col("s").str.extract(m.lit("(a.)")),
    "str.head": lambda b, m: b.col("s").str.head(m.col("n")),
    "str.slice": lambda b, m: b.col("s").str.slice(m.col("n"), m.col("n")),
    "str.pad_start": lambda b, m: b.col("s").str.pad_start(m.col("n") + 5),
    "str.zfill": lambda b, m: b.col("dig").str.zfill(m.col("n") + 2),
    "str.split": lambda b, m: b.col("s").str.split(m.col("pat")),
    "str.to_integer": lambda b, m: b.col("dig").str.to_integer(base=m.lit(8)),
    "dt.offset_by": lambda b, m: b.col("d").dt.offset_by(m.col("off")),
    "dt.truncate": lambda b, m: b.col("d").dt.truncate(m.col("off")),
    "dt.add_business_days": lambda b, m: b.col("d").dt.add_business_days(m.col("n")),
    "dt.combine": lambda b, m: b.col("d").dt.combine(m.lit(time(1, 2))),
    "dt.replace": lambda b, m: b.col("d").dt.replace(year=m.col("n") + 2000),
    "list.contains": lambda b, m: b.col("l").list.contains(m.col("n")),
    "list.get": lambda b, m: b.col("l").list.get(m.col("n"), null_on_oob=True),
    "list.head": lambda b, m: b.col("l").list.head(m.col("n")),
    "list.slice": lambda b, m: b.col("l").list.slice(m.col("n"), m.col("n")),
    "list.shift": lambda b, m: b.col("l").list.shift(m.col("n")),
    "list.join": lambda b, m: b.col("ls").list.join(m.col("sep")),
    "list.concat": lambda b, m: b.col("l").list.concat(m.col("l2")),
    "list.set_union": lambda b, m: b.col("l").list.set_union(m.col("l2")),
    "list.count_matches": lambda b, m: b.col("l").list.count_matches(m.lit(1)),
    "list.gather_every": lambda b, m: b.col("l").list.gather_every(m.col("n") + 1),
}


def _node(frame: ff.FlowFrame):
    return frame.flow_graph.get_node(frame.node_id)


def _double(x):
    return x * 2


def _shout(text):
    return text.upper()


@pytest.mark.parametrize("args", [ff, pl], ids=["ff-args", "pl-args"])
@pytest.mark.parametrize("build", CASES.values(), ids=CASES.keys())
def test_an_expression_argument_matches_polars(build, args):
    expr = build(ff, args)
    assert expr.expr is not None
    out = ff.from_dict(DATA).select(expr.alias("out"))
    assert_frame_equal(out.collect(), pl.LazyFrame(DATA).select(build(pl, pl).alias("out")).collect())


def test_a_method_and_an_operator_still_apply_after_an_expression_argument():
    clipped = ff.col("x").clip(ff.col("lo"), ff.col("hi"))
    out = ff.from_dict(DATA).with_columns(clipped.cum_sum().alias("running")).filter(clipped > 3)
    polars_clipped = pl.col("x").clip(pl.col("lo"), pl.col("hi"))
    expected = pl.LazyFrame(DATA).with_columns(polars_clipped.cum_sum().alias("running")).filter(polars_clipped > 3)
    assert_frame_equal(out.collect(), expected.collect())


def test_a_polars_column_renders_as_pl_col():
    assert repr(ff.col("x").clip(pl.col("lo"))) == "pl.col('x').clip(pl.col('lo'))"


def test_any_other_polars_expression_renders_as_its_plan():
    assert repr(ff.col("x").clip(pl.col("lo") * 2)).startswith(
        "pl.col('x').clip(pl.Expr.deserialize(BytesIO(b'{\"BinaryExpr\""
    )


def test_a_polars_expression_holding_a_python_function_is_run_instead_of_written():
    argument = pl.col("lo").map_elements(_double, return_dtype=pl.Float64)
    expr = ff.col("x").clip(argument)
    assert not expr.convertable_to_code
    out = ff.from_dict(DATA).select(expr.alias("out"))
    assert_frame_equal(out.collect(), pl.LazyFrame(DATA).select(pl.col("x").clip(argument).alias("out")).collect())


@pytest.mark.parametrize("method", ["starts_with", "ends_with"])
def test_a_column_affix_lowers_onto_a_formula(method):
    out = ff.from_dict(DATA).with_columns(getattr(ff.col("s").str, method)(ff.col("pat")).alias("out"))
    assert _node(out).node_type == "formula"
    assert _node(out).setting_input.function.function == f"{method}([s], [pat])"
    expected = pl.LazyFrame(DATA).with_columns(getattr(pl.col("s").str, method)(pl.col("pat")).alias("out"))
    assert_frame_equal(out.collect(), expected.collect())


@pytest.mark.parametrize(
    ("build", "definition"),
    [
        (lambda: ff.col("x").clip(ff.col("lo").map_elements(_double, return_dtype=pl.Float64)), "def _double"),
        (lambda: ff.col("s").map_elements(_shout, return_dtype=pl.String).str.to_lowercase(), "def _shout"),
        (lambda: ff.col("s").str.contains(ff.col("pat").map_elements(_shout, return_dtype=pl.String)), "def _shout"),
    ],
    ids=["argument", "namespace-receiver", "namespace-argument"],
)
def test_a_function_definition_travels_with_the_expression(build, definition):
    out = ff.from_dict(DATA).select(build().alias("out"))
    assert definition in _node(out).setting_input.polars_code_input.polars_code
    out.collect()


def _flow(b, m):
    frame = ff.from_dict(DATA) if b is ff else pl.LazyFrame(DATA)
    return (
        frame.with_columns(b.col("x").clip(m.col("lo"), m.col("hi")).alias("clipped"))
        .with_columns(b.col("s").str.contains(m.col("pat")).alias("has_pattern"))
        .with_columns(b.col("d").dt.offset_by(m.col("off")).alias("later"))
        .with_columns(b.col("l").list.concat(m.col("l2")).alias("joined"))
        .with_columns(b.col("x").fill_null(m.col("lo").median().over("p")).alias("by_group"))
        .with_columns((b.col("x") * (m.col("lo") * 2)).alias("scaled"))
    )


@pytest.mark.parametrize("args", [ff, pl], ids=["ff-args", "pl-args"])
@pytest.mark.parametrize("export", [export_flow_to_flowframe, export_flow_to_polars], ids=["flowframe", "polars"])
def test_exported_code_reruns_to_the_same_result(args, export):
    out = _flow(ff, args)
    assert_frame_equal(out.collect(), _flow(pl, pl).collect())
    assert_frame_equal(run_exported(export(out.flow_graph)), out.collect())
