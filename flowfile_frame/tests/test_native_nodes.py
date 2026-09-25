"""Fluent-API calls that lower onto native nodes instead of Polars-code nodes,
and the save → reopen → re-save round trip of a graph mixing both kinds."""


import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_frame import col, lit, when
from flowfile_frame import selectors as cs

from .native_helpers import core_node, round_trip

DATA = {"a": [1, 2, 3, None], "g": ["x", "x", "y", "z"], "name": ["alex", "auke", "bob", None]}


def _frame() -> ff.FlowFrame:
    return ff.from_dict(DATA)


def _pl() -> pl.LazyFrame:
    return pl.LazyFrame(DATA)


# drop


def test_drop_renders_select_node_with_columns_unchecked():
    out = _frame().drop("g")
    node = core_node(out)
    assert node.node_type == "select"
    settings = node.setting_input
    assert [(s.old_name, s.keep) for s in settings.select_input] == [("a", True), ("g", False), ("name", True)]
    assert settings.keep_missing is True
    assert settings.get_default_description() == "Drop: g"
    assert out.columns == ["a", "name"]
    assert_frame_equal(out.collect(), _pl().drop("g").collect())


@pytest.mark.parametrize("columns", [(["g", "name"],), ("g", "name"), (col("g"), "name")])
def test_drop_accepts_lists_and_column_objects(columns):
    out = _frame().drop(*columns)
    assert core_node(out).node_type == "select"
    assert out.columns == ["a"]


def test_drop_keeps_explicit_description():
    assert core_node(_frame().drop("g", description="no group")).setting_input.description == "no group"


def test_drop_missing_column_raises_like_polars():
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        _frame().drop("nope")


def test_drop_missing_column_non_strict_is_ignored():
    out = _frame().drop("nope", "g", strict=False)
    assert core_node(out).node_type == "select"
    assert out.columns == ["a", "name"]


def test_drop_selector_falls_back_to_polars_code():
    out = _frame().drop(cs.numeric())
    node = core_node(out)
    assert node.node_type == "polars_code"
    assert node.setting_input.polars_code_input.polars_code == "output_df = input_df.drop(pl.selectors.numeric())"
    assert out.columns == ["g", "name"]


# filter


@pytest.mark.parametrize(
    "predicates, formula",
    [
        ((col("a") > 1,), "([a] > 1)"),
        ((col("name") == "alex",), '([name] == "alex")'),
        ((col("name").is_in(["alex", "auke"]),), '[name] in ("alex", "auke")'),
        ((col("a").is_in([1, 3]),), "[a] in (1, 3)"),
        ((~col("name").is_in(["alex"]),), 'not([name] in ("alex"))'),
        ((col("a").is_null() | (col("g") == "x"),), '(is_empty([a]) or ([g] == "x"))'),
        ((col("a") > 1, col("g") != "z"), '([a] > 1) and ([g] != "z")'),
        (([col("a") >= 2, col("name").str.starts_with("a")],), '([a] >= 2) and starts_with([name], "a")'),
    ],
)
def test_filter_predicates_render_filter_node(predicates, formula):
    out = _frame().filter(*predicates)
    node = core_node(out)
    assert node.node_type == "filter"
    assert node.setting_input.filter_input.mode == "advanced"
    assert node.setting_input.filter_input.advanced_filter == formula
    assert node.setting_input.get_default_description() == formula
    flat = [p for item in predicates for p in (item if isinstance(item, list) else [item])]
    assert_frame_equal(out.collect(), _pl().filter(*[p.expr for p in flat]).collect())


def test_filter_keyword_constraints_render_filter_node():
    out = _frame().filter(g="x", a=1)
    node = core_node(out)
    assert node.node_type == "filter"
    assert node.setting_input.filter_input.advanced_filter == '([g] == "x") and ([a] == 1)'
    assert_frame_equal(out.collect(), _pl().filter(g="x", a=1).collect())


def test_filter_value_with_double_quotes_renders_filter_node():
    frame = ff.from_dict({"s": ['say "hi"', "plain"]})
    out = frame.filter(col("s") == 'say "hi"')
    node = core_node(out)
    assert node.node_type == "filter"
    assert node.setting_input.filter_input.advanced_filter == "([s] == 'say \"hi\"')"
    assert out.collect()["s"].to_list() == ['say "hi"']


def test_filter_value_with_both_quote_kinds_falls_back_to_polars_code():
    value = "it's \"x\""
    out = ff.from_dict({"s": [value, "plain"]}).filter(col("s") == value)
    assert core_node(out).node_type == "polars_code"
    assert out.collect()["s"].to_list() == [value]


def test_filter_lambda_predicate_falls_back_to_polars_code():
    out = _frame().filter(col("a").map_elements(lambda x: x > 1, return_dtype=pl.Boolean))
    node = core_node(out)
    assert node.node_type == "polars_code"
    assert node.setting_input.description == "Filter on: a"
    assert out.collect()["a"].to_list() == [2, 3]


def test_filter_split_uses_the_formula_predicate():
    passed, failed = _frame().filter_split(col("a") > 1)
    node = core_node(passed)
    assert node.node_type == "filter"
    assert node.setting_input.split_mode is True
    assert node.setting_input.filter_input.advanced_filter == "([a] > 1)"
    assert passed.collect()["a"].to_list() == [2, 3]
    assert failed.collect()["a"].to_list() == [1]


def test_filter_split_without_formula_form_raises():
    """The split node has no Polars-code fallback, so an untranslatable predicate fails loudly."""
    with pytest.raises(ValueError, match="filter_split predicates must have a flowfile-formula form"):
        _frame().filter_split(col("a").map_elements(lambda x: x > 1, return_dtype=pl.Boolean))
    with pytest.raises(ValueError, match="filter_split predicates must have a flowfile-formula form"):
        _frame().filter_split(col("a").shift(1) > 1)


# when / then / otherwise


def test_when_chain_renders_formula_node():
    out = _frame().with_columns(
        [when(col("name").is_in(["alex", "auke"])).then(lit("DS")).otherwise(lit("DE")).alias("team")]
    )
    node = core_node(out)
    assert node.node_type == "formula"
    assert node.setting_input.function.function == 'if [name] in ("alex", "auke") then "DS" else "DE" endif'
    assert node.setting_input.function.field.name == "team"
    expected = _pl().with_columns(
        pl.when(pl.col("name").is_in(["alex", "auke"])).then(pl.lit("DS")).otherwise(pl.lit("DE")).alias("team")
    )
    assert_frame_equal(out.collect(), expected.collect())


def test_multi_branch_when_renders_elseif_chain():
    chain = when(col("a") > 2).then(lit("hi")).when(col("a") > 1).then(lit("mid")).otherwise(lit("lo"))
    out = _frame().with_columns([chain.alias("b")])
    node = core_node(out)
    assert node.node_type == "formula"
    assert (
        node.setting_input.function.function == 'if ([a] > 2) then "hi" elseif ([a] > 1) then "mid" else "lo" endif'
    )
    expected = _pl().with_columns(
        pl.when(pl.col("a") > 2)
        .then(pl.lit("hi"))
        .when(pl.col("a") > 1)
        .then(pl.lit("mid"))
        .otherwise(pl.lit("lo"))
        .alias("b")
    )
    assert_frame_equal(out.collect(), expected.collect())


def test_when_branch_values_can_be_columns():
    out = _frame().with_columns([when(col("a") > 1).then(col("name")).otherwise(col("g")).alias("pick")])
    node = core_node(out)
    assert node.node_type == "formula"
    assert node.setting_input.function.function == "if ([a] > 1) then [name] else [g] endif"
    expected = _pl().with_columns(pl.when(pl.col("a") > 1).then(pl.col("name")).otherwise(pl.col("g")).alias("pick"))
    assert_frame_equal(out.collect(), expected.collect())


def test_when_without_otherwise_defaults_to_null():
    out = _frame().with_columns([when(col("a") > 2).then(lit("hi")).alias("b")])
    node = core_node(out)
    assert node.node_type == "formula"
    assert node.setting_input.function.function == 'if ([a] > 2) then "hi" else null endif'
    expected = _pl().with_columns(pl.when(pl.col("a") > 2).then(pl.lit("hi")).alias("b"))
    assert_frame_equal(out.collect(), expected.collect())


def test_chained_when_polars_expression_keeps_every_branch():
    chain = when(col("a") > 2).then(lit("hi")).when(col("a") > 1).then(lit("mid")).otherwise(lit("lo"))
    assert pl.DataFrame(DATA).select(chain.expr.alias("r"))["r"].to_list() == ["lo", "mid", "hi", "lo"]


def test_when_with_untranslatable_condition_falls_back_to_polars_code():
    out = _frame().with_columns([when(col("a").shift(1) > 1).then(lit(1)).otherwise(lit(0)).alias("b")])
    node = core_node(out)
    assert node.node_type == "polars_code"
    assert node.setting_input.description == "Add columns: b"
    expected = _pl().with_columns(pl.when(pl.col("a").shift(1) > 1).then(pl.lit(1)).otherwise(pl.lit(0)).alias("b"))
    assert_frame_equal(out.collect(), expected.collect())


# audit of the other single-input operations


def test_head_limit_unique_select_and_count_stay_native():
    frame = _frame()
    assert core_node(frame.head(2)).node_type == "sample"
    assert core_node(frame.limit(2)).node_type == "sample"
    assert core_node(frame.unique(["g"])).node_type == "unique"
    assert core_node(frame.select("a", "g")).node_type == "select"
    assert core_node(frame.select(ff.len().alias("number_of_records"))).node_type == "record_count"


def test_polars_code_nodes_get_descriptive_labels():
    frame = _frame()
    shifted = frame.with_columns([col("a").shift(1).alias("prev"), col("a").shift(-1).alias("next")])
    assert core_node(shifted).setting_input.description == "Add columns: prev, next"
    assert core_node(frame.sort(col("a").abs())).setting_input.description == "Sort by: a"
    assert core_node(frame.select(cs.numeric())).setting_input.description == "Select columns"
    assert core_node(frame.tail(2)).setting_input.description == "Tail operation"
    assert core_node(frame.tail(2, description="last two")).setting_input.description == "last two"


# round trip


def test_mixed_graph_round_trips_through_save_reopen_and_resave():
    """Native and Polars-code nodes alike survive save → open → save unchanged."""
    out = (
        _frame()
        .drop("g")
        .filter(col("name").is_in(["alex", "auke", "bob"]))
        .with_columns([when(col("a") > 1).then(lit("big")).otherwise(lit("small")).alias("size")])
        .with_columns([col("a").shift(1).alias("prev")], description="previous value")
        .tail(2)
    )
    expected = out.collect()
    reopened, _ = round_trip(out, "roundtrip.yaml")

    types = {node.node_id: node.node_type for node in reopened.nodes}
    assert types == {node.node_id: node.node_type for node in out.flow_graph.nodes}
    assert sorted(types.values()) == sorted(
        ["manual_input", "select", "filter", "formula", "polars_code", "polars_code"]
    )
    by_type = {node.node_type: node for node in reopened.nodes if node.node_type != "polars_code"}
    assert [s.old_name for s in by_type["select"].setting_input.select_input if not s.keep] == ["g"]
    assert by_type["filter"].setting_input.filter_input.advanced_filter == '[name] in ("alex", "auke", "bob")'
    assert by_type["formula"].setting_input.function.function == 'if ([a] > 1) then "big" else "small" endif'
    code_nodes = sorted(
        (n for n in reopened.nodes if n.node_type == "polars_code"), key=lambda n: n.node_id
    )
    assert code_nodes[0].setting_input.description == "previous value"
    assert code_nodes[0].setting_input.polars_code_input.polars_code == (
        "input_df.with_columns([pl.col('a').shift(1).alias('prev')])"
    )
    assert code_nodes[1].setting_input.description == "Tail operation"

    reopened.run_graph()
    result = reopened.get_node(out.node_id).get_resulting_data().data_frame
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    assert_frame_equal(result, expected)
