"""Flow parameters in the Python API.

``${name}`` references resolve when a node is built, the way the run loop resolves them;
``ff.Parameter`` is declared once and used as a value in expressions, gates and subflow calls;
a parameter in a column-name position is refused up front.
"""

import os
import tempfile
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.flowfile.manage.io_flowfile import open_flow

ORDERS = {"id": [1, 2, 3, 4], "amount": [10.0, 25.0, 70.0, 120.0], "flag": [True, False, True, False]}
SUPPORTED_USES = "fl.lit(...), in comparisons, in fl.Gate(parameter=...) or in fl.RunFlow(params=...)"


def _node(frame: ff.FlowFrame):
    return frame.flow_graph.get_node(frame.node_id)


def _orders() -> tuple[ff.FlowGraph, ff.FlowFrame]:
    child = ff.create_flow_graph()
    return child, ff.FlowInput("orders", sample=ORDERS, flow_graph=child)


def _min_amount() -> ff.Parameter:
    return ff.Parameter("min_amount", default=0, type="integer")


def test_symptom_builds_with_the_declared_type_and_schema():
    child = ff.create_flow_graph()
    raw = ff.FlowInput("orders", schema={"id": ff.Int64, "amount": ff.Float64}, flow_graph=child)
    ff.add_flow_parameter(child, _min_amount())
    kept = raw.filter(ff.col("amount") >= ff.lit("${min_amount}").cast(ff.Int64))
    kept.to_flow_output("orders_clean")
    assert kept.collect().schema == pl.Schema({"id": pl.Int64, "amount": pl.Float64})


def test_build_uses_the_parameter_value_at_the_moment_the_node_is_built():
    child, raw = _orders()
    ff.add_flow_parameter(child, _min_amount())
    at_zero = raw.filter(ff.col("amount") >= ff.lit("${min_amount}").cast(ff.Int64))
    assert at_zero.collect()["id"].to_list() == [1, 2, 3, 4]

    ff.set_flow_parameter(child, "min_amount", 20)
    at_twenty = raw.filter(ff.col("amount") >= ff.lit("${min_amount}").cast(ff.Int64))
    assert at_twenty.collect()["id"].to_list() == [2, 3, 4]
    assert at_zero.collect()["id"].to_list() == [1, 2, 3, 4]


@pytest.mark.parametrize("wrap", [lambda p: p, ff.lit], ids=["bare", "lit"])
def test_parameter_predicate_lowers_onto_a_native_filter_keeping_the_reference(wrap):
    child, raw = _orders()
    min_amount = _min_amount()
    ff.add_flow_parameter(child, min_amount)
    ff.set_flow_parameter(child, min_amount, 20)
    kept = raw.filter(ff.col("amount") >= wrap(min_amount))
    node = _node(kept)
    assert node.node_type == "filter"
    assert node.setting_input.filter_input.advanced_filter == "([amount] >= ${min_amount})"
    assert kept.collect()["id"].to_list() == [2, 3, 4]


def test_string_parameter_lowers_onto_a_native_formula():
    child, raw = _orders()
    region = ff.Parameter("region", default="EU")
    ff.add_flow_parameter(child, region)
    tagged = raw.with_columns(ff.lit(region).alias("r"))
    node = _node(tagged)
    assert node.node_type == "formula"
    assert [entry.function for entry in node.setting_input.entries] == ["${region}"]
    assert tagged.collect()["r"].to_list() == ["EU"] * 4


def test_boolean_parameter_compares_on_both_emission_paths():
    child, raw = _orders()
    flag = ff.Parameter("flag_on", default=True, type="boolean")
    ff.add_flow_parameter(child, flag)
    native = raw.filter(ff.col("flag") == flag)
    assert _node(native).node_type == "filter"
    assert native.collect()["id"].to_list() == [1, 3]

    ff.set_flow_parameter(child, flag, False)
    as_code = raw.with_columns((ff.col("flag") == flag).map_elements(lambda v: v, return_dtype=ff.Boolean).alias("m"))
    assert _node(as_code).node_type == "polars_code"
    assert as_code.collect()["m"].to_list() == [False, True, False, True]


@pytest.mark.parametrize(
    "kwargs, dtype, repr_str",
    [
        ({}, pl.String, 'pl.lit("${p}")'),
        ({"type": "enum", "default": "a", "enum_values": ["a", "b"]}, pl.String, 'pl.lit("${p}")'),
        ({"type": "integer"}, pl.Int64, 'pl.lit("${p}").cast(pl.Int64, strict=True)'),
        ({"type": "float"}, pl.Float64, 'pl.lit("${p}").cast(pl.Float64, strict=True)'),
        ({"type": "boolean"}, pl.Boolean, '(pl.lit("${p}") == "true")'),
    ],
)
def test_to_expr_renders_by_type(kwargs, dtype, repr_str):
    parameter = ff.Parameter("p", **kwargs)
    expr = parameter.to_expr()
    assert parameter.dtype == dtype
    assert expr._ff_repr == "${p}" == parameter.ref
    assert expr._repr_str == repr_str
    assert pl.select(expr.expr.alias("x")).schema["x"] == dtype
    literal = ff.lit(1)
    assert (expr.column_name, expr.agg_func, expr.is_complex) == (
        literal.column_name,
        literal.agg_func,
        literal.is_complex,
    )


def test_parameter_object_contract():
    parameter = ff.Parameter("limit", default="5", type="integer", description="rows")
    assert repr(parameter) == "Parameter('limit', type='integer', default=5)"
    assert (parameter.name, parameter.type, parameter.default) == ("limit", "integer", 5)
    assert parameter.model.default_value == "5" and parameter.model.description == "rows"
    assert parameter == ff.Parameter("limit") and parameter != ff.Parameter("other")
    assert parameter != "limit"
    assert {parameter: 1}[ff.Parameter("limit", type="float")] == 1
    with pytest.raises(ff.NativeNodeError, match="not a valid integer"):
        ff.Parameter("limit", default="many", type="integer")


def test_add_flow_parameter_takes_only_a_parameter_and_stores_a_copy():
    child, raw = _orders()
    min_amount = _min_amount()
    assert ff.add_flow_parameter(child, min_amount) is min_amount
    ff.set_flow_parameter(raw, min_amount, 7)
    assert child.flow_settings.parameters[-1].default_value == "7"
    assert min_amount.default == 0
    with pytest.raises(ff.NativeNodeError, match="already declared"):
        ff.add_flow_parameter(child, ff.Parameter("min_amount"))
    with pytest.raises(ff.NativeNodeError, match=r"takes a fl\.Parameter.*fl\.Parameter\(name"):
        ff.add_flow_parameter(child, "region")
    with pytest.raises(TypeError):
        ff.add_flow_parameter(child, "region", default="EU")


def test_gate_takes_a_parameter():
    source = ff.from_dict({"a": [1, 2]})
    env = ff.Parameter("env", default="prod")
    ff.add_flow_parameter(source, env)
    gate = ff.Gate(source, parameter=env, value="prod")
    assert gate.node.setting_input.gate_input.parameter == "env"
    assert gate.is_open is True
    ff.set_flow_parameter(source, env, "dev")
    assert gate.is_open is False


@pytest.mark.parametrize("declare_other", [False, True], ids=["no-parameters", "other-parameters"])
@pytest.mark.parametrize(
    "build",
    [
        lambda raw: raw.filter(ff.col("amount") >= ff.Parameter("nope", type="integer")),
        lambda raw: raw.filter(ff.col("amount") >= ff.lit("${nope}").cast(ff.Int64)),
        lambda raw: raw.with_columns(ff.lit("${nope}").alias("x")),
        lambda raw: ff.Node("filter", raw, settings={"filter_input": {"advanced_filter": "[amount] > ${nope}"}}),
    ],
    ids=["parameter", "lit-cast", "formula", "native-node"],
)
def test_undeclared_parameter_raises_at_build(build, declare_other):
    child, raw = _orders()
    if declare_other:
        ff.add_flow_parameter(child, ff.Parameter("other", default=1, type="integer"))
    with pytest.raises(ff.NativeNodeError, match=r"undeclared flow parameter\(s\) \['nope'\].*add_flow_parameter"):
        build(raw)


def test_polars_code_references_sit_inside_a_string_literal():
    child, raw = _orders()
    ff.add_flow_parameter(child, _min_amount())
    ff.set_flow_parameter(child, "min_amount", 50)
    code = 'output_df = input_df.filter(pl.col("amount") >= pl.lit("${min_amount}").cast(pl.Float64))'
    node = ff.Node("polars_code", raw, settings={"polars_code_input": {"polars_code": code}})
    assert node.output.collect()["id"].to_list() == [3, 4]
    assert "${min_amount}" in node.node.setting_input.polars_code_input.polars_code

    bare = 'output_df = input_df.filter(pl.col("amount") >= ${min_amount})'
    with pytest.raises(ff.NativeNodeError, match="(?i)syntax"):
        ff.Node("polars_code", raw, settings={"polars_code_input": {"polars_code": bare}})


def test_save_and_open_keeps_the_reference():
    child, raw = _orders()
    ff.add_flow_parameter(child, _min_amount())
    kept = raw.filter(ff.col("amount") >= _min_amount())
    kept.collect()
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "child.yaml")
        kept.save_graph(path)
        reopened = open_flow(Path(path))
    node = reopened.get_node(kept.node_id)
    assert node.setting_input.filter_input.advanced_filter == "([amount] >= ${min_amount})"
    assert [p.name for p in reopened.flow_settings.parameters] == ["min_amount"]
    reopened.run_graph()
    result = node.get_resulting_data().data_frame
    result = result.collect() if isinstance(result, pl.LazyFrame) else result
    assert_frame_equal(result, pl.DataFrame(ORDERS))


@pytest.mark.parametrize(
    "build, position",
    [
        (lambda raw, p: ff.col(p), "fl.col"),
        (lambda raw, p: raw.select(p), "select"),
        (lambda raw, p: raw.select("${region}"), "select"),
        (lambda raw, p: raw.group_by(p), "group_by"),
        (lambda raw, p: raw.join(raw, on=p), "join keys"),
        (lambda raw, p: raw.rename({"id": p}), "rename"),
        (lambda raw, p: ff.col("id").alias(p), "alias"),
        (lambda raw, p: raw.sort("${x}"), "sort"),
        (lambda raw, p: raw.drop(p), "drop"),
        (lambda raw, p: raw.unique(subset=[p]), "unique(subset=)"),
        (lambda raw, p: raw.with_row_index(name=p), "with_row_index(name=)"),
        (lambda raw, p: ff.FlowInput("i", schema={p: ff.Int64}), "FlowInput(schema=)"),
        (lambda raw, p: raw.to_flow_output(p), "to_flow_output"),
        (lambda raw, p: raw.filter(flowfile_formula="[${region}] == 'EU'"), "filter formula"),
        (
            lambda raw, p: raw.with_columns(flowfile_formulas=["[${region}]"], output_column_names=["r"]),
            "with_columns(flowfile_formulas=)",
        ),
        (lambda raw, p: ff.Gate(raw, "[${region}] == 'EU'"), "Gate formula"),
    ],
)
def test_parameter_in_a_column_name_position_raises(build, position):
    child, raw = _orders()
    region = ff.Parameter("region", default="EU")
    ff.add_flow_parameter(child, region)
    with pytest.raises(ff.NativeNodeError) as info:
        build(raw, region)
    message = str(info.value)
    assert f"`{position}" in message
    assert "values, not column names" in message and SUPPORTED_USES in message


def test_parameter_as_a_formula_value_is_supported():
    child, raw = _orders()
    ff.add_flow_parameter(child, ff.Parameter("region", default="EU"))
    tagged = raw.with_columns(ff.lit("EU").alias("region"))
    kept = tagged.filter(flowfile_formula="[region] == ${region}")
    assert _node(kept).node_type == "filter"
    assert kept.collect().height == 4
