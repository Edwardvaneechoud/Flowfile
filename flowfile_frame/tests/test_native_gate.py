"""``ff.Gate``: build, route, probe and round-trip a gate node from Python.

Routing is asserted the way ``flowfile_core/tests/flowfile/test_gate_node.py`` does: a closed
exit's downstream is deliberately skipped (``NodeResult.skipped``), the gate itself runs, and
the run stays green with every node accounted for.
"""

import os
import tempfile
from pathlib import Path

import polars as pl
import pytest
import yaml
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.flowfile.manage.io_flowfile import open_flow

DATA = {"a": [1, 2, 3], "g": ["x", "x", "y"]}
SALES = {"region": ["EU", "US", "EU"], "amount": [10, 20, 30]}
TEN_X = "output_df = input_df.with_columns((pl.col('a') * 10).alias('a10'))"


def results_by_id(run_info) -> dict:
    return {result.node_id: result for result in run_info.node_step_result}


def _handle_into(target: ff.FlowFrame, source_id: int) -> str:
    return target.flow_graph.get_node(target.node_id)._input_output_handles[source_id]


def _env_gate(env: str, **gate_kwargs) -> tuple[ff.FlowFrame, ff.Gate]:
    source = ff.from_dict(DATA)
    ff.add_flow_parameter(source, "env", default=env)
    return source, ff.Gate(source, parameter="env", value="prod", **gate_kwargs)


def _deferred_source() -> ff.FlowFrame:
    source = ff.from_dict(DATA)
    return ff.Node("polars_code", source, settings={"polars_code_input": {"polars_code": TEN_X}}, deferred=True).output


# building


def test_parameter_gate_node_settings_and_exits():
    source, gate = _env_gate("prod", description="only in prod")
    node = gate.node
    assert node.node_type == "gate"
    settings = node.setting_input
    assert settings.else_output is True
    assert settings.description == "only in prod"
    assert settings.depending_on_id == source.node_id
    assert settings.gate_input.model_dump() == {
        "condition_source": "parameter",
        "parameter": "env",
        "operator": "equals",
        "value": "prod",
        "formula": "",
    }
    assert gate.outputs == ["then", "else"]
    assert (gate.then.output_handle, gate.otherwise.output_handle) == ("output-0", "output-1")
    assert gate.else_ is gate.otherwise and gate.output is gate.then and gate["else"] is gate.otherwise
    assert node.node_inputs.main_inputs[0].node_id == source.node_id
    assert_frame_equal(gate.then.collect(), pl.DataFrame(DATA))
    assert_frame_equal(gate.otherwise.collect(), pl.DataFrame(DATA).clear())


def test_values_are_stored_in_their_canvas_form():
    source = ff.from_dict(DATA)
    ff.add_flow_parameter(source, "flag", default=True, type="boolean")
    ff.add_flow_parameter(source, "env", default="prod")
    as_bool = ff.Gate(source, parameter="flag", value=True)
    as_list = ff.Gate(source, parameter="env", operator="in", value=["prod", "staging"])
    assert as_bool.node.setting_input.gate_input.value == "true"
    assert as_list.node.setting_input.gate_input.value == "prod,staging"
    assert as_bool.is_open and as_list.is_open
    with pytest.raises(ff.NativeNodeError, match="'in' or 'not_in'"):
        ff.Gate(source, parameter="env", value=["prod"])


def test_else_output_false_has_one_exit():
    _, gate = _env_gate("prod", else_output=False)
    assert gate.outputs == ["main"]
    assert gate.output is gate.then
    with pytest.raises(ff.NativeNodeError, match="else_output=True"):
        gate.otherwise


def test_undeclared_parameter_raises_and_leaves_no_node():
    source = ff.from_dict(DATA)
    graph = source.flow_graph
    with pytest.raises(ff.NativeNodeError, match="add_flow_parameter"):
        ff.Gate(source, parameter="missing", value="x")
    assert [n.node_id for n in graph.nodes] == [source.node_id]


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({}, "exactly one condition"),
        ({"formula": "[a] > 1", "parameter": "env"}, "exactly one condition"),
        ({"formula": "  "}, "empty"),
        ({"parameter": "env", "control": "frame"}, "control= only applies"),
        ({"parameter": "env", "operator": "bigger"}, "Invalid gate condition"),
        ({"parameter": "limit", "value": "many"}, "not a valid integer"),
    ],
)
def test_invalid_conditions_raise(kwargs, match):
    source = ff.from_dict(DATA)
    ff.add_flow_parameter(source, "env", default="prod")
    ff.add_flow_parameter(source, "limit", default=3, type="integer")
    if kwargs.get("control") == "frame":
        kwargs["control"] = source
    with pytest.raises(ff.NativeNodeError, match=match):
        ff.Gate(source, **kwargs)


# routing through run_graph


@pytest.mark.parametrize("mode", ["full", "quick"])
def test_parameter_diamond_skips_the_dead_side(mode):
    source = ff.from_dict(DATA)
    ff.add_flow_parameter(source, "mode", default="full", type="enum", enum_values=["full", "quick"])
    gate = ff.Gate(source, parameter="mode", operator=ff.GateOperator.EQUALS, value="full")
    full = gate.then.with_columns(ff.lit("full").alias("tag"))
    quick = gate.otherwise.with_columns(ff.lit("quick").alias("tag"))
    merged = ff.concat([full, quick], how="diagonal_relaxed")
    ff.set_flow_parameter(source, "mode", mode)
    assert gate.is_open is (mode == "full")

    run_info = merged.flow_graph.run_graph()

    by_id = results_by_id(run_info)
    live, dead = (full, quick) if mode == "full" else (quick, full)
    assert by_id[dead.node_id].skipped is True
    assert by_id[live.node_id].skipped is False
    assert by_id[gate.node_id].skipped is False and by_id[gate.node_id].success is True
    assert by_id[merged.node_id].skipped is False
    assert run_info.success is True
    assert run_info.nodes_completed == run_info.number_of_nodes == len(merged.flow_graph.nodes)
    union = merged.flow_graph.get_node(merged.node_id).get_resulting_data().collect()
    assert union["tag"].to_list() == [mode] * 3


@pytest.mark.parametrize("levels, is_open", [(["info", "error"], True), (["info", "debug"], False)])
def test_formula_gate_probes_the_control_frame(levels, is_open):
    source = ff.from_dict(DATA)
    log = ff.from_dict({"level": levels})
    gate = ff.Gate(source, "[level] == 'error'", control=log)

    assert source.flow_graph is log.flow_graph is gate.flow_graph
    settings = gate.node.setting_input.gate_input
    assert (settings.condition_source, settings.formula) == ("formula", "[level] == 'error'")
    assert gate.node.node_inputs.right_input.node_id == log.node_id
    assert gate.is_open is is_open

    downstream = gate.then.select("a")
    run_info = downstream.flow_graph.run_graph()
    assert run_info.success is True
    assert results_by_id(run_info)[downstream.node_id].skipped is (not is_open)


@pytest.mark.parametrize("formula, is_open", [("[a] > 2", True), ("[a] > 5", False)])
def test_formula_gate_without_control_probes_its_data(formula, is_open):
    gate = ff.Gate(ff.from_dict(DATA), formula)
    assert gate.is_open is is_open


def test_is_open_follows_parameter_changes():
    source, gate = _env_gate("prod")
    assert gate.is_open is True
    ff.set_flow_parameter(source, "env", "dev")
    assert gate.is_open is False


def test_is_open_on_a_deferred_probe_raises():
    gate = ff.Gate(_deferred_source(), "[a] > 1")
    with pytest.raises(ff.NativeNodeError, match="run the graph"):
        gate.is_open


@pytest.mark.parametrize("env", ["prod", "dev"])
def test_gate_on_a_deferred_frame_collects_live_and_dead_exits(env):
    deferred = _deferred_source()
    ff.add_flow_parameter(deferred, "env", default=env)
    gate = ff.Gate(deferred, parameter="env", value="prod")
    assert gate.then._deferred and gate.otherwise._deferred
    live, dead = (gate.then, gate.otherwise) if env == "prod" else (gate.otherwise, gate.then)

    expected = pl.DataFrame(DATA).with_columns((pl.col("a") * 10).alias("a10"))
    assert_frame_equal(live.collect(), expected)
    dead_rows = dead.collect()
    assert dead_rows.height == 0 and dead_rows.columns == ["a", "g", "a10"]


# collect() below a gate runs the flow


def _routed_union(gate: ff.Gate) -> ff.FlowFrame:
    full = gate.then.group_by("region").agg(ff.col("amount").sum().alias("revenue")).head(2)
    quick = gate.otherwise.head(1)
    return ff.concat([full, quick], how="diagonal_relaxed")


def test_union_below_a_parameter_gate_collects_only_the_live_side():
    sales = ff.from_dict(SALES)
    ff.add_flow_parameter(sales, "mode", default="full", type="enum", enum_values=["full", "quick"])
    gate = ff.Gate(sales, parameter="mode", operator=ff.GateOperator.EQUALS, value="full")
    output = _routed_union(gate)
    assert output._deferred is False

    full_rows = output.collect()
    assert sorted(full_rows["revenue"].to_list()) == [20, 40]
    ff.set_flow_parameter(sales, "mode", "quick")
    quick_rows = output.collect()
    assert quick_rows.height == 1 and quick_rows["amount"].to_list() == [10]


@pytest.mark.parametrize("formula, height", [("[amount] > 15", 2), ("[amount] > 100", 1)])
def test_union_below_a_formula_gate_collects_only_the_live_side(formula, height):
    gate = ff.Gate(ff.from_dict(SALES), formula)
    assert _routed_union(gate).collect().height == height
    dead = gate.otherwise if height == 2 else gate.then
    assert dead.head(1).collect().height == 0


@pytest.mark.parametrize("env", ["prod", "dev"])
def test_fluent_node_below_an_exit_follows_the_routing(env):
    _, gate = _env_gate(env)
    live, dead = (gate.then, gate.otherwise) if env == "prod" else (gate.otherwise, gate.then)

    assert_frame_equal(live.head(1).collect(), pl.DataFrame(DATA).head(1))
    dead_rows = dead.head(1).collect()
    assert dead_rows.height == 0 and dead_rows.columns == ["a", "g"]


def test_describe_below_a_gate_runs_the_flow():
    _, gate = _env_gate("dev")
    count = pl.col("statistic") == "count"
    assert gate.then.select("a").describe().filter(count)["a"].item() == 0
    assert gate.otherwise.select("a").describe().filter(count)["a"].item() == 3
    assert gate.flow_graph.latest_run_info is not None


# exits into other nodes


def test_otherwise_into_join_and_concat_keeps_output_1():
    source, gate = _env_gate("dev")
    other = source.select("a", (ff.col("a") * 100).alias("hundred"))

    joined = gate.otherwise.join(other, on="a")
    combined = ff.concat([source.filter(ff.col("a") > 2), gate.otherwise])

    assert _handle_into(joined, gate.node_id) == "output-1"
    assert _handle_into(combined, gate.node_id) == "output-1"
    run_info = joined.flow_graph.run_graph()
    assert results_by_id(run_info)[joined.node_id].skipped is False
    assert sorted(joined.flow_graph.get_node(joined.node_id).get_resulting_data().collect()["hundred"]) == [
        100,
        200,
        300,
    ]


def test_both_exits_into_one_consumer_raise():
    _, gate = _env_gate("prod")
    with pytest.raises(ff.NativeNodeError, match="single output handle"):
        gate.then.join(gate.otherwise, on="a")
    graph = gate.flow_graph
    node_count = len(graph.nodes)
    with pytest.raises(ff.NativeNodeError):
        ff.Node("union", gate.then, gate.otherwise)
    assert len(graph.nodes) == node_count


# save / open


def test_round_trip_keeps_the_control_edge():
    source = ff.from_dict(DATA)
    log = source.select(ff.col("g").alias("level"))
    gate = ff.Gate(source, "[level] == 'y'", control=log, description="has y")
    out = gate.then.select("a")
    expected = out.collect()

    with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
        first = os.path.join(first_dir, "gate_roundtrip.yaml")
        second = os.path.join(second_dir, "gate_roundtrip.yaml")
        out.save_graph(first)
        reopened = open_flow(Path(first))
        reopened.save_flow(second)
        with open(first, encoding="utf-8") as f:
            first_doc = yaml.safe_load(f)
        with open(second, encoding="utf-8") as f:
            second_doc = yaml.safe_load(f)

    for key in ("nodes", "flowfile_settings", "groups", "comments"):
        assert first_doc[key] == second_doc[key], key
    reopened_gate = reopened.get_node(gate.node_id)
    assert reopened_gate.node_inputs.right_input.node_id == log.node_id
    assert reopened_gate.node_inputs.main_inputs[0].node_id == source.node_id
    assert reopened_gate.setting_input.gate_input.formula == "[level] == 'y'"
    assert reopened_gate.setting_input.else_output is True
    saved_gate = next(n for n in first_doc["nodes"] if n["id"] == gate.node_id)
    assert saved_gate["right_input_id"] == log.node_id

    run_info = reopened.run_graph()
    assert run_info.success is True
    assert results_by_id(run_info)[out.node_id].skipped is False
    assert_frame_equal(reopened.get_node(out.node_id).get_resulting_data().collect(), expected)


def test_dead_exit_built_after_an_earlier_run_still_collects_empty():
    """A frame built after the graph already ran wraps real rows; the gate routing must still win."""
    import flowfile_frame as ff
    from flowfile_frame.gate import Gate
    from flowfile_frame.parameters import add_flow_parameter

    orders = ff.from_dict({"id": [1, 2], "amount": [5, 50]})
    add_flow_parameter(orders, "mode", default="a")
    orders.flow_graph.run_graph()
    gate = Gate(orders, parameter="mode", value="a")
    assert gate.then.collect().height == 2
    dead = gate.otherwise
    dead._deferred = True
    empty = dead.collect()
    assert empty.height == 0
    assert empty.columns == ["id", "amount"]
