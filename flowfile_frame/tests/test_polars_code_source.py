"""``fl.polars_code``: a Polars Code node over zero or more frames; with none it is a source node."""

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_frame.native import NativeNodeError

from .native_helpers import core_node, round_trip


def test_no_input_places_a_source_polars_code_node():
    out = ff.polars_code("output_df = pl.LazyFrame({'a': [1, 2, 3]})", description="three rows")
    node = core_node(out)
    assert node.node_type == "polars_code" and node.all_inputs == []
    assert node.setting_input.description == "three rows"
    assert out.collect()["a"].to_list() == [1, 2, 3]


def test_a_def_without_parameters_stores_its_body():
    def make():
        output_df = pl.LazyFrame({"a": [1, 2]})
        return output_df

    out = ff.polars_code(make)
    assert core_node(out).setting_input.polars_code_input.polars_code == 'output_df = pl.LazyFrame({"a": [1, 2]})'
    assert out.collect().height == 2


def test_flow_graph_places_the_source_on_that_graph():
    graph = ff.create_flow_graph()
    out = ff.polars_code("pl.LazyFrame({'a': [1]})", flow_graph=graph)
    assert out.flow_graph is graph and [n.node_type for n in graph.nodes] == ["polars_code"]


def test_inputs_are_the_frame_method():
    left, right = ff.from_dict({"a": [1, 2]}), ff.from_dict({"a": [3, 4]})
    out = ff.polars_code("pl.concat([input_df_1, input_df_2])", left, right)
    assert sorted(out.collect()["a"].to_list()) == [1, 2, 3, 4]
    assert [n.node_id for n in core_node(out).all_inputs] == [left.node_id, right.node_id]


def test_inputs_on_another_graph_than_flow_graph_are_refused():
    with pytest.raises(NativeNodeError, match="another graph"):
        ff.polars_code("input_df", ff.from_dict({"a": [1, 2]}), flow_graph=ff.create_flow_graph())


def test_failing_code_leaves_no_node_behind():
    graph = ff.create_flow_graph()
    with pytest.raises(NativeNodeError):
        ff.polars_code("output_df = no_such_name", flow_graph=graph)
    assert graph.nodes == []


def test_a_source_polars_code_round_trips():
    out = ff.polars_code("pl.LazyFrame({'a': [1]})")
    reopened, _ = round_trip(out, "source_polars_code.yaml")
    assert reopened.get_node(out.node_id).node_type == "polars_code"
