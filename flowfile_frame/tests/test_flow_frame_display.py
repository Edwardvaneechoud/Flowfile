"""FlowFrame display stays compact and never executes the data pipeline."""

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_core.flowfile.flow_node.flow_node import FlowNode


@pytest.fixture
def no_display_execution(monkeypatch):
    def refuse_execution(*args, **kwargs):
        pytest.fail("Displaying a FlowFrame must not resolve its schema, execute data, or request its plan")

    def guard(frame):
        monkeypatch.setattr(pl.LazyFrame, "collect", refuse_execution)
        monkeypatch.setattr(pl.LazyFrame, "collect_schema", refuse_execution)
        monkeypatch.setattr(pl.LazyFrame, "explain", refuse_execution)
        monkeypatch.setattr(ff.FlowFrame, "collect", refuse_execution)
        monkeypatch.setattr(FlowNode, "get_predicted_schema", refuse_execution)
        monkeypatch.setattr(FlowNode, "get_resulting_data", refuse_execution)
        monkeypatch.setattr(FlowNode, "schema", property(refuse_execution))
        monkeypatch.setattr(frame.flow_graph, "run_graph", refuse_execution)

    return guard


def test_print_and_repr_show_metadata_with_hints_in_help(no_display_execution, capsys):
    frame = ff.concat(
        [ff.FlowFrame({"name": ["Alice"]}), ff.FlowFrame({"name": ["Bob"]})], how="diagonal_relaxed"
    )
    original_data = frame.data
    original_nodes = list(frame.flow_graph.nodes)
    no_display_execution(frame)

    display = repr(frame)
    assert display == (
        f"FlowFrame (lazy)\n  Node id: {frame.node_id} (union)\n  Flow id: {frame.flow_graph.flow_id}\n"
        f"  Data: {frame.data!r}"
    )
    print(frame)
    assert capsys.readouterr().out == display + "\n"
    frame.help()
    help_text = capsys.readouterr().out
    for hint in (".select(...)", ".filter(...)", ".with_columns(...)", ".get_node_settings()", ".explain()", ".save_graph("):
        assert hint in help_text
    assert frame.data is original_data
    assert list(frame.flow_graph.nodes) == original_nodes


def test_display_does_not_need_schema(no_display_execution):
    frame = ff.FlowFrame({"a": [1]})
    node = frame.get_node_settings()
    node.node_schema = None
    node._named_schemas = None
    no_display_execution(frame)

    assert f"Node id: {frame.node_id} (manual_input)" in repr(frame)


def test_missing_node_still_displays_flow(no_display_execution, monkeypatch):
    frame = ff.FlowFrame({"a": [1]})
    monkeypatch.setattr(frame, "get_node_settings", lambda: None)
    no_display_execution(frame)

    display = repr(frame)
    assert f"Node id: {frame.node_id} (unavailable)" in display
    assert f"Flow id: {frame.flow_graph.flow_id}" in display
