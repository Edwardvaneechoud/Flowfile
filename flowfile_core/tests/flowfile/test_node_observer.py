"""The FlowGraph node-observer seam: every add_<type> call reports (node_id, node_type, settings, is_new)."""

import ast
import inspect
import textwrap

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, transform_schema

NON_NODE_ADD_METHODS = {
    "add_include_cols",
    "add_node_observer",
    "add_node_step",
    "add_node_to_starting_list",
    "add_nodes_to_group",
}


class Recorder:
    def __init__(self):
        self.calls: list[tuple] = []

    def __call__(self, node_id, node_type, settings, is_new):
        self.calls.append((node_id, node_type, settings, is_new))

    @property
    def summary(self) -> list[tuple]:
        return [(node_id, node_type, is_new) for node_id, node_type, _, is_new in self.calls]


def _manual_input(graph: FlowGraph, node_id: int = 1) -> input_schema.NodeManualInput:
    settings = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist([{"a": 1}, {"a": 2}]),
    )
    graph.add_manual_input(settings)
    return settings


def _filter(graph: FlowGraph, node_id: int = 2, depending_on_id: int = 1) -> input_schema.NodeFilter:
    settings = input_schema.NodeFilter(
        flow_id=graph.flow_id,
        node_id=node_id,
        depending_on_id=depending_on_id,
        filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[a] > 1"),
    )
    graph.add_filter(settings)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))
    return settings


def test_observer_sees_every_node_in_creation_order(tmp_path):
    csv_path = tmp_path / "data.csv"
    pl.DataFrame({"x": [1, 2]}).write_csv(csv_path)
    graph = FlowGraph()
    recorder = Recorder()
    graph.add_node_observer(recorder)

    manual = _manual_input(graph, 1)
    filter_settings = _filter(graph, 2, 1)
    graph.add_record_count(input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=3, depending_on_id=2))
    graph.add_read(
        input_schema.NodeRead(
            flow_id=graph.flow_id,
            node_id=4,
            received_file=input_schema.ReceivedTable(file_type="csv", name="data.csv", path=str(csv_path)),
        )
    )
    graph.add_list_files(input_schema.NodeListFiles(flow_id=graph.flow_id, node_id=5, path=str(tmp_path)))
    graph.add_flow_input(input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=6, input_name="orders"))

    assert recorder.summary == [
        (1, "manual_input", True),
        (2, "filter", True),
        (3, "record_count", True),
        (4, "read", True),
        (5, "list_files", True),
        (6, "flow_input", True),
    ]
    assert recorder.calls[0][2] is manual
    assert recorder.calls[1][2] is filter_settings


def test_update_in_place_reports_not_new_and_type_change_reports_new():
    graph = FlowGraph()
    _manual_input(graph, 1)
    _filter(graph, 2, 1)
    recorder = Recorder()

    with graph.observe_nodes(recorder):
        _manual_input(graph, 1)
        _filter(graph, 2, 1)
        graph.add_record_count(input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=2, depending_on_id=1))

    assert recorder.summary == [(1, "manual_input", False), (2, "filter", False), (2, "record_count", True)]


def test_node_promise_then_settings_fires_twice():
    graph = FlowGraph()
    _manual_input(graph, 1)
    recorder = Recorder()
    graph.add_node_observer(recorder)

    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter"))
    _filter(graph, 2, 1)

    assert recorder.summary == [(2, "filter", True), (2, "filter", False)]
    assert isinstance(recorder.calls[0][2], input_schema.NodePromise)
    assert isinstance(recorder.calls[1][2], input_schema.NodeFilter)


def test_unregistered_observers_stop_receiving():
    graph = FlowGraph()
    removed, scoped = Recorder(), Recorder()
    graph.add_node_observer(removed)
    graph.remove_node_observer(removed)
    graph.remove_node_observer(removed)
    with graph.observe_nodes(scoped):
        _manual_input(graph, 1)
    _filter(graph, 2, 1)

    assert removed.calls == []
    assert scoped.summary == [(1, "manual_input", True)]
    assert graph._node_observers == []


def test_observe_nodes_unregisters_when_the_block_raises():
    graph = FlowGraph()
    recorder = Recorder()
    with pytest.raises(RuntimeError), graph.observe_nodes(recorder):
        raise RuntimeError("boom")
    assert graph._node_observers == []


def test_failing_observer_never_corrupts_the_graph():
    graph = FlowGraph()

    def broken(*_args):
        raise ValueError("observer bug")

    recorder = Recorder()
    graph.add_node_observer(broken)
    graph.add_node_observer(recorder)

    _manual_input(graph, 1)
    _filter(graph, 2, 1)

    assert recorder.summary == [(1, "manual_input", True), (2, "filter", True)]
    assert {n.node_id for n in graph.nodes} == {1, 2}
    assert [c.column_name for c in graph.get_node(2).schema] == ["a"]
    run_info = graph.run_graph()
    assert run_info.success
    assert graph.get_node(2).get_resulting_data().data_frame.collect().to_dicts() == [{"a": 2}]


def _flow_graph_methods() -> dict[str, ast.FunctionDef]:
    methods: dict[str, ast.FunctionDef] = {}
    for cls in reversed(FlowGraph.__mro__[:-1]):
        tree = ast.parse(textwrap.dedent(inspect.getsource(cls)))
        methods.update({i.name: i for i in tree.body[0].body if isinstance(i, ast.FunctionDef | ast.AsyncFunctionDef)})
    return methods


def _self_calls(func: ast.AST) -> set[str]:
    return {
        node.func.attr
        for node in ast.walk(func)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "self"
    }


def _builds_flow_node(func: ast.AST) -> bool:
    return any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "FlowNode"
        for node in ast.walk(func)
    )


def test_every_add_method_notifies_observers():
    """Driven from FlowGraph's own add_* methods, so a new node type that forgets to notify fails here.

    A method notifies when it calls ``add_node_step`` or ``_notify_node_observers``, directly or
    through another FlowGraph method. One that builds ``FlowNode`` itself must make one of those
    calls in its own body, next to the construction, rather than rely on a helper it delegates to.
    """
    methods = _flow_graph_methods()
    calls = {name: _self_calls(func) for name, func in methods.items()}

    def notifies(name: str, seen: frozenset = frozenset()) -> bool:
        direct = calls.get(name, set())
        if direct & {"add_node_step", "_notify_node_observers"}:
            return True
        return any(notifies(callee, seen | {name}) for callee in direct if callee in calls and callee not in seen)

    add_methods = sorted(
        name for name in dir(FlowGraph) if name.startswith("add_") and callable(getattr(FlowGraph, name))
    )
    node_methods = [name for name in add_methods if name not in NON_NODE_ADD_METHODS]
    assert "add_python_script" in node_methods and "add_read" in node_methods

    silent = [name for name in node_methods if not notifies(name)]
    assert silent == [], f"add_* methods that never notify node observers: {silent}"

    hand_built = [
        name
        for name in node_methods
        if _builds_flow_node(methods[name]) and not calls[name] & {"add_node_step", "_notify_node_observers"}
    ]
    assert hand_built == [], f"add_* methods that build FlowNode without notifying: {hand_built}"
