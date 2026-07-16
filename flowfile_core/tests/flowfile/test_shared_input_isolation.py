"""Shared-input isolation: consumers must never mutate a shared upstream engine.

``get_resulting_data`` memoizes and returns engines by reference, and sibling
consumers in one parallel stage read the same upstream concurrently (no
cross-node locks since the AB-BA deadlock fix). Some node functions mutate
their input (``df.lazy = True`` in the writers, cross_join/fuzzy prep) or
return it unchanged (output/filter passthroughs), after which the framework's
own post-processing (``set_streamable``) mutates whatever was returned. The
input-collection seam therefore hands every node function a
``FlowDataEngine.shallow_copy()`` — these tests pin that isolation.
"""

import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema


def _make_graph(execution_location: str = "remote") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=1, name="isolation", path=".", execution_location=execution_location
        )
    )
    return handler.get_flow(1)


def _add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(data)
        )
    )


def test_shallow_copy_shares_frame_but_isolates_flags():
    src = FlowDataEngine(pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
    assert src.lazy is False

    copy = src.shallow_copy()
    assert copy.data_frame is src.data_frame, "copy must share the underlying frame"
    assert copy.number_of_records == src.number_of_records
    assert [c.column_name for c in copy.schema] == [c.column_name for c in src.schema]

    # Mutations on the copy must not reach the source.
    copy.lazy = True
    copy.set_streamable(False)
    assert src.lazy is False, "source flipped to lazy through the copy"
    assert src._streamable is True, "source streamable flag mutated through the copy"
    assert copy.lazy is True
    assert isinstance(src.data_frame, pl.DataFrame), "source frame rebound through the copy"


def test_shallow_copy_is_collect_free_when_schema_cached(monkeypatch):
    src = FlowDataEngine(pl.LazyFrame({"a": [1, 2, 3]}))
    assert src._schema is not None, "test precondition: schema cached on the source"

    calls: list[int] = []
    orig = pl.LazyFrame.collect_schema

    def _spy(self, *args, **kwargs):
        calls.append(1)
        return orig(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect_schema", _spy)
    src.shallow_copy()
    assert calls == [], "shallow_copy must not resolve the schema when it is already cached"


def test_passthrough_node_result_is_not_the_upstream_engine(flowfile_worker):
    """The filter passthrough edge (basic mode, basic_filter=None) returns its
    input unchanged; the seam copy must keep the node's memoized result — and
    the framework's set_streamable — off the upstream's engine."""
    graph = _make_graph()
    _add_manual_input(graph, [{"a": 1}, {"a": 2}], node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="filter"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="a", operator="equals", value="1"),
            ),
        )
    )
    # Force the audited passthrough edge: basic mode with basic_filter=None.
    graph.get_node(2).setting_input.filter_input.basic_filter = None

    run_info = graph.run_graph()
    assert run_info.success

    upstream = graph.get_node(1).results.resulting_data
    downstream = graph.get_node(2).results.resulting_data
    assert downstream is not None and upstream is not None
    assert downstream is not upstream, "passthrough node memoized the shared upstream engine"


def test_pivot_schema_callback_does_not_mutate_memoized_upstream():
    graph = _make_graph()
    _add_manual_input(graph, [{"Country": "NL", "groups": "g1", "sales": 10}], node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="pivot"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_pivot(
        input_schema.NodePivot(
            flow_id=1,
            node_id=2,
            pivot_input=transform_schema.PivotInput(
                pivot_column="groups", value_col="sales", index_columns=["Country"], aggregations=["sum"]
            ),
        )
    )

    # Plant an EAGER memoized result on the upstream: before the fix, the
    # pivot schema callback flipped it lazy in place (a cross-thread mutation
    # of the shared engine); now it must build a local lazy frame instead.
    eager = FlowDataEngine(pl.DataFrame({"Country": ["NL"], "groups": ["g1"], "sales": [10]}))
    upstream = graph.get_node(1)
    upstream.results.resulting_data = eager
    frame_before = eager.data_frame

    schema = graph.get_node(2).schema_callback()
    assert schema is not None

    assert eager.lazy is False, "schema callback flipped the shared memoized engine to lazy"
    assert eager.data_frame is frame_before, "schema callback rebound the shared engine's frame"


def test_writer_style_mutation_on_copy_does_not_leak_upstream():
    """Simulates what database/cloud writers do to their input (df.lazy = True)
    and what the framework does to a passthrough return (set_streamable):
    both must land on the seam copy, never the shared source."""
    src = FlowDataEngine(pl.DataFrame({"a": [1]}))
    handed_to_node_function = src.shallow_copy()

    handed_to_node_function.lazy = True          # writer behavior
    handed_to_node_function.set_streamable(False)  # framework post-processing

    assert src.lazy is False
    assert src._streamable is True
    assert isinstance(src.data_frame, pl.DataFrame)
