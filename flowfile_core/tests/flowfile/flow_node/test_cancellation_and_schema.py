"""Tests for cancellation handling and database_reader schema-callback wiring.

Covers the fix for remote flows that hung / re-spawned worker tasks on cancel
when a source was unreachable:
  - get_resulting_data must not (re)run the node function once the node is canceled
  - reset() and _prepare_for_execution must clear the cancel flag between runs
  - the database_reader keeps its lightweight get_schema callback across reset
    (instead of falling back to a full worker data read for schema prediction)
"""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


def _graph(flow_id: int, execution_location: str = "local") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="cancel_test",
            path=".",
            execution_mode="Development",
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def _add_manual_input(graph: FlowGraph, node_id: int = 1) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    )
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Alice", "age": 30}]),
        )
    )


def test_canceled_node_does_not_run_function():
    graph = _graph(9101)
    _add_manual_input(graph)
    node = graph.get_node(1)
    assert node.get_resulting_data() is not None  # normal path produces data

    # Simulate a cancel arriving as the node (re)enters execution: results cleared
    # (as on a fresh run) and the node flagged canceled. The guard must stop it from
    # running the function again (which previously spawned a fresh worker task).
    node.results.resulting_data = None
    node.results.errors = None
    node._execution_state.is_canceled = True

    with pytest.raises(Exception, match="cancel"):
        node.get_resulting_data()
    assert node.results.errors and "cancel" in node.results.errors.lower()


def test_reset_clears_cancel_flag():
    graph = _graph(9102)
    _add_manual_input(graph)
    node = graph.get_node(1)
    node._execution_state.is_canceled = True
    node.reset(deep=True)
    assert node._execution_state.is_canceled is False


def test_prepare_for_execution_clears_cancel_flag():
    graph = _graph(9103)
    _add_manual_input(graph)
    node = graph.get_node(1)
    node._execution_state.is_canceled = True
    node.executor._prepare_for_execution(node._execution_state)
    assert node._execution_state.is_canceled is False


def test_database_reader_keeps_lightweight_schema_callback(sqlite_db):
    """Regression: the DB reader's direct get_schema callback must survive the
    reset() triggered by setting_input, instead of falling back to a full worker
    data read (the root cause of the hang / cancel re-trigger)."""
    graph = _graph(9104, execution_location="local")
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="database_reader")
    )
    connection = input_schema.DatabaseConnection(
        database_type="sqlite", password_ref="", database=f"sqlite:///{sqlite_db}"
    )
    settings = input_schema.DatabaseSettings(database_connection=connection, table_name="movies")
    reader = input_schema.NodeDatabaseReader(
        database_settings=settings, node_id=1, flow_id=graph.flow_id, user_id=1
    )
    graph.add_database_reader(reader)

    node = graph.get_node(1)
    assert node.user_provided_schema_callback is not None

    # After a reset (which nulls _schema_callback) schema prediction still works
    # via the lightweight callback — no worker / full data read needed.
    node.reset(deep=True)
    schema = node.get_predicted_schema()
    assert schema is not None and len(schema) == 6
