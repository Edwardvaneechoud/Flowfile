"""Tests for deferred (run-time) connection/secret resolution.

Connection-backed nodes (database reader/writer, kafka source, google
analytics reader, rest api reader) resolve their connection inside run-time
closures instead of at add-time, so opening / undoing a flow never requires
the current session to own the connection. These tests pin that contract,
the schema-from-cached-fields behavior, and the user_id re-stamp on undo.
"""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.history_schema import HistoryActionType

MISSING_CONNECTION = "definitely_not_a_real_connection_xyz"
OWNER_USER_ID = 1


# Helpers


def create_graph(flow_id: int = 1, track_history: bool = False) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="test_flow",
            path=".",
            execution_mode="Development",
            execution_location="local",
            track_history=track_history,
        )
    )
    return handler.get_flow(flow_id)


def add_node_promise(graph: FlowGraph, node_type: str, node_id: int) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type=node_type))


def make_fields() -> list[input_schema.MinimalFieldInfo]:
    return [
        input_schema.MinimalFieldInfo(name="a", data_type="String"),
        input_schema.MinimalFieldInfo(name="b", data_type="Int64"),
    ]


def make_database_reader(
    node_id: int = 1,
    fields: list[input_schema.MinimalFieldInfo] | None = None,
) -> input_schema.NodeDatabaseReader:
    database_settings = input_schema.DatabaseSettings(
        connection_mode="reference",
        database_connection_name=MISSING_CONNECTION,
        schema_name="public",
        table_name="movies",
    )
    return input_schema.NodeDatabaseReader(
        database_settings=database_settings,
        node_id=node_id,
        flow_id=1,
        user_id=OWNER_USER_ID,
        fields=fields,
    )


def make_kafka_source(
    node_id: int = 1,
    fields: list[input_schema.MinimalFieldInfo] | None = None,
    topic: str = "topic-a",
    connection_id: int = 987654,
) -> input_schema.NodeKafkaSource:
    return input_schema.NodeKafkaSource(
        node_id=node_id,
        flow_id=1,
        user_id=OWNER_USER_ID,
        fields=fields,
        kafka_settings=input_schema.KafkaSourceSettings(
            kafka_connection_id=connection_id,
            topic_name=topic,
        ),
    )


# Adding nodes never needs the connection


def test_add_database_reader_with_missing_connection_does_not_raise():
    graph = create_graph()
    add_node_promise(graph, "database_reader", 1)
    graph.add_database_reader(make_database_reader())
    assert graph.get_node(1) is not None, "Node should be added without resolving the connection"


def test_add_database_writer_with_missing_connection_does_not_raise():
    graph = create_graph()
    add_node_promise(graph, "database_writer", 1)
    database_write_settings = input_schema.DatabaseWriteSettings(
        connection_mode="reference",
        database_connection_name=MISSING_CONNECTION,
        table_name="some_table",
        if_exists="replace",
    )
    graph.add_database_writer(
        input_schema.NodeDatabaseWriter(
            database_write_settings=database_write_settings, node_id=1, flow_id=1, user_id=OWNER_USER_ID
        )
    )
    assert graph.get_node(1) is not None, "Node should be added without resolving the connection"


def test_add_kafka_source_with_missing_connection_does_not_raise():
    graph = create_graph()
    add_node_promise(graph, "kafka_source", 1)
    graph.add_kafka_source(make_kafka_source())
    assert graph.get_node(1) is not None, "Node should be added without resolving the connection"


def test_add_google_analytics_reader_with_missing_connection_does_not_raise():
    graph = create_graph()
    add_node_promise(graph, "google_analytics_reader", 1)
    ga_settings = input_schema.GoogleAnalyticsSettings(
        ga_connection_name=MISSING_CONNECTION,
        property_id="123456",
        metrics=["activeUsers"],
        dimensions=["country"],
    )
    graph.add_google_analytics_reader(
        input_schema.NodeGoogleAnalyticsReader(
            google_analytics_settings=ga_settings, node_id=1, flow_id=1, user_id=OWNER_USER_ID
        )
    )
    node = graph.get_node(1)
    assert node is not None, "Node should be added without resolving the connection"
    # The predicted schema is derived eagerly (pure-python, no connection needed).
    assert node.setting_input.fields, "derive_schema should stamp predicted fields at add time"
    assert [f.name for f in node.setting_input.fields] == ["country", "activeUsers"]


def test_add_rest_api_reader_with_missing_secret_does_not_raise():
    graph = create_graph()
    add_node_promise(graph, "rest_api_reader", 1)
    rest_api_settings = input_schema.RestApiSettings(
        url="https://example.com/api",
        auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret_name="definitely_not_a_real_secret_xyz"),
    )
    graph.add_rest_api_reader(
        input_schema.NodeRestApiReader(rest_api_settings=rest_api_settings, node_id=1, flow_id=1, user_id=OWNER_USER_ID)
    )
    assert graph.get_node(1) is not None, "Node should be added without resolving the secret"


def test_add_rest_api_reader_nulls_inline_secret_eagerly():
    """The inline plaintext must be encrypted and cleared at add time (never persisted)."""
    graph = create_graph()
    add_node_promise(graph, "rest_api_reader", 1)
    rest_api_settings = input_schema.RestApiSettings(
        url="https://example.com/api",
        auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret="super-secret-token"),
    )
    graph.add_rest_api_reader(
        input_schema.NodeRestApiReader(rest_api_settings=rest_api_settings, node_id=1, flow_id=1, user_id=OWNER_USER_ID)
    )
    assert graph.get_node(1).setting_input.rest_api_settings.auth.secret is None


# Failures surface at run time with a clear error


def test_run_database_reader_with_missing_connection_fails_with_clear_error():
    graph = create_graph()
    add_node_promise(graph, "database_reader", 1)
    graph.add_database_reader(make_database_reader())
    run_info = graph.run_graph()
    assert not run_info.success, "Run should fail when the connection cannot be resolved"
    node_result = next(r for r in run_info.node_step_result if r.node_id == 1)
    assert "not found" in (node_result.error or ""), f"Expected a clear error, got: {node_result.error!r}"


# Schema callbacks prefer cached fields


def test_database_reader_schema_uses_cached_fields_without_connection():
    graph = create_graph()
    add_node_promise(graph, "database_reader", 1)
    graph.add_database_reader(make_database_reader(fields=make_fields()))
    predicted = graph.get_node(1).get_predicted_schema()
    assert [c.name for c in predicted] == ["a", "b"], "Schema should come from cached fields, not the connection"


def test_kafka_source_schema_uses_cached_fields_without_connection():
    graph = create_graph()
    add_node_promise(graph, "kafka_source", 1)
    graph.add_kafka_source(make_kafka_source(fields=make_fields()))
    predicted = graph.get_node(1).get_predicted_schema()
    assert [c.name for c in predicted] == ["a", "b"], "Schema should come from cached fields, not the topic"


# Cached kafka fields are invalidated on settings change


def test_kafka_cached_fields_dropped_when_topic_changes():
    graph = create_graph()
    add_node_promise(graph, "kafka_source", 1)
    graph.add_kafka_source(make_kafka_source(fields=make_fields(), topic="topic-a"))
    assert graph.get_node(1).setting_input.fields, "Fields should be cached on the node"

    # A programmatic settings update may echo the old fields back with a new topic.
    graph.add_kafka_source(make_kafka_source(fields=make_fields(), topic="topic-b"))
    assert graph.get_node(1).setting_input.fields is None, "Stale fields must be dropped when the topic changes"


def test_kafka_cached_fields_kept_when_settings_unchanged():
    graph = create_graph()
    add_node_promise(graph, "kafka_source", 1)
    graph.add_kafka_source(make_kafka_source(fields=make_fields(), topic="topic-a"))
    graph.add_kafka_source(make_kafka_source(fields=make_fields(), topic="topic-a"))
    assert graph.get_node(1).setting_input.fields, "Fields should survive a no-op settings update"


# Undo re-stamps the owning user_id


def _delete_node_with_history(graph: FlowGraph, node_id: int) -> None:
    """Mimic the delete_node route: capture the pre-delete snapshot, then delete."""
    graph.capture_history_snapshot(HistoryActionType.DELETE_NODE, "Delete node", node_id=node_id)
    graph.delete_node(node_id)


def test_undo_restores_user_id_from_remaining_nodes():
    graph = create_graph(track_history=True)
    add_node_promise(graph, "manual_input", 1)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            user_id=OWNER_USER_ID,
            raw_data_format=input_schema.RawData.from_pylist([{"x": 1}]),
        )
    )
    add_node_promise(graph, "database_reader", 2)
    graph.add_database_reader(make_database_reader(node_id=2, fields=make_fields()))

    _delete_node_with_history(graph, 2)
    assert graph.get_node(2) is None

    result = graph.undo()
    assert result.success, result.error_message
    restored = graph.get_node(2)
    assert restored is not None, "Undo should restore the deleted node"
    assert restored.setting_input.user_id == OWNER_USER_ID, "Undo must re-stamp the owning user_id"


def test_undo_after_deleting_only_node_restores_owner_user_id():
    """Edge case: at undo time the live graph holds no nodes carrying a user_id,
    so the re-stamp must fall back to the session owner remembered by the graph."""
    graph = create_graph(track_history=True)
    add_node_promise(graph, "database_reader", 1)
    graph.add_database_reader(make_database_reader(fields=make_fields()))

    _delete_node_with_history(graph, 1)
    assert len(graph.nodes) == 0, "Graph should be empty before the undo"

    result = graph.undo()
    assert result.success, result.error_message
    restored = graph.get_node(1)
    assert restored is not None, "Undo should restore the deleted node"
    assert restored.setting_input.user_id == OWNER_USER_ID, (
        "Undo on an empty graph must re-stamp the session owner's user_id, not None"
    )
