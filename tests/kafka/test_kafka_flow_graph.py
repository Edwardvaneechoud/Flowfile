"""Integration tests for the kafka_source node within a FlowGraph.

These tests verify that the kafka_source node can be added to a flow graph,
connected to downstream nodes, and executed against a real Redpanda broker.

All tests are marked with @pytest.mark.kafka and require:
- A running Redpanda container (started via `poetry run start_redpanda`)
- The flowfile_worker service (started automatically by the test session)

Run with:  poetry run pytest tests/kafka/test_kafka_flow_graph.py -m kafka -v
"""
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowRegistration,
    FlowSchedule,
    KafkaConnection,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.kafka.connection_manager import reset_consumer_group, store_kafka_connection
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate
from test_utils.kafka.fixtures import (
    BOOTSTRAP_SERVERS,
    REDPANDA_CONTAINER_NAME,
    create_topic,
    is_container_running,
    produce_json_messages,
    start_redpanda_container,
    stop_redpanda_container,
)

pytestmark = pytest.mark.kafka


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def redpanda_broker():
    """Ensure Redpanda is running for the entire test session."""
    already_running = is_container_running(REDPANDA_CONTAINER_NAME)

    if not already_running:
        if not start_redpanda_container():
            pytest.skip("Could not start Redpanda container (Docker not available?)")
            return

    yield {"bootstrap_servers": BOOTSTRAP_SERVERS}

    if not already_running:
        stop_redpanda_container()


@pytest.fixture(autouse=True)
def clean_state():
    """Clean database state before and after each test."""
    _cleanup()
    yield
    _cleanup()


@pytest.fixture()
def kafka_connection_id() -> int:
    """Create a Kafka connection in the DB and return its ID."""
    with get_db_context() as db:
        conn = store_kafka_connection(
            db,
            KafkaConnectionCreate(
                connection_name="test-redpanda",
                bootstrap_servers=BOOTSTRAP_SERVERS,
                security_protocol="PLAINTEXT",
            ),
            user_id=1,
        )
    return conn.id


@pytest.fixture()
def kafka_topic(request):
    """Create a unique topic per test."""
    topic_name = f"flowgraph_{request.node.name}".replace("[", "_").replace("]", "_")[:249]
    create_topic(topic_name, num_partitions=2)
    return topic_name


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cleanup():
    """Remove test rows so each test starts clean."""
    with get_db_context() as db:
        db.query(CatalogTableReadLink).delete()
        db.query(FlowSchedule).delete()
        db.query(CatalogTable).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.query(KafkaConnection).delete()
        db.commit()


def _create_graph(flow_id: int = 1) -> FlowGraph:
    """Create a FlowGraph for testing."""
    handler = FlowfileHandler()
    settings = schemas.FlowSettings(
        flow_id=flow_id,
        name="kafka_test_flow",
        path=".",
        execution_mode="Performance",
        execution_location="remote",  # Uses worker for ExternalKafkaFetcher
    )
    handler.register_flow(settings)
    return handler.get_flow(flow_id)


def _run_graph(graph: FlowGraph) -> RunInformation:
    """Execute the graph and raise on failure."""
    run_info = graph.run_graph()
    if not run_info.success:
        errors = []
        for step in run_info.node_step_result:
            if not step.success:
                errors.append(f"node {step.node_id}: {step.error}")
        raise AssertionError("Graph execution failed:\n" + "\n".join(errors))
    return run_info


def _add_kafka_source(
    graph: FlowGraph,
    kafka_connection_id: int,
    topic_name: str,
    node_id: int = 1,
    sync_name: str = "test_sync",
    start_offset: str = "earliest",
):
    """Add a kafka_source node to the graph."""
    promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type="kafka_source",
    )
    graph.add_node_promise(promise)

    kafka_settings = input_schema.KafkaSourceSettings(
        kafka_connection_id=kafka_connection_id,
        topic_name=topic_name,
        value_format="json",
        sync_name=sync_name,
        start_offset=start_offset,
        poll_timeout_seconds=10.0,
    )
    node_kafka = input_schema.NodeKafkaSource(
        flow_id=graph.flow_id,
        node_id=node_id,
        kafka_settings=kafka_settings,
        user_id=1,
    )
    graph.add_kafka_source(node_kafka)


class TestKafkaSourceFlowGraph:
    """Tests for adding and executing kafka_source nodes in a FlowGraph."""

    def test_add_kafka_source_node(self, kafka_connection_id, kafka_topic):
        """A kafka_source node should be created and registered as a starting node."""
        graph = _create_graph()
        _add_kafka_source(graph, kafka_connection_id, kafka_topic)

        node = graph.get_node(1)
        assert node is not None
        assert node.node_type == "kafka_source"
        assert node.name == "kafka_source"

    def test_kafka_source_reads_messages(self, kafka_connection_id, kafka_topic):
        """A kafka_source node should consume messages and produce a DataFrame."""
        messages = [
            {"user": "alice", "event": "login"},
            {"user": "bob", "event": "purchase"},
            {"user": "charlie", "event": "logout"},
        ]
        produce_json_messages(kafka_topic, messages)

        graph = _create_graph()
        _add_kafka_source(graph, kafka_connection_id, kafka_topic)

        _run_graph(graph)

        node = graph.get_node(1)
        result = node.get_resulting_data()
        df = result.collect() if hasattr(result, "collect") else result.data_frame.collect()

        assert len(df) == 3
        assert "user" in df.columns
        assert "event" in df.columns
        assert "_kafka_partition" in df.columns
        assert "_kafka_offset" in df.columns

    def test_kafka_source_empty_topic(self, kafka_connection_id, kafka_topic):
        """A kafka_source on an empty topic should produce an empty result without failing."""
        graph = _create_graph()
        _add_kafka_source(graph, kafka_connection_id, kafka_topic)

        _run_graph(graph)

        node = graph.get_node(1)
        result = node.get_resulting_data()
        df = result.collect() if hasattr(result, "collect") else result.data_frame.collect()

        assert len(df) == 0


class TestKafkaSourceWithDownstream:
    """Tests for kafka_source connected to downstream nodes (select, filter, etc.)."""

    def test_kafka_source_to_select(self, kafka_connection_id, kafka_topic):
        """Data from kafka_source should flow through a select node."""
        messages = [
            {"user": "alice", "score": 100, "extra": "drop_me"},
            {"user": "bob", "score": 200, "extra": "drop_me"},
        ]
        produce_json_messages(kafka_topic, messages)

        graph = _create_graph()
        _add_kafka_source(graph, kafka_connection_id, kafka_topic, node_id=1)

        # Add a select node that keeps only user + score
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="select")
        graph.add_node_promise(promise)

        _run_graph_partial_to_node(graph, node_id=1)

        # Get schema from the kafka node to build select input
        node1 = graph.get_node(1)
        result1 = node1.get_resulting_data()
        df1 = result1.collect() if hasattr(result1, "collect") else result1.data_frame.collect()

        assert len(df1) == 2
        assert "user" in df1.columns
        assert "score" in df1.columns

    def test_kafka_source_to_catalog_writer(self, kafka_connection_id, kafka_topic):
        """Data from kafka_source should write to a Delta table via catalog_writer."""
        messages = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        produce_json_messages(kafka_topic, messages)

        # Create namespace for catalog writer
        ns_id = _create_namespace()

        graph = _create_graph()

        # Node 1: kafka source
        _add_kafka_source(graph, kafka_connection_id, kafka_topic, node_id=1)
        # Node 2: catalog writer
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        writer_settings = input_schema.CatalogWriteSettings(
            table_name="kafka_output",
            namespace_id=ns_id,
            write_mode="append"
        )

        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=writer_settings,
            user_id=1,
        )
        graph.add_catalog_writer(writer)

        connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
        add_connection(graph, connection)
        _run_graph(graph)
        with get_db_context() as db:
            from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table = tables[0]
            table_path = table.file_path

        initial_df = pl.read_delta(table_path)
        assert len(initial_df) == 2
        # check if a new run does not add any records:
        _run_graph(graph)
        update_df_after_no_message = pl.read_delta(table_path)

        assert_frame_equal(update_df_after_no_message, initial_df)
        more_messages = [
            {"name": "Jan", "age": 30},
            {"name": "Erin", "age": 25},
        ]
        produce_json_messages(kafka_topic, more_messages)
        _run_graph(graph)
        after_insert_df = pl.read_delta(table_path)
        assert len(after_insert_df) > len(update_df_after_no_message)


def _create_namespace() -> int:
    """Create a two-level namespace hierarchy and return the schema-level id."""
    with get_db_context() as db:
        cat = CatalogNamespace(name="KafkaTestCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="KafkaTestSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return schema.id


def _run_graph_partial_to_node(graph: FlowGraph, node_id: int):
    """Run the graph — used when we only care about a specific node's output."""
    run_info = graph.run_graph()
    if not run_info.success:
        # Check if the specific node we care about succeeded
        for step in run_info.node_step_result:
            if step.node_id == node_id and step.success:
                return run_info
        errors = [f"node {s.node_id}: {s.error}" for s in run_info.node_step_result if not s.success]
        raise AssertionError("Graph execution failed:\n" + "\n".join(errors))
    return run_info


def _create_graph_dev(flow_id: int = 1) -> FlowGraph:
    """Create a FlowGraph in Development mode for testing cache behavior."""
    handler = FlowfileHandler()
    settings = schemas.FlowSettings(
        flow_id=flow_id,
        name="kafka_test_flow_dev",
        path=".",
        execution_mode="Development",
        execution_location="remote",
    )
    handler.register_flow(settings)
    return handler.get_flow(flow_id)


def _get_node_df(graph: FlowGraph, node_id: int = 1) -> pl.DataFrame:
    """Get the collected DataFrame from a node's result."""
    node = graph.get_node(node_id)
    result = node.get_resulting_data()
    return result.collect()


# ---------------------------------------------------------------------------
# Tests: Empty result schema preservation
# ---------------------------------------------------------------------------


class TestKafkaSourceEmptySchema:
    """Tests that empty Kafka results preserve the expected data schema.

    When no new messages are available (all consumed), the result DataFrame
    should still have the correct columns from the topic schema — not just
    the Kafka metadata columns (_kafka_key, _kafka_partition, etc.).
    """

    def test_empty_result_after_consume_preserves_data_columns(self, kafka_connection_id, kafka_topic):
        """After consuming all messages, the empty result should retain data columns.

        Currently fails: empty result only has metadata columns.
        Expected: empty result includes 'user', 'score' (inferred from topic schema).
        """
        messages = [
            {"user": "alice", "score": 100},
            {"user": "bob", "score": 200},
        ]
        produce_json_messages(kafka_topic, messages)
        graph = _create_graph()
        _add_kafka_source(graph, kafka_connection_id, kafka_topic)

        # First run: consume all messages
        _run_graph(graph)
        df1 = _get_node_df(graph)
        assert len(df1) == 2
        assert "user" in df1.columns
        assert "score" in df1.columns
        # Second run: no new messages → empty result
        _run_graph(graph)
        df2 = _get_node_df(graph)
        assert len(df2) == 0
        # The empty DataFrame should have the data columns from the topic schema,
        # not just the Kafka metadata columns.
        assert "user" in df2.columns, (
            f"Expected 'user' column in empty result, got {df2.columns}. "
            "Empty results should preserve the topic's data schema."
        )
        assert "score" in df2.columns, (
            f"Expected 'score' column in empty result, got {df2.columns}."
        )

    def test_empty_result_with_output_field_config(self, kafka_connection_id, kafka_topic):
        """When output_field_config is set, empty results should have the configured columns.

        This tests that apply_output_field_config works correctly even when the
        upstream Kafka consumer returns an empty DataFrame with only metadata columns.
        """
        messages = [{"name": "alice", "age": 30}]
        produce_json_messages(kafka_topic, messages)

        graph = _create_graph()

        # Add kafka source with output_field_config
        promise = input_schema.NodePromise(
            flow_id=graph.flow_id, node_id=1, node_type="kafka_source"
        )
        graph.add_node_promise(promise)

        kafka_settings = input_schema.KafkaSourceSettings(
            kafka_connection_id=kafka_connection_id,
            topic_name=kafka_topic,
            value_format="json",
            sync_name="test_ofc_empty_sync",
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )
        output_config = input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="add_missing",
            fields=[
                input_schema.OutputFieldInfo(name="name", data_type="String"),
                input_schema.OutputFieldInfo(name="age", data_type="Int64"),
            ],
        )
        node_kafka = input_schema.NodeKafkaSource(
            flow_id=graph.flow_id,
            node_id=1,
            kafka_settings=kafka_settings,
            output_field_config=output_config,
            user_id=1,
        )
        graph.add_kafka_source(node_kafka)

        # First run: consume the message
        _run_graph(graph)
        df1 = _get_node_df(graph)
        assert len(df1) == 1
        assert "name" in df1.columns
        assert "age" in df1.columns

        # Second run: no new messages
        _run_graph(graph)
        df2 = _get_node_df(graph)
        assert len(df2) == 0
        assert "name" in df2.columns, (
            f"Expected 'name' column in empty result with output_field_config, got {df2.columns}."
        )
        assert "age" in df2.columns, (
            f"Expected 'age' column in empty result with output_field_config, got {df2.columns}."
        )

    def test_truly_empty_topic_uses_schema_callback(self, kafka_connection_id, kafka_topic):
        """A topic with NO messages should use the schema callback for column names.

        When the topic has never had any messages, even the schema probe returns
        nothing. In that case, the result should still have the Kafka metadata
        columns at minimum.
        """
        graph = _create_graph()
        _add_kafka_source(graph, kafka_connection_id, kafka_topic)

        _run_graph(graph)
        df = _get_node_df(graph)

        assert len(df) == 0
        # At minimum, metadata columns should always be present
        assert "_kafka_key" in df.columns
        assert "_kafka_partition" in df.columns
        assert "_kafka_offset" in df.columns
        assert "_kafka_timestamp" in df.columns


# ---------------------------------------------------------------------------
# Tests: Cache invalidation after offset reset
# ---------------------------------------------------------------------------


class TestKafkaSourceCacheInvalidation:
    """Tests for cache behavior after consumer group offset reset.

    When a user resets Kafka consumer offsets, subsequent flow runs should
    re-read messages from the topic — not return stale cached results.
    """

    def test_reset_nonexistent_consumer_group_succeeds(self, kafka_connection_id, kafka_topic):
        """Resetting offsets for a group that doesn't exist should not raise an error.

        This happens when the user clicks 'Reset Offsets' before ever running the flow,
        or when using an auto-generated sync_name that was never committed.
        """
        with get_db_context() as db:
            # Should not raise — GROUP_ID_NOT_FOUND is treated as success
            reset_consumer_group(db, "nonexistent-group-id", kafka_connection_id, user_id=1, topic=kafka_topic)

    def test_development_mode_rerun_after_offset_reset(self, kafka_connection_id, kafka_topic):
        """In Development mode, resetting offsets should cause the node to re-execute.

        Currently fails: Development mode caches the first run's result in memory.
        After offset reset, the node hash hasn't changed, so the node is skipped
        and stale data is returned.
        """
        messages = [
            {"user": "alice", "event": "login"},
            {"user": "bob", "event": "purchase"},
        ]
        produce_json_messages(kafka_topic, messages)

        graph = _create_graph_dev(flow_id=2)
        sync_name = "test_dev_reset_sync"
        _add_kafka_source(graph, kafka_connection_id, kafka_topic, sync_name=sync_name)

        # First run: consume all messages
        _run_graph(graph)
        df1 = _get_node_df(graph)
        assert len(df1) == 2

        # Produce additional messages so we can distinguish re-read from stale cache
        produce_json_messages(kafka_topic, [{"user": "charlie", "event": "logout"}])

        # Reset consumer group offsets
        with get_db_context() as db:
            reset_consumer_group(db, sync_name, kafka_connection_id, user_id=1, topic=kafka_topic)

        # Invalidate node cache after external state change
        graph.get_node(1).invalidate_cache()

        # Run again: should re-read ALL messages (3 total, from beginning)
        _run_graph(graph)
        df2 = _get_node_df(graph)
        assert len(df2) == 3, (
            f"Expected 3 messages after offset reset, got {len(df2)}. "
            f"The node likely returned stale cached data instead of re-reading."
        )

    def test_performance_mode_rerun_after_offset_reset(self, kafka_connection_id, kafka_topic):
        """In Performance mode, resetting offsets should cause messages to be re-read.

        Performance mode always re-executes nodes, so this should work correctly.
        """
        messages = [
            {"user": "alice", "event": "login"},
            {"user": "bob", "event": "purchase"},
        ]
        produce_json_messages(kafka_topic, messages)

        graph = _create_graph()  # Performance mode
        sync_name = "test_perf_reset_sync"
        _add_kafka_source(graph, kafka_connection_id, kafka_topic, sync_name=sync_name)

        # First run: consume all messages
        _run_graph(graph)
        df1 = _get_node_df(graph)
        assert len(df1) == 2

        # Second run: no new messages
        _run_graph(graph)
        df2 = _get_node_df(graph)
        assert len(df2) == 0

        # Reset consumer group offsets
        with get_db_context() as db:
            reset_consumer_group(db, sync_name, kafka_connection_id, user_id=1, topic=kafka_topic)

        # Third run: should re-read all messages from beginning
        _run_graph(graph)
        df3 = _get_node_df(graph)
        assert len(df3) == 2, (
            f"Expected 2 messages after offset reset in Performance mode, got {len(df3)}."
        )

    def test_downstream_receives_fresh_data_after_offset_reset(self, kafka_connection_id, kafka_topic):
        """After offset reset, downstream nodes should process the re-read data.

        This tests the full pipeline: kafka_source → catalog_writer.
        After reset, the re-read messages should flow through to the downstream node.
        """
        messages = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        produce_json_messages(kafka_topic, messages)

        ns_id = _create_namespace()
        graph = _create_graph()
        sync_name = "test_downstream_reset_sync"

        # Node 1: kafka source
        _add_kafka_source(graph, kafka_connection_id, kafka_topic, node_id=1, sync_name=sync_name)

        # Node 2: catalog writer
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        writer_settings = input_schema.CatalogWriteSettings(
            table_name="kafka_reset_output",
            namespace_id=ns_id,
            write_mode="overwrite",
        )
        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=writer_settings,
            user_id=1,
        )
        graph.add_catalog_writer(writer)
        connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
        add_connection(graph, connection)

        # First run: consume and write
        _run_graph(graph)
        with get_db_context() as db:
            from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table_path = tables[0].file_path

        initial_df = pl.read_delta(table_path)
        assert len(initial_df) == 2

        # Second run: no new messages → overwrite with empty
        _run_graph(graph)

        # Reset offsets
        with get_db_context() as db:
            reset_consumer_group(db, sync_name, kafka_connection_id, user_id=1, topic=kafka_topic)

        # Third run: should re-read and write all messages again
        _run_graph(graph)
        after_reset_df = pl.read_delta(table_path)
        assert len(after_reset_df) == 2, (
            f"Expected 2 rows after offset reset + re-read, got {len(after_reset_df)}. "
            "Downstream node did not receive fresh data after offset reset."
        )
