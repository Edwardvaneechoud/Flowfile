"""Integration tests for the kafka_source node within a FlowGraph.

These tests verify that the kafka_source node can be added to a flow graph,
connected to downstream nodes, and executed against a real Redpanda broker.

All tests are marked with @pytest.mark.kafka and require:
- A running Redpanda container (started via `poetry run start_redpanda`)
- The flowfile_worker service (started automatically by the test session)

Run with:  poetry run pytest tests/kafka/test_kafka_flow_graph.py -m kafka -v
"""

import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowRegistration,
    FlowSchedule,
    KafkaConnection,
    KafkaSyncOffset,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.kafka.connection_manager import store_kafka_connection
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
        db.query(KafkaSyncOffset).delete()
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
        execution_mode="Development",
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


# ---------------------------------------------------------------------------
# Tests: Kafka source node in flow graph
# ---------------------------------------------------------------------------


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

        # Verify the table was created in the catalog
        with get_db_context() as db:
            from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository

            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table = tables[0]
            assert table.name == "kafka_output"
            assert table.row_count == 2


# ---------------------------------------------------------------------------
# Helpers (additional)
# ---------------------------------------------------------------------------


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
