"""Fixtures for shared Kafka tests.

Mirrors tests/kafka/conftest.py — ensures Redpanda is available.
"""

import uuid

import pytest

from test_utils.kafka.fixtures import (
    BOOTSTRAP_SERVERS,
    create_topic,
    is_container_running,
    produce_json_messages,
    REDPANDA_CONTAINER_NAME,
    start_redpanda_container,
    stop_redpanda_container,
)


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


@pytest.fixture()
def kafka_topic(request):
    """Create a unique topic for each test."""
    short_id = uuid.uuid4().hex[:8]
    safe_name = request.node.name.replace("[", "_").replace("]", "_").replace(" ", "_")
    topic_name = f"test_{safe_name}_{short_id}"[:249]
    create_topic(topic_name, num_partitions=1)
    return topic_name


@pytest.fixture()
def produce_messages():
    """Factory fixture for producing JSON messages to a topic."""
    return produce_json_messages
