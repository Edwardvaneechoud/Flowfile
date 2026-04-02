"""Fixtures for Kafka integration tests.

All tests in this directory require a running Redpanda container.
Start it with:  poetry run start_redpanda
Or let the CI workflow handle it.
"""

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
    """Ensure Redpanda is running for the entire test session.

    If the container is already running (e.g., started via CLI), reuse it.
    Otherwise, start it and clean up after the session.
    """
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
    """Create a unique topic for each test.

    Usage:
        def test_something(kafka_topic):
            # kafka_topic is a string like "test_something_0"
            produce_json_messages(kafka_topic, [...])
    """
    topic_name = f"test_{request.node.name}".replace("[", "_").replace("]", "_").replace(" ", "_")[:249]
    create_topic(topic_name, num_partitions=2)
    return topic_name


@pytest.fixture()
def produce_messages():
    """Factory fixture for producing JSON messages to a topic."""
    return produce_json_messages
