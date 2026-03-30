"""CLI commands for managing the Redpanda test container.

Registered in pyproject.toml as:
    start_redpanda = "test_utils.kafka.commands:start_redpanda"
    stop_redpanda  = "test_utils.kafka.commands:stop_redpanda"
"""

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("kafka_commands")


def start_redpanda():
    """Start Redpanda container for Kafka testing."""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start Redpanda container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    if fixtures.start_redpanda_container():
        print(f"Redpanda started at {fixtures.BOOTSTRAP_SERVERS}")
        return 0
    return 1


def stop_redpanda():
    """Stop Redpanda container."""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot stop Redpanda container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("=" * 50 + "\n")
        return 0

    if fixtures.stop_redpanda_container():
        print("Redpanda stopped successfully")
        return 0
    return 1
