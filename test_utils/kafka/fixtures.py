"""Redpanda container management for Kafka integration tests.

Follows the same pattern as test_utils.s3.fixtures (Docker CLI wrappers).
Redpanda is used instead of Apache Kafka because it's a single binary,
starts in seconds, and is fully Kafka-protocol-compatible.
"""

import json
import logging
import os
import shutil
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager

logger = logging.getLogger("kafka_fixture")

REDPANDA_HOST = os.environ.get("TEST_REDPANDA_HOST", "localhost")
REDPANDA_PORT = int(os.environ.get("TEST_REDPANDA_PORT", 19092))
REDPANDA_CONTAINER_NAME = os.environ.get("TEST_REDPANDA_CONTAINER", "test-redpanda-kafka")
REDPANDA_IMAGE = os.environ.get("TEST_REDPANDA_IMAGE", "docker.redpanda.com/redpandadata/redpanda:latest")

BOOTSTRAP_SERVERS = f"{REDPANDA_HOST}:{REDPANDA_PORT}"

# Operating system detection
IS_MACOS = os.uname().sysname == "Darwin" if hasattr(os, "uname") else False
IS_WINDOWS = os.name == "nt"


def is_docker_available() -> bool:
    """Check if Docker is available on the system."""
    if (IS_MACOS or IS_WINDOWS) and os.environ.get("CI", "").lower() in ("true", "1", "yes"):
        logger.info("Skipping Docker on macOS/Windows in CI environment")
        return False

    if shutil.which("docker") is None:
        logger.warning("Docker executable not found in PATH")
        return False

    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
            check=False,
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, OSError):
        logger.warning("Error running Docker command")
        return False


def is_container_running(container_name: str) -> bool:
    """Check if a container is already running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return container_name in result.stdout.strip()
    except subprocess.CalledProcessError:
        return False


def wait_for_redpanda(max_retries: int = 30, interval: float = 1.0) -> bool:
    """Wait for Redpanda to be ready by attempting to list topics."""
    from confluent_kafka.admin import AdminClient

    admin_config = {"bootstrap.servers": BOOTSTRAP_SERVERS}

    for i in range(max_retries):
        try:
            admin = AdminClient(admin_config)
            metadata = admin.list_topics(timeout=5.0)
            logger.info("Redpanda is ready (%d internal topics found)", len(metadata.topics))
            return True
        except Exception:
            if i < max_retries - 1:
                time.sleep(interval)
            continue
    logger.error("Redpanda did not become ready within %d retries", max_retries)
    return False


def start_redpanda_container() -> bool:
    """Start a Redpanda container for Kafka integration testing."""
    if is_container_running(REDPANDA_CONTAINER_NAME):
        logger.info("Container %s is already running", REDPANDA_CONTAINER_NAME)
        return True

    # Remove any stopped container with the same name
    subprocess.run(
        ["docker", "rm", "-f", REDPANDA_CONTAINER_NAME],
        capture_output=True,
        check=False,
    )

    try:
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                REDPANDA_CONTAINER_NAME,
                "-p",
                f"{REDPANDA_PORT}:9092",
                REDPANDA_IMAGE,
                "redpanda",
                "start",
                "--smp",
                "1",
                "--memory",
                "256M",
                "--overprovisioned",
                "--kafka-addr",
                "0.0.0.0:9092",
                "--advertise-kafka-addr",
                f"{REDPANDA_HOST}:{REDPANDA_PORT}",
                "--mode",
                "dev-container",
            ],
            check=True,
            capture_output=True,
        )
        logger.info("Redpanda container started, waiting for readiness...")

        if wait_for_redpanda():
            logger.info("Redpanda is ready at %s", BOOTSTRAP_SERVERS)
            return True

        # Log container status for debugging
        logs_result = subprocess.run(
            ["docker", "logs", "--tail", "30", REDPANDA_CONTAINER_NAME],
            capture_output=True,
            text=True,
            check=False,
        )
        if logs_result.stdout:
            logger.error("Redpanda container logs:\n%s", logs_result.stdout)
        if logs_result.stderr:
            logger.error("Redpanda container stderr:\n%s", logs_result.stderr)
        return False

    except Exception as e:
        logger.error("Failed to start Redpanda: %s", e)
        stop_redpanda_container()
        return False


def stop_redpanda_container() -> bool:
    """Stop and remove the Redpanda container."""
    if not is_container_running(REDPANDA_CONTAINER_NAME):
        logger.info("Container '%s' is not running.", REDPANDA_CONTAINER_NAME)
        return True

    logger.info("Stopping container '%s'...", REDPANDA_CONTAINER_NAME)
    try:
        subprocess.run(["docker", "stop", REDPANDA_CONTAINER_NAME], check=True, capture_output=True)
        subprocess.run(["docker", "rm", REDPANDA_CONTAINER_NAME], check=True, capture_output=True)
        logger.info("Redpanda container stopped and removed.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error("Failed to clean up Redpanda container: %s", e.stderr.decode() if e.stderr else str(e))
        return False


@contextmanager
def managed_redpanda() -> Generator[dict[str, str | int], None, None]:
    """Context manager for Redpanda container with connection info."""
    if not start_redpanda_container():
        yield {}
        return

    try:
        yield {
            "bootstrap_servers": BOOTSTRAP_SERVERS,
            "host": REDPANDA_HOST,
            "port": REDPANDA_PORT,
        }
    finally:
        if os.environ.get("KEEP_REDPANDA_RUNNING", "false").lower() != "true":
            stop_redpanda_container()


# ---------------------------------------------------------------------------
# Topic helpers
# ---------------------------------------------------------------------------


def create_topic(topic_name: str, num_partitions: int = 1) -> None:
    """Create a Kafka topic via the admin API."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    futures = admin.create_topics([NewTopic(topic_name, num_partitions=num_partitions, replication_factor=1)])
    for topic, future in futures.items():
        future.result()  # raises on failure
        logger.info("Created topic: %s (%d partitions)", topic, num_partitions)


def produce_json_messages(topic_name: str, messages: list[dict], key_field: str | None = None) -> int:
    """Produce JSON messages to a Kafka topic.

    Args:
        topic_name: Target topic.
        messages: List of dicts to serialize as JSON.
        key_field: Optional field name to use as the message key.

    Returns:
        Number of messages produced.
    """
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    count = 0
    for msg in messages:
        key = str(msg[key_field]).encode() if key_field and key_field in msg else None
        producer.produce(topic_name, value=json.dumps(msg).encode(), key=key)
        count += 1
    producer.flush(timeout=10.0)
    logger.info("Produced %d messages to topic %s", count, topic_name)
    return count
