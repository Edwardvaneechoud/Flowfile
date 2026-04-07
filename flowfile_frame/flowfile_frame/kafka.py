"""Kafka helper functions for FlowFrame operations.

This module provides functions for reading from Kafka topics using
stored Flowfile connections.
"""

from __future__ import annotations

import tempfile

import polars as pl


def get_current_user_id() -> int:
    """Get the current user ID for Kafka operations.

    Returns:
        int: The current user ID (defaults to 1 for single-user mode).
    """
    return 1


def read_kafka(
    connection_name: str,
    *,
    topic_name: str,
    max_messages: int = 100_000,
    start_offset: str = "latest",
    poll_timeout_seconds: float = 30.0,
    value_format: str = "json",
) -> pl.LazyFrame:
    """Read messages from a Kafka topic using a named Flowfile connection.

    Resolves the Kafka connection by name, consumes messages from the
    specified topic, and returns the data as a LazyFrame.

    Args:
        connection_name: Name of the stored Kafka connection to use.
        topic_name: Kafka topic to consume from.
        max_messages: Maximum number of messages to consume.
        start_offset: Where to start consuming ('earliest' or 'latest').
        poll_timeout_seconds: How long to poll for messages.
        value_format: Message value format ('json').

    Returns:
        pl.LazyFrame: The consumed messages as a LazyFrame.

    Raises:
        ValueError: If the connection is not found.
    """
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.kafka.connection_manager import build_consumer_config, get_kafka_connection_by_name
    from flowfile_core.secret_manager.secret_manager import decrypt_secret
    from shared.kafka.consumer import read_kafka_source
    from shared.kafka.models import KafkaReadSettings

    user_id = get_current_user_id()

    with get_db_context() as db:
        db_conn = get_kafka_connection_by_name(db, connection_name, user_id)
        if db_conn is None:
            raise ValueError(f"Kafka connection '{connection_name}' not found")
        consumer_config = build_consumer_config(db, db_conn, user_id)

    kafka_read_settings = KafkaReadSettings.from_consumer_config(
        consumer_config,
        topic=topic_name,
        value_format=value_format,
        group_id=f"flowfile-frame-{connection_name}-{topic_name}",
        start_offset=start_offset,
        max_messages=max_messages,
        poll_timeout_seconds=poll_timeout_seconds,
    )

    def _decrypt_fn(encrypted: str) -> str:
        return decrypt_secret(encrypted).get_secret_value()

    # Consume messages with spill-to-IPC for memory efficiency
    fd, spill_file = tempfile.mkstemp(suffix=".arrow", prefix="kafka_")
    import os

    os.close(fd)

    result, _kafka_result = read_kafka_source(
        kafka_read_settings,
        commit=True,
        decrypt_fn=_decrypt_fn,
        spill_path=spill_file,
    )

    if isinstance(result, pl.LazyFrame):
        return result
    return result.lazy()
