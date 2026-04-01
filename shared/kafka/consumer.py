"""Shared Kafka consumer logic.

Pure function that consumes from a Kafka topic and returns a Polars DataFrame
plus new offsets. Uses Kafka consumer group offset management — the broker
tracks committed offsets internally via the ``__consumer_offsets`` topic.

Callable from any context (worker subprocess, core CLI mode, tests).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import polars as pl
from confluent_kafka import Consumer, KafkaError

from shared.kafka.deserializers import get_deserializer
from shared.kafka.models import KafkaReadResult, KafkaReadSettings

logger = logging.getLogger(__name__)

# How many messages to fetch per consume() call (batch size for the C layer)
_CONSUME_BATCH_SIZE = 500


def read_kafka_source(
    settings: KafkaReadSettings,
    *,
    commit: bool = True,
) -> tuple[pl.DataFrame, KafkaReadResult]:
    """Consume messages from a Kafka topic and return as a Polars DataFrame.

    Uses ``subscribe()`` with Kafka consumer groups. Offsets are committed
    broker-side after a successful batch (unless ``commit=False``).

    Args:
        settings: Kafka connection and read configuration.
        commit: Whether to commit offsets after consuming. Set to False
            for schema probes or dry runs.

    Returns:
        Tuple of (DataFrame with message data + metadata columns,
        KafkaReadResult with informational offsets).
    """
    deserializer = get_deserializer(settings.value_format)
    consumer_config = settings.to_consumer_config()
    consumer = Consumer(consumer_config)

    try:
        # Track assigned partitions via on_assign callback
        assigned_partitions: set[int] = set()

        def _on_assign(consumer, partitions):
            assigned_partitions.update(tp.partition for tp in partitions)

        consumer.subscribe([settings.topic], on_assign=_on_assign)

        # Consume messages
        rows: list[dict] = []
        high_watermarks: dict[int, int] = {}
        eof_partitions: set[int] = set()
        messages_consumed = 0
        empty_polls = 0
        first_poll = True
        poll_deadline = time.monotonic() + settings.poll_timeout_seconds

        while messages_consumed < settings.max_messages and time.monotonic() < poll_deadline:
            remaining = max(0.1, poll_deadline - time.monotonic())
            # First poll needs longer timeout for consumer group join/rebalance
            timeout = min(remaining, 10.0) if first_poll else min(remaining, 1.0)
            first_poll = False

            # Batch consume — fewer Python↔C boundary crossings
            batch_size = min(_CONSUME_BATCH_SIZE, settings.max_messages - messages_consumed)
            batch = consumer.consume(num_messages=batch_size, timeout=timeout)

            if not batch:
                empty_polls += 1
                # After consuming messages, 2 consecutive empty polls means we're done
                if messages_consumed > 0 and empty_polls >= 2:
                    break
                continue

            empty_polls = 0

            for msg in batch:
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        eof_partitions.add(msg.partition())
                        # If all assigned partitions hit EOF, we're done
                        if assigned_partitions and eof_partitions >= assigned_partitions:
                            break
                        continue
                    logger.error("Kafka consumer error: %s", msg.error())
                    continue

                # Deserialize value
                record = deserializer.deserialize(msg.value())
                if record is None:
                    high_watermarks[msg.partition()] = msg.offset() + 1
                    continue

                # Add Kafka metadata columns
                record["_kafka_key"] = msg.key().decode("utf-8", errors="replace") if msg.key() else None
                record["_kafka_partition"] = msg.partition()
                record["_kafka_offset"] = msg.offset()
                record["_kafka_timestamp"] = _extract_timestamp(msg)

                rows.append(record)
                high_watermarks[msg.partition()] = msg.offset() + 1
                messages_consumed += 1

                if messages_consumed >= settings.max_messages:
                    break
            else:
                # Inner loop completed without break — continue outer loop
                continue
            # Inner loop broke (EOF all partitions or max_messages) — break outer too
            break

        # Build DataFrame
        if rows:
            df = pl.DataFrame(rows)
            if "_kafka_key" in df.columns and df["_kafka_key"].dtype == pl.Null:
                df = df.with_columns(pl.col("_kafka_key").cast(pl.String))
        else:
            df = pl.DataFrame(
                schema={
                    "_kafka_key": pl.String,
                    "_kafka_partition": pl.Int64,
                    "_kafka_offset": pl.Int64,
                    "_kafka_timestamp": pl.Datetime("ms"),
                }
            )

        # Commit offsets broker-side after successful batch
        if commit and messages_consumed > 0:
            consumer.commit(asynchronous=False)

        result = KafkaReadResult(
            new_offsets=high_watermarks,
            messages_consumed=messages_consumed,
            partitions_read=len(high_watermarks),
        )

        logger.info(
            "Consumed %d messages from topic %r (group=%s)",
            messages_consumed,
            settings.topic,
            settings.group_id,
        )

        return df, result

    finally:
        consumer.close()


def infer_topic_schema(
    settings: KafkaReadSettings,
    sample_size: int = 10,
) -> list[tuple[str, pl.DataType]]:
    """Consume a small sample to infer the topic's schema.

    Uses a dedicated consumer group (``group_id + "__schema_probe"``) so it
    doesn't interfere with the actual sync's committed offsets.
    Does NOT commit offsets — the probe group is disposable.

    Args:
        settings: Base settings (connection, topic, auth). The ``group_id``
            will be suffixed with ``__schema_probe``.
        sample_size: Number of messages to sample.

    Returns:
        List of (column_name, polars_dtype) pairs, or empty list if
        the topic has no messages.
    """
    probe_settings = KafkaReadSettings(
        bootstrap_servers=settings.bootstrap_servers,
        topic=settings.topic,
        group_id=f"{settings.group_id}__schema_probe",
        value_format=settings.value_format,
        start_offset="earliest",
        max_messages=sample_size,
        poll_timeout_seconds=10.0,
        security_protocol=settings.security_protocol,
        sasl_mechanism=settings.sasl_mechanism,
        sasl_username=settings.sasl_username,
        sasl_password=settings.sasl_password,
        ssl_ca_location=settings.ssl_ca_location,
        ssl_cert_location=settings.ssl_cert_location,
        ssl_key_pem=settings.ssl_key_pem,
    )
    df, _ = read_kafka_source(probe_settings, commit=False)
    return list(df.schema.items())


def _extract_timestamp(msg) -> datetime | None:
    """Extract message timestamp as a datetime."""
    ts_type, ts_value = msg.timestamp()
    if ts_type == 0:  # TIMESTAMP_NOT_AVAILABLE
        return None
    return datetime.fromtimestamp(ts_value / 1000.0, tz=timezone.utc)
