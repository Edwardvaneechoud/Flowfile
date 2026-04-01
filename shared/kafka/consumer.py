"""Shared Kafka consumer logic.

Pure function that consumes from a Kafka topic and returns a Polars DataFrame
plus new offsets. No DB access, no side effects. Callable from any context
(worker subprocess, core CLI mode, tests).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import polars as pl
from confluent_kafka import Consumer, KafkaError, TopicPartition

from shared.kafka.deserializers import get_deserializer
from shared.kafka.models import KafkaReadResult, KafkaReadSettings

logger = logging.getLogger(__name__)


def read_kafka_source(
    settings: KafkaReadSettings,
) -> tuple[pl.DataFrame, KafkaReadResult]:
    """Consume messages from a Kafka topic and return as a Polars DataFrame.

    Args:
        settings: Kafka connection and read configuration.

    Returns:
        Tuple of (DataFrame with message data + metadata columns, KafkaReadResult with new offsets).
        If no messages are available, returns an empty DataFrame with the metadata columns.
    """
    deserializer = get_deserializer(settings.value_format)
    consumer_config = settings.to_consumer_config()
    consumer = Consumer(consumer_config)

    try:
        # Discover partitions for the topic
        metadata = consumer.list_topics(settings.topic, timeout=10.0)
        topic_metadata = metadata.topics.get(settings.topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise ValueError(f"Topic {settings.topic!r} not found or inaccessible")

        partition_ids = sorted(topic_metadata.partitions.keys())

        # Assign partitions at specified offsets or use auto.offset.reset
        partitions = []
        for pid in partition_ids:
            if settings.offsets and pid in settings.offsets:
                offset = settings.offsets[pid]
            else:
                # -1 = OFFSET_STORED, -2 = OFFSET_BEGINNING, -1001 = OFFSET_END
                # Use -2 for earliest, -1001 for latest (matching auto.offset.reset)
                offset = -2 if settings.start_offset == "earliest" else -1001
            partitions.append(TopicPartition(settings.topic, pid, offset))

        consumer.assign(partitions)

        # Consume messages
        rows: list[dict] = []
        high_watermarks: dict[int, int] = {}
        messages_consumed = 0
        poll_deadline = time.monotonic() + settings.poll_timeout_seconds

        while messages_consumed < settings.max_messages and time.monotonic() < poll_deadline:
            remaining = max(0.1, poll_deadline - time.monotonic())
            msg = consumer.poll(timeout=min(remaining, 1.0))

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition — track which partitions are done
                    continue
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            # Deserialize value
            record = deserializer.deserialize(msg.value())
            if record is None:
                # Failed deserialization — skip message but still track offset
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

        # Build DataFrame
        if rows:
            df = pl.DataFrame(rows)
            # Ensure _kafka_key is always String (not Null when all keys are None)
            if "_kafka_key" in df.columns and df["_kafka_key"].dtype == pl.Null:
                df = df.with_columns(pl.col("_kafka_key").cast(pl.String))
        else:
            # Empty DataFrame with expected metadata columns
            df = pl.DataFrame(
                schema={
                    "_kafka_key": pl.String,
                    "_kafka_partition": pl.Int64,
                    "_kafka_offset": pl.Int64,
                    "_kafka_timestamp": pl.Datetime("ms"),
                }
            )

        # Build offsets for ALL partitions (not just those that received messages).
        # For partitions without new messages, query the consumer's current position
        # so that subsequent runs don't re-read or skip messages.
        new_offsets = dict(settings.offsets) if settings.offsets else {}
        new_offsets.update(high_watermarks)
        for pid in partition_ids:
            if pid not in new_offsets:
                try:
                    tp = consumer.position([TopicPartition(settings.topic, pid)])
                    if tp and tp[0].offset >= 0:
                        new_offsets[pid] = tp[0].offset
                    else:
                        # Consumer never read from this partition (e.g. it was empty)
                        # Query the high watermark so we start from the right place next time
                        lo, hi = consumer.get_watermark_offsets(TopicPartition(settings.topic, pid), timeout=5.0)
                        new_offsets[pid] = hi
                except Exception:
                    logger.debug("Could not query position for partition %d", pid)
                    pass

        result = KafkaReadResult(
            new_offsets=new_offsets,
            messages_consumed=messages_consumed,
            partitions_read=len(partition_ids),
        )

        logger.info(
            "Consumed %d messages from topic %r (%d partitions)",
            messages_consumed,
            settings.topic,
            len(partition_ids),
        )

        return df, result

    finally:
        consumer.close()


def _extract_timestamp(msg) -> datetime | None:
    """Extract message timestamp as a datetime."""
    ts_type, ts_value = msg.timestamp()
    if ts_type == 0:  # TIMESTAMP_NOT_AVAILABLE
        return None
    # Kafka timestamps are in milliseconds since epoch
    return datetime.fromtimestamp(ts_value / 1000.0, tz=timezone.utc)
