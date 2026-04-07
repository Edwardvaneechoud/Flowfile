"""Shared Kafka consumer logic.

Pure function that consumes from a Kafka topic and returns a Polars DataFrame
plus new offsets. Uses Kafka consumer group offset management — the broker
tracks committed offsets internally via the ``__consumer_offsets`` topic.

Callable from any context (worker subprocess, core CLI mode, tests).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import NamedTuple

import polars as pl
import pyarrow as pa
import pyarrow.ipc
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

from shared.kafka.deserializers import get_deserializer
from shared.kafka.models import KafkaReadResult, KafkaReadSettings

logger = logging.getLogger(__name__)

_CONSUME_BATCH_SIZE = 500
_FLUSH_SIZE = 100_000

_EMPTY_SCHEMA: dict[str, pl.DataType] = {
    "_kafka_key": pl.String,
    "_kafka_partition": pl.Int64,
    "_kafka_offset": pl.Int64,
    "_kafka_timestamp": pl.Datetime("us"),
}


class _ConsumeResult(NamedTuple):
    """Result of the internal consume loop."""

    remaining_rows: list[dict]
    high_watermarks: dict[int, int]  # {partition: next_offset}
    messages_consumed: int


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def read_kafka_source(
    settings: KafkaReadSettings,
    *,
    commit: bool = True,
    decrypt_fn: Callable[[str], str] | None = None,
    spill_path: str | None = None,
) -> tuple[pl.DataFrame | pl.LazyFrame, KafkaReadResult]:
    """Consume messages from a Kafka topic and return a Polars result.

    Args:
        settings: Kafka connection and read configuration.
        commit: Commit offsets after consuming. ``False`` for probes / dry runs.
        decrypt_fn: Decrypts encrypted credential strings at point-of-use.
        spill_path: When set, rows are flushed to an IPC file in micro-batches
            of ``_FLUSH_SIZE`` rows and a ``pl.LazyFrame`` is returned.
            When ``None`` an in-memory ``pl.DataFrame`` is returned.

    Returns:
        ``(data, KafkaReadResult)``
    """
    consumer = Consumer(settings.to_consumer_config(decrypt_fn=decrypt_fn))
    try:
        writer = _IpcWriter(spill_path) if spill_path else None
        consumed = _consume_messages(consumer, settings, writer)
        result_data = _build_result(consumed.remaining_rows, writer)

        if commit and consumed.messages_consumed > 0:
            consumer.commit(asynchronous=False)

        result = KafkaReadResult(
            new_offsets=consumed.high_watermarks,
            messages_consumed=consumed.messages_consumed,
            partitions_read=len(consumed.high_watermarks),
        )
        logger.info(
            "Consumed %d messages from topic %r (group=%s)",
            consumed.messages_consumed, settings.topic, settings.group_id,
        )
        return result_data, result
    finally:
        consumer.close()


def infer_topic_schema(
    settings: KafkaReadSettings,
    sample_size: int = 10,
    decrypt_fn: Callable[[str], str] | None = None,
) -> list[tuple[str, pl.DataType]]:
    """Consume a small sample to infer the topic's schema.

    Uses a dedicated consumer group so it doesn't affect real offsets.
    """
    probe_settings = settings.model_copy(update={
        "group_id": f"{settings.group_id}__schema_probe",
        "start_offset": "earliest",
        "max_messages": sample_size,
        "poll_timeout_seconds": 10.0,
    })
    df_or_lf, _ = read_kafka_source(probe_settings, commit=False, decrypt_fn=decrypt_fn)
    schema = df_or_lf.collect_schema() if isinstance(df_or_lf, pl.LazyFrame) else df_or_lf.schema
    return list(schema.items())


# ---------------------------------------------------------------------------
# Internal: consume loop
# ---------------------------------------------------------------------------


def _consume_messages(
    consumer: Consumer,
    settings: KafkaReadSettings,
    writer: _IpcWriter | None = None,
) -> _ConsumeResult:
    """Run the consume loop.

    When *writer* is provided, rows are flushed to IPC every ``_FLUSH_SIZE``
    rows during consumption, keeping memory bounded.
    """
    deserializer = get_deserializer(settings.value_format)

    assigned: set[int] = set()
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda _c, parts: assigned.update(tp.partition for tp in parts),
    )

    rows: list[dict] = []
    high_watermarks: dict[int, int] = {}
    eof_partitions: set[int] = set()
    count = 0
    empty_polls = 0
    first_poll = True
    deadline = time.monotonic() + settings.poll_timeout_seconds

    while count < settings.max_messages and time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())
        timeout = min(remaining, 10.0) if first_poll else min(remaining, 1.0)
        first_poll = False

        batch = consumer.consume(
            num_messages=min(_CONSUME_BATCH_SIZE, settings.max_messages - count),
            timeout=timeout,
        )

        if not batch:
            empty_polls += 1
            if count > 0 and empty_polls >= 2:
                break
            continue
        empty_polls = 0

        for msg in batch:
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_partitions.add(msg.partition())
                    if assigned and eof_partitions >= assigned:
                        break
                    continue
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            record = deserializer.deserialize(msg.value())
            if record is None:
                high_watermarks[msg.partition()] = msg.offset() + 1
                continue

            record["_kafka_key"] = msg.key().decode("utf-8", errors="replace") if msg.key() else None
            record["_kafka_partition"] = msg.partition()
            record["_kafka_offset"] = msg.offset()
            record["_kafka_timestamp"] = _extract_timestamp(msg)

            rows.append(record)
            high_watermarks[msg.partition()] = msg.offset() + 1
            count += 1

            if writer and len(rows) >= _FLUSH_SIZE:
                writer.write_batch(_coerce_metadata_types(pl.DataFrame(rows)))
                rows.clear()

            if count >= settings.max_messages:
                break
        else:
            continue
        break

    return _ConsumeResult(
        remaining_rows=rows,
        high_watermarks=high_watermarks,
        messages_consumed=count,
    )


class _IpcWriter:
    """Manages streaming writes of row batches to an Arrow IPC file."""

    def __init__(self, path: str):
        self.path = path
        self._writer: pa.ipc.RecordBatchFileWriter | None = None
        self._sink: pa.OSFile | None = None

    def write_batch(self, df: pl.DataFrame) -> None:
        table = df.to_arrow()
        if self._writer is None:
            self._sink = pa.OSFile(self.path, "wb")
            self._writer = pa.ipc.new_file(self._sink, table.schema)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
        if self._sink is not None:
            self._sink.close()

    @property
    def has_written(self) -> bool:
        return self._writer is not None


# ---------------------------------------------------------------------------
# Internal: build result from rows
# ---------------------------------------------------------------------------


def _coerce_metadata_types(df: pl.DataFrame) -> pl.DataFrame:
    """Cast Kafka metadata columns to their canonical types."""
    if "_kafka_key" in df.columns and df["_kafka_key"].dtype == pl.Null:
        df = df.with_columns(pl.col("_kafka_key").cast(pl.String))
    if "_kafka_timestamp" in df.columns:
        df = df.with_columns(pl.col("_kafka_timestamp").cast(pl.Datetime("us")))
    return df


def _build_result(
    rows: list[dict],
    writer: _IpcWriter | None,
) -> pl.DataFrame | pl.LazyFrame:
    """Finalize the result from any remaining (unflushed) rows.

    Rows may already have been partially flushed to *writer* during
    consumption.  This handles the remainder and closes the writer.
    """
    if writer is not None:
        if rows:
            writer.write_batch(_coerce_metadata_types(pl.DataFrame(rows)))

        if writer.has_written:
            writer.close()
            return pl.scan_ipc(writer.path)

        # No rows at all — write empty schema
        pl.DataFrame(schema=_EMPTY_SCHEMA).write_ipc(writer.path)
        return pl.scan_ipc(writer.path)

    # In-memory path
    if rows:
        return _coerce_metadata_types(pl.DataFrame(rows))
    return pl.DataFrame(schema=_EMPTY_SCHEMA)


def _extract_timestamp(msg) -> datetime | None:
    """Extract message timestamp as a datetime."""
    ts_type, ts_value = msg.timestamp()
    if ts_type == 0:  # TIMESTAMP_NOT_AVAILABLE
        return None
    return datetime.fromtimestamp(ts_value / 1000.0, tz=timezone.utc)


def commit_offsets(
    settings: KafkaReadSettings,
    offsets: dict[int, int],
    *,
    decrypt_fn: Callable[[str], str] | None = None,
) -> None:
    """Commit specific offsets for a consumer group using the AdminClient.

    Uses ``alter_consumer_group_offsets`` — no subscribe() / rebalance needed.

    Args:
        settings: Kafka connection settings (used for broker address and group_id).
        offsets: Mapping of ``{partition: next_offset}`` to commit.
        decrypt_fn: Callable that decrypts encrypted secret strings.
    """
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

    config = settings.to_consumer_config(decrypt_fn=decrypt_fn)
    # AdminClient only needs a subset of keys; remove consumer-specific ones
    admin_config = {
        k: v
        for k, v in config.items()
        if not k.startswith(("auto.offset.reset", "enable.", "session.", "heartbeat.", "fetch."))
    }
    tps = [TopicPartition(settings.topic, int(p), int(o)) for p, o in offsets.items()]
    admin = AdminClient(admin_config)
    futures = admin.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(settings.group_id, tps)])
    for _, future in futures.items():
        future.result()  # raises on error
    logger.info(
        "Committed offsets for group %s topic %s: %s",
        settings.group_id,
        settings.topic,
        offsets,
    )


def make_kafka_commit_callback(
    settings: KafkaReadSettings,
    offsets: dict[int, int],
    node_id: int | str,
    flow_logger,
    decrypt_fn: Callable[[str], str] | None = None,
) -> Callable[[bool], None]:
    """Create a post-execution callback that commits Kafka offsets on success.

    Intended to be stored on ``FlowNode._on_flow_complete`` so that
    ``run_graph()`` can invoke it after all downstream nodes complete.

    Args:
        settings: Kafka connection settings.
        offsets: Mapping of ``{partition: next_offset}`` to commit.
        node_id: Node identifier for log messages.
        flow_logger: Logger instance for the flow.
        decrypt_fn: Callable that decrypts encrypted secret strings.
    """

    def _on_complete(success: bool) -> None:
        if not success:
            flow_logger.warning(
                f"Kafka offsets NOT committed for node {node_id} (downstream failure or cancel)"
            )
            return
        try:
            commit_offsets(settings, offsets, decrypt_fn=decrypt_fn)
            flow_logger.info(f"Committed Kafka offsets for node {node_id}: {offsets}")
        except Exception as e:
            flow_logger.error(f"Failed to commit Kafka offsets for node {node_id}: {e}")

    return _on_complete
