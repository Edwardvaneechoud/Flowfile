"""Kafka integration tests using a real Redpanda broker.

All tests are marked with @pytest.mark.kafka and require Docker.
Run with:  poetry run pytest tests/kafka -m kafka -v
"""

import json
import os
import tempfile
import uuid
from unittest.mock import patch

import polars as pl
import pytest

from shared.kafka.consumer import commit_offsets, infer_topic_schema, read_kafka_source
from shared.kafka.models import KafkaReadSettings
from test_utils.kafka.fixtures import BOOTSTRAP_SERVERS, create_topic, produce_json_messages

pytestmark = pytest.mark.kafka


def _unique_group() -> str:
    """Generate a unique consumer group ID per test invocation."""
    return f"test-{uuid.uuid4().hex[:12]}"


def _produce_n_messages(topic: str, n: int) -> None:
    """Bulk-produce n JSON messages to a topic (optimised for throughput)."""
    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": "50",
        "batch.num.messages": "10000",
        "queue.buffering.max.messages": "1000000",
    })
    for i in range(n):
        producer.produce(
            topic,
            key=f"k{i}".encode(),
            value=json.dumps({"seq": i, "val": f"row_{i}"}).encode(),
        )
        if i % 10_000 == 0:
            producer.poll(0)
    remaining = producer.flush(timeout=60.0)
    assert remaining == 0, f"Producer flush incomplete: {remaining} messages still in queue"


# ---------------------------------------------------------------------------
# Consumer integration tests
# ---------------------------------------------------------------------------


class TestKafkaConsumerIntegration:
    """Tests that consume from a real Redpanda broker."""

    def test_consume_json_messages(self, kafka_topic, produce_messages):
        """Produce JSON messages, then consume and verify the DataFrame."""
        messages = [
            {"user": "alice", "event": "login", "value": 1},
            {"user": "bob", "event": "purchase", "value": 42},
            {"user": "charlie", "event": "logout", "value": 0},
        ]
        produce_messages(kafka_topic, messages)

        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 3
        assert set(df["user"].to_list()) == {"alice", "bob", "charlie"}
        assert "_kafka_key" in df.columns
        assert "_kafka_partition" in df.columns
        assert "_kafka_offset" in df.columns
        assert "_kafka_timestamp" in df.columns
        assert result.messages_consumed == 3

    def test_consume_empty_topic(self, kafka_topic):
        """Consuming from an empty topic returns an empty DataFrame."""
        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
            poll_timeout_seconds=3.0,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 0
        assert result.messages_consumed == 0

    def test_consume_with_message_keys(self, kafka_topic, produce_messages):
        """Messages produced with keys should have _kafka_key populated."""
        messages = [
            {"id": "k1", "data": "hello"},
            {"id": "k2", "data": "world"},
        ]
        produce_messages(kafka_topic, messages, key_field="id")

        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 2
        keys = df["_kafka_key"].to_list()
        assert "k1" in keys
        assert "k2" in keys

    def test_consume_respects_max_messages(self, kafka_topic, produce_messages):
        """max_messages should cap the number of consumed messages."""
        messages = [{"i": i} for i in range(20)]
        produce_messages(kafka_topic, messages)

        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
            max_messages=5,
            poll_timeout_seconds=10.0,
        )

        df, result = read_kafka_source(settings)

        assert result.messages_consumed == 5
        assert df.height == 5


# ---------------------------------------------------------------------------
# Offset tracking via consumer groups
# ---------------------------------------------------------------------------


class TestKafkaOffsetTracking:
    """Tests verifying Kafka consumer group offset tracking across calls."""

    def test_incremental_consume(self, kafka_topic, produce_messages):
        """Two sequential consumes with the same group_id should not re-read messages."""
        group = _unique_group()

        # Produce first batch
        batch1 = [{"batch": 1, "i": i} for i in range(5)]
        produce_messages(kafka_topic, batch1)

        # First consume — read everything
        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=group,
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )
        df1, result1 = read_kafka_source(settings)
        assert result1.messages_consumed == 5

        # Produce second batch
        batch2 = [{"batch": 2, "i": i} for i in range(3)]
        produce_messages(kafka_topic, batch2)

        # Second consume — same group_id, should auto-resume from committed offsets
        settings2 = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=group,
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )
        df2, result2 = read_kafka_source(settings2)

        assert result2.messages_consumed == 3
        assert df2.height == 3
        assert all(v == 2 for v in df2["batch"].to_list())

    def test_offsets_survive_reconnect(self, kafka_topic, produce_messages):
        """Offsets committed by first consume should persist for the second."""
        group = _unique_group()

        messages = [{"seq": i} for i in range(10)]
        produce_messages(kafka_topic, messages)

        # First consume — only take 5
        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=group,
            start_offset="earliest",
            max_messages=5,
            poll_timeout_seconds=10.0,
        )
        df1, result1 = read_kafka_source(settings)
        assert result1.messages_consumed == 5

        # Second consume — same group, should get remaining 5
        settings2 = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=group,
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )
        df2, result2 = read_kafka_source(settings2)

        assert result2.messages_consumed == 5
        assert df2.height == 5

        # Combined should cover all 10 seq values
        all_seqs = sorted(df1["seq"].to_list() + df2["seq"].to_list())
        assert all_seqs == list(range(10))


# ---------------------------------------------------------------------------
# Multi-partition tests
# ---------------------------------------------------------------------------


class TestKafkaMultiPartition:
    """Tests verifying behavior across multiple partitions."""

    def test_consume_from_multi_partition_topic(self, produce_messages):
        """Messages spread across partitions should all be consumed."""
        from test_utils.kafka.fixtures import create_topic

        topic = f"test_multi_{uuid.uuid4().hex[:8]}"
        create_topic(topic, num_partitions=4)

        messages = [{"partition_test": i} for i in range(50)]
        produce_messages(topic, messages)

        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=topic,
            group_id=_unique_group(),
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )

        df, result = read_kafka_source(settings)

        assert result.messages_consumed == 50
        assert df.height == 50


# ---------------------------------------------------------------------------
# Schema inference tests
# ---------------------------------------------------------------------------


class TestSchemaInference:
    """Tests for sample-based schema inference."""

    def test_infer_schema_from_json_messages(self, kafka_topic, produce_messages):
        """Schema inference should detect column names and types from samples."""
        messages = [
            {"name": "Alice", "age": 30, "score": 95.5},
            {"name": "Bob", "age": 25, "score": 88.0},
        ]
        produce_messages(kafka_topic, messages)

        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
        )

        schema = infer_topic_schema(settings, sample_size=5)

        column_names = [name for name, _ in schema]
        assert "name" in column_names
        assert "age" in column_names
        assert "score" in column_names
        assert "_kafka_key" in column_names
        assert "_kafka_partition" in column_names

    def test_infer_schema_empty_topic(self, kafka_topic):
        """Schema inference on empty topic returns metadata columns only."""
        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
        )

        schema = infer_topic_schema(settings, sample_size=5)

        column_names = [name for name, _ in schema]
        assert "_kafka_key" in column_names
        assert "_kafka_partition" in column_names

    def test_infer_schema_does_not_commit(self, kafka_topic, produce_messages):
        """Schema inference should not commit offsets (uses separate probe group)."""
        messages = [{"x": i} for i in range(5)]
        produce_messages(kafka_topic, messages)

        group = _unique_group()
        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=group,
            start_offset="earliest",
        )

        # Probe schema — should NOT commit to the main group
        infer_topic_schema(settings, sample_size=3)

        # Now consume with the actual group — should get all 5 messages
        df, result = read_kafka_source(settings)
        assert result.messages_consumed == 5


class TestKafkaErrorHandling:
    """Tests for error conditions with a real broker."""

    def test_mixed_valid_invalid_json(self, kafka_topic):
        """Invalid JSON messages should be skipped, valid ones consumed."""
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
        producer.produce(kafka_topic, value=b'{"valid": true, "seq": 1}')
        producer.produce(kafka_topic, value=b"not valid json at all")
        producer.produce(kafka_topic, value=b'{"valid": true, "seq": 2}')
        producer.flush(timeout=10.0)

        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 2
        assert sorted(df["seq"].to_list()) == [1, 2]


class TestSpillToIpc:
    """Tests for the spill_path parameter that streams rows to an IPC file."""

    def test_spill_path_returns_lazyframe(self, kafka_topic, produce_messages):
        """When spill_path is set, result should be a LazyFrame."""
        produce_messages(kafka_topic, [{"name": "Alice"}, {"name": "Bob"}])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=kafka_topic,
                group_id=_unique_group(),
                start_offset="earliest",
                poll_timeout_seconds=10.0,
            )
            result_data, result = read_kafka_source(settings, commit=False, spill_path=spill_path)

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 2

            df = result_data.collect()
            assert df.height == 2
            assert "name" in df.columns
            assert "_kafka_key" in df.columns
        finally:
            os.unlink(spill_path)

    def test_spill_path_empty_topic(self, kafka_topic):
        """Spill path with empty topic returns LazyFrame with correct schema."""
        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=kafka_topic,
                group_id=_unique_group(),
                start_offset="earliest",
                poll_timeout_seconds=3.0,
            )
            result_data, result = read_kafka_source(settings, commit=False, spill_path=spill_path)

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 0
            assert result_data.collect().height == 0
        finally:
            os.unlink(spill_path)

    def test_without_spill_path_returns_dataframe(self, kafka_topic, produce_messages):
        """Without spill_path, result should be a DataFrame (backward compat)."""
        produce_messages(kafka_topic, [{"a": 1}])

        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=_unique_group(),
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )
        result_data, _ = read_kafka_source(settings, commit=False)

        assert isinstance(result_data, pl.DataFrame)


# ---------------------------------------------------------------------------
# Flush threshold tests (patched _FLUSH_SIZE=3)
# ---------------------------------------------------------------------------


class TestSpillFlushMechanics:
    """Tests IPC flush mechanics with _FLUSH_SIZE patched to 3.

    Uses real messages through a real broker — only the threshold is patched
    so we can trigger multi-flush with just a handful of messages.
    """

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_flush_at_exactly_flush_size(self, kafka_topic, produce_messages):
        """Exactly _FLUSH_SIZE messages: one flush, no remainder."""
        produce_messages(kafka_topic, [{"n": i} for i in range(3)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=kafka_topic,
                group_id=_unique_group(),
                start_offset="earliest",
                poll_timeout_seconds=10.0,
            )
            result_data, result = read_kafka_source(settings, commit=False, spill_path=spill_path)

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 3
            assert result_data.collect().height == 3
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_flush_with_remainder(self, kafka_topic, produce_messages):
        """5 messages with _FLUSH_SIZE=3: one flush (3) + remainder (2)."""
        produce_messages(kafka_topic, [{"seq": i, "val": f"row_{i}"} for i in range(5)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=kafka_topic,
                group_id=_unique_group(),
                start_offset="earliest",
                poll_timeout_seconds=10.0,
            )
            result_data, result = read_kafka_source(settings, commit=False, spill_path=spill_path)

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 5

            df = result_data.collect()
            assert df.height == 5
            assert df.select(pl.struct("_kafka_partition", "_kafka_offset")).n_unique() == 5
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_two_flushes_plus_remainder(self, kafka_topic, produce_messages):
        """8 messages with _FLUSH_SIZE=3: two flushes (3+3) + remainder (2)."""
        produce_messages(kafka_topic, [{"seq": i} for i in range(8)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=kafka_topic,
                group_id=_unique_group(),
                start_offset="earliest",
                poll_timeout_seconds=10.0,
            )
            result_data, result = read_kafka_source(settings, commit=False, spill_path=spill_path)

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 8
            assert result_data.collect().height == 8
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_ipc_file_has_multiple_record_batches(self, kafka_topic, produce_messages):
        """IPC file should contain multiple Arrow record batches after flush."""
        import pyarrow.ipc

        produce_messages(kafka_topic, [{"n": i} for i in range(4)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=kafka_topic,
                group_id=_unique_group(),
                start_offset="earliest",
                poll_timeout_seconds=10.0,
            )
            read_kafka_source(settings, commit=False, spill_path=spill_path)

            with pyarrow.ipc.open_file(spill_path) as reader:
                assert reader.num_record_batches == 2  # flush (3) + remainder (1)
                total = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
                assert total == 4
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_below_threshold_single_batch(self, kafka_topic, produce_messages):
        """Below _FLUSH_SIZE: no streaming flush, single-batch IPC file."""
        import pyarrow.ipc

        produce_messages(kafka_topic, [{"n": i} for i in range(2)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=kafka_topic,
                group_id=_unique_group(),
                start_offset="earliest",
                poll_timeout_seconds=10.0,
            )
            result_data, _ = read_kafka_source(settings, commit=False, spill_path=spill_path)

            assert isinstance(result_data, pl.LazyFrame)
            assert result_data.collect().height == 2

            with pyarrow.ipc.open_file(spill_path) as reader:
                assert reader.num_record_batches == 1
        finally:
            os.unlink(spill_path)


class TestLargeVolumeFlush:
    """Tests that produce 100K+ real messages to verify flush at production scale.

    These use the real _FLUSH_SIZE (100_000). They take a few seconds each
    due to producing/consuming large volumes through a real broker.
    """

    def _run(self, n: int, expected_min_batches: int):
        import pyarrow.ipc

        topic = f"large_vol_{n}_{uuid.uuid4().hex[:8]}"
        create_topic(topic, num_partitions=1)
        _produce_n_messages(topic, n)

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            settings = KafkaReadSettings(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=topic,
                group_id=_unique_group(),
                start_offset="earliest",
                max_messages=n,
                poll_timeout_seconds=120.0,
            )
            result_data, result = read_kafka_source(settings, commit=False, spill_path=spill_path)

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == n

            with pyarrow.ipc.open_file(spill_path) as reader:
                assert reader.num_record_batches >= expected_min_batches
                total = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
                assert total == n

            df = result_data.collect()
            assert df.height == n
            assert df["_kafka_offset"].n_unique() == n

            # Spot-check middle row
            mid = n // 2
            mid_rows = df.filter(pl.col("seq") == mid)
            assert mid_rows.height == 1
            assert mid_rows["val"][0] == f"row_{mid}"
        finally:
            os.unlink(spill_path)

    def test_50k_below_threshold(self):
        """50K messages: below _FLUSH_SIZE, single batch."""
        self._run(50_000, expected_min_batches=1)

    def test_100k_one_flush(self):
        """100K messages: exactly one flush."""
        self._run(100_000, expected_min_batches=1)

    def test_150k_flush_plus_remainder(self):
        """150K messages: one flush (100K) + remainder (50K)."""
        self._run(150_000, expected_min_batches=2)

    def test_250k_two_flushes(self):
        """250K messages: two flushes + remainder."""
        self._run(250_000, expected_min_batches=3)

    def test_500k_five_flushes(self):
        """500K messages: five flushes."""
        self._run(500_000, expected_min_batches=5)


class TestDeferredCommitOffsets:
    """Tests for commit_offsets() with a real broker."""

    def test_commit_offsets_then_resume(self, kafka_topic, produce_messages):
        """Manually commit offsets, then verify the next consume resumes from there."""
        produce_messages(kafka_topic, [{"i": i} for i in range(10)])

        group = _unique_group()
        settings = KafkaReadSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            group_id=group,
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )

        # Consume all 10 without committing
        df, result = read_kafka_source(settings, commit=False)
        assert result.messages_consumed == 10

        # Commit all offsets from the first read
        commit_offsets(settings, result.new_offsets)

        # Produce 3 more messages
        produce_messages(kafka_topic, [{"i": i} for i in range(10, 13)])

        # Re-consume — should only get the 3 new messages
        df2, result2 = read_kafka_source(settings)
        assert result2.messages_consumed == 3
        assert df2.height == 3
