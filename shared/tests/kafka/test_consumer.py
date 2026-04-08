"""Tests for Kafka consumer logic using a real Redpanda broker.

Requires a running Redpanda container. Start with:
    poetry run start_redpanda

Run tests:
    poetry run pytest shared/tests/kafka/test_consumer.py -v
"""

import os
import tempfile
import uuid
from unittest.mock import patch

import polars as pl

from shared.kafka.consumer import commit_offsets, read_kafka_source
from shared.kafka.models import KafkaReadSettings
from test_utils.kafka.fixtures import BOOTSTRAP_SERVERS, produce_json_messages


def _unique_group() -> str:
    return f"test-{uuid.uuid4().hex[:12]}"


def _settings(topic: str, **overrides) -> KafkaReadSettings:
    defaults = dict(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=topic,
        group_id=_unique_group(),
        start_offset="earliest",
        poll_timeout_seconds=10.0,
    )
    defaults.update(overrides)
    return KafkaReadSettings(**defaults)


# ---------------------------------------------------------------------------
# Basic consumer tests
# ---------------------------------------------------------------------------


class TestReadKafkaSource:
    def test_consume_json_messages(self, kafka_topic):
        """Produce JSON messages, consume them, verify the DataFrame."""
        messages = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        produce_json_messages(kafka_topic, messages)

        df, result = read_kafka_source(_settings(kafka_topic))

        assert df.height == 3
        assert "name" in df.columns
        assert "age" in df.columns
        assert "_kafka_key" in df.columns
        assert "_kafka_partition" in df.columns
        assert "_kafka_offset" in df.columns
        assert "_kafka_timestamp" in df.columns
        assert result.messages_consumed == 3
        assert result.new_offsets == {0: 3}

    def test_consume_no_messages(self, kafka_topic):
        """Consuming from an empty topic returns an empty DataFrame."""
        df, result = read_kafka_source(_settings(kafka_topic, poll_timeout_seconds=3.0))

        assert df.height == 0
        assert "_kafka_key" in df.columns
        assert result.messages_consumed == 0

    def test_consume_respects_max_messages(self, kafka_topic):
        """max_messages should cap consumed messages."""
        produce_json_messages(kafka_topic, [{"x": i} for i in range(10)])

        df, result = read_kafka_source(_settings(kafka_topic, max_messages=5))

        assert result.messages_consumed == 5
        assert df.height == 5

    def test_consume_skips_deserialization_errors(self, kafka_topic):
        """Invalid JSON messages should be skipped."""
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
        producer.produce(kafka_topic, value=b'{"valid": true, "seq": 1}')
        producer.produce(kafka_topic, value=b"not json")
        producer.produce(kafka_topic, value=b'{"valid": true, "seq": 2}')
        producer.flush(timeout=10.0)

        df, result = read_kafka_source(_settings(kafka_topic))

        assert df.height == 2
        assert result.messages_consumed == 2

    def test_no_commit_when_commit_false(self, kafka_topic):
        """commit=False should not commit offsets; re-consuming gets same messages."""
        produce_json_messages(kafka_topic, [{"x": 1}])

        group = _unique_group()
        settings = _settings(kafka_topic, group_id=group)

        df1, _ = read_kafka_source(settings, commit=False)
        assert df1.height == 1

        # Same group, should re-read the same message since we didn't commit
        df2, result2 = read_kafka_source(settings)
        assert result2.messages_consumed == 1

    def test_null_kafka_key_cast_to_string(self, kafka_topic):
        """_kafka_key should be String type even when all keys are None."""
        produce_json_messages(kafka_topic, [{"a": 1}, {"a": 2}])

        df, _ = read_kafka_source(_settings(kafka_topic))

        assert df["_kafka_key"].dtype == pl.String

    def test_consume_with_message_keys(self, kafka_topic):
        """Messages produced with keys should have _kafka_key populated."""
        produce_json_messages(kafka_topic, [{"id": "k1", "v": 1}, {"id": "k2", "v": 2}], key_field="id")

        df, _ = read_kafka_source(_settings(kafka_topic))

        assert df.height == 2
        assert set(df["_kafka_key"].to_list()) == {"k1", "k2"}


# ---------------------------------------------------------------------------
# Spill-to-IPC tests
# ---------------------------------------------------------------------------


class TestSpillToIpc:
    """Tests for the spill_path parameter that streams rows to IPC."""

    def test_spill_path_returns_lazyframe(self, kafka_topic):
        """When spill_path is set, result should be a LazyFrame backed by the IPC file."""
        produce_json_messages(kafka_topic, [{"name": "Alice"}, {"name": "Bob"}])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            result_data, result = read_kafka_source(
                _settings(kafka_topic), commit=False, spill_path=spill_path
            )

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 2
            assert os.path.exists(spill_path)

            df = result_data.collect()
            assert df.height == 2
            assert "name" in df.columns
            assert "_kafka_key" in df.columns
        finally:
            os.unlink(spill_path)

    def test_spill_path_empty_topic(self, kafka_topic):
        """Spill path with empty topic still returns a LazyFrame with correct schema."""
        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            result_data, result = read_kafka_source(
                _settings(kafka_topic, poll_timeout_seconds=3.0),
                commit=False,
                spill_path=spill_path,
            )

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 0
            df = result_data.collect()
            assert df.height == 0
            assert "_kafka_key" in df.columns
        finally:
            os.unlink(spill_path)

    def test_without_spill_path_returns_dataframe(self, kafka_topic):
        """Without spill_path, result should still be a DataFrame (backward compat)."""
        produce_json_messages(kafka_topic, [{"a": 1}])

        result_data, result = read_kafka_source(_settings(kafka_topic), commit=False)

        assert isinstance(result_data, pl.DataFrame)
        assert result.messages_consumed == 1


class TestSpillFlushOverThreshold:
    """Tests that IPC flush mechanics work correctly.

    Patches _FLUSH_SIZE to 3 so we trigger multi-flush with just a handful
    of real messages — no mocks needed.
    """

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_flush_at_exactly_flush_size(self, kafka_topic):
        """Exactly _FLUSH_SIZE messages: one flush, no remainder."""
        produce_json_messages(kafka_topic, [{"n": i} for i in range(3)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            result_data, result = read_kafka_source(
                _settings(kafka_topic), commit=False, spill_path=spill_path
            )

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 3
            assert os.path.getsize(spill_path) > 0

            df = result_data.collect()
            assert df.height == 3
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_flush_with_remainder(self, kafka_topic):
        """5 messages with _FLUSH_SIZE=3: one flush (3) + remainder (2)."""
        produce_json_messages(kafka_topic, [{"seq": i, "val": f"row_{i}"} for i in range(5)])
        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            result_data, result = read_kafka_source(
                _settings(kafka_topic), commit=False, spill_path=spill_path
            )

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 5

            df = result_data.collect()
            assert df.height == 5

            for col in ("seq", "val", "_kafka_key", "_kafka_partition", "_kafka_offset", "_kafka_timestamp"):
                assert col in df.columns, f"Missing column: {col}"

            assert df["_kafka_offset"].n_unique() == 5
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_two_full_flushes_plus_remainder(self, kafka_topic):
        """8 messages with _FLUSH_SIZE=3: two flushes (3+3) + remainder (2)."""
        produce_json_messages(kafka_topic, [{"seq": i} for i in range(8)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            result_data, result = read_kafka_source(
                _settings(kafka_topic), commit=False, spill_path=spill_path
            )

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 8

            df = result_data.collect()
            assert df.height == 8
            assert df["_kafka_offset"].n_unique() == 8
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_flush_ipc_file_is_valid_arrow(self, kafka_topic):
        """The spilled IPC file is valid Arrow with multiple record batches."""
        import pyarrow.ipc

        produce_json_messages(kafka_topic, [{"n": i} for i in range(4)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            read_kafka_source(_settings(kafka_topic), commit=False, spill_path=spill_path)

            with pyarrow.ipc.open_file(spill_path) as reader:
                assert reader.num_record_batches == 2  # flush (3) + remainder (1)
                total_rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
                assert total_rows == 4

                schema = reader.schema
                assert "n" in schema.names
                assert "_kafka_offset" in schema.names
        finally:
            os.unlink(spill_path)

    @patch("shared.kafka.consumer._FLUSH_SIZE", 3)
    def test_below_threshold_writes_single_batch(self, kafka_topic):
        """Messages below _FLUSH_SIZE with spill_path: single-batch IPC file."""
        import pyarrow.ipc

        produce_json_messages(kafka_topic, [{"n": i} for i in range(2)])

        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
            spill_path = f.name

        try:
            result_data, result = read_kafka_source(
                _settings(kafka_topic), commit=False, spill_path=spill_path
            )

            assert isinstance(result_data, pl.LazyFrame)
            assert result.messages_consumed == 2

            df = result_data.collect()
            assert df.height == 2

            with pyarrow.ipc.open_file(spill_path) as reader:
                assert reader.num_record_batches == 1
        finally:
            os.unlink(spill_path)


# ---------------------------------------------------------------------------
# commit_offsets tests
# ---------------------------------------------------------------------------


class TestCommitOffsets:
    """Tests for the commit_offsets() function."""

    def test_commit_offsets_then_resume(self, kafka_topic):
        """Manually commit offsets, then verify the next consume resumes from there."""
        produce_json_messages(kafka_topic, [{"i": i} for i in range(10)])

        group = _unique_group()

        # Consume all 10 without committing
        settings = _settings(kafka_topic, group_id=group)
        df, result = read_kafka_source(settings, commit=False)
        assert result.messages_consumed == 10

        # Manually commit offsets for only the first 5
        commit_offsets(settings, {0: 5})

        # Re-consume with the same group — should get the remaining 5
        df2, result2 = read_kafka_source(settings)
        assert result2.messages_consumed == 5
        assert df2.height == 5
