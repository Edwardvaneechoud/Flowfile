"""Tests for Kafka consumer logic using mocked confluent_kafka."""

from unittest.mock import MagicMock, patch

import pytest

from shared.kafka.consumer import read_kafka_source
from shared.kafka.models import KafkaReadSettings


def _make_mock_message(key, value, partition, offset, timestamp_ms=1700000000000):
    """Create a mock Kafka message."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.key.return_value = key.encode() if key else None
    msg.value.return_value = value.encode() if isinstance(value, str) else value
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = (1, timestamp_ms)  # CREATE_TIME
    return msg


def _make_eof_message(partition):
    """Create a mock EOF message."""
    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = -191  # KafkaError._PARTITION_EOF
    msg.error.return_value = err
    msg.partition.return_value = partition
    return msg


def _setup_consumer(mock_cls, topic, partitions=None):
    """Set up a mock consumer with topic subscription and partition assignment."""
    consumer = MagicMock()
    mock_cls.return_value = consumer

    if partitions is None:
        partitions = {0}

    # Simulate on_assign callback firing during subscribe
    def fake_subscribe(topics, on_assign=None):
        if on_assign:
            tps = [MagicMock(partition=p) for p in partitions]
            on_assign(consumer, tps)

    consumer.subscribe.side_effect = fake_subscribe
    return consumer


def _make_consume_fn(batches):
    """Create a consume() side_effect that returns batches then empty lists."""
    it = iter(batches)

    def consume_fn(**kwargs):
        return next(it, [])

    return consume_fn


class TestReadKafkaSource:
    @patch("shared.kafka.consumer.Consumer")
    def test_consume_json_messages(self, mock_consumer_cls):
        """Test consuming JSON messages returns a proper DataFrame."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0})

        batch1 = [
            _make_mock_message("k1", '{"name": "Alice", "age": 30}', 0, 0),
            _make_mock_message("k2", '{"name": "Bob", "age": 25}', 0, 1),
            _make_mock_message(None, '{"name": "Charlie", "age": 35}', 0, 2),
        ]
        consumer.consume.side_effect = _make_consume_fn([batch1, [_make_eof_message(0)]])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=5.0,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 3
        assert "name" in df.columns
        assert "age" in df.columns
        assert "_kafka_key" in df.columns
        assert "_kafka_partition" in df.columns
        assert "_kafka_offset" in df.columns
        assert "_kafka_timestamp" in df.columns

        assert result.messages_consumed == 3
        assert result.new_offsets == {0: 3}
        consumer.subscribe.assert_called_once()
        consumer.commit.assert_called_once()
        consumer.close.assert_called_once()

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_no_messages(self, mock_consumer_cls):
        """Test consuming from empty topic returns empty DataFrame."""
        consumer = _setup_consumer(mock_consumer_cls, "empty-topic", {0})

        # Empty batches + EOF
        consumer.consume.side_effect = _make_consume_fn([[_make_eof_message(0)]])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="empty-topic",
            group_id="test-group",
            poll_timeout_seconds=5.0,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 0
        assert "_kafka_key" in df.columns
        assert result.messages_consumed == 0
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()

    @patch("shared.kafka.consumer.Consumer")
    def test_subscribe_uses_group_id(self, mock_consumer_cls):
        """Test that the consumer group ID from settings is used."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0})
        consumer.consume.side_effect = _make_consume_fn([[_make_eof_message(0)]])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="my-custom-group",
            poll_timeout_seconds=5.0,
        )

        read_kafka_source(settings)

        config = mock_consumer_cls.call_args[0][0]
        assert config["group.id"] == "my-custom-group"

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_respects_max_messages(self, mock_consumer_cls):
        """Test that max_messages cap is respected."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0})

        # Return batches of messages — more than max_messages
        big_batch = [_make_mock_message("k", '{"x": 1}', 0, i) for i in range(10)]
        consumer.consume.side_effect = _make_consume_fn([big_batch, big_batch])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            max_messages=5,
            poll_timeout_seconds=10.0,
        )

        df, result = read_kafka_source(settings)

        assert result.messages_consumed == 5
        assert df.height == 5
        consumer.commit.assert_called_once()

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_skips_deserialization_errors(self, mock_consumer_cls):
        """Test that messages failing deserialization are skipped."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0})

        batch = [
            _make_mock_message("k1", '{"valid": true}', 0, 0),
            _make_mock_message("k2", "not json", 0, 1),
            _make_mock_message("k3", '{"valid": true}', 0, 2),
        ]
        consumer.consume.side_effect = _make_consume_fn([batch, [_make_eof_message(0)]])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=5.0,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 2
        assert result.messages_consumed == 2
        assert result.new_offsets[0] == 3

    @patch("shared.kafka.consumer.Consumer")
    def test_no_commit_when_commit_false(self, mock_consumer_cls):
        """Test that commit=False skips committing (used for schema probes)."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0})

        batch = [_make_mock_message("k", '{"x": 1}', 0, 0)]
        consumer.consume.side_effect = _make_consume_fn([batch, [_make_eof_message(0)]])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="probe-group",
            poll_timeout_seconds=5.0,
        )

        df, result = read_kafka_source(settings, commit=False)

        assert result.messages_consumed == 1
        consumer.commit.assert_not_called()

    @patch("shared.kafka.consumer.Consumer")
    def test_null_kafka_key_cast_to_string(self, mock_consumer_cls):
        """Test that _kafka_key is String type even when all keys are None."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0})

        batch = [
            _make_mock_message(None, '{"a": 1}', 0, 0),
            _make_mock_message(None, '{"a": 2}', 0, 1),
        ]
        consumer.consume.side_effect = _make_consume_fn([batch, [_make_eof_message(0)]])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=5.0,
        )

        df, _ = read_kafka_source(settings)

        import polars as pl

        assert df["_kafka_key"].dtype == pl.String

    @patch("shared.kafka.consumer.Consumer")
    def test_breaks_on_all_partitions_eof(self, mock_consumer_cls):
        """Test that consumer breaks immediately when all partitions reach EOF."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0, 1})

        batch = [
            _make_mock_message("k1", '{"x": 1}', 0, 0),
            _make_mock_message("k2", '{"x": 2}', 1, 0),
            _make_eof_message(0),
            _make_eof_message(1),
        ]
        consumer.consume.side_effect = _make_consume_fn([batch])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=30.0,  # Long timeout — should break early via EOF
        )

        import time

        start = time.monotonic()
        df, result = read_kafka_source(settings)
        elapsed = time.monotonic() - start

        assert result.messages_consumed == 2
        assert elapsed < 15  # Should complete well before 30s timeout

    @patch("shared.kafka.consumer.Consumer")
    def test_breaks_on_consecutive_empty_polls(self, mock_consumer_cls):
        """Test early exit after consecutive empty consume() results."""
        consumer = _setup_consumer(mock_consumer_cls, "test-topic", {0})

        batch = [_make_mock_message("k", '{"x": 1}', 0, 0)]
        # One batch of messages, then empty results
        consumer.consume.side_effect = _make_consume_fn([batch, [], []])

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=30.0,
        )

        import time

        start = time.monotonic()
        df, result = read_kafka_source(settings)
        elapsed = time.monotonic() - start

        assert result.messages_consumed == 1
        assert elapsed < 15
