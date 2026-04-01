"""Tests for Kafka consumer logic using mocked confluent_kafka."""

from unittest.mock import MagicMock, patch

import pytest

from shared.kafka.consumer import read_kafka_source
from shared.kafka.models import KafkaReadSettings


def _make_poll_fn(messages):
    """Create a poll side_effect function that returns messages then None."""
    it = iter(messages)

    def poll_fn(**kwargs):
        return next(it, None)

    return poll_fn


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


class TestReadKafkaSource:
    @patch("shared.kafka.consumer.Consumer")
    def test_consume_json_messages(self, mock_consumer_cls):
        """Test consuming JSON messages returns a proper DataFrame."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        messages = [
            _make_mock_message("k1", '{"name": "Alice", "age": 30}', 0, 0),
            _make_mock_message("k2", '{"name": "Bob", "age": 25}', 0, 1),
            _make_mock_message(None, '{"name": "Charlie", "age": 35}', 0, 2),
        ]
        consumer.poll.side_effect = _make_poll_fn(messages)

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=0.1,
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
        consumer.subscribe.assert_called_once_with(["test-topic"])
        consumer.commit.assert_called_once()
        consumer.close.assert_called_once()

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_no_messages(self, mock_consumer_cls):
        """Test consuming from empty topic returns empty DataFrame."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        consumer.poll.return_value = None

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="empty-topic",
            group_id="test-group",
            poll_timeout_seconds=0.1,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 0
        assert "_kafka_key" in df.columns
        assert result.messages_consumed == 0
        consumer.subscribe.assert_called_once_with(["empty-topic"])
        consumer.commit.assert_not_called()  # No messages, no commit
        consumer.close.assert_called_once()

    @patch("shared.kafka.consumer.Consumer")
    def test_subscribe_uses_group_id(self, mock_consumer_cls):
        """Test that the consumer group ID from settings is used."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer
        consumer.poll.return_value = None

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="my-custom-group",
            poll_timeout_seconds=0.1,
        )

        read_kafka_source(settings)

        # Verify group.id was passed to Consumer constructor
        config = mock_consumer_cls.call_args[0][0]
        assert config["group.id"] == "my-custom-group"

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_respects_max_messages(self, mock_consumer_cls):
        """Test that max_messages cap is respected."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        msg = _make_mock_message("k", '{"x": 1}', 0, 0)
        consumer.poll.return_value = msg

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
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        messages = [
            _make_mock_message("k1", '{"valid": true}', 0, 0),
            _make_mock_message("k2", "not json", 0, 1),  # will fail
            _make_mock_message("k3", '{"valid": true}', 0, 2),
        ]
        consumer.poll.side_effect = _make_poll_fn(messages)

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=0.1,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 2
        assert result.messages_consumed == 2
        assert result.new_offsets[0] == 3  # past offset 2

    @patch("shared.kafka.consumer.Consumer")
    def test_no_commit_when_commit_false(self, mock_consumer_cls):
        """Test that commit=False skips committing (used for schema probes)."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        messages = [_make_mock_message("k", '{"x": 1}', 0, 0)]
        consumer.poll.side_effect = _make_poll_fn(messages)

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="probe-group",
            poll_timeout_seconds=0.1,
        )

        df, result = read_kafka_source(settings, commit=False)

        assert result.messages_consumed == 1
        consumer.commit.assert_not_called()

    @patch("shared.kafka.consumer.Consumer")
    def test_null_kafka_key_cast_to_string(self, mock_consumer_cls):
        """Test that _kafka_key is String type even when all keys are None."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        messages = [
            _make_mock_message(None, '{"a": 1}', 0, 0),
            _make_mock_message(None, '{"a": 2}', 0, 1),
        ]
        consumer.poll.side_effect = _make_poll_fn(messages)

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            poll_timeout_seconds=0.1,
        )

        df, _ = read_kafka_source(settings)

        import polars as pl

        assert df["_kafka_key"].dtype == pl.String
