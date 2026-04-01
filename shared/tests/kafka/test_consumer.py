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


def _setup_position_mock(consumer, topic, partition_offsets):
    """Set up consumer.position() and get_watermark_offsets() mocks.

    partition_offsets: dict of {partition_id: offset} for position results.
    """

    def position_fn(tps):
        result = []
        for tp in tps:
            mock_tp = MagicMock()
            mock_tp.offset = partition_offsets.get(tp.partition, -1001)
            result.append(mock_tp)
        return result

    consumer.position.side_effect = position_fn
    consumer.get_watermark_offsets.return_value = (0, 0)


class TestReadKafkaSource:
    @patch("shared.kafka.consumer.Consumer")
    def test_consume_json_messages(self, mock_consumer_cls):
        """Test consuming JSON messages returns a proper DataFrame."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        # Mock topic metadata
        partition_meta = {0: MagicMock()}
        topic_meta = MagicMock()
        topic_meta.error = None
        topic_meta.partitions = partition_meta
        cluster_meta = MagicMock()
        cluster_meta.topics = {"test-topic": topic_meta}
        consumer.list_topics.return_value = cluster_meta

        # Return 3 messages then None (end of poll)
        messages = [
            _make_mock_message("k1", '{"name": "Alice", "age": 30}', 0, 0),
            _make_mock_message("k2", '{"name": "Bob", "age": 25}', 0, 1),
            _make_mock_message(None, '{"name": "Charlie", "age": 35}', 0, 2),
            None,  # No more messages
            None,
        ]
        consumer.poll.side_effect = _make_poll_fn(messages)

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
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
        assert result.partitions_read == 1
        consumer.close.assert_called_once()

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_no_messages(self, mock_consumer_cls):
        """Test consuming from empty topic returns empty DataFrame."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        partition_meta = {0: MagicMock()}
        topic_meta = MagicMock()
        topic_meta.error = None
        topic_meta.partitions = partition_meta
        cluster_meta = MagicMock()
        cluster_meta.topics = {"empty-topic": topic_meta}
        consumer.list_topics.return_value = cluster_meta

        consumer.poll.return_value = None
        _setup_position_mock(consumer, "empty-topic", {0: -1001})

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="empty-topic",
            poll_timeout_seconds=0.1,
        )

        df, result = read_kafka_source(settings)

        assert df.height == 0
        assert "_kafka_key" in df.columns
        assert result.messages_consumed == 0
        consumer.close.assert_called_once()

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_with_offsets(self, mock_consumer_cls):
        """Test that provided offsets are used for partition assignment."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        partition_meta = {0: MagicMock(), 1: MagicMock()}
        topic_meta = MagicMock()
        topic_meta.error = None
        topic_meta.partitions = partition_meta
        cluster_meta = MagicMock()
        cluster_meta.topics = {"test-topic": topic_meta}
        consumer.list_topics.return_value = cluster_meta
        consumer.poll.return_value = None
        _setup_position_mock(consumer, "test-topic", {0: 100, 1: 200})

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            offsets={0: 100, 1: 200},
            poll_timeout_seconds=0.1,
        )

        df, result = read_kafka_source(settings)

        # Verify assign was called with correct partitions
        consumer.assign.assert_called_once()
        assigned = consumer.assign.call_args[0][0]
        assert len(assigned) == 2
        assert assigned[0].offset == 100
        assert assigned[1].offset == 200

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_respects_max_messages(self, mock_consumer_cls):
        """Test that max_messages cap is respected."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        partition_meta = {0: MagicMock()}
        topic_meta = MagicMock()
        topic_meta.error = None
        topic_meta.partitions = partition_meta
        cluster_meta = MagicMock()
        cluster_meta.topics = {"test-topic": topic_meta}
        consumer.list_topics.return_value = cluster_meta

        # Return unlimited messages
        msg = _make_mock_message("k", '{"x": 1}', 0, 0)
        consumer.poll.return_value = msg

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            max_messages=5,
            poll_timeout_seconds=10.0,
        )

        df, result = read_kafka_source(settings)

        assert result.messages_consumed == 5
        assert df.height == 5

    @patch("shared.kafka.consumer.Consumer")
    def test_consume_skips_deserialization_errors(self, mock_consumer_cls):
        """Test that messages failing deserialization are skipped."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        partition_meta = {0: MagicMock()}
        topic_meta = MagicMock()
        topic_meta.error = None
        topic_meta.partitions = partition_meta
        cluster_meta = MagicMock()
        cluster_meta.topics = {"test-topic": topic_meta}
        consumer.list_topics.return_value = cluster_meta

        messages = [
            _make_mock_message("k1", '{"valid": true}', 0, 0),
            _make_mock_message("k2", "not json", 0, 1),  # will fail
            _make_mock_message("k3", '{"valid": true}', 0, 2),
            None,
            None,
        ]
        consumer.poll.side_effect = _make_poll_fn(messages)

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            poll_timeout_seconds=0.1,
        )

        df, result = read_kafka_source(settings)

        # Only 2 valid messages, but offset advances past the failed one too
        assert df.height == 2
        assert result.messages_consumed == 2
        assert result.new_offsets[0] == 3  # past offset 2

    @patch("shared.kafka.consumer.Consumer")
    def test_topic_not_found(self, mock_consumer_cls):
        """Test that missing topic raises ValueError."""
        consumer = MagicMock()
        mock_consumer_cls.return_value = consumer

        cluster_meta = MagicMock()
        cluster_meta.topics = {}
        consumer.list_topics.return_value = cluster_meta

        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="nonexistent",
            poll_timeout_seconds=0.1,
        )

        with pytest.raises(ValueError, match="not found"):
            read_kafka_source(settings)

        consumer.close.assert_called_once()
