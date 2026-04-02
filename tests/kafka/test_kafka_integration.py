"""Kafka integration tests using a real Redpanda broker.

All tests are marked with @pytest.mark.kafka and require Docker.
Run with:  poetry run pytest tests/kafka -m kafka -v
"""

import uuid

import pytest

from shared.kafka.consumer import infer_topic_schema, read_kafka_source
from shared.kafka.models import KafkaReadSettings
from test_utils.kafka.fixtures import BOOTSTRAP_SERVERS, produce_json_messages

pytestmark = pytest.mark.kafka


def _unique_group() -> str:
    """Generate a unique consumer group ID per test invocation."""
    return f"test-{uuid.uuid4().hex[:12]}"


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


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


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
