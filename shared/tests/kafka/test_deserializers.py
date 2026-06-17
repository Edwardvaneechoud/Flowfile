"""Tests for Kafka deserializers."""

import pytest

from shared.kafka.deserializers import JsonDeserializer, get_deserializer


class TestJsonDeserializer:
    def test_deserialize_valid_json(self):
        d = JsonDeserializer()
        result = d.deserialize(b'{"name": "Alice", "age": 30}')
        assert result == {"name": "Alice", "age": 30}

    def test_deserialize_nested_json(self):
        d = JsonDeserializer()
        result = d.deserialize(b'{"user": {"name": "Bob"}, "scores": [1, 2, 3]}')
        assert result == {"user": {"name": "Bob"}, "scores": [1, 2, 3]}

    def test_deserialize_none_value(self):
        d = JsonDeserializer()
        result = d.deserialize(None)
        assert result is None

    def test_deserialize_invalid_json(self):
        d = JsonDeserializer()
        result = d.deserialize(b"not valid json")
        assert result is None

    def test_deserialize_empty_bytes(self):
        d = JsonDeserializer()
        result = d.deserialize(b"")
        assert result is None


class TestGetDeserializer:
    def test_get_json_deserializer(self):
        d = get_deserializer("json")
        assert isinstance(d, JsonDeserializer)

    def test_get_unsupported_format(self):
        with pytest.raises(ValueError, match="Unsupported Kafka value format"):
            get_deserializer("avro")
