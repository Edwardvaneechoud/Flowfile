"""Kafka message deserializers.

Provides a clean interface for adding new formats (Avro, Protobuf)
without modifying the consumer logic.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class KafkaDeserializer(ABC):
    """Base class for Kafka message deserialization."""

    @abstractmethod
    def deserialize(self, value: bytes | None) -> dict | None:
        """Deserialize a single Kafka message value to a dict.

        Returns None if the message cannot be deserialized.
        """


class JsonDeserializer(KafkaDeserializer):
    """Deserializes JSON-encoded Kafka messages."""

    def deserialize(self, value: bytes | None) -> dict | None:
        if value is None:
            return None
        try:
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                logger.warning("JSON message is not an object (got %s), wrapping", type(parsed).__name__)
                return {"value": parsed}
            return parsed
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("Failed to deserialize JSON message: %s", e)
            return None


# Registry of available deserializers — extend here for Avro, Protobuf, etc.
DESERIALIZERS: dict[str, type[KafkaDeserializer]] = {
    "json": JsonDeserializer,
}


def get_deserializer(value_format: str) -> KafkaDeserializer:
    """Get a deserializer instance by format name."""
    cls = DESERIALIZERS.get(value_format)
    if cls is None:
        raise ValueError(f"Unsupported Kafka value format: {value_format!r}. Available: {list(DESERIALIZERS.keys())}")
    return cls()
