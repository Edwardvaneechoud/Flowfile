"""Shared Kafka consumer logic used by both flowfile_core and flowfile_worker."""

from shared.kafka.consumer import read_kafka_source
from shared.kafka.deserializers import DESERIALIZERS, JsonDeserializer, KafkaDeserializer
from shared.kafka.models import KafkaReadSettings

__all__ = [
    "DESERIALIZERS",
    "JsonDeserializer",
    "KafkaDeserializer",
    "KafkaReadSettings",
    "read_kafka_source",
]
