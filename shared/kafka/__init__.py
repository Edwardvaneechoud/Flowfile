"""Shared Kafka consumer logic used by both flowfile_core and flowfile_worker."""

from shared.kafka.consumer import infer_topic_schema, read_kafka_source
from shared.kafka.deserializers import DESERIALIZERS, JsonDeserializer, KafkaDeserializer
from shared.kafka.models import KafkaReadSettings

__all__ = [
    "DESERIALIZERS",
    "JsonDeserializer",
    "KafkaDeserializer",
    "KafkaReadSettings",
    "infer_topic_schema",
    "read_kafka_source",
]
