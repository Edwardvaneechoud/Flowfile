"""Shared Kafka consumer logic used by both flowfile_core and flowfile_worker."""

from shared.kafka.consumer import commit_offsets, infer_topic_schema, make_kafka_commit_callback, read_kafka_source
from shared.kafka.deserializers import DESERIALIZERS, JsonDeserializer, KafkaDeserializer
from shared.kafka.models import DeferredKafkaCommit, KafkaReadSettings

__all__ = [
    "DESERIALIZERS",
    "DeferredKafkaCommit",
    "JsonDeserializer",
    "KafkaDeserializer",
    "KafkaReadSettings",
    "commit_offsets",
    "infer_topic_schema",
    "make_kafka_commit_callback",
    "read_kafka_source",
]
