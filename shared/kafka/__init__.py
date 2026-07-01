"""Shared Kafka consumer logic used by both flowfile_core and flowfile_worker."""

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

_CONSUMER_EXPORTS = ("commit_offsets", "infer_topic_schema", "make_kafka_commit_callback", "read_kafka_source")


def __getattr__(name: str):
    # consumer.py imports confluent_kafka + pyarrow.ipc at module level; loading
    # it eagerly would tax every `import shared.kafka.models` (e.g. the
    # `import flowfile_frame` path). Resolve the consumer surface on first use.
    if name in _CONSUMER_EXPORTS:
        from shared.kafka import consumer

        return getattr(consumer, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
