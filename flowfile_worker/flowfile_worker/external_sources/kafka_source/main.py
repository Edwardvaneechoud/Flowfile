"""Worker-side Kafka source reader.

Returns a pl.DataFrame — the generic process machinery handles
serialization to IPC file, same as read_sql_source.
Offset tracking is handled by Kafka consumer groups (subscribe + commit).
"""

from __future__ import annotations

import polars as pl

from flowfile_worker.secrets import decrypt_secret
from shared.kafka.consumer import read_kafka_source
from shared.kafka.models import KafkaReadSettings


def _decrypt_fn(encrypted: str) -> str:
    """Decrypt an encrypted secret using the worker's shared master key."""
    return decrypt_secret(encrypted).get_secret_value()


def read_kafka(kafka_read_settings: KafkaReadSettings) -> pl.DataFrame:
    """Consume messages from a Kafka topic and return as a DataFrame."""
    df, _ = read_kafka_source(kafka_read_settings, decrypt_fn=_decrypt_fn)
    return df
