"""Worker-side Kafka source reader.

Returns a pl.DataFrame — the generic process machinery handles
serialization to IPC file, same as read_sql_source.
Offset tracking is handled by Kafka consumer groups (subscribe + commit).
"""

from __future__ import annotations

import polars as pl

from shared.kafka.consumer import read_kafka_source
from shared.kafka.models import KafkaReadSettings


def read_kafka(kafka_read_settings: KafkaReadSettings) -> pl.DataFrame:
    """Consume messages from a Kafka topic and return as a DataFrame."""
    df, _ = read_kafka_source(kafka_read_settings)
    return df
