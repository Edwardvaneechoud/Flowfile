"""Worker-side Kafka source reader.

Returns a pl.DataFrame — the generic process machinery handles
serialization to IPC file, same as read_sql_source.
"""

from __future__ import annotations

import polars as pl

from shared.kafka.consumer import read_kafka_source
from shared.kafka.models import KafkaReadSettings


def read_kafka(kafka_read_settings: KafkaReadSettings) -> pl.DataFrame:
    """Consume messages from a Kafka topic and return as a DataFrame.

    The KafkaReadResult (new offsets, messages consumed) is embedded as
    DataFrame metadata so the core can extract it after deserialization.
    """
    df, result = read_kafka_source(kafka_read_settings)

    # Attach offset metadata to the DataFrame so it survives serialization.
    # Core will read this via df.collect().to_frame().schema or custom metadata.
    # We encode it as extra columns that the core can strip after reading.
    if df.height > 0:
        df = df.with_columns(
            pl.lit(result.messages_consumed).alias("_kafka_meta_messages_consumed"),
        )
    return df
