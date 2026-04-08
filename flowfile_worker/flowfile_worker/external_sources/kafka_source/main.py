"""Worker-side Kafka source reader.

Returns a pl.DataFrame or None — the generic process machinery handles
serialization to IPC file, same as read_sql_source.
Offset tracking is deferred: offsets are written to a sidecar JSON file
and committed by core after downstream success.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from flowfile_worker.secrets import decrypt_secret
from shared.kafka.consumer import read_kafka_source
from shared.kafka.models import KafkaReadSettings


def _decrypt_fn(encrypted: str) -> str:
    """Decrypt an encrypted secret using the worker's shared master key."""
    return decrypt_secret(encrypted).get_secret_value()


def read_kafka(
    kafka_read_settings: KafkaReadSettings,
    sidecar_path: str | None = None,
    file_path: str | None = None,
    **_kw,
) -> pl.DataFrame | None:
    """Consume messages from a Kafka topic.

    When *file_path* is provided, messages are streamed directly to IPC via
    ``spill_path``, avoiding a second write in ``generic_task``.  Returns
    ``None`` to signal that the file is already written.

    Offset metadata is written to *sidecar_path* so that core can commit
    offsets after downstream success (deferred commit pattern).
    """
    df_or_lf, kafka_result = read_kafka_source(
        kafka_read_settings,
        commit=False,
        decrypt_fn=_decrypt_fn,
        spill_path=file_path,
    )

    # Write offset sidecar for deferred commit
    if sidecar_path and kafka_result.messages_consumed > 0:
        Path(sidecar_path).write_text(kafka_result.model_dump_json())

    # If spill_path was used, the file is already written — return None
    # so generic_task skips writing.
    if file_path is not None:
        return None

    # Fallback: return DataFrame for non-spill callers
    if isinstance(df_or_lf, pl.LazyFrame):
        return df_or_lf.collect()
    return df_or_lf
