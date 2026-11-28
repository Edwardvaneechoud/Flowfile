"""A shared table's Delta log as the Delta Sharing protocol's ``protocol``, ``metaData`` and ``file`` lines.

Only the log is read (``deltalake.DeltaTable``); no data file is opened and no frame is built.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from deltalake import DeltaTable

from flowfile_core.delta_sharing.errors import invalid

_UNSUPPORTED_READER_FEATURES = {"deletionVectors", "columnMapping"}


@dataclass(frozen=True)
class FileEntry:
    """One data file of a snapshot: its path relative to the table, and what the protocol says about it."""

    rel_path: str
    size: int
    partition_values: dict[str, str | None]
    stats: str | None
    modification_time: int | None


def open_table(path: str, storage_options: dict[str, str] | None, version: int | None = None) -> DeltaTable:
    return DeltaTable(path, storage_options=storage_options or None, version=version)


def reader_refusal(table: DeltaTable) -> str | None:
    """Why a parquet-format recipient could not read this table correctly, or ``None``."""
    features = set(table.protocol().reader_features or [])
    blocked = features & _UNSUPPORTED_READER_FEATURES
    if "deletionVectors" in blocked:
        return "It uses deletion vectors, which Delta Sharing's parquet format cannot express."
    mode = (table.metadata().configuration or {}).get("delta.columnMapping.mode", "none")
    if "columnMapping" in blocked or mode not in ("none", None):
        return "It uses column mapping, which Delta Sharing's parquet format cannot express."
    return None


def parse_timestamp(timestamp: str) -> dt.datetime:
    """An ISO 8601 timestamp as an aware datetime (UTC when it names no zone)."""
    try:
        when = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise invalid(f"timestamp {timestamp!r} is not an ISO 8601 timestamp.") from exc
    return when if when.tzinfo is not None else when.replace(tzinfo=dt.UTC)


def parse_timestamp_ms(timestamp: str) -> int:
    return int(parse_timestamp(timestamp).timestamp() * 1000)


def resolve_version(table: DeltaTable, version: int | None, timestamp: str | None) -> int | None:
    """The version a query asked for (``None`` = head), with a timestamp resolved to the version current then."""
    if version is not None and timestamp is not None:
        raise invalid("Pass either version or timestamp, not both.")
    if version is not None:
        if version < 0:
            raise invalid("version must be zero or positive.")
        return int(version)
    if timestamp is not None:
        when = parse_timestamp(timestamp)
        try:
            table.load_as_version(when)
        except Exception as exc:
            raise invalid(f"The table has no version at or before {timestamp}.") from exc
        return table.version()
    return None


def _json_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return value.astimezone(dt.UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, bytes | bytearray | memoryview):
        return None
    if isinstance(value, dict):
        cleaned = {k: _json_value(v) for k, v in value.items()}
        return {k: v for k, v in cleaned.items() if v is not None}
    if isinstance(value, float) and value != value:
        return None
    return value


def _stats(row: dict[str, Any]) -> str | None:
    if row.get("num_records") is None:
        return None
    stats: dict[str, Any] = {"numRecords": row["num_records"]}
    for source, key in (("min", "minValues"), ("max", "maxValues"), ("null_count", "nullCount")):
        value = row.get(source)
        if isinstance(value, dict):
            stats[key] = _json_value(value)
    return json.dumps(stats, separators=(",", ":"))


def _partition_values(row: dict[str, Any]) -> dict[str, str | None]:
    values = row.get("partition") or {}
    return {k: None if v is None else (_json_value(v) if not isinstance(v, str) else v) for k, v in values.items()}


def snapshot_files(table: DeltaTable) -> list[FileEntry]:
    """The data files of the table's loaded snapshot, in a stable order."""
    import polars as pl

    actions = pl.from_arrow(table.get_add_actions(flatten=False))
    if isinstance(actions, pl.Series):
        actions = actions.struct.unnest() if actions.dtype == pl.Struct else pl.DataFrame()
    rows = actions.to_dicts()
    entries = [
        FileEntry(
            rel_path=str(row["path"]),
            size=int(row.get("size_bytes") or 0),
            partition_values={k: None if v is None else str(v) for k, v in _partition_values(row).items()},
            stats=_stats(row),
            modification_time=row.get("modification_time"),
        )
        for row in rows
    ]
    return sorted(entries, key=lambda e: e.rel_path)


def protocol_line() -> dict[str, Any]:
    return {"protocol": {"minReaderVersion": 1}}


def metadata_line(table: DeltaTable, files: list[FileEntry] | None = None) -> dict[str, Any]:
    meta = table.metadata()
    body: dict[str, Any] = {
        "id": meta.id,
        "format": {"provider": "parquet"},
        "schemaString": table.schema().to_json(),
        "partitionColumns": list(meta.partition_columns or []),
        "configuration": dict(meta.configuration or {}),
        "version": table.version(),
    }
    if meta.name:
        body["name"] = meta.name
    if meta.description:
        body["description"] = meta.description
    if files is not None:
        body["size"] = sum(f.size for f in files)
        body["numFiles"] = len(files)
    return {"metaData": body}


def file_id(table_id: str, rel_path: str) -> str:
    """A stable id for a file of a table, as the protocol asks for one per file."""
    return hashlib.md5(f"{table_id}/{rel_path}".encode(), usedforsecurity=False).hexdigest()
