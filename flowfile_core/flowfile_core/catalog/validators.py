"""Pure input validators for the catalog domain.

No side effects, no DB access. Each function raises an existing
exception from ``catalog.exceptions`` (or ``ValueError``) on failure.
"""

from __future__ import annotations

from typing import Callable

from flowfile_core.catalog.constants import (
    MAX_THUMBNAIL_BYTES,
    MIN_SCHEDULE_INTERVAL_SECONDS,
)
from flowfile_core.catalog.exceptions import TableNotFoundError


def reject_dot_in_name(name: str, kind: str) -> None:
    """Reject names containing '.' to avoid namespace.table ambiguity."""
    if "." in name:
        raise ValueError(
            f"{kind} name '{name}' must not contain '.' — the dot is reserved for qualified "
            f"table references (e.g. 'schema.table_name')."
        )


def format_full_name(namespace_name: str | None, table_name: str) -> str:
    """``namespace.table`` if a namespace is supplied, else bare ``table``."""
    if namespace_name:
        return f"{namespace_name}.{table_name}"
    return table_name


def validate_thumbnail(value: str | None) -> str | None:
    """Validate base64-thumbnail size and prefix."""
    if value is None:
        return None
    if not value.startswith("data:image/"):
        raise ValueError("thumbnail_data_url must be a data:image/* URL")
    if len(value) > MAX_THUMBNAIL_BYTES:
        raise ValueError(f"thumbnail_data_url exceeds {MAX_THUMBNAIL_BYTES} bytes")
    return value


def validate_viz_source(payload) -> None:
    """Ensure the source descriptor on a viz payload is internally consistent."""
    source_type = getattr(payload, "source_type", None)
    if source_type == "table":
        if payload.catalog_table_id is None:
            raise ValueError("catalog_table_id is required when source_type='table'")
    elif source_type == "sql":
        if not payload.sql_query or not payload.sql_query.strip():
            raise ValueError("sql_query is required when source_type='sql'")


def validate_schedule_create(
    schedule_type: str,
    interval_seconds: int | None,
    trigger_table_id: int | None,
    trigger_table_ids: list[int] | None,
    table_exists: Callable[[int], bool],
) -> None:
    """Validate inputs for ScheduleService.create_schedule.

    De-duplicates the rules used by both create and (partially) update.
    ``table_exists`` is a caller-supplied callback so this validator
    stays pure (no repo dependency).
    """
    if schedule_type not in ("interval", "table_trigger", "table_set_trigger"):
        raise ValueError(f"Invalid schedule_type: {schedule_type}")

    if schedule_type == "interval":
        if interval_seconds is None or interval_seconds < MIN_SCHEDULE_INTERVAL_SECONDS:
            raise ValueError(f"interval_seconds must be >= {MIN_SCHEDULE_INTERVAL_SECONDS}")
    elif schedule_type == "table_trigger":
        if trigger_table_id is None:
            raise ValueError("trigger_table_id is required for table_trigger schedules")
        if not table_exists(trigger_table_id):
            raise TableNotFoundError(table_id=trigger_table_id)
    elif schedule_type == "table_set_trigger":
        if not trigger_table_ids or len(trigger_table_ids) < 2:
            raise ValueError("table_set_trigger requires at least 2 trigger_table_ids")
        for table_id in trigger_table_ids:
            if not table_exists(table_id):
                raise TableNotFoundError(table_id=table_id)


def validate_schedule_update(interval_seconds: int | None) -> None:
    """Validate inputs for ScheduleService.update_schedule."""
    if interval_seconds is not None and interval_seconds < MIN_SCHEDULE_INTERVAL_SECONDS:
        raise ValueError(f"interval_seconds must be >= {MIN_SCHEDULE_INTERVAL_SECONDS}")
