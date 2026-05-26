"""Pure input validators for the catalog domain.

No side effects, no DB access. Each function raises an existing
exception from ``catalog.exceptions`` (or ``ValueError``) on failure.
"""

from __future__ import annotations

import zoneinfo
from collections.abc import Callable

from flowfile_core.catalog.constants import (
    MAX_THUMBNAIL_BYTES,
    MIN_SCHEDULE_INTERVAL_SECONDS,
)
from flowfile_core.catalog.exceptions import (
    NamespaceNotFoundError,
    TableExistsError,
    TableNotFoundError,
)


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


def validate_table_registration(
    name: str,
    namespace_id: int | None,
    namespace_exists: Callable[[int], bool],
    table_by_name_exists: Callable[[str, int | None], bool],
) -> None:
    """Validate a table registration: name shape, namespace presence, name uniqueness."""
    reject_dot_in_name(name, "Table")
    if namespace_id is not None and not namespace_exists(namespace_id):
        raise NamespaceNotFoundError(namespace_id=namespace_id)
    if table_by_name_exists(name, namespace_id):
        raise TableExistsError(name=name, namespace_id=namespace_id)


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


def validate_cron_expression(cron_expression: str | None) -> None:
    """Require a non-empty, syntactically valid 5-field cron expression."""
    from croniter import croniter  # local import: only needed for cron schedules

    if not cron_expression or not cron_expression.strip():
        raise ValueError("cron_expression is required for cron schedules")
    # croniter.is_valid also accepts 6-field (seconds-granularity) exprs and @-macros
    # ("@daily"), but the scheduler only evaluates standard 5-field cron — so enforce the
    # documented contract: no leading @, exactly 5 whitespace-separated fields.
    expr = cron_expression.strip()
    if expr.startswith("@") or len(expr.split()) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expression!r}")
    if not croniter.is_valid(expr):
        raise ValueError(f"Invalid cron expression: {cron_expression!r}")


def validate_cron_timezone(cron_timezone: str | None) -> None:
    """Accept ``None``/empty (defaults to UTC); otherwise require a resolvable IANA zone."""
    if not cron_timezone:
        return
    try:
        zoneinfo.ZoneInfo(cron_timezone)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"Invalid timezone: {cron_timezone!r}") from exc


def validate_schedule_create(
    schedule_type: str,
    interval_seconds: int | None,
    trigger_table_id: int | None,
    trigger_table_ids: list[int] | None,
    table_exists: Callable[[int], bool],
    cron_expression: str | None = None,
    cron_timezone: str | None = None,
) -> None:
    """Validate inputs for ScheduleService.create_schedule.

    De-duplicates the rules used by both create and (partially) update.
    ``table_exists`` is a caller-supplied callback so this validator
    stays pure (no repo dependency).
    """
    if schedule_type not in ("interval", "cron", "table_trigger", "table_set_trigger"):
        raise ValueError(f"Invalid schedule_type: {schedule_type}")

    if schedule_type == "interval":
        if interval_seconds is None or interval_seconds < MIN_SCHEDULE_INTERVAL_SECONDS:
            raise ValueError(f"interval_seconds must be >= {MIN_SCHEDULE_INTERVAL_SECONDS}")
    elif schedule_type == "cron":
        validate_cron_expression(cron_expression)
        validate_cron_timezone(cron_timezone)
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


def validate_schedule_update(
    interval_seconds: int | None,
    cron_expression: str | None = None,
    cron_timezone: str | None = None,
) -> None:
    """Validate inputs for ScheduleService.update_schedule."""
    if interval_seconds is not None and interval_seconds < MIN_SCHEDULE_INTERVAL_SECONDS:
        raise ValueError(f"interval_seconds must be >= {MIN_SCHEDULE_INTERVAL_SECONDS}")
    if cron_expression is not None:
        validate_cron_expression(cron_expression)
    validate_cron_timezone(cron_timezone)
