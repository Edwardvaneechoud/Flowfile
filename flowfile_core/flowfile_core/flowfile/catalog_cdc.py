"""Change-window resolution and cursor bookkeeping for change-feed (CDC) readers.

``resolve_change_window`` is shared by the catalog reader and the cloud Delta reader; cursors are
catalog-only. A cursor is the last Delta commit version one consumer fully processed, keyed by
``(table_id, consumer_key)``. Advancing it is **at-least-once**: the commit callback is stored on
``FlowNode._on_flow_complete`` and only ``run_graph`` invokes those, after a full run in which the
reader and everything downstream of it completed — so a preview, a single-node run or a cancel
never moves a cursor, and a re-run after a downstream failure re-reads the same window.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime

from sqlalchemy.orm import Session

from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogCdcCursor, FlowRegistration
from shared.delta_utils import get_delta_version_at_or_after, scd2_parse_iso_utc

logger = logging.getLogger(__name__)

UNRESOLVABLE_CONSUMER_MESSAGE = (
    "Reading changes since the last run needs a stable flow identity. Save this flow first, "
    "or give the cursor a name."
)


def resolve_consumer_key(graph, node_settings) -> tuple[str, str | None]:
    """Resolve the ``(consumer_key, label)`` a since-last-run reader commits against.

    A typed name is global to the table — deliberately shared across flows and notebooks. Without
    one the key is anchored on ``FlowRegistration.flow_uuid``, the only flow identity that survives
    a delete-and-recreate, so an unregistered flow has no cursor to advance.

    Raises:
        ValueError: The flow has neither a typed cursor name nor a registration.
    """
    from flowfile_core.flowfile.catalog_helpers import resolve_source_registration_id

    name = (node_settings.cdc_consumer_name or "").strip()
    if name:
        return f"name:{name}", name

    resolve_source_registration_id(graph)
    registration_id = getattr(graph.flow_settings, "source_registration_id", None)
    flow_uuid = None
    if registration_id is not None:
        with get_db_context() as db:
            registration = db.get(FlowRegistration, registration_id)
            flow_uuid = getattr(registration, "flow_uuid", None)
    if not flow_uuid:
        raise ValueError(UNRESOLVABLE_CONSUMER_MESSAGE)
    return f"flow:{flow_uuid}:node:{node_settings.node_id}", getattr(graph.flow_settings, "name", None)


def read_cursor(
    db: Session, table_id: int, consumer_key: str, table_path: str | None = None
) -> CatalogCdcCursor | None:
    """The stored cursor, or ``None``.

    A cursor whose recorded *table_path* no longer matches the table is treated as absent: SQLite
    reuses rowids, so resuming it would replay an unrelated table's history.
    """
    cursor = SQLAlchemyCatalogRepository(db).get_cdc_cursor(table_id, consumer_key)
    if cursor is None:
        return None
    if table_path is not None and cursor.table_path is not None and str(cursor.table_path) != str(table_path):
        logger.warning(
            "Change cursor %s on table %s was minted against %s, re-initialising",
            consumer_key,
            table_id,
            cursor.table_path,
        )
        return None
    return cursor


def init_cursor_value(cdc_start: str, head: int, cdc_enabled_version: int | None) -> int:
    """The value a first read starts from.

    ``"now"`` initialises at head, so the first run reads nothing new. ``"beginning"`` initialises
    one below the enablement floor, so the next read starts exactly at the first tracked commit.
    """
    if cdc_start == "beginning":
        floor = cdc_enabled_version if cdc_enabled_version is not None else 0
        return floor - 1
    return head


def _resolved_cdc_version(value: object) -> int:
    """The commit version a since-version reader starts after, once ``${param}`` refs are substituted."""
    if value is None or isinstance(value, bool):
        raise ValueError(f"cdc_from_version must resolve to a commit version, got {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"cdc_from_version must resolve to a commit version, got {value!r} — is that flow parameter defined?"
        ) from exc


def _resolved_cdc_instant(value: object) -> datetime:
    """The UTC instant a since-time reader starts from, once ``${param}`` refs are substituted."""
    try:
        return scd2_parse_iso_utc(str(value))
    except ValueError as exc:
        raise ValueError(
            f"cdc_from_timestamp must resolve to an ISO-8601 instant, got {value!r} — is that flow parameter defined?"
        ) from exc


def resolve_change_window(
    cdc_mode: str,
    from_version: object,
    from_timestamp: object,
    *,
    head: int,
    floor: int | None,
    path: str,
    storage_options: dict | None,
    last_version: int | None = None,
    cdc_start: str = "now",
) -> int:
    """The first commit version a change read covers; the window ends at *head*.

    ``since_version`` starts after the resolved *from_version*. ``since_timestamp`` starts at the
    first commit at or after the resolved *from_timestamp* — resolved to a version here so the floor
    clamp applies to it too — or at ``head + 1`` (an empty window) when every commit predates it.
    ``since_last_run`` starts after the stored cursor *last_version*, or where ``init_cursor_value``
    places a first read. Every start is clamped to the enablement *floor* (``None`` ⇒ no clamp),
    since reads below it are inconsistent.
    """
    if cdc_mode == "since_timestamp":
        instant = _resolved_cdc_instant(from_timestamp)
        resolved = get_delta_version_at_or_after(path, instant, storage_options=storage_options)
        starting_version = resolved if resolved is not None else head + 1
    elif cdc_mode == "since_version":
        starting_version = _resolved_cdc_version(from_version) + 1
    else:
        if last_version is None:
            last_version = init_cursor_value(cdc_start, head, floor)
        starting_version = last_version + 1
    return starting_version if floor is None else max(starting_version, floor)


def commit_cursor(
    db: Session,
    table_id: int,
    consumer_key: str,
    version: int,
    *,
    timestamp: datetime | None = None,
    owner_id: int | None = None,
    label: str | None = None,
    table_path: str | None = None,
    run_id: int | None = None,
) -> CatalogCdcCursor:
    """Record *version* as fully processed for this consumer."""
    return SQLAlchemyCatalogRepository(db).upsert_cdc_cursor(
        table_id,
        consumer_key,
        version,
        last_commit_timestamp=timestamp,
        owner_id=owner_id,
        consumer_label=label,
        table_path=table_path,
        last_run_id=run_id,
    )


def make_cdc_commit_callback(
    table_id: int,
    consumer_key: str,
    head: int,
    node_id: int | str,
    flow_logger,
    *,
    owner_id: int | None = None,
    label: str | None = None,
    table_path: str | None = None,
) -> Callable[[bool], None]:
    """Create the post-execution callback that advances a change cursor on success.

    Stored on ``FlowNode._on_flow_complete`` so ``run_graph`` invokes it after all downstream
    nodes complete. Mirrors ``shared.kafka.consumer.make_kafka_commit_callback``.
    """

    def _on_complete(success: bool) -> None:
        if not success:
            flow_logger.warning(f"Change cursor NOT advanced for node {node_id} (downstream failure or cancel)")
            return
        try:
            with get_db_context() as db:
                commit_cursor(
                    db,
                    table_id,
                    consumer_key,
                    head,
                    owner_id=owner_id,
                    label=label,
                    table_path=table_path,
                )
            flow_logger.info(f"Advanced change cursor for node {node_id} to v{head} ({consumer_key})")
        except Exception as e:
            flow_logger.error(f"Failed to advance change cursor for node {node_id}: {e}")

    return _on_complete


def reset_cursor(
    db: Session,
    table_id: int,
    consumer_key: str,
    to: str | int,
    head: int,
    floor: int | None,
) -> CatalogCdcCursor | None:
    """Move an existing cursor to head (``"now"``), below the floor (``"beginning"``) or a version.

    Returns ``None`` when no such cursor exists.
    """
    if to == "now":
        version = head
    elif to == "beginning":
        version = init_cursor_value("beginning", head, floor)
    else:
        version = int(to)
    return SQLAlchemyCatalogRepository(db).reset_cdc_cursor(table_id, consumer_key, version)
