"""The recipient's side of Delta Sharing: what an API consumer's token may list and read, and the answers.

Every request resolves from the token again: the consumer must hold a grant on the share, the share's owner must
be active and still manage the table, and the table must still be one that can be served. Anything else is the
same 404 an unknown name gets, so a recipient learns of nothing it may not read.
"""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import active_db_user
from flowfile_core.catalog.storage_backend import CatalogStorageTarget
from flowfile_core.database import models as db_models
from flowfile_core.delta_sharing import config, eligibility, file_tokens, log_reader, presign
from flowfile_core.delta_sharing.errors import SharingError, invalid, not_found

MAX_PAGE_SIZE = 1000


@dataclass(frozen=True)
class ServedTable:
    """A share's table as a recipient addresses it, with the catalog row behind it."""

    share: db_models.DeltaShare
    entry: db_models.DeltaShareTable
    table: db_models.CatalogTable


@dataclass(frozen=True)
class OpenedTable:
    served: ServedTable
    target: CatalogStorageTarget
    delta: Any


def _page(items: list[dict], max_results: int | None, page_token: str | None) -> dict[str, Any]:
    start = 0
    if page_token:
        try:
            start = int(base64.urlsafe_b64decode(page_token.encode()).decode())
        except (ValueError, UnicodeDecodeError) as exc:
            raise invalid("pageToken is not one this server handed out.") from exc
    if max_results is not None and max_results < 0:
        raise invalid("maxResults must be zero or positive.")
    size = MAX_PAGE_SIZE if not max_results else min(max_results, MAX_PAGE_SIZE)
    chunk = items[start : start + size]
    body: dict[str, Any] = {"items": chunk}
    if start + size < len(items):
        body["nextPageToken"] = base64.urlsafe_b64encode(str(start + size).encode()).decode()
    return body


def readable_shares(db: Session, consumer: db_models.ApiConsumer) -> list[db_models.DeltaShare]:
    """The shares the consumer is granted whose owner is still active, by name."""
    rows = (
        db.query(db_models.DeltaShare)
        .join(db_models.ApiConsumerDeltaShare, db_models.ApiConsumerDeltaShare.share_id == db_models.DeltaShare.id)
        .filter(db_models.ApiConsumerDeltaShare.consumer_id == consumer.id)
        .order_by(db_models.DeltaShare.name)
        .all()
    )
    return [s for s in rows if active_db_user(db, s.owner_id) is not None]


def share_named(db: Session, consumer: db_models.ApiConsumer, name: str) -> db_models.DeltaShare:
    for share in readable_shares(db, consumer):
        if share.name == name:
            return share
    raise not_found(f"Share {name!r}")


def served_tables(db: Session, share: db_models.DeltaShare) -> list[ServedTable]:
    """The share's tables a recipient sees now: rows that still name their table, which its owner manages."""
    entries = (
        db.query(db_models.DeltaShareTable)
        .filter(db_models.DeltaShareTable.share_id == share.id)
        .order_by(db_models.DeltaShareTable.schema_name, db_models.DeltaShareTable.table_name)
        .all()
    )
    served: list[ServedTable] = []
    for entry in entries:
        table = db.get(db_models.CatalogTable, entry.table_id)
        if table is None or table.file_path != entry.table_path:
            continue
        if eligibility.row_refusal(table) is not None:
            continue
        if not eligibility.owner_manages(db, share.owner_id, table):
            continue
        served.append(ServedTable(share=share, entry=entry, table=table))
    return served


def _share_item(share: db_models.DeltaShare) -> dict[str, Any]:
    return {"name": share.name, "id": share.uuid}


def _table_item(served: ServedTable) -> dict[str, Any]:
    return {
        "name": served.entry.table_name,
        "schema": served.entry.schema_name,
        "share": served.share.name,
        "shareId": served.share.uuid,
        "id": served.entry.uuid,
    }


def list_shares(db, consumer, max_results, page_token) -> dict[str, Any]:
    return _page([_share_item(s) for s in readable_shares(db, consumer)], max_results, page_token)


def get_share(db, consumer, share_name) -> dict[str, Any]:
    return {"share": _share_item(share_named(db, consumer, share_name))}


def list_schemas(db, consumer, share_name, max_results, page_token) -> dict[str, Any]:
    share = share_named(db, consumer, share_name)
    names = sorted({t.entry.schema_name for t in served_tables(db, share)})
    return _page([{"name": n, "share": share.name} for n in names], max_results, page_token)


def list_tables(db, consumer, share_name, schema_name, max_results, page_token) -> dict[str, Any]:
    share = share_named(db, consumer, share_name)
    tables = [t for t in served_tables(db, share) if t.entry.schema_name == schema_name]
    if not tables:
        raise not_found(f"Schema {share_name}.{schema_name!r}")
    return _page([_table_item(t) for t in tables], max_results, page_token)


def list_all_tables(db, consumer, share_name, max_results, page_token) -> dict[str, Any]:
    share = share_named(db, consumer, share_name)
    return _page([_table_item(t) for t in served_tables(db, share)], max_results, page_token)


def _served(db, consumer, share_name, schema_name, table_name) -> ServedTable:
    share = share_named(db, consumer, share_name)
    for served in served_tables(db, share):
        if served.entry.schema_name == schema_name and served.entry.table_name == table_name:
            return served
    raise not_found(f"Table {share_name}.{schema_name}.{table_name}")


def open_served(db, consumer, share_name, schema_name, table_name, version: int | None = None) -> OpenedTable:
    """The served table opened at *version* (head when ``None``); a table that can no longer be served is a 404."""
    served = _served(db, consumer, share_name, schema_name, table_name)
    table = served.table
    try:
        target = eligibility.storage_target(table)
    except ValueError as exc:
        raise not_found(f"Table {share_name}.{schema_name}.{table_name}") from exc
    if presign.signing_refusal(target, table.file_path) is not None:
        raise not_found(f"Table {share_name}.{schema_name}.{table_name}")
    try:
        delta = log_reader.open_table(table.file_path, target.storage_options, version)
    except Exception as exc:
        if version is not None:
            raise invalid(f"Version {version} of the table is not available.") from exc
        raise SharingError(500, "INTERNAL_ERROR", "The table's Delta log could not be read.") from exc
    if log_reader.reader_refusal(delta) is not None:
        raise not_found(f"Table {share_name}.{schema_name}.{table_name}")
    return OpenedTable(served=served, target=target, delta=delta)


def _require_history(served: ServedTable) -> None:
    if not served.entry.share_history:
        raise SharingError(403, "PERMISSION_DENIED", "This table is shared without its history.")


def table_version(db, consumer, share_name, schema_name, table_name, starting_timestamp: str | None) -> int:
    opened = open_served(db, consumer, share_name, schema_name, table_name)
    if starting_timestamp is None:
        return opened.delta.version()
    _require_history(opened.served)
    wanted = log_reader.parse_timestamp_ms(starting_timestamp)
    for commit in sorted(opened.delta.history(), key=lambda c: c["version"]):
        if commit.get("timestamp", 0) >= wanted:
            return int(commit["version"])
    raise invalid(f"The table has no version at or after {starting_timestamp}.")


def metadata_lines(db, consumer, share_name, schema_name, table_name) -> tuple[int, list[dict]]:
    opened = open_served(db, consumer, share_name, schema_name, table_name)
    return opened.delta.version(), [log_reader.protocol_line(), log_reader.metadata_line(opened.delta)]


def ndjson(lines: list[dict]) -> Iterator[bytes]:
    for line in lines:
        yield (json.dumps(line, separators=(",", ":")) + "\n").encode()


def query_lines(
    db,
    consumer,
    share_name,
    schema_name,
    table_name,
    body: dict[str, Any],
    protocol_base_url: str,
) -> tuple[int, list[dict]]:
    """``(version, lines)`` answering a ``/query`` for a snapshot of the table."""
    if body.get("startingVersion") is not None or body.get("endingVersion") is not None:
        raise invalid("Reading a range of versions (change data feed) is not served yet; query one version.")
    version = body.get("version")
    timestamp = body.get("timestamp")
    if version is not None and not isinstance(version, int):
        raise invalid("version must be an integer.")
    pinned = version is not None or timestamp is not None
    opened = open_served(db, consumer, share_name, schema_name, table_name)
    if pinned:
        _require_history(opened.served)
        resolved = log_reader.resolve_version(opened.delta, version, timestamp)
        if resolved != opened.delta.version():
            opened = open_served(db, consumer, share_name, schema_name, table_name, resolved)
    delta = opened.delta
    files = log_reader.snapshot_files(delta)
    meta = log_reader.metadata_line(delta, files)
    table_uuid = meta["metaData"]["id"]
    ttl = config.url_ttl_seconds()
    now = time.time()
    lines: list[dict] = [log_reader.protocol_line(), meta]
    for entry in files:
        try:
            url, expires_ms = presign.file_url(
                target=opened.target,
                table_path=opened.served.table.file_path,
                rel_path=entry.rel_path,
                share_table_id=opened.served.entry.id,
                protocol_base_url=protocol_base_url,
                ttl=ttl,
                now=now,
            )
        except ValueError as exc:
            raise SharingError(500, "INTERNAL_ERROR", str(exc)) from exc
        file_line: dict[str, Any] = {
            "url": url,
            "id": log_reader.file_id(table_uuid, entry.rel_path),
            "partitionValues": entry.partition_values,
            "size": entry.size,
            "expirationTimestamp": expires_ms,
        }
        if entry.stats is not None:
            file_line["stats"] = entry.stats
        if pinned:
            file_line["version"] = delta.version()
            if entry.modification_time is not None:
                file_line["timestamp"] = int(entry.modification_time)
        lines.append({"file": file_line})
    return delta.version(), lines


def local_file_for(db: Session, token: str):
    """The local file a signed URL grants, or ``None`` when the token or the table behind it no longer holds."""
    grant = file_tokens.verify(token)
    if grant is None:
        return None
    entry = db.get(db_models.DeltaShareTable, grant.share_table_id)
    if entry is None:
        return None
    table = db.get(db_models.CatalogTable, entry.table_id)
    if table is None or not table.file_path or table.file_path != entry.table_path:
        return None
    if file_tokens.table_digest(table.file_path) != grant.table_digest:
        return None
    share = db.get(db_models.DeltaShare, entry.share_id)
    if share is None or eligibility.row_refusal(table) is not None:
        return None
    if not eligibility.owner_manages(db, share.owner_id, table):
        return None
    try:
        if eligibility.storage_target(table).is_cloud:
            return None
    except ValueError:
        return None
    return presign.local_file(table.file_path, grant.rel_path)
