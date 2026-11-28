"""Managing Delta shares: their tables, the API consumers granted them, and the profile files recipients use.

Anyone may create a share; only its owner (or an admin) changes it. A table is added only by someone who manages
it, and is served only while the share's owner still does. A share is granted to an API consumer the caller owns
(an admin may grant any). Raises ``LookupError`` (404), ``PermissionError`` (403), ``ShareConflict`` (409) and
``ValueError`` (422).
"""

from __future__ import annotations

import datetime as dt
import re

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from flowfile_core.catalog.access import AccessResolver
from flowfile_core.database import models as db_models
from flowfile_core.delta_sharing import config, eligibility
from flowfile_core.delta_sharing.presign import signing_refusal
from flowfile_core.flowfile import api_consumer_manager
from flowfile_core.schemas import delta_sharing_schema as schema


class ShareConflict(ValueError):
    """A name or grant that already exists."""


_UNSAFE = re.compile(r"[^A-Za-z0-9_-]+")


def safe_name(text: str) -> str:
    """*text* as a protocol-safe name: letters, digits, ``_`` and ``-``, never empty or leading ``-``."""
    cleaned = _UNSAFE.sub("_", text.strip()).strip("_") or "table"
    return cleaned if not cleaned.startswith("-") else f"_{cleaned}"


def _is_admin(user) -> bool:
    return bool(getattr(user, "is_admin", False))


class DeltaSharingService:
    def __init__(self, db: Session, user) -> None:
        self.db = db
        self.user = user
        self.access = AccessResolver(db, user)

    def _namespace_path(self, namespace_id: int | None) -> list[str]:
        parts: list[str] = []
        seen: set[int] = set()
        current = namespace_id
        while current is not None and current not in seen:
            seen.add(current)
            row = self.db.get(db_models.CatalogNamespace, current)
            if row is None:
                break
            parts.append(row.name)
            current = row.parent_id
        return list(reversed(parts))

    def _catalog_name(self, table: db_models.CatalogTable) -> str:
        return ".".join([*self._namespace_path(table.namespace_id), table.name])

    def _default_names(self, table: db_models.CatalogTable) -> tuple[str, str]:
        path = self._namespace_path(table.namespace_id)
        return safe_name(path[-1] if path else "default"), safe_name(table.name)

    def _can_manage_share(self, share: db_models.DeltaShare) -> bool:
        return _is_admin(self.user) or share.owner_id == self.user.id

    def _share(self, share_id: int) -> db_models.DeltaShare:
        share = self.db.get(db_models.DeltaShare, share_id)
        if share is None or not self._can_manage_share(share):
            raise LookupError("Share not found")
        return share

    def _consumer(self, consumer_id: int) -> db_models.ApiConsumer:
        owner = None if _is_admin(self.user) else self.user.id
        return api_consumer_manager.get_consumer(self.db, consumer_id, owner)

    def _commit(self, conflict: str) -> None:
        try:
            self.db.commit()
        except IntegrityError as exc:
            self.db.rollback()
            raise ShareConflict(conflict) from exc

    def _table_out(self, share: db_models.DeltaShare, entry: db_models.DeltaShareTable) -> schema.DeltaShareTableOut:
        table = self.db.get(db_models.CatalogTable, entry.table_id)
        if table is None or table.file_path != entry.table_path:
            refusal: str | None = "The table no longer exists."
            name = "(deleted table)"
        else:
            name = self._catalog_name(table)
            refusal = eligibility.row_refusal(table)
            if refusal is None and not eligibility.owner_manages(self.db, share.owner_id, table):
                refusal = "The share's owner no longer manages this table."
        return schema.DeltaShareTableOut(
            id=entry.id,
            table_id=entry.table_id,
            catalog_name=name,
            schema_name=entry.schema_name,
            table_name=entry.table_name,
            share_history=entry.share_history,
            served=refusal is None,
            refusal=refusal,
        )

    def share_out(self, share: db_models.DeltaShare) -> schema.DeltaShareOut:
        entries = (
            self.db.query(db_models.DeltaShareTable)
            .filter_by(share_id=share.id)
            .order_by(db_models.DeltaShareTable.schema_name, db_models.DeltaShareTable.table_name)
            .all()
        )
        recipients = (
            self.db.query(db_models.ApiConsumer)
            .join(
                db_models.ApiConsumerDeltaShare, db_models.ApiConsumerDeltaShare.consumer_id == db_models.ApiConsumer.id
            )
            .filter(db_models.ApiConsumerDeltaShare.share_id == share.id)
            .order_by(db_models.ApiConsumer.name)
            .all()
        )
        return schema.DeltaShareOut(
            id=share.id,
            name=share.name,
            description=share.description,
            owner_id=share.owner_id,
            uuid=share.uuid,
            created_at=share.created_at,
            can_manage=self._can_manage_share(share),
            tables=[self._table_out(share, e) for e in entries],
            recipients=[
                schema.DeltaShareRecipientOut(consumer_id=c.id, name=c.name, enabled=c.enabled) for c in recipients
            ],
        )

    def list_shares(self) -> list[schema.DeltaShareOut]:
        query = self.db.query(db_models.DeltaShare)
        if not _is_admin(self.user):
            query = query.filter(db_models.DeltaShare.owner_id == self.user.id)
        return [self.share_out(s) for s in query.order_by(db_models.DeltaShare.name).all()]

    def get_share(self, share_id: int) -> schema.DeltaShareOut:
        return self.share_out(self._share(share_id))

    def create_share(self, body: schema.DeltaShareCreate) -> schema.DeltaShareOut:
        share = db_models.DeltaShare(name=body.name, description=body.description, owner_id=self.user.id)
        self.db.add(share)
        self._commit(f"A share named {body.name!r} already exists.")
        self.db.refresh(share)
        return self.share_out(share)

    def update_share(self, share_id: int, body: schema.DeltaShareUpdate) -> schema.DeltaShareOut:
        share = self._share(share_id)
        if body.name is not None:
            share.name = body.name
        if body.description is not None:
            share.description = body.description
        self._commit(f"A share named {body.name!r} already exists.")
        self.db.refresh(share)
        return self.share_out(share)

    def delete_share(self, share_id: int) -> None:
        share = self._share(share_id)
        self.db.query(db_models.ApiConsumerDeltaShare).filter_by(share_id=share.id).delete(synchronize_session=False)
        self.db.query(db_models.DeltaShareTable).filter_by(share_id=share.id).delete(synchronize_session=False)
        self.db.delete(share)
        self.db.commit()

    def add_table(self, share_id: int, body: schema.DeltaShareTableAdd) -> schema.DeltaShareOut:
        share = self._share(share_id)
        table = self.db.get(db_models.CatalogTable, body.table_id)
        if table is None or table.id <= 0:
            raise LookupError("Table not found")
        if not self.access.can_manage("catalog_table", table.id, table.owner_id):
            raise PermissionError("Only someone who manages a table may share it.")
        refusal = eligibility.full_refusal(table)
        if refusal is not None:
            raise ValueError(refusal)
        default_schema, default_table = self._default_names(table)
        entry = db_models.DeltaShareTable(
            share_id=share.id,
            table_id=table.id,
            schema_name=body.schema_name or default_schema,
            table_name=body.table_name or default_table,
            share_history=body.share_history,
            table_path=table.file_path,
        )
        self.db.add(entry)
        self._commit(
            "The share already holds this table, or another table under " f"{entry.schema_name}.{entry.table_name}."
        )
        return self.share_out(share)

    def update_table(self, share_id: int, entry_id: int, body: schema.DeltaShareTableUpdate) -> schema.DeltaShareOut:
        share = self._share(share_id)
        entry = self.db.get(db_models.DeltaShareTable, entry_id)
        if entry is None or entry.share_id != share.id:
            raise LookupError("Shared table not found")
        if body.schema_name is not None:
            entry.schema_name = body.schema_name
        if body.table_name is not None:
            entry.table_name = body.table_name
        if body.share_history is not None:
            entry.share_history = body.share_history
        self._commit(f"The share already holds a table under {entry.schema_name}.{entry.table_name}.")
        return self.share_out(share)

    def remove_table(self, share_id: int, entry_id: int) -> schema.DeltaShareOut:
        share = self._share(share_id)
        self.db.query(db_models.DeltaShareTable).filter_by(id=entry_id, share_id=share.id).delete(
            synchronize_session=False
        )
        self.db.commit()
        return self.share_out(share)

    def grant(self, share_id: int, consumer_id: int) -> schema.DeltaShareOut:
        share = self._share(share_id)
        consumer = self._consumer(consumer_id)
        self.db.add(db_models.ApiConsumerDeltaShare(consumer_id=consumer.id, share_id=share.id))
        self._commit("That consumer can already read this share.")
        return self.share_out(share)

    def revoke(self, share_id: int, consumer_id: int) -> schema.DeltaShareOut:
        share = self._share(share_id)
        self.db.query(db_models.ApiConsumerDeltaShare).filter_by(share_id=share.id, consumer_id=consumer_id).delete(
            synchronize_session=False
        )
        self.db.commit()
        return self.share_out(share)

    def eligible_tables(self) -> list[schema.DeltaShareEligibleTableOut]:
        """Physical Delta tables of this install the caller manages, each with why it cannot be shared, if so."""
        rows = (
            self.db.query(db_models.CatalogTable)
            .filter(db_models.CatalogTable.table_type == "physical", db_models.CatalogTable.id > 0)
            .filter(db_models.CatalogTable.storage_format != "iceberg")
            .order_by(db_models.CatalogTable.name)
            .all()
        )
        out: list[schema.DeltaShareEligibleTableOut] = []
        for table in rows:
            if not self.access.can_manage("catalog_table", table.id, table.owner_id):
                continue
            refusal = eligibility.row_refusal(table)
            if refusal is None:
                try:
                    refusal = signing_refusal(eligibility.storage_target(table), table.file_path)
                except ValueError as exc:
                    refusal = str(exc)
            default_schema, default_table = self._default_names(table)
            out.append(
                schema.DeltaShareEligibleTableOut(
                    table_id=table.id,
                    catalog_name=self._catalog_name(table),
                    default_schema_name=default_schema,
                    default_table_name=default_table,
                    refusal=refusal,
                )
            )
        return sorted(out, key=lambda t: t.catalog_name.lower())

    def shares_of_consumer(self, consumer_id: int) -> list[schema.DeltaShareOut]:
        consumer = self._consumer(consumer_id)
        shares = (
            self.db.query(db_models.DeltaShare)
            .join(db_models.ApiConsumerDeltaShare, db_models.ApiConsumerDeltaShare.share_id == db_models.DeltaShare.id)
            .filter(db_models.ApiConsumerDeltaShare.consumer_id == consumer.id)
            .order_by(db_models.DeltaShare.name)
            .all()
        )
        return [self.share_out(s) for s in shares]

    def create_profile(self, share_id: int, body: schema.DeltaShareProfileCreate, fallback_endpoint: str):
        """A new key for a consumer granted the share, written as a Delta Sharing profile file (shown once)."""
        share = self._share(share_id)
        consumer = self._consumer(body.consumer_id)
        granted = (
            self.db.query(db_models.ApiConsumerDeltaShare).filter_by(share_id=share.id, consumer_id=consumer.id).first()
        )
        if granted is None:
            raise ValueError("Grant the consumer access to this share before creating its profile.")
        endpoint = _endpoint(body.endpoint, fallback_endpoint)
        expires_at = None
        if body.expires_in_days:
            expires_at = dt.datetime.now(dt.UTC) + dt.timedelta(days=body.expires_in_days)
        key, raw = api_consumer_manager.create_key(
            self.db, consumer, name=body.key_name or f"Delta Sharing: {share.name}", expires_at=expires_at
        )
        profile: dict = {"shareCredentialsVersion": 1, "endpoint": endpoint, "bearerToken": raw}
        if expires_at is not None:
            profile["expirationTime"] = expires_at.isoformat(timespec="seconds").replace("+00:00", "Z")
        out = self.share_out(share)
        urls = [f"{share.name}.{t.schema_name}.{t.table_name}" for t in out.tables if t.served]
        return schema.DeltaShareProfileOut(
            profile=profile, file_name=f"{share.name}.share", key_prefix=key.key_prefix, table_urls=urls
        )


def _endpoint(requested: str | None, fallback: str) -> str:
    """The protocol endpoint for a profile: the configured one, else the API base the caller names, else *fallback*."""
    configured = config.configured_endpoint()
    if configured:
        return configured
    if requested:
        base = requested.strip().rstrip("/")
        if not re.match(r"^https?://", base):
            raise ValueError("endpoint must be an http(s) URL.")
        return base if base.endswith(config.PROTOCOL_PREFIX) else base + config.PROTOCOL_PREFIX
    return fallback
