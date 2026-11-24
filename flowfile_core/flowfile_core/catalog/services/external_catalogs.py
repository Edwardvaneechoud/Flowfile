"""Catalogs kept by another provider: an Iceberg REST catalog added to Flowfile's catalog by its address and sign-in.

The catalog is a level-0 ``catalog_namespaces`` row marked ``kept_elsewhere`` with one ``external_catalogs`` row
saying how to reach it, and the other catalog's top-level namespaces are level-1 rows marked the same way, kept in
line by ``sync``. Its tables are never rows: they are listed live and kept a minute per schema in this process. The
sign-in is one secret of the owner's, a bearer token or a client id and secret, and only its ciphertext leaves this
module. Nothing here asks the other catalog for a storage key.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from flowfile_core.catalog.exceptions import (
    KeptElsewhereError,
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.validators import reject_dot_in_name
from flowfile_core.database.models import CatalogNamespace, ExternalCatalog, Secret
from flowfile_core.schemas.catalog_schema import (
    ColumnSchema,
    ExternalCatalogConnection,
    ExternalCatalogCreate,
    ExternalCatalogOut,
    ExternalCatalogSyncResult,
    ExternalCatalogTest,
    ExternalCatalogTestResult,
    ExternalCatalogUpdate,
    ExternalTableDetail,
    ExternalTableList,
    ExternalTableOut,
    NamespaceOut,
)
from shared.iceberg_utils import IcebergCatalogRef, IcebergTableRef, client_credentials_sign_in

logger = logging.getLogger(__name__)

SYNC_MIN_SECONDS = 30
"""A sync asked for within this many seconds of the last one answers the stored state, unless forced."""

TABLE_LIST_SECONDS = 60
"""How long a schema's table names are kept in this process before the other catalog is asked again."""

KIND = "iceberg_rest"

_SIGN_IN_AGAIN = "Enter the sign-in again to change where it is sent."

_TABLE_LISTS: dict[tuple[int, str], tuple[float, datetime, list[str]]] = {}


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def forget_tables(external_catalog_id: int) -> None:
    """Drop the kept table names of one catalog, so its next listing asks the other catalog."""
    for key in [key for key in _TABLE_LISTS if key[0] == external_catalog_id]:
        _TABLE_LISTS.pop(key, None)


def gone_sentence(catalog_name: str, gone_since: datetime) -> str:
    return f"No longer in {catalog_name}'s own catalog since {gone_since:%Y-%m-%d %H:%M} UTC."


def refuse_kept_elsewhere(db: Session, namespace_id: int | None, action: str) -> None:
    """Raise ``KeptElsewhereError`` when ``namespace_id`` is a catalog or schema another provider keeps."""
    if namespace_id is None:
        return
    namespace = db.get(CatalogNamespace, namespace_id)
    if namespace is not None and namespace.kept_elsewhere:
        raise KeptElsewhereError(namespace.name, action)


def _sign_in_text(connection: ExternalCatalogConnection, secret: str | None) -> str | None:
    """What the stored secret holds for this sign-in: the token, or the client id and secret as Flowfile writes them."""
    if connection.sign_in == "none" or secret is None:
        return None
    if connection.sign_in == "token":
        return secret
    return client_credentials_sign_in(connection.client_id, secret, connection.scope, connection.token_endpoint)


def _decrypting():
    """Core's decryptor for a ``$ffsec$`` value, registered when this module is imported."""
    from flowfile_core.flowfile.flow_data_engine import cloud_storage_reader  # noqa: F401


class ExternalCatalogService:
    """Add, test, change, remove and sync catalogs kept elsewhere, and list their tables."""

    def __init__(self, repo: CatalogRepository, namespaces) -> None:
        self.repo = repo
        self._namespaces = namespaces

    @property
    def _db(self) -> Session:
        return self.repo._db

    # -- refs --

    def _ref(self, connection: ExternalCatalogConnection, plain_sign_in: str | None, user_id: int) -> IcebergCatalogRef:
        """The catalog of an unsaved form, its sign-in encrypted in memory as a stored one is, so both run the same."""
        from flowfile_core.secret_manager.secret_manager import encrypt_secret

        _decrypting()
        token = encrypt_secret(plain_sign_in, user_id) if plain_sign_in else None
        return IcebergCatalogRef(connection.uri, connection.warehouse, token)

    def _secret_row(self, row: ExternalCatalog) -> Secret | None:
        return self._db.get(Secret, row.secret_id) if row.secret_id is not None else None

    def _stored_ref(self, row: ExternalCatalog) -> IcebergCatalogRef:
        _decrypting()
        secret = self._secret_row(row)
        return IcebergCatalogRef(row.uri, row.warehouse, secret.encrypted_value if secret is not None else None)

    def _stored_secret(self, row: ExternalCatalog) -> str | None:
        """The token or the client secret the row's secret holds, decrypted here in core."""
        import json

        from flowfile_core.secret_manager.secret_manager import decrypt_secret

        secret = self._secret_row(row)
        if secret is None:
            return None
        plain = decrypt_secret(secret.encrypted_value).get_secret_value()
        if row.sign_in == "client_credentials":
            return json.loads(plain)["client_secret"]
        return plain

    def _row(self, namespace_id: int) -> tuple[CatalogNamespace, ExternalCatalog]:
        namespace = self.repo.get_namespace(namespace_id)
        row = self.repo.get_external_catalog(namespace_id) if namespace is not None else None
        if namespace is None or row is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        return namespace, row

    def _connection(self, row: ExternalCatalog) -> ExternalCatalogConnection:
        return ExternalCatalogConnection(
            uri=row.uri,
            warehouse=row.warehouse,
            sign_in=row.sign_in,
            client_id=row.client_id,
            scope=row.scope,
            token_endpoint=row.token_endpoint,
        )

    # -- out --

    def out(self, row: ExternalCatalog) -> ExternalCatalogOut:
        return ExternalCatalogOut.model_validate(row)

    def namespace_out(self, namespace: CatalogNamespace) -> NamespaceOut:
        out = NamespaceOut.model_validate(namespace)
        row = self.repo.get_external_catalog(namespace.id) if namespace.level == 0 else None
        if row is not None:
            out.external = self.out(row)
        return out

    # -- test, add, change, remove --

    def _try(self, ref: IcebergCatalogRef) -> ExternalCatalogTestResult:
        from shared.iceberg_utils import list_schemas

        try:
            shown, _ = list_schemas(ref)
        except ValueError as exc:
            return ExternalCatalogTestResult(ok=False, message=str(exc))
        said = "Signed in." if ref.token else "Reached."
        return ExternalCatalogTestResult(ok=True, message=f"{said} {len(shown)} schemas.", schemas=len(shown))

    def test(self, body: ExternalCatalogTest | ExternalCatalogConnection, user_id: int) -> ExternalCatalogTestResult:
        """Whether the catalog answers with this sign-in, in one sentence; with a stored catalog, its secret if none."""
        secret = body.sign_in_secret()
        namespace_id = getattr(body, "namespace_id", None)
        if secret is None and namespace_id is not None and body.sign_in != "none":
            _, row = self._row(namespace_id)
            same_target = (body.uri, body.token_endpoint, body.sign_in) == (row.uri, row.token_endpoint, row.sign_in)
            if not same_target:
                return ExternalCatalogTestResult(
                    ok=False, message="Enter the sign-in again to change where it is sent."
                )
            secret = self._stored_secret(row)
            user_id = self._secret_row(row).user_id
        if body.sign_in != "none" and secret is None:
            return ExternalCatalogTestResult(ok=False, message="Enter the sign-in to test it.")
        return self._try(self._ref(body, _sign_in_text(body, secret), user_id))

    def create(self, body: ExternalCatalogCreate, owner_id: int) -> CatalogNamespace:
        """Add the catalog once a test passes, then bring its schemas in; nothing stays when a step fails."""
        from flowfile_core.auth.models import SecretInput
        from flowfile_core.secret_manager.secret_manager import store_secret

        reject_dot_in_name(body.name, "Catalog")
        if self.repo.get_namespace_by_name(body.name, None) is not None:
            raise NamespaceExistsError(name=body.name)
        tried = self.test(body, owner_id)
        if not tried.ok:
            raise ValueError(tried.message)
        text = _sign_in_text(body, body.sign_in_secret())
        made: dict = {}
        try:
            if text is not None:
                made["secret"] = store_secret(
                    self._db, SecretInput(name=f"{body.name}__catalog_sign_in", value=text), owner_id
                )
            namespace = CatalogNamespace(
                name=body.name, level=0, description=body.description, owner_id=owner_id, kept_elsewhere=True
            )
            made["namespace"] = self.repo.create_namespace(namespace)
            made["row"] = self.repo.create_external_catalog(
                ExternalCatalog(
                    namespace_id=namespace.id,
                    kind=KIND,
                    uri=body.uri,
                    warehouse=body.warehouse,
                    sign_in=body.sign_in,
                    client_id=body.client_id,
                    scope=body.scope,
                    token_endpoint=body.token_endpoint,
                    secret_id=made["secret"].id if "secret" in made else None,
                )
            )
            self.sync(namespace.id, force=True)
        except Exception:
            self._db.rollback()
            self._undo(made)
            raise
        logger.info("Added the catalog %s kept at %s", body.name, body.uri)
        return self.repo.get_namespace(namespace.id)

    def _undo(self, made: dict) -> None:
        if "namespace" in made:
            for schema in self.repo.list_child_namespaces(made["namespace"].id):
                self.repo.delete_namespace(schema.id)
            self.repo.delete_external_catalog(made["namespace"].id)
            self.repo.delete_namespace(made["namespace"].id)
        if "secret" in made:
            self._db.delete(made["secret"])
            self._db.commit()

    def update(self, namespace_id: int, body: ExternalCatalogUpdate) -> CatalogNamespace:
        """Change the description or how the catalog is reached; a new target or kind of sign-in needs the secret."""
        from flowfile_core.auth.models import SecretInput
        from flowfile_core.secret_manager.secret_manager import encrypt_secret, store_secret

        namespace, row = self._row(namespace_id)
        given = body.model_dump(exclude_unset=True, exclude={"description"})
        base = self._connection(row).model_dump()
        if given.get("sign_in", row.sign_in) != row.sign_in:
            base.update(client_id=None, scope=None, token_endpoint=None)
        connection = ExternalCatalogConnection(**{**base, **given})
        new_secret = connection.sign_in_secret()
        moved = (connection.uri, connection.token_endpoint, connection.sign_in) != (
            row.uri,
            row.token_endpoint,
            row.sign_in,
        )
        if moved and new_secret is None and connection.sign_in != "none":
            raise ValueError(_SIGN_IN_AGAIN)
        secret = new_secret if new_secret is not None else self._stored_secret(row)
        text = _sign_in_text(connection, secret)
        stored = self._secret_row(row)
        if given:
            tried = self._try(self._ref(connection, text, stored.user_id if stored else namespace.owner_id))
            if not tried.ok:
                raise ValueError(tried.message)
        if text is not None and stored is not None:
            stored.encrypted_value = encrypt_secret(text, stored.user_id)
        elif text is not None:
            stored = store_secret(
                self._db, SecretInput(name=f"{namespace.name}__catalog_sign_in", value=text), namespace.owner_id
            )
            row.secret_id = stored.id
        dropped = stored if text is None else None
        for name in ("uri", "warehouse", "sign_in", "client_id", "scope", "token_endpoint"):
            setattr(row, name, getattr(connection, name))
        if dropped is not None:
            row.secret_id = None
        if "description" in body.model_fields_set:
            namespace.description = body.description
        self.repo.update_external_catalog(row)
        if dropped is not None:
            self._db.delete(dropped)
            self._db.commit()
        forget_tables(row.id)
        logger.info("Changed the catalog %s kept at %s", namespace.name, row.uri)
        return self.repo.get_namespace(namespace_id)

    def delete(self, namespace_id: int) -> None:
        """Remove the catalog from Flowfile: its rows, their grants and its secret. The other catalog is not touched."""
        namespace, row = self._row(namespace_id)
        schemas = self.repo.list_child_namespaces(namespace_id)
        for schema in schemas:
            tables = self.repo.count_tables_in_namespace(schema.id)
            flows = self.repo.count_flows_in_namespace(schema.id)
            notebooks = self.repo.count_notebooks_in_namespace(schema.id)
            if tables or flows or notebooks:
                raise NamespaceNotEmptyError(schema.id, tables=tables, flows=flows, notebooks=notebooks)
        secret = self._secret_row(row)
        for schema in schemas:
            self.repo.delete_namespace(schema.id)
        forget_tables(row.id)
        self.repo.delete_external_catalog(namespace_id)
        self.repo.delete_namespace(namespace_id)
        if secret is not None:
            self._db.delete(secret)
            self._db.commit()
        logger.info("Removed the catalog %s kept at %s from Flowfile", namespace.name, row.uri)

    # -- sync --

    def sync(self, namespace_id: int, *, force: bool = False) -> ExternalCatalogSyncResult:
        """Bring the schema rows in line with the other catalog's namespaces; never deletes a row."""
        from shared.iceberg_utils import list_schemas

        namespace, row = self._row(namespace_id)
        recent = row.synced_at is not None and (_now() - row.synced_at).total_seconds() < SYNC_MIN_SECONDS
        if recent and not force:
            return ExternalCatalogSyncResult(
                not_shown=row.not_shown, synced_at=row.synced_at, skipped=True, error=row.sync_error
            )
        try:
            names, not_shown = list_schemas(self._stored_ref(row))
        except ValueError as exc:
            row.sync_error = str(exc)
            self.repo.update_external_catalog(row)
            logger.info("Could not bring the catalog %s at %s in line", namespace.name, row.uri)
            return ExternalCatalogSyncResult(not_shown=row.not_shown, synced_at=row.synced_at, error=row.sync_error)
        now = _now()
        existing = {schema.name: schema for schema in self.repo.list_child_namespaces(namespace_id)}
        result = ExternalCatalogSyncResult(not_shown=not_shown, synced_at=now)
        for name in names:
            schema = existing.get(name)
            if schema is None:
                self._db.add(
                    CatalogNamespace(
                        name=name, parent_id=namespace_id, level=1, owner_id=namespace.owner_id, kept_elsewhere=True
                    )
                )
                result.added.append(name)
            elif schema.gone_since is not None:
                schema.gone_since = None
                result.back.append(name)
        for name, schema in existing.items():
            if name not in names and schema.gone_since is None:
                schema.gone_since = now
                result.gone.append(name)
        row.synced_at, row.sync_error, row.not_shown = now, None, not_shown
        self.repo.update_external_catalog(row)
        if force:
            forget_tables(row.id)
        return result

    # -- tables --

    def _schema(self, namespace_id: int, schema_id: int) -> tuple[CatalogNamespace, ExternalCatalog, CatalogNamespace]:
        namespace, row = self._row(namespace_id)
        schema = self.repo.get_namespace(schema_id)
        if schema is None or schema.parent_id != namespace_id:
            raise NamespaceNotFoundError(namespace_id=schema_id)
        if schema.gone_since is not None:
            raise ValueError(gone_sentence(namespace.name, schema.gone_since))
        return namespace, row, schema

    def tables(self, namespace_id: int, schema_id: int, *, refresh: bool = False) -> ExternalTableList:
        """The schema's tables as the other catalog lists them, kept ``TABLE_LIST_SECONDS`` in this process."""
        from shared.iceberg_utils import list_table_names

        namespace, row, schema = self._schema(namespace_id, schema_id)
        key = (row.id, schema.name)
        kept = _TABLE_LISTS.get(key)
        cached = kept is not None and not refresh and time.monotonic() - kept[0] < TABLE_LIST_SECONDS
        if not cached:
            kept = (time.monotonic(), _now(), list_table_names(self._stored_ref(row), schema.name))
            _TABLE_LISTS[key] = kept
        return ExternalTableList(
            tables=[
                ExternalTableOut(name=name, reference=f"{namespace.name}.{schema.name}.{name}") for name in kept[2]
            ],
            listed_at=kept[1],
            cached=cached,
        )

    def table(self, namespace_id: int, schema_id: int, name: str) -> ExternalTableDetail:
        """One table as the other catalog describes it, asked each time: its columns and metadata, no row count."""
        from shared.iceberg_utils import describe_table

        namespace, row, schema = self._schema(namespace_id, schema_id)
        described = describe_table(self._stored_ref(row).table(f"{schema.name}.{name}"))
        updated = described.updated_at_ms
        return ExternalTableDetail(
            name=name,
            reference=f"{namespace.name}.{schema.name}.{name}",
            table=f"{schema.name}.{name}",
            columns=[ColumnSchema(name=column, dtype=str(dtype)) for column, dtype in described.columns.items()],
            format_version=described.format_version,
            location=described.location,
            partitioned_by=list(described.partitioned_by),
            updated_at=datetime.fromtimestamp(updated / 1000, UTC).replace(tzinfo=None) if updated else None,
        )


def table_ref_for_run(
    db: Session, catalog_name: str, table: str, user_id: int | None, *, writes: bool
) -> IcebergTableRef:
    """The table a flow names through a catalog kept elsewhere, as an ``IcebergTableRef`` holding only ciphertext.

    The catalog is found by name, the run's own catalog first, else the lowest id the user may reach. Allowed when
    the run is for nobody (an internal or scheduled run), sharing is off or the user is an administrator; otherwise
    Use on the table's schema (the catalog row when there is no such schema row) for a read, and a writable schema
    for a write. An unresolvable user is refused.
    """
    from flowfile_core.auth import sharing
    from flowfile_core.database.models import User

    rows = (
        db.query(ExternalCatalog)
        .join(CatalogNamespace, CatalogNamespace.id == ExternalCatalog.namespace_id)
        .filter(CatalogNamespace.name == catalog_name, CatalogNamespace.parent_id.is_(None))
        .order_by(ExternalCatalog.id)
        .all()
    )
    namespaces = {row.namespace_id: db.get(CatalogNamespace, row.namespace_id) for row in rows}
    own = [row for row in rows if user_id is not None and namespaces[row.namespace_id].owner_id == user_id]
    restricted = user_id is not None and sharing.sharing_enabled()
    user = db.get(User, user_id) if restricted else None
    if restricted and user is None:
        raise ValueError("The user this run is for could not be found; nothing is read or written.")
    if restricted and getattr(user, "is_admin", False):
        restricted = False
    schema_name = table.split(".", 1)[0]

    def target(row: ExternalCatalog) -> int:
        schema = db.query(CatalogNamespace).filter_by(parent_id=row.namespace_id, name=schema_name).first()
        return schema.id if schema is not None else row.namespace_id

    def reachable(row: ExternalCatalog) -> bool:
        return not restricted or sharing.can_use(db, user, "catalog_namespace", target(row))

    chosen = own[0] if own else next((row for row in rows if reachable(row)), None)
    if chosen is None:
        if rows:
            raise ValueError(f"You have no access to {catalog_name}.{schema_name}; ask its owner to share it.")
        raise ValueError(f"No catalog named {catalog_name!r} is kept elsewhere in Flowfile's catalog.")
    if restricted:
        from flowfile_core.catalog.access import AccessResolver

        allowed = (
            target(chosen) in AccessResolver(db, user).writable_namespace_ids()
            if writes
            else sharing.can_use(db, user, "catalog_namespace", target(chosen))
        )
        if not allowed:
            to_write = " to write it" if writes else ""
            raise ValueError(
                f"You have no access to {catalog_name}.{schema_name}; ask its owner to share it{to_write}."
            )
    secret = db.get(Secret, chosen.secret_id) if chosen.secret_id is not None else None
    return IcebergTableRef(chosen.uri, table, chosen.warehouse, secret.encrypted_value if secret is not None else None)
