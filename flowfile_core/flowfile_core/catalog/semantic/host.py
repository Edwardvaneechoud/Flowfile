"""The glosslayer ledger embedded in core: one per process, on core's own engine.

The ledger's tables live in the catalog database (migration 046). ``host()`` builds ``Gloss`` + ``FlowfileCatalog`` +
``Embedded`` lazily and rebuilds them when ``sharing_enabled()`` flips, because the review policy (solo on a
desktop, team review where sharing is on) is chosen from it and ``FLOWFILE_MODE`` is read live.

Dataset keys are a table's qualified ``catalog.schema.table`` name; namespace paths are ``catalog`` or
``catalog.schema``. Access is handed to glosslayer as path prefixes (``view_for``). A prefix covers everything under
it, so a catalog's path is only handed over when every schema under it is in the same set: otherwise reading or
deciding the catalog-level layer would also open the schemas the person may not see.

The catalog hooks run after the catalog change is committed (a glosslayer write while another session holds an
uncommitted SQLite write waits for the lock and fails) and never inside ``apply_transaction``, whose connection holds
the write lock until it ends; ``sync()`` reconciles whatever a hook missed, except renames, which a pull sync can only
see as drop + add, so a failed rename is logged as an error.

Every ``glosslayer`` / ``glosslayer_flowfile`` import is function-local, so core imports without them.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any

from flowfile_core.auth import sharing

logger = logging.getLogger(__name__)

CATALOG_NAME = "flowfile"
CATALOG_ACTOR = f"catalog:{CATALOG_NAME}"

_lock = threading.RLock()
_host: _Host | None = None
_unkeyable_logged: set[str] = set()


@dataclass
class _Host:
    gloss: Any
    catalog: Any
    embedded: Any
    sharing: bool


class _CatalogPort:
    """The slice of ``CatalogService`` the plugin calls, each call on a short session of its own.

    ``access`` is ``None`` so the plugin takes the listing as the whole local catalog. Tables whose key glosslayer
    cannot hold (a ``::`` in a name) are left out, since one of them would abort the whole sync.
    """

    access = None

    def list_tables(self) -> list[Any]:
        from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
        from flowfile_core.catalog.service import CatalogService
        from flowfile_core.database.connection import get_db_context

        with get_db_context() as db:
            tables = CatalogService(SQLAlchemyCatalogRepository(db)).list_tables()
        return [t for t in tables if _keyable(t.qualified_name)]

    def update_table(self, table_id: int, description: str | None = None) -> Any:
        from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
        from flowfile_core.catalog.service import CatalogService
        from flowfile_core.database.connection import get_db_context

        with get_db_context() as db:
            return CatalogService(SQLAlchemyCatalogRepository(db)).update_table(table_id, description=description)


def _keyable(key: str | None) -> bool:
    """Whether glosslayer can hold ``key`` as a dataset key; ``False`` (logged once) when it cannot."""
    try:
        from glosslayer.ids import SubjectId
    except ImportError:
        return False
    try:
        SubjectId.parse(f"dataset:{key}")
    except ValueError:
        if key not in _unkeyable_logged:
            _unkeyable_logged.add(key)
            logger.warning("Catalog table %r is left out of the semantic ledger: its name cannot be a key", key)
        return False
    return bool(key)


def _build(sharing_on: bool) -> _Host:
    from glosslayer import Gloss
    from glosslayer.store.sql import SQLStore
    from glosslayer_flowfile import Embedded, FlowfileCatalog, policy_for

    from flowfile_core.database.connection import engine

    gloss = Gloss(SQLStore(engine), policy=policy_for(sharing=sharing_on))
    catalog = FlowfileCatalog(service=_CatalogPort(), name=CATALOG_NAME, complete=True)
    return _Host(gloss=gloss, catalog=catalog, embedded=Embedded(gloss, catalog), sharing=sharing_on)


def host() -> _Host:
    """The process-wide ledger, built on first use and rebuilt when sharing is switched on or off."""
    global _host
    sharing_on = sharing.sharing_enabled()
    with _lock:
        if _host is None or _host.sharing != sharing_on:
            if _host is not None:
                _host.embedded.close()
            _host = _build(sharing_on)
        return _host


def reset() -> None:
    """Drop the process-wide ledger (tests, or after the database was swapped)."""
    global _host
    with _lock:
        if _host is not None:
            _host.embedded.close()
        _host = None
        _unkeyable_logged.clear()


def _namespace_rows(db) -> dict[int, tuple[str, int | None, int]]:
    from flowfile_core.database.models import CatalogNamespace

    rows = db.query(CatalogNamespace.id, CatalogNamespace.name, CatalogNamespace.parent_id, CatalogNamespace.level)
    return {ns_id: (name, parent_id, level) for ns_id, name, parent_id, level in rows}


def _path_of(ns_id: int, rows: dict[int, tuple[str, int | None, int]]) -> str | None:
    row = rows.get(ns_id)
    if row is None:
        return None
    name, parent_id, _level = row
    if parent_id is None:
        return name
    parent = _path_of(parent_id, rows)
    return f"{parent}.{name}" if parent else name


def namespace_path(namespace: Any, db=None) -> str:
    """A namespace's dotted path: ``catalog`` for a catalog, ``catalog.schema`` for a schema."""
    if namespace.parent_id is None:
        return namespace.name
    if db is None:
        from flowfile_core.database.connection import get_db_context

        with get_db_context() as session:
            return namespace_path(namespace, session)
    from flowfile_core.database.models import CatalogNamespace

    parent = db.get(CatalogNamespace, namespace.parent_id)
    return f"{namespace_path(parent, db)}.{namespace.name}" if parent is not None else namespace.name


def table_key(table: Any, db=None) -> str:
    """A table's dataset key: the plugin's ``table_key`` for a ``CatalogTableOut``, the qualified name for a row."""
    if getattr(table, "qualified_name", None):
        from glosslayer_flowfile import table_key as plugin_table_key

        return plugin_table_key(table)
    if table.namespace_id is None:
        return table.name
    if db is None:
        from flowfile_core.database.connection import get_db_context

        with get_db_context() as session:
            return table_key(table, session)
    from flowfile_core.database.models import CatalogNamespace

    ns = db.get(CatalogNamespace, table.namespace_id)
    return f"{namespace_path(ns, db)}.{table.name}" if ns is not None else table.name


def _closed_paths(ns_ids: set[int], rows: dict[int, tuple[str, int | None, int]]) -> list[str]:
    """Paths of ``ns_ids``; a catalog only when every schema under it is in the set too."""
    children: dict[int, list[int]] = {}
    for ns_id, (_name, parent_id, _level) in rows.items():
        if parent_id is not None:
            children.setdefault(parent_id, []).append(ns_id)
    paths = []
    for ns_id in sorted(ns_ids):
        if ns_id not in rows:
            continue
        if rows[ns_id][1] is None and not all(c in ns_ids for c in children.get(ns_id, ())):
            continue
        path = _path_of(ns_id, rows)
        if path:
            paths.append(path)
    return paths


def view_for(user: Any, access: Any, via: str | None = None) -> Any:
    """The ledger bound to ``user``, with their read and manage paths resolved from the catalog's own grants.

    ``read`` is ``None`` (everything) when ``access`` is absent or unrestricted, else the visible namespaces plus the
    keys of tables granted one by one. ``manage`` is the namespaces the user owns or holds a ``manage`` grant on;
    public namespaces are writable for everyone but managed by nobody, so they do not count.
    """
    embedded = host().embedded
    user_id = getattr(user, "id", None)
    restricted = access is not None and access.restricted
    if not restricted:
        return embedded.view(user_id=user_id, admin=True, manage=(), read=None, via=via)

    from flowfile_core.database.models import CatalogNamespace, CatalogTable

    db = access.db
    rows = _namespace_rows(db)
    visible = access.visible_namespace_ids()
    read = _closed_paths(visible, rows)
    covered = set(read)

    table_ids = access.accessible_ids("catalog_table")
    if table_ids:
        tables = db.query(CatalogTable.id, CatalogTable.name, CatalogTable.namespace_id).filter(
            CatalogTable.id.in_(table_ids)
        )
        for _tid, name, ns_id in tables:
            ns_path = _path_of(ns_id, rows) if ns_id is not None else None
            if ns_path is not None and ns_path in covered:
                continue
            read.append(f"{ns_path}.{name}" if ns_path else name)

    own = {r[0] for r in db.query(CatalogNamespace.id).filter(CatalogNamespace.owner_id == user_id)}
    granted = {ns_id for ns_id, perm in access.namespace_permissions().items() if perm == sharing.PERMISSION_MANAGE}
    manage = _closed_paths(own | granted, rows)

    return embedded.view(
        user_id=user_id, admin=bool(getattr(user, "is_admin", False)), manage=manage, read=read, via=via
    )


def sync() -> Any:
    """Pull sync of the whole catalog into the ledger; returns glosslayer's ``SyncReport``."""
    with _lock:
        h = host()
        h.catalog.tables(refresh=True)
        return h.embedded.sync()


def _in_apply_transaction() -> bool:
    from flowfile_core.database.connection import _apply_connection

    return _apply_connection.get() is not None


def on_table_written(key: str) -> None:
    """After a table was created or overwritten: re-read it so new and dropped columns reach the ledger."""
    if _in_apply_transaction() or not _keyable(key):
        return
    try:
        with _lock:
            h = host()
            h.catalog.tables(refresh=True)
            h.embedded.table_written(key)
    except Exception:
        logger.warning("Semantic ledger: could not record the write of %s", key, exc_info=True)


def on_table_renamed(old_key: str, new_key: str) -> None:
    """After a table was renamed or moved: its facts follow it. A lost rename cannot be recovered by a sync."""
    if old_key == new_key:
        return
    if _in_apply_transaction():
        logger.error("Semantic ledger: rename %s -> %s made inside a project apply is not recorded", old_key, new_key)
        return
    if not _keyable(new_key):
        logger.error("Semantic ledger: %s was renamed to %s, which cannot be a key; its facts stay", old_key, new_key)
        return
    try:
        with _lock:
            host().embedded.table_renamed(old_key, new_key)
    except Exception:
        logger.error("Semantic ledger: could not move the facts of %s to %s", old_key, new_key, exc_info=True)


def on_namespace_renamed(old_path: str, new_path: str, table_keys: list[tuple[str, str]]) -> None:
    """After a catalog or schema was renamed: everything under its path follows, then each table's record."""
    if old_path == new_path:
        return
    if _in_apply_transaction():
        logger.error("Semantic ledger: rename %s -> %s made inside a project apply is not recorded", old_path, new_path)
        return
    try:
        with _lock:
            h = host()
            try:
                h.gloss.rekey_namespace(old_path, new_path, actor=CATALOG_ACTOR, comment="renamed in the catalog")
            except KeyError:
                pass  # nothing was recorded under the old path
            for old_key, new_key in table_keys:
                if _keyable(new_key):
                    h.embedded.table_renamed(old_key, new_key)
    except Exception:
        logger.error("Semantic ledger: could not move %s to %s", old_path, new_path, exc_info=True)


def on_table_deleted(key: str) -> None:
    """After a table was deleted: its facts are marked stale (kept, never removed)."""
    if _in_apply_transaction() or not _keyable(key):
        return
    try:
        with _lock:
            h = host()
            h.catalog.tables(refresh=True)
            h.embedded.table_deleted(key)
    except Exception:
        logger.warning("Semantic ledger: could not retire the facts of %s", key, exc_info=True)
