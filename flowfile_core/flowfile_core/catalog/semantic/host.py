"""The glosslayer ledger embedded in core: one per process, on core's own engine (migration 046's tables).

Dataset keys are a table's qualified ``catalog.schema.table`` name; namespace paths are ``catalog`` or
``catalog.schema``. Access is handed to glosslayer as path prefixes (``view_for``), and a prefix covers everything under
it, so a catalog's path is only handed over when every schema under it is in the same set.

The catalog hooks run after the catalog change is committed (a glosslayer write while another session holds an
uncommitted SQLite write waits for the lock and fails) and never inside ``apply_transaction``; ``sync()`` reconciles
whatever a hook missed, except renames, which a pull sync can only see as drop + add, so a failed rename is an error.

Every ``glosslayer`` / ``glosslayer_flowfile`` import is function-local, so core imports without them.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import Any

from flowfile_core.auth import sharing

logger = logging.getLogger(__name__)

CATALOG_NAME = "flowfile"
CATALOG_ACTOR = f"catalog:{CATALOG_NAME}"

_lock = threading.RLock()
_host: _Host | None = None
_unkeyable_logged: set[str] = set()
_missing_logged = False


@dataclass
class _Host:
    gloss: Any
    catalog: Any
    embedded: Any
    sharing: bool


class _CatalogPort:
    """The slice of ``CatalogService`` the plugin calls, each call on a short session of its own.

    Tables whose key glosslayer cannot hold (a ``::`` in a name) are left out, since one would abort the whole sync.
    """

    access = None

    @staticmethod
    @contextmanager
    def _service() -> Iterator[Any]:
        from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
        from flowfile_core.catalog.service import CatalogService
        from flowfile_core.database.connection import get_db_context

        with get_db_context() as db:
            yield CatalogService(SQLAlchemyCatalogRepository(db))

    def list_tables(self) -> list[Any]:
        with self._service() as service:
            return [t for t in service.list_tables() if is_keyable(t.qualified_name)]

    def update_table(self, table_id: int, description: str | None = None) -> Any:
        with self._service() as service:
            return service.update_table(table_id, description=description)


def is_keyable(key: str | None) -> bool:
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


def namespace_path(namespace: Any, db: Any) -> str:
    """A namespace's dotted path: ``catalog`` for a catalog, ``catalog.schema`` for a schema."""
    from flowfile_core.database.models import CatalogNamespace

    parent = db.get(CatalogNamespace, namespace.parent_id) if namespace.parent_id is not None else None
    return f"{namespace_path(parent, db)}.{namespace.name}" if parent is not None else namespace.name


def table_key(table: Any, db: Any) -> str:
    """A ``CatalogTable`` row's dataset key (its qualified name)."""
    from flowfile_core.database.models import CatalogNamespace

    ns = db.get(CatalogNamespace, table.namespace_id) if table.namespace_id is not None else None
    return f"{namespace_path(ns, db)}.{table.name}" if ns is not None else table.name


def view_for(user: Any, access: Any, via: str | None = None) -> Any:
    """The ledger bound to ``user``, with their read and manage paths resolved from the catalog's own grants.

    ``read`` is ``None`` (everything) when ``access`` is absent or unrestricted, else the visible namespaces plus the
    keys of tables granted one by one. ``manage`` is the namespaces the user owns or holds a ``manage`` grant on;
    public namespaces are writable for everyone but managed by nobody, so they do not count.
    """
    embedded = host().embedded
    user_id = getattr(user, "id", None)
    if access is None or not access.restricted:
        return embedded.view(user_id=user_id, admin=True, manage=(), read=None, via=via)

    from flowfile_core.database.models import CatalogNamespace, CatalogTable

    db = access.db
    namespaces = {ns.id: ns for ns in db.query(CatalogNamespace)}

    def closed_paths(ns_ids: set[int]) -> list[str]:
        """Paths of ``ns_ids``; a catalog only when every schema under it is in the set too."""
        open_catalogs = {ns.parent_id for ns in namespaces.values() if ns.id not in ns_ids}
        kept = sorted((ns_ids & namespaces.keys()) - open_catalogs)
        return [namespace_path(namespaces[i], db) for i in kept]

    read = closed_paths(access.visible_namespace_ids())
    covered = set(read)
    table_ids = access.accessible_ids("catalog_table")
    if table_ids:
        for table in db.query(CatalogTable).filter(CatalogTable.id.in_(table_ids)):
            key = table_key(table, db)
            if key.rpartition(".")[0] not in covered:
                read.append(key)

    own = {ns.id for ns in namespaces.values() if ns.owner_id == user_id}
    granted = {ns_id for ns_id, perm in access.namespace_permissions().items() if perm == sharing.PERMISSION_MANAGE}
    return embedded.view(
        user_id=user_id,
        admin=bool(getattr(user, "is_admin", False)),
        manage=closed_paths(own | granted),
        read=read,
        via=via,
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


def _record(level: int, what: str, args: tuple, change: Callable[[_Host], None]) -> None:
    """Run ``change`` on the ledger under the lock; a failure is logged, never raised into the catalog change.

    An install without the ledger's packages records nothing: that is said once, not on every catalog change.
    """
    global _missing_logged
    try:
        with _lock:
            change(host())
    except ImportError as exc:
        if not _missing_logged:
            _missing_logged = True
            logger.warning("Semantic ledger: not installed (%s); catalog changes are not recorded", exc)
    except Exception:
        logger.log(level, f"Semantic ledger: could not {what}", *args, exc_info=True)


def on_table_written(key: str) -> None:
    """After a table was created or overwritten: re-read it so new and dropped columns reach the ledger."""
    if _in_apply_transaction() or not is_keyable(key):
        return

    def change(h: _Host) -> None:
        h.catalog.tables(refresh=True)
        h.embedded.table_written(key)

    _record(logging.WARNING, "record the write of %s", (key,), change)


def _rename_is_lost(old: str, new: str) -> bool:
    if _in_apply_transaction():
        logger.error("Semantic ledger: rename %s -> %s made inside a project apply is not recorded", old, new)
        return True
    return False


def on_table_renamed(old_key: str, new_key: str) -> None:
    """After a table was renamed or moved: its facts follow it. A lost rename cannot be recovered by a sync."""
    if old_key == new_key or _rename_is_lost(old_key, new_key):
        return
    if not is_keyable(new_key):
        logger.error("Semantic ledger: %s was renamed to %s, which cannot be a key; its facts stay", old_key, new_key)
        return

    def change(h: _Host) -> None:
        h.embedded.table_renamed(old_key, new_key)

    _record(logging.ERROR, "move the facts of %s to %s", (old_key, new_key), change)


def on_namespace_renamed(old_path: str, new_path: str, table_keys: list[tuple[str, str]]) -> None:
    """After a catalog or schema was renamed: everything under its path follows, then each table's record."""
    if old_path == new_path or _rename_is_lost(old_path, new_path):
        return

    def change(h: _Host) -> None:
        with suppress(KeyError):  # nothing was recorded under the old path
            h.gloss.rekey_namespace(old_path, new_path, actor=CATALOG_ACTOR, comment="renamed in the catalog")
        for old_key, new_key in table_keys:
            if is_keyable(new_key):
                h.embedded.table_renamed(old_key, new_key)

    _record(logging.ERROR, "move %s to %s", (old_path, new_path), change)


def on_table_deleted(key: str) -> None:
    """After a table was deleted: its facts are marked stale (kept, never removed)."""
    if _in_apply_transaction() or not is_keyable(key):
        return

    def change(h: _Host) -> None:
        h.catalog.tables(refresh=True)
        h.embedded.table_deleted(key)

    _record(logging.WARNING, "retire the facts of %s", (key,), change)
