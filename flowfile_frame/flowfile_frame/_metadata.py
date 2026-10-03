"""The frame's catalog metadata reads, and how a notebook kernel session answers them through core.

Every public function here reads catalog metadata: namespaces, tables, flow registrations, the user's saved
kernels, connections without their secrets. Outside a kernel session it opens the catalog database, as the
frame always did. Inside one, ``flowfile_core.notebook.lookup.metadata_lookup`` is set for the op
(:func:`installed`) and the function asks core instead (:class:`SessionLookup`), so the kernel opens no
database connection and no secret, not even a ciphertext, reaches it. Core answers a kind by calling the
``local_*`` twin of the same function, so both paths compute the same thing.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Callable, Iterator
from typing import Any, NamedTuple

from pydantic import BaseModel

from flowfile_core.notebook.lookup import metadata_lookup
from flowfile_core.schemas.catalog_schema import CatalogTableOut
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnectionInterface
from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_frame._identity import current_user_id

_NEVER_ANSWERED: frozenset[str] = frozenset({"password"})


class Namespace(NamedTuple):
    """A catalog namespace row: a catalog (``parent_id`` ``None``) or a schema."""

    id: int
    name: str
    parent_id: int | None

    @classmethod
    def from_row(cls, row: Any) -> Namespace:
        return cls(row.id, row.name, row.parent_id)


class Registration(NamedTuple):
    """A flow registration row; ``usable`` is whether the user may use the flow (``sharing``)."""

    id: int
    flow_uuid: str | None
    flow_path: str
    name: str
    namespace_id: int | None
    usable: bool


class SavedKernel(NamedTuple):
    """A saved kernel definition, as ``ff.kernels`` lists it."""

    id: str
    name: str
    flavour: str
    packages: list[str]


class SessionLookup:
    """The ``metadata_lookup`` a notebook kernel session installs for one op: every lookup goes to core.

    Answers are memoised for the op by kind and arguments (a cell that places the same table twice asks
    once; the next op asks again, as the catalog may have changed). ``cloud_tables`` holds the node id of
    every ``catalog_table`` answer whose data lives in cloud storage (:func:`is_cloud_table`): the kernel
    holds no credentials, so notebook mode defers such a reader and core runs it.
    """

    def __init__(self, flow_id: int, transport: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
        self.flow_id = flow_id
        self.transport = transport
        self.answers: dict[tuple[str, str], Any] = {}
        self.cloud_tables: set[int] = set()

    def __call__(self, kind: str, args: dict[str, Any]) -> Any:
        key = (kind, json.dumps(args, sort_keys=True, default=str))
        if key not in self.answers:
            self.answers[key] = self.transport({"flow_id": self.flow_id, "kind": kind, "args": args})["result"]
            if kind == "catalog_table":
                from flowfile_core.catalog.storage_backend import _is_cloud_uri

                if _is_cloud_uri(self.answers[key].get("file_path") or ""):
                    self.cloud_tables.add(int(args["node_id"]))
        return self.answers[key]


@contextlib.contextmanager
def installed(flow_id: int, transport: Callable[[dict[str, Any]], dict[str, Any]]) -> Iterator[SessionLookup]:
    """Answer every metadata lookup in this context through ``transport`` while the block runs."""
    hook = SessionLookup(flow_id, transport)
    token = metadata_lookup.set(hook)
    try:
        yield hook
    finally:
        metadata_lookup.reset(token)


def is_cloud_table(node_id: int) -> bool:
    """Whether core resolved catalog reader ``node_id``'s table to cloud storage in this op (a kernel session)."""
    hook = metadata_lookup.get()
    return isinstance(hook, SessionLookup) and node_id in hook.cloud_tables


def dumped(value: Any) -> Any:
    """``value`` as JSON: rows and models become dicts (never a password), lists item by item."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude=set(_NEVER_ANSWERED))
    if isinstance(value, tuple) and hasattr(value, "_asdict"):
        return value._asdict()
    if isinstance(value, list):
        return [dumped(item) for item in value]
    return value


def _ask(kind: str, **args: Any) -> tuple[bool, Any]:
    hook = metadata_lookup.get()
    if hook is None:
        return False, None
    return True, hook(kind, args)


def _service(db):
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository

    return CatalogService(SQLAlchemyCatalogRepository(db))


def namespaces(parent_id: int | None) -> list[Namespace]:
    """The namespaces under ``parent_id``: the catalogs for ``None``, else a catalog's schemas."""
    asked, answer = _ask("namespaces", parent_id=parent_id)
    if asked:
        return [Namespace(**row) for row in answer]
    return local_namespaces(parent_id, user_id=current_user_id())


def local_namespaces(parent_id: int | None, *, user_id: int) -> list[Namespace]:
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return [Namespace.from_row(row) for row in _service(db).list_namespaces(parent_id=parent_id)]


def namespace(namespace_id: int) -> Namespace | None:
    asked, answer = _ask("namespace", namespace_id=namespace_id)
    if asked:
        return None if answer is None else Namespace(**answer)
    return local_namespace(namespace_id, user_id=current_user_id())


def local_namespace(namespace_id: int, *, user_id: int) -> Namespace | None:
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        row = _service(db).repo.get_namespace(namespace_id)
        return None if row is None else Namespace.from_row(row)


def namespace_by_name(name: str, parent_id: int | None) -> Namespace | None:
    asked, answer = _ask("namespace_by_name", name=name, parent_id=parent_id)
    if asked:
        return None if answer is None else Namespace(**answer)
    return local_namespace_by_name(name, parent_id, user_id=current_user_id())


def local_namespace_by_name(name: str, parent_id: int | None, *, user_id: int) -> Namespace | None:
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        row = _service(db).repo.get_namespace_by_name(name, parent_id=parent_id)
        return None if row is None else Namespace.from_row(row)


def default_namespace_id() -> int | None:
    """The id of the seeded ``General/default`` schema, or ``None``."""
    asked, answer = _ask("default_namespace_id")
    if asked:
        return answer
    return local_default_namespace_id(user_id=current_user_id())


def local_default_namespace_id(*, user_id: int) -> int | None:
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return _service(db).get_default_namespace_id()


def namespace_full_name(namespace_id: int | None) -> str | None:
    """The ``"catalog.schema"`` name of ``namespace_id``, or ``None`` when unset or gone."""
    asked, answer = _ask("namespace_full_name", namespace_id=namespace_id)
    if asked:
        return answer
    return local_namespace_full_name(namespace_id, user_id=current_user_id())


def local_namespace_full_name(namespace_id: int | None, *, user_id: int) -> str | None:
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return _service(db).resolve_namespace_full_name(namespace_id)


def namespace_id_by_full_name(full_name: str) -> int | None:
    asked, answer = _ask("namespace_id_by_full_name", full_name=full_name)
    if asked:
        return answer
    return local_namespace_id_by_full_name(full_name, user_id=current_user_id())


def local_namespace_id_by_full_name(full_name: str, *, user_id: int) -> int | None:
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return _service(db).resolve_namespace_id_by_full_name(full_name)


def tables(namespace_id: int) -> list[CatalogTableOut]:
    """The tables registered in a schema, as the catalog lists them for the user."""
    asked, answer = _ask("tables", namespace_id=namespace_id)
    if asked:
        return [CatalogTableOut.model_validate(row) for row in answer]
    return local_tables(namespace_id, user_id=current_user_id())


def local_tables(namespace_id: int, *, user_id: int) -> list[CatalogTableOut]:
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return _service(db).list_tables(namespace_id=namespace_id, user_id=user_id)


def flow_registrations(
    namespace_id: int | None = None,
    name: str | None = None,
    flow_uuid: str | None = None,
    registration_id: int | None = None,
) -> list[Registration]:
    """Flow registrations: the one with ``flow_uuid`` or ``registration_id``, else those called ``name`` in
    ``namespace_id``, else every one in ``namespace_id``; each marked ``usable`` for the user."""
    asked, answer = _ask(
        "flow_registrations",
        namespace_id=namespace_id,
        name=name,
        flow_uuid=flow_uuid,
        registration_id=registration_id,
    )
    if asked:
        return [Registration(**row) for row in answer]
    return local_flow_registrations(namespace_id, name, flow_uuid, registration_id, user_id=current_user_id())


def local_flow_registrations(
    namespace_id: int | None = None,
    name: str | None = None,
    flow_uuid: str | None = None,
    registration_id: int | None = None,
    *,
    user_id: int,
) -> list[Registration]:
    from flowfile_core.auth import sharing
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        repo = _service(db).repo
        if flow_uuid is not None:
            rows = [repo.get_flow_by_uuid(flow_uuid)]
        elif registration_id is not None:
            rows = [repo.get_flow(registration_id)]
        elif name is not None:
            rows = repo.list_flows_by_name(name, namespace_id)
        else:
            rows = repo.list_flows(namespace_id=namespace_id)
        return [
            Registration(
                row.id,
                row.flow_uuid,
                row.flow_path,
                row.name,
                row.namespace_id,
                sharing.user_id_can_use(db, user_id, "flow", row.id),
            )
            for row in rows
            if row is not None
        ]


def kernels() -> list[SavedKernel]:
    """The user's saved kernel definitions; no container state, so Docker need not run."""
    asked, answer = _ask("kernels")
    if asked:
        return [SavedKernel(**row) for row in answer]
    return local_kernels(user_id=current_user_id())


def local_kernels(*, user_id: int) -> list[SavedKernel]:
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.kernel import persistence

    with get_db_context() as db:
        configs = persistence.get_kernels_for_user(db, user_id)
    return [SavedKernel(c.id, c.name, c.image_flavour.value, list(c.packages)) for c in configs]


def cloud_connections() -> list[FullCloudStorageConnectionInterface]:
    """The user's cloud storage connections, public fields only."""
    asked, answer = _ask("cloud_connections")
    if asked:
        return [FullCloudStorageConnectionInterface.model_validate(row) for row in answer]
    return local_cloud_connections(user_id=current_user_id())


def local_cloud_connections(*, user_id: int) -> list[FullCloudStorageConnectionInterface]:
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.flowfile.database_connection_manager.db_connections import get_all_cloud_connections_interface

    with get_db_context() as db:
        return get_all_cloud_connections_interface(db, user_id)


def database_connection(name: str) -> FullDatabaseConnection | None:
    """The user's database connection ``name``; in a kernel session its ``password`` is empty (no secret
    reaches the kernel), in a script it is the stored ciphertext."""
    asked, answer = _ask("database_connection", name=name)
    if asked:
        return None if answer is None else FullDatabaseConnection.model_validate({**answer, "password": ""})
    return local_database_connection(name, user_id=current_user_id())


def local_database_connection(name: str, *, user_id: int) -> FullDatabaseConnection | None:
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.flowfile.database_connection_manager.db_connections import get_database_connection_schema

    with get_db_context() as db:
        return get_database_connection_schema(db, name, user_id)


def database_connections() -> list[FullDatabaseConnectionInterface]:
    """The user's own database connections, without passwords."""
    asked, answer = _ask("database_connections")
    if asked:
        return [FullDatabaseConnectionInterface.model_validate(row) for row in answer]
    return local_database_connections(user_id=current_user_id())


def local_database_connections(*, user_id: int) -> list[FullDatabaseConnectionInterface]:
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import DatabaseConnection

    with get_db_context() as db:
        rows = db.query(DatabaseConnection).filter(DatabaseConnection.user_id == user_id).all()
        return [
            FullDatabaseConnectionInterface(
                connection_name=row.connection_name,
                database_type=row.database_type,
                username=row.username,
                host=row.host,
                port=row.port,
                database=row.database,
                ssl_enabled=row.ssl_enabled,
            )
            for row in rows
        ]
