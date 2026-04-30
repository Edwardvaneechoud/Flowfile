"""Typed reference objects for the Flowfile catalog hierarchy.

The catalog backend is a 2-level Unity-style hierarchy: ``catalog`` (level 0)
contains ``schema`` (level 1) namespaces, and tables live under schemas.
Identifying a namespace requires the database autoincrement ``id``, which is
not user-friendly: developers know catalog and schema names, not opaque IDs.

This module exposes :class:`CatalogReference` and :class:`SchemaReference` as
validated, hashable handles. Construction resolves the name against the
backend (with optional auto-create) and stores the resolved ``id`` once. Pass
the reference to :func:`flowfile_frame.read_catalog_table` (and friends) via
``schema=...`` instead of remembering namespace IDs.

Example
-------
>>> import flowfile_frame as ff
>>> catalog = ff.CatalogReference("sales", auto_create=True)
>>> schema = catalog.schema("raw", auto_create=True)
>>> schema.write_table(df, "orders", write_mode="overwrite")
>>> schema.read_table("orders")  # FlowFrame
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypeAlias

from flowfile_core.catalog import (
    CatalogService,
    NamespaceExistsError,
    SQLAlchemyCatalogRepository,
)
from flowfile_core.database.connection import get_db_context

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from flowfile_core.database.models import CatalogNamespace
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.schemas.catalog_schema import CatalogTableOut
    from flowfile_frame.flow_frame import FlowFrame

WriteMode: TypeAlias = Literal["overwrite", "error", "append", "upsert", "update", "delete"]


def _get_current_user_id() -> int:
    """User id used for ownership in single-user mode (matches catalog.py)."""
    return 1


def _get_service(db: Session) -> CatalogService:
    return CatalogService(SQLAlchemyCatalogRepository(db))


def _reject_dot(name: str, kind: str) -> None:
    if "." in name:
        raise ValueError(f"{kind} name '{name}' cannot contain '.'")


class CatalogReference:
    """Validated handle to a level-0 catalog namespace.

    Constructing a ``CatalogReference`` performs a database lookup: the
    catalog must already exist, or ``auto_create=True`` must be passed. The
    resolved ``id`` is captured at construction time; later operations on the
    reference do not re-validate. If the underlying namespace is deleted
    after construction, subsequent calls (e.g. :meth:`list_schemas`) will
    raise :class:`flowfile_core.catalog.NamespaceNotFoundError`.

    Attributes
    ----------
    name:
        Catalog name as stored in the backend.
    id:
        Database-internal autoincrement id. Stable within a single deployment
        but not portable across environments. Pass via ``schema=`` keyword to
        functions like :func:`flowfile_frame.read_catalog_table` instead of
        using this directly.

    Parameters
    ----------
    name:
        Catalog name. Must not contain ``.`` (reserved for fully-qualified
        references).
    auto_create:
        When ``True``, create the catalog if it does not exist. When
        ``False`` (default), raise
        :class:`flowfile_core.catalog.NamespaceNotFoundError` if missing.
    description:
        Optional description, applied only when the catalog is created.

    Raises
    ------
    flowfile_core.catalog.NamespaceNotFoundError
        Catalog does not exist and ``auto_create`` is ``False``.
    """

    __slots__ = ("name", "id")

    name: str
    id: int

    def __init__(
        self,
        name: str,
        *,
        auto_create: bool = False,
        description: str | None = None,
    ) -> None:
        _reject_dot(name, "Catalog")
        ns = _resolve_catalog(name, auto_create=auto_create, description=description)
        object.__setattr__(self, "name", ns.name)
        object.__setattr__(self, "id", ns.id)

    @classmethod
    def _from_namespace(cls, ns: CatalogNamespace) -> CatalogReference:
        instance = object.__new__(cls)
        object.__setattr__(instance, "name", ns.name)
        object.__setattr__(instance, "id", ns.id)
        return instance

    def __setattr__(self, key: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, key: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __repr__(self) -> str:
        return f"CatalogReference(name={self.name!r}, id={self.id})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CatalogReference):
            return NotImplemented
        return self.id == other.id and self.name == other.name

    def __hash__(self) -> int:
        return hash(("CatalogReference", self.id))

    def __getstate__(self) -> tuple[str, int]:
        return (self.name, self.id)

    def __setstate__(self, state: tuple[str, int]) -> None:
        object.__setattr__(self, "name", state[0])
        object.__setattr__(self, "id", state[1])

    def schema(
        self,
        name: str,
        *,
        auto_create: bool = False,
        description: str | None = None,
    ) -> SchemaReference:
        """Return a :class:`SchemaReference` for a child schema of this catalog.

        Parameters
        ----------
        name:
            Schema name. Must not contain ``.``.
        auto_create:
            When ``True``, create the schema if it does not exist.
        description:
            Optional description, applied only when the schema is created.
        """
        return SchemaReference(self, name, auto_create=auto_create, description=description)

    def list_schemas(self) -> list[SchemaReference]:
        """Return all schemas (level-1 namespaces) under this catalog."""
        with get_db_context() as db:
            namespaces = _get_service(db).list_namespaces(parent_id=self.id)
            return [SchemaReference._from_namespace(self, ns) for ns in namespaces]

    def list_tables(self) -> list[CatalogTableOut]:
        """Return tables across every schema in this catalog (flat list).

        Each row's ``namespace_id`` identifies which schema it belongs to.
        Use :meth:`SchemaReference.list_tables` for a single-schema view.
        """
        with get_db_context() as db:
            service = _get_service(db)
            schemas = service.list_namespaces(parent_id=self.id)
            user_id = _get_current_user_id()
            tables: list[CatalogTableOut] = []
            for schema in schemas:
                tables.extend(service.list_tables(namespace_id=schema.id, user_id=user_id))
            return tables


class SchemaReference:
    """Validated handle to a level-1 schema namespace.

    Constructing a ``SchemaReference`` performs a database lookup against the
    parent catalog: the schema must already exist under that catalog, or
    ``auto_create=True`` must be passed. The resolved ``id`` is captured
    once; later operations do not re-validate.

    Attributes
    ----------
    catalog:
        The parent :class:`CatalogReference`.
    name:
        Schema name as stored in the backend.
    id:
        Database-internal autoincrement id. Equivalent to the legacy
        ``namespace_id`` parameter.

    Parameters
    ----------
    catalog:
        Parent catalog reference. The schema is created or looked up under
        this catalog's ``id``.
    name:
        Schema name. Must not contain ``.``.
    auto_create:
        When ``True``, create the schema if it does not exist.
    description:
        Optional description, applied only when the schema is created.

    Raises
    ------
    flowfile_core.catalog.NamespaceNotFoundError
        Schema does not exist under the catalog and ``auto_create`` is
        ``False``.
    """

    __slots__ = ("catalog", "name", "id")

    catalog: CatalogReference
    name: str
    id: int

    def __init__(
        self,
        catalog: CatalogReference,
        name: str,
        *,
        auto_create: bool = False,
        description: str | None = None,
    ) -> None:
        _reject_dot(name, "Schema")
        ns = _resolve_schema(catalog, name, auto_create=auto_create, description=description)
        object.__setattr__(self, "catalog", catalog)
        object.__setattr__(self, "name", ns.name)
        object.__setattr__(self, "id", ns.id)

    @classmethod
    def _from_namespace(cls, catalog: CatalogReference, ns: CatalogNamespace) -> SchemaReference:
        instance = object.__new__(cls)
        object.__setattr__(instance, "catalog", catalog)
        object.__setattr__(instance, "name", ns.name)
        object.__setattr__(instance, "id", ns.id)
        return instance

    def __setattr__(self, key: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, key: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __repr__(self) -> str:
        return f"SchemaReference(catalog={self.catalog.name!r}, name={self.name!r}, id={self.id})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SchemaReference):
            return NotImplemented
        return self.id == other.id and self.catalog == other.catalog

    def __hash__(self) -> int:
        return hash(("SchemaReference", self.id))

    def __getstate__(self) -> tuple[CatalogReference, str, int]:
        return (self.catalog, self.name, self.id)

    def __setstate__(self, state: tuple[CatalogReference, str, int]) -> None:
        object.__setattr__(self, "catalog", state[0])
        object.__setattr__(self, "name", state[1])
        object.__setattr__(self, "id", state[2])

    def list_tables(self) -> list[CatalogTableOut]:
        """Return tables registered in this schema."""
        with get_db_context() as db:
            return _get_service(db).list_tables(
                namespace_id=self.id,
                user_id=_get_current_user_id(),
            )

    def read_table(
        self,
        name: str,
        *,
        delta_version: int | None = None,
        flow_graph: FlowGraph | None = None,
    ) -> FlowFrame:
        """Read a table from this schema as a :class:`FlowFrame`.

        Equivalent to
        ``flowfile_frame.read_catalog_table(name, schema=self, ...)``.
        """
        from flowfile_frame.catalog import read_catalog_table

        return read_catalog_table(
            name,
            schema=self,
            delta_version=delta_version,
            flow_graph=flow_graph,
        )

    def write_table(
        self,
        df: FlowFrame,
        name: str,
        *,
        write_mode: WriteMode = "overwrite",
        merge_keys: list[str] | None = None,
        description: str | None = None,
    ) -> None:
        """Write a :class:`FlowFrame` to a table in this schema."""
        df.write_catalog_table(
            table_name=name,
            schema=self,
            write_mode=write_mode,
            merge_keys=merge_keys,
            description=description,
        )


def list_catalogs() -> list[CatalogReference]:
    """Return every catalog (level-0 namespace) in the backend."""
    with get_db_context() as db:
        namespaces = _get_service(db).list_namespaces(parent_id=None)
        return [CatalogReference._from_namespace(ns) for ns in namespaces]


def default_schema() -> SchemaReference:
    """Return the seeded ``General/default`` schema reference.

    Raises
    ------
    LookupError
        If the default schema has not been initialized.
    """
    with get_db_context() as db:
        service = _get_service(db)
        default_id = service.get_default_namespace_id()
        if default_id is None:
            raise LookupError(
                "Default schema 'General/default' is not initialized. "
                "Create it by running the catalog seeding routine, or pass an explicit "
                "CatalogReference / SchemaReference."
            )
        schema_ns = service.get_namespace(default_id)
        if schema_ns.parent_id is None:
            raise LookupError("Default namespace has no parent catalog (data integrity issue).")
        catalog_ns = service.get_namespace(schema_ns.parent_id)
        catalog = CatalogReference._from_namespace(catalog_ns)
        return SchemaReference._from_namespace(catalog, schema_ns)


def _resolve_namespace_id(
    schema: SchemaReference | None,
    namespace_id: int | None,
) -> int | None:
    """Resolve a ``namespace_id`` from either ``schema=`` or the legacy kwarg.

    Pass either ``schema`` (a :class:`SchemaReference`) or ``namespace_id``
    (the raw integer), not both. Returns ``None`` when neither is given.
    """
    if schema is not None and namespace_id is not None:
        raise ValueError("Pass either schema= or namespace_id=, not both")
    if schema is not None:
        return schema.id
    return namespace_id


def _resolve_catalog(
    name: str,
    *,
    auto_create: bool,
    description: str | None,
) -> CatalogNamespace:
    from flowfile_core.catalog import NamespaceNotFoundError

    with get_db_context() as db:
        service = _get_service(db)
        repo = service.repo
        existing = repo.get_namespace_by_name(name, parent_id=None)
        if existing is not None:
            return existing
        if not auto_create:
            raise NamespaceNotFoundError(name=name)
        try:
            return service.create_namespace(
                name=name,
                owner_id=_get_current_user_id(),
                parent_id=None,
                description=description,
            )
        except NamespaceExistsError:
            existing = repo.get_namespace_by_name(name, parent_id=None)
            if existing is None:
                raise
            return existing


def _resolve_schema(
    catalog: CatalogReference,
    name: str,
    *,
    auto_create: bool,
    description: str | None,
) -> CatalogNamespace:
    from flowfile_core.catalog import NamespaceNotFoundError

    with get_db_context() as db:
        service = _get_service(db)
        repo = service.repo
        existing = repo.get_namespace_by_name(name, parent_id=catalog.id)
        if existing is not None:
            return existing
        if not auto_create:
            raise NamespaceNotFoundError(name=f"{catalog.name}.{name}")
        try:
            return service.create_namespace(
                name=name,
                owner_id=_get_current_user_id(),
                parent_id=catalog.id,
                description=description,
            )
        except NamespaceExistsError:
            existing = repo.get_namespace_by_name(name, parent_id=catalog.id)
            if existing is None:
                raise
            return existing
