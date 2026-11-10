"""A table in the hub's catalog as this Flowfile reads it, and what the hub's client answers ``table_sources`` for a
read outside a run.

A reader step names a table by its qualified name (``readable_named``). The Catalog page's preview and history, the
SQL editor, a visual and a notebook cell go through the catalog's own services, which take a table's row
(``table``), the tables a query may name (``tree``) and their storage targets (``read_targets``) from here and read a
local table on object storage with the same code. Each such read asks the hub for one credential for the tables it
names (``vend.request_for_tables``, keyed by table, not by a namespace's name) under a run id of its own
(``preview-…``, ``sql-…``, ``visual-…`` or ``notebook-…``, which the hub's vend ledger keeps).
"""

import uuid
from collections.abc import Sequence

from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.hub_client import catalog, vend
from flowfile_core.schemas.catalog_schema import CatalogTableOut, NamespaceTree


def readable(user_id: int | None, hub_table_id: int) -> dict:
    """The hub's table ``hub_table_id`` (a row of the catalog tree) as a reader takes it: a physical table on object
    storage.

    Raises:
        LookupError: The hub has no such table, does not answer, or this Flowfile is not connected to it.
        ValueError: The hub keeps the table on its own disk, or it is a virtual table.
    """
    table = catalog.table(user_id, hub_table_id)
    return _physical(table, lambda: catalog.locate(user_id, table["namespace_id"]).full_name)


def readable_named(user_id: int | None, qualified_name: str) -> dict:
    """The hub's table ``qualified_name`` (``catalog.schema.table``, as a reader step names it) as a reader takes it:
    a physical table on object storage.

    Raises:
        LookupError: The hub has no table of that name, does not answer, or this Flowfile is not connected to it.
        ValueError: The hub keeps the table on its own disk, or it is a virtual table.
    """
    table = catalog.table_named(user_id, qualified_name)
    return _physical(table, lambda: qualified_name.rpartition(".")[0])


def _physical(table: dict, namespace_name) -> dict:
    if table.get("table_type", "physical") != "physical":
        raise ValueError("A virtual table on the hub cannot be read from this Flowfile")
    if not table.get("file_path") or not _is_cloud_uri(table["file_path"]):
        raise ValueError(catalog.on_own_disk(namespace_name()))
    return table


def table(user_id: int | None, table_id: int) -> CatalogTableOut:
    """The hub's table whose row in the catalog tree is ``table_id``, as that row, for a read outside a run
    (``table_sources.table_row``); raises as ``readable`` does."""
    return CatalogTableOut.model_validate(catalog.marked(readable(user_id, -table_id)))


def read_targets(user_id: int, tables: Sequence[CatalogTableOut], purpose: str) -> list[vend.VendedTarget]:
    """The storage target of each of ``tables``, rows of the catalog tree, for one read outside a run
    (``table_sources.row_targets``): the Catalog page's preview and history, the SQL editor, a visual, a notebook cell.

    The hub vends one read of the tables' schemas to ``user_id`` now, keyed by table and not by a schema's name,
    under a run id of its own that starts with ``purpose`` (the hub's vend ledger keeps it). The credential lives
    for the call, or as long as the plan built from the target carries it, encrypted.

    Raises:
        LookupError: A sentence for the person: not connected to the hub, or the hub's refusal.
    """
    rows = [{"namespace_id": row.hub_namespace_id, "name": row.name} for row in tables]
    vended = vend.request_for_tables(user_id, f"{purpose}-{uuid.uuid4().hex}", rows)
    return [vend.VendedTarget(vended, row.file_path.rstrip("/").rpartition("/")[0], user_id) for row in tables]


def tree(user_id: int | None) -> list[NamespaceTree]:
    """The person's hub catalogs with their schemas and tables, read now (``table_sources.RemoteCatalog.tree``);
    empty without a hub session, or when the hub does not answer."""
    return catalog.tree(user_id) if user_id is not None else []
