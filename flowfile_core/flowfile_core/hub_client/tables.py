"""A table in the hub's catalog as this Flowfile reads it: the table itself, its first rows for the Catalog page, a
query of the SQL editor over it, a visual on such a query and a notebook cell's read of it.

A reader step names a table by its qualified name (``readable_named``); the Catalog page's preview, the SQL editor,
a visual and a notebook cell name one by the hub's id on its row of the catalog tree (``readable``), and all take it
the same way. Each of the last four asks the hub for one read of the tables it names (``vend.request_for_tables``,
keyed by table, not by a namespace's name) under a run id of its own
(``preview-…``, ``sql-…``, ``visual-…`` or ``notebook-…``, which the hub's vend ledger keeps), scans each table with
that credential as a run's reader does (``RunVend.scan_delta``) and reads the rows on this machine's own worker. The
credential lives for the call; a visual's plan carries it, encrypted, for a few minutes (``visual_source``), and a
notebook cell reads a file the worker wrote (``snapshot``).
"""

import hashlib
import time
import uuid
from collections.abc import Sequence

from requests.exceptions import RequestException

from flowfile_core.catalog.exceptions import WorkerUnavailableError
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.catalog.text_utils import plain_reference
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_resolve_virtual_table,
)
from flowfile_core.hub_client import catalog, vend
from flowfile_core.schemas.catalog_schema import (
    CatalogTableMaterializeResult,
    CatalogTableOut,
    NamespaceTree,
)
from shared.delta_utils import validate_catalog_path
from shared.storage_config import storage

NOTEBOOK_RUN_PREFIX = "notebook-"
LISTED_SECONDS = 10


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


_trees: dict[int, tuple[float, list[NamespaceTree]]] = {}


def notebook_namespaces(user_id: int, parent_id: int | None) -> list[NamespaceTree]:
    """The hub's catalogs as a kernel lists them beside this machine's (``parent_id`` ``None``), or the schemas of
    the hub catalog whose row id is ``parent_id``. Nothing without a hub, or when it does not answer."""
    listed = _tree(user_id)
    if parent_id is None:
        return listed
    return [schema for root in listed if root.id == parent_id for schema in root.children]


def notebook_tables(user_id: int, namespace_id: int) -> list[CatalogTableOut]:
    """The tables of the hub namespace whose row id is ``namespace_id``, as a kernel lists them: each a view."""
    return [
        _as_view(table) for holder in _holders(_tree(user_id)) if holder.id == namespace_id for table in holder.tables
    ]


def for_notebook(user_id: int, reference: str, namespace_id: int | None = None) -> CatalogTableOut | None:
    """The hub table a notebook cell names by ``reference``, as the row its kernel resolves, or ``None`` when the
    person's hub lists no table by that name. With the row id of a hub namespace, the table of that name in it.

    The row says it is a view: a kernel reads a view through a file core has the worker write (``snapshot``), which is
    how a table on the hub's storage reaches a cell that holds no credential.
    """
    name = plain_reference(reference)
    if namespace_id is not None:
        return next((table for table in notebook_tables(user_id, namespace_id) if table.name == name), None)
    listed = _tree(user_id)
    hub_id = _sql_names(listed, set()).get(name)
    found = (table for holder in _holders(listed) for table in holder.tables if table.hub_id == hub_id)
    return next((_as_view(table) for table in found), None) if hub_id is not None else None


def tree(user_id: int | None) -> list[NamespaceTree]:
    """The person's hub catalogs with their schemas and tables, read now (``table_sources.RemoteCatalog.tree``);
    empty without a hub session, or when the hub does not answer."""
    return catalog.tree(user_id) if user_id is not None else []


def _tree(user_id: int) -> list[NamespaceTree]:
    """``tree`` for a kernel's lookups, kept ``LISTED_SECONDS``: one cell asks for it several times in a row."""
    held = _trees.get(user_id)
    if held is not None and time.monotonic() - held[0] < LISTED_SECONDS:
        return held[1]
    listed = tree(user_id)
    _trees[user_id] = (time.monotonic(), listed)
    return listed


def _holders(tree: list[NamespaceTree]):
    """Every hub namespace that holds tables: each catalog, then its schemas."""
    for root in tree:
        yield root
        yield from root.children


def _as_view(table: CatalogTableOut) -> CatalogTableOut:
    return table.model_copy(update={"table_type": "virtual"})


def snapshot(user_id: int, hub_table_id: int) -> CatalogTableMaterializeResult:
    """The hub's table ``hub_table_id`` written whole to an Arrow file on the kernels' shared volume, as a view is
    for a notebook cell: one read the hub vends to ``user_id``, the rows written by this machine's worker.

    Every call writes the table again over that person's file for it, so a cell reads the table as it was at its
    last read.

    Raises:
        ValueError: A sentence for the person: no such table, one this Flowfile cannot read, or the hub's refusal.
        WorkerUnavailableError: The worker could not write the file.
    """
    try:
        table = readable(user_id, hub_table_id)
        run_id = f"{NOTEBOOK_RUN_PREFIX}{uuid.uuid4().hex}"
        vended = vend.request_for_tables(user_id, run_id, [table])
        plan = vended.scan_delta(table["file_path"], user_id, {})
    except LookupError as e:
        raise ValueError(str(e)) from e
    name = hashlib.sha256(f"hub:{hub_table_id}:{user_id}".encode()).hexdigest()
    try:
        result = trigger_resolve_virtual_table(
            catalog.row_id(hub_table_id),
            plan.serialize(),
            name,
            target="kernel_shared",
            worker_url=registry.worker_url(),
        )
    except (RequestException, RuntimeError, ComputeError) as e:
        raise WorkerUnavailableError(str(e)) from e
    path = validate_catalog_path(result["ipc_path"], storage.shared_virtual_results_directory)
    return CatalogTableMaterializeResult(path=str(path), row_count=result["row_count"], mtime=result["mtime"])


def _sql_names(tree: list[NamespaceTree], own: set[str]) -> dict[str, int]:
    """Each SQL name of the hub's tables in ``tree`` with the hub's id of its table, named as this machine's own are:
    ``catalog.schema.table`` always, ``schema.table`` and the bare name where the hub has one table by it. A name of
    ``own`` stays this machine's table."""
    full: dict[str, int] = {}
    short: dict[str, set[int]] = {}
    for root in tree:
        for holder in (root, *root.children):
            for table in holder.tables:
                if table.hub_id is None:
                    continue
                parts = [root.name, table.name] if holder is root else [root.name, holder.name, table.name]
                full[".".join(parts)] = table.hub_id
                for name in (".".join(parts[1:]), table.name):
                    short.setdefault(name, set()).add(table.hub_id)
    names = {name: next(iter(ids)) for name, ids in short.items() if len(ids) == 1}
    names.update(full)
    return {name: hub_id for name, hub_id in names.items() if name not in own}
