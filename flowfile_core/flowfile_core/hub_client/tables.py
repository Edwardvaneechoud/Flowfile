"""A table in the hub's catalog as this Flowfile reads it: the table itself, its first rows for the Catalog page, a
query of the SQL editor over it, a visual on such a query and a notebook cell's read of it.

A reader step names a table by its qualified name (``readable_named``); the Catalog page's preview, the SQL editor,
a visual and a notebook cell name one by the hub's id on its row of the catalog tree (``readable``), and all take it
the same way. Each of the last four asks the hub for one read of the tables' namespaces under a run id of its own
(``preview-…``, ``sql-…``, ``visual-…`` or ``notebook-…``, which the hub's vend ledger keeps), scans each table with
that credential as a run's reader does (``RunVend.scan_delta``) and reads the rows on this machine's own worker. The
credential lives for the call; a visual's plan carries it, encrypted, for a few minutes (``visual_source``), and a
notebook cell reads a file the worker wrote (``snapshot``).
"""

import base64
import hashlib
import re
import time
import uuid

import polars as pl
from requests.exceptions import RequestException

from flowfile_core.catalog.exceptions import WorkerUnavailableError
from flowfile_core.catalog.serializers import format_pyarrow_preview
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.catalog.text_utils import friendly_relation_error, is_table_reference, rewrite_qualified_references
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.bounded import count_rows, head_rows
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_resolve_virtual_table,
)
from flowfile_core.hub_client import catalog, session, vend
from flowfile_core.schemas.catalog_schema import (
    CatalogTableMaterializeResult,
    CatalogTableOut,
    CatalogTablePreview,
    NamespaceTree,
    SqlQueryResult,
)
from shared.delta_utils import make_json_safe, validate_catalog_path
from shared.storage_config import storage

PREVIEW_RUN_PREFIX = "preview-"
SQL_RUN_PREFIX = "sql-"
VISUAL_RUN_PREFIX = "visual-"
NOTEBOOK_RUN_PREFIX = "notebook-"
LISTED_SECONDS = 10
VISUAL_KEPT_SECONDS = 240
"""How long one visual's source is reused: well inside the 15 minutes a hub's credential lives by default."""
SAMPLE_ROWS = 10_000
"""The most rows a backend hands back in one bounded read (the worker's ``SAMPLE_ROWS_CAP``)."""
MIXED = (
    "This query names tables on the hub and tables of this Flowfile; the SQL editor reads one or the other. "
    "Combine them in a flow."
)
_NAME = r'(?:"[^"]+"|[\w-]+)'
_READ_FROM = re.compile(rf"\b(?:FROM|JOIN)\s+({_NAME}(?:\s*\.\s*{_NAME})*)", re.IGNORECASE)
_WITH_NAME = re.compile(rf"({_NAME})\s+AS\s*\(", re.IGNORECASE)


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


def preview(user_id: int, hub_table_id: int, limit: int) -> CatalogTablePreview:
    """The first ``limit`` rows of the hub's table ``hub_table_id`` and its row count, as a local table's preview.

    The row count is the one the hub keeps for the table; a table the hub has no count for is counted where the rows
    are read.

    Raises:
        LookupError: A sentence for the person: not connected to the hub, no such table, or the hub's refusal.
        ValueError: The hub keeps the table on its own disk, or it is a virtual table.
        ComputeError: The backend could not read the rows.
    """
    table = readable(user_id, hub_table_id)
    vended = vend.request(user_id, f"{PREVIEW_RUN_PREFIX}{uuid.uuid4().hex}", {table["namespace_id"]}, set())
    plan = vended.scan_delta(table["file_path"], user_id, _columns(table))
    backend = current_context().backend
    rows = head_rows(backend, plan, limit)
    total_rows = table.get("row_count")
    if total_rows is None:
        total_rows = count_rows(backend, plan)
    return format_pyarrow_preview(rows, total_rows=total_rows)


def named(user_id: int | None, query: str, own: set[str]) -> dict[str, int]:
    """The hub tables ``query`` names: each SQL name it uses for one, with the hub's id of that table.

    ``own`` are the names this machine's own catalog answers, which win over the hub's. The hub is asked only when the
    person is connected to one and the query reads from a name ``own`` does not hold, so a query over this machine's
    tables costs no request. A hub that does not answer names nothing (``catalog.tree`` logs why).
    """
    if user_id is None or session.load(user_id) is None or not _reads_other(query, own):
        return {}
    names = _sql_names(catalog.tree(user_id), own)
    collapsed = rewrite_qualified_references(query, names)
    return {name: hub_id for name, hub_id in names.items() if is_table_reference(name, collapsed)}


def sql(user_id: int, query: str, tables: dict[str, int], own: set[str], max_rows: int) -> SqlQueryResult:
    """Run ``query`` over the hub tables it names (``named``) and answer as the SQL editor's run over this machine's
    tables does.

    The hub vends one read of the tables' namespaces and the plan, a SQL context over the vended scans, is read on
    this machine's default backend: at most ``max_rows`` rows and never more than one bounded read gives
    (``SAMPLE_ROWS``), and the count of all of them only when the read came back full.
    A query that also names one of this machine's own tables (``own``) is refused with one sentence. Every failure is
    the result's ``error``, as the local run's is.
    """
    started = time.perf_counter()
    try:
        plan = _plan(user_id, query, tables, own, SQL_RUN_PREFIX)
        backend = current_context().backend
        limit = min(max_rows, SAMPLE_ROWS)
        frame = pl.from_arrow(head_rows(backend, plan, limit))
        total_rows = frame.height if frame.height < limit else count_rows(backend, plan)
    except (LookupError, ValueError, ComputeError, pl.exceptions.PolarsError) as e:
        return SqlQueryResult(error=friendly_relation_error(str(e), query))
    return SqlQueryResult(
        columns=frame.columns,
        dtypes=[str(dtype) for dtype in frame.dtypes],
        rows=[[make_json_safe(value) for value in row] for row in frame.rows()],
        total_rows=total_rows,
        truncated=total_rows > frame.height,
        execution_time_ms=(time.perf_counter() - started) * 1000,
        used_tables=sorted(tables),
    )


_visual_sources: dict[tuple[int, str], tuple[float, dict]] = {}


def visual_source(user_id: int, query: str, tables: dict[str, int], own: set[str]) -> dict:
    """``query`` over the hub tables it names (``named``) as the worker's visualisation source: its plan, which the
    worker holds lazily and reads for each request of a chart.

    The plan carries the credential of one read the hub vends, encrypted for the person. A source is reused for
    ``VISUAL_KEPT_SECONDS`` per person and query, so a chart's requests share one vend and one session on the worker,
    and the tables are read as they were when it was built; after that the hub is asked again.

    Raises:
        ValueError: A sentence for the person: the query also names one of this machine's tables, names a table this
            Flowfile cannot read, does not hold over the tables' columns, or the hub refuses the read.
    """
    now = time.monotonic()
    for key in [key for key, (built, _) in _visual_sources.items() if now - built >= VISUAL_KEPT_SECONDS]:
        del _visual_sources[key]
    held = _visual_sources.get((user_id, query))
    if held is not None:
        return held[1]
    try:
        plan = _plan(user_id, query, tables, own, VISUAL_RUN_PREFIX)
    except LookupError as e:
        raise ValueError(str(e)) from e
    except pl.exceptions.PolarsError as e:
        raise ValueError(friendly_relation_error(str(e), query)) from e
    source = {
        "kind": "plan",
        "session_key": f"hub:{uuid.uuid4().hex}",
        "plan_bytes": base64.b64encode(plan.serialize()).decode("ascii"),
    }
    _visual_sources[(user_id, query)] = (now, source)
    return source


_trees: dict[int, tuple[float, list[NamespaceTree]]] = {}


def notebook_namespaces(user_id: int, parent_id: int | None) -> list[NamespaceTree]:
    """The hub's catalogs as a kernel lists them beside this machine's (``parent_id`` ``None``), or the schemas of
    the hub catalog whose row id is ``parent_id``. Nothing without a hub, or when it does not answer."""
    tree = _tree(user_id)
    if parent_id is None:
        return tree
    return [schema for root in tree if root.id == parent_id for schema in root.children]


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
    name = _plain(reference)
    if namespace_id is not None:
        return next((table for table in notebook_tables(user_id, namespace_id) if table.name == name), None)
    tree = _tree(user_id)
    hub_id = _sql_names(tree, set()).get(name)
    found = (table for holder in _holders(tree) for table in holder.tables if table.hub_id == hub_id)
    return next((_as_view(table) for table in found), None) if hub_id is not None else None


def _tree(user_id: int) -> list[NamespaceTree]:
    """The person's hub catalogs for a kernel's lookups, kept ``LISTED_SECONDS``: one cell asks for them several
    times in a row. Empty without a hub session."""
    held = _trees.get(user_id)
    if held is not None and time.monotonic() - held[0] < LISTED_SECONDS:
        return held[1]
    tree = catalog.tree(user_id) if session.load(user_id) is not None else []
    _trees[user_id] = (time.monotonic(), tree)
    return tree


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
        vended = vend.request(user_id, run_id, {table["namespace_id"]}, set())
        plan = vended.scan_delta(table["file_path"], user_id, _columns(table))
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


def _plan(user_id: int, query: str, tables: dict[str, int], own: set[str], run_prefix: str) -> pl.LazyFrame:
    """``query`` as a lazy plan over the hub tables it names, each scanned with one read the hub vends now.

    Raises:
        LookupError, ValueError: A sentence for the person: the query also names one of this machine's own tables,
            names a table this Flowfile cannot read, or the hub refuses the read.
        pl.exceptions.PolarsError: The SQL does not hold over the tables' columns.
    """
    collapsed = rewrite_qualified_references(query, set(tables) | own)
    if any(is_table_reference(name, collapsed) for name in own):
        raise ValueError(MIXED)
    found = {hub_id: readable(user_id, hub_id) for hub_id in set(tables.values())}
    run_id = f"{run_prefix}{uuid.uuid4().hex}"
    vended = vend.request(user_id, run_id, {table["namespace_id"] for table in found.values()}, set())
    context = pl.SQLContext()
    for name, hub_id in tables.items():
        context.register(name, vended.scan_delta(found[hub_id]["file_path"], user_id, _columns(found[hub_id])))
    return context.execute(collapsed)


def _columns(table: dict) -> dict[str, pl.DataType]:
    """The columns the hub lists for ``table``, as a sealed scan needs them."""
    return {column["name"]: cast_str_to_polars_type(column["dtype"]) for column in table.get("schema_columns") or ()}


def _reads_other(query: str, own: set[str]) -> bool:
    """Whether ``query`` reads from a name that is neither one of ``own`` nor one of its own ``WITH`` names."""
    collapsed = rewrite_qualified_references(query, own)
    known = own | {_plain(name) for name in _WITH_NAME.findall(collapsed)}
    return any(_plain(name) not in known for name in _READ_FROM.findall(collapsed))


def _plain(reference: str) -> str:
    """A table reference without its quotes and spaces: ``"a" . b`` as ``a.b``."""
    if re.fullmatch(_NAME, reference):
        return reference.strip('"')
    return ".".join(part.strip().strip('"') for part in reference.split("."))


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
