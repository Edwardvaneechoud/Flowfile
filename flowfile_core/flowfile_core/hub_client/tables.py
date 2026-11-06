"""A table in the hub's catalog as this Flowfile reads it: the table itself, its first rows for the Catalog page, and
a query of the SQL editor over it.

A reader step, the Catalog page's preview and the SQL editor find the table the same way (``readable``). A preview or
a query asks the hub for one read of the tables' namespaces under a run id of its own (``preview-…`` or ``sql-…``,
which the hub's vend ledger keeps), scans each table with that credential as a run's reader does
(``RunVend.scan_delta``) and reads the rows on this machine's default backend, the worker beside its core. The
credential lives for the call and nothing is kept.
"""

import re
import time
import uuid

import polars as pl

from flowfile_core.catalog.serializers import format_pyarrow_preview
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.catalog.text_utils import friendly_relation_error, is_table_reference, rewrite_qualified_references
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.bounded import count_rows, head_rows
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.hub_client import catalog, session, vend
from flowfile_core.schemas.catalog_schema import CatalogTablePreview, NamespaceTree, SqlQueryResult
from shared.delta_utils import make_json_safe

PREVIEW_RUN_PREFIX = "preview-"
SQL_RUN_PREFIX = "sql-"
MIXED = (
    "This query names tables on the hub and tables of this Flowfile; the SQL editor reads one or the other. "
    "Combine them in a flow."
)
_NAME = r'(?:"[^"]+"|[\w-]+)'
_READ_FROM = re.compile(rf"\b(?:FROM|JOIN)\s+({_NAME}(?:\s*\.\s*{_NAME})*)", re.IGNORECASE)
_WITH_NAME = re.compile(rf"({_NAME})\s+AS\s*\(", re.IGNORECASE)


def readable(user_id: int | None, hub_table_id: int) -> dict:
    """The hub's table ``hub_table_id`` as a reader takes it: a physical table on object storage.

    Raises:
        LookupError: The hub has no such table, does not answer, or this Flowfile is not connected to it.
        ValueError: The hub keeps the table on its own disk, or it is a virtual table.
    """
    table = catalog.table(user_id, hub_table_id)
    if table.get("table_type", "physical") != "physical":
        raise ValueError("A virtual table on the hub cannot be read from this Flowfile")
    if not table.get("file_path") or not _is_cloud_uri(table["file_path"]):
        namespace = catalog.locate(user_id, table["namespace_id"]).full_name
        raise ValueError(catalog.on_own_disk(namespace))
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
    this machine's default backend: at most ``max_rows`` rows, and the count of all of them only when there are more.
    A query that also names one of this machine's own tables (``own``) is refused with one sentence. Every failure is
    the result's ``error``, as the local run's is.
    """
    started = time.perf_counter()
    collapsed = rewrite_qualified_references(query, set(tables) | own)
    if any(is_table_reference(name, collapsed) for name in own):
        return SqlQueryResult(error=MIXED)
    try:
        found = {hub_id: readable(user_id, hub_id) for hub_id in set(tables.values())}
        run_id = f"{SQL_RUN_PREFIX}{uuid.uuid4().hex}"
        vended = vend.request(user_id, run_id, {table["namespace_id"] for table in found.values()}, set())
        context = pl.SQLContext()
        for name, hub_id in tables.items():
            context.register(name, vended.scan_delta(found[hub_id]["file_path"], user_id, _columns(found[hub_id])))
        plan = context.execute(collapsed)
        backend = current_context().backend
        frame = pl.from_arrow(head_rows(backend, plan, max_rows + 1))
        total_rows = frame.height if frame.height <= max_rows else count_rows(backend, plan)
    except (LookupError, ValueError, ComputeError, pl.exceptions.PolarsError) as e:
        return SqlQueryResult(error=friendly_relation_error(str(e), query))
    shown = frame.head(max_rows)
    return SqlQueryResult(
        columns=shown.columns,
        dtypes=[str(dtype) for dtype in shown.dtypes],
        rows=[[make_json_safe(value) for value in row] for row in shown.rows()],
        total_rows=total_rows,
        truncated=total_rows > max_rows,
        execution_time_ms=(time.perf_counter() - started) * 1000,
        used_tables=sorted(tables),
    )


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
