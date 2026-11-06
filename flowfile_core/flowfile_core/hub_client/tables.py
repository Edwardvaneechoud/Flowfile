"""A table in the hub's catalog as this Flowfile reads it: the table itself, and its first rows for the Catalog page.

A reader step and the Catalog page's preview find the table the same way (``readable``). The preview asks the hub for
one read of the table's namespace under a run id of its own (``preview-…``, which the hub's vend ledger keeps), scans
the table with that credential as a run's reader does (``RunVend.scan_delta``) and reads the rows on this machine's
default backend, the worker beside its core. The credential lives for the call and nothing is kept.
"""

import uuid

from flowfile_core.catalog.serializers import format_pyarrow_preview
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.flow_data_engine.bounded import count_rows, head_rows
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.hub_client import catalog, vend
from flowfile_core.schemas.catalog_schema import CatalogTablePreview

PREVIEW_RUN_PREFIX = "preview-"


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
    columns = {column["name"]: cast_str_to_polars_type(column["dtype"]) for column in table.get("schema_columns") or ()}
    plan = vended.scan_delta(table["file_path"], user_id, columns)
    backend = current_context().backend
    rows = head_rows(backend, plan, limit)
    total_rows = table.get("row_count")
    if total_rows is None:
        total_rows = count_rows(backend, plan)
    return format_pyarrow_preview(rows, total_rows=total_rows)
