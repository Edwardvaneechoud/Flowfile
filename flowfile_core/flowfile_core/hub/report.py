"""The report a finished run sends to the hub at ``FLOWFILE_HUB_URL``.

The hub keeps the organisation's record of runs. It receives who ran which flow (the bearer token says who), the
outcome and timing of every node, the row counts core already knows and the catalog tables the run's catalog nodes
read and wrote. A report never carries a plan, a sample, a schema or a column value, and the hub refuses any field
it does not know, so the shape built here is exact.
"""

import uuid
from datetime import datetime, timezone

import httpx

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.exceptions import CatalogError
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph.catalog_resolution import _effective_namespace_id
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.output_model import RunInformation


def _rows_out(node) -> int | None:
    """The row count the run already knows for ``node``'s output, else ``None``; nothing is counted."""
    ref = node.results.result_ref
    if ref is not None and ref.row_count is not None:
        return ref.row_count
    engine = node.peek_output_engine()
    return engine.known_record_count() if engine is not None else None


def build_run_report(flow, run_info: RunInformation, core_run_id: int, flow_uuid: str) -> dict:
    """The report of a finished run of ``flow``, for the run row ``core_run_id`` of the registration ``flow_uuid``.

    ``inputs`` and ``outputs`` are the catalog tables of the catalog reader and writer nodes that ran and
    succeeded, resolved to ids the way the nodes resolve them: a reader by its table id, else its table reference;
    a writer by its ``namespace_full_name``, else its ``namespace_id``, and its table name. A table that no longer
    resolves keeps its namespace and name with no table id; one whose namespace cannot be found is left out.
    Core's flow document carries no published version, so ``flow_version`` is ``None``.
    """
    nodes, readers, writers = [], [], []
    for result in run_info.node_step_result or []:
        node = flow.get_node(result.node_id)
        if node is None:
            continue
        nodes.append(
            {
                "node_id": result.node_id,
                "node_type": node.node_type,
                "success": bool(result.success),
                "skipped": result.skipped,
                "duration_seconds": result.run_time_ms / 1000 if result.run_time_ms >= 0 else None,
                "rows_out": _rows_out(node),
                "error": result.error or None,
            }
        )
        if not result.success or result.skipped:
            continue
        if isinstance(node.setting_input, input_schema.NodeCatalogReader):
            readers.append(node.setting_input)
        elif isinstance(node.setting_input, input_schema.NodeCatalogWriter):
            writers.append(node.setting_input.catalog_write_settings)

    inputs, outputs = [], []
    if readers or writers:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            service = CatalogService(repo)
            for reader in readers:
                table = None
                reference = reader.catalog_full_table_name or reader.catalog_table_name
                if reader.catalog_table_id:
                    table = repo.get_table(reader.catalog_table_id)
                elif reference:
                    try:
                        table = service.resolve_table(reference, default_namespace_id=reader.catalog_namespace_id)
                    except CatalogError:
                        table = None
                if table is not None and table.namespace_id is not None:
                    ref = {"namespace_id": table.namespace_id, "table_id": table.id, "table_name": table.name}
                elif reader.catalog_namespace_id is not None and reader.catalog_table_name:
                    ref = {
                        "namespace_id": reader.catalog_namespace_id,
                        "table_id": None,
                        "table_name": reader.catalog_table_name,
                    }
                else:
                    continue
                if ref not in inputs:
                    inputs.append(ref)
            for settings in writers:
                namespace_id = _effective_namespace_id(service, settings)
                if namespace_id is None or not settings.table_name:
                    continue
                table = repo.get_table_by_name(settings.table_name, namespace_id)
                ref = {
                    "namespace_id": namespace_id,
                    "table_id": table.id if table is not None else None,
                    "table_name": settings.table_name,
                }
                if ref not in outputs:
                    outputs.append(ref)

    ended_at = run_info.end_time or datetime.now()
    started_at = run_info.start_time or ended_at
    return {
        "run_id": str(uuid.uuid4()),
        "flow_uuid": flow_uuid,
        "flow_version": None,
        "compute_node_id": None,
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "ended_at": ended_at.astimezone(timezone.utc).isoformat(),
        "success": bool(run_info.success),
        "nodes": nodes,
        "inputs": inputs,
        "outputs": outputs,
        "vend_ids": [],
        "core_run_id": core_run_id,
    }


def post_run_report(hub_url: str, token: str, report: dict) -> None:
    """Post ``report`` to ``{hub_url}/hub/runs`` as the user ``token`` belongs to; never raises.

    The run is already recorded locally, so a hub that is down, slow or refusing costs one warning and nothing
    else: no retry, and the run's outcome is unchanged.
    """
    url = f"{hub_url.rstrip('/')}/hub/runs"
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.post(url, json=report, headers={"Authorization": f"Bearer {token}"})
            response.raise_for_status()
    except Exception as exc:
        logger.warning(f"Could not report run {report.get('core_run_id')} to the hub at {url}: {exc}")
