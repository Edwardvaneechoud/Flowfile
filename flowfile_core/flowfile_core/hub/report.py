"""The report a finished run sends to a hub, and which hub that is.

The hub keeps the organisation's record of runs. It receives who ran which flow (the bearer token says who), the
outcome and timing of every node, the row counts core already knows and the catalog tables the run's catalog nodes
read and wrote. A report never carries a plan, a sample, a schema or a column value, and the hub refuses any field
it does not know, so the shape built here is exact.

Two kinds of core report. A team node has ``FLOWFILE_HUB_URL`` and shares the hub's secret, so it reports every run
with the bearer token of the request that started it. A desktop has neither; it has a hub session and flows that
came from the hub, and reports a run of such a flow with a token minted from the session, so the organisation's
ledger also sees the version that ran on the desktop.
"""

import uuid
from datetime import datetime, timezone

import httpx

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.exceptions import CatalogError
from flowfile_core.configs import logger
from flowfile_core.configs.settings import FLOWFILE_HUB_URL
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph.catalog_resolution import _effective_namespace_id
from flowfile_core.hub_client import session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.output_model import RunInformation


def hub_target(flow, request_token: str | None, user_id: int | None) -> tuple[str, str] | None:
    """Where a run of ``flow`` is reported: ``(hub_url, bearer_token)``, or ``None`` when it is not.

    With ``FLOWFILE_HUB_URL`` set this core is a team node: the run goes there with ``request_token``, the bearer
    token of the request that started it, and a run with no token is not reported. Without the setting this core
    is a desktop: the run is reported only when ``user_id`` has a hub session and ``flow`` carries a ``hub_origin``
    on that same hub, with an access token minted from the session. A refresh the hub does not grant is one
    warning; every other reason not to report is said at debug level.
    """
    if FLOWFILE_HUB_URL:
        if request_token:
            return FLOWFILE_HUB_URL, request_token
        logger.debug("Run not reported to the hub: the request that started it carried no token")
        return None
    stored = session.load(user_id) if user_id is not None else None
    if stored is None:
        logger.debug("Run not reported to a hub: no FLOWFILE_HUB_URL and no hub session for this user")
        return None
    origin = flow.flow_settings.hub_origin
    if origin is None:
        logger.debug("Run not reported to the hub: the flow did not come from a hub")
        return None
    hub_url = stored["hub_url"]
    if origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
        logger.debug(f"Run not reported to the hub: the flow came from another hub ({origin.hub_url})")
        return None
    try:
        token = session.access_token(user_id)
    except (HubUnreachable, HubRefused, HubVersionRefused) as exc:
        logger.warning(f"Could not get a token for the hub at {hub_url} to report the run: {exc}")
        return None
    if token is None:
        logger.debug("Run not reported to the hub: the hub session is gone")
        return None
    return hub_url, token


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
    ``flow_version`` is the hub version of the flow's ``hub_origin``, ``None`` for a flow that never met a hub.
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

    origin = flow.flow_settings.hub_origin
    ended_at = run_info.end_time or datetime.now()
    started_at = run_info.start_time or ended_at
    return {
        "run_id": str(uuid.uuid4()),
        "flow_uuid": flow_uuid,
        "flow_version": origin.version if origin is not None else None,
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
