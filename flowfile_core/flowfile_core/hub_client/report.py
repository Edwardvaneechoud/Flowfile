"""The report a finished run sends to a hub, and which hub that is.

A report never carries a plan, a sample, a schema or a column value, and the hub refuses any field it does not know,
so the shape built here is exact. It names only the hub's tables, by the hub's ids, never a run row or table of this
core's catalog, whose ids are not the hub's.
"""

import uuid
from datetime import datetime, timezone

from flowfile_core.catalog import run_records
from flowfile_core.configs import logger
from flowfile_core.flowfile.execution import registry
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.output_model import RunInformation
from shared import server_extension


def hub_target(flow, user_id: int | None) -> tuple[str, str] | None:
    """``(hub_url, bearer_token)`` a run of ``flow`` is reported to, or ``None``.

    On a server joined with keys, every run of a person signed in through the hub, with their delegated token. Else
    only a run of a flow from that hub, or one that vended from it, with a token minted from the session. The token of
    the request that started the run is this core's own and is never sent.
    """
    if server_extension.joined() is not None:
        return _delegated_target(user_id)
    stored = session.load(user_id) if user_id is not None else None
    if stored is None:
        logger.debug("Run not reported to a hub: no hub session for this user")
        return None
    origin = getattr(flow.flow_settings, "hub_origin", None)
    hub_url = stored["hub_url"]
    if getattr(flow, "last_run_grant", None) is None:
        if origin is None:
            logger.debug("Run not reported to the hub: the flow did not come from a hub and used none of its tables")
            return None
        if origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
            logger.debug(f"Run not reported to the hub: the flow came from another hub ({origin.hub_url})")
            return None
    return _session_target(user_id, hub_url)


def _delegated_target(user_id: int | None) -> tuple[str, str] | None:
    stored = session.load(user_id) if user_id is not None else None
    if stored is None or stored.get("kind") != "delegation":
        logger.debug("Run not reported to the hub: the person who ran it did not sign in through the hub")
        return None
    return _session_target(user_id, stored["hub_url"])


def _session_target(user_id: int, hub_url: str) -> tuple[str, str] | None:
    try:
        return session.bearer(user_id, "the hub session is gone", ended=())
    except LookupError as exc:
        logger.debug(f"Run not reported to the hub: {exc}")
    except (HubUnreachable, HubRefused, HubVersionRefused) as exc:
        logger.warning(f"Could not get a token for the hub at {hub_url} to report the run: {exc}")
    return None


def _rows_out(flow, node) -> int | None:
    """The row count the run already knows for ``node``'s output; nothing is counted."""
    released = getattr(flow, "known_rows_out", {}).get(node.node_id)
    if released is not None:
        return released
    ref = node.results.result_ref
    if ref is not None and ref.row_count is not None:
        return ref.row_count
    engine = node.peek_output_engine()
    return engine.known_record_count() if engine is not None else None


def _hub_table_ids(user_id: int, names: set[str]) -> tuple[dict[str, int], dict[str, int]]:
    """The hub's namespace ids by ``catalog.schema`` and table ids by qualified name among ``names``; a hub that does
    not answer costs one warning and names nothing."""
    if not names:
        return {}, {}
    try:
        listed = hub_catalog.schemas(*session.bearer(user_id, hub_catalog.NOT_CONNECTED))
    except (LookupError, HubUnreachable, HubRefused, HubVersionRefused) as exc:
        logger.warning(f"Could not read the hub's ids of the tables this run read and wrote: {exc}")
        return {}, {}
    rows = [row for _, tables in listed for row in tables]
    tables = {row["qualified_name"]: row["id"] for row in rows if row.get("qualified_name") in names}
    return {namespace.full_name: namespace.id for namespace, _ in listed}, tables


def hub_flow_uuid(flow, hub_url: str) -> str | None:
    """The flow's uuid on that hub from its ``hub_origin``, preferred over the local registration's, which differs for
    a copy whose uuid another person's copy holds here."""
    origin = getattr(flow.flow_settings, "hub_origin", None)
    if origin is None or origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
        return None
    return origin.flow_uuid


def build_run_report(flow, run_info: RunInformation, flow_uuid: str, run_row_id: int | None = None) -> dict:
    """The one write of the hub's record of a finished run.

    ``inputs`` and ``outputs`` are the hub tables the successful catalog readers and writers used. The run id is the
    vend's for a run that vended, the hub's for a run it dispatched, else a new one.
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
                "rows_out": _rows_out(flow, node),
                "error": result.error or None,
            }
        )
        if not result.success or result.skipped:
            continue
        given = node.setting_input
        if isinstance(given, input_schema.NodeCatalogReader):
            if given.hub_namespace_name is not None:
                readers.append(given)
        elif isinstance(given, input_schema.NodeCatalogWriter):
            settings = given.catalog_write_settings
            if settings.source == "hub" and settings.namespace_full_name and settings.table_name:
                writers.append(given)

    named = {reader.catalog_full_table_name for reader in readers}
    named |= {f"{w.catalog_write_settings.namespace_full_name}.{w.catalog_write_settings.table_name}" for w in writers}
    steps = readers + writers
    namespace_ids, table_ids = _hub_table_ids(steps[0].user_id or 1, named) if steps else ({}, {})
    inputs, outputs = [], []
    for reader in readers:
        namespace_id = namespace_ids.get(reader.hub_namespace_name)
        if namespace_id is None:
            continue
        ref = {
            "namespace_id": namespace_id,
            "table_id": table_ids.get(reader.catalog_full_table_name),
            "table_name": reader.catalog_table_name or reader.catalog_full_table_name.rpartition(".")[2],
        }
        if ref not in inputs:
            inputs.append(ref)
    for writer in writers:
        settings = writer.catalog_write_settings
        namespace_id = namespace_ids.get(settings.namespace_full_name)
        if namespace_id is None:
            continue
        ref = {
            "namespace_id": namespace_id,
            "table_id": table_ids.get(f"{settings.namespace_full_name}.{settings.table_name}"),
            "table_name": settings.table_name,
        }
        if ref not in outputs:
            outputs.append(ref)

    origin = getattr(flow.flow_settings, "hub_origin", None)
    vend = getattr(flow, "last_run_grant", None)
    ended_at = run_info.end_time or datetime.now()
    started_at = run_info.start_time or ended_at
    executed_on = registry.server_name(registry.WORKER_BACKEND_ID)
    return {
        "run_id": vend[0] if vend is not None else getattr(flow, "hub_run_id", None) or str(uuid.uuid4()),
        "flow_uuid": flow_uuid,
        "flow_version": origin.version if origin is not None else None,
        "flow_name": getattr(flow.flow_settings, "name", None) or None,
        "executed_on": executed_on,
        "log_ref": run_records.log_ref(executed_on, run_row_id) if executed_on and run_row_id is not None else None,
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "ended_at": ended_at.astimezone(timezone.utc).isoformat(),
        "success": bool(run_info.success),
        "nodes": nodes,
        "inputs": inputs,
        "outputs": outputs,
        "vend_ids": [vend[1]] if vend is not None else [],
    }


def post_run_report(hub_url: str, token: str, report: dict) -> None:
    """Never raises: the run is recorded locally, so a failed post is one warning and no retry."""
    try:
        hub_client._send("POST", hub_url, "/hub/runs", None, json=report, headers=hub_client._bearer(token))
    except (HubUnreachable, HubRefused, HubVersionRefused) as exc:
        logger.warning(f"Could not report run {report.get('run_id')} to the hub at {hub_url}: {exc}")
