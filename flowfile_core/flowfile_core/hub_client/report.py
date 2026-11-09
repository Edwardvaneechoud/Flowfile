"""The report a finished run sends to a hub, and which hub that is.

The hub keeps the organisation's record of runs. It receives who ran which flow (the bearer token says who), the
outcome and timing of every node, the row counts core already knows and the catalog tables the run's catalog nodes
read and wrote. A report never carries a plan, a sample, a schema or a column value, and the hub refuses any field
it does not know, so the shape built here is exact.

Two kinds of core report. A server that joined its hub with keys of its own has people of its own, each signed in
through the hub, and reports every run of such a person with their delegated access token, so the hub's ledger names the
person who ran it. A desktop, or any core not joined with keys, has a hub session and flows that came from the hub,
and reports a run of such a flow with a token minted from the session, so the organisation's ledger also sees the
version that ran on the desktop. It also reports a run that read or wrote the hub's tables, whatever flow it was: the
hub vended that run a credential, so the run is the organisation's business too, and the report names the vend and the
run id the vend was made for.

Neither shares the hub's database or catalog, so a report names no run row of this core and no table of its own
catalog, whose ids are not the hub's: only the hub's tables, by the hub's ids, which the report resolves on the hub
from the names the run's steps carry (``_hub_table_ids``). The hub writes its run row itself.
"""

import uuid
from datetime import datetime, timezone

from flowfile_core.auth import issuer_accounts
from flowfile_core.configs import logger
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.output_model import RunInformation


def hub_target(flow, user_id: int | None) -> tuple[str, str] | None:
    """Where a run of ``flow`` is reported: ``(hub_url, bearer_token)``, or ``None`` when it is not.

    While this storage dir's node is joined with keys of its own (``issuer_accounts.joined_with_keys()``), every run of
    a person signed in through the hub (``user_id`` has a session of ``kind: "delegation"``) goes to that session's hub
    with the person's delegated access token. A person with no such session (an account from before the join) is not
    reported. Anywhere else the run is reported only when ``user_id`` has a hub session and either ``flow`` carries a
    ``hub_origin`` on that same hub or the run vended a credential from it (``flow.last_hub_vend``), with an access
    token minted from the session. The token of the request that started the run is this core's own and never sent. A
    refresh the hub does not grant is one warning; every other reason not to report is said at debug level.
    """
    if issuer_accounts.joined_with_keys() is not None:
        return _delegated_target(user_id)
    stored = session.load(user_id) if user_id is not None else None
    if stored is None:
        logger.debug("Run not reported to a hub: no hub session for this user")
        return None
    origin = getattr(flow.flow_settings, "hub_origin", None)
    hub_url = stored["hub_url"]
    if getattr(flow, "last_hub_vend", None) is None:
        if origin is None:
            logger.debug("Run not reported to the hub: the flow did not come from a hub and used none of its tables")
            return None
        if origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
            logger.debug(f"Run not reported to the hub: the flow came from another hub ({origin.hub_url})")
            return None
    return _session_target(user_id, hub_url)


def _delegated_target(user_id: int | None) -> tuple[str, str] | None:
    """The hub and the delegated access token of ``user_id``'s sign-in through the hub, on a server joined with keys."""
    stored = session.load(user_id) if user_id is not None else None
    if stored is None or stored.get("kind") != "delegation":
        logger.debug("Run not reported to the hub: the person who ran it did not sign in through the hub")
        return None
    return _session_target(user_id, stored["hub_url"])


def _session_target(user_id: int, hub_url: str) -> tuple[str, str] | None:
    try:
        token = session.access_token(user_id)
    except (HubUnreachable, HubRefused, HubVersionRefused) as exc:
        logger.warning(f"Could not get a token for the hub at {hub_url} to report the run: {exc}")
        return None
    if token is None:
        logger.debug("Run not reported to the hub: the hub session is gone")
        return None
    return hub_url, token


def _rows_out(flow, node) -> int | None:
    """The row count the run already knows for ``node``'s output, else ``None``; nothing is counted.

    ``flow.hub_rows_out`` holds what no result does: what a table write's report says about its steps, and the count a
    step knew when the end of the run's vend released its result.
    """
    released = getattr(flow, "hub_rows_out", {}).get(node.node_id)
    if released is not None:
        return released
    ref = node.results.result_ref
    if ref is not None and ref.row_count is not None:
        return ref.row_count
    engine = node.peek_output_engine()
    return engine.known_record_count() if engine is not None else None


def _hub_table_ids(user_id: int, names: set[str]) -> tuple[dict[str, int], dict[str, int]]:
    """The hub's ids behind the names a run's catalog steps carry: of each namespace, by ``catalog.schema``, and of
    each table, by ``catalog.schema.table``, among ``names``.

    One walk of the hub's catalogs and one listing of the tables the account sees, through the steps' hub session. A
    hub that does not answer costs one warning and answers nothing, so the report names no table.
    """
    if not names:
        return {}, {}
    try:
        hub_url, access_token = hub_catalog.signed_in(user_id)
        namespaces = {ns.full_name: ns.id for ns in hub_catalog.namespaces(hub_url, access_token).values()}
        rows = hub_client.list_tables(hub_url, access_token)
    except (LookupError, HubUnreachable, HubRefused, HubVersionRefused) as exc:
        logger.warning(f"Could not read the hub's ids of the tables this run read and wrote: {exc}")
        return {}, {}
    tables = {row["qualified_name"]: row["id"] for row in rows if row.get("qualified_name") in names}
    return namespaces, tables


def hub_flow_uuid(flow, hub_url: str) -> str | None:
    """The uuid ``flow`` has on the hub at ``hub_url``: its ``hub_origin``'s, ``None`` for a flow from elsewhere.

    A run is reported under it before the local registration's uuid, which is another one for a copy of a hub flow
    whose uuid a different person's copy holds in this catalog.
    """
    origin = getattr(flow.flow_settings, "hub_origin", None)
    if origin is None or origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
        return None
    return origin.flow_uuid


def build_run_report(flow, run_info: RunInformation, flow_uuid: str) -> dict:
    """The report of a finished run of ``flow``, a run of the hub flow or local registration ``flow_uuid``.

    ``inputs`` and ``outputs`` are the hub's tables that the catalog reader and writer nodes that ran and succeeded read
    and wrote, named by the hub's ids, resolved on the hub from the qualified names the steps carry
    (``_hub_table_ids``): a reader's table and its schema, a writer's schema and the id the hub lists for the written
    name (``None`` when it lists none). A table whose schema the hub does not name, or a hub that does not answer,
    leaves the table out. A table of this core's own catalog is never named. ``flow_version`` is the hub version of
    the flow's ``hub_origin``, ``None`` for a flow that never met a hub. A run that vended from the hub
    (``flow.last_hub_vend``) is reported under the vend's run id with its ``vend_ids``; a run the hub dispatched here
    under the id the hub gave it (``flow.hub_run_id``); any other run gets a new run id and no vend.
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
    vend = getattr(flow, "last_hub_vend", None)
    ended_at = run_info.end_time or datetime.now()
    started_at = run_info.start_time or ended_at
    return {
        "run_id": vend[0] if vend is not None else getattr(flow, "hub_run_id", None) or str(uuid.uuid4()),
        "flow_uuid": flow_uuid,
        "flow_version": origin.version if origin is not None else None,
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "ended_at": ended_at.astimezone(timezone.utc).isoformat(),
        "success": bool(run_info.success),
        "nodes": nodes,
        "inputs": inputs,
        "outputs": outputs,
        "vend_ids": [vend[1]] if vend is not None else [],
    }


def post_run_report(hub_url: str, token: str, report: dict) -> None:
    """Post ``report`` to ``{hub_url}/hub/runs`` as the user ``token`` belongs to; never raises.

    The run is already recorded locally, so a hub that is down, slow or refusing costs one warning and nothing
    else: no retry, and the run's outcome is unchanged.
    """
    try:
        hub_client._send("POST", hub_url, "/hub/runs", None, json=report, headers=hub_client._bearer(token))
    except (HubUnreachable, HubRefused, HubVersionRefused) as exc:
        logger.warning(f"Could not report run {report.get('run_id')} to the hub at {hub_url}: {exc}")
