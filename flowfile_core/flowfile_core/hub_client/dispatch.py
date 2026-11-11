"""A run the hub sends this server: one version of a hub flow, run here as the person the hub names.

The ``dispatch`` token carries the run id, the flow's uuid and version and the person, never the flow; the server
pulls the version from the hub as that person. Every run is reported to the hub, failed or not, so it never waits for
one. A run id is taken once per process (``taken``) against replay while its token could still be alive.
"""

import json
import shutil
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from flowfile_core.catalog import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.runs import FlowRunService
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import get_flow_log_file
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRun
from flowfile_core.hub_client import client, node_tokens, signin
from flowfile_core.hub_client import session as hub_session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.hub_client.report import build_run_report, post_run_report
from flowfile_core.schemas import schemas
from shared import hub_node
from shared.hub_tokens import DispatchClaims
from shared.run_logs import HUB_RUN_LOG_PREFIX, run_log_path
from shared.storage_config import storage

CANCELLED, FINISHED, UNKNOWN = "cancelled", "finished", "unknown"

_taken: dict[str, int] = {}
_flows: dict[str, object] = {}
_cancelled: set[str] = set()
_lock = threading.Lock()


def _forget_expired() -> None:
    now = int(time.time())
    for run_id in [run_id for run_id, until in _taken.items() if until < now]:
        del _taken[run_id]
        _cancelled.discard(run_id)


def taken(claims: DispatchClaims) -> bool:
    """Whether this process took ``claims.run_id`` already; takes it otherwise."""
    with _lock:
        _forget_expired()
        if claims.run_id in _taken:
            return True
        _taken[claims.run_id] = claims.expires_at
        return False


def cancel(claims: DispatchClaims) -> str:
    """Stop the hub's run ``claims.run_id``: ``cancelled``, ``finished`` (nothing changed) or ``unknown``. The run id
    is taken, so the cancel's token is never a dispatch here afterwards."""
    with _lock:
        _forget_expired()
        known = claims.run_id in _taken
        _taken.setdefault(claims.run_id, claims.expires_at)
        _cancelled.add(claims.run_id)
        flow = _flows.get(claims.run_id)
    if flow is not None:
        flow.cancel()
    with get_db_context() as db:
        row = (
            db.query(FlowRun)
            .filter(FlowRun.remote_run_id == claims.run_id, FlowRun.run_type == "dispatched")
            .order_by(FlowRun.id.desc())
            .first()
        )
        if row is not None and row.ended_at is not None:
            return FINISHED
        if row is not None:
            FlowRunService(SQLAlchemyCatalogRepository(db)).cancel_run(row.id)
            return CANCELLED
    return CANCELLED if known or flow is not None else UNKNOWN


def _run_graph(run_id: str, flow):
    """``None`` when the hub cancelled the run before it got here."""
    with _lock:
        if run_id in _cancelled:
            return None
        _flows[run_id] = flow
    try:
        return flow.run_graph()
    finally:
        with _lock:
            _flows.pop(run_id, None)


def start(claims: DispatchClaims, token: str) -> None:
    threading.Thread(target=run, args=(claims, token), name=f"hub-dispatch-{claims.run_id[:8]}", daemon=True).start()


def run(claims: DispatchClaims, token: str) -> None:
    started_at = datetime.now(timezone.utc)
    label = f"run {claims.run_id} of hub flow {claims.flow_uuid} v{claims.flow_version}"
    try:
        user_id = _person(claims, token)
        document = _checked_out(user_id, claims)
    except Exception as e:
        logger.warning(f"The hub's {label} did not start: {e}")
        _report_failure(claims, started_at, after=None)
        return
    try:
        _run(user_id, claims, document, label)
    except Exception as e:
        logger.exception(f"The hub's {label} failed before it could be reported: {e}")
        _report_failure(claims, started_at, after=user_id)


def _person(claims: DispatchClaims, token: str) -> int:
    """The person's local account id: their live sign-in, else the dispatch token traded as one."""
    with get_db_context() as db:
        user = signin.account(db, claims.user_id)
        if user is not None and not user.disabled and signin.live(user.id):
            return user.id
        return signin.sign_in(db, token, claims.user_id).user.id


def _checked_out(user_id: int, claims: DispatchClaims) -> schemas.FlowfileData:
    hub_url = hub_node.resolved().hub_url
    access_token = hub_session.access_token(user_id)
    if access_token is None:
        raise LookupError("the person's sign-in through the hub is gone")
    checked_out = client.checkout(hub_url, access_token, claims.flow_uuid, claims.flow_version)
    document = schemas.FlowfileData.model_validate(checked_out["definition"])
    document.flowfile_settings.source_registration_id = None
    document.flowfile_settings.hub_origin = schemas.HubOrigin(
        hub_url=hub_url, flow_uuid=checked_out["flow_uuid"], version=checked_out["version"]
    )
    document.flowfile_name = checked_out["name"]
    return document


def _run(user_id: int, claims: DispatchClaims, document: schemas.FlowfileData, label: str) -> None:
    from flowfile_core.flowfile.manage.ephemeral_graph import ephemeral_flow_graph
    from flowfile_core.hub_client.report import hub_flow_uuid, hub_target
    from flowfile_core.routes.routes import _hub_store_for

    folder = Path(tempfile.mkdtemp(prefix="hub-dispatch-", dir=storage.temp_directory))
    path = folder / f"{claims.flow_uuid}.yaml"
    try:
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(document.model_dump(mode="json"), f, default_flow_style=False, sort_keys=False)
        with ephemeral_flow_graph(path, user_id) as flow:
            flow.hub_run_id = claims.run_id
            logger.info(f"The hub's {label} runs here as user {user_id}, flow id {flow.flow_id}")
            core_run_id = _open_record(flow, user_id, claims, document.flowfile_name)
            run_info = None
            try:
                flow.hub_exchange_store = _hub_store_for(flow, user_id)
                with node_tokens.acting_for(user_id):
                    run_info = _run_graph(claims.run_id, flow)
            finally:
                flow.hub_exchange_store = None
                _close_record(core_run_id, run_info)
                _keep_log(flow.flow_id, claims.run_id, core_run_id)
            if run_info is None:
                cancelled = claims.run_id in _cancelled
                raise RuntimeError("the hub cancelled the run" if cancelled else "the run gave no result")
            target = hub_target(flow, user_id)
            if target is None:
                raise LookupError("the person's sign-in through the hub is gone")
            hub_url, access_token = target
            report = build_run_report(
                flow, run_info, hub_flow_uuid(flow, hub_url) or claims.flow_uuid, run_row_id=core_run_id
            )
        post_run_report(hub_url, access_token, report)
        logger.info(f"The hub's {label} ended: success={run_info.success}, reported to {hub_url}")
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def _open_record(flow, user_id: int, claims: DispatchClaims, name: str | None) -> int | None:
    """The run's row in this server's run history, named from the checkout since the built graph is named after its
    temp file; ``None`` when it could not be written, and the run goes on."""
    try:
        with get_db_context() as db:
            row = FlowRunService(SQLAlchemyCatalogRepository(db)).start_run(
                registration_id=None,
                flow_name=name or claims.flow_uuid,
                flow_path=f"hub://flows/{claims.flow_uuid}",
                user_id=user_id,
                number_of_nodes=len(flow.nodes),
                run_type="dispatched",
                flow_uuid=claims.flow_uuid,
                remote_run_id=claims.run_id,
                flow_version=claims.flow_version,
            )
            return row.id
    except Exception as e:
        logger.warning(f"The hub's run {claims.run_id} is not in this server's run history: {e}")
        return None


def _close_record(core_run_id: int | None, run_info) -> None:
    if core_run_id is None:
        return
    try:
        results = None
        if run_info is not None:
            results = json.dumps([r.model_dump(mode="json") for r in (run_info.node_step_result or [])])
        with get_db_context() as db:
            FlowRunService(SQLAlchemyCatalogRepository(db)).complete_run(
                run_id=core_run_id,
                success=bool(run_info is not None and run_info.success),
                nodes_completed=run_info.nodes_completed if run_info is not None else 0,
                node_results_json=results,
            )
    except Exception as e:
        logger.warning(f"This server's run history row {core_run_id} was not closed: {e}")


def _keep_log(flow_id: int, run_id: str, core_run_id: int | None) -> None:
    """Copy the flow log as ``hub_run_<run id>.log`` and under the run history's name; the graph's release removes
    the original."""
    source = get_flow_log_file(flow_id)
    try:
        if source.exists():
            shutil.copyfile(source, source.with_name(f"{HUB_RUN_LOG_PREFIX}{run_id}.log"))
            if core_run_id is not None:
                shutil.copyfile(source, run_log_path(core_run_id))
    except OSError as e:
        logger.warning(f"The log of the hub's run {run_id} was not kept: {e}")


def _report_failure(claims: DispatchClaims, started_at: datetime, after: int | None) -> None:
    """Report the run failed with no nodes, so the hub closes it instead of waiting an hour for it."""
    if after is None:
        with get_db_context() as db:
            user = signin.account(db, claims.user_id)
        after = user.id if user is not None else None
    if after is None:
        logger.warning(f"The hub's run {claims.run_id} is not reported: nobody is signed in here for it")
        return
    try:
        hub_url = hub_node.resolved().hub_url
        access_token = hub_session.access_token(after)
    except (HubRefused, HubUnreachable, HubVersionRefused, LookupError) as e:
        logger.warning(f"The hub's run {claims.run_id} is not reported: {e}")
        return
    if access_token is None:
        logger.warning(f"The hub's run {claims.run_id} is not reported: the person's sign-in through the hub is gone")
        return
    ended_at = datetime.now(timezone.utc)
    report = {
        "run_id": claims.run_id,
        "flow_uuid": claims.flow_uuid,
        "flow_version": claims.flow_version,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "success": False,
        "nodes": [],
        "inputs": [],
        "outputs": [],
        "vend_ids": [],
    }
    post_run_report(hub_url, access_token, report)
