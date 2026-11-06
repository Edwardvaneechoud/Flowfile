"""A run the hub sends this server: one version of a hub flow, run here as the person the hub names.

The hub pushes, the server pulls the flow: the ``dispatch`` token (``shared.hub_tokens.verify_dispatch``) carries the
run id the hub keeps the run as, the flow's uuid and version and the person, never the flow itself. ``start`` answers
at once and the rest runs in a thread of its own: the person is signed in here when they are not yet (the dispatch token
is also their sign-in, ``hub_client.signin``), the version is checked out from the hub as them, built under a fresh flow
id and run as a flow opened from the hub runs for them, with their vends, their run token on the hub's other servers and
the hub's exchange store, and reported to the hub under the hub's run id with their delegated token, failed or not. A
run that cannot start, a checkout the hub refuses or a flow that does not build, is reported failed with no nodes, so
the hub does not wait for it. Nothing is registered or saved here; the run's log is kept beside the flow logs as
``hub_run_<run id>.log``.

A run id is taken once per process (``taken``): a second dispatch with it is refused while its token could still be
alive. A restart forgets them; the hub's row still says the run was dispatched.
"""

import shutil
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import get_flow_log_file
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import client, node_tokens, signin
from flowfile_core.hub_client import session as hub_session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.hub_client.report import build_run_report, post_run_report
from flowfile_core.schemas import schemas
from shared import hub_node
from shared.hub_tokens import DispatchClaims
from shared.run_logs import HUB_RUN_LOG_PREFIX
from shared.storage_config import storage

_taken: dict[str, int] = {}
_lock = threading.Lock()


def taken(claims: DispatchClaims) -> bool:
    """Whether this process took a run with ``claims.run_id`` already; takes it otherwise. Run ids whose tokens have
    expired are forgotten first."""
    now = int(time.time())
    with _lock:
        for run_id in [run_id for run_id, until in _taken.items() if until < now]:
            del _taken[run_id]
        if claims.run_id in _taken:
            return True
        _taken[claims.run_id] = claims.expires_at
        return False


def start(claims: DispatchClaims, token: str) -> None:
    """Run the dispatch in a thread of its own; answers at once."""
    threading.Thread(target=run, args=(claims, token), name=f"hub-dispatch-{claims.run_id[:8]}", daemon=True).start()


def run(claims: DispatchClaims, token: str) -> None:
    """The whole run, in this thread: sign the person in when needed, check out, build, run, report."""
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
    """The local account id of the person the token names, signed in here: their live sign-in when they have one, else
    the dispatch token traded as their sign-in."""
    with get_db_context() as db:
        user = signin.account(db, claims.user_id)
        if user is not None and not user.disabled and signin.live(user.id):
            return user.id
        return signin.sign_in(db, token, claims.user_id).user.id


def _checked_out(user_id: int, claims: DispatchClaims) -> schemas.FlowfileData:
    """The version's document from the hub, as the person, carrying the hub origin a run of it is reported under."""
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
    """Build the document under a fresh id in a file of its own, run it as the person and report it; the file goes,
    the log stays."""
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
            try:
                flow.hub_exchange_store = _hub_store_for(flow, user_id)
                with node_tokens.acting_for(user_id):
                    run_info = flow.run_graph()
            finally:
                flow.hub_exchange_store = None
                _keep_log(flow.flow_id, claims.run_id)
            if run_info is None:
                raise RuntimeError("the run gave no result")
            target = hub_target(flow, user_id)
            if target is None:
                raise LookupError("the person's sign-in through the hub is gone")
            hub_url, access_token = target
            report = build_run_report(flow, run_info, hub_flow_uuid(flow, hub_url) or claims.flow_uuid)
        post_run_report(hub_url, access_token, report)
        logger.info(f"The hub's {label} ended: success={run_info.success}, reported to {hub_url}")
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def _keep_log(flow_id: int, run_id: str) -> None:
    """Copy the run's flow log beside the flow logs as ``hub_run_<run id>.log``; the graph's release removes the
    original."""
    source = get_flow_log_file(flow_id)
    try:
        if source.exists():
            shutil.copyfile(source, source.with_name(f"{HUB_RUN_LOG_PREFIX}{run_id}.log"))
    except OSError as e:
        logger.warning(f"The log of the hub's run {run_id} was not kept: {e}")


def _report_failure(claims: DispatchClaims, started_at: datetime, after: int | None) -> None:
    """Tell the hub the run failed with no nodes, as the person when they are signed in here (``after``), so the hub
    closes the run instead of waiting an hour for it."""
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
