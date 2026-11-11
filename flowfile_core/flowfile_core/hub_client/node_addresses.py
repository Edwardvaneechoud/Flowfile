"""The hub's compute nodes as rows of core's backends table.

A desktop syncs when its user connects or asks; a server joined with keys syncs by itself with each person's sign-in
through the hub, at sign-in and before a run that names a hub server, since the node's own assertion cannot read the
list. There each person's list holds only the servers they may run on, so a row another person's list brought stays
theirs. A hub that cannot be read leaves the rows as they are.
"""

from collections.abc import Iterable

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from flowfile_core.auth import issuer_accounts
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import client, node_tokens, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from shared import hub_node

_UNREAD = (HubUnreachable, HubRefused, HubVersionRefused)


def drop_row(db: Session, row: ConfiguredBackend) -> None:
    """Remove *row* as ``DELETE /compute_backends/{id}`` does."""
    db.delete(row)
    registry.forget(row.id)
    handshake.reset_handshake(row.url)


def sync_nodes(db: Session, user_id: int, hub_url: str, access_token: str) -> tuple[list[str], list[str]]:
    """Make the hub-filled ``worker:<name>`` rows follow ``user_id``'s list of enabled nodes; ``(synced, skipped)``.

    A row added by hand is never touched and its node is skipped; this core's own node gets no row. On a joined server
    a row of another person still signed in through the hub keeps its person. Nothing is committed on a hub error.
    """
    nodes = client.list_compute_nodes(hub_url, access_token)
    own = hub_node.resolved().name
    joined = issuer_accounts.joined_with_keys() is not None
    wanted = {
        registry.CONFIGURED_WORKER_PREFIX + node["name"]: node
        for node in nodes
        if node["enabled"] and node["name"] != own
    }

    def follows(row: ConfiguredBackend) -> bool:
        if not joined or row.account_user_id in (None, user_id):
            return True
        return not node_tokens.signed_in_through_hub(row.account_user_id)

    rows = {row.id: row for row in db.query(ConfiguredBackend).all()}
    for backend_id, row in rows.items():
        if row.remote_id is not None and backend_id not in wanted and follows(row):
            drop_row(db, row)
    skipped: list[str] = []
    synced: list[str] = []
    for backend_id, node in wanted.items():
        url = node["worker_url"].rstrip("/")
        row = rows.get(backend_id)
        if row is None:
            db.add(ConfiguredBackend(id=backend_id, url=url, remote_id=node["id"], account_user_id=user_id))
        elif row.remote_id is None:
            skipped.append(backend_id)
            continue
        else:
            person = user_id if follows(row) else row.account_user_id
            if (row.url, row.remote_id, row.account_user_id) != (url, node["id"], person):
                if row.url != url:
                    logger.info('The hub moved server "%s" from %s to %s', node["name"], row.url, url)
                    handshake.reset_handshake(row.url)
                row.url, row.remote_id, row.account_user_id, row.token, row.ca_cert = (
                    url,
                    node["id"],
                    person,
                    None,
                    None,
                )
                registry.forget(backend_id)
        synced.append(backend_id)
    db.commit()
    return synced, skipped


def server_for(user_id: int) -> str | None:
    """The hub server a step of ``user_id`` moves to: the hub's default when listed for them, else the first; its row
    is written when missing. ``None`` when there is none or the hub cannot be read."""
    stored = session.load(user_id)
    if stored is None:
        return None
    try:
        token = session.access_token(user_id)
        if token is None:
            return None
        nodes = [node for node in client.list_compute_nodes(stored["hub_url"], token) if node["enabled"]]
    except _UNREAD as e:
        logger.warning("Could not read %s's servers to move a step to one: %s", stored["hub_url"], e)
        return None
    if not nodes:
        return None
    node = next((node for node in nodes if node.get("is_default")), nodes[0])
    if node["name"] == hub_node.resolved().name:
        return registry.WORKER_BACKEND_ID
    backend_id = registry.CONFIGURED_WORKER_PREFIX + node["name"]
    with get_db_context() as db:
        if db.get(ConfiguredBackend, backend_id) is None:
            _sync_for(db, user_id, "to move a step to one")
            if db.get(ConfiguredBackend, backend_id) is None:
                return None
    return backend_id


def _sync_for(db: Session, user_id: int, when: str) -> bool:
    """``sync_nodes`` with ``user_id``'s session, best effort; whether the list was read."""
    stored = session.load(user_id)
    if stored is None:
        return False
    try:
        token = session.access_token(user_id)
        if token is None:
            return False
        sync_nodes(db, user_id, stored["hub_url"], token)
    except (*_UNREAD, IntegrityError) as e:
        db.rollback()
        logger.warning("Could not read %s's servers %s; the rows stay as they were: %s", stored["hub_url"], when, e)
        return False
    return True


def sync_for(user_id: int) -> None:
    """Sync right after a sign-in on a joined server, so the servers are there to pick before anyone opens Compute."""
    if issuer_accounts.joined_with_keys() is None:
        return
    with get_db_context() as db:
        _sync_for(db, user_id, "at a sign-in")


def refresh(backend_ids: Iterable[str]) -> None:
    """Before a run on a joined server that names hub servers, sync with the acting person's session (the named rows'
    people for a run for nobody), so a moved server is reached at its new address."""
    if issuer_accounts.joined_with_keys() is None:
        return
    ids = [backend_id for backend_id in backend_ids if backend_id.startswith(registry.CONFIGURED_WORKER_PREFIX)]
    if not ids:
        return
    person = node_tokens.acting()
    with get_db_context() as db:
        rows = {row.id: row for row in db.query(ConfiguredBackend).filter(ConfiguredBackend.id.in_(ids)).all()}
        hub_rows = [row for row in rows.values() if row.remote_id is not None]
        if not hub_rows and len(rows) == len(ids):
            return
        if person is not None:
            readers = [person]
        else:
            readers = list(dict.fromkeys(row.account_user_id for row in hub_rows if row.account_user_id is not None))
        for user_id in readers:
            _sync_for(db, user_id, "before the run")


registry.remote_servers.refresh, registry.remote_servers.server_for = refresh, server_for
