"""The hub's compute nodes as rows of core's backends table: the one sync, and when a server joined to its hub runs it.

``sync_nodes`` makes the table follow one person's list of the hub's servers. A desktop runs it when its user connects
or asks (``/hub_client/compute_nodes/sync``), and its one user's list is the table's whole. A server joined with keys
of its own runs it by itself, with the person's own sign-in through the hub, so nobody has to open the Compute page
first: when a person signs in (``sync_for``) and before a run that names one of the hub's servers (``refresh``).

On a joined server each person's list is the servers the hub lets that person run on, so one list is not the table's
whole: a row another person's list brought stays theirs. Whether a person may run on a row is still the hub's answer
when the run's token is minted for them. The node's own assertion cannot read the list: the hub's
``GET /hub/compute-nodes`` answers a person.

One list request per person and refresh, whatever the number of servers. A hub that cannot be read, or a person
without a session, leaves the rows as they are.
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
    """Remove *row* as ``DELETE /compute_backends/{id}`` does: the registry's instance and its URL's verdicts go too."""
    db.delete(row)
    registry.forget(row.id)
    handshake.reset_handshake(row.url)


def sync_nodes(db: Session, user_id: int, hub_url: str, access_token: str) -> tuple[list[str], list[str]]:
    """Fill the backends table from the hub's enabled compute nodes for ``user_id``; ``(synced ids, skipped ids)``.

    One ``worker:<name>`` row per enabled node, at the node's worker URL, with no token and no CA, ``remote_id`` the
    node's id on the hub and ``account_user_id`` the caller, whose hub session mints the tokens the backend signs with
    outside a run. A hub-filled row whose node is gone, disabled or renamed is removed; a row added by hand is never
    touched and its node is reported as skipped. A row written or changed is forgotten in the registry, so its next
    use reads it; one the hub lists as before keeps its backend and the verdict it has. The node this core is itself
    (``FLOWFILE_NODE_NAME``, or the name it joined with: ``hub_node.resolved``) is neither written nor counted nor
    skipped, and a row an earlier sync wrote for it goes. The hub's errors propagate; nothing is committed then.

    On a server joined with keys of its own the list is only the servers the hub lets ``user_id`` run on, so a row of
    another person who is still signed in through the hub here is theirs: it stays when the list lacks it, and keeps
    its person when the list has it (its address and node id still follow the hub). A row of ``user_id``'s own, or
    of a person no longer signed in through the hub, follows the list as on a desktop.
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
    """The backend id of the hub server a step of ``user_id`` moves to when the backend its settings name is no server
    of the hub: the hub's default server when the person may run on it, else the first server the hub lists for them;
    ``None`` when the person has no hub session, the hub lists them no server, or it cannot be read.

    The hub lists a person only the enabled servers they may run on. The server's row is written when it has none yet
    (``sync_nodes``), so the run reaches it. On a hub server whose own name the hub answers, that is this server's
    ``worker``.
    """
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
    """``sync_nodes`` with the hub session of ``user_id``, best effort; whether the hub's list was read.

    A person without a session reads nothing; a hub that cannot be read, or a row another sync wrote at the same time,
    leaves the rows as they were and one warning naming ``when``.
    """
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
    """Fill the backends table with the hub's servers ``user_id`` may run on, right after their sign-in through the
    hub on a server joined with keys of its own, so the servers are there to pick before anyone opens Compute."""
    if issuer_accounts.joined_with_keys() is None:
        return
    with get_db_context() as db:
        _sync_for(db, user_id, "at a sign-in")


def refresh(backend_ids: Iterable[str]) -> None:
    """Before a run on a server joined with keys of its own that names one of the hub's servers, make the rows follow
    the hub's list now: a moved server is reached at its new address, one the hub no longer lists is gone, and one the
    hub lists that has no row yet gets one.

    The list is read with the hub session of the person the run is for (``node_tokens.acting``), the one whose sign-in
    through the hub also mints the run's node tokens; a run for nobody reads with the named rows' own people. A run
    naming no ``worker:`` id, or only rows added by hand, reads nothing. A desktop is left as it is: there the hub's
    list is read when its user asks (``/hub_client/compute_nodes/sync``).
    """
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
