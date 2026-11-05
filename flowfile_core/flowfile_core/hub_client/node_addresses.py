"""Before a run on a server joined to its hub, the addresses of the hub's servers the run names, read from the hub.

A hub-filled row keeps the worker URL of the last sync, and a sync runs when someone opens the Compute page, so a
server the hub has moved since (a laptop on another network) would be called at its old address. ``refresh`` reads
the hub's servers once before such a run, with the access token of the person the run is for
(``node_tokens.acting``), the one whose sign-in through the hub also mints the run's node tokens, so the token is a
delegated one; a refresh made for nobody reads with the rows' own person (``hub_user_id``). The node's own assertion
cannot read the list: the hub's ``GET /hub/compute-nodes`` answers a person, the servers that person may run on.

One list request per person, whatever the number of servers. A row the hub gives another worker URL moves to it and
is checked again on its next use; a hub that cannot be read, a person without a session, or a node the hub no longer
lists leaves the rows as they are. Adding and removing rows stays with the Compute page's sync.
"""

from collections.abc import Iterable

from flowfile_core.auth import issuer_accounts
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import client, node_tokens, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused


def _workers(user_id: int) -> dict[int, str] | None:
    """Hub node id -> worker URL of the hub's enabled servers ``user_id`` may run on; ``None`` when unread."""
    stored = session.load(user_id)
    if stored is None:
        return None
    try:
        token = session.access_token(user_id)
        if token is None:
            return None
        nodes = client.list_compute_nodes(stored["hub_url"], token)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        logger.warning(
            "Could not read %s's servers before the run; their addresses stay as they were: %s", stored["hub_url"], e
        )
        return None
    return {node["id"]: node["worker_url"].rstrip("/") for node in nodes if node["enabled"]}


def refresh(backend_ids: Iterable[str]) -> None:
    """Move each hub-filled row of ``backend_ids`` to the worker URL its hub lists now, on a server joined with keys.

    A desktop is left as it is: there the hub's list is read when its user asks (``/hub_client/compute_nodes/sync``).
    """
    if issuer_accounts.joined_with_keys() is None:
        return
    ids = [backend_id for backend_id in backend_ids if backend_id.startswith(registry.CONFIGURED_WORKER_PREFIX)]
    if not ids:
        return
    moved: list[tuple[str, str]] = []
    person = node_tokens.acting()
    with get_db_context() as db:
        by_user: dict[int, list[ConfiguredBackend]] = {}
        for row in db.query(ConfiguredBackend).filter(ConfiguredBackend.id.in_(ids)).all():
            reader = person if person is not None else row.hub_user_id
            if row.hub_node_id is not None and reader is not None:
                by_user.setdefault(reader, []).append(row)
        for user_id, rows in by_user.items():
            workers = _workers(user_id)
            for row in rows if workers is not None else []:
                url = workers.get(row.hub_node_id)
                if url is None or url == row.url:
                    continue
                logger.info(
                    'The hub moved server "%s" from %s to %s (read with the hub sign-in of user %s)',
                    row.id.removeprefix(registry.CONFIGURED_WORKER_PREFIX),
                    row.url,
                    url,
                    user_id,
                )
                moved.append((row.id, row.url))
                row.url = url
        db.commit()
    for backend_id, old_url in moved:
        handshake.reset_handshake(old_url)
        registry.forget(backend_id)
