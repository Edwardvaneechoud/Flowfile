"""The exchange store a team node, or a server joined with keys of its own, takes from its hub.

An organisation sets its exchange store once, on its hub. A team node (a core in server mode with
``FLOWFILE_HUB_URL``) shares the hub's database, so the S3 connection the hub's store names is a connection this core
reads too, and the crossing opens the store through it as it opens a store set here. A desktop asks nothing: its
database does not hold the hub's connection. A server that joined its hub with keys of its own, ``FLOWFILE_HUB_URL``
set or not, holds none of the hub's connections either, and its people's bearer tokens are its own: it asks with the
person's hub session instead (``joined_server_store``), and each run that crosses between servers has the hub vend the
store's prefix sealed to the servers it runs on, so no connection is looked up here.

The hub is asked once per run, with the bearer token of the request that started it: every session token on a team
node is one the hub accepts, so no token is minted, cached or stored for this, and a run that no request started
(a scheduled run) takes no store from the hub. A hub that does not answer is no store, never a failed run.
"""

from flowfile_core.auth import issuer_accounts
from flowfile_core.configs import logger, settings
from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused

_warned: set[tuple[str, str]] = set()


def team_node_store(token: str | None) -> ExchangeConfig | None:
    """The exchange store this node's hub sets, asked with ``token``; ``None`` when there is none to take.

    ``None`` without asking unless this core is a team node (``FLOWFILE_HUB_URL`` set and ``is_server_mode()``, its
    storage dir not joined with keys of its own) and ``token`` is given. ``None`` too when the hub sets no store, and
    when it cannot be asked or refuses, with one warning per process for each hub and status (or error class) that
    names neither the token nor the hub's text.
    """
    hub_url = settings.FLOWFILE_HUB_URL
    if not hub_url or not settings.is_server_mode() or not token:
        return None
    if issuer_accounts.joined_with_keys() is not None:
        return None
    try:
        store = hub_client.get_exchange_store(hub_url, token)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        kind = str(e.status) if isinstance(e, HubRefused) else type(e).__name__
        if (hub_url, kind) not in _warned:
            _warned.add((hub_url, kind))
            logger.warning(
                f"Could not take the exchange store from the hub at {hub_url} ({kind}); runs use no store from the "
                "hub until it answers"
            )
        return None
    if store is None:
        return None
    return ExchangeConfig(uri=store["uri"], connection=store["connection"], max_rows=store.get("max_rows"))


def joined_server_store(user_id: int | None) -> ExchangeConfig | None:
    """The exchange store the hub of this server sets, asked with ``user_id``'s hub session; ``None`` when none applies.

    ``None`` without asking unless the storage dir joined a hub with keys of its own and ``user_id`` is given. ``None``
    too when the person has no hub session here, when the hub sets no store, and when it cannot be asked or refuses,
    with one warning per process for each hub and status as ``team_node_store`` gives. The answer's ``vended_for`` is
    ``user_id``: the store is opened by what the hub vends each run through that session, never by a connection here.
    """
    if user_id is None or issuer_accounts.joined_with_keys() is None:
        return None
    try:
        hub_url, access_token = hub_catalog.signed_in(user_id)
    except LookupError:
        logger.debug(f"User {user_id} has no hub session on this server; its runs take no exchange store from the hub")
        return None
    except (HubUnreachable, HubRefused, HubVersionRefused):
        return None
    try:
        store = hub_client.get_exchange_store(hub_url, access_token)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        kind = str(e.status) if isinstance(e, HubRefused) else type(e).__name__
        if (hub_url, kind) not in _warned:
            _warned.add((hub_url, kind))
            logger.warning(
                f"Could not take the exchange store from the hub at {hub_url} ({kind}); runs use no store from the "
                "hub until it answers"
            )
        return None
    if store is None:
        return None
    return ExchangeConfig(
        uri=store["uri"], connection=store["connection"], max_rows=store.get("max_rows"), vended_for=user_id
    )
