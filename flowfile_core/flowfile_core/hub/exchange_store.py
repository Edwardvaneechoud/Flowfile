"""The exchange store a team node takes from its hub when it has none of its own.

An organisation sets its exchange store once, on its hub. A team node (a core in server mode with
``FLOWFILE_HUB_URL``) shares the hub's database, so the S3 connection the hub's store names is a connection this core
reads too, and the crossing opens the store through it as it opens a store set here. A desktop asks nothing: its
database does not hold the hub's connection, and the credential the hub could vend for the store has no way to the
worker yet, since the exchange's export and import name a stored connection. Nor does a server that joined its hub
with keys of its own, ``FLOWFILE_HUB_URL`` set or not: its database is its own and holds no connection of the hub's,
and its people's bearer tokens are its own, which the hub does not take.

The hub is asked once per run, with the bearer token of the request that started it: every session token on a team
node is one the hub accepts, so no token is minted, cached or stored for this, and a run that no request started
(a scheduled run) takes no store from the hub. A hub that does not answer is no store, never a failed run.
"""

from flowfile_core.auth import issuer_accounts
from flowfile_core.configs import logger, settings
from flowfile_core.configs.exchange_config import ExchangeConfig
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
