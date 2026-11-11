"""The exchange store a machine signed in to a hub takes from it; each crossing has the hub vend the store's prefix,
so no connection is looked up here. A hub that does not answer is no store, never a failed run."""

from flowfile_core.configs import logger
from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused

_warned: set[tuple[str, str]] = set()


def hub_store(user_id: int | None) -> ExchangeConfig | None:
    """The store ``user_id``'s hub sets, else ``None``; one warning per hub and status, naming neither the token nor
    the hub's text."""
    if user_id is None:
        return None
    try:
        hub_url, access_token = session.bearer(user_id, hub_catalog.NOT_CONNECTED)
    except LookupError:
        logger.debug(f"User {user_id} has no hub session here; its runs take no exchange store from a hub")
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
