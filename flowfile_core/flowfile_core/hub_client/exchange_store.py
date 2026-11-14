"""The exchange store a machine signed in to a hub takes from it; each crossing has the hub vend the store's prefix,
so no connection is looked up here. A hub that does not answer is no store, never a failed run."""

from flowfile_core.configs import logger
from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context
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


def store_for_run(flow, user_id: int | None) -> ExchangeConfig | None:
    """The hub's exchange store for a run of ``flow`` on a machine signed in to a hub, when one could apply.

    A flow that runs whole on this machine's own worker, and that uses no connection or secret of the hub, crosses
    nothing, so the hub is not asked for it. A flow set to another backend, or with a step set to one, may cross: a
    step that backend cannot run, such as a read of a file on this machine, moves back here through the store.
    """
    from flowfile_core.flowfile.execution.placement import SingleBackendPolicy, unmoved_without_store

    if user_id is None or session.load(user_id) is None:
        return None
    run_backend_id = current_context(flow).backend_id
    elsewhere = run_backend_id.startswith(registry.CONFIGURED_WORKER_PREFIX)
    if not elsewhere and not unmoved_without_store(flow.nodes, SingleBackendPolicy(run_backend_id)):
        if not flow.uses_hub_secrets():
            return None
    return hub_store(user_id)
