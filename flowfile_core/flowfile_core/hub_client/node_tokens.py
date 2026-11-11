"""The ``run`` tokens the hub signs for its compute nodes, cached in memory per local user and node until a minute
before ``exp`` and cleared with the user's hub session; never stored.

On a server joined with keys of its own a token is minted only with the sign-in through the hub of the person the call
is made for (``acting_for``), never another's, so the hub's ledger names who ran and one sign-out ends only their
tokens.
"""

import threading
import time

from flowfile_core.flowfile.execution import context, registry
from flowfile_core.flowfile.execution.registry import acting, acting_for, scheduled  # noqa: F401
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused

RECONNECT = "connect to the hub again"
SIGN_IN_AGAIN = "sign in through the hub again"
SCHEDULED = "Scheduled runs can't use the hub's servers yet"
_MARGIN_SECONDS = 60

_tokens: dict[tuple[int, int], tuple[str, float | None]] = {}
_lock = threading.RLock()  # a refresh inside token_for that ends the session clears these tokens


class SignedOut(registry.Refused):
    """No token for the hub's server ``node_name``: no sign-in through the hub for the person, or nobody behind the
    run. ``shown`` is the sentence capitalised with a full stop."""

    def __init__(self, node_name: str) -> None:
        sentence = signed_out(node_name)
        super().__init__(sentence)
        self.shown = f"{sentence[0].upper()}{sentence[1:]}."


def signed_in_through_hub(user_id: int) -> bool:
    stored = session.load(user_id)
    return stored is not None and stored.get("kind") == "delegation"


def signed_out(node_name: str) -> str:
    return (
        f"you are not signed in through the hub, so its server {node_name!r} cannot run this flow for you; "
        f"{SIGN_IN_AGAIN}"
    )


def token_for(user_id: int, node_id: int, node_name: str, *, through_hub: bool = False) -> str:
    """A cached or newly minted ``run`` token for the hub node ``node_id``; with ``through_hub`` only a sign-in through
    the hub mints one. Every failure is a ``LookupError`` sentence (``SignedOut`` with ``through_hub``)."""
    with _lock:
        cached = _tokens.get((user_id, node_id))
        if cached is not None:
            token, exp = cached
            if exp is not None and exp - time.time() > _MARGIN_SECONDS:
                return token
        mint = _mint_through_hub if through_hub else _mint
        token = mint(user_id, node_id, node_name)
        _tokens[(user_id, node_id)] = (token, session._expiry(token))
        return token


def server_token(name: str, account_user_id: int, remote_id: int, through_hub: bool) -> str:
    """The token of a call to the hub's server ``name``: with ``through_hub``, the acting person's; with nobody acting
    inside a run, ``SignedOut``; otherwise the row's user's."""
    person = acting() if through_hub else None
    if person is not None:
        return token_for(person, remote_id, name, through_hub=True)
    if through_hub and context.entered():
        raise SignedOut(name)
    return token_for(account_user_id, remote_id, name)


def run_refused(on_servers) -> list | None:
    """``(node, sentence)`` for the run's nodes on the hub's servers when the acting person has no sign-in through the
    hub, else ``None``: the first server's nodes, or every such node of a schedule's run."""
    from flowfile_core.flowfile.execution.placement import run_here, steps_in_words

    person = acting()
    if person is not None and signed_in_through_hub(person):
        return None
    on_hub = on_servers()
    if not on_hub:
        return None
    if person is None and scheduled():
        steps = [node for on in on_hub.values() for node in on]
        return [(node, f"{SCHEDULED}, so {run_here(steps_in_words(steps), len(steps))}.") for node in steps]
    name, on = next(iter(on_hub.items()))
    sentence = (
        f"You are not signed in through the hub, so its server {name!r} cannot run {steps_in_words(on)} for "
        f"you; {SIGN_IN_AGAIN}, or {run_here('it' if len(on) == 1 else 'them', len(on))}."
    )
    return [(node, sentence) for node in on]


registry.remote_servers.token, registry.remote_servers.run_refused = server_token, run_refused


def clear(user_id: int) -> None:
    with _lock:
        for key in [key for key in _tokens if key[0] == user_id]:
            del _tokens[key]


def _mint(user_id: int, node_id: int, node_name: str) -> str:
    stored = session.load(user_id)
    if stored is None:
        raise LookupError(f"no hub session mints a token for node {node_name!r}; {RECONNECT}")
    try:
        access_token = session.access_token(user_id)
        if access_token is None:
            raise LookupError(f"no hub session mints a token for node {node_name!r}; {RECONNECT}")
        minted = client.mint_node_token(stored["hub_url"], access_token, node_id)
    except HubRefused as e:
        if e.status == 401:
            raise LookupError(
                f"the hub session that mints tokens for node {node_name!r} has expired; {RECONNECT}"
            ) from e
        raise LookupError(f"the hub refused a token for node {node_name!r}: {e.detail}") from e
    except HubVersionRefused as e:
        raise LookupError(f"the hub refused a token for node {node_name!r}: {e.detail}") from e
    except HubUnreachable as e:
        raise LookupError(f"no token for node {node_name!r}: {e}") from e
    return minted["token"]


def _mint_through_hub(user_id: int, node_id: int, node_name: str) -> str:
    stored = session.load(user_id)
    if stored is None or stored.get("kind") != "delegation":
        raise SignedOut(node_name)
    try:
        access_token = session.access_token(user_id)
        if access_token is None:
            raise SignedOut(node_name)
        minted = client.mint_node_token(stored["hub_url"], access_token, node_id)
    except HubRefused as e:
        if e.status == 401 or session._ended(e) or session.load(user_id) is None:
            raise SignedOut(node_name) from e
        raise LookupError(f"the hub refused a token for node {node_name!r}: {e.detail}") from e
    except HubVersionRefused as e:
        raise LookupError(f"the hub refused a token for node {node_name!r}: {e.detail}") from e
    except HubUnreachable as e:
        raise LookupError(f"no token for node {node_name!r}: {e}") from e
    return minted["token"]
