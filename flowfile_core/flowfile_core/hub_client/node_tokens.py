"""The tokens a connected desktop holds for the hub's compute nodes: one per node, an hour at a time.

A team node accepts a token the hub signs for it (its name as the audience, scope ``run``). The desktop asks the hub
for one the first time it uses a node and keeps it in this process's memory until it is within a minute of its
``exp``, then asks again. The cache is one dict keyed by local user and hub node, cleared with the user's hub
session (``session.clear``); no token is ever stored. A token's expiry is its own ``exp`` claim, read unverified as
``session`` reads its own: the node verifies the signature, this side only needs to know when to ask again.

On a server that joined its hub with keys of its own, each person signs in through the hub and a run's token is minted
with the delegated session of the person the run is for (``acting_for``), never with another's: the hub's ledger then
names who ran, and one person's sign-out ends only their own tokens. ``token_for(..., through_hub=True)`` mints only
through such a sign-in, and says to sign in through the hub again when there is none (``SignedOut``). The editor's
calls (a schema prediction, a preview, a node fetch) are made for the person signed in to the editor in the same way.
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
    """No token for the hub's server ``node_name``: the person the call is made for has no sign-in through the hub here,
    or there is no person behind a run. Its message is ``signed_out``'s sentence, which a refused node or editor call
    shows with a capital first letter and a full stop."""

    def __init__(self, node_name: str) -> None:
        sentence = signed_out(node_name)
        super().__init__(sentence)
        self.shown = f"{sentence[0].upper()}{sentence[1:]}."


def signed_in_through_hub(user_id: int) -> bool:
    """Whether the hub session of ``user_id`` here is a sign-in through the hub (``kind: "delegation"``), read
    without asking the hub."""
    stored = session.load(user_id)
    return stored is not None and stored.get("kind") == "delegation"


def signed_out(node_name: str) -> str:
    """Why a run on the hub's server ``node_name`` is refused to a person with no sign-in through the hub, and what to
    do about it."""
    return (
        f"you are not signed in through the hub, so its server {node_name!r} cannot run this flow for you; "
        f"{SIGN_IN_AGAIN}"
    )


def token_for(user_id: int, node_id: int, node_name: str, *, through_hub: bool = False) -> str:
    """A ``run`` token for the hub node ``node_id``, named ``node_name``, minted through ``user_id``'s hub session.

    The token cached for that user and node is answered while it is more than a minute from its ``exp``; otherwise
    a new one is minted and cached in its place. Nothing is minted for a node that is not used. With ``through_hub``
    only a sign-in through the hub (``kind: "delegation"``) mints one.

    Raises:
        LookupError: ``user_id`` has no hub session, or the hub no longer accepts it (the message says to connect to
            the hub again; with ``through_hub``, it is a ``SignedOut``); the hub did not answer; or the hub
            refused the node, with its reason.
    """
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
    """The token of a call to the hub's server ``name``, through the hub session of the row's user ``account_user_id``.

    With ``through_hub`` (a server joined with keys of its own) it is the token of the person the call is made for
    (``acting``), minted through their sign-in through the hub. With no such person, a call inside a run or a fetch
    (``context.entered``) is refused with a ``SignedOut``, and any other call takes the row's user's token.
    """
    person = acting() if through_hub else None
    if person is not None:
        return token_for(person, remote_id, name, through_hub=True)
    if through_hub and context.entered():
        raise SignedOut(name)
    return token_for(account_user_id, remote_id, name)


def run_refused(on_servers) -> list | None:
    """The run's nodes on the hub's other servers (``on_servers()``, by server name), each with the sentence that names
    them and what to do, when the person the run is for has no sign-in through the hub here, else ``None``: the nodes
    on the first such server, or every such node of a schedule's run, which has nobody behind it."""
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
    """Forget every node token minted through ``user_id``'s hub session."""
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
    """``_mint`` for a person who must have signed in through the hub; every way that sign-in is missing or over is
    ``signed_out``'s sentence."""
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
