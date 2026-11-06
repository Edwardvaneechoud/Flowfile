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
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused

RECONNECT = "connect to the hub again"
SIGN_IN_AGAIN = "sign in through the hub again"
SCHEDULED = "Scheduled runs can't use the hub's servers yet, so place those steps on this server"
_MARGIN_SECONDS = 60

_tokens: dict[tuple[int, int], tuple[str, float | None]] = {}
_lock = threading.RLock()  # a refresh inside token_for that ends the session clears these tokens
_acting: ContextVar[int | None] = ContextVar("hub_acting_user", default=None)
_scheduled: ContextVar[bool] = ContextVar("hub_scheduled_run", default=False)


@contextmanager
def acting_for(user_id: int | None) -> Iterator[None]:
    """Make ``user_id`` the person the calls inside the block are made for, as a run route does around its run.

    A run's stage threads run under a copy of the context, so they see it too.
    """
    token = _acting.set(user_id)
    try:
        yield
    finally:
        _acting.reset(token)


def acting() -> int | None:
    """The person ``acting_for`` set for the calls made now; ``None`` outside such a block."""
    return _acting.get()


@contextmanager
def for_run_record(run_id: int | None) -> Iterator[None]:
    """The block a headless runner runs the run record ``run_id`` in: a schedule's run (its record names a schedule)
    is marked so (``scheduled``), since no person is behind it to mint the hub's tokens with."""
    from shared.run_completion import get_run_schedule_id

    token = _scheduled.set(run_id is not None and get_run_schedule_id(run_id) is not None)
    try:
        yield
    finally:
        _scheduled.reset(token)


def scheduled() -> bool:
    """Whether the calls made now are a schedule's run (``for_run_record``)."""
    return _scheduled.get()


class SignedOut(LookupError):
    """No token for the hub's server ``node_name``: the person the call is made for has no sign-in through the hub here,
    or there is no person behind a run. Its message is ``signed_out``'s sentence."""

    def __init__(self, node_name: str) -> None:
        super().__init__(signed_out(node_name))
        self.node_name = node_name


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


def refused(node_name: str) -> str:
    """``signed_out``'s sentence as a refused node or editor call shows it: a capital first letter, a full stop."""
    sentence = signed_out(node_name)
    return f"{sentence[0].upper()}{sentence[1:]}."


def refusal(error: BaseException) -> str | None:
    """``refused``'s sentence when ``error`` is a ``SignedOut`` or was raised from one, however it was wrapped on the
    way (a backend's ``TaskFailed``, a route's ``HTTPException``); else ``None``."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        if isinstance(current, SignedOut):
            return refused(current.node_name)
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return None


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
