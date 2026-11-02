"""The tokens a connected desktop holds for the hub's compute nodes: one per node, an hour at a time.

A team node accepts a token the hub signs for it (its name as the audience, scope ``run``). The desktop asks the hub
for one the first time it uses a node and keeps it in this process's memory until it is within a minute of its
``exp``, then asks again. The cache is one dict keyed by local user and hub node, cleared with the user's hub
session (``session.clear``); no token is ever stored. A token's expiry is its own ``exp`` claim, read unverified as
``session`` reads its own: the node verifies the signature, this side only needs to know when to ask again.
"""

import threading
import time

from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused

SCOPE = "run"
"""The one scope a desktop asks for; ``dispatch`` is the hub's own."""
RECONNECT = "connect to the hub again"
_MARGIN_SECONDS = 60

_tokens: dict[tuple[int, int], tuple[str, float | None]] = {}
_lock = threading.Lock()


def token_for(user_id: int, node_id: int, node_name: str) -> str:
    """A ``run`` token for the hub node ``node_id``, named ``node_name``, minted through ``user_id``'s hub session.

    The token cached for that user and node is answered while it is more than a minute from its ``exp``; otherwise
    a new one is minted and cached in its place. Nothing is minted for a node that is not used.

    Raises:
        LookupError: ``user_id`` has no hub session, or the hub no longer accepts it (the message says to connect to
            the hub again); the hub did not answer; or the hub refused the node, with its reason.
    """
    with _lock:
        cached = _tokens.get((user_id, node_id))
        if cached is not None:
            token, exp = cached
            if exp is not None and exp - time.time() > _MARGIN_SECONDS:
                return token
        token = _mint(user_id, node_id, node_name)
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
        minted = client.mint_node_token(stored["hub_url"], access_token, node_id, SCOPE)
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
