"""Who this node is on its hub, for what the hub seals to it: importing this module tells ``shared.sealed``, whose
names the hub's client and older plans find here."""

from shared import hub_node, hub_node_key, sealed
from shared.issuer_tokens import JWKS_PATH
from shared.sealed import *  # noqa: F403


def this_server() -> tuple:
    """This node's name, keys, hub's JWKS URL and issuer, and whether it joined with keys, read on every call."""
    node = hub_node.resolved()
    try:
        keys = hub_node_key.load()
    except (OSError, ValueError):
        keys = None
    return node.name, keys, node.hub_url and node.hub_url + JWKS_PATH, node.issuer, hub_node.joined() is not None


sealed.server, sealed.sealer = this_server, "hub"
