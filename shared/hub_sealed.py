"""Who this node is on its hub, for what the hub seals to it; importing this module tells ``shared.sealed``, whose names
older plans find here."""

from shared import hub_node, hub_node_key, sealed
from shared.issuer_tokens import JWKS_PATH
from shared.sealed import *  # noqa: F403


def this_server() -> tuple:
    node = hub_node.resolved()
    try:
        keys = hub_node_key.load()
    except (OSError, ValueError):
        keys = None
    return node.name, keys, node.hub_url and node.hub_url + JWKS_PATH, node.issuer, hub_node.joined() is not None


sealed.server, sealed.sealer = this_server, "hub"
