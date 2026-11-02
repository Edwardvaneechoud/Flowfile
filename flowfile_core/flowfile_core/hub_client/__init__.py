"""A desktop's connection to a Flowfile hub.

A desktop keeps its own core, worker, catalog and local identity. Connecting it to a hub signs in there once, registers
this machine as a device, and keeps the hub's refresh token in the local secure store beside that identity, never in
its place. ``client`` speaks the hub's HTTP contract; ``session`` keeps the stored connection and the access tokens
minted from it; ``node_tokens`` keeps the short-lived tokens the hub signs for its compute nodes.
"""

HUB_CONTRACT_VERSION = "0.1.0"
"""The version of the hub's contract this client was written against."""
