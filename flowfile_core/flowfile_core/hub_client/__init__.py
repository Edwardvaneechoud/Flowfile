"""A desktop's connection to a Flowfile hub.

A desktop keeps its own core, worker, catalog and local identity. Connecting it to a hub signs in there once, registers
this machine as a device, and keeps the hub's refresh token in the local secure store beside that identity, never in
its place. ``client`` speaks the hub's HTTP contract; ``session`` keeps the stored connection and the access tokens
minted from it; ``node_tokens`` keeps the short-lived tokens the hub signs for its compute nodes. On a server that
joined a hub, ``delegation`` trades a person's sign-in through the hub for the tokens the server acts for them with,
kept in the same per-user ``session`` store. What core sends a hub lives here too: ``report`` the record of a finished
run, ``exchange_store`` the ask for the store a joined server's hub sets. ``connections`` and ``vend`` are imported
here so the hub is core's provider of credentials and its remote catalog wherever any of this package is: a served
core, a run's own process, a library.
"""

from flowfile_core.hub_client import connections as connections
from flowfile_core.hub_client import vend as vend
