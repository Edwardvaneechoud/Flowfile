"""Core's command line, read with the standard library alone.

``flowfile_core`` reads it at import, before anything opens the storage dir, to give a core that joins a hub a folder of
its own (``shared.hub_node.node_storage``); ``settings`` reads it for everything else.
"""

import argparse
import os

DEFAULT_SERVER_HOST = "0.0.0.0"


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Flowfile Backend Server")
    parser.add_argument("--host", type=str, default=DEFAULT_SERVER_HOST, help="Host to bind to")
    parser.add_argument(
        "--port",
        type=int,
        help="Port to bind to (63578 when not given; a core joining a hub moves to a free port when that one is "
        "in use)",
    )
    parser.add_argument(
        "--worker-port",
        type=int,
        help="Port for the worker process (63579 when not given; a core joining a hub gives its worker a free port "
        "when that one is in use)",
    )
    parser.add_argument(
        "--join",
        type=str,
        default=os.environ.get("FLOWFILE_HUB_JOIN_TOKEN") or None,
        help="A join token from the hub's Compute page; core joins that hub for itself and its worker before it serves",
    )
    parser.add_argument("--hub", type=str, default=os.environ.get("FLOWFILE_HUB_URL") or None, help="The hub to join")
    parser.add_argument(
        "--name",
        type=str,
        default=os.environ.get("FLOWFILE_NODE_NAME") or None,
        help="This node's name on the hub; the machine's name when not given",
    )
    parser.add_argument("--url", type=str, default=None, help="This core's address as a browser and the hub reach it")
    parser.add_argument("--worker-url", type=str, default=None, help="The worker's address as desktops reach it")
    args = parser.parse_known_args()[0]

    if args.join and not args.hub:
        raise ValueError("--join needs --hub, the address of the hub to join")

    return args
