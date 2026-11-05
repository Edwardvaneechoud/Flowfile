"""Core's command line, read with the standard library alone.

``flowfile_core`` reads it at import, before anything opens the storage dir, to refuse a core that joins a hub without a
storage dir of its own (``shared.hub_node.require_own_storage_dir``); ``settings`` reads it for everything else.
"""

import argparse
import os

DEFAULT_SERVER_HOST = "0.0.0.0"


def _server_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", type=str, default=DEFAULT_SERVER_HOST, help="Host to bind to")
    parser.add_argument(
        "--port",
        type=int,
        help="Port to bind to (63578 when not given; a core joining a hub exits when that port is in use)",
    )
    parser.add_argument(
        "--worker-port",
        type=int,
        help="Port for the worker process (63579 when not given)",
    )


def _join_flags(parser: argparse.ArgumentParser) -> None:
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


def parse_args():
    """Parse command line arguments.

    Every process reads ``--host``, ``--port`` and ``--worker-port``, abbreviated or not, as it always has. Only one
    that serves core (``flowfile_core._starts_core``) reads the join flags, by their full names only, and refuses
    ``--join`` without ``--hub``: a program that imports Flowfile keeps its own command line, ``--join`` or ``--worker``
    included.
    """
    import flowfile_core

    serves = flowfile_core._starts_core()
    parser = argparse.ArgumentParser(description="Flowfile Backend Server", add_help=not serves)
    _server_flags(parser)
    joining = argparse.Namespace(join=None, hub=None, name=None, url=None, worker_url=None)
    if serves:
        full = argparse.ArgumentParser(description="Flowfile Backend Server", parents=[parser], allow_abbrev=False)
        _join_flags(full)
        joining = full.parse_known_args()[0]
    args = parser.parse_known_args()[0]
    for field in ("join", "hub", "name", "url", "worker_url"):
        setattr(args, field, getattr(joining, field))

    if args.join and not args.hub:
        raise ValueError("--join needs --hub, the address of the hub to join")

    return args
