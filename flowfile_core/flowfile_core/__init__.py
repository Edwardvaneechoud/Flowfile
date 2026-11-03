# ruff: noqa: E402

import logging
import multiprocessing
import os
import sys


def _runs_as_service() -> bool:
    """Whether this process is core itself (its console script, ``python -m flowfile_core.main``, a frozen sidecar)
    rather than a program importing it as a library."""
    if getattr(sys, "frozen", False):
        return True
    argv = sys.orig_argv[:3]
    return "flowfile_core.main" in argv or any(
        os.path.splitext(os.path.basename(arg))[0] == "flowfile_core" for arg in argv
    )


def _starts_core() -> bool:
    """Whether this process serves core: core itself, or the ``flowfile`` command's ``run ui`` or ``run core``.

    Never a spawned child, which is handed its parent's command line. While ``python -m flowfile`` imports the package,
    ``sys.argv[0]`` is ``-m`` and ``sys.orig_argv`` names the module.
    """
    if multiprocessing.current_process().name != "MainProcess" or not sys.argv:
        return False
    if _runs_as_service():
        return True
    command = getattr(sys, "orig_argv", [])
    program = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    if sys.argv[0] == "-m" and "-m" in command:
        program = (command[command.index("-m") + 1 :] or [""])[0]
    return program == "flowfile" and sys.argv[1:3] in (["run", "ui"], ["run", "core"])


# The storage dir is chosen before `shared.storage_config` reads it, for this process and the ones it starts.
NODE_STORAGE_DIR = None
if _starts_core():
    from flowfile_core.configs.command_line import parse_args as _parse_args
    from shared import hub_node as _hub_node

    _args = _parse_args()
    NODE_STORAGE_DIR = _hub_node.node_storage(_args.join, _args.name)

from flowfile_core.utils.validate_setup import validate_setup
from shared._version import get_version

if _runs_as_service():
    # The migration below runs at import, so its log lines belong in the service log; a library import stays quiet.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

validate_setup()
from flowfile_core.database.init_db import init_db
from flowfile_core.flowfile.handler import FlowfileHandler

if "FLOWFILE_MODE" not in os.environ:
    os.environ["FLOWFILE_MODE"] = "electron"

if os.environ.get("FLOWFILE_SKIP_INIT_DB", "").strip().lower() not in ("1", "true", "yes", "on"):
    init_db()


class ServerRun:
    exit: bool = False


__version__ = get_version()

flow_file_handler = FlowfileHandler()
