# ruff: noqa: E402

import logging
import os
import sys

from flowfile_core.utils.validate_setup import validate_setup
from shared._version import get_version


def _runs_as_service() -> bool:
    """Whether this process is core itself (its console script, ``python -m flowfile_core.main``, a frozen sidecar)
    rather than a program importing it as a library."""
    if getattr(sys, "frozen", False):
        return True
    argv = sys.orig_argv[:3]
    return "flowfile_core.main" in argv or any(
        os.path.splitext(os.path.basename(arg))[0] == "flowfile_core" for arg in argv
    )


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
