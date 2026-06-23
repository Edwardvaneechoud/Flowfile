# ruff: noqa: E402

import os

from flowfile_core.utils.validate_setup import validate_setup
from shared._version import get_version

validate_setup()
from flowfile_core.database.init_db import init_db
from flowfile_core.flowfile.handler import FlowfileHandler

if "FLOWFILE_MODE" not in os.environ:
    os.environ["FLOWFILE_MODE"] = "electron"

init_db()


class ServerRun:
    exit: bool = False


__version__ = get_version()

flow_file_handler = FlowfileHandler()
