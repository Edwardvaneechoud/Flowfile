# ruff: noqa: E402

import os
from importlib.metadata import PackageNotFoundError, version

from flowfile_core.utils.validate_setup import validate_setup

validate_setup()
from flowfile_core.database.init_db import init_db
from flowfile_core.flowfile.handler import FlowfileHandler

if "FLOWFILE_MODE" not in os.environ:
    os.environ["FLOWFILE_MODE"] = "electron"

init_db()


class ServerRun:
    exit: bool = False


try:
    __version__ = version("Flowfile")
except PackageNotFoundError:
    __version__ = "0.5.0"

flow_file_handler = FlowfileHandler()
