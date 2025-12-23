from importlib.metadata import version


import os
from flowfile_core.utils.validate_setup import validate_setup
validate_setup()
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.database.init_db import init_db

os.environ["FLOWFILE_MODE"] = "electron"
init_db()


class ServerRun:
    exit: bool = False


__version__ = version("Flowfile")
flow_file_handler = FlowfileHandler()
