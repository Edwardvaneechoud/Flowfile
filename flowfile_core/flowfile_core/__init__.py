import flowfile_core.configs
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.database.init_db import init_db

init_db()

class ServerRun:
    exit: bool = False


flow_file_handler = FlowfileHandler()
