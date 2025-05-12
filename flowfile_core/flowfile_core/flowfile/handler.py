import time
import random
import uuid
import socket
import hashlib

from dataclasses import dataclass
from typing import Dict, List
import os
from pathlib import Path

from flowfile_core.flowfile.manage.open_flowfile import open_flow
from flowfile_core.flowfile.FlowfileFlow import FlowGraph
from flowfile_core.schemas.schemas import FlowSettings
from flowfile_core.configs import logger


def create_unique_id() -> int:
    """
    Create a unique id for the flowfile with increased randomness while maintaining 32-bit compatibility
    Returns:
        int: unique id within 32 bits (4 bytes)
    """
    # Get various entropy sources
    time_ms = int(time.time() * 1000)
    pid = os.getpid()
    random_bytes = random.getrandbits(32)
    mac_addr = uuid.getnode()  # MAC address as integer
    hostname = socket.gethostname()

    # Combine all sources into a string
    seed = f"{time_ms}-{pid}-{random_bytes}-{mac_addr}-{hostname}-{uuid.uuid4()}"

    # Create a hash of all entropy sources
    hash_obj = hashlib.md5(seed.encode())
    hash_int = int(hash_obj.hexdigest(), 16)

    # Ensure the result fits within 32 bits (4 bytes)
    unique_id = hash_int & 0xFFFFFFFF

    return unique_id


@dataclass
class FlowfileHandler:
    _flows: Dict[int, FlowGraph]

    def __init__(self):
        self._flows = {}

    @property
    def flowfile_flows(self) -> List[FlowGraph]:
        return list(self._flows.values())

    def __add__(self, other: FlowGraph) -> int:
        self._flows[other.flow_id] = other
        return other.flow_id

    def import_flow(self, flow_path: Path|str) -> int:
        if isinstance(flow_path, str):
            flow_path = Path(flow_path)
        imported_flow = open_flow(flow_path)
        self._flows[imported_flow.flow_id] = imported_flow
        imported_flow.flow_settings = self.get_flow_info(imported_flow.flow_id)
        imported_flow.flow_settings.is_running = False
        return imported_flow.flow_id

    def register_flow(self, flow_settings: FlowSettings):
        if flow_settings.flow_id in self._flows:
            self.delete_flow(flow_settings.flow_id)
            raise 'flow already registered'
        else:
            name = flow_settings.name if flow_settings.name else flow_settings.flow_id
            self._flows[flow_settings.flow_id] = FlowGraph(name=name, flow_id=flow_settings.flow_id, flow_settings=flow_settings)
        return self.get_flow(flow_settings.flow_id)

    def get_flow(self, flow_id: int) -> FlowGraph | None:
        return self._flows.get(flow_id, None)

    def delete_flow(self, flow_id: int):
        flow = self._flows.pop(flow_id)
        del flow

    def save_flow(self, flow_id: int, flow_path: str):
        flow = self.get_flow(flow_id)
        if flow:
            flow.save_flow(flow_path)
        else:
            raise Exception('Flow not found')

    def add_flow(self, name: str, flow_path: str) -> int:
        """
        Creates a new flow with a reference to the flow path
        Args:
            name (str): The name of the flow
            flow_path (str): The path to the flow file

        Returns:
            int: The flow id

        """
        next_id = create_unique_id()
        flow_info = FlowSettings(name=name, flow_id=next_id, save_location='', path=flow_path)
        _ = self.register_flow(flow_info)
        return next_id

    def get_flow_info(self, flow_id: int) -> FlowSettings:
        flow = self.get_flow(flow_id)
        if not flow:
            raise Exception(f'Flow {flow_id} not found')
        flow_exists = os.path.exists(flow.flow_settings.path)
        last_modified_ts = os.path.getmtime(flow.flow_settings.path) if flow_exists else -1
        flow.flow_settings.modified_on = last_modified_ts
        return flow.flow_settings

    def get_node(self, flow_id: int, node_id: int):
        flow = self.get_flow(flow_id)
        if not flow:
            raise Exception(f'Flow {flow_id} not found')
        node = flow.get_node(node_id)
        if not node:
            raise Exception(f'Node {node_id} not found in flow {flow_id}')
        return node
