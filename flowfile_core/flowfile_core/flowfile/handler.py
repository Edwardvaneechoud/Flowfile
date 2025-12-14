
from dataclasses import dataclass
from typing import Dict, List
import os
from pathlib import Path
from datetime import datetime

from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas.schemas import FlowSettings
from flowfile_core.flowfile.utils import create_unique_id
from shared.storage_config import storage


def get_flow_save_location(flow_name: str) -> Path:
    """Gets the initial save location for flow files"""
    if ".yaml" not in flow_name and ".yml" not in flow_name:
        flow_name += ".yaml"
    return storage.temp_directory_for_flows / flow_name


def create_flow_name() -> str:
    """Creates a unique flow name"""
    return datetime.now().strftime("%Y%m%d_%H_%M_%S")+"_flow.yaml"


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

    def import_flow(self, flow_path: Path | str) -> int:
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
            self._flows[flow_settings.flow_id] = FlowGraph(name=name, flow_settings=flow_settings)
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

    def add_flow(self, name: str = None, flow_path: str = None) -> int:
        """
        Creates a new flow with a reference to the flow path
        Args:
            name (str): The name of the flow
            flow_path (str): The path to the flow file

        Returns:
            int: The flow id

        """
        next_id = create_unique_id()
        if not name:
            name = create_flow_name()
        if not flow_path:
            flow_path = get_flow_save_location(name)
        flow_info = FlowSettings(name=name, flow_id=next_id, save_location=str(flow_path), path=str(flow_path))
        flow = self.register_flow(flow_info)
        flow.save_flow(flow.flow_settings.path)
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
