from dataclasses import dataclass
from typing import Dict, List
import os
from pathlib import Path

from flowfile_core.flowfile.manage.open_flowfile import open_flow
from flowfile_core.flowfile.FlowfileFlow import EtlGraph
from flowfile_core.schemas.schemas import FlowSettings


@dataclass
class FlowfileHandler:
    _flows: Dict[int, EtlGraph]

    def __init__(self):
        self._flows = {}

    @property
    def flowfile_flows(self) -> List[EtlGraph]:
        return list(self._flows.values())

    def __add__(self, other: EtlGraph) -> int:
        _id = len(self._flows) + 1
        self._flows[_id] = other
        return _id

    def import_flow(self, flow_path: Path) -> int:
        imported_flow = open_flow(flow_path)
        new_id = len(self._flows) + 1  # to ensure there is no overlap
        existing_flow = self._flows.get(imported_flow.flow_id)
        if existing_flow:
            existing_flow.flow_id = new_id
            self._flows[new_id] = existing_flow
        self._flows[imported_flow.flow_id] = imported_flow
        imported_flow.flow_settings = self.get_flow_info(imported_flow.flow_id)
        return imported_flow.flow_id

    def register_flow(self, flow_settings: FlowSettings):
        if flow_settings.flow_id in self._flows:
            raise Exception('flow already registered')
        else:
            name = flow_settings.name if flow_settings.name else flow_settings.flow_id
            self._flows[flow_settings.flow_id] = EtlGraph(name=name, flow_id=flow_settings.flow_id, flow_settings=flow_settings)
        return self.get_flow(flow_settings.flow_id)

    def get_flow(self, flow_id: int) -> EtlGraph | None:
        return self._flows.get(flow_id, None)

    def delete_flow(self, flow_id: int):
        self._flows.pop(flow_id)

    def save_flow(self, flow_id: int, flow_path: str):
        flow = self.get_flow(flow_id)
        if flow:
            flow.save_flow(flow_path)
        else:
            raise Exception('Flow not found')

    def add_flow(self, name: str, flow_path: str) -> int | str:
        # next_id = max(self._flows.keys(), default=0) + 1
        next_id = 1
        flow_info = FlowSettings(name=name, flow_id=next_id, save_location='saved_flows', path=flow_path)
        flows = [(flow_id, flow) for flow_id, flow in self._flows.items()]
        for flow_id, flow in flows:
            flow.save_flow(flow_path)
            self.delete_flow(flow_id)
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
