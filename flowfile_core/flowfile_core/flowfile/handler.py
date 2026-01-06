import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.schemas.schemas import FlowSettings
from shared.storage_config import storage


def get_flow_save_location(flow_name: str) -> Path:
    """Gets the initial save location for flow files"""
    if ".yaml" not in flow_name and ".yml" not in flow_name:
        flow_name += ".yaml"
    return storage.temp_directory_for_flows / flow_name


def create_flow_name() -> str:
    """Creates a unique flow name"""
    return datetime.now().strftime("%Y%m%d_%H_%M_%S") + "_flow.yaml"


@dataclass
class FlowfileHandler:
    _flows: dict[int, FlowGraph]
    _user_sessions: dict[int, set[int]]  # Maps user_id -> set of flow_ids

    def __init__(self):
        self._flows = {}
        self._user_sessions = {}

    @property
    def flowfile_flows(self) -> list[FlowGraph]:
        return list(self._flows.values())

    def _register_user_session(self, user_id: int | None, flow_id: int):
        """Register a flow as belonging to a user's session."""
        if user_id is not None:
            if user_id not in self._user_sessions:
                self._user_sessions[user_id] = set()
            self._user_sessions[user_id].add(flow_id)

    def _unregister_user_session(self, user_id: int | None, flow_id: int):
        """Remove a flow from a user's session."""
        if user_id is not None and user_id in self._user_sessions:
            self._user_sessions[user_id].discard(flow_id)

    def get_user_flows(self, user_id: int | None) -> list[FlowGraph]:
        """Get all flows belonging to a specific user's session."""
        if user_id is None:
            return self.flowfile_flows
        user_flow_ids = self._user_sessions.get(user_id, set())
        return [f for f in self._flows.values() if f.flow_id in user_flow_ids]

    def user_has_flow(self, user_id: int | None, flow_id: int) -> bool:
        """Check if a user has access to a specific flow."""
        if user_id is None:
            return flow_id in self._flows
        return flow_id in self._user_sessions.get(user_id, set())

    def __add__(self, other: FlowGraph) -> int:
        self._flows[other.flow_id] = other
        return other.flow_id

    def import_flow(self, flow_path: Path | str, user_id: int | None = None) -> int:
        if isinstance(flow_path, str):
            flow_path = Path(flow_path)
        imported_flow = open_flow(flow_path)
        self._flows[imported_flow.flow_id] = imported_flow
        imported_flow.flow_settings = self.get_flow_info(imported_flow.flow_id)
        imported_flow.flow_settings.is_running = False
        self._register_user_session(user_id, imported_flow.flow_id)
        return imported_flow.flow_id

    def register_flow(self, flow_settings: FlowSettings, user_id: int | None = None) -> FlowGraph:
        """Register a flow with the handler and associate it with a user session."""
        if flow_settings.flow_id in self._flows:
            self.delete_flow(flow_settings.flow_id)
            raise ValueError("Flow already registered")
        name = flow_settings.name if flow_settings.name else str(flow_settings.flow_id)
        self._flows[flow_settings.flow_id] = FlowGraph(name=name, flow_settings=flow_settings)
        self._register_user_session(user_id, flow_settings.flow_id)
        return self.get_flow(flow_settings.flow_id)

    def get_flow(self, flow_id: int, user_id: int | None = None) -> FlowGraph | None:
        """Get a flow by ID, optionally checking user access."""
        flow = self._flows.get(flow_id, None)
        if flow and user_id is not None:
            # Only return the flow if user has access
            if not self.user_has_flow(user_id, flow_id):
                return None
        return flow

    def delete_flow(self, flow_id: int, user_id: int | None = None):
        """Remove flow from user's session. Flow data remains until all users close it."""
        if user_id is not None:
            if not self.user_has_flow(user_id, flow_id):
                raise Exception(f"Flow {flow_id} not found in user's session")
            self._unregister_user_session(user_id, flow_id)
            # Check if any user still has this flow open
            flow_still_open = any(flow_id in flows for flows in self._user_sessions.values())
            if not flow_still_open and flow_id in self._flows:
                flow = self._flows.pop(flow_id)
                del flow
        else:
            # No user context - delete directly
            if flow_id in self._flows:
                flow = self._flows.pop(flow_id)
                del flow

    def save_flow(self, flow_id: int, flow_path: str, user_id: int | None = None):
        flow = self.get_flow(flow_id, user_id)
        if flow:
            flow.save_flow(flow_path)
        else:
            raise Exception("Flow not found or not accessible by user")

    def add_flow(self, name: str = None, flow_path: str = None, user_id: int | None = None) -> int:
        """
        Creates a new flow with a reference to the flow path
        Args:
            name (str): The name of the flow
            flow_path (str): The path to the flow file
            user_id (int): The ID of the user creating the flow

        Returns:
            int: The flow id

        """
        next_id = create_unique_id()
        if not name:
            name = create_flow_name()
        if not flow_path:
            flow_path = get_flow_save_location(name)
        flow_info = FlowSettings(name=name, flow_id=next_id, save_location=str(flow_path), path=str(flow_path))
        flow = self.register_flow(flow_info, user_id=user_id)
        flow.save_flow(flow.flow_settings.path)
        return next_id

    def get_flow_info(self, flow_id: int) -> FlowSettings:
        flow = self.get_flow(flow_id)
        if not flow:
            raise Exception(f"Flow {flow_id} not found")
        flow_exists = os.path.exists(flow.flow_settings.path)
        last_modified_ts = os.path.getmtime(flow.flow_settings.path) if flow_exists else -1
        flow.flow_settings.modified_on = last_modified_ts
        return flow.flow_settings

    def get_node(self, flow_id: int, node_id: int):
        flow = self.get_flow(flow_id)
        if not flow:
            raise Exception(f"Flow {flow_id} not found")
        node = flow.get_node(node_id)
        if not node:
            raise Exception(f"Node {node_id} not found in flow {flow_id}")
        return node
