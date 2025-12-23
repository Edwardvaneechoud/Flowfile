from typing import Optional, List, Dict, Tuple, Any, Literal, ClassVar
from pydantic import BaseModel, field_validator, ConfigDict, Field, ValidationInfo, field_serializer
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
from flowfile_core.schemas import input_schema
ExecutionModeLiteral = Literal['Development', 'Performance']
ExecutionLocationsLiteral = Literal['local', 'remote']

# Type literals for classifying nodes.
NodeTypeLiteral = Literal['input', 'output', 'process']
TransformTypeLiteral = Literal['narrow', 'wide', 'other']
_custom_node_store_cache = None

NODE_TYPE_TO_SETTINGS_CLASS = {
    'manual_input': input_schema.NodeManualInput,
    'filter': input_schema.NodeFilter,
    'formula': input_schema.NodeFormula,
    'select': input_schema.NodeSelect,
    'sort': input_schema.NodeSort,
    'record_id': input_schema.NodeRecordId,
    'sample': input_schema.NodeSample,
    'unique': input_schema.NodeUnique,
    'group_by': input_schema.NodeGroupBy,
    'pivot': input_schema.NodePivot,
    'unpivot': input_schema.NodeUnpivot,
    'text_to_rows': input_schema.NodeTextToRows,
    'graph_solver': input_schema.NodeGraphSolver,
    'polars_code': input_schema.NodePolarsCode,
    'join': input_schema.NodeJoin,
    'cross_join': input_schema.NodeCrossJoin,
    'fuzzy_match': input_schema.NodeFuzzyMatch,
    'record_count': input_schema.NodeRecordCount,
    'explore_data': input_schema.NodeExploreData,
    'union': input_schema.NodeUnion,
    'output': input_schema.NodeOutput,
    'read': input_schema.NodeRead,
    'database_reader': input_schema.NodeDatabaseReader,
    'database_writer': input_schema.NodeDatabaseWriter,
    'cloud_storage_reader': input_schema.NodeCloudStorageReader,
    'cloud_storage_writer': input_schema.NodeCloudStorageWriter,
    'external_source': input_schema.NodeExternalSource,
    'promise': input_schema.NodePromise,
    'user_defined': input_schema.UserDefinedNode,
}


def get_global_execution_location() -> ExecutionLocationsLiteral:
    """
    Calculates the default execution location based on the global settings
    Returns
    -------
    ExecutionLocationsLiteral where the current
    """
    if OFFLOAD_TO_WORKER:
        return "remote"
    return "local"


def _get_custom_node_store():
    """Lazy load CUSTOM_NODE_STORE once and cache it."""
    global _custom_node_store_cache
    if _custom_node_store_cache is None:
        from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
        _custom_node_store_cache = CUSTOM_NODE_STORE
    return _custom_node_store_cache


def get_settings_class_for_node_type(node_type: str):
    """Get the settings class for a node type, supporting both standard and user-defined nodes."""
    model_class = NODE_TYPE_TO_SETTINGS_CLASS.get(node_type)
    if model_class is None:
        if node_type in _get_custom_node_store():
            return input_schema.UserDefinedNode
        return None
    return model_class


def is_valid_execution_location_in_current_global_settings(execution_location: ExecutionLocationsLiteral) -> bool:
    return not (get_global_execution_location() == "local" and execution_location == "remote")


def get_prio_execution_location(local_execution_location: ExecutionLocationsLiteral,
                                global_execution_location: ExecutionLocationsLiteral) -> ExecutionLocationsLiteral:
    if local_execution_location == global_execution_location:
        return local_execution_location
    elif global_execution_location == "local" and local_execution_location == "remote":
        return "local"
    else:
        return local_execution_location


class FlowGraphConfig(BaseModel):
    """
    Configuration model for a flow graph's basic properties.

    Attributes:
        flow_id (int): Unique identifier for the flow.
        description (Optional[str]): A description of the flow.
        save_location (Optional[str]): The location where the flow is saved.
        name (str): The name of the flow.
        path (str): The file path associated with the flow.
        execution_mode (ExecutionModeLiteral): The mode of execution ('Development' or 'Performance').
        execution_location (ExecutionLocationsLiteral): The location for execution ('local', 'remote').
    """
    flow_id: int = Field(default_factory=create_unique_id, description="Unique identifier for the flow.")
    description: Optional[str] = None
    save_location: Optional[str] = None
    name: str = ''
    path: str = ''
    execution_mode: ExecutionModeLiteral = 'Performance'
    execution_location: ExecutionLocationsLiteral = Field(default_factory=get_global_execution_location)

    @field_validator('execution_location', mode='before')
    def validate_and_set_execution_location(cls, v: Optional[ExecutionLocationsLiteral]) -> ExecutionLocationsLiteral:
        """
        Validates and sets the execution location.
        1.  **If `None` is provided**: It defaults to the location determined by global settings.
        2.  **If a value is provided**: It checks if the value is compatible with the global
            settings. If not (e.g., requesting 'remote' when only 'local' is possible),
            it corrects the value to a compatible one.
        """
        if v is None:
            return get_global_execution_location()
        if v == "auto":
            return get_global_execution_location()

        return get_prio_execution_location(v, get_global_execution_location())


class FlowSettings(FlowGraphConfig):
    """
    Extends FlowGraphConfig with additional operational settings for a flow.

    Attributes:
        auto_save (bool): Flag to enable or disable automatic saving.
        modified_on (Optional[float]): Timestamp of the last modification.
        show_detailed_progress (bool): Flag to show detailed progress during execution.
        is_running (bool): Indicates if the flow is currently running.
        is_canceled (bool): Indicates if the flow execution has been canceled.
    """
    auto_save: bool = False
    modified_on: Optional[float] = None
    show_detailed_progress: bool = True
    is_running: bool = False
    is_canceled: bool = False

    @classmethod
    def from_flow_settings_input(cls, flow_graph_config: FlowGraphConfig):
        """
        Creates a FlowSettings instance from a FlowGraphConfig instance.

        :param flow_graph_config: The base flow graph configuration.
        :return: A new instance of FlowSettings with data from flow_graph_config.
        """
        return cls.model_validate(flow_graph_config.model_dump())


class RawLogInput(BaseModel):
    """
    Schema for a raw log message.

    Attributes:
        flowfile_flow_id (int): The ID of the flow that generated the log.
        log_message (str): The content of the log message.
        log_type (Literal["INFO", "ERROR"]): The type of log.
        extra (Optional[dict]): Extra context data for the log.
    """
    flowfile_flow_id: int
    log_message: str
    log_type: Literal["INFO", "ERROR"]
    extra: Optional[dict] = None


class FlowfileSettings(BaseModel):
    """Settings for flowfile serialization (YAML/JSON).

    Excludes runtime state fields like is_running, is_canceled, modified_on.
    """
    description: Optional[str] = None
    execution_mode: ExecutionModeLiteral = 'Performance'
    execution_location: ExecutionLocationsLiteral = 'local'
    auto_save: bool = False
    show_detailed_progress: bool = True


class FlowfileNode(BaseModel):
    """Node representation for flowfile serialization (YAML/JSON)."""
    id: int
    type: str
    is_start_node: bool = False
    description: Optional[str] = ''
    x_position: Optional[int] = 0
    y_position: Optional[int] = 0
    left_input_id: Optional[int] = None
    right_input_id: Optional[int] = None
    input_ids: Optional[List[int]] = Field(default_factory=list)
    outputs: Optional[List[int]] = Field(default_factory=list)
    setting_input: Optional[Any] = None

    _setting_input_exclude: ClassVar[set] = {
        'flow_id', 'node_id', 'pos_x', 'pos_y', 'is_setup',
        'description', 'user_id', 'is_flow_output', 'is_user_defined',
        'depending_on_id', 'depending_on_ids'
    }

    @field_serializer('setting_input')
    def serialize_setting_input(self, value, _info):
        if value is None:
            return None
        if isinstance(value, input_schema.NodePromise):
            return None
        if hasattr(value, 'to_yaml_dict'):
            return value.to_yaml_dict()
        if hasattr(value, 'to_yaml_dict'):
            return value.to_yaml_dict()
        return value.model_dump(exclude=self._setting_input_exclude)


class FlowfileData(BaseModel):
    """Root model for flowfile serialization (YAML/JSON)."""
    flowfile_version: str
    flowfile_id: int
    flowfile_name: str
    flowfile_settings: FlowfileSettings
    nodes: List[FlowfileNode]


class NodeTemplate(BaseModel):
    """
    Defines the template for a node type, specifying its UI and functional characteristics.

    Attributes:
        name (str): The display name of the node.
        item (str): The unique identifier for the node type.
        input (int): The number of required input connections.
        output (int): The number of output connections.
        image (str): The filename of the icon for the node.
        multi (bool): Whether the node accepts multiple main input connections.
        node_group (str): The category group the node belongs to (e.g., 'input', 'transform').
        prod_ready (bool): Whether the node is considered production-ready.
        can_be_start (bool): Whether the node can be a starting point in a flow.
    """
    name: str
    item: str
    input: int
    output: int
    image: str
    multi: bool = False
    node_type: NodeTypeLiteral
    transform_type: TransformTypeLiteral
    node_group: str
    prod_ready: bool = True
    can_be_start: bool = False
    drawer_title: str = "Node title"
    drawer_intro: str = "Drawer into"
    custom_node: Optional[bool] = False


class NodeInformation(BaseModel):
    """
    Stores the state and configuration of a specific node instance within a flow.
    """
    id: Optional[int] = None
    type: Optional[str] = None
    is_setup: Optional[bool] = None
    is_start_node: bool = False
    description: Optional[str] = ''
    x_position: Optional[int] = 0
    y_position: Optional[int] = 0
    left_input_id: Optional[int] = None
    right_input_id: Optional[int] = None
    input_ids: Optional[List[int]] = Field(default_factory=list)
    outputs: Optional[List[int]] = Field(default_factory=list)
    setting_input: Optional[Any] = None

    @property
    def data(self) -> Any:
        return self.setting_input

    @property
    def main_input_ids(self) -> Optional[List[int]]:
        return self.input_ids

    @field_validator('setting_input', mode='before')
    @classmethod
    def validate_setting_input(cls, v, info: ValidationInfo):
        if v is None:
            return None
        if isinstance(v, BaseModel):
            return v

        node_type = info.data.get('type')
        model_class = get_settings_class_for_node_type(node_type)

        if model_class is None:
            raise ValueError(f"Unknown node type: {node_type}")

        if isinstance(v, model_class):
            return v

        return model_class.model_validate(v)


class FlowInformation(BaseModel):
    """
    Represents the complete state of a flow, including settings, nodes, and connections.

    Attributes:
        flow_id (int): The unique ID of the flow.
        flow_name (Optional[str]): The name of the flow.
        flow_settings (FlowSettings): The settings for the flow.
        data (Dict[int, NodeInformation]): A dictionary mapping node IDs to their information.
        node_starts (List[int]): A list of starting node IDs.
        node_connections (List[Tuple[int, int]]): A list of tuples representing connections between nodes.
    """
    flow_id: int
    flow_name: Optional[str] = ''
    flow_settings: FlowSettings
    data: Dict[int, NodeInformation] = {}
    node_starts: List[int]
    node_connections: List[Tuple[int, int]] = []

    @field_validator('flow_name', mode="before")
    def ensure_string(cls, v):
        """
        Validator to ensure the flow_name is always a string.
        :param v: The value to validate.
        :return: The value as a string, or an empty string if it's None.
        """
        return str(v) if v is not None else ''


class NodeConnection(BaseModel):
    """
    Represents a connection between two nodes in the flow.

    Attributes:
        from_node_id (int): The ID of the source node.
        to_node_id (int): The ID of the target node.
    """
    model_config = ConfigDict(frozen=True)
    from_node_id: int
    to_node_id: int


class NodeInput(NodeTemplate):
    """
    Represents a node as it is received from the frontend, including position.

    Attributes:
        id (int): The unique ID of the node instance.
        pos_x (float): The x-coordinate on the canvas.
        pos_y (float): The y-coordinate on the canvas.
    """
    id: int
    pos_x: float
    pos_y: float


class NodeEdge(BaseModel):
    """
    Represents a connection (edge) between two nodes in the frontend.

    Attributes:
        id (str): A unique identifier for the edge.
        source (str): The ID of the source node.
        target (str): The ID of the target node.
        targetHandle (str): The specific input handle on the target node.
        sourceHandle (str): The specific output handle on the source node.
    """
    model_config = ConfigDict(coerce_numbers_to_str=True)
    id: str
    source: str
    target: str
    targetHandle: str
    sourceHandle: str


class VueFlowInput(BaseModel):
    """

    Represents the complete graph structure from the Vue-based frontend.

    Attributes:
        node_edges (List[NodeEdge]): A list of all edges in the graph.
        node_inputs (List[NodeInput]): A list of all nodes in the graph.
    """
    node_edges: List[NodeEdge]
    node_inputs: List[NodeInput]


class NodeDefault(BaseModel):
    """
    Defines default properties for a node type.

    Attributes:
        node_name (str): The name of the node.
        node_type (NodeTypeLiteral): The functional type of the node ('input', 'output', 'process').
        transform_type (TransformTypeLiteral): The data transformation behavior ('narrow', 'wide', 'other').
        has_default_settings (Optional[Any]): Indicates if the node has predefined default settings.
    """
    node_name: str
    node_type: NodeTypeLiteral
    transform_type: TransformTypeLiteral
    has_default_settings: Optional[Any] = None
