from typing import Optional, List, Dict, Tuple, Any, Literal, Annotated
from pydantic import BaseModel, field_validator, ConfigDict, Field, StringConstraints
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
ExecutionModeLiteral = Literal['Development', 'Performance']
ExecutionLocationsLiteral = Literal['local', 'remote']

# Type literals for classifying nodes.
NodeTypeLiteral = Literal['input', 'output', 'process']
TransformTypeLiteral = Literal['narrow', 'wide', 'other']

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

    Attributes:
        id (Optional[int]): The unique ID of the node instance.
        type (Optional[str]): The type of the node (e.g., 'join', 'filter').
        is_setup (Optional[bool]): Whether the node has been configured.
        description (Optional[str]): A user-provided description.
        x_position (Optional[int]): The x-coordinate on the canvas.
        y_position (Optional[int]): The y-coordinate on the canvas.
        left_input_id (Optional[int]): The ID of the node connected to the left input.
        right_input_id (Optional[int]): The ID of the node connected to the right input.
        input_ids (Optional[List[int]]): A list of IDs for main input nodes.
        outputs (Optional[List[int]]): A list of IDs for nodes this node outputs to.
        setting_input (Optional[Any]): The specific settings for this node instance.
    """
    id: Optional[int] = None
    type: Optional[str] = None
    is_setup: Optional[bool] = None
    description: Optional[str] = ''
    x_position: Optional[int] = 0
    y_position: Optional[int] = 0
    left_input_id: Optional[int] = None
    right_input_id: Optional[int] = None
    input_ids: Optional[List[int]] = [-1]
    outputs: Optional[List[int]] = [-1]
    setting_input: Optional[Any] = None

    @property
    def data(self) -> Any:
        """
        Property to access the node's specific settings.
        :return: The settings of the node.
        """
        return self.setting_input

    @property
    def main_input_ids(self) -> Optional[List[int]]:
        """
        Property to access the main input node IDs.
        :return: A list of main input node IDs.
        """
        return self.input_ids


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


# Define SecretRef here if not in a common location
SecretRef = Annotated[str, StringConstraints(min_length=1, max_length=100),
                      Field(description="An ID referencing an encrypted secret.")]
