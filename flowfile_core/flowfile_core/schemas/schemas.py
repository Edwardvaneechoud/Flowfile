from pydantic import BaseModel, field_validator, ConfigDict
from typing import List, Dict, Tuple, Iterable, Optional, Any, Literal


ExecutionModeLiteral = Literal['Development', 'Performance']
ExecutionLocationsLiteral = Literal['auto', 'local', 'remote']


class FlowSettings(BaseModel):
    flow_id: int
    description: Optional[str] = None
    save_location: Optional[str] = None
    auto_save: bool = False
    name: str = ''
    modified_on: Optional[float] = None
    path: str
    execution_mode: ExecutionModeLiteral = 'Performance'
    show_detailed_progress: bool = True
    is_running: bool = False
    is_canceled: bool = False
    execution_location: ExecutionLocationsLiteral = "auto"


class RawLogInput(BaseModel):
    flowfile_flow_id: int
    log_message: str
    log_type: Literal["INFO", "ERROR"]
    extra: Optional[dict] = None


class NodeTemplate(BaseModel):
    name: str
    item: str
    input: int
    output: int
    image: str
    multi: bool = False
    node_group: str
    prod_ready: bool = True
    can_be_start: bool = False


class NodeInformation(BaseModel):
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
    def data(self):
        return self.setting_input

    @property
    def main_input_ids(self):
        return self.input_ids


class FlowInformation(BaseModel):
    flow_id: int
    flow_name: Optional[str] = ''
    flow_settings: FlowSettings
    data: Dict[int, NodeInformation] = {}
    node_starts: List[int]
    node_connections: List[Tuple[int, int]] = []

    @field_validator('flow_name', mode="before")
    def ensure_string(cls, v):
        return str(v) if v is not None else ''


class NodeInput(NodeTemplate):
    id: int
    pos_x: float
    pos_y: float


class NodeEdge(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    id: str
    source: str
    target: str
    targetHandle: str
    sourceHandle: str


class VueFlowInput(BaseModel):
    node_edges: List[NodeEdge]
    node_inputs: List[NodeInput]


NodeTypeLiteral = Literal['input', 'output', 'process']
TransformTypeLiteral = Literal['narrow', 'wide', 'other']


class NodeDefault(BaseModel):
    node_name: str
    node_type: NodeTypeLiteral
    transform_type: TransformTypeLiteral
    has_default_settings: Optional[Any] = None
