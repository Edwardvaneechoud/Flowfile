
import pyarrow as pa
from typing import List, Union, Callable, Optional

from flowfile_core.flowfile.flowfile_table.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.schemas import schemas
from dataclasses import dataclass


@dataclass
class NodeStepPromise:
    node_id: Union[str, int]
    name: str
    is_start: bool
    leads_to_id: Optional[List[Union[str, int]]] = None
    left_input: Optional[Union[str, int]] = None
    right_input: Optional[Union[str, int]] = None
    depends_on: Optional[List[Union[str, int]]] = None


class NodeStepStats:
    error: str = None
    has_run: bool = False
    active: bool = True


class NodeStepSettings:
    cache_results: bool = False
    renew_schema: bool = True
    streamable: bool = True
    setup_errors: bool = False
    execute_location: schemas.ExecutionLocationsLiteral = 'auto'


class NodeStepInputs:
    left_input: "NodeStep" = None
    right_input: "NodeStep" = None
    main_inputs: List["NodeStep"] = None

    @property
    def input_ids(self) -> List[int]:
        if self.main_inputs is not None:
            return [node_input.node_information.id for node_input in self.get_all_inputs()]

    def get_all_inputs(self) -> List["NodeStep"]:
        main_inputs = self.main_inputs or []
        return [v for v in main_inputs + [self.left_input, self.right_input] if v is not None]

    def __repr__(self) -> str:
        left_repr = f"Left Input: {self.left_input}" if self.left_input else "Left Input: None"
        right_repr = f"Right Input: {self.right_input}" if self.right_input else "Right Input: None"
        main_inputs_repr = f"Main Inputs: {self.main_inputs}" if self.main_inputs else "Main Inputs: None"
        return f"{self.__class__.__name__}({left_repr}, {right_repr}, {main_inputs_repr})"


class NodeSchemaInformation:
    result_schema: Optional[List[FlowfileColumn]] = []  # resulting schema of the function
    predicted_schema: Optional[List[FlowfileColumn]] = []  # predicted resulting schema of the function
    input_columns: List[str] = []  # columns that are needed for the function
    drop_columns: List[str] = []  # columns that will not be available after the function
    output_columns: List[FlowfileColumn] = []  # columns that will be added with the function


class NodeResults:
    _resulting_data: Optional[FlowfileTable] = None  # after successful execution this will contain the Flowfile
    example_data: Optional[
        FlowfileTable] = None  # after success this will contain a sample of the data (to provide frontend data)
    example_data_generator: Optional[Callable[[], pa.Table]] = None
    run_time: int = -1
    errors: Optional[str] = None
    warnings: Optional[str] = None

    def __init__(self):
        self._resulting_data = None
        self.example_data = None
        self.run_time = -1
        self.errors = None
        self.warnings = None
        self.example_data_generator = None

    def get_example_data(self) -> pa.Table | None:
        if self.example_data_generator:
            return self.example_data_generator()

    @property
    def resulting_data(self) -> FlowfileTable:
        return self._resulting_data

    @resulting_data.setter
    def resulting_data(self, d: FlowfileTable):
        self._resulting_data = d

    def reset(self):
        self.example_data = None
        self._resulting_data = None
        self.run_time = -1
