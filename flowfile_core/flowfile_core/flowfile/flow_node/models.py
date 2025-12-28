
import pyarrow as pa
from typing import List, Union, Callable, Optional, Literal
from dataclasses import dataclass

# Forward declaration for type hints to avoid circular imports
if False:
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import schemas


@dataclass
class NodeStepPromise:
    """
    A lightweight, temporary representation of a node during the initial
    graph construction phase, before full `FlowNode` objects are created.

    Attributes:
        node_id: The unique identifier for the node.
        name: The display name of the node.
        is_start: A boolean indicating if this is a starting node with no inputs.
        leads_to_id: A list of node IDs that this node connects to.
        left_input: The ID of the node connected to the left input port.
        right_input: The ID of the node connected to the right input port.
        depends_on: A list of node IDs that this node depends on for main inputs.
    """
    node_id: Union[str, int]
    name: str
    is_start: bool
    leads_to_id: Optional[List[Union[str, int]]] = None
    left_input: Optional[Union[str, int]] = None
    right_input: Optional[Union[str, int]] = None
    depends_on: Optional[List[Union[str, int]]] = None


class NodeStepStats:
    """
    Tracks the execution status and statistics of a `FlowNode`.
    """
    error: str = None
    _has_run_with_current_setup: bool = False
    has_completed_last_run: bool = False
    active: bool = True
    is_canceled: bool = False

    def __init__(self, error: str = None,
                 has_run_with_current_setup: bool = False,
                 has_completed_last_run: bool = False,
                 active: bool = True,
                 is_canceled: bool = False):
        """
        Initializes the node's statistics.

        :param error: Any error message from the last run.
        :param has_run_with_current_setup: Flag indicating if the node has run successfully with its current configuration.
        :param has_completed_last_run: Flag indicating if the last triggered run finished (successfully or not).
        :param active: Flag indicating if the node is active in the flow.
        :param is_canceled: Flag indicating if the last run was canceled.
        """
        self.error = error
        self._has_run_with_current_setup = has_run_with_current_setup
        self.has_completed_last_run = has_completed_last_run
        self.active = active
        self.is_canceled = is_canceled

    def __repr__(self) -> str:
        """
        Provides a string representation of the node's stats.
        :return: A string detailing the current stats.
        """
        return (f"NodeStepStats(error={self.error}, has_run_with_current_setup={self.has_run_with_current_setup}, "
                f"has_completed_last_run={self.has_completed_last_run}, "
                f"active={self.active}, is_canceled={self.is_canceled})")

    @property
    def has_run_with_current_setup(self) -> bool:
        """
        Checks if the node has run successfully with its current settings and inputs.
        This is the primary flag for caching.
        :return: True if the node is considered up-to-date, False otherwise.
        """
        return self._has_run_with_current_setup

    @has_run_with_current_setup.setter
    def has_run_with_current_setup(self, value: bool):
        """
        Sets the run status of the node.
        If set to True, it implies the last run was completed successfully.
        :param value: The new boolean status.
        """
        if value:
            self._has_run_with_current_setup = True
            self.has_completed_last_run = True
        else:
            self._has_run_with_current_setup = False


class NodeStepSettings:
    """
    Holds the configuration settings that control a node's execution behavior.

    Attributes:
        cache_results: If True, the node will cache its results to avoid re-computation.
        renew_schema: If True, the schema will be re-evaluated on changes.
        streamable: If True, the node can process data in a streaming fashion.
        setup_errors: If True, indicates a non-blocking error occurred during setup.
        breaking_setup_errors: If True, indicates an error occurred that prevents execution.
    """
    cache_results: bool = False
    renew_schema: bool = True
    streamable: bool = True
    setup_errors: bool = False
    breaking_setup_errors: bool = False


class NodeStepInputs:
    """
    Manages the input connections for a `FlowNode`.

    Attributes:
        left_input: The `FlowNode` connected to the left input port.
        right_input: The `FlowNode` connected to the right input port.
        main_inputs: A list of `FlowNode` objects connected to the main input port(s).
    """
    left_input: "FlowNode" = None
    right_input: "FlowNode" = None
    main_inputs: List["FlowNode"] = None

    @property
    def input_ids(self) -> List[int]:
        """
        Gets the IDs of all connected input nodes.
        :return: A list of integer node IDs.
        """
        if self.main_inputs is not None:
            return [node_input.node_information.id for node_input in self.get_all_inputs()]
        return []

    def get_all_inputs(self) -> List["FlowNode"]:
        """
        Retrieves a single list containing all input nodes (main, left, and right).
        :return: A list of all connected `FlowNode` objects.
        """
        main_inputs = self.main_inputs or []
        return [v for v in main_inputs + [self.left_input, self.right_input] if v is not None]

    def __repr__(self) -> str:
        """
        Provides a string representation of the node's inputs.
        :return: A string detailing the connected inputs.
        """
        left_repr = f"Left Input: {self.left_input}" if self.left_input else "Left Input: None"
        right_repr = f"Right Input: {self.right_input}" if self.right_input else "Right Input: None"
        main_inputs_repr = f"Main Inputs: {self.main_inputs}" if self.main_inputs else "Main Inputs: None"
        return f"{self.__class__.__name__}({left_repr}, {right_repr}, {main_inputs_repr})"

    def validate_if_input_connection_exists(self, node_input_id: int,
                                            connection_name: Literal['main', 'left', 'right']) -> bool:
        """
        Checks if a connection from a specific node ID exists on a given port.

        :param node_input_id: The ID of the source node to check for.
        :param connection_name: The name of the input port ('main', 'left', 'right').
        :return: True if the connection exists, False otherwise.
        """
        if connection_name == 'main' and self.main_inputs:
            return any(node_input.node_information.id == node_input_id for node_input in self.main_inputs)
        if connection_name == 'left' and self.left_input:
            return self.left_input.node_information.id == node_input_id
        if connection_name == 'right':
            return self.right_input.node_information.id == node_input_id


class NodeSchemaInformation:
    """
    Stores all schema-related information for a `FlowNode`.

    Attributes:
        result_schema: The actual output schema after a successful execution.
        predicted_schema: The predicted output schema, calculated without full execution.
        input_columns: A list of column names the node requires from its inputs.
        drop_columns: A list of column names that will be dropped by the node.
        output_columns: A list of `FlowfileColumn` objects that will be added by the node.
    """
    result_schema: Optional[List[FlowfileColumn]] = None
    predicted_schema: Optional[List[FlowfileColumn]] = None
    input_columns: List[str] = []
    drop_columns: List[str] = []
    output_columns: List[FlowfileColumn] = []


class NodeResults:
    """
    Stores the outputs of a `FlowNode`'s execution, including data, errors, and metadata.
    """
    _resulting_data: Optional[FlowDataEngine] = None
    example_data: Optional[FlowDataEngine] = None
    example_data_path: Optional[str] = None
    example_data_generator: Optional[Callable[[], pa.Table]] = None
    run_time: int = -1
    errors: Optional[str] = None
    warnings: Optional[str] = None
    analysis_data_generator: Optional[Callable[[], pa.Table]] = None

    def __init__(self):
        self._resulting_data = None
        self.example_data = None
        self.run_time = -1
        self.errors = None
        self.warnings = None
        self.example_data_generator = None
        self.analysis_data_generator = None

    def get_example_data(self) -> Optional[pa.Table]:
        """
        Executes the generator to fetch a sample of the resulting data.
        :return: A PyArrow Table containing a sample of the data, or None.
        """
        if self.example_data_generator:
            return self.example_data_generator()

    @property
    def resulting_data(self) -> Optional[FlowDataEngine]:
        """
        Gets the full resulting data from the node's execution.
        :return: A `FlowDataEngine` instance containing the result, or None.
        """
        return self._resulting_data

    @resulting_data.setter
    def resulting_data(self, d: Optional[FlowDataEngine]):
        """
        Sets the resulting data.
        :param d: The `FlowDataEngine` instance to store.
        """
        self._resulting_data = d

    def reset(self):
        """Resets all result attributes to their default, empty state."""
        self._resulting_data = None
        self.run_time = -1