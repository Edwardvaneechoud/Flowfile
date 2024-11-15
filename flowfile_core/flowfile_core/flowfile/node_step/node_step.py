from typing import List, Union, Callable, Any, Optional, Generator
import polars as pl


from flowfile_core.configs import logger
from flowfile_core.flowfile.flowfile_table.flow_file_column.main import FlowFileColumn
from flowfile_core.flowfile.flowfile_table.flowFilePolars import FlowFileTable
from flowfile_core.schemas import input_schema, schemas
from dataclasses import dataclass

from flowfile_core.schemas.output_model import TableExample, FileColumn, NodeData
from flowfile_core.flowfile.utils import get_hash
from flowfile_core.configs.node_store import nodes as node_interface
from flowfile_core.flowfile.setting_generator import setting_generator, setting_updator
from time import sleep
from flowfile_core.flowfile.flowfile_table.subprocess_operations import ExternalDfFetcher, results_exists, get_external_df_result


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
    execute_location: schemas.ExecutionLocationsLiteral = 'remote'


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
    result_schema: Optional[List[FlowFileColumn]] = []  # resulting schema of the function
    predicted_schema: Optional[List[FlowFileColumn]] = []  # predicted resulting schema of the function
    input_columns: List[str] = []  # columns that are needed for the function
    drop_columns: List[str] = []  # columns that will not be available after the function
    output_columns: List[FlowFileColumn] = []  # columns that will be added with the function


class NodeResults:
    _resulting_data: Optional[FlowFileTable] = None  # after successful execution this will contain the FlowFile
    example_data: Optional[
        FlowFileTable] = None  # after success this will contain a sample of the data (to provide frontend data)
    run_time: int = -1
    errors: Optional[str] = None
    warnings: Optional[str] = None

    def __init__(self):
        self._resulting_data = None
        self.example_data = None
        self.run_time = -1
        self.errors = None
        self.warnings = None

    @property
    def resulting_data(self) -> FlowFileTable:
        return self._resulting_data

    @resulting_data.setter
    def resulting_data(self, d: FlowFileTable):
        self._resulting_data = d

    def reset(self):
        self.example_data = None
        self._resulting_data = None
        self.run_time = -1


class NodeStep:
    parent_uuid: str
    node_type: str
    node_template: node_interface.NodeTemplate
    node_default: node_interface.node_defaults
    node_schema: NodeSchemaInformation
    node_inputs: NodeStepInputs
    node_stats: NodeStepStats
    node_settings: NodeStepSettings
    results: NodeResults
    node_information: Optional[schemas.NodeInformation] = None
    leads_to_nodes: List["NodeStep"] = []  # list with target flows, after execution the step will trigger those step(s)
    _setting_input: Any = None
    _hash: Optional[str] = None  # host this for caching results
    _function: Callable = None  # the function that needs to be executed when triggered
    _schema_callback: Optional[Callable] = None  # Function that calculates the schema without executing the process
    _state_needs_reset: bool = False
    _fetch_cached_df: Optional[ExternalDfFetcher] = None

    def post_init(self):
        self.node_inputs = NodeStepInputs()
        self.node_stats = NodeStepStats()
        self.node_settings = NodeStepSettings()
        self.node_schema = NodeSchemaInformation()
        self.results = NodeResults()
        self.node_information = schemas.NodeInformation()
        self.leads_to_nodes = []
        self._setting_input = None
        self._cache_progress = None
        self._schema_callback = None
        self._state_needs_reset = False

    @property
    def state_needs_reset(self):
        return self._state_needs_reset

    @state_needs_reset.setter
    def state_needs_reset(self, v: bool):
        self._state_needs_reset = v

    @property
    def schema_callback(self):
        return self._schema_callback

    @schema_callback.setter
    def schema_callback(self, f: Callable):
        if f is None:
            return

        def schema_call_back_func():
            try:
                return f()
            except Exception as e:
                logger.warn(e)
                self.node_settings.setup_errors = True
                return []

        self._schema_callback = schema_call_back_func

    @property
    def is_start(self) -> bool:
        return not self.has_input

    def get_input_type(self, node_id: int) -> List:
        relation_type = []
        if node_id in [n.node_id for n in self.node_inputs.main_inputs]:
            relation_type.append('main')
        if self.node_inputs.left_input is not None and node_id == self.node_inputs.left_input.node_id:
            relation_type.append('left')
        if self.node_inputs.right_input is not None and node_id == self.node_inputs.right_input.node_id:
            relation_type.append('right')
        return list(set(relation_type))

    def __init__(self, node_id: Union[str, int], function: Callable,
                 parent_uuid: str,
                 setting_input: Any,
                 name: str,
                 node_type: str,
                 input_columns: List[str] = None,
                 output_schema: List[FlowFileColumn] = None,
                 drop_columns: List[str] = None,
                 renew_schema: bool = True,
                 cache_results: bool = False,
                 pos_x: float = 0,
                 pos_y: float = 0,
                 schema_callback: Callable = None,
                 ):
        self.parent_uuid = parent_uuid
        self.post_init()
        self.active = True
        self.node_information.id = node_id
        self.node_type = node_type
        self.node_settings.renew_schema = renew_schema
        self.update_node(function=function,
                         input_columns=input_columns,
                         output_schema=output_schema,
                         drop_columns=drop_columns,
                         setting_input=setting_input,
                         cache_results=cache_results,
                         name=name,
                         pos_x=pos_x,
                         pos_y=pos_y,
                         schema_callback=schema_callback,
                         )

    def update_node(self,
                    function: Callable,
                    input_columns: List[str] = None,
                    output_schema: List[FlowFileColumn] = None,
                    drop_columns: List[str] = None,
                    name: str = None,
                    setting_input: Any = None,
                    cache_results: bool = False,
                    pos_x: float = 0,
                    pos_y: float = 0,
                    schema_callback: Callable = None,
                    ):
        self.schema_callback = schema_callback
        self.node_information.y_position = pos_y
        self.node_information.x_position = pos_x
        self.node_information.setting_input = setting_input
        self.node_settings.cache_results = self.node_settings.cache_results or cache_results
        self.name = self.node_type if name is None else name
        self._function = function
        self.node_schema.input_columns = [] if input_columns is None else input_columns
        self.node_schema.output_columns = [] if output_schema is None else output_schema
        self.node_schema.drop_columns = [] if drop_columns is None else drop_columns
        self.node_settings.renew_schema = True
        self.setting_input = setting_input
        self.results.errors = None
        self.add_lead_to_in_depend_source()
        _ = self.hash
        self.node_template = node_interface.node_dict.get(self.node_type)
        self.node_default = node_interface.node_defaults.get(self.node_type)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name
        self.__name__ = name

    @property
    def setting_input(self):
        return self._setting_input

    @setting_input.setter
    def setting_input(self, setting_input: Any):
        self._setting_input = setting_input
        self.set_node_information()
        if self.node_type == 'manual_input' and isinstance(self._setting_input, input_schema.NodeManualInput):
            if self.hash != self.calculate_hash(setting_input) or not self.node_stats.has_run:
                self.function = self.function.__class__(setting_input.raw_data)
                self.reset()
                self.get_predicted_schema()
        elif self._setting_input is not None:
            self.reset()

    @property
    def node_id(self):
        return self.node_information.id

    @property
    def left_input(self):
        return self.node_inputs.left_input

    @property
    def right_input(self):
        return self.node_inputs.right_input

    @property
    def main_input(self) -> List["NodeStep"]:
        return self.node_inputs.main_inputs

    @property
    def is_correct(self):
        return (self.node_template.input == len(self.node_inputs.get_all_inputs()) or
                (self.node_template.multi and len(self.node_inputs.get_all_inputs())> 0)
                )

    def set_node_information(self):
        logger.info('setting node information')
        node_information = self.node_information
        node_information.left_input_id = self.node_inputs.left_input.node_id if self.left_input else None
        node_information.right_input_id = self.node_inputs.right_input.node_id if self.right_input else None
        node_information.input_ids = [mi.node_id for mi in
                                      self.node_inputs.main_inputs] if self.node_inputs.main_inputs is not None else None
        node_information.setting_input = self.setting_input
        node_information.outputs = [n.node_id for n in self.leads_to_nodes]
        node_information.is_setup = self.is_setup
        node_information.x_position = self.setting_input.pos_x
        node_information.y_position = self.setting_input.pos_y
        node_information.type = self.node_type

    def get_node_information(self) -> schemas.NodeInformation:
        self.set_node_information()
        return self.node_information

    @property
    def function(self):
        return self._function

    def reset_hash(self) -> bool:
        old_hash = self._hash
        self._hash = None
        if self.hash != old_hash:
            if self.node_settings.cache_results:
                self.remove_cache()
            return True
        return False

    @property
    def all_inputs(self) -> List["NodeStep"]:
        return self.node_inputs.get_all_inputs()

    def calculate_hash(self, setting_input: Any):
        depends_on_hashes = [_node.hash for _node in self.all_inputs]
        node_data_hash = get_hash(setting_input)
        return get_hash(depends_on_hashes + [node_data_hash, self.parent_uuid])

    @property
    def hash(self):
        if not self._hash:
            self._hash = self.calculate_hash(self.setting_input)
        return self._hash

    @function.setter
    def function(self, function: Callable):
        self._function = function
        # self.reset()

    def add_node_connection(self, from_node: "NodeStep", insert_type: str = 'main'):
        from_node.leads_to_nodes.append(self)
        if insert_type == 'main':
            if self.node_template.input <= 2 or self.node_inputs.main_inputs is None:
                self.node_inputs.main_inputs = [from_node]
            else:
                self.node_inputs.main_inputs.append(from_node)
        elif insert_type == 'right':
            self.node_inputs.right_input = from_node
        elif insert_type == 'left':
            self.node_inputs.left_input = from_node
        else:
            raise Exception('Cannot find the connection')
        if self.setting_input.is_setup:
            if hasattr(self.setting_input, 'depending_on_id') and insert_type == 'main':
                self.setting_input.depending_on_id = from_node.node_id
        self.reset()
        from_node.reset()

    def evaluate_nodes(self, deep: bool = False):
        for node in self.leads_to_nodes:
            self.print(f'resetting node: {node.node_id}')
            node.reset(deep)

    def get_flow_file_column_schema(self, col_name: str) -> FlowFileColumn:
        for s in self.schema:
            if s.column_name == col_name:
                return s

    def get_predicted_schema(self):
        """
        Method to get a predicted schema based on the columns that are dropped and added
        :return:
        """
        if self.node_schema.predicted_schema is not None:
            return self.node_schema.predicted_schema
        if self.schema_callback is not None and self.node_schema.predicted_schema is None:
            self.print('Getting the data from a schema callback')
            schema = self.schema_callback()
            if schema is not None:
                self.node_schema.predicted_schema = schema
                return self.node_schema.predicted_schema
        predicted_data = self._predicted_data_getter()
        if predicted_data is not None and predicted_data.schema is not None:
            self.print('Calculating the schema based on the predicted resulting data')
            self.node_schema.predicted_schema = self._predicted_data_getter().schema
        return self.node_schema.predicted_schema

    @property
    def is_setup(self) -> bool:
        if not self.node_information.is_setup:
            if self.function.__name__ != 'placeholder':
                self.node_information.is_setup = True
                self.setting_input.is_setup = True
        return self.node_information.is_setup

    def print(self, v: Any):
        print(f'{self.node_type}, node_id: {self.node_id}: {v}')

    def get_resulting_data(self) -> FlowFileTable:
        if self.is_setup:
            if self.results.resulting_data is None and self.results.errors is None:
                self.print('getting resulting data')
                try:
                    if isinstance(self.function, FlowFileTable):
                        fl: FlowFileTable = self.function
                    elif self.node_type in ('external_source', 'airbyte_reader'):
                        fl: FlowFileTable = self.function()
                        fl.collect_external()
                        self.node_settings.streamable = False
                    else:
                        try:
                            fl = self._function(*[v.get_resulting_data() for v in self.all_inputs])
                        except Exception as e:
                            raise e
                    fl.set_streamable(self.node_settings.streamable)
                    self.results.resulting_data = fl
                except Exception as e:
                    self.results.resulting_data = FlowFileTable()
                    self.results.errors = str(e)
                    self.node_stats.has_run = False
                    raise e
            return self.results.resulting_data

    def _predicted_data_getter(self) -> FlowFileTable|None:
        try:
            fl = self._function(*[v.get_predicted_resulting_data() for v in self.all_inputs])
            return fl
        except ValueError as e:
            if str(e) == "generator already executing":
                logger.info('Generator already executing, waiting for the result')
                sleep(1)
                return self._predicted_data_getter()
            fl = FlowFileTable()
            return fl

        except Exception as e:
            logger.warning('there was an issue with the function, returning an empty FlowFile')
            logger.warning(e)

    def get_predicted_resulting_data(self) -> FlowFileTable:
        if self.needs_run() and self.schema_callback is not None or self.node_schema.result_schema is not None:
            self.print('Getting data based on the schema')
            _s = self.schema_callback() if self.node_schema.result_schema is None else self.node_schema.result_schema
            return FlowFileTable.create_from_schema(_s)
        else:
            if isinstance(self.function, FlowFileTable):
                fl = self.function
            else:
                fl = FlowFileTable.create_from_schema(self.get_predicted_schema())
            return fl

    def add_lead_to_in_depend_source(self):
        for input_node in self.all_inputs:
            if self.node_id not in [n.node_id for n in input_node.leads_to_nodes]:
                input_node.leads_to_nodes.append(self)

    def get_all_dependent_nodes(self) -> Generator["NodeStep", None, None]:
        for node in self.leads_to_nodes:
            yield node
            for n in node.get_all_dependent_nodes():
                yield n

    def get_all_dependent_node_ids(self) -> Generator[int, None, None]:
        for node in self.leads_to_nodes:
            yield node.node_id
            for n in node.get_all_dependent_node_ids():
                yield n

    @property
    def schema(self) -> List[FlowFileColumn]:
        try:
            if self.is_setup and self.results.errors is None:
                if self.node_schema.result_schema is not None and len(self.node_schema.result_schema) > 0:
                    return self.node_schema.result_schema
                elif self.node_type == 'output':
                    if len(self.node_inputs.main_inputs) > 0:
                        self.node_schema.result_schema = self.node_inputs.main_inputs[0].schema
                else:
                    self.node_schema.result_schema = self.get_predicted_schema()
                return self.node_schema.result_schema
            else:
                return []
        except:
            return []

    def load_from_cache(self) -> FlowFileTable:
        if results_exists(self.hash):
            try:
                return FlowFileTable(self._fetch_cached_df.get_result())
            except Exception as e:
                logger.error(e)

    def remove_cache(self):
        if results_exists(self.hash):
            logger.warning('Not implemented')

    def needs_run(self) -> bool:
        if not self.node_stats.has_run:
            return True
        if self.node_settings.cache_results and results_exists(self.hash):
            return False
        elif self.node_settings.cache_results and not results_exists(self.hash):
            return True
        else:
            return False

    def __call__(self, *args, **kwargs):
        self.execute_node(*args, **kwargs)

    # @profile
    def execute_local(self, performance_mode: bool = False):
        try:
            resulting_data = self.get_resulting_data()
            if not performance_mode:
                sample_data = resulting_data.__get_sample__(streamable=self.node_settings.streamable)
                if len(sample_data) == 0 and len(resulting_data) > 0:
                    # detect if the result gives null records where it should give at least one
                    self.node_settings.streamable = False
                    sample_data = resulting_data.__get_sample__(streamable=self.node_settings.streamable)
                self.results.example_data = sample_data
            self.node_schema.result_schema = resulting_data.schema
            if self.results.errors is None:
                self.node_stats.has_run = True

        except Exception as e:
            logger.warn(f"Error with step {self.__name__}")
            logger.error(str(e))
            self.results.errors = str(e)
            self.node_stats.has_run = False
            raise e

        if self.node_stats.has_run:
            for step in self.leads_to_nodes:
                if not self.node_settings.streamable:
                    step.node_settings.streamable = self.node_settings.streamable

    def execute_remote(self, performance_mode: bool = False):
        if self.node_settings.cache_results and results_exists(self.hash):
            try:
                self.results.resulting_data = get_external_df_result(self.hash)
                self._cache_progress = None
                return
            except Exception as e:
                logger.warning('Failed to read the cache, rerunning the code')
        if self.node_type == 'output':
            self.results.resulting_data = self.get_resulting_data()
            self.node_stats.has_run = True
            return
        try:
            self.get_resulting_data()
        except Exception as e:
            self.results.errors = 'Error with creating the lazy frame, most likely due to invalid graph'
            raise e
        external_df_catcher = ExternalDfFetcher(lf=self.get_resulting_data().data_frame,
                                                file_ref=self.hash, wait_on_completion=False)
        self._fetch_cached_df = external_df_catcher
        try:
            lf = external_df_catcher.get_result()
            self.results.resulting_data = FlowFileTable(lf, number_of_records=lf.select(pl.len()).collect()[0, 0])
            if not performance_mode:
                self.get_sample_data_from_cache()
        except Exception as e:
            if external_df_catcher.error_code == -1:
                try:
                    self.results.resulting_data = self.get_resulting_data()
                    self.results.warnings = ('Error with external process (unknown error), '
                                             'likely the process was killed by the server because of memory constraints, '
                                             'continue with the process. '
                                             'We cannot display example data...')
                except Exception as e:
                    self.results.errors = str(e)
                    raise e
            elif external_df_catcher.error_description is None:
                self.results.errors = str(e)
                raise e
            else:
                self.results.errors = external_df_catcher.error_description
                raise external_df_catcher.error_description

    def prepare_before_run(self):
        self.results.errors = None
        self.results.resulting_data = None
        self.results.example_data = None

    # @profile
    def execute_node(self, run_location: schemas.ExecutionLocationsLiteral, reset_cache: bool = False,
                     performance_mode: bool = False):
        if reset_cache:
            self.remove_cache()
            self.node_stats.has_run = False
        if self.is_setup:
            logger.info(f'Starting to run {self.__name__}')
            if self.needs_run():
                self.prepare_before_run()
                try:
                    if ((run_location == 'remote' or (self.node_default.transform_type == 'wide')
                            and not run_location == 'local')) or self.node_settings.cache_results:
                        logger.info('Running the node remotely')
                        self.execute_remote(performance_mode=performance_mode)
                    else:
                        logger.info('Running the node locally')
                        self.execute_local(performance_mode=performance_mode)
                except Exception as e:
                    self.node_stats.has_run = False
                    self.results.errors = str(e)
                    logger.error('Error with running the node')
            else:
                logger.info('Node has already run, not running the node')
        else:
            logger.warning(f'Node {self.__name__} is not setup, cannot run the node')

    def get_sample_data_from_cache(self):
        resulting_data = self.results.resulting_data
        self.results.example_data = resulting_data.__get_sample__(streamable=True)
        self.node_schema.result_schema = resulting_data.schema
        if self.results.errors is None:
            self.node_stats.has_run = True

    def get_sample_data(self):
        resulting_data = self.get_resulting_data()
        try:
            logger.info(f'getting sample data from the resulting data: using streaming ='
                        f' {self.node_settings.streamable}')
            sample_data = resulting_data.__get_sample__(streamable=self.node_settings.streamable)
            if len(sample_data) == 0 and len(resulting_data) > 0:
                # detect if the result gives null records where it should give at least one
                self.node_settings.streamable = False
                sample_data = resulting_data.__get_sample__(streamable=self.node_settings.streamable)

            logger.info('setting the example data')
            self.results.example_data = sample_data
            self.node_schema.result_schema = resulting_data.schema
            if self.results.errors is None:
                self.node_stats.has_run = True
        except Exception as e:
            logger.warn(str(e))
            logger.warn(f"Error with step {self.__name__}")
            self.results.errors = str(e)
            self.node_stats.has_run = False

    def needs_reset(self) -> bool:
        return self._hash != self.calculate_hash(self.setting_input)

    def reset(self, deep: bool = False):
        needs_reset = self.needs_reset() or deep
        if needs_reset:
            logger.info(f'{self.node_id}: Node needs reset')
            self.node_stats.has_run = False
            self.results.reset()
            self.node_schema.result_schema = None
            self.node_schema.predicted_schema = None
            self._hash = None
            self.node_information.is_setup = None
            self.evaluate_nodes()

    def delete_lead_to_node(self, node_id: int) -> bool:
        logger.info(f'Deleting lead to node: {node_id}')
        for i, lead_to_node in enumerate(self.leads_to_nodes):
            logger.info(f'Checking lead to node: {lead_to_node.node_id}')
            if lead_to_node.node_id == node_id:
                logger.info(f'Found the node to delete: {node_id}')
                self.leads_to_nodes.pop(i)
                return True
        return False

    def delete_input_node(self, node_id: int, connection_type: input_schema.InputConnectionClass = 'input-0',
                          complete: bool = False) -> bool:
        #  connection type must be in right, left or main
        deleted: bool = False
        print(connection_type)
        if connection_type == 'input-0':
            for i, node in enumerate(self.node_inputs.main_inputs):
                print(node, node.node_id == node_id, node_id)
                if node.node_id == node_id:
                    self.node_inputs.main_inputs.pop(i)
                    deleted = True
                    if not complete:
                        continue
        elif connection_type == 'input-1' or complete:
            if self.node_inputs.right_input is not None and self.node_inputs.right_input.node_id == node_id:
                self.node_inputs.right_input = None
                deleted = True
        elif connection_type == 'input-2' or complete:
            if self.node_inputs.left_input is not None and self.node_inputs.right_input.node_id == node_id:
                self.node_inputs.left_input = None
                deleted = True
        else:
            logger.warning('Could not find the connection to delete...')
        if deleted:
            self.reset()
        return deleted

    def __repr__(self):
        if 1 == 2:
            v = '\n        '.join(str(s) for s in self.schema)
            if len(self.all_inputs) > 0:
                depends_on = ', '.join(d.__name__ for d in self.all_inputs if d.__name__ is not None)
            else:
                depends_on = ''
            return (f"NodeStep(node_id={self.node_id}, function={self._function.__name__}, "
                    f"depends_on={depends_on}, input_columns={self.node_schema.input_columns}, "
                    f"drop_columns={self.node_schema.drop_columns}, output_columns={self.node_schema.output_columns}, "
                    f"schema=\n{v})")
        else:
            return f"Node id: {self.node_id} ({self.node_type})"

    def _get_readable_schema(self):
        if self.is_setup:
            output = []
            for s in self.schema:
                output.append(dict(column_name=s.column_name, data_type=s.data_type))
            return output

    def get_repr(self):
        return dict(NodeStep=
                    dict(node_id=self.node_id,
                         step_name=self.__name__,
                         output_columns=self.node_schema.output_columns,
                         output_schema=self._get_readable_schema()))

    @property
    def number_of_leads_to_nodes(self) -> int:
        if self.is_setup:
            return len(self.leads_to_nodes)

    @property
    def has_next_step(self) -> bool:
        return len(self.leads_to_nodes) > 0

    @property
    def has_input(self) -> bool:
        return len(self.all_inputs) > 0

    @property
    def singular_input(self) -> bool:
        return self.node_template.input == 1

    @property
    def singular_main_input(self) -> "NodeStep":
        if self.singular_input:
            return self.all_inputs[0]

    def get_table_example(self, include_data: bool = False) -> TableExample | None:
        self.print('Getting a table example')
        if self.node_type == 'output':
            self.print('getting the table example')
            return self.main_input[0].get_table_example(include_data)
        if self.node_stats.has_run and self.is_setup:
            print('getting the table example since the node has run')
            fl = self.results.example_data
            schema = [FileColumn.parse_obj(c.get_column_repr()) for c in self.schema]
            any(fl.schema)
            if include_data:
                data = fl.get_output_sample(10)
            else:
                data = []
            return TableExample(node_id=self.node_id,
                                name=str(self.node_id), number_of_records=fl.number_of_records,
                                number_of_columns=fl.number_of_fields,
                                table_schema=schema, columns=fl.columns, data=data)
        else:
            print('getting the table example but the node has not run')
            try:
                schema = [FileColumn.parse_obj(c.get_column_repr()) for c in self.schema]
            except Exception as e:
                logger.warning(e)
                schema = []
            columns = [s.name for s in schema]
            return TableExample(node_id=self.node_id,
                                name=str(self.node_id), number_of_records=0,
                                number_of_columns=len(columns),
                                table_schema=schema, columns=columns,
                                data=[])

    def calculate_settings_out_select(self):
        pass

    def get_node_data(self, flow_id: int, include_example: bool = False) -> NodeData:
        node = NodeData(flow_id=flow_id,
                        node_id=self.node_id,
                        has_run=self.node_stats.has_run,
                        setting_input=self.setting_input,
                        flow_type=self.node_type)
        print('flow id of node: ', node.flow_id)
        if self.main_input:
            node.main_input = self.main_input[0].get_table_example()
        if self.left_input:
            node.left_input = self.left_input.get_table_example()
        if self.right_input:
            node.right_input = self.right_input.get_table_example()
        if self.is_setup:
            node.main_output = self.get_table_example(include_example)
        node = setting_generator.get_setting_generator(self.node_type)(node)

        node = setting_updator.get_setting_updator(self.node_type)(node)
        return node

    def get_output_data(self) -> TableExample:
        return self.get_table_example(True)

    def get_node_input(self) -> schemas.NodeInput:
        return schemas.NodeInput(pos_y=self.setting_input.pos_y,
                                 pos_x=self.setting_input.pos_x,
                                 id=self.node_id,
                                 **self.node_template.__dict__)

    def get_edge_input(self) -> List[schemas.NodeEdge]:
        edges = []
        if self.node_inputs.main_inputs is not None:
            for i, main_input in enumerate(self.node_inputs.main_inputs):
                edges.append(schemas.NodeEdge(id=f'{main_input.node_id}-{self.node_id}-{i}',
                                              source=main_input.node_id,
                                              target=self.node_id,
                                              sourceHandle='output-0',
                                              targetHandle='input-0',
                                              ))
        if self.node_inputs.left_input is not None:
            edges.append(schemas.NodeEdge(id=f'{self.node_inputs.left_input.node_id}-{self.node_id}-right',
                                          source=self.node_inputs.left_input.node_id,
                                          target=self.node_id,
                                          sourceHandle='output-0',
                                          targetHandle='input-2',
                                          ))
        if self.node_inputs.right_input is not None:
            edges.append(schemas.NodeEdge(id=f'{self.node_inputs.right_input.node_id}-{self.node_id}-left',
                                          source=self.node_inputs.right_input.node_id,
                                          target=self.node_id,
                                          sourceHandle='output-0',
                                          targetHandle='input-1',
                                          ))
        return edges
