
from typing import List, Union, Callable, Any, Optional, Generator, Literal
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.utils.arrow_reader import get_read_top_n
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.configs.flow_logger import NodeLogger

from flowfile_core.schemas.output_model import TableExample, FileColumn, NodeData
from flowfile_core.flowfile.utils import get_hash
from flowfile_core.configs.node_store import nodes as node_interface
from flowfile_core.flowfile.setting_generator import setting_generator, setting_updator
from time import sleep
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import (
    ExternalDfFetcher, ExternalSampler, results_exists, get_external_df_result, ExternalDatabaseFetcher, ExternalDatabaseWriter)
from flowfile_core.flowfile.flow_node.models import (NodeStepSettings, NodeStepInputs, NodeSchemaInformation,
                                                     NodeStepStats, NodeResults)
from flowfile_core.flowfile.flow_node.schema_callback import SingleExecutionFuture


class FlowNode:
    parent_uuid: str
    node_type: str
    node_template: node_interface.NodeTemplate
    node_default: schemas.NodeDefault
    node_schema: NodeSchemaInformation
    node_inputs: NodeStepInputs
    node_stats: NodeStepStats
    node_settings: NodeStepSettings
    results: NodeResults
    node_information: Optional[schemas.NodeInformation] = None
    leads_to_nodes: List["FlowNode"] = []  # list with target flows, after execution the step will trigger those step(s)
    _setting_input: Any = None
    _hash: Optional[str] = None  # host this for caching results
    _function: Callable = None  # the function that needs to be executed when triggered
    _schema_callback: Optional[SingleExecutionFuture] = None  # Function that calculates the schema without executing
    _state_needs_reset: bool = False
    _fetch_cached_df: Optional[ExternalDfFetcher | ExternalDatabaseFetcher | ExternalDatabaseWriter] = None
    _cache_progress: Optional[ExternalDfFetcher | ExternalDatabaseFetcher | ExternalDatabaseWriter] = None

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

        def error_callback(e: Exception) -> List:
            logger.warning(e)

            self.node_settings.setup_errors = True
            return []

        self._schema_callback = SingleExecutionFuture(f, error_callback)
        self._schema_callback.start()

    @property
    def is_start(self) -> bool:
        return not self.has_input and self.node_template.input == 0

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
                 output_schema: List[FlowfileColumn] = None,
                 drop_columns: List[str] = None,
                 renew_schema: bool = True,
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
                         name=name,
                         pos_x=pos_x,
                         pos_y=pos_y,
                         schema_callback=schema_callback,
                         )

    def update_node(self,
                    function: Callable,
                    input_columns: List[str] = None,
                    output_schema: List[FlowfileColumn] = None,
                    drop_columns: List[str] = None,
                    name: str = None,
                    setting_input: Any = None,
                    pos_x: float = 0,
                    pos_y: float = 0,
                    schema_callback: Callable = None,
                    ):

        self.schema_callback = schema_callback
        self.node_information.y_position = pos_y
        self.node_information.x_position = pos_x
        self.node_information.setting_input = setting_input
        self.name = self.node_type if name is None else name
        self._function = function
        self.node_schema.input_columns = [] if input_columns is None else input_columns
        self.node_schema.output_columns = [] if output_schema is None else output_schema
        self.node_schema.drop_columns = [] if drop_columns is None else drop_columns
        self.node_settings.renew_schema = True
        if hasattr(setting_input, 'cache_results'):
            self.node_settings.cache_results = setting_input.cache_results
        self.setting_input = setting_input
        self.results.errors = None
        self.add_lead_to_in_depend_source()
        _ = self.hash
        self.node_template = node_interface.node_dict.get(self.node_type)
        if self.node_template is None:
            raise Exception(f'Node template {self.node_type} not found')
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
    def main_input(self) -> List["FlowNode"]:
        return self.node_inputs.main_inputs

    @property
    def is_correct(self):
        # Check if inputs meet requirements
        if isinstance(self.setting_input, input_schema.NodePromise):
            return False
        return (self.node_template.input == len(self.node_inputs.get_all_inputs()) or
                (self.node_template.multi and len(self.node_inputs.get_all_inputs()) > 0) or
                (self.node_template.multi and self.node_template.can_be_start))

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
    def all_inputs(self) -> List["FlowNode"]:
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

    def add_node_connection(self, from_node: "FlowNode", insert_type: Literal['main', 'left', 'right'] = 'main'):
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

    def get_flow_file_column_schema(self, col_name: str) -> FlowfileColumn:
        for s in self.schema:
            if s.column_name == col_name:
                return s

    def get_predicted_schema(self, force: bool = False):
        """
        Method to get a predicted schema based on the columns that are dropped and added
        :return:
        """
        if self.node_schema.predicted_schema is not None and not force:
            return self.node_schema.predicted_schema
        if self.schema_callback is not None and (self.node_schema.predicted_schema is None or force):
            self.print('Getting the data from a schema callback')
            if force:
                # Force the schema callback to reset, so that it will be executed again
                self.schema_callback.reset()
            schema = self.schema_callback()
            if schema is not None:
                self.print('Calculating the schema based on the schema callback')
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
        logger.info(f'{self.node_type}, node_id: {self.node_id}: {v}')

    def get_resulting_data(self) -> FlowDataEngine:
        if self.is_setup:
            if self.results.resulting_data is None and self.results.errors is None:
                self.print('getting resulting data')
                try:
                    if isinstance(self.function, FlowDataEngine):
                        fl: FlowDataEngine = self.function
                    elif self.node_type in ('external_source', 'airbyte_reader'):
                        fl: FlowDataEngine = self.function()
                        fl.collect_external()
                        self.node_settings.streamable = False
                    else:
                        try:
                            fl = self._function(*[v.get_resulting_data() for v in self.all_inputs])
                        except Exception as e:
                            raise e
                    fl.set_streamable(self.node_settings.streamable)
                    self.results.resulting_data = fl
                    self.node_schema.result_schema = fl.schema
                except Exception as e:
                    self.results.resulting_data = FlowDataEngine()
                    self.results.errors = str(e)
                    self.node_stats.has_run = False
                    raise e
            return self.results.resulting_data

    def _predicted_data_getter(self) -> FlowDataEngine|None:
        try:
            fl = self._function(*[v.get_predicted_resulting_data() for v in self.all_inputs])
            return fl
        except ValueError as e:
            if str(e) == "generator already executing":
                logger.info('Generator already executing, waiting for the result')
                sleep(1)
                return self._predicted_data_getter()
            fl = FlowDataEngine()
            return fl

        except Exception as e:
            logger.warning('there was an issue with the function, returning an empty Flowfile')
            logger.warning(e)

    def get_predicted_resulting_data(self) -> FlowDataEngine:
        if self.needs_run(False) and self.schema_callback is not None or self.node_schema.result_schema is not None:
            self.print('Getting data based on the schema')
            _s = self.schema_callback() if self.node_schema.result_schema is None else self.node_schema.result_schema
            return FlowDataEngine.create_from_schema(_s)
        else:
            if isinstance(self.function, FlowDataEngine):
                fl = self.function
            else:
                fl = FlowDataEngine.create_from_schema(self.get_predicted_schema())
            return fl

    def add_lead_to_in_depend_source(self):
        for input_node in self.all_inputs:
            if self.node_id not in [n.node_id for n in input_node.leads_to_nodes]:
                input_node.leads_to_nodes.append(self)

    def get_all_dependent_nodes(self) -> Generator["FlowNode", None, None]:
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
    def schema(self) -> List[FlowfileColumn]:
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

    def load_from_cache(self) -> FlowDataEngine:
        if results_exists(self.hash):
            try:
                return FlowDataEngine(self._fetch_cached_df.get_result())
            except Exception as e:
                logger.error(e)

    def remove_cache(self):
        if results_exists(self.hash):
            logger.warning('Not implemented')

    def needs_run(self, performance_mode: bool, node_logger: NodeLogger = None,
                  execution_location: schemas.ExecutionLocationsLiteral = "auto") -> bool:
        if execution_location == "local":
            return False
        flow_logger = logger if node_logger is None else node_logger
        cache_result_exists = results_exists(self.hash)
        if not self.node_stats.has_run:
            flow_logger.info('Node has not run, needs to run')
            return True
        if self.node_settings.cache_results and cache_result_exists:

            return False
        elif self.node_settings.cache_results and not cache_result_exists:
            return True
        elif not performance_mode and cache_result_exists:
            return False
        else:
            return True

    def __call__(self, *args, **kwargs):
        self.execute_node(*args, **kwargs)

    def execute_local(self, flow_id: int, performance_mode: bool = False):
        try:
            resulting_data = self.get_resulting_data()
            if not performance_mode:
                external_sampler = ExternalSampler(lf=resulting_data.data_frame, file_ref=self.hash,
                                                   wait_on_completion=True, node_id=self.node_id, flow_id=flow_id)
                self.store_example_data_generator(external_sampler)
                if self.results.errors is None and not self.node_stats.is_canceled:
                    self.node_stats.has_run = True
            self.node_schema.result_schema = resulting_data.schema

        except Exception as e:
            logger.warning(f"Error with step {self.__name__}")
            logger.error(str(e))
            self.results.errors = str(e)
            self.node_stats.has_run = False
            raise e

        if self.node_stats.has_run:
            for step in self.leads_to_nodes:
                if not self.node_settings.streamable:
                    step.node_settings.streamable = self.node_settings.streamable

    def execute_remote(self, performance_mode: bool = False, node_logger: NodeLogger = None):
        # flow_logger = logger if flow_logger is None else flow_logger
        if node_logger is None:
            raise Exception('Node logger is not defined')
        if self.node_settings.cache_results and results_exists(self.hash):
            try:
                self.results.resulting_data = get_external_df_result(self.hash)
                self._cache_progress = None
                return
            except Exception as e:
                node_logger.warning('Failed to read the cache, rerunning the code')
        if self.node_type == 'output':
            self.results.resulting_data = self.get_resulting_data()
            self.node_stats.has_run = True
            return
        try:
            self.get_resulting_data()
        except Exception as e:
            self.results.errors = 'Error with creating the lazy frame, most likely due to invalid graph'
            raise e
        if not performance_mode:
            external_df_fetcher = ExternalDfFetcher(lf=self.get_resulting_data().data_frame,
                                                    file_ref=self.hash, wait_on_completion=False,
                                                    flow_id=node_logger.flow_id,
                                                    node_id=self.node_id)
            self._fetch_cached_df = external_df_fetcher
            try:
                lf = external_df_fetcher.get_result()
                self.results.resulting_data = FlowDataEngine(
                    lf, number_of_records=ExternalDfFetcher(lf=lf, operation_type='calculate_number_of_records',
                                                            flow_id=node_logger.flow_id, node_id=self.node_id).result
                )
                if not performance_mode:
                    self.store_example_data_generator(external_df_fetcher)
                    self.node_stats.has_run = True

            except Exception as e:
                node_logger.error('Error with external process')
                if external_df_fetcher.error_code == -1:
                    try:
                        self.results.resulting_data = self.get_resulting_data()
                        self.results.warnings = ('Error with external process (unknown error), '
                                                 'likely the process was killed by the server because of memory constraints, '
                                                 'continue with the process. '
                                                 'We cannot display example data...')
                    except Exception as e:
                        self.results.errors = str(e)
                        raise e
                elif external_df_fetcher.error_description is None:
                    self.results.errors = str(e)
                    raise e
                else:
                    self.results.errors = external_df_fetcher.error_description
                    raise Exception(external_df_fetcher.error_description)
            finally:
                self._fetch_cached_df = None

    def prepare_before_run(self):
        self.results.errors = None
        self.results.resulting_data = None
        self.results.example_data = None

    def cancel(self):
        if self._fetch_cached_df is not None:
            self._fetch_cached_df.cancel()
            self.node_stats.is_canceled = True
        else:
            logger.warning('No external process to cancel')
        self.node_stats.is_canceled = True

    def execute_node(self, run_location: schemas.ExecutionLocationsLiteral, reset_cache: bool = False,
                     performance_mode: bool = False, retry: bool = True, node_logger: NodeLogger = None):
        if node_logger is None:
            raise Exception('Flow logger is not defined')
        # node_logger = flow_logger.get_node_logger(self.node_id)
        if reset_cache:
            self.remove_cache()
            self.node_stats.has_run = False
        if self.is_setup:
            node_logger.info(f'Starting to run {self.__name__}')
            if self.needs_run(performance_mode, node_logger, run_location):
                self.prepare_before_run()
                try:
                    if ((run_location == 'remote' or (self.node_default.transform_type == 'wide')
                            and not run_location == 'local')) or self.node_settings.cache_results:
                        node_logger.info('Running the node remotely')
                        if self.node_settings.cache_results:
                            performance_mode = False
                        self.execute_remote(performance_mode=(performance_mode if not self.node_settings.cache_results
                                                              else False),
                                            node_logger=node_logger
                                            )
                    else:
                        node_logger.info('Running the node locally')
                        self.execute_local(performance_mode=performance_mode, flow_id=node_logger.flow_id)
                except Exception as e:
                    if 'No such file or directory (os error' in str(e) and retry:
                        logger.warning('Error with the input node, starting to rerun the input node...')
                        all_inputs: List[FlowNode] = self.node_inputs.get_all_inputs()
                        for node_input in all_inputs:
                            node_input.execute_node(run_location=run_location,
                                                    performance_mode=performance_mode, retry=True,
                                                    reset_cache=True,
                                                    node_logger=node_logger)
                        self.execute_node(run_location=run_location,
                                          performance_mode=performance_mode, retry=False,
                                          node_logger=node_logger)
                    else:
                        self.node_stats.has_run = False
                        self.results.errors = str(e)
                        node_logger.error(f'Error with running the node: {e}')

            else:
                node_logger.info('Node has already run, not running the node')
        else:
            node_logger.warning(f'Node {self.__name__} is not setup, cannot run the node')

    def store_example_data_generator(self, external_df_fetcher: ExternalDfFetcher | ExternalSampler):
        if external_df_fetcher.status is not None:
            file_ref = external_df_fetcher.status.file_ref
            self.results.example_data_path = file_ref
            self.results.example_data_generator = get_read_top_n(file_path=file_ref, n=100)
        else:
            logger.error('Could not get the sample data, the external process is not ready')

    def needs_reset(self) -> bool:
        return self._hash != self.calculate_hash(self.setting_input)

    def reset(self, deep: bool = False):
        needs_reset = self.needs_reset() or deep
        if needs_reset:
            logger.info(f'{self.node_id}: Node needs reset')
            self.node_stats.has_run = False
            self.results.reset()
            if self.schema_callback:
                self.schema_callback.reset()
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
        if connection_type == 'input-0':
            for i, node in enumerate(self.node_inputs.main_inputs):
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
        return f"Node id: {self.node_id} ({self.node_type})"

    def _get_readable_schema(self):
        if self.is_setup:
            output = []
            for s in self.schema:
                output.append(dict(column_name=s.column_name, data_type=s.data_type))
            return output

    def get_repr(self):
        return dict(FlowNode=
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
    def singular_main_input(self) -> "FlowNode":
        if self.singular_input:
            return self.all_inputs[0]

    def get_table_example(self, include_data: bool = False) -> TableExample | None:
        self.print('Getting a table example')
        if self.node_type == 'output':
            self.print('getting the table example')
            return self.main_input[0].get_table_example(include_data)
        if self.is_setup and include_data:
            logger.info('getting the table example since the node has run')
            example_data_getter = self.results.example_data_generator
            if example_data_getter is not None:
                data = example_data_getter().to_pylist()
                if data is None:
                    data = []
            else:
                data = []
            schema = [FileColumn.model_validate(c.get_column_repr()) for c in self.schema]
            fl = self.get_resulting_data()
            return TableExample(node_id=self.node_id,
                                name=str(self.node_id), number_of_records=999,
                                number_of_columns=fl.number_of_fields,
                                table_schema=schema, columns=fl.columns, data=data)
        else:
            logger.warning('getting the table example but the node has not run')
            try:
                schema = [FileColumn.model_validate(c.get_column_repr()) for c in self.schema]
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
