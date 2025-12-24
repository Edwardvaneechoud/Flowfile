import datetime

import os
import yaml
import json

import polars as pl
from pathlib import Path

import fastexcel
from fastapi.exceptions import HTTPException
from time import time
from functools import partial
from typing import List, Dict, Union, Callable, Any, Optional, Tuple, Literal
from uuid import uuid1
from copy import deepcopy
from pyarrow.parquet import ParquetFile
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.flowfile.sources.external_sources.factory import data_source_factory
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn, cast_str_to_polars_type

from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.schemas.transform_schema import FuzzyMatchInputManager
from flowfile_core.utils.arrow_reader import get_read_top_n
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine, execute_polars_code
from flowfile_core.flowfile.flow_data_engine.read_excel_tables import (get_open_xlsx_datatypes,
                                                                       get_calamine_xlsx_data_types)

from flowfile_core.flowfile.schema_callbacks import (calculate_fuzzy_match_schema, pre_calculate_pivot_schema)
from flowfile_core.flowfile.sources import external_sources
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import NodeData, NodeResult, RunInformation
from flowfile_core.schemas.cloud_storage_schemas import (CloudStorageReadSettingsInternal,
                                                         CloudStorageWriteSettingsInternal,
                                                         FullCloudStorageConnection,
                                                         get_cloud_storage_write_settings_worker_interface, AuthMethod)
from flowfile_core.flowfile.utils import snake_case_to_camel_case
from flowfile_core.flowfile.analytics.utils import create_graphic_walker_node_from_node_promise
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
from flowfile_core.flowfile.graph_tree.graph_tree import (add_un_drawn_nodes, build_flow_paths,
                                                          build_node_info, calculate_depth,
                                                          define_node_connections, draw_merged_paths,
                                                          draw_standalone_paths, group_nodes_by_depth)
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (ExternalDatabaseFetcher,
                                                                                                 ExternalDatabaseWriter,
                                                                                                 ExternalDfFetcher,
                                                                                                 ExternalCloudWriter)
from flowfile_core.secret_manager.secret_manager import get_encrypted_secret, decrypt_secret
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils, models as sql_models
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource, BaseSqlSource
from flowfile_core.flowfile.database_connection_manager.db_connections import (get_local_database_connection,
                                                                               get_local_cloud_connection)
from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("Flowfile")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"


def represent_list_json(dumper, data):
    """Use inline style for short simple lists, block style for complex ones."""
    if len(data) <= 10 and all(isinstance(item, (int, str, float, bool, type(None))) for item in data):
        return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)
    return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=False)


yaml.add_representer(list, represent_list_json)


def get_xlsx_schema(engine: str, file_path: str, sheet_name: str, start_row: int, start_column: int,
                    end_row: int, end_column: int, has_headers: bool):
    """Calculates the schema of an XLSX file by reading a sample of rows.

    Args:
        engine: The engine to use for reading ('openpyxl' or 'calamine').
        file_path: The path to the XLSX file.
        sheet_name: The name of the sheet to read.
        start_row: The starting row for data reading.
        start_column: The starting column for data reading.
        end_row: The ending row for data reading.
        end_column: The ending column for data reading.
        has_headers: A boolean indicating if the file has a header row.

    Returns:
        A list of FlowfileColumn objects representing the schema.
    """
    try:
        logger.info('Starting to calculate the schema')
        if engine == 'openpyxl':
            max_col = end_column if end_column > 0 else None
            return get_open_xlsx_datatypes(file_path=file_path,
                                           sheet_name=sheet_name,
                                           min_row=start_row + 1,
                                           min_col=start_column + 1,
                                           max_row=100,
                                           max_col=max_col, has_headers=has_headers)
        elif engine == 'calamine':
            return get_calamine_xlsx_data_types(file_path=file_path,
                                                sheet_name=sheet_name,
                                                start_row=start_row,
                                                end_row=end_row)
        logger.info('done calculating the schema')
    except Exception as e:
        logger.error(e)
        return []


def skip_node_message(flow_logger: FlowLogger, nodes: List[FlowNode]) -> None:
    """Logs a warning message listing all nodes that will be skipped during execution.

    Args:
        flow_logger: The logger instance for the flow.
        nodes: A list of FlowNode objects to be skipped.
    """
    if len(nodes) > 0:
        msg = "\n".join(str(node) for node in nodes)
        flow_logger.warning(f'skipping nodes:\n{msg}')


def execution_order_message(flow_logger: FlowLogger, nodes: List[FlowNode]) -> None:
    """Logs an informational message showing the determined execution order of nodes.

    Args:
        flow_logger: The logger instance for the flow.
        nodes: A list of FlowNode objects in the order they will be executed.
    """
    msg = "\n".join(str(node) for node in nodes)
    flow_logger.info(f'execution order:\n{msg}')


def get_xlsx_schema_callback(engine: str, file_path: str, sheet_name: str, start_row: int, start_column: int,
                             end_row: int, end_column: int, has_headers: bool):
    """Creates a partially applied function for lazy calculation of an XLSX schema.

    Args:
        engine: The engine to use for reading.
        file_path: The path to the XLSX file.
        sheet_name: The name of the sheet.
        start_row: The starting row.
        start_column: The starting column.
        end_row: The ending row.
        end_column: The ending column.
        has_headers: A boolean indicating if the file has headers.

    Returns:
        A callable function that, when called, will execute `get_xlsx_schema`.
    """
    return partial(get_xlsx_schema, engine=engine, file_path=file_path, sheet_name=sheet_name, start_row=start_row,
                   start_column=start_column, end_row=end_row, end_column=end_column, has_headers=has_headers)


def get_cloud_connection_settings(connection_name: str,
                                  user_id: int, auth_mode: AuthMethod) -> FullCloudStorageConnection:
    """Retrieves cloud storage connection settings, falling back to environment variables if needed.

    Args:
        connection_name: The name of the saved connection.
        user_id: The ID of the user owning the connection.
        auth_mode: The authentication method specified by the user.

    Returns:
        A FullCloudStorageConnection object with the connection details.

    Raises:
        HTTPException: If the connection settings cannot be found.
    """
    cloud_connection_settings = get_local_cloud_connection(connection_name, user_id)
    if cloud_connection_settings is None and auth_mode in ("env_vars", transform_schema.AUTO_DATA_TYPE):
        # If the auth mode is aws-cli, we do not need connection settings
        cloud_connection_settings = FullCloudStorageConnection(storage_type="s3", auth_method="env_vars")
    elif cloud_connection_settings is None and auth_mode == "aws-cli":
        cloud_connection_settings = FullCloudStorageConnection(storage_type="s3", auth_method="aws-cli")
    if cloud_connection_settings is None:
        raise HTTPException(status_code=400, detail="Cloud connection settings not found")
    return cloud_connection_settings


class FlowGraph:
    """A class representing a Directed Acyclic Graph (DAG) for data processing pipelines.

    It manages nodes, connections, and the execution of the entire flow.
    """
    uuid: str
    depends_on: Dict[int, Union[ParquetFile, FlowDataEngine, "FlowGraph", pl.DataFrame,]]
    _flow_id: int
    _input_data: Union[ParquetFile, FlowDataEngine, "FlowGraph"]
    _input_cols: List[str]
    _output_cols: List[str]
    _node_db: Dict[Union[str, int], FlowNode]
    _node_ids: List[Union[str, int]]
    _results: Optional[FlowDataEngine] = None
    cache_results: bool = False
    schema: Optional[List[FlowfileColumn]] = None
    has_over_row_function: bool = False
    _flow_starts: List[Union[int, str]] = None
    latest_run_info: Optional[RunInformation] = None
    start_datetime: datetime = None
    end_datetime: datetime = None
    _flow_settings: schemas.FlowSettings = None
    flow_logger: FlowLogger

    def __init__(self,
                 flow_settings: schemas.FlowSettings | schemas.FlowGraphConfig,
                 name: str = None, input_cols: List[str] = None,
                 output_cols: List[str] = None,
                 path_ref: str = None,
                 input_flow: Union[ParquetFile, FlowDataEngine, "FlowGraph"] = None,
                 cache_results: bool = False):
        """Initializes a new FlowGraph instance.

        Args:
            flow_settings: The configuration settings for the flow.
            name: The name of the flow.
            input_cols: A list of input column names.
            output_cols: A list of output column names.
            path_ref: An optional path to an initial data source.
            input_flow: An optional existing data object to start the flow with.
            cache_results: A global flag to enable or disable result caching.
        """
        if isinstance(flow_settings, schemas.FlowGraphConfig):
            flow_settings = schemas.FlowSettings.from_flow_settings_input(flow_settings)

        self._flow_settings = flow_settings
        self.uuid = str(uuid1())
        self.start_datetime = None
        self.end_datetime = None
        self.latest_run_info = None
        self._flow_id = flow_settings.flow_id
        self.flow_logger = FlowLogger(flow_settings.flow_id)
        self._flow_starts: List[FlowNode] = []
        self._results = None
        self.schema = None
        self.has_over_row_function = False
        self._input_cols = [] if input_cols is None else input_cols
        self._output_cols = [] if output_cols is None else output_cols
        self._node_ids = []
        self._node_db = {}
        self.cache_results = cache_results
        self.__name__ = name if name else "flow_" + str(id(self))
        self.depends_on = {}
        if path_ref is not None:
            self.add_datasource(input_schema.NodeDatasource(file_path=path_ref))
        elif input_flow is not None:
            self.add_datasource(input_file=input_flow)

    @property
    def flow_settings(self) -> schemas.FlowSettings:
        return self._flow_settings

    @flow_settings.setter
    def flow_settings(self, flow_settings: schemas.FlowSettings):
        if (
                (self._flow_settings.execution_location != flow_settings.execution_location) or
                (self._flow_settings.execution_mode != flow_settings.execution_mode)
        ):
            self.reset()
        self._flow_settings = flow_settings

    def add_node_promise(self, node_promise: input_schema.NodePromise):
        """Adds a placeholder node to the graph that is not yet fully configured.

        Useful for building the graph structure before all settings are available.

        Args:
            node_promise: A promise object containing basic node information.
        """
        def placeholder(n: FlowNode = None):
            if n is None:
                return FlowDataEngine()
            return n

        self.add_node_step(node_id=node_promise.node_id, node_type=node_promise.node_type, function=placeholder,
                           setting_input=node_promise)

    def apply_layout(self, y_spacing: int = 150, x_spacing: int = 200, initial_y: int = 100):
        """Calculates and applies a layered layout to all nodes in the graph.

        This updates their x and y positions for UI rendering.

        Args:
            y_spacing: The vertical spacing between layers.
            x_spacing: The horizontal spacing between nodes in the same layer.
            initial_y: The initial y-position for the first layer.
        """
        self.flow_logger.info("Applying layered layout...")
        start_time = time()
        try:
            # Calculate new positions for all nodes
            new_positions = calculate_layered_layout(
                self, y_spacing=y_spacing, x_spacing=x_spacing, initial_y=initial_y
            )

            if not new_positions:
                self.flow_logger.warning("Layout calculation returned no positions.")
                return

            # Apply the new positions to the setting_input of each node
            updated_count = 0
            for node_id, (pos_x, pos_y) in new_positions.items():
                node = self.get_node(node_id)
                if node and hasattr(node, 'setting_input'):
                    setting = node.setting_input
                    if hasattr(setting, 'pos_x') and hasattr(setting, 'pos_y'):
                        setting.pos_x = pos_x
                        setting.pos_y = pos_y
                        updated_count += 1
                    else:
                        self.flow_logger.warning(f"Node {node_id} setting_input ({type(setting)}) lacks pos_x/pos_y attributes.")
                elif node:
                    self.flow_logger.warning(f"Node {node_id} lacks setting_input attribute.")
                # else: Node not found, already warned by calculate_layered_layout

            end_time = time()
            self.flow_logger.info(f"Layout applied to {updated_count}/{len(self.nodes)} nodes in {end_time - start_time:.2f} seconds.")

        except Exception as e:
            self.flow_logger.error(f"Error applying layout: {e}")
            raise  # Optional: re-raise the exception

    @property
    def flow_id(self) -> int:
        """Gets the unique identifier of the flow."""
        return self._flow_id

    @flow_id.setter
    def flow_id(self, new_id: int):
        """Sets the unique identifier for the flow and updates all child nodes.

        Args:
            new_id: The new flow ID.
        """
        self._flow_id = new_id
        for node in self.nodes:
            if hasattr(node.setting_input, 'flow_id'):
                node.setting_input.flow_id = new_id
        self.flow_settings.flow_id = new_id

    def __repr__(self):
        """Provides the official string representation of the FlowGraph instance."""
        settings_str = "  -" + '\n  -'.join(f"{k}: {v}" for k, v in self.flow_settings)
        return f"FlowGraph(\nNodes: {self._node_db}\n\nSettings:\n{settings_str}"

    def print_tree(self):
        """Print flow_graph as a visual tree structure, showing the DAG relationships with ASCII art."""
        if not self._node_db:
            self.flow_logger.info("Empty flow graph")
            return

        # Build node information
        node_info = build_node_info(self.nodes)

        # Calculate depths for all nodes
        for node_id in node_info:
            calculate_depth(node_id, node_info)

        # Group nodes by depth
        depth_groups, max_depth = group_nodes_by_depth(node_info)
        
        # Sort nodes within each depth group
        for depth in depth_groups:
            depth_groups[depth].sort()

        # Create the main flow visualization
        lines = ["=" * 80, "Flow Graph Visualization", "=" * 80, ""]

        # Track which nodes connect to what
        merge_points = define_node_connections(node_info)
        
        # Build the flow paths

        # Find the maximum label length for each depth level
        max_label_length = {}
        for depth in range(max_depth + 1):
            if depth in depth_groups:
                max_len = max(len(node_info[nid].label) for nid in depth_groups[depth])
                max_label_length[depth] = max_len
        
        # Draw the paths
        drawn_nodes = set()
        merge_drawn = set()
        
        # Group paths by their merge points
        paths_by_merge = {}
        standalone_paths = []
        
        # Build flow paths
        paths = build_flow_paths(node_info, self._flow_starts, merge_points)

        # Define paths to merge and standalone paths
        for path in paths:
            if len(path) > 1 and path[-1] in merge_points and len(merge_points[path[-1]]) > 1:
                merge_id = path[-1]
                if merge_id not in paths_by_merge:
                    paths_by_merge[merge_id] = []
                paths_by_merge[merge_id].append(path)
            else:
                standalone_paths.append(path)

        # Draw merged paths
        draw_merged_paths(node_info, merge_points, paths_by_merge, merge_drawn, drawn_nodes, lines)

        # Draw standlone paths
        draw_standalone_paths(drawn_nodes, standalone_paths, lines, node_info)

        # Add undrawn nodes
        add_un_drawn_nodes(drawn_nodes, node_info, lines)
        
        try:
            skip_nodes, ordered_nodes = compute_execution_plan(
                nodes=self.nodes,
                flow_starts=self._flow_starts+self.get_implicit_starter_nodes())
            if ordered_nodes:
                for i, node in enumerate(ordered_nodes, 1):
                    lines.append(f"  {i:3d}. {node_info[node.node_id].label}")
        except Exception as e:
            lines.append(f"  Could not determine execution order: {e}")
        
        # Print everything
        output = "\n".join(lines)
        
        print(output)
        
    def get_nodes_overview(self):
        """Gets a list of dictionary representations for all nodes in the graph."""
        output = []
        for v in self._node_db.values():
            output.append(v.get_repr())
        return output

    def remove_from_output_cols(self, columns: List[str]):
        """Removes specified columns from the list of expected output columns.

        Args:
            columns: A list of column names to remove.
        """
        cols = set(columns)
        self._output_cols = [c for c in self._output_cols if c not in cols]

    def get_node(self, node_id: Union[int, str] = None) -> FlowNode | None:
        """Retrieves a node from the graph by its ID.

        Args:
            node_id: The ID of the node to retrieve. If None, retrieves the last added node.

        Returns:
            The FlowNode object, or None if not found.
        """
        if node_id is None:
            node_id = self._node_ids[-1]
        node = self._node_db.get(node_id)
        if node is not None:
            return node
        
    def add_user_defined_node(self, *,
                              custom_node: CustomNodeBase,
                              user_defined_node_settings: input_schema.UserDefinedNode
                              ):
       
        def _func(*fdes: FlowDataEngine) -> FlowDataEngine | None:
            output = custom_node.process(*(fde.data_frame for fde in fdes))
            if isinstance(output, pl.LazyFrame | pl.DataFrame):
                return FlowDataEngine(output)
            return None
        
        self.add_node_step(node_id=user_defined_node_settings.node_id,
                           function=_func,
                           setting_input=user_defined_node_settings,
                           input_node_ids=user_defined_node_settings.depending_on_ids,
                           node_type=custom_node.item,
                           )

    def add_pivot(self, pivot_settings: input_schema.NodePivot):
        """Adds a pivot node to the graph.

        Args:
            pivot_settings: The settings for the pivot operation.
        """

        def _func(fl: FlowDataEngine):
            return fl.do_pivot(pivot_settings.pivot_input, self.flow_logger.get_node_logger(pivot_settings.node_id))

        self.add_node_step(node_id=pivot_settings.node_id,
                           function=_func,
                           node_type='pivot',
                           setting_input=pivot_settings,
                           input_node_ids=[pivot_settings.depending_on_id])

        node = self.get_node(pivot_settings.node_id)

        def schema_callback():
            input_data = node.singular_main_input.get_resulting_data()  # get from the previous step the data
            input_data.lazy = True  # ensure the dataset is lazy
            input_lf = input_data.data_frame  # get the lazy frame
            return pre_calculate_pivot_schema(input_data.schema, pivot_settings.pivot_input, input_lf=input_lf)
        node.schema_callback = schema_callback

    def add_unpivot(self, unpivot_settings: input_schema.NodeUnpivot):
        """Adds an unpivot node to the graph.

        Args:
            unpivot_settings: The settings for the unpivot operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.unpivot(unpivot_settings.unpivot_input)

        self.add_node_step(node_id=unpivot_settings.node_id,
                           function=_func,
                           node_type='unpivot',
                           setting_input=unpivot_settings,
                           input_node_ids=[unpivot_settings.depending_on_id])

    def add_union(self, union_settings: input_schema.NodeUnion):
        """Adds a union node to combine multiple data streams.

        Args:
            union_settings: The settings for the union operation.
        """

        def _func(*flowfile_tables: FlowDataEngine):
            dfs: List[pl.LazyFrame] | List[pl.DataFrame] = [flt.data_frame for flt in flowfile_tables]
            return FlowDataEngine(pl.concat(dfs, how='diagonal_relaxed'))

        self.add_node_step(node_id=union_settings.node_id,
                           function=_func,
                           node_type=f'union',
                           setting_input=union_settings,
                           input_node_ids=union_settings.depending_on_ids)

    def add_initial_node_analysis(self, node_promise: input_schema.NodePromise):
        """Adds a data exploration/analysis node based on a node promise.

        Args:
            node_promise: The promise representing the node to be analyzed.
        """
        node_analysis = create_graphic_walker_node_from_node_promise(node_promise)
        self.add_explore_data(node_analysis)

    def add_explore_data(self, node_analysis: input_schema.NodeExploreData):
        """Adds a specialized node for data exploration and visualization.

        Args:
            node_analysis: The settings for the data exploration node.
        """
        sample_size: int = 10000

        def analysis_preparation(flowfile_table: FlowDataEngine):
            if flowfile_table.number_of_records <= 0:
                number_of_records = flowfile_table.get_number_of_records(calculate_in_worker_process=True)
            else:
                number_of_records = flowfile_table.number_of_records
            if number_of_records > sample_size:
                flowfile_table = flowfile_table.get_sample(sample_size, random=True)
            external_sampler = ExternalDfFetcher(
                lf=flowfile_table.data_frame,
                file_ref="__gf_walker"+node.hash,
                wait_on_completion=True,
                node_id=node.node_id,
                flow_id=self.flow_id,
            )
            node.results.analysis_data_generator = get_read_top_n(external_sampler.status.file_ref,
                                                                  n=min(sample_size, number_of_records))
            return flowfile_table

        def schema_callback():
            node = self.get_node(node_analysis.node_id)
            if len(node.all_inputs) == 1:
                input_node = node.all_inputs[0]
                return input_node.schema
            else:
                return [FlowfileColumn.from_input('col_1', 'na')]

        self.add_node_step(node_id=node_analysis.node_id, node_type='explore_data',
                           function=analysis_preparation,
                           setting_input=node_analysis, schema_callback=schema_callback)
        node = self.get_node(node_analysis.node_id)

    def add_group_by(self, group_by_settings: input_schema.NodeGroupBy):
        """Adds a group-by aggregation node to the graph.

        Args:
            group_by_settings: The settings for the group-by operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.do_group_by(group_by_settings.groupby_input, False)

        self.add_node_step(node_id=group_by_settings.node_id,
                           function=_func,
                           node_type=f'group_by',
                           setting_input=group_by_settings,
                           input_node_ids=[group_by_settings.depending_on_id])

        node = self.get_node(group_by_settings.node_id)

        def schema_callback():

            output_columns = [(c.old_name, c.new_name, c.output_type) for c in group_by_settings.groupby_input.agg_cols]
            depends_on = node.node_inputs.main_inputs[0]
            input_schema_dict: Dict[str, str] = {s.name: s.data_type for s in depends_on.schema}
            output_schema = []
            for old_name, new_name, data_type in output_columns:
                data_type = input_schema_dict[old_name] if data_type is None else data_type
                output_schema.append(FlowfileColumn.from_input(data_type=data_type, column_name=new_name))
            return output_schema

        node.schema_callback = schema_callback

    def add_filter(self, filter_settings: input_schema.NodeFilter):
        """Adds a filter node to the graph.

        Args:
            filter_settings: The settings for the filter operation.
        """

        is_advanced = filter_settings.filter_input.filter_type == 'advanced'
        if is_advanced:
            predicate = filter_settings.filter_input.advanced_filter
        else:
            _basic_filter = filter_settings.filter_input.basic_filter
            filter_settings.filter_input.advanced_filter = (f'[{_basic_filter.field}]{_basic_filter.filter_type}"'
                                                            f'{_basic_filter.filter_value}"')

        def _func(fl: FlowDataEngine):
            is_advanced = filter_settings.filter_input.filter_type == 'advanced'
            if is_advanced:
                return fl.do_filter(predicate)
            else:
                basic_filter = filter_settings.filter_input.basic_filter
                if basic_filter.filter_value.isnumeric():
                    field_data_type = fl.get_schema_column(basic_filter.field).generic_datatype()
                    if field_data_type == 'str':
                        _f = f'[{basic_filter.field}]{basic_filter.filter_type}"{basic_filter.filter_value}"'
                    else:
                        _f = f'[{basic_filter.field}]{basic_filter.filter_type}{basic_filter.filter_value}'
                else:
                    _f = f'[{basic_filter.field}]{basic_filter.filter_type}"{basic_filter.filter_value}"'
                filter_settings.filter_input.advanced_filter = _f
                return fl.do_filter(_f)

        self.add_node_step(filter_settings.node_id, _func,
                           node_type='filter',
                           renew_schema=False,
                           setting_input=filter_settings,
                           input_node_ids=[filter_settings.depending_on_id]
                           )

    def add_record_count(self, node_number_of_records: input_schema.NodeRecordCount):
        """Adds a filter node to the graph.

        Args:
            node_number_of_records: The settings for the record count operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.get_record_count()

        self.add_node_step(node_id=node_number_of_records.node_id,
                           function=_func,
                           node_type='record_count',
                           setting_input=node_number_of_records,
                           input_node_ids=[node_number_of_records.depending_on_id])

    def add_polars_code(self, node_polars_code: input_schema.NodePolarsCode):
        """Adds a node that executes custom Polars code.

        Args:
            node_polars_code: The settings for the Polars code node.
        """

        def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
            return execute_polars_code(*flowfile_tables, code=node_polars_code.polars_code_input.polars_code)
        self.add_node_step(node_id=node_polars_code.node_id,
                           function=_func,
                           node_type='polars_code',
                           setting_input=node_polars_code,
                           input_node_ids=node_polars_code.depending_on_ids)

        try:
            polars_code_parser.validate_code(node_polars_code.polars_code_input.polars_code)
        except Exception as e:
            node = self.get_node(node_id=node_polars_code.node_id)
            node.results.errors = str(e)

    def add_dependency_on_polars_lazy_frame(self,
                                            lazy_frame: pl.LazyFrame,
                                            node_id: int):
        """Adds a special node that directly injects a Polars LazyFrame into the graph.

        Note: This is intended for backend use and will not work in the UI editor.

        Args:
            lazy_frame: The Polars LazyFrame to inject.
            node_id: The ID for the new node.
        """
        def _func():
            return FlowDataEngine(lazy_frame)
        node_promise = input_schema.NodePromise(flow_id=self.flow_id,
                                                node_id=node_id, node_type="polars_lazy_frame",
                                                is_setup=True)
        self.add_node_step(node_id=node_promise.node_id, node_type=node_promise.node_type, function=_func,
                           setting_input=node_promise)

    def add_unique(self, unique_settings: input_schema.NodeUnique):
        """Adds a node to find and remove duplicate rows.

        Args:
            unique_settings: The settings for the unique operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.make_unique(unique_settings.unique_input)

        self.add_node_step(node_id=unique_settings.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='unique',
                           setting_input=unique_settings,
                           input_node_ids=[unique_settings.depending_on_id])

    def add_graph_solver(self, graph_solver_settings: input_schema.NodeGraphSolver):
        """Adds a node that solves graph-like problems within the data.

        This node can be used for operations like finding network paths,
        calculating connected components, or performing other graph algorithms
        on relational data that represents nodes and edges.

        Args:
            graph_solver_settings: The settings object defining the graph inputs
                and the specific algorithm to apply.
        """
        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.solve_graph(graph_solver_settings.graph_solver_input)

        self.add_node_step(node_id=graph_solver_settings.node_id,
                           function=_func,
                           node_type='graph_solver',
                           setting_input=graph_solver_settings,
                           input_node_ids=[graph_solver_settings.depending_on_id])

    def add_formula(self, function_settings: input_schema.NodeFormula):
        """Adds a node that applies a formula to create or modify a column.

        Args:
            function_settings: The settings for the formula operation.
        """

        error = ""
        if function_settings.function.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
            output_type = cast_str_to_polars_type(function_settings.function.field.data_type)
        else:
            output_type = None
        if output_type not in (None, transform_schema.AUTO_DATA_TYPE):
            new_col = [FlowfileColumn.from_input(column_name=function_settings.function.field.name,
                                                 data_type=str(output_type))]
        else:
            new_col = [FlowfileColumn.from_input(function_settings.function.field.name, 'String')]

        def _func(fl: FlowDataEngine):
            return fl.apply_sql_formula(func=function_settings.function.function,
                                        col_name=function_settings.function.field.name,
                                        output_data_type=output_type)

        self.add_node_step(function_settings.node_id, _func,
                           output_schema=new_col,
                           node_type='formula',
                           renew_schema=False,
                           setting_input=function_settings,
                           input_node_ids=[function_settings.depending_on_id]
                           )
        # TODO: Add validation here
        if error != "":
            node = self.get_node(function_settings.node_id)
            node.results.errors = error
            return False, error
        else:
            return True, ""

    def add_cross_join(self, cross_join_settings: input_schema.NodeCrossJoin) -> "FlowGraph":
        """Adds a cross join node to the graph.

        Args:
            cross_join_settings: The settings for the cross join operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            for left_select in cross_join_settings.cross_join_input.left_select.renames:
                left_select.is_available = True if left_select.old_name in main.schema else False
            for right_select in cross_join_settings.cross_join_input.right_select.renames:
                right_select.is_available = True if right_select.old_name in right.schema else False
            return main.do_cross_join(cross_join_input=cross_join_settings.cross_join_input,
                                      auto_generate_selection=cross_join_settings.auto_generate_selection,
                                      verify_integrity=False,
                                      other=right)

        self.add_node_step(node_id=cross_join_settings.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='cross_join',
                           setting_input=cross_join_settings,
                           input_node_ids=cross_join_settings.depending_on_ids)
        return self

    def add_join(self, join_settings: input_schema.NodeJoin) -> "FlowGraph":
        """Adds a join node to combine two data streams based on key columns.

        Args:
            join_settings: The settings for the join operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            for left_select in join_settings.join_input.left_select.renames:
                left_select.is_available = True if left_select.old_name in main.schema else False
            for right_select in join_settings.join_input.right_select.renames:
                right_select.is_available = True if right_select.old_name in right.schema else False
            return main.join(join_input=join_settings.join_input,
                             auto_generate_selection=join_settings.auto_generate_selection,
                             verify_integrity=False,
                             other=right)

        self.add_node_step(node_id=join_settings.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='join',
                           setting_input=join_settings,
                           input_node_ids=join_settings.depending_on_ids)
        return self

    def add_fuzzy_match(self, fuzzy_settings: input_schema.NodeFuzzyMatch) -> "FlowGraph":
        """Adds a fuzzy matching node to join data on approximate string matches.

        Args:
            fuzzy_settings: The settings for the fuzzy match operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            node = self.get_node(node_id=fuzzy_settings.node_id)
            if self.execution_location == "local":
                return main.fuzzy_join(fuzzy_match_input=deepcopy(fuzzy_settings.join_input),
                                       other=right,
                                       node_logger=self.flow_logger.get_node_logger(fuzzy_settings.node_id))

            f = main.start_fuzzy_join(fuzzy_match_input=deepcopy(fuzzy_settings.join_input), other=right, file_ref=node.hash,
                                      flow_id=self.flow_id, node_id=fuzzy_settings.node_id)
            logger.info("Started the fuzzy match action")
            node._fetch_cached_df = f  # Add to the node so it can be cancelled and fetch later if needed
            return FlowDataEngine(f.get_result())

        def schema_callback():
            fm_input_copy = FuzzyMatchInputManager(fuzzy_settings.join_input)  # Deepcopy create an unique object per func
            node = self.get_node(node_id=fuzzy_settings.node_id)
            return calculate_fuzzy_match_schema(fm_input_copy,
                                                left_schema=node.node_inputs.main_inputs[0].schema,
                                                right_schema=node.node_inputs.right_input.schema
                                                )

        self.add_node_step(node_id=fuzzy_settings.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='fuzzy_match',
                           setting_input=fuzzy_settings,
                           input_node_ids=fuzzy_settings.depending_on_ids,
                           schema_callback=schema_callback)

        return self

    def add_text_to_rows(self, node_text_to_rows: input_schema.NodeTextToRows) -> "FlowGraph":
        """Adds a node that splits cell values into multiple rows.

        This is useful for un-nesting data where a single field contains multiple
        values separated by a delimiter.

        Args:
            node_text_to_rows: The settings object that specifies the column to split
                and the delimiter to use.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.split(node_text_to_rows.text_to_rows_input)

        self.add_node_step(node_id=node_text_to_rows.node_id,
                           function=_func,
                           node_type='text_to_rows',
                           setting_input=node_text_to_rows,
                           input_node_ids=[node_text_to_rows.depending_on_id])
        return self

    def add_sort(self, sort_settings: input_schema.NodeSort) -> "FlowGraph":
        """Adds a node to sort the data based on one or more columns.

        Args:
            sort_settings: The settings for the sort operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.do_sort(sort_settings.sort_input)

        self.add_node_step(node_id=sort_settings.node_id,
                           function=_func,
                           node_type='sort',
                           setting_input=sort_settings,
                           input_node_ids=[sort_settings.depending_on_id])
        return self

    def add_sample(self, sample_settings: input_schema.NodeSample) -> "FlowGraph":
        """Adds a node to take a random or top-N sample of the data.

        Args:
            sample_settings: The settings object specifying the size of the sample.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.get_sample(sample_settings.sample_size)

        self.add_node_step(node_id=sample_settings.node_id,
                           function=_func,
                           node_type='sample',
                           setting_input=sample_settings,
                           input_node_ids=[sample_settings.depending_on_id]
                           )
        return self

    def add_record_id(self, record_id_settings: input_schema.NodeRecordId) -> "FlowGraph":
        """Adds a node to create a new column with a unique ID for each record.

        Args:
            record_id_settings: The settings object specifying the name of the
                new record ID column.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.add_record_id(record_id_settings.record_id_input)

        self.add_node_step(node_id=record_id_settings.node_id,
                           function=_func,
                           node_type='record_id',
                           setting_input=record_id_settings,
                           input_node_ids=[record_id_settings.depending_on_id]
                           )
        return self

    def add_select(self, select_settings: input_schema.NodeSelect) -> "FlowGraph":
        """Adds a node to select, rename, reorder, or drop columns.

        Args:
            select_settings: The settings for the select operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        select_cols = select_settings.select_input
        drop_cols = tuple(s.old_name for s in select_settings.select_input)

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            input_cols = set(f.name for f in table.schema)
            ids_to_remove = []
            for i, select_col in enumerate(select_cols):
                if select_col.data_type is None:
                    select_col.data_type = table.get_schema_column(select_col.old_name).data_type
                if select_col.old_name not in input_cols:
                    select_col.is_available = False
                    if not select_col.keep:
                        ids_to_remove.append(i)
                else:
                    select_col.is_available = True
            ids_to_remove.reverse()
            for i in ids_to_remove:
                v = select_cols.pop(i)
                del v
            return table.do_select(select_inputs=transform_schema.SelectInputs(select_cols),
                                   keep_missing=select_settings.keep_missing)

        self.add_node_step(node_id=select_settings.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='select',
                           drop_columns=list(drop_cols),
                           setting_input=select_settings,
                           input_node_ids=[select_settings.depending_on_id])
        return self

    @property
    def graph_has_functions(self) -> bool:
        """Checks if the graph has any nodes."""
        return len(self._node_ids) > 0

    def delete_node(self, node_id: Union[int, str]):
        """Deletes a node from the graph and updates all its connections.

        Args:
            node_id: The ID of the node to delete.

        Raises:
            Exception: If the node with the given ID does not exist.
        """
        logger.info(f"Starting deletion of node with ID: {node_id}")

        node = self._node_db.get(node_id)
        if node:
            logger.info(f"Found node: {node_id}, processing deletion")

            lead_to_steps: List[FlowNode] = node.leads_to_nodes
            logger.debug(f"Node {node_id} leads to {len(lead_to_steps)} other nodes")

            if len(lead_to_steps) > 0:
                for lead_to_step in lead_to_steps:
                    logger.debug(f"Deleting input node {node_id} from dependent node {lead_to_step}")
                    lead_to_step.delete_input_node(node_id, complete=True)

            if not node.is_start:
                depends_on: List[FlowNode] = node.node_inputs.get_all_inputs()
                logger.debug(f"Node {node_id} depends on {len(depends_on)} other nodes")

                for depend_on in depends_on:
                    logger.debug(f"Removing lead_to reference {node_id} from node {depend_on}")
                    depend_on.delete_lead_to_node(node_id)

            self._node_db.pop(node_id)
            logger.debug(f"Successfully removed node {node_id} from node_db")
            del node
            logger.info("Node object deleted")
        else:
            logger.error(f"Failed to find node with id {node_id}")
            raise Exception(f"Node with id {node_id} does not exist")

    @property
    def graph_has_input_data(self) -> bool:
        """Checks if the graph has an initial input data source."""
        return self._input_data is not None

    def add_node_step(self,
                      node_id: Union[int, str],
                      function: Callable,
                      input_columns: List[str] = None,
                      output_schema: List[FlowfileColumn] = None,
                      node_type: str = None,
                      drop_columns: List[str] = None,
                      renew_schema: bool = True,
                      setting_input: Any = None,
                      cache_results: bool = None,
                      schema_callback: Callable = None,
                      input_node_ids: List[int] = None) -> FlowNode:
        """The core method for adding or updating a node in the graph.

        Args:
            node_id: The unique ID for the node.
            function: The core processing function for the node.
            input_columns: A list of input column names required by the function.
            output_schema: A predefined schema for the node's output.
            node_type: A string identifying the type of node (e.g., 'filter', 'join').
            drop_columns: A list of columns to be dropped after the function executes.
            renew_schema: If True, the schema is recalculated after execution.
            setting_input: A configuration object containing settings for the node.
            cache_results: If True, the node's results are cached for future runs.
            schema_callback: A function that dynamically calculates the output schema.
            input_node_ids: A list of IDs for the nodes that this node depends on.

        Returns:
            The created or updated FlowNode object.
        """
        existing_node = self.get_node(node_id)
        if existing_node is not None:
            if existing_node.node_type != node_type:
                self.delete_node(existing_node.node_id)
                existing_node = None
        if existing_node:
            input_nodes = existing_node.all_inputs
        elif input_node_ids is not None:
            input_nodes = [self.get_node(node_id) for node_id in input_node_ids]
        else:
            input_nodes = None
        if isinstance(input_columns, str):
            input_columns = [input_columns]
        if (
                input_nodes is not None or
                function.__name__ in ('placeholder', 'analysis_preparation') or
                node_type in ("cloud_storage_reader", "polars_lazy_frame", "input_data")
        ):
            if not existing_node:
                node = FlowNode(node_id=node_id,
                                function=function,
                                output_schema=output_schema,
                                input_columns=input_columns,
                                drop_columns=drop_columns,
                                renew_schema=renew_schema,
                                setting_input=setting_input,
                                node_type=node_type,
                                name=function.__name__,
                                schema_callback=schema_callback,
                                parent_uuid=self.uuid)
            else:
                existing_node.update_node(function=function,
                                          output_schema=output_schema,
                                          input_columns=input_columns,
                                          drop_columns=drop_columns,
                                          setting_input=setting_input,
                                          schema_callback=schema_callback)
                node = existing_node
        else:
            raise Exception("No data initialized")
        self._node_db[node_id] = node
        self._node_ids.append(node_id)
        return node

    def add_include_cols(self, include_columns: List[str]):
        """Adds columns to both the input and output column lists.

        Args:
            include_columns: A list of column names to include.
        """
        for column in include_columns:
            if column not in self._input_cols:
                self._input_cols.append(column)
            if column not in self._output_cols:
                self._output_cols.append(column)
        return self

    def add_output(self, output_file: input_schema.NodeOutput):
        """Adds an output node to write the final data to a destination.

        Args:
            output_file: The settings for the output file.
        """

        def _func(df: FlowDataEngine):
            execute_remote = self.execution_location != 'local'
            df.output(output_fs=output_file.output_settings, flow_id=self.flow_id, node_id=output_file.node_id,
                      execute_remote=execute_remote)
            return df

        def schema_callback():
            input_node: FlowNode = self.get_node(output_file.node_id).node_inputs.main_inputs[0]

            return input_node.schema
        input_node_id = getattr(output_file, "depending_on_id") if hasattr(output_file, 'depending_on_id') else None
        self.add_node_step(node_id=output_file.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='output',
                           setting_input=output_file,
                           schema_callback=schema_callback,
                           input_node_ids=[input_node_id])

    def add_database_writer(self, node_database_writer: input_schema.NodeDatabaseWriter):
        """Adds a node to write data to a database.

        Args:
            node_database_writer: The settings for the database writer node.
        """

        node_type = 'database_writer'
        database_settings: input_schema.DatabaseWriteSettings = node_database_writer.database_write_settings
        database_connection: Optional[input_schema.DatabaseConnection | input_schema.FullDatabaseConnection]
        if database_settings.connection_mode == 'inline':
            database_connection: input_schema.DatabaseConnection = database_settings.database_connection
            encrypted_password = get_encrypted_secret(current_user_id=node_database_writer.user_id,
                                                      secret_name=database_connection.password_ref)
            if encrypted_password is None:
                raise HTTPException(status_code=400, detail="Password not found")
        else:
            database_reference_settings = get_local_database_connection(database_settings.database_connection_name,
                                                                        node_database_writer.user_id)
            encrypted_password = database_reference_settings.password.get_secret_value()

        def _func(df: FlowDataEngine):
            df.lazy = True
            database_external_write_settings = (
                sql_models.DatabaseExternalWriteSettings.create_from_from_node_database_writer(
                    node_database_writer=node_database_writer,
                    password=encrypted_password,
                    table_name=(database_settings.schema_name+'.'+database_settings.table_name
                                if database_settings.schema_name else database_settings.table_name),
                    database_reference_settings=(database_reference_settings if database_settings.connection_mode == 'reference'
                                                 else None),
                    lf=df.data_frame
                )
            )
            external_database_writer = ExternalDatabaseWriter(database_external_write_settings, wait_on_completion=False)
            node._fetch_cached_df = external_database_writer
            external_database_writer.get_result()
            return df

        def schema_callback():
            input_node: FlowNode = self.get_node(node_database_writer.node_id).node_inputs.main_inputs[0]
            return input_node.schema

        self.add_node_step(
            node_id=node_database_writer.node_id,
            function=_func,
            input_columns=[],
            node_type=node_type,
            setting_input=node_database_writer,
            schema_callback=schema_callback,
        )
        node = self.get_node(node_database_writer.node_id)

    def add_database_reader(self, node_database_reader: input_schema.NodeDatabaseReader):
        """Adds a node to read data from a database.

        Args:
            node_database_reader: The settings for the database reader node.
        """

        logger.info("Adding database reader")
        node_type = 'database_reader'
        database_settings: input_schema.DatabaseSettings = node_database_reader.database_settings
        database_connection: Optional[input_schema.DatabaseConnection | input_schema.FullDatabaseConnection]
        if database_settings.connection_mode == 'inline':
            database_connection: input_schema.DatabaseConnection = database_settings.database_connection
            encrypted_password = get_encrypted_secret(current_user_id=node_database_reader.user_id,
                                                      secret_name=database_connection.password_ref)
            if encrypted_password is None:
                raise HTTPException(status_code=400, detail="Password not found")
        else:
            database_reference_settings = get_local_database_connection(database_settings.database_connection_name,
                                                                        node_database_reader.user_id)
            database_connection = database_reference_settings
            encrypted_password = database_reference_settings.password.get_secret_value()

        def _func():
            sql_source = BaseSqlSource(query=None if database_settings.query_mode == 'table' else database_settings.query,
                                       table_name=database_settings.table_name,
                                       schema_name=database_settings.schema_name,
                                       fields=node_database_reader.fields,
                                       )
            database_external_read_settings = (
                sql_models.DatabaseExternalReadSettings.create_from_from_node_database_reader(
                    node_database_reader=node_database_reader,
                    password=encrypted_password,
                    query=sql_source.query,
                    database_reference_settings=(database_reference_settings if database_settings.connection_mode == 'reference'
                                                 else None),
                )
            )

            external_database_fetcher = ExternalDatabaseFetcher(database_external_read_settings, wait_on_completion=False)
            node._fetch_cached_df = external_database_fetcher
            fl = FlowDataEngine(external_database_fetcher.get_result())
            node_database_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def schema_callback():
            sql_source = SqlSource(connection_string=
                                   sql_utils.construct_sql_uri(database_type=database_connection.database_type,
                                                               host=database_connection.host,
                                                               port=database_connection.port,
                                                               database=database_connection.database,
                                                               username=database_connection.username,
                                                               password=decrypt_secret(encrypted_password)),
                                   query=None if database_settings.query_mode == 'table' else database_settings.query,
                                   table_name=database_settings.table_name,
                                   schema_name=database_settings.schema_name,
                                   fields=node_database_reader.fields,
                                   )
            return sql_source.get_schema()

        node = self.get_node(node_database_reader.node_id)
        if node:
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_database_reader
            node.node_settings.cache_results = node_database_reader.cache_results
            if node_database_reader.node_id not in set(start_node.node_id for start_node in self._flow_starts):
                self._flow_starts.append(node)
            node.schema_callback = schema_callback
        else:
            node = FlowNode(node_database_reader.node_id, function=_func,
                            setting_input=node_database_reader,
                            name=node_type, node_type=node_type, parent_uuid=self.uuid,
                            schema_callback=schema_callback)
            self._node_db[node_database_reader.node_id] = node
            self._flow_starts.append(node)
            self._node_ids.append(node_database_reader.node_id)

    def add_sql_source(self, external_source_input: input_schema.NodeExternalSource):
        """Adds a node that reads data from a SQL source.

        This is a convenience alias for `add_external_source`.

        Args:
            external_source_input: The settings for the external SQL source node.
        """
        logger.info('Adding sql source')
        self.add_external_source(external_source_input)

    def add_cloud_storage_writer(self, node_cloud_storage_writer: input_schema.NodeCloudStorageWriter) -> None:
        """Adds a node to write data to a cloud storage provider.

        Args:
            node_cloud_storage_writer: The settings for the cloud storage writer node.
        """

        node_type = "cloud_storage_writer"
        def _func(df: FlowDataEngine):
            df.lazy = True
            execute_remote = self.execution_location != 'local'
            cloud_connection_settings = get_cloud_connection_settings(
                connection_name=node_cloud_storage_writer.cloud_storage_settings.connection_name,
                user_id=node_cloud_storage_writer.user_id,
                auth_mode=node_cloud_storage_writer.cloud_storage_settings.auth_mode
            )
            full_cloud_storage_connection = FullCloudStorageConnection(
                storage_type=cloud_connection_settings.storage_type,
                auth_method=cloud_connection_settings.auth_method,
                aws_allow_unsafe_html=cloud_connection_settings.aws_allow_unsafe_html,
                **CloudStorageReader.get_storage_options(cloud_connection_settings)
            )
            if execute_remote:
                settings = get_cloud_storage_write_settings_worker_interface(
                    write_settings=node_cloud_storage_writer.cloud_storage_settings,
                    connection=full_cloud_storage_connection,
                    lf=df.data_frame,
                    flowfile_node_id=node_cloud_storage_writer.node_id,
                    flowfile_flow_id=self.flow_id)
                external_database_writer = ExternalCloudWriter(settings, wait_on_completion=False)
                node._fetch_cached_df = external_database_writer
                external_database_writer.get_result()
            else:
                cloud_storage_write_settings_internal = CloudStorageWriteSettingsInternal(
                    connection=full_cloud_storage_connection,
                    write_settings=node_cloud_storage_writer.cloud_storage_settings,
                )
                df.to_cloud_storage_obj(cloud_storage_write_settings_internal)
            return df

        def schema_callback():
            logger.info("Starting to run the schema callback for cloud storage writer")
            if self.get_node(node_cloud_storage_writer.node_id).is_correct:
                return self.get_node(node_cloud_storage_writer.node_id).node_inputs.main_inputs[0].schema
            else:
                return [FlowfileColumn.from_input(column_name="__error__", data_type="String")]

        self.add_node_step(
            node_id=node_cloud_storage_writer.node_id,
            function=_func,
            input_columns=[],
            node_type=node_type,
            setting_input=node_cloud_storage_writer,
            schema_callback=schema_callback,
            input_node_ids=[node_cloud_storage_writer.depending_on_id]
        )

        node = self.get_node(node_cloud_storage_writer.node_id)

    def add_cloud_storage_reader(self, node_cloud_storage_reader: input_schema.NodeCloudStorageReader) -> None:
        """Adds a cloud storage read node to the flow graph.

        Args:
            node_cloud_storage_reader: The settings for the cloud storage read node.
        """
        node_type = "cloud_storage_reader"
        logger.info("Adding cloud storage reader")
        cloud_storage_read_settings = node_cloud_storage_reader.cloud_storage_settings

        def _func():
            logger.info("Starting to run the schema callback for cloud storage reader")
            self.flow_logger.info("Starting to run the schema callback for cloud storage reader")
            settings = CloudStorageReadSettingsInternal(read_settings=cloud_storage_read_settings,
                                                        connection=get_cloud_connection_settings(
                                                            connection_name=cloud_storage_read_settings.connection_name,
                                                            user_id=node_cloud_storage_reader.user_id,
                                                            auth_mode=cloud_storage_read_settings.auth_mode
                                                        ))
            fl = FlowDataEngine.from_cloud_storage_obj(settings)
            return fl

        node = self.add_node_step(node_id=node_cloud_storage_reader.node_id,
                                  function=_func,
                                  cache_results=node_cloud_storage_reader.cache_results,
                                  setting_input=node_cloud_storage_reader,
                                  node_type=node_type,
                                  )
        if node_cloud_storage_reader.node_id not in set(start_node.node_id for start_node in self._flow_starts):
            self._flow_starts.append(node)

    def add_external_source(self,
                            external_source_input: input_schema.NodeExternalSource):
        """Adds a node for a custom external data source.

        Args:
            external_source_input: The settings for the external source node.
        """

        node_type = 'external_source'
        external_source_script = getattr(external_sources.custom_external_sources, external_source_input.identifier)
        source_settings = (getattr(input_schema, snake_case_to_camel_case(external_source_input.identifier)).
                           model_validate(external_source_input.source_settings))
        if hasattr(external_source_script, 'initial_getter'):
            initial_getter = getattr(external_source_script, 'initial_getter')(source_settings)
        else:
            initial_getter = None
        data_getter = external_source_script.getter(source_settings)
        external_source = data_source_factory(source_type='custom',
                                              data_getter=data_getter,
                                              initial_data_getter=initial_getter,
                                              orientation=external_source_input.source_settings.orientation,
                                              schema=None)

        def _func():
            logger.info('Calling external source')
            fl = FlowDataEngine.create_from_external_source(external_source=external_source)
            external_source_input.source_settings.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        node = self.get_node(external_source_input.node_id)
        if node:
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = external_source_input
            node.node_settings.cache_results = external_source_input.cache_results
            if external_source_input.node_id not in set(start_node.node_id for start_node in self._flow_starts):
                self._flow_starts.append(node)
        else:
            node = FlowNode(external_source_input.node_id, function=_func,
                            setting_input=external_source_input,
                            name=node_type, node_type=node_type, parent_uuid=self.uuid)
            self._node_db[external_source_input.node_id] = node
            self._flow_starts.append(node)
            self._node_ids.append(external_source_input.node_id)
        if external_source_input.source_settings.fields and len(external_source_input.source_settings.fields) > 0:
            logger.info('Using provided schema in the node')

            def schema_callback():
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in
                        external_source_input.source_settings.fields]

            node.schema_callback = schema_callback
        else:
            logger.warning('Removing schema')
            node._schema_callback = None
        self.add_node_step(node_id=external_source_input.node_id,
                           function=_func,
                           input_columns=[],
                           node_type=node_type,
                           setting_input=external_source_input)

    def add_read(self, input_file: input_schema.NodeRead):
        """Adds a node to read data from a local file (e.g., CSV, Parquet, Excel).

        Args:
            input_file: The settings for the read operation.
        """
        if (input_file.received_file.file_type in ('xlsx', 'excel') and
                input_file.received_file.table_settings.sheet_name == ''):
            sheet_name = fastexcel.read_excel(input_file.received_file.path).sheet_names[0]
            input_file.received_file.table_settings.sheet_name = sheet_name

        received_file = input_file.received_file
        input_file.received_file.set_absolute_filepath()

        def _func():
            input_file.received_file.set_absolute_filepath()
            if input_file.received_file.file_type == 'parquet':
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            elif input_file.received_file.file_type == 'csv' and 'utf' in input_file.received_file.table_settings.encoding:
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            else:
                input_data = FlowDataEngine.create_from_path_worker(input_file.received_file,
                                                                    node_id=input_file.node_id,
                                                                    flow_id=self.flow_id)
            input_data.name = input_file.received_file.name
            return input_data

        node = self.get_node(input_file.node_id)
        schema_callback = None
        if node:
            start_hash = node.hash
            node.node_type = 'read'
            node.name = 'read'
            node.function = _func
            node.setting_input = input_file
            if input_file.node_id not in set(start_node.node_id for start_node in self._flow_starts):
                self._flow_starts.append(node)

            if start_hash != node.hash:
                logger.info('Hash changed, updating schema')
                if len(received_file.fields) > 0:
                    # If the file has fields defined, we can use them to create the schema
                    def schema_callback():
                        return [FlowfileColumn.from_input(f.name, f.data_type) for f in received_file.fields]

                elif input_file.received_file.file_type in ('csv', 'json', 'parquet'):
                    # everything that can be scanned by polars
                    def schema_callback():
                        input_data = FlowDataEngine.create_from_path(input_file.received_file)
                        return input_data.schema

                elif input_file.received_file.file_type in ('xlsx', 'excel'):
                    # If the file is an Excel file, we need to use the openpyxl engine to read the schema
                    schema_callback = get_xlsx_schema_callback(engine='openpyxl',
                                                               file_path=received_file.file_path,
                                                               sheet_name=received_file.table_settings.sheet_name,
                                                               start_row=received_file.table_settings.start_row,
                                                               end_row=received_file.table_settings.end_row,
                                                               start_column=received_file.table_settings.start_column,
                                                               end_column=received_file.table_settings.end_column,
                                                               has_headers=received_file.table_settings.has_headers)
                else:
                    schema_callback = None
        else:
            node = FlowNode(input_file.node_id, function=_func,
                            setting_input=input_file,
                            name='read', node_type='read', parent_uuid=self.uuid)
            self._node_db[input_file.node_id] = node
            self._flow_starts.append(node)
            self._node_ids.append(input_file.node_id)

        if schema_callback is not None:
            node.schema_callback = schema_callback
        return self

    def add_datasource(self, input_file: Union[input_schema.NodeDatasource, input_schema.NodeManualInput]) -> "FlowGraph":
        """Adds a data source node to the graph.

        This method serves as a factory for creating starting nodes, handling both
        file-based sources and direct manual data entry.

        Args:
            input_file: The configuration object for the data source.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        if isinstance(input_file, input_schema.NodeManualInput):
            input_data = FlowDataEngine(input_file.raw_data_format)
            ref = 'manual_input'
        else:
            input_data = FlowDataEngine(path_ref=input_file.file_ref)
            ref = 'datasource'
        node = self.get_node(input_file.node_id)
        if node:
            node.node_type = ref
            node.name = ref
            node.function = input_data
            node.setting_input = input_file
            if not input_file.node_id in set(start_node.node_id for start_node in self._flow_starts):
                self._flow_starts.append(node)
        else:
            input_data.collect()
            node = FlowNode(input_file.node_id, function=input_data,
                            setting_input=input_file,
                            name=ref, node_type=ref, parent_uuid=self.uuid)
            self._node_db[input_file.node_id] = node
            self._flow_starts.append(node)
            self._node_ids.append(input_file.node_id)
        return self

    def add_manual_input(self, input_file: input_schema.NodeManualInput):
        """Adds a node for manual data entry.

        This is a convenience alias for `add_datasource`.

        Args:
            input_file: The settings and data for the manual input node.
        """
        self.add_datasource(input_file)

    @property
    def nodes(self) -> List[FlowNode]:
        """Gets a list of all FlowNode objects in the graph."""

        return list(self._node_db.values())

    @property
    def execution_mode(self) -> schemas.ExecutionModeLiteral:
        """Gets the current execution mode ('Development' or 'Performance')."""
        return self.flow_settings.execution_mode

    def get_implicit_starter_nodes(self) -> List[FlowNode]:
        """Finds nodes that can act as starting points but are not explicitly defined as such.

        Some nodes, like the Polars Code node, can function without an input. This
        method identifies such nodes if they have no incoming connections.

        Returns:
            A list of `FlowNode` objects that are implicit starting nodes.
        """
        starting_node_ids = [node.node_id for node in self._flow_starts]
        implicit_starting_nodes = []
        for node in self.nodes:
            if node.node_template.can_be_start and not node.has_input and node.node_id not in starting_node_ids:
                implicit_starting_nodes.append(node)
        return implicit_starting_nodes

    @execution_mode.setter
    def execution_mode(self, mode: schemas.ExecutionModeLiteral):
        """Sets the execution mode for the flow.

        Args:
            mode: The execution mode to set.
        """
        self.flow_settings.execution_mode = mode

    @property
    def execution_location(self) -> schemas.ExecutionLocationsLiteral:
        """Gets the current execution location."""
        return self.flow_settings.execution_location

    @execution_location.setter
    def execution_location(self, execution_location: schemas.ExecutionLocationsLiteral):
        """Sets the execution location for the flow.

        Args:
            execution_location: The execution location to set.
        """
        if self.flow_settings.execution_location != execution_location:
            self.reset()
        self.flow_settings.execution_location = execution_location

    def validate_if_node_can_be_fetched(self, node_id: int) -> None:
        flow_node = self._node_db.get(node_id)
        if not flow_node:
            raise Exception("Node not found found")
        skip_nodes, execution_order = compute_execution_plan(
            nodes=self.nodes, flow_starts=self._flow_starts+self.get_implicit_starter_nodes()
        )
        if flow_node.node_id in [skip_node.node_id for skip_node in skip_nodes]:
            raise Exception("Node can not be executed because it does not have it's inputs")

    def create_initial_run_information(self, number_of_nodes: int,
                                       run_type: Literal["fetch_one", "full_run"]):
        return RunInformation(
            flow_id=self.flow_id, start_time=datetime.datetime.now(), end_time=None,
            success=None, number_of_nodes=number_of_nodes, node_step_result=[],
            run_type=run_type
        )

    def create_empty_run_information(self) -> RunInformation:
        return RunInformation(
            flow_id=self.flow_id, start_time=None, end_time=None,
            success=None, number_of_nodes=0, node_step_result=[],
            run_type="init"
        )

    def trigger_fetch_node(self, node_id: int) -> RunInformation | None:
        """Executes a specific node in the graph by its ID."""
        if self.flow_settings.is_running:
            raise Exception("Flow is already running")
        flow_node = self.get_node(node_id)
        self.flow_settings.is_running = True
        self.flow_settings.is_canceled = False
        self.flow_logger.clear_log_file()
        self.latest_run_info = self.create_initial_run_information(1, "fetch_one")
        node_logger = self.flow_logger.get_node_logger(flow_node.node_id)
        node_result = NodeResult(node_id=flow_node.node_id, node_name=flow_node.name)
        logger.info(f'Starting to run: node {flow_node.node_id}, start time: {node_result.start_timestamp}')
        try:
            self.latest_run_info.node_step_result.append(node_result)
            flow_node.execute_node(run_location=self.flow_settings.execution_location,
                                   performance_mode=False,
                                   node_logger=node_logger,
                                   optimize_for_downstream=False,
                                   reset_cache=True)
            node_result.error = str(flow_node.results.errors)
            if self.flow_settings.is_canceled:
                node_result.success = None
                node_result.success = None
                node_result.is_running = False
            node_result.success = flow_node.results.errors is None
            node_result.end_timestamp = time()
            node_result.run_time = int(node_result.end_timestamp - node_result.start_timestamp)
            node_result.is_running = False
            self.latest_run_info.nodes_completed += 1
            self.latest_run_info.end_time = datetime.datetime.now()
            self.flow_settings.is_running = False
            return self.get_run_info()
        except Exception as e:
            node_result.error = 'Node did not run'
            node_result.success = False
            node_result.end_timestamp = time()
            node_result.run_time = int(node_result.end_timestamp - node_result.start_timestamp)
            node_result.is_running = False
            node_logger.error(f'Error in node {flow_node.node_id}: {e}')
        finally:
            self.flow_settings.is_running = False

    def run_graph(self) -> RunInformation | None:
        """Executes the entire data flow graph from start to finish.

        It determines the correct execution order, runs each node,
        collects results, and handles errors and cancellations.

        Returns:
            A RunInformation object summarizing the execution results.

        Raises:
            Exception: If the flow is already running.
        """
        if self.flow_settings.is_running:
            raise Exception('Flow is already running')
        try:

            self.flow_settings.is_running = True
            self.flow_settings.is_canceled = False
            self.flow_logger.clear_log_file()
            self.flow_logger.info('Starting to run flowfile flow...')

            skip_nodes, execution_order = compute_execution_plan(
                nodes=self.nodes,
                flow_starts=self._flow_starts+self.get_implicit_starter_nodes()
            )

            self.latest_run_info = self.create_initial_run_information(len(execution_order), "full_run")

            skip_node_message(self.flow_logger, skip_nodes)
            execution_order_message(self.flow_logger, execution_order)
            performance_mode = self.flow_settings.execution_mode == 'Performance'

            for node in execution_order:
                node_logger = self.flow_logger.get_node_logger(node.node_id)
                if self.flow_settings.is_canceled:
                    self.flow_logger.info('Flow canceled')
                    break
                if node in skip_nodes:
                    node_logger.info(f'Skipping node {node.node_id}')
                    continue
                node_result = NodeResult(node_id=node.node_id, node_name=node.name)
                self.latest_run_info.node_step_result.append(node_result)
                logger.info(f'Starting to run: node {node.node_id}, start time: {node_result.start_timestamp}')
                node.execute_node(run_location=self.flow_settings.execution_location,
                                  performance_mode=performance_mode,
                                  node_logger=node_logger)
                try:
                    node_result.error = str(node.results.errors)
                    if self.flow_settings.is_canceled:
                        node_result.success = None
                        node_result.success = None
                        node_result.is_running = False
                        continue
                    node_result.success = node.results.errors is None
                    node_result.end_timestamp = time()
                    node_result.run_time = int(node_result.end_timestamp - node_result.start_timestamp)
                    node_result.is_running = False
                except Exception as e:
                    node_result.error = 'Node did not run'
                    node_result.success = False
                    node_result.end_timestamp = time()
                    node_result.run_time = int(node_result.end_timestamp - node_result.start_timestamp)
                    node_result.is_running = False
                    node_logger.error(f'Error in node {node.node_id}: {e}')
                if not node_result.success:
                    skip_nodes.extend(list(node.get_all_dependent_nodes()))
                node_logger.info(f'Completed node with success: {node_result.success}')
                self.latest_run_info.nodes_completed += 1
            self.latest_run_info.end_time = datetime.datetime.now()
            self.flow_logger.info('Flow completed!')
            self.end_datetime = datetime.datetime.now()
            self.flow_settings.is_running = False
            if self.flow_settings.is_canceled:
                self.flow_logger.info('Flow canceled')
            return self.get_run_info()
        except Exception as e:
            raise e
        finally:
            self.flow_settings.is_running = False

    def get_run_info(self) -> RunInformation:
        """Gets a summary of the most recent graph execution.

        Returns:
            A RunInformation object with details about the last run.
        """
        is_running = self.flow_settings.is_running
        if self.latest_run_info is None:
            return self.create_empty_run_information()

        elif not is_running and self.latest_run_info.success is not None:
            return self.latest_run_info

        run_info = self.latest_run_info
        if not is_running:
            run_info.success = all(nr.success for nr in run_info.node_step_result)
        return run_info

    @property
    def node_connections(self) -> List[Tuple[int, int]]:
        """Computes and returns a list of all connections in the graph.

        Returns:
            A list of tuples, where each tuple is a (source_id, target_id) pair.
        """
        connections = set()
        for node in self.nodes:
            outgoing_connections = [(node.node_id, ltn.node_id) for ltn in node.leads_to_nodes]
            incoming_connections = [(don.node_id, node.node_id) for don in node.all_inputs]
            node_connections = [c for c in outgoing_connections + incoming_connections if (c[0] is not None
                                                                                           and c[1] is not None)]
            for node_connection in node_connections:
                if node_connection not in connections:
                    connections.add(node_connection)
        return list(connections)

    def get_node_data(self, node_id: int, include_example: bool = True) -> NodeData:
        """Retrieves all data needed to render a node in the UI.

        Args:
            node_id: The ID of the node.
            include_example: Whether to include data samples in the result.

        Returns:
            A NodeData object, or None if the node is not found.
        """
        node = self._node_db[node_id]
        return node.get_node_data(flow_id=self.flow_id, include_example=include_example)

    def get_flowfile_data(self) -> schemas.FlowfileData:
        start_node_ids = {v.node_id for v in self._flow_starts}

        nodes = []
        for node in self.nodes:
            node_info = node.get_node_information()
            flowfile_node = schemas.FlowfileNode(
                id=node_info.id,
                type=node_info.type,
                is_start_node=node.node_id in start_node_ids,
                description=node_info.description,
                x_position=int(node_info.x_position),
                y_position=int(node_info.y_position),
                left_input_id=node_info.left_input_id,
                right_input_id=node_info.right_input_id,
                input_ids=node_info.input_ids,
                outputs=node_info.outputs,
                setting_input=node_info.setting_input,
            )
            nodes.append(flowfile_node)

        settings = schemas.FlowfileSettings(
            description=self.flow_settings.description,
            execution_mode=self.flow_settings.execution_mode,
            execution_location=self.flow_settings.execution_location,
            auto_save=self.flow_settings.auto_save,
            show_detailed_progress=self.flow_settings.show_detailed_progress,
        )
        return schemas.FlowfileData(
            flowfile_version=__version__,
            flowfile_id=self.flow_id,
            flowfile_name=self.__name__,
            flowfile_settings=settings,
            nodes=nodes,
        )

    def get_node_storage(self) -> schemas.FlowInformation:
        """Serializes the entire graph's state into a storable format.

        Returns:
            A FlowInformation object representing the complete graph.
        """
        node_information = {node.node_id: node.get_node_information() for
                            node in self.nodes if node.is_setup and node.is_correct}

        return schemas.FlowInformation(flow_id=self.flow_id,
                                       flow_name=self.__name__,
                                       flow_settings=self.flow_settings,
                                       data=node_information,
                                       node_starts=[v.node_id for v in self._flow_starts],
                                       node_connections=self.node_connections
                                       )

    def cancel(self):
        """Cancels an ongoing graph execution."""

        if not self.flow_settings.is_running:
            return
        self.flow_settings.is_canceled = True
        for node in self.nodes:
            node.cancel()

    def close_flow(self):
        """Performs cleanup operations, such as clearing node caches."""

        for node in self.nodes:
            node.remove_cache()

    def _handle_flow_renaming(self, new_name: str, new_path: Path):
        """
        Handle the rename of a flow when it is being saved.
        """
        if self.flow_settings and self.flow_settings.path and Path(self.flow_settings.path).absolute() != new_path.absolute():
            self.__name__ = new_name
            self.flow_settings.save_location = str(new_path.absolute())
            self.flow_settings.name = new_name
        if self.flow_settings and not self.flow_settings.save_location:
            self.flow_settings.save_location = str(new_path.absolute())
            self.__name__ = new_name
            self.flow_settings.name = new_name

    def save_flow(self, flow_path: str):
        """Saves the current state of the flow graph to a file.

        Supports multiple formats based on file extension:
        - .yaml / .yml: New YAML format
        - .json: JSON format

        Args:
            flow_path: The path where the flow file will be saved.
        """
        logger.info("Saving flow to %s", flow_path)
        path = Path(flow_path)
        os.makedirs(path.parent, exist_ok=True)
        suffix = path.suffix.lower()
        new_flow_name = path.name.replace(suffix, "")
        self._handle_flow_renaming(new_flow_name, path)
        self.flow_settings.modified_on = datetime.datetime.now().timestamp()
        try:
            if suffix == '.flowfile':
                raise DeprecationWarning(
                    f"The .flowfile format is deprecated. Please use .yaml or .json formats.\n\n"
                    "Or stay on v0.4.1 if you still need .flowfile support.\n\n"
                )
            elif suffix in ('.yaml', '.yml'):
                flowfile_data = self.get_flowfile_data()
                data = flowfile_data.model_dump(mode='json')
                with open(flow_path, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            elif suffix == '.json':
                flowfile_data = self.get_flowfile_data()
                data = flowfile_data.model_dump(mode='json')
                with open(flow_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

            else:
                flowfile_data = self.get_flowfile_data()
                logger.warning(f"Unknown file extension {suffix}. Defaulting to YAML format.")
                data = flowfile_data.model_dump(mode='json')
                with open(flow_path, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        except Exception as e:
            logger.error(f"Error saving flow: {e}")
            raise

        self.flow_settings.path = flow_path

    def get_frontend_data(self) -> dict:
        """Formats the graph structure into a JSON-like dictionary for a specific legacy frontend.

        This method transforms the graph's state into a format compatible with the
        Drawflow.js library.

        Returns:
            A dictionary representing the graph in Drawflow format.
        """
        result = {
            'Home': {
                "data": {}
            }
        }
        flow_info: schemas.FlowInformation = self.get_node_storage()

        for node_id, node_info in flow_info.data.items():
            if node_info.is_setup:
                try:
                    pos_x = node_info.data.pos_x
                    pos_y = node_info.data.pos_y
                    # Basic node structure
                    result["Home"]["data"][str(node_id)] = {
                        "id": node_info.id,
                        "name": node_info.type,
                        "data": {},  # Additional data can go here
                        "class": node_info.type,
                        "html": node_info.type,
                        "typenode": "vue",
                        "inputs": {},
                        "outputs": {},
                        "pos_x": pos_x,
                        "pos_y": pos_y
                    }
                except Exception as e:
                    logger.error(e)
            # Add outputs to the node based on `outputs` in your backend data
            if node_info.outputs:
                outputs = {o: 0 for o in node_info.outputs}
                for o in node_info.outputs:
                    outputs[o] += 1
                connections = []
                for output_node_id, n_connections in outputs.items():
                    leading_to_node = self.get_node(output_node_id)
                    input_types = leading_to_node.get_input_type(node_info.id)
                    for input_type in input_types:
                        if input_type == 'main':
                            input_frontend_id = 'input_1'
                        elif input_type == 'right':
                            input_frontend_id = 'input_2'
                        elif input_type == 'left':
                            input_frontend_id = 'input_3'
                        else:
                            input_frontend_id = 'input_1'
                        connection = {"node": str(output_node_id), "input": input_frontend_id}
                        connections.append(connection)

                result["Home"]["data"][str(node_id)]["outputs"]["output_1"] = {
                    "connections": connections}
            else:
                result["Home"]["data"][str(node_id)]["outputs"] = {"output_1": {"connections": []}}

            # Add input to the node based on `depending_on_id` in your backend data
            if node_info.left_input_id is not None or node_info.right_input_id is not None or node_info.input_ids is not None:
                main_inputs = node_info.main_input_ids
                result["Home"]["data"][str(node_id)]["inputs"]["input_1"] = {
                    "connections": [{"node": str(main_node_id), "input": "output_1"} for main_node_id in main_inputs]
                }
                if node_info.right_input_id is not None:
                    result["Home"]["data"][str(node_id)]["inputs"]["input_2"] = {
                        "connections": [{"node": str(node_info.right_input_id), "input": "output_1"}]
                    }
                if node_info.left_input_id is not None:
                    result["Home"]["data"][str(node_id)]["inputs"]["input_3"] = {
                        "connections": [{"node": str(node_info.left_input_id), "input": "output_1"}]
                    }
        return result

    def get_vue_flow_input(self) -> schemas.VueFlowInput:
        """Formats the graph's nodes and edges into a schema suitable for the VueFlow frontend.

        Returns:
            A VueFlowInput object.
        """
        edges: List[schemas.NodeEdge] = []
        nodes: List[schemas.NodeInput] = []
        for node in self.nodes:
            nodes.append(node.get_node_input())
            edges.extend(node.get_edge_input())
        return schemas.VueFlowInput(node_edges=edges, node_inputs=nodes)

    def reset(self):
        """Forces a deep reset on all nodes in the graph."""

        for node in self.nodes:
            node.reset(True)

    def copy_node(self, new_node_settings: input_schema.NodePromise, existing_setting_input: Any, node_type: str) -> None:
        """Creates a copy of an existing node.

        Args:
            new_node_settings: The promise containing new settings (like ID and position).
            existing_setting_input: The settings object from the node being copied.
            node_type: The type of the node being copied.
        """
        self.add_node_promise(new_node_settings)

        if isinstance(existing_setting_input, input_schema.NodePromise):
            return

        combined_settings = combine_existing_settings_and_new_settings(
            existing_setting_input, new_node_settings
        )
        getattr(self, f"add_{node_type}")(combined_settings)

    def generate_code(self):
        """Generates code for the flow graph.
        This method exports the flow graph to a Polars-compatible format.
        """
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars
        print(export_flow_to_polars(self))


def combine_existing_settings_and_new_settings(setting_input: Any, new_settings: input_schema.NodePromise) -> Any:
    """Merges settings from an existing object with new settings from a NodePromise.

    Typically used when copying a node to apply a new ID and position.

    Args:
        setting_input: The original settings object.
        new_settings: The NodePromise with new positional and ID data.

    Returns:
        A new settings object with the merged properties.
    """
    copied_setting_input = deepcopy(setting_input)

    # Update only attributes that exist on new_settings
    fields_to_update = (
        "node_id",
        "pos_x",
        "pos_y",
        "description",
        "flow_id"
    )

    for field in fields_to_update:
        if hasattr(new_settings, field) and getattr(new_settings, field) is not None:
            setattr(copied_setting_input, field, getattr(new_settings, field))

    return copied_setting_input


def add_connection(flow: FlowGraph, node_connection: input_schema.NodeConnection) -> None:
    """Adds a connection between two nodes in the flow graph.

    Args:
        flow: The FlowGraph instance to modify.
        node_connection: An object defining the source and target of the connection.
    """
    logger.info('adding a connection')
    from_node = flow.get_node(node_connection.output_connection.node_id)
    to_node = flow.get_node(node_connection.input_connection.node_id)
    logger.info(f'from_node={from_node}, to_node={to_node}')
    if not (from_node and to_node):
        raise HTTPException(404, 'Not not available')
    else:
        to_node.add_node_connection(from_node, node_connection.input_connection.get_node_input_connection_type())


def delete_connection(graph, node_connection: input_schema.NodeConnection):
    """Deletes a connection between two nodes in the flow graph.

    Args:
        graph: The FlowGraph instance to modify.
        node_connection: An object defining the connection to be removed.
    """
    from_node = graph.get_node(node_connection.output_connection.node_id)
    to_node = graph.get_node(node_connection.input_connection.node_id)
    connection_valid = to_node.node_inputs.validate_if_input_connection_exists(
        node_input_id=from_node.node_id,
        connection_name=node_connection.input_connection.get_node_input_connection_type(),
    )
    if not connection_valid:
        raise HTTPException(422, "Connection does not exist on the input node")
    if from_node is not None:
        from_node.delete_lead_to_node(node_connection.input_connection.node_id)

    if to_node is not None:
        to_node.delete_input_node(
            node_connection.output_connection.node_id,
            connection_type=node_connection.input_connection.connection_class,
        )
