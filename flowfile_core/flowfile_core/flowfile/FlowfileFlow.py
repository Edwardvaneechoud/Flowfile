import datetime
import pickle
import polars as pl
import fastexcel
from fastapi.exceptions import HTTPException
from time import time
from functools import partial
from typing import List, Dict, Union, Callable, Any, Optional, Tuple
from uuid import uuid1
from copy import deepcopy
from pyarrow.parquet import ParquetFile
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.flowfile.sources.external_sources.factory import data_source_factory
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.settings import airbyte_settings_from_config
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import type_to_polars_str, FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.fuzzy_matching.settings_validator import (calculate_fuzzy_match_schema,
                                                                                       pre_calculate_pivot_schema)
from flowfile_core.utils.arrow_reader import get_read_top_n
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine, execute_polars_code
from flowfile_core.flowfile.flow_data_engine.read_excel_tables import get_open_xlsx_datatypes, \
    get_calamine_xlsx_data_types
from flowfile_core.flowfile.sources import external_sources
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import TableExample, NodeData, NodeResult, RunInformation
from flowfile_core.flowfile.utils import snake_case_to_camel_case
from flowfile_core.flowfile.analytics.utils import create_graphic_walker_node_from_node_promise
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.util.execution_orderer import determine_execution_order
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (ExternalAirbyteFetcher,
                                                                                                 ExternalDatabaseFetcher,
                                                                                                 ExternalDatabaseWriter,
                                                                                                 ExternalDfFetcher)
from flowfile_core.secrets.secrets import get_encrypted_secret, decrypt_secret
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils, models as sql_models
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource, BaseSqlSource
from flowfile_core.flowfile.database_connection_manager.db_connections import get_local_database_connection
from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout


def get_xlsx_schema(engine: str, file_path: str, sheet_name: str, start_row: int, start_column: int,
                    end_row: int, end_column: int, has_headers: bool):
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
    if len(nodes) > 0:
        msg = "\n".join(str(node) for node in nodes)
        flow_logger.warning(f'skipping nodes:\n{msg}')


def execution_order_message(flow_logger: FlowLogger, nodes: List[FlowNode]) -> None:
    msg = "\n".join(str(node) for node in nodes)
    flow_logger.info(f'execution order:\n{msg}')


def get_xlsx_schema_callback(engine: str, file_path: str, sheet_name: str, start_row: int, start_column: int,
                             end_row: int, end_column: int, has_headers: bool):
    return partial(get_xlsx_schema, engine=engine, file_path=file_path, sheet_name=sheet_name, start_row=start_row,
                   start_column=start_column, end_row=end_row, end_column=end_column, has_headers=has_headers)


class FlowGraph:
    """
       FlowGraph is a class that enables Extract, Transform and Load (ETL) operations
       on data. It allows you to create a Directed Acyclic Graph (DAG) where each
       node represents a step in the ETL pipeline.

       The class offers methods to add transformations and data sources, as well as
       methods to run the transformations and generate results.

       Attributes:
           _input_cols (set): A set that stores the input columns for the transformations.
           _output_cols (set): A set that stores the output columns from the transformations.
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
    node_results: List[NodeResult] = None
    latest_run_info: Optional[RunInformation] = None
    start_datetime: datetime = None
    end_datetime: datetime = None
    nodes_completed: int = 0
    flow_settings: schemas.FlowSettings = None
    flow_logger: FlowLogger

    def __init__(self, flow_id: int,
                 flow_settings: schemas.FlowSettings,
                 name: str = None, input_cols: List[str] = None,
                 output_cols: List[str] = None,
                 path_ref: str = None,
                 input_flow: Union[ParquetFile, FlowDataEngine, "FlowGraph"] = None,
                 cache_results: bool = False):
        self.flow_settings = flow_settings
        self.uuid = str(uuid1())
        self.nodes_completed = 0
        self.start_datetime = None
        self.end_datetime = None
        self.latest_run_info = None
        self.node_results = []
        self._flow_id = flow_id
        self.flow_logger = FlowLogger(flow_id)
        self._flow_starts: List[FlowNode] = []
        self._results = None
        self.schema = None
        self.has_over_row_function = False
        self._input_cols = [] if input_cols is None else input_cols
        self._output_cols = [] if output_cols is None else output_cols
        self._node_ids = []
        self._node_db = {}
        self.cache_results = cache_results
        self.__name__ = name if name else id(self)
        self.depends_on = {}
        if path_ref is not None:
            self.add_datasource(input_schema.NodeDatasource(file_path=path_ref))
        elif input_flow is not None:
            self.add_datasource(input_file=input_flow)

    def add_node_promise(self, node_promise: input_schema.NodePromise):

        def placeholder(n: FlowNode = None):
            if n is None:
                return FlowDataEngine()
            return n

        self.add_node_step(node_id=node_promise.node_id, node_type=node_promise.node_type, function=placeholder,
                           setting_input=node_promise)

    def apply_layout(self, y_spacing: int = 150, x_spacing: int = 200, initial_y: int = 100):
        """
        Calculates and applies a layered layout to all nodes in the graph.
        Updates the pos_x and pos_y attributes of the node setting inputs.
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
            raise # Optional: re-raise the exception

    def add_initial_node_analysis(self, node_promise: input_schema.NodePromise):
        node_analysis = create_graphic_walker_node_from_node_promise(node_promise)
        self.add_explore_data(node_analysis)

    def add_explore_data(self, node_analysis: input_schema.NodeExploreData):
        sample_size: int = 10000

        def analysis_preparation(flowfile_table: FlowDataEngine):

            if flowfile_table.number_of_records<0:

                number_of_records = ExternalDfFetcher(
                    lf=flowfile_table.data_frame,
                    operation_type="calculate_number_of_records",
                    flow_id=self.flow_id,
                    node_id=node.node_id,
                ).result
            else:
                number_of_records = flowfile_table.number_of_records
            if number_of_records > sample_size:
                flowfile_table = flowfile_table.get_sample(sample_size, random=True)

            external_sampler = ExternalDfFetcher(
                lf=flowfile_table.data_frame,
                file_ref=node.hash,
                wait_on_completion=True,
                node_id=node.node_id,
                flow_id=self.flow_id,
            )
            node.results.analysis_data_generator = get_read_top_n(external_sampler.status.file_ref, 10000)
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

    @property
    def flow_id(self) -> int:
        return self._flow_id

    @flow_id.setter
    def flow_id(self, new_id: int):
        self._flow_id = new_id
        for node in self.nodes:
            if hasattr(node.setting_input, 'flow_id'):
                node.setting_input.flow_id = new_id
        self.flow_settings.flow_id = new_id

    def __repr__(self):
        """
        Official string representation of the FlowGraph class.
        """
        settings_str = "  -" + '\n  -'.join(f"{k}: {v}" for k, v in self.flow_settings)
        return f"FlowGraph(\nNodes: {self._node_db}\n\nSettings:\n{settings_str}"

    def get_nodes_overview(self):
        output = []
        for v in self._node_db.values():
            output.append(v.get_repr())
        return output

    def remove_from_output_cols(self, columns: List[str]):
        cols = set(columns)
        self._output_cols = [c for c in self._output_cols if c not in cols]

    def get_node(self, node_id: Union[int, str] = None) -> FlowNode:
        if node_id is None:
            node_id = self._node_ids[-1]
        node = self._node_db.get(node_id)
        if node is not None:
            return node

    def add_pivot(self, pivot_settings: input_schema.NodePivot):
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

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.unpivot(unpivot_settings.unpivot_input)

        self.add_node_step(node_id=unpivot_settings.node_id,
                           function=_func,
                           node_type='unpivot',
                           setting_input=unpivot_settings,
                           input_node_ids=[unpivot_settings.depending_on_id])

    def add_union(self, union_settings: input_schema.NodeUnion):
        def _func(*flowfile_tables: FlowDataEngine):
            dfs: List[pl.LazyFrame] | List[pl.DataFrame] = [flt.data_frame for flt in flowfile_tables]
            return FlowDataEngine(pl.concat(dfs, how='diagonal_relaxed'))

        self.add_node_step(node_id=union_settings.node_id,
                           function=_func,
                           node_type=f'union',
                           setting_input=union_settings,
                           input_node_ids=union_settings.depending_on_ids)

    def add_group_by(self, group_by_settings: input_schema.NodeGroupBy):

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

    def add_or_update_column_func(self, col_name: str, pl_dtype: pl.DataType, depends_on: FlowNode):
        col_output = FlowfileColumn.from_input(column_name=col_name, data_type=str(pl_dtype))
        schema = depends_on.schema
        col_exist = depends_on.get_flow_file_column_schema(col_name)
        if col_exist is None:
            new_schema = schema + [col_output]
        else:
            new_schema = []
            for s in self.schema:
                if s.name == col_name:
                    new_schema.append(col_output)
                else:
                    new_schema.append(s)
        return new_schema

    def add_filter(self, filter_settings: input_schema.NodeFilter):
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
        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.get_record_count()

        self.add_node_step(node_id=node_number_of_records.node_id,
                           function=_func,
                           node_type='record_count',
                           setting_input=node_number_of_records,
                           input_node_ids=[node_number_of_records.depending_on_id])

    def add_polars_code(self, node_polars_code: input_schema.NodePolarsCode):
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

    def add_unique(self, unique_settings: input_schema.NodeUnique):

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.make_unique(unique_settings.unique_input)

        self.add_node_step(node_id=unique_settings.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='unique',
                           setting_input=unique_settings,
                           input_node_ids=[unique_settings.node_id])

    def add_graph_solver(self, graph_solver_settings: input_schema.NodeGraphSolver):
        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.solve_graph(graph_solver_settings.graph_solver_input)

        self.add_node_step(node_id=graph_solver_settings.node_id,
                           function=_func,
                           node_type='graph_solver',
                           setting_input=graph_solver_settings)

    def add_formula(self, function_settings: input_schema.NodeFormula):
        error = ""
        if function_settings.function.field.data_type is not None:
            output_type = type_to_polars_str(function_settings.function.field.data_type)
        else:
            output_type = None
        if output_type is not None:
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
        if error != "":
            node = self.get_node(function_settings.node_id)
            node.results.errors = error
            return False, error
        else:
            return True, ""

    def add_cross_join(self, cross_join_settings: input_schema.NodeCrossJoin) -> "FlowGraph":

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
                           setting_input=cross_join_settings)
        return self

    def add_join(self, join_settings: input_schema.NodeJoin) -> "FlowGraph":
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
        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            f = main.start_fuzzy_join(fuzzy_match_input=fuzzy_settings.join_input, other=right, file_ref=node.hash,
                                      flow_id=self.flow_id, node_id=fuzzy_settings.node_id)
            logger.info("Started the fuzzy match action")
            node._fetch_cached_df = f
            return FlowDataEngine(f.get_result())

        self.add_node_step(node_id=fuzzy_settings.node_id,
                           function=_func,
                           input_columns=[],
                           node_type='fuzzy_match',
                           setting_input=fuzzy_settings)
        node = self.get_node(node_id=fuzzy_settings.node_id)

        def schema_callback():
            return calculate_fuzzy_match_schema(fuzzy_settings.join_input,
                                                left_schema=node.node_inputs.main_inputs[0].schema,
                                                right_schema=node.node_inputs.right_input.schema
                                                )

        node.schema_callback = schema_callback
        return self

    def add_text_to_rows(self, node_text_to_rows: input_schema.NodeTextToRows) -> "FlowGraph":
        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.split(node_text_to_rows.text_to_rows_input)

        self.add_node_step(node_id=node_text_to_rows.node_id,
                           function=_func,
                           node_type='text_to_rows',
                           setting_input=node_text_to_rows,
                           input_node_ids=[node_text_to_rows.depending_on_id])
        return self

    def add_sort(self, sort_settings: input_schema.NodeSort) -> "FlowGraph":
        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.do_sort(sort_settings.sort_input)

        self.add_node_step(node_id=sort_settings.node_id,
                           function=_func,
                           node_type='sort',
                           setting_input=sort_settings,
                           input_node_ids=[sort_settings.depending_on_id])
        return self

    def add_sample(self, sample_settings: input_schema.NodeSample) -> "FlowGraph":
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
        select_cols = select_settings.select_input
        drop_cols = tuple(s.old_name for s in select_settings.select_input)

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            input_cols = set(f.name for f in table.schema)
            ids_to_remove = []
            for i, select_col in enumerate(select_cols):
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
        return len(self._node_ids) > 0

    def delete_node(self, node_id: Union[int, str]):
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
                      input_node_ids: List[int] = None):
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
        if cache_results is None:
            if hasattr(setting_input, 'cache_results'):
                cache_results = getattr(setting_input, 'cache_results')
                cache_results = False if cache_results is None else cache_results
        if isinstance(input_columns, str):
            input_columns = [input_columns]

        if input_nodes is not None or function.__name__ in ('placeholder', 'analysis_preparation'):

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
        elif node_type == 'input_data':
            node = None
        else:
            raise Exception("No data initialized")
        self._node_db[node_id] = node
        self._node_ids.append(node_id)

    def add_include_cols(self, include_columns: List[str]):
        for column in include_columns:
            if column not in self._input_cols:
                self._input_cols.append(column)
            if column not in self._output_cols:
                self._output_cols.append(column)
        return self

    def add_output(self, output_file: input_schema.NodeOutput):
        def _func(df: FlowDataEngine):
            output_file.output_settings.populate_abs_file_path()
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
        logger.info("Adding database reader")
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

    def add_airbyte_reader(self, external_source_input: input_schema.NodeAirbyteReader):
        logger.info('Adding airbyte reader')
        node_type = 'airbyte_reader'
        source_settings: input_schema.AirbyteReader = external_source_input.source_settings
        airbyte_settings = airbyte_settings_from_config(source_settings, flow_id=self.flow_id,
                                                        node_id=external_source_input.node_id)

        logger.info("Airbyte settings created")
        airbyte_settings.fields = source_settings.fields
        external_source = data_source_factory(source_type='airbyte', airbyte_settings=airbyte_settings)

        def _func():
            logger.info('Calling external source')
            external_fetcher = ExternalAirbyteFetcher(airbyte_settings, wait_on_completion=False)
            node._fetch_cached_df = external_fetcher
            fl = FlowDataEngine(external_fetcher.get_result())
            external_source_input.source_settings.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def schema_callback():
            return [FlowfileColumn.from_input(f.name, f.data_type) for f in external_source.schema]

        node = self.get_node(external_source_input.node_id)
        if node:
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = external_source_input
            node.node_settings.cache_results = external_source_input.cache_results
            if external_source_input.node_id not in set(start_node.node_id for start_node in self._flow_starts):
                self._flow_starts.append(node)
            node.schema_callback = schema_callback
        else:
            node = FlowNode(external_source_input.node_id, function=_func,
                            setting_input=external_source_input,
                            name=node_type, node_type=node_type, parent_uuid=self.uuid,
                            schema_callback=schema_callback)
            self._node_db[external_source_input.node_id] = node
            self._flow_starts.append(node)
            self._node_ids.append(external_source_input.node_id)
        if external_source_input.source_settings.fields and len(external_source_input.source_settings.fields) > 0:
            logger.info('Using provided schema in the node')

    def add_google_sheet(self, external_source_input: input_schema.NodeExternalSource):
        logger.info('Adding google sheet reader')
        self.add_external_source(external_source_input)

    def add_sql_source(self, external_source_input: input_schema.NodeExternalSource):
        logger.info('Adding sql source')
        self.add_external_source(external_source_input)

    def add_external_source(self,
                            external_source_input: input_schema.NodeExternalSource | input_schema.NodeAirbyteReader):

        custom_source_type = external_source_input.identifier != 'airbyte'
        if custom_source_type:
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
        else:
            node_type = 'airbyte_reader'
            source_settings: input_schema.AirbyteReader = external_source_input.source_settings
            airbyte_settings = airbyte_settings_from_config(source_settings, flow_id=self.flow_id,
                                                            node_id=external_source_input.node_id)
            airbyte_settings.fields = source_settings.fields
            external_source = data_source_factory(source_type='airbyte', airbyte_settings=airbyte_settings)

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
        if input_file.received_file.file_type in ('xlsx', 'excel') and input_file.received_file.sheet_name == '':
            sheet_name = fastexcel.read_excel(input_file.received_file.path).sheet_names[0]
            input_file.received_file.sheet_name = sheet_name

        received_file = input_file.received_file
        input_file.received_file.set_absolute_filepath()

        def _func():
            input_file.received_file.set_absolute_filepath()
            if input_file.received_file.file_type == 'parquet':
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            elif input_file.received_file.file_type == 'csv' and 'utf' in input_file.received_file.encoding:
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
                                                               sheet_name=received_file.sheet_name,
                                                               start_row=received_file.start_row,
                                                               end_row=received_file.end_row,
                                                               start_column=received_file.start_column,
                                                               end_column=received_file.end_column,
                                                               has_headers=received_file.has_headers)
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

    def add_datasource(self, input_file: input_schema.NodeDatasource | input_schema.NodeManualInput):

        if isinstance(input_file, input_schema.NodeManualInput):
            input_data = FlowDataEngine(input_file.raw_data)
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
            node = FlowNode(input_file.node_id, function=input_data,
                            setting_input=input_file,
                            name=ref, node_type=ref, parent_uuid=self.uuid)
            self._node_db[input_file.node_id] = node
            self._flow_starts.append(node)
            self._node_ids.append(input_file.node_id)
        return self

    def add_manual_input(self, input_file: input_schema.NodeManualInput):
        self.add_datasource(input_file)

    @property
    def nodes(self) -> List[FlowNode]:
        return list(self._node_db.values())

    def check_for_missed_cols(self, expected_cols: List):
        not_filled_cols = set(expected_cols) - set(self._output_cols)
        cols_available = list(not_filled_cols & set([c.name for c in self._input_data.schema]))
        self._output_cols += cols_available

    @property
    def input_data_columns(self) -> List[str]:
        if self._input_cols:
            return list(set([col for col in self._input_cols if
                             col in [table_col.name for table_col in self._input_data.schema]]))

    @property
    def execution_mode(self) -> str:
        return self.flow_settings.execution_mode

    def get_implicit_starter_nodes(self) -> List[FlowNode]:
        """Ensures that nodes that can be a start (e.g. polars code), will be a starting node"""
        starting_node_ids = [node.node_id for node in self._flow_starts]
        implicit_starting_nodes = []
        for node in self.nodes:
            if node.node_template.can_be_start and not node.has_input and node.node_id not in starting_node_ids:
                implicit_starting_nodes.append(node)
        return implicit_starting_nodes

    @execution_mode.setter
    def execution_mode(self, mode: str):
        self.flow_settings.execution_mode = mode

    @property
    def execution_location(self) -> schemas.ExecutionLocationsLiteral:
        return self.flow_settings.execution_location

    @execution_location.setter
    def execution_location(self, execution_location: schemas.ExecutionLocationsLiteral):
        self.flow_settings.execution_location = execution_location

    def run_graph(self):
        if self.flow_settings.is_running:
            raise Exception('Flow is already running')
        try:
            self.flow_settings.is_running = True
            self.flow_settings.is_canceled = False
            self.flow_logger.clear_log_file()
            self.nodes_completed = 0
            self.node_results = []
            self.start_datetime = datetime.datetime.now()
            self.end_datetime = None
            self.latest_run_info = None
            self.flow_logger.info('Starting to run flowfile flow...')
            skip_nodes = [node for node in self.nodes if not node.is_correct]
            skip_nodes.extend([lead_to_node for node in skip_nodes for lead_to_node in node.leads_to_nodes])
            execution_order = determine_execution_order(all_nodes=[node for node in self.nodes if
                                                                   node not in skip_nodes],
                                                        flow_starts=self._flow_starts+self.get_implicit_starter_nodes())

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
                self.node_results.append(node_result)
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
                    node_result.run_time = node_result.end_timestamp - node_result.start_timestamp
                    node_result.is_running = False
                except Exception as e:
                    node_result.error = 'Node did not run'
                    node_result.success = False
                    node_result.end_timestamp = time()
                    node_result.run_time = node_result.end_timestamp - node_result.start_timestamp
                    node_result.is_running = False
                    node_logger.error(f'Error in node {node.node_id}: {e}')
                if not node_result.success:
                    skip_nodes.extend(list(node.get_all_dependent_nodes()))
                node_logger.info(f'Completed node with success: {node_result.success}')
                self.nodes_completed += 1
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
        if self.latest_run_info is None:
            node_results = self.node_results
            success = all(nr.success for nr in node_results)
            self.latest_run_info = RunInformation(start_time=self.start_datetime, end_time=self.end_datetime,
                                                  success=success,
                                                  node_step_result=node_results, flow_id=self.flow_id,
                                                  nodes_completed=self.nodes_completed,
                                                  number_of_nodes=len(self.nodes))
        elif self.latest_run_info.nodes_completed != self.nodes_completed:
            node_results = self.node_results
            self.latest_run_info = RunInformation(start_time=self.start_datetime, end_time=self.end_datetime,
                                                  success=all(nr.success for nr in node_results),
                                                  node_step_result=node_results, flow_id=self.flow_id,
                                                  nodes_completed=self.nodes_completed,
                                                  number_of_nodes=len(self.nodes))
        return self.latest_run_info

    @property
    def node_connections(self) -> List[Tuple[int, int]]:
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

    def get_schema(self) -> List[FlowfileColumn]:
        if self.schema is None:
            if len(self._node_ids) > 0:
                self.schema = self._node_db[self._node_ids[0]].schema
        return self.schema

    def get_example_data(self, node_id: int) -> TableExample | None:
        node = self._node_db[node_id]
        return node.get_table_example(include_data=True)

    def get_node_data(self, node_id: int, include_example: bool = True) -> NodeData:
        node = self._node_db[node_id]
        return node.get_node_data(flow_id=self.flow_id, include_example=include_example)

    def get_node_storage(self) -> schemas.FlowInformation:

        node_information = {node.node_id: node.get_node_information() for
                            node in self.nodes if node.is_setup and node.is_correct}

        return schemas.FlowInformation(flow_id=self.flow_id,
                                       flow_name=self.__name__,
                                       storage_location=self.flow_settings.path,
                                       flow_settings=self.flow_settings,
                                       data=node_information,
                                       node_starts=[v.node_id for v in self._flow_starts],
                                       node_connections=self.node_connections
                                       )

    def cancel(self):
        if not self.flow_settings.is_running:
            return
        self.flow_settings.is_canceled = True
        for node in self.nodes:
            node.cancel()

    def close_flow(self):
        for node in self.nodes:
            node.remove_cache()

    def save_flow(self, flow_path: str):
        with open(flow_path, 'wb') as f:
            pickle.dump(self.get_node_storage(), f)
        self.flow_settings.path = flow_path

    def get_frontend_data(self):
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
        edges: List[schemas.NodeEdge] = []
        nodes: List[schemas.NodeInput] = []
        for node in self.nodes:
            nodes.append(node.get_node_input())
            edges.extend(node.get_edge_input())
        return schemas.VueFlowInput(node_edges=edges, node_inputs=nodes)

    def reset(self):
        for node in self.nodes:
            node.reset(True)

    def copy_node(self, new_node_settings: input_schema.NodePromise, existing_setting_input: Any, node_type: str) -> None:
        """Copy an existing node with potentially new settings."""
        self.add_node_promise(new_node_settings)

        if isinstance(existing_setting_input, input_schema.NodePromise):
            return

        combined_settings = combine_existing_settings_and_new_settings(
            existing_setting_input, new_node_settings
        )
        getattr(self, f"add_{node_type}")(combined_settings)


def combine_existing_settings_and_new_settings(setting_input: Any, new_settings: input_schema.NodePromise) -> Any:
    """Combine excopy_nodeisting settings with new settings from a NodePromise."""
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


def add_connection(flow: FlowGraph, node_connection: input_schema.NodeConnection):
    logger.info('adding a connection')
    from_node = flow.get_node(node_connection.output_connection.node_id)
    to_node = flow.get_node(node_connection.input_connection.node_id)
    logger.info(f'from_node={from_node}, to_node={to_node}')
    if not (from_node and to_node):
        raise HTTPException(404, 'Not not available')
    else:
        to_node.add_node_connection(from_node, node_connection.input_connection.get_node_input_connection_type())


def delete_connection(graph, node_connection: input_schema.NodeConnection):
    """Delete the connection between two nodes."""
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


