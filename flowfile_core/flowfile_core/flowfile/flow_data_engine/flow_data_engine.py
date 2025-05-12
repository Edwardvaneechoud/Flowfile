# Standard library imports
import logging
import os
from copy import deepcopy
from dataclasses import dataclass
from math import ceil
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

# Third-party imports
from loky import Future
import polars as pl
from polars.exceptions import PanicException
from polars_grouper import graph_solver
from polars_expr_transformer import simple_function_to_expr as to_expr
from pyarrow.parquet import ParquetFile

# Local imports - Core
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import NodeLogger
from flowfile_core.schemas import (
    input_schema,
    transform_schema as transform_schemas
)

# Local imports - Flow File Components
from flowfile_core.flowfile.flow_data_engine import utils
from flowfile_core.flowfile.flow_data_engine.create import funcs as create_funcs
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import (
    FlowfileColumn,
    convert_stats_to_column_info
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import type_to_polars
from flowfile_core.flowfile.flow_data_engine.fuzzy_matching.prepare_for_fuzzy_match import prepare_for_fuzzy_match
from flowfile_core.flowfile.flow_data_engine.join import (
    verify_join_select_integrity,
    verify_join_map_integrity
)
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.flowfile.flow_data_engine.sample_data import create_fake_data
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    ExternalCreateFetcher,
    ExternalDfFetcher,
    ExternalExecutorTracker,
    ExternalFuzzyMatchFetcher,
    fetch_unique_values
)
from flowfile_core.flowfile.flow_data_engine.threaded_processes import (
    get_join_count,
    write_threaded
)

from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource


@dataclass
class FlowDataEngine:
    """
    A class that provides a unified interface for working with tabular data, supporting both eager and lazy evaluation.

    The class is organized into several logical sections:
    1. Core properties and initialization
    2. Data access and manipulation
    3. Schema and metadata operations
    4. Transformations and operations
    5. I/O operations
    """

    # Core attributes
    _data_frame: Union[pl.DataFrame, pl.LazyFrame]
    columns: List[Any]

    # Metadata attributes
    name: str = None
    number_of_records: int = None
    errors: List = None
    _schema: Optional[List['FlowfileColumn']] = None

    # Configuration attributes
    _optimize_memory: bool = False
    _lazy: bool = None
    _streamable: bool = True
    _calculate_schema_stats: bool = False

    # Cache and optimization attributes
    __col_name_idx_map: Dict = None
    __data_map: Dict = None
    __optimized_columns: List = None
    __sample__: str = None
    __number_of_fields: int = None
    _col_idx: Dict[str, int] = None

    # Source tracking
    _org_path: Optional[str] = None
    _external_source: Optional[ExternalDataSource] = None

    # State tracking
    sorted_by: int = None
    is_future: bool = False
    is_collected: bool = True
    ind_schema_calculated: bool = False

    # Callbacks
    _future: Future = None
    _number_of_records_callback: Callable = None
    _data_callback: Callable = None

    # Tracking info
    # node_id: int = None  # TODO: Implement node_id
    # flow_id: int = None  # TODO: Implement flow_id

    def __init__(self,
                 raw_data: Union[List[Dict], List[Any], 'ParquetFile', pl.DataFrame, pl.LazyFrame] = None,
                 path_ref: str = None,
                 name: str = None,
                 optimize_memory: bool = True,
                 schema: List['FlowfileColumn'] | List[str] | pl.Schema = None,
                 number_of_records: int = None,
                 calculate_schema_stats: bool = False,
                 streamable: bool = True,
                 number_of_records_callback: Callable = None,
                 data_callback: Callable = None):
        """Initialize FlowDataEngine with various data sources and configuration options."""
        self._initialize_attributes(number_of_records_callback, data_callback, streamable)

        if raw_data is not None:
            self._handle_raw_data(raw_data, number_of_records, optimize_memory)
        elif path_ref:
            self._handle_path_ref(path_ref, optimize_memory)
        else:
            self.initialize_empty_fl()

        self._finalize_initialization(name, optimize_memory, schema, calculate_schema_stats)

    def _initialize_attributes(self, number_of_records_callback, data_callback, streamable):
        """Initialize basic attributes with default values."""
        self._external_source = None
        self._number_of_records_callback = number_of_records_callback
        self._data_callback = data_callback
        self.ind_schema_calculated = False
        self._streamable = streamable
        self._org_path = None
        self._lazy = False
        self.errors = []
        self._calculate_schema_stats = False
        self.is_collected = True
        self.is_future = False

    def _handle_raw_data(self, raw_data, number_of_records, optimize_memory):
        """Process different types of input data."""
        if isinstance(raw_data, pl.DataFrame):
            self._handle_polars_dataframe(raw_data, number_of_records)
        elif isinstance(raw_data, pl.LazyFrame):
            self._handle_polars_lazy_frame(raw_data, number_of_records, optimize_memory)
        elif isinstance(raw_data, (list, dict)):
            self._handle_python_data(raw_data)

    def _handle_polars_dataframe(self, df: pl.DataFrame, number_of_records: Optional[int]):
        """Handle Polars DataFrame input."""
        self.data_frame = df
        self.number_of_records = number_of_records or df.select(pl.len())[0, 0]

    def _handle_polars_lazy_frame(self, lf: pl.LazyFrame, number_of_records: Optional[int], optimize_memory: bool):
        """Handle Polars LazyFrame input."""
        self.data_frame = lf
        self._lazy = True
        if number_of_records is not None:
            self.number_of_records = number_of_records
        elif optimize_memory:
            self.number_of_records = -1
        else:
            self.number_of_records = lf.select(pl.len()).collect()[0, 0]

    def _handle_python_data(self, data: Union[List, Dict]):
        """Handle Python list or dict input."""
        if isinstance(data, dict):
            self._handle_dict_input(data)
        else:
            self._handle_list_input(data)

    def _handle_dict_input(self, data: Dict):
        """Handle dictionary input."""
        if len(data) == 0:
            self.initialize_empty_fl()
        lengths = [len(v) if isinstance(v, (list, tuple)) else 1 for v in data.values()]

        if len(set(lengths)) == 1 and lengths[0]>1:
            self.number_of_records = lengths[0]
            self.data_frame = pl.DataFrame(data)
        else:
            self.number_of_records = 1
            self.data_frame = pl.DataFrame([data])

    def _handle_list_input(self, data: List):
        """Handle list input."""
        number_of_records = len(data)
        if number_of_records > 0:
            processed_data = self._process_list_data(data)
            self.number_of_records = number_of_records
            self.data_frame = pl.DataFrame(processed_data)
            self.lazy = True
        else:
            self.initialize_empty_fl()
            self.number_of_records = 0

    @staticmethod
    def _process_list_data(data: List) -> List[Dict]:
        """Process list data into a format suitable for DataFrame creation."""
        if not (isinstance(data[0], dict) or hasattr(data[0], '__dict__')):
            try:
                return pl.DataFrame(data).to_dicts()
            except:
                raise Exception('Value must be able to be converted to dictionary')

        if not isinstance(data[0], dict):
            data = [row.__dict__ for row in data]

        return utils.ensure_similarity_dicts(data)

    def _handle_path_ref(self, path_ref: str, optimize_memory: bool):
        """Handle file path reference input."""
        try:
            pf = ParquetFile(path_ref)
        except Exception as e:
            logger.error(e)
            raise Exception("Provided ref is not a parquet file")

        self.number_of_records = pf.metadata.num_rows
        if optimize_memory:
            self._lazy = True
            self.data_frame = pl.scan_parquet(path_ref)
        else:
            self.data_frame = pl.read_parquet(path_ref)

    def _finalize_initialization(self, name: str, optimize_memory: bool, schema: Optional[Any],
                                 calculate_schema_stats: bool):
        """Finalize initialization by setting remaining attributes."""
        _ = calculate_schema_stats
        self.name = name
        self._optimize_memory = optimize_memory
        pl_schema = self.data_frame.collect_schema()
        self._schema = self._handle_schema(schema, pl_schema)
        self.columns = [c.column_name for c in self._schema] if self._schema else pl_schema.names()

    def __getitem__(self, item):
        """Access a specific column or item from the DataFrame."""
        return self.data_frame.select([item])

    @property
    def data_frame(self) -> pl.LazyFrame | pl.DataFrame:
        """Get the underlying DataFrame with appropriate handling of different states."""
        if self._data_frame is not None and not self.is_future:
            return self._data_frame
        elif self.is_future:
            return self._data_frame
        elif self._external_source is not None and self.lazy:
            return self._data_frame
        elif self._external_source is not None and not self.lazy:
            if self._external_source.get_pl_df() is None:
                data_frame = list(self._external_source.get_iter())
                if len(data_frame) > 0:
                    self.data_frame = pl.DataFrame(data_frame)
            else:
                self.data_frame = self._external_source.get_pl_df()
            self.calculate_schema()
            return self._data_frame

    @data_frame.setter
    def data_frame(self, df: pl.LazyFrame | pl.DataFrame):
        """Set the underlying DataFrame with validation."""
        if self.lazy and isinstance(df, pl.DataFrame):
            raise Exception('Cannot set a non-lazy dataframe to a lazy flowfile')
        self._data_frame = df

    @property
    def schema(self) -> List[FlowfileColumn]:
        """Get the schema of the DataFrame, calculating if necessary."""
        if self.number_of_fields == 0:
            return []
        if self._schema is None or (self._calculate_schema_stats and not self.ind_schema_calculated):
            if self._calculate_schema_stats and not self.ind_schema_calculated:
                schema_stats = self._calculate_schema()
                self.ind_schema_calculated = True
            else:
                schema_stats = [
                    dict(column_name=k, pl_datatype=v, col_index=i)
                    for i, (k, v) in enumerate(self.data_frame.collect_schema().items())
                ]
            self._schema = convert_stats_to_column_info(schema_stats)
        return self._schema

    @property
    def number_of_fields(self) -> int:
        """Get the number of fields in the DataFrame."""
        if self.__number_of_fields is None:
            self.__number_of_fields = len(self.columns)
        return self.__number_of_fields

    # Data Collection and Sampling Methods

    def collect(self, n_records: int = None) -> pl.DataFrame:
        """
        Collect data from the DataFrame, optionally limiting the number of records.
        Handles streaming and error cases appropriately.
        """
        if n_records is None:
            logger.info(f'Fetching all data for Table object "{id(self)}". Settings: streaming={self._streamable}')
        else:
            logger.info(f'Fetching {n_records} record(s) for Table object "{id(self)}". '
                        f'Settings: streaming={self._streamable}')

        if not self.lazy:
            return self.data_frame

        try:
            return self._collect_data(n_records)
        except Exception as e:
            self.errors = [e]
            return self._handle_collection_error(n_records)

    def _collect_data(self, n_records: int = None) -> pl.DataFrame:
        """Internal method to handle data collection."""
        if n_records is None:
            self.collect_external()
            if self._streamable:
                try:
                    logger.info('Collecting data in streaming mode')
                    return self.data_frame.collect(engine="streaming")
                except PanicException:
                    self._streamable = False

            logger.info('Collecting data in non-streaming mode')
            return self.data_frame.collect()

        if self.external_source is not None:
            return self._collect_from_external_source(n_records)

        if self._streamable:
            return self.data_frame.head(n_records).collect(engine="streaming", comm_subplan_elim=False)
        return self.data_frame.head(n_records).collect()

    def _collect_from_external_source(self, n_records: int) -> pl.DataFrame:
        """Handle collection from external source."""
        if self.external_source.get_pl_df() is not None:
            all_data = self.external_source.get_pl_df().head(n_records)
            self.data_frame = all_data
        else:
            all_data = self.external_source.get_sample(n_records)
            self.data_frame = pl.LazyFrame(all_data)
        return self.data_frame

    def _handle_collection_error(self, n_records: int) -> pl.DataFrame:
        """Handle errors during collection by attempting partial collection."""
        n_records = 100000000 if n_records is None else n_records
        ok_cols, error_cols = self._identify_valid_columns(n_records)

        if len(ok_cols) > 0:
            return self._create_partial_dataframe(ok_cols, error_cols, n_records)
        return self._create_empty_dataframe(n_records)

    def _identify_valid_columns(self, n_records: int) -> Tuple[List[str], List[Tuple[str, Any]]]:
        """Identify which columns can be collected successfully."""
        ok_cols = []
        error_cols = []
        for c in self.columns:
            try:
                _ = self.data_frame.select(c).head(n_records).collect()
                ok_cols.append(c)
            except:
                error_cols.append((c, self.data_frame.schema[c]))
        return ok_cols, error_cols

    def _create_partial_dataframe(self, ok_cols: List[str], error_cols: List[Tuple[str, Any]],
                                  n_records: int) -> pl.DataFrame:
        """Create a DataFrame with partial data for columns that could be collected."""
        df = self.data_frame.select(ok_cols)
        df = df.with_columns([
            pl.lit(None).alias(column_name).cast(data_type)
            for column_name, data_type in error_cols
        ])
        return df.select(self.columns).head(n_records).collect()

    def _create_empty_dataframe(self, n_records: int) -> pl.DataFrame:
        """Create an empty DataFrame with the correct schema."""
        if self.number_of_records > 0:
            return pl.DataFrame({
                column_name: pl.Series(
                    name=column_name,
                    values=[None] * min(self.number_of_records, n_records)
                ).cast(data_type)
                for column_name, data_type in self.data_frame.schema.items()
            })
        return pl.DataFrame(schema=self.data_frame.schema)

    # Data Transformation Methods

    def do_group_by(self, group_by_input: transform_schemas.GroupByInput,
                    calculate_schema_stats: bool = True) -> "FlowDataEngine":
        """Perform group by operations on the DataFrame."""
        aggregations = [c for c in group_by_input.agg_cols if c.agg != 'groupby']
        group_columns = [c for c in group_by_input.agg_cols if c.agg == 'groupby']

        if len(group_columns) == 0:
            return FlowDataEngine(
                self.data_frame.select(
                    ac.agg_func(ac.old_name).alias(ac.new_name) for ac in aggregations
                ),
                calculate_schema_stats=calculate_schema_stats
            )

        df = self.data_frame.rename({c.old_name: c.new_name for c in group_columns})
        group_by_columns = [n_c.new_name for n_c in group_columns]
        return FlowDataEngine(
            df.group_by(*group_by_columns).agg(
                ac.agg_func(ac.old_name).alias(ac.new_name) for ac in aggregations
            ),
            calculate_schema_stats=calculate_schema_stats
        )

    def do_sort(self, sorts: List[transform_schemas.SortByInput]) -> "FlowDataEngine":
        """Sort the DataFrame based on specified columns and directions."""
        if not sorts:
            return self

        descending = [s.how == 'desc' or s.how.lower() == 'descending' for s in sorts]
        df = self.data_frame.sort([sort_by.column for sort_by in sorts], descending=descending)
        return FlowDataEngine(df, number_of_records=self.number_of_records, schema=self.schema)

    def change_column_types(self, transforms: List[transform_schemas.SelectInput],
                            calculate_schema: bool = False) -> "FlowDataEngine":
        """Change the data types of specified columns."""
        dtypes = [dtype.base_type() for dtype in self.data_frame.collect_schema().dtypes()]
        idx_mapping = list(
            (transform.old_name, self.cols_idx.get(transform.old_name), getattr(pl, transform.polars_type))
            for transform in transforms if transform.data_type is not None
        )

        actual_transforms = [c for c in idx_mapping if c[2] != dtypes[c[1]]]
        transformations = [
            utils.define_pl_col_transformation(col_name=transform[0], col_type=transform[2])
            for transform in actual_transforms
        ]

        df = self.data_frame.with_columns(transformations)
        return FlowDataEngine(
            df,
            number_of_records=self.number_of_records,
            calculate_schema_stats=calculate_schema,
            streamable=self._streamable
        )

    # Data Export and Conversion Methods

    def save(self, path: str, data_type: str = 'parquet') -> Future:
        """Save the DataFrame to a file."""
        estimated_size = deepcopy(self.get_estimated_file_size() * 4)
        df = deepcopy(self.data_frame)
        return write_threaded(_df=df, path=path, data_type=data_type, estimated_size=estimated_size)

    def to_pylist(self) -> List[Dict]:
        """Convert the DataFrame to a list of dictionaries."""
        if self.lazy:
            return self.data_frame.collect(engine="streaming" if self._streamable else "auto").to_dicts()
        return self.data_frame.to_dicts()

    @classmethod
    def create_from_external_source(cls, external_source: ExternalDataSource) -> "FlowDataEngine":
        """Create a FlowDataEngine from an external data source."""
        if external_source.schema is not None:
            ff = cls.create_from_schema(external_source.schema)
        elif external_source.initial_data_getter is not None:
            ff = cls(raw_data=external_source.initial_data_getter())
        else:
            ff = cls()
        ff._external_source = external_source
        return ff

    @classmethod
    def create_from_sql(cls, sql: str, conn: Any) -> "FlowDataEngine":
        """Create a FlowDataEngine from a SQL query."""
        return cls(pl.read_sql(sql, conn))

    @classmethod
    def create_from_schema(cls, schema: List[FlowfileColumn]) -> "FlowDataEngine":
        """Create a FlowDataEngine from a schema definition."""
        pl_schema = []
        for i, flow_file_column in enumerate(schema):
            pl_schema.append((flow_file_column.name, type_to_polars(flow_file_column.data_type)))
            schema[i].col_index = i
        df = pl.LazyFrame(schema=pl_schema)
        return cls(df, schema=schema, calculate_schema_stats=False, number_of_records=0)

    @classmethod
    def create_from_path(cls, received_table: input_schema.ReceivedTableBase) -> "FlowDataEngine":
        """Create a FlowDataEngine from a file path."""
        received_table.set_absolute_filepath()

        file_type_handlers = {
            'csv': create_funcs.create_from_path_csv,
            'parquet': create_funcs.create_from_path_parquet,
            'excel': create_funcs.create_from_path_excel
        }

        handler = file_type_handlers.get(received_table.file_type)
        if not handler:
            raise Exception(f'Cannot create from {received_table.file_type}')

        flow_file = cls(handler(received_table))
        flow_file._org_path = received_table.abs_file_path
        return flow_file

    @classmethod
    def create_random(cls, number_of_records: int = 1000) -> "FlowDataEngine":
        """Create a FlowDataEngine with random data."""
        return cls(create_fake_data(number_of_records))

    @classmethod
    def generate_enumerator(cls, length: int = 1000, output_name: str = 'output_column') -> "FlowDataEngine":
        """Generate a sequence of numbers as a FlowDataEngine."""
        if length > 10_000_000:
            length = 10_000_000
        return cls(pl.LazyFrame().select((pl.int_range(0, length, dtype=pl.UInt32)).alias(output_name)))

    # Schema Handling Methods

    def _handle_schema(self, schema: List[FlowfileColumn] | List[str] | pl.Schema,
                       pl_schema: pl.Schema) -> List[FlowfileColumn] | None:
        """Handle schema processing and validation."""
        if schema is None:
            return None

        if schema.__len__() != pl_schema.__len__():
            raise Exception(
                f'Schema does not match the data got {schema.__len__()} columns expected {pl_schema.__len__()}')

        if isinstance(schema, pl.Schema):
            return self._handle_polars_schema(schema, pl_schema)
        elif isinstance(schema, list) and len(schema) == 0:
            return []
        elif isinstance(schema[0], str):
            return self._handle_string_schema(schema, pl_schema)
        return schema

    def _handle_polars_schema(self, schema: pl.Schema, pl_schema: pl.Schema) -> List[FlowfileColumn]:
        """Handle Polars schema conversion."""
        flow_file_columns = [
            FlowfileColumn.create_from_polars_dtype(column_name=col_name, data_type=dtype)
            for col_name, dtype in zip(schema.names(), schema.dtypes())
        ]

        select_arg = [
            pl.col(o).alias(n).cast(schema_dtype)
            for o, n, schema_dtype in zip(pl_schema.names(), schema.names(), schema.dtypes())
        ]

        self.data_frame = self.data_frame.select(select_arg)
        return flow_file_columns

    def _handle_string_schema(self, schema: List[str], pl_schema: pl.Schema) -> List[FlowfileColumn]:
        """Handle string-based schema conversion."""
        flow_file_columns = [
            FlowfileColumn.create_from_polars_dtype(column_name=col_name, data_type=dtype)
            for col_name, dtype in zip(schema, pl_schema.dtypes())
        ]

        self.data_frame = self.data_frame.rename({
            o: n for o, n in zip(pl_schema.names(), schema)
        })

        return flow_file_columns

    # Data Manipulation Methods

    def split(self, split_input: transform_schemas.TextToRowsInput) -> "FlowDataEngine":
        """Split a column into multiple rows based on a delimiter."""
        output_column_name = (
            split_input.output_column_name
            if split_input.output_column_name
            else split_input.column_to_split
        )

        split_value = (
            split_input.split_fixed_value
            if split_input.split_by_fixed_value
            else pl.col(split_input.split_by_column)
        )

        df = (
            self.data_frame.with_columns(
                pl.col(split_input.column_to_split)
                .str.split(by=split_value)
                .alias(output_column_name)
            )
            .explode(output_column_name)
        )

        return FlowDataEngine(df)

    def unpivot(self, unpivot_input: transform_schemas.UnpivotInput) -> "FlowDataEngine":
        """Convert data from wide to long format."""
        lf = self.data_frame

        if unpivot_input.data_type_selector_expr is not None:
            result = lf.unpivot(
                on=unpivot_input.data_type_selector_expr(),
                index=unpivot_input.index_columns
            )
        elif unpivot_input.value_columns is not None:
            result = lf.unpivot(
                on=unpivot_input.value_columns,
                index=unpivot_input.index_columns
            )
        else:
            result = lf.unpivot()

        return FlowDataEngine(result)

    def do_pivot(self, pivot_input: transform_schemas.PivotInput, node_logger: NodeLogger = None) -> "FlowDataEngine":
        """Convert data from long to wide format with aggregations."""
        # Get unique values for pivot columns
        max_unique_vals = 200
        new_cols_unique = fetch_unique_values(self.data_frame.select(pivot_input.pivot_column)
                                              .unique()
                                              .sort(pivot_input.pivot_column)
                                              .limit(max_unique_vals).cast(pl.String))
        if len(new_cols_unique) >= max_unique_vals:
            if node_logger:
                node_logger.warning('Pivot column has too many unique values. Please consider using a different column.'
                                    f' Max unique values: {max_unique_vals}')

        if len(pivot_input.index_columns) == 0:
            no_index_cols = True
            pivot_input.index_columns = ['__temp__']
            ff = self.apply_flowfile_formula('1', col_name='__temp__')
        else:
            no_index_cols = False
            ff = self

        # Perform pivot operations
        index_columns = pivot_input.get_index_columns()
        grouped_ff = ff.do_group_by(pivot_input.get_group_by_input(), False)
        pivot_column = pivot_input.get_pivot_column()

        input_df = grouped_ff.data_frame.with_columns(
            pivot_column.cast(pl.String).alias(pivot_input.pivot_column)
        )
        number_of_aggregations = len(pivot_input.aggregations)
        df = (
            input_df.select(
                *index_columns,
                pivot_column,
                pivot_input.get_values_expr()
            )
            .group_by(*index_columns)
            .agg([
                (pl.col('vals').filter(pivot_column == new_col_value))
                .first()
                .alias(new_col_value)
                for new_col_value in new_cols_unique
            ])
            .select(
                *index_columns,
                *[
                    pl.col(new_col).struct.field(agg).alias(f'{new_col + "_" + agg if number_of_aggregations > 1 else new_col }')
                    for new_col in new_cols_unique
                    for agg in pivot_input.aggregations
                ]
            )
        )

        # Clean up temporary columns if needed
        if no_index_cols:
            df = df.drop('__temp__')
            pivot_input.index_columns = []

        return FlowDataEngine(df, calculate_schema_stats=False)

    def do_filter(self, predicate: str) -> "FlowDataEngine":
        """Filter the DataFrame based on a predicate expression."""
        try:
            f = to_expr(predicate)
        except Exception as e:
            logger.warning(f'Error in filter expression: {e}')
            f = to_expr("False")
        df = self.data_frame.filter(f)
        return FlowDataEngine(df, schema=self.schema, streamable=self._streamable)

    def add_record_id(self, record_id_settings: transform_schemas.RecordIdInput) -> "FlowDataEngine":
        """Add a record ID column with optional grouping."""
        if record_id_settings.group_by and len(record_id_settings.group_by_columns) > 0:
            return self._add_grouped_record_id(record_id_settings)
        return self._add_simple_record_id(record_id_settings)

    def _add_grouped_record_id(self, record_id_settings: transform_schemas.RecordIdInput) -> "FlowDataEngine":
        """Add a record ID column with grouping."""
        select_cols = [pl.col(record_id_settings.output_column_name)] + [pl.col(c) for c in self.columns]

        df = (
            self.data_frame
            .with_columns(pl.lit(1).alias(record_id_settings.output_column_name))
            .with_columns(
                (pl.cum_count(record_id_settings.output_column_name)
                 .over(record_id_settings.group_by_columns) + record_id_settings.offset - 1)
                .alias(record_id_settings.output_column_name)
            )
            .select(select_cols)
        )

        output_schema = [FlowfileColumn.from_input(record_id_settings.output_column_name, 'UInt64')]
        output_schema.extend(self.schema)

        return FlowDataEngine(df, schema=output_schema)

    def _add_simple_record_id(self, record_id_settings: transform_schemas.RecordIdInput) -> "FlowDataEngine":
        """Add a simple sequential record ID column."""
        df = self.data_frame.with_row_index(
            record_id_settings.output_column_name,
            record_id_settings.offset
        )

        output_schema = [FlowfileColumn.from_input(record_id_settings.output_column_name, 'UInt64')]
        output_schema.extend(self.schema)

        return FlowDataEngine(df, schema=output_schema)

    # Utility Methods

    def get_schema_column(self, col_name: str) -> FlowfileColumn:
        """Get schema information for a specific column."""
        for s in self.schema:
            if s.name == col_name:
                return s

    def get_estimated_file_size(self) -> int:
        """Get the estimated size of the file in bytes."""
        if self._org_path is not None:
            return os.path.getsize(self._org_path)
        return 0

    def __repr__(self) -> str:
        """Return string representation of the FlowDataEngine."""
        return f'flowfile table\n{self.data_frame.__repr__()}'

    def __call__(self) -> "FlowDataEngine":
        """Make the class callable, returning self."""
        return self

    def __len__(self) -> int:
        """Get the number of records in the table."""
        return self.number_of_records if self.number_of_records >= 0 else self.get_number_of_records()

    def cache(self) -> "FlowDataEngine":
        """
        Cache the data in background and update the DataFrame reference.

        Returns:
            FlowDataEngine: Self with cached data
        """
        edf = ExternalDfFetcher(lf=self.data_frame, file_ref=str(id(self)), wait_on_completion=False,
                                flow_id=-1,
                                node_id=-1)
        logger.info('Caching data in background')
        result = edf.get_result()
        if isinstance(result, pl.LazyFrame):
            logger.info('Data cached')
            del self._data_frame
            self.data_frame = result
            logger.info('Data loaded from cache')
        return self

    def collect_external(self):
        """Collect data from external source if present."""
        if self._external_source is not None:
            logger.info('Collecting external source')
            if self.external_source.get_pl_df() is not None:
                self.data_frame = self.external_source.get_pl_df().lazy()
            else:
                self.data_frame = pl.LazyFrame(list(self.external_source.get_iter()))
            self._schema = None  # enforce reset schema

    # Data Access Methods
    def get_output_sample(self, n_rows: int = 10) -> List[Dict]:
        """
        Get a sample of the data as a list of dictionaries.

        Args:
            n_rows: Number of rows to sample

        Returns:
            List[Dict]: Sample data as dictionaries
        """
        if self.number_of_records > n_rows or self.number_of_records < 0:
            df = self.collect(n_rows)
        else:
            df = self.collect()
        return df.to_dicts()

    def __get_sample__(self, n_rows: int = 100, streamable: bool = True) -> "FlowDataEngine":
        if not self.lazy:
            df = self.data_frame.lazy()
        else:
            df = self.data_frame

        if streamable:
            try:
                df = df.head(n_rows).collect()
            except Exception as e:
                logger.warning(f'Error in getting sample: {e}')
                df = df.head(n_rows).collect(engine="auto")
        else:
            df = self.collect()
        return FlowDataEngine(df, number_of_records=len(df), schema=self.schema)

    def get_sample(self, n_rows: int = 100, random: bool = False, shuffle: bool = False,
                   seed: int = None) -> "FlowDataEngine":
        """
        Get a sample of rows from the DataFrame.

        Args:
            n_rows: Number of rows to sample
            random: Whether to randomly sample
            shuffle: Whether to shuffle the sample
            seed: Random seed for reproducibility

        Returns:
            FlowDataEngine: New instance with sampled data
        """
        n_records = min(n_rows, self.number_of_records)
        logging.info(f'Getting sample of {n_rows} rows')

        if random:
            if self.lazy and self.external_source is not None:
                self.collect_external()

            if self.lazy and shuffle:
                sample_df = self.data_frame.collect(engine="streaming" if self._streamable else "auto").sample(n_rows,
                                                                                                               seed=seed,
                                                                                     shuffle=shuffle)
            elif shuffle:
                sample_df = self.data_frame.sample(n_rows, seed=seed, shuffle=shuffle)
            else:
                every_n_records = ceil(self.number_of_records / n_rows)
                sample_df = self.data_frame.gather_every(every_n_records)
        else:
            if self.external_source:
                self.collect(n_rows)
            sample_df = self.data_frame.head(n_rows)

        return FlowDataEngine(sample_df, schema=self.schema, number_of_records=n_records)

    def get_subset(self, n_rows: int = 100) -> "FlowDataEngine":
        """
        Get a subset of rows from the DataFrame.

        Args:
            n_rows: Number of rows to include

        Returns:
            FlowDataEngine: New instance with subset of data
        """
        if not self.lazy:
            return FlowDataEngine(self.data_frame.head(n_rows), calculate_schema_stats=True)
        else:
            return FlowDataEngine(self.data_frame.head(n_rows), calculate_schema_stats=True)

    # Iterator Methods
    def iter_batches(self, batch_size: int = 1000, columns: Union[List, Tuple, str] = None):
        """
        Iterate over the DataFrame in batches.

        Args:
            batch_size: Size of each batch
            columns: Columns to include

        Yields:
            FlowDataEngine: New instance for each batch
        """
        if columns:
            self.data_frame = self.data_frame.select(columns)
        self.lazy = False
        batches = self.data_frame.iter_slices(batch_size)
        for batch in batches:
            yield FlowDataEngine(batch)

    def start_fuzzy_join(self, fuzzy_match_input: transform_schemas.FuzzyMatchInput,
                         other: "FlowDataEngine", file_ref: str, flow_id: int = -1,
                         node_id: int | str = -1) -> ExternalFuzzyMatchFetcher:
        """
        Starts a fuzzy join with another DataFrame and returns the object to track.

        Args:
            fuzzy_match_input: Fuzzy matching parameters
            other: Right DataFrame for join
            file_ref: Reference for temporary files
            flow_id: Flow ID for tracking
            node_id: Node ID for tracking
        Returns:
            FlowDataEngine: New instance with joined data
        """
        left_df, right_df = prepare_for_fuzzy_match(left=self, right=other,
                                                    fuzzy_match_input=fuzzy_match_input)
        return ExternalFuzzyMatchFetcher(left_df, right_df,
                                         fuzzy_maps=fuzzy_match_input.fuzzy_maps,
                                         file_ref=file_ref + '_fm',
                                         wait_on_completion=False,
                                         flow_id=flow_id,
                                         node_id=node_id)

    def do_fuzzy_join(self, fuzzy_match_input: transform_schemas.FuzzyMatchInput,
                      other: "FlowDataEngine", file_ref: str, flow_id: int = -1,
                      node_id: int | str = -1) -> "FlowDataEngine":
        """
        Perform a fuzzy join with another DataFrame.

        Args:
            fuzzy_match_input: Fuzzy matching parameters
            other: Right DataFrame for join
            file_ref: Reference for temporary files
            flow_id: Flow ID for tracking
            node_id: Node ID for tracking
        Returns:
            FlowDataEngine: New instance with joined data
        """
        left_df, right_df = prepare_for_fuzzy_match(left=self, right=other,
                                                    fuzzy_match_input=fuzzy_match_input)
        f = ExternalFuzzyMatchFetcher(left_df, right_df,
                                      fuzzy_maps=fuzzy_match_input.fuzzy_maps,
                                      file_ref=file_ref + '_fm',
                                      wait_on_completion=True,
                                      flow_id=flow_id,
                                      node_id=node_id)
        return FlowDataEngine(f.get_result())

    def fuzzy_match(self, right: "FlowDataEngine", left_on: str, right_on: str,
                    fuzzy_method: str = 'levenshtein', threshold: float = 0.75) -> "FlowDataEngine":
        """
        Perform fuzzy matching between two DataFrames.

        Args:
            right: Right DataFrame for matching
            left_on: Column from left DataFrame
            right_on: Column from right DataFrame
            fuzzy_method: Method for fuzzy matching
            threshold: Matching threshold

        Returns:
            FlowDataEngine: New instance with matched data
        """
        fuzzy_match_input = transform_schemas.FuzzyMatchInput(
            [transform_schemas.FuzzyMap(
                left_on, right_on,
                fuzzy_type=fuzzy_method,
                threshold_score=threshold
            )],
            left_select=self.columns,
            right_select=right.columns
        )
        return self.do_fuzzy_join(fuzzy_match_input, right, str(id(self)))

    def do_cross_join(self, cross_join_input: transform_schemas.CrossJoinInput,
                      auto_generate_selection: bool, verify_integrity: bool,
                      other: "FlowDataEngine") -> "FlowDataEngine":
        """
        Perform a cross join with another DataFrame.

        Args:
            cross_join_input: Cross join parameters
            auto_generate_selection: Whether to auto-generate column selection
            verify_integrity: Whether to verify join integrity
            other: Right DataFrame for join

        Returns:
            FlowDataEngine: New instance with joined data

        Raises:
            Exception: If join would result in too many records
        """
        self.lazy = True
        other.lazy = True

        verify_join_select_integrity(cross_join_input, left_columns=self.columns, right_columns=other.columns)

        # if auto_generate_selection:
        #     cross_join_input.auto_rename()

        right_select = [v.old_name for v in cross_join_input.right_select.renames
                        if (v.keep or v.join_key) and v.is_available]
        left_select = [v.old_name for v in cross_join_input.left_select.renames
                       if (v.keep or v.join_key) and v.is_available]

        left = self.data_frame.select(left_select).rename(cross_join_input.left_select.rename_table)
        right = other.data_frame.select(right_select).rename(cross_join_input.right_select.rename_table)

        if verify_integrity:
            n_records = self.get_number_of_records() * other.get_number_of_records()
            if n_records > 1_000_000_000:
                raise Exception("Join will result in too many records, ending process")
        else:
            n_records = -1

        joined_df = left.join(right, how='cross')

        cols_to_delete_after = [col.new_name for col in
                                cross_join_input.left_select.renames + cross_join_input.left_select.renames
                                if col.join_key and not col.keep and col.is_available]

        if verify_integrity:
            return FlowDataEngine(joined_df.drop(cols_to_delete_after), calculate_schema_stats=False,
                                 number_of_records=n_records, streamable=False)
        else:
            fl = FlowDataEngine(joined_df.drop(cols_to_delete_after), calculate_schema_stats=False,
                               number_of_records=0, streamable=False)
            return fl

    def join(self, join_input: transform_schemas.JoinInput, auto_generate_selection: bool,
             verify_integrity: bool, other: "FlowDataEngine") -> "FlowDataEngine":
        """
        Perform a join operation with another DataFrame.

        Args:
            join_input: Join parameters
            auto_generate_selection: Whether to auto-generate column selection
            verify_integrity: Whether to verify join integrity
            other: Right DataFrame for join

        Returns:
            FlowDataEngine: New instance with joined data

        Raises:
            Exception: If join would result in too many records or is invalid
        """
        # self.lazy = False if join_input.how == 'right' else True
        # other.lazy = False if join_input.how == 'right' else True

        verify_join_select_integrity(join_input, left_columns=self.columns, right_columns=other.columns)
        if not verify_join_map_integrity(join_input, left_columns=self.schema, right_columns=other.schema):
            raise Exception('Join is not valid by the data fields')
        if auto_generate_selection:
            join_input.auto_rename()

        right_select = [v.old_name for v in join_input.right_select.renames
                        if (v.keep or v.join_key) and v.is_available]
        left_select = [v.old_name for v in join_input.left_select.renames
                       if (v.keep or v.join_key) and v.is_available]
        left = self.data_frame.select(left_select).rename(join_input.left_select.rename_table)
        right = other.data_frame.select(right_select).rename(join_input.right_select.rename_table)

        if verify_integrity and join_input.how != 'right':
            n_records = get_join_count(left, right, left_on_keys=join_input.left_join_keys,
                                       right_on_keys=join_input.right_join_keys, how=join_input.how)
            if n_records > 1_000_000_000:
                raise Exception("Join will result in too many records, ending process")
        else:
            n_records = -1
        if join_input.how == 'right':
            #  Default to left join since right join can give panic issues in execution plan downstream
            joined_df = right.join(left, left_on=join_input.right_join_keys,
                                   right_on=join_input.left_join_keys, how="left", suffix="")
        else:
            joined_df = left.join(right, left_on=join_input.left_join_keys,
                                  right_on=join_input.right_join_keys,
                                  how=join_input.how, suffix="")
        cols_to_delete_after = [col.new_name for col in
                                join_input.left_select.renames + join_input.left_select.renames
                                if col.join_key and not col.keep and col.is_available]
        if len(cols_to_delete_after) > 0:
            joined_df = joined_df.drop(cols_to_delete_after)
        if verify_integrity:
            return FlowDataEngine(joined_df, calculate_schema_stats=True,
                                 number_of_records=n_records, streamable=False)
        else:
            fl = FlowDataEngine(joined_df, calculate_schema_stats=False,
                               number_of_records=0, streamable=False)
            return fl

    # Graph Operations
    def solve_graph(self, graph_solver_input: transform_schemas.GraphSolverInput) -> "FlowDataEngine":
        """
        Solve a graph problem using the specified columns.

        Args:
            graph_solver_input: Graph solving parameters

        Returns:
            FlowDataEngine: New instance with solved graph data
        """
        lf = self.data_frame.with_columns(
            graph_solver(graph_solver_input.col_from, graph_solver_input.col_to)
            .alias(graph_solver_input.output_column_name)
        )
        return FlowDataEngine(lf)

    # Data Modification Methods
    def add_new_values(self, values: Iterable, col_name: str = None) -> "FlowDataEngine":
        """
        Add a new column with specified values.

        Args:
            values: Values to add
            col_name: Name for new column

        Returns:
            FlowDataEngine: New instance with added column
        """
        if col_name is None:
            col_name = 'new_values'
        return FlowDataEngine(self.data_frame.with_columns(pl.Series(values).alias(col_name)))

    def get_record_count(self) -> "FlowDataEngine":
        """
        Get the total number of records.

        Returns:
            FlowDataEngine: New instance with record count
        """
        return FlowDataEngine(self.data_frame.select(pl.len().alias('number_of_records')))

    def assert_equal(self, other: "FlowDataEngine", ordered: bool = True, strict_schema: bool = False):
        """
        Assert that this DataFrame is equal to another.

        Args:
            other: DataFrame to compare with
            ordered: Whether to consider row order
            strict_schema: Whether to strictly compare schemas

        Raises:
            Exception: If DataFrames are not equal
        """
        org_laziness = self.lazy, other.lazy
        self.lazy = False
        other.lazy = False
        self.number_of_records = -1
        other.number_of_records = -1

        if self.get_number_of_records() != other.get_number_of_records():
            raise Exception('Number of records is not equal')

        if self.columns != other.columns:
            raise Exception('Schema is not equal')

        if strict_schema:
            assert self.data_frame.schema == other.data_frame.schema, 'Data types do not match'

        if ordered:
            self_lf = self.data_frame.sort(by=self.columns)
            other_lf = other.data_frame.sort(by=other.columns)
        else:
            self_lf = self.data_frame
            other_lf = other.data_frame

        self.lazy, other.lazy = org_laziness
        assert self_lf.equals(other_lf), 'Data is not equal'

    # Initialization Methods
    def initialize_empty_fl(self):
        """Initialize an empty LazyFrame."""
        self.data_frame = pl.LazyFrame()
        self.number_of_records = 0
        self._lazy = True

    def get_number_of_records(self, warn: bool = False, force_calculate: bool = False) -> int:
        """
        Get the total number of records in the DataFrame.

        Args:
            warn: Whether to warn about expensive operations
            force_calculate: Whether to force recalculation

        Returns:
            int: Number of records

        Raises:
            Exception: If unable to get number of records
        """
        if self.is_future and not self.is_collected:
            return -1

        if self.number_of_records is None or self.number_of_records < 0 or force_calculate:
            if self._number_of_records_callback is not None:
                self._number_of_records_callback(self)

            if self.lazy:
                if warn:
                    logger.warning('Calculating the number of records this can be expensive on a lazy frame')
                try:
                    self.number_of_records = self.data_frame.select(pl.len()).collect(
                        engine="streaming" if self._streamable else "auto")[0, 0]
                except Exception:
                    raise Exception('Could not get number of records')
            else:
                self.number_of_records = self.data_frame.__len__()

        return self.number_of_records

    # Properties
    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0

    @property
    def lazy(self) -> bool:
        """Check if DataFrame is lazy."""
        return self._lazy

    @lazy.setter
    def lazy(self, exec_lazy: bool = False):
        """
        Set the laziness of the DataFrame.

        Args:
            exec_lazy: Whether to make DataFrame lazy
        """
        if exec_lazy != self._lazy:
            if exec_lazy:
                self.data_frame = self.data_frame.lazy()
            else:
                self._lazy = exec_lazy
                if self.external_source is not None:
                    df = self.collect()
                    self.data_frame = df
                else:
                    self.data_frame = self.data_frame.collect(engine="streaming" if self._streamable else "auto")
            self._lazy = exec_lazy

    @property
    def external_source(self) -> ExternalDataSource:
        """Get the external data source."""
        return self._external_source

    @property
    def cols_idx(self) -> Dict[str, int]:
        """Get column index mapping."""
        if self._col_idx is None:
            self._col_idx = {c: i for i, c in enumerate(self.columns)}
        return self._col_idx

    @property
    def __name__(self) -> str:
        """Get table name."""
        return self.name

    # Schema and Column Operations
    def get_select_inputs(self) -> transform_schemas.SelectInputs:
        """
        Get select inputs for all columns.

        Returns:
            SelectInputs: Input specifications for all columns
        """
        return transform_schemas.SelectInputs(
            [transform_schemas.SelectInput(old_name=c.name, data_type=c.data_type) for c in self.schema]
        )

    def select_columns(self, list_select: Union[List[str], Tuple[str], str]) -> "FlowDataEngine":
        """
        Select specific columns from the DataFrame.

        Args:
            list_select: Columns to select

        Returns:
            FlowDataEngine: New instance with selected columns
        """
        if isinstance(list_select, str):
            list_select = [list_select]

        idx_to_keep = [self.cols_idx.get(c) for c in list_select]
        selects = [ls for ls, id_to_keep in zip(list_select, idx_to_keep) if id_to_keep is not None]
        new_schema = [self.schema[i] for i in idx_to_keep if i is not None]

        return FlowDataEngine(
            self.data_frame.select(selects),
            number_of_records=self.number_of_records,
            schema=new_schema,
            streamable=self._streamable
        )

    def drop_columns(self, columns: List[str]) -> "FlowDataEngine":
        """
        Drop specified columns from the DataFrame.

        Args:
            columns: Columns to drop

        Returns:
            FlowDataEngine: New instance without dropped columns
        """
        cols_for_select = tuple(set(self.columns) - set(columns))
        idx_to_keep = [self.cols_idx.get(c) for c in cols_for_select]
        new_schema = [self.schema[i] for i in idx_to_keep]

        return FlowDataEngine(
            self.data_frame.select(cols_for_select),
            number_of_records=self.number_of_records,
            schema=new_schema
        )

    def reorganize_order(self, column_order: List[str]) -> "FlowDataEngine":
        """
        Reorganize columns in specified order.

        Args:
            column_order: Desired column order

        Returns:
            FlowDataEngine: New instance with reordered columns
        """
        df = self.data_frame.select(column_order)
        schema = sorted(self.schema, key=lambda x: column_order.index(x.column_name))
        return FlowDataEngine(df, schema=schema, number_of_records=self.number_of_records)

    def apply_flowfile_formula(self, func: str, col_name: str,
                               output_data_type: pl.DataType = None) -> "FlowDataEngine":
        """
        Apply a formula to create a new column.

        Args:
            func: Formula to apply
            col_name: Name for new column
            output_data_type: Data type for output

        Returns:
            FlowDataEngine: New instance with added column
        """
        parsed_func = to_expr(func)
        if output_data_type is not None:
            df2 = self.data_frame.with_columns(parsed_func.cast(output_data_type).alias(col_name))
        else:
            df2 = self.data_frame.with_columns(parsed_func.alias(col_name))

        return FlowDataEngine(df2, number_of_records=self.number_of_records)

    def apply_sql_formula(self, func: str, col_name: str,
                          output_data_type: pl.DataType = None) -> "FlowDataEngine":
        """
        Apply an SQL-style formula to create a new column.

        Args:
            func: SQL formula to apply
            col_name: Name for new column
            output_data_type: Data type for output

        Returns:
            FlowDataEngine: New instance with added column
        """
        expr = to_expr(func)
        if output_data_type is not None:
            df = self.data_frame.with_columns(expr.cast(output_data_type).alias(col_name))
        else:
            df = self.data_frame.with_columns(expr.alias(col_name))

        return FlowDataEngine(df, number_of_records=self.number_of_records)

    def output(self, output_fs: input_schema.OutputSettings, flow_id: int, node_id: int | str,
               execute_remote: bool = True) -> "FlowDataEngine":
        """
        Write DataFrame to output file.

        Args:
            output_fs: Output settings.
            flow_id: Flow ID for tracking.
            node_id: Node ID for tracking.
            execute_remote: If the output should be executed at the flowfile worker process.
        Returns:
            FlowDataEngine: Self for chaining
        """
        logger.info('Starting to write output')
        if execute_remote:
            status = utils.write_output(
                self.data_frame,
                data_type=output_fs.file_type,
                path=output_fs.abs_file_path,
                write_mode=output_fs.write_mode,
                sheet_name=output_fs.output_excel_table.sheet_name,
                delimiter=output_fs.output_csv_table.delimiter,
                flow_id=flow_id,
                node_id=node_id
            )
            tracker = ExternalExecutorTracker(status)
            tracker.get_result()
            logger.info('Finished writing output')
        else:
            logger.info("Starting to write results locally")
            utils.local_write_output(
                self.data_frame,
                data_type=output_fs.file_type,
                path=output_fs.abs_file_path,
                write_mode=output_fs.write_mode,
                sheet_name=output_fs.output_excel_table.sheet_name,
                delimiter=output_fs.output_csv_table.delimiter,
                flow_id=flow_id,
                node_id=node_id,
            )
            logger.info("Finished writing output")
        return self

    # Data Operations
    def make_unique(self, unique_input: transform_schemas.UniqueInput = None) -> "FlowDataEngine":
        """
        Get unique rows based on specified columns.

        Args:
            unique_input: Unique operation parameters

        Returns:
            FlowDataEngine: New instance with unique rows
        """
        if unique_input is None or unique_input.columns is None:
            return FlowDataEngine(self.data_frame.unique())
        return FlowDataEngine(self.data_frame.unique(unique_input.columns, keep=unique_input.strategy))

    def concat(self, other: Iterable["FlowDataEngine"] | "FlowDataEngine") -> "FlowDataEngine":
        """
        Concatenate with other DataFrames.

        Args:
            other: DataFrames to concatenate

        Returns:
            FlowDataEngine: Concatenated DataFrame
        """
        if isinstance(other, FlowDataEngine):
            other = [other]

        dfs: List[pl.LazyFrame] | List[pl.DataFrame] = [self.data_frame] + [flt.data_frame for flt in other]
        return FlowDataEngine(pl.concat(dfs, how='diagonal_relaxed'))

    def do_select(self, select_inputs: transform_schemas.SelectInputs,
                  keep_missing: bool = True) -> "FlowDataEngine":
        """
        Perform complex column selection and transformation.

        Args:
            select_inputs: Selection specifications
            keep_missing: Whether to keep columns not specified

        Returns:
            FlowDataEngine: New instance with selected/transformed columns
        """
        new_schema = deepcopy(self.schema)
        renames = [r for r in select_inputs.renames if r.is_available]

        if not keep_missing:
            drop_cols = set(self.data_frame.collect_schema().names()) - set(r.old_name for r in renames).union(
                set(r.old_name for r in renames if not r.keep))
            keep_cols = []
        else:
            keep_cols = list(set(self.data_frame.collect_schema().names()) - set(r.old_name for r in renames))
            drop_cols = set(r.old_name for r in renames if not r.keep)

        if len(drop_cols) > 0:
            new_schema = [s for s in new_schema if s.name not in drop_cols]
        new_schema_mapping = {v.name: v for v in new_schema}

        available_renames = []
        for rename in renames:
            if (rename.new_name != rename.old_name or rename.new_name not in new_schema_mapping) and rename.keep:
                schema_entry = new_schema_mapping.get(rename.old_name)
                if schema_entry is not None:
                    available_renames.append(rename)
                    schema_entry.column_name = rename.new_name

        rename_dict = {r.old_name: r.new_name for r in available_renames}
        fl = self.select_columns(
            list_select=[col_to_keep.old_name for col_to_keep in renames if col_to_keep.keep] + keep_cols)
        fl = fl.change_column_types(transforms=[r for r in renames if r.keep])
        ndf = fl.data_frame.rename(rename_dict)
        renames.sort(key=lambda r: 0 if r.position is None else r.position)
        sorted_cols = utils.match_order(ndf.collect_schema().names(),
                                        [r.new_name for r in renames] + self.data_frame.collect_schema().names())
        output_file = FlowDataEngine(ndf, number_of_records=self.number_of_records)
        return output_file.reorganize_order(sorted_cols)

    # Utility Methods
    def set_streamable(self, streamable: bool = False):
        """Set whether DataFrame operations should be streamable."""
        self._streamable = streamable

    def _calculate_schema(self) -> List[Dict]:
        """Calculate schema statistics."""
        if self.external_source is not None:
            self.collect_external()
        v = utils.calculate_schema(self.data_frame)
        return v

    def calculate_schema(self):
        """Calculate and return schema."""
        self._calculate_schema_stats = True
        return self.schema

    def count(self) -> int:
        """Get total number of records."""
        return self.get_number_of_records()

    @classmethod
    def create_from_path_worker(cls, received_table: input_schema.ReceivedTable, flow_id: int, node_id: int | str):
        received_table.set_absolute_filepath()
        external_fetcher = ExternalCreateFetcher(received_table=received_table,
                                                 file_type=received_table.file_type, flow_id=flow_id, node_id=node_id)
        return cls(external_fetcher.get_result())


def execute_polars_code(*flowfile_tables: "FlowDataEngine", code: str) -> "FlowDataEngine":
    """
    Execute arbitrary Polars code.

    Args:
        code: Polars code to execute

    Returns:
        FlowDataEngine: Result of code execution
    """
    polars_executable = polars_code_parser.get_executable(code, num_inputs=len(flowfile_tables))
    if len(flowfile_tables) == 0:
        kwargs = {}
    elif len(flowfile_tables) == 1:
        kwargs = {'input_df': flowfile_tables[0].data_frame}
    else:
        kwargs = {f'input_df_{i+1}': flowfile_table.data_frame for i, flowfile_table in enumerate(flowfile_tables)}
    return FlowDataEngine(polars_executable(**kwargs))
