# Standard library imports
import logging
import os
from copy import deepcopy
from dataclasses import dataclass
from math import ceil
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union, TypeVar, Literal, Generator

from pl_fuzzy_frame_match import FuzzyMapping, fuzzy_match_dfs

# Third-party imports
from loky import Future
import polars as pl
from polars.exceptions import PanicException
from polars_grouper import graph_solver
from polars_expr_transformer import simple_function_to_expr as to_expr
from pyarrow import Table as PaTable
from pyarrow.parquet import ParquetFile

# Local imports - Core
from flowfile_core.configs import logger
from flowfile_core.utils.utils import ensure_similarity_dicts
from flowfile_core.configs.flow_logger import NodeLogger
from flowfile_core.schemas import (
    cloud_storage_schemas,
    input_schema,
    transform_schema as transform_schemas
)
from flowfile_core.schemas.schemas import ExecutionLocationsLiteral, get_global_execution_location

# Local imports - Flow File Components
from flowfile_core.flowfile.flow_data_engine import utils
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import (CloudStorageReader,
                                                                          ensure_path_has_wildcard_pattern,
                                                                          get_first_file_from_s3_dir)
from flowfile_core.flowfile.flow_data_engine.create import funcs as create_funcs
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import (
    FlowfileColumn,
    assert_if_flowfile_schema,
    convert_stats_to_column_info
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_data_engine.fuzzy_matching.prepare_for_fuzzy_match import prepare_for_fuzzy_match
from flowfile_core.flowfile.flow_data_engine.join import (
    verify_join_select_integrity,
    verify_join_map_integrity,
    rename_df_table_for_join,
    get_undo_rename_mapping_join,
    get_col_name_to_delete
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

T = TypeVar('T', pl.DataFrame, pl.LazyFrame)


def _handle_duplication_join_keys(left_df: T, right_df: T, join_manager: transform_schemas.JoinInputManager) -> Tuple[T, T, Dict[str, str]]:
    """Temporarily renames join keys to avoid conflicts during a join.

    This helper function checks the join type and renames the join key columns
    in either the left or right DataFrame to a temporary name (`__FL_TEMP__...`).
    This prevents Polars from automatically suffixing columns with `_right` when
    join keys have the same name.

    Args:
        left_df: The left Polars DataFrame or LazyFrame.
        right_df: The right Polars DataFrame or LazyFrame.
        join_input: The JoinInput settings object defining the join.

    Returns:
        A tuple containing:
        - The (potentially modified) left DataFrame.
        - The (potentially modified) right DataFrame.
        - A dictionary mapping the temporary names back to their desired final names.
    """

    def _construct_temp_name(column_name: str) -> str:
        return "__FL_TEMP__"+column_name

    if join_manager.how == 'right':
        left_df = left_df.with_columns(pl.col(jk.new_name).alias(_construct_temp_name(jk.new_name))
                                       for jk in join_manager.left_manager.get_join_key_selects())
        reverse_actions = {
            _construct_temp_name(jk.new_name): transform_schemas.construct_join_key_name("left", jk.new_name)
            for jk in join_manager.left_manager.get_join_key_selects()}
    elif join_manager.how in ('left', 'inner'):
        right_df = right_df.with_columns(pl.col(jk.new_name).alias(_construct_temp_name(jk.new_name))
                                         for jk in join_manager.right_manager.get_join_key_selects())
        reverse_actions = {
            _construct_temp_name(jk.new_name): transform_schemas.construct_join_key_name("right", jk.new_name)
            for jk in join_manager.right_manager.get_join_key_selects()}
    else:
        reverse_actions = {}
    return left_df, right_df, reverse_actions


def ensure_right_unselect_for_semi_and_anti_joins(join_input: transform_schemas.JoinInput) -> None:
    """Modifies JoinInput for semi/anti joins to not keep right-side columns.

    For 'semi' and 'anti' joins, Polars only returns columns from the left
    DataFrame. This function enforces that behavior by modifying the `join_input`
    in-place, setting the `keep` flag to `False` for all columns in the
    right-side selection.

    Args:
        join_input: The JoinInput settings object to modify.
    """
    if join_input.how in ('semi', 'anti'):
        for jk in join_input.right_select.renames:
            jk.keep = False


def get_select_columns(full_select_input: List[transform_schemas.SelectInput]) -> List[str]:
    """Extracts a list of column names to be selected from a SelectInput list.

    This function filters a list of `SelectInput` objects to return the names
    of columns that are marked as available and are either a join key or
    explicitly marked to be kept.

    Args:
        full_select_input: A list of SelectInput objects.

    Returns:
        A list of column names to be selected.
    """
    return [v.old_name for v in full_select_input if (v.keep or v.join_key) and v.is_available]


@dataclass
class FlowDataEngine:
    """The core data handling engine for Flowfile.

    This class acts as a high-level wrapper around a Polars DataFrame or
    LazyFrame, providing a unified API for data ingestion, transformation,
    and output. It manages data state (lazy vs. eager), schema information,
    and execution logic.

    Attributes:
        _data_frame: The underlying Polars DataFrame or LazyFrame.
        columns: A list of column names in the current data frame.
        name: An optional name for the data engine instance.
        number_of_records: The number of records. Can be -1 for lazy frames.
        errors: A list of errors encountered during operations.
        _schema: A cached list of `FlowfileColumn` objects representing the schema.
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

    def __init__(self,
                 raw_data: Union[List[Dict], List[Any], Dict[str, Any], 'ParquetFile', pl.DataFrame, pl.LazyFrame, input_schema.RawData] = None,
                 path_ref: str = None,
                 name: str = None,
                 optimize_memory: bool = True,
                 schema: List['FlowfileColumn'] | List[str] | pl.Schema = None,
                 number_of_records: int = None,
                 calculate_schema_stats: bool = False,
                 streamable: bool = True,
                 number_of_records_callback: Callable = None,
                 data_callback: Callable = None):
        """Initializes the FlowDataEngine from various data sources.

        Args:
            raw_data: The input data. Can be a list of dicts, a Polars DataFrame/LazyFrame,
                or a `RawData` schema object.
            path_ref: A string path to a Parquet file.
            name: An optional name for the data engine instance.
            optimize_memory: If True, prefers lazy operations to conserve memory.
            schema: An optional schema definition. Can be a list of `FlowfileColumn` objects,
                a list of column names, or a Polars `Schema`.
            number_of_records: The number of records, if known.
            calculate_schema_stats: If True, computes detailed statistics for each column.
            streamable: If True, allows for streaming operations when possible.
            number_of_records_callback: A callback function to retrieve the number of records.
            data_callback: A callback function to retrieve the data.
        """
        self._initialize_attributes(number_of_records_callback, data_callback, streamable)

        if raw_data is not None:
            self._handle_raw_data(raw_data, number_of_records, optimize_memory)
        elif path_ref:
            self._handle_path_ref(path_ref, optimize_memory)
        else:
            self.initialize_empty_fl()
        self._finalize_initialization(name, optimize_memory, schema, calculate_schema_stats)

    def _initialize_attributes(self, number_of_records_callback, data_callback, streamable):
        """(Internal) Sets the initial default attributes for a new instance.

        This helper is called first during initialization to ensure all state-tracking
        and configuration attributes have a clean default value before data is processed.
        """
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
        """(Internal) Dispatches raw data to the appropriate handler based on its type.

        This acts as a router during initialization, inspecting the type of `raw_data`
        and calling the corresponding specialized `_handle_*` method to process it.
        """
        if isinstance(raw_data, input_schema.RawData):
            self._handle_raw_data_format(raw_data)
        elif isinstance(raw_data, pl.DataFrame):
            self._handle_polars_dataframe(raw_data, number_of_records)
        elif isinstance(raw_data, pl.LazyFrame):
            self._handle_polars_lazy_frame(raw_data, number_of_records, optimize_memory)
        elif isinstance(raw_data, (list, dict)):
            self._handle_python_data(raw_data)

    def _handle_polars_dataframe(self, df: pl.DataFrame, number_of_records: Optional[int]):
        """(Internal) Initializes the engine from an eager Polars DataFrame."""
        self.data_frame = df
        self.number_of_records = number_of_records or df.select(pl.len())[0, 0]

    def _handle_polars_lazy_frame(self, lf: pl.LazyFrame, number_of_records: Optional[int], optimize_memory: bool):
        """(Internal) Initializes the engine from a Polars LazyFrame."""
        self.data_frame = lf
        self._lazy = True
        if number_of_records is not None:
            self.number_of_records = number_of_records
        elif optimize_memory:
            self.number_of_records = -1
        else:
            self.number_of_records = lf.select(pl.len()).collect()[0, 0]

    def _handle_python_data(self, data: Union[List, Dict]):
        """(Internal) Dispatches Python collections to the correct handler."""
        if isinstance(data, dict):
            self._handle_dict_input(data)
        else:
            self._handle_list_input(data)

    def _handle_dict_input(self, data: Dict):
        """(Internal) Initializes the engine from a Python dictionary."""
        if len(data) == 0:
            self.initialize_empty_fl()
        lengths = [len(v) if isinstance(v, (list, tuple)) else 1 for v in data.values()]

        if len(set(lengths)) == 1 and lengths[0] > 1:
            self.number_of_records = lengths[0]
            self.data_frame = pl.DataFrame(data)
        else:
            self.number_of_records = 1
            self.data_frame = pl.DataFrame([data])
        self.lazy = True

    def _handle_raw_data_format(self, raw_data: input_schema.RawData):
        """(Internal) Initializes the engine from a `RawData` schema object.

        This method uses the schema provided in the `RawData` object to correctly
        infer data types when creating the Polars DataFrame.

        Args:
            raw_data: An instance of `RawData` containing the data and schema.
        """
        flowfile_schema = list(FlowfileColumn.create_from_minimal_field_info(c) for c in raw_data.columns)
        polars_schema = pl.Schema([(flowfile_column.column_name, flowfile_column.get_polars_type().pl_datatype)
                                   for flowfile_column in flowfile_schema])
        try:
            df = pl.DataFrame(raw_data.data, polars_schema, strict=False)
        except TypeError as e:
            logger.warning(f"Could not parse the data with the schema:\n{e}")
            df = pl.DataFrame(raw_data.data)
        self.number_of_records = len(df)
        self.data_frame = df.lazy()
        self.lazy = True

    def _handle_list_input(self, data: List):
        """(Internal) Initializes the engine from a list of records."""
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
        """(Internal) Normalizes list data into a list of dictionaries.

        Ensures that a list of objects or non-dict items is converted into a
        uniform list of dictionaries suitable for Polars DataFrame creation.
        """
        if not (isinstance(data[0], dict) or hasattr(data[0], '__dict__')):
            try:
                return pl.DataFrame(data).to_dicts()
            except TypeError:
                raise Exception('Value must be able to be converted to dictionary')
            except Exception as e:
                raise Exception(f'Value must be able to be converted to dictionary: {e}')

        if not isinstance(data[0], dict):
            data = [row.__dict__ for row in data]

        return ensure_similarity_dicts(data)

    def to_cloud_storage_obj(self, settings: cloud_storage_schemas.CloudStorageWriteSettingsInternal):
        """Writes the DataFrame to an object in cloud storage.

        This method supports writing to various cloud storage providers like AWS S3,
        Azure Data Lake Storage, and Google Cloud Storage.

        Args:
            settings: A `CloudStorageWriteSettingsInternal` object containing connection
                details, file format, and write options.

        Raises:
            ValueError: If the specified file format is not supported for writing.
            NotImplementedError: If the 'append' write mode is used with an unsupported format.
            Exception: If the write operation to cloud storage fails for any reason.
        """
        connection = settings.connection
        write_settings = settings.write_settings

        logger.info(f"Writing to {connection.storage_type} storage: {write_settings.resource_path}")

        if write_settings.write_mode == 'append' and write_settings.file_format != "delta":
            raise NotImplementedError("The 'append' write mode is not yet supported for this destination.")
        storage_options = CloudStorageReader.get_storage_options(connection)
        credential_provider = CloudStorageReader.get_credential_provider(connection)
        # Dispatch to the correct writer based on file format
        if write_settings.file_format == "parquet":
            self._write_parquet_to_cloud(
                write_settings.resource_path,
                storage_options,
                credential_provider,
                write_settings
            )
        elif write_settings.file_format == "delta":
            self._write_delta_to_cloud(
                write_settings.resource_path,
                storage_options,
                credential_provider,
                write_settings
            )
        elif write_settings.file_format == "csv":
            self._write_csv_to_cloud(
                write_settings.resource_path,
                storage_options,
                credential_provider,
                write_settings
            )
        elif write_settings.file_format == "json":
            self._write_json_to_cloud(
                write_settings.resource_path,
                storage_options,
                credential_provider,
                write_settings
            )
        else:
            raise ValueError(f"Unsupported file format for writing: {write_settings.file_format}")

        logger.info(f"Successfully wrote data to {write_settings.resource_path}")

    def _write_parquet_to_cloud(self,
                                resource_path: str,
                                storage_options: Dict[str, Any],
                                credential_provider: Optional[Callable],
                                write_settings: cloud_storage_schemas.CloudStorageWriteSettings):
        """(Internal) Writes the DataFrame to a Parquet file in cloud storage.

        Uses `sink_parquet` for efficient streaming writes. Falls back to a
        collect-then-write pattern if sinking fails.
        """
        try:
            sink_kwargs = {
                "path": resource_path,
                "compression": write_settings.parquet_compression,
            }
            if storage_options:
                sink_kwargs["storage_options"] = storage_options
            if credential_provider:
                sink_kwargs["credential_provider"] = credential_provider
            try:
                self.data_frame.sink_parquet(**sink_kwargs)
            except Exception as e:
                logger.warning(f"Failed to sink the data, falling back to collecing and writing. \n {e}")
                pl_df = self.collect()
                sink_kwargs['file'] = sink_kwargs.pop("path")
                pl_df.write_parquet(**sink_kwargs)

        except Exception as e:
            logger.error(f"Failed to write Parquet to {resource_path}: {str(e)}")
            raise Exception(f"Failed to write Parquet to cloud storage: {str(e)}")

    def _write_delta_to_cloud(self,
                              resource_path: str,
                              storage_options: Dict[str, Any],
                              credential_provider: Optional[Callable],
                              write_settings: cloud_storage_schemas.CloudStorageWriteSettings):
        """(Internal) Writes the DataFrame to a Delta Lake table in cloud storage.

        This operation requires collecting the data first, as `write_delta` operates
        on an eager DataFrame.
        """
        sink_kwargs = {
            "target": resource_path,
            "mode": write_settings.write_mode,
        }
        if storage_options:
            sink_kwargs["storage_options"] = storage_options
        if credential_provider:
            sink_kwargs["credential_provider"] = credential_provider
        self.collect().write_delta(**sink_kwargs)

    def _write_csv_to_cloud(self,
                            resource_path: str,
                            storage_options: Dict[str, Any],
                            credential_provider: Optional[Callable],
                            write_settings: cloud_storage_schemas.CloudStorageWriteSettings):
        """(Internal) Writes the DataFrame to a CSV file in cloud storage.

        Uses `sink_csv` for efficient, streaming writes of the data.
        """
        try:
            sink_kwargs = {
                "path": resource_path,
                "separator": write_settings.csv_delimiter,
            }
            if storage_options:
                sink_kwargs["storage_options"] = storage_options
            if credential_provider:
                sink_kwargs["credential_provider"] = credential_provider

            # sink_csv executes the lazy query and writes the result
            self.data_frame.sink_csv(**sink_kwargs)

        except Exception as e:
            logger.error(f"Failed to write CSV to {resource_path}: {str(e)}")
            raise Exception(f"Failed to write CSV to cloud storage: {str(e)}")

    def _write_json_to_cloud(self,
                             resource_path: str,
                             storage_options: Dict[str, Any],
                             credential_provider: Optional[Callable],
                             write_settings: cloud_storage_schemas.CloudStorageWriteSettings):
        """(Internal) Writes the DataFrame to a line-delimited JSON (NDJSON) file.

        Uses `sink_ndjson` for efficient, streaming writes.
        """
        try:
            sink_kwargs = {"path": resource_path}
            if storage_options:
                sink_kwargs["storage_options"] = storage_options
            if credential_provider:
                sink_kwargs["credential_provider"] = credential_provider
            self.data_frame.sink_ndjson(**sink_kwargs)

        except Exception as e:
            logger.error(f"Failed to write JSON to {resource_path}: {str(e)}")
            raise Exception(f"Failed to write JSON to cloud storage: {str(e)}")

    @classmethod
    def from_cloud_storage_obj(cls, settings: cloud_storage_schemas.CloudStorageReadSettingsInternal) -> "FlowDataEngine":
        """Creates a FlowDataEngine from an object in cloud storage.

        This method supports reading from various cloud storage providers like AWS S3,
        Azure Data Lake Storage, and Google Cloud Storage, with support for
        various authentication methods.

        Args:
            settings: A `CloudStorageReadSettingsInternal` object containing connection
                details, file format, and read options.

        Returns:
            A new `FlowDataEngine` instance containing the data from cloud storage.

        Raises:
            ValueError: If the storage type or file format is not supported.
            NotImplementedError: If a requested file format like "delta" or "iceberg"
                is not yet implemented.
            Exception: If reading from cloud storage fails.
        """
        connection = settings.connection
        read_settings = settings.read_settings

        logger.info(f"Reading from {connection.storage_type} storage: {read_settings.resource_path}")
        # Get storage options based on connection type
        storage_options = CloudStorageReader.get_storage_options(connection)
        # Get credential provider if needed
        credential_provider = CloudStorageReader.get_credential_provider(connection)
        if read_settings.file_format == "parquet":
            return cls._read_parquet_from_cloud(
                read_settings.resource_path,
                storage_options,
                credential_provider,
                read_settings.scan_mode == "directory",
            )
        elif read_settings.file_format == "delta":
            return cls._read_delta_from_cloud(
                read_settings.resource_path,
                storage_options,
                credential_provider,
                read_settings
            )
        elif read_settings.file_format == "csv":
            return cls._read_csv_from_cloud(
                read_settings.resource_path,
                storage_options,
                credential_provider,
                read_settings
            )
        elif read_settings.file_format == "json":
            return cls._read_json_from_cloud(
                read_settings.resource_path,
                storage_options,
                credential_provider,
                read_settings.scan_mode == "directory"
            )
        elif read_settings.file_format == "iceberg":
            return cls._read_iceberg_from_cloud(
                read_settings.resource_path,
                storage_options,
                credential_provider,
                read_settings
            )

        elif read_settings.file_format in ["delta", "iceberg"]:
            # These would require additional libraries
            raise NotImplementedError(f"File format {read_settings.file_format} not yet implemented")
        else:
            raise ValueError(f"Unsupported file format: {read_settings.file_format}")

    @staticmethod
    def _get_schema_from_first_file_in_dir(source: str, storage_options: Dict[str, Any],
                                           file_format: Literal["csv", "parquet", "json", "delta"]) -> List[FlowfileColumn] | None:
        """Infers the schema by scanning the first file in a cloud directory."""
        try:
            scan_func = getattr(pl, "scan_" + file_format)
            first_file_ref = get_first_file_from_s3_dir(source, storage_options=storage_options)
            return convert_stats_to_column_info(FlowDataEngine._create_schema_stats_from_pl_schema(
                scan_func(first_file_ref, storage_options=storage_options).collect_schema()))
        except Exception as e:
            logger.warning(f"Could not read schema from first file in directory, using default schema: {e}")


    @classmethod
    def _read_iceberg_from_cloud(cls,
                                 resource_path: str,
                                 storage_options: Dict[str, Any],
                                 credential_provider: Optional[Callable],
                                 read_settings: cloud_storage_schemas.CloudStorageReadSettings) -> "FlowDataEngine":
        """Reads Iceberg table(s) from cloud storage."""
        raise NotImplementedError(f"Failed to read Iceberg table from cloud storage: Not yet implemented")

    @classmethod
    def _read_parquet_from_cloud(cls,
                                 resource_path: str,
                                 storage_options: Dict[str, Any],
                                 credential_provider: Optional[Callable],
                                 is_directory: bool) -> "FlowDataEngine":
        """Reads Parquet file(s) from cloud storage."""
        try:
            # Use scan_parquet for lazy evaluation
            if is_directory:
                resource_path = ensure_path_has_wildcard_pattern(resource_path=resource_path, file_format="parquet")
            scan_kwargs = {"source": resource_path}

            if storage_options:
                scan_kwargs["storage_options"] = storage_options

            if credential_provider:
                scan_kwargs["credential_provider"] = credential_provider
            if storage_options and is_directory:
                schema = cls._get_schema_from_first_file_in_dir(resource_path, storage_options, "parquet")
            else:
                schema = None
            lf = pl.scan_parquet(**scan_kwargs)

            return cls(
                lf,
                number_of_records=6_666_666,  # Set so the provider is not accessed for this stat
                optimize_memory=True,
                streamable=True,
                schema=schema
            )

        except Exception as e:
            logger.error(f"Failed to read Parquet from {resource_path}: {str(e)}")
            raise Exception(f"Failed to read Parquet from cloud storage: {str(e)}")

    @classmethod
    def _read_delta_from_cloud(cls,
                               resource_path: str,
                               storage_options: Dict[str, Any],
                               credential_provider: Optional[Callable],
                               read_settings: cloud_storage_schemas.CloudStorageReadSettings) -> "FlowDataEngine":
        """Reads a Delta Lake table from cloud storage."""
        try:
            logger.info("Reading Delta file from cloud storage...")
            logger.info(f"read_settings: {read_settings}")
            scan_kwargs = {"source": resource_path}
            if read_settings.delta_version:
                scan_kwargs['version'] = read_settings.delta_version
            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            if credential_provider:
                scan_kwargs["credential_provider"] = credential_provider
            lf = pl.scan_delta(**scan_kwargs)

            return cls(
                lf,
                number_of_records=6_666_666,  # Set so the provider is not accessed for this stat
                optimize_memory=True,
                streamable=True
            )
        except Exception as e:
            logger.error(f"Failed to read Delta file from {resource_path}: {str(e)}")
            raise Exception(f"Failed to read Delta file from cloud storage: {str(e)}")

    @classmethod
    def _read_csv_from_cloud(cls,
                             resource_path: str,
                             storage_options: Dict[str, Any],
                             credential_provider: Optional[Callable],
                             read_settings: cloud_storage_schemas.CloudStorageReadSettings) -> "FlowDataEngine":
        """Reads CSV file(s) from cloud storage."""
        try:
            scan_kwargs = {
                "source": resource_path,
                "has_header": read_settings.csv_has_header,
                "separator": read_settings.csv_delimiter,
                "encoding": read_settings.csv_encoding,
            }
            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            if credential_provider:
                scan_kwargs["credential_provider"] = credential_provider

            if read_settings.scan_mode == "directory":
                resource_path = ensure_path_has_wildcard_pattern(resource_path=resource_path, file_format="csv")
                scan_kwargs["source"] = resource_path
            if storage_options and read_settings.scan_mode == "directory":
                schema = cls._get_schema_from_first_file_in_dir(resource_path, storage_options, "csv")
            else:
                schema = None

            lf = pl.scan_csv(**scan_kwargs)

            return cls(
                lf,
                number_of_records=6_666_666,  # Will be calculated lazily
                optimize_memory=True,
                streamable=True,
                schema=schema
            )

        except Exception as e:
            logger.error(f"Failed to read CSV from {resource_path}: {str(e)}")
            raise Exception(f"Failed to read CSV from cloud storage: {str(e)}")

    @classmethod
    def _read_json_from_cloud(cls,
                              resource_path: str,
                              storage_options: Dict[str, Any],
                              credential_provider: Optional[Callable],
                              is_directory: bool) -> "FlowDataEngine":
        """Reads JSON file(s) from cloud storage."""
        try:
            if is_directory:
                resource_path = ensure_path_has_wildcard_pattern(resource_path, "json")
            scan_kwargs = {"source": resource_path}

            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            if credential_provider:
                scan_kwargs["credential_provider"] = credential_provider

            lf = pl.scan_ndjson(**scan_kwargs)  # Using NDJSON for line-delimited JSON

            return cls(
                lf,
                number_of_records=-1,
                optimize_memory=True,
                streamable=True,
            )

        except Exception as e:
            logger.error(f"Failed to read JSON from {resource_path}: {str(e)}")
            raise Exception(f"Failed to read JSON from cloud storage: {str(e)}")

    def _handle_path_ref(self, path_ref: str, optimize_memory: bool):
        """Handles file path reference input."""
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
        """Finalizes initialization by setting remaining attributes."""
        _ = calculate_schema_stats
        self.name = name
        self._optimize_memory = optimize_memory
        if assert_if_flowfile_schema(schema):
            self._schema = schema
            self.columns = [c.column_name for c in self._schema]
        else:
            pl_schema = self.data_frame.collect_schema()
            self._schema = self._handle_schema(schema, pl_schema)
            self.columns = [c.column_name for c in self._schema] if self._schema else pl_schema.names()

    def __getitem__(self, item):
        """Accesses a specific column or item from the DataFrame."""
        return self.data_frame.select([item])

    @property
    def data_frame(self) -> pl.LazyFrame | pl.DataFrame | None:
        """The underlying Polars DataFrame or LazyFrame.

        This property provides access to the Polars object that backs the
        FlowDataEngine. It handles lazy-loading from external sources if necessary.

        Returns:
            The active Polars `DataFrame` or `LazyFrame`.
        """
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
        """Sets the underlying Polars DataFrame or LazyFrame."""
        if self.lazy and isinstance(df, pl.DataFrame):
            raise Exception('Cannot set a non-lazy dataframe to a lazy flowfile')
        self._data_frame = df

    @staticmethod
    def _create_schema_stats_from_pl_schema(pl_schema: pl.Schema) -> List[Dict]:
        """Converts a Polars Schema into a list of schema statistics dictionaries."""
        return [
            dict(column_name=k, pl_datatype=v, col_index=i)
            for i, (k, v) in enumerate(pl_schema.items())
        ]

    def _add_schema_from_schema_stats(self, schema_stats: List[Dict]):
        """Populates the schema from a list of schema statistics dictionaries."""
        self._schema = convert_stats_to_column_info(schema_stats)

    @property
    def schema(self) -> List[FlowfileColumn]:
        """The schema of the DataFrame as a list of `FlowfileColumn` objects.

        This property lazily calculates the schema if it hasn't been determined yet.

        Returns:
            A list of `FlowfileColumn` objects describing the schema.
        """
        if self.number_of_fields == 0:
            return []
        if self._schema is None or (self._calculate_schema_stats and not self.ind_schema_calculated):
            if self._calculate_schema_stats and not self.ind_schema_calculated:
                schema_stats = self._calculate_schema()
                self.ind_schema_calculated = True
            else:
                schema_stats = self._create_schema_stats_from_pl_schema(self.data_frame.collect_schema())
            self._add_schema_from_schema_stats(schema_stats)
        return self._schema

    @property
    def number_of_fields(self) -> int:
        """The number of columns (fields) in the DataFrame.

        Returns:
            The integer count of columns.
        """
        if self.__number_of_fields is None:
            self.__number_of_fields = len(self.columns)
        return self.__number_of_fields

    def collect(self, n_records: int = None) -> pl.DataFrame:
        """Collects the data and returns it as a Polars DataFrame.

        This method triggers the execution of the lazy query plan (if applicable)
        and returns the result. It supports streaming to optimize memory usage
        for large datasets.

        Args:
            n_records: The maximum number of records to collect. If None, all
                records are collected.

        Returns:
            A Polars `DataFrame` containing the collected data.
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
        """Internal method to handle data collection logic."""
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
            return self.data_frame.head(n_records).collect(engine="streaming")
        return self.data_frame.head(n_records).collect()

    def _collect_from_external_source(self, n_records: int) -> pl.DataFrame:
        """Handles collection from an external source."""
        if self.external_source.get_pl_df() is not None:
            all_data = self.external_source.get_pl_df().head(n_records)
            self.data_frame = all_data
        else:
            all_data = self.external_source.get_sample(n_records)
            self.data_frame = pl.LazyFrame(all_data)
        return self.data_frame

    def _handle_collection_error(self, n_records: int) -> pl.DataFrame:
        """Handles errors during collection by attempting partial collection."""
        n_records = 100000000 if n_records is None else n_records
        ok_cols, error_cols = self._identify_valid_columns(n_records)

        if len(ok_cols) > 0:
            return self._create_partial_dataframe(ok_cols, error_cols, n_records)
        return self._create_empty_dataframe(n_records)

    def _identify_valid_columns(self, n_records: int) -> Tuple[List[str], List[Tuple[str, Any]]]:
        """Identifies which columns can be collected successfully."""
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
        """Creates a DataFrame with partial data for columns that could be collected."""
        df = self.data_frame.select(ok_cols)
        df = df.with_columns([
            pl.lit(None).alias(column_name).cast(data_type)
            for column_name, data_type in error_cols
        ])
        return df.select(self.columns).head(n_records).collect()

    def _create_empty_dataframe(self, n_records: int) -> pl.DataFrame:
        """Creates an empty DataFrame with the correct schema."""
        if self.number_of_records > 0:
            return pl.DataFrame({
                column_name: pl.Series(
                    name=column_name,
                    values=[None] * min(self.number_of_records, n_records)
                ).cast(data_type)
                for column_name, data_type in self.data_frame.schema.items()
            })
        return pl.DataFrame(schema=self.data_frame.schema)

    def do_group_by(self, group_by_input: transform_schemas.GroupByInput,
                    calculate_schema_stats: bool = True) -> "FlowDataEngine":
        """Performs a group-by operation on the DataFrame.

        Args:
            group_by_input: A `GroupByInput` object defining the grouping columns
                and aggregations.
            calculate_schema_stats: If True, calculates schema statistics for the
                resulting DataFrame.

        Returns:
            A new `FlowDataEngine` instance with the grouped and aggregated data.
        """
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
        """Sorts the DataFrame by one or more columns.

        Args:
            sorts: A list of `SortByInput` objects, each specifying a column
                and sort direction ('asc' or 'desc').

        Returns:
            A new `FlowDataEngine` instance with the sorted data.
        """
        if not sorts:
            return self

        descending = [s.how == 'desc' or s.how.lower() == 'descending' for s in sorts]
        df = self.data_frame.sort([sort_by.column for sort_by in sorts], descending=descending)
        return FlowDataEngine(df, number_of_records=self.number_of_records, schema=self.schema)

    def change_column_types(self, transforms: List[transform_schemas.SelectInput],
                            calculate_schema: bool = False) -> "FlowDataEngine":
        """Changes the data type of one or more columns.

        Args:
            transforms: A list of `SelectInput` objects, where each object specifies
                the column and its new `polars_type`.
            calculate_schema: If True, recalculates the schema after the type change.

        Returns:
            A new `FlowDataEngine` instance with the updated column types.
        """
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

    def save(self, path: str, data_type: str = 'parquet') -> Future:
        """Saves the DataFrame to a file in a separate thread.

        Args:
            path: The file path to save to.
            data_type: The format to save in (e.g., 'parquet', 'csv').

        Returns:
            A `loky.Future` object representing the asynchronous save operation.
        """
        estimated_size = deepcopy(self.get_estimated_file_size() * 4)
        df = deepcopy(self.data_frame)
        return write_threaded(_df=df, path=path, data_type=data_type, estimated_size=estimated_size)

    def to_pylist(self) -> List[Dict]:
        """Converts the DataFrame to a list of Python dictionaries.

        Returns:
            A list where each item is a dictionary representing a row.
        """
        if self.lazy:
            return self.data_frame.collect(engine="streaming" if self._streamable else "auto").to_dicts()
        return self.data_frame.to_dicts()

    def to_arrow(self) -> PaTable:
        """Converts the DataFrame to a PyArrow Table.

        This method triggers a `.collect()` call if the data is lazy,
        then converts the resulting eager DataFrame into a `pyarrow.Table`.

        Returns:
            A `pyarrow.Table` instance representing the data.
        """
        if self.lazy:
            return self.data_frame.collect(engine="streaming" if self._streamable else "auto").to_arrow()
        else:
            return self.data_frame.to_arrow()

    def to_raw_data(self) -> input_schema.RawData:
        """Converts the DataFrame to a `RawData` schema object.

        Returns:
            An `input_schema.RawData` object containing the schema and data.
        """
        columns = [c.get_minimal_field_info() for c in self.schema]
        data = list(self.to_dict().values())
        return input_schema.RawData(columns=columns, data=data)

    def to_dict(self) -> Dict[str, List]:
        """Converts the DataFrame to a Python dictionary of columns.

         Each key in the dictionary is a column name, and the corresponding value
         is a list of the data in that column.

         Returns:
             A dictionary mapping column names to lists of their values.
         """
        if self.lazy:
            return self.data_frame.collect(engine="streaming" if self._streamable else "auto").to_dict(as_series=False)
        else:
            return self.data_frame.to_dict(as_series=False)

    @classmethod
    def create_from_external_source(cls, external_source: ExternalDataSource) -> "FlowDataEngine":
        """Creates a FlowDataEngine from an external data source.

        Args:
            external_source: An object that conforms to the `ExternalDataSource`
                interface.

        Returns:
            A new `FlowDataEngine` instance.
        """
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
        """Creates a FlowDataEngine by executing a SQL query.

        Args:
            sql: The SQL query string to execute.
            conn: A database connection object or connection URI string.

        Returns:
            A new `FlowDataEngine` instance with the query result.
        """
        return cls(pl.read_sql(sql, conn))

    @classmethod
    def create_from_schema(cls, schema: List[FlowfileColumn]) -> "FlowDataEngine":
        """Creates an empty FlowDataEngine from a schema definition.

        Args:
            schema: A list of `FlowfileColumn` objects defining the schema.

        Returns:
            A new, empty `FlowDataEngine` instance with the specified schema.
        """
        pl_schema = []
        for i, flow_file_column in enumerate(schema):
            pl_schema.append((flow_file_column.name, cast_str_to_polars_type(flow_file_column.data_type)))
            schema[i].col_index = i
        df = pl.LazyFrame(schema=pl_schema)
        return cls(df, schema=schema, calculate_schema_stats=False, number_of_records=0)

    @classmethod
    def create_from_path(cls, received_table: input_schema.ReceivedTable) -> "FlowDataEngine":
        """Creates a FlowDataEngine from a local file path.

        Supports various file types like CSV, Parquet, and Excel.

        Args:
            received_table: A `ReceivedTableBase` object containing the file path
                and format details.

        Returns:
            A new `FlowDataEngine` instance with data from the file.
        """
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
        """Creates a FlowDataEngine with randomly generated data.

        Useful for testing and examples.

        Args:
            number_of_records: The number of random records to generate.

        Returns:
            A new `FlowDataEngine` instance with fake data.
        """
        return cls(create_fake_data(number_of_records))

    @classmethod
    def generate_enumerator(cls, length: int = 1000, output_name: str = 'output_column') -> "FlowDataEngine":
        """Generates a FlowDataEngine with a single column containing a sequence of integers.

        Args:
            length: The number of integers to generate in the sequence.
            output_name: The name of the output column.

        Returns:
            A new `FlowDataEngine` instance.
        """
        if length > 10_000_000:
            length = 10_000_000
        return cls(pl.LazyFrame().select((pl.int_range(0, length, dtype=pl.UInt32)).alias(output_name)))

    def _handle_schema(self, schema: List[FlowfileColumn] | List[str] | pl.Schema | None,
                       pl_schema: pl.Schema) -> List[FlowfileColumn] | None:
        """Handles schema processing and validation during initialization."""
        if schema is None and pl_schema is not None:
            return convert_stats_to_column_info(self._create_schema_stats_from_pl_schema(pl_schema))
        elif schema is None and pl_schema is None:
            return None
        elif assert_if_flowfile_schema(schema) and pl_schema is None:
            return schema
        elif pl_schema is not None and schema is not None:
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
        """Handles Polars schema conversion."""
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
        """Handles string-based schema conversion."""
        flow_file_columns = [
            FlowfileColumn.create_from_polars_dtype(column_name=col_name, data_type=dtype)
            for col_name, dtype in zip(schema, pl_schema.dtypes())
        ]

        self.data_frame = self.data_frame.rename({
            o: n for o, n in zip(pl_schema.names(), schema)
        })

        return flow_file_columns

    def split(self, split_input: transform_schemas.TextToRowsInput) -> "FlowDataEngine":
        """Splits a column's text values into multiple rows based on a delimiter.

        This operation is often referred to as "exploding" the DataFrame, as it
        increases the number of rows.

        Args:
            split_input: A `TextToRowsInput` object specifying the column to split,
                the delimiter, and the output column name.

        Returns:
            A new `FlowDataEngine` instance with the exploded rows.
        """
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
        """Converts the DataFrame from a wide to a long format.

        This is the inverse of a pivot operation, taking columns and transforming
        them into `variable` and `value` rows.

        Args:
            unpivot_input: An `UnpivotInput` object specifying which columns to
                unpivot and which to keep as index columns.

        Returns:
            A new, unpivoted `FlowDataEngine` instance.
        """
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
        """Converts the DataFrame from a long to a wide format, aggregating values.

        Args:
            pivot_input: A `PivotInput` object defining the index, pivot, and value
                columns, along with the aggregation logic.
            node_logger: An optional logger for reporting warnings, e.g., if the
                pivot column has too many unique values.

        Returns:
            A new, pivoted `FlowDataEngine` instance.
        """
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
        """Filters rows based on a predicate expression.

        Args:
            predicate: A string containing a Polars expression that evaluates to
                a boolean value.

        Returns:
            A new `FlowDataEngine` instance containing only the rows that match
            the predicate.
        """
        try:
            f = to_expr(predicate)
        except Exception as e:
            logger.warning(f'Error in filter expression: {e}')
            f = to_expr("False")
        df = self.data_frame.filter(f)
        return FlowDataEngine(df, schema=self.schema, streamable=self._streamable)

    def add_record_id(self, record_id_settings: transform_schemas.RecordIdInput) -> "FlowDataEngine":
        """Adds a record ID (row number) column to the DataFrame.

        Can generate a simple sequential ID or a grouped ID that resets for
        each group.

        Args:
            record_id_settings: A `RecordIdInput` object specifying the output
                column name, offset, and optional grouping columns.

        Returns:
            A new `FlowDataEngine` instance with the added record ID column.
        """
        if record_id_settings.group_by and len(record_id_settings.group_by_columns) > 0:
            return self._add_grouped_record_id(record_id_settings)
        return self._add_simple_record_id(record_id_settings)

    def _add_grouped_record_id(self, record_id_settings: transform_schemas.RecordIdInput) -> "FlowDataEngine":
        """Adds a record ID column with grouping."""
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
        """Adds a simple sequential record ID column."""
        df = self.data_frame.with_row_index(
            record_id_settings.output_column_name,
            record_id_settings.offset
        )

        output_schema = [FlowfileColumn.from_input(record_id_settings.output_column_name, 'UInt64')]
        output_schema.extend(self.schema)

        return FlowDataEngine(df, schema=output_schema)

    def get_schema_column(self, col_name: str) -> FlowfileColumn:
        """Retrieves the schema information for a single column by its name.

        Args:
            col_name: The name of the column to retrieve.

        Returns:
            A `FlowfileColumn` object for the specified column, or `None` if not found.
        """
        for s in self.schema:
            if s.name == col_name:
                return s

    def get_estimated_file_size(self) -> int:
        """Estimates the file size in bytes if the data originated from a local file.

        This relies on the original path being tracked during file ingestion.

        Returns:
            The file size in bytes, or 0 if the original path is unknown.
        """
        if self._org_path is not None:
            return os.path.getsize(self._org_path)
        return 0

    def __repr__(self) -> str:
        """Returns a string representation of the FlowDataEngine."""
        return f'flow data engine\n{self.data_frame.__repr__()}'

    def __call__(self) -> "FlowDataEngine":
        """Makes the class instance callable, returning itself."""
        return self

    def __len__(self) -> int:
        """Returns the number of records in the table."""
        return self.number_of_records if self.number_of_records >= 0 else self.get_number_of_records()

    def cache(self) -> "FlowDataEngine":
        """Caches the current DataFrame to disk and updates the internal reference.

        This triggers a background process to write the current LazyFrame's result
        to a temporary file. Subsequent operations on this `FlowDataEngine` instance
        will read from the cached file, which can speed up downstream computations.

        Returns:
            The same `FlowDataEngine` instance, now backed by the cached data.
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
        """Materializes data from a tracked external source.

        If the `FlowDataEngine` was created from an `ExternalDataSource`, this
        method will trigger the data retrieval, update the internal `_data_frame`
        to a `LazyFrame` of the collected data, and reset the schema to be
        re-evaluated.
        """
        if self._external_source is not None:
            logger.info('Collecting external source')
            if self.external_source.get_pl_df() is not None:
                self.data_frame = self.external_source.get_pl_df().lazy()
            else:
                self.data_frame = pl.LazyFrame(list(self.external_source.get_iter()))
            self._schema = None  # enforce reset schema

    def get_output_sample(self, n_rows: int = 10) -> List[Dict]:
        """Gets a sample of the data as a list of dictionaries.

        This is typically used to display a preview of the data in a UI.

        Args:
            n_rows: The number of rows to sample.

        Returns:
            A list of dictionaries, where each dictionary represents a row.
        """
        if self.number_of_records > n_rows or self.number_of_records < 0:
            df = self.collect(n_rows)
        else:
            df = self.collect()
        return df.to_dicts()

    def __get_sample__(self, n_rows: int = 100, streamable: bool = True) -> "FlowDataEngine":
        """Internal method to get a sample of the data."""
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
                   seed: int = None, execution_location: Optional[ExecutionLocationsLiteral] = None) -> "FlowDataEngine":
        """Gets a sample of rows from the DataFrame.

        Args:
            n_rows: The number of rows to sample.
            random: If True, performs random sampling. If False, takes the first n_rows.
            shuffle: If True (and `random` is True), shuffles the data before sampling.
            seed: A random seed for reproducibility.
            execution_location: Location which is used to calculate the size of the dataframe
        Returns:
            A new `FlowDataEngine` instance containing the sampled data.
        """
        logging.info(f'Getting sample of {n_rows} rows')
        if random:
            if self.lazy and self.external_source is not None:
                self.collect_external()

            if self.lazy and shuffle:
                sample_df = (self.data_frame.collect(engine="streaming" if self._streamable else "auto")
                             .sample(n_rows, seed=seed, shuffle=shuffle))
            elif shuffle:
                sample_df = self.data_frame.sample(n_rows, seed=seed, shuffle=shuffle)
            else:
                if execution_location is None:
                    execution_location = get_global_execution_location()
                n_rows = min(n_rows, self.get_number_of_records(
                    calculate_in_worker_process=execution_location == "remote")
                             )

                every_n_records = ceil(self.number_of_records / n_rows)
                sample_df = self.data_frame.gather_every(every_n_records)
        else:
            if self.external_source:
                self.collect(n_rows)
            sample_df = self.data_frame.head(n_rows)

        return FlowDataEngine(sample_df, schema=self.schema)

    def get_subset(self, n_rows: int = 100) -> "FlowDataEngine":
        """Gets the first `n_rows` from the DataFrame.

        Args:
            n_rows: The number of rows to include in the subset.

        Returns:
            A new `FlowDataEngine` instance containing the subset of data.
        """
        if not self.lazy:
            return FlowDataEngine(self.data_frame.head(n_rows), calculate_schema_stats=True)
        else:
            return FlowDataEngine(self.data_frame.head(n_rows), calculate_schema_stats=True)

    def iter_batches(self, batch_size: int = 1000,
                     columns: Union[List, Tuple, str] = None) -> Generator["FlowDataEngine", None, None]:
        """Iterates over the DataFrame in batches.

        Args:
            batch_size: The size of each batch.
            columns: A list of column names to include in the batches. If None,
                all columns are included.

        Yields:
            A `FlowDataEngine` instance for each batch.
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
        """Starts a fuzzy join operation in a background process.

        This method prepares the data and initiates the fuzzy matching in a
        separate process, returning a tracker object immediately.

        Args:
            fuzzy_match_input: A `FuzzyMatchInput` object with the matching parameters.
            other: The right `FlowDataEngine` to join with.
            file_ref: A reference string for temporary files.
            flow_id: The flow ID for tracking.
            node_id: The node ID for tracking.

        Returns:
            An `ExternalFuzzyMatchFetcher` object that can be used to track the
            progress and retrieve the result of the fuzzy join.
        """
        fuzzy_match_input_manager = transform_schemas.FuzzyMatchInputManager(fuzzy_match_input)
        left_df, right_df = prepare_for_fuzzy_match(left=self, right=other,
                                                    fuzzy_match_input_manager=fuzzy_match_input_manager)

        return ExternalFuzzyMatchFetcher(left_df, right_df,
                                         fuzzy_maps=fuzzy_match_input_manager.fuzzy_maps,
                                         file_ref=file_ref + '_fm',
                                         wait_on_completion=False,
                                         flow_id=flow_id,
                                         node_id=node_id)

    def fuzzy_join_external(self,
                            fuzzy_match_input: transform_schemas.FuzzyMatchInput,
                            other: "FlowDataEngine",
                            file_ref: str = None,
                            flow_id: int = -1,
                            node_id: int = -1
                            ):
        if file_ref is None:
            file_ref = str(id(self)) + '_' + str(id(other))
        fuzzy_match_input_manager = transform_schemas.FuzzyMatchInputManager(fuzzy_match_input)

        left_df, right_df = prepare_for_fuzzy_match(left=self, right=other,
                                                    fuzzy_match_input_manager=fuzzy_match_input_manager)
        external_tracker = ExternalFuzzyMatchFetcher(left_df, right_df,
                                                     fuzzy_maps=fuzzy_match_input_manager.fuzzy_maps,
                                                     file_ref=file_ref + '_fm',
                                                     wait_on_completion=False,
                                                     flow_id=flow_id,
                                                     node_id=node_id)
        return FlowDataEngine(external_tracker.get_result())

    def fuzzy_join(self, fuzzy_match_input: transform_schemas.FuzzyMatchInput,
                   other: "FlowDataEngine",
                   node_logger: NodeLogger = None) -> "FlowDataEngine":
        fuzzy_match_input_manager = transform_schemas.FuzzyMatchInputManager(fuzzy_match_input)
        left_df, right_df = prepare_for_fuzzy_match(left=self, right=other,
                                                    fuzzy_match_input_manager=fuzzy_match_input_manager)
        fuzzy_mappings = [FuzzyMapping(**fm.__dict__) for fm in fuzzy_match_input_manager.fuzzy_maps]
        return FlowDataEngine(fuzzy_match_dfs(left_df, right_df, fuzzy_maps=fuzzy_mappings,
                                              logger=node_logger.logger if node_logger else logger)
                              .lazy())

    def do_cross_join(self, cross_join_input: transform_schemas.CrossJoinInput,
                      auto_generate_selection: bool, verify_integrity: bool,
                      other: "FlowDataEngine") -> "FlowDataEngine":
        """Performs a cross join with another DataFrame.

        A cross join produces the Cartesian product of the two DataFrames.

        Args:
            cross_join_input: A `CrossJoinInput` object specifying column selections.
            auto_generate_selection: If True, automatically renames columns to avoid conflicts.
            verify_integrity: If True, checks if the resulting join would be too large.
            other: The right `FlowDataEngine` to join with.

        Returns:
            A new `FlowDataEngine` with the result of the cross join.

        Raises:
            Exception: If `verify_integrity` is True and the join would result in
                an excessively large number of records.
        """
        self.lazy = True
        other.lazy = True
        cross_join_input_manager = transform_schemas.CrossJoinInputManager(cross_join_input)
        verify_join_select_integrity(cross_join_input_manager.input, left_columns=self.columns, right_columns=other.columns)
        right_select = [v.old_name for v in cross_join_input_manager.right_select.renames
                        if (v.keep or v.join_key) and v.is_available]
        left_select = [v.old_name for v in cross_join_input_manager.left_select.renames
                       if (v.keep or v.join_key) and v.is_available]
        cross_join_input_manager.auto_rename(rename_mode="suffix")
        left = self.data_frame.select(left_select).rename(cross_join_input_manager.left_select.rename_table)
        right = other.data_frame.select(right_select).rename(cross_join_input_manager.right_select.rename_table)

        joined_df = left.join(right, how='cross')

        cols_to_delete_after = [col.new_name for col in
                                cross_join_input_manager.left_select.renames + cross_join_input_manager.left_select.renames
                                if col.join_key and not col.keep and col.is_available]

        fl = FlowDataEngine(joined_df.drop(cols_to_delete_after), calculate_schema_stats=False, streamable=False)
        return fl

    def join(self, join_input: transform_schemas.JoinInput, auto_generate_selection: bool,
             verify_integrity: bool, other: "FlowDataEngine") -> "FlowDataEngine":
        """Performs a standard SQL-style join with another DataFrame."""
        # Create manager from input
        join_manager = transform_schemas.JoinInputManager(join_input)
        ensure_right_unselect_for_semi_and_anti_joins(join_manager.input)
        for jk in join_manager.join_mapping:
            if jk.left_col not in {c.old_name for c in join_manager.left_select.renames}:
                join_manager.left_select.append(transform_schemas.SelectInput(jk.left_col, keep=False))
            if jk.right_col not in {c.old_name for c in join_manager.right_select.renames}:
                join_manager.right_select.append(transform_schemas.SelectInput(jk.right_col, keep=False))
        verify_join_select_integrity(join_manager.input, left_columns=self.columns, right_columns=other.columns)
        if not verify_join_map_integrity(join_manager.input, left_columns=self.schema, right_columns=other.schema):
            raise Exception('Join is not valid by the data fields')

        if auto_generate_selection:
            join_manager.auto_rename()

        # Use manager properties throughout
        left = self.data_frame.select(join_manager.left_manager.get_select_cols()).rename(join_manager.left_manager.get_rename_table())
        right = other.data_frame.select(join_manager.right_manager.get_select_cols()).rename(join_manager.right_manager.get_rename_table())

        left, right, reverse_join_key_mapping = _handle_duplication_join_keys(left, right, join_manager)
        left, right = rename_df_table_for_join(left, right, join_manager.get_join_key_renames())
        if join_manager.how == 'right':
            joined_df = right.join(
                other=left,
                left_on=join_manager.right_join_keys,
                right_on=join_manager.left_join_keys,
                how="left",
                suffix="").rename(reverse_join_key_mapping)
        else:
            joined_df = left.join(
                other=right,
                left_on=join_manager.left_join_keys,
                right_on=join_manager.right_join_keys,
                how=join_manager.how,
                suffix="").rename(reverse_join_key_mapping)

        left_cols_to_delete_after = [get_col_name_to_delete(col, 'left')
                                     for col in join_manager.input.left_select.renames
                                     if not col.keep and col.is_available and col.join_key]

        right_cols_to_delete_after = [get_col_name_to_delete(col, 'right')
                                      for col in join_manager.input.right_select.renames
                                      if not col.keep and col.is_available and col.join_key
                                      and join_manager.how in ("left", "right", "inner", "cross", "outer")]

        if len(right_cols_to_delete_after + left_cols_to_delete_after) > 0:
            joined_df = joined_df.drop(left_cols_to_delete_after + right_cols_to_delete_after)

        undo_join_key_remapping = get_undo_rename_mapping_join(join_manager)
        joined_df = joined_df.rename(undo_join_key_remapping)

        return FlowDataEngine(joined_df, calculate_schema_stats=False,
                              number_of_records=0, streamable=False)

    def solve_graph(self, graph_solver_input: transform_schemas.GraphSolverInput) -> "FlowDataEngine":
        """Solves a graph problem represented by 'from' and 'to' columns.

        This is used for operations like finding connected components in a graph.

        Args:
            graph_solver_input: A `GraphSolverInput` object defining the source,
                destination, and output column names.

        Returns:
            A new `FlowDataEngine` instance with the solved graph data.
        """
        lf = self.data_frame.with_columns(
            graph_solver(graph_solver_input.col_from, graph_solver_input.col_to)
            .alias(graph_solver_input.output_column_name)
        )
        return FlowDataEngine(lf)

    def add_new_values(self, values: Iterable, col_name: str = None) -> "FlowDataEngine":
        """Adds a new column with the provided values.

        Args:
            values: An iterable (e.g., list, tuple) of values to add as a new column.
            col_name: The name for the new column. Defaults to 'new_values'.

        Returns:
            A new `FlowDataEngine` instance with the added column.
        """
        if col_name is None:
            col_name = 'new_values'
        return FlowDataEngine(self.data_frame.with_columns(pl.Series(values).alias(col_name)))

    def get_record_count(self) -> "FlowDataEngine":
        """Returns a new FlowDataEngine with a single column 'number_of_records'
        containing the total number of records.

        Returns:
            A new `FlowDataEngine` instance.
        """
        return FlowDataEngine(self.data_frame.select(pl.len().alias('number_of_records')))

    def assert_equal(self, other: "FlowDataEngine", ordered: bool = True, strict_schema: bool = False):
        """Asserts that this DataFrame is equal to another.

        Useful for testing.

        Args:
            other: The other `FlowDataEngine` to compare with.
            ordered: If True, the row order must be identical.
            strict_schema: If True, the data types of the schemas must be identical.

        Raises:
            Exception: If the DataFrames are not equal based on the specified criteria.
        """
        org_laziness = self.lazy, other.lazy
        self.lazy = False
        other.lazy = False
        self.number_of_records = -1
        other.number_of_records = -1
        other = other.select_columns(self.columns)

        if self.get_number_of_records_in_process() != other.get_number_of_records_in_process():
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

    def initialize_empty_fl(self):
        """Initializes an empty LazyFrame."""
        self.data_frame = pl.LazyFrame()
        self.number_of_records = 0
        self._lazy = True

    def _calculate_number_of_records_in_worker(self) -> int:
        """Calculates the number of records in a worker process."""
        number_of_records = ExternalDfFetcher(
            lf=self.data_frame,
            operation_type="calculate_number_of_records",
            flow_id=-1,
            node_id=-1,
            wait_on_completion=True
        ).result
        return number_of_records

    def get_number_of_records_in_process(self, force_calculate: bool = False):
        """
        Get the number of records in the DataFrame in the local process.

        args:
            force_calculate: If True, forces recalculation even if a value is cached.

        Returns:
            The total number of records.
        """
        return self.get_number_of_records(force_calculate=force_calculate)

    def get_number_of_records(self, warn: bool = False, force_calculate: bool = False,
                              calculate_in_worker_process: bool = False) -> int:
        """Gets the total number of records in the DataFrame.

        For lazy frames, this may trigger a full data scan, which can be expensive.

        Args:
            warn: If True, logs a warning if a potentially expensive calculation is triggered.
            force_calculate: If True, forces recalculation even if a value is cached.
            calculate_in_worker_process: If True, offloads the calculation to a worker process.

        Returns:
            The total number of records.

        Raises:
            ValueError: If the number of records could not be determined.
        """
        if self.is_future and not self.is_collected:
            return -1
        if self.number_of_records is None or self.number_of_records < 0 or force_calculate:
            if self._number_of_records_callback is not None:
                self._number_of_records_callback(self)

            if self.lazy:
                if calculate_in_worker_process:
                    try:
                        self.number_of_records = self._calculate_number_of_records_in_worker()
                        return self.number_of_records
                    except Exception as e:
                        logger.error(f"Error: {e}")
                if warn:
                    logger.warning('Calculating the number of records this can be expensive on a lazy frame')
                try:
                    self.number_of_records = self.data_frame.select(pl.len()).collect(
                        engine="streaming" if self._streamable else "auto")[0, 0]
                except Exception:
                    raise ValueError('Could not get number of records')
            else:
                self.number_of_records = self.data_frame.__len__()
        return self.number_of_records

    @property
    def has_errors(self) -> bool:
        """Checks if there are any errors."""
        return len(self.errors) > 0

    @property
    def lazy(self) -> bool:
        """Indicates if the DataFrame is in lazy mode."""
        return self._lazy

    @lazy.setter
    def lazy(self, exec_lazy: bool = False):
        """Sets the laziness of the DataFrame.

        Args:
            exec_lazy: If True, converts the DataFrame to a LazyFrame. If False,
                collects the data and converts it to an eager DataFrame.
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
        """The external data source, if any."""
        return self._external_source

    @property
    def cols_idx(self) -> Dict[str, int]:
        """A dictionary mapping column names to their integer index."""
        if self._col_idx is None:
            self._col_idx = {c: i for i, c in enumerate(self.columns)}
        return self._col_idx

    @property
    def __name__(self) -> str:
        """The name of the table."""
        return self.name

    def get_select_inputs(self) -> transform_schemas.SelectInputs:
        """Gets `SelectInput` specifications for all columns in the current schema.

        Returns:
            A `SelectInputs` object that can be used to configure selection or
            transformation operations.
        """
        return transform_schemas.SelectInputs(
            [transform_schemas.SelectInput(old_name=c.name, data_type=c.data_type) for c in self.schema]
        )

    def select_columns(self, list_select: Union[List[str], Tuple[str], str]) -> "FlowDataEngine":
        """Selects a subset of columns from the DataFrame.

        Args:
            list_select: A list, tuple, or single string of column names to select.

        Returns:
            A new `FlowDataEngine` instance containing only the selected columns.
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
        """Drops specified columns from the DataFrame.

        Args:
            columns: A list of column names to drop.

        Returns:
            A new `FlowDataEngine` instance without the dropped columns.
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
        """Reorganizes columns into a specified order.

        Args:
            column_order: A list of column names in the desired order.

        Returns:
            A new `FlowDataEngine` instance with the columns reordered.
        """
        df = self.data_frame.select(column_order)
        schema = sorted(self.schema, key=lambda x: column_order.index(x.column_name))
        return FlowDataEngine(df, schema=schema, number_of_records=self.number_of_records)

    def apply_flowfile_formula(self, func: str, col_name: str,
                               output_data_type: pl.DataType = None) -> "FlowDataEngine":
        """Applies a formula to create a new column or transform an existing one.

        Args:
            func: A string containing a Polars expression formula.
            col_name: The name of the new or transformed column.
            output_data_type: The desired Polars data type for the output column.

        Returns:
            A new `FlowDataEngine` instance with the applied formula.
        """
        parsed_func = to_expr(func)
        if output_data_type is not None:
            df2 = self.data_frame.with_columns(parsed_func.cast(output_data_type).alias(col_name))
        else:
            df2 = self.data_frame.with_columns(parsed_func.alias(col_name))

        return FlowDataEngine(df2, number_of_records=self.number_of_records)

    def apply_sql_formula(self, func: str, col_name: str,
                          output_data_type: pl.DataType = None) -> "FlowDataEngine":
        """Applies an SQL-style formula using `pl.sql_expr`.

        Args:
            func: A string containing an SQL expression.
            col_name: The name of the new or transformed column.
            output_data_type: The desired Polars data type for the output column.

        Returns:
            A new `FlowDataEngine` instance with the applied formula.
        """
        expr = to_expr(func)
        if output_data_type not in (None, transform_schemas.AUTO_DATA_TYPE):
            df = self.data_frame.with_columns(expr.cast(output_data_type).alias(col_name))
        else:
            df = self.data_frame.with_columns(expr.alias(col_name))

        return FlowDataEngine(df, number_of_records=self.number_of_records)

    def output(self, output_fs: input_schema.OutputSettings, flow_id: int, node_id: int | str,
               execute_remote: bool = True) -> "FlowDataEngine":
        """Writes the DataFrame to an output file.

        Can execute the write operation locally or in a remote worker process.

        Args:
            output_fs: An `OutputSettings` object with details about the output file.
            flow_id: The flow ID for tracking.
            node_id: The node ID for tracking.
            execute_remote: If True, executes the write in a worker process.

        Returns:
            The same `FlowDataEngine` instance for chaining.
        """
        logger.info('Starting to write output')
        if execute_remote:
            status = utils.write_output(
                self.data_frame,
                data_type=output_fs.file_type,
                path=output_fs.abs_file_path,
                write_mode=output_fs.write_mode,
                sheet_name=output_fs.sheet_name,
                delimiter=output_fs.delimiter,
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
                sheet_name=output_fs.sheet_name,
                delimiter=output_fs.delimiter,
                flow_id=flow_id,
                node_id=node_id,
            )
            logger.info("Finished writing output")
        return self

    def make_unique(self, unique_input: transform_schemas.UniqueInput = None) -> "FlowDataEngine":
        """Gets the unique rows from the DataFrame.

        Args:
            unique_input: A `UniqueInput` object specifying a subset of columns
                to consider for uniqueness and a strategy for keeping rows.

        Returns:
            A new `FlowDataEngine` instance with unique rows.
        """
        if unique_input is None or unique_input.columns is None:
            return FlowDataEngine(self.data_frame.unique())
        return FlowDataEngine(self.data_frame.unique(unique_input.columns, keep=unique_input.strategy))

    def concat(self, other: Iterable["FlowDataEngine"] | "FlowDataEngine") -> "FlowDataEngine":
        """Concatenates this DataFrame with one or more other DataFrames.

        Args:
            other: A single `FlowDataEngine` or an iterable of them.

        Returns:
            A new `FlowDataEngine` containing the concatenated data.
        """
        if isinstance(other, FlowDataEngine):
            other = [other]

        dfs: List[pl.LazyFrame] | List[pl.DataFrame] = [self.data_frame] + [flt.data_frame for flt in other]
        return FlowDataEngine(pl.concat(dfs, how='diagonal_relaxed'))

    def do_select(self, select_inputs: transform_schemas.SelectInputs,
                  keep_missing: bool = True) -> "FlowDataEngine":
        """Performs a complex column selection, renaming, and reordering operation.

        Args:
            select_inputs: A `SelectInputs` object defining the desired transformations.
            keep_missing: If True, columns not specified in `select_inputs` are kept.
                If False, they are dropped.

        Returns:
            A new `FlowDataEngine` with the transformed selection.
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

    def set_streamable(self, streamable: bool = False):
        """Sets whether DataFrame operations should be streamable."""
        self._streamable = streamable

    def _calculate_schema(self) -> List[Dict]:
        """Calculates schema statistics."""
        if self.external_source is not None:

            self.collect_external()
        v = utils.calculate_schema(self.data_frame)
        return v

    def calculate_schema(self):
        """Calculates and returns the schema."""
        self._calculate_schema_stats = True
        return self.schema

    def count(self) -> int:
        """Gets the total number of records."""
        return self.get_number_of_records()

    @classmethod
    def create_from_path_worker(cls, received_table: input_schema.ReceivedTable, flow_id: int, node_id: int | str):
        """Creates a FlowDataEngine from a path in a worker process."""
        received_table.set_absolute_filepath()

        external_fetcher = ExternalCreateFetcher(received_table=received_table,
                                                 file_type=received_table.file_type, flow_id=flow_id, node_id=node_id)
        return cls(external_fetcher.get_result())


def execute_polars_code(*flowfile_tables: "FlowDataEngine", code: str) -> "FlowDataEngine":
    """Executes arbitrary Polars code on one or more FlowDataEngine objects.

    This function takes a string of Python code that uses Polars and executes it.
    Input `FlowDataEngine` objects are made available in the code's scope as
    `input_df` (for a single input) or `input_df_1`, `input_df_2`, etc.

    Args:
        *flowfile_tables: A variable number of `FlowDataEngine` objects to be
            used as input to the code.
        code: A string containing the Polars code to execute.

    Returns:
        A new `FlowDataEngine` instance containing the result of the executed code.
    """
    polars_executable = polars_code_parser.get_executable(code, num_inputs=len(flowfile_tables))
    if len(flowfile_tables) == 0:
        kwargs = {}
    elif len(flowfile_tables) == 1:
        kwargs = {'input_df': flowfile_tables[0].data_frame}
    else:
        kwargs = {f'input_df_{i+1}': flowfile_table.data_frame for i, flowfile_table in enumerate(flowfile_tables)}
    df = polars_executable(**kwargs)
    if isinstance(df, pl.DataFrame):
        logger.warning("Got a non lazy DataFrame, possibly harming performance, if possible, try to use a lazy method")
    return FlowDataEngine(df)