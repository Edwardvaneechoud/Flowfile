import logging

import polars as pl
import os

from polars_grouper import graph_solver

from flowfile_core.flowfile.flowfile_table import utils
from flowfile_core.flowfile.flowfile_table.threaded_processes import write_threaded, get_join_count
from typing import Dict, List, Tuple, Union, Any, Optional, Callable, Iterable
from dataclasses import dataclass
from pyarrow.parquet import ParquetFile
from math import ceil

from copy import deepcopy
from flowfile_core.schemas import transform_schema as transform_schemas
from flowfile_core.schemas import input_schema
from flowfile_core.configs import logger
from flowfile_core.flowfile.flowfile_table.flow_file_column.main import FlowfileColumn, convert_stats_to_column_info
from flowfile_core.flowfile.flowfile_table.flow_file_column.utils import type_to_polars
from polars_expr_transformer import simple_function_to_expr as to_expr
from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource
from flowfile_core.flowfile.flowfile_table.join import verify_join_select_integrity, verify_join_map_integrity
from flowfile_core.flowfile.flowfile_table.fuzzy_matching.prepare_for_fuzzy_match import prepare_for_fuzzy_match
from flowfile_core.flowfile.flowfile_table.subprocess_operations.subprocess_operations import ExternalDfFetcher, \
    ExternalExecutorTracker, ExternalFuzzyMatchFetcher, ExternalCreateFetcher, fetch_unique_values
from flowfile_core.flowfile.flowfile_table.create import funcs as create_funcs
from flowfile_core.flowfile.flowfile_table.sample_data import create_fake_data
from loky import Future
from flowfile_core.flowfile.flowfile_table.polars_code_parser import polars_code_parser


@dataclass
class FlowfileTable:
    # class to mimic the behaviour of the ParquetFile
    _data_frame: Union[pl.DataFrame, pl.LazyFrame]
    columns: List[Any]
    errors: List = None
    _schema: List[FlowfileColumn] = None
    number_of_records: int = None
    name: str = None
    __col_name_idx_map: Dict = None
    __data_map: Dict = None
    _optimize_memory: bool = False
    __optimized_columns: List = None
    __sample__: str = None
    __number_of_fields: int = None
    sorted_by: int = None
    _lazy: bool = None
    _col_idx: Dict[str, int] = None
    _calculate_schema_stats: bool = False
    _org_path: Optional[str] = None
    _streamable: bool = True
    _future: Future = None
    is_future: bool = False
    is_collected: bool = True
    _number_of_records_callback: Callable = None
    _data_callback: Callable = None
    _external_source: ExternalDataSource = None
    ind_schema_calculated: bool = False

    def __init__(self, raw_data: Union[List[Dict], List[Any], ParquetFile, pl.DataFrame, pl.LazyFrame] = None,
                 path_ref: str = None,
                 name: str = None,
                 optimize_memory: bool = True,
                 schema: List[FlowfileColumn] | List[str] | pl.Schema = None,
                 number_of_records: int = None,
                 calculate_schema_stats: bool = False,
                 streamable: bool = True,
                 number_of_records_callback: Callable = None,
                 data_callback: Callable = None):
        self._external_source = None
        self._number_of_records_callback = number_of_records_callback
        self._data_callback = data_callback
        self.ind_schema_calculated = False
        self._streamable = streamable
        self._org_path = None
        self._lazy = False
        self.errors = []
        self._calculate_schema_stats = calculate_schema_stats
        self.collected = True
        self.is_future = False
        if raw_data is not None:
            if isinstance(raw_data, pl.DataFrame):
                self.data_frame = raw_data
                if number_of_records is not None:
                    self.number_of_records = number_of_records
                else:
                    self.number_of_records = raw_data.select(pl.len())[0, 0]

            elif isinstance(raw_data, pl.LazyFrame):
                self.data_frame = raw_data
                self._lazy = True
                if number_of_records is not None:
                    self.number_of_records = number_of_records
                elif optimize_memory:
                    self.number_of_records = -1
                else:
                    self.number_of_records = raw_data.select(pl.len()).collect()[0, 0]

            elif isinstance(raw_data, list):
                number_of_records = len(raw_data)
                if number_of_records > 0:
                    raw_data = self._handle_list_input(raw_data)
                    self.number_of_records = number_of_records
                    self.data_frame = pl.DataFrame(raw_data)
                    self.lazy = True
                else:
                    self.initialize_empty_fl()
                    self.number_of_records = 0
            elif isinstance(raw_data, dict):
                number_of_records = 1 if len(raw_data) > 0 else 0
                if number_of_records > 0:
                    self.number_of_records = number_of_records
                    self.data_frame = pl.DataFrame([raw_data])
                else:
                    self.initialize_empty_fl()

        elif path_ref:
            try:
                pf = ParquetFile(path_ref)
            except Exception as e:
                raise Exception("Provided ref is not a parquet file")
            self.number_of_records = pf.metadata.num_rows
            if optimize_memory:
                self._lazy = True
                self.data_frame = pl.scan_parquet(path_ref)
            else:
                self.data_frame = pl.read_parquet(path_ref)

        else:
            self.initialize_empty_fl()
        self.name = name
        self._optimize_memory = optimize_memory
        pl_schema = self.data_frame.collect_schema()
        self._schema = self._handle_schema(schema, pl_schema)
        self.columns = [c.column_name for c in self._schema] if self._schema else pl_schema.names()

    def _handle_schema(self, schema: List[FlowfileColumn] | List[str] | pl.Schema,
                       pl_schema: pl.Schema) -> List[FlowfileColumn] | None:
        if schema is None:
            return None
        if schema.__len__() != pl_schema.__len__():
            raise Exception(
                f'Schema does not match the data got {schema.__len__()} columns expected {pl_schema.__len__()}')
        if isinstance(schema, pl.Schema):
            flow_file_columns = [FlowfileColumn.create_from_polars_dtype(column_name=col_name, data_type=dtype)
                                 for col_name, dtype in zip(schema.names(), schema.dtypes())]
            select_arg = [pl.col(o).alias(n).cast(schema_dtype)
                          for o, n, schema_dtype in zip(pl_schema.names(), schema.names(), schema.dtypes())]
            self.data_frame = self.data_frame.select(select_arg)

            return flow_file_columns
        elif isinstance(schema, list) and len(schema) == 0:
            return []
        elif isinstance(schema[0], str):
            flow_file_columns = [FlowfileColumn.create_from_polars_dtype(column_name=col_name, data_type=dtype)
                                 for col_name, dtype in zip(schema, pl_schema.dtypes())]
            self.data_frame = self.data_frame.rename({o: n for o, n in zip(pl_schema.names(), schema)})
            return flow_file_columns
        return schema

    def get_estimated_file_size(self) -> int:
        if self._org_path is not None:
            return os.path.getsize(self._org_path)
        else:
            return 0

    @classmethod
    def create_from_external_source(cls, external_source: ExternalDataSource):
        if external_source.schema is not None:
            ff = cls.create_from_schema(external_source.schema)
        elif external_source.initial_data_getter is not None:
            ff = cls(raw_data=external_source.initial_data_getter())
        else:
            ff = cls()
        ff._external_source = external_source
        return ff

    @classmethod
    def create_from_schema(cls, schema: List[FlowfileColumn]):
        pl_schema = []
        for i, flow_file_column in enumerate(schema):
            pl_schema.append((flow_file_column.name, type_to_polars(flow_file_column.data_type)))
            schema[i].col_index = i
        df = pl.LazyFrame(schema=pl_schema)
        return cls(df, schema=schema, calculate_schema_stats=False, number_of_records=0)

    def __repr__(self):
        return self.data_frame.__repr__()

    def output(self, output_fs: input_schema.OutputSettings):
        logger.info('Starting to write output')
        status = utils.write_output(self.data_frame, data_type=output_fs.file_type,
                                    path=output_fs.directory + os.sep + output_fs.name,
                                    write_mode=output_fs.write_mode,
                                    sheet_name=output_fs.output_excel_table.sheet_name,
                                    delimiter=output_fs.output_csv_table.delimiter)
        tracker = ExternalExecutorTracker(status)
        tracker.get_result()
        logger.info('Finished writing output')
        return self

    @property
    def data_frame(self) -> pl.LazyFrame | pl.DataFrame:
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
        if self.lazy and isinstance(df, pl.DataFrame):
            raise Exception('Cannot set a non-lazy dataframe to a lazy flowfile')
        self._data_frame = df

    @classmethod
    def create_from_path(cls, received_table: input_schema.ReceivedTableBase):
        received_table.set_absolute_filepath()
        if received_table.file_type == 'csv':
            flow_file = cls(create_funcs.create_from_path_csv(received_table))
        elif received_table.file_type == 'parquet':
            flow_file = cls(create_funcs.create_from_path_parquet(received_table))
        elif received_table.file_type == 'excel':
            flow_file = cls(create_funcs.create_from_path_excel(received_table))
        else:
            raise Exception(f'Cannot create from {received_table.file_type}')
        flow_file._org_path = received_table.file_path
        return flow_file

    @classmethod
    def create_from_path_worker(cls, received_table: input_schema.ReceivedTable):
        received_table.set_absolute_filepath()
        external_fetcher = ExternalCreateFetcher(received_table, received_table.file_type)
        return cls(external_fetcher.get_result())

    @staticmethod
    def _handle_list_input(raw_data: List[Any]) -> List[Dict]:
        if len(raw_data) > 0:
            if not (isinstance(raw_data[0], dict) or hasattr(raw_data[0], '__dict__')):
                try:
                    return pl.DataFrame(raw_data).to_dicts()
                except:
                    raise BaseException('Value must be able to be converted to dictionary')
            is_dict = False if not (isinstance(raw_data[0], dict)) else True
            if not is_dict:
                raw_data = [__row__.__dict__ for __row__ in raw_data]
            raw_data = utils.ensure_similarity_dicts(raw_data)
        return raw_data

    def initialize_empty_fl(self):
        self.data_frame = pl.LazyFrame()
        self.number_of_records = 0
        self._lazy = True

    def get_number_of_records(self, warn: bool = False):
        if self.is_future and not self.is_collected:
            return -1
        if self.number_of_records is None or self.number_of_records < 0:
            if self._number_of_records_callback is not None:
                self._number_of_records_callback(self)

            if self.lazy:
                if warn:
                    logger.warning('Calculating the number of records this can be expensive on a lazy frame')
                try:
                    self.number_of_records = self.data_frame.select(pl.len()).collect(streaming=self._streamable)[0, 0]
                except Exception:
                    raise Exception('Could not get number of records')
            else:
                self.number_of_records = self.data_frame.__len__()
        return self.number_of_records

    def __len__(self):
        return self.number_of_records if self.number_of_records >= 0 else self.get_number_of_records()

    def count(self):
        return self.get_number_of_records()

    @property
    def has_errors(self):
        return len(self.errors) > 0

    @property
    def lazy(self):
        return self._lazy

    @property
    def external_source(self):
        return self._external_source

    def collect_external(self):
        if self._external_source is not None:
            logger.info('Collecting external source')
            if self.external_source.get_pl_df() is not None:
                self.data_frame = self.external_source.get_pl_df().lazy()
            else:
                self.data_frame = pl.LazyFrame(list(self.external_source.get_iter()))

    def collect(self, n_records: int = None) -> pl.DataFrame:
        if n_records is None:
            logger.info(f'Fetching all data for Table object "{id(self)}". Settings: streaming={self._streamable}')
        else:
            logger.info(f'Fetching {n_records} record(s) for Table object "{id(self)}". '
                        f'Settings: streaming={self._streamable}')

        if not self.lazy:
            return self.data_frame
        try:
            if n_records is None:
                self.collect_external()
                if self._streamable:
                    logger.info('Collecting data in streaming mode')
                    return self.data_frame.collect(streaming=True)
                else:
                    logger.info('Collecting data in non-streaming mode')
                    return self.data_frame.collect()

            else:
                if self.external_source is not None:
                    if self.external_source.get_pl_df() is not None:
                        all_data = self.external_source.get_pl_df().head(n_records)
                        self.data_frame = all_data
                    else:
                        all_data = self.external_source.get_sample(n_records)
                        self.data_frame = pl.LazyFrame(all_data)
                if self._streamable:
                    return self.data_frame.head(n_records).collect(streaming=True, comm_subplan_elim=False)
                return self.data_frame.head(n_records).collect()
        except Exception as e:
            self.errors = [e]
            pass
        n_records = 100000000 if n_records is None else n_records
        ok_cols = []
        error_cols = []
        for c in self.columns:
            try:
                _ = self.data_frame.select(c).head(n_records).collect()
                ok_cols.append(c)
            except:
                error_cols.append((c, self.data_frame.schema[c]))
        if len(ok_cols) > 0:
            df = self.data_frame.select(ok_cols)
            df = df.with_columns(
                [pl.lit(None).alias(column_name).cast(data_type) for column_name, data_type in error_cols])
            return df.select(self.columns).head(n_records).collect()
        else:
            if self.number_of_records > 0:
                df = pl.DataFrame({
                    column_name: pl.Series(name=column_name,
                                           values=[None] * min(self.number_of_records, n_records)).cast(data_type)
                    for column_name, data_type in self.data_frame.schema.items()
                })
            else:
                df = pl.DataFrame(schema=self.data_frame.schema)
        return df

    @classmethod
    def create_random(cls, number_of_records: int = 1000):
        return cls(create_fake_data(number_of_records))

    @lazy.setter
    def lazy(self, exec_lazy: bool = False):
        if exec_lazy != self._lazy:
            # print(f'{id(self)}: changing laziness')
            if exec_lazy:
                self.data_frame = self.data_frame.lazy()
            else:
                self._lazy = exec_lazy
                if self.external_source is not None:
                    df = self.collect()
                    self.data_frame = df
                else:
                    self.data_frame = self.data_frame.collect(streaming=self._streamable)
            self._lazy = exec_lazy

    @property
    def number_of_fields(self) -> int:
        if self.__number_of_fields is None:
            self.__number_of_fields = len(self.columns)
        return self.__number_of_fields

    def __call__(self) -> "FlowfileTable":
        return self

    def save(self, path: str, data_type: str = 'parquet') -> Future:
        estimated_size = deepcopy(self.get_estimated_file_size() * 4)
        df = deepcopy(self.data_frame)
        return write_threaded(_df=df, path=path, data_type=data_type, estimated_size=estimated_size)

    def _calculate_schema(self) -> List[Dict]:
        if self.external_source is not None:
            self.collect_external()
        v = utils.calculate_schema(self.data_frame)
        return v

    def calculate_schema(self):
        self._calculate_schema_stats = True
        return self.schema

    @property
    def schema(self) -> List[FlowfileColumn]:
        if self.number_of_fields == 0:
            return []
        if self._schema is None or (self._calculate_schema_stats and not self.ind_schema_calculated):
            if self._calculate_schema_stats and not self.ind_schema_calculated:
                schema_stats = self._calculate_schema()
                self.ind_schema_calculated = True
            else:
                schema_stats = [dict(column_name=k, pl_datatype=v, col_index=i) for i, (k, v) in
                                enumerate(self.data_frame.collect_schema().items())]
            self._schema = convert_stats_to_column_info(schema_stats)
        return self._schema

    def get_output_sample(self, n_rows: int = 10) -> List[Dict]:
        if self.number_of_records > n_rows or self.number_of_records < 0:
            df = self.collect(n_rows)
        else:
            df = self.collect()
        return df.to_dicts()

    def get_sample(self, n_rows: int = 100, random: bool = False, shuffle: bool = False,
                   seed: int = None) -> "FlowfileTable":
        n_records = min(n_rows, self.number_of_records)
        logging.info(f'Getting sample of {n_rows} rows')
        if random:
            if self.lazy and self.external_source is not None:
                self.collect_external()
            if self.lazy and shuffle:
                sample_df = self.data_frame.collect(streaming=self._streamable).sample(n_rows, seed=seed,
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
        return FlowfileTable(sample_df, schema=self.schema, number_of_records=n_records)

    def add_record_id(self, record_id_settings: transform_schemas.RecordIdInput) -> "FlowfileTable":
        if record_id_settings.group_by and len(record_id_settings.group_by_columns) > 0:
            select_cols = [pl.col(record_id_settings.output_column_name)] + [pl.col(c) for c in self.columns]

            df = (
                self.data_frame.with_columns(pl.lit(1).alias(record_id_settings.output_column_name))
                .with_columns((pl.cum_count(record_id_settings.output_column_name)
                               .over(record_id_settings.group_by_columns) + record_id_settings.offset - 1)
                              .alias(record_id_settings.output_column_name))
            ).select(select_cols)
            output_schema = [FlowfileColumn.from_input(record_id_settings.output_column_name, 'UInt64')]
            output_schema.extend(self.schema)
            return FlowfileTable(df, schema=output_schema)
        else:
            df = self.data_frame.with_row_index(record_id_settings.output_column_name, record_id_settings.offset)
            output_schema = [FlowfileColumn.from_input(record_id_settings.output_column_name, 'UInt64')]
            output_schema.extend(self.schema)
            return FlowfileTable(df, schema=output_schema)

    def split(self, split_input: transform_schemas.TextToRowsInput) -> "FlowfileTable":
        output_column_name = (split_input.output_column_name if split_input.output_column_name
                              else split_input.column_to_split)
        split_value = (split_input.split_fixed_value if split_input.split_by_fixed_value
                       else pl.col(split_input.split_by_column))
        df = (self.data_frame.with_columns(
            pl.col(split_input.column_to_split).str.split(by=split_value).alias(output_column_name))
              .explode(output_column_name)
              )
        return FlowfileTable(df)

    def __get_sample__(self, n_rows: int = 100, streamable: bool = True) -> "FlowfileTable":
        if not self.lazy:
            df = self.data_frame.lazy()
        else:
            df = self.data_frame

        if streamable:
            try:
                df = df.head(n_rows).collect()
            except Exception as e:
                logger.warning(f'Error in getting sample: {e}')
                df = df.head(n_rows).collect(streaming=False)
        else:
            df = self.collect()
        return FlowfileTable(df, number_of_records=len(df), schema=self.schema)

    def get_subset(self, n_rows: int = 100) -> "FlowfileTable":
        if not self.lazy:
            return FlowfileTable(self.data_frame.head(n_rows), calculate_schema_stats=True)
        else:
            return FlowfileTable(self.data_frame.head(n_rows), calculate_schema_stats=True)

    def __getitem__(self, item):
        return self.data_frame.select([item])

    def iter_batches(self, batch_size: int = 1000, columns: Union[List, Tuple, str] = None):
        if columns:
            self.data_frame = self.data_frame.select(columns)
        self.lazy = False
        batches = self.data_frame.iter_slices(batch_size)
        for batch in batches:
            yield FlowfileTable(batch)

    def to_pylist(self) -> List[Dict]:
        if self.lazy:
            return self.data_frame.collect(streaming=self._streamable).to_dicts()
        else:
            return self.data_frame.to_dicts()

    @property
    def cols_idx(self):
        if self._col_idx is None:
            self._col_idx = {c: i for i, c in enumerate(self.columns)}
        return self._col_idx

    def do_group_by(self,
                    group_by_input: transform_schemas.GroupByInput,
                    calculate_schema_stats: bool = True) -> "FlowfileTable":
        aggregations = [c for c in group_by_input.agg_cols if c.agg != 'groupby']
        group_columns = [c for c in group_by_input.agg_cols if c.agg == 'groupby']
        if len(group_columns) == 0:
            return FlowfileTable(
                self.data_frame.select(ac.agg_func(ac.old_name).alias(ac.new_name) for ac in aggregations),
                calculate_schema_stats=calculate_schema_stats)
        else:
            df = self.data_frame.rename({c.old_name: c.new_name for c in group_columns})
            group_by_columns = [n_c.new_name for n_c in group_columns]
            return FlowfileTable(df.group_by(*group_by_columns).agg(ac.agg_func(ac.old_name).alias(ac.new_name)
                                                                    for ac in aggregations),
                                 calculate_schema_stats=calculate_schema_stats)

    def do_select(self, select_inputs: transform_schemas.SelectInputs, keep_missing: bool = True) -> "FlowfileTable":
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
        output_file = FlowfileTable(ndf, number_of_records=self.number_of_records)
        return output_file.reorganize_order(sorted_cols)

    def select_columns(self, list_select: Union[List[str], Tuple[str], str]) -> "FlowfileTable":
        if isinstance(list_select, str):
            list_select = [list_select]
        idx_to_keep = [self.cols_idx.get(c) for c in list_select]
        selects = [ls for ls, id_to_keep in zip(list_select, idx_to_keep) if id_to_keep is not None]
        new_schema = [self.schema[i] for i in idx_to_keep if i is not None]
        return FlowfileTable(self.data_frame.select(selects),
                             number_of_records=self.number_of_records,
                             schema=new_schema,
                             streamable=self._streamable)

    def drop_columns(self, columns: List[str]):
        cols_for_select = tuple(set(self.columns) - set(columns))
        idx_to_keep = [self.cols_idx.get(c) for c in cols_for_select]
        new_schema = [self.schema[i] for i in idx_to_keep]
        return FlowfileTable(self.data_frame.select(cols_for_select),
                             number_of_records=self.number_of_records,
                             schema=new_schema)

    def change_column_types(self, transforms: List[transform_schemas.SelectInput],
                            calculate_schema: bool = False) -> "FlowfileTable":

        dtypes = [dtype.base_type() for dtype in self.data_frame.collect_schema().dtypes()]
        idx_mapping = list(
            (transform.old_name, self.cols_idx.get(transform.old_name), getattr(pl, transform.polars_type))
            for transform in transforms if transform.data_type is not None)
        actual_transforms = [c for c in idx_mapping if c[2] != dtypes[c[1]]]
        transformations = [utils.define_pl_col_transformation(col_name=transform[0],
                                                              col_type=transform[2]) for transform in actual_transforms]
        df = self.data_frame.with_columns(transformations)
        return FlowfileTable(df, number_of_records=self.number_of_records, calculate_schema_stats=calculate_schema,
                             streamable=self._streamable)

    def reorganize_order(self, column_order: List[str]) -> "FlowfileTable":
        df = self.data_frame.select(column_order)
        schema = sorted(self.schema, key=lambda x: column_order.index(x.column_name))
        return FlowfileTable(df, schema=schema, number_of_records=self.number_of_records)

    def group_by(self, group_by_input: transform_schemas.GroupByInput, calculate_schema: bool = True):
        df2 = self.data_frame.group_by(by=group_by_input.agg_cols).agg(ac.agg(ac.old_name).alias(ac.new_name)
                                                                       for ac in group_by_input.agg_cols)
        return FlowfileTable(df2, calculate_schema_stats=calculate_schema)

    @staticmethod
    def calculate_n_records_join(left_keys: List[Tuple], right_keys: List[Tuple]):
        pass

    def execute_polars_code(self, code: str) -> "FlowfileTable":
        polars_executable = polars_code_parser.get_executable(code)
        return FlowfileTable(polars_executable(self.data_frame))

    def join(self,
             join_input: transform_schemas.JoinInput,
             auto_generate_selection: bool,
             verify_integrity: bool,
             other: "FlowfileTable") -> "FlowfileTable":
        self.lazy = False if join_input.how == 'right' else True
        other.lazy = False if join_input.how == 'right' else True
        verify_join_select_integrity(join_input, left_columns=self.columns, right_columns=other.columns)
        if not verify_join_map_integrity(join_input, left_columns=self.schema, right_columns=other.schema):
            raise Exception('Join is not valid by the data fields')
        if auto_generate_selection:
            join_input.auto_rename()
        right_select = [v.old_name for v in join_input.right_select.renames if
                        (v.keep or v.join_key) and v.is_available]

        left_select = [v.old_name for v in join_input.left_select.renames if (v.keep or v.join_key) and v.is_available]
        left: pl.LazyFrame | pl.DataFrame = self.data_frame.select(left_select).rename(
            join_input.left_select.rename_table)
        right: pl.LazyFrame | pl.DataFrame = other.data_frame.select(right_select).rename(
            join_input.right_select.rename_table)
        if verify_integrity:
            n_records = get_join_count(left, right, left_on_keys=join_input.left_join_keys,
                                       right_on_keys=join_input.right_join_keys, how=join_input.how)
        else:
            n_records = -1
        if n_records > 1_000_000_000:
            raise Exception("Join will result in to many records, ending process")
        joined_df: pl.LazyFrame | pl.DataFrame = left.join(right, left_on=join_input.left_join_keys,
                                                           right_on=join_input.right_join_keys,
                                                           how=join_input.how, suffix="")
        cols_to_delete_after = [col.new_name for col in join_input.left_select.renames + join_input.left_select.renames
                                if col.join_key and not col.keep and col.is_available]
        if verify_integrity:
            return FlowfileTable(joined_df.drop(cols_to_delete_after), calculate_schema_stats=True,
                                 number_of_records=n_records, streamable=False)
        else:
            fl = FlowfileTable(joined_df.drop(cols_to_delete_after), calculate_schema_stats=False,
                               number_of_records=0, streamable=False)
            return fl

    def cache(self):
        edf = ExternalDfFetcher(lf=self.data_frame, file_ref=str(id(self)), wait_on_completion=False)
        logger.info('Caching data in background')
        result = edf.get_result()
        if result:
            logger.info('Data cached')
            del self._data_frame
            self.data_frame = result
            logger.info('Data loaded from cache')
        return self

    def do_fuzzy_join(self, fuzzy_match_input: transform_schemas.FuzzyMatchInput, other: "FlowfileTable",
                      file_ref: str):
        left_df, right_df = prepare_for_fuzzy_match(left=self, right=other,
                                                    fuzzy_match_input=fuzzy_match_input)
        f = ExternalFuzzyMatchFetcher(left_df, right_df, fuzzy_maps=fuzzy_match_input.fuzzy_maps,
                                      file_ref=file_ref + '_fm',
                                      wait_on_completion=True)
        return FlowfileTable(f.get_result())

    def fuzzy_match(self, right: "FlowfileTable", left_on: str, right_on: str, fuzzy_method: str = 'levenshtein',
                    threshold: float = 0.75):
        fuzzy_match_input = transform_schemas.FuzzyMatchInput([transform_schemas.FuzzyMap(left_on, right_on,
                                                                                          fuzzy_type=fuzzy_method,
                                                                                          threshold_score=threshold)]
                                                              , left_select=self.columns, right_select=right.columns)
        return self.do_fuzzy_join(fuzzy_match_input, right)

    def do_sort(self, sorts: List[transform_schemas.SortByInput]):
        if len(sorts) == 0:
            return self
        descending = [s.how == 'desc' or s.how.lower() == 'descending' for s in sorts]
        df = self.data_frame.sort([sort_by.column for sort_by in sorts], descending=descending)
        return FlowfileTable(df, number_of_records=self.number_of_records, schema=self.schema)

    def do_pivot(self, pivot_input: transform_schemas.PivotInput) -> "FlowfileTable":
        lf = self.data_frame.select(pivot_input.pivot_column).unique().cast(pl.String)
        new_cols_unique = fetch_unique_values(lf)

        if len(pivot_input.index_columns) == 0:
            no_index_cols = True
            pivot_input.index_columns = ['__temp__']
            ff = self.apply_flowfile_formula('1', col_name='__temp__')
        else:
            no_index_cols = False
            ff = self
        index_columns = pivot_input.get_index_columns()
        grouped_ff = ff.do_group_by(pivot_input.get_group_by_input(), False)
        pivot_column = pivot_input.get_pivot_column()
        input_df = grouped_ff.data_frame.with_columns(pivot_column.cast(pl.String).alias(pivot_input.pivot_column))
        df = (input_df.select(*index_columns,
                              pivot_column,
                              pivot_input.get_values_expr())
              .group_by(*index_columns)
              .agg([(pl.col('vals').filter(pivot_column == new_col_value)).first().alias(new_col_value)
                    for new_col_value in new_cols_unique]
                   )
              .select(*index_columns, *[pl.col(new_col).struct.field(agg).alias(f'{new_col}_{agg}')
                                        for new_col in new_cols_unique for agg in pivot_input.aggregations]
                      )
              )
        if no_index_cols:
            df = df.drop('__temp__')
            pivot_input.index_columns = []
        return FlowfileTable(df, calculate_schema_stats=False)

    def do_filter(self, predicate: str):
        try:
            f = to_expr(predicate)
        except:
            f = to_expr("False")
        df = self.data_frame.filter(f)
        return FlowfileTable(df, schema=self.schema, streamable=self._streamable)

    def set_streamable(self, streamable: bool = False):
        self._streamable = streamable

    def get_schema_column(self, col_name: str) -> FlowfileColumn:
        for s in self.schema:
            if s.name == col_name:
                return s

    def apply_flowfile_formula(self, func: str, col_name: str, output_data_type: pl.DataType = None):
        parsed_func = to_expr(func)
        if output_data_type is not None:
            df2 = self.data_frame.with_columns(parsed_func.cast(output_data_type).alias(col_name))
        else:
            df2 = self.data_frame.with_columns(parsed_func.alias(col_name))

        return FlowfileTable(df2, number_of_records=self.number_of_records)

    def apply_sql_formula(self, func: str, col_name: str, output_data_type: pl.DataType = None):
        expr = to_expr(func)
        if output_data_type is None:
            df = self.data_frame.with_columns(expr.cast(output_data_type).alias(col_name))
        else:
            df = self.data_frame.with_columns(expr.cast(output_data_type).alias(col_name))
        return FlowfileTable(df, number_of_records=self.number_of_records)

    @property
    def __name__(self):
        return self.name

    def get_select_inputs(self) -> transform_schemas.SelectInputs:
        return transform_schemas.SelectInputs(
            [transform_schemas.SelectInput(old_name=c.name, data_type=c.data_type) for c in self.schema])

    def unpivot(self, unpivot_input: transform_schemas.UnpivotInput):
        lf = self.data_frame
        if unpivot_input.data_type_selector_expr is not None:
            result = lf.unpivot(on=unpivot_input.data_type_selector_expr(), index=unpivot_input.index_columns)
        elif unpivot_input.value_columns is not None:
            result = lf.unpivot(on=unpivot_input.value_columns, index=unpivot_input.index_columns)
        else:
            result = lf.unpivot()
        return FlowfileTable(result)

    def concat(self, other: Iterable["FlowfileTable"] | "FlowfileTable") -> "FlowfileTable":
        if isinstance(other, FlowfileTable):
            other = [other]
        dfs: List[pl.LazyFrame] | List[pl.DataFrame] = [self.data_frame] + [flt.data_frame for flt in other]
        return FlowfileTable(pl.concat(dfs, how='diagonal_relaxed'))

    def make_unique(self, unique_input: transform_schemas.UniqueInput = None) -> "FlowfileTable":
        if unique_input is None or unique_input.columns is None:
            return FlowfileTable(self.data_frame.unique())
        return FlowfileTable(self.data_frame.unique(unique_input.columns, keep=unique_input.strategy))

    def solve_graph(self, graph_solver_input: transform_schemas.GraphSolverInput) -> "FlowfileTable":
        lf = self.data_frame.with_columns(graph_solver(graph_solver_input.col_from, graph_solver_input.col_to)
                                          .alias(graph_solver_input.output_column_name))
        return FlowfileTable(lf)

    def add_new_values(self, values: Iterable, col_name: str = None):
        if col_name is None:
            print('doing this')
            col_name = 'new_values'
        return FlowfileTable(self.data_frame.with_columns(pl.Series(values).alias(col_name)))

    def assert_equal(self, other: "FlowfileTable", ordered: bool = True, strict_schema: bool = False):
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

    def get_record_count(self) -> "FlowfileTable":
        return FlowfileTable(self.data_frame.select(pl.len().alias('number_of_records')))

    def do_cross_join(self,
                      cross_join_input: transform_schemas.CrossJoinInput,
                      auto_generate_selection: bool,
                      verify_integrity: bool,
                      other: "FlowfileTable") -> "FlowfileTable":
        self.lazy = True
        other.lazy = True
        verify_join_select_integrity(cross_join_input, left_columns=self.columns, right_columns=other.columns)
        if auto_generate_selection:
            cross_join_input.auto_rename()
        right_select = [v.old_name for v in cross_join_input.right_select.renames if
                        (v.keep or v.join_key) and v.is_available]

        left_select = [v.old_name for v in cross_join_input.left_select.renames if (v.keep or v.join_key)
                       and v.is_available]
        left: pl.LazyFrame | pl.DataFrame = self.data_frame.select(left_select).rename(
            cross_join_input.left_select.rename_table)
        right: pl.LazyFrame | pl.DataFrame = other.data_frame.select(right_select).rename(
            cross_join_input.right_select.rename_table)
        if verify_integrity:
            n_records = self.get_number_of_records() * other.get_number_of_records()
        else:
            n_records = -1
        if n_records > 1_000_000_000:
            raise Exception("Join will result in to many records, ending process")
        joined_df: pl.LazyFrame | pl.DataFrame = left.join(right, how='cross')
        cols_to_delete_after = [col.new_name for col in
                                cross_join_input.left_select.renames + cross_join_input.left_select.renames
                                if col.join_key and not col.keep and col.is_available]
        if verify_integrity:
            return FlowfileTable(joined_df.drop(cols_to_delete_after), calculate_schema_stats=False,
                                 number_of_records=n_records, streamable=False)
        else:
            fl = FlowfileTable(joined_df.drop(cols_to_delete_after), calculate_schema_stats=False,
                               number_of_records=0, streamable=False)
            return fl

    @classmethod
    def generate_enumerator(cls, length: int = 1000, output_name: str = 'output_column') -> "FlowfileTable":
        if length > 10_000_000:
            length = 10_000_000
        return cls(pl.LazyFrame().select((pl.int_range(0, length, dtype=pl.UInt32)).alias(output_name)))
