# Standard library imports
import collections
import inspect
import os
import sys
import typing
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping, Sequence
from datetime import timedelta
from io import IOBase
from pathlib import Path
from typing import (
    IO,
    Any,
    ForwardRef,
    Literal,
    TypeVar,
)

# Third-party imports
import polars as pl
from polars import DataFrame, LazyFrame, QueryOptFlags
from polars._typing import *
from polars._typing import ParquetMetadata, PlanStage
from polars._utils.async_ import _GeventDataFrameResult
from polars.dependencies import polars_cloud as pc
from polars.io.cloud import CredentialProviderFunction
from polars.io.parquet import ParquetFieldOverwrites
from polars.lazyframe.frame import LazyGroupBy
from polars.lazyframe.opt_flags import DEFAULT_QUERY_OPT_FLAGS
from polars.type_aliases import (
    AsofJoinStrategy,
    ClosedInterval,
    CompatLevel,
    EngineType,
    ExplainFormat,
    IntoExpr,
    IpcCompression,
    Label,
    Schema,
    SerializationFormat,
    StartBy,
    SyncOnCloseMethod,
)

# Local application/library specific imports
import flowfile_frame
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import transform_schema
from flowfile_frame import group_frame
from flowfile_frame.expr import Expr

# Conditional imports
if sys.version_info >= (3, 10):
    from typing import Concatenate
else:
    from typing import Concatenate

T = TypeVar('T')
P = typing.ParamSpec('P')
LazyFrameT = TypeVar('LazyFrameT', bound='LazyFrame')
FlowFrameT = TypeVar('FlowFrameT', bound='FlowFrame')
Self = TypeVar('Self', bound='FlowFrame')
NoneType = type(None)

# Module-level functions (example from your input)
def can_be_expr(param: inspect.Parameter) -> bool: ...
def generate_node_id() -> int: ...
def get_method_name_from_code(code: str) -> str | None: ...
def _contains_lambda_pattern(text: str) -> bool: ...
def _to_string_val(v) -> str: ...
def _extract_expr_parts(expr_obj) -> tuple[str, str]: ...
def _check_ok_for_serialization(method_name: str = None, polars_expr: pl.Expr | None = None, group_expr: pl.Expr | None = None) -> None: ...

class FlowFrame:
    data: LazyFrame
    flow_graph: FlowGraph
    node_id: int
    parent_node_id: int | None

    # This special method determines how the object behaves in boolean contexts.
    def __bool__(self) -> Any: ...

    # This special method enables the 'in' operator to work with FlowFrame objects.
    def __contains__(self, key) -> Any: ...

    def __eq__(self, other: object) -> typing.NoReturn: ...

    def __ge__(self, other: Any) -> typing.NoReturn: ...

    def __gt__(self, other: Any) -> typing.NoReturn: ...

    # The __init__ method is intentionally left empty.
    def __init__(self, *args, **kwargs) -> None: ...

    def __le__(self, other: Any) -> typing.NoReturn: ...

    def __lt__(self, other: Any) -> typing.NoReturn: ...

    def __ne__(self, other: object) -> typing.NoReturn: ...

    # Unified constructor for FlowFrame.
    def __new__(cls, data: LazyFrame | collections.abc.Mapping[str, collections.abc.Sequence[object] | collections.abc.Mapping[str, collections.abc.Sequence[object]] | ForwardRef('Series')] | collections.abc.Sequence[typing.Any] | ForwardRef('np.ndarray[Any, Any]') | ForwardRef('pa.Table') | ForwardRef('pd.DataFrame') | ForwardRef('ArrowArrayExportable') | ForwardRef('ArrowStreamExportable') | ForwardRef('torch.Tensor') = None, schema: collections.abc.Mapping[str, ForwardRef('DataTypeClass') | ForwardRef('DataType') | type[int] | type[float] | type[bool] | type[str] | type[date] | type[time] | type[datetime] | type[timedelta] | type[list[typing.Any]] | type[tuple[typing.Any, ...]] | type[bytes] | type[object] | type[Decimal] | type[None] | NoneType] | collections.abc.Sequence[str | tuple[str, ForwardRef('DataTypeClass') | ForwardRef('DataType') | type[int] | type[float] | type[bool] | type[str] | type[date] | type[time] | type[datetime] | type[timedelta] | type[list[typing.Any]] | type[tuple[typing.Any, ...]] | type[bytes] | type[object] | type[Decimal] | type[None] | NoneType]] | NoneType = None, schema_overrides: collections.abc.Mapping[str, ForwardRef('DataTypeClass') | ForwardRef('DataType')] | None = None, strict: bool = True, orient: typing.Literal['col', 'row'] | None = None, infer_schema_length: int | None = 100, nan_to_null: bool = False, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None = None, node_id: int | None = None, parent_node_id: int | None = None, **kwargs) -> Self: ...

    def __repr__(self) -> Any: ...

    # Helper method to add a connection between nodes
    def _add_connection(self, from_id, to_id, input_type: typing.Literal['main', 'left', 'right'] = 'main') -> Any: ...

    # Add a cross join node to the graph.
    def _add_cross_join_node(self, new_node_id: int, join_input: transform_schema.CrossJoinInput, description: str, other: FlowFrame) -> None: ...

    def _add_number_of_records(self, new_node_id: int, description: str = None) -> FlowFrame: ...

    def _add_polars_code(self, new_node_id: int, code: str, depending_on_ids: list[str] | None = None, convertable_to_code: bool = True, method_name: str = None, polars_expr: flowfile_frame.expr.Expr | list[flowfile_frame.expr.Expr] | NoneType = None, group_expr: flowfile_frame.expr.Expr | list[flowfile_frame.expr.Expr] | NoneType = None, kwargs_expr: dict | None = None, group_kwargs: dict | None = None, description: str = None) -> Any: ...

    # Add a regular join node to the graph.
    def _add_regular_join_node(self, new_node_id: int, join_input: transform_schema.JoinInput, description: str, other: FlowFrame) -> None: ...

    # Build kwargs dictionary for Polars join code.
    def _build_polars_join_kwargs(self, on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, left_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, right_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, left_columns: list[str] | None, right_columns: list[str] | None, how: str, suffix: str, validate: str, nulls_equal: bool, coalesce: bool, maintain_order: typing.Literal[None, 'left', 'right', 'left_right', 'right_left']) -> dict: ...

    def _comparison_error(self, operator: str) -> typing.NoReturn: ...

    # Helper method to create a new FlowFrame that's a child of this one
    def _create_child_frame(self, new_node_id) -> FlowFrame: ...

    # Detect if the expression is a cum_count operation and use record_id if possible.
    def _detect_cum_count_record_id(self, expr: Any, new_node_id: int, description: str | None = None) -> FlowFrame: ...

    # Ensure both FlowFrames are in the same graph, combining if necessary.
    def _ensure_same_graph(self, other: FlowFrame) -> None: ...

    # Execute join using native FlowFile join nodes.
    def _execute_native_join(self, other: FlowFrame, new_node_id: int, join_mappings: list | None, how: str, description: str) -> FlowFrame: ...

    # Execute join using Polars code approach.
    def _execute_polars_code_join(self, other: FlowFrame, new_node_id: int, on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, left_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, right_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, left_columns: list[str] | None, right_columns: list[str] | None, how: str, suffix: str, validate: str, nulls_equal: bool, coalesce: bool, maintain_order: typing.Literal[None, 'left', 'right', 'left_right', 'right_left'], description: str) -> FlowFrame: ...

    # Generates the `input_df.sort(...)` Polars code string using pure expression strings.
    def _generate_sort_polars_code(self, pure_sort_expr_strs: list[str], descending_values: list[bool], nulls_last_values: list[bool], multithreaded: bool, maintain_order: bool) -> str: ...

    # Parse and validate join column specifications.
    def _parse_join_columns(self, on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, left_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, right_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column, how: str) -> tuple[list[str] | None, list[str] | None]: ...

    # Determine if we should use Polars code instead of native join.
    def _should_use_polars_code_for_join(self, maintain_order, coalesce, nulls_equal, validate, suffix) -> bool: ...

    def _with_flowfile_formula(self, flowfile_formula: str, output_column_name, description: str = None) -> FlowFrame: ...

    # Approximate count of unique values.
    def approx_n_unique(self, description: str | None = None) -> FlowFrame: ...

    # Return the `k` smallest rows.
    def bottom_k(self, k: int, by: IntoExpr | Iterable[IntoExpr], reverse: bool | Sequence[bool] = False, description: str | None = None) -> FlowFrame: ...

    def cache(self, description: str | None = None) -> FlowFrame: ...

    # Cast LazyFrame column(s) to the specified dtype(s).
    def cast(self, dtypes: Mapping[ColumnNameOrSelector | PolarsDataType, PolarsDataType | PythonDataType] | PolarsDataType | pl.DataTypeExpr, strict: bool = True, description: str | None = None) -> FlowFrame: ...

    # Create an empty copy of the current LazyFrame, with zero to 'n' rows.
    def clear(self, n: int = 0, description: str | None = None) -> FlowFrame: ...

    # Create a copy of this LazyFrame.
    def clone(self, description: str | None = None) -> FlowFrame: ...

    # Collect lazy data into memory.
    def collect(self, *args, **kwargs) -> DataFrame: ...

    # Collect DataFrame asynchronously in thread pool.
    def collect_async(self, gevent: bool = False, engine: EngineType = 'auto', optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS) -> Awaitable[DataFrame] | _GeventDataFrameResult[DataFrame]: ...

    # Resolve the schema of this LazyFrame.
    def collect_schema(self) -> Schema: ...

    # Get the column names.
    @property
    def columns(self) -> list[str]: ...

    # Combine multiple FlowFrames into a single FlowFrame.
    def concat(self, other: ForwardRef('FlowFrame') | list[ForwardRef('FlowFrame')], how: str = 'vertical', rechunk: bool = False, parallel: bool = True, description: str = None) -> FlowFrame: ...

    # Return the number of non-null elements for each column.
    def count(self, description: str | None = None) -> FlowFrame: ...

    # Simple naive implementation of creating the frame from any type. It converts the data to a polars frame,
    def create_from_any_type(self, data: collections.abc.Mapping[str, collections.abc.Sequence[object] | collections.abc.Mapping[str, collections.abc.Sequence[object]] | ForwardRef('Series')] | collections.abc.Sequence[typing.Any] | ForwardRef('np.ndarray[Any, Any]') | ForwardRef('pa.Table') | ForwardRef('pd.DataFrame') | ForwardRef('ArrowArrayExportable') | ForwardRef('ArrowStreamExportable') | ForwardRef('torch.Tensor') = None, schema: collections.abc.Mapping[str, ForwardRef('DataTypeClass') | ForwardRef('DataType') | type[int] | type[float] | type[bool] | type[str] | type[date] | type[time] | type[datetime] | type[timedelta] | type[list[typing.Any]] | type[tuple[typing.Any, ...]] | type[bytes] | type[object] | type[Decimal] | type[None] | NoneType] | collections.abc.Sequence[str | tuple[str, ForwardRef('DataTypeClass') | ForwardRef('DataType') | type[int] | type[float] | type[bool] | type[str] | type[date] | type[time] | type[datetime] | type[timedelta] | type[list[typing.Any]] | type[tuple[typing.Any, ...]] | type[bytes] | type[object] | type[Decimal] | type[None] | NoneType]] | NoneType = None, schema_overrides: collections.abc.Mapping[str, ForwardRef('DataTypeClass') | ForwardRef('DataType')] | None = None, strict: bool = True, orient: typing.Literal['col', 'row'] | None = None, infer_schema_length: int | None = 100, nan_to_null: bool = False, flow_graph = None, node_id = None, parent_node_id = None, description: str | None = None) -> Any: ...

    # Creates a summary of statistics for a LazyFrame, returning a DataFrame.
    def describe(self, percentiles: Sequence[float] | float | None = ..., interpolation: QuantileMethod = 'nearest') -> DataFrame: ...

    # Read a logical plan from a file to construct a LazyFrame.
    def deserialize(self, source: str | Path | IOBase, format: SerializationFormat = 'binary', description: str | None = None) -> FlowFrame: ...

    # Remove columns from the DataFrame.
    def drop(self, *columns, strict: bool = True, description: str | None = None) -> FlowFrame: ...

    # Drop all rows that contain one or more NaN values.
    def drop_nans(self, subset: ColumnNameOrSelector | Collection[ColumnNameOrSelector] | None = None, description: str | None = None) -> FlowFrame: ...

    # Drop all rows that contain one or more null values.
    def drop_nulls(self, subset: ColumnNameOrSelector | Collection[ColumnNameOrSelector] | None = None, description: str | None = None) -> FlowFrame: ...

    # Get the column data types.
    @property
    def dtypes(self) -> list[pl.classes.DataType]: ...

    # Create a string representation of the query plan.
    def explain(self, format: ExplainFormat = 'plain', optimized: bool = True, type_coercion: bool = True, predicate_pushdown: bool = True, projection_pushdown: bool = True, simplify_expression: bool = True, slice_pushdown: bool = True, comm_subplan_elim: bool = True, comm_subexpr_elim: bool = True, cluster_with_columns: bool = True, collapse_joins: bool = True, streaming: bool = False, engine: EngineType = 'auto', tree_format: bool | None = None, optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS) -> str: ...

    # Explode the dataframe to long format by exploding the given columns.
    def explode(self, columns: str | flowfile_frame.expr.Column | typing.Iterable[str | flowfile_frame.expr.Column], *more_columns, description: str = None) -> FlowFrame: ...

    # Collect a small number of rows for debugging purposes.
    def fetch(self, n_rows: int = 500, type_coercion: bool = True, _type_check: bool = True, predicate_pushdown: bool = True, projection_pushdown: bool = True, simplify_expression: bool = True, no_optimization: bool = False, slice_pushdown: bool = True, comm_subplan_elim: bool = True, comm_subexpr_elim: bool = True, cluster_with_columns: bool = True, collapse_joins: bool = True) -> DataFrame: ...

    # Fill floating point NaN values.
    def fill_nan(self, value: int | float | Expr | None, description: str | None = None) -> FlowFrame: ...

    # Fill null values using the specified value or strategy.
    def fill_null(self, value: Any | Expr | None = None, strategy: FillNullStrategy | None = None, limit: int | None = None, matches_supertype: bool = True, description: str | None = None) -> FlowFrame: ...

    # Filter rows based on a predicate.
    def filter(self, *predicates, flowfile_formula: str | None = None, description: str | None = None, **constraints) -> FlowFrame: ...

    # Get the first row of the DataFrame.
    def first(self, description: str | None = None) -> FlowFrame: ...

    def fuzzy_match(self, other: FlowFrame, fuzzy_mappings: list[flowfile_core.schemas.transform_schema.FuzzyMap], description: str = None) -> FlowFrame: ...

    # Take every nth row in the LazyFrame and return as a new LazyFrame.
    def gather_every(self, n: int, offset: int = 0, description: str | None = None) -> FlowFrame: ...

    def get_node_settings(self, description: str | None = None) -> FlowNode: ...

    # Start a group by operation.
    def group_by(self, *by, description: str | None = None, maintain_order: bool = False, **named_by) -> group_frame.GroupByFrame: ...

    # Group based on a time value (or index value of type Int32, Int64).
    def group_by_dynamic(self, index_column: IntoExpr, every: str | timedelta, period: str | timedelta | None = None, offset: str | timedelta | None = None, include_boundaries: bool = False, closed: ClosedInterval = 'left', label: Label = 'left', group_by: IntoExpr | Iterable[IntoExpr] | None = None, start_by: StartBy = 'window', description: str | None = None) -> LazyGroupBy: ...

    def head(self, n: int, description: str = None) -> Any: ...

    # Inspect a node in the computation graph.
    def inspect(self, fmt: str = '{}', description: str | None = None) -> FlowFrame: ...

    # Interpolate intermediate values. The interpolation method is linear.
    def interpolate(self, description: str | None = None) -> FlowFrame: ...

    # Add a join operation to the Logical Plan.
    def join(self, other, on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column = None, how: str = 'inner', left_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column = None, right_on: list[str | flowfile_frame.expr.Column] | str | flowfile_frame.expr.Column = None, suffix: str = '_right', validate: str = None, nulls_equal: bool = False, coalesce: bool = None, maintain_order: typing.Literal[None, 'left', 'right', 'left_right', 'right_left'] = None, description: str = None) -> FlowFrame: ...

    # Perform an asof join.
    def join_asof(self, other: FlowFrame, left_on: str | None | Expr = None, right_on: str | None | Expr = None, on: str | None | Expr = None, by_left: str | Sequence[str] | None = None, by_right: str | Sequence[str] | None = None, by: str | Sequence[str] | None = None, strategy: AsofJoinStrategy = 'backward', suffix: str = '_right', tolerance: str | int | float | timedelta | None = None, allow_parallel: bool = True, force_parallel: bool = False, coalesce: bool = True, allow_exact_matches: bool = True, check_sortedness: bool = True, description: str | None = None) -> FlowFrame: ...

    # Perform a join based on one or multiple (in)equality predicates.
    def join_where(self, other: FlowFrame, *predicates, suffix: str = '_right', description: str | None = None) -> FlowFrame: ...

    # Get the last row of the DataFrame.
    def last(self, description: str | None = None) -> FlowFrame: ...

    # Return lazy representation, i.e. itself.
    def lazy(self, description: str | None = None) -> FlowFrame: ...

    def limit(self, n: int, description: str = None) -> Any: ...

    # Apply a custom function.
    def map_batches(self, function: Callable[[DataFrame], DataFrame], predicate_pushdown: bool = True, projection_pushdown: bool = True, slice_pushdown: bool = True, no_optimizations: bool = False, schema: None | SchemaDict = None, validate_output_schema: bool = True, streamable: bool = False, description: str | None = None) -> FlowFrame: ...

    # Match or evolve the schema of a LazyFrame into a specific schema.
    def match_to_schema(self, schema: SchemaDict | Schema, missing_columns: Literal['insert', 'raise'] | Mapping[str, Literal['insert', 'raise'] | Expr] = 'raise', missing_struct_fields: Literal['insert', 'raise'] | Mapping[str, Literal['insert', 'raise']] = 'raise', extra_columns: Literal['ignore', 'raise'] = 'raise', extra_struct_fields: Literal['ignore', 'raise'] | Mapping[str, Literal['ignore', 'raise']] = 'raise', integer_cast: Literal['upcast', 'forbid'] | Mapping[str, Literal['upcast', 'forbid']] = 'forbid', float_cast: Literal['upcast', 'forbid'] | Mapping[str, Literal['upcast', 'forbid']] = 'forbid', description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame to their maximum value.
    def max(self, description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame to their mean value.
    def mean(self, description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame to their median value.
    def median(self, description: str | None = None) -> FlowFrame: ...

    # Unpivot a DataFrame from wide to long format.
    def melt(self, id_vars: ColumnNameOrSelector | Sequence[ColumnNameOrSelector] | None = None, value_vars: ColumnNameOrSelector | Sequence[ColumnNameOrSelector] | None = None, variable_name: str | None = None, value_name: str | None = None, streamable: bool = True, description: str | None = None) -> FlowFrame: ...

    # Take two sorted DataFrames and merge them by the sorted key.
    def merge_sorted(self, other: FlowFrame, key: str, description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame to their minimum value.
    def min(self, description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame as the sum of their null value count.
    def null_count(self, description: str | None = None) -> FlowFrame: ...

    # Offers a structured way to apply a sequence of user-defined functions (UDFs).
    def pipe(self, function: Callable[Concatenate[LazyFrame, P], T], *args, description: str | None = None, **kwargs) -> T: ...

    # Pivot a DataFrame from long to wide format.
    def pivot(self, on: str | list[str], index: str | list[str] | None = None, values: str | list[str] | None = None, aggregate_function: str | None = 'first', maintain_order: bool = True, sort_columns: bool = False, separator: str = '_', description: str = None) -> FlowFrame: ...

    # Profile a LazyFrame.
    def profile(self, type_coercion: bool = True, predicate_pushdown: bool = True, projection_pushdown: bool = True, simplify_expression: bool = True, no_optimization: bool = False, slice_pushdown: bool = True, comm_subplan_elim: bool = True, comm_subexpr_elim: bool = True, cluster_with_columns: bool = True, collapse_joins: bool = True, show_plot: bool = False, truncate_nodes: int = 0, figsize: tuple[int, int] = ..., engine: EngineType = 'auto', optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS, **_kwargs) -> tuple[DataFrame, DataFrame]: ...

    # Aggregate the columns in the LazyFrame to their quantile value.
    def quantile(self, quantile: float | Expr, interpolation: QuantileMethod = 'nearest', description: str | None = None) -> FlowFrame: ...

    # Run a query remotely on Polars Cloud.
    def remote(self, context: pc.ComputeContext | None = None, plan_type: pc._typing.PlanTypePreference = 'dot', description: str | None = None) -> pc.LazyFrameExt: ...

    # Remove rows, dropping those that match the given predicate expression(s).
    def remove(self, *predicates, description: str | None = None, **constraints) -> FlowFrame: ...

    # Rename column names.
    def rename(self, mapping: Mapping[str, str] | Callable[[str], str], strict: bool = True, description: str | None = None) -> FlowFrame: ...

    # Reverse the DataFrame.
    def reverse(self, description: str | None = None) -> FlowFrame: ...

    # Create rolling groups based on a temporal or integer column.
    def rolling(self, index_column: IntoExpr, period: str | timedelta, offset: str | timedelta | None = None, closed: ClosedInterval = 'right', group_by: IntoExpr | Iterable[IntoExpr] | None = None, description: str | None = None) -> LazyGroupBy: ...

    # Save the graph
    def save_graph(self, file_path: str, auto_arrange: bool = True, description: str | None = None) -> Any: ...

    # Get an ordered mapping of column names to their data type.
    @property
    def schema(self) -> pl.Schema: ...

    # Select columns from the frame.
    def select(self, *columns, description: str | None = None) -> FlowFrame: ...

    # Select columns from this LazyFrame.
    def select_seq(self, *exprs, description: str | None = None, **named_exprs) -> FlowFrame: ...

    # Serialize the logical plan of this LazyFrame to a file or string in JSON format.
    def serialize(self, file: IOBase | str | Path | None = None, format: SerializationFormat = 'binary', description: str | None = None) -> bytes | str | None: ...

    # Flag a column as sorted.
    def set_sorted(self, column: str, descending: bool = False, description: str | None = None) -> FlowFrame: ...

    # Shift values by the given number of indices.
    def shift(self, n: int | IntoExprColumn = 1, fill_value: IntoExpr | None = None, description: str | None = None) -> FlowFrame: ...

    # Show a plot of the query plan.
    def show_graph(self, optimized: bool = True, show: bool = True, output_path: str | Path | None = None, raw_output: bool = False, figsize: tuple[float, float] = ..., type_coercion: bool = True, _type_check: bool = True, predicate_pushdown: bool = True, projection_pushdown: bool = True, simplify_expression: bool = True, slice_pushdown: bool = True, comm_subplan_elim: bool = True, comm_subexpr_elim: bool = True, cluster_with_columns: bool = True, collapse_joins: bool = True, engine: EngineType = 'auto', plan_stage: PlanStage = 'ir', _check_order: bool = True, optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS) -> str | None: ...

    # Write the data to a CSV file.
    def sink_csv(self, file: str, *args, separator: str = ',', encoding: str = 'utf-8', description: str = None) -> FlowFrame: ...

    # Evaluate the query in streaming mode and write to an IPC file.
    def sink_ipc(self, path: str | Path | IO[bytes] | PartitioningScheme, compression: IpcCompression | None = 'uncompressed', compat_level: CompatLevel | None = None, maintain_order: bool = True, storage_options: dict[str, Any] | None = None, credential_provider: CredentialProviderFunction | Literal['auto'] | None = 'auto', retries: int = 2, sync_on_close: SyncOnCloseMethod | None = None, mkdir: bool = False, lazy: bool = False, engine: EngineType = 'auto', optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS, description: str | None = None) -> LazyFrame | None: ...

    # Evaluate the query in streaming mode and write to an NDJSON file.
    def sink_ndjson(self, path: str | Path | IO[bytes] | IO[str] | PartitioningScheme, maintain_order: bool = True, storage_options: dict[str, Any] | None = None, credential_provider: CredentialProviderFunction | Literal['auto'] | None = 'auto', retries: int = 2, sync_on_close: SyncOnCloseMethod | None = None, mkdir: bool = False, lazy: bool = False, engine: EngineType = 'auto', optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS, description: str | None = None) -> LazyFrame | None: ...

    # Evaluate the query in streaming mode and write to a Parquet file.
    def sink_parquet(self, path: str | Path | IO[bytes] | PartitioningScheme, compression: str = 'zstd', compression_level: int | None = None, statistics: bool | str | dict[str, bool] = True, row_group_size: int | None = None, data_page_size: int | None = None, maintain_order: bool = True, storage_options: dict[str, Any] | None = None, credential_provider: CredentialProviderFunction | Literal['auto'] | None = 'auto', retries: int = 2, sync_on_close: SyncOnCloseMethod | None = None, metadata: ParquetMetadata | None = None, mkdir: bool = False, lazy: bool = False, field_overwrites: ParquetFieldOverwrites | Sequence[ParquetFieldOverwrites] | Mapping[str, ParquetFieldOverwrites] | None = None, engine: EngineType = 'auto', optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS, description: str | None = None) -> LazyFrame | None: ...

    # Get a slice of this DataFrame.
    def slice(self, offset: int, length: int | None = None, description: str | None = None) -> FlowFrame: ...

    # Sort the dataframe by the given columns.
    def sort(self, by: list[flowfile_frame.expr.Expr | str] | flowfile_frame.expr.Expr | str, *more_by, descending: bool | list[bool] = False, nulls_last: bool | list[bool] = False, multithreaded: bool = True, maintain_order: bool = False, description: str | None = None) -> FlowFrame: ...

    # Execute a SQL query against the LazyFrame.
    def sql(self, query: str, table_name: str = 'self', description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame to their standard deviation value.
    def std(self, ddof: int = 1, description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame to their sum value.
    def sum(self, description: str | None = None) -> FlowFrame: ...

    # Get the last `n` rows.
    def tail(self, n: int = 5, description: str | None = None) -> FlowFrame: ...

    # Split text in a column into multiple rows.
    def text_to_rows(self, column: str | flowfile_frame.expr.Column, output_column: str = None, delimiter: str = None, split_by_column: str = None, description: str = None) -> FlowFrame: ...

    # Get the underlying ETL graph.
    def to_graph(self, description: str | None = None) -> Any: ...

    # Return the `k` largest rows.
    def top_k(self, k: int, by: IntoExpr | Iterable[IntoExpr], reverse: bool | Sequence[bool] = False, description: str | None = None) -> FlowFrame: ...

    # Drop duplicate rows from this dataframe.
    def unique(self, subset: str | ForwardRef('Expr') | list[ForwardRef('Expr') | str] = None, keep: typing.Literal['first', 'last', 'any', 'none'] = 'any', maintain_order: bool = False, description: str = None) -> FlowFrame: ...

    # Decompose struct columns into separate columns for each of their fields.
    def unnest(self, columns: ColumnNameOrSelector | Collection[ColumnNameOrSelector], *more_columns, description: str | None = None) -> FlowFrame: ...

    # Unpivot a DataFrame from wide to long format.
    def unpivot(self, on: list[str | flowfile_frame.selectors.Selector] | str | None | flowfile_frame.selectors.Selector = None, index: list[str] | str | None = None, variable_name: str = 'variable', value_name: str = 'value', description: str = None) -> FlowFrame: ...

    # Update the values in this `LazyFrame` with the values in `other`.
    def update(self, other: FlowFrame, on: str | Sequence[str] | None = None, how: Literal['left', 'inner', 'full'] = 'left', left_on: str | Sequence[str] | None = None, right_on: str | Sequence[str] | None = None, include_nulls: bool = False, maintain_order: MaintainOrderJoin | None = 'left', description: str | None = None) -> FlowFrame: ...

    # Aggregate the columns in the LazyFrame to their variance value.
    def var(self, ddof: int = 1, description: str | None = None) -> FlowFrame: ...

    # Get the number of columns.
    @property
    def width(self) -> int: ...

    # Add or replace columns in the DataFrame.
    def with_columns(self, *exprs: Expr | Iterable[Expr] | Any, flowfile_formulas: list[str] | None = None, output_column_names: list[str] | None = None, description: str | None = None, **named_exprs: Expr | Any) -> FlowFrame: ...

    # Add columns to this LazyFrame.
    def with_columns_seq(self, *exprs, description: str | None = None, **named_exprs) -> FlowFrame: ...

    # Add an external context to the computation graph.
    def with_context(self, other: Self | list[Self], description: str | None = None) -> FlowFrame: ...

    # Add a column at index 0 that counts the rows.
    def with_row_count(self, name: str = 'row_nr', offset: int = 0, description: str | None = None) -> FlowFrame: ...

    # Add a row index as the first column in the DataFrame.
    def with_row_index(self, name: str = 'index', offset: int = 0, description: str = None) -> FlowFrame: ...

    def write_csv(self, file: str | os.PathLike, separator: str = ',', encoding: str = 'utf-8', convert_to_absolute_path: bool = True, description: str = None, **kwargs) -> FlowFrame: ...

    # Write the data frame to cloud storage in CSV format.
    def write_csv_to_cloud_storage(self, path: str, connection_name: str | None = None, delimiter: str = ';', encoding: typing.Literal['utf8', 'utf8-lossy'] = 'utf8', description: str | None = None) -> FlowFrame: ...

    # Write the data frame to cloud storage in Delta Lake format.
    def write_delta(self, path: str, connection_name: str | None = None, write_mode: typing.Literal['overwrite', 'append'] = 'overwrite', description: str | None = None) -> FlowFrame: ...

    # Write the data frame to cloud storage in JSON format.
    def write_json_to_cloud_storage(self, path: str, connection_name: str | None = None, description: str | None = None) -> FlowFrame: ...

    # Write the data to a Parquet file. Creates a standard Output node if only
    def write_parquet(self, path: str | os.PathLike, convert_to_absolute_path: bool = True, description: str = None, **kwargs) -> FlowFrame: ...

    # Write the data frame to cloud storage in Parquet format.
    def write_parquet_to_cloud_storage(self, path: str, connection_name: str | None = None, compression: typing.Literal['snappy', 'gzip', 'brotli', 'lz4', 'zstd'] = 'snappy', description: str | None = None) -> FlowFrame: ...
