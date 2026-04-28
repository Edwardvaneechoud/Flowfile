# This file was auto-generated to provide type information for flowfile_frame.lazy
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

import inspect
import warnings
from collections.abc import Callable
from functools import wraps
from typing import Any, Literal, cast
import polars as pl
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame.callable_utils import resolve_callable
from flowfile_frame.expr import Expr
from flowfile_frame.flow_frame import FlowFrame, can_be_expr, generate_node_id
from flowfile_frame.utils import create_flow_graph
from flowfile_frame.expr import _get_expr_and_repr
import io

def fold(acc: IntoExpr, function: Callable[[Series, Series], Series], exprs: Sequence[Expr | str] | Expr, returns_scalar: bool=False, return_dtype: pl.DataTypeExpr | PolarsDataType | None=None) -> Expr: ...

def polars_function_wrapper(polars_func_name_or_callable: str | Callable, is_agg: bool=False, return_type: Union[Literal['FlowFrame', 'Expr'], None]=None) -> Any: ...

def read_avro(source: str | Path | IO[bytes] | bytes, columns: list[int] | list[str] | None=None, n_rows: int | None=None, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None=None) -> FlowFrame: ...

def read_json(source: str | Path | IOBase | bytes, schema: SchemaDefinition | None=None, schema_overrides: SchemaDefinition | None=None, infer_schema_length: int | None=100, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None=None) -> FlowFrame: ...

def read_ndjson(source: str | Path | IO[str] | IO[bytes] | bytes | list[str] | list[Path] | list[IO[str]] | list[IO[bytes]], schema: SchemaDefinition | None=None, schema_overrides: SchemaDefinition | None=None, infer_schema_length: int | None=100, batch_size: int | None=1024, n_rows: int | None=None, low_memory: bool=False, rechunk: bool=False, row_index_name: str | None=None, row_index_offset: int=0, ignore_errors: bool=False, storage_options: StorageOptionsDict | None=None, credential_provider: CredentialProviderFunction | Literal['auto'] | None='auto', retries: int | None=None, file_cache_ttl: int | None=None, include_file_paths: str | None=None, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None=None) -> FlowFrame: ...

