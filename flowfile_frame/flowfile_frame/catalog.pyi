# This file was auto-generated to provide type information for flowfile_frame.catalog
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

from typing import TYPE_CHECKING, Literal
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_frame.flow_frame import FlowFrame
from flowfile_core.schemas import input_schema
from flowfile_frame.utils import generate_node_id
from flowfile_frame.utils import create_flow_graph, generate_node_id

def add_write_to_catalog(flow_graph: FlowGraph, depends_on_node_id: int, table_name: str, namespace_id: int | None=None, write_mode: str='overwrite', merge_keys: list[str] | None=None, description: str | None=None) -> int: ...

def get_current_user_id() -> int: ...

def read_catalog_sql(sql_query: str, flow_graph=None) -> FlowFrame: ...

def read_catalog_table(table_name: str, namespace_id: int | None=None, delta_version: int | None=None, flow_graph=None) -> FlowFrame: ...

def write_catalog_table(df: FlowFrame, table_name: str, namespace_id: int | None=None, write_mode: Literal['overwrite', 'error', 'append', 'upsert', 'update', 'delete']='overwrite', merge_keys: list[str] | None=None, description: str | None=None) -> None: ...

