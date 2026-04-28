# This file was auto-generated to provide type information for flowfile_frame.database.frame_helpers
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

from typing import TYPE_CHECKING, Literal
import polars as pl
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema
from flowfile_frame.database.connection_manager import get_current_user_id
from flowfile_frame.utils import generate_node_id
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.utils import create_flow_graph

def add_read_from_database(flow_graph: FlowGraph, connection_name: str, table_name: str | None=None, schema_name: str | None=None, query: str | None=None, description: str | None=None) -> int: ...

def add_write_to_database(flow_graph: FlowGraph, depends_on_node_id: int, connection_name: str, table_name: str, schema_name: str | None=None, if_exists: Literal['append', 'replace', 'fail']='append', description: str | None=None) -> int: ...

def read_database(connection_name: str, table_name: str | None=None, schema_name: str | None=None, query: str | None=None, flow_graph: FlowGraph | None=None) -> FlowFrame: ...

def write_database(df: pl.LazyFrame, connection_name: str, table_name: str, schema_name: str | None=None, if_exists: Literal['append', 'replace', 'fail']='append') -> None: ...

