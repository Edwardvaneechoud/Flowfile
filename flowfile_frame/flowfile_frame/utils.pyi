# This file was auto-generated to provide type information for flowfile_frame.utils
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

import uuid
from collections.abc import Iterable
from typing import Any
import polars as pl
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import schemas
from flowfile_frame.callable_utils import _extract_lambda_source, _get_function_source, _is_safely_representable
from flowfile_frame.expr import Expr

def create_flow_graph(flow_id: int=None) -> FlowGraph: ...

def ensure_inputs_as_iterable(inputs: Any | Iterable[Any]) -> list[Any]: ...

def generate_node_id() -> int: ...

def get_pl_expr_from_expr(expr: Any) -> Expr: ...

def set_node_id(node_id) -> Any: ...

def stringify_values(v: Any) -> str: ...

