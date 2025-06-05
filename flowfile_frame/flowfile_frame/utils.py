import uuid
import time
import os
import requests
import subprocess
from pathlib import Path
import polars as pl
from typing import Iterable, Any, List, Optional

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import schemas
from tempfile import TemporaryDirectory
import inspect
import textwrap

def _is_iterable(obj: Any) -> bool:
    # Avoid treating strings as iterables in this context
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))


def _check_if_convertible_to_code(expressions: List[Any]) -> bool:
    from flowfile_frame.expr import Expr
    for expr in expressions:
        if isinstance(expr, Expr):
            if not expr.convertable_to_code:
                return False
    return True


def _parse_inputs_as_iterable(inputs: tuple[Any, ...] | tuple[Iterable[Any]],) -> List[Any]:
    if not inputs:
        return []

    # Treat elements of a single iterable as separate inputs
    if len(inputs) == 1 and _is_iterable(inputs[0]):
        return list(inputs[0])

    return list(inputs)


def get_pl_expr_from_expr(expr: Any) -> pl.Expr:
    """Get the polars expression from the given expression."""
    return expr.expr


def _get_function_source(func):
    """
    Get the source code of a function if possible.

    Returns:
        tuple: (source_code, is_module_level)
    """
    try:
        # Try to get the source code
        source = inspect.getsource(func)

        # Check if it's a lambda
        if func.__name__ == '<lambda>':
            # Extract just the lambda expression
            # This is tricky as getsource returns the entire line
            return None, False

        # Check if it's a module-level function
        is_module_level = func.__code__.co_flags & 0x10 == 0

        # Dedent the source to remove any indentation
        source = textwrap.dedent(source)

        return source, is_module_level
    except (OSError, TypeError):
        # Can't get source (e.g., built-in function, C extension)
        return None, False


def ensure_inputs_as_iterable(inputs: Any | Iterable[Any]) -> List[Any]:
    """Convert inputs to list, treating strings as single items."""
    if inputs is None or (hasattr(inputs, '__len__') and len(inputs) == 0):
        return []
    # Treat strings/bytes as atomic items, everything else check if iterable
    if isinstance(inputs, (str, bytes)) or not _is_iterable(inputs):
        return [inputs]

    return list(inputs)


def _generate_id() -> int:
    """Generate a simple unique ID for nodes."""
    return int(uuid.uuid4().int % 100000)


def create_flow_graph() -> FlowGraph:
    flow_id = _generate_id()
    flow_settings = schemas.FlowSettings(
        flow_id=flow_id,
        name=f"Flow_{flow_id}",
        path=f"flow_{flow_id}"
    )
    flow_graph = FlowGraph(flow_id=flow_id, flow_settings=flow_settings)
    flow_graph.flow_settings.execution_location = 'local'  # always create a local frame so that the run time does not attempt to use the flowfile_worker process
    return flow_graph


def stringify_values(v: Any) -> str:
    """Convert various types of values to a string representation.

    Strings are wrapped in double quotes with proper escaping.
    All other types are converted to their string representation.
    """
    if isinstance(v, str):
        # Escape any existing double quotes in the string
        escaped_str = v.replace('"', '\\"')
        return '"' + escaped_str + '"'
    elif isinstance(v, bool):
        # Handle booleans explicitly (returns "True" or "False")
        return str(v)
    elif isinstance(v, (int, float, complex, type(None))):
        # Handle numbers and None explicitly
        return str(v)
    else:
        # Handle any other types
        return str(v)
