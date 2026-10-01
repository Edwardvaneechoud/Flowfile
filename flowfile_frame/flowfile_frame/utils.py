from collections.abc import Iterable
from typing import Any

import polars as pl

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import schemas

# Re-export for backwards compatibility — canonical home is callable_utils
from flowfile_frame.callable_utils import (  # noqa: F401
    _extract_lambda_source,
    _get_function_source,
    _is_safely_representable,
)


def _is_iterable(obj: Any) -> bool:
    # Avoid treating strings as iterables in this context
    return isinstance(obj, Iterable) and not isinstance(obj, str | bytes)


def _check_if_convertible_to_code(expressions: list[Any]) -> bool:
    from flowfile_frame.expr import Expr

    for expr in expressions:
        if isinstance(expr, Expr):
            if not expr.convertable_to_code:
                return False
    return True


def _parse_inputs_as_iterable(
    inputs: tuple[Any, ...] | tuple[Iterable[Any]],
) -> list[Any]:
    if not inputs:
        return []

    if len(inputs) == 1 and _is_iterable(inputs[0]):
        return list(inputs[0])

    return list(inputs)


def get_pl_expr_from_expr(expr: Any) -> pl.Expr:
    """Get the polars expression from the given expression."""
    return expr.expr


def ensure_inputs_as_iterable(inputs: Any | Iterable[Any]) -> list[Any]:
    """Convert inputs to list, treating strings as single items."""
    if inputs is None or (hasattr(inputs, "__len__") and len(inputs) == 0):
        return []
    if isinstance(inputs, str | bytes) or not _is_iterable(inputs):
        return [inputs]

    return list(inputs)


def create_flow_graph(flow_id: int = None) -> FlowGraph:
    """
    Create a new FlowGraph instance with a unique flow ID.
    Parameters
       - flow_id (int): Optional flow ID. If not provided, a new unique ID will be generated.
    Returns
       - FlowGraph: A new instance of FlowGraph with the specified or generated flow ID, no file path,
         history off and local execution (the same defaults as ``FlowGraph()``).

    """
    if flow_id is None:
        return FlowGraph()
    flow_settings = schemas.FlowSettings(
        flow_id=flow_id,
        name=f"Flow_{flow_id}",
        path="",
        track_history=False,
        execution_location="local",
    )
    return FlowGraph(flow_settings=flow_settings)


def stringify_values(v: Any) -> str:
    """Convert various types of values to a string representation.

    Strings are wrapped in double quotes with proper escaping.
    All other types are converted to their string representation.
    """
    if isinstance(v, str):
        escaped_str = v.replace('"', '\\"')
        return '"' + escaped_str + '"'
    elif isinstance(v, bool):
        return str(v)
    elif isinstance(v, int | float | complex | type(None)):
        return str(v)
    else:
        return str(v)


data = {"c": 0}


def generate_node_id() -> int:
    data["c"] += 1
    return data["c"]


def set_node_id(node_id):
    """Set the node ID to a specific value."""
    data["c"] = node_id
