import uuid
import time
import os
import requests
import subprocess
from pathlib import Path
from typing import Iterable, Any, List, Optional

from flowfile_core.flowfile.FlowfileFlow import FlowGraph
from flowfile_core.schemas import schemas
from tempfile import TemporaryDirectory


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


def _parse_inputs_as_iterable(
        inputs: tuple[Any, ...] | tuple[Iterable[Any]],
) -> List[Any]:
    if not inputs:
        return []

    # Treat elements of a single iterable as separate inputs
    if len(inputs) == 1 and _is_iterable(inputs[0]):
        return list(inputs[0])

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
