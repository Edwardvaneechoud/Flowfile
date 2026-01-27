import ast
import inspect
import textwrap
import uuid
from collections.abc import Iterable
from typing import Any

import polars as pl

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import schemas


def _is_iterable(obj: Any) -> bool:
    # Avoid treating strings as iterables in this context
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))


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
        if func.__name__ == "<lambda>":
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


def _is_safely_representable(value: Any) -> bool:
    """Check if a value can be safely round-tripped through repr()."""
    if isinstance(value, (int, float, bool, str, bytes, type(None))):
        return True
    if isinstance(value, (list, tuple)):
        return all(_is_safely_representable(item) for item in value)
    if isinstance(value, dict):
        return all(
            _is_safely_representable(k) and _is_safely_representable(v)
            for k, v in value.items()
        )
    if isinstance(value, set):
        return all(_is_safely_representable(item) for item in value)
    return False


def _extract_lambda_source(func) -> tuple[str | None, str | None]:
    """
    Extract a lambda function's source code and convert it to a named function definition.

    Uses inspect.getsource() + AST parsing to find the lambda's argument list and body,
    then generates a named function definition. Also captures closure variables so that
    the generated code is self-contained.

    Parameters
    ----------
    func : callable
        A lambda function to extract source from.

    Returns
    -------
    tuple[str | None, str | None]
        (function_definition_source, function_name) or (None, None) if extraction fails.
    """
    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        return None, None

    source = textwrap.dedent(source).strip()

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None, None

    # Find all lambda nodes in the source
    lambdas = [node for node in ast.walk(tree) if isinstance(node, ast.Lambda)]
    if not lambdas:
        return None, None

    # Match the lambda to our function based on argument names
    expected_args = list(func.__code__.co_varnames[: func.__code__.co_argcount])
    matched_lambda = None
    for lambda_node in lambdas:
        node_args = [arg.arg for arg in lambda_node.args.args]
        if node_args == expected_args:
            matched_lambda = lambda_node
            break

    if matched_lambda is None:
        # Fall back to first lambda if exact match not found
        matched_lambda = lambdas[0]

    # Generate a deterministic function name from the code object
    func_name = f"_lambda_fn_{abs(hash(func.__code__)) % 100000}"

    # Extract args and body via AST
    args_str = ast.unparse(matched_lambda.args)
    body_str = ast.unparse(matched_lambda.body)

    # Capture closure variables so the generated code is self-contained
    closure_defs: list[str] = []
    if func.__code__.co_freevars and func.__closure__:
        for var_name, cell in zip(func.__code__.co_freevars, func.__closure__):
            try:
                value = cell.cell_contents
            except ValueError:
                # Cell is empty (variable not yet assigned)
                return None, None

            if not _is_safely_representable(value):
                # Cannot safely serialize this closure variable
                return None, None

            closure_defs.append(f"{var_name} = {repr(value)}")

    # Build function definition
    lines: list[str] = []
    if closure_defs:
        lines.extend(closure_defs)
        lines.append("")
    lines.append(f"def {func_name}({args_str}):")
    lines.append(f"    return {body_str}")

    func_def = "\n".join(lines)
    return func_def, func_name


def ensure_inputs_as_iterable(inputs: Any | Iterable[Any]) -> list[Any]:
    """Convert inputs to list, treating strings as single items."""
    if inputs is None or (hasattr(inputs, "__len__") and len(inputs) == 0):
        return []
    # Treat strings/bytes as atomic items, everything else check if iterable
    if isinstance(inputs, (str, bytes)) or not _is_iterable(inputs):
        return [inputs]

    return list(inputs)


def _generate_id() -> int:
    """Generate a simple unique ID for nodes."""
    return int(uuid.uuid4().int % 100000)


def create_flow_graph(flow_id: int = None) -> FlowGraph:
    """
    Create a new FlowGraph instance with a unique flow ID.
    Parameters
       - flow_id (int): Optional flow ID. If not provided, a new unique ID will be generated.
    Returns
       - FlowGraph: A new instance of FlowGraph with the specified or generated flow ID.

    """
    if flow_id is None:
        flow_id = _generate_id()
    flow_settings = schemas.FlowSettings(
        flow_id=flow_id,
        name=f"Flow_{flow_id}",
        path=f"flow_{flow_id}",
        track_history=False,  # Disable undo/redo history for flowfile_frame
    )
    flow_graph = FlowGraph(flow_settings=flow_settings)
    flow_graph.flow_settings.execution_location = (
        "local"  # always create a local frame so that the run time does not attempt to use the flowfile_worker process
    )
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


data = {"c": 0}


def generate_node_id() -> int:
    data["c"] += 1
    return data["c"]


def set_node_id(node_id):
    """Set the node ID to a specific value."""
    data["c"] = node_id
