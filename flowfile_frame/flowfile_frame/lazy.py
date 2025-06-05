import inspect
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union
import polars as pl
from flowfile_frame.flow_frame import FlowFrame, can_be_expr, generate_node_id
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_frame.expr import Expr
from flowfile_frame.utils import _get_function_source
from typing import cast
from functools import wraps


def _determine_return_type(func_signature: inspect.Signature) -> Literal["FlowFrame", "Expr"]:
    """
    Determine the return type based on the function signature.

    Args:
        func_signature: The inspect.Signature of the polars function

    Returns:
        Either "FlowFrame" or "Expr" based on the return annotation

    Raises:
        ValueError: If the function doesn't return a Frame or Expr
    """
    return_annotation = str(func_signature.return_annotation)

    if return_annotation in ("DataFrame", "LazyFrame"):
        return "FlowFrame"
    elif return_annotation == "Expr":
        return "Expr"
    else:
        # Allow for type aliases or Union types that might include DataFrame/LazyFrame/Expr
        if "DataFrame" in return_annotation or "LazyFrame" in return_annotation:
            return "FlowFrame"
        if "Expr" in return_annotation and "DataFrame" not in return_annotation and "LazyFrame" not in return_annotation:
            return "Expr"
        raise ValueError(
            f"Function does not return a Frame or Expr. "
            f"Got return annotation: {return_annotation}"
        )


def _analyze_parameters(func_signature: inspect.Signature) -> Tuple[
    Dict[str, bool], List[Tuple[str, inspect.Parameter]]]:
    """
    Analyze function parameters to determine which can accept Expr types.

    Args:
        func_signature: The inspect.Signature of the polars function

    Returns:
        Tuple of (param_can_be_expr dict, param_list)
    """
    param_can_be_expr = {}
    param_list = list(func_signature.parameters.items())

    for param_name, param in param_list:
        param_can_be_expr[param_name] = can_be_expr(param)

    return param_can_be_expr, param_list


def _deep_convert_to_polars_expr(obj: Any) -> Any:
    """
    Recursively convert FlowFile Expr objects to Polars expressions in nested structures.

    Args:
        obj: Object to convert (can be Expr, list, dict, tuple, or any other type)

    Returns:
        The object with all FlowFile Expr instances converted to pl.Expr
    """
    if isinstance(obj, Expr):
        # Convert FlowFile Expr to Polars expr
        return obj.expr
    elif isinstance(obj, list):
        # Recursively process list elements
        return [_deep_convert_to_polars_expr(item) for item in obj]
    elif isinstance(obj, tuple):
        # Recursively process tuple elements
        return tuple(_deep_convert_to_polars_expr(item) for item in obj)
    elif isinstance(obj, dict):
        # Recursively process dictionary values
        return {k: _deep_convert_to_polars_expr(v) for k, v in obj.items()}
    else:
        # Return as-is for other types (including pl.Expr which is already correct)
        return obj


def _deep_get_repr(obj: Any, can_be_expr: bool = False) -> str:
    """
    Get string representation of an object, handling nested structures with Expr objects.

    Args:
        obj: Object to get representation for
        can_be_expr: Whether this parameter can accept Expr types

    Returns:
        String representation suitable for code generation
    """
    from flowfile_frame.expr import _get_expr_and_repr
    if isinstance(obj, Expr):
        # FlowFile Expr - get its representation
        _, repr_str = _get_expr_and_repr(obj)
        return repr_str
    elif isinstance(obj, pl.Expr):
        # Polars Expr - try to get representation through _get_expr_and_repr
        _, repr_str = _get_expr_and_repr(obj)
        return repr_str
    elif isinstance(obj, list):
        # Recursively process list elements
        inner_reprs = [_deep_get_repr(item, can_be_expr) for item in obj]
        return f"[{', '.join(inner_reprs)}]"
    elif isinstance(obj, tuple):
        # Recursively process tuple elements
        inner_reprs = [_deep_get_repr(item, can_be_expr) for item in obj]
        return f"({', '.join(inner_reprs)})"
    elif isinstance(obj, dict):
        # Recursively process dictionary items
        items = [f"{repr(k)}: {_deep_get_repr(v, can_be_expr)}" for k, v in obj.items()]
        return f"{{{', '.join(items)}}}"
    elif callable(obj) and hasattr(obj, "__name__") and obj.__name__ != "<lambda>":
        # Named function
        return obj.__name__
    elif can_be_expr:
        # Try to convert to expr and get representation
        expr_obj, repr_str = _get_expr_and_repr(obj)
        return repr_str
    else:
        # Default representation
        return repr(obj)


def _process_callable_arg(arg: Any) -> Tuple[str, Any, bool, Optional[str]]:
    """
    Process a callable argument for representation and conversion.

    Args:
        arg: The callable argument

    Returns:
        Tuple of (repr_string, processed_arg, convertible_to_code, function_source)
    """
    function_source = None
    if hasattr(arg, "__name__") and arg.__name__ != "<lambda>":
        # Try to get function source
        try:
            function_source, _ = _get_function_source(arg)
        except:
            pass
        return arg.__name__, arg, True, function_source
    else:
        # For lambdas or callables without a proper name
        return repr(arg), arg, False, None


def _process_argument(arg: Any, can_be_expr: bool) -> Tuple[str, Any, bool, Optional[str]]:
    """
    Process a single argument, handling all types including nested structures.

    Args:
        arg: The argument to process
        can_be_expr: Whether this parameter can accept Expr types

    Returns:
        Tuple of (repr_string, processed_arg_for_polars, convertible_to_code, function_source)
    """
    # Special handling for callables (but not Expr objects which might be callable)
    if callable(arg) and not isinstance(arg, (Expr, pl.Expr)) and not hasattr(arg, 'expr'):
        return _process_callable_arg(arg)
    repr_str = _deep_get_repr(arg, can_be_expr)

    processed_arg = _deep_convert_to_polars_expr(arg)

    convertible = not (callable(arg) and hasattr(arg, "__name__") and arg.__name__ == "<lambda>")

    return repr_str, processed_arg, convertible, None


def _process_arguments(args: Tuple[Any, ...], param_can_be_expr: Dict[str, bool],
                       param_list: List[Tuple[str, inspect.Parameter]]) -> Tuple[List[str], List[Any], bool, List[str]]:
    """
    Process positional arguments for the wrapper function.

    Args:
        args: Positional arguments passed to the wrapper
        param_can_be_expr: Dictionary indicating which parameters can be Expr
        param_list: List of parameter names and objects from the original Polars function

    Returns:
        Tuple of (args_repr, pl_args, convertible_to_code, function_sources)
    """
    args_repr = []
    pl_args = []
    convertible_to_code = True
    function_sources = []

    for i, arg in enumerate(args):
        can_be_expr_arg = False
        if i < len(param_list):
            param_name = param_list[i][0]
            if param_list[i][1].kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.POSITIONAL_ONLY):
                can_be_expr_arg = param_can_be_expr.get(param_name, False)

        repr_str, processed_arg, is_convertible, func_source = _process_argument(arg, can_be_expr_arg)
        args_repr.append(repr_str)
        pl_args.append(processed_arg)
        if not is_convertible:
            convertible_to_code = False
        if func_source:
            function_sources.append(func_source)

    return args_repr, pl_args, convertible_to_code, function_sources


def _process_keyword_arguments(kwargs: Dict[str, Any],
                               param_can_be_expr: Dict[str, bool]) -> Tuple[List[str], Dict[str, Any], bool, List[str]]:
    """
    Process keyword arguments for the wrapper function.

    Args:
        kwargs: Keyword arguments passed to the wrapper
        param_can_be_expr: Dictionary indicating which parameters can be Expr

    Returns:
        Tuple of (kwargs_repr, pl_kwargs, convertible_to_code, function_sources)
    """
    kwargs_repr = []
    pl_kwargs = {}
    convertible_to_code = True
    function_sources = []

    for key, value in kwargs.items():
        can_be_expr_kwarg = param_can_be_expr.get(key, False)

        repr_str, processed_value, is_convertible, func_source = _process_argument(value, can_be_expr_kwarg)
        kwargs_repr.append(f"{key}={repr_str}")
        pl_kwargs[key] = processed_value
        if not is_convertible:
            convertible_to_code = False
        if func_source:
            function_sources.append(func_source)

    return kwargs_repr, pl_kwargs, convertible_to_code, function_sources


def _build_repr_string(polars_func_name: str, args_repr: List[str], kwargs_repr: List[str],
                       function_sources: List[str] = None) -> str:
    """
    Build the string representation of the function call.

    Args:
        polars_func_name: Name of the polars function
        args_repr: List of argument representations
        kwargs_repr: List of keyword argument representations
        function_sources: List of function source code strings

    Returns:
        Complete function call representation string
    """
    prefix = "pl."
    if polars_func_name.startswith("pl."):
        prefix = ""

    all_args_str = ", ".join(args_repr)
    all_kwargs_str = ", ".join(kwargs_repr)

    if all_args_str and all_kwargs_str:
        call_repr = f"{prefix}{polars_func_name}({all_args_str}, {all_kwargs_str})"
    elif all_args_str:
        call_repr = f"{prefix}{polars_func_name}({all_args_str})"
    elif all_kwargs_str:
        call_repr = f"{prefix}{polars_func_name}({all_kwargs_str})"
    else:
        call_repr = f"{prefix}{polars_func_name}()"

    # If we have function sources, prepend them with separator
    if function_sources:
        # Remove duplicates while preserving order
        unique_sources = []
        seen = set()
        for source in function_sources:
            if source not in seen:
                seen.add(source)
                unique_sources.append(source)

        functions = "# Function definitions\n" + "\n\n".join(unique_sources)
        return functions + "\n\n─────SPLIT─────\n\noutput_df = " + call_repr
    else:
        return call_repr


def _create_flowframe_result(polars_func_name: str, full_repr: str, flow_graph: Optional[Any]) -> "FlowFrame":
    """
    Create a FlowFrame result for functions that return DataFrames/LazyFrames.

    Args:
        polars_func_name: Name of the polars function
        full_repr: String representation of the function call
        flow_graph: Optional flow graph to use

    Returns:
        FlowFrame instance with the operation added to the graph
    """
    from flowfile_core.schemas import input_schema, transform_schema
    from flowfile_frame.utils import create_flow_graph

    node_id = generate_node_id()
    if not flow_graph:
        flow_graph = create_flow_graph()

    # Check if we have function definitions (indicated by SPLIT separator)
    if "─────SPLIT─────" in full_repr:
        polars_code = full_repr
    else:
        polars_code = f"output_df = {full_repr}"

    node_polars_code = input_schema.NodePolarsCode(
        flow_id=flow_graph.flow_id,
        node_id=node_id,
        depending_on_ids=[],
        description=f"Execute: {polars_func_name}",
        polars_code_input=transform_schema.PolarsCodeInput(polars_code)
    )
    flow_graph.add_polars_code(node_polars_code)

    try:
        class MockNode:
            def get_resulting_data(self):
                class MockData:
                    data_frame = pl.DataFrame()

                return MockData()

        if not hasattr(flow_graph, 'get_node'):
            flow_graph.get_node = lambda nid: MockNode()

        actual_data = flow_graph.get_node(node_id).get_resulting_data().data_frame

    except Exception as e:
        print(f"Warning: Could not simulate DataFrame creation for graph node {node_id} for {polars_func_name}: {e}")
        actual_data = pl.DataFrame()

    return FlowFrame(
        data=actual_data,
        flow_graph=flow_graph,
        node_id=node_id,
    )


def _check_for_non_serializable_functions(args: List[Any], kwargs: Dict[str, Any]) -> List[str]:
    """
    Check for non-serializable functions in arguments and return warnings.

    Args:
        args: Processed arguments
        kwargs: Processed keyword arguments

    Returns:
        List of warning messages for non-serializable functions
    """
    warnings = []

    def check_value(value: Any, path: str) -> None:
        """Recursively check for non-serializable functions."""
        if callable(value) and not isinstance(value, (type, pl.Expr)):
            # Check if it's a lambda or local function
            if hasattr(value, '__name__'):
                if value.__name__ == '<lambda>':
                    warnings.append(
                        f"Lambda function found at {path}. "
                        "This will cause 'serialization not supported for this opaque function' error. "
                        "Consider using a named function at module level instead."
                    )
                elif hasattr(value, '__code__') and value.__code__.co_flags & 0x10:  # CO_NESTED flag
                    # Check if it's a local/nested function (excluding top-level module functions)
                    if value.__code__.co_name != '<module>':  # Ensure it's not a module itself
                        warnings.append(
                            f"Local function '{value.__name__}' found at {path}. "
                            "This may cause serialization issues. "
                            "Consider defining it at module level instead."
                        )
        elif isinstance(value, list):
            for i, item in enumerate(value):
                check_value(item, f"{path}[{i}]")
        elif isinstance(value, tuple):
            for i, item in enumerate(value):
                check_value(item, f"{path}[{i}]")
        elif isinstance(value, dict):
            for k, v in value.items():
                check_value(v, f"{path}[{k!r}]")

    # Check positional arguments
    for i, arg in enumerate(args):
        check_value(arg, f"argument {i}")

    # Check keyword arguments
    for key, value in kwargs.items():
        check_value(value, f"keyword argument '{key}'")

    return warnings


def _create_expr_result(polars_func: Callable, pl_args: List[Any], pl_kwargs: Dict[str, Any],
                        polars_func_name: str, full_repr: str, is_agg: bool,
                        convertible_to_code: bool, function_sources: List[str] = None) -> "Expr":
    """
    Create an Expr result for functions that return expressions.

    Note: pl_args and pl_kwargs should already have all Expr objects converted to pl.Expr

    Args:
        polars_func: The actual polars function
        pl_args: Processed positional arguments (already converted)
        pl_kwargs: Processed keyword arguments (already converted)
        polars_func_name: Name of the polars function
        full_repr: String representation of the function call
        is_agg: Whether this is an aggregation function
        convertible_to_code: Whether the expression can be converted to code
        function_sources: List of function source code strings

    Returns:
        Expr instance wrapping the polars expression
    """
    from flowfile_frame.expr import Expr
    import warnings

    # Check for non-serializable functions
    serialization_warnings = _check_for_non_serializable_functions(pl_args, pl_kwargs)

    pl_expr = None
    serialization_error = None

    try:
        # Try to create the expression
        pl_expr = polars_func(*pl_args, **pl_kwargs)

        # Try to serialize to check if it will work in FlowFile
        if pl_expr is not None and serialization_warnings:
            try:
                # Test serialization
                import io
                buffer = io.BytesIO()
                pl_expr.serialize(file=buffer, format='json')
            except Exception as e:
                serialization_error = str(e)

    except Exception as e:
        print(
            f"Warning: Polars function '{polars_func_name}' failed to create an expression with provided arguments. Error: {e}")
        if "serialization not supported" in str(e).lower():
            serialization_error = str(e)

    # Issue warnings if we found non-serializable functions
    if serialization_warnings:
        warnings.warn(
            f"\n⚠️  SERIALIZATION WARNING for {polars_func_name}:\n" +
            "\n".join(f"  • {w}" for w in serialization_warnings) +
            "\n\nThis expression cannot be saved to a FlowFile format and will need to be " +
            "recreated from scratch when loading the flow. The expression will work in the " +
            "current session but won't persist.\n" +
            (f"\nActual error from Polars: {serialization_error}" if serialization_error else ""),
            category=UserWarning,
            stacklevel=3
        )

    # Extract just the expression part without function definitions for repr_str
    if function_sources and "─────SPLIT─────" in full_repr:
        # Get the part after the split
        repr_str = full_repr.split("─────SPLIT─────")[-1].strip()
        if repr_str.startswith("output_df = "):
            repr_str = repr_str[len("output_df = "):]
    else:
        repr_str = full_repr

    return Expr(
        pl_expr,
        repr_str=repr_str,
        agg_func=polars_func_name if is_agg else None,
        is_complex=True,
        convertable_to_code=convertible_to_code and (pl_expr is not None),
        _function_sources=function_sources  # Pass function sources to Expr
    )


def _copy_function_metadata(original_func: Callable, polars_func_name: str) -> Tuple[str, str]:
    """
    Copy metadata from the original polars function.

    Args:
        original_func: The original polars function
        polars_func_name: Name of the polars function

    Returns:
        Tuple of (function_name, docstring)
    """
    original_doc = getattr(original_func, '__doc__', None) or ""
    enhanced_doc = f"""FlowFile wrapper for pl.{polars_func_name}.

Original Polars documentation:
{original_doc}

Note: This is a FlowFile wrapper. If it returns a FlowFrame, it may accept an additional
'flow_graph: Optional[FlowGraph]' keyword argument to associate the operation with a specific graph.
Otherwise, a new graph is implicitly created or an existing one is used if chained from a FlowFrame method.
Wrapped functions returning Exprs will produce FlowFile Expr objects.
    """
    return polars_func_name, enhanced_doc.strip()


def polars_function_wrapper(
        polars_func_name_or_callable: Union[str, Callable],
        is_agg: bool = False,
        return_type: Optional[Literal["FlowFrame", "Expr"]] = None
):
    """
    Create a wrapper for a polars function that returns either a FlowFrame or Expr.

    Args:
        polars_func_name_or_callable: Name of the polars function to wrap (str) or
                                      the function itself if using @polars_function_wrapper directly.
        is_agg: Whether this is an aggregation function (relevant for Expr results).
        return_type: Expected return type ("FlowFrame" or "Expr"). If None, will be inferred.

    Returns:
        Wrapped function that integrates with the FlowFile framework.

    Raises:
        ValueError: If the polars function is not found or doesn't return Frame/Expr.
    """
    # Handle the case where the decorator is used as @polars_function_wrapper directly
    if callable(polars_func_name_or_callable) and not isinstance(polars_func_name_or_callable, str):
        actual_polars_func_name = polars_func_name_or_callable.__name__

        def decorator_inner_for_direct_use(func_to_decorate: Callable):
            polars_f = getattr(pl, actual_polars_func_name, None)
            if polars_f is None:
                raise ValueError(f"Polars function '{actual_polars_func_name}' (inferred) not found.")

            original_polars_sig = inspect.signature(polars_f)
            determined_rt = return_type or _determine_return_type(original_polars_sig)
            param_can_be_expr_map, param_list_for_processing = _analyze_parameters(original_polars_sig)
            wrapper_name, wrapper_doc = _copy_function_metadata(polars_f, actual_polars_func_name)

            current_params = list(original_polars_sig.parameters.values())
            final_params_for_sig = current_params[:]
            wrapper_return_annotation_str: str

            if determined_rt == "FlowFrame":
                wrapper_return_annotation_str = 'FlowFrame'
                if not any(p.name == 'flow_graph' for p in final_params_for_sig):
                    fg_param = inspect.Parameter(
                        name='flow_graph', kind=inspect.Parameter.KEYWORD_ONLY,
                        default=None, annotation=Optional[FlowGraph]  # Corrected annotation
                    )
                    var_kw_idx = next(
                        (i for i, p in enumerate(final_params_for_sig) if p.kind == inspect.Parameter.VAR_KEYWORD), -1)
                    if var_kw_idx != -1:
                        final_params_for_sig.insert(var_kw_idx, fg_param)
                    else:
                        final_params_for_sig.append(fg_param)
            elif determined_rt == "Expr":
                wrapper_return_annotation_str = 'Expr'
            else:
                wrapper_return_annotation_str = str(original_polars_sig.return_annotation)

            wrapper_sig = inspect.Signature(parameters=final_params_for_sig,
                                            return_annotation=wrapper_return_annotation_str)

            @wraps(polars_f)
            def wrapper(*args, **kwargs):
                flow_graph_val = None
                if determined_rt == "FlowFrame":
                    flow_graph_val = kwargs.pop('flow_graph', None)

                args_repr_val, pl_args_val, args_conv, args_func_sources = _process_arguments(
                    args, param_can_be_expr_map, param_list_for_processing
                )
                kwargs_repr_val, pl_kwargs_val, kwargs_conv, kwargs_func_sources = _process_keyword_arguments(
                    kwargs, param_can_be_expr_map
                )

                conv_to_code = args_conv and kwargs_conv
                all_func_sources = args_func_sources + kwargs_func_sources
                full_repr_val = _build_repr_string(
                    actual_polars_func_name, args_repr_val, kwargs_repr_val, all_func_sources
                )

                if determined_rt == 'FlowFrame':
                    return _create_flowframe_result(actual_polars_func_name, full_repr_val, flow_graph_val)
                else:  # Expr
                    return _create_expr_result(
                        polars_f, pl_args_val, pl_kwargs_val, actual_polars_func_name,
                        full_repr_val, is_agg, conv_to_code, all_func_sources  # Pass function sources
                    )

            wrapper.__name__ = wrapper_name
            wrapper.__doc__ = wrapper_doc
            wrapper.__signature__ = wrapper_sig
            return wrapper

        return decorator_inner_for_direct_use(polars_func_name_or_callable)

    else:  # Used as @polars_function_wrapper("name", ...) or assigned
        actual_polars_func_name = cast(str, polars_func_name_or_callable)

        def decorator(func: Optional[Callable] = None):  # func is the decorated placeholder
            polars_f = getattr(pl, actual_polars_func_name, None)
            if polars_f is None:
                raise ValueError(f"Polars function '{actual_polars_func_name}' not found.")

            original_polars_sig = inspect.signature(polars_f)
            determined_rt = return_type or _determine_return_type(original_polars_sig)

            param_can_be_expr_map, param_list_for_processing = _analyze_parameters(original_polars_sig)
            wrapper_name, wrapper_doc = _copy_function_metadata(polars_f, actual_polars_func_name)

            current_params = list(original_polars_sig.parameters.values())
            final_params_for_sig = current_params[:]
            wrapper_return_annotation_str: str

            if determined_rt == "FlowFrame":
                wrapper_return_annotation_str = 'FlowFrame'
                if not any(p.name == 'flow_graph' for p in final_params_for_sig):
                    flow_graph_param = inspect.Parameter(
                        name='flow_graph',
                        kind=inspect.Parameter.KEYWORD_ONLY,
                        default=None,
                        annotation=Optional[FlowGraph]  # Corrected annotation
                    )
                    var_kw_idx = next(
                        (i for i, p in enumerate(final_params_for_sig) if p.kind == inspect.Parameter.VAR_KEYWORD), -1)
                    if var_kw_idx != -1:
                        final_params_for_sig.insert(var_kw_idx, flow_graph_param)
                    else:
                        final_params_for_sig.append(flow_graph_param)
            elif determined_rt == "Expr":
                wrapper_return_annotation_str = 'Expr'
            else:
                wrapper_return_annotation_str = str(original_polars_sig.return_annotation)

            wrapper_signature = inspect.Signature(
                parameters=final_params_for_sig,
                return_annotation=wrapper_return_annotation_str
            )

            @wraps(polars_f)
            def wrapper(*args, **kwargs):
                flow_graph_val = None
                if determined_rt == "FlowFrame":
                    flow_graph_val = kwargs.pop('flow_graph', None)

                args_repr_val, pl_args_val, args_convertible_val, args_func_sources = _process_arguments(
                    args, param_can_be_expr_map, param_list_for_processing
                )
                kwargs_repr_val, pl_kwargs_val, kwargs_convertible_val, kwargs_func_sources = _process_keyword_arguments(
                    kwargs, param_can_be_expr_map
                )

                convertible_to_code_val = args_convertible_val and kwargs_convertible_val  # Correct variable for this scope
                all_func_sources = args_func_sources + kwargs_func_sources

                full_repr_val = _build_repr_string(
                    actual_polars_func_name, args_repr_val, kwargs_repr_val, all_func_sources  # Corrected variable
                )

                if determined_rt == 'FlowFrame':
                    return _create_flowframe_result(actual_polars_func_name, full_repr_val, flow_graph_val)
                else:  # Expr
                    return _create_expr_result(polars_f, pl_args_val, pl_kwargs_val, actual_polars_func_name,
                                               full_repr_val, is_agg, convertible_to_code_val,
                                               all_func_sources)  # Pass function sources

            wrapper.__name__ = wrapper_name
            wrapper.__doc__ = wrapper_doc
            wrapper.__signature__ = wrapper_signature
            # If func is provided (typically by decorator syntax), it's usually for @wraps or similar.
            # Here, we are replacing func entirely, so we just return the new wrapper.
            return wrapper

        return decorator


# Example usage with the new decorator (from original snippet):

# For functions that return FlowFrames
@polars_function_wrapper('read_json', return_type="FlowFrame")
def read_json(*args, flow_graph: Optional[FlowGraph] = None, **kwargs) -> FlowFrame:
    pass


@polars_function_wrapper('read_avro', return_type="FlowFrame")
def read_avro(*args, flow_graph: Optional[FlowGraph] = None, **kwargs) -> FlowFrame:
    pass


@polars_function_wrapper('read_ndjson', return_type="FlowFrame")
def read_ndjson(*args, flow_graph: Optional[FlowGraph] = None, **kwargs) -> FlowFrame:
    pass


@polars_function_wrapper('fold', return_type="Expr")
def fold(*args, **kwargs) -> 'Expr':  # Type hint 'Expr' refers to flowfile_frame.expr.Expr
    pass