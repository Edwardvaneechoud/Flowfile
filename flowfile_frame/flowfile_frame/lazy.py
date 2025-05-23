import inspect
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union, get_args, get_origin
import polars as pl
from flowfile_frame.flow_frame import FlowFrame, can_be_expr, generate_node_id
from flowfile_core.flowfile.FlowfileFlow import FlowGraph
from flowfile_frame.expr import Expr
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
        # This is a simplified check; a more robust one might inspect Union args.
        if "DataFrame" in return_annotation or "LazyFrame" in return_annotation:
            return "FlowFrame"
        if "Expr" in return_annotation and "DataFrame" not in return_annotation and "LazyFrame" not in return_annotation : # Avoid matching complex Expr + DF unions to just Expr
            return "Expr"
        raise ValueError(
            f"Function does not return a Frame or Expr. "
            f"Got return annotation: {return_annotation}"
        )


def _analyze_parameters(func_signature: inspect.Signature) -> Tuple[Dict[str, bool], List[Tuple[str, inspect.Parameter]]]:
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


def _process_callable_arg(arg: Any) -> Tuple[str, Any, bool]:
    """
    Process a callable argument for representation and conversion.

    Args:
        arg: The callable argument

    Returns:
        Tuple of (repr_string, processed_arg, convertible_to_code)
    """
    if hasattr(arg, "__name__") and arg.__name__ != "<lambda>":
        return arg.__name__, arg, True
    else:
        # For lambdas or callables without a proper name, use repr() for the string
        # and mark as not directly convertible to simple code representation.
        return repr(arg), arg, False


def _process_expr_arg(arg: Any, can_be_expr_arg: bool) -> Tuple[str, Any]:
    """
    Process an argument that might be convertible to an expression.

    Args:
        arg: The argument to process
        can_be_expr_arg: Whether this parameter can accept Expr types

    Returns:
        Tuple of (repr_string, processed_arg)
    """
    from flowfile_frame.expr import _get_expr_and_repr # Assuming this import is correct

    if can_be_expr_arg:
        arg_expr, repr_str = _get_expr_and_repr(arg)
        return repr_str, arg_expr if arg_expr is not None else arg
    else:
        return repr(arg), arg


def _process_arguments(args: Tuple[Any, ...], param_can_be_expr: Dict[str, bool],
                       param_list: List[Tuple[str, inspect.Parameter]]) -> Tuple[List[str], List[Any], bool]:
    """
    Process positional arguments for the wrapper function.

    Args:
        args: Positional arguments passed to the wrapper
        param_can_be_expr: Dictionary indicating which parameters can be Expr
        param_list: List of parameter names and objects from the original Polars function

    Returns:
        Tuple of (args_repr, pl_args, convertible_to_code)
    """
    args_repr = []
    pl_args = []
    convertible_to_code = True

    for i, arg in enumerate(args):
        can_be_expr_arg = False
        # Ensure we don't go out of bounds for param_list if more args are passed than params (e.g. for *args in wrapped func)
        if i < len(param_list):
            param_name = param_list[i][0]
            # Only consider it if it's not a VAR_POSITIONAL or VAR_KEYWORD, simple positional mapping
            if param_list[i][1].kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.POSITIONAL_ONLY):
                can_be_expr_arg = param_can_be_expr.get(param_name, False)
        # If the original function has *args, subsequent positional arguments might also be expressions.
        # This heuristic might need refinement based on how Polars handles *args with expressions.
        # For now, we assume extra positional args are not expressions unless a specific rule is added.

        if isinstance(arg, Expr) or (isinstance(arg, pl.Expr)): # if it's already an Expr type
            repr_str, processed_arg = _process_expr_arg(arg, True) # Treat as Expr capable
            args_repr.append(repr_str)
            pl_args.append(processed_arg)
        elif callable(arg) and not hasattr(arg, 'expr') and not isinstance(arg, (Expr, pl.Expr)): # Check not already an Expr
            repr_str, processed_arg, is_convertible = _process_callable_arg(arg)
            args_repr.append(repr_str)
            pl_args.append(processed_arg)
            if not is_convertible:
                convertible_to_code = False
        else:
            repr_str, processed_arg = _process_expr_arg(arg, can_be_expr_arg)
            args_repr.append(repr_str)
            pl_args.append(processed_arg)

    return args_repr, pl_args, convertible_to_code


def _process_keyword_arguments(kwargs: Dict[str, Any],
                               param_can_be_expr: Dict[str, bool]) -> Tuple[List[str], Dict[str, Any], bool]:
    """
    Process keyword arguments for the wrapper function.

    Args:
        kwargs: Keyword arguments passed to the wrapper
        param_can_be_expr: Dictionary indicating which parameters can be Expr

    Returns:
        Tuple of (kwargs_repr, pl_kwargs, convertible_to_code)
    """
    kwargs_repr = []
    pl_kwargs = {}
    convertible_to_code = True

    for key, value in kwargs.items():
        can_be_expr_kwarg = param_can_be_expr.get(key, False) # Default to False if key not in signature (e.g. **kwargs)

        if isinstance(value, Expr) or (isinstance(value, pl.Expr)): # if it's already an Expr type
            repr_str, processed_value = _process_expr_arg(value, True)
            kwargs_repr.append(f"{key}={repr_str}")
            pl_kwargs[key] = processed_value
        elif callable(value) and not hasattr(value, 'expr') and not isinstance(value, (Expr, pl.Expr)):
            repr_str, processed_value, is_convertible = _process_callable_arg(value)
            kwargs_repr.append(f"{key}={repr_str}")
            pl_kwargs[key] = processed_value
            if not is_convertible:
                convertible_to_code = False
        else:
            repr_str, processed_value = _process_expr_arg(value, can_be_expr_kwarg)
            kwargs_repr.append(f"{key}={repr_str}")
            pl_kwargs[key] = processed_value
    return kwargs_repr, pl_kwargs, convertible_to_code

def _build_repr_string(polars_func_name: str, args_repr: List[str], kwargs_repr: List[str]) -> str:
    """
    Build the string representation of the function call.

    Args:
        polars_func_name: Name of the polars function
        args_repr: List of argument representations
        kwargs_repr: List of keyword argument representations

    Returns:
        Complete function call representation string
    """
    # Ensure polars_func_name doesn't already contain "pl." if it's from an aliased import or direct attribute
    prefix = "pl."
    if polars_func_name.startswith("pl."):
        prefix = ""

    all_args_str = ", ".join(args_repr)
    all_kwargs_str = ", ".join(kwargs_repr)

    if all_args_str and all_kwargs_str:
        full_repr = f"{prefix}{polars_func_name}({all_args_str}, {all_kwargs_str})"
    elif all_args_str:
        full_repr = f"{prefix}{polars_func_name}({all_args_str})"
    elif all_kwargs_str:
        full_repr = f"{prefix}{polars_func_name}({all_kwargs_str})"
    else:
        full_repr = f"{prefix}{polars_func_name}()"
    return full_repr


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
    from flowfile_core.schemas import input_schema, transform_schema # type: ignore
    from flowfile_frame.utils import create_flow_graph # type: ignore

    node_id = generate_node_id()
    if not flow_graph:
        flow_graph = create_flow_graph()

    # Ensure full_repr is a valid Polars expression string
    # If the polars_func_name is for a top-level pl function (e.g. pl.scan_csv),
    # the full_repr should directly result in a DataFrame/LazyFrame
    polars_code = f"output_df = {full_repr}"

    node_polars_code = input_schema.NodePolarsCode(
        flow_id=flow_graph.flow_id,
        node_id=node_id,
        depending_on_ids=[], # This would need to be dynamic if wrapping methods of a FlowFrame
        description=f"Execute: {polars_func_name}",
        polars_code_input=transform_schema.PolarsCodeInput(polars_code)
    )
    flow_graph.add_polars_code(node_polars_code)

    # Execute the graph up to this node to get the data.
    # This is a simplification; in a real scenario, execution might be deferred.
    # For now, assume get_resulting_data() triggers necessary computations.
    # A placeholder for actual execution and data retrieval:
    # flow_graph.execute_node(node_id) # Or similar mechanism
    # resulting_data_object = flow_graph.get_node_data(node_id)

    # The original code implies get_resulting_data() returns an object with a .data_frame attribute
    # This part is highly dependent on how your flow_graph execution model works.
    # For now, we'll assume a placeholder that it would eventually produce a Polars DataFrame.
    # To make this runnable without full FlowFile execution, we might need to eval the code.
    # This is dangerous and for testing/illustration only.
    temp_df_store = {}
    try:
        # This is a simplified execution model for the sake of getting a DF.
        # In a real system, the flow_graph would handle this.
        # For top-level functions like pl.scan_csv(), they are directly callable.
        # We need to execute the `polars_code` in a context where `pl` is available.
        # `full_repr` should be `pl.function(...)`
        # This direct eval is risky and not for production.
        # It assumes `full_repr` can be directly evaluated.
        # A better way is to call the actual polars_func with pl_args, pl_kwargs
        # which _create_flowframe_result does not currently receive.
        # This function should ideally take polars_func, pl_args, pl_kwargs
        # temp_df = polars_func(*pl_args, **pl_kwargs) # If we had them
        # For now, let's assume `full_repr` is evaluatable, e.g. "pl.DataFrame({...})"
        # Or for pl.scan_parquet("path"), it's directly callable via getattr(pl, polars_func_name)
        # Let's use a placeholder for now, as this function is about graph construction.
        # The .data_frame should be populated by the graph execution logic.
        # A robust solution would involve the flow_graph actually running the operation.
        # Placeholder:
        class MockNode:
            def get_resulting_data(self):
                class MockData:
                    data_frame = pl.DataFrame() # Placeholder
                return MockData()
        if not hasattr(flow_graph, 'get_node'): # simple mock
            flow_graph.get_node = lambda nid: MockNode()

        actual_data = flow_graph.get_node(node_id).get_resulting_data().data_frame

    except Exception as e:
        print(f"Warning: Could not simulate DataFrame creation for graph node {node_id} for {polars_func_name}: {e}")
        actual_data = pl.DataFrame() # Fallback to empty DataFrame

    return FlowFrame(
        data=actual_data,
        flow_graph=flow_graph,
        node_id=node_id,
    )


def _create_expr_result(polars_func: Callable, pl_args: List[Any], pl_kwargs: Dict[str, Any],
                        polars_func_name: str, full_repr: str, is_agg: bool,
                        convertible_to_code: bool) -> "Expr":
    """
    Create an Expr result for functions that return expressions.

    Args:
        polars_func: The actual polars function
        pl_args: Processed positional arguments
        pl_kwargs: Processed keyword arguments
        polars_func_name: Name of the polars function
        full_repr: String representation of the function call
        is_agg: Whether this is an aggregation function
        convertible_to_code: Whether the expression can be converted to code

    Returns:
        Expr instance wrapping the polars expression
    """
    from flowfile_frame.expr import Expr # Ensure Expr is imported

    pl_expr = None
    try:
        # Dereference FlowFrame Expr wrappers if they are in pl_args/pl_kwargs
        processed_pl_args = [arg.expr if isinstance(arg, Expr) else arg for arg in pl_args]
        processed_pl_kwargs = {k: (v.expr if isinstance(v, Expr) else v) for k, v in pl_kwargs.items()}
        pl_expr = polars_func(*processed_pl_args, **processed_pl_kwargs)
    except Exception as e:
        # It's crucial to know if an expression could not be formed, as it impacts later operations.
        # Depending on strictness, this could be an error or a logged warning.
        # For now, print warning and proceed with None, which Expr should handle.
        print(f"Warning: Polars function '{polars_func_name}' failed to create an expression with provided arguments. Error: {e}")
        # full_repr might still be useful if the expression is "abstract"
        # and meant to be translated later rather than immediately evaluated by Polars.

    return Expr(
        pl_expr, # This can be None if the above try-except failed
        repr_str=full_repr,
        agg_func=polars_func_name if is_agg else None,
        is_complex=True, # Assuming most wrapped functions create "complex" expressions
        convertable_to_code=convertible_to_code and (pl_expr is not None) # If pl_expr is None, not really convertible to code
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
    # without calling it: @polars_function_wrapper vs @polars_function_wrapper(...)
    if callable(polars_func_name_or_callable) and not isinstance(polars_func_name_or_callable, str):
        # It's being used as @polars_function_wrapper
        # The first argument is the function to be decorated.
        # We infer polars_func_name from the decorated function's name.
        actual_polars_func_name = polars_func_name_or_callable.__name__
        # The 'func' for the decorator_inner will be this polars_func_name_or_callable

        # Define the actual decorator logic that takes the (dummy) function
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
                        default=None, annotation="TypingOptional['FlowGraph']"
                    )
                    var_kw_idx = next((i for i, p in enumerate(final_params_for_sig) if p.kind == inspect.Parameter.VAR_KEYWORD), -1)
                    if var_kw_idx != -1: final_params_for_sig.insert(var_kw_idx, fg_param)
                    else: final_params_for_sig.append(fg_param)
            elif determined_rt == "Expr":
                wrapper_return_annotation_str = 'Expr'
            else: # Should be caught by _determine_return_type
                wrapper_return_annotation_str = str(original_polars_sig.return_annotation)

            wrapper_sig = inspect.Signature(parameters=final_params_for_sig, return_annotation=wrapper_return_annotation_str)

            @functools.wraps(polars_f) # Wrap the actual Polars function
            def wrapper(*args, **kwargs):
                flow_graph_val = None
                if determined_rt == "FlowFrame":
                    flow_graph_val = kwargs.pop('flow_graph', None)

                args_repr_val, pl_args_val, args_conv = _process_arguments(args, param_can_be_expr_map, param_list_for_processing)
                kwargs_repr_val, pl_kwargs_val, kwargs_conv = _process_keyword_arguments(kwargs, param_can_be_expr_map)
                conv_to_code = args_conv and kwargs_conv
                full_repr_val = _build_repr_string(actual_polars_func_name, args_repr_val, kwargs_repr_val)

                if determined_rt == 'FlowFrame':
                    return _create_flowframe_result(actual_polars_func_name, full_repr_val, flow_graph_val)
                else:
                    return _create_expr_result(polars_f, pl_args_val, pl_kwargs_val, actual_polars_func_name, full_repr_val, is_agg, conv_to_code)

            wrapper.__name__ = wrapper_name
            wrapper.__doc__ = wrapper_doc
            wrapper.__signature__ = wrapper_sig
            return wrapper

        return decorator_inner_for_direct_use(polars_func_name_or_callable)

    else: # Used as @polars_function_wrapper("name", ...) or assigned
        actual_polars_func_name = cast(str, polars_func_name_or_callable)

        def decorator(func: Optional[Callable] = None): # func is the dummy function being decorated, or None if assigned
            polars_f = getattr(pl, actual_polars_func_name, None)
            if polars_f is None:
                raise ValueError(f"Polars function '{actual_polars_func_name}' not found.")

            original_polars_sig = inspect.signature(polars_f)
            # Use explicit return_type if provided, otherwise infer
            determined_rt = return_type or _determine_return_type(original_polars_sig)

            param_can_be_expr_map, param_list_for_processing = _analyze_parameters(original_polars_sig)
            wrapper_name, wrapper_doc = _copy_function_metadata(polars_f, actual_polars_func_name)

            # Construct the signature for the wrapper
            current_params = list(original_polars_sig.parameters.values())
            final_params_for_sig = current_params[:] # Make a mutable copy
            wrapper_return_annotation_str: str

            if determined_rt == "FlowFrame":
                wrapper_return_annotation_str = 'FlowFrame' # String literal for annotation
                # Add flow_graph parameter if it's a FlowFrame function and param doesn't already exist
                if not any(p.name == 'flow_graph' for p in final_params_for_sig):
                    flow_graph_param = inspect.Parameter(
                        name='flow_graph',
                        kind=inspect.Parameter.KEYWORD_ONLY, # Good default, makes it explicit
                        default=None,
                        annotation="TypingOptional['FlowGraph']" # String literal
                    )
                    # Insert flow_graph before **kwargs if it exists, otherwise append
                    var_kw_idx = next((i for i, p in enumerate(final_params_for_sig) if p.kind == inspect.Parameter.VAR_KEYWORD), -1)
                    if var_kw_idx != -1:
                        final_params_for_sig.insert(var_kw_idx, flow_graph_param)
                    else:
                        final_params_for_sig.append(flow_graph_param)
            elif determined_rt == "Expr":
                wrapper_return_annotation_str = 'Expr' # String literal
            else: # Should not be reached if _determine_return_type is robust
                wrapper_return_annotation_str = str(original_polars_sig.return_annotation)

            wrapper_signature = inspect.Signature(
                parameters=final_params_for_sig,
                return_annotation=wrapper_return_annotation_str
            )

            @wraps(polars_f) # Wrap the actual Polars function for metadata like __module__
            def wrapper(*args, **kwargs):
                # Extract flow_graph from kwargs if present (it's defined in wrapper_signature)
                flow_graph_val = None
                if determined_rt == "FlowFrame":
                    flow_graph_val = kwargs.pop('flow_graph', None) # pop if passed

                # Process arguments based on the original polars function's parameter needs
                args_repr_val, pl_args_val, args_convertible_val = _process_arguments(
                    args, param_can_be_expr_map, param_list_for_processing
                )
                kwargs_repr_val, pl_kwargs_val, kwargs_convertible_val = _process_keyword_arguments(
                    kwargs, param_can_be_expr_map
                )

                convertible_to_code_val = args_convertible_val and kwargs_convertible_val
                full_repr_val = _build_repr_string(actual_polars_func_name, args_repr_val, kwargs_repr_val)

                if determined_rt == 'FlowFrame':
                    return _create_flowframe_result(actual_polars_func_name, full_repr_val, flow_graph_val)
                else:  # Expr
                    return _create_expr_result(
                        polars_f, pl_args_val, pl_kwargs_val, actual_polars_func_name,
                        full_repr_val, is_agg, convertible_to_code_val
                    )

            # Set the custom name, docstring (overriding functools.wraps if needed for these)
            wrapper.__name__ = wrapper_name
            wrapper.__doc__ = wrapper_doc

            # Crucially, set the modified signature
            wrapper.__signature__ = wrapper_signature

            # The old way of setting __annotations__ directly is now handled by __signature__
            return wrapper
        return decorator


# Example usage with the new decorator:

# For functions that return FlowFrames
@polars_function_wrapper('read_json', return_type="FlowFrame")
def read_json(*args, flow_graph: Optional[FlowGraph] = None, **kwargs) -> FlowFrame:
    pass


@polars_function_wrapper('read_avro', return_type="FlowFrame")
def read_avro(*args, flow_graph: Optional[FlowGraph] = None, **kwargs) -> FlowFrame:
    pass


@polars_function_wrapper('read_avro', return_type="FlowFrame")
def read_avro(*args, flow_graph: Optional[FlowGraph] = None, **kwargs) -> FlowFrame:
    pass


@polars_function_wrapper('read_ndjson', return_type="FlowFrame")
def read_ndjson(*args, flow_graph: Optional[FlowGraph] = None, **kwargs) -> FlowFrame:
    pass


@polars_function_wrapper('fold', return_type="Expr")
def fold(*args, **kwargs) -> 'Expr':
    pass
