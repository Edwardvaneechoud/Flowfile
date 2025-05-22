"""
Improved utility to generate comprehensive type stubs for FlowFrame.

This script generates a complete type stub file (.pyi) for the FlowFrame class
that includes both native FlowFrame methods and LazyFrame methods that are
added by the @add_lazyframe_methods decorator.
"""
import os
import inspect
import sys
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Type, get_type_hints, Union, Collection


PASSTHROUGH_METHODS = {
    'collect', 'collect_async', 'profile', 'describe', 'explain',
    'show_graph', 'fetch', 'collect_schema', 'columns', 'dtypes',
    'schema', 'width', 'estimated_size', 'n_chunks', 'is_empty',
    'chunk_lengths', 'get_meta'
}


def format_type_annotation(annotation_obj) -> str:
    """
    Properly format a type annotation object to a string representation.

    Parameters
    ----------
    annotation_obj : Any
        The type annotation object to format

    Returns
    -------
    str
        A properly formatted string representation of the type
    """
    # Handle None/NoneType specially
    if annotation_obj is None or annotation_obj is type(None):
        return "None"

    # Check if it's a class object like <class 'str'>
    if isinstance(annotation_obj, type):
        # Get the module and name
        module = annotation_obj.__module__
        name = annotation_obj.__name__

        # If it's a builtin type, just use the name
        if module == 'builtins':
            return name

        # For other types, use the qualified name
        return f"{name}"

    # Handle string representations of class objects
    elif isinstance(annotation_obj, str):
        # Match patterns like <class 'module.submodule.ClassName'>
        class_match = re.match(r"<class '([^']+)'>", annotation_obj)
        if class_match:
            full_path = class_match.group(1)

            # Special case for NoneType
            if full_path == 'NoneType' or full_path.endswith('.NoneType'):
                return "None"

            class_name = full_path.split('.')[-1]
            return class_name

    # For other cases, convert to string and remove unnecessary details
    str_rep = str(annotation_obj).replace("<class '", "").replace("'>", "")

    # Special case for NoneType
    if str_rep == 'NoneType' or str_rep.endswith('.NoneType'):
        return "None"

    return str_rep


def generate_improved_type_stub(
    flowframe_class: Type,
    output_file: Optional[str] = None,
    include_constructors: bool = True,
    include_inherited: bool = True,
    include_lazyframe: bool = True
) -> str:
    """
    Generate a comprehensive type stub file for FlowFrame.

    This improved version includes:
    1. All methods from the FlowFrame class itself (with proper signatures)
    2. All methods from LazyFrame that would be added by the decorator
    3. Proper handling of the __init__ method

    Parameters
    ----------
    flowframe_class : Type
        The FlowFrame class to examine
    output_file : str, optional
        Path to the output .pyi file. If None, will use "flow_frame.pyi"
        in the current directory.
    include_constructors : bool, default True
        Whether to include __init__ and other special methods
    include_inherited : bool, default True
        Whether to include methods inherited from parent classes
    include_lazyframe : bool, default True
        Whether to include methods from LazyFrame

    Returns
    -------
    str
        Path to the generated stub file
    """
    import polars as pl
    from polars.lazyframe.frame import LazyFrame

    # Analyze LazyFrame methods to determine which return LazyFrame
    lazyframe_returning_methods = set()
    non_lazyframe_methods = set()

    print("Analyzing LazyFrame methods to determine return types...")

    # Get all public methods from LazyFrame
    all_lazyframe_methods = [name for name, method in inspect.getmembers(LazyFrame, predicate=lambda x: inspect.ismethod(x) or inspect.isfunction(x))
                            if not name.startswith('_')]

    # Scan all methods in LazyFrame
    for name in all_lazyframe_methods:
        method = getattr(LazyFrame, name)
        # Get the method signature
        sig = inspect.signature(method)

        # Check if return annotation is available
        if sig.return_annotation is not inspect.Signature.empty:
            return_annotation_str = str(sig.return_annotation)

            # Check different ways the annotation might specify LazyFrame return
            if ('LazyFrame' in return_annotation_str or
                "polars.lazyframe.frame.LazyFrame" in return_annotation_str or
                "<class 'polars.lazyframe.frame.LazyFrame'>" in return_annotation_str):
                lazyframe_returning_methods.add(name)
                print(f"  - Method '{name}' returns LazyFrame")
            else:
                non_lazyframe_methods.add(name)
                print(f"  - Method '{name}' returns {return_annotation_str}")
        else:
            # If no annotation, try to infer from docstring
            if method.__doc__ and "Returns" in method.__doc__:
                doc_lines = method.__doc__.split('\n')
                for i, line in enumerate(doc_lines):
                    if "Returns" in line and i+1 < len(doc_lines):
                        return_line = doc_lines[i+1].lower().strip()
                        if "lazyframe" in return_line:
                            lazyframe_returning_methods.add(name)
                            print(f"  - Method '{name}' returns LazyFrame (inferred from docstring)")
                            break

    # Special case for group_by which returns GroupByFrame
    if 'group_by' in lazyframe_returning_methods:
        lazyframe_returning_methods.remove('group_by')
        print("  - Special case: 'group_by' explicitly set to NOT return FlowFrame")

    print(f"Analysis complete: {len(lazyframe_returning_methods)} LazyFrame-returning methods, "
          f"{len(non_lazyframe_methods)} non-LazyFrame-returning methods")

    # Dictionary of FlowFrame-specific methods that we know return FlowFrame
    flowframe_specific_methods = {
        "cache", "rename", "pivot", "concat", "write_csv", "write_parquet",
        "sink_csv", "sink_parquet", "_create_child_frame", "text_to_rows",
        "_with_flowfile_formula", "_add_number_of_records", "clear", "clone",
        "gather_every", "approx_n_unique", "set_sorted"
    }

    if output_file is None:
        output_file = os.path.join(os.path.dirname(__file__), "flowfile_frame", "flow_frame.pyi")


    # Start building the stub file content
    content = [
        "# This file was auto-generated to provide type information for FlowFrame",
        "# DO NOT MODIFY THIS FILE MANUALLY",
        "import collections",
        "import typing",
        "from typing import Any, List, Dict, Optional, Union, Callable, TypeVar, overload, Literal, Sequence, Tuple, Set, Iterable, ForwardRef, Collection",
        "import polars as pl",
        "from polars.lazyframe.frame import *",  # Import all from LazyFrame
        "from polars._typing import FrameInitTypes, SchemaDefinition, SchemaDict, Orientation",
        "from polars.type_aliases import ColumnNameOrSelector, FillNullStrategy",
        "",
        "import flowfile_frame",
        "from flowfile_core.flowfile.flow_node.flow_node import FlowNode",
        "from flowfile_core.flowfile.FlowfileFlow import FlowGraph",
        "from flowfile_frame import group_frame",
        "from flowfile_frame.expr import Expr, Column",
        "from flowfile_frame.selectors import Selector",
        "",
        "T = TypeVar('T')",
        "FlowFrameT = TypeVar('FlowFrameT', bound='FlowFrame')",
        "# Define NoneType to handle type hints with None",
        "NoneType = type(None)",
        ""
    ]

    # Store the class declaration
    class_name = flowframe_class.__name__
    content.append(f"class {class_name}:")

    # Add core attributes
    content.append("    data: LazyFrame")
    content.append("    flow_graph: FlowGraph")
    content.append("    node_id: int")
    content.append("    parent_node_id: Optional[int]")
    content.append("")

    # Get all methods from FlowFrame including inherited ones
    flowframe_methods = {}

    # Determine which methods to include
    for name, method in inspect.getmembers(flowframe_class, predicate=inspect.isfunction):
        # Skip private methods unless it's __init__
        if name.startswith('_') and name != '__init__' and not include_constructors:
            continue

        # Skip inherited methods if not requested
        if not include_inherited and method.__module__ != flowframe_class.__module__:
            continue

        flowframe_methods[name] = method

    # Get all methods from LazyFrame if requested
    lazyframe_methods = {}
    if include_lazyframe:
        for name, method in inspect.getmembers(LazyFrame, predicate=inspect.ismethod):
            # Skip private methods
            if name.startswith('_'):
                continue

            # Skip methods already in FlowFrame
            if name in flowframe_methods:
                continue

            lazyframe_methods[name] = method
    # Helper function for handling special methods
    def handle_special_methods(name: str, sig: inspect.Signature) -> Optional[str]:
        """Handle methods that need special formatting in the stub file."""

        # Special case for group_by which has a complex signature
        if name == "group_by":
            return "    def group_by(self, *by, description: Optional[str] = None, maintain_order: bool = False, **named_by) -> group_frame.GroupByFrame: ..."

        # Special case for with_columns
        if name == "with_columns":
            return ("    def with_columns(self, exprs: Union[Expr, List[Union[Expr, None]]] = None, *, "
                   "flowfile_formulas: Optional[List[str]] = None, "
                   "output_column_names: Optional[List[str]] = None, "
                   "description: Optional[str] = None) -> 'FlowFrame': ...")

        # No special handling needed
        return None

    # Helper function to determine if a method returns a FlowFrame
    def returns_flowframe(name: str, method) -> bool:
        """
        Determine if a method should return a FlowFrame instance.

        Parameters
        ----------
        name : str
            Name of the method
        method : Callable
            The method itself

        Returns
        -------
        bool
            True if the method should return a FlowFrame, False otherwise
        """
        # First check for FlowFrame-specific methods
        if name in flowframe_specific_methods:
            return True

        # For already implemented methods in FlowFrame, try to use their type hints
        if name in flowframe_methods:
            try:
                hints = get_type_hints(method)
                if 'return' in hints:
                    return_type = hints['return']
                    # Check if return type contains 'FlowFrame'
                    return_type_str = str(return_type)
                    if 'FlowFrame' in return_type_str:
                        return True
                    # Also check if it returns 'GroupByFrame'
                    if 'GroupByFrame' in return_type_str:
                        return False
            except Exception:
                pass

        # Special handling for group_by
        if name == "group_by":
            return False  # group_by returns GroupByFrame, not FlowFrame

        # Use the information we gathered by scanning LazyFrame methods
        if name in lazyframe_returning_methods:
            return True
        if name in non_lazyframe_methods:
            return False

        # For LazyFrame methods we couldn't analyze, use a heuristic
        if not name.startswith("_") and name not in ["collect_schema", "fetch",
                                                     "columns", "dtypes", "schema", "width",
                                                     "describe", "explain", "profile", "show_graph"]:
            return True

        return False

    # Process FlowFrame methods
    for name, method in sorted(flowframe_methods.items()):
        try:
            # Get the signature
            sig = inspect.signature(method)

            # Check if this method needs special handling
            special_signature = handle_special_methods(name, sig)
            if special_signature:
                # Add docstring if available
                if method.__doc__:
                    doc_lines = method.__doc__.strip().split('\n')
                    content.append(f"    # {doc_lines[0].strip()}")

                # Add the special signature
                content.append(special_signature)
                content.append("")
                continue

            # Process parameters - skip 'self'
            processed_params = []
            has_var_keyword = False  # Flag to track if **kwargs is present
            var_keyword_param = None # Hold the **kwargs parameter
            description_param = None # Hold the description parameter if we need to reposition it

            for i, (param_name, param) in enumerate(sig.parameters.items()):
                if i == 0 and param_name == 'self':
                    continue

                # Format parameter with type annotation if available
                param_str = param_name
                if param.annotation is not inspect.Parameter.empty:
                    # Handle Union types more carefully
                    if hasattr(param.annotation, "__origin__") and param.annotation.__origin__ is Union:
                        type_parts = []
                        for arg in param.annotation.__args__:
                            if hasattr(arg, "__name__"):
                                type_parts.append(arg.__name__)
                            else:
                                type_parts.append(str(arg))
                        param_str = f"{param_name}: Union[{', '.join(type_parts)}]"
                    else:
                        # Use our format_type_annotation helper
                        formatted_type = format_type_annotation(param.annotation)
                        param_str = f"{param_name}: {formatted_type}"

                # Add default value if available
                if param.default is not inspect.Parameter.empty:
                    default_repr = repr(param.default)
                    param_str = f"{param_str}={default_repr}"

                # Special handling for **kwargs type parameters
                if param.kind == inspect.Parameter.VAR_KEYWORD:
                    has_var_keyword = True
                    var_keyword_param = f"**{param_name}"
                    continue  # Skip adding this now, will add at the end
                # Special handling for *args type parameters
                elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                    param_str = f"*{param_name}"

                # If this is the description parameter, hold onto it if we have **kwargs
                if param_name == "description" and has_var_keyword:
                    description_param = param_str
                    continue  # Skip adding this now, will add before **kwargs

                processed_params.append(param_str)

            # Add description parameter for non-private methods if not already present and not a special case
            if not has_var_keyword and not name.startswith('_') and not any(p.startswith('description') for p in processed_params) and name not in PASSTHROUGH_METHODS:
                processed_params.append("description: Optional[str] = None")

            # If we have a description param that was saved, add it before var_keyword
            if description_param:
                processed_params.append(description_param)

            # Add the **kwargs parameter at the end if it exists
            if var_keyword_param:
                processed_params.append(var_keyword_param)
            # Determine return type
            if name == "_generate_sort_polars_code":
                return_type = "str"
            elif name == "get_node_settings":
                return_type = "FlowNode"
            elif name == "group_by":
                return_type = "group_frame.GroupByFrame"
            elif returns_flowframe(name, method):

                return_type = f"'{class_name}'"
            else:
                return_type = "Any"

            if sig.return_annotation is not inspect.Signature.empty and return_type != "'FlowFrame'":
                # Use the explicit return annotation if available but format it properly
                return_type = format_type_annotation(sig.return_annotation)
                if return_type != "None" and "'FlowFrame'" not in return_type and "FlowFrame" not in return_type:
                    # Keep the formatted return type
                    pass

            # Build the method signature
            params_str = ", ".join(processed_params)
            method_sig = f"    def {name}(self, {params_str}) -> {return_type}: ..."

            # Add docstring if available
            if method.__doc__:
                doc_lines = method.__doc__.strip().split('\n')
                content.append(f"    # {doc_lines[0].strip()}")

            # Add the method to the stub
            content.append(method_sig)
            content.append("")

        except Exception as e:
            content.append(f"    # Error generating stub for {name}: {str(e)}")
            content.append("")

    # Process LazyFrame methods
    for name, method in sorted(lazyframe_methods.items()):
        try:
            # Skip methods that we know are already properly handled
            if name in flowframe_methods and name != 'collect':
                continue

            # Check if this method needs special handling
            sig = inspect.signature(method)
            special_signature = handle_special_methods(name, sig)
            if special_signature:
                # Add docstring if available
                if method.__doc__:
                    doc_lines = method.__doc__.strip().split('\n')
                    content.append(f"    # {doc_lines[0].strip()}")

                # Add the special signature
                content.append(special_signature)
                content.append("")
                continue

            # For LazyFrame methods, most of them should return FlowFrame when wrapped
            # but only if they originally return a LazyFrame
            return_type = f"'{class_name}'"

            # If it's a method that doesn't return a LazyFrame/FlowFrame, use the original type
            if not returns_flowframe(name, method):
                return_type = "Any"

            # Get signature if possible
            sig = inspect.signature(method)

            # Process parameters - skip first param (cls or self)
            processed_params = []
            has_var_keyword = False  # Flag to track if **kwargs is present
            var_keyword_param = None # Hold the **kwargs parameter
            description_param = None # Hold the description parameter if we need to reposition it
            for i, (param_name, param) in enumerate(sig.parameters.items()):
                if i == 0:  # Skip first parameter (self/cls)
                    continue

                # Format parameter
                param_str = param_name
                if param.annotation is not inspect.Parameter.empty:
                    # Handle Union types
                    if hasattr(param.annotation, "__origin__") and param.annotation.__origin__ is Union:
                        type_parts = []
                        for arg in param.annotation.__args__:
                            formatted_type = format_type_annotation(arg)
                            type_parts.append(formatted_type)
                        param_str = f"{param_name}: Union[{', '.join(type_parts)}]"
                    else:
                        # Format the type annotation
                        formatted_type = format_type_annotation(param.annotation)
                        param_str = f"{param_name}: {formatted_type}"

                # Add default value if available
                if param.default is not inspect.Parameter.empty:
                    default_repr = repr(param.default)
                    param_str = f"{param_str}={default_repr}"

                # Special handling for **kwargs type parameters
                if param.kind == inspect.Parameter.VAR_KEYWORD:
                    has_var_keyword = True
                    var_keyword_param = f"**{param_name}"
                    continue  # Skip adding this now, will add at the end
                # Special handling for *args type parameters
                elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                    param_str = f"*{param_name}"

                # If this is the description parameter, hold onto it if we have **kwargs
                if param_name == "description" and has_var_keyword:
                    description_param = param_str
                    continue  # Skip adding this now, will add before **kwargs

                processed_params.append(param_str)

            # Add description parameter if not already present
            if not any(p.startswith('description') for p in processed_params) and param_name != 'collect_schema':
                breakpoint()
                processed_params.append("description: Optional[str] = None")

            # If we have a description param that was saved, add it before var_keyword
            if description_param:
                processed_params.append(description_param)

            # Add the **kwargs parameter at the end if it exists
            if var_keyword_param:
                processed_params.append(var_keyword_param)

            # Build the method signature
            params_str = ", ".join(processed_params)
            method_sig = f"    def {name}(self, {params_str}) -> {return_type}: ..."

            # Add method to the stub
            if method.__doc__:
                doc_lines = method.__doc__.strip().split('\n')
                content.append(f"    # {doc_lines[0].strip()}")

            content.append(method_sig)
            content.append("")

        except Exception as e:
            content.append(f"    # Error generating stub for {name}: {str(e)}")
            content.append("")

    # Write the stub file
    with open(output_file, "w") as f:
        f.write("\n".join(content))

    return output_file


if __name__ == "__main__":
    import argparse
    from importlib import import_module

    parser = argparse.ArgumentParser(description='Generate comprehensive type stub file for FlowFrame')
    parser.add_argument('--output', '-o', help='Output file path for the stub file')
    parser.add_argument('--module', '-m', default='flowfile_frame.flow_frame', help='Module containing FlowFrame class')
    parser.add_argument('--class-name', '-c', default='FlowFrame', help='Name of the FlowFrame class')
    args = parser.parse_args()

    # Import the FlowFrame class
    try:
        module = import_module(args.module)
        flowframe_class = getattr(module, args.class_name)

        # Generate the type stub
        output_file = generate_improved_type_stub(
            flowframe_class=flowframe_class,
            output_file=args.output
        )

        print(f"Type stub file generated successfully: {output_file}")

    except ImportError:
        print(f"Error: Could not import {args.class_name} from {args.module}")
        print("Please specify the correct module with --module")
        sys.exit(1)
    except AttributeError:
        print(f"Error: {args.class_name} not found in module {args.module}")
        print("Please specify the correct class name with --class-name")
        sys.exit(1)