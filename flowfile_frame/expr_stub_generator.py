"""
Utility to generate comprehensive type stubs for Expr and Column classes.

This script generates a complete type stub file (.pyi) for the Expr and related classes
that includes both native methods and any methods dynamically added.
"""
import os
import inspect
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union, get_type_hints


def format_type_annotation(annotation_obj) -> str:
    """
    Properly format a type annotation object to a string representation.
    """
    # Handle None/NoneType specially
    if annotation_obj is None or annotation_obj is type(None):
        return "None"

    # For class objects like <class 'str'>
    if isinstance(annotation_obj, type):
        module = annotation_obj.__module__
        name = annotation_obj.__name__
        return name if module == 'builtins' else name

    # Handle strings
    if isinstance(annotation_obj, str):
        # Clean up class references
        class_match = re.match(r"<class '([^']+)'>", annotation_obj)
        if class_match:
            full_path = class_match.group(1)
            return "None" if full_path == 'NoneType' or full_path.endswith('.NoneType') else full_path.split('.')[-1]
        return annotation_obj

    # For other cases, convert to string and clean up
    str_rep = str(annotation_obj).replace("<class '", "").replace("'>", "")
    return "None" if str_rep == 'NoneType' or str_rep.endswith('.NoneType') else str_rep


def process_method_signature(method, name, class_name) -> Tuple[str, str, List[str]]:
    """
    Process a method and generate its stub signature.

    Returns:
    - docstring: Single line docstring or empty string
    - method_sig: The method signature for the stub
    - lines: Additional lines to add (like empty line for readability)
    """
    lines = []
    try:
        # Get the signature
        sig = inspect.signature(method)

        # Process parameters - skip 'self'
        processed_params = []
        for i, (param_name, param) in enumerate(sig.parameters.items()):
            if i == 0 and param_name == 'self':
                continue

            # Format parameter with type annotation
            param_str = param_name
            if param.annotation is not inspect.Parameter.empty:
                # Handle Union types
                if hasattr(param.annotation, "__origin__") and param.annotation.__origin__ is Union:
                    type_parts = []
                    for arg in param.annotation.__args__:
                        type_parts.append(format_type_annotation(arg))
                    param_str = f"{param_name}: Union[{', '.join(type_parts)}]"
                else:
                    formatted_type = format_type_annotation(param.annotation)
                    param_str = f"{param_name}: {formatted_type}"

            # Add default value if available
            if param.default is not inspect.Parameter.empty:
                default_repr = repr(param.default)
                param_str = f"{param_str}={default_repr}"

            # Handle special parameter types (*args, **kwargs)
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                param_str = f"**{param_name}"
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                param_str = f"*{param_name}"

            processed_params.append(param_str)

        # Determine return type
        return_type = "Any"
        if sig.return_annotation is not inspect.Parameter.empty:
            return_type = format_type_annotation(sig.return_annotation)
        elif name in ["__add__", "__sub__", "__mul__", "__truediv__", "__floordiv__",
                      "__mod__", "__pow__", "__and__", "__or__", "__eq__", "__ne__",
                      "__gt__", "__lt__", "__ge__", "__le__", "__invert__",
                      "sum", "mean", "min", "max", "count", "first", "last",
                      "is_null", "is_not_null", "filter", "alias", "fill_null", "fill_nan",
                      "over", "sort", "cast"]:
            return_type = f"'{class_name}'"
        elif name in ["str", "dt", "name"] and class_name == "Expr":
            # Special properties
            property_types = {
                "str": "StringMethods",
                "dt": "DateTimeMethods",
                "name": "ExprNameNameSpace"
            }
            return_type = property_types.get(name, "Any")

        # Build the method signature
        params_str = ", ".join(processed_params)
        method_sig = f"    def {name}(self, {params_str}) -> {return_type}: ..."

        # Extract docstring if available
        docstring = ""
        if method.__doc__:
            doc_lines = method.__doc__.strip().split('\n')
            docstring = f"    # {doc_lines[0].strip()}"

        # Add an empty line after each method for readability
        lines.append("")
        return docstring, method_sig, lines

    except Exception as e:
        return f"    # Error generating stub for {name}: {str(e)}", "", []

def generate_expr_type_stub(
        expr_module,
        output_file: Optional[str] = None,
        include_constructors: bool = True,
        include_inherited: bool = True,
        include_polars_methods: bool = True
) -> str:
    """
    Generate a comprehensive type stub file for Expr and related classes.
    """
    import polars as pl

    if output_file is None:
        output_file = os.path.join(os.path.dirname(__file__), "flowfile_frame", "expr.pyi")


    # Extract classes from the module
    class_map = {
        "Expr": getattr(expr_module, "Expr"),
        "Column": getattr(expr_module, "Column"),
        "StringMethods": getattr(expr_module, "StringMethods"),
        "DateTimeMethods": getattr(expr_module, "DateTimeMethods"),
        "When": getattr(expr_module, "When")
    }

    # Discover top-level functions dynamically
    top_level_functions = []
    for name, obj in inspect.getmembers(expr_module):
        if inspect.isfunction(obj) and not name.startswith('_') and obj.__module__ == expr_module.__name__:
            top_level_functions.append(name)
    # Start building the stub file content
    content = [
        "# This file was auto-generated to provide type information for Expr",
        "# DO NOT MODIFY THIS FILE MANUALLY",
        "from __future__ import annotations",
        "",
        "from typing import Any, List, Optional, Union,  TypeVar, TYPE_CHECKING",
        "import polars as pl",
        "from polars.expr.expr import Expr as PolarsExpr",
        "from polars.expr.string import ExprStringNameSpace",
        "",
        "if TYPE_CHECKING:",
        "    from collections.abc import Iterable",
        "    from io import IOBase",
        "    from polars import DataFrame, LazyFrame, Series",
        "    from polars._typing import *",
        "",
        "    if sys.version_info >= (3, 11):",
        "        from typing import Concatenate, ParamSpec",
        "    else:",
        "        from typing_extensions import Concatenate, ParamSpec",
        "    T = TypeVar('T')",
        "    P = ParamSpec('P')",
        "    from flowfile_core.schemas import transform_schema",

        "import flowfile_frame",
        "from flowfile_frame.selectors import Selector",
        "from flowfile_frame.expr_name import ExprNameNameSpace",
        "",
        "# Define NoneType to handle type hints with None",
        "NoneType = type(None)",
        "",
        "ExprOrStr = Union['Expr', str]",
        "ExprOrStrList = List[ExprOrStr]",
        "ExprStrOrList = Union[ExprOrStr, ExprOrStrList]",
        ""
    ]

    # Discover polars Expr methods dynamically
    common_polars_methods = set()
    for name, method in inspect.getmembers(pl.Expr):
        if callable(method) and not isinstance(method, property):
            common_polars_methods.add(name)

    def process_class(cls_name, cls, is_subclass=False):
        parent_class = f"({cls.__base__.__name__})" if is_subclass else ""
        class_lines = [f"class {cls_name}{parent_class}:"]
        # Add class attributes (properties)
        properties_added = False
        for name, value in inspect.getmembers(cls, lambda x: isinstance(x, property)):
            properties_added = True
            return_type = "Any"
            if hasattr(value.fget, "__annotations__") and "return" in value.fget.__annotations__:
                return_type = format_type_annotation(value.fget.__annotations__["return"])
            class_lines.append(f"    {name}: {return_type}")
        # Add other attributes

        attrs_added = False
        for name, value in vars(cls).items():
            if not callable(value) and not isinstance(value, property):
                type_annotation = vars(cls)['__annotations__'].get(name, "Any")

                attrs_added = True
                class_lines.append(f"    {name}: {type_annotation}")
        # Add an empty line after attributes if any were added
        if properties_added or attrs_added:
            class_lines.append("")

        # Process methods
        existing_methods = set()
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            # Skip private methods unless it's __init__
            if name.startswith('_') and name != '__init__' and not include_constructors:
                continue
            # Skip inherited methods if not requested
            if not include_inherited and method.__module__ != cls.__module__:
                continue

            docstring, method_sig, extra_lines = process_method_signature(method, name, cls_name)
            if docstring:
                class_lines.append(docstring)
            if method_sig:
                class_lines.append(method_sig.replace(" | NoDefault=<no_default>", ""))
                existing_methods.add(name)
            class_lines.extend(extra_lines)

        # If it's the Expr class, add common polars.Expr methods
        if include_polars_methods and cls_name == "Expr":
            for method_name in common_polars_methods:

                if method_name not in existing_methods:
                    class_lines.append(f"    def {method_name}(self, *args, **kwargs) -> 'Expr': ...")
                    class_lines.append("")

        return class_lines

    # Process each class in defined order
    for class_name in ["StringMethods", "DateTimeMethods", "Expr", "Column", "When"]:
        cls = class_map[class_name]
        is_subclass = class_name in ["Column", "When"]
        content.extend(process_class(class_name, cls, is_subclass))
        content.append("")

    # Process top-level functions
    for func_name in top_level_functions:
        if hasattr(expr_module, func_name):
            func = getattr(expr_module, func_name)
            try:
                sig = inspect.signature(func)

                # Process parameters
                params = []
                for param_name, param in sig.parameters.items():
                    param_str = param_name
                    if param.annotation is not inspect.Parameter.empty:
                        formatted_type = format_type_annotation(param.annotation)
                        param_str = f"{param_name}: {formatted_type}"

                    if param.default is not inspect.Parameter.empty:
                        default_repr = repr(param.default)
                        param_str = f"{param_str}={default_repr}"

                    if param.kind == inspect.Parameter.VAR_KEYWORD:
                        param_str = f"**{param_name}"
                    elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                        param_str = f"*{param_name}"

                    params.append(param_str)

                # Determine return type
                return_type = "Expr"
                if sig.return_annotation is not inspect.Parameter.empty:
                    return_type = format_type_annotation(sig.return_annotation)

                params_str = ", ".join(params)
                func_def = f"def {func_name}({params_str}) -> {return_type}: ..."

                if func.__doc__:
                    doc_lines = func.__doc__.strip().split('\n')
                    content.append(f"# {doc_lines[0].strip()}")

                content.append(func_def)
                content.append("")

            except Exception as e:
                content.append(f"# Error generating stub for {func_name}: {str(e)}")
                content.append(f"def {func_name}(*args, **kwargs) -> Any: ...")
                content.append("")

    # Write the stub file
    with open(output_file, "w") as f:
        f.write("\n".join(content))

    return output_file


if __name__ == "__main__":
    import argparse
    from importlib import import_module

    parser = argparse.ArgumentParser(description='Generate comprehensive type stub file for Expr')
    parser.add_argument('--output', '-o', help='Output file path for the stub file')
    parser.add_argument('--module', '-m', default='flowfile_frame.expr', help='Module containing Expr class')
    args = parser.parse_args()

    try:
        # Import the Expr module
        module = import_module(args.module)

        # Generate the type stub
        output_file = generate_expr_type_stub(
            expr_module=module,
            output_file=args.output
        )

        print(f"Type stub file generated successfully: {output_file}")

    except ImportError:
        print(f"Error: Could not import module {args.module}")
        print("Please specify the correct module with --module")
        sys.exit(1)
    except AttributeError as e:
        print(f"Error: {e}")
        print("Module does not contain expected classes")
        sys.exit(1)