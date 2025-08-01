"""
Improved utility to generate comprehensive type stubs for FlowFrame.

This script generates a complete type stub file (.pyi) for the FlowFrame class
that includes both native FlowFrame methods and LazyFrame methods that are
added by the @add_lazyframe_methods decorator, plus module-level functions.
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


def format_default_value(param: inspect.Parameter) -> Optional[str]:
    """
    Properly format a parameter's default value for a .pyi stub file.

    Parameters
    ----------
    param : inspect.Parameter
        The parameter object to inspect.

    Returns
    -------
    Optional[str]
        A string representation of the default value, or None if there is no default.
    """
    if param.default is inspect.Parameter.empty:
        return None

    default = param.default

    if isinstance(default, (str, int, float, bool, type(None))):
        return repr(default)

    type_name = type(default).__name__
    if type_name == 'QueryOptFlags':
        return "DEFAULT_QUERY_OPT_FLAGS"

    return "..."

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

        # For known types like polars itself, prefix with pl if appropriate
        if module.startswith("polars.") and name not in ['DataFrame', 'LazyFrame', 'Series', 'Expr']:
            return f"pl.{name}"
        if module.startswith("polars.") and name == "DataType":  # Handle pl.DataType specifically
            return f"pl.{name}"

        return f"{name}"

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
    str_rep = str_rep.replace("polars.internals.type_aliases.", "")
    str_rep = str_rep.replace("polars.internals.datatypes.", "pl.")
    str_rep = str_rep.replace("polars.datatypes.", "pl.")
    str_rep = str_rep.replace("polars.type_aliases.", "")
    str_rep = str_rep.replace("polars.lazyframe.frame.", "") # For LazyFrame, LazyGroupBy if directly referenced
    str_rep = str_rep.replace("flowfile_frame.group_frame.", "group_frame.")
    if str_rep == "LazyFrame":
        str_rep = "FlowFrame"

    # Special case for NoneType
    if str_rep == 'NoneType' or str_rep.endswith('.NoneType'):
        return "None"

    return str_rep


def generate_improved_type_stub(
    flowframe_class: Type,
    output_file: Optional[str] = None,
    include_constructors: bool = True,
    include_inherited: bool = True,
    include_lazyframe: bool = True,
    include_module_functions: bool = True
) -> str:
    """
    Generate a comprehensive type stub file for FlowFrame.

    This improved version includes:
    1. All methods and properties from the FlowFrame class itself (with proper signatures)
    2. All methods from LazyFrame that would be added by the decorator
    3. Proper handling of the __init__ method
    4. Module-level functions

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
    include_module_functions : bool, default True
        Whether to include module-level functions

    Returns
    -------
    str
        Path to the generated stub file
    """
    import polars as pl
    from polars.lazyframe.frame import LazyFrame

    lazyframe_returning_methods = set()
    non_lazyframe_methods = set()

    print("Analyzing LazyFrame methods to determine return types...")
    all_lazyframe_members = inspect.getmembers(LazyFrame)

    for name, member in all_lazyframe_members:
        if not (inspect.ismethod(member) or inspect.isfunction(member)):
            continue
        if name.startswith('_'):
            continue

        try:
            sig = inspect.signature(member)
            if sig.return_annotation is not inspect.Signature.empty:
                return_annotation_str = str(sig.return_annotation).lower()
                if 'lazyframe' in return_annotation_str or 'polars.lazyframe.frame.lazyframe' in return_annotation_str:
                    lazyframe_returning_methods.add(name)
                else:
                    non_lazyframe_methods.add(name)
            elif member.__doc__ and "Returns" in member.__doc__:
                doc_lines = member.__doc__.split('\n')
                for i, line in enumerate(doc_lines):
                    if "Returns" in line and i+1 < len(doc_lines):
                        return_line = doc_lines[i+1].lower().strip()
                        if "lazyframe" in return_line:
                            lazyframe_returning_methods.add(name)
                            break
                else:
                    non_lazyframe_methods.add(name)
            else:
                non_lazyframe_methods.add(name)
        except (ValueError, TypeError):
            non_lazyframe_methods.add(name)

    if 'group_by' in lazyframe_returning_methods:
        lazyframe_returning_methods.remove('group_by')
        non_lazyframe_methods.add('group_by')

    # print(f"LazyFrame-returning methods: {lazyframe_returning_methods}")
    # print(f"Non-LazyFrame-returning methods from LazyFrame: {non_lazyframe_methods}")
    print(f"Analysis complete: {len(lazyframe_returning_methods)} LazyFrame-returning methods, "
          f"{len(non_lazyframe_methods)} non-LazyFrame-returning methods from LazyFrame.")

    flowframe_specific_methods = {
        "cache", "rename", "pivot", "concat", "write_csv", "write_parquet",
        "sink_csv", "sink_parquet", "_create_child_frame", "text_to_rows",
        "_with_flowfile_formula", "_add_number_of_records", "clear", "clone",
        "gather_every", "approx_n_unique", "set_sorted", "write_json_to_cloud_storage",
        "write_delta", "write_parquet_to_cloud_storage", "write_csv_to_cloud_storage"
    }

    if output_file is None:
        default_dir = os.getcwd()
        try:
            default_dir = os.path.dirname(__file__)
        except NameError:
            pass
        output_file = os.path.join(default_dir, "flow_frame.pyi")

    content = [
        "# Standard library imports",
        "import collections",
        "import inspect",
        "import os",
        "import sys",
        "import typing",
        "from io import IOBase",
        "from typing import List, Optional, ForwardRef, TypeVar, Any, Iterable, Sequence, Mapping, Collection, Callable, Literal, IO, Union",
        "from datetime import timedelta",
        "from pathlib import Path",
        "from collections.abc import Awaitable",
        "",
        "# Third-party imports",
        "import polars as pl",
        "from polars._typing import *",
        "from polars._typing import ParquetMetadata, PlanStage",
        "from polars._utils.async_ import _GeventDataFrameResult",
        "from polars.dependencies import polars_cloud as pc",
        "from polars.io.cloud import CredentialProviderFunction",
        "from polars.lazyframe.frame import LazyGroupBy",
        "from polars import LazyFrame, DataFrame, QueryOptFlags",
        "from polars.io.parquet import ParquetFieldOverwrites",
        "from polars.lazyframe.opt_flags import DEFAULT_QUERY_OPT_FLAGS",
        "from polars.type_aliases import (Schema, IntoExpr, ClosedInterval, Label, StartBy, RollingInterpolationMethod, IpcCompression, CompatLevel, SyncOnCloseMethod, ExplainFormat, EngineType, SerializationFormat, AsofJoinStrategy)",
        "",
        "# Local application/library specific imports",
        "import flowfile_frame",
        "from flowfile_core.flowfile.flow_graph import FlowGraph",
        "from flowfile_core.flowfile.flow_node.flow_node import FlowNode",
        "from flowfile_frame import group_frame",
        "from flowfile_frame.expr import Expr",
        "from flowfile_core.schemas import transform_schema",
        "",
        "# Conditional imports",
        "if sys.version_info >= (3, 10):",
        "    from typing import Concatenate",
        "else:",
        "    from typing_extensions import Concatenate",
        "",
        "T = TypeVar('T')",
        "P = typing.ParamSpec('P')",
        "LazyFrameT = TypeVar('LazyFrameT', bound='LazyFrame')",
        "FlowFrameT = TypeVar('FlowFrameT', bound='FlowFrame')",
        "Self = TypeVar('Self', bound='FlowFrame')", # For __new__
        "NoneType = type(None)",
        ""
    ]

    if include_module_functions:
        content.extend([
            "# Module-level functions (example from your input)",
            "def can_be_expr(param: inspect.Parameter) -> bool: ...",
            "def generate_node_id() -> int: ...",
            "def get_method_name_from_code(code: str) -> str | None: ...",
            "def _contains_lambda_pattern(text: str) -> bool: ...",
            "def _to_string_val(v) -> str: ...",
            "def _extract_expr_parts(expr_obj) -> tuple[str, str]: ...",
            "def _check_ok_for_serialization(method_name: str = None, polars_expr: pl.Expr | None = None, group_expr: pl.Expr | None = None) -> None: ...",
            ""
        ])

    class_name = flowframe_class.__name__
    content.append(f"class {class_name}:")

    core_attr_types = {
        "data": "LazyFrame",
        "flow_graph": "FlowGraph",
        "node_id": "int",
        "parent_node_id": "Optional[int]"
    }
    try:

        class_annotations = get_type_hints(flowframe_class, getattr(flowframe_class, '__globals__', globals()), locals())
        if "data" in class_annotations:
            core_attr_types["data"] = format_type_annotation(class_annotations["data"])
        if "flow_graph" in class_annotations:
            core_attr_types["flow_graph"] = format_type_annotation(class_annotations["flow_graph"])
    except Exception as e:
        print(f"Warning: Could not get class annotations for core attributes, using defaults: {e}")

    for attr_name, attr_type_str in core_attr_types.items():
        content.append(f"    {attr_name}: {attr_type_str}")
    content.append("")

    # Helper function for handling special methods - DEFINED HERE
    def handle_special_methods(name: str, sig: inspect.Signature) -> Optional[str]:
        """Handle methods that need special formatting in the stub file."""
        if name == "group_by":
            return "    def group_by(self, *by, description: Optional[str] = None, maintain_order: bool = False, **named_by) -> group_frame.GroupByFrame: ..."
        if name == "with_columns": # This specific one is from FlowFrame
            return ("    def with_columns(self, *exprs: Union[Expr, Iterable[Expr], Any], " # Match FlowFrame signature
                   "flowfile_formulas: Optional[List[str]] = None, "
                   "output_column_names: Optional[List[str]] = None, "
                   "description: Optional[str] = None, "
                   "**named_exprs: Union[Expr, Any]) -> 'FlowFrame': ...")
        return None

    def method_returns_flowframe(method_name: str, method_obj, class_obj) -> bool:
        if method_name in flowframe_specific_methods:
            return True

        try:
            hints = get_type_hints(method_obj, getattr(class_obj, '__globals__', globals()), locals())
            if 'return' in hints:
                return_type = hints['return']
                return_type_str = str(return_type)
                if class_name in return_type_str or f"'{class_name}'" in return_type_str or \
                   "FlowFrame" in return_type_str or f"'FlowFrame'" in return_type_str:
                    return True
                if 'GroupByFrame' in return_type_str:
                    return False
        except Exception:
            pass
        return False
    flowframe_members_to_process = {}
    for name, member in inspect.getmembers(flowframe_class):
        if not (inspect.isfunction(member) or isinstance(member, property)):
            continue

        allowed_special_methods = {'__init__', '__new__', '__repr__', '__bool__', '__contains__', '__eq__', '__ne__', '__gt__', '__lt__', '__ge__', '__le__'}
        known_internal_methods = {"_add_connection", "_create_child_frame", "_generate_sort_polars_code",
                                  "_add_polars_code", "_comparison_error", "_detect_cum_count_record_id",
                                  "_add_number_of_records", "_with_flowfile_formula"}

        if name == '__init__' or name == '__new__':
            if not include_constructors:
                continue
        elif name in allowed_special_methods:
            pass
        elif name.startswith('_'):
            if name not in known_internal_methods and not include_constructors:
                 continue
            elif name in known_internal_methods and not include_constructors and name not in flowframe_specific_methods: # if it's internal but not specifically marked as returning FlowFrame
                 pass

        if not include_inherited and name not in flowframe_class.__dict__:
            continue

        flowframe_members_to_process[name] = member

    for name, member in sorted(flowframe_members_to_process.items()):
        try:
            if inspect.isfunction(member):
                method = member
                sig = inspect.signature(method)

                special_signature_str = handle_special_methods(name, sig)
                if special_signature_str:
                    if method.__doc__:
                        doc_lines = method.__doc__.strip().split('\n')
                        content.append(f"    # {doc_lines[0].strip()}")
                    content.append(special_signature_str)
                    content.append("")
                    continue

                processed_params = []
                has_var_keyword = False
                var_keyword_param = None
                description_param_str = None

                is_new_method = (name == "__new__")

                for i, (param_name, param) in enumerate(sig.parameters.items()):
                    if i == 0 and (param_name == 'self' or (is_new_method and param_name == 'cls')):
                        continue

                    param_str = param_name
                    if param.annotation is not inspect.Parameter.empty:
                        formatted_type = format_type_annotation(param.annotation)
                        param_str = f"{param_name}: {formatted_type}"

                    default_str = format_default_value(param)
                    if default_str is not None:
                        param_str = f"{param_str} = {default_str}"

                    if param.kind == inspect.Parameter.VAR_KEYWORD:
                        has_var_keyword = True
                        var_keyword_param = f"**{param_name}"
                        continue
                    elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                        param_str = f"*{param_name}"

                    if param_name == "description" and param.default is not inspect.Parameter.empty:
                        description_param_str = param_str
                        continue

                    processed_params.append(param_str)

                if not name.startswith('_') and name not in PASSTHROUGH_METHODS and \
                   name not in {'__init__', '__new__', 'group_by', 'with_columns'} and \
                   not any(p.startswith('description:') for p in processed_params) and not description_param_str :
                    processed_params.append("description: Optional[str] = None")
                elif description_param_str:
                    processed_params.append(description_param_str)

                if var_keyword_param:
                    processed_params.append(var_keyword_param)

                final_return_type = "Any"
                if name == "__init__":
                    final_return_type = "None"
                elif name == "__new__":
                    final_return_type = "Self" # PEP 673 for Self type
                elif name == "_generate_sort_polars_code":
                    final_return_type = "str"
                elif sig.return_annotation is not inspect.Signature.empty:
                    annotated_return = format_type_annotation(sig.return_annotation)
                    # If FlowFrame specific methods like _create_child_frame are annotated with FlowFrame, use it
                    if class_name in annotated_return or f"'{class_name}'" in annotated_return or "FlowFrame" in annotated_return:
                        final_return_type = f"'{class_name}'"
                    else:
                        final_return_type = annotated_return
                elif method_returns_flowframe(name, method, flowframe_class):
                    final_return_type = f"'{class_name}'"
                params_prefix = "cls" if is_new_method else "self"
                params_str = ", ".join(processed_params)
                method_sig_str = f"    def {name}({params_prefix}, {params_str}) -> {final_return_type}: ..."
                if not processed_params and not params_str : # Methods like __repr__ might have no other params
                     method_sig_str = f"    def {name}({params_prefix}) -> {final_return_type}: ..."

                if method.__doc__:
                    doc_lines = method.__doc__.strip().split('\n')
                    content.append(f"    # {doc_lines[0].strip()}")
                content.append(method_sig_str)
                content.append("")

            elif isinstance(member, property):
                doc = inspect.getdoc(member) or (hasattr(member, 'fget') and member.fget and inspect.getdoc(member.fget))
                if doc:
                    doc_lines = doc.strip().split('\n')
                    content.append(f"    # {doc_lines[0].strip()}")

                content.append(f"    @property")
                return_type_str = "Any"
                if hasattr(member, 'fget') and member.fget is not None:
                    try:
                        hints = get_type_hints(member.fget, getattr(flowframe_class, '__globals__', globals()), locals())
                        if 'return' in hints:
                            return_type_str = format_type_annotation(hints['return'])
                        else:
                            prop_sig = inspect.signature(member.fget)
                            if prop_sig.return_annotation is not inspect.Signature.empty:
                                return_type_str = format_type_annotation(prop_sig.return_annotation)
                    except Exception:
                        pass
                content.append(f"    def {name}(self) -> {return_type_str}: ...")
                content.append("")

        except Exception as e:
            content.append(f"    # Error generating stub for member {name}: {str(e)}")
            content.append("")

    lazyframe_members_to_add = {}
    if include_lazyframe:
        for lf_name, lf_member in inspect.getmembers(LazyFrame):
            if not (inspect.ismethod(lf_member) or inspect.isfunction(lf_member) or isinstance(lf_member, property)):
                continue

            if lf_name.startswith('_'):
                continue

            if lf_name in flowframe_members_to_process:
                continue

            lazyframe_members_to_add[lf_name] = lf_member

    for name, member in sorted(lazyframe_members_to_add.items()):
        try:
            if inspect.isfunction(member) or inspect.ismethod(member):
                method = member
                sig = inspect.signature(method)

                special_signature_str = handle_special_methods(name, sig)
                if special_signature_str:
                    if method.__doc__:
                        doc_lines = method.__doc__.strip().split('\n')
                        content.append(f"    # {doc_lines[0].strip()}")
                    content.append(special_signature_str)
                    content.append("")
                    continue

                processed_params = []
                has_var_keyword = False
                var_keyword_param = None
                description_param_added = False

                for i, (param_name, param) in enumerate(sig.parameters.items()):
                    if i == 0:
                        continue

                    param_str = param_name
                    if param.annotation is not inspect.Parameter.empty:
                        formatted_type = format_type_annotation(param.annotation)
                        param_str = f"{param_name}: {formatted_type}"

                    if param.default is not inspect.Parameter.empty:
                        default_repr = repr(param.default)
                        param_str = f"{param_str}={default_repr}"

                    if param.kind == inspect.Parameter.VAR_KEYWORD:
                        has_var_keyword = True
                        var_keyword_param = f"**{param_name}"
                        continue
                    elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                        param_str = f"*{param_name}"

                    if param_name == "description":
                        description_param_added = True

                    processed_params.append(param_str)

                if name not in PASSTHROUGH_METHODS and not description_param_added :
                     processed_params.append("description: Optional[str] = None")

                if var_keyword_param:
                    processed_params.append(var_keyword_param)
                return_type = f"'{class_name}'"
                if name in non_lazyframe_methods or name in PASSTHROUGH_METHODS:
                    if sig.return_annotation is not inspect.Signature.empty:
                        return_type = format_type_annotation(sig.return_annotation)
                    else:
                        return_type = "Any"
                elif name not in lazyframe_returning_methods:
                    if sig.return_annotation is not inspect.Signature.empty:
                         original_return = format_type_annotation(sig.return_annotation)
                         if "LazyFrame" not in original_return and class_name not in original_return:
                              return_type = original_return
                    else:
                         return_type = "Any"

                params_str = ", ".join(processed_params)
                method_sig_str = f"    def {name}(self, {params_str}) -> {return_type}: ..."

                if method.__doc__:
                    doc_lines = method.__doc__.strip().split('\n')
                    content.append(f"    # {doc_lines[0].strip()}")
                content.append(method_sig_str)
                content.append("")

            elif isinstance(member, property):
                if name in PASSTHROUGH_METHODS:
                    doc = inspect.getdoc(member) or (hasattr(member, 'fget') and member.fget and inspect.getdoc(member.fget))
                    if doc:
                        doc_lines = doc.strip().split('\n')
                        content.append(f"    # {doc_lines[0].strip()}")

                    content.append(f"    @property")
                    return_type_str = "Any"
                    if hasattr(member, 'fget') and member.fget is not None:
                        try:
                            hints = get_type_hints(member.fget, getattr(LazyFrame, '__globals__', globals()), locals())
                            if 'return' in hints:
                                return_type_str = format_type_annotation(hints['return'])
                            else:
                                prop_sig = inspect.signature(member.fget)
                                if prop_sig.return_annotation is not inspect.Signature.empty:
                                    return_type_str = format_type_annotation(prop_sig.return_annotation)
                        except Exception:
                            pass
                    content.append(f"    def {name}(self) -> {return_type_str}: ...")
                    content.append("")

        except Exception as e:
            content.append(f"    # Error generating stub for LazyFrame member {name}: {str(e)}")
            content.append("")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        f.write("\n".join(content))

    return output_file


if __name__ == "__main__":
    import argparse
    from importlib import import_module

    parser = argparse.ArgumentParser(description='Generate comprehensive type stub file for FlowFrame')
    parser.add_argument('--output', '-o', help='Output file path for the stub file (e.g., flow_frame.pyi)')
    parser.add_argument('--module', '-m', default='flowfile_frame.flow_frame', help='Module containing FlowFrame class')
    parser.add_argument('--class-name', '-c', default='FlowFrame', help='Name of the FlowFrame class')
    parser.add_argument('--no-constructors', action='store_true', help='Skip __init__, __new__ and other special methods')
    parser.add_argument('--no-inherited', action='store_true', help='Skip inherited methods not overridden in FlowFrame')
    parser.add_argument('--no-lazyframe', action='store_true', help='Skip methods from LazyFrame')
    parser.add_argument('--no-module-functions', action='store_true', help='Skip module-level functions')

    args = parser.parse_args()

    try:
        module_obj = import_module(args.module) # Renamed to avoid conflict
        flowframe_class_actual = getattr(module_obj, args.class_name)

        output_path = args.output
        if not output_path:
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__))
            except NameError:
                script_dir = os.getcwd()

            # Heuristic for output path based on common project structures
            # Assuming flow_frame.py is in a module directory like 'flowfile_frame'
            module_path_part = args.module.replace('.', os.sep) # e.g., flowfile_frame/flow_frame

            # Check if the target class's module path can be determined
            class_module_file = getattr(module_obj, '__file__', None)
            if class_module_file:
                 output_path = os.path.splitext(class_module_file)[0] + ".pyi"
            else: # Fallback if module path is not available (e.g. some C extensions or built-ins)
                # Try to create it in a folder structure mimicking the module path
                path_parts = args.module.split('.')
                class_file_name = path_parts[-1] + ".pyi"
                if len(path_parts) > 1:
                    dir_path = os.path.join(script_dir, *path_parts[:-1])
                    output_path = os.path.join(dir_path, class_file_name)
                else:
                    output_path = os.path.join(script_dir, class_file_name)
                # If script_dir is not ideal (e.g. running from system path), fallback to CWD
                if not os.path.isdir(os.path.dirname(output_path)) and len(path_parts) > 1:
                    dir_path_cwd = os.path.join(os.getcwd(), *path_parts[:-1])
                    output_path = os.path.join(dir_path_cwd, class_file_name)
                elif not os.path.isdir(os.path.dirname(output_path)):
                     output_path = os.path.join(os.getcwd(), class_file_name)


        print(f"Attempting to generate stub at: {os.path.abspath(output_path)}")

        generated_file = generate_improved_type_stub(
            flowframe_class=flowframe_class_actual,
            output_file=output_path,
            include_constructors=not args.no_constructors,
            include_inherited=not args.no_inherited,
            include_lazyframe=not args.no_lazyframe,
            include_module_functions=not args.no_module_functions
        )
        print(f"Type stub file generated successfully: {generated_file}")

    except ImportError:
        print(f"Error: Could not import module '{args.module}'. Ensure it's in PYTHONPATH and all dependencies are installed.")
        sys.exit(1)
    except AttributeError:
        print(f"Error: Class '{args.class_name}' not found in module '{args.module}'.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
