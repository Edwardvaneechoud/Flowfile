import __future__

import ast
import base64
import re
import textwrap
import time
from collections.abc import Callable
from io import BytesIO
from typing import Any

import polars as pl


def remove_comments_and_docstrings(source: str) -> str:
    """
    Remove comments and docstrings from Python source code.

    Args:
        source: Python source code as string

    Returns:
        Cleaned Python source code
    """
    if not source.strip():
        return ""

    def remove_comments_from_line(line: str) -> str:
        """Remove comments while preserving string literals."""
        result = []
        i = 0
        in_string = False
        string_char = None

        while i < len(line):
            char = line[i]

            if char in ('"', "'"):
                if i > 0 and line[i - 1] == "\\":
                    result.append(char)
                    i += 1
                    continue

                if not in_string:
                    in_string = True
                    string_char = char
                elif string_char == char:
                    in_string = False
                    string_char = None

            elif char == "#" and not in_string:
                break

            result.append(char)
            i += 1

        return "".join(result).rstrip()

    lines = [remove_comments_from_line(line) for line in source.splitlines()]
    source = "\n".join(line for line in lines if line.strip())

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return source

    class DocstringRemover(ast.NodeTransformer):
        def visit_Module(self, node):
            while (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_FunctionDef(self, node):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_ClassDef(self, node):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_AsyncFunctionDef(self, node):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_Expr(self, node):
            if isinstance(node.value, ast.Str | ast.Constant) and isinstance(getattr(node.value, "value", None), str):
                return None
            return self.generic_visit(node)

    try:
        tree = DocstringRemover().visit(tree)
        ast.fix_missing_locations(tree)
        result = ast.unparse(tree)
        return "\n".join(line for line in result.splitlines() if line.strip())
    except Exception:
        return source


def _lone_def(code: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """The one top-level def of ``code`` (after an optional module docstring), plain or not, else ``None``."""
    try:
        body = ast.parse(textwrap.dedent(code).strip()).body
    except SyntaxError:
        return None
    if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0].value, "value", None), str):
        body = body[1:]
    if len(body) == 1 and isinstance(body[0], ast.FunctionDef | ast.AsyncFunctionDef):
        return body[0]
    return None


def function_form(code: str) -> ast.FunctionDef | None:
    """The ``def`` of function-form Polars Code, else ``None`` (snippet form, or code that does not parse).

    Function form is code whose top level is exactly one undecorated ``def``, optionally after a module
    docstring; the node calls it with its inputs, in connection order, and its ``return`` is the output.
    """
    entry = _lone_def(code)
    if isinstance(entry, ast.FunctionDef) and not entry.decorator_list:
        return entry
    return None


def unrunnable_def_error(code: str) -> str | None:
    """Why a lone top-level def is not function form: it is async or decorated, which Polars Code cannot run."""
    entry = _lone_def(code)
    if entry is None or not (isinstance(entry, ast.AsyncFunctionDef) or entry.decorator_list):
        return None
    what = "async" if isinstance(entry, ast.AsyncFunctionDef) else "decorated"
    return f"`{entry.name}` is {what}: Polars Code runs a plain `def` (no `async`, no decorator)"


def function_form_error(entry: ast.FunctionDef, num_inputs: int) -> str | None:
    """Why ``entry`` cannot run as the node's code with ``num_inputs`` frames, or ``None`` when it can."""
    if not _returns_a_value(entry):
        return f"`{entry.name}` returns nothing: end it with `return <frame>`, which is the node's output"
    args = entry.args
    keywords = [arg.arg for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True) if default is None]
    if keywords:
        names = ", ".join(f"`{name}`" for name in keywords)
        return f"`{entry.name}` has keyword-only {names} without a default: the node passes its inputs by position"
    positional = len(args.posonlyargs) + len(args.args)
    required = positional - len(args.defaults)
    if required <= num_inputs <= positional or (args.vararg and num_inputs >= required):
        return None
    if args.vararg:
        takes = f"at least {_count(required, 'frame')}"
    elif required == positional:
        takes = _count(positional, "frame")
    elif required == 0:
        takes = f"up to {_count(positional, 'frame')}"
    else:
        takes = f"{required} to {_count(positional, 'frame')}"
    return (
        f"`{entry.name}` takes {takes} but the node has {_count(num_inputs, 'input')}: "
        "give it one parameter per connected input"
    )


def _returns_a_value(entry: ast.FunctionDef) -> bool:
    """Whether ``entry`` has a ``return <value>`` of its own (not one of a function nested in it)."""
    stack: list[ast.AST] = list(entry.body)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Return) and node.value is not None:
            return True
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef):
            stack.extend(ast.iter_child_nodes(node))
    return False


def _count(number: int, noun: str) -> str:
    return f"{number} {noun}" if number == 1 else f"{number or 'no'} {noun}s"


class PolarsCodeParser:
    """
    Securely executes Polars code with restricted access to Python functionality.
    Supports multiple input DataFrames or no input DataFrames.
    """

    def __init__(self):
        import datetime

        self.safe_globals = {
            "__builtins__": {},
            # Polars functionality
            "pl": pl,
            "cs": pl.selectors,
            "col": pl.col,
            "lit": pl.lit,
            "expr": pl.expr,
            # Polars datatypes - added directly
            "Int8": pl.Int8,
            "Int16": pl.Int16,
            "Int32": pl.Int32,
            "Int64": pl.Int64,
            "Int128": pl.Int128,
            "UInt8": pl.UInt8,
            "UInt16": pl.UInt16,
            "UInt32": pl.UInt32,
            "UInt64": pl.UInt64,
            "UInt128": pl.UInt128,
            "Float16": pl.Float16,
            "Float32": pl.Float32,
            "Float64": pl.Float64,
            "Boolean": pl.Boolean,
            "String": pl.String,
            "Utf8": pl.Utf8,
            "Binary": pl.Binary,
            "Null": pl.Null,
            "List": pl.List,
            "Array": pl.Array,
            "Struct": pl.Struct,
            "Object": pl.Object,
            "Date": pl.Date,
            "Time": pl.Time,
            "Datetime": pl.Datetime,
            "Duration": pl.Duration,
            "Categorical": pl.Categorical,
            "Decimal": pl.Decimal,
            "Enum": pl.Enum,
            "Unknown": pl.Unknown,
            # Basic Python built-ins
            "print": print,
            "len": len,
            "range": range,
            "enumerate": enumerate,
            "zip": zip,
            "list": list,
            "dict": dict,
            "set": set,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "True": True,
            "False": False,
            "None": None,
            "time": time,
            "BytesIO": BytesIO,
            "base64": base64,
            "datetime": datetime,
        }

    @staticmethod
    def _validate_code(code: str) -> None:
        """
        Validate code for security concerns before execution.
        """
        # Blocked function names that can be used to escape the sandbox
        _blocked_functions = {
            "exec",
            "eval",
            "compile",
            "__import__",
            "globals",
            "locals",
            "getattr",
            "setattr",
            "delattr",
            "vars",
            "dir",
            "open",
            "breakpoint",
            "input",
            "memoryview",
            "super",
            "classmethod",
            "staticmethod",
            "property",
        }
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import | ast.ImportFrom):
                    raise ValueError("Import statements are not allowed")

                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name):
                        if node.func.id in _blocked_functions:
                            raise ValueError(f"Function '{node.func.id}' is not allowed")

                if isinstance(node, ast.Attribute):
                    if node.attr.startswith("__"):
                        raise ValueError(f"Access to '{node.attr}' is not allowed")

                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if re.search(r"__\w+__", node.value):
                        raise ValueError("Strings containing dunder patterns are not allowed")

        except SyntaxError as e:
            raise ValueError(f"Invalid Python syntax: {str(e)}") from e

    def _wrap_in_function(self, code: str, num_inputs: int = 1) -> str:
        """
        Wraps code in a function definition that can accept multiple input DataFrames or none.

        Args:
            code: The code to wrap
            num_inputs: Number of expected input DataFrames (0 for none)

        Returns:
            Wrapped code as a function
        """
        # Dedent the code first to handle various indentation styles
        code = textwrap.dedent(code).strip()

        if num_inputs == 0:
            function_def = "def _transform():\n"
        elif num_inputs == 1:
            function_def = "def _transform(input_df):\n"
        else:
            params = ", ".join([f"input_df_{i+1}" for i in range(num_inputs)])
            function_def = f"def _transform({params}):\n"

        if "\n" not in code:
            if any(code.startswith(prefix) for prefix in ["pl.", "col(", "input_df", "expr("]):
                return function_def + f"    return {code}"
            else:
                return function_def + f"    {code}\n    return output_df"

        indented_code = "\n".join(f"    {line}" for line in code.split("\n"))
        return function_def + indented_code + "\n    return output_df"

    def get_executable(self, code: str, num_inputs: int = 1) -> Callable:
        """
        Securely get a function that can be executed with multiple DataFrames or none.

        Args:
            code: The code to execute
            num_inputs: Number of expected input DataFrames (0 for none)

        Returns:
            Callable: A function that takes the specified number of DataFrames
        """
        code = remove_comments_and_docstrings(code)
        code = textwrap.dedent(code).strip()
        self._validate_code(code)

        entry = function_form(code)
        if entry is not None:
            error = function_form_error(entry, num_inputs)
            if error is not None:
                raise ValueError(error)
            return self._function_form_executable(code, entry.name)
        error = unrunnable_def_error(code)
        if error is not None:
            raise ValueError(error)

        wrapped_code = self._wrap_in_function(code, num_inputs)
        try:
            local_namespace: dict[str, Any] = {}

            exec(wrapped_code, self.safe_globals, local_namespace)

            transform_func = local_namespace["_transform"]
            return transform_func
        except Exception as e:
            raise ValueError(f"Error executing code: {str(e)}") from e

    def _function_form_executable(self, code: str, name: str) -> Callable:
        """The ``def`` of function-form code, called with the node's inputs positionally as LazyFrames.

        The code runs in its own copy of the sandbox globals (one namespace, so the shared globals
        never see it) and its annotations are never evaluated, so they are documentation only.
        """
        namespace = dict(self.safe_globals)
        try:
            flags = __future__.annotations.compiler_flag
            compiled = compile(code, "<polars_code>", "exec", flags=flags, dont_inherit=True)
            exec(compiled, namespace)
        except Exception as e:
            raise ValueError(f"Error executing code: {str(e)}") from e
        function = namespace[name]

        def _call(*frames):
            result = function(*(frame.lazy() for frame in frames))
            if not isinstance(result, pl.LazyFrame | pl.DataFrame):
                raise ValueError(f"`{name}` returned {type(result).__name__}, not a Polars LazyFrame or DataFrame")
            return result

        return _call

    def validate_code(self, code: str):
        """
        Validate code for security concerns before execution
        """
        code = remove_comments_and_docstrings(code)
        code = textwrap.dedent(code).strip()
        self._validate_code(code)


polars_code_parser = PolarsCodeParser()
