import __future__

import ast
import base64
import datetime as _datetime
import decimal
import json
import os
import re
import textwrap
import time
import types
import zoneinfo
from collections.abc import Callable
from io import BytesIO
from typing import Any

import polars as pl


def _namespace(**members: Any) -> types.SimpleNamespace:
    """A stand-in for a module that exposes only ``members``.

    A real module carries imported submodules (``datetime.sys``, ``pl.config.os``) that reach ``os`` by
    attribute walk, so the sandbox hands user code these instead.
    """
    return types.SimpleNamespace(**members)


def _module_namespace(module: types.ModuleType) -> types.SimpleNamespace:
    """A namespace of ``module``'s public, non-module attributes, used for ``pl`` and its subpackages.

    Polars' submodules (``pl.config``, ``pl.plugins``) reach ``os``/``sys``, so they are dropped.
    ``shared/tests`` proves no module survives.
    """
    members = {}
    for name in dir(module):
        if name.startswith("_"):
            continue
        try:
            value = getattr(module, name)
        except Exception:
            continue
        if isinstance(value, types.ModuleType):
            continue
        members[name] = value
    return _namespace(**members)


_PL_SELECTORS = _module_namespace(pl.selectors)
_PL_EXPR = _module_namespace(pl.expr)
_PL = _module_namespace(pl)
_PL.selectors = _PL_SELECTORS
_PL.expr = _PL_EXPR

_DATETIME = _namespace(
    datetime=_datetime.datetime,
    date=_datetime.date,
    time=_datetime.time,
    timedelta=_datetime.timedelta,
    timezone=_datetime.timezone,
)
_ZONEINFO = _namespace(ZoneInfo=zoneinfo.ZoneInfo, ZoneInfoNotFoundError=zoneinfo.ZoneInfoNotFoundError)
_BASE64 = _namespace(
    b64encode=base64.b64encode,
    b64decode=base64.b64decode,
    urlsafe_b64encode=base64.urlsafe_b64encode,
    urlsafe_b64decode=base64.urlsafe_b64decode,
    b32encode=base64.b32encode,
    b32decode=base64.b32decode,
    b16encode=base64.b16encode,
    b16decode=base64.b16decode,
)
_DECIMAL = _namespace(
    Decimal=decimal.Decimal,
    Context=decimal.Context,
    getcontext=decimal.getcontext,
    localcontext=decimal.localcontext,
    InvalidOperation=decimal.InvalidOperation,
    ROUND_HALF_UP=decimal.ROUND_HALF_UP,
    ROUND_HALF_EVEN=decimal.ROUND_HALF_EVEN,
    ROUND_DOWN=decimal.ROUND_DOWN,
    ROUND_UP=decimal.ROUND_UP,
    ROUND_FLOOR=decimal.ROUND_FLOOR,
    ROUND_CEILING=decimal.ROUND_CEILING,
)
_TIME = _namespace(
    time=time.time,
    time_ns=time.time_ns,
    perf_counter=time.perf_counter,
    perf_counter_ns=time.perf_counter_ns,
    monotonic=time.monotonic,
    monotonic_ns=time.monotonic_ns,
    sleep=time.sleep,
    strftime=time.strftime,
    strptime=time.strptime,
    gmtime=time.gmtime,
    localtime=time.localtime,
    struct_time=time.struct_time,
)


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
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
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


def _assigns_attribute(target: ast.AST) -> bool:
    """Whether an assignment target rebinds an attribute (``pl.X = …``, ``lf.sink = …``), unpacking included.

    Item assignment (``d['k'] = …``) stays allowed. Attribute rebinding is refused because it is how user
    code would patch a shared Polars function for a later task.
    """
    if isinstance(target, ast.Attribute):
        return True
    if isinstance(target, ast.Tuple | ast.List):
        return any(_assigns_attribute(element) for element in target.elts)
    if isinstance(target, ast.Starred):
        return _assigns_attribute(target.value)
    return False


STALE_PLAN_MESSAGE = (
    "This Polars Code step holds a query plan saved by an older Polars, which this version cannot load. "
    "flowfile_frame writes such a plan for an operation it cannot express as code: rebuild the step from code."
)

_STALE_PLAN_MARKERS = ("DSL_SCHEMA_HASH", "DSL_VERSION", "could not deserialize input into an expression")


def _stale_plan_guard(function: Callable) -> Callable:
    """``function``, with a Polars error loading a plan saved by another Polars raised as ``STALE_PLAN_MESSAGE``."""

    def _guarded(*frames):
        try:
            return function(*frames)
        except pl.exceptions.ComputeError as e:
            if any(marker in str(e) for marker in _STALE_PLAN_MARKERS):
                raise ValueError(STALE_PLAN_MESSAGE) from e
            raise

    return _guarded


PLAN_LOAD_REFUSED_MESSAGE = (
    "This Polars Code step loads a serialized Polars plan, which can run arbitrary code, so this server does not "
    "allow it: rebuild the step from code."
)


def plan_loads_refused() -> bool:
    """Whether the sandbox refuses ``deserialize``: on a multi-user server (``FLOWFILE_MODE`` docker or server).

    Read per call, like ``flowfile_core.auth.sharing``. A desktop or pip user already runs their own Python, and
    flowfile_frame writes a pickled plan for a function it cannot spell as code.
    """
    return os.environ.get("FLOWFILE_MODE", "electron") in ("docker", "server")


IN_PROCESS_REFUSED_MESSAGE = "On a multi-user server Polars Code runs on a kernel, never in a server process."


def refuse_in_process_on_server() -> None:
    """Raise ``IN_PROCESS_REFUSED_MESSAGE`` on a multi-user server, the mode ``plan_loads_refused`` reads.

    The sandbox limits the names code can use, which is no boundary for code that means to leave it, so the
    processes that hold a server's secrets (core, the worker, a run's children) call this before they run Polars
    Code. A kernel's environment has no ``FLOWFILE_MODE``, so the same sandbox runs the code in the person's own
    kernel.
    """
    if plan_loads_refused():
        raise PermissionError(IN_PROCESS_REFUSED_MESSAGE)


_PLAN_BYTE_KEYS = frozenset({"List", "Series", "Binary"})
_CALLABLE_PLAN_KEYS = frozenset({"AnonymousFunction", "callback", "Map"})
_CALLABLE_KEY_FRAGMENTS = ("python", "opaque", "udf", "pickle", "plugin")
_PLAN_NAMES = frozenset({"pl", "BytesIO"})


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate key in plan")
    return dict(pairs)


def _is_byte_list(value: list) -> bool:
    return bool(value) and all(type(item) is int and 0 <= item <= 255 for item in value)


def _plan_part_is_inert(value: Any, key: str | None = None) -> bool:
    if isinstance(value, dict):
        return all(
            name not in _CALLABLE_PLAN_KEYS
            and not any(fragment in name.lower() for fragment in _CALLABLE_KEY_FRAGMENTS)
            and _plan_part_is_inert(part, name)
            for name, part in value.items()
        )
    if isinstance(value, list):
        if _is_byte_list(value) and key not in _PLAN_BYTE_KEYS:
            return False
        return all(_plan_part_is_inert(part) for part in value)
    return True


def expression_plan_is_inert(plan: str | bytes) -> bool:
    """Whether a JSON expression plan holds nothing Polars would unpickle or load as a plugin.

    Polars unpickles a Python function wherever its plan carries one (``map_elements``, ``fold``, ``name.map``),
    as bytes. Bytes are accepted only as the value of a literal (``List``, ``Series``, ``Binary``, which Polars reads
    as Arrow IPC or raw bytes), and no key may name a callable. The plan is parsed, not searched, so whitespace or
    ``\\u`` escapes cannot hide a key, and a duplicated key refuses it.
    """
    try:
        return _plan_part_is_inert(json.loads(plan, object_pairs_hook=_unique_keys))
    except (ValueError, TypeError, RecursionError):
        return False


def _bound_names(tree: ast.AST) -> set[str]:
    """Every name ``tree`` binds anywhere: assignment targets, parameters, definitions, ``global``, handlers."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
            names.add(node.id)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Global | ast.Nonlocal):
            names.update(node.names)
        elif isinstance(node, ast.ExceptHandler | ast.MatchAs | ast.MatchStar) and node.name:
            names.add(node.name)
        elif isinstance(node, ast.MatchMapping) and node.rest:
            names.add(node.rest)
        elif type(node).__name__ in ("TypeVar", "ParamSpec", "TypeVarTuple"):
            names.add(node.name)
    return names


def _is_inert_expression_load(call: ast.Call) -> bool:
    """Whether ``call`` is ``pl.Expr.deserialize(BytesIO(<bytes literal>), format='json')`` over an inert plan.

    This is the form flowfile_frame writes for an expression it cannot spell as code; every other
    ``deserialize`` can unpickle, so the validator refuses it.
    """
    func = call.func
    if not (
        isinstance(func, ast.Attribute)
        and func.attr == "deserialize"
        and isinstance(func.value, ast.Attribute)
        and func.value.attr == "Expr"
        and isinstance(func.value.value, ast.Name)
        and func.value.value.id == "pl"
    ):
        return False
    if len(call.args) != 1 or len(call.keywords) != 1:
        return False
    keyword = call.keywords[0]
    if keyword.arg != "format" or not (isinstance(keyword.value, ast.Constant) and keyword.value.value == "json"):
        return False
    source = call.args[0]
    if not (
        isinstance(source, ast.Call)
        and isinstance(source.func, ast.Name)
        and source.func.id == "BytesIO"
        and len(source.args) == 1
        and not source.keywords
        and isinstance(source.args[0], ast.Constant)
        and isinstance(source.args[0].value, bytes)
    ):
        return False
    return expression_plan_is_inert(source.args[0].value)


def _refuse_plan_loads(tree: ast.AST) -> None:
    """Raise ``PLAN_LOAD_REFUSED_MESSAGE`` for any ``deserialize`` in ``tree`` but an inert expression load.

    A class pattern's keyword (``case pl.LazyFrame(deserialize=load)``) reads that attribute too, so it counts.
    """
    inert_loads = {
        id(node.func) for node in ast.walk(tree) if isinstance(node, ast.Call) and _is_inert_expression_load(node)
    }
    if inert_loads and _PLAN_NAMES & _bound_names(tree):
        raise ValueError(PLAN_LOAD_REFUSED_MESSAGE)
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr == "deserialize" and id(node) not in inert_loads:
            raise ValueError(PLAN_LOAD_REFUSED_MESSAGE)
        if isinstance(node, ast.MatchClass) and "deserialize" in node.kwd_attrs:
            raise ValueError(PLAN_LOAD_REFUSED_MESSAGE)


class PolarsCodeParser:
    """
    Securely executes Polars code with restricted access to Python functionality.
    Supports multiple input DataFrames or no input DataFrames.
    """

    def __init__(self):
        self.safe_globals = {
            "__builtins__": {},
            # Polars functionality (narrow namespaces, never the raw modules)
            "pl": _PL,
            "cs": _PL_SELECTORS,
            "col": pl.col,
            "lit": pl.lit,
            "expr": _PL_EXPR,
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
            "time": _TIME,
            "BytesIO": BytesIO,
            "base64": _BASE64,
            "datetime": _DATETIME,
            "decimal": _DECIMAL,
            "zoneinfo": _ZONEINFO,
            "ZoneInfo": zoneinfo.ZoneInfo,
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
            if plan_loads_refused():
                _refuse_plan_loads(tree)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import | ast.ImportFrom):
                    raise ValueError("Import statements are not allowed")

                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name):
                        if node.func.id in _blocked_functions:
                            raise ValueError(f"Function '{node.func.id}' is not allowed")

                if isinstance(node, ast.Attribute):
                    if node.attr.startswith("_"):
                        raise ValueError(f"Access to '{node.attr}' is not allowed")

                if isinstance(node, ast.MatchClass):
                    # A class pattern's keyword reads that attribute of the subject.
                    for name in node.kwd_attrs:
                        if name.startswith("_"):
                            raise ValueError(f"Access to '{name}' is not allowed")

                if isinstance(node, ast.Assign | ast.AugAssign | ast.AnnAssign | ast.Delete):
                    targets = node.targets if isinstance(node, ast.Assign | ast.Delete) else [node.target]
                    for target in targets:
                        if _assigns_attribute(target):
                            raise ValueError("Assigning to an attribute is not allowed")

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

            exec(wrapped_code, self._fresh_globals(), local_namespace)

            transform_func = local_namespace["_transform"]
        except Exception as e:
            raise ValueError(f"Error executing code: {str(e)}") from e

        return _stale_plan_guard(transform_func)

    def _fresh_globals(self) -> dict[str, Any]:
        """A copy of the sandbox globals for one executable, builtins included.

        No task can then rebind a name (``global col``, ``__builtins__[...] = ...``) for the tasks after it.
        """
        namespace = dict(self.safe_globals)
        namespace["__builtins__"] = dict(self.safe_globals["__builtins__"])
        return namespace

    def _function_form_executable(self, code: str, name: str) -> Callable:
        """The ``def`` of function-form code, called with the node's inputs positionally as LazyFrames.

        The code runs in its own copy of the sandbox globals (one namespace, so the shared globals
        never see it) and its annotations are never evaluated, so they are documentation only.
        """
        namespace = self._fresh_globals()
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

        return _stale_plan_guard(_call)

    def validate_code(self, code: str):
        """
        Validate code for security concerns before execution
        """
        code = remove_comments_and_docstrings(code)
        code = textwrap.dedent(code).strip()
        self._validate_code(code)


polars_code_parser = PolarsCodeParser()
