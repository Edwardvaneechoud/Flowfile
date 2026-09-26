"""``fl.PythonScript`` and ``fl.python_script``: a Python Script node, run on a kernel container when the flow runs.

``PythonScript`` is the canonical form: notebook cells as strings, one-to-one with what the node
stores. ``python_script(...)`` turns a module-level function into those cells: its parameters are
the inputs, its body is the notebook, and its ``return`` is what the node publishes.
"""

from __future__ import annotations

import ast
import builtins
import dis
import functools
import inspect
import io
import json
import re
import tokenize
import types
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import polars as pl
from pydantic import ValidationError

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import input_schema
from flowfile_frame.callable_utils import _get_function_source, _is_safely_representable
from flowfile_frame.native import NativeNode, NativeNodeError, _kernel_id

if TYPE_CHECKING:
    from polars._typing import PolarsDataType

    from flowfile_frame.flow_frame import FlowFrame

INPUTS_MARKER = "# flowfile: inputs"
OUTPUTS_MARKER = "# flowfile: outputs"

# Names the kernel defines in every script's namespace (kernel_runtime/main.py).
_KERNEL_NAMES: frozenset[str] = frozenset({"flowfile_ctx", "display", "explore"})
_CELL_MARKER = re.compile(r"#\s*%%(?:\s+(?P<rest>.*))?$")
_MARKDOWN_TAG = re.compile(r"\[(?:markdown|md)\]")
_NEW_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
_GLOBAL_LOADS: frozenset[str] = frozenset({"LOAD_GLOBAL", "LOAD_NAME", "LOAD_FROM_DICT_OR_GLOBALS"})
_PARAMETER_KINDS: dict[Any, str] = {
    inspect.Parameter.VAR_POSITIONAL: "*args",
    inspect.Parameter.VAR_KEYWORD: "**kwargs",
    inspect.Parameter.KEYWORD_ONLY: "keyword-only",
    inspect.Parameter.POSITIONAL_ONLY: "positional-only",
}


def _is_dtype(value: Any) -> bool:
    return isinstance(value, pl.DataType) or (isinstance(value, type) and issubclass(value, pl.DataType))


def _declared_columns(
    schemas: Mapping[str, Mapping[str, PolarsDataType]] | None, output_names: Sequence[str], argument: str
) -> dict[str, list[FlowfileColumn]]:
    """``schemas`` as seed columns per output name; an unknown output or a value that is no dtype raises."""
    if schemas is None:
        return {}
    if not isinstance(schemas, Mapping):
        raise NativeNodeError(f"{argument} maps output names to {{column: dtype}}, got {type(schemas).__name__}")
    unknown = [name for name in schemas if name not in output_names]
    if unknown:
        raise NativeNodeError(f"{argument} declares {unknown}, which are not outputs of the node: {list(output_names)}")
    declared: dict[str, list[FlowfileColumn]] = {}
    for name, columns in schemas.items():
        if not isinstance(columns, Mapping):
            raise NativeNodeError(
                f"{argument} maps each output to {{column: dtype}}; output {name!r} got {type(columns).__name__}"
            )
        bad = [column for column, dtype in columns.items() if not isinstance(column, str) or not _is_dtype(dtype)]
        if bad:
            raise NativeNodeError(
                f"{argument} maps column names to Polars dtypes such as pl.Int64; output {name!r} has {bad}"
            )
        try:
            declared[name] = [FlowfileColumn.from_input(column, str(dtype)) for column, dtype in columns.items()]
        except (TypeError, ValueError) as exc:
            raise NativeNodeError(f"{argument}: output {name!r} has an incomplete dtype ({exc})") from exc
    return declared


class PythonScript(NativeNode):
    """A Python Script node: its code runs on a kernel container when the flow runs.

    Give the script as ``code`` (one cell) or ``cells`` (notebook cells, run in order as one
    script). Every input frame is wired to the script in order; in the kernel each is named
    after its upstream node's ``node_reference``, else ``df_<node id>``. ``outputs`` names the
    output handles (``["main"]`` by default), reached with ``.output`` or ``node[name]``. ``kernel``
    is a kernel id (or an object with an ``.id``) stored as given: it is not checked until the
    flow runs, and a node without one fails the run. The outputs are deferred: typed zero-row
    placeholders until ``collect()`` runs the flow. ``schemas`` declares an output's columns as
    ``{output: {column: dtype}}``; an undeclared output carries the first input's schema (no
    columns without inputs). The declared schema only shapes the placeholder; it is not saved.
    """

    code: str
    cells: list[str]
    kernel: str | None

    def __init__(
        self,
        *inputs: FlowFrame,
        code: str | None = None,
        cells: list[str] | None = None,
        kernel: str | Any | None = None,
        outputs: list[str] | None = None,
        schemas: Mapping[str, Mapping[str, PolarsDataType]] | None = None,
        description: str | None = None,
        flow_graph: FlowGraph | None = None,
    ) -> None:
        if (code is None) == (cells is None):
            raise NativeNodeError("PythonScript takes exactly one of code= or cells=")
        cell_codes = [code] if code is not None else list(cells)
        if not cell_codes:
            raise NativeNodeError("cells= needs at least one cell")
        if not all(isinstance(cell, str) for cell in cell_codes):
            raise NativeNodeError("code= is a string and cells= a list of strings")
        if outputs is not None and not outputs:
            raise NativeNodeError("outputs= needs at least one output name")
        self._declared = _declared_columns(schemas, list(outputs) if outputs is not None else ["main"], "schemas=")
        self.cells = cell_codes
        # Core runs only .code; the drawer joins non-empty cells the same way.
        self.code = "\n\n".join(cell for cell in cell_codes if cell)
        self.kernel = _kernel_id(kernel)

        def make_settings(base: dict[str, Any]) -> input_schema.NodePythonScript:
            try:
                return input_schema.NodePythonScript(
                    python_script_input=input_schema.PythonScriptInput(
                        code=self.code,
                        kernel_id=self.kernel,
                        cells=[input_schema.NotebookCell(id=uuid4().hex, code=cell) for cell in cell_codes],
                    ),
                    output_names=list(outputs) if outputs is not None else ["main"],
                    **base,
                )
            except ValidationError as exc:
                raise NativeNodeError(f"Invalid python_script settings: {exc}") from exc

        self._build(
            "python_script",
            input_schema.NodePythonScript,
            inputs,
            make_settings,
            deferred=None,
            description=description,
            flow_graph=flow_graph,
        )

    def _seed_schemas(
        self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]
    ) -> dict[str, list[FlowfileColumn]]:
        """Each declared output's own schema; the others keep the first input's."""
        seeded = super()._seed_schemas(node, frames, handles)
        for output_name, handle in zip(self.output_names, handles, strict=True):
            if output_name in self._declared:
                seeded[handle] = list(self._declared[output_name])
        return seeded


def _output_names(outputs: Sequence[str] | None) -> list[str]:
    """``outputs=`` checked by the node's own output-name rule, ``["main"]`` when not given."""
    if outputs is None:
        return ["main"]
    if isinstance(outputs, str) or not isinstance(outputs, Sequence) or not outputs:
        raise NativeNodeError("outputs= is a non-empty list of output names")
    if not all(isinstance(name, str) for name in outputs):
        raise NativeNodeError(f"outputs= is a list of output names (strings), got {list(outputs)}")
    try:
        return input_schema._validate_output_names(list(outputs))
    except ValueError as exc:
        raise NativeNodeError(f"Invalid outputs=: {exc}") from exc


def _returns_as_schemas(
    returns: Mapping[str, Any] | None, output_names: list[str]
) -> dict[str, Mapping[str, PolarsDataType]] | None:
    """``returns=`` as ``{output: {column: dtype}}``; the flat form belongs to a single-output function."""
    if returns is None:
        return None
    if not isinstance(returns, Mapping):
        raise NativeNodeError(f"returns= maps column names to Polars dtypes, got {type(returns).__name__}")
    nested = [isinstance(value, Mapping) for value in returns.values()]
    if nested and all(nested):
        return dict(returns)
    if any(nested):
        raise NativeNodeError("returns= is {column: dtype} for one output or {output: {column: dtype}}, not a mix")
    if len(output_names) != 1:
        raise NativeNodeError(f"returns= of a function with outputs {output_names} is {{output: {{column: dtype}}}}")
    return {output_names[0]: dict(returns)}


def _check_function(fn: Any) -> str:
    """``fn``'s name, when it is a module-level ``def`` whose body can become top-level cells."""
    if not inspect.isfunction(fn):
        raise NativeNodeError(f"python_script decorates a function, got {type(fn).__name__}")
    name = fn.__name__
    if name == "<lambda>":
        raise NativeNodeError("python_script needs a def function; a lambda has no body to turn into cells")
    if "<locals>" in fn.__qualname__:
        raise NativeNodeError(
            f"`{name}` is defined inside another function; define it at module level "
            "(its body becomes the notebook, so it has no enclosing scope)"
        )
    if fn.__qualname__ != name:
        raise NativeNodeError(f"`{fn.__qualname__}` is a method; python_script decorates a module-level function")
    if hasattr(fn, "__wrapped__"):
        raise NativeNodeError(f"`{name}` is wrapped by another decorator; @fl.python_script must be its only decorator")
    if inspect.iscoroutinefunction(fn) or inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        raise NativeNodeError(f"`{name}` is async or a generator; its body cannot run as notebook cells")
    return name


def _parameters(fn: Callable[..., Any], name: str) -> list[str]:
    """The parameter names, one per input frame."""
    names = []
    for parameter in inspect.signature(fn).parameters.values():
        kind = _PARAMETER_KINDS.get(parameter.kind)
        if kind is not None:
            raise NativeNodeError(
                f"`{name}` takes its inputs as plain parameters, one per input frame; `{parameter.name}` is {kind}"
            )
        if parameter.default is not inspect.Parameter.empty:
            raise NativeNodeError(
                f"`{name}` parameter `{parameter.name}` has a default, but every parameter is an input frame"
            )
        if parameter.name in _KERNEL_NAMES:
            raise NativeNodeError(f"`{name}` parameter `{parameter.name}` would hide the kernel's own; rename it")
        names.append(parameter.name)
    return names


def _parse(name: str, source: str) -> tuple[ast.FunctionDef, list[tokenize.TokenInfo]]:
    try:
        tree = ast.parse(source)
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (SyntaxError, tokenize.TokenError) as exc:
        raise NativeNodeError(f"Could not parse the source of `{name}`: {exc}") from exc
    func = tree.body[0] if tree.body else None
    if not isinstance(func, ast.FunctionDef) or func.name != name:
        raise NativeNodeError(f"The source found for `{name}` does not define it; is the file saved?")
    return func, tokens


def _header_end(tokens: list[tokenize.TokenInfo], func: ast.FunctionDef) -> tuple[int, int]:
    """Row and column just past the ``:`` that ends ``func``'s ``def`` header."""
    depth, in_header = 0, False
    for token in tokens:
        if not in_header:
            in_header = token.type == tokenize.NAME and token.string == "def" and token.start[0] == func.lineno
        elif token.type == tokenize.OP:
            if token.string in ("(", "[", "{"):
                depth += 1
            elif token.string in (")", "]", "}"):
                depth -= 1
            elif token.string == ":" and depth == 0:
                return token.end
    raise NativeNodeError(f"Could not find the end of the def header of `{func.name}`")


def _char_col(line: str, byte_col: int) -> int:
    """An ``ast`` column (UTF-8 bytes) as an index into ``line``."""
    return len(line.encode("utf-8")[:byte_col].decode("utf-8"))


def _function_source(fn: Callable[..., Any], name: str) -> tuple[list[str], ast.FunctionDef, list, int]:
    """Source lines, ``def`` node, tokens and header row; a one-line function gets its body moved below the header."""
    try:
        source, _ = _get_function_source(fn)
    except (SyntaxError, tokenize.TokenError) as exc:
        raise NativeNodeError(f"Could not read the source of `{name}` ({exc}); is the file saved?") from exc
    if source is None:
        raise NativeNodeError(
            f"python_script needs the source of `{name}`; define it in a file or a notebook cell, "
            "not at an interactive prompt"
        )
    func, tokens = _parse(name, source)
    lines = source.split("\n")  # ast and tokenize count rows on "\n" only
    row, col = _header_end(tokens, func)
    if func.body[0].lineno == row:
        line = lines[row - 1]
        lines[row - 1 : row] = [line[:col], "    " + line[_char_col(line, func.body[0].col_offset) :]]
        func, tokens = _parse(name, "\n".join(lines) + "\n")
    return lines, func, tokens, row


def _scope_nodes(statements: list[ast.stmt]) -> Iterator[ast.AST]:
    """Every node of ``statements`` in the function's own scope: nested defs, lambdas and classes are not entered."""
    stack: list[ast.AST] = list(statements)
    while stack:
        node = stack.pop()
        yield node
        if not isinstance(node, _NEW_SCOPES):
            stack.extend(ast.iter_child_nodes(node))


def _final_return(name: str, func: ast.FunctionDef) -> ast.Return:
    """The one top-level ``return``, which must end the body and return a value."""
    returns = [node for node in _scope_nodes(func.body) if isinstance(node, ast.Return)]
    if not returns:
        raise NativeNodeError(f"`{name}` must end with `return <frame>`: what it returns is the node's output")
    last = func.body[-1]
    if len(returns) > 1 or returns[0] is not last:
        raise NativeNodeError(
            f"`{name}` must return exactly once, as its last top-level statement (not inside an if, loop, "
            f"with or try); found {len(returns)} return statement(s)"
        )
    if last.value is None:
        raise NativeNodeError(f"`{name}` must return a frame, or a dict of frames with outputs=[...]")
    if isinstance(last.value, ast.Tuple | ast.List | ast.Set):
        kind = type(last.value).__name__.lower()
        raise NativeNodeError(f"`{name}` returns a {kind}; return one frame, or a dict of frames with outputs=[...]")
    return last


def _returns_dict(value: ast.expr) -> bool:
    """Whether the returned expression is visibly a dict (a display, a comprehension or a ``dict(...)`` call)."""
    if isinstance(value, ast.Dict | ast.DictComp):
        return True
    return isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "dict"


def _publish_lines(name: str, value: ast.expr, outputs: Sequence[str] | None) -> list[str]:
    """What follows ``_result = <value>``: one publish, or a checked loop over a dict's outputs."""
    if not _returns_dict(value) and (outputs is None or len(outputs) == 1):
        output = outputs[0] if outputs is not None else "main"
        return [f'flowfile_ctx.publish_output(_result, "{output}")']
    if outputs is None:
        raise NativeNodeError(f"`{name}` returns a dict; name its outputs with outputs=[...], one per key")
    if isinstance(value, ast.Dict) and all(isinstance(k, ast.Constant) for k in value.keys):
        keys = [k.value for k in value.keys]
        if set(keys) != set(outputs):
            raise NativeNodeError(f"`{name}` returns the keys {keys}, but outputs= is {list(outputs)}")
    names = json.dumps(list(outputs))
    expected = "{" + ", ".join(json.dumps(output) for output in outputs) + "}"
    return [
        f"if not isinstance(_result, dict) or set(_result) != {expected}:",
        f'    raise ValueError("{name} must return a dict with the keys {list(outputs)}")',
        f"for _name in {names}:",
        "    flowfile_ctx.publish_output(_result[_name], _name)",
    ]


def _global_names(code: types.CodeType) -> list[str]:
    """Names ``code`` and every code object nested in it read from the module namespace, in first-use order.

    ``inspect.getclosurevars`` reads only the function's own code, so it misses names used in
    comprehensions (their own code objects before Python 3.12), lambdas and inner functions, and
    before 3.12 it also reports attribute names. A class body reads its own attributes with
    ``LOAD_NAME`` as well, so the names it assigns itself are left out.
    """
    instructions = list(dis.get_instructions(code))
    assigned = {i.argval for i in instructions if i.opname == "STORE_NAME"}
    names: dict[str, None] = {}
    for instruction in instructions:
        if instruction.opname in _GLOBAL_LOADS and (
            instruction.opname == "LOAD_GLOBAL" or instruction.argval not in assigned
        ):
            names.setdefault(instruction.argval)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            for name in _global_names(const):
                names.setdefault(name)
    return list(names)


def _literal(value: Any) -> str | None:
    """``repr(value)`` when it evaluates back to an equal value of the same type, else ``None``."""
    try:
        if not _is_safely_representable(value):
            return None
        text = repr(value)
        restored = ast.literal_eval(text)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
        return None
    return text if type(restored) is type(value) and restored == value else None


def _prelude(fn: Callable[..., Any], func: ast.FunctionDef, name: str) -> list[str]:
    """Import lines for the modules the body reads from outside, then assignments for the constants.

    Annotations of the body's own variables are not compiled into the function, but become
    top-level statements that Python evaluates once the body is unwrapped, so their names count too.
    """
    candidates = dict.fromkeys(_global_names(fn.__code__))
    local = set(fn.__code__.co_varnames) | set(fn.__code__.co_cellvars)
    for node in _scope_nodes(func.body):
        if isinstance(node, ast.AnnAssign):
            names = (n.id for n in ast.walk(node.annotation) if isinstance(n, ast.Name) and n.id not in local)
            candidates.update(dict.fromkeys(names))
    namespace = fn.__globals__
    imports, constants = [], []
    for used in candidates:
        # Dunders come from class bodies (__name__, __annotations__); the kernel sets its own.
        if used in _KERNEL_NAMES or (used.startswith("__") and used.endswith("__")):
            continue
        builtin = getattr(builtins, used, None)
        if used not in namespace:
            if builtin is not None:
                continue
            raise NativeNodeError(
                f"`{used}` is used inside `{name}` but is not defined where `{name}` is decorated; "
                "define or import it above the function, or inside it"
            )
        value = namespace[used]
        if builtin is not None and (value is builtin or getattr(value, "__wrapped__", None) is builtin):
            continue  # IPython binds its own wrapper of open()
        if isinstance(value, types.ModuleType):
            module = value.__name__
            imports.append(f"import {module}" if module == used else f"import {module} as {used}")
            continue
        literal = _literal(value)
        if literal is None:
            raise NativeNodeError(
                f"`{used}` is used inside `{name}` but lives outside it; "
                "move it into the function or pass it as an input"
            )
        constants.append(f"{used} = {literal}")
    return imports + constants


def _trim_blank(lines: list[str]) -> list[str]:
    start, end = 0, len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def _notebook_cells(fn: Callable[..., Any], outputs: Sequence[str] | None = None) -> list[str]:
    """The notebook cells of a ``@python_script`` function; the layout is a contract the canvas→code export reads.

    In order, each only when non-empty: the prelude (``import`` lines for modules the body uses
    but does not import, then module-level constants), the inputs cell (``# flowfile: inputs`` and
    ``<param> = flowfile_ctx.read_inputs()["main"][i]`` per parameter), the docstring as a note,
    then the body split at its Jupytext percent markers (``# %%`` code, ``# %% [markdown]`` note)
    and dedented. The top-level ``return <value>`` becomes ``# flowfile: outputs``,
    ``_result = <value>`` and the publish: to ``outputs[0]`` (default ``"main"``) for a frame, or a
    key-checked loop for a dict. Notes are cells of ``#`` comments, as the editor makes of
    imported markdown cells; core runs the cells joined as one script, so cell boundaries only
    matter in the designer.

    The kernel defines ``flowfile_ctx`` but imports nothing (``kernel_runtime/main.py``), hence
    ``import polars as pl`` in the prelude when the body uses ``pl``. ``read_inputs()["main"]``
    holds every input file in wiring order (``kernel_runtime/flowfile_client.py::read_inputs``,
    ``flowfile_core/kernel/execution.py::write_inputs_to_parquet``), and ``PythonScript`` wires
    frame *i* to the one multi-input handle in call order, so position *i* is parameter *i*. An
    upstream node whose reference is ``main`` takes over that key, which then holds only its file.
    ``publish_output`` takes a DataFrame or a LazyFrame (``flowfile_client.py::publish_output``).
    """
    name = _check_function(fn)
    parameters = _parameters(fn, name)
    lines, func, tokens, header_row = _function_source(fn, name)
    returned = _final_return(name, func)
    publish = _publish_lines(name, returned.value, outputs)

    body = func.body
    indent = body[0].col_offset
    docstring = None
    first = body[0]
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
        docstring = first
        if len(body) > 1 and body[1].lineno == docstring.end_lineno:
            raise NativeNodeError(f"The docstring of `{name}` must end on its own line")
    statements = body[1:] if docstring is not None else body
    spans = [(min([s.lineno] + [d.lineno for d in getattr(s, "decorator_list", ())]), s.end_lineno) for s in statements]

    own_comments = {
        token.start[0]: token
        for token in tokens
        if token.type == tokenize.COMMENT and not lines[token.start[0] - 1][: token.start[1]].strip()
    }
    # Rows after the first of a multi-line string keep their whitespace: it belongs to the string.
    verbatim = {row for t in tokens if t.type == tokenize.STRING for row in range(t.start[0] + 1, t.end[0] + 1)}
    verbatim.update(
        row for n in ast.walk(func) if isinstance(n, ast.JoinedStr) for row in range(n.lineno + 1, n.end_lineno + 1)
    )

    markers: dict[int, str] = {}
    for row, token in own_comments.items():
        match = _CELL_MARKER.match(token.string)
        if match is None or row <= header_row:
            continue
        if token.start[1] > indent or any(start < row <= end for start, end in spans):
            raise NativeNodeError(
                f"cell markers must sit between top-level statements of `{name}`; "
                f"the one on line {fn.__code__.co_firstlineno + row - 1} is inside a block"
            )
        markers[row] = match.group("rest") or ""

    def text(row: int) -> str:
        raw = lines[row - 1]
        if row in verbatim:
            return raw
        if not raw.strip():
            return ""
        return raw[indent:] if raw[:indent].isspace() else raw

    raw = lines[returned.lineno - 1]
    col = _char_col(raw, returned.col_offset)
    head = raw[:col].strip().rstrip(";").rstrip()
    rewritten = {returned.lineno: ([head] if head else []) + [OUTPUTS_MARKER, f"_result = {raw[col + 6 :].lstrip()}"]}
    if returned.end_lineno == returned.lineno:
        rewritten[returned.lineno] += publish
    else:
        rewritten[returned.end_lineno] = [text(returned.end_lineno), *publish]

    cells: list[str] = []
    prelude = _prelude(fn, func, name)
    if prelude:
        cells.append("\n".join(prelude))
    if parameters:
        reads = [f'{p} = flowfile_ctx.read_inputs()["main"][{i}]' for i, p in enumerate(parameters)]
        cells.append("\n".join([INPUTS_MARKER, *reads]))
    note = ast.get_docstring(func, clean=True) if docstring is not None else None
    if note:
        cells.append("\n".join(f"# {line}".rstrip() for line in note.splitlines()))

    skipped = set(range(docstring.lineno, docstring.end_lineno + 1)) if docstring is not None else set()
    kind, title, buffer = "code", "", []

    def close() -> None:
        content = ([f"# {title}"] if title else []) + _trim_blank(buffer)
        if content:
            cells.append("\n".join(content))

    for row in range(header_row + 1, len(lines) + 1):
        if row in skipped:
            continue
        if row in markers:
            close()
            rest = markers[row]
            kind = "note" if _MARKDOWN_TAG.search(rest) else "code"
            title, buffer = _MARKDOWN_TAG.sub("", rest).strip(), []
            continue
        if kind == "note" and row not in own_comments:
            close()
            kind, title, buffer = "code", "", []
        buffer.extend(rewritten[row] if row in rewritten else [text(row)])
    close()
    try:
        compile("\n\n".join(cells), f"<{name}>", "exec", dont_inherit=True)
    except SyntaxError as exc:
        raise NativeNodeError(
            f"The body of `{name}` does not run as top-level notebook cells: {exc.msg} ({(exc.text or '').strip()})"
        ) from exc
    return cells


class PythonScriptFunction:
    """A function placed as a Python Script node: its body is the notebook, its parameters the inputs.

    ``fn(*frames)`` places the node and returns its output frame; ``fn.node(*frames)`` returns the
    :class:`PythonScript` (``.output``, ``[name]``, ``.outputs``) and is the way to reach the frames
    of a function with several outputs. One frame is passed per parameter, in order. ``.fn`` is
    the undecorated function, to run it locally on Polars frames; ``.cells`` holds the notebook
    cells, built once when the function is decorated. Every error is :class:`NativeNodeError`.
    """

    fn: Callable[..., Any]
    cells: list[str]

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        kernel: str | Any | None = None,
        outputs: list[str] | None = None,
        returns: Mapping[str, Any] | None = None,
        description: str | None = None,
        flow_graph: FlowGraph | None = None,
    ) -> None:
        functools.update_wrapper(self, fn)
        self._outputs = _output_names(outputs)
        self._kernel = _kernel_id(kernel)
        self.cells = _notebook_cells(fn, outputs)
        self._parameters = _parameters(fn, fn.__name__)
        self._schemas = _returns_as_schemas(returns, self._outputs)
        _declared_columns(self._schemas, self._outputs, "returns=")
        self._description = description if description is not None else fn.__name__
        self._flow_graph = flow_graph
        self.fn = fn

    def __call__(self, *frames: FlowFrame) -> FlowFrame:
        if len(self._outputs) != 1:
            raise NativeNodeError(
                f"`{self.fn.__name__}` has outputs {self._outputs}; place it with "
                f"{self.fn.__name__}.node(...) and pick an output with [name]"
            )
        return self.node(*frames).output

    def node(self, *frames: FlowFrame) -> PythonScript:
        """Place the node and return it, for its ``.output``, ``[name]`` and ``.outputs``."""
        from flowfile_frame.flow_frame import FlowFrame

        name = self.fn.__name__
        if len(frames) != len(self._parameters):
            expected = f"{len(self._parameters)} input frame(s) ({', '.join(self._parameters)})"
            takes = expected if self._parameters else "no input frames"
            raise NativeNodeError(f"`{name}` takes {takes}, got {len(frames)}")
        for parameter, frame in zip(self._parameters, frames, strict=True):
            if not isinstance(frame, FlowFrame):
                raise NativeNodeError(f"`{name}` takes FlowFrames; `{parameter}` got {type(frame).__name__}")
        return PythonScript(
            *frames,
            cells=list(self.cells),
            kernel=self._kernel,
            outputs=list(self._outputs),
            schemas=self._schemas,
            description=self._description,
            flow_graph=None if self._parameters else self._flow_graph,
        )


def python_script(
    *,
    kernel: str | Any | None = None,
    outputs: list[str] | None = None,
    returns: Mapping[str, PolarsDataType] | Mapping[str, Mapping[str, PolarsDataType]] | None = None,
    description: str | None = None,
    flow_graph: FlowGraph | None = None,
) -> Callable[[Callable[..., Any]], PythonScriptFunction]:
    """Decorate a module-level function to place it as a Python Script (notebook) node.

    The body becomes the notebook: split at Jupytext ``# %%`` markers, ``# %% [markdown]`` notes
    and the docstring as the first note (see :func:`_notebook_cells`). Its parameters are the input
    frames and its single, final ``return`` is what the node publishes: a frame to the one output,
    or a dict of frames with ``outputs=[...]`` naming its keys. Modules and plain constants it
    reads from its module become prelude lines; any other outside name raises. ``returns``
    declares the output columns (``{column: dtype}``, or ``{output: {column: dtype}}`` for
    several) so frames built on the output know them before the flow runs. ``kernel`` and
    ``description`` (default: the function name) are the node's; ``flow_graph`` places a function
    without inputs. Everything is checked here, when the function is decorated.
    """

    def decorate(fn: Callable[..., Any]) -> PythonScriptFunction:
        return PythonScriptFunction(
            fn, kernel=kernel, outputs=outputs, returns=returns, description=description, flow_graph=flow_graph
        )

    return decorate
