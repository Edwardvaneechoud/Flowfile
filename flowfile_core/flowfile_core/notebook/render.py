"""Render a canvas flow as notebook cells: one ``fl``-dialect cell per node, never raising.

The renderer drives the FlowFrame exporter's per-node handlers (pure text emitters) in its own
walk instead of ``convert()``: nodes are visited in Kahn order with a min-heap on node id, every
node gets a cell (nodes the handlers cannot or must not render become ``fl.canvas_node``
placeholders), there is no chain fusion, no boundary renaming, no ``run_etl_pipeline`` wrapper and
no sentinel pass. ``${name}`` references stay verbatim in the emitted text and formulas holding one
are never translated, so rendering neither mutates live settings nor evaluates parameter text.
Gates, subflows, Python Scripts, flow ports and custom nodes render as their native ``fl.*`` classes;
a few exporter lowerings are replaced by the frame method that rebuilds the same node type.
"""

from __future__ import annotations

import ast
import builtins
import hashlib
import heapq
import io
import json
import keyword
import re
import textwrap
import tokenize
import types
from typing import Literal

import polars as pl
from pydantic import BaseModel, Field

from flowfile_core.configs import logger
from flowfile_core.flowfile.code_generator.code_generator import (
    NODE_TYPE_VAR_LABEL,
    FlowGraphToFlowFrameConverter,
    _polars_code_function_body,
)
from flowfile_core.flowfile.code_generator.connector_handlers import ConnectorHandlersMixin
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.param_types import stringify_param_value
from flowfile_core.flowfile.parameter_resolver import find_unresolved_in_model
from flowfile_core.flowfile.util.skip_rules import classify_graph, is_error_ish
from flowfile_core.notebook.compare import param_comparison_filter, strip_outer_parens
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.schemas.transform_schema import AUTO_DATA_TYPE

CellKind = Literal["imports", "parameters", "node"]
CellStatus = Literal["code", "placeholder", "unsupported"]

IMPORTS_CELL_ID = "imports"
PARAMETERS_CELL_ID = "parameters"
FLOW_VAR = "flow"
FL_IMPORT = "import flowfile as fl"
NATIVE_TYPES = frozenset({"gate", "run_flow", "python_script", "flow_input", "flow_output"})
_LAYOUT_FIELDS = frozenset({"pos_x", "pos_y", "is_setup", "flow_id", "user_id"})
_RESERVED = frozenset(keyword.kwlist) | frozenset(dir(builtins)) | {"fl", "ff", "pl", FLOW_VAR, "main"}


class EmittedCell(BaseModel):
    """One notebook cell: the code for ``node_ids`` (empty for the imports and parameters cells)."""

    cell_id: str
    node_ids: list[int] = Field(default_factory=list)
    kind: CellKind
    code: str
    defines: list[str] = Field(default_factory=list)
    uses: list[str] = Field(default_factory=list)
    status: CellStatus = "code"
    reason: str | None = None


class NotebookRendering(BaseModel):
    """The rendered notebook for one flow; ``code_fingerprint`` is :func:`code_fingerprint` of it."""

    cells: list[EmittedCell]
    warnings: list[str] = Field(default_factory=list)
    var_by_node: dict[int, str] = Field(default_factory=dict)
    code_fingerprint: str


def cell_id_for(node_id: int) -> str:
    """The stable cell id of a node's cell."""
    return f"node-{node_id}"


def node_label(node_type: str, node_id: int) -> str:
    """The deterministic variable name of an unnamed node, matching the frame's ``node_label``."""
    label = NODE_TYPE_VAR_LABEL.get(node_type) or re.sub(r"\W", "_", node_type)
    return f"{label}_{node_id}"


def topological_order(nodes: list[FlowNode]) -> list[FlowNode]:
    """Kahn's algorithm with a min-heap on node id; nodes left in a cycle follow in id order."""
    by_id = {node.node_id: node for node in nodes}
    in_degree = dict.fromkeys(by_id, 0)
    for node in nodes:
        for downstream in node.leads_to_nodes:
            if downstream.node_id in in_degree:
                in_degree[downstream.node_id] += 1
    heap = [node_id for node_id, degree in in_degree.items() if degree == 0]
    heapq.heapify(heap)
    order: list[FlowNode] = []
    while heap:
        node = by_id[heapq.heappop(heap)]
        order.append(node)
        for downstream in node.leads_to_nodes:
            if downstream.node_id in in_degree:
                in_degree[downstream.node_id] -= 1
                if in_degree[downstream.node_id] == 0:
                    heapq.heappush(heap, downstream.node_id)
    seen = {node.node_id for node in order}
    order.extend(by_id[node_id] for node_id in sorted(set(by_id) - seen))
    return order


def _settings_payload(node: FlowNode):
    settings = node.setting_input
    if settings is None:
        return None
    try:
        dumped = settings.model_dump(mode="json")
    except Exception:
        return {"unserializable": type(settings).__name__}
    return {key: value for key, value in dumped.items() if key not in _LAYOUT_FIELDS}


def code_fingerprint(flow_graph: FlowGraph) -> str:
    """sha256 over every node's settings (layout fields dropped), the edges and the parameters.

    Position-free and deterministic (sorted keys, nodes in id order), so moving a node on the canvas
    keeps the fingerprint and any settings, wiring or parameter change moves it. ``flow_id`` (fresh
    on every open), ``user_id`` (server-stamped) and the ``is_setup`` flag are dropped with the
    positions because none of them changes the rendered code.
    """
    nodes = []
    for node in sorted(flow_graph.nodes, key=lambda n: n.node_id):
        try:
            edges = [[e.source, e.sourceHandle, e.target, e.targetHandle] for e in node.get_edge_input()]
        except Exception:
            edges = []
        nodes.append({"id": node.node_id, "type": node.node_type, "settings": _settings_payload(node), "edges": edges})
    parameters = [p.model_dump(mode="json") for p in flow_graph.flow_settings.parameters]
    payload = json.dumps({"nodes": nodes, "parameters": parameters}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _is_setup(node: FlowNode) -> bool:
    """``FlowNode.is_setup`` without its side effect of flipping the live settings' flag."""
    if node.node_information.is_setup:
        return True
    return getattr(node.function, "__name__", None) != "placeholder"


def _has_sensitive_rest_key(settings) -> bool:
    rest = getattr(settings, "rest_api_settings", None)
    if rest is None:
        return False
    keys = list((rest.headers or {}).keys()) + list((rest.query_params or {}).keys())
    return any(isinstance(k, str) and k.lower() in ConnectorHandlersMixin._SENSITIVE_KEYS for k in keys)


def _is_user_defined(settings) -> bool:
    return isinstance(settings, input_schema.UserDefinedNode) or bool(getattr(settings, "is_user_defined", False))


def _to_fl(code: str) -> str:
    """Rename the exporter's ``ff`` module alias to ``fl`` (NAME tokens only, never inside strings)."""
    if "ff" not in code:
        return code
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(code).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return re.sub(r"\bff\.", "fl.", code)
    lines = code.split("\n")
    for tok in reversed(tokens):
        if tok.type == tokenize.NAME and tok.string == "ff":
            row, col = tok.start
            line = lines[row - 1]
            lines[row - 1] = line[:col] + "fl" + line[col + 2 :]
    return "\n".join(lines)


_DEFINITIONS = (ast.FunctionDef, ast.Import, ast.ImportFrom)
_PARAM_REF = re.compile(r"\$\{(\w+)\}")
_COMPARISON_SYMBOLS = {
    "equals": "==",
    "not_equals": "!=",
    "greater_than": ">",
    "greater_than_or_equals": ">=",
    "less_than": "<",
    "less_than_or_equals": "<=",
}


def _with_description(code: str, description: str) -> str | None:
    """Add ``description=`` to the call a cell's last statement assigns (after defs and imports only), else None."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return None
    *head, last = tree.body or [None]
    if not isinstance(last, ast.Assign) or not all(isinstance(stmt, _DEFINITIONS) for stmt in head):
        return None
    call = last.value
    if not isinstance(call, ast.Call):
        return None
    while isinstance(call.func, ast.Attribute) and call.func.attr == "agg" and isinstance(call.func.value, ast.Call):
        call = call.func.value
    lines = code.split("\n")
    line = lines[call.end_lineno - 1]
    col = len(line.encode("utf-8")[: call.end_col_offset - 1].decode("utf-8", errors="ignore"))
    offset = sum(len(prior) + 1 for prior in lines[: call.end_lineno - 1]) + col
    before = code[:offset].rstrip()
    separator = "" if before.endswith("(") else " " if before.endswith(",") else ", "
    kwarg = f"description={json.dumps(description, ensure_ascii=False)}"
    updated = before + separator + kwarg + code[offset:]
    try:
        ast.parse(updated)
    except SyntaxError:
        return None
    return updated


def _literal(value) -> str:
    return json.dumps(value, ensure_ascii=False) if isinstance(value, str) else repr(value)


def _assigned_names(tree: ast.Module) -> list[str]:
    """Top-level names a cell binds, in order: assignment targets, defs and imports."""
    names: list[str] = []
    for stmt in tree.body:
        found: list[str] = []
        if isinstance(stmt, ast.Assign | ast.AnnAssign | ast.AugAssign):
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            found = [sub.id for target in targets for sub in ast.walk(target) if isinstance(sub, ast.Name)]
        elif isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            found = [stmt.name]
        elif isinstance(stmt, ast.Import | ast.ImportFrom):
            found = [alias.asname or alias.name.split(".")[0] for alias in stmt.names]
        names.extend(name for name in found if name not in names)
    return names


def _loaded_names(tree: ast.Module) -> set[str]:
    return {sub.id for sub in ast.walk(tree) if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load)}


def _producer_ids(converter: FlowGraphToFlowFrameConverter, node: FlowNode) -> list[int]:
    """Upstream node ids: main, left and right inputs plus keyed inputs (run_flow slots)."""
    keyed = node.node_inputs.keyed_inputs or {}
    return converter._raw_producer_ids(node) + [source.node_id for source in keyed.values() if source is not None]


def _str_literal(text: str) -> str:
    """A string literal: triple-quoted for multi-line text it spells verbatim, else JSON-escaped."""
    if "\n" in text and '"""' not in text and "\\" not in text and "\r" not in text and not text.endswith('"'):
        return f'"""{text}"""'
    return json.dumps(text, ensure_ascii=False)


def _value_literal(value) -> str | None:
    """A Python literal that evaluates back to ``value``, or None."""
    if isinstance(value, str):
        return _str_literal(value)
    text = repr(value)
    try:
        restored = ast.literal_eval(text)
    except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
        return None
    return text if type(restored) is type(value) and restored == value else None


def _call(func: str, args: list[str]) -> str:
    """``func(args)`` on one line when it fits, else one argument per line."""
    one = f"{func}({', '.join(args)})"
    if len(one) <= 100 and "\n" not in one:
        return one
    return f"{func}(\n" + "".join(f"    {arg},\n" for arg in args) + ")"


def _dtype_expr(data_type: str) -> str | None:
    """``fl.<dtype>`` for a stored dtype string, when it evaluates to a Polars dtype."""
    text = f"pl.{data_type}"
    try:
        value = eval(text, {"__builtins__": {}}, {"pl": pl})  # noqa: S307
    except Exception:
        return None
    if isinstance(value, pl.DataType) or (isinstance(value, type) and issubclass(value, pl.DataType)):
        return "fl." + data_type
    return None


def _schema_literal(columns: list[tuple[str, str]]) -> str | None:
    """``{"col": fl.Int64, ...}`` for ``(name, dtype string)`` pairs, or None when a dtype has no form."""
    entries = []
    for name, data_type in columns:
        dtype = _dtype_expr(data_type)
        if dtype is None:
            return None
        entries.append(f"{json.dumps(name, ensure_ascii=False)}: {dtype}")
    return "{" + ", ".join(entries) + "}"


_INPUT_READ = re.compile(r'^(?P<name>[A-Za-z_]\w*) = flowfile_ctx\.read_inputs\(\)\["main"\]\[(?P<index>\d+)\]$')
_PUBLISH_STARTS = (
    "flowfile_ctx.publish_output(_result",
    "if isinstance(_result, dict):",
    "if not isinstance(_result, dict)",
)


_SESSION_PRELUDE = {"pl": "import polars as pl"}


def _nested_literal(literals: dict[str, str]) -> str:
    return "{" + ", ".join(f"{json.dumps(key)}: {value}" for key, value in literals.items()) + "}"


def _is_note(cell: str) -> bool:
    lines = cell.split("\n")
    return bool(lines) and all(line.startswith("#") for line in lines)


def _undocstring(cell: str) -> str | None:
    """The docstring text a ``#``-note cell was generated from, or None when it cannot be spelled back."""
    text = "\n".join(line[2:] if line.startswith("# ") else line[1:] for line in cell.split("\n"))
    if '"""' in text or "\\" in text or text.endswith('"') or not text.strip():
        return None
    return text


def _script_function_text(name: str, parameters: list[str], body_cells: list[str], docstring: str | None) -> str:
    """A ``def`` whose body is ``body_cells`` joined at ``# %%`` markers (``# %% [markdown]`` for notes)."""
    lines = [f"def {name}({', '.join(parameters)}):"]
    if docstring is not None:
        doc = docstring.split("\n")
        lines.append(f'    """{doc[0]}' + ("" if len(doc) > 1 else '"""'))
        if len(doc) > 1:
            lines.extend(f"    {line}" if line else "" for line in doc[1:])
            lines.append('    """')
    for index, cell in enumerate(body_cells):
        cell_lines = cell.split("\n")
        if index:
            lines.append("")
            if _is_note(cell):
                lines.append("    # %% [markdown]")
            elif len(cell_lines) > 1 and re.fullmatch(r"# [^%\s].*", cell_lines[0]):
                lines.append(f"    # %% {cell_lines.pop(0)[2:]}")
            else:
                lines.append("    # %%")
        lines.extend(f"    {line}" if line else "" for line in cell_lines)
    return "\n".join(lines)


def _decorator_parts(cells: list[str]) -> tuple | None:
    """Split stored cells into prelude lines, parameters, candidate ``(docstring, body cells)`` and a name, or None.

    The layout is ``python_script._notebook_cells``': optional prelude, the inputs cell, an
    optional docstring note, the body, and a last cell holding the outputs marker. Each body
    candidate (with or without the docstring) is checked by regenerating it in the renderer. The
    function name is only recoverable from a dict guard's message, which spells it.
    """
    from flowfile_frame.python_script import INPUTS_MARKER, OUTPUTS_MARKER

    marker = next((i for i, cell in enumerate(cells) if cell.split("\n", 1)[0] == INPUTS_MARKER), None)
    if marker is None or marker > 1 or len(cells) < marker + 2:
        return None
    reads = [_INPUT_READ.match(line) for line in cells[marker].split("\n")[1:]]
    if not reads or any(match is None for match in reads):
        return None
    if [int(match["index"]) for match in reads] != list(range(len(reads))):
        return None
    last = cells[-1].split("\n")
    if OUTPUTS_MARKER not in last:
        return None
    at = last.index(OUTPUTS_MARKER)
    if at + 1 >= len(last) or not last[at + 1].startswith("_result = "):
        return None
    end = next((j for j in range(at + 2, len(last)) if last[j].startswith(_PUBLISH_STARTS)), None)
    if end is None:
        return None
    value = [last[at + 1][len("_result = ") :], *last[at + 2 : end]]
    closing = "\n".join([*last[:at], "return " + value[0], *value[1:]])
    body = [*cells[marker + 1 : -1], closing]
    candidates: list[tuple[str | None, list[str]]] = [(None, body)]
    docstring = _undocstring(body[0]) if len(body) > 1 and _is_note(body[0]) else None
    if docstring is not None:
        candidates.insert(0, (docstring, body[1:]))
    prelude = cells[0].split("\n") if marker == 1 else []
    named = re.search(r'raise \w+Error\("(?P<name>[A-Za-z_]\w*) (?:returns|must return) a dict', "\n".join(last[end:]))
    return prelude, [match["name"] for match in reads], candidates, named["name"] if named else None


class _NotebookConverter(FlowGraphToFlowFrameConverter):
    """The FlowFrame converter with the notebook's settings access and formula policy."""

    def _settings_for(self, node: FlowNode):
        """Live settings, or a private deep copy when they hold ``${name}`` refs; never substituted."""
        cached = self._settings_cache.get(node.node_id)
        if cached is not None:
            return cached
        settings = node.setting_input
        if settings is not None and not isinstance(settings, input_schema.NodePromise):
            try:
                if find_unresolved_in_model(settings):
                    settings = settings.model_copy(deep=True)
            except Exception:
                settings = settings.model_copy(deep=True)
        if settings is not None:
            self._settings_cache[node.node_id] = settings
        return settings

    def _compute_gate_conditions(self, execution_plan) -> None:
        """Gates render as cells of their own; the exporter's if-block machinery stays off."""

    def _translate_to_ff_code(self, formula: str) -> str | None:
        """A formula holding ``${name}`` keeps its verbatim string form; it is never translated.

        Redundant outer parentheses are dropped first: the frame stores ``[a] > 1`` as ``([a] > 1)``,
        and the translator spells the literal differently inside them, so both spellings render alike.
        """
        if "${" in formula:
            return None
        return super()._translate_to_ff_code(strip_outer_parens(formula))

    param_vars: dict[str, str] = {}

    def _param_var(self, value: str | None) -> str | None:
        """The declared parameter variable a whole-field ``${name}`` value stands for, else None."""
        match = _PARAM_REF.fullmatch(value) if isinstance(value, str) else None
        return self.param_vars.get(match.group(1)) if match else None

    def _create_basic_filter_expr(self, basic: transform_schema.BasicFilter, field_dtype: str | None = None) -> str:
        """A comparison against a whole-field ``${name}`` compares with the parameter's variable.

        ``fl.col("x") > MIN_SALARY`` is what the frame stores back as the ``${min_salary}`` ref; the
        literal ``"${min_salary}"`` would compare with text once substituted.
        """
        try:
            operator = str(basic.get_operator())
        except (ValueError, AttributeError):
            operator = str(basic.operator)
        column = f"{self.framework}.col({self._py_str(basic.field)})"
        value = self._param_var(basic.value)
        if value is not None and operator in _COMPARISON_SYMBOLS:
            return f"{column} {_COMPARISON_SYMBOLS[operator]} {value}"
        value2 = self._param_var(basic.value2)
        if operator == "between" and value is not None and value2 is not None:
            return f"({column} >= {value}) & ({column} <= {value2})"
        return super()._create_basic_filter_expr(basic, field_dtype)

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        """An advanced filter that is exactly one basic comparison renders as the basic filter does.

        The frame stores every filter it builds as advanced text, so a canvas basic filter comes back
        advanced; rendering both through the basic emission keeps the cell text stable. That covers a
        comparison with a declared parameter (``([x] > ${p})``) as well.
        """
        from flowfile_core.flowfile.share.filter_translation import translate_advanced_filter

        filter_input = settings.filter_input
        if not settings.split_mode and filter_input.is_advanced():
            advanced = filter_input.advanced_filter
            if "${" not in advanced:
                basic = translate_advanced_filter(strip_outer_parens(advanced))
            else:
                basic = param_comparison_filter(advanced)
                refs = [basic["basic_filter"]["value"], basic["basic_filter"].get("value2")] if basic else []
                if not all(self._param_var(ref) for ref in refs if ref is not None):
                    basic = None
            if basic is not None:
                settings = settings.model_copy(
                    update={"filter_input": transform_schema.FilterInput.model_validate(basic)}
                )
        super()._handle_filter(settings, var_name, input_vars)

    @staticmethod
    def _join_suffix(settings: transform_schema.JoinInputManager) -> str | None:
        """The exporter's suffix rule without its key-order condition.

        The Polars export needs kept right keys after the other right columns to match the canvas
        column order; the frame's native join (``keep_right_keys=True``) rebuilds the same node, so
        any order is exact here.
        """
        if settings.how not in ("left", "inner"):
            return None
        left = settings.left_select.renames
        if any(not column.keep or column.new_name != column.old_name for column in left):
            return None
        left_names = {column.old_name for column in left}
        right = settings.right_select.renames
        if len({column.keep for column in right if column.join_key}) != 1:
            return None
        suffixes = set()
        for column in right:
            if not column.keep:
                if not column.join_key:
                    return None
            elif column.old_name in left_names:
                if not column.new_name.startswith(column.old_name) or column.new_name in left_names:
                    return None
                suffixes.add(column.new_name[len(column.old_name) :])
            elif column.new_name != column.old_name:
                return None
        if len(suffixes) > 1:
            return None
        return suffixes.pop() if suffixes else "_right"

    def _emit_suffix_join(
        self,
        settings: input_schema.NodeJoin,
        var_name: str,
        left_df: str,
        right_df: str,
        left_on: list[str],
        right_on: list[str],
        suffix: str,
        keep_right_keys: bool,
    ) -> None:
        """One native ``.join``: kept right keys as ``keep_right_keys=True``, never ``coalesce=False`` (Polars code)."""
        kwargs = [f"left_on={left_on}", f"right_on={right_on}", f'how="{settings.join_input.how}"']
        if suffix != "_right":
            kwargs.append(f"suffix={self._py_str(suffix)}")
        if keep_right_keys:
            kwargs.append("keep_right_keys=True")
        self._add_code(f"{var_name} = {left_df}.join(")
        self._add_code(f"        {right_df},")
        for index, kwarg in enumerate(kwargs):
            self._add_code(f"        {kwarg}{',' if index < len(kwargs) - 1 else ''}")
        self._add_code("    )")

    def _handle_polars_code(
        self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """``<input>.polars_code(_polars_code_N, *others)`` over a ``def`` holding the stored code verbatim.

        The frame reads the function's source rather than calling it, so ``pl.`` stays as written. The
        body ends in the ``return`` the node's runtime wrapper adds (the frame drops a lone ``return
        <expr>`` or a closing ``return output_df`` again); a body whose result is another assigned
        name is left without one, since the frame would keep it. A node with no input is a source:
        ``ff.polars_code(_polars_code_N)`` over a ``def`` without parameters.
        """
        if len(input_vars) == 1:
            inputs = list(input_vars.values())
        else:
            inputs = [input_vars[key] for key in sorted(input_vars) if key.startswith("main")]
        code = textwrap.dedent(settings.polars_code_input.polars_code).strip()
        names = [f"input_df_{i}" for i in range(1, len(inputs) + 1)] if len(inputs) > 1 else ["input_df"][: len(inputs)]
        function = f"_polars_code_{settings.node_id}"
        if re.search(r"\bpl\.", code):
            self.imports.add("import polars as pl")
        body, returned = _polars_code_function_body(code)
        assigned = returned is not None and re.search(rf"^{re.escape(returned)}\s*=[^=]", "\n".join(body), re.M)
        if assigned and returned != "output_df":
            returned = None
        self._add_code(f"def {function}({', '.join(f'{name}: ff.FlowFrame' for name in names)}):")
        for line in body:
            self._add_code(f"    {line}")
        if returned is not None:
            self._add_code(f"    return {returned}")
        elif not body:
            self._add_code("    pass")
        self._add_code("")
        self._add_code("")
        if inputs:
            self._add_code(f"{var_name} = {inputs[0]}.polars_code({', '.join([function, *inputs[1:]])})")
        else:
            self._add_code(f"{var_name} = ff.polars_code({function})")
        self._add_code("")

    def _formula_entry_native_expr(self, entry) -> str | None:
        """A typed entry keeps its keyword form: ``.cast()`` would rebuild as ``to_float(...)`` with an Auto type."""
        if entry.field.data_type not in (None, AUTO_DATA_TYPE):
            return None
        return super()._formula_entry_native_expr(entry)

    def _csv_scan_kwarg_lines(self, file_settings: input_schema.ReceivedTable) -> list[str]:
        """The stored UTF-8 variant (strict or lossy) and each option that differs from ``scan_csv``'s default."""
        lines = super()._csv_scan_kwarg_lines(file_settings)
        table = file_settings.table_settings
        encoding = "utf8-lossy" if "LOSSY" in (table.encoding or "").upper() else "utf8"
        lines = [f'    encoding="{encoding}",' if line.strip().startswith("encoding=") else line for line in lines]
        if table.infer_schema_length != 100:
            lines.append(f"    infer_schema_length={table.infer_schema_length},")
        if not table.infer_schema:
            lines.append("    infer_schema=False,")
        if table.quote_char != '"':
            lines.append(f"    quote_char={self._py_str(table.quote_char) if table.quote_char else None},")
        if table.truncate_ragged_lines:
            lines.append("    truncate_ragged_lines=True,")
        return lines

    def _handle_catalog_reader(
        self, settings: input_schema.NodeCatalogReader, var_name: str, input_vars: dict[str, str]
    ) -> None:
        full_table = settings.catalog_full_table_name
        self._stored_namespace = full_table.rsplit(".", 1)[0] if full_table and "." in full_table else None
        try:
            super()._handle_catalog_reader(settings, var_name, input_vars)
        finally:
            self._stored_namespace = None

    def _emit_catalog_namespace(self, full_name: str | None, namespace_id: int | None) -> None:
        """The namespace as the node stores it (a full name, else its id), so the rebuilt node matches."""
        full_name = full_name or getattr(self, "_stored_namespace", None)
        if full_name:
            self._add_code(f"    namespace_full_name={self._py_str(full_name)},")
        elif namespace_id is not None:
            self._add_code(f"    namespace_id={namespace_id},")

    def _handle_output(self, settings: input_schema.NodeOutput, var_name: str, input_vars: dict[str, str]) -> None:
        """CSV and Parquet outputs as the frame's typed writers (a native Output node), never ``sink_*``."""
        output = settings.output_settings
        source = input_vars.get("main", "df")
        path = self._py_path(output.abs_file_path)
        table = output.table_settings
        if output.file_type == "csv":
            kwargs = [f"separator={self._py_str(table.delimiter)}"]
            if table.encoding != input_schema.OutputCsvTable().encoding:
                kwargs.append(f"encoding={self._py_str(table.encoding)}")
            self._add_code(f"{var_name} = {source}.write_csv({path}, {', '.join(kwargs)})")
        elif output.file_type == "parquet":
            compression = getattr(table, "compression", None)
            extra = "" if compression in (None, "zstd") else f", compression={self._py_str(compression)}"
            self._add_code(f"{var_name} = {source}.write_parquet({path}{extra})")
        else:
            super()._handle_output(settings, var_name, input_vars)
            return
        self._add_code("")

    def _handle_text_to_rows(
        self, settings: input_schema.NodeTextToRows, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """``.text_to_rows(...)``, the frame's native Text to Rows node."""
        text = settings.text_to_rows_input
        args = [self._py_str(text.column_to_split)]
        if text.output_column_name and text.output_column_name != text.column_to_split:
            args.append(f"output_column={self._py_str(text.output_column_name)}")
        if text.split_by_fixed_value or not text.split_by_column:
            args.append(f"delimiter={self._py_str(text.split_fixed_value or ',')}")
        else:
            args.append(f"split_by_column={self._py_str(text.split_by_column)}")
        self._add_code(f"{var_name} = {input_vars.get('main', 'df')}.text_to_rows({', '.join(args)})")
        self._add_code("")

    def _handle_record_id(self, settings: input_schema.NodeRecordId, var_name: str, input_vars: dict[str, str]) -> None:
        """``.with_row_index(name, offset=..., group_by=[...])``, the frame's native Record Id node."""
        record = settings.record_id_input
        args = [self._py_str(record.output_column_name), f"offset={record.offset}"]
        if record.group_by and record.group_by_columns:
            args.append(f"group_by={[str(column) for column in record.group_by_columns]}")
        self._add_code(f"{var_name} = {input_vars.get('main', 'df')}.with_row_index({', '.join(args)})")
        self._add_code("")

    def _handle_unpivot(self, settings: input_schema.NodeUnpivot, var_name: str, input_vars: dict[str, str]) -> None:
        """``.unpivot(on=[...], index=[...])``, the frame's native Unpivot node; a dtype selector keeps its form."""
        unpivot = settings.unpivot_input
        if unpivot.data_type_selector_mode != "column":
            super()._handle_unpivot(settings, var_name, input_vars)
            return
        args = []
        if unpivot.value_columns:
            args.append(f"on={list(unpivot.value_columns)}")
        if unpivot.index_columns:
            args.append(f"index={list(unpivot.index_columns)}")
        self._add_code(f"{var_name} = {input_vars.get('main', 'df')}.unpivot({', '.join(args)})")
        self._add_code("")


class NotebookRenderer:
    """Render a flow graph into :class:`NotebookRendering`; one instance per render."""

    def __init__(self, flow_graph: FlowGraph):
        self.flow_graph = flow_graph
        self.converter = _NotebookConverter(flow_graph)
        self.warnings: list[str] = []
        self.used: set[str] = set(_RESERVED)
        self.param_vars: dict[str, str] = {}
        self.converter.param_vars = self.param_vars
        self.prelude: dict[str, str] = {}

    def render(self) -> NotebookRendering:
        """Build every cell; never raises for a node (a failing node becomes a placeholder)."""
        graph = self.flow_graph
        order = topological_order(list(graph.nodes))
        param_cell = self._parameters_cell()
        blocked = self._blocked_reasons(order)
        var_by_node = self._assign_names(order)
        node_cells = [self._node_cell(node, var_by_node[node.node_id], blocked.get(node.node_id)) for node in order]
        effective = {node.node_id: self.converter.node_var_mapping.get(node.node_id) for node in order}
        cells = [self._imports_cell()] + ([param_cell] if param_cell else []) + node_cells
        self._fill_uses(cells)
        self.warnings.extend(self.converter.warnings)
        return NotebookRendering(
            cells=cells,
            warnings=self.warnings,
            var_by_node={nid: var or var_by_node[nid] for nid, var in effective.items()},
            code_fingerprint=code_fingerprint(graph),
        )

    def _imports_cell(self) -> EmittedCell:
        imports = {FL_IMPORT} | {line for line in self.converter.imports if line != "import flowfile as ff"}
        ordered = [FL_IMPORT] + sorted(imports - {FL_IMPORT})
        return EmittedCell(cell_id=IMPORTS_CELL_ID, kind="imports", code="\n".join(ordered))

    def _parameters_cell(self) -> EmittedCell | None:
        parameters = list(self.flow_graph.flow_settings.parameters)
        if not parameters:
            return None
        lines = []
        for param in parameters:
            base = re.sub(r"\W", "_", param.name).upper() or "PARAM"
            if base[0].isdigit():
                base = f"P_{base}"
            var = self.converter._uniquify(base, self.used)
            self.param_vars[param.name] = var
            lines.append(f"{var} = fl.add_flow_parameter({FLOW_VAR}, {self._parameter_call(param)})")
        return EmittedCell(cell_id=PARAMETERS_CELL_ID, kind="parameters", code="\n".join(lines))

    @staticmethod
    def _parameter_call(param) -> str:
        """``fl.Parameter(...)``; the default is typed when it stringifies back to the stored text."""
        args = [json.dumps(param.name)]
        if param.default_value != "":
            default = param.default_value
            try:
                typed = param.typed_default()
                if stringify_param_value(typed) == param.default_value:
                    default = typed
            except Exception:
                pass
            args.append(f"default={_literal(default)}")
        if param.type != "string":
            args.append(f"type={json.dumps(param.type)}")
        if param.description:
            args.append(f"description={json.dumps(param.description, ensure_ascii=False)}")
        if param.enum_values:
            args.append(f"enum_values={json.dumps(list(param.enum_values), ensure_ascii=False)}")
        return f"fl.Parameter({', '.join(args)})"

    def _blocked_reasons(self, order: list[FlowNode]) -> dict[int, str]:
        """Placeholder reasons for unconfigured / mis-wired nodes and their whole downstream closure."""
        try:
            statuses = classify_graph(list(self.flow_graph.nodes))
        except Exception:
            statuses = {}
        reasons: dict[int, str] = {}
        for node in order:
            upstream = [pid for pid in _producer_ids(self.converter, node) if pid in reasons]
            try:
                correct = node.is_correct
            except Exception:
                correct = False
            if not _is_setup(node) or isinstance(node.setting_input, input_schema.NodePromise):
                reasons[node.node_id] = "not configured yet"
            elif not correct:
                reasons[node.node_id] = "inputs not fully connected"
            elif upstream:
                reasons[node.node_id] = f"downstream of node {min(upstream)}, which is not editable as code"
            elif is_error_ish(statuses.get(node.node_id)):
                reasons[node.node_id] = "skipped on error by the engine"
        return reasons

    def _assign_names(self, order: list[FlowNode]) -> dict[int, str]:
        names: dict[int, str] = {}
        references = {}
        for node in order:
            ref = getattr(node.setting_input, "node_reference", None)
            if ref and ref not in references.values():
                references[node.node_id] = ref
        self.used |= set(references.values())
        for node in order:
            ref = references.get(node.node_id)
            if ref:
                names[node.node_id] = ref
                continue
            dup = getattr(node.setting_input, "node_reference", None)
            if dup:
                self.warnings.append(f"Node {node.node_id} repeats the reference {dup!r}; it renders under a label")
            names[node.node_id] = self.converter._uniquify(node_label(node.node_type, node.node_id), self.used)
        return names

    def _node_cell(self, node: FlowNode, var: str, blocked_reason: str | None) -> EmittedCell:
        settings = node.setting_input
        status: CellStatus = "placeholder"
        reason = blocked_reason
        if node.node_type == "polars_lazy_frame":
            status, reason = "unsupported", "a Polars LazyFrame node cannot be rebuilt; replace it on the canvas"
        elif reason is None:
            if node.node_type == "explore_data":
                reason = "explore data is interactive only"
            elif node.node_type == "rest_api_reader" and _has_sensitive_rest_key(settings):
                reason = "headers or query parameters hold a credential"
        if reason is None:
            native = node.node_type in NATIVE_TYPES or _is_user_defined(settings)
            code, reason = self._emit_native(node, var) if native else self._emit(node, var)
            if code is not None:
                return self._cell(node, code, "code", None)
        return self._cell(node, self._placeholder(node, var, reason), status, reason)

    def _emit(self, node: FlowNode, var: str) -> tuple[str | None, str | None]:
        """Run the node's handler; ``(code, None)`` or ``(None, reason)`` with all state rolled back."""
        conv = self.converter
        start, imports = len(conv.code_lines), set(conv.imports)
        unsupported = len(conv.unsupported_nodes)
        handle_vars = dict(conv.node_handle_var_mapping)
        conv.node_var_mapping[node.node_id] = var
        settings = conv._settings_for(node)
        try:
            handler = getattr(conv, f"_handle_{node.node_type}", None)
            if handler is None:
                reason = f"no code form for node type {node.node_type!r} yet"
            else:
                handler(settings, var, conv._get_input_vars(node))
                reason = conv.unsupported_nodes[unsupported][2] if len(conv.unsupported_nodes) > unsupported else None
        except Exception as exc:
            logger.warning("Notebook render: node %s (%s) failed: %s", node.node_id, node.node_type, exc)
            reason = f"could not render: {exc}"
        lines = conv.code_lines[start:]
        while lines and not lines[-1].strip():
            lines = lines[:-1]
        if reason is None and not lines:
            reason = "renders no code"
        if reason is None:
            code = _to_fl("\n".join(lines))
            description = getattr(node.setting_input, "description", None)
            if description:
                described = _with_description(code, description)
                if described is None:
                    self.warnings.append(f"Node {node.node_id}: description not rendered for a multi-statement cell")
                code = described or code
            return code, None
        del conv.code_lines[start:]
        del conv.unsupported_nodes[unsupported:]
        conv.imports = imports
        conv.node_handle_var_mapping = handle_vars
        conv.node_var_mapping[node.node_id] = var
        return None, reason

    def _placeholder(self, node: FlowNode, var: str, reason: str) -> str:
        conv = self.converter
        args = [str(node.node_id)] + list(conv._get_input_vars(node).values())
        conv.node_var_mapping[node.node_id] = var
        output_names = getattr(node.setting_input, "output_names", None) or ["main"]
        if len(output_names) > 1:
            for index in range(len(output_names)):
                conv.node_handle_var_mapping[(node.node_id, f"output-{index}")] = f'{var}["output-{index}"]'
        label = getattr(node.node_template, "name", None) or node.node_type
        comment = " ".join(f"{label}: {reason}".split())
        return f"{var} = fl.canvas_node({', '.join(args)})  # {comment}"

    def _emit_native(self, node: FlowNode, var: str) -> tuple[str | None, str | None]:
        """A native-class cell (gate, run_flow, python_script, flow ports, custom nodes); rolled back on failure."""
        conv = self.converter
        handle_vars = dict(conv.node_handle_var_mapping)
        imports = set(conv.imports)
        conv.node_var_mapping[node.node_id] = var
        settings = conv._settings_for(node)
        try:
            if _is_user_defined(settings):
                code, reason = self._native_custom_node(node, var, settings)
            else:
                code, reason = getattr(self, f"_native_{node.node_type}")(node, var, settings)
        except Exception as exc:
            logger.warning("Notebook render: node %s (%s) failed: %s", node.node_id, node.node_type, exc)
            code, reason = None, f"could not render: {exc}"
        if code is None:
            conv.node_handle_var_mapping = handle_vars
            conv.imports = imports
            conv.node_var_mapping[node.node_id] = var
        return code, reason

    def _description_args(self, node: FlowNode) -> list[str]:
        description = getattr(node.setting_input, "description", None)
        return [f"description={json.dumps(description, ensure_ascii=False)}"] if description else []

    def _bind_outputs(self, node: FlowNode, var: str, accessors: list[str]) -> None:
        """Consumers of output handle ``k`` read ``var`` + ``accessors[k]`` (``.then``, ``["name"]``, ...)."""
        for index, accessor in enumerate(accessors):
            self.converter.node_handle_var_mapping[(node.node_id, f"output-{index}")] = f"{var}{accessor}"

    def _param_ref(self, value: str) -> str | None:
        """The parameter variable a whole-value ``${name}`` reference stands for, if the flow declares it."""
        match = re.fullmatch(r"\$\{(\w+)\}", value) if isinstance(value, str) else None
        return self.param_vars.get(match.group(1)) if match else None

    def _native_gate(self, node: FlowNode, var: str, settings: input_schema.NodeGate) -> tuple[str | None, str | None]:
        """``fl.Gate(frame, formula | parameter=, operator=, value=, control=, else_output=<stored>)``."""
        from flowfile_core.flowfile.util.skip_rules import parameter_gate_is_open

        gate = settings.gate_input
        inputs = self.converter._get_input_vars(node)
        data, control = inputs.get("main"), inputs.get("right")
        if data is None:
            return None, "the gate has no data input"
        args = [data]
        if gate.condition_source == "formula":
            formula = gate.formula or ""
            if not formula.strip():
                return None, "the gate has no formula yet"
            if any(f"[${{{name}}}" in formula for name in find_unresolved_in_model(formula)):
                return None, "the formula names a column through a parameter"
            args.append(_str_literal(formula))
            if control is not None:
                args.append(f"control={control}")
        else:
            if control is not None:
                return None, "a parameter gate with a control input has no fl.Gate form"
            parameters = self.flow_graph.flow_settings.parameters
            if gate.parameter not in {p.name for p in parameters}:
                return None, f"the gate reads parameter {gate.parameter!r}, which the flow does not declare"
            try:
                parameter_gate_is_open(gate, parameters)
            except ValueError as exc:
                return None, f"the gate condition cannot be evaluated: {exc}"
            args.append(f"parameter={self.param_vars.get(gate.parameter) or json.dumps(gate.parameter)}")
            if gate.operator != "equals":
                args.append(f"operator={json.dumps(gate.operator)}")
            args.append(f"value={json.dumps(gate.value, ensure_ascii=False)}")
        args.append(f"else_output={settings.else_output}")
        args += self._description_args(node)
        self._bind_outputs(node, var, [".then", ".otherwise"] if settings.else_output else [".then"])
        return f"{var} = {_call('fl.Gate', args)}", None

    def _keyed_vars(self, node: FlowNode) -> dict[str, str]:
        """Target handle -> the variable feeding it, for keyed-input nodes."""
        conv = self.converter
        keyed = node.node_inputs.keyed_inputs or {}
        sources = node.node_inputs.keyed_source_handles or {}
        out = {}
        for handle, source in keyed.items():
            if source is None:
                continue
            per_handle = conv.node_handle_var_mapping.get((source.node_id, sources.get(handle, "output-0")))
            out[handle] = per_handle or conv.node_var_mapping.get(source.node_id, f"df_{source.node_id}")
        return out

    def _binding_literal(self, binding, specs: dict) -> str:
        """A constant binding: the parameter variable, a typed literal when it stringifies back, else the text."""
        from flowfile_core.flowfile.param_types import coerce_param_value

        value = binding.constant_value
        ref = self._param_ref(value)
        if ref is not None:
            return ref
        spec = specs.get(binding.parameter_name)
        if spec is not None and "${" not in value:
            try:
                typed = coerce_param_value(spec.type, value, spec.enum_values)
                literal = _value_literal(typed)
                if literal is not None and stringify_param_value(typed) == value:
                    return literal
            except Exception:
                pass
        return json.dumps(value, ensure_ascii=False)

    def _native_run_flow(
        self, node: FlowNode, var: str, settings: input_schema.NodeRunFlow
    ) -> tuple[str | None, str | None]:
        """``fl.RunFlow(fl.flow_ref(uuid=...), <slot>=frame, params={...})``; an unresolvable child is a placeholder."""
        from flowfile_core.flowfile.subflow import resolve_subflow_path
        from flowfile_frame.run_flow import _RUN_FLOW_KEYWORDS

        ref = settings.flow_reference
        try:
            resolve_subflow_path(ref, settings.user_id)
        except Exception as exc:
            return None, f"the child flow does not resolve: {exc}"
        if ref.flow_uuid:
            target = f"fl.flow_ref(uuid={json.dumps(ref.flow_uuid)})"
        elif ref.registration_id is not None:
            target = f"fl.flow_ref(registration_id={ref.registration_id})"
        else:
            return None, "the node names no registered flow"
        keyed = self._keyed_vars(node)
        args, slots = [target], []
        for index, slot in enumerate(settings.input_slots):
            source = keyed.get(f"input-{index + 1}")
            if source is None:
                continue
            if slot.isidentifier() and not keyword.iskeyword(slot) and slot not in _RUN_FLOW_KEYWORDS:
                args.append(f"{slot}={source}")
            else:
                slots.append(f"{json.dumps(slot, ensure_ascii=False)}: {source}")
        if slots:
            args.append("inputs={" + ", ".join(slots) + "}")
        specs = {spec.name: spec for spec in settings.parameter_specs}
        params = []
        for binding in settings.parameter_bindings:
            name = json.dumps(binding.parameter_name, ensure_ascii=False)
            if binding.source == "constant":
                params.append(f"{name}: {self._binding_literal(binding, specs)}")
            elif binding.source == "column":
                params.append(f"{name}: fl.col({json.dumps(binding.column_name, ensure_ascii=False)})")
        if params:
            args.append("params={" + ", ".join(params) + "}")
        if keyed.get("input-0") is not None:
            args.append(f"param_frame={keyed['input-0']}")
        if settings.iteration_mode == "iterate":
            args.append("iterate=True")
        if not settings.append_run_metadata:
            args.append("append_metadata=False")
        args += self._description_args(node)
        outputs = settings.output_slots
        if len(outputs) > 1:
            self._bind_outputs(node, var, [f"[{json.dumps(name, ensure_ascii=False)}]" for name in outputs])
        else:
            self._bind_outputs(node, var, [".output"])
        return f"{var} = {_call('fl.RunFlow', args)}", None

    def _native_flow_input(
        self, node: FlowNode, var: str, settings: input_schema.NodeFlowInput
    ) -> tuple[str | None, str | None]:
        """``fl.FlowInput(name, schema= | sample=pl.DataFrame(...), flow_graph=flow)``."""
        raw = settings.raw_data_format
        args = [json.dumps(settings.input_name, ensure_ascii=False)]
        if raw is not None and raw.columns:
            schema = _schema_literal([(column.name, column.data_type) for column in raw.columns])
            if schema is None:
                return None, "a sample column has a dtype with no fl.* form"
            if not raw.data or not any(raw.data[0] if raw.data else []):
                args.append(f"schema={schema}")
            else:
                columns = []
                for column, values in zip(raw.columns, raw.data, strict=False):
                    literal = _value_literal(list(values))
                    if literal is None:
                        return None, f"sample column {column.name!r} holds values with no literal form"
                    columns.append(f"{json.dumps(column.name, ensure_ascii=False)}: {literal}")
                data = "{" + ", ".join(columns) + "}"
                self.converter.imports.add("import polars as pl")
                args.append(f"sample=pl.DataFrame({data}, schema={schema}, strict=False)")
        args.append(f"flow_graph={FLOW_VAR}")
        args += self._description_args(node)
        return f"{var} = {_call('fl.FlowInput', args)}", None

    def _native_flow_output(
        self, node: FlowNode, var: str, settings: input_schema.NodeFlowOutput
    ) -> tuple[str | None, str | None]:
        """``frame.to_flow_output(name)``; it returns its input, so the cell binds nothing."""
        source = self.converter._get_input_vars(node).get("main")
        if source is None:
            return None, "the flow output has no input"
        self.converter.node_var_mapping[node.node_id] = source
        args = [json.dumps(settings.output_name, ensure_ascii=False)] + self._description_args(node)
        return f"{source}.to_flow_output({', '.join(args)})", None

    def _script_schemas(self, node: FlowNode, settings: input_schema.NodePythonScript, outputs: list[str]):
        """``{output: [(column, dtype)]}``: declared ``output_schemas``, else seeded schemas beyond the input's."""
        if settings.output_schemas:
            return {name: [(f.name, f.data_type) for f in fields] for name, fields in settings.output_schemas.items()}
        seeded = getattr(node, "_named_schemas", None) or {}
        inputs = node.all_inputs
        try:
            first = [(c.column_name, c.data_type) for c in (inputs[0].schema if inputs else [])]
        except Exception:
            first = []
        declared = {}
        for index, name in enumerate(outputs):
            columns = [(c.column_name, c.data_type) for c in seeded.get(f"output-{index}") or []]
            if columns and columns != first:
                declared[name] = columns
        return declared or None

    def _native_python_script(
        self, node: FlowNode, var: str, settings: input_schema.NodePythonScript
    ) -> tuple[str | None, str | None]:
        """``@fl.python_script`` when the cells regenerate byte for byte, else ``fl.PythonScript(cells=...)``."""
        script = settings.python_script_input
        if not script.cells:
            return None, "a code-only Python Script has no cells to render; open it in the drawer to give it cells"
        inputs = list(self.converter._get_input_vars(node).values())
        outputs = list(settings.output_names or ["main"])
        schemas = self._script_schemas(node, settings, outputs)
        schema_literals = {}
        for name, columns in (schemas or {}).items():
            literal = _schema_literal(columns)
            if literal is None:
                return None, f"output {name!r} has a column dtype with no fl.* form"
            schema_literals[name] = literal
        code = self._decorated_script(node, var, settings, inputs, outputs, schema_literals)
        if code is not None:
            return code, None
        cells = (
            "cells=[\n"
            + "".join(f"        ({json.dumps(cell.id)}, {_str_literal(cell.code)}),\n" for cell in script.cells)
            + "    ]"
        )
        args = [*inputs, cells]
        if script.kernel_id:
            args.append(f"kernel={json.dumps(script.kernel_id)}")
        if outputs != ["main"]:
            args.append(f"outputs={json.dumps(outputs)}")
        if schema_literals:
            args.append(f"schemas={_nested_literal(schema_literals)}")
        args += self._description_args(node)
        call = "fl.PythonScript(\n" + "".join(f"    {arg},\n" for arg in args) + ")"
        if len(outputs) == 1:
            self._bind_outputs(node, var, [""])
            return f"{var} = {call}.output", None
        self._bind_outputs(node, var, [f"[{json.dumps(name)}]" for name in outputs])
        return f"{var} = {call}", None

    def _decorated_script(
        self,
        node: FlowNode,
        var: str,
        settings: input_schema.NodePythonScript,
        inputs: list[str],
        outputs: list[str],
        schema_literals: dict[str, str],
    ) -> str | None:
        """The ``@fl.python_script`` cell, when regenerating its cells reproduces the stored ones exactly.

        Pure text on the core side: prelude imports become stub modules (each must pass
        ``importlib.util.find_spec``) and constants literals, so nothing the script imports is loaded;
        only the ``def`` is compiled, then the frame's own ``_notebook_cells`` regenerates the cells.
        """
        import importlib.util
        import linecache

        from flowfile_frame.python_script import _notebook_cells

        cells = [cell.code for cell in settings.python_script_input.cells]
        parts = _decorator_parts(cells)
        if parts is None or len(parts[1]) != len(inputs):
            return None
        prelude, parameters, candidates, function = parts
        namespace: dict = {"__builtins__": builtins}
        bound: dict[str, str] = {}
        for line in prelude:
            try:
                statement = ast.parse(line).body
            except SyntaxError:
                return None
            if len(statement) != 1:
                return None
            stmt = statement[0]
            if isinstance(stmt, ast.Import) and len(stmt.names) == 1:
                alias = stmt.names[0]
                if alias.asname is None and "." in alias.name:
                    return None
                if importlib.util.find_spec(alias.name.partition(".")[0]) is None:
                    return None
                name = alias.asname or alias.name
                namespace[name] = types.ModuleType(alias.name)
            elif isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                name = stmt.targets[0].id
                try:
                    namespace[name] = ast.literal_eval(stmt.value)
                except ValueError:
                    return None
            else:
                return None
            known = self.prelude.get(name, _SESSION_PRELUDE.get(name, line))
            if known != line or (name in self.used and name not in self.prelude and name not in _SESSION_PRELUDE):
                return None
            bound[name] = line
        function = function or f"{var}_script"
        if function in self.used or keyword.iskeyword(function):
            return None
        option_sets = [list(outputs)] if outputs != ["main"] else [None, ["main"]]
        for docstring, body in candidates:
            source = _script_function_text(function, parameters, body, docstring)
            for option in option_sets:
                filename = f"<notebook-render-{node.node_id}>"
                linecache.cache[filename] = (len(source), None, source.splitlines(True), filename)
                try:
                    exec(compile(source, filename, "exec", dont_inherit=True), namespace)  # noqa: S102
                    regenerated = _notebook_cells(namespace[function], option)
                except Exception:
                    continue
                finally:
                    linecache.cache.pop(filename, None)
                if regenerated == cells:
                    self.prelude.update(bound)
                    self.used.add(function)
                    return self._decorated_cell(
                        node, var, settings, (prelude, source, function), option, inputs, schema_literals
                    )
        return None

    def _decorated_cell(self, node, var, settings, parts, option, inputs, schema_literals) -> str:
        """The cell text of a decorated script: prelude, ``@fl.python_script(...)``, the ``def`` and the call."""
        prelude, source, function = parts
        kwargs = []
        if settings.python_script_input.kernel_id:
            kwargs.append(f"kernel={json.dumps(settings.python_script_input.kernel_id)}")
        if option is not None:
            kwargs.append(f"outputs={json.dumps(option)}")
        if schema_literals:
            if len(schema_literals) == 1 and len(settings.output_names or ["main"]) == 1:
                kwargs.append(f"returns={next(iter(schema_literals.values()))}")
            else:
                kwargs.append(f"returns={_nested_literal(schema_literals)}")
        description = getattr(node.setting_input, "description", None) or ""
        kwargs.append(f"description={json.dumps(description, ensure_ascii=False)}")
        decorator = _call("@fl.python_script", kwargs)
        outputs = list(settings.output_names or ["main"])
        if len(outputs) == 1:
            self._bind_outputs(node, var, [""])
            call = f"{var} = {function}({', '.join(inputs)})"
        else:
            self._bind_outputs(node, var, [f"[{json.dumps(name)}]" for name in outputs])
            call = f"{var} = {function}.node({', '.join(inputs)})"
        head = "\n".join(prelude) + "\n\n\n" if prelude else ""
        return f"{head}{decorator}\n{source}\n\n\n{call}"

    def _native_custom_node(
        self, node: FlowNode, var: str, settings: input_schema.UserDefinedNode
    ) -> tuple[str | None, str | None]:
        """``fl.custom_nodes.<key>(frame, <component>=value, ..., kernel=...)``; settings drift is a placeholder."""
        import inspect

        from flowfile_frame.custom_node import CustomNodeFactory
        from flowfile_frame.custom_nodes import CustomNodes

        key = node.node_type
        try:
            factory = CustomNodeFactory(key)
        except Exception as exc:
            return None, f"custom node {key!r} is not available: {exc}"
        by_location = {location: name for name, location in factory.parameters.items()}
        defaults = {name: p.default for name, p in inspect.signature(factory).parameters.items()}
        kwargs = []
        for section, values in (settings.settings or {}).items():
            for component, value in (values or {}).items():
                name = by_location.get((section, component))
                if name is None:
                    return None, f"settings drift: {section}.{component} is not a setting of {key!r}"
                if value == defaults.get(name):
                    continue
                literal = self._param_ref(value) or _value_literal(value)
                if literal is None:
                    return None, f"setting {name!r} has no literal form"
                kwargs.append(f"{name}={literal}")
        uses_kernel = bool(getattr(factory.node_class(), "uses_kernel", False))
        if settings.kernel_id:
            if not uses_kernel:
                return None, "a local custom node carries a kernel id"
            kwargs.append(f"kernel={json.dumps(settings.kernel_id)}")
        elif uses_kernel:
            return None, "a kernel custom node with no kernel selected"
        kwargs += self._description_args(node)
        attribute = key.isidentifier() and not keyword.iskeyword(key) and not key.startswith("_")
        if attribute and hasattr(CustomNodes, key):
            attribute = False
        func = f"fl.custom_nodes.{key}" if attribute else f"fl.custom_nodes[{json.dumps(key)}]"
        inputs = list(self.converter._get_input_vars(node).values())
        if len(factory.output_names) == 1:
            self._bind_outputs(node, var, [""])
            return f"{var} = {_call(func, inputs + kwargs)}", None
        self._bind_outputs(node, var, [f"[{json.dumps(name)}]" for name in factory.output_names])
        return f"{var} = {_call(func + '.node', inputs + kwargs)}", None

    @staticmethod
    def _cell(node: FlowNode, code: str, status: CellStatus, reason: str | None) -> EmittedCell:
        return EmittedCell(
            cell_id=cell_id_for(node.node_id),
            node_ids=[node.node_id],
            kind="node",
            code=code,
            status=status,
            reason=reason,
        )

    @staticmethod
    def _fill_uses(cells: list[EmittedCell]) -> None:
        trees: list[ast.Module | None] = []
        for cell in cells:
            try:
                trees.append(ast.parse(cell.code))
            except SyntaxError:
                trees.append(None)
        for cell, tree in zip(cells, trees, strict=True):
            cell.defines = _assigned_names(tree) if tree is not None else []
        defined = {name for cell in cells for name in cell.defines} | {FLOW_VAR}
        for cell, tree in zip(cells, trees, strict=True):
            if tree is not None:
                cell.uses = sorted((_loaded_names(tree) - set(cell.defines)) & defined)


def render(flow_graph: FlowGraph) -> NotebookRendering:
    """Render ``flow_graph`` as notebook cells (see :class:`NotebookRenderer`)."""
    return NotebookRenderer(flow_graph).render()
