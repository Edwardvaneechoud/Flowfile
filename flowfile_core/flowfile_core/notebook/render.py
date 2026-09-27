"""Render a canvas flow as notebook cells: the FlowFrame export, split at its own statement boundaries.

The FlowFrame exporter (``placeholders=True``) is the one code path. Its imports and module helpers
make the first cell, the flow parameters the second, then every fused statement it emits becomes a
cell carrying the node ids of its span; a node the exporter cannot express is a ``fl.canvas_node``
placeholder cell. Cells therefore need not align one per node.
"""

from __future__ import annotations

import ast
import hashlib
import io
import json
import re
import textwrap
import tokenize
from typing import Literal

from pydantic import BaseModel, Field

from flowfile_core.configs import logger
from flowfile_core.flowfile.code_generator.code_generator import (
    NODE_TYPE_VAR_LABEL,
    FlowGraphToFlowFrameConverter,
    _polars_code_function_body,
)
from flowfile_core.flowfile.code_generator.native_handlers import FLOW_VAR, user_description
from flowfile_core.flowfile.code_generator.param_codegen import (
    SENTINEL_PREFIX,
    codegen_parameters,
    resolve_param_sentinels,
    restore_sentinels_to_refs,
)
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.param_types import FlowParameter, stringify_param_value
from flowfile_core.notebook.compare import strip_outer_parens
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.schemas.transform_schema import AUTO_DATA_TYPE

CellKind = Literal["imports", "parameters", "node"]
CellStatus = Literal["code", "placeholder", "unsupported"]

IMPORTS_CELL_ID = "imports"
PARAMETERS_CELL_ID = "parameters"
_LAYOUT_FIELDS = frozenset({"pos_x", "pos_y", "is_setup", "flow_id", "user_id"})
_NATIVE_TYPES = frozenset({"gate", "run_flow", "python_script", "flow_input", "flow_output"})
_WHOLE_REF = re.compile(rf"""(['"]){SENTINEL_PREFIX}\w+__\1""")
_NOTEBOOK_HELPERS = {
    "_flowfile_flow_parameter": (
        "def _flowfile_flow_parameter(frame, name, value, **declaration):\n"
        '    """The parameters cell declares every parameter, so a gate only names it."""\n'
        "    return name"
    ),
    "_flowfile_expr_literal": (
        "def _flowfile_expr_literal(value):\n"
        '    """A parameter inside a formula stays its ``${name}`` reference."""\n'
        "    return value.ref if isinstance(value, fl.Parameter) else value"
    ),
}


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


def node_label(node_type: str, node_id: int) -> str:
    """The deterministic variable name of an unnamed node, matching the frame's ``node_label``."""
    label = NODE_TYPE_VAR_LABEL.get(node_type) or re.sub(r"\W", "_", node_type)
    return f"{label}_{node_id}"


class _NotebookConverter(FlowGraphToFlowFrameConverter):
    """The FlowFrame exporter plus the frame methods that rebuild a node as the same node type.

    A notebook push rebuilds the canvas from the cells, so where the export lowers a node into other
    frame calls (``sink_*``, a direct call of a Polars-code function, row-index arithmetic, an explode,
    a rename-based join, a cast, the defaults of ``scan_csv``) the notebook writes the frame method
    that adds the node itself. These are round-trip lowerings the FlowFrame export could adopt too.
    """

    def _plan_boundary_names(self, emissions, survivors: set[int], node_by_id: dict) -> dict[str, str]:
        """Unnamed boundaries are ``<type_label>_<id>``: what a seeded session binds, never captured as a reference."""
        rename: dict[str, str] = {}
        for em in emissions:
            node = node_by_id[em.node_id]
            if em.node_id not in survivors or em.pinned:
                continue
            prefix, label = f"df_{em.node_id}", node_label(node.node_type, em.node_id)
            suffixes = [split.name for split in getattr(self._settings_for(node), "splits", None) or []]
            if self._is_filter_split(node):
                rename[f"_filter_{em.node_id}_pred"], suffixes = f"{label}_pred", ["pass", "fail"]
            for name in [em.var_name, *(f"{prefix}_{suffix}" for suffix in suffixes)]:
                if name == prefix or name.startswith(f"{prefix}_"):
                    rename[name] = label + name[len(prefix) :]
        return rename

    def _is_flow_output(self, node: FlowNode) -> bool:
        """A node with a user description ends its statement, so ``description=`` lands on its own call."""
        described = node.node_type not in _NATIVE_TYPES and bool(user_description(node.setting_input))
        return super()._is_flow_output(node) or described

    def _handle_output(self, settings: input_schema.NodeOutput, var_name: str, input_vars: dict[str, str]) -> None:
        output, source = settings.output_settings, input_vars.get("main", "df")
        path, table = self._py_path(output.abs_file_path), output.table_settings
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
            return super()._handle_output(settings, var_name, input_vars)
        self._add_code("")

    def _handle_polars_code(
        self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """``<input>.polars_code(fn, *others)`` (``fl.polars_code(fn)`` without input) over the stored code verbatim."""
        inputs = [input_vars[key] for key in sorted(input_vars) if key.startswith("main")]
        code = textwrap.dedent(settings.polars_code_input.polars_code).strip()
        names = [f"input_df_{i}" for i in range(1, len(inputs) + 1)] if len(inputs) > 1 else ["input_df"][: len(inputs)]
        function = f"_polars_code_{settings.node_id}"
        if re.search(r"\bpl\.", code):
            self.imports.add("import polars as pl")
        body, returned = _polars_code_function_body(code)
        if returned not in (None, "output_df") and re.search(rf"^{re.escape(returned)}\s*=[^=]", "\n".join(body), re.M):
            returned = None
        self._add_code(f"def {function}({', '.join(f'{name}: fl.FlowFrame' for name in names)}):")
        for line in [*body, *([f"return {returned}"] if returned else [] if body else ["pass"])]:
            self._add_code(f"    {line}")
        self._add_code("")
        self._add_code("")
        call = (
            f"{inputs[0]}.polars_code({', '.join([function, *inputs[1:]])})"
            if inputs
            else f"fl.polars_code({function})"
        )
        self._add_code(f"{var_name} = {call}")
        self._add_code("")

    @staticmethod
    def _join_suffix(settings: transform_schema.JoinInputManager) -> str | None:
        """The export's suffix rule without its key-order condition: ``keep_right_keys=True`` rebuilds any order."""
        if settings.how not in ("left", "inner"):
            return None
        left = settings.left_select.renames
        if any(not column.keep or column.new_name != column.old_name for column in left):
            return None
        left_names, right = {column.old_name for column in left}, settings.right_select.renames
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
        return None if len(suffixes) > 1 else suffixes.pop() if suffixes else "_right"

    def _emit_suffix_join(self, settings, var_name, left_df, right_df, left_on, right_on, suffix, keep_right_keys):
        kwargs = [f"left_on={left_on}", f"right_on={right_on}", f'how="{settings.join_input.how}"']
        kwargs += [f"suffix={self._py_str(suffix)}"] if suffix != "_right" else []
        kwargs += ["keep_right_keys=True"] if keep_right_keys else []
        self._add_code(f"{var_name} = {left_df}.join(")
        self._add_code(f"        {right_df},")
        for index, kwarg in enumerate(kwargs):
            self._add_code(f"        {kwarg}{',' if index < len(kwargs) - 1 else ''}")
        self._add_code("    )")

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        """An advanced filter that is one basic comparison renders as the basic one: the frame stores both as text."""
        from flowfile_core.flowfile.share.filter_translation import translate_advanced_filter

        advanced = settings.filter_input.advanced_filter
        if not settings.split_mode and settings.filter_input.is_advanced() and SENTINEL_PREFIX not in advanced:
            basic = translate_advanced_filter(strip_outer_parens(advanced))
            if basic is not None:
                filter_input = transform_schema.FilterInput.model_validate(basic)
                settings = settings.model_copy(update={"filter_input": filter_input})
        super()._handle_filter(settings, var_name, input_vars)

    def _translate_to_ff_code(self, formula: str) -> str | None:
        """A formula holding a parameter keeps its text; outer parentheses (the frame stores ``([a] > 1)``) drop."""
        return None if SENTINEL_PREFIX in formula else super()._translate_to_ff_code(strip_outer_parens(formula))

    def _formula_entry_native_expr(self, entry) -> str | None:
        """A typed entry keeps its keyword form: ``.cast()`` would rebuild as ``to_float(...)`` with an Auto type."""
        return (
            None if entry.field.data_type not in (None, AUTO_DATA_TYPE) else super()._formula_entry_native_expr(entry)
        )

    def _csv_scan_kwarg_lines(self, file_settings: input_schema.ReceivedTable) -> list[str]:
        """The stored UTF-8 variant (strict or lossy) and each option that differs from ``scan_csv``'s default."""
        table = file_settings.table_settings
        encoding = "utf8-lossy" if "LOSSY" in (table.encoding or "").upper() else "utf8"
        lines = [
            f'    encoding="{encoding}",' if line.strip().startswith("encoding=") else line
            for line in super()._csv_scan_kwarg_lines(file_settings)
        ]
        lines += [f"    infer_schema_length={table.infer_schema_length},"] if table.infer_schema_length != 100 else []
        lines += ["    infer_schema=False,"] if not table.infer_schema else []
        if table.quote_char != '"':
            lines.append(f"    quote_char={self._py_str(table.quote_char) if table.quote_char else None},")
        return lines + (["    truncate_ragged_lines=True,"] if table.truncate_ragged_lines else [])

    def _handle_catalog_reader(self, settings, var_name: str, input_vars: dict[str, str]) -> None:
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

    def _handle_text_to_rows(self, settings, var_name: str, input_vars: dict[str, str]) -> None:
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

    def _handle_record_id(self, settings, var_name: str, input_vars: dict[str, str]) -> None:
        record = settings.record_id_input
        args = [self._py_str(record.output_column_name), f"offset={record.offset}"]
        if record.group_by and record.group_by_columns:
            args.append(f"group_by={[str(column) for column in record.group_by_columns]}")
        self._add_code(f"{var_name} = {input_vars.get('main', 'df')}.with_row_index({', '.join(args)})")
        self._add_code("")


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


def _parameter_line(param: FlowParameter, bind: bool) -> str:
    """``name = fl.add_flow_parameter(flow, fl.Parameter(...))``; the default is typed when it stringifies back."""
    args = [json.dumps(param.name, ensure_ascii=False)]
    if param.default_value != "":
        default = param.default_value
        try:
            typed = param.typed_default()
            default = typed if stringify_param_value(typed) == param.default_value else default
        except Exception:
            pass
        literal = json.dumps(default, ensure_ascii=False) if isinstance(default, str) else repr(default)
        args.append(f"default={literal}")
    if param.type != "string":
        args.append(f"type={json.dumps(param.type)}")
    if param.description:
        args.append(f"description={json.dumps(param.description, ensure_ascii=False)}")
    if param.enum_values:
        args.append(f"enum_values={json.dumps(list(param.enum_values), ensure_ascii=False)}")
    line = f"fl.add_flow_parameter({FLOW_VAR}, fl.Parameter({', '.join(args)}))"
    return f"{param.name} = {line}" if bind else line


def _resolve_refs(code: str, names: set[str]) -> str:
    """Parameter sentinels the notebook way: text keeps ``${name}``, a whole-string or bare ref is the variable.

    The variable is the ``fl.Parameter`` the parameters cell binds, which the frame stores back as the
    ``${name}`` reference; inside text the reference stays verbatim, as the canvas stores it.
    """
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(code).readline))
    except (tokenize.TokenError, SyntaxError):
        tokens = []
    starts = [0]
    for line in code.split("\n"):
        starts.append(starts[-1] + len(line) + 1)
    for tok in reversed(tokens):
        if tok.type == tokenize.STRING and SENTINEL_PREFIX in tok.string and not _WHOLE_REF.fullmatch(tok.string):
            begin, end = starts[tok.start[0] - 1] + tok.start[1], starts[tok.end[0] - 1] + tok.end[1]
            code = code[:begin] + restore_sentinels_to_refs(tok.string) + code[end:]
    return resolve_param_sentinels(code, names)[0]


def _with_description(code: str, description: str) -> str | None:
    """``description=`` added to the call the cell's last statement assigns (after defs and imports), else None."""
    try:
        *head, last = ast.parse(code).body or [None]
    except SyntaxError:
        return None
    if not isinstance(last, ast.Assign) or not isinstance(last.value, ast.Call):
        return None
    if not all(isinstance(stmt, ast.FunctionDef | ast.Import | ast.ImportFrom) for stmt in head):
        return None
    call = last.value
    while isinstance(call.func, ast.Attribute) and call.func.attr == "agg" and isinstance(call.func.value, ast.Call):
        call = call.func.value
    if any(keyword.arg == "description" for keyword in call.keywords):
        return code
    lines = code.split("\n")
    line = lines[call.end_lineno - 1]
    col = len(line.encode("utf-8")[: call.end_col_offset - 1].decode("utf-8", errors="ignore"))
    before = code[: sum(len(prior) + 1 for prior in lines[: call.end_lineno - 1]) + col]
    rest = code[len(before) :]
    before = before.rstrip()
    separator = "" if before.endswith("(") else " " if before.endswith(",") else ", "
    return f"{before}{separator}description={json.dumps(description, ensure_ascii=False)}{rest}"


def _fill_names(cells: list[EmittedCell]) -> None:
    """``defines`` (top-level bindings) and ``uses`` (loaded names another cell defines) from each cell's AST."""
    loads: list[set[str]] = []
    for cell in cells:
        try:
            tree = ast.parse(cell.code)
        except SyntaxError:
            loads.append(set())
            continue
        defined: list[str] = []
        for stmt in tree.body:
            if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                found = [stmt.name]
            elif isinstance(stmt, ast.Import | ast.ImportFrom):
                found = [alias.asname or alias.name.split(".")[0] for alias in stmt.names]
            else:
                targets = getattr(stmt, "targets", None) or [getattr(stmt, "target", None)]
                found = [n.id for t in targets if t is not None for n in ast.walk(t) if isinstance(n, ast.Name)]
            defined += [name for name in found if name not in defined]
        cell.defines = defined
        loads.append({n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)})
    known = {name for cell in cells for name in cell.defines} | {FLOW_VAR}
    for cell, loaded in zip(cells, loads, strict=True):
        cell.uses = sorted((loaded - set(cell.defines)) & known)


def render(flow_graph: FlowGraph) -> NotebookRendering:
    """Render ``flow_graph`` as notebook cells; never raises (an export failure is a warning and no node cells)."""
    converter = _NotebookConverter(flow_graph, placeholders=True)
    warnings: list[str] = []
    try:
        converter.convert()
        emissions = converter._fused
    except Exception as exc:
        logger.warning("Notebook render of flow %s failed: %s", flow_graph.flow_id, exc)
        emissions, warnings = [], [f"The flow could not be rendered as code: {exc}"]
    imports = ["import flowfile as fl", *(line for line in converter.import_lines() if line != "import flowfile as fl")]
    helpers = [_NOTEBOOK_HELPERS.get(h.split("(")[0].removeprefix("def "), h) for h in converter.helpers()]
    cells = [EmittedCell(cell_id=IMPORTS_CELL_ID, kind="imports", code="\n\n\n".join(["\n".join(imports), *helpers]))]
    parameters = list(flow_graph.flow_settings.parameters)
    bound = {p.name for p in codegen_parameters(parameters)}
    if parameters:
        code = "\n".join(_parameter_line(p, p.name in bound) for p in parameters)
        cells.append(EmittedCell(cell_id=PARAMETERS_CELL_ID, kind="parameters", code=code))
    for em in emissions:
        ids, code = em.node_ids or [em.node_id], _resolve_refs(em.code, bound)
        node = flow_graph.get_node(em.node_id)
        native = node.node_type in _NATIVE_TYPES or getattr(node.setting_input, "is_user_defined", False)
        description = None if native or em.placeholder_reason else user_description(node.setting_input)
        if description:
            described = _with_description(code, description)
            if described is None:
                warnings.append(f"Node {em.node_id}: its description has no place in a multi-statement cell")
            code = described or code
        cells.append(
            EmittedCell(
                cell_id=f"cell-{ids[0]}",
                node_ids=ids,
                kind="node",
                code=code,
                status="placeholder" if em.placeholder_reason else "code",
                reason=em.placeholder_reason,
            )
        )
    _fill_names(cells)
    return NotebookRendering(
        cells=cells,
        warnings=warnings + converter.warnings,
        var_by_node={em.node_id: em.var_name for em in emissions},
        code_fingerprint=code_fingerprint(flow_graph),
    )
