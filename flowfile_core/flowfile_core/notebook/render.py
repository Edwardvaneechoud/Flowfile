"""Render a canvas flow as notebook cells: the FlowFrame export, split at its own statement boundaries.

The FlowFrame exporter (``placeholders=True``) is the one code path. Its imports and module helpers
make the first cell, the flow parameters the second, then every fused statement it emits becomes a
cell carrying the node ids of its span; a node the exporter cannot express is a ``ff.canvas_node``
placeholder cell. Cells therefore need not align one per node.
"""

from __future__ import annotations

import ast
import hashlib
import json
from typing import Literal

from pydantic import BaseModel, Field

from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter
from flowfile_core.flowfile.code_generator.native_handlers import FLOW_VAR
from flowfile_core.flowfile.code_generator.param_codegen import codegen_parameters
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.param_types import FlowParameter, stringify_param_value

CellKind = Literal["imports", "parameters", "node"]
CellStatus = Literal["code", "placeholder", "unsupported"]

IMPORTS_CELL_ID = "imports"
PARAMETERS_CELL_ID = "parameters"
_LAYOUT_FIELDS = frozenset({"pos_x", "pos_y", "is_setup", "flow_id", "user_id"})
_NOTEBOOK_HELPERS = {
    "_flowfile_flow_parameter": (
        "def _flowfile_flow_parameter(frame, name, value, **declaration):\n"
        '    """The parameters cell declares every parameter, so a gate only names it."""\n'
        "    return name"
    ),
    "_flowfile_expr_literal": (
        "def _flowfile_expr_literal(value):\n"
        '    """A parameter inside a formula stays its ``${name}`` reference."""\n'
        "    return value.ref if isinstance(value, ff.Parameter) else value"
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
    """``name = ff.add_flow_parameter(flow, ff.Parameter(...))``; the default is typed when it stringifies back."""
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
    line = f"ff.add_flow_parameter({FLOW_VAR}, ff.Parameter({', '.join(args)}))"
    return f"{param.name} = {line}" if bind else line


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
    """Render ``flow_graph`` as notebook cells; an export failure propagates.

    A rendering without its node cells would read as a flow with no nodes, and pushing it back would
    plan deleting every canvas node, so a failed export is never turned into a partial rendering.

    The fingerprint is taken before the export, so an edit landing mid-render leaves an older fingerprint
    and the next refresh renders again instead of keeping cells for the pre-edit graph.
    """
    fingerprint = code_fingerprint(flow_graph)
    converter = FlowGraphToFlowFrameConverter(
        flow_graph, placeholders=True, deterministic_names=True, decorated_scripts=True
    )
    converter.convert()
    emissions = converter.emissions(verbatim_refs=True)
    # The notebook never evaluates a def's annotations, and the interpreter refuses a __future__ import
    dropped = {"import flowfile as ff", "from __future__ import annotations"}
    imports = ["import flowfile as ff", *(line for line in converter.import_lines() if line not in dropped)]
    helpers = [_NOTEBOOK_HELPERS.get(h.split("(")[0].removeprefix("def "), h) for h in converter.helpers()]
    cells = [EmittedCell(cell_id=IMPORTS_CELL_ID, kind="imports", code="\n\n\n".join(["\n".join(imports), *helpers]))]
    parameters = list(flow_graph.flow_settings.parameters)
    bound = {p.name for p in codegen_parameters(parameters)}
    if parameters:
        code = "\n".join(_parameter_line(p, p.name in bound) for p in parameters)
        cells.append(EmittedCell(cell_id=PARAMETERS_CELL_ID, kind="parameters", code=code))
    for em in emissions:
        ids = em.node_ids or [em.node_id]
        status = "placeholder" if em.placeholder_reason else "code"
        cells.append(
            EmittedCell(
                cell_id=f"cell-{ids[0]}",
                node_ids=ids,
                kind="node",
                code=em.code,
                status=status,
                reason=em.placeholder_reason,
            )
        )
    _fill_names(cells)
    return NotebookRendering(
        cells=cells,
        warnings=converter.warnings,
        var_by_node={em.node_id: em.var_name for em in emissions},
        code_fingerprint=fingerprint,
    )
