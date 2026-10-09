"""Simple build in ``code`` mode: the model writes FlowFrame code and core turns it into nodes without running it.

The model answers a plain-English request with one ```python block in the dialect the canvas notebook renders
(``import flowfile as ff``, one assignment per step; ``prompts/code_build.md``). LLMs have seen far more Polars-shaped
code than Flowfile's settings JSON, so the script comes back faster and better structured than the ``{nodes, edges}``
object the JSON modes ask for, and it is the same language the user sees in the Notebook pane.

Nothing the model writes ever executes in core. The script passes three gates, in order:

1. :func:`prescan`, ``ast`` only: it refuses a ``def``, ``lambda``, a loop, a comprehension, an import other than
   ``flowfile`` / ``polars`` / ``datetime``, a bare function call, a dunder or private name, and every call that would
   reach a stored resource (``ff.read_database``, catalog and cloud readers), write data (``write_*``, ``sink_*``),
   place code that runs (``polars_code``, ``sql``, ``PythonScript``) or run the flow (``collect``). This is what keeps
   ``POST /ai/generate`` JWT-only in every mode: the notebook's ``require_notebook_sync`` admin gate exists because
   the frame's catalog lookups do not check the caller's grants, and nothing the pre-scan lets through can make one.
   The canvas's own rendered cells are not scanned, so where sharing is enabled a canvas holding a node that reaches
   a stored resource (:data:`STORED_RESOURCE_NODE_TYPES`, a custom node) gets no context at all
   (:func:`canvas_context`) and the build starts from scratch.
2. The notebook's clean run (:func:`~flowfile_core.notebook.bridge.get_clean_runner`): the installed
   :class:`~flowfile_core.notebook.runner.NotebookRunner` interprets the cell through
   :mod:`flowfile_core.notebook.allowlist` and builds the nodes in frame build mode, executing none of them.
3. :func:`spec_from_flowfile_data`: the clean run's save-format payload becomes the ``{nodes, edges}`` spec
   :func:`~flowfile_core.ai.local_model.oneshot._build_simple_diff` already consumes, after refusing any node that
   would carry executable text or reach a stored resource (:data:`REFUSED_NODE_TYPES`). Writers are dropped there
   as in every other mode.

A failure at any gate names a line when it has one. The model gets exactly one repair round
(:data:`REPAIR_ROUNDS`), so a small model cannot loop; a second failure is a :class:`CodeBuildError` with the line and
the code, which the route answers as a 422 the chat renders with the offending line.
"""

from __future__ import annotations

import ast
import asyncio
import functools
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowfile_core.ai import safety
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.auth import sharing
from flowfile_core.notebook import allowlist, reconcile
from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult, get_clean_runner

logger = logging.getLogger(__name__)

SURFACE = "code_build"
REPAIR_ROUNDS = 1
BUILD_CELL = "build"
CONTEXT_CHAR_BUDGET = 6000
_COLUMN_HINT_CAP = 12
_PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "code_build.md"
_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
_FENCE_RE = re.compile(r"```[ \t]*([A-Za-z0-9_+-]*)[^\n]*\n(.*?)```", re.DOTALL)
_OPEN_FENCE_RE = re.compile(r"```[ \t]*([A-Za-z0-9_+-]*)[^\n]*\n([^`]*)\Z", re.DOTALL)
_IMPORT_RE = re.compile(r"^\s*import flowfile\b", re.MULTILINE)
_FLOWFILE_IMPORT = "import flowfile as ff"
_NEEDS_KERNEL = "; this needs a kernel"

_STORED_SOURCE = (
    "read_database", "read_kafka", "read_api", "read_from_cloud_storage", "read_catalog_table", "read_catalog_sql",
    "list_files",
)  # fmt: skip
_WRITERS = ("write_catalog_table", "write_database", "write_to_cloud_storage")
_RUNS_CODE = ("polars_code", "sql", "PythonScript", "python_script")
_NOT_IN_SIMPLE_BUILD = (
    "RunFlow", "flow_ref", "FlowInput", "canvas_node", "custom_nodes", "Gate", "Parameter", "add_flow_parameter",
    "concat", "FuzzyMapping",
)  # fmt: skip

_STORED_REASON = "reads a stored connection or catalog table; Simple build reads files and inline data only"
_WRITES_REASON = "writes data; Simple build never writes, attach the destination after inserting"
_RUNS_CODE_REASON = "places code that would run; write the step with the listed methods instead"
_UNAVAILABLE_REASON = "is not available in Simple build"
_POLARS_CODE_REASON = "becomes a Polars Code node, which Simple build does not place; use the listed methods"

REFUSED_FF_NAMES: dict[str, str] = {
    **{name: _STORED_REASON for name in _STORED_SOURCE},
    **{name: _WRITES_REASON for name in _WRITERS},
    **{name: _RUNS_CODE_REASON for name in _RUNS_CODE},
    **{name: _UNAVAILABLE_REASON for name in _NOT_IN_SIMPLE_BUILD},
}
"""``ff.<name>`` the pre-scan refuses, with the reason the model is told."""

_FRAME_WRITERS = (
    "write_csv", "write_excel", "write_parquet", "to_flow_output",
    *(name for name in allowlist.INPUT_ONLY["FlowFrame"] if name.startswith(("write_", "sink_"))),
)  # fmt: skip
_FRAME_CODE_NODES = ("polars_code", "sql")
_FRAME_OTHER = ("apply_model", "train_model", "evaluate_model", "fuzzy_join", "wait_for", "solve_graph")
_FRAME_PURE = tuple(
    name
    for name in allowlist.INPUT_ONLY["FlowFrame"]
    if not name.startswith(("write_", "sink_"))
    and name not in _FRAME_CODE_NODES
    and name not in allowlist.ALLOWLIST["Expr"]
)
"""Frame methods that fall back to a Polars Code node and share no name with an expression method (``count``,
``sum``, ``cast`` stay: the pre-scan sees names, not receivers; the node-type net below catches those)."""

REFUSED_METHODS: dict[str, str] = {
    "collect": "runs the flow; Simple build only describes it",
    **{name: _WRITES_REASON for name in _FRAME_WRITERS},
    **{name: _RUNS_CODE_REASON for name in _FRAME_CODE_NODES},
    **{name: _UNAVAILABLE_REASON for name in _FRAME_OTHER},
    **{name: _POLARS_CODE_REASON for name in _FRAME_PURE},
}
"""Method names the pre-scan refuses on any receiver, with the reason the model is told."""

REFUSED_NODE_TYPES: frozenset[str] = safety.AGENT_BLOCKED_NODE_TYPES | frozenset(
    {
        "polars_code", "sql_query", "polars_lazy_frame", "run_flow", "external_source", "database_reader",
        "cloud_storage_reader", "catalog_reader", "kafka_reader", "rest_api_reader", "google_analytics",
    }
)  # fmt: skip
"""Node types a clean run may still produce that Simple build never places (the pre-scan catches the direct routes;
this is the second net, for a frame method that falls back to a Polars Code node)."""

STORED_RESOURCE_NODE_TYPES: frozenset[str] = frozenset(
    {
        "database_reader", "database_writer", "cloud_storage_reader", "cloud_storage_writer", "catalog_reader",
        "catalog_writer", "kafka_reader", "rest_api_reader", "google_analytics", "external_source", "run_flow",
    }
)  # fmt: skip
"""Live node types whose rendered cell makes a grant-less lookup when the clean run rebuilds it; with sharing
enabled such a canvas gets no context (the push of the same cells is admin-only)."""

_ALLOWED_IMPORTS = frozenset((module, alias) for (module, alias), kind in allowlist.IMPORTS.items() if kind != "inert")
_IDENTITY_KEYS = frozenset(
    {"flow_id", "node_id", "pos_x", "pos_y", "is_setup", "user_id", "depending_on_id", "depending_on_ids"}
)
_REFUSED_EXPRESSIONS = (
    ast.Lambda, ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp, ast.Await, ast.Yield, ast.YieldFrom,
    ast.NamedExpr,
)  # fmt: skip

_ADVERTISED_FF = ("read_csv", "read_excel", "scan_parquet", "from_raw_data", "col", "lit", "len", "when")
_ADVERTISED_DTYPES = ("Int64", "Float64", "String", "Boolean", "Date", "Datetime")
_FRAME_SIGNATURES: dict[str, str] = {
    "filter": 'filter(ff.col("a") > 1)',
    "select": 'select(["a", "b"])',
    "rename": 'rename({"old": "new"})',
    "drop": 'drop(["a"])',
    "with_columns": 'with_columns(<expr>.alias("name"), ...)',
    "group_by": 'group_by(["a"]).agg(<expr>.sum().alias("name"), ...)',
    "sort": 'sort(["a"], descending=[True])',
    "unique": 'unique(["a"])',
    "head": "head(n)",
    "sample": "sample(n)",
    "join": 'join(other, left_on="a", right_on="b", how="left")',
    "pivot": 'pivot(on="col", index=["a"], values="v", aggregate_function="sum")',
    "unpivot": 'unpivot(on=["jan", "feb"], index=["region"])',
    "text_to_rows": 'text_to_rows("col", delimiter=",")',
    "with_row_index": 'with_row_index("id")',
    "data_cleansing": 'data_cleansing(["a"], trim_whitespace=True)',
}
_ADVERTISED_EXPR = (
    "alias", "cast", "is_in", "is_null", "is_not_null", "fill_null", "round", "sum", "mean", "min", "max", "count",
    "n_unique", "first", "last", "median", "std", "abs", "floor", "ceil",
)  # fmt: skip
_ADVERTISED_STR = ("to_uppercase", "to_lowercase", "to_titlecase", "len_chars", "starts_with", "ends_with")
_ADVERTISED_DT = ("year", "month", "day")


class CodeBuildRefusal(Exception):
    """One gate refused the script: ``message`` for the model, ``line`` (1-based in the script) when known."""

    def __init__(self, message: str, line: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.line = line


class CanvasCellFailure(CodeBuildRefusal):
    """A canvas cell, not the script, failed in the clean run: the context is dropped and the build restarts."""


class CodeBuildError(RuntimeError):
    """The script still failed after the repair round; carries the line and the code the chat shows."""

    def __init__(self, message: str, *, line: int | None, code: str | None) -> None:
        super().__init__(message)
        self.message = message
        self.line = line
        self.code = code


def advertised_names() -> dict[str, tuple[str, ...]]:
    """Every name the dialect block offers, by receiver kind, so a test can pin them to the allowlist."""
    return {
        "ff": (*_ADVERTISED_FF, *_ADVERTISED_DTYPES),
        "FlowFrame": tuple(_FRAME_SIGNATURES),
        "Expr": _ADVERTISED_EXPR,
        "StringNS": _ADVERTISED_STR,
        "DateTimeNS": _ADVERTISED_DT,
    }


def render_dialect_block() -> str:
    """The "Available calls" section, built from the allowlist so the prompt never offers a refused call."""
    verdicts = allowlist.FL_VERDICTS
    frame = allowlist.ALLOWLIST["FlowFrame"]
    ff_calls = [n for n in _ADVERTISED_FF if verdicts.get(n, ("",))[0] == allowlist.ALLOW and n not in REFUSED_FF_NAMES]
    dtypes = [n for n in _ADVERTISED_DTYPES if verdicts.get(n, ("",))[0] == allowlist.ALLOW]
    methods = [sig for name, sig in _FRAME_SIGNATURES.items() if name in frame and name not in REFUSED_METHODS]
    exprs = [n for n in _ADVERTISED_EXPR if n in allowlist.ALLOWLIST["Expr"]]
    strs = [n for n in _ADVERTISED_STR if n in allowlist.ALLOWLIST["StringNS"]]
    dts = [n for n in _ADVERTISED_DT if n in allowlist.ALLOWLIST["DateTimeNS"]]
    lines = [
        "Readers and helpers: " + ", ".join(f"ff.{n}" for n in ff_calls),
        "Types for cast: " + ", ".join(f"ff.{n}" for n in dtypes),
        "Frame methods (each is one node):",
        *(f"- .{sig}" for sig in methods),
        "Expression methods on ff.col(...): " + ", ".join(f".{n}()" for n in exprs),
        "Text helpers: " + ", ".join(f".str.{n}()" for n in strs),
        "Date helpers: " + ", ".join(f".dt.{n}()" for n in dts),
        "Nothing else exists. ff.concat, collect(), write_*, sink_*, polars_code, sql and database or cloud readers "
        "are refused.",
    ]
    return "\n".join(lines)


@functools.cache
def system_prompt() -> str:
    """``prompts/code_build.md`` plus the generated dialect block."""
    try:
        text = _PROMPT_PATH.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - the prompt ships with the package
        logger.warning("code_build prompt missing at %s: %s", _PROMPT_PATH, exc)
        text = "You write a Flowfile FlowFrame script (import flowfile as ff) in one ```python block and nothing else."
    return f"{text.rstrip()}\n\n## Available calls\n\n{render_dialect_block()}\n"


def extract_code(text: str) -> str | None:
    """The script in the model's reply: the first ```python block, an untagged block that imports flowfile, or the
    whole reply when it starts with the import; ``None`` for anything else (an answer object, prose)."""
    text = _THINK_RE.sub("", text or "").strip()
    if not text:
        return None
    for lang, body in _FENCE_RE.findall(text):
        body = body.strip()
        if lang.lower() in ("python", "py") or (not lang and _IMPORT_RE.search(body)):
            return body or None
    if _IMPORT_RE.search(text) and "```" not in text:
        return text
    # A fence the reply never closed (the output was cut off): the script so far, so the gates report the
    # broken line and the repair round rewrites it, instead of the half script passing for an answer.
    unclosed = _OPEN_FENCE_RE.search(text)
    if unclosed and text.count("```") % 2 == 1:
        lang, body = unclosed.group(1), unclosed.group(2).strip()
        if lang.lower() in ("python", "py") or (not lang and _IMPORT_RE.search(body)):
            return body or None
    return None


def ensure_import(code: str) -> str:
    """The script with ``import flowfile as ff`` on its first line when the model left it out."""
    return code if _IMPORT_RE.search(code) else f"{_FLOWFILE_IMPORT}\n{code}"


def prescan(code: str) -> None:
    """Refuse, with :class:`CodeBuildRefusal`, anything outside plain assignments in the dialect (see the module
    docstring); reads the ``ast`` only and runs nothing."""
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise CodeBuildRefusal(f"invalid Python: {exc.msg}", exc.lineno) from None
    for statement in tree.body:
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                if (alias.name, alias.asname) not in _ALLOWED_IMPORTS:
                    raise CodeBuildRefusal(
                        f"`import {alias.name}` is not allowed; the script imports only flowfile as ff",
                        statement.lineno,
                    )
        elif isinstance(statement, ast.ImportFrom):
            raise CodeBuildRefusal("`from ... import` is not allowed; use `import flowfile as ff`", statement.lineno)
        elif not isinstance(statement, ast.Assign | ast.Expr):
            raise CodeBuildRefusal(
                f"`{type(statement).__name__}` statements are not allowed; write one assignment per step",
                statement.lineno,
            )
    for node in ast.walk(tree):
        line = getattr(node, "lineno", None)
        if isinstance(node, _REFUSED_EXPRESSIONS):
            raise CodeBuildRefusal(f"`{type(node).__name__}` is not allowed; write one assignment per step", line)
        if isinstance(node, ast.Name) and node.id.startswith("_"):
            raise CodeBuildRefusal(f"`{node.id}` is not allowed; use plain names", line)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            raise CodeBuildRefusal(f"`{node.func.id}(...)` is not available; call ff.<name> or a frame method", line)
        if isinstance(node, ast.Attribute):
            attr = node.attr
            if attr.startswith("_"):
                raise CodeBuildRefusal(f"`.{attr}` is not allowed", line)
            if isinstance(node.value, ast.Name) and node.value.id == "ff" and attr in REFUSED_FF_NAMES:
                raise CodeBuildRefusal(f"`ff.{attr}` {REFUSED_FF_NAMES[attr]}", line)
            if attr in REFUSED_METHODS:
                raise CodeBuildRefusal(f"`.{attr}` {REFUSED_METHODS[attr]}", line)
            if attr.startswith(("write_", "sink_")):
                raise CodeBuildRefusal(f"`.{attr}` {_WRITES_REASON}", line)


@dataclass
class CanvasContext:
    """The flow on the canvas as the model and the clean run see it.

    ``cells`` are the notebook's rendered cells and ``provenance`` their canvas nodes, exactly what a push sends,
    so the clean run rebuilds them onto their canvas ids; ``snapshot`` seeds the session; ``block`` is the prompt
    text: the same code with a ``# columns:`` hint per step, capped at :data:`CONTEXT_CHAR_BUDGET` characters
    (the oldest steps go first when it is over). An empty canvas gives an empty context.
    """

    cells: list[tuple[str, str]] = field(default_factory=list)
    provenance: dict[str, list[tuple[str, int]]] = field(default_factory=dict)
    snapshot: dict[str, Any] = field(default_factory=dict)
    block: str = ""


def stored_resource_nodes(flow: Any) -> list[str]:
    """The live nodes whose cell would reach a stored resource in the clean run, as ``type:id``."""
    found = []
    for node in getattr(flow, "nodes", None) or []:
        node_type = getattr(node, "node_type", None)
        if node_type in STORED_RESOURCE_NODE_TYPES or getattr(node.setting_input, "is_user_defined", False):
            found.append(f"{node_type}:{node.node_id}")
    return found


def canvas_context(flow: Any) -> CanvasContext:
    """Render the live flow for the model; an empty context when there is nothing on the canvas, the exporter
    cannot express it (logged), or sharing is enabled and a live node reaches a stored resource (its cell would
    make the grant-less lookup the pre-scan keeps out of the model's code), so a build then starts from scratch."""
    if not getattr(flow, "nodes", None):
        return CanvasContext()
    if sharing.sharing_enabled() and (stored := stored_resource_nodes(flow)):
        logger.info("code build: flow %s holds %s; building without context", flow.flow_id, ", ".join(stored))
        return CanvasContext()
    from flowfile_core.notebook.push import seed_snapshot
    from flowfile_core.notebook.render import render

    try:
        rendering = render(flow)
        snapshot = seed_snapshot(flow)
    except Exception:
        logger.warning(
            "code build: could not render flow %s for the prompt; building without context", flow.flow_id, exc_info=True
        )
        return CanvasContext()
    cells = [(cell.cell_id, cell.code) for cell in rendering.cells]
    provenance: dict[str, list[tuple[str, int]]] = {}
    hinted: list[str] = []
    for cell in rendering.cells:
        text = cell.code.rstrip()
        if cell.node_ids:
            provenance[cell.cell_id] = [(flow.get_node(nid).node_type, nid) for nid in cell.node_ids]
            columns = [c["name"] for c in (snapshot["schemas"].get(cell.node_ids[-1]) or {}).get("output-0") or []]
            if columns:
                shown = ", ".join(columns[:_COLUMN_HINT_CAP]) + (", …" if len(columns) > _COLUMN_HINT_CAP else "")
                text = f"{text}  # columns: {shown}"
        hinted.append(text)
    block = "\n".join(hinted)
    if len(block) > CONTEXT_CHAR_BUDGET:
        kept: list[str] = []
        size = 0
        for text in reversed(hinted[1:]):
            if size + len(text) > CONTEXT_CHAR_BUDGET:
                break
            kept.insert(0, text)
            size += len(text) + 1
        block = "\n".join([hinted[0], "# ... earlier steps omitted ...", *kept])
    return CanvasContext(cells=cells, provenance=provenance, snapshot=snapshot, block=block)


def user_message(user_request: str, context: CanvasContext) -> str:
    """The user turn: the request alone on an empty canvas, else the current flow block first."""
    if not context.block:
        return user_request
    return f"## Current flow\n```python\n{context.block}\n```\n## Request\n{user_request}"


_HINT_RE = re.compile(r"\s*# columns:.*$")


def strip_echoed(code: str, context: CanvasContext) -> str:
    """The script without the lines it copied from the ``## Current flow`` block.

    A small model tends to restate the context before adding to it; run as written, every restated step would
    become a second copy of a node already on the canvas. A line is echoed when, trailing ``# columns:`` hint and
    surrounding whitespace aside, it equals a line of the block; the import is never dropped (it is harmless and
    the clean run's imports cell binds ``ff`` anyway).
    """
    if not context.block:
        return code
    seen = {_HINT_RE.sub("", line).strip() for line in context.block.splitlines()}
    seen.discard(_FLOWFILE_IMPORT)
    kept = [line for line in code.splitlines() if _HINT_RE.sub("", line).strip() not in seen]
    return "\n".join(kept)


def interpret_script(
    code: str, *, user_id: int, flow_id: int, ceiling: int, context: CanvasContext | None = None
) -> CleanRunResult:
    """Interpret ``code`` as the last notebook cell through the installed runner, after the canvas's own cells
    when ``context`` has them (so their nodes keep their canvas ids and the script's variables resolve)."""
    context = context or CanvasContext()
    request = CleanRunRequest(
        cells=[*context.cells, (BUILD_CELL, code)],
        provenance=context.provenance,
        ceiling=ceiling,
        snapshot=context.snapshot,
    )
    return get_clean_runner().clean_run(user_id, flow_id, request)


def _relative_paths(settings: Any) -> None:
    """Undo the frame's cwd-absolute path for a file that does not exist: the user typed a bare name and will point
    the Read node at the real file; a path under core's working directory would only mislead."""
    if not isinstance(settings, dict):
        return
    cwd = os.getcwd()
    for key in ("path", "abs_file_path"):
        value = settings.get(key)
        if isinstance(value, str) and value.startswith(cwd + os.sep) and not os.path.exists(value):
            settings[key] = value[len(cwd) + 1 :]
    for value in settings.values():
        _relative_paths(value)


def spec_from_flowfile_data(flowfile_data: dict[str, Any], new_ids: set[int] | None = None) -> dict[str, Any]:
    """The clean run's save-format payload as the ``{nodes, edges}`` spec ``_build_simple_diff`` consumes.

    Each node keeps its ``setting_input`` minus the identity and wiring keys the diff assigns again (the ids are
    renumbered onto the target flow); edges come from ``input_ids`` and, for a join, ``right_input_id`` second,
    each with the source handle it leaves from (``source_handle`` on the edge when it is not ``output-0``: a
    split's second frame, a gate's else side). With ``new_ids`` (the nodes the script's own cell built) only
    those become spec nodes, and an input that is not one of them is a live canvas node, carried as
    ``canvas_upstream_ids`` / ``canvas_right_input_id`` with ``canvas_source_handles``.
    Raises :class:`CodeBuildRefusal` for any node in :data:`REFUSED_NODE_TYPES` or an installed custom node.
    """
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, str]] = []
    by_id = {node["id"]: node for node in flowfile_data.get("nodes") or [] if isinstance(node, dict) and "id" in node}
    for node in by_id.values():
        if new_ids is not None and node.get("id") not in new_ids:
            continue
        node_type = str(node.get("type") or "")
        settings = node.get("setting_input") if isinstance(node.get("setting_input"), dict) else {}
        label = node.get("description") or node_type
        if node_type in REFUSED_NODE_TYPES or settings.get("is_user_defined"):
            what = "a Polars Code node" if node_type == "polars_code" else f"a {node_type} node"
            raise CodeBuildRefusal(
                f"the step `{label}` becomes {what}, which Simple build does not place; "
                "rewrite it with the listed methods"
            )
        kept = {key: value for key, value in settings.items() if key not in _IDENTITY_KEYS}
        _relative_paths(kept)
        node_id = str(node["id"])
        entry: dict[str, Any] = {"id": node_id, "type": node_type, "settings": kept}
        incoming = reconcile.incoming_edges(node, by_id)
        for slot in (reconcile.MAIN, reconcile.RIGHT):
            for upstream, handle in incoming.get(slot) or []:
                if new_ids is None or upstream in new_ids:
                    edge = {"source": str(upstream), "target": node_id}
                    if handle != reconcile.DEFAULT_HANDLE:
                        edge["source_handle"] = handle
                    edges.append(edge)
                    continue
                if slot == reconcile.MAIN:
                    entry.setdefault("canvas_upstream_ids", []).append(upstream)
                else:
                    entry["canvas_right_input_id"] = upstream
                if handle != reconcile.DEFAULT_HANDLE:
                    entry.setdefault("canvas_source_handles", {})[upstream] = handle
        nodes.append(entry)
    return {"nodes": nodes, "edges": edges}


def _build(code: str, *, flow: Any, flow_id: int, user_id: int, context: CanvasContext) -> dict[str, Any]:
    """The three gates on one script, then the diff; raises :class:`CodeBuildRefusal` at the first failure.

    A failure inside a canvas cell (a live node this caller's clean run cannot rebuild) is a
    :class:`CanvasCellFailure`: not the script's fault, so the caller builds again without the context.
    """
    from flowfile_core.ai.local_model import oneshot

    prescan(code)
    ceiling = int(getattr(flow, "node_id_ceiling", 0) or 0)
    result = interpret_script(code, user_id=user_id, flow_id=flow_id, ceiling=ceiling, context=context)
    if result.error is not None:
        message = result.error.strip().replace(_NEEDS_KERNEL, "")
        if result.cell_id not in (None, BUILD_CELL):
            raise CanvasCellFailure(f"cell {result.cell_id}: {message}", result.line)
        raise CodeBuildRefusal(message, result.line)
    new_ids = set(result.node_ids_by_cell.get(BUILD_CELL) or []) if context.cells else None
    if new_ids is not None and not new_ids:
        raise CodeBuildRefusal("the script added no new step; write the new lines that continue from the current flow")
    spec = spec_from_flowfile_data(result.flowfile_data, new_ids)
    try:
        built = oneshot._build_simple_diff(
            flow=flow, flow_id=flow_id, spec=spec, rationale="Generated flow (code mode)"
        )
    except oneshot.OneShotError as exc:
        raise CodeBuildRefusal(str(exc)) from exc
    # The interpreter's "placed unchecked" warnings are expected here: the files the script names rarely exist yet.
    built["code"] = code
    built["answer"] = None
    return built


def _repair_message(refusal: CodeBuildRefusal) -> str:
    where = f"at line {refusal.line}" if refusal.line else "and could not be placed"
    return (
        f"Your script failed {where}: {refusal.message}\n"
        "Rewrite the whole script and fix this. Keep the same steps. Output only one ```python block."
    )


async def generate_code_flow(
    *,
    provider: Provider,
    flow: Any,
    flow_id: int,
    user_id: int,
    user_request: str,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    """Ask the model for a FlowFrame script and turn it into a registered GraphDiff.

    Returns the ``_build_simple_diff`` result plus ``code`` (the accepted script) and ``answer`` (``None``); when the
    model answered instead of building, the answer result ``oneshot.generate_flow`` documents. A script that fails a
    gate, or a reply without one, gets one repair round; a second failure raises :class:`CodeBuildError`.

    With nodes on the canvas the model sees them as the notebook's rendered code (:func:`canvas_context`) and
    writes only the new steps; those are staged wired to the live nodes they continue from. A canvas cell the
    clean run cannot rebuild drops the context and the build starts over from scratch (logged).
    """
    context = await asyncio.to_thread(canvas_context, flow)
    while True:
        try:
            return await _generate_with(
                provider,
                context,
                flow=flow,
                flow_id=flow_id,
                user_id=user_id,
                user_request=user_request,
                max_tokens=max_tokens,
            )
        except CanvasCellFailure as failure:
            logger.warning("code build: flow %s canvas failed in the clean run (%s); retrying without context",
                           flow_id, failure.message)  # fmt: skip
            context = CanvasContext()


async def _generate_with(
    provider: Provider,
    context: CanvasContext,
    *,
    flow: Any,
    flow_id: int,
    user_id: int,
    user_request: str,
    max_tokens: int | None,
) -> dict[str, Any]:
    """One generation with ``context``: the model call, the gates and the repair round."""
    from flowfile_core.ai.local_model import oneshot

    messages = [
        Message(role="system", content=system_prompt()),
        Message(role="user", content=user_message(user_request, context)),
    ]
    for attempt in range(REPAIR_ROUNDS + 1):
        response = await provider.chat(messages, max_tokens=max_tokens or 1024, surface=SURFACE, user_id=user_id)
        content = response.content or ""
        code = extract_code(content)
        if code is None:
            answer = oneshot.extract_answer(content)
            if answer is not None:
                return {
                    "diff_id": None,
                    "op_count": 0,
                    "created": [],
                    "warnings": [],
                    "rationale": "Answered without building (the request did not describe a pipeline)",
                    "diff_payload": None,
                    "answer": answer,
                    "code": None,
                }
            refusal = CodeBuildRefusal("the reply held no ```python block with the script")
        else:
            code = ensure_import(strip_echoed(code, context))
            try:
                return await asyncio.to_thread(
                    _build, code, flow=flow, flow_id=flow_id, user_id=user_id, context=context
                )
            except CanvasCellFailure:
                raise
            except CodeBuildRefusal as exc:
                refusal = exc
        logger.info("code build attempt %d refused at line %s: %s", attempt + 1, refusal.line, refusal.message)
        if attempt == REPAIR_ROUNDS:
            raise CodeBuildError(refusal.message, line=refusal.line, code=code) from refusal
        messages = [
            *messages,
            Message(role="assistant", content=content),
            Message(role="user", content=_repair_message(refusal)),
        ]
    raise AssertionError("unreachable")  # pragma: no cover
