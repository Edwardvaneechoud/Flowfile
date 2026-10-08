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
from pathlib import Path
from typing import Any

from flowfile_core.ai import safety
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.notebook import allowlist
from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult, get_clean_runner

logger = logging.getLogger(__name__)

SURFACE = "code_build"
REPAIR_ROUNDS = 1
_PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "code_build.md"
_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
_FENCE_RE = re.compile(r"```[ \t]*([A-Za-z0-9_+-]*)[^\n]*\n(.*?)```", re.DOTALL)
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


def interpret_script(code: str, *, user_id: int, flow_id: int, ceiling: int) -> CleanRunResult:
    """Interpret ``code`` as one notebook cell through the installed runner on a fresh session graph."""
    request = CleanRunRequest(cells=[("build", code)], ceiling=ceiling, snapshot={})
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


def spec_from_flowfile_data(flowfile_data: dict[str, Any]) -> dict[str, Any]:
    """The clean run's save-format payload as the ``{nodes, edges}`` spec ``_build_simple_diff`` consumes.

    Each node keeps its ``setting_input`` minus the identity and wiring keys the diff assigns again (the ids are
    renumbered onto the target flow); edges come from ``input_ids`` and, for a join, ``right_input_id`` second.
    Raises :class:`CodeBuildRefusal` for any node in :data:`REFUSED_NODE_TYPES` or an installed custom node.
    """
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, str]] = []
    for node in flowfile_data.get("nodes") or []:
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
        nodes.append({"id": node_id, "type": node_type, "settings": kept})
        edges.extend({"source": str(upstream), "target": node_id} for upstream in node.get("input_ids") or [])
        if node.get("right_input_id") is not None:
            edges.append({"source": str(node["right_input_id"]), "target": node_id})
    return {"nodes": nodes, "edges": edges}


def _build(code: str, *, flow: Any, flow_id: int, user_id: int) -> dict[str, Any]:
    """The three gates on one script, then the diff; raises :class:`CodeBuildRefusal` at the first failure."""
    from flowfile_core.ai.local_model import oneshot

    prescan(code)
    ceiling = int(getattr(flow, "node_id_ceiling", 0) or 0)
    result = interpret_script(code, user_id=user_id, flow_id=flow_id, ceiling=ceiling)
    if result.error is not None:
        raise CodeBuildRefusal(result.error.strip().replace(_NEEDS_KERNEL, ""), result.line)
    spec = spec_from_flowfile_data(result.flowfile_data)
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
    gate gets one repair round; a second failure raises :class:`CodeBuildError`.
    """
    from flowfile_core.ai.local_model import oneshot

    messages = [Message(role="system", content=system_prompt()), Message(role="user", content=user_request)]
    code: str | None = None
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
            raise CodeBuildError("the model returned no script", line=None, code=None)
        code = ensure_import(code)
        try:
            return await asyncio.to_thread(_build, code, flow=flow, flow_id=flow_id, user_id=user_id)
        except CodeBuildRefusal as refusal:
            logger.info("code build attempt %d refused at line %s: %s", attempt + 1, refusal.line, refusal.message)
            if attempt == REPAIR_ROUNDS:
                raise CodeBuildError(refusal.message, line=refusal.line, code=code) from refusal
            messages = [
                *messages,
                Message(role="assistant", content=content),
                Message(role="user", content=_repair_message(refusal)),
            ]
    raise CodeBuildError("the model returned no script", line=None, code=code)  # pragma: no cover
