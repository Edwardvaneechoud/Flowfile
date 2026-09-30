import ast
import builtins
import functools
import heapq
import io
import json
import keyword
import os
import re
import textwrap
import tokenize
import types
from dataclasses import replace

import polars as pl
from polars_expr_transformer import PolarsCodeGenError, to_flowframe_code, to_polars_code

from flowfile_core.configs import logger
from flowfile_core.flowfile.code_generator.chain_fusion import NodeEmission, render_pipeline
from flowfile_core.flowfile.code_generator.connector_handlers import ConnectorHandlersMixin
from flowfile_core.flowfile.code_generator.custom_node_handlers import CustomNodeHandlersMixin
from flowfile_core.flowfile.code_generator.expression_helpers import ExpressionHelpersMixin
from flowfile_core.flowfile.code_generator.join_handlers import JoinHandlersMixin
from flowfile_core.flowfile.code_generator.native_handlers import FLOW_VAR, NativeHandlersMixin
from flowfile_core.flowfile.code_generator.param_codegen import (
    _SENTINEL_RE,
    SENTINEL_PREFIX,
    apply_param_sentinels,
    codegen_parameters,
    param_arg,
    param_sentinel,
    resolve_param_sentinels,
    restore_sentinels_to_refs,
)
from flowfile_core.flowfile.code_generator.transform_handlers import TransformHandlersMixin
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.param_types import coerce_param_value
from flowfile_core.flowfile.parameter_resolver import find_unresolved_in_model
from flowfile_core.flowfile.share.transform import _user_description
from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
from flowfile_core.flowfile.util.skip_rules import classify_graph, is_error_ish, uses_any_rule
from flowfile_core.notebook.compare import strip_outer_parens
from flowfile_core.schemas import input_schema, transform_schema
from shared.excel_writer import resolve_excel_write_mode
from shared.path_utils import DirectoryScanUnsupportedError, assert_directory_scan_supported

# repr() of nested Polars dtypes leaves inner names unqualified
# (repr(pl.List(pl.Int64)) == 'List(Int64)') — qualify every known dtype name
# so emitted schema literals are valid in a module that only imports pl.
_POLARS_DTYPE_NAMES = sorted((name for name in dir(pl.datatypes) if name[:1].isupper()), key=len, reverse=True)
_DTYPE_NAME_RE = re.compile(r"\b(" + "|".join(_POLARS_DTYPE_NAMES) + r")\b")


def _render_polars_dtype(dtype) -> str:
    return _DTYPE_NAME_RE.sub(r"pl.\1", repr(dtype))


# Emitted once at module level when a flow contains formula gates; kept in
# exact behavioral parity with flow_graph._gate_formula_matches.
_GATE_FORMULA_HELPER = '''\
def _flowfile_gate_formula_matches(df, predicate):
    """True when at least one row satisfies the gate's formula predicate."""
    frame = df.lazy() if isinstance(df, pl.DataFrame) else df
    return frame.filter(predicate).head(1).collect().height > 0'''

# Emitted when a gate formula references an exportable parameter; kept in
# exact behavioral parity with param_types.render_param_as_expr_literal.
_EXPR_LITERAL_HELPER = '''\
def _flowfile_expr_literal(value):
    """Render a parameter value as a flowfile-formula literal (parity with the engine)."""
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, float)):
        return repr(value)
    return json.dumps(str(value))'''


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


class UnsupportedNodeError(Exception):
    """Raised when code generation encounters a node type that cannot be converted to standalone code."""

    def __init__(self, node_type: str, node_id: int, reason: str):
        self.node_type = node_type
        self.node_id = node_id
        self.reason = reason
        super().__init__(f"Cannot generate code for node '{node_type}' (node_id={node_id}): {reason}")


@functools.lru_cache(maxsize=2048)
def _try_translate_to_ff_code(formula: str) -> str | None:
    """Translate a flowfile formula into native ``ff.``-prefixed FlowFrame expression code.

    Returns the validated code string, or None so callers fall back to the
    legacy ``flowfile_formula(s)`` emission (which is always correct). Mirrors
    flowfile_frame.flow_frame._try_translate_flowfile_formulas: generate via
    polars_expr_transformer.to_flowframe_code, then validate by eval'ing in a
    restricted namespace and checking the result is a FlowFrame Expr.

    Memoised on the formula text: translation takes no schema and the validation
    namespace is constant, so the result depends on nothing else.
    """
    try:
        generated = to_flowframe_code(formula)
    except PolarsCodeGenError:
        return None
    except Exception as e:
        logger.debug(f"to_flowframe_code failed for {formula!r}: {e}")
        return None
    if not generated:
        return None
    try:
        from flowfile_frame.expr import Expr as FlowFrameExpr
    except ImportError:
        logger.debug("flowfile package unavailable; cannot validate generated ff code")
        return None
    try:
        result = _eval_in_validation_namespace(generated)
    except Exception as e:
        logger.debug(f"Generated ff code failed validation for {formula!r}: {e}")
        return None
    return generated if isinstance(result, FlowFrameExpr) else None


@functools.lru_cache(maxsize=2048)
def _interprets_without_a_kernel(fl_code: str) -> bool:
    """Whether a notebook cell interprets a translated ``fl.`` snippet: every call and read is an allowlist entry.

    A ``lambda`` (hashing), a clock read (``datetime.datetime.now()``) or a method the allowlist does
    not name fails it. Memoised on the snippet text, the only input the verdict depends on.
    """
    from flowfile_core.notebook.interpret import interprets_expression

    return interprets_expression(fl_code)


@functools.lru_cache(maxsize=2048)
def _try_translate_to_polars_code(formula: str) -> str | None:
    """Translate a flowfile formula into native ``pl.``-prefixed Polars expression code.

    Returns the validated code string, or None so callers fall back to the
    ``simple_function_to_expr`` runtime emission (always correct, but it keeps
    the exported script dependent on polars_expr_transformer). Validation evals
    the snippet with the modules a generated expression may reference. Memoised
    on the formula text, the only input the result depends on.
    """
    try:
        generated = to_polars_code(formula)
    except PolarsCodeGenError:
        return None
    except Exception as e:
        logger.debug(f"to_polars_code failed for {formula!r}: {e}")
        return None
    if not generated:
        return None
    import datetime
    import hashlib

    try:
        result = eval(generated, {"__builtins__": {}}, {"pl": pl, "datetime": datetime, "hashlib": hashlib})  # noqa: S307
    except Exception as e:
        logger.debug(f"Generated polars code failed validation for {formula!r}: {e}")
        return None
    return generated if isinstance(result, pl.Expr) else None


def _polars_code_header(settings: input_schema.NodePolarsCode) -> str:
    """Comment naming a Polars-code node by its description, so the exported function is recognisable."""
    description = (settings.description or "").strip().splitlines()
    return f"# Custom Polars code: {description[0]}" if description else "# Custom Polars code"


def _legacy_polars_code_body(code: str) -> tuple[list[str], str | None]:
    """Text heuristics for Polars code that does not parse, so a node with broken code still exports."""
    if "output_df" not in code:
        return [], code
    lines = [line for line in code.split("\n") if line.strip()]
    if "return" in code:
        return lines, None
    assignments = [line.strip() for line in lines if "=" in line]
    return lines, (assignments[-1].split("=")[0].strip() if assignments else None)


def _assigns_name(statements: list[ast.stmt], name: str) -> bool:
    return any(
        isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Store)
        for statement in statements
        for node in ast.walk(statement)
    )


def _polars_code_function_body(code: str) -> tuple[list[str], str | None]:
    """Split a Polars-code node's source into function body lines and the expression to return.

    Like the runtime ``PolarsCodeParser._wrap_in_function`` the source is dedented and a function
    that assigns ``output_df`` returns it; unlike it, any lone expression is returned as is and
    otherwise the last top-level plain-name assignment is returned, so code the runtime accepts
    always exports. The code is parsed inside a function so a user-written top-level ``return`` is
    valid; when one is present nothing is appended. ``return_expr`` is None when nothing should be
    appended, and the body is empty when the whole source is the returned expression.
    """
    code = textwrap.dedent(code).strip()
    try:
        tree = ast.parse("def _f():\n" + textwrap.indent(code, "    "))
    except SyntaxError:
        return _legacy_polars_code_body(code)
    statements = tree.body[0].body
    lines = [line for line in code.split("\n") if line.strip()]
    if any(isinstance(statement, ast.Return) for statement in statements):
        return lines, None
    if _assigns_name(statements, "output_df"):
        return lines, "output_df"
    if len(statements) == 1 and isinstance(statements[0], ast.Expr):
        # Line 1 of the parsed source is the synthetic def; leading comment lines stay in the body.
        source_lines = code.split("\n")
        start = statements[0].lineno - 2
        return [line for line in source_lines[:start] if line.strip()], "\n".join(source_lines[start:])
    for statement in reversed(statements):
        if isinstance(statement, ast.Assign) and isinstance(statement.targets[0], ast.Name):
            return lines, statement.targets[0].id
    return lines, None


_FRAME_CLASS_NAMES = frozenset({"LazyFrame", "DataFrame"})


def _polars_code_to_flowframe(code: str, modules: tuple[str, ...] = ("pl", "ff")) -> str:
    """Rewrite ``modules`` attribute access to ``fl.`` and ``<module>.LazyFrame``/``DataFrame`` to ``fl.FlowFrame``.

    Token-level so string literals, comments and names such as ``df_pl`` are left alone;
    code that does not tokenize falls back to the plain text replacement.
    """
    try:
        tokens = [
            tok
            for tok in tokenize.generate_tokens(io.StringIO(code).readline)
            if tok.type not in (tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.COMMENT)
        ]
    except (tokenize.TokenError, IndentationError, SyntaxError):
        for module in modules:
            code = re.sub(rf"\b{module}\.", "fl.", code)
        return code.replace("fl.LazyFrame", "fl.FlowFrame").replace("fl.DataFrame", "fl.FlowFrame")

    def is_module_ref(index: int) -> bool:
        tok = tokens[index]
        preceded_by_dot = index > 0 and tokens[index - 1].string == "."
        followed_by_dot = index + 1 < len(tokens) and tokens[index + 1].string == "."
        return tok.type == tokenize.NAME and tok.string in modules and followed_by_dot and not preceded_by_dot

    edits: dict[int, list[tuple[int, int, str]]] = {}
    for index, tok in enumerate(tokens):
        replacement = None
        if is_module_ref(index):
            replacement = "fl"
        elif tok.type == tokenize.NAME and tok.string in _FRAME_CLASS_NAMES and index >= 2 and is_module_ref(index - 2):
            replacement = "FlowFrame"
        if replacement is not None:
            edits.setdefault(tok.start[0] - 1, []).append((tok.start[1], tok.end[1], replacement))
    lines = code.split("\n")
    for row, row_edits in edits.items():
        line = lines[row]
        for start, end, replacement in sorted(row_edits, reverse=True):
            line = line[:start] + replacement + line[end:]
        lines[row] = line
    return "\n".join(lines)


def _sql_query_literal_lines(sql_code: str) -> list[str]:
    """A SQL Query node's query as one single-line string literal per SQL line, for implicit concatenation.

    Single-line literals keep every escape exact through the export's indentation and let the
    parameter post-pass turn a ``${name}`` line into an f-string; a triple-quoted block would do neither.
    A last line that is only a parameter joins the line before it, since the post-pass turns a literal
    that is exactly one reference into a bare name, which cannot be concatenated with a string.
    """
    lines = sql_code.strip().split("\n")
    if len(lines) > 1 and _SENTINEL_RE.fullmatch(lines[-1]):
        lines[-2:] = [f"{lines[-2]}\n{lines[-1]}"]
    return [json.dumps(line + "\n", ensure_ascii=False) for line in lines[:-1]] + [
        json.dumps(lines[-1], ensure_ascii=False)
    ]


def _sql_query_input_vars(input_vars: dict[str, str]) -> list[str]:
    """The SQL Query node's inputs in canvas order: ``input_1``, ``input_2``, ... in the query."""
    return [var for key, var in input_vars.items() if key.startswith("main")]


# Expression names ``flowfile`` re-exports from flowfile_frame; a parity test pins them.
FF_VALIDATION_NAMES: tuple[str, ...] = (
    "col", "column", "count", "cum_count", "len", "lit", "max", "mean", "min", "sum", "when",
    "all_", "boolean", "by_dtype", "categorical", "contains", "date", "datetime", "duration", "ends_with",
    "float_", "integer", "list_", "matches", "numeric", "object_", "starts_with", "string", "struct",
    "temporal", "time",
    "Array", "Binary", "Boolean", "Categorical", "DataType", "DataTypeClass", "Date", "Datetime", "Decimal",
    "Duration", "Enum", "Field", "Float32", "Float64", "Int8", "Int16", "Int32", "Int64", "Int128", "List",
    "Null", "Object", "String", "Struct", "Time", "UInt8", "UInt16", "UInt32", "UInt64", "Unknown", "Utf8",
)  # fmt: skip


@functools.lru_cache(maxsize=1)
def _ff_validation_namespace() -> types.SimpleNamespace:
    """Stand-in for ``import flowfile as ff`` (or ``as fl``) built from flowfile_frame alone.

    Importing the top-level ``flowfile`` package mutates ``os.environ`` (single-file
    worker mode) and pulls in the web UI, so export validation must not do it. The
    namespace holds exactly ``FF_VALIDATION_NAMES`` — never more than ``flowfile``
    exports, so a snippet that validates here also runs under ``import flowfile``.
    Lazy import: flowfile_frame imports flowfile_core, so a module-level import is circular.
    """
    import flowfile_frame

    return types.SimpleNamespace(**{name: getattr(flowfile_frame, name) for name in FF_VALIDATION_NAMES})


def _eval_in_validation_namespace(code: str):
    """Eval generated ff code in the restricted namespace used for validation.

    Raises on any failure. The namespace mirrors what generated snippets may
    reference — callers emitting a validated snippet must also emit the matching
    imports (see _translate_to_ff_code).
    """
    import datetime

    return eval(  # noqa: S307
        code,
        {"__builtins__": {}},
        {"ff": _ff_validation_namespace(), "fl": _ff_validation_namespace(), "pl": pl, "datetime": datetime},
    )


# Operation-based variable labels for the few boundary variables that survive
# chain fusion. A bare label is used when it occurs once; a numeric suffix is
# appended only when a label is shared by several boundaries (see _plan_boundary_names).
NODE_TYPE_VAR_LABEL: dict[str, str] = {
    "read": "source",
    "list_files": "source",
    "csv_read": "source",
    "excel_read": "source",
    "manual_input": "source",
    "catalog_reader": "source",
    "catalog_sql_reader": "source",
    "cloud_storage_reader": "source",
    "database_reader": "source",
    "rest_api_reader": "source",
    "kafka_source": "source",
    "external_source": "source",
    "filter": "filtered",
    "formula": "computed",
    "multi_field_formula": "computed",
    "select": "selected",
    "dynamic_rename": "renamed",
    "data_cleansing": "cleansed",
    "sort": "ordered",
    "group_by": "grouped",
    "pivot": "pivoted",
    "pivot_no_index": "pivoted",
    "unpivot": "unpivoted",
    "join": "joined",
    "cross_join": "joined",
    "fuzzy_match": "joined",
    "union": "combined",
    "unique": "deduped",
    "record_id": "with_record_id",
    "record_count": "counted",
    "sample": "sampled",
    "text_to_rows": "exploded",
    "polars_code": "transformed",
    "graph_solver": "solved",
    "window_functions": "windowed",
    "train_model": "trained",
    "apply_model": "scored",
    "evaluate_model": "evaluation",
    "wait_for": "ready",
}

_NATIVE_LABELS = {"gate": "gate", "run_flow": "subflow", "python_script": "scripted", "flow_input": "flow_input"}
_NATIVE_TYPES = frozenset({"gate", "run_flow", "python_script", "flow_input", "flow_output"})


def node_label(node_type: str, node_id: int) -> str:
    """The deterministic variable name of an unnamed node: ``<type_label>_<id>`` (``filtered_12``)."""
    label = NODE_TYPE_VAR_LABEL.get(node_type) or re.sub(r"\W", "_", node_type)
    return f"{label}_{node_id}"


def _with_description(code: str, description: str) -> str | None:
    """``description=`` added to the call the last statement assigns (after defs and imports), else None."""
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
    if any(kw.arg == "description" for kw in call.keywords):
        return code
    lines = code.split("\n")
    line = lines[call.end_lineno - 1]
    col = len(line.encode("utf-8")[: call.end_col_offset - 1].decode("utf-8", errors="ignore"))
    before = code[: sum(len(prior) + 1 for prior in lines[: call.end_lineno - 1]) + col]
    rest, before = code[len(before) :], before.rstrip()
    separator = "" if before.endswith("(") else " " if before.endswith(",") else ", "
    return f"{before}{separator}description={json.dumps(description, ensure_ascii=False)}{rest}"


# Generated variable names are uniquified against these so they never shadow a
# Python builtin or keyword (e.g. a split literally named ``sorted`` or ``type``).
_RESERVED_NAMES: frozenset[str] = frozenset(keyword.kwlist) | frozenset(dir(builtins))


_FLAT_CTX_SHIM = '''# Minimal flowfile_ctx shim for standalone execution (logging/display only).
# Export the flow as a project (code_to_project) for the full flowfile_ctx API.
class _FlowfileCtx:
    def is_dry_run(self):
        # A single-file export runs for real, against real data.
        return False

    def log(self, message, level="INFO"):
        print(f"[{level}] {message}")

    def log_info(self, message):
        self.log(message, "INFO")

    def log_warning(self, message):
        self.log(message, "WARNING")

    def log_error(self, message):
        self.log(message, "ERROR")

    def display(self, obj, title="", max_rows=10_000):
        if title:
            print(f"=== {title} ===")
        print(obj)

    def explore(self, obj, title="", max_rows=10_000):
        self.display(obj, title)

    def __getattr__(self, name):
        raise NotImplementedError(
            f"flowfile_ctx.{name}() is not available in a single-file export; "
            "export the flow as a project (code_to_project) for the full flowfile_ctx API."
        )


flowfile_ctx = _FlowfileCtx()
globals().setdefault("display", flowfile_ctx.display)
globals().setdefault("explore", flowfile_ctx.explore)'''


class FlowGraphCodeConverter(
    JoinHandlersMixin,
    TransformHandlersMixin,
    ConnectorHandlersMixin,
    CustomNodeHandlersMixin,
    ExpressionHelpersMixin,
):
    """
    Base class for converting a FlowGraph into executable Python code.

    Node-type handlers live in the mixins above; this class owns orchestration
    (dispatch, chain fusion, boundary naming, final code assembly).

    Subclasses set `framework` to control whether code targets Polars or FlowFrame.
    """

    framework: str = "pl"
    flowfile_alias: str = "ff"
    placeholders: bool = False
    function_name: str = "run_etl_pipeline"

    def __init__(self, flow_graph: FlowGraph):
        self.flow_graph = flow_graph
        self.node_var_mapping: dict[int, str] = {}
        # (upstream_node_id, output_handle) -> variable name; populated by
        # multi-output handlers and consulted by _get_input_vars.
        self.node_handle_var_mapping: dict[tuple[int, str], str] = {}
        self.imports: set[str] = set()
        self.code_lines: list[str] = []
        self.output_nodes: list[tuple[int, str]] = []
        self.last_node_var: str | None = None
        self.unsupported_nodes: list[tuple[int, str, str]] = []
        self.custom_node_classes: dict[str, str] = {}
        # True once a custom node's source references flowfile_ctx: the flat export
        # then emits an inline shim; the project export ships flowfile_ctx.py.
        self._needs_flowfile_ctx = False
        # (node, effective_var, start, end) per emitting node; (start, end) slices code_lines.
        self._node_spans: list[tuple[FlowNode, str, int, int]] = []
        # node_id -> upstream node_id for nodes that emit nothing (passthroughs).
        self._passthrough: dict[int, int] = {}
        # Flow parameters that become function kwargs (set in convert()).
        self._codegen_params: list = []
        # node_id -> settings the handlers read (a sentinel-substituted copy when the node has refs).
        self._settings_cache: dict[int, object] = {}
        self.warnings: list[str] = []
        # Gate machinery: per-node conjunction of signed gate atoms
        # ((gate_id, needs_open) — False for else-handle consumers, rendered
        # as `not (...)`) driving if-block emission, per-gate condition
        # expression, and the runtime flag vars of formula gates. Empty when
        # the flow has no gates, which keeps the fused single-block path.
        self._gate_conds: dict[int, frozenset[tuple[int, bool]]] = {}
        self._gate_exprs: dict[int, str] = {}
        self._runtime_gate_flags: dict[int, str] = {}
        # ANY-rule (union) inputs guarded by gates beyond the union's own
        # condition: union node id -> {producer id -> extra signed atoms}.
        self._any_input_edge_conds: dict[int, dict[int, frozenset[tuple[int, bool]]]] = {}
        self._module_helpers: list[str] = []
        self._placeholder_reasons: dict[int, str] = {}
        self._fused: list[NodeEmission] = []

    def convert(self) -> str:
        """Convert the FlowGraph to code, turning ``${name}`` parameter references
        into references to same-named function arguments on the generated function.

        Handlers read settings through ``_settings_for``, which substitutes refs with
        sentinels on a private copy (the live node settings are never mutated, so a
        failing or concurrent export cannot leave sentinel text on the canvas). A
        token-level post-pass rewrites sentinels to argument references; anything
        that can't be rewritten safely degrades back to literal ``${name}`` text and
        is reported in ``self.warnings``.
        """
        self._codegen_params = codegen_parameters(self.flow_graph.flow_settings.parameters)
        self._settings_cache = {}
        code = self._convert_core()
        code, leaked = resolve_param_sentinels(code, {p.name for p in self._codegen_params})
        if leaked:
            self.warnings.append(
                f"Parameter reference(s) {sorted(leaked)} appear in places that cannot reference a "
                "function argument (e.g. multi-line strings) and were left as literal ${...} text."
            )
        return code

    def _convert_core(self) -> str:
        """
        Main method to convert the FlowGraph to Polars code.

        Returns:
            str: Complete Python code that can be executed standalone

        Raises:
            UnsupportedNodeError: If the graph contains nodes that cannot be converted
                to standalone code (e.g., database nodes, explore_data, external_source).
        """
        execution_plan = compute_execution_plan(
            nodes=self.flow_graph.nodes,
            flow_starts=self.flow_graph._flow_starts + self.flow_graph.get_implicit_starter_nodes(),
        )
        skip_ids = {node.node_id for node in execution_plan.skip_nodes}

        self._compute_gate_conditions(execution_plan)

        if self.placeholders:
            nodes = topological_order(list(self.flow_graph.nodes))
        else:
            nodes = [node for stage in execution_plan.stages for node in stage if node.node_id not in skip_ids]
        for node in nodes:
            self._generate_node_code(node)

        if self.unsupported_nodes:
            error_messages = []
            for node_id, node_type, reason in self.unsupported_nodes:
                error_messages.append(f"  - Node {node_id} ({node_type}): {reason}")
            raise UnsupportedNodeError(
                node_type=self.unsupported_nodes[0][1],
                node_id=self.unsupported_nodes[0][0],
                reason=(
                    f"The flow contains {len(self.unsupported_nodes)} node(s) that cannot be converted to code:\n"
                    + "\n".join(error_messages)
                ),
            )

        return self._build_final_code()

    def _settings_for(self, node: FlowNode):
        """The settings export handlers read for *node*, with ``${name}`` refs as sentinels.

        Returns the live ``setting_input`` when there is nothing to substitute, otherwise a
        deep copy with the sentinels applied; memoised per node for the current ``convert()``.
        """
        cached = self._settings_cache.get(node.node_id)
        if cached is not None:
            return cached
        live = node.setting_input
        settings = live
        param_names = {p.name for p in self._codegen_params}
        if live is not None and param_names and find_unresolved_in_model(live) & param_names:
            settings = live.model_copy(deep=True)
            apply_param_sentinels([settings], self._codegen_params)
        if settings is not None:
            self._settings_cache[node.node_id] = settings
        return settings

    def handle_output_node(self, node: FlowNode, var_name: str) -> None:
        settings = self._settings_for(node)
        if hasattr(settings, "is_flow_output") and settings.is_flow_output:
            self.output_nodes.append((node.node_id, var_name))

    def _generate_node_code(self, node: FlowNode) -> None:
        """Generate code for a node and record the line span it emitted.

        Output bookkeeping runs *after* the handler and reads the effective var
        from ``node_var_mapping`` so passthrough/remap handlers are honoured. A
        handler that emits nothing is recorded as a passthrough to its input.
        """
        node_type = node.node_type
        settings = self._settings_for(node)
        if isinstance(settings, input_schema.NodePromise):
            self._add_comment(f"# Skipping uninitialized node: {node.node_id}")
            return
        node_reference = getattr(settings, "node_reference", None)
        var_name = node_reference if node_reference else f"df_{node.node_id}"
        self.node_var_mapping[node.node_id] = var_name
        input_vars = self._get_input_vars(node)

        start = len(self.code_lines)
        if isinstance(settings, input_schema.UserDefinedNode) or getattr(settings, "is_user_defined", False):
            self._handle_user_defined(node, var_name, input_vars)
        else:
            handler = getattr(self, f"_handle_{node_type}", None)
            if handler:
                handler(settings, var_name, input_vars)
            else:
                self.unsupported_nodes.append(
                    (node.node_id, node_type, f"No code generator implemented for node type '{node_type}'")
                )
                self._add_comment(
                    f"# WARNING: Cannot generate code for node type '{node_type}' (node_id={node.node_id})"
                )
                self._add_comment("# This node type is not supported for code export")
        end = len(self.code_lines)

        if end == start:
            main_inputs = node.node_inputs.main_inputs or []
            if len(main_inputs) == 1:
                node_cond = self._gate_conds.get(node.node_id, frozenset())
                input_cond = self._gate_conds.get(main_inputs[0].node_id, frozenset())
                if self._gate_exprs and node.node_type != "gate" and node_cond != input_cond:
                    # A gated no-op passthrough must still materialize under
                    # its condition: eliding it would resolve downstream and
                    # return references to the ungated upstream variable,
                    # leaking data past a closed gate in the exported script.
                    # (Gates themselves stay elided — their passthrough is
                    # unconditional; only their consumers are guarded.)
                    input_var = self._resolve_upstream_var(node, main_inputs[0].node_id, "df")
                    self.node_var_mapping[node.node_id] = var_name
                    self._add_code(f"{var_name} = {input_var}")
                    self._add_code("")
                    end = len(self.code_lines)
                else:
                    self._passthrough[node.node_id] = main_inputs[0].node_id

        effective_var = self.node_var_mapping[node.node_id]
        self.handle_output_node(node, effective_var)
        if node.node_template.output > 0:
            self.last_node_var = effective_var

        if end > start:
            self._node_spans.append((node, effective_var, start, end))

    def _compute_gate_conditions(self, execution_plan) -> None:
        """Compute, per node, which gates must be open for it to run.

        Mirrors the runtime trigger rules symbolically: an edge from a gate
        adds that gate to the consumer's condition set; ALL-rule nodes need
        every input's gates open (union of edge conditions) while the
        ANY-rule union runs when any branch survives (intersection). The
        resulting sets drive if-block emission in _render_body_with_gates.
        """
        gates = [node for node in self.flow_graph.nodes if node.node_type == "gate"]
        if not gates:
            return
        parameters_by_name = {p.name: p for p in self.flow_graph.flow_settings.parameters}
        codegen_param_names = {p.name for p in self._codegen_params}
        for gate in gates:
            gate_input = getattr(self._settings_for(gate), "gate_input", None)
            if gate_input is None:
                continue
            if gate_input.condition_source == "formula":
                self._runtime_gate_flags[gate.node_id] = f"_gate_{gate.node_id}_open"
                self._gate_exprs[gate.node_id] = self._runtime_gate_flags[gate.node_id]
                continue
            expr = self._gate_condition_expr(gate_input, parameters_by_name, codegen_param_names)
            if expr is None:
                self.unsupported_nodes.append(
                    (
                        gate.node_id,
                        "gate",
                        self._gate_refusal_reason(gate_input, parameters_by_name, codegen_param_names),
                    )
                )
                continue
            self._gate_exprs[gate.node_id] = expr

        conds: dict[int, frozenset[tuple[int, bool]]] = {}
        for node in execution_plan.all_nodes:
            edge_conds: list[tuple[int, frozenset[tuple[int, bool]]]] = []
            for input_node in node.all_inputs:
                if input_node is None:
                    continue
                edge = conds.get(input_node.node_id, frozenset())
                if input_node.node_type == "gate" and input_node.node_id in self._gate_exprs:
                    # Polarity mirrors the engine: an edge off the else handle
                    # of an else_output gate needs the condition FALSE. A stale
                    # output-1 edge on a single-output gate keeps needs-open —
                    # matching dead_gate_handles killing both handles when
                    # such a gate closes.
                    needs_open = not (
                        node._input_output_handles.get(input_node.node_id, "output-0") == "output-1"
                        and getattr(self._settings_for(input_node), "else_output", False)
                    )
                    edge = edge | {(input_node.node_id, needs_open)}
                edge_conds.append((input_node.node_id, edge))
            if not edge_conds:
                continue
            if uses_any_rule(node):
                combined = frozenset.intersection(*(edge for _, edge in edge_conds))
                # Per-input gates beyond the node's own condition: the union
                # runs regardless, but each such input's contribution must be
                # guarded in the emitted concat (engine parity: a closed-gate
                # input is dropped, so it never enters the concat list).
                extras = {
                    producer_id: edge - combined for producer_id, edge in edge_conds if edge - combined
                }
                if extras:
                    self._any_input_edge_conds[node.node_id] = extras
            else:
                combined = frozenset().union(*(edge for _, edge in edge_conds))
            if combined:
                conds[node.node_id] = combined
        self._gate_conds = conds
        if self._runtime_gate_flags:
            self._module_helpers.append(_GATE_FORMULA_HELPER)
        if self._gate_exprs:
            # Registered here, not during body rendering: _build_final_code
            # serializes imports before _render_body runs, so an import added
            # while emitting gated pre-inits/helpers would be dropped. The
            # gate machinery emits pl-based literals in every framework.
            self.imports.add("import polars as pl")

    @staticmethod
    def _gate_refusal_reason(gate_input, parameters_by_name, codegen_param_names) -> str:
        """An accurate diagnosis for a parameter-mode gate the export refuses.

        Distinguishes an unknown/unexportable parameter from a coercion
        failure. A coercion failure caused by a ``${ref}`` in the comparison
        value is a real engine feature (refs resolve at run time) that the
        export simply does not support for typed parameters — the message
        must say so instead of denying the parameter exists.
        """
        if gate_input.parameter not in parameters_by_name or gate_input.parameter not in codegen_param_names:
            return (
                f"Gate condition references parameter '{gate_input.parameter}' which does not "
                "exist as an exportable flow parameter"
            )
        value = restore_sentinels_to_refs(gate_input.value)
        if "${" in value:
            return (
                f"Gate compares typed parameter '{gate_input.parameter}' against a parameter "
                f"reference ({value!r}); typed parameter-to-parameter comparison is not supported "
                "in code export (the engine resolves it at run time)"
            )
        return (
            f"Gate condition value {value!r} cannot be coerced to the declared type of "
            f"parameter '{gate_input.parameter}'"
        )

    @staticmethod
    def _gate_condition_expr(gate_input, parameters_by_name, codegen_param_names) -> str | None:
        """Render a parameter-mode gate condition over the function's kwargs.

        Comparison values are coerced with the parameter's declared type at
        export time so the emitted literal compares like the runtime does.
        Returns None when the parameter is unknown/unexportable or a value
        cannot be coerced — the caller refuses the export.
        """
        param = parameters_by_name.get(gate_input.parameter)
        if param is None or gate_input.parameter not in codegen_param_names:
            return None
        operator = gate_input.operator
        try:
            if operator in ("equals", "not_equals"):
                literal = repr(coerce_param_value(param.type, gate_input.value, param.enum_values))
                expr = f"{gate_input.parameter} {'==' if operator == 'equals' else '!='} {literal}"
            elif operator in ("in", "not_in"):
                items = [
                    repr(coerce_param_value(param.type, raw.strip(), param.enum_values))
                    for raw in gate_input.value.split(",")
                    if raw.strip()
                ]
                membership = f"{gate_input.parameter} in [{', '.join(items)}]"
                expr = membership if operator == "in" else f"not ({membership})"
            elif operator in ("is_true", "is_false"):
                truthy = f"str({gate_input.parameter}).strip().lower() in ('true', '1', 'yes', 'on')"
                expr = truthy if operator == "is_true" else f"not ({truthy})"
            else:  # is_set
                expr = f"str({gate_input.parameter}).strip() != ''"
        except ValueError:
            return None
        return expr

    @staticmethod
    def _empty_frame_schema_expr(node: FlowNode) -> str | None:
        """``pl.LazyFrame(schema={...})`` matching the node's predicted schema."""
        try:
            schema = node.schema
            if not schema:
                return None
            # Some schema strings are lossy (a bare "List" without its inner
            # type) and can't be cast back — fall back to None (drop) so the
            # emitted module never carries an uninstantiable dtype literal.
            fields = ", ".join(
                f"{column.name!r}: {_render_polars_dtype(cast_str_to_polars_type(column.data_type))}"
                for column in schema
            )
        except Exception:
            return None
        return f"pl.LazyFrame(schema={{{fields}}})"

    @staticmethod
    def _conds_are_complementary(a: frozenset[tuple[int, bool]], b: frozenset[tuple[int, bool]]) -> bool:
        """Exactly {(g, True)} vs {(g, False)} for the same gate — nothing else.

        The sole trigger for if/else fusion: shared prefixes (P∧g / P∧¬g),
        different gates, and multi-atom sets never qualify, because the else
        of ``if P and g`` is not ``P and not g``.
        """
        if len(a) != 1 or len(b) != 1:
            return False
        (gate_a, open_a), = a
        (gate_b, open_b), = b
        return gate_a == gate_b and open_a != open_b

    def _render_gate_cond(self, cond: frozenset[tuple[int, bool]]) -> str:
        parts = []
        flag_names = set(self._runtime_gate_flags.values())
        for gate_id, needs_open in sorted(cond):
            expr = self._gate_exprs[gate_id]
            if needs_open:
                parts.append(expr)
            elif expr in flag_names:
                parts.append(f"not {expr}")
            else:
                parts.append(f"not ({expr})")
        if len(parts) == 1:
            return parts[0]
        return " and ".join(f"({part})" for part in parts)

    def _gate_probe_expr(self, input_var: str) -> str:
        """The frame expression handed to the gate formula helper (a polars frame)."""
        return input_var

    def _mark_expr_literal_needed(self) -> None:
        self.imports.add("import json")
        if _EXPR_LITERAL_HELPER not in self._module_helpers:
            self._module_helpers.append(_EXPR_LITERAL_HELPER)

    def _gate_formula_arg(self, formula: str) -> str:
        """The formula expression handed to ``simple_function_to_expr``.

        ``${param}`` refs are consumed here — never left for the generic
        sentinel post-pass, which would inject the raw kwarg into an f-string
        (a bare token, breaking string params). The engine renders refs as
        typed expression literals (``resolve_expression_parameters`` →
        ``render_param_as_expr_literal``), so each ref becomes a
        ``_flowfile_expr_literal(<name>)`` call in the emitted module.
        """
        # Cosmetic strip: the engine's parser is whitespace-insensitive, so
        # padding cannot change behavior — embed the trimmed text.
        stripped = formula.strip()
        sentinel_to_name = {
            param_sentinel(p.name): p.name for p in self._codegen_params if param_sentinel(p.name) in stripped
        }
        if not sentinel_to_name:
            return self._py_str(stripped)
        self._mark_expr_literal_needed()
        if stripped in sentinel_to_name:
            return f"_flowfile_expr_literal({sentinel_to_name[stripped]})"
        pattern = re.compile("|".join(re.escape(s) for s in sorted(sentinel_to_name, key=len, reverse=True)))
        parts: list[str] = []
        pos = 0
        for match in pattern.finditer(stripped):
            parts.append(self._fstring_segment(stripped[pos : match.start()]))
            parts.append("{_flowfile_expr_literal(" + sentinel_to_name[match.group(0)] + ")}")
            pos = match.end()
        parts.append(self._fstring_segment(stripped[pos:]))
        return 'f"' + "".join(parts) + '"'

    @staticmethod
    def _fstring_segment(text: str) -> str:
        """A literal f-string segment: braces doubled, then _py_str's quote-escaping."""
        return json.dumps(text.replace("{", "{{").replace("}", "}}"), ensure_ascii=False)[1:-1]

    def _handle_gate(self, settings: input_schema.NodeGate, var_name: str, input_vars: dict[str, str]) -> None:
        """Gates are passthroughs: remap downstream references to the data input.

        Shared by the Polars and FlowFrame exports. Parameter-mode gates emit
        nothing here — their condition becomes the if-block guarding
        downstream nodes. Formula gates emit their routing flag: the formula
        applied as a row predicate to the control (right) input when wired,
        else to the data input — open iff any row matches, mirroring
        flow_graph._formula_gate_is_closed. The probed frame goes through
        ``_gate_probe_expr`` so the FlowFrame export can hand the helper the
        underlying LazyFrame instead of the FlowFrame wrapper.
        """
        main_df = input_vars.get("main", "df")
        self.node_var_mapping[settings.node_id] = main_df
        gate_input = settings.gate_input
        if gate_input.condition_source != "formula":
            return
        if not gate_input.formula.strip():
            self.unsupported_nodes.append(
                (settings.node_id, "gate", "Gate routes on a formula, but no formula is configured")
            )
            return
        self.imports.add(
            "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr"
        )
        source_df = input_vars.get("right") or main_df
        flag = self._runtime_gate_flags[settings.node_id]
        formula_arg = self._gate_formula_arg(gate_input.formula)
        self._add_code(
            f"{flag} = _flowfile_gate_formula_matches("
            f"{self._gate_probe_expr(source_df)}, simple_function_to_expr({formula_arg}))"
        )
        self._add_code("")

    def _resolve_upstream_var(self, downstream: FlowNode, upstream_id: int, default: str) -> str:
        """Resolve the variable name for an upstream node, honouring its output handle.

        Multi-output upstream nodes register per-handle variable names in
        ``node_handle_var_mapping``; for single-output upstreams the legacy
        ``node_var_mapping`` is the single source of truth.
        """
        handle = downstream._input_output_handles.get(upstream_id, "output-0")
        per_handle = self.node_handle_var_mapping.get((upstream_id, handle))
        if per_handle is not None:
            return per_handle
        return self.node_var_mapping.get(upstream_id, default)

    def _get_input_vars(self, node: FlowNode) -> dict[str, str]:
        """Get input variable names for a node, keyed by port role.

        Insertion order is the positional-argument order for custom-node ``process()``
        calls, so it must match ``FlowNode._slot_input_pairs`` — canvas handle order:
        main, then right (input-1), then left (input-2).
        """
        input_vars = {}

        if node.node_inputs.main_inputs:
            if len(node.node_inputs.main_inputs) == 1:
                input_vars["main"] = self._resolve_upstream_var(
                    node, node.node_inputs.main_inputs[0].node_id, "df"
                )
            else:
                for i, input_node in enumerate(node.node_inputs.main_inputs):
                    input_vars[f"main_{i}"] = self._resolve_upstream_var(node, input_node.node_id, f"df_{i}")

        if node.node_inputs.right_input:
            input_vars["right"] = self._resolve_upstream_var(
                node, node.node_inputs.right_input.node_id, "df_right"
            )

        if node.node_inputs.left_input:
            input_vars["left"] = self._resolve_upstream_var(
                node, node.node_inputs.left_input.node_id, "df_left"
            )

        return input_vars

    def _csv_scan_kwarg_lines(self, file_settings: input_schema.ReceivedTable) -> list[str]:
        """The indented csv reader kwargs, shared by the single-file and directory emissions."""
        table_settings = file_settings.table_settings
        return [
            f'    separator="{table_settings.delimiter}",',
            f"    has_header={table_settings.has_headers},",
            f"    ignore_errors={table_settings.ignore_errors},",
            '    encoding="utf8-lossy",',
            f"    skip_rows={table_settings.starting_from_line},",
        ]

    def _handle_csv_read(self, file_settings: input_schema.ReceivedTable, var_name: str):
        if file_settings.table_settings.encoding.lower() in ("utf-8", "utf8"):
            self._add_code(f"{var_name} = {self.framework}.scan_csv(")
            self._add_code(f"    {self._py_path(file_settings.abs_file_path)},")
            for kwarg_line in self._csv_scan_kwarg_lines(file_settings):
                self._add_code(kwarg_line)
            self._emit_include_file_paths(file_settings)
            self._add_code(")")
        else:
            self._handle_csv_read_non_utf8(file_settings, var_name)

    def _handle_csv_read_non_utf8(self, file_settings: input_schema.ReceivedTable, var_name: str):
        self._add_code(f"{var_name} = {self.framework}.read_csv(")
        self._add_code(f"    {self._py_path(file_settings.abs_file_path)},")
        self._add_code(f'    separator="{file_settings.table_settings.delimiter}",')
        self._add_code(f"    has_header={file_settings.table_settings.has_headers},")
        self._add_code(f"    ignore_errors={file_settings.table_settings.ignore_errors},")
        if file_settings.table_settings.encoding:
            self._add_code(f'    encoding="{file_settings.table_settings.encoding}",')
        self._add_code(f"    skip_rows={file_settings.table_settings.starting_from_line},")
        self._add_code(").lazy()")

    def _handle_read(self, settings: input_schema.NodeRead, var_name: str, input_vars: dict[str, str]) -> None:
        file_settings = settings.received_file
        if file_settings.scan_mode == "directory":
            self._handle_directory_read(settings, file_settings, var_name)
            return
        if file_settings.file_type == "csv":
            self._handle_csv_read(file_settings, var_name)
        elif file_settings.file_type in ("parquet", "ipc", "ndjson"):
            self._emit_single_file_scan(file_settings.file_type, file_settings, var_name)
        elif file_settings.file_type in ("avro", "ipc_stream"):
            self._handle_eager_read(file_settings.file_type, file_settings, var_name)
        elif file_settings.file_type in ("xlsx", "excel"):
            self._handle_excel_read(file_settings, var_name)
        else:
            # No emission for this type; refuse loudly instead of leaving var_name unbound.
            self.unsupported_nodes.append(
                (settings.node_id, "read", f"No code generation for file type '{file_settings.file_type}'.")
            )
            return
        self._add_code("")

    def _emit_single_file_scan(self, file_type: str, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        source = self._py_path(file_settings.abs_file_path)
        reader = self._scan_callable(file_type)
        if file_settings.include_file_paths:
            self._add_code(f"{var_name} = {reader}(")
            self._add_code(f"    {source},")
            self._emit_include_file_paths(file_settings)
            self._add_code(")")
        else:
            self._add_code(f"{var_name} = {reader}({source})")

    def _emit_include_file_paths(self, file_settings: input_schema.ReceivedTable) -> None:
        """The engine adds the source-path column in every scan mode, so the export must too."""
        if file_settings.include_file_paths:
            self._add_code(f"    include_file_paths={self._py_str(file_settings.include_file_paths)},")

    def _handle_directory_read(
        self, settings: input_schema.NodeRead, file_settings: input_schema.ReceivedTable, var_name: str
    ) -> None:
        """Emit a directory-mode read, or refuse the node when the format cannot be scanned as a set.

        The capability check is the engine's own, so a flow that runs exports and a flow that
        would fail at run time is refused here instead of emitting a script that breaks later.
        """
        try:
            assert_directory_scan_supported(
                file_settings.file_type,
                getattr(file_settings.table_settings, "encoding", None),
                file_settings.path,
            )
        except DirectoryScanUnsupportedError as e:
            self.unsupported_nodes.append((settings.node_id, "read", str(e)))
            return
        self._emit_directory_read(file_settings, var_name)
        self._add_code("")

    def _scan_callable(self, file_type: str) -> str:
        """The reader expression for ``file_type``, registering whatever import it needs."""
        return f"{self.framework}.scan_{file_type}"

    def _read_callable(self, file_type: str) -> str:
        """The eager reader expression for formats polars cannot scan (avro, ipc_stream)."""
        return f"{self.framework}.read_{file_type}"

    def _handle_eager_read(self, file_type: str, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        source = self._py_path(file_settings.abs_file_path)
        self._add_code(f"{var_name} = {self._read_callable(file_type)}({source})")

    def _directory_scan_source(self, file_settings: input_schema.ReceivedTable) -> str:
        """Render the file-list expression a directory scan reads, mirroring the engine.

        The engine hands polars the sorted list of existing matches rather than the pattern
        itself, so the export repeats that expansion — polars' own globbing includes dotfiles
        and does not promise the same ordering.
        """
        self.imports.add("import glob")
        self.imports.add("import os")
        pattern = self._py_path(file_settings.abs_file_path)
        return f"sorted(p for p in glob.glob({pattern}, recursive=True) if os.path.isfile(p))"

    def _emit_directory_read(self, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        """Emit the polars-shaped directory scan; FlowFrame overrides this with its own reader."""
        self._add_code(f"{var_name} = {self._scan_callable(file_settings.file_type)}(")
        self._add_code(f"    {self._directory_scan_source(file_settings)},")
        # The list is already fully expanded; polars must not re-glob literal filenames.
        self._add_code("    glob=False,")
        if file_settings.file_type == "csv":
            for kwarg_line in self._csv_scan_kwarg_lines(file_settings):
                self._add_code(kwarg_line)
        self._emit_include_file_paths(file_settings)
        self._add_code(")")

    def _handle_excel_read(self, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        self._add_code(f"{var_name} = {self.framework}.read_excel(")
        self._add_code(f"    {self._py_path(file_settings.abs_file_path)},")
        if file_settings.table_settings.sheet_name:
            self._add_code(f'    sheet_name="{file_settings.table_settings.sheet_name}",')
        self._add_code(")")

    def _generate_pl_schema_with_typing(self, flowfile_schema: list[FlowfileColumn]) -> str:
        polars_schema_str = (
            f"{self.framework}.Schema(["
            + ", ".join(
                f"({self._py_str(flowfile_column.column_name)}, {self.framework}.{flowfile_column.data_type})"
                for flowfile_column in flowfile_schema
            )
            + "])"
        )
        return polars_schema_str

    def get_manual_schema_input(self, flowfile_schema: list[FlowfileColumn]) -> str:
        polars_schema_str = self._generate_pl_schema_with_typing(flowfile_schema)
        is_valid_pl_schema = self._validate_pl_schema(polars_schema_str)
        if is_valid_pl_schema:
            return polars_schema_str
        else:
            return "[" + ", ".join([self._py_str(c.name) for c in flowfile_schema]) + "]"

    @staticmethod
    def _validate_pl_schema(pl_schema_str: str) -> bool:
        try:
            _globals = {"pl": pl}
            eval(pl_schema_str, _globals)
            return True
        except Exception as e:
            logger.error(f"Invalid Polars schema: {e}")
            return False

    def _handle_manual_input(
        self, settings: input_schema.NodeManualInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle manual data input nodes."""
        data = settings.raw_data_format.data
        flowfile_schema = list(
            FlowfileColumn.create_from_minimal_field_info(c) for c in settings.raw_data_format.columns
        )
        schema = self.get_manual_schema_input(flowfile_schema)
        self._add_code(f"{var_name} = {self.framework}.LazyFrame({data}, schema={schema}, strict=False)")
        self._add_code("")

    def _handle_flow_input(
        self, settings: input_schema.NodeFlowInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Subflow input placeholder: exported code uses its sample data (or an empty frame)."""
        self._add_code(f"# flow_input '{settings.input_name}' — sample data used for standalone export")
        if settings.raw_data_format is not None and settings.raw_data_format.columns:
            self._handle_manual_input(settings, var_name, input_vars)
        else:
            self._add_code(f"{var_name} = {self.framework}.LazyFrame()")
            self._add_code("")

    def _handle_flow_output(
        self, settings: input_schema.NodeFlowOutput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Subflow output marker: passthrough."""
        input_df = input_vars.get("main", "df")
        self._add_code(f"{var_name} = {input_df}  # flow_output '{settings.output_name}'")
        self._add_code("")

    def _column_dtype(self, node_id: int, field: str) -> str | None:
        """Predicted Polars dtype string (e.g. ``"Boolean"``) of ``field`` on ``node_id``'s
        output schema, or None when unavailable. Filter is schema-preserving, so the filter
        node's own predicted schema carries the input column dtypes."""
        try:
            node = self.flow_graph.get_node(node_id)
            schema = node.get_predicted_schema() if node is not None else None
        except Exception:
            return None
        for col in schema or []:
            if col.column_name == field:
                return col.data_type
        return None

    def _advanced_filter_predicate(self, formula: str) -> str:
        """Polars source for an advanced-filter formula: the native ``pl`` expression
        when polars_expr_transformer can translate it, else a ``simple_function_to_expr`` call."""
        pl_code = _try_translate_to_polars_code(formula)
        if pl_code:
            self._register_expr_stdlib_imports(pl_code)
            return pl_code
        self.imports.add("from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr")
        return f"simple_function_to_expr({self._py_str(formula)})"

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle filter nodes."""
        input_df = input_vars.get("main", "df")

        if settings.split_mode:
            self._handle_filter_split(settings, var_name, input_df)
            return

        if settings.filter_input.is_advanced():
            predicate = self._advanced_filter_predicate(settings.filter_input.advanced_filter)
            self._add_code(f"{var_name} = {input_df}.filter({predicate})")
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                field_dtype = self._column_dtype(settings.node_id, basic.field)
                filter_expr = self._create_basic_filter_expr(basic, field_dtype)
                self._add_code(f"{var_name} = {input_df}.filter({filter_expr})")
            else:
                self._add_code(f"{var_name} = {input_df}  # No filter applied")
        self._add_code("")

    def _handle_filter_split(self, settings: input_schema.NodeFilter, var_name: str, input_df: str) -> None:
        """Emit a pass/fail split filter (output-0 = pass, output-1 = fail).

        Mirrors FlowDataEngine.filter_split: ``df.filter(pred)`` for pass,
        ``df.filter(~pred)`` for fail. Rows where the predicate is null are
        dropped from both (polars filter semantics).
        """
        node_id = settings.node_id
        pred_var = f"_filter_{node_id}_pred"
        if settings.filter_input.is_advanced():
            self._add_code(f"{pred_var} = {self._advanced_filter_predicate(settings.filter_input.advanced_filter)}")
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                field_dtype = self._column_dtype(settings.node_id, basic.field)
                self._add_code(f"{pred_var} = {self._create_basic_filter_expr(basic, field_dtype)}")
            else:
                # No predicate -> pass keeps everything, fail is empty.
                self._add_code(f"{pred_var} = {self.framework}.lit(True)")
        pass_var = f"{var_name}_pass"
        fail_var = f"{var_name}_fail"
        self._add_code(f"{pass_var} = {input_df}.filter({pred_var})")
        self._add_code(f"{fail_var} = {input_df}.filter(~({pred_var}))")
        self.node_handle_var_mapping[(node_id, "output-0")] = pass_var
        self.node_handle_var_mapping[(node_id, "output-1")] = fail_var
        self.node_var_mapping[node_id] = pass_var
        self._add_code("")

    def _cleansing_target_columns(self, settings: input_schema.NodeDataCleansing) -> tuple[list[str], list[str]]:
        """String- and Numeric-group cleansing targets from the node's export-time schema.

        Data cleansing is schema-preserving, so the node's own predicted schema carries
        the incoming columns and dtypes. Targeting keys off ``data_type_group`` exactly
        like FlowDataEngine.build_data_cleansing_expressions; columns of any other group
        (and selected names that no longer exist) simply drop out.
        """
        cleansing = settings.cleansing_input
        try:
            node = self.flow_graph.get_node(settings.node_id)
            schema = node.get_predicted_schema() if node is not None else None
        except Exception:
            schema = None
        columns = [(col.column_name, col.data_type_group) for col in schema or []]
        if cleansing.selection_mode == "list":
            selected = set(cleansing.selected_columns)
            columns = [(name, group) for name, group in columns if name in selected]
        string_columns = [name for name, group in columns if group == "String"]
        numeric_columns = [name for name, group in columns if group == "Numeric"]
        return string_columns, numeric_columns

    def _cleansing_drops_columns(self, node_id: int) -> bool:
        """True when this node or any ancestor can drop columns at run time.

        ``remove_null_columns`` is data-dependent while schema prediction is static, so
        this node's export-time column list can name a column that an upstream cleansing
        node — or this one — has already removed by the time the script reaches it.
        """
        seen: set[int] = {node_id}
        stack = [node_id]
        while stack:
            node = self.flow_graph.get_node(stack.pop())
            if node is None:
                continue
            cleansing = getattr(self._settings_for(node), "cleansing_input", None)
            if cleansing is not None and cleansing.remove_null_columns:
                return True
            for producer_id in self._raw_producer_ids(node):
                if producer_id not in seen:
                    seen.add(producer_id)
                    stack.append(producer_id)
        return False

    def _cleansing_col_expr(self, columns: list[str], schema_var: str | None) -> str:
        """Render the ``pl.col(...)`` head for a group of identically-cleansed columns.

        When a null-column drop is in play the export-time candidates may not all survive
        the run, so the list is intersected with the frame's runtime schema instead of
        being emitted as a literal.
        """
        literal = f"[{', '.join(self._py_str(name) for name in columns)}]"
        if schema_var is None:
            return f"pl.col({literal})"
        return f"pl.col([c for c in {literal} if c in {schema_var}])"

    def _cleansing_string_expr(self, settings: transform_schema.DataCleansingInput, head: str) -> str:
        """Chain the string rules onto ``head`` in the order the engine applies them."""
        expr = head
        if settings.replace_nulls_with_blank:
            expr += '.fill_null("")'
        if settings.remove_letters:
            expr += f'.str.replace_all({transform_schema.CLEANSING_LETTERS_REGEX!r}, "")'
        if settings.remove_numbers:
            expr += f'.str.replace_all({transform_schema.CLEANSING_NUMBERS_REGEX!r}, "")'
        if settings.remove_punctuation:
            expr += f'.str.replace_all({transform_schema.CLEANSING_PUNCTUATION_REGEX!r}, "")'
        if settings.remove_all_whitespace:
            expr += f'.str.replace_all({transform_schema.CLEANSING_WHITESPACE_REGEX!r}, "")'
        else:
            if settings.normalize_whitespace:
                expr += f'.str.replace_all({transform_schema.CLEANSING_WHITESPACE_RUN_REGEX!r}, " ")'
            if settings.trim_whitespace:
                expr += ".str.strip_chars()"
        if settings.case_mode == "uppercase":
            expr += ".str.to_uppercase()"
        elif settings.case_mode == "lowercase":
            expr += ".str.to_lowercase()"
        elif settings.case_mode == "titlecase":
            expr += ".str.to_titlecase()"
        return expr

    def _handle_data_cleansing(
        self, settings: input_schema.NodeDataCleansing, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle data cleansing nodes, emitting pure Polars.

        Mirrors FlowDataEngine.apply_data_cleansing stage for stage: drop rows that are
        null in every column, drop columns that are null in every row, then one
        ``with_columns`` carrying the per-column rules. The null profile is taken from
        the incoming frame (not the row-filtered one) because that is what the engine
        profiles, and the character classes are interpolated from the transform_schema
        constants so the engine and the exported script cannot drift apart.
        """
        self.imports.add("import polars as pl")
        input_df = input_vars.get("main", "df")
        cleansing = settings.cleansing_input
        current = input_df

        if cleansing.remove_null_rows:
            self._add_code(f"{var_name} = {current}.filter(~pl.all_horizontal(pl.all().is_null()))")
            current = var_name

        if cleansing.remove_null_columns:
            height_var = f"_dc_{settings.node_id}_height"
            nulls_var = f"_dc_{settings.node_id}_nulls"
            drop_var = f"_dc_{settings.node_id}_drop"
            self._add_code(f"{height_var} = {input_df}.select(pl.len()).collect().item()")
            self._add_code(f"{nulls_var} = {input_df}.select(pl.all().null_count()).collect().to_dicts()[0]")
            self._add_code(
                f"{drop_var} = [c for c, n in {nulls_var}.items() if {height_var} > 0 and n == {height_var}]"
            )
            self._add_code(f"{var_name} = {current}.drop({drop_var})")
            current = var_name

        schema_var = f"_dc_{settings.node_id}_cols" if self._cleansing_drops_columns(settings.node_id) else None
        string_columns, numeric_columns = self._cleansing_target_columns(settings)
        exprs: list[str] = []
        if string_columns:
            head = self._cleansing_col_expr(string_columns, schema_var)
            string_expr = self._cleansing_string_expr(cleansing, head)
            if string_expr != head:  # no string rule enabled -> nothing to emit
                exprs.append(string_expr)
        if numeric_columns and cleansing.replace_nulls_with_zero:
            exprs.append(f'{self._cleansing_col_expr(numeric_columns, schema_var)}.fill_null(strategy="zero")')

        if exprs:
            if schema_var is not None:
                self._add_code(f"{schema_var} = set({current}.collect_schema().names())")
            self._add_code(f"{var_name} = {current}.with_columns(")
            for expr in exprs:
                self._add_code(f"    {expr},")
            self._add_code(")")
            current = var_name

        if current == input_df:
            self._add_code(f"{var_name} = {input_df}  # No cleansing rules enabled")
        self._add_code("")

    def _handle_record_count(self, settings: input_schema.NodeRecordCount, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        self._add_code(f"{var_name} = {input_df}.select({self.framework}.len().alias('number_of_records'))")

    def _handle_graph_solver(self, settings: input_schema.NodeGraphSolver, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        from_col_name = settings.graph_solver_input.col_from
        to_col_name = settings.graph_solver_input.col_to
        output_col_name = settings.graph_solver_input.output_column_name
        self._add_code(
            f"{var_name} = {input_df}.with_columns(graph_solver({self.framework}.col({self._py_str(from_col_name)}), "
            f"{self.framework}.col({self._py_str(to_col_name)}))"
            f".alias({self._py_str(output_col_name)}))"
        )
        self._add_code("")
        self.imports.add("from polars_grouper import graph_solver")

    def _input_column_names(self, node_id: int) -> list[str] | None:
        """Column names on the single main input of ``node_id``, or None when unavailable."""
        try:
            node = self.flow_graph.get_node(node_id)
            inputs = node.node_inputs.main_inputs or []
            if len(inputs) != 1:
                return None
            schema = inputs[0].get_predicted_schema()
        except Exception:
            return None
        return [c.column_name for c in schema] if schema else None

    def _drop_shaped_select(self, settings: input_schema.NodeSelect) -> list[str] | None:
        """Columns to drop when a select node only unchecks columns — no rename, cast or
        reorder, unlisted columns kept — so it exports as ``.drop([...])``; else None."""
        if not settings.keep_missing or settings.sorted_by not in (None, "none"):
            return None
        rows = sorted(settings.select_input, key=lambda r: 0 if r.position is None else r.position)
        kept: list[str] = []
        dropped: list[str] = []
        for row in rows:
            if row.is_altered or row.data_type_change or row.new_name not in (None, row.old_name):
                return None
            (kept if row.keep else dropped).append(row.old_name)
        if not dropped:
            return None
        input_names = self._input_column_names(settings.node_id)
        if input_names is None:
            return None
        dropped = [c for c in dropped if c in input_names]
        kept = [c for c in kept if c in input_names]
        listed = set(kept) | set(dropped)
        if kept + [c for c in input_names if c not in listed] != [c for c in input_names if c not in dropped]:
            return None
        return dropped or None

    def _emit_select_chain(self, settings: input_schema.NodeSelect, var_name: str, input_df: str) -> None:
        """A ``keep_missing`` select over an unknown input as ``.drop()``, ``.rename()`` and a cast, in that order.

        A ``.select([...])`` of the listed columns would drop every column the canvas passes through.
        Dropping first lets a rename take the name of a dropped column. Column order follows the
        input, where the canvas moves the listed columns first.
        """
        rows = [row for row in settings.select_input if row.is_available]
        renames = {row.old_name: row.new_name for row in rows if row.keep and row.new_name != row.old_name}
        drops = [row.old_name for row in rows if not row.keep]
        casts = [
            f"{self.framework}.col({self._py_str(row.new_name)}).cast({self._get_polars_dtype(row.data_type)})"
            for row in rows
            if row.keep and (row.data_type_change or row.is_altered) and row.data_type
        ]
        chain = ""
        if drops:
            chain += f".drop([{', '.join(self._py_str(name) for name in drops)}])"
        if renames:
            chain += ".rename({" + ", ".join(f"{self._py_str(k)}: {self._py_str(v)}" for k, v in renames.items()) + "})"
        if casts:
            chain += f".with_columns([{', '.join(casts)}])"
        if not chain:
            self.node_var_mapping[settings.node_id] = input_df
            return
        self._add_code(f"{var_name} = {input_df}{chain}")
        self._add_code("")

    def _handle_select(self, settings: input_schema.NodeSelect, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle select/rename nodes."""
        input_df = input_vars.get("main", "df")
        drop_cols = self._drop_shaped_select(settings)
        if drop_cols:
            self._add_code(f"{var_name} = {input_df}.drop([{', '.join(self._py_str(c) for c in drop_cols)}])")
            self._add_code("")
            return
        unlisted: list[str] = []
        if settings.keep_missing and settings.select_input:
            input_names = self._input_column_names(settings.node_id)
            if input_names is None:
                self._emit_select_chain(settings, var_name, input_df)
                return
            listed = {row.old_name for row in settings.select_input}
            unlisted = [name for name in input_names if name not in listed]
        select_exprs = []
        for select_input in settings.select_input:
            if select_input.keep and select_input.is_available:
                col = f"{self.framework}.col({self._py_str(select_input.old_name)})"
                if select_input.old_name != select_input.new_name:
                    expr = f"{col}.alias({self._py_str(select_input.new_name)})"
                else:
                    expr = col

                if (select_input.data_type_change or select_input.is_altered) and select_input.data_type:
                    polars_dtype = self._get_polars_dtype(select_input.data_type)
                    expr = f"{expr}.cast({polars_dtype})"

                select_exprs.append(expr)
        # keep_missing passes every unlisted input column through, after the listed ones
        select_exprs += [f"{self.framework}.col({self._py_str(name)})" for name in unlisted]

        if select_exprs:
            self._add_code(f"{var_name} = {input_df}.select([")
            for expr in select_exprs:
                self._add_code(f"    {expr},")
            self._add_code("])")
            self._add_code("")
        else:
            # Nothing selected -> transparent passthrough; remap and emit nothing.
            self.node_var_mapping[settings.node_id] = input_df

    def _handle_output(self, settings: input_schema.NodeOutput, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        output_settings = settings.output_settings

        if output_settings.file_type == "csv":
            self._add_code(f"{input_df}.sink_csv(")
            self._add_code(f"    {self._py_path(output_settings.abs_file_path)},")
            self._add_code(f'    separator="{output_settings.table_settings.delimiter}"')
            self._add_code(")")
        elif output_settings.file_type == "parquet":
            self._add_code(f"{input_df}.sink_parquet({self._py_path(output_settings.abs_file_path)})")
        elif output_settings.file_type == "excel":
            self._handle_output_excel(input_df, output_settings, settings.node_id)

        self._add_code("")

    def _handle_output_excel(self, input_df: str, output_settings, node_id: int) -> None:
        write_mode = resolve_excel_write_mode(output_settings.write_mode)
        self._add_code(f"{input_df}.write_excel(")
        self._add_code(f"    {self._py_path(output_settings.abs_file_path)},")
        if write_mode == "overwrite":
            self._add_code(f'    worksheet="{output_settings.table_settings.sheet_name}"')
        else:
            self._add_code(f'    worksheet="{output_settings.table_settings.sheet_name}",')
            self._add_code(f'    write_mode="{write_mode}"')
        self._add_code(")")

    def _handle_polars_code(
        self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle custom Polars code nodes."""
        code = textwrap.dedent(settings.polars_code_input.polars_code).strip()
        if len(input_vars) == 0:
            params = ""
            args = ""
        elif len(input_vars) == 1:
            params = f"input_df: {self.framework}.LazyFrame"
            input_df = list(input_vars.values())[0]
            args = input_df
        else:
            param_list = []
            arg_list = []
            i = 1
            for key in sorted(input_vars.keys()):
                if key.startswith("main"):
                    param_list.append(f"input_df_{i}: {self.framework}.LazyFrame")
                    arg_list.append(input_vars[key])
                    i += 1
            params = ", ".join(param_list)
            args = ", ".join(arg_list)

        self._add_code(_polars_code_header(settings))
        self._add_code(f"def _polars_code_{settings.node_id}({params}):")
        self._emit_polars_code_body(code)

        self._add_code("")

        self._add_code(f"{var_name} = _polars_code_{settings.node_id}({args})")
        self._add_code("")

    def _emit_polars_code_body(self, code: str) -> None:
        """Emit a Polars-code node's function body, with the return the runtime wrapper would add."""
        body_lines, return_expr = _polars_code_function_body(code)
        for line in body_lines:
            self._add_code(f"    {line}")
        if return_expr is not None:
            self._add_code(f"    return {return_expr}")

    def _handle_sql_query(self, settings: input_schema.NodeSqlQuery, var_name: str, input_vars: dict[str, str]) -> None:
        """A SQL Query node as ``pl.SQLContext``, its inputs registered as ``input_1``, ``input_2``, ..."""
        tables = ", ".join(f"input_{i}={var}" for i, var in enumerate(_sql_query_input_vars(input_vars), start=1))
        self._add_code(f"{var_name} = pl.SQLContext({tables}).execute(")
        for literal in _sql_query_literal_lines(settings.sql_query_input.sql_code):
            self._add_code(f"    {literal}")
        self._add_code(")")
        self._add_code("")

    # Handlers for unsupported node types - these add nodes to the unsupported list

    def _handle_explore_data(
        self, settings: input_schema.NodeExploreData, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Elide explore_data (interactive-only): remap to its input and emit nothing.

        Downstream references and the return resolve straight to the upstream
        frame, so no dead ``var = input`` passthrough appears in the script —
        matching ``--run-flow``, which already drops UI-only explore_data nodes.
        """
        self.node_var_mapping[settings.node_id] = input_vars.get("main", "df")

    # Helper methods

    def _add_code(self, line: str) -> None:
        """Add a line of code."""
        self.code_lines.append(line)

    def _add_comment(self, comment: str) -> None:
        """Add a comment line."""
        self.code_lines.append(comment)

    def _raw_producer_ids(self, node: FlowNode) -> list[int]:
        """Upstream node ids feeding ``node`` (main + left + right)."""
        ids = [inp.node_id for inp in (node.node_inputs.main_inputs or [])]
        if node.node_inputs.left_input:
            ids.append(node.node_inputs.left_input.node_id)
        if node.node_inputs.right_input:
            ids.append(node.node_inputs.right_input.node_id)
        return ids

    def _resolve_producer(self, node_id: int) -> int:
        """Hop through elided passthrough nodes to the real producing node."""
        seen: set[int] = set()
        while node_id in self._passthrough and node_id not in seen:
            seen.add(node_id)
            node_id = self._passthrough[node_id]
        return node_id

    def _ends_statement(self, node: FlowNode) -> bool:
        """A flow output ends its statement instead of fusing into the next call."""
        return bool(getattr(self._settings_for(node), "is_flow_output", False))

    def _is_filter_split(self, node: FlowNode) -> bool:
        return node.node_type == "filter" and bool(getattr(self._settings_for(node), "split_mode", False))

    def _var_label(self, node: FlowNode) -> str:
        if self._is_filter_split(node):
            return "split"
        return NODE_TYPE_VAR_LABEL.get(node.node_type, "df")

    def _render_body(self) -> list[str]:
        """Fuse linear single-use chains and give the surviving boundaries clean names."""
        if self._gate_exprs:
            return self._render_body_with_gates()
        emissions: list[NodeEmission] = []
        node_by_id: dict[int, FlowNode] = {}
        routed = {node_id for node_id, _handle in self.node_handle_var_mapping}
        for node, effective_var, start, end in self._node_spans:
            lines = self.code_lines[start:end]
            while lines and lines[-1] == "":
                lines = lines[:-1]
            if not lines:
                continue
            resolved = {self._resolve_producer(pid) for pid in self._raw_producer_ids(node)}
            num_inputs = len(resolved)
            main_producer_id = next(iter(resolved)) if num_inputs == 1 else None
            pinned = bool(getattr(self._settings_for(node), "node_reference", None))
            # A node read through per-exit accessors (a gate's ``.then``) stays its own statement.
            boundary = self._ends_statement(node) or node.node_id in routed
            emissions.append(
                NodeEmission(
                    node.node_id, effective_var, lines, main_producer_id,
                    num_inputs, boundary, pinned,
                    self._placeholder_reasons.get(node.node_id),
                )
            )
            node_by_id[node.node_id] = node

        consumers: dict[int, list[int]] = {em.node_id: [] for em in emissions}
        for em in emissions:
            node = node_by_id[em.node_id]
            for pid in {self._resolve_producer(p) for p in self._raw_producer_ids(node)}:
                if pid in consumers:
                    consumers[pid].append(em.node_id)

        fused = render_pipeline(emissions, consumers)
        rename = self._plan_boundary_names(emissions, {em.node_id for em in fused}, node_by_id)
        body = self._apply_renames([line for em in fused for line in em.lines], rename)
        self._fused = []
        for em in fused:
            lines, body = body[: len(em.lines)], body[len(em.lines) :]
            self._fused.append(replace(em, lines=lines, var_name=rename.get(em.var_name, em.var_name)))
        return [line for index, em in enumerate(self._fused) for line in ([""] if index else []) + em.lines]

    def emissions(self, verbatim_refs: bool = False) -> list[NodeEmission]:
        """The body's statements after ``convert()``: one per fused chain, names final, parameter refs resolved.

        Each emission's ``node_ids`` lists every node the statement contains in data-flow order,
        ``code`` is its text as the body spells it, and ``placeholder_reason`` is set for a
        ``fl.canvas_node`` placeholder. Empty for a gated Polars export, which renders if-blocks.
        With ``verbatim_refs`` a reference inside text stays ``${name}`` rather than an f-string field.
        """
        names = {p.name for p in self._codegen_params}
        return [
            replace(em, lines=resolve_param_sentinels(em.code, names, verbatim_refs)[0].split("\n"))
            for em in self._fused
        ]

    def import_lines(self) -> list[str]:
        """The sorted import statements the exported module declares."""
        return sorted(self.imports)

    def helpers(self) -> list[str]:
        """Module-level helper definitions the body calls (e.g. ``_flowfile_flow_parameter``), in emission order."""
        return list(self._module_helpers)

    def parameters(self) -> list:
        """The flow parameters the wrapper declares as ``run_etl_pipeline(*, name=default)`` keyword arguments."""
        return list(self._codegen_params)

    def _render_body_with_gates(self) -> list[str]:
        """Emit the body with real ``if`` blocks around gated segments.

        Nodes whose condition set is non-empty emit inside an
        ``if <conjunction>:`` block; consecutive same-condition nodes share
        one block, and a block whose condition is exactly complementary to
        the previous one (one gate's then/else pair, singleton atoms only)
        emits as its ``else:``. Union references to gated vars are guarded at the union's
        own emission site, so the only remaining ungated references are the
        module's return path — only gated vars the return will reference get
        a stand-in pre-init (control-gate flags stay False-initialized).
        Chain fusion is deliberately bypassed here: fused chains and block
        boundaries don't compose, and correctness beats prettiness for gated
        exports.
        """
        body: list[str] = []
        for flag in sorted(self._runtime_gate_flags.values()):
            body.append(f"{flag} = False")
        return_vars = self._return_referenced_vars()
        seen_vars: set[str] = set()
        for node, effective_var, _start, _end in self._node_spans:
            cond = self._gate_conds.get(node.node_id, frozenset())
            if not cond or effective_var in seen_vars or effective_var not in return_vars:
                continue
            seen_vars.add(effective_var)
            # A gated-off terminal still gets returned: a zero-row frame with
            # its predicted schema keeps the return well-defined when the
            # branch didn't run. None when the schema can't be predicted.
            schema_expr = self._empty_frame_schema_expr(node)
            body.append(f"{effective_var} = {schema_expr or 'None'}")
        # Multi-output nodes under a gate bind sibling per-handle vars
        # (df_N_fail, df_N_test, output-N vars) — same rule: pre-init (to
        # None, per-handle schemas aren't statically known) only when the
        # return path references them.
        for (handle_node_id, _handle), handle_var in sorted(self.node_handle_var_mapping.items(), key=str):
            if (
                self._gate_conds.get(handle_node_id)
                and handle_var not in seen_vars
                and handle_var in return_vars
            ):
                seen_vars.add(handle_var)
                body.append(f"{handle_var} = None")
        if body:
            body.append("")

        current_cond: frozenset[tuple[int, bool]] = frozenset()
        in_else_block = False
        for node, _effective_var, start, end in self._node_spans:
            lines = self.code_lines[start:end]
            while lines and lines[-1] == "":
                lines = lines[:-1]
            if not lines:
                continue
            cond = self._gate_conds.get(node.node_id, frozenset())
            if cond != current_cond:
                # Adjacent exactly-complementary singleton blocks fuse into a
                # real if/else; an else block never chains a second else.
                merge_else = not in_else_block and self._conds_are_complementary(current_cond, cond)
                if merge_else:
                    while body and body[-1] == "":
                        body.pop()
                    body.append("else:")
                else:
                    if body and body[-1] != "":
                        body.append("")
                    if cond:
                        body.append(f"if {self._render_gate_cond(cond)}:")
                in_else_block = merge_else
                current_cond = cond
            indent = "    " if cond else ""
            for line in lines:
                body.append(f"{indent}{line}" if line else "")
            body.append("")
        while body and body[-1] == "":
            body.pop()
        return body

    def _is_renameable(self, em: NodeEmission, node: FlowNode) -> bool:
        """Renameable boundaries: single-output ``df_N`` assignments and filter splits.

        Multi-output nodes (random_split, ML train/apply) bind a *derived* var
        (``df_N_train``) with sibling outputs, so renaming only the primary would
        break consistency — leave their tokens untouched.
        """
        if em.pinned:
            return False
        if self._is_filter_split(node):
            return True
        return em.var_name == f"df_{em.node_id}"

    def _plan_boundary_names(
        self, emissions: list[NodeEmission], survivors: set[int], node_by_id: dict[int, FlowNode]
    ) -> dict[str, str]:
        """Map provisional ``df_N`` tokens to clean labels; suffix only on collision."""
        ordered = [em for em in emissions if em.node_id in survivors]
        renameable = [em for em in ordered if self._is_renameable(em, node_by_id[em.node_id])]
        labels = {em.node_id: self._var_label(node_by_id[em.node_id]) for em in renameable}
        counts: dict[str, int] = {}
        for label in labels.values():
            counts[label] = counts.get(label, 0) + 1

        used: set[str] = {em.var_name for em in ordered if em.pinned} | set(_RESERVED_NAMES) | self._imported_names()
        final: dict[int, str] = {}
        seen: dict[str, int] = {}
        for em in renameable:
            label = labels[em.node_id]
            if counts[label] > 1:
                seen[label] = seen.get(label, 0) + 1
                name = f"{label}_{seen[label]}"
            else:
                name = label
            name = self._uniquify(name, used)
            final[em.node_id] = name

        rename: dict[str, str] = {}
        for em in renameable:
            name = final[em.node_id]
            nid = em.node_id
            if self._is_filter_split(node_by_id[nid]):
                rename[f"df_{nid}_pass"] = f"{name}_pass"
                rename[f"df_{nid}_fail"] = f"{name}_fail"
                rename[f"_filter_{nid}_pred"] = f"{name}_pred"
            else:
                rename[em.var_name] = name

        # random_split: name each output by its split (train/test/val), not df_N_<split>.
        for em in ordered:
            node = node_by_id[em.node_id]
            if em.pinned or node.node_type != "random_split":
                continue
            for split in getattr(self._settings_for(node), "splits", []):
                rename[f"df_{em.node_id}_{split.name}"] = self._uniquify(split.name, used)
        return rename

    def _imported_names(self) -> set[str]:
        """Names bound by ``self.imports`` so generated vars never shadow an import alias."""
        names: set[str] = set()
        for statement in self.imports:
            try:
                tree = ast.parse(statement)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        names.add(alias.asname or alias.name.split(".")[0])
                elif isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        names.add(alias.asname or alias.name)
        return names

    @staticmethod
    def _uniquify(name: str, used: set[str]) -> str:
        """Return ``name`` (or a numbered variant) not already in ``used``; records it."""
        candidate, bump = name, 2
        while candidate in used:
            candidate, bump = f"{name}_{bump}", bump + 1
        used.add(candidate)
        return candidate

    def _apply_renames(self, body: list[str], rename: dict[str, str]) -> list[str]:
        """Substitute provisional tokens with final names across body + return targets.

        Renames only identifier (NAME) tokens, never identifiers inside string or
        comment tokens, so a column literal like ``pl.col("df_2")`` survives intact.
        Falls back to the legacy regex when the body is not tokenizable, so behaviour
        degrades (less precise) rather than breaking.
        """
        if not rename:
            return body
        out = self._rename_tokens(body, rename)
        if out is None:
            out = self._rename_regex(body, rename)

        def renamed(var: str) -> str:  # token-level, so ``gate_2.then`` follows its gate
            return (self._rename_tokens([var], rename) or [rename.get(var, var)])[0]

        self.output_nodes = [(nid, renamed(var)) for nid, var in self.output_nodes]
        if self.last_node_var is not None:
            self.last_node_var = renamed(self.last_node_var)
        return out

    @staticmethod
    def _rename_regex(body: list[str], rename: dict[str, str]) -> list[str]:
        """Legacy fallback: regex substitution over whole lines (used only when the
        body cannot be tokenized)."""
        # `(?!=)` leaves keyword-argument names (``param=``, no surrounding spaces)
        # untouched while still renaming assignment targets (``var = ``) and value
        # references — e.g. a notebook call ``run(df_1=df_1.data)`` keeps its
        # contract param ``df_1`` but renames the upstream value.
        patterns = [(re.compile(r"\b" + re.escape(old) + r"\b(?!=)"), new) for old, new in rename.items()]
        out = []
        for line in body:
            for pat, new in patterns:
                line = pat.sub(new, line)
            out.append(line)
        return out

    @staticmethod
    def _rename_tokens(body: list[str], rename: dict[str, str]) -> list[str] | None:
        """Rename NAME tokens only, leaving string/comment tokens untouched.

        Returns the rewritten body (same element structure as the input), or None
        when the joined body cannot be tokenized so the caller can fall back.
        """
        source = "\n".join(body)
        try:
            tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
        except (tokenize.TokenError, IndentationError, SyntaxError, ValueError):
            return None
        physical = source.split("\n")
        edits: dict[int, list[tuple[int, int, str]]] = {}
        for tok in tokens:
            if tok.type != tokenize.NAME:
                continue
            new = rename.get(tok.string)
            if new is None:
                continue
            (srow, scol), (_erow, ecol) = tok.start, tok.end  # NAME tokens never span lines
            line = physical[srow - 1]
            # Replicate the legacy `(?!=)`: skip ``param=`` (kwarg/contract names) while
            # still renaming assignment targets (``var = ``) and value references.
            if ecol < len(line) and line[ecol] == "=":
                continue
            edits.setdefault(srow - 1, []).append((scol, ecol, new))
        for idx, line_edits in edits.items():
            line = physical[idx]
            for scol, ecol, new in sorted(line_edits, reverse=True):  # right-to-left keeps cols valid
                line = line[:scol] + new + line[ecol:]
            physical[idx] = line
        # Re-group physical lines back into the original body element structure
        # (some elements carry embedded newlines, e.g. the fuzzy-join emit).
        out: list[str] = []
        pos = 0
        for element in body:
            span = element.count("\n") + 1
            out.append("\n".join(physical[pos : pos + span]))
            pos += span
        return out

    def _return_referenced_vars(self) -> set[str]:
        """The variable names ``add_return_code`` will reference, mirroring its logic.

        Used by ``_render_body_with_gates`` (after all spans are emitted, so
        ``output_nodes``/``last_node_var`` are final) to decide which gated
        vars need a stand-in pre-init for the return path.
        """
        if self.output_nodes:
            return {var_name for _, var_name in self.output_nodes}
        if self.last_node_var:
            return {self.last_node_var}
        return set()

    def add_return_code(self, lines: list[str]) -> None:
        if self.output_nodes:
            if len(self.output_nodes) == 1:
                _, var_name = self.output_nodes[0]
                lines.append(f"    return {var_name}")
            else:
                lines.append("    return {")
                for node_id, var_name in self.output_nodes:
                    lines.append(f'        "node_{node_id}": {var_name},')
                lines.append("    }")
        elif self.last_node_var:
            lines.append(f"    return {self.last_node_var}")
        else:
            lines.append("    return None")

    def _build_final_code(self) -> str:
        """Build the final Python code."""
        lines = []

        lines.extend(sorted(self.imports))
        lines.append("")
        lines.append("")

        for helper in self._module_helpers:
            lines.extend(helper.split("\n"))
            lines.append("")
            lines.append("")

        # Only the flat export inlines custom-node classes; the project export
        # writes them to modules and ships flowfile_ctx.py instead, so this
        # inline shim never lands in a project's pipeline.py.
        if self.custom_node_classes and self._needs_flowfile_ctx:
            lines.extend(_FLAT_CTX_SHIM.split("\n"))
            lines.append("")
            lines.append("")

        if self.custom_node_classes:
            lines.append("# Custom Node Class Definitions")
            lines.append("# These classes are user-defined nodes that were included in the flow")
            lines.append("")
            for _class_name, source_code in self.custom_node_classes.items():
                for source_line in source_code.split("\n"):
                    lines.append(source_line)
                lines.append("")
            lines.append("")

        lines.append(self._function_def_line())
        lines.append('    """')
        lines.append(f"    ETL Pipeline: {self.flow_graph.__name__}")
        lines.append("    Generated from Flowfile")
        lines.append('    """')
        lines.append("    ")

        for line in self._render_body():
            if line:
                lines.append(f"    {line}")
            else:
                lines.append("")
        lines.append("")
        self.add_return_code(lines)
        self._append_module_epilogue(lines)

        return "\n".join(lines)

    def _function_def_line(self) -> str:
        if not self._codegen_params:
            return f"def {self.function_name}():"
        args = ", ".join(param_arg(p) for p in self._codegen_params)
        return f"def {self.function_name}(*, {args}):"

    def _append_module_epilogue(self, lines: list[str]) -> None:
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append(f"    pipeline_output = {self.function_name}()")


class FlowGraphToPolarsConverter(FlowGraphCodeConverter):
    """Generates standalone Polars code from a FlowGraph."""

    framework = "pl"

    def __init__(self, flow_graph: FlowGraph):
        super().__init__(flow_graph)
        self.imports.add("import polars as pl")

    def _handle_random_split(
        self, settings: input_schema.NodeRandomSplit, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Inline a polars-equivalent random split that mirrors FlowDataEngine.random_split.

        The shuffled frame is materialised once so each split shares the same
        permutation; lengths use the FlowDataEngine offset accumulator (last
        split absorbs the remainder) so generated output equals flow output
        for the same seed.
        """
        input_df = input_vars.get("main", "df")
        node_id = settings.node_id
        if settings.seed is None:
            self.imports.add("import random")
            seed_expr = "random.randint(0, 2**31 - 1)"
        else:
            seed_expr = str(settings.seed)
        self._add_code(f"_split_{node_id}_seed = {seed_expr}")
        self._add_code(f"_split_{node_id}_shuffled = (")
        self._add_code(f"    {input_df}")
        self._add_code(
            f"    .with_columns(pl.int_range(0, pl.len()).shuffle(seed=_split_{node_id}_seed)"
            ".alias('__split_rank__'))"
        )
        self._add_code("    .sort('__split_rank__').drop('__split_rank__')")
        self._add_code("    .collect()")
        self._add_code(")")
        self._add_code(f"_split_{node_id}_total = _split_{node_id}_shuffled.height")
        self._add_code(f"_split_{node_id}_off = 0")
        splits = settings.splits
        for i, s in enumerate(splits):
            split_var = f"{var_name}_{s.name}"
            if i == len(splits) - 1:
                self._add_code(
                    f"{split_var} = _split_{node_id}_shuffled.slice("
                    f"_split_{node_id}_off, max(0, _split_{node_id}_total - _split_{node_id}_off)"
                    ").lazy()"
                )
            else:
                self._add_code(
                    f"_split_{node_id}_len = int(round(_split_{node_id}_total * {s.percentage} / 100.0))"
                )
                self._add_code(
                    f"{split_var} = _split_{node_id}_shuffled.slice("
                    f"_split_{node_id}_off, max(0, _split_{node_id}_len)"
                    ").lazy()"
                )
                self._add_code(f"_split_{node_id}_off += _split_{node_id}_len")
            self.node_handle_var_mapping[(node_id, f"output-{i}")] = split_var
        default_var = f"{var_name}_{splits[0].name}"
        self.node_var_mapping[node_id] = default_var
        self._add_code("")

    def _handle_catalog_reader(
        self, settings: input_schema.NodeCatalogReader, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Catalog Reader is not supported for standalone Polars code. Use FlowFrame export."""
        msg = (
            "Catalog SQL Reader requires FlowFrame code generation. " "Please use FlowFrame code generation instead."
            if settings.sql_query
            else "Catalog Reader requires a FlowFrame and is not supported by Polars code generation. "
            "Please use FlowFrame code generation instead."
        )
        self.unsupported_nodes.append((settings.node_id, "catalog_reader", msg))

    def _handle_catalog_writer(
        self, settings: input_schema.NodeCatalogWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Catalog Writer is not supported for standalone Polars code. Use FlowFrame export."""
        self.unsupported_nodes.append(
            (
                settings.node_id,
                "catalog_writer",
                "Catalog Writer requires a FlowFrame and is not supported by Polars code generation. "
                "Please use FlowFrame code generation instead.",
            )
        )

    def _handle_csv_read_non_utf8(self, file_settings: input_schema.ReceivedTable, var_name: str):
        self._add_code(f"{var_name} = {self.framework}.read_csv(")
        self._add_code(f"    {self._py_path(file_settings.abs_file_path)},")
        self._add_code(f'    separator="{file_settings.table_settings.delimiter}",')
        self._add_code(f"    has_header={file_settings.table_settings.has_headers},")
        self._add_code(f"    ignore_errors={file_settings.table_settings.ignore_errors},")
        if file_settings.table_settings.encoding:
            self._add_code(f'    encoding="{file_settings.table_settings.encoding}",')
        self._add_code(f"    skip_rows={file_settings.table_settings.starting_from_line},")
        self._add_code(").lazy()")

    def _handle_excel_read(self, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        self._add_code(f"{var_name} = {self.framework}.read_excel(")
        self._add_code(f"    {self._py_path(file_settings.abs_file_path)},")
        if file_settings.table_settings.sheet_name:
            self._add_code(f'    sheet_name="{file_settings.table_settings.sheet_name}",')
        self._add_code(").lazy()")

    def _handle_eager_read(self, file_type: str, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        source = self._py_path(file_settings.abs_file_path)
        self._add_code(f"{var_name} = {self._read_callable(file_type)}({source}).lazy()")

    def _build_final_code(self) -> str:
        """Build the final Python code with a performance note when flowfile is used."""
        code = super()._build_final_code()
        if "import flowfile as ff" in code:
            perf_note = (
                "# NOTE: This pipeline uses flowfile (ff) for some I/O operations (e.g. database, catalog).\n"
                "# For better performance, consider exporting as FlowFrame code instead of Polars.\n"
                "# FlowFrame keeps the entire pipeline lazy and avoids unnecessary data materialization.\n"
            )
            code = perf_note + code
        return code

    def _handle_output_excel(self, input_df: str, output_settings, node_id: int) -> None:
        """Emit the polars excel write, honouring the node's write mode instead of always overwriting.

        ``create`` means "refuse to touch an existing file", which stdlib expresses exactly, so it
        becomes an explicit guard and the export stays runnable. ``update`` needs an openpyxl
        read-modify-write round trip that has no polars equivalent, so it aborts the export rather
        than silently degrading into a destructive overwrite.
        """
        write_mode = resolve_excel_write_mode(output_settings.write_mode)
        if write_mode == "update":
            self.unsupported_nodes.append(
                (
                    node_id,
                    "output",
                    "Excel 'update' write mode cannot be expressed in standalone Polars code. "
                    "Export as FlowFrame code instead.",
                )
            )
            return
        path = output_settings.abs_file_path
        if write_mode == "create":
            self.imports.add("import os")
            reason = f"Cannot write '{path}': the file already exists (write mode 'create')."
            self._add_code(f"if os.path.exists({self._py_path(path)}):")
            self._add_code(f"    raise FileExistsError({self._py_str(reason)})")
        self._add_code(f"{input_df}.collect().write_excel(")
        self._add_code(f"    {self._py_path(path)},")
        self._add_code(f'    worksheet="{output_settings.table_settings.sheet_name}"')
        self._add_code(")")


class FlowGraphToFlowFrameConverter(NativeHandlersMixin, FlowGraphCodeConverter):
    """Generates FlowFrame code (``import flowfile as fl``) from a FlowGraph. Supports all node types including I/O.

    Gates, subflows, Python Scripts, flow ports and custom nodes emit as the frame's native classes,
    so a gate routes when a frame below it is collected. With ``placeholders=True`` a node the
    converter cannot express becomes ``<var> = fl.canvas_node(<id>, <inputs>)  # <type>: <reason>``
    (its reason on the emission) so downstream statements still bind; otherwise such a node fails
    the export with ``UnsupportedNodeError``. Every node is written as the frame call that adds that
    same node type back (``write_csv``, ``.polars_code``, ``with_row_index``, ...), with its user
    description as ``description=``. ``deterministic_names=True`` names an unnamed boundary
    ``<type_label>_<id>``, what a seeded notebook session binds.

    The notebook render (``placeholders=True``) keeps every cell inside what a notebook interprets
    without a kernel: a formula whose translation a cell does not interpret (a ``lambda`` for hashing,
    a clock read for ``now()``, a method outside the allowlist) keeps its formula text, and a formula
    entry kept as text always names its stored output type, so the frame never translates it again.
    """

    framework = "fl"
    flowfile_alias = "fl"

    def __init__(self, flow_graph: FlowGraph, placeholders: bool = False, deterministic_names: bool = False):
        super().__init__(flow_graph)
        self.placeholders = placeholders
        self.deterministic_names = deterministic_names
        self._blocked: set[int] = set()
        self._statuses: dict | None = None
        self.imports.add("import flowfile as fl")

    def _compute_gate_conditions(self, execution_plan) -> None:
        """Gates emit as ``fl.Gate``; the if-block machinery belongs to the Polars export."""

    def _render_body(self) -> list[str]:
        """The body, preceded by the ``flow`` graph that ``fl.FlowInput(flow_graph=flow)`` builds on."""
        body = super()._render_body()
        if any(f"flow_graph={FLOW_VAR}" in line for line in body):
            return [f"{FLOW_VAR} = fl.create_flow_graph()", "", *body]
        return body

    def _var_label(self, node: FlowNode) -> str:
        return _NATIVE_LABELS.get(node.node_type) or super()._var_label(node)

    def _plan_boundary_names(self, emissions, survivors: set[int], node_by_id: dict) -> dict[str, str]:
        if not self.deterministic_names:
            return super()._plan_boundary_names(emissions, survivors, node_by_id)
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

    def _description(self, node: FlowNode) -> str:
        """The user description a frame call carries; the native classes spell their own."""
        return "" if node.node_type in _NATIVE_TYPES else _user_description(node.setting_input)

    def _ends_statement(self, node: FlowNode) -> bool:
        """A flow output or a described node ends its statement, so ``description=`` lands on its own call."""
        return super()._ends_statement(node) or bool(self._description(node))

    def _describe(self, node: FlowNode) -> None:
        """Add ``description=`` to the call the node's span assigns, when it has one to carry."""
        description = self._description(node)
        if not description or getattr(node.setting_input, "is_user_defined", False):
            return
        if not self._node_spans or self._node_spans[-1][0] is not node:
            return
        _, var, start, end = self._node_spans[-1]
        described = _with_description("\n".join(self.code_lines[start:end]).rstrip(), description)
        if described is None:
            self.warnings.append(f"Node {node.node_id}: its description has no frame call to attach to")
            return
        self.code_lines[start:end] = [*described.split("\n"), ""]
        self._node_spans[-1] = (node, var, start, len(self.code_lines))

    def _producer_ids(self, node: FlowNode) -> list[int]:
        keyed = node.node_inputs.keyed_inputs or {}
        return self._raw_producer_ids(node) + [source.node_id for source in keyed.values() if source is not None]

    def _static_placeholder_reason(self, node: FlowNode) -> str | None:
        """Why ``node`` is a placeholder before its handler runs; unconfigured nodes block their downstream."""
        settings = node.setting_input
        upstream = [pid for pid in self._producer_ids(node) if pid in self._blocked]
        try:
            correct = node.is_correct
        except Exception:
            correct = False
        if self._statuses is None:
            try:
                self._statuses = classify_graph(list(self.flow_graph.nodes))
            except Exception:
                self._statuses = {}
        setup = node.node_information.is_setup or getattr(node.function, "__name__", None) != "placeholder"
        if not setup or isinstance(settings, input_schema.NodePromise):
            reason = "not configured yet"
        elif not correct:
            reason = "inputs not fully connected"
        elif upstream:
            reason = f"downstream of node {min(upstream)}, which is not editable as code"
        elif is_error_ish(self._statuses.get(node.node_id)):
            reason = "skipped on error by the engine"
        else:
            if node.node_type == "polars_lazy_frame":
                return "a Polars LazyFrame node cannot be rebuilt; replace it on the canvas"
            if node.node_type == "explore_data":
                return "explore data is interactive only"
            rest = getattr(settings, "rest_api_settings", None)
            keys = list((rest.headers or {}).keys()) + list((rest.query_params or {}).keys()) if rest else []
            if any(self._is_sensitive_key(k) for k in keys):
                return "headers or query parameters hold a credential"
            return None
        self._blocked.add(node.node_id)
        return reason

    def _generate_node_code(self, node: FlowNode) -> None:
        """With placeholders on, a node whose handler cannot express it rolls back and becomes a placeholder."""
        if not self.placeholders:
            super()._generate_node_code(node)
            return self._describe(node)
        reason = self._static_placeholder_reason(node)
        if reason is None:
            mark = (len(self.code_lines), len(self.unsupported_nodes), len(self._node_spans), len(self.output_nodes))
            saved = (set(self.imports), dict(self.node_handle_var_mapping), list(self._module_helpers))
            try:
                super()._generate_node_code(node)
            except Exception as exc:
                logger.warning("Code export: node %s (%s) failed: %s", node.node_id, node.node_type, exc)
                reason = f"could not render: {exc}"
            else:
                if len(self.unsupported_nodes) > mark[1]:
                    reason = self.unsupported_nodes[mark[1]][2]
                elif len(self._node_spans) == mark[2]:
                    reason = "renders no code"
            if reason is None:
                return self._describe(node)
            del self.code_lines[mark[0] :], self.unsupported_nodes[mark[1] :], self._node_spans[mark[2] :]
            del self.output_nodes[mark[3] :]
            self.imports, self.node_handle_var_mapping, self._module_helpers = saved
            self._passthrough.pop(node.node_id, None)
        var = getattr(node.setting_input, "node_reference", None) or f"df_{node.node_id}"
        args = [str(node.node_id), *self._get_input_vars(node).values()]
        self.node_var_mapping[node.node_id] = var
        outputs = getattr(node.setting_input, "output_names", None) or ["main"]
        if len(outputs) > 1:
            self._bind_outputs(node.node_id, var, [f'["output-{index}"]' for index in range(len(outputs))])
        comment = " ".join(f"{getattr(node.node_template, 'name', None) or node.node_type}: {reason}".split())
        self._placeholder_reasons[node.node_id] = reason
        self._node_spans.append((node, var, len(self.code_lines), len(self.code_lines) + 1))
        self._add_code(f"{var} = fl.canvas_node({', '.join(args)})  # {comment}")
        if node.node_template.output > 0:
            self.last_node_var = var

    def _custom_node_input_expr(self, input_var: str) -> str:
        """Bridge a FlowFrame input down to the polars LazyFrame ``process()`` expects."""
        return f"{input_var}.data"

    def _normalize_custom_output(self, out_expr: str) -> str:
        """Re-wrap ``process()``'s polars return as a FlowFrame for downstream fl.* ops."""
        return f"fl.FlowFrame({out_expr})"

    def _translate_to_ff_code(self, formula: str) -> str | None:
        """Translate a formula to native fl code, registering the imports the snippet needs.

        The validation namespace includes ``pl`` and ``datetime``, so generated
        snippets may reference them (e.g. ``today()`` translates to
        ``fl.lit(datetime.datetime.today())``); the emitted script must import
        whatever the snippet uses or it fails with NameError at runtime. Hashing
        functions reference ``hashlib`` from inside a ``map_elements`` lambda, whose
        body never runs during validation — so only the emitted import catches it.
        """
        if SENTINEL_PREFIX in formula:
            return None
        ff_code = _try_translate_to_ff_code(strip_outer_parens(formula))
        if ff_code:
            ff_code = _polars_code_to_flowframe(ff_code, modules=("ff",))
            if self.placeholders and not _interprets_without_a_kernel(ff_code):
                return None
            self._register_expr_stdlib_imports(ff_code)
            if re.search(r"\bpl\.", ff_code):
                self.imports.add("import polars as pl")
        return ff_code

    def _handle_random_split(
        self, settings: input_schema.NodeRandomSplit, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Delegate to FlowFrame.random_split, which already returns a tuple of frames."""
        input_df = input_vars.get("main", "df")
        node_id = settings.node_id
        split_vars = [f"{var_name}_{s.name}" for s in settings.splits]
        splits_arg = ", ".join(f'"{s.name}": {s.percentage}' for s in settings.splits)
        seed_arg = "" if settings.seed is None else f", seed={settings.seed}"
        self._add_code(
            f"{', '.join(split_vars)} = {input_df}.random_split({{{splits_arg}}}{seed_arg})"
        )
        for i, sv in enumerate(split_vars):
            self.node_handle_var_mapping[(node_id, f"output-{i}")] = sv
        self.node_var_mapping[node_id] = split_vars[0]
        self._add_code("")

    def _handle_sample(self, settings: input_schema.NodeSample, var_name: str, input_vars: dict[str, str]) -> None:
        """Delegate to FlowFrame.head/sample instead of the polars rank-filter form."""
        input_df = input_vars.get("main", "df")
        if settings.sample_method == "first":
            self._add_code(f"{var_name} = {input_df}.head({settings.sample_size})")
            self._add_code("")
            return
        seed_arg = "" if settings.seed is None else f", seed={settings.seed}"
        if settings.sample_method == "random_fraction":
            size_arg = f"fraction={settings.fraction / 100.0}"
        else:
            size_arg = str(max(0, settings.sample_size))
        self._add_code(f"{var_name} = {input_df}.sample({size_arg}{seed_arg})")
        self._add_code("")

    def _handle_manual_input(
        self, settings: input_schema.NodeManualInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        # Public API only: fl.from_raw_data coerces the dict into RawData via pydantic.
        raw_data = settings.raw_data_format
        self._add_code(f"{var_name} = fl.from_raw_data({raw_data.model_dump()})")
        self._add_code("")

    def _scan_callable(self, file_type: str) -> str:
        return self._frame_reader(f"scan_{file_type}")

    def _read_callable(self, file_type: str) -> str:
        return self._frame_reader(f"read_{file_type}")

    def _frame_reader(self, name: str) -> str:
        """``flowfile`` re-exports scan_csv/scan_parquet only; every other reader is imported
        from flowfile_frame instead of emitting an ``fl.`` attribute that does not exist."""
        if name in ("scan_csv", "scan_parquet"):
            return f"fl.{name}"
        self.imports.add(f"from flowfile_frame import {name}")
        return name

    def _emit_directory_read(self, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        """Emit the FlowFrame reader in directory mode so the export re-imports as the same node.

        The reader re-derives the glob from the source path, so no expansion is emitted here;
        that keeps the exported call editable and round-trippable, unlike a baked file list.
        """
        self._add_code(f"{var_name} = {self._scan_callable(file_settings.file_type)}(")
        self._add_code(f"    {self._py_str(self._directory_source_path(file_settings))},")
        self._add_code('    scan_mode="directory",')
        if file_settings.file_type == "csv":
            for kwarg_line in self._csv_scan_kwarg_lines(file_settings):
                self._add_code(kwarg_line)
        self._emit_include_file_paths(file_settings)
        self._add_code(")")

    @staticmethod
    def _directory_source_path(file_settings: input_schema.ReceivedTable) -> str:
        """The path the FlowFrame reader should re-resolve into a glob.

        The node's own path is preferred so a folder stays a folder in the export, but a
        relative one would resolve against the running script's cwd, so that falls back to
        the pattern the engine already resolved.
        """
        path = file_settings.path
        if path and os.path.isabs(os.path.expanduser(path)):
            return path
        return file_settings.abs_file_path

    def _handle_cloud_storage_reader(
        self, settings: input_schema.NodeCloudStorageReader, var_name: str, input_vars: dict[str, str]
    ):
        cs = settings.cloud_storage_settings
        self._add_code(f"{var_name} = fl.read_from_cloud_storage(")
        self._add_code(f'    "{cs.resource_path}",')
        self._add_code(f'    file_format="{cs.file_format}",')
        if cs.connection_name:
            self._add_code(f'    connection_name="{cs.connection_name}",')
        if cs.scan_mode and cs.scan_mode != "single_file":
            self._add_code(f'    scan_mode="{cs.scan_mode}",')
        if cs.file_format == "csv":
            csv = cs.with_csv_defaults()
            if csv.csv_delimiter != ";":
                self._add_code(f'    delimiter="{csv.csv_delimiter}",')
            if not csv.csv_has_header:
                self._add_code(f"    has_header={csv.csv_has_header},")
            if csv.csv_encoding != "utf8":
                self._add_code(f'    encoding="{csv.csv_encoding}",')
        if cs.file_format == "delta" and cs.delta_version is not None:
            self._add_code(f"    delta_version={cs.delta_version},")
        if cs.file_format == "delta":
            self._emit_change_feed_kwargs(cs)
        self._add_code(")")
        self._add_code("")

    def _handle_cloud_storage_writer(
        self, settings: input_schema.NodeCloudStorageWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        input_df = input_vars.get("main", "df")
        cs = settings.cloud_storage_settings
        self._add_code("fl.write_to_cloud_storage(")
        self._add_code(f"    {input_df},")
        self._add_code(f'    "{cs.resource_path}",')
        self._add_code(f'    file_format="{cs.file_format}",')
        if cs.connection_name:
            self._add_code(f'    connection_name="{cs.connection_name}",')
        if cs.file_format == "csv":
            if cs.csv_delimiter != ";":
                self._add_code(f'    delimiter="{cs.csv_delimiter}",')
            if cs.csv_encoding != "utf8":
                self._add_code(f'    encoding="{cs.csv_encoding}",')
        if cs.file_format == "parquet" and cs.parquet_compression != "snappy":
            self._add_code(f'    compression="{cs.parquet_compression}",')
        if cs.file_format == "delta" and cs.write_mode != "overwrite":
            self._add_code(f'    write_mode="{cs.write_mode}",')
        if cs.file_format == "delta" and cs.partition_by:
            self._add_code(f"    partition_by={cs.partition_by},")
        if cs.file_format == "delta" and cs.merge_keys:
            self._add_code(f"    merge_keys={cs.merge_keys},")
        if cs.file_format == "delta" and cs.track_changes:
            self._add_code("    track_changes=True,")
        self._add_code(")")
        self._add_code(f"{var_name} = {input_df}")
        self._add_code("")

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        """Filter nodes; an advanced filter that is one basic comparison renders as the basic one."""
        from flowfile_core.flowfile.share.filter_translation import translate_advanced_filter

        input_df = input_vars.get("main", "df")
        advanced = settings.filter_input.advanced_filter
        if not settings.split_mode and settings.filter_input.is_advanced() and SENTINEL_PREFIX not in advanced:
            basic = translate_advanced_filter(strip_outer_parens(advanced))
            if basic is not None:
                filter_input = transform_schema.FilterInput.model_validate(basic)
                settings = settings.model_copy(update={"filter_input": filter_input})

        if settings.split_mode:
            self._handle_filter_split(settings, var_name, input_df)
            return

        if settings.filter_input.is_advanced():
            ff_code = self._translate_to_ff_code(settings.filter_input.advanced_filter)
            if ff_code:
                self._add_code(f"{var_name} = {input_df}.filter({ff_code})")
            else:
                self._add_code(
                    f"{var_name} = {input_df}.filter(flowfile_formula={settings.filter_input.advanced_filter!r})"
                )
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                field_dtype = self._column_dtype(settings.node_id, basic.field)
                filter_expr = self._create_basic_filter_expr(basic, field_dtype)
                self._add_code(f"{var_name} = {input_df}.filter({filter_expr})")
            else:
                self._add_code(f"{var_name} = {input_df}  # No filter applied")
        self._add_code("")

    def _handle_filter_split(self, settings: input_schema.NodeFilter, var_name: str, input_df: str) -> None:
        """FlowFrame variant: delegate to FlowFrame.filter_split which already returns (pass, fail)."""
        node_id = settings.node_id
        pass_var = f"{var_name}_pass"
        fail_var = f"{var_name}_fail"
        if settings.filter_input.is_advanced():
            ff_code = self._translate_to_ff_code(settings.filter_input.advanced_filter)
            if ff_code:
                self._add_code(f"{pass_var}, {fail_var} = {input_df}.filter_split({ff_code})")
            else:
                self._add_code(
                    f"{pass_var}, {fail_var} = {input_df}.filter_split("
                    f"flowfile_formula={settings.filter_input.advanced_filter!r})"
                )
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                field_dtype = self._column_dtype(settings.node_id, basic.field)
                filter_expr = self._create_basic_filter_expr(basic, field_dtype)
                self._add_code(f"{pass_var}, {fail_var} = {input_df}.filter_split({filter_expr})")
            else:
                # No predicate -> mirror polars-variant fallback (pass keeps all, fail empty).
                self._add_code(
                    f'{pass_var}, {fail_var} = {input_df}.filter_split(flowfile_formula="True")'
                )
        self.node_handle_var_mapping[(node_id, "output-0")] = pass_var
        self.node_handle_var_mapping[(node_id, "output-1")] = fail_var
        self.node_var_mapping[node_id] = pass_var
        self._add_code("")

    def _handle_formula(self, settings: input_schema.NodeFormula, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle formula nodes, preferring native fl expressions over the flowfile_formulas parameter.

        Entries always emit one chained `with_columns` call each, in order, never the
        `flowfile_formulas=` list form for a whole node: a later entry may read a column an
        earlier one writes, and a chain says so where a keyword form whose evaluation is
        silently sequential does not. A single entry emits exactly the one-liner it always has;
        zero active entries is a pass-through assignment.
        """
        input_df = input_vars.get("main", "df")
        entries = [entry for _, entry in settings.active_entries()]
        if not entries:
            self._add_code(f"{var_name} = {input_df}")
        elif len(entries) == 1:
            self._add_code(f"{var_name} = {input_df}{self._formula_entry_call(entries[0])}")
        else:
            self._add_code(f"{var_name} = ({input_df}")
            for entry in entries:
                self._add_code(f"    {self._formula_entry_call(entry)}")
            self._add_code(")")
        self._add_code("")

    def _formula_entry_native_expr(self, entry: transform_schema.FunctionInput) -> str | None:
        """One untyped entry as a native fl expression; a typed one keeps the keyword form that stores its type."""
        if entry.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
            return None
        ff_code = self._translate_to_ff_code(entry.function)
        return f'({ff_code}).alias("{entry.field.name}")' if ff_code else None

    def _formula_entry_call(self, entry: transform_schema.FunctionInput) -> str:
        """The `.with_columns(...)` method-chain call one formula entry emits."""
        expr_str = self._formula_entry_native_expr(entry)
        if expr_str:
            return f".with_columns({expr_str})"
        formula = entry.function
        col_name = entry.field.name
        data_type = entry.field.data_type
        if data_type not in (None, transform_schema.AUTO_DATA_TYPE) or self.placeholders:
            return (
                f".with_columns(flowfile_formulas=[{repr(formula)}], output_column_names=[{repr(col_name)}], "
                f"output_column_datatypes=[{repr(data_type)}])"
            )
        return f".with_columns(flowfile_formulas=[{repr(formula)}], output_column_names=[{repr(col_name)}])"

    def _handle_graph_solver(self, settings: input_schema.NodeGraphSolver, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        gs = settings.graph_solver_input
        self._add_code(
            f'{var_name} = {input_df}.solve_graph("{gs.col_from}", "{gs.col_to}", '
            f'output_column_name="{gs.output_column_name}")'
        )
        self._add_code("")

    def _execute_join_with_post_processing(
        self,
        settings: input_schema.NodeJoin,
        var_name: str,
        left_df: str,
        right_df: str,
        left_on: list[str],
        right_on: list[str],
        after_join_drop_cols: list[str],
        reverse_action: dict | None,
    ) -> None:
        """FlowFrame override: use coalesce for right/outer joins instead of .collect()/.lazy().

        Passing coalesce explicitly routes FlowFrame through its Polars code path,
        giving the same join semantics as Polars without needing .collect()/.lazy()
        which FlowFrame doesn't support. FlowFrame's native join always drops right
        join keys, which is incorrect for right and outer joins.
        """
        if settings.join_input.how not in ("right", "outer"):
            super()._execute_join_with_post_processing(
                settings, var_name, left_df, right_df, left_on, right_on, after_join_drop_cols, reverse_action
            )
            return

        how = settings.join_input.how
        # coalesce=True for right joins (Polars default: drop left key, keep right key)
        # coalesce=False for outer joins (preserve both join keys for post-processing)
        coalesce = how == "right"

        has_post = bool(after_join_drop_cols) or bool(reverse_action)
        self._add_code(f"{var_name} = {'(' if has_post else ''}{left_df}.join(")
        self._add_code(f"        {right_df},")
        self._add_code(f"        left_on={left_on},")
        self._add_code(f"        right_on={right_on},")
        self._add_code(f'        how="{how}",')
        self._add_code(f"        coalesce={coalesce}")
        self._add_code("    )")

        if after_join_drop_cols:
            self._add_code(f".drop({after_join_drop_cols})")

        if reverse_action:
            self._add_code(f".rename({reverse_action})")

        if has_post:
            self._add_code(")")

    def _handle_kafka_source(
        self, settings: input_schema.NodeKafkaSource, var_name: str, input_vars: dict[str, str]
    ) -> None:
        ks = settings.kafka_settings

        if not ks.kafka_connection_name and not ks.kafka_connection_id:
            self.unsupported_nodes.append(
                (settings.node_id, "kafka_source", "Kafka Source node has no connection configured")
            )
            return

        if not ks.kafka_connection_name:
            self.unsupported_nodes.append(
                (
                    settings.node_id,
                    "kafka_source",
                    "Kafka Source node uses a connection ID instead of a name. "
                    "Please use a named connection for code export.",
                )
            )
            return

        self._add_code(f"# Read from Kafka topic: {ks.topic_name}")
        self._add_code(f"{var_name} = fl.read_kafka(")
        self._add_code(f'    "{ks.kafka_connection_name}",')
        self._add_code(f'    topic_name="{ks.topic_name}",')
        if ks.max_messages != 100_000:
            self._add_code(f"    max_messages={ks.max_messages},")
        if ks.start_offset != "latest":
            self._add_code(f'    start_offset="{ks.start_offset}",')
        if ks.poll_timeout_seconds != 30.0:
            self._add_code(f"    poll_timeout_seconds={ks.poll_timeout_seconds},")
        if ks.value_format != "json":
            self._add_code(f'    value_format="{ks.value_format}",')
        self._add_code(")")
        self._add_code("")

    def _handle_pivot_no_index(self, settings: input_schema.NodePivot, var_name: str, input_df: str, agg_func: str):
        pivot_input = settings.pivot_input
        self._add_code(f"{var_name} = ({input_df}")
        self._add_code(f'    .with_columns({self.framework}.lit(1).alias("_temp_index_"))')
        self._add_code("    .pivot(")
        self._add_code(f'        values="{pivot_input.value_col}",')
        self._add_code('        index=["_temp_index_"],')
        self._add_code(f'        on="{pivot_input.pivot_column}",')
        self._add_code(f'        aggregate_function="{agg_func}"')
        self._add_code("    )")
        self._add_code('    .drop("_temp_index_")')
        self._add_code(")")
        self._add_code("")

    def _handle_pivot(self, settings: input_schema.NodePivot, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle pivot nodes."""
        input_df = input_vars.get("main", "df")
        pivot_input = settings.pivot_input
        if len(pivot_input.aggregations) > 1:
            logger.error("Multiple aggregations are not convertable to polars code. " "Taking the first value")
        if len(pivot_input.aggregations) > 0:
            agg_func = pivot_input.aggregations[0]
        else:
            agg_func = "first"
        if len(settings.pivot_input.index_columns) == 0:
            self._handle_pivot_no_index(settings, var_name, input_df, agg_func)
        else:
            self._add_code(f"{var_name} = {input_df}.pivot(")
            self._add_code(f"    values='{pivot_input.value_col}',")
            self._add_code(f"    index={pivot_input.index_columns},")
            self._add_code(f"    on='{pivot_input.pivot_column}',")

            self._add_code(f"    aggregate_function='{agg_func}'")
            self._add_code(")")
            self._add_code("")

    def _handle_polars_code(
        self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """``<input>.polars_code(fn, *others)`` (``fl.polars_code(fn)`` without input) over the stored code verbatim.

        Code holding a parameter reference is passed as text instead, one literal per line, so the reference
        resolves in the text the rebuilt node stores rather than inside a function body it cannot see.
        """
        inputs = [input_vars[key] for key in sorted(input_vars) if key.startswith("main")]
        code = textwrap.dedent(settings.polars_code_input.polars_code).strip()
        names = [f"input_df_{i}" for i in range(1, len(inputs) + 1)] if len(inputs) > 1 else ["input_df"][: len(inputs)]
        function = f"_polars_code_{settings.node_id}"
        if SENTINEL_PREFIX in code:
            lines = _sql_query_literal_lines(code)
            lines[-1] += ","
            args = "".join(f"\n    {line}" for line in [*lines, *(f"{var}," for var in inputs[1:])])
            target = f"{inputs[0]}.polars_code" if inputs else "fl.polars_code"
            return self._add_statement(f"{var_name} = {target}({args}\n)")
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
        call = f"{inputs[0]}.polars_code({', '.join([function, *inputs[1:]])})" if inputs else None
        self._add_code(f"{var_name} = {call or f'fl.polars_code({function})'}")
        self._add_code("")

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
        self.last_node_var = var_name
        self._add_code("")

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

    def _handle_sql_query(self, settings: input_schema.NodeSqlQuery, var_name: str, input_vars: dict[str, str]) -> None:
        """A SQL Query node as ``fl.sql``: positional frames are ``input_1``, ``input_2``, ... in the stored query."""
        literals = _sql_query_literal_lines(settings.sql_query_input.sql_code)
        literals[-1] += ","
        self._add_code(f"{var_name} = fl.sql(")
        for line in [*literals, *(f"{var}," for var in _sql_query_input_vars(input_vars))]:
            self._add_code(f"    {line}")
        self._add_code(")")
        self._add_code("")

    def _handle_fuzzy_match(
        self, settings: input_schema.NodeFuzzyMatch, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle fuzzy match nodes using FlowFrame's native fuzzy_join method."""
        fuzzy_match_handler = transform_schema.FuzzyMatchInputManager(settings.join_input)
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))

        if left_df == right_df:
            right_df = "df_right"
            self._add_code(f"{right_df} = {left_df}")

        # Drop into node-local temps so a fanned-out upstream frame isn't rebound.
        if fuzzy_match_handler.left_select.has_drop_cols():
            left_drop_cols = [c.old_name for c in fuzzy_match_handler.left_select.non_jk_drop_columns]
            fuzzy_left = f"_fuzzy_left_{settings.node_id}"
            self._add_code(f"{fuzzy_left} = {left_df}.drop({left_drop_cols})")
            left_df = fuzzy_left
        if fuzzy_match_handler.right_select.has_drop_cols():
            right_drop_cols = [c.old_name for c in fuzzy_match_handler.right_select.non_jk_drop_columns]
            fuzzy_right = f"_fuzzy_right_{settings.node_id}"
            self._add_code(f"{fuzzy_right} = {right_df}.drop({right_drop_cols})")
            right_df = fuzzy_right

        fuzzy_join_mapping_settings = self._transform_fuzzy_mappings_to_string(
            fuzzy_match_handler.join_mapping, prefix="fl."
        )
        self._add_code(
            f"{var_name} = {left_df}.fuzzy_join(\n"
            f"       {right_df},\n"
            f"       fuzzy_mappings={fuzzy_join_mapping_settings}\n"
            f"       )"
        )

    def _handle_train_model(
        self, settings: input_schema.NodeTrainModel, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Train Model nodes — emit ``df.train_model(...)``."""
        input_df = input_vars.get("main", "df")
        s = settings.train_input
        args = [f"target={s.target_column!r}"]
        if s.feature_columns:
            args.append(f"features={s.feature_columns!r}")
        if s.model_type != "linear_regression":
            args.append(f"model_type={s.model_type!r}")
        if s.params:
            args.append(f"params={s.params!r}")
        if s.publish_to_catalog:
            args.append("publish_to_catalog=True")
            if s.model_name:
                args.append(f"model_name={s.model_name!r}")
            if s.namespace_id is not None:
                args.append(f"namespace_id={s.namespace_id}")
            if s.catalog_description:
                args.append(f"catalog_description={s.catalog_description!r}")
            if s.catalog_tags:
                args.append(f"catalog_tags={s.catalog_tags!r}")
        self._add_code(f"{var_name} = {input_df}.train_model({', '.join(args)})")
        self._add_code("")

    def _handle_apply_model(
        self, settings: input_schema.NodeApplyModel, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Apply Model nodes — emit ``df.apply_model(...)`` for both upstream and catalog modes."""
        input_df = input_vars.get("main", "df")
        s = settings.apply_input
        args: list[str] = []

        if s.source == "upstream":
            if s.upstream_node_id is None:
                self.unsupported_nodes.append(
                    (settings.node_id, "apply_model", "apply_model in upstream mode has no upstream_node_id")
                )
                return
            upstream_var = self.node_var_mapping.get(s.upstream_node_id)
            if upstream_var is None:
                self.unsupported_nodes.append(
                    (
                        settings.node_id,
                        "apply_model",
                        f"apply_model upstream_node_id={s.upstream_node_id} is not present in the exported graph",
                    )
                )
                return
            args.append(f"upstream={upstream_var}")
        else:
            if not s.model_name:
                self.unsupported_nodes.append(
                    (settings.node_id, "apply_model", "apply_model in catalog mode has no model_name configured")
                )
                return
            args.append(f"model_name={s.model_name!r}")
            if s.model_version is not None:
                args.append(f"version={s.model_version}")
            if s.namespace_id is not None:
                args.append(f"namespace_id={s.namespace_id}")

        if s.output_column != "prediction":
            args.append(f"output_column={s.output_column!r}")

        self._add_code(f"{var_name} = {input_df}.apply_model({', '.join(args)})")
        self._add_code("")

    def _handle_evaluate_model(
        self, settings: input_schema.NodeEvaluateModel, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Evaluate Model nodes — emit ``df.evaluate_model(...)``."""
        input_df = input_vars.get("main", "df")
        s = settings.evaluate_input
        args = [repr(s.actual_column)]
        if s.predicted_column != "prediction":
            args.append(f"predicted_column={s.predicted_column!r}")
        if s.task_type != "auto":
            args.append(f"task_type={s.task_type!r}")
        if s.upstream_train_node_id is not None:
            upstream_var = self.node_var_mapping.get(s.upstream_train_node_id)
            # Drop upstream silently when unresolvable: evaluate_model's task_type="auto"
            # falls back to "regression", so the export is degraded but still runs.
            # (Contrast _handle_apply_model, which marks unsupported — apply needs the model.)
            if upstream_var is not None:
                args.append(f"upstream={upstream_var}")
        self._add_code(f"{var_name} = {input_df}.evaluate_model({', '.join(args)})")
        self._add_code("")

    def _handle_wait_for(
        self, settings: input_schema.NodeWaitFor, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Wait For nodes — emit ``df.wait_for(dependency)``."""
        main_df = input_vars.get("main", "df")
        dep_df = input_vars.get("right")
        if dep_df is None:
            self.unsupported_nodes.append(
                (settings.node_id, "wait_for", "wait_for node has no dependency input wired to its right handle")
            )
            return
        self._add_code(f"{var_name} = {main_df}.wait_for({dep_df})")
        self._add_code("")

    def _handle_data_cleansing(
        self, settings: input_schema.NodeDataCleansing, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Data Cleansing nodes — emit ``df.data_cleansing(...)``.

        Every keyword is emitted explicitly rather than left to the method's defaults,
        so a non-default flow setting survives export even if a default ever changes.
        ``columns`` is omitted entirely for the "all columns" mode: passing an empty
        list would select nothing instead of everything.
        """
        input_df = input_vars.get("main", "df")
        cleansing = settings.cleansing_input
        args: list[str] = []
        if cleansing.selection_mode == "list":
            args.append(f"columns={cleansing.selected_columns!r}")
        args.extend(
            [
                f"remove_null_rows={cleansing.remove_null_rows}",
                f"remove_null_columns={cleansing.remove_null_columns}",
                f"replace_nulls_with_blank={cleansing.replace_nulls_with_blank}",
                f"replace_nulls_with_zero={cleansing.replace_nulls_with_zero}",
                f"trim_whitespace={cleansing.trim_whitespace}",
                f"normalize_whitespace={cleansing.normalize_whitespace}",
                f"remove_all_whitespace={cleansing.remove_all_whitespace}",
                f"remove_letters={cleansing.remove_letters}",
                f"remove_numbers={cleansing.remove_numbers}",
                f"remove_punctuation={cleansing.remove_punctuation}",
                f"case_mode={cleansing.case_mode!r}",
            ]
        )
        self._add_code(f"{var_name} = {input_df}.data_cleansing(")
        for arg in args:
            self._add_code(f"    {arg},")
        self._add_code(")")
        self._add_code("")

    def _handle_dynamic_rename(
        self, settings: input_schema.NodeDynamicRename, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Dynamic Rename nodes — emit ``df.dynamic_rename(...)``."""
        input_df = input_vars.get("main", "df")
        s = settings.dynamic_rename_input
        args = [f"mode={s.rename_mode!r}"]
        if s.prefix:
            args.append(f"prefix={s.prefix!r}")
        if s.suffix:
            args.append(f"suffix={s.suffix!r}")
        if s.formula:
            args.append(f"formula={s.formula!r}")
        if s.selection_mode == "list":
            args.append(f"columns={s.selected_columns!r}")
        elif s.selection_mode == "data_type" and s.selected_data_type is not None:
            args.append(f"data_type={s.selected_data_type!r}")
        self._add_code(f"{var_name} = {input_df}.dynamic_rename({', '.join(args)})")
        self._add_code("")

    def _handle_multi_field_formula(
        self, settings: input_schema.NodeMultiFieldFormula, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Multi-Field Formula nodes — emit ``df.multi_field_formula(...)``.

        The selection kwargs mirror the engine's target rules rather than the method's
        defaults: `data_type` mode with no data type selected picks no columns, so it has
        to render as an explicit empty `columns` list — omitting the kwarg would mean
        "all columns" and change what the exported script computes.

        A `"new"` node with neither affix is rejected outright: the FlowFrame API reads the
        absence of both as replace mode, so exporting it would silently overwrite the source
        columns the graph itself refuses to touch.
        """
        input_df = input_vars.get("main", "df")
        s = settings.multi_field_formula_input
        if s.output_mode == "new" and not (s.output_prefix or s.output_suffix):
            self.unsupported_nodes.append(
                (settings.node_id, "multi_field_formula", "writing to new columns requires a prefix or a suffix")
            )
            return
        args = [repr(s.formula)]
        if s.selection_mode == "list":
            args.append(f"columns={s.selected_columns!r}")
        elif s.selection_mode == "data_type":
            if s.selected_data_type is not None:
                args.append(f"data_type={s.selected_data_type!r}")
            else:
                args.append("columns=[]")
        if s.output_mode == "new":
            if s.output_prefix:
                args.append(f"prefix={s.output_prefix!r}")
            if s.output_suffix:
                args.append(f"suffix={s.output_suffix!r}")
        if s.output_data_type not in (None, transform_schema.AUTO_DATA_TYPE):
            args.append(f"output_data_type={str(s.output_data_type)!r}")
        self._add_code(f"{var_name} = {input_df}.multi_field_formula({', '.join(args)})")
        self._add_code("")


def export_flow_to_polars(flow_graph: FlowGraph) -> str:
    converter = FlowGraphToPolarsConverter(flow_graph)
    return converter.convert()


def export_flow_to_flowframe(flow_graph: FlowGraph) -> str:
    converter = FlowGraphToFlowFrameConverter(flow_graph)
    return converter.convert()
