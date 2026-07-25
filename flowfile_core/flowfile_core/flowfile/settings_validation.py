"""Conservative static validation of node settings against predicted input schemas.

Flags settings that reference input columns which are no longer available (e.g. after an
upstream rename) so the frontend can warn on the node before the flow is run.

Invariants:
- Never executes data: only cached result schemas and the exec-free prediction ladder are read.
- Warns only on certainty: every "cannot tell" path yields no issue — silence over noise.
- Never mutates node settings.
- ``validate_flow_settings`` never raises; a node that cannot be analyzed is skipped.

Node types without a registered extractor (custom nodes, raw polars/SQL/python code, anything
new) are never analyzed. To cover a node type, register a small pure function with
``@_extractor`` that maps its settings model to the input columns it references, per input
handle. Flowfile formulas (formula node, advanced filter) are covered by parsing the
expression with polars_expr_transformer and collecting ``pl.col`` references from the parse
tree — unparseable expressions are skipped, never guessed at.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from flowfile_core.configs.flow_logger import main_logger
from flowfile_core.schemas import input_schema

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

InputHandle = Literal["main", "left", "right"]


class SettingsValidationIssue(BaseModel):
    input_handle: InputHandle
    missing_columns: list[str]
    message: str


class NodeSettingsValidation(BaseModel):
    node_id: int
    issues: list[SettingsValidationIssue]


class FlowSettingsValidation(BaseModel):
    enabled: bool
    nodes: list[NodeSettingsValidation]


@dataclass(frozen=True)
class ColumnReferences:
    """Input columns a node's settings reference, per input handle."""

    main: Sequence[str] = ()
    left: Sequence[str] = ()
    right: Sequence[str] = ()


def _collect_expression_columns(node: Any, out: list[str]) -> None:
    func_ref = getattr(node, "func_ref", None)
    args = getattr(node, "args", None) or []
    if getattr(func_ref, "val", None) == "pl.col" and len(args) == 1:
        val = getattr(args[0], "val", None)
        if isinstance(val, str) and len(val) >= 2 and val[0] == '"' and val[-1] == '"':
            out.append(val[1:-1])
            return
    for child in args:
        _collect_expression_columns(child, out)
    for condition_val in getattr(node, "conditions", None) or []:
        _collect_expression_columns(condition_val.condition, out)
        _collect_expression_columns(condition_val.val, out)
    else_val = getattr(node, "else_val", None)
    if else_val is not None:
        _collect_expression_columns(else_val, out)


def _expression_column_references(expression: str) -> list[str] | None:
    """Column names a flowfile formula references, or None when they cannot be determined."""
    if not expression or not expression.strip():
        return None
    try:
        from polars_expr_transformer.process.polars_expr_transformer import build_func

        tree = build_func(expression)
        refs: list[str] = []
        _collect_expression_columns(tree, refs)
        return refs
    except Exception:
        return None  # unparseable expressions never warn; the node surfaces its own errors


# Extractors return None when the configuration is not analyzable (unparseable expression,
# dtype-based selection, unconfigured sub-model) — the node is then skipped entirely.
_EXTRACTORS: dict[str, Callable[[Any], ColumnReferences | None]] = {}


def _extractor(node_type: str):
    def wrap(fn):
        _EXTRACTORS[node_type] = fn
        return fn

    return wrap


@_extractor("select")
def _select(settings: input_schema.NodeSelect) -> ColumnReferences:
    return ColumnReferences(main=[s.old_name for s in settings.select_input if s.keep or s.join_key])


@_extractor("filter")
def _filter(settings: input_schema.NodeFilter) -> ColumnReferences | None:
    fi = settings.filter_input
    if fi.mode == "basic":
        if fi.basic_filter is None:
            return None
        return ColumnReferences(main=[fi.basic_filter.field])
    refs = _expression_column_references(fi.advanced_filter)
    if refs is None:
        return None
    return ColumnReferences(main=refs)


@_extractor("formula")
def _formula(settings: input_schema.NodeFormula) -> ColumnReferences | None:
    if settings.function is None:
        return None
    refs = _expression_column_references(settings.function.function)
    if refs is None:
        return None
    return ColumnReferences(main=refs)


@_extractor("group_by")
def _group_by(settings: input_schema.NodeGroupBy) -> ColumnReferences | None:
    if settings.groupby_input is None:
        return None
    return ColumnReferences(main=[c.old_name for c in settings.groupby_input.agg_cols])


@_extractor("sort")
def _sort(settings: input_schema.NodeSort) -> ColumnReferences:
    return ColumnReferences(main=[s.column for s in settings.sort_input])


@_extractor("unique")
def _unique(settings: input_schema.NodeUnique) -> ColumnReferences:
    return ColumnReferences(main=settings.unique_input.columns or ())


@_extractor("record_id")
def _record_id(settings: input_schema.NodeRecordId) -> ColumnReferences:
    ri = settings.record_id_input
    if not ri.group_by:
        return ColumnReferences()
    return ColumnReferences(main=ri.group_by_columns or ())


@_extractor("text_to_rows")
def _text_to_rows(settings: input_schema.NodeTextToRows) -> ColumnReferences:
    t = settings.text_to_rows_input
    refs = [t.column_to_split]
    if not t.split_by_fixed_value and t.split_by_column:
        refs.append(t.split_by_column)
    return ColumnReferences(main=refs)


@_extractor("graph_solver")
def _graph_solver(settings: input_schema.NodeGraphSolver) -> ColumnReferences:
    g = settings.graph_solver_input
    return ColumnReferences(main=[g.col_from, g.col_to])


@_extractor("pivot")
def _pivot(settings: input_schema.NodePivot) -> ColumnReferences | None:
    p = settings.pivot_input
    if p is None:
        return None
    return ColumnReferences(main=[*p.index_columns, p.pivot_column, p.value_col])


@_extractor("unpivot")
def _unpivot(settings: input_schema.NodeUnpivot) -> ColumnReferences | None:
    u = settings.unpivot_input
    if u is None or u.data_type_selector_mode == "data_type":
        return None
    return ColumnReferences(main=[*u.index_columns, *u.value_columns])


@_extractor("join")
def _join(settings: input_schema.NodeJoin) -> ColumnReferences:
    # Only join keys warn; the left/right select lists are auto-reconciled by the
    # schema callbacks and would be noise.
    mapping = settings.join_input.join_mapping
    return ColumnReferences(
        left=[m.left_col for m in mapping if m.left_col],
        right=[m.right_col for m in mapping if m.right_col],
    )


@_extractor("fuzzy_match")
def _fuzzy_match(settings: input_schema.NodeFuzzyMatch) -> ColumnReferences:
    mapping = settings.join_input.join_mapping
    return ColumnReferences(
        left=[m.left_col for m in mapping if m.left_col],
        right=[m.right_col for m in mapping if m.right_col],
    )


@_extractor("window_functions")
def _window_functions(settings: input_schema.NodeWindowFunctions) -> ColumnReferences:
    w = settings.window_input
    refs = [*w.partition_by, *(s.column for s in w.order_by)]
    refs += [f.column for f in w.window_functions if f.column and f.function != "tile"]
    return ColumnReferences(main=refs)


@_extractor("train_model")
def _train_model(settings: input_schema.NodeTrainModel) -> ColumnReferences:
    t = settings.train_input
    return ColumnReferences(main=[t.target_column, *t.feature_columns])


@_extractor("evaluate_model")
def _evaluate_model(settings: input_schema.NodeEvaluateModel) -> ColumnReferences:
    e = settings.evaluate_input
    return ColumnReferences(main=[e.actual_column, e.predicted_column])


@_extractor("catalog_writer")
def _catalog_writer(settings: input_schema.NodeCatalogWriter) -> ColumnReferences:
    c = settings.catalog_write_settings
    return ColumnReferences(main=[*c.merge_keys, *c.partition_by])


@_extractor("run_flow")
def _run_flow(settings: input_schema.NodeRunFlow) -> ColumnReferences:
    refs = [b.column_name for b in settings.parameter_bindings if b.source == "column" and b.column_name]
    return ColumnReferences(main=refs)


@_extractor("dynamic_rename")
def _dynamic_rename(settings: input_schema.NodeDynamicRename) -> ColumnReferences | None:
    d = settings.dynamic_rename_input
    if d.selection_mode != "list":
        return None
    return ColumnReferences(main=d.selected_columns)


def _clean_refs(refs: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for ref in refs:
        if ref and ref not in seen:
            seen.add(ref)
            out.append(ref)
    return out


def _input_node(node: "FlowNode", handle: InputHandle) -> "FlowNode | None":
    inputs = node.node_inputs
    main_inputs = inputs.main_inputs or []
    if handle == "main":
        return main_inputs[0] if len(main_inputs) == 1 else None
    if handle == "left":
        # Join-family left connections land in main_inputs (see add_join's schema_callback).
        if inputs.left_input is not None:
            return inputs.left_input
        return main_inputs[0] if len(main_inputs) == 1 else None
    return inputs.right_input


def _resolved_schema(input_node: "FlowNode", allow_prediction: bool):
    """Return the input node's schema only when it is confidently known, else None."""
    try:
        if input_node.node_schema.result_schema:
            return input_node.node_schema.result_schema
        if not allow_prediction:
            return input_node.node_schema.predicted_schema or None
        schema = input_node.schema
    except Exception:
        return None
    if not schema:  # None and [] both mean "unknown": the schema-callback error path returns [].
        return None
    if input_node._schema_prediction_blocked:
        return None
    return schema


def _validate_node(node: "FlowNode", allow_prediction: bool) -> list[SettingsValidationIssue]:
    extractor = _EXTRACTORS.get(node.node_type)
    if extractor is None or not node.is_setup:
        return []
    refs = extractor(node.setting_input)
    if refs is None:
        return []
    issues: list[SettingsValidationIssue] = []
    for handle in ("main", "left", "right"):
        handle_refs = _clean_refs(getattr(refs, handle))
        if not handle_refs:
            continue
        input_node = _input_node(node, handle)
        if input_node is None:
            continue
        schema = _resolved_schema(input_node, allow_prediction)
        if schema is None:
            continue
        available = {c.name for c in schema}
        missing = [ref for ref in handle_refs if ref not in available]
        if missing:
            issues.append(
                SettingsValidationIssue(
                    input_handle=handle,
                    missing_columns=missing,
                    message=f"Column(s) not available from the {handle} input: {', '.join(missing)}",
                )
            )
    return issues


def validate_flow_settings(flow: "FlowGraph") -> FlowSettingsValidation:
    """Run the conservative settings check for every node in the flow."""
    if not flow.flow_settings.validate_settings:
        return FlowSettingsValidation(enabled=False, nodes=[])
    allow_prediction = not flow.flow_settings.is_running
    results: list[NodeSettingsValidation] = []
    for node in flow.nodes:
        try:
            issues = _validate_node(node, allow_prediction)
            if issues:
                results.append(NodeSettingsValidation(node_id=int(node.node_id), issues=issues))
        except Exception:
            main_logger.debug("Settings validation skipped node %s", node.node_id, exc_info=True)
    return FlowSettingsValidation(enabled=True, nodes=results)
