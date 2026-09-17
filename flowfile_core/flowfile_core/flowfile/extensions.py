from collections.abc import Sequence
from functools import lru_cache

import polars as pl

from flowfile_core.flowfile._extensions.real_time_interface import (
    ExpressionIssue,
    check_expression_chain,
    get_realtime_expr_results,
)
from flowfile_core.flowfile.flow_data_engine.formula_entries import (
    FormulaEntry,
    FormulaEntryError,
    apply_formula_entries,
    build_expression,
    classify_polars_error,
    formula_entry,
)
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.parameter_resolver import resolve_expression_parameters
from flowfile_core.schemas import transform_schema
from flowfile_core.schemas.output_model import (
    FormulaChainCheckResponse,
    FormulaChainColumn,
    FormulaChainEntryResult,
    FormulaChainIssue,
    FormulaChainSuggestion,
    InstantFuncResult,
)
from flowfile_core.utils.arrow_reader import read_top_n


@lru_cache(maxsize=16)
def get_first_row(arrow_path: str) -> pl.DataFrame:
    return pl.from_arrow(read_top_n(arrow_path, 1, strict=True))


def _gate_control_source(node_step: FlowNode) -> tuple[FlowNode, str] | None:
    """The gate's control input and its recorded source handle, when relevant.

    A gate routing on a formula evaluates it against the control input when one
    is connected (flow_graph._formula_gate_is_closed), so edit-time validation
    must resolve columns from the same frame. The source handle is looked up
    the way input assembly does (FlowNode._input_output_handles), so a control
    fed from e.g. a filter split's second output previews that partition.
    Returns None for every other node/configuration.
    """
    if node_step.node_type != "gate":
        return None
    gate_input = getattr(node_step.setting_input, "gate_input", None)
    if gate_input is None or gate_input.condition_source != "formula":
        return None
    control = node_step.node_inputs.right_input
    if control is None:
        return None
    return control, node_step._input_output_handles.get(control.node_id, DEFAULT_OUTPUT_HANDLE)


def _first_preview_row(node_input: FlowNode, source_handle: str) -> pl.DataFrame:
    """First row of an input's edit-time preview.

    Cached example data when the node has run (routed by handle for
    multi-output sources — memoized, so no re-execution), else the predicted
    schema-only frame. Never triggers a real execution of an un-run node.
    """
    has_current_result = node_input.node_stats.has_run_with_current_setup and node_input.is_setup
    if has_current_result and source_handle == DEFAULT_OUTPUT_HANDLE and node_input.results.example_data_path:
        return get_first_row(node_input.results.example_data_path)
    if has_current_result and source_handle != DEFAULT_OUTPUT_HANDLE:
        result = node_input.get_output(source_handle)
        if result is not None:
            frame = result.data_frame
            lazy_frame = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
            return lazy_frame.head(1).collect()
    return node_input.get_predicted_resulting_data(source_handle).data_frame.collect()


def _resolve_preview_frame(node_step: FlowNode) -> tuple[pl.DataFrame | None, InstantFuncResult | None]:
    """The single preview row an edit-time evaluation runs on, or the result explaining why not."""
    control_source = _gate_control_source(node_step)
    if control_source is None and len(node_step.main_input) == 0:
        return None, InstantFuncResult(result="No input data connected, so cannot evaluate the result", success=None)
    node_input, source_handle = control_source or (node_step.main_input[0], DEFAULT_OUTPUT_HANDLE)
    try:
        return _first_preview_row(node_input, source_handle), None
    except Exception:
        return None, InstantFuncResult(result="Could not get data from previous step", success=None)


def _resolve_params(node_step: FlowNode, func_string: str) -> str:
    """Substitute ``${param}`` references so the preview matches what execution produces.

    Typed literals: strings quoted, numbers/bools bare. Unknown refs are left as-is. Mirrors
    the expression-field substitution done at run time.
    """
    params_getter = getattr(node_step, "_params_getter", None)
    if params_getter and "${" in func_string:
        return resolve_expression_parameters(func_string, params_getter())
    return func_string


def evaluate_preview_entry(df: pl.DataFrame, entry: FormulaEntry, *, as_predicate: bool = False) -> InstantFuncResult:
    """Evaluate one entry on the preview row, describing a failure exactly as the chain check would.

    Parse, missing-column and type failures go through the same classifier as
    `apply_formula_entries`, so the preview strip and the row diagnostic never disagree. What
    remains the preview's own: a non-boolean result for a predicate, and data-dependent
    failures (a strict cast on real values) that a schema-only check cannot see.
    """
    try:
        real_time_result = get_realtime_expr_results(df, build_expression(entry))
    except FormulaEntryError as exc:
        return InstantFuncResult(result=exc.detail, success=False)
    except pl.exceptions.PolarsError as exc:
        return InstantFuncResult(result=classify_polars_error(entry, exc, df.columns).detail, success=False)
    except Exception:
        return InstantFuncResult(result="expression could not be evaluated", success=False)
    if as_predicate and not real_time_result.is_filterable_result():
        return InstantFuncResult(
            result="Result is not filterable, make sure the function results in a true or false output",
            success=False,
        )
    return InstantFuncResult(result=real_time_result.readable_result, success=real_time_result.success)


def get_instant_func_results(node_step: FlowNode, func_string: str) -> InstantFuncResult:
    df, failure = _resolve_preview_frame(node_step)
    if failure is not None:
        return failure
    entry = FormulaEntry(1, "", _resolve_params(node_step, func_string))
    return evaluate_preview_entry(df, entry, as_predicate=node_step.name == "filter")


def _resolved_entries(
    node_step: FlowNode, entries: Sequence[transform_schema.FunctionInput]
) -> list[transform_schema.FunctionInput]:
    """Copies of *entries* with their ``${param}`` references substituted; the originals stand."""
    return [e.model_copy(update={"function": _resolve_params(node_step, e.function or "")}) for e in entries]


def _chain_columns(schema: dict) -> list[FormulaChainColumn]:
    return [FormulaChainColumn(name=name, data_type=str(dtype)) for name, dtype in schema.items()]


def _chain_issue(issue: ExpressionIssue) -> FormulaChainIssue:
    suggestion = issue.suggestion
    return FormulaChainIssue(
        message=issue.message,
        kind=issue.kind,
        suggestion=None
        if suggestion is None
        else FormulaChainSuggestion(from_column=suggestion.from_name, to=suggestion.to_name),
    )


def get_formula_chain_check(
    node_step: FlowNode, entries: Sequence[transform_schema.FunctionInput]
) -> FormulaChainCheckResponse:
    """Validate the editor's formula entries against the schema each one will actually see.

    Reads no data. ``available`` is False when the node's main-input schema is not confidently
    known — the same "silence over guessing" rule the static settings validation follows.
    """
    from flowfile_core.flowfile.settings_validation import _main_input_polars_schema

    pl_schema = _main_input_polars_schema(node_step, True)
    if pl_schema is None:
        return FormulaChainCheckResponse(
            available=False, base_columns=[], entries=[FormulaChainEntryResult() for _ in entries]
        )
    result = check_expression_chain(pl_schema, _resolved_entries(node_step, entries))
    return FormulaChainCheckResponse(
        available=True,
        base_columns=_chain_columns(result.base_schema),
        entries=[
            FormulaChainEntryResult(
                issue=None if issue is None else _chain_issue(issue),
                columns=_chain_columns(schema),
            )
            for issue, schema in zip(result.issues, result.schemas, strict=True)
        ],
    )


def get_formula_chain_instant_result(
    node_step: FlowNode, entries: Sequence[transform_schema.FunctionInput], index: int
) -> InstantFuncResult:
    """Evaluate entry *index* on the preview row, with the entries above it applied first."""
    if index < 0 or index >= len(entries):
        return InstantFuncResult(result="No formula selected, so cannot evaluate the result", success=None)
    chain = _resolved_entries(node_step, entries)
    if not chain[index].function.strip():
        return InstantFuncResult(result="", success=None)  # a blank row is skipped, never evaluated
    df, failure = _resolve_preview_frame(node_step)
    if failure is not None:
        return failure
    prefix = [formula_entry(position, fn) for position, fn in enumerate(chain[:index], start=1) if fn.function.strip()]
    try:
        frame = apply_formula_entries(df.lazy(), prefix).collect()
    except FormulaEntryError as exc:
        return InstantFuncResult(result=str(exc), success=False)
    return evaluate_preview_entry(frame, formula_entry(index + 1, chain[index]))
