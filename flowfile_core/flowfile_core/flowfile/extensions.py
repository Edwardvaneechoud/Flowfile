from functools import lru_cache

import polars as pl

from flowfile_core.flowfile._extensions.real_time_interface import get_realtime_func_results
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.parameter_resolver import resolve_expression_parameters
from flowfile_core.schemas.output_model import InstantFuncResult
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


def get_instant_func_results(node_step: FlowNode, func_string: str) -> InstantFuncResult:
    control_source = _gate_control_source(node_step)
    if control_source is None and len(node_step.main_input) == 0:
        return InstantFuncResult(result="No input data connected, so cannot evaluate the result", success=None)
    # Resolve ${param} references so the preview matches what execution produces
    # (typed literals: strings quoted, numbers/bools bare). Unknown refs are left
    # as-is. Mirrors the expression-field substitution done at run time.
    params_getter = getattr(node_step, "_params_getter", None)
    if params_getter and "${" in func_string:
        func_string = resolve_expression_parameters(func_string, params_getter())
    if control_source is not None:
        node_input, source_handle = control_source
    else:
        node_input, source_handle = node_step.main_input[0], DEFAULT_OUTPUT_HANDLE
    try:
        df = _first_preview_row(node_input, source_handle)
    except Exception:
        return InstantFuncResult(result="Could not get data from previous step", success=None)
    try:
        real_time_result = get_realtime_func_results(df=df, func_string=func_string)
        if node_step.name == "filter" and not real_time_result.is_filterable_result():
            return InstantFuncResult(
                result="Result is not filterable," " make sure the function results in a true or false output",
                success=False,
            )
        r = InstantFuncResult(result=real_time_result.readable_result, success=real_time_result.success)
    except Exception as e:
        r = InstantFuncResult(result=str(e), success=False)
    return r
