from flowfile_core.flowfile._extensions.real_time_interface import get_realtime_func_results
from flowfile_core.flowfile.node_step.node_step import NodeStep
from flowfile_core.schemas.output_model import InstantFuncResult


def get_instant_func_results(node_step: NodeStep, func_string: str) -> InstantFuncResult:

    if len(node_step.main_input) == 0:
        return InstantFuncResult(result='No input data connected, so cannot evaluate the result', success=None)
    node_input = node_step.main_input[0]
    try:
        if node_input.node_stats.has_run and node_input.is_setup:
            df = node_input.results.example_data.data_frame
        else:
            df = node_input.get_predicted_resulting_data().data_frame.collect()
    except:
        pass
        return InstantFuncResult(result='Could not get data from previous step', success=None)
    try:
        real_time_result = get_realtime_func_results(df=df,
                                                     func_string=func_string)
        if node_step.name == 'filter' and not real_time_result.is_filterable_result():
            return InstantFuncResult(result='Result is not filterable,'
                                            ' make sure the function results in a true or false output', success=False)
        r = InstantFuncResult(result=real_time_result.readable_result, success=real_time_result.success)
    except Exception as e:
        r = InstantFuncResult(result=str(e), success=False)
    return r


