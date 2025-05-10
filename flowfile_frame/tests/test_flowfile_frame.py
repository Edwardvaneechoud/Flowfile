import flowfile_frame as ff
from pathlib import Path
import os


def create_flow_frame_with_parquet_read() -> ff.FlowFrame:
    etl_graph = ff.create_etl_graph()
    etl_graph.execution_location = 'local'
    file_path = str(Path('flowfile_core') / 'tests' / 'support_files' / 'data' / 'fake_data.parquet')
    input_df = ff.read_parquet(file_path=file_path, description="fake_data_df", flow_graph=etl_graph)

    sorted_df = input_df.sort(by=ff.col('sales_data'), descending=True, description='cached_df').cache()
    filtered_df = sorted_df.filter(flowfile_formula='contains([Email],"@")', description='filter with flowfile formula')
    filtered_df2 = filtered_df.filter(ff.col("Email").str.contains('@'), description='Filter with polars code')

    data_with_formula_2 = filtered_df2.with_columns(flowfile_formulas=['now()'], output_column_names=['today'],
                                                    description='this is ff formula for the frontend')
    data_with_formula_2.write_csv('output_csv.csv', separator=';', description='output_csv')

    return data_with_formula_2


def test_read_from_parquet_in_performance():
    flow_frame = create_flow_frame_with_parquet_read()
    graph = flow_frame.flow_graph
    graph.flow_settings.execution_mode = 'Performance'
    output_node = graph.nodes[-1]  # get the output node
    execution_plan_before_run = output_node.get_resulting_data().data_frame.explain(format="plain")  # get the execution plan

    # Create a normalized path fragment that works across platforms
    expected_path_fragment = os.path.normpath("tests/support_files/data/fake_data.parquet")
    # For Windows paths, need to handle backslashes in string comparisons
    expected_path_fragment = expected_path_fragment.replace('\\', '\\\\')

    assert expected_path_fragment in execution_plan_before_run, f'The execution plan should contain the parquet file path. Expected path fragment: {expected_path_fragment}'

    graph.run_graph()
    execution_plan_after_run = output_node.get_resulting_data().data_frame.explain(format="plain")  # get the execution plan
    assert expected_path_fragment in execution_plan_after_run, f'The execution plan should contain the parquet file path. Expected path fragment: {expected_path_fragment}'