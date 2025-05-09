import flowfile_frame as ff
from flowfile_frame.utils import open_graph_in_editor


def create_flow_frame_with_parquet_read() -> ff.FlowFrame:
    etl_graph = ff.create_etl_graph()
    etl_graph.execution_location = 'local'
    input_df = (ff.read_parquet(file_path='flowfile_core/tests/support_files/data/fake_data.parquet',
                                description='fake_data_df', flow_graph=etl_graph))

    sorted_df = input_df.sort(by=ff.col('sales_data'), descending=True, description='cached_df').cache()
    filtered_df = sorted_df.filter(flowfile_formula='contains([Email],"@")', description='filter with flowfile formula')
    filtered_df2 = filtered_df.filter(ff.col("Email").str.contains('@'), description='Filter with polars code')

    data_with_formula_2 = filtered_df2.with_columns(flowfile_formulas=['now()'], output_column_names=['today'],
                                                    description='this is ff formula for the frontend')
    data_with_formula_2.write_csv('output_csv.csv', separator=';', description='output_csv')

    return data_with_formula_2


def test_read_from_parquet_in_performance():
    # ensure there is a good flowframe:
    flow_frame = create_flow_frame_with_parquet_read()
    graph = flow_frame.flow_graph
    graph.flow_settings.execution_mode = 'Performance'
    output_node = graph.nodes[-1]  # get the output node
    execution_plan_before_run = output_node.get_resulting_data().data_frame.explain(format="plain")  # get the execution plan
    assert "tests/support_files/data/fake_data.parquet" in execution_plan_before_run, 'The execution plan should contain the parquet file path'
    graph.run_graph()
    execution_plan_after_run = output_node.get_resulting_data().data_frame.explain(format="plain")  # get the execution plan
    assert "tests/support_files/data/fake_data.parquet" in execution_plan_after_run, 'The execution plan should contain the parquet file path'
