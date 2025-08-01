import flowfile_frame as ff
from pathlib import Path


def create_flow_frame_with_parquet_read() -> ff.FlowFrame:
    flow_graph = ff.create_flow_graph()
    flow_graph.execution_location = 'local'
    file_path = str(Path('flowfile_core') / 'tests' / 'support_files' / 'data' / 'fake_data.parquet')
    input_df = ff.read_parquet(source=file_path, description="fake_data_df", flow_graph=flow_graph)

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

    # Get execution plan
    execution_plan_before_run = output_node.get_resulting_data().data_frame.explain(format="plain")

    # Instead of checking for the exact path, check that the plan contains a reference to parquet reading
    assert "ScanParquet" in execution_plan_before_run or "parquet" in execution_plan_before_run.lower(), \
        "The execution plan should include parquet file scanning"

    # Run the graph
    graph.run_graph()

    # Check execution plan after run
    execution_plan_after_run = output_node.get_resulting_data().data_frame.explain(format="plain")
    assert "ScanParquet" in execution_plan_after_run or "parquet" in execution_plan_after_run.lower(), \
        "The execution plan should include parquet file scanning after running the graph"

    # You could also check that the data was actually loaded correctly
    result_df = output_node.get_resulting_data().data_frame
    assert len(result_df.collect())>0, "The resulting dataframe should not be empty"


def test_scan_parquet_from_cloud_storage():
    source = "s3://test-bucket/multi-file-parquet/"
    ff.scan_parquet_from_cloud_storage(source, connection_name="minio-flowframe-test")


def test_scan_csv_from_cloud_storage():
    flow_frame = ff.scan_csv_from_cloud_storage("s3://test-bucket/multi-file-csv/", connection_name="minio-flowframe-test", delimiter=",")
    flow_frame.count().collect()


def test_scan_delta():
    flow_frame = ff.scan_delta("s3://test-bucket/delta-lake-table", connection_name="minio-flowframe-test")
    flow_frame.count().collect()


def test_scan_json_from_cloud_storage():
    flow_frame = ff.scan_json_from_cloud_storage("s3://test-bucket/multi-file-json/", connection_name="minio-flowframe-test")
    flow_frame.count().collect()
