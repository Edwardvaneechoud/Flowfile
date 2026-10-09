"""A designer Read node given an object-storage URI keeps it verbatim and fails the run with a pointer
to the cloud readers, instead of resolving it into ``<cwd>/s3:/bucket/...``."""

from flowfile_core.schemas import input_schema
from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph


def test_received_table_keeps_cloud_uri_verbatim():
    table = input_schema.ReceivedTable(path="s3://bucket/sales.parquet", name="sales.parquet", file_type="parquet")
    assert table.abs_file_path == "s3://bucket/sales.parquet"
    table.set_absolute_filepath()
    assert table.abs_file_path == "s3://bucket/sales.parquet"


def test_read_node_with_cloud_uri_fails_with_actionable_error():
    graph = create_graph(execution_location="local")
    add_node_promise_on_type(graph, "read", 1)
    received = input_schema.ReceivedTable(path="s3://bucket/sales.parquet", name="sales.parquet", file_type="parquet")
    graph.add_read(input_schema.NodeRead(flow_id=1, node_id=1, received_file=received))

    run_info = graph.run_graph()

    assert run_info.success is False
    error = str(graph.get_node(1).results.errors)
    assert "cloud storage location" in error
    assert "Cloud Storage Reader" in error
    assert "s3:/bucket" not in error.replace("s3://bucket", "")
