"""A designer Read node given an object-storage URI keeps it verbatim and fails the run with a pointer
to the cloud readers, instead of resolving it into ``<cwd>/s3:/bucket/...``. No reader may open it."""

from unittest import mock

import polars as pl
import pytest

from flowfile_core.schemas import input_schema
from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph


def _read_node(execution_location: str, path: str, file_type: str):
    graph = create_graph(execution_location=execution_location)
    add_node_promise_on_type(graph, "read", 1)
    received = input_schema.ReceivedTable(path=path, name=path.rsplit("/", 1)[-1], file_type=file_type)
    graph.add_read(input_schema.NodeRead(flow_id=1, node_id=1, received_file=received))
    return graph


def test_received_table_keeps_cloud_uri_verbatim():
    table = input_schema.ReceivedTable(path="s3://bucket/sales.parquet", name="sales.parquet", file_type="parquet")
    assert table.abs_file_path == "s3://bucket/sales.parquet"
    table.set_absolute_filepath()
    assert table.abs_file_path == "s3://bucket/sales.parquet"


@pytest.mark.parametrize(
    "execution_location, file_type, path",
    [
        ("local", "parquet", "s3://bucket/sales.parquet"),
        ("remote", "parquet", "s3://bucket/sales.parquet"),
        # excel runs through create_from_path_worker in remote mode
        ("remote", "excel", "s3://bucket/sales.xlsx"),
    ],
)
def test_read_node_with_cloud_uri_fails_with_actionable_error(execution_location, file_type, path):
    graph = _read_node(execution_location, path, file_type)

    run_info = graph.run_graph()

    assert run_info.success is False
    error = str(graph.get_node(1).results.errors)
    assert "not a local file path" in error
    assert "s3:/bucket" not in error.replace("s3://bucket", "")


@pytest.mark.parametrize("file_type, reader", [("ipc_stream", "read_ipc_stream"), ("avro", "read_avro")])
def test_eager_schema_probe_never_opens_a_cloud_uri(file_type, reader):
    """The avro/ipc_stream schema callback reads in core; it must not reach object storage."""
    calls = []
    real = getattr(pl, reader)

    def spy(source, *args, **kwargs):
        calls.append(source)
        return real(source, *args, **kwargs)

    with mock.patch.object(pl, reader, spy):
        graph = _read_node("remote", f"s3://bucket/data.{file_type}", file_type)
        assert graph.get_node(1).schema == []

    assert calls == []
