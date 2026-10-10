"""The local file readers refuse object-storage URIs instead of resolving them against the cwd."""

from pathlib import Path

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_core.schemas import input_schema
from shared.path_utils import CloudPathNotSupportedError

CLOUD_PATHS = ["s3://bucket/sales.parquet", "S3://bucket/sales.parquet", "gs://bucket/sales.parquet", "abfss://c@a/x"]


@pytest.mark.parametrize("path", CLOUD_PATHS)
@pytest.mark.parametrize("reader", [ff.read_parquet, ff.scan_parquet])
def test_parquet_readers_refuse_cloud_uris(reader, path):
    with pytest.raises(CloudPathNotSupportedError, match="ff.scan_parquet_from_cloud_storage") as exc:
        reader(path)
    assert path in str(exc.value)
    assert "No such file" not in str(exc.value)


def test_collapsed_pathlib_uri_is_refused():
    with pytest.raises(CloudPathNotSupportedError):
        ff.read_parquet(Path("s3://bucket/sales.parquet"))


@pytest.mark.parametrize("reader", [ff.read_csv, ff.scan_csv])
def test_csv_readers_refuse_cloud_uris_on_the_native_path(reader):
    with pytest.raises(CloudPathNotSupportedError, match="ff.scan_csv_from_cloud_storage"):
        reader("s3://bucket/sales.csv")


def test_csv_refusal_does_not_depend_on_parsing_options():
    with pytest.raises(CloudPathNotSupportedError, match="ff.scan_csv_from_cloud_storage"):
        ff.read_csv("s3://bucket/sales.csv", null_values=["NA"])


def test_csv_list_with_a_cloud_uri_after_a_local_path_is_refused(tmp_path):
    local = tmp_path / "a.csv"
    pl.DataFrame({"a": [1]}).write_csv(local)
    with pytest.raises(CloudPathNotSupportedError):
        ff.read_csv([str(local), "s3://bucket/b.csv"])


def test_csv_list_reads_every_file(tmp_path):
    pl.DataFrame({"a": [1]}).write_csv(tmp_path / "a.csv")
    pl.DataFrame({"a": [2]}).write_csv(tmp_path / "b.csv")
    result = ff.read_csv([str(tmp_path / "a.csv"), str(tmp_path / "b.csv")]).collect()
    assert sorted(result["a"].to_list()) == [1, 2]


def test_ndjson_reader_points_at_the_json_cloud_reader():
    with pytest.raises(CloudPathNotSupportedError, match="ff.scan_json_from_cloud_storage"):
        ff.read_ndjson("s3://bucket/data.ndjson")


@pytest.mark.parametrize("reader", [ff.read_ipc, ff.read_avro, ff.read_ipc_stream, ff.read_excel])
def test_readers_without_a_cloud_twin_say_so(reader):
    with pytest.raises(CloudPathNotSupportedError, match="No cloud storage reader supports"):
        reader("s3://bucket/data.file")


def test_cloud_directory_uri_is_refused_too():
    with pytest.raises(CloudPathNotSupportedError):
        ff.read_parquet("s3://bucket/sales/")


def test_notebook_push_keeps_a_cloud_read_node_as_written():
    """A pushed notebook must round-trip a flow holding such a node; core refuses the path when it runs."""
    token = input_schema.keep_paths_as_written.set(True)
    try:
        frame = ff.scan_parquet("s3://bucket/sales.parquet")
    finally:
        input_schema.keep_paths_as_written.reset(token)
    received = frame.flow_graph.get_node(frame.node_id).setting_input.received_file
    assert received.path == "s3://bucket/sales.parquet"
    assert received.abs_file_path == "s3://bucket/sales.parquet"
