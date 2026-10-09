"""The local file readers refuse object-storage URIs instead of resolving them against the cwd."""

import pytest

import flowfile_frame as ff
from shared.path_utils import CloudPathNotSupportedError

CLOUD_PATHS = ["s3://bucket/sales.parquet", "gs://bucket/sales.parquet", "az://c/sales.parquet", "abfss://c@a/x"]


@pytest.mark.parametrize("path", CLOUD_PATHS)
@pytest.mark.parametrize("reader", [ff.read_parquet, ff.scan_parquet])
def test_parquet_readers_refuse_cloud_uris(reader, path):
    with pytest.raises(CloudPathNotSupportedError, match="ff.scan_parquet_from_cloud_storage") as exc:
        reader(path)
    assert path in str(exc.value)
    assert "No such file" not in str(exc.value)


@pytest.mark.parametrize("reader", [ff.read_csv, ff.scan_csv])
def test_csv_readers_refuse_cloud_uris_on_the_native_path(reader):
    with pytest.raises(CloudPathNotSupportedError, match="ff.scan_csv_from_cloud_storage"):
        reader("s3://bucket/sales.csv")


@pytest.mark.parametrize("reader", [ff.read_ipc, ff.read_ndjson, ff.read_avro, ff.read_ipc_stream, ff.read_excel])
def test_other_file_readers_refuse_cloud_uris(reader):
    with pytest.raises(CloudPathNotSupportedError, match="cloud storage location"):
        reader("s3://bucket/data.file")


def test_cloud_directory_uri_is_refused_too():
    with pytest.raises(CloudPathNotSupportedError):
        ff.read_parquet("s3://bucket/sales/")

