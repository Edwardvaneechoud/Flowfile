"""Settings of the cloud storage reader and writer with ``file_format="iceberg"``: a table's name and its catalog."""

import pytest
from pydantic import ValidationError

from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageReadSettings,
    CloudStorageWriteSettings,
    IcebergCatalogSettings,
    WriteSettingsWorkerInterface,
)

CATALOG = IcebergCatalogSettings(uri="http://127.0.0.1:18181/catalog", warehouse="proof")


def _read(**kwargs) -> CloudStorageReadSettings:
    return CloudStorageReadSettings(**{"resource_path": "demo.orders", "file_format": "iceberg",
                                       "iceberg_catalog": CATALOG, **kwargs})


def _write(**kwargs) -> CloudStorageWriteSettings:
    return CloudStorageWriteSettings(**{"resource_path": "demo.orders", "file_format": "iceberg",
                                        "iceberg_catalog": CATALOG, **kwargs})


def test_an_iceberg_node_needs_its_catalog():
    for build in (_read, _write):
        with pytest.raises(ValidationError, match="reached through its catalog"):
            build(iceberg_catalog=None)


def test_another_format_refuses_a_catalog():
    with pytest.raises(ValidationError, match="only used with the 'iceberg' file format"):
        CloudStorageReadSettings(resource_path="s3://b/x.parquet", iceberg_catalog=CATALOG)
    with pytest.raises(ValidationError, match="only used with the 'iceberg' file format"):
        CloudStorageWriteSettings(resource_path="s3://b/x.parquet", iceberg_catalog=CATALOG)


def test_an_iceberg_node_refuses_a_storage_connection():
    for build in (_read, _write):
        with pytest.raises(ValidationError, match="storage keys come from its catalog"):
            build(connection_name="minio")


@pytest.mark.parametrize("name", ["orders", "s3://bucket/orders", "demo/orders", "demo. orders", ".orders"])
def test_an_iceberg_table_is_named_namespace_dot_table(name):
    with pytest.raises(ValidationError, match="named namespace.table"):
        _read(resource_path=name)


def test_a_parameterised_table_name_is_left_to_run_time():
    assert _read(resource_path="${schema}.orders").resource_path == "${schema}.orders"
    assert _read(resource_path="lake.demo.orders").resource_path == "lake.demo.orders"


def test_a_catalog_address_is_http_or_https():
    with pytest.raises(ValidationError, match="starts with http"):
        IcebergCatalogSettings(uri="thrift://127.0.0.1:9083")
    assert IcebergCatalogSettings(uri="${catalog}").uri == "${catalog}"


def test_an_iceberg_write_appends_or_overwrites_and_takes_the_tables_own_partitioning():
    assert _write(write_mode="append").write_mode == "append"
    with pytest.raises(ValidationError, match="own partitioning applies"):
        _write(partition_by=["grp"])
    for mode, kwargs in (("upsert", {"merge_keys": ["id"]}), ("error", {})):
        with pytest.raises(ValidationError, match="only supported for the 'delta' file format"):
            _write(write_mode=mode, **kwargs)
    with pytest.raises(ValidationError, match="only supported for the 'delta' file format"):
        _write(write_mode="append", track_changes=True)


def test_the_worker_write_settings_still_refuse_iceberg():
    """The cloud writer's worker interface does not move: an Iceberg write is an operation of its own."""
    with pytest.raises(ValidationError):
        WriteSettingsWorkerInterface(resource_path="s3://b/x", file_format="iceberg")


def test_a_node_of_another_format_dumps_no_catalog_key():
    reader = CloudStorageReadSettings(resource_path="s3://b/x.parquet")
    writer = CloudStorageWriteSettings(resource_path="s3://b/x.parquet")
    assert "iceberg_catalog" not in reader.model_dump()
    assert "iceberg_catalog" not in writer.model_dump()
    assert "iceberg_catalog" not in reader.model_dump_json()
    node = input_schema.NodeCloudStorageReader(flow_id=1, node_id=1, cloud_storage_settings=reader)
    assert "iceberg_catalog" not in node.model_dump()["cloud_storage_settings"]


def test_an_iceberg_node_round_trips_and_saves_only_the_secrets_name():
    settings = _read(iceberg_catalog=IcebergCatalogSettings(uri=CATALOG.uri, token_secret="lake-token"))
    node = input_schema.NodeCloudStorageReader(flow_id=1, node_id=1, cloud_storage_settings=settings)
    dumped = node.model_dump()
    assert dumped["cloud_storage_settings"]["iceberg_catalog"] == {
        "uri": CATALOG.uri, "warehouse": None, "token_secret": "lake-token"
    }
    assert input_schema.NodeCloudStorageReader.model_validate(dumped) == node
    writer = input_schema.NodeCloudStorageWriter(flow_id=1, node_id=2, cloud_storage_settings=_write())
    assert input_schema.NodeCloudStorageWriter.model_validate(writer.model_dump()) == writer
    assert node.get_default_description() == "Read demo.orders (iceberg)"
