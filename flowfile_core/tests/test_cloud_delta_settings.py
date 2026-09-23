"""Settings validation for the cloud Delta writer's merge modes and the cloud reader's change feed."""

import pytest
from pydantic import ValidationError

from flowfile_core.flowfile.manage.compatibility_enhancements import ensure_compatibility_node_cloud_storage_reader
from flowfile_core.flowfile.settings_validation import _EXTRACTORS
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageReadSettings, CloudStorageWriteSettings

_PATH = "s3://bucket/orders"


def _write(**kwargs) -> CloudStorageWriteSettings:
    return CloudStorageWriteSettings(resource_path=_PATH, **{"file_format": "delta", **kwargs})


def _read(**kwargs) -> CloudStorageReadSettings:
    return CloudStorageReadSettings(resource_path=_PATH, **{"file_format": "delta", **kwargs})


def test_upsert_with_keys_is_valid_and_described():
    settings = _write(write_mode="upsert", merge_keys=["id"], track_changes=True)
    node = input_schema.NodeCloudStorageWriter(flow_id=1, node_id=1, cloud_storage_settings=settings)
    assert node.get_default_description() == f"Write to {_PATH} (delta, upsert)"
    assert _EXTRACTORS["cloud_storage_writer"](node).main == ["id"]


def test_upsert_without_merge_keys_is_rejected():
    with pytest.raises(ValidationError, match="merge_keys must be non-empty"):
        _write(write_mode="upsert")


def test_merge_mode_requires_delta():
    with pytest.raises(ValidationError, match="only supported for the 'delta' file format"):
        _write(file_format="parquet", write_mode="upsert", merge_keys=["id"])


def test_track_changes_rejected_with_overwrite():
    with pytest.raises(ValidationError, match="track_changes is not supported with write_mode 'overwrite'"):
        _write(write_mode="overwrite", track_changes=True)


def test_worker_interface_carries_merge_fields():
    worker = _write(write_mode="delete", merge_keys=["id", "region"], track_changes=True)
    interface = worker.get_write_setting_worker_interface()
    assert (interface.write_mode, interface.merge_keys, interface.track_changes) == ("delete", ["id", "region"], True)


def test_reader_since_version_requires_a_version():
    with pytest.raises(ValidationError, match="cdc_from_version is required"):
        _read(cdc_mode="since_version")
    reader = input_schema.NodeCloudStorageReader(
        flow_id=1, node_id=1, cloud_storage_settings=_read(cdc_mode="since_version", cdc_from_version=3)
    )
    assert reader.get_default_description().endswith("[changes since v3]")


def test_reader_change_modes_require_delta():
    with pytest.raises(ValidationError, match="only available for the 'delta' file format"):
        _read(file_format="csv", cdc_mode="since_timestamp", cdc_from_timestamp="2026-01-01T00:00:00Z")


def test_reader_change_modes_reject_a_pinned_version():
    with pytest.raises(ValidationError, match="pinned table version"):
        _read(delta_version=2, cdc_mode="since_version", cdc_from_version="${since}")


def test_reader_has_no_since_last_run():
    with pytest.raises(ValidationError, match="cdc_mode"):
        _read(cdc_mode="since_last_run")


def test_catalog_reader_keeps_its_own_rules():
    reader = input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=1, cdc_mode="since_last_run")
    assert reader.cdc_mode == "since_last_run"
    with pytest.raises(ValidationError, match="SQL catalog readers"):
        input_schema.NodeCatalogReader(flow_id=1, node_id=1, sql_query="select 1", cdc_mode="since_last_run")


def test_legacy_reader_pickle_gets_change_feed_defaults():
    node = input_schema.NodeCloudStorageReader(flow_id=1, node_id=1, cloud_storage_settings=_read())
    for name in ("cdc_mode", "cdc_from_version", "cdc_from_timestamp", "cdc_include_preimage"):
        del node.cloud_storage_settings.__dict__[name]
    ensure_compatibility_node_cloud_storage_reader(node)
    assert node.cloud_storage_settings.cdc_mode == "off"
    assert node.cloud_storage_settings.cdc_include_preimage is False
