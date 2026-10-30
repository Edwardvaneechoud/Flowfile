"""Catalog totals must describe the Delta snapshot, not cached metadata."""

from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from flowfile_core.catalog.services import previews
from flowfile_core.catalog.services.previews import TablePreviewService


@pytest.mark.parametrize("version", [None, 0])
def test_managed_preview_uses_worker_without_core_files(monkeypatch, version):
    from flowfile_core.schemas.catalog_schema import CatalogTablePreview
    from shared.storage_config import storage

    path = storage.catalog_tables_directory / "worker_only_orders"
    assert not path.exists()
    repo = Mock()
    repo.get_table.return_value = SimpleNamespace(file_path=str(path), table_type="physical", namespace_id=1)
    expected = CatalogTablePreview(columns=["id"], dtypes=["int64"], rows=[[1]], total_rows=220)
    worker = Mock(return_value=expected)
    monkeypatch.setattr(previews, "uses_worker", lambda: True)
    monkeypatch.setattr(previews.registry, "worker_url", lambda: "http://worker.test:63579")
    name = "trigger_delta_preview" if version is None else "trigger_delta_version_preview"
    monkeypatch.setattr(previews, name, worker)
    result = TablePreviewService(repo, Mock(), Mock()).get_table_preview(1, limit=1, version=version)
    assert result == expected
    args = (path.name, 1) if version is None else (path.name, version, 1)
    worker.assert_called_once_with(*args, storage=None, worker_url="http://worker.test:63579")


@pytest.mark.parametrize("size, stored_count", [(0, 10), (20, 5000), (100, 10), (250, 10)])
@pytest.mark.parametrize("cloud_path", [False, True])
def test_latest_delta_preview_counts_snapshot(tmp_path, monkeypatch, size, stored_count, cloud_path):
    path = tmp_path / "table"
    write_deltalake(path, pa.table({"a": pa.array(range(size), type=pa.int64())}))
    file_path = str(path)

    if cloud_path:
        file_path = "s3://bucket/table"
        target = Mock(storage_options={})
        monkeypatch.setattr(previews, "resolve_for_namespace", lambda _: target)
        monkeypatch.setattr(previews, "uses_worker", lambda: False)

        def local_delta(table_path, storage_options=None):
            assert table_path == file_path
            assert storage_options is None
            return DeltaTable(str(path))

        monkeypatch.setattr(previews, "DeltaTable", local_delta)

    table = SimpleNamespace(file_path=file_path, row_count=stored_count, namespace_id=1, table_type="physical")
    repo = Mock()
    repo.get_table.return_value = table
    result = TablePreviewService(repo, Mock(), Mock()).get_table_preview(1, limit=100)

    assert len(result.rows) == min(size, 100)
    assert result.total_rows == size
