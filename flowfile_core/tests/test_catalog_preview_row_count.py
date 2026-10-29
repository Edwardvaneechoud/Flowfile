"""Catalog totals must describe the Delta snapshot, not cached metadata."""

from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from flowfile_core.catalog.services import previews
from flowfile_core.catalog.services.previews import TablePreviewService


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


@pytest.mark.parametrize("cloud_path", [False, True])
def test_unreadable_later_fragment_propagates_error(tmp_path, monkeypatch, cloud_path):
    path = tmp_path / "table"
    for i in range(40):
        write_deltalake(path, pa.table({"a": [i]}), mode="append")
    dt = DeltaTable(str(path))
    dataset = dt.to_pyarrow_dataset()
    victim = path / list(dataset.get_fragments())[-1].path
    hidden = victim.with_suffix(".unavailable")
    file_path = "s3://bucket/table" if cloud_path else str(path)
    opened = []

    def open_snapshot(table_path, storage_options=None):
        assert table_path == file_path
        assert storage_options is None
        opened.append(dt)
        return dt

    monkeypatch.setattr(previews, "DeltaTable", open_snapshot)
    if cloud_path:
        monkeypatch.setattr(previews, "resolve_for_namespace", lambda _: Mock(storage_options={}))
        monkeypatch.setattr(previews, "uses_worker", lambda: False)

    repo = Mock()
    repo.get_table.return_value = SimpleNamespace(
        file_path=file_path, row_count=999, namespace_id=1, table_type="physical"
    )
    victim.rename(hidden)
    try:
        assert dataset.head(1).num_rows == 1
        with pytest.raises(FileNotFoundError):
            TablePreviewService(repo, Mock(), Mock()).get_table_preview(1, limit=1)
        assert opened == [dt]
    finally:
        hidden.rename(victim)
