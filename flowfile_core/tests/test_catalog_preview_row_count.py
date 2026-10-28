"""Catalog totals must describe the Delta snapshot, not cached metadata."""

from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from flowfile_core.catalog.services import previews
from flowfile_core.catalog.services.previews import TablePreviewService


@pytest.mark.parametrize("size, stored_count", [(0, 10), (20, 5000), (100, 10), (250, 10)])
@pytest.mark.parametrize("worker_fails", [False, True])
def test_latest_delta_preview_counts_snapshot(tmp_path, monkeypatch, size, stored_count, worker_fails):
    path = tmp_path / "table"
    write_deltalake(path, pa.table({"a": pa.array(range(size), type=pa.int64())}))
    file_path = str(path)

    if worker_fails:
        file_path = "s3://bucket/table"
        target = Mock(storage_options={})
        monkeypatch.setattr(previews, "resolve_for_namespace", lambda _: target)
        monkeypatch.setattr(previews, "_should_offload", lambda: True)

        def unavailable_worker(*args, **kwargs):
            raise RuntimeError("worker unavailable")

        def local_delta(table_path, storage_options=None):
            assert table_path == file_path
            assert storage_options is None
            return DeltaTable(str(path))

        monkeypatch.setattr(previews, "trigger_delta_preview", unavailable_worker)
        monkeypatch.setattr(previews, "DeltaTable", local_delta)

    table = SimpleNamespace(file_path=file_path, row_count=stored_count, namespace_id=1, table_type="physical")
    repo = Mock()
    repo.get_table.return_value = table
    result = TablePreviewService(repo, Mock(), Mock()).get_table_preview(1, limit=100)

    assert len(result.rows) == min(size, 100)
    assert result.total_rows == size


@pytest.mark.parametrize("worker_fails", [False, True])
def test_unreadable_later_fragment_propagates_error(tmp_path, monkeypatch, worker_fails):
    path = tmp_path / "table"
    for i in range(40):
        write_deltalake(path, pa.table({"a": [i]}), mode="append")
    dt = DeltaTable(str(path))
    dataset = dt.to_pyarrow_dataset()
    victim = path / list(dataset.get_fragments())[-1].path
    hidden = victim.with_suffix(".unavailable")
    file_path = "s3://bucket/table" if worker_fails else str(path)
    opened = []

    def open_snapshot(table_path, storage_options=None):
        assert table_path == file_path
        assert storage_options is None
        opened.append(dt)
        return dt

    monkeypatch.setattr(previews, "DeltaTable", open_snapshot)
    if worker_fails:
        monkeypatch.setattr(previews, "resolve_for_namespace", lambda _: Mock(storage_options={}))
        monkeypatch.setattr(previews, "_should_offload", lambda: True)
        monkeypatch.setattr(previews, "trigger_delta_preview", Mock(side_effect=RuntimeError("worker unavailable")))

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
