"""Real-file regressions for historical Delta and legacy Parquet previews."""

from unittest.mock import Mock

import polars as pl
import pytest

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import previews, tables
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def catalog():
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        service = CatalogService(repo)
        yield service
        for table in repo.list_tables():
            if table.name.startswith("preview_format_"):
                service.delete_table(table.id)


@pytest.mark.parametrize("worker_selected", [False, True])
@pytest.mark.parametrize("version,row_count", [(0, 7), (1, 11)])
def test_historical_preview_counts_selected_snapshot(
    catalog, tmp_path, monkeypatch, worker_selected, version, row_count
):
    path = tmp_path / "history"
    for snapshot, count in enumerate([7, 11, 3]):
        pl.DataFrame({"snapshot": [snapshot] * count}).write_delta(str(path), mode="overwrite")
    table = catalog.register_table_from_data(
        name="preview_format_history",
        table_path=str(path),
        owner_id=1,
        storage_format="delta",
        schema=[{"name": "snapshot", "dtype": "Int64"}],
        row_count=999,
        size_bytes=0,
    )
    monkeypatch.setattr(previews, "uses_worker", lambda: worker_selected)
    worker = Mock(side_effect=AssertionError("External local paths must not be sent to the worker"))
    monkeypatch.setattr(previews, "trigger_delta_version_preview", worker)

    preview = catalog.get_table_preview(table.id, limit=2, version=version)

    assert preview.columns == ["snapshot"]
    assert preview.rows == [[version], [version]]
    assert preview.total_rows == row_count
    worker.assert_not_called()


@pytest.mark.parametrize("row_count", [0, 7])
@pytest.mark.parametrize("limit", [0, 2, 20])
def test_registered_parquet_preview_reads_current_file(catalog, tmp_path, monkeypatch, row_count, limit):
    path = tmp_path / "legacy.parquet"
    pl.DataFrame({"value": [1, 2, 3]}).write_parquet(path)
    monkeypatch.setattr(tables, "uses_worker", lambda: False)
    table = catalog.register_table_from_parquet(name="preview_format_parquet", parquet_path=str(path), owner_id=1)
    assert catalog.repo.get_table(table.id).storage_format == "parquet"
    assert table.row_count == 3
    pl.DataFrame({"value": pl.Series(range(row_count), dtype=pl.Int64)}).write_parquet(path)

    preview = catalog.get_table_preview(table.id, limit=limit)

    assert preview.columns == ["value"]
    assert preview.dtypes == ["int64"]
    assert preview.rows == [[value] for value in range(min(limit, row_count))]
    assert preview.total_rows == row_count
