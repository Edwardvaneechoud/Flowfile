from pathlib import Path

import polars as pl
from fastapi.testclient import TestClient

from flowfile_worker import main
from shared.storage_config import storage
from tests.utils import find_parent_directory


def test_catalog_materialize_xlsx(tmp_path, monkeypatch):
    storage._base_dir = tmp_path
    storage._user_data_dir = tmp_path
    storage._ensure_directories()

    client = TestClient(main.app)
    source_path = find_parent_directory("Flowfile") / "flowfile_core/tests/support_files/data/fake_data.xlsx"

    response = client.post(
        "/catalog/materialize",
        json={"source_file_path": str(source_path), "table_name": "test_table"},
    )

    assert response.status_code == 200, response.text
    data = response.json()

    dest_path = Path(data["parquet_path"])
    assert dest_path.exists()
    assert dest_path.parent == storage.catalog_tables_directory

    df = pl.read_excel(source_path)
    expected_schema = [{"name": col, "dtype": str(df[col].dtype)} for col in df.columns]

    assert data["schema"] == expected_schema
    assert data["row_count"] == df.height
    assert data["column_count"] == len(df.columns)
    assert data["size_bytes"] == dest_path.stat().st_size
