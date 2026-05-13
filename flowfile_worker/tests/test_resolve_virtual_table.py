"""Tests for the flow-virtual-table resolve primitive on the worker."""
from __future__ import annotations

from base64 import b64encode
from unittest.mock import patch

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import funcs, main, models
from shared.storage_config import storage


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path):
    old_base, old_user = storage._base_dir, storage._user_data_dir
    storage._base_dir = tmp_path
    storage._user_data_dir = tmp_path
    storage._ensure_directories()
    yield
    storage._base_dir = old_base
    storage._user_data_dir = old_user


def _plan_bytes() -> bytes:
    lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    return lf.serialize()


@pytest.mark.worker
def test_resolve_virtual_table_round_trip():
    client = TestClient(main.app)
    payload = {
        "table_id": 7,
        "plan_bytes": b64encode(_plan_bytes()).decode("ascii"),
        "source_versions_hash": "deadbeef" * 4,
    }

    r = client.post("/flow/resolve_virtual_table", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["row_count"] == 3
    assert body["mtime"] > 0
    assert body["ipc_path"].startswith("fvt-7-")
    assert body["ipc_path"].endswith(".arrow")

    target = storage.catalog_virtual_results_directory / body["ipc_path"]
    assert target.exists()
    df = pl.read_ipc(target)
    assert df.shape == (3, 2)
    assert df["a"].to_list() == [1, 2, 3]


@pytest.mark.worker
def test_resolve_virtual_table_is_idempotent_on_versions_hash():
    """Second call with the same (table_id, source_versions_hash) is a cache hit.

    The cache file's mtime must not change and the spawn helper must not run.
    """
    client = TestClient(main.app)
    payload = {
        "table_id": 11,
        "plan_bytes": b64encode(_plan_bytes()).decode("ascii"),
        "source_versions_hash": "cafebabe" * 4,
    }

    r1 = client.post("/flow/resolve_virtual_table", json=payload)
    assert r1.status_code == 200, r1.text
    first = r1.json()
    target = storage.catalog_virtual_results_directory / first["ipc_path"]
    assert target.exists()

    with patch("flowfile_worker.funcs.mp_context") as mock_mp:
        r2 = client.post("/flow/resolve_virtual_table", json=payload)
        assert mock_mp.Process.call_count == 0
        assert mock_mp.Queue.call_count == 0

    assert r2.status_code == 200, r2.text
    second = r2.json()
    assert second["ipc_path"] == first["ipc_path"]
    assert second["mtime"] == first["mtime"]
    assert second["row_count"] == first["row_count"]


@pytest.mark.worker
def test_resolve_virtual_table_invalid_plan_returns_500():
    client = TestClient(main.app)
    payload = {
        "table_id": 99,
        "plan_bytes": b64encode(b"not a valid polars plan").decode("ascii"),
        "source_versions_hash": "facade01" * 4,
    }
    r = client.post("/flow/resolve_virtual_table", json=payload)
    assert r.status_code == 500
    assert "resolve_virtual_table" in r.text or "child" in r.text


@pytest.mark.worker
def test_resolve_virtual_table_function_path_normalises_under_results_dir():
    """The handler writes inside the dedicated catalog_virtual_results dir, not the tables dir."""
    req = models.ResolveVirtualTableRequest(
        table_id=42,
        plan_bytes=_plan_bytes(),
        source_versions_hash="abcdef01" * 4,
    )
    res = funcs.resolve_virtual_table(req)
    target = storage.catalog_virtual_results_directory / res.ipc_path
    assert target.exists()
    # Must NOT live in the catalog_tables_directory (separation of concerns).
    assert not (storage.catalog_tables_directory / res.ipc_path).exists()
