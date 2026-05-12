"""Tests for the flow-virtual-table resolve primitive on the worker."""
from __future__ import annotations

from base64 import b64encode

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
    }

    r = client.post("/flow/resolve_virtual_table", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["row_count"] == 3
    assert body["mtime"] > 0
    assert body["ipc_path"] == "fvt-7.arrow"

    target = storage.catalog_virtual_results_directory / body["ipc_path"]
    assert target.exists()
    df = pl.read_ipc(target)
    assert df.shape == (3, 2)
    assert df["a"].to_list() == [1, 2, 3]


@pytest.mark.worker
def test_resolve_virtual_table_overwrites_on_repeat_call():
    """Repeat calls re-execute the plan and overwrite the per-table IPC file."""
    client = TestClient(main.app)
    payload = {
        "table_id": 11,
        "plan_bytes": b64encode(_plan_bytes()).decode("ascii"),
    }

    r1 = client.post("/flow/resolve_virtual_table", json=payload)
    assert r1.status_code == 200, r1.text
    first = r1.json()

    r2 = client.post("/flow/resolve_virtual_table", json=payload)
    assert r2.status_code == 200, r2.text
    second = r2.json()

    assert second["ipc_path"] == first["ipc_path"]
    assert second["row_count"] == first["row_count"]


@pytest.mark.worker
def test_resolve_virtual_table_invalid_plan_returns_500():
    client = TestClient(main.app)
    payload = {
        "table_id": 99,
        "plan_bytes": b64encode(b"not a valid polars plan").decode("ascii"),
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
    )
    res = funcs.resolve_virtual_table(req)
    target = storage.catalog_virtual_results_directory / res.ipc_path
    assert target.exists()
    # Must NOT live in the catalog_tables_directory (separation of concerns).
    assert not (storage.catalog_tables_directory / res.ipc_path).exists()
