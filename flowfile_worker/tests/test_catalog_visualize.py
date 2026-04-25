"""Tests for the catalog visualization compute path on the worker."""
from __future__ import annotations

import time

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main
from flowfile_worker.viz_sessions import VizSessionManager, viz_session_manager
from shared.storage_config import storage


def _setup_storage(tmp_path):
    storage._base_dir = tmp_path
    storage._user_data_dir = tmp_path
    storage._ensure_directories()


def _write_delta_table(tmp_path, name: str = "viz_test") -> str:
    """Write a small Delta table under the catalog tables directory."""
    df = pl.DataFrame(
        {
            "category": ["a", "b", "a", "c", "b"],
            "value": [1, 2, 3, 4, 5],
        }
    )
    target = storage.catalog_tables_directory / name
    target.mkdir(parents=True, exist_ok=True)
    df.write_delta(str(target))
    return name


def test_viz_session_manager_caches_loader_calls():
    mgr = VizSessionManager()
    calls = {"n": 0}

    def loader() -> pl.LazyFrame:
        calls["n"] += 1
        return pl.LazyFrame({"x": [1, 2, 3]})

    _, hit_first = mgr.execute("k1", loader, lambda lf: lf.collect().to_dicts())
    _, hit_second = mgr.execute("k1", loader, lambda lf: lf.collect().to_dicts())

    assert calls["n"] == 1
    assert hit_first is False
    assert hit_second is True


def test_viz_session_manager_evicts_idle_sessions():
    mgr = VizSessionManager()
    mgr.IDLE_TTL_SECONDS = 0  # type: ignore[misc]
    mgr.REAP_INTERVAL_SECONDS = 0  # type: ignore[misc]

    mgr.execute("k1", lambda: pl.LazyFrame({"x": [1]}), lambda lf: lf.collect().to_dicts())
    assert "k1" in mgr.stats()["keys"]

    # Manually drive eviction without waiting for the reap thread.
    cutoff = time.time() - mgr.IDLE_TTL_SECONDS
    with mgr._lock:
        stale = [k for k, s in mgr._sessions.items() if s.last_used_at < cutoff]
        for k in stale:
            mgr._sessions.pop(k, None)
    assert "k1" not in mgr.stats()["keys"]


@pytest.mark.worker
def test_visualize_query_endpoint_physical_delta(tmp_path, monkeypatch):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_manager.evict_all()

    client = TestClient(main.app)

    # Aggregate: sum(value) by category — uses polars-gw view+aggregate path.
    payload = {
        "source": {
            "kind": "physical",
            "session_key": "test:phys:1",
            "table_path": table_dir,
            "storage_format": "delta",
        },
        "payload": {
            "workflow": [
                {
                    "type": "view",
                    "query": [
                        {
                            "op": "aggregate",
                            "groupBy": ["category"],
                            "measures": [
                                {"field": "value", "agg": "sum", "asFieldKey": "value_sum"}
                            ],
                        }
                    ],
                },
            ],
        },
    }

    r1 = client.post("/catalog/visualize_query", json=payload)
    assert r1.status_code == 200, r1.text
    body1 = r1.json()
    assert body1["error"] is None
    assert body1["cache_hit"] is False
    assert body1["total_rows"] == 3
    by_cat = {row["category"]: row["value_sum"] for row in body1["rows"]}
    assert by_cat == {"a": 4, "b": 7, "c": 4}

    # Second call with the same session_key should hit the cache.
    r2 = client.post("/catalog/visualize_query", json=payload)
    assert r2.status_code == 200, r2.text
    assert r2.json()["cache_hit"] is True


@pytest.mark.worker
def test_visualize_fields_endpoint_returns_imutfields(tmp_path, monkeypatch):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_manager.evict_all()

    client = TestClient(main.app)
    body = {
        "source": {
            "kind": "physical",
            "session_key": "test:fields:1",
            "table_path": table_dir,
            "storage_format": "delta",
        }
    }
    r = client.post("/catalog/visualize_fields", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["error"] is None
    field_names = {f["fid"] for f in data["fields"]}
    assert field_names == {"category", "value"}


@pytest.mark.worker
def test_visualize_evict_endpoint(tmp_path):
    _setup_storage(tmp_path)
    _write_delta_table(tmp_path)
    viz_session_manager.evict_all()
    client = TestClient(main.app)

    # Seed a session.
    client.post(
        "/catalog/visualize_query",
        json={
            "source": {
                "kind": "physical",
                "session_key": "evict:1",
                "table_path": "viz_test",
                "storage_format": "delta",
            },
            "payload": {"workflow": [{"type": "view", "query": [{"op": "raw", "fields": ["*"]}]}]},
        },
    )
    assert "evict:1" in viz_session_manager.stats()["keys"]

    r = client.post("/catalog/visualize_evict", params={"session_key": "evict:1"})
    assert r.status_code == 200
    assert "evict:1" not in viz_session_manager.stats()["keys"]
