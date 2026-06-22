"""Tests for the catalog visualization compute path on the worker."""
from __future__ import annotations

import concurrent.futures
import sys
import threading
import time
from typing import Any

import polars as pl
import pytest
from fastapi.testclient import TestClient
from fastapi import HTTPException

from flowfile_worker import main, models
from flowfile_worker import viz_sessions as viz_sessions_module
from flowfile_worker.viz_sessions import (
    REQUEST_QUEUE_MAXSIZE,
    SHUTDOWN_GRACE_SECONDS,
    VizSessionRegistry,
    viz_session_registry,
)
from shared.storage_config import storage


def _setup_storage(tmp_path, monkeypatch=None):
    storage._base_dir = tmp_path
    storage._user_data_dir = tmp_path
    storage._ensure_directories()
    # Spawned children re-import storage and read FLOWFILE_STORAGE_DIR from env.
    if monkeypatch is not None:
        monkeypatch.setenv("FLOWFILE_STORAGE_DIR", str(tmp_path))
    else:
        import os as _os

        _os.environ["FLOWFILE_STORAGE_DIR"] = str(tmp_path)


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


def _write_constant_delta_table(tmp_path, name: str = "viz_degenerate") -> str:
    """Delta table whose numeric column has a single distinct value.

    Binning such a column used to divide by a zero-width step (step == 0 ->
    0/0 == NaN) and crash the strict Int64 cast inside polars-gw, which the
    worker surfaced as an error response.  polars-gw >= 0.1.3 collapses a
    degenerate column to a single bucket instead.
    """
    df = pl.DataFrame(
        {
            "category": ["a", "a", "a", "a", "a"],
            "value": [5, 5, 5, 5, 5],
        }
    )
    target = storage.catalog_tables_directory / name
    target.mkdir(parents=True, exist_ok=True)
    df.write_delta(str(target))
    return name


def _physical_source(session_key: str, table_path: str) -> models.VizWorkerSource:
    return models.VizWorkerSource(
        kind="physical",
        session_key=session_key,
        table_path=table_path,
    )


_RAW_PAYLOAD = {"workflow": [{"type": "view", "query": [{"op": "raw", "fields": ["*"]}]}]}
_AGG_PAYLOAD = {
    "workflow": [
        {
            "type": "view",
            "query": [
                {
                    "op": "aggregate",
                    "groupBy": ["category"],
                    "measures": [{"field": "value", "agg": "sum", "asFieldKey": "value_sum"}],
                }
            ],
        }
    ]
}


def _wait_dead(handle, deadline: float = 5.0) -> bool:
    end = time.time() + deadline
    while time.time() < end:
        if not handle.process.is_alive():
            return True
        time.sleep(0.05)
    return not handle.process.is_alive()


# 1-12: registry-level unit tests (no FastAPI)


def test_spawn_on_first_request(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    try:
        src = _physical_source("unit:spawn:1", table_dir)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        h1 = reg._sessions[src.session_key]
        pid1 = h1.pid
        assert h1.process.is_alive()
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        h2 = reg._sessions[src.session_key]
        assert h2.pid == pid1
    finally:
        reg.shutdown()


def test_second_request_hits_same_child(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    try:
        src = _physical_source("unit:hit:1", table_dir)
        _, hit1 = reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        _, hit2 = reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        assert hit1 is False
        assert hit2 is True
        assert reg._sessions[src.session_key].requests_served == 2
    finally:
        reg.shutdown()


def test_eviction_kills_os_process(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    reg.IDLE_TTL_SECONDS = 0
    try:
        src = _physical_source("unit:evict:1", table_dir)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        handle = reg._sessions[src.session_key]
        # Manually drive reap loop instead of waiting for the daemon thread tick.
        cutoff = time.time() - reg.IDLE_TTL_SECONDS
        with reg._lock:
            stale = [(k, h) for k, h in reg._sessions.items() if h.last_used_at <= cutoff]
            for k, _ in stale:
                reg._sessions.pop(k, None)
        for _, h in stale:
            reg._kill_handle(h)
        assert _wait_dead(handle, deadline=5.0)
    finally:
        reg.shutdown()


@pytest.mark.slow
def test_two_session_keys_run_in_parallel(tmp_path):
    _setup_storage(tmp_path)
    big_df = pl.DataFrame(
        {
            "category": ["a", "b", "c", "d"] * 250_000,
            "value": list(range(1_000_000)),
        }
    )
    big_dir = storage.catalog_tables_directory / "viz_par"
    big_dir.mkdir(parents=True, exist_ok=True)
    big_df.write_delta(str(big_dir))
    reg = VizSessionRegistry()
    # Each call uses a fresh session_key so spawn + first-collect cost lands
    # in the timing — polars caches subsequent agg calls down to microseconds,
    # which would make a warmed comparison degenerate.
    payload = _RAW_PAYLOAD
    max_rows = 1_000_000
    try:
        s1 = _physical_source("unit:par:s1", "viz_par")
        t0 = time.perf_counter()
        reg.execute(s1, "execute", payload, max_rows)
        t1 = time.perf_counter() - t0
        s2 = _physical_source("unit:par:s2", "viz_par")
        t0 = time.perf_counter()
        reg.execute(s2, "execute", payload, max_rows)
        t2 = time.perf_counter() - t0

        if t1 + t2 < 0.1:
            pytest.skip(f"workload too fast to measure overlap: t1+t2={t1 + t2:.4f}s")

        p1 = _physical_source("unit:par:p1", "viz_par")
        p2 = _physical_source("unit:par:p2", "viz_par")

        def _run(src):
            reg.execute(src, "execute", payload, max_rows)

        threads = [threading.Thread(target=_run, args=(s,)) for s in (p1, p2)]
        t_start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        t_par = time.perf_counter() - t_start
        # 0.85 (was 0.7): GitHub-hosted runners only have 4 vCPUs and Polars
        # is multithreaded, so two parallel workloads compete for cores.
        # Catastrophic serialisation still gives ratio ~1.0 and trips this.
        assert t_par < 0.85 * (t1 + t2), f"t_par={t_par:.3f}s t1+t2={t1 + t2:.3f}s"
    finally:
        reg.shutdown()


@pytest.mark.slow
def test_concurrent_same_session_key_no_response_steals(tmp_path):
    _setup_storage(tmp_path)
    big_df = pl.DataFrame(
        {
            "category": ["a", "b", "c", "d"] * 12_500,
            "value": list(range(50_000)),
        }
    )
    target = storage.catalog_tables_directory / "viz_race"
    target.mkdir(parents=True, exist_ok=True)
    big_df.write_delta(str(target))

    reg = VizSessionRegistry()
    src = _physical_source("unit:race:1", "viz_race")
    try:
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)

        n = 8

        def _payload(i: int) -> dict:
            return {
                "workflow": [
                    {
                        "type": "view",
                        "query": [
                            {
                                "op": "aggregate",
                                "groupBy": ["category"],
                                "measures": [
                                    {
                                        "field": "value",
                                        "agg": "sum",
                                        "asFieldKey": f"v_{i}",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }

        results: list[tuple[int, Any]] = []
        errors: list[tuple[int, BaseException]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
            futures = {
                pool.submit(reg.execute, src, "execute", _payload(i), 1_000): i
                for i in range(n)
            }
            for fut in concurrent.futures.as_completed(futures, timeout=60):
                i = futures[fut]
                try:
                    res, _ = fut.result(timeout=0)
                    results.append((i, res))
                except BaseException as e:
                    errors.append((i, e))

        assert not errors, f"some calls failed: {errors}"
        for i, res in results:
            keys = {k for row in res["rows"] for k in row.keys()}
            assert f"v_{i}" in keys, (
                f"call {i} got back rows with keys {keys}, "
                f"expected v_{i} — response was stolen by another caller"
            )
    finally:
        reg.shutdown()


def test_concurrent_different_session_keys_not_serialised_by_lock(tmp_path):
    """parent_lock is per SessionHandle: holding one must not block another key."""
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    try:
        s1 = _physical_source("unit:locksep:s1", table_dir)
        s2 = _physical_source("unit:locksep:s2", table_dir)
        reg.execute(s1, "execute", _AGG_PAYLOAD, 100)
        reg.execute(s2, "execute", _AGG_PAYLOAD, 100)
        h1 = reg._sessions[s1.session_key]
        h2 = reg._sessions[s2.session_key]
        assert h1.parent_lock is not h2.parent_lock

        result: list = []
        error: list = []

        def _run_s2():
            try:
                res, _ = reg.execute(s2, "execute", _AGG_PAYLOAD, 100)
                result.append(res)
            except BaseException as exc:
                error.append(exc)

        with h1.parent_lock:
            t = threading.Thread(target=_run_s2)
            t.start()
            t.join(timeout=15.0)
            assert not t.is_alive(), (
                "execute(s2) blocked while s1.parent_lock held — "
                "parent_lock must be per-handle, not shared across keys"
            )
        assert not error, f"execute(s2) failed: {error[0]!r}"
        assert result, "execute(s2) returned no result"
    finally:
        reg.shutdown()


def test_max_requests_per_child_triggers_respawn(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    reg.MAX_REQUESTS_PER_CHILD = 3
    try:
        src = _physical_source("unit:budget:1", table_dir)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        pid_before = reg._sessions[src.session_key].pid
        # Fourth request must respawn since requests_served >= MAX.
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        pid_after = reg._sessions[src.session_key].pid
        assert pid_after != pid_before
    finally:
        reg.shutdown()


def test_max_lifetime_triggers_respawn(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    reg.MAX_CHILD_LIFETIME_SECONDS = 0.1
    try:
        src = _physical_source("unit:lifetime:1", table_dir)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        pid_before = reg._sessions[src.session_key].pid
        time.sleep(0.2)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        pid_after = reg._sessions[src.session_key].pid
        assert pid_after != pid_before
    finally:
        reg.shutdown()


@pytest.mark.worker
def test_child_crash_propagates_as_5xx(tmp_path):
    """Bad table_path → child raises during load → fatal-load → 502."""
    _setup_storage(tmp_path)
    _write_delta_table(tmp_path)
    viz_session_registry.evict_all()
    client = TestClient(main.app)
    payload = {
        "source": {
            "kind": "physical",
            "session_key": "test:crash:1",
            "table_path": "does_not_exist_xyz",
        },
        "payload": _AGG_PAYLOAD,
    }
    r = client.post("/catalog/visualize_query", json=payload)
    # A bad table_path raises ValueError in the validator → ValueError → 200 with error
    # OR the spawn loads but fails → 502. Either path must be a non-2xx body or an error string.
    assert r.status_code in (200, 502)
    if r.status_code == 200:
        body = r.json()
        assert body.get("error")


@pytest.mark.worker
def test_value_error_propagates_as_response_error(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_registry.evict_all()
    client = TestClient(main.app)
    # Bad workflow op → child loads ok, raises during execute → ValueError → 200 + error.
    payload = {
        "source": {
            "kind": "physical",
            "session_key": "test:bad-payload:1",
            "table_path": table_dir,
        },
        "payload": {"workflow": "this is not a valid workflow"},
    }
    r = client.post("/catalog/visualize_query", json=payload)
    # Either the malformed payload returns 200 with error, or a 4xx; just must not crash 5xx.
    assert r.status_code in (200, 422)
    if r.status_code == 200:
        assert r.json().get("error") is not None


def test_fastapi_shutdown_kills_all_children(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    handles = []
    for i in range(3):
        src = _physical_source(f"unit:shutd:{i}", table_dir)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        handles.append(reg._sessions[src.session_key])
    reg.shutdown()
    deadline = time.time() + 2 * SHUTDOWN_GRACE_SECONDS
    for h in handles:
        while time.time() < deadline and h.process.is_alive():
            time.sleep(0.05)
        assert not h.process.is_alive()


@pytest.mark.worker
def test_visualize_stats_shape(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_registry.evict_all()
    client = TestClient(main.app)
    payload = {
        "source": {
            "kind": "physical",
            "session_key": "test:stats:1",
            "table_path": table_dir,
        },
        "payload": _AGG_PAYLOAD,
    }
    r = client.post("/catalog/visualize_query", json=payload)
    assert r.status_code == 200, r.text
    stats = client.get("/catalog/visualize_stats").json()
    assert isinstance(stats, list)
    assert any(item["session_key"] == "test:stats:1" for item in stats)
    item = next(i for i in stats if i["session_key"] == "test:stats:1")
    assert item["pid"] > 0
    assert item["rss_bytes"] >= -1
    assert item["requests_served"] == 1
    assert item["age_seconds"] >= 0


def test_request_queue_full_returns_503(tmp_path, monkeypatch):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    monkeypatch.setattr(viz_sessions_module, "REQUEST_QUEUE_MAXSIZE", 1)
    reg = VizSessionRegistry()
    try:
        src = _physical_source("unit:full:1", table_dir)
        # Warm the child so spawn isn't on the busy path.
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        handle = reg._sessions[src.session_key]
        # Replace request_q with a tiny fake to exercise the queue.Full path deterministically.
        import queue as _q

        class _Full:
            def put(self, *_a, **_k):
                raise _q.Full()

            def put_nowait(self, *_a, **_k):
                raise _q.Full()

            def get(self, *_a, **_k):
                raise _q.Empty()

            def get_nowait(self, *_a, **_k):
                raise _q.Empty()

            def empty(self):
                return True

            def cancel_join_thread(self):
                pass

            def close(self):
                pass

        handle.request_q = _Full()
        with pytest.raises(HTTPException) as exc_info:
            reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        assert exc_info.value.status_code == 503
    finally:
        reg.shutdown()


def test_evict_all_kills_every_child(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    reg = VizSessionRegistry()
    handles = []
    for i in range(3):
        src = _physical_source(f"unit:evict_all:{i}", table_dir)
        reg.execute(src, "execute", _AGG_PAYLOAD, 100)
        handles.append(reg._sessions[src.session_key])
    try:
        reg.evict_all()
        for h in handles:
            assert _wait_dead(h, deadline=2 * SHUTDOWN_GRACE_SECONDS)
    finally:
        reg.shutdown()


@pytest.mark.worker
def test_polars_gw_not_in_parent_sys_modules(tmp_path):
    """Architectural canary: the FastAPI process must never import polars_gw."""
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_registry.evict_all()
    sys.modules.pop("polars_gw", None)
    client = TestClient(main.app)
    r = client.post(
        "/catalog/visualize_query",
        json={
            "source": {
                "kind": "physical",
                "session_key": "test:gw_leak:1",
                "table_path": table_dir,
                },
            "payload": _AGG_PAYLOAD,
        },
    )
    assert r.status_code == 200, r.text
    # Wait for a moment in case the spawn is not fully synchronous.
    time.sleep(0.1)
    assert "polars_gw" not in sys.modules, "polars_gw leaked into FastAPI parent process"


# Existing HTTP-level tests (kept; flipped manager → registry)


@pytest.mark.worker
def test_visualize_query_endpoint_physical_delta(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_registry.evict_all()

    client = TestClient(main.app)
    payload = {
        "source": {
            "kind": "physical",
            "session_key": "test:phys:1",
            "table_path": table_dir,
        },
        "payload": _AGG_PAYLOAD,
    }

    r1 = client.post("/catalog/visualize_query", json=payload)
    assert r1.status_code == 200, r1.text
    body1 = r1.json()
    assert body1["error"] is None
    assert body1["cache_hit"] is False
    assert body1["total_rows"] == 3
    by_cat = {row["category"]: row["value_sum"] for row in body1["rows"]}
    assert by_cat == {"a": 4, "b": 7, "c": 4}

    r2 = client.post("/catalog/visualize_query", json=payload)
    assert r2.status_code == 200, r2.text
    assert r2.json()["cache_hit"] is True


@pytest.mark.worker
def test_visualize_fields_endpoint_returns_imutfields(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_registry.evict_all()

    client = TestClient(main.app)
    body = {
        "source": {
            "kind": "physical",
            "session_key": "test:fields:1",
            "table_path": table_dir,
        }
    }
    r = client.post("/catalog/visualize_fields", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["error"] is None
    field_names = {f["fid"] for f in data["fields"]}
    assert field_names == {"category", "value"}


@pytest.mark.worker
def test_visualize_query_endpoint_ipc_path(tmp_path):
    _setup_storage(tmp_path)
    viz_session_registry.evict_all()

    target_dir = storage.catalog_virtual_results_directory
    target_dir.mkdir(parents=True, exist_ok=True)
    ipc_name = "fvt-1-deadbeefdeadbeef.arrow"
    pl.DataFrame({"category": ["a", "b", "a"], "value": [1, 2, 3]}).write_ipc(str(target_dir / ipc_name))

    client = TestClient(main.app)
    payload = {
        "source": {
            "kind": "ipc_path",
            "session_key": "test:fvt:1",
            "ipc_path": ipc_name,
            "mtime": 1.0,
        },
        "payload": _AGG_PAYLOAD,
    }
    r = client.post("/catalog/visualize_query", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["error"] is None
    by_cat = {row["category"]: row["value_sum"] for row in body["rows"]}
    assert by_cat == {"a": 4, "b": 2}


@pytest.mark.worker
def test_visualize_query_sql_with_virtual_refs(tmp_path):
    _setup_storage(tmp_path)
    table_dir = _write_delta_table(tmp_path)
    viz_session_registry.evict_all()

    target_dir = storage.catalog_virtual_results_directory
    target_dir.mkdir(parents=True, exist_ok=True)
    ipc_name = "fvt-2-cafebabecafebabe.arrow"
    pl.DataFrame({"category": ["a", "b"], "boost": [10, 20]}).write_ipc(str(target_dir / ipc_name))

    client = TestClient(main.app)
    payload = {
        "source": {
            "kind": "sql",
            "session_key": "test:sql:virt:1",
            "sql_query": (
                'SELECT t.category, t.value + b.boost AS combined '
                'FROM "viz_test" AS t JOIN "boosts" AS b ON t.category = b.category'
            ),
            "tables": {"viz_test": table_dir},
            "virtual_refs": {"boosts": ipc_name},
        },
        "payload": _RAW_PAYLOAD,
    }
    r = client.post("/catalog/visualize_query", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["error"] is None
    rows = sorted(body["rows"], key=lambda r: (r["category"], r["combined"]))
    assert rows[0]["combined"] == 11
    assert rows[-1]["combined"] == 25


@pytest.mark.worker
def test_visualize_evict_endpoint(tmp_path):
    _setup_storage(tmp_path)
    _write_delta_table(tmp_path)
    viz_session_registry.evict_all()
    client = TestClient(main.app)

    client.post(
        "/catalog/visualize_query",
        json={
            "source": {
                "kind": "physical",
                "session_key": "evict:1",
                "table_path": "viz_test",
                },
            "payload": _RAW_PAYLOAD,
        },
    )
    keys = {item["session_key"] for item in viz_session_registry.stats()}
    assert "evict:1" in keys

    r = client.post("/catalog/visualize_evict", params={"session_key": "evict:1"})
    assert r.status_code == 200
    keys = {item["session_key"] for item in viz_session_registry.stats()}
    assert "evict:1" not in keys


@pytest.mark.worker
def test_visualize_query_degenerate_column_bins_without_error(tmp_path):
    """Binning a single-distinct-value column must not surface an error.

    Regression for the polars-gw divide-by-zero NaN crash: a constant column
    gave a zero-width bin step (0/0 == NaN) and the strict Int64 cast raised,
    which the worker caught and returned as an error response (with a noisy
    traceback) instead of a chart.  polars-gw >= 0.1.3 collapses the column to
    a single bucket, so every binning op now succeeds.
    """
    _setup_storage(tmp_path)
    table_dir = _write_constant_delta_table(tmp_path)
    viz_session_registry.evict_all()
    client = TestClient(main.app)

    degenerate_payloads = {
        # equal-width bin (the reported crash): step == 0 on a constant column.
        "bin": {"workflow": [{"type": "transform", "transform": [
            {"key": "value_bin", "expression": {"op": "bin", "params": ["value"], "as": "value_bin", "num": 10}}
        ]}]},
        # quantile binCount on a constant column.
        "binCount": {"workflow": [{"type": "transform", "transform": [
            {"key": "value_q", "expression": {"op": "binCount", "params": ["value"], "as": "value_q", "num": 4}}
        ]}]},
        # binBy view op with a zero bin size (0/0 -> NaN), which falls back to
        # the default width.
        "binBy": {"workflow": [{"type": "view", "query": [
            {"op": "bin", "binBy": "value", "newBinCol": "value_bin", "binSize": 0}
        ]}]},
    }
    for name, payload in degenerate_payloads.items():
        r = client.post(
            "/catalog/visualize_query",
            json={
                "source": {
                    "kind": "physical",
                    "session_key": f"test:degenerate:{name}",
                    "table_path": table_dir,
                },
                "payload": payload,
            },
        )
        assert r.status_code == 200, f"{name}: {r.text}"
        body = r.json()
        assert body["error"] is None, f"{name}: {body['error']}"
        assert body["total_rows"] == 5, f"{name}: {body['total_rows']}"


# Silence ruff: imported for type-only constants
_ = REQUEST_QUEUE_MAXSIZE
