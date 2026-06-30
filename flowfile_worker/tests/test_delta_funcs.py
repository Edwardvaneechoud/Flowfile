"""Tests for Delta Lake worker functions and routes.

Covers:
- write_delta (multiprocessing task)
- read_table_metadata (delta + parquet)
- _get_delta_size_bytes
- format_delta_timestamp
- get_delta_history
- read_delta_version_preview
- materialize_catalog_table_task (now writes delta)
- Worker routes: /catalog/table_metadata, /catalog/delta_history,
  /catalog/delta_version_preview, /catalog/materialize (updated)
"""

import io
from multiprocessing import Queue
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main, mp_context
from flowfile_worker.funcs import (
    _get_delta_size_bytes,
    get_delta_history,
    materialize_catalog_table_task,
    optimize_catalog_table_task,
    read_delta_version_preview,
    read_table_metadata,
    vacuum_catalog_table_task,
    write_delta,
)
from shared.delta_utils import format_delta_timestamp
from shared.storage_config import storage


# Fixtures


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path):
    """Point storage at tmp_path so catalog_tables_directory is inside tmp_path."""
    old_base, old_user = storage._base_dir, storage._user_data_dir
    storage._base_dir = tmp_path
    storage._user_data_dir = tmp_path
    storage._ensure_directories()
    yield
    storage._base_dir = old_base
    storage._user_data_dir = old_user


@pytest.fixture()
def delta_path(tmp_path: Path) -> Path:
    """Create a real Delta table inside the catalog directory."""
    df = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    dest = storage.catalog_tables_directory / "test_delta"
    df.write_delta(str(dest), mode="error")
    return dest


@pytest.fixture()
def parquet_path(tmp_path: Path) -> Path:
    """Create a Parquet file inside the catalog directory."""
    df = pl.DataFrame({"x": [100, 200], "y": ["a", "b"]})
    dest = storage.catalog_tables_directory / "test.parquet"
    df.write_parquet(dest)
    return dest


@pytest.fixture()
def versioned_delta(tmp_path: Path) -> Path:
    """Create a Delta table with two versions inside the catalog directory."""
    dest = storage.catalog_tables_directory / "versioned"
    pl.DataFrame({"v": [1]}).write_delta(str(dest), mode="error")
    pl.DataFrame({"v": [1, 2]}).write_delta(str(dest), mode="overwrite")
    return dest


@pytest.fixture()
def worker_client(tmp_path) -> TestClient:
    return TestClient(main.app)


# _get_delta_size_bytes


class TestGetDeltaSizeBytes:
    def test_returns_positive(self, delta_path):
        size = _get_delta_size_bytes(delta_path)
        assert size > 0

    def test_matches_parquet_file_sizes(self, delta_path):
        fs_size = sum(f.stat().st_size for f in delta_path.rglob("*.parquet"))
        assert _get_delta_size_bytes(delta_path) == fs_size


# format_delta_timestamp


class TestFormatDeltaTimestamp:
    def test_none(self):
        assert format_delta_timestamp(None) is None

    def test_string_passthrough(self):
        assert format_delta_timestamp("2024-01-01") == "2024-01-01"

    def test_datetime(self):
        from datetime import datetime, timezone

        dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = format_delta_timestamp(dt)
        assert "2024-01-15" in result

    def test_millis(self):
        result = format_delta_timestamp(1700000000000)
        assert result is not None
        assert "2023" in result


# read_table_metadata


class TestReadTableMetadata:
    def test_delta_metadata(self, delta_path):
        result = read_table_metadata("test_delta")
        assert result["row_count"] == 3
        assert result["column_count"] == 2
        assert result["size_bytes"] > 0
        assert len(result["schema"]) == 2
        names = {s["name"] for s in result["schema"]}
        assert names == {"id", "value"}


# get_delta_history


class TestGetDeltaHistory:
    def test_single_version(self, delta_path):
        result = get_delta_history("test_delta")
        assert result.current_version == 0
        assert len(result.history) >= 1
        assert result.history[0].version == 0

    def test_multiple_versions(self, versioned_delta):
        result = get_delta_history("versioned")
        assert result.current_version == 1
        assert len(result.history) >= 2

    def test_limit(self, versioned_delta):
        result = get_delta_history("versioned", limit=1)
        assert len(result.history) == 1


# read_delta_version_preview


class TestReadDeltaVersionPreview:
    def test_version_0(self, versioned_delta):
        result = read_delta_version_preview("versioned", version=0)
        assert result.version == 0
        assert result.columns == ["v"]
        assert len(result.rows) == 1

    def test_version_1(self, versioned_delta):
        result = read_delta_version_preview("versioned", version=1)
        assert result.version == 1
        assert len(result.rows) == 2

    def test_n_rows_limit(self, versioned_delta):
        result = read_delta_version_preview("versioned", version=1, n_rows=1)
        assert len(result.rows) == 1


# write_delta (multiprocessing function)


class TestWriteDelta:
    def test_writes_delta_table(self, tmp_path):
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        serialized = lf.serialize()

        output_path = str(tmp_path / "write_out")
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        write_delta(
            polars_serializable_object=serialized,
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            mode="overwrite",
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["table_path"] == output_path
        assert result["row_count"] == 3
        assert result["column_count"] == 2
        assert result["size_bytes"] > 0
        assert len(result["schema"]) == 2

        df = pl.read_delta(output_path)
        assert df.height == 3

    def test_write_delta_error(self, tmp_path):
        """Invalid serialized bytes should set progress to -1."""
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        write_delta(
            polars_serializable_object=b"invalid_bytes",
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=str(tmp_path / "err_out"),
            mode="overwrite",
        )

        assert progress.value == -1

    def test_write_delta_partitioned(self, tmp_path):
        from deltalake import DeltaTable

        lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "x"]})
        output_path = str(tmp_path / "part_out")
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        write_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            mode="overwrite",
            partition_by=["b"],
        )

        assert progress.value == 100
        assert DeltaTable(output_path).metadata().partition_columns == ["b"]


# optimize / vacuum catalog tasks


class TestOptimizeVacuumTasks:
    def test_optimize_task(self, delta_path):
        # add a second file so compaction has something to do
        pl.DataFrame({"id": [4], "value": [40.0]}).write_delta(str(delta_path), mode="append")
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        optimize_catalog_table_task(
            table_path=str(delta_path),
            z_order_columns=None,
            progress=progress,
            error_message=error_message,
            queue=queue,
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert isinstance(result["metrics"], dict)
        assert result["size_bytes"] > 0

    def test_vacuum_task_dry_run(self, delta_path):
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        vacuum_catalog_table_task(
            table_path=str(delta_path),
            retention_hours=0,
            dry_run=True,
            progress=progress,
            error_message=error_message,
            queue=queue,
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert isinstance(result["files_removed"], list)
        assert result["file_count"] == len(result["files_removed"])

    def test_optimize_task_missing_table(self, tmp_path):
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        optimize_catalog_table_task(
            table_path=str(tmp_path / "does_not_exist"),
            z_order_columns=None,
            progress=progress,
            error_message=error_message,
            queue=queue,
        )

        assert progress.value == -1


# materialize_catalog_table_task (now writes delta)


class TestMaterializeCatalogTableTask:
    def test_csv_to_delta(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,x\n2,y\n3,z\n")
        dest = str(tmp_path / "out_delta")

        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        materialize_catalog_table_task(
            source_file_path=str(csv_file),
            dest_path=dest,
            progress=progress,
            error_message=error_message,
            queue=queue,
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["table_path"] == dest
        assert result["row_count"] == 3
        assert Path(dest, "_delta_log").is_dir()

    def test_parquet_to_delta(self, tmp_path, parquet_path):
        dest = str(tmp_path / "pq_to_delta")
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        materialize_catalog_table_task(
            source_file_path=str(parquet_path),
            dest_path=dest,
            progress=progress,
            error_message=error_message,
            queue=queue,
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 2

    def test_unsupported_extension(self, tmp_path):
        bad_file = tmp_path / "data.json"
        bad_file.write_text('{"a": 1}')
        dest = str(tmp_path / "bad_out")

        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = Queue(maxsize=1)

        materialize_catalog_table_task(
            source_file_path=str(bad_file),
            dest_path=dest,
            progress=progress,
            error_message=error_message,
            queue=queue,
        )

        assert progress.value == -1


# _drain_then_join


def _put_large_payload(queue):
    # ~200KB: well past the OS pipe buffer, so a join-before-get would deadlock
    queue.put({"files_removed": ["x" * 100 for _ in range(2000)]})


class TestDrainThenJoin:
    def test_large_payload_does_not_deadlock(self):
        from flowfile_worker.routes import _drain_then_join

        queue = mp_context.Queue(maxsize=1)
        p = mp_context.Process(target=_put_large_payload, args=(queue,))
        p.start()
        result = _drain_then_join(p, queue)
        assert result is not None
        assert len(result["files_removed"]) == 2000

    def test_child_error_returns_none(self, tmp_path):
        from flowfile_worker.routes import _drain_then_join

        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = mp_context.Queue(maxsize=1)
        p = mp_context.Process(
            target=optimize_catalog_table_task,
            kwargs={
                "table_path": str(tmp_path / "does_not_exist"),
                "z_order_columns": None,
                "progress": progress,
                "error_message": error_message,
                "queue": queue,
            },
        )
        p.start()
        result = _drain_then_join(p, queue)
        assert result is None
        assert progress.value == -1


# Worker Routes


class TestWorkerRoutes:
    def test_catalog_materialize_returns_delta(self, worker_client, tmp_path):
        """POST /catalog/materialize should now produce a Delta table."""
        csv_file = tmp_path / "route_data.csv"
        csv_file.write_text("col1,col2\n10,hello\n20,world\n")

        resp = worker_client.post(
            "/catalog/materialize",
            json={"source_file_path": str(csv_file), "table_name": "route_test"},
        )
        assert resp.status_code == 200
        data = resp.json()

        assert "table_path" in data
        assert data["row_count"] == 2
        assert data["column_count"] == 2
        assert Path(data["table_path"], "_delta_log").is_dir()

    def test_table_metadata_delta(self, worker_client, delta_path):
        resp = worker_client.post(
            "/catalog/table_metadata",
            json={"table_path": "test_delta"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert data["column_count"] == 2
        assert data["size_bytes"] > 0
        assert len(data["column_schema"]) == 2

    def test_delta_history(self, worker_client, versioned_delta):
        resp = worker_client.post(
            "/catalog/delta_history",
            json={"table_path": "versioned"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["current_version"] == 1
        assert len(data["history"]) >= 2

    def test_delta_history_with_limit(self, worker_client, versioned_delta):
        resp = worker_client.post(
            "/catalog/delta_history",
            json={"table_path": "versioned", "limit": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["history"]) == 1

    def test_delta_version_preview(self, worker_client, versioned_delta):
        resp = worker_client.post(
            "/catalog/delta_version_preview",
            json={"table_path": "versioned", "version": 0, "n_rows": 100},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == 0
        assert data["columns"] == ["v"]
        assert len(data["rows"]) == 1

    def test_delta_version_preview_v1(self, worker_client, versioned_delta):
        resp = worker_client.post(
            "/catalog/delta_version_preview",
            json={"table_path": "versioned", "version": 1, "n_rows": 100},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["rows"]) == 2

    def test_delta_version_preview_n_rows(self, worker_client, versioned_delta):
        resp = worker_client.post(
            "/catalog/delta_version_preview",
            json={"table_path": "versioned", "version": 1, "n_rows": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["rows"]) == 1

    def test_delta_preview(self, worker_client, versioned_delta):
        resp = worker_client.post(
            "/catalog/delta_preview",
            json={"table_path": "versioned", "n_rows": 100},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "version" not in data  # non-versioned response shape
        assert data["columns"] == ["v"]
        assert len(data["rows"]) == 2  # latest version (the overwrite)

    def test_delta_preview_n_rows(self, worker_client, versioned_delta):
        resp = worker_client.post(
            "/catalog/delta_preview",
            json={"table_path": "versioned", "n_rows": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["rows"]) == 1  # page is bounded

    def test_delta_preview_path_traversal(self, worker_client):
        resp = worker_client.post(
            "/catalog/delta_preview",
            json={"table_path": "../etc/passwd"},
        )
        assert resp.status_code == 400

    def test_table_metadata_path_traversal(self, worker_client):
        """Path with separators should be rejected as 400."""
        resp = worker_client.post(
            "/catalog/table_metadata",
            json={"table_path": "../etc/passwd"},
        )
        assert resp.status_code == 400

    def test_table_metadata_absolute_path(self, worker_client):
        """Absolute path should be rejected as 400."""
        resp = worker_client.post(
            "/catalog/table_metadata",
            json={"table_path": "/nonexistent/path"},
        )
        assert resp.status_code == 400

    def test_optimize_route(self, worker_client, delta_path):
        resp = worker_client.post("/catalog/optimize", json={"table_path": "test_delta"})
        assert resp.status_code == 200
        data = resp.json()
        assert "metrics" in data
        assert data["size_bytes"] > 0

    def test_vacuum_route_dry_run(self, worker_client, delta_path):
        resp = worker_client.post(
            "/catalog/vacuum",
            json={"table_path": "test_delta", "retention_hours": 0, "dry_run": True},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "files_removed" in data
        assert data["file_count"] == len(data["files_removed"])

    def test_optimize_route_path_traversal(self, worker_client):
        resp = worker_client.post("/catalog/optimize", json={"table_path": "../escape"})
        assert resp.status_code == 400

    def test_vacuum_route_path_traversal(self, worker_client):
        resp = worker_client.post("/catalog/vacuum", json={"table_path": "../escape"})
        assert resp.status_code == 400
