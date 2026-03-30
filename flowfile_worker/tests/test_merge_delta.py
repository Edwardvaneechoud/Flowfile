"""Tests for merge_delta worker function.

Covers upsert, update, and delete merge modes on both new and existing Delta tables,
including schema evolution (new columns in source).
"""

from multiprocessing import Queue
from pathlib import Path

import polars as pl
import pytest

from flowfile_worker import mp_context
from flowfile_worker.funcs import merge_delta
from shared.storage_config import storage


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


def _make_helpers():
    """Create shared multiprocessing objects for tests."""
    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    queue = Queue(maxsize=1)
    return progress, error_message, queue


def _create_delta_table(path: str, df: pl.DataFrame):
    """Write a Delta table at the given path."""
    import os

    os.makedirs(path, exist_ok=True)
    df.write_delta(path, mode="error")


class TestMergeDeltaUpsert:
    def test_upsert_creates_table_when_missing(self, tmp_path):
        lf = pl.LazyFrame({"id": [1, 2], "val": ["a", "b"]})
        output_path = str(tmp_path / "new_table")
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="upsert",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 2
        assert result["storage_format"] == "delta"
        assert Path(output_path, "_delta_log").is_dir()

    def test_upsert_updates_and_inserts(self, tmp_path):
        output_path = str(tmp_path / "upsert_table")
        _create_delta_table(output_path, pl.DataFrame({"id": [1, 2], "val": ["old_a", "old_b"]}))

        # Upsert: update id=1, insert id=3
        lf = pl.LazyFrame({"id": [1, 3], "val": ["new_a", "c"]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="upsert",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 3

        df = pl.read_delta(output_path).sort("id")
        assert df["id"].to_list() == [1, 2, 3]
        assert df["val"].to_list() == ["new_a", "old_b", "c"]

    def test_upsert_with_composite_keys(self, tmp_path):
        output_path = str(tmp_path / "composite_table")
        _create_delta_table(
            output_path,
            pl.DataFrame({"k1": [1, 1, 2], "k2": ["a", "b", "a"], "val": [10, 20, 30]}),
        )

        lf = pl.LazyFrame({"k1": [1, 2], "k2": ["a", "b"], "val": [99, 88]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="upsert",
            merge_keys=["k1", "k2"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 4  # (1,a), (1,b), (2,a), (2,b)

        df = pl.read_delta(output_path).sort(["k1", "k2"])
        assert df.filter((pl.col("k1") == 1) & (pl.col("k2") == "a"))["val"].item() == 99
        assert df.filter((pl.col("k1") == 2) & (pl.col("k2") == "b"))["val"].item() == 88
        assert df.filter((pl.col("k1") == 1) & (pl.col("k2") == "b"))["val"].item() == 20
        assert df.filter((pl.col("k1") == 2) & (pl.col("k2") == "a"))["val"].item() == 30

    def test_upsert_with_new_column(self, tmp_path):
        """Upsert adds new columns from source, filling null for unmatched target rows."""
        output_path = str(tmp_path / "upsert_new_col")
        _create_delta_table(output_path, pl.DataFrame({"id": [1, 2], "val": ["a", "b"]}))

        lf = pl.LazyFrame({"id": [1, 3], "val": ["updated_a", "c"], "new_col": [100, 300]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="upsert",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 3

        df = pl.read_delta(output_path).sort("id")
        assert df.columns == ["id", "val", "new_col"]
        assert df["new_col"].to_list() == [100, None, 300]


class TestMergeDeltaUpdate:
    def test_update_only_matched(self, tmp_path):
        output_path = str(tmp_path / "update_table")
        _create_delta_table(output_path, pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}))

        # Update id=2 only, id=4 should NOT be inserted
        lf = pl.LazyFrame({"id": [2, 4], "val": ["updated_b", "d"]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="update",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 3  # No new rows inserted

        df = pl.read_delta(output_path).sort("id")
        assert df["id"].to_list() == [1, 2, 3]
        assert df["val"].to_list() == ["a", "updated_b", "c"]

    def test_update_creates_empty_table_when_missing(self, tmp_path):
        """Update on non-existent table creates empty table (no rows to update)."""
        lf = pl.LazyFrame({"id": [1], "val": ["x"]})
        output_path = str(tmp_path / "update_new")
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="update",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 0
        assert Path(output_path, "_delta_log").is_dir()

    def test_update_with_new_column(self, tmp_path):
        """Update adds new columns from source, filling null for unmatched target rows."""
        output_path = str(tmp_path / "update_new_col")
        _create_delta_table(output_path, pl.DataFrame({"id": [1, 2], "val": ["a", "b"]}))

        lf = pl.LazyFrame({"id": [1], "val": ["updated_a"], "new_col": [42]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="update",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 2

        df = pl.read_delta(output_path).sort("id")
        assert "new_col" in df.columns
        assert df["new_col"].to_list() == [42, None]


class TestMergeDeltaDelete:
    def test_delete_matched_rows(self, tmp_path):
        output_path = str(tmp_path / "delete_table")
        _create_delta_table(output_path, pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}))

        # Delete rows matching id=1 and id=3
        lf = pl.LazyFrame({"id": [1, 3], "val": ["a", "c"]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="delete",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 1

        df = pl.read_delta(output_path)
        assert df["id"].to_list() == [2]

    def test_delete_on_nonexistent_table(self, tmp_path):
        lf = pl.LazyFrame({"id": [1], "val": ["x"]})
        output_path = str(tmp_path / "delete_new")
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="delete",
            merge_keys=["id"],
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 0  # Empty table created
        assert Path(output_path, "_delta_log").is_dir()


class TestMergeDeltaErrors:
    def test_invalid_bytes(self, tmp_path):
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=b"invalid",
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=str(tmp_path / "bad"),
            merge_mode="upsert",
            merge_keys=["id"],
        )

        assert progress.value == -1

    def test_missing_merge_keys_on_existing_table(self, tmp_path):
        """merge_keys=None on an existing table should set progress to -1 (ValueError)."""
        output_path = str(tmp_path / "no_keys")
        _create_delta_table(output_path, pl.DataFrame({"id": [1], "val": ["a"]}))

        lf = pl.LazyFrame({"id": [1], "val": ["b"]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="upsert",
            merge_keys=None,
        )

        assert progress.value == -1
        error = error_message.raw.decode().rstrip("\x00")
        assert "merge_keys" in error.lower()

    def test_empty_merge_keys_on_existing_table(self, tmp_path):
        """merge_keys=[] on an existing table should set progress to -1 (ValueError)."""
        output_path = str(tmp_path / "empty_keys")
        _create_delta_table(output_path, pl.DataFrame({"id": [1], "val": ["a"]}))

        lf = pl.LazyFrame({"id": [1], "val": ["b"]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="upsert",
            merge_keys=[],
        )

        assert progress.value == -1

    def test_unknown_merge_mode(self, tmp_path):
        output_path = str(tmp_path / "unknown_mode")
        _create_delta_table(output_path, pl.DataFrame({"id": [1], "val": ["a"]}))

        lf = pl.LazyFrame({"id": [1], "val": ["b"]})
        progress, error_message, queue = _make_helpers()

        merge_delta(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=output_path,
            merge_mode="invalid_mode",
            merge_keys=["id"],
        )

        assert progress.value == -1
