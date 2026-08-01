"""Tests for the scd2_delta worker function.

Mirrors the merge_delta protocol tests: initial load, incremental change, the nothing-changed
skip, full-snapshot close-out, and the error paths (progress -1 + a useful error message).
"""

from multiprocessing import Queue
from pathlib import Path

import polars as pl
import pytest

from flowfile_worker import mp_context
from flowfile_worker.funcs import scd2_delta
from shared.storage_config import storage

RUN_1 = "2024-01-01T00:00:00+00:00"
RUN_2 = "2024-02-01T00:00:00+00:00"


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


def _run(lf: pl.LazyFrame, output_path: str, run_timestamp: str = RUN_1, **kwargs):
    """Invoke scd2_delta in-process and return (progress, error, queue)."""
    progress, error_message, queue = _make_helpers()
    scd2_delta(
        polars_serializable_object=lf.serialize(),
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path="",
        output_path=output_path,
        run_timestamp=run_timestamp,
        business_keys=kwargs.pop("business_keys", ["id"]),
        compare_columns=kwargs.pop("compare_columns", []),
        **kwargs,
    )
    return progress, error_message, queue


def _error_text(error_message) -> str:
    return error_message.raw.decode(errors="replace").rstrip("\x00")


class TestScd2Delta:
    def test_initial_load_creates_table_with_system_columns(self, tmp_path):
        output_path = str(tmp_path / "scd2_new")
        lf = pl.LazyFrame({"id": [1, 2], "val": ["a", "b"]})

        progress, _, queue = _run(lf, output_path)

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["row_count"] == 2
        assert result["scd2_metrics"]["rows_inserted"] == 2
        assert result["scd2_metrics"]["rows_closed"] == 0
        assert result["scd2_metrics"]["created"] is True
        assert Path(output_path, "_delta_log").is_dir()

        df = pl.read_delta(output_path)
        assert set(df.columns) == {"id", "val", "sk", "valid_from", "valid_to", "is_current"}
        assert df["is_current"].to_list() == [True, True]
        assert df["valid_to"].null_count() == 2

    def test_change_closes_old_version_and_inserts_new(self, tmp_path):
        output_path = str(tmp_path / "scd2_change")
        _run(pl.LazyFrame({"id": [1, 2], "val": ["a", "b"]}), output_path)

        progress, _, queue = _run(
            pl.LazyFrame({"id": [1, 2, 3], "val": ["A", "b", "c"]}), output_path, run_timestamp=RUN_2
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        metrics = result["scd2_metrics"]
        assert metrics["rows_inserted"] == 2  # id=1 new version + id=3 new key
        assert metrics["rows_closed"] == 1  # id=1's prior version
        assert metrics["rows_total"] == 4
        assert metrics["rows_current"] == 3
        assert result["row_count"] == 4
        assert result["column_count"] == 6

        df = pl.read_delta(output_path)
        closed = df.filter(~pl.col("is_current"))
        assert closed.height == 1
        assert closed["id"].to_list() == [1]
        assert closed["val"].to_list() == ["a"]
        assert closed["valid_to"].null_count() == 0
        current = df.filter(pl.col("is_current")).sort("id")
        assert current["val"].to_list() == ["A", "b", "c"]

    def test_unchanged_batch_is_skipped(self, tmp_path):
        output_path = str(tmp_path / "scd2_skip")
        lf = pl.LazyFrame({"id": [1, 2], "val": ["a", "b"]})
        _run(lf, output_path)
        version_before = pl.read_delta(output_path).height

        progress, _, queue = _run(lf, output_path, run_timestamp=RUN_2)

        assert progress.value == 100
        assert queue.get(timeout=5) == {"skipped": True}
        assert pl.read_delta(output_path).height == version_before

    def test_full_snapshot_closes_absent_keys(self, tmp_path):
        output_path = str(tmp_path / "scd2_snapshot")
        _run(pl.LazyFrame({"id": [1, 2], "val": ["a", "b"]}), output_path)

        progress, _, queue = _run(
            pl.LazyFrame({"id": [1], "val": ["a"]}), output_path, run_timestamp=RUN_2, full_snapshot=True
        )

        assert progress.value == 100
        result = queue.get(timeout=5)
        assert result["scd2_metrics"]["rows_closed"] == 1

        df = pl.read_delta(output_path)
        assert df.filter(pl.col("is_current"))["id"].to_list() == [1]
        assert df.filter(~pl.col("is_current"))["id"].to_list() == [2]

    def test_custom_column_names_are_honoured(self, tmp_path):
        output_path = str(tmp_path / "scd2_custom")
        progress, _, queue = _run(
            pl.LazyFrame({"id": [1], "val": ["a"]}),
            output_path,
            surrogate_key_column="row_key",
            valid_from_column="from_ts",
            valid_to_column="to_ts",
            is_current_column="active",
        )

        assert progress.value == 100
        queue.get(timeout=5)
        assert set(pl.read_delta(output_path).columns) == {"id", "val", "row_key", "from_ts", "to_ts", "active"}


class TestScd2DeltaErrors:
    def test_invalid_bytes(self, tmp_path):
        progress, error_message, queue = _make_helpers()
        scd2_delta(
            polars_serializable_object=b"invalid",
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path="",
            output_path=str(tmp_path / "bad"),
            run_timestamp=RUN_1,
            business_keys=["id"],
        )
        assert progress.value == -1

    def test_duplicate_business_keys(self, tmp_path):
        output_path = str(tmp_path / "scd2_dupes")
        progress, error_message, _ = _run(pl.LazyFrame({"id": [1, 1], "val": ["a", "b"]}), output_path)

        assert progress.value == -1
        error = _error_text(error_message)
        assert "not" in error and "unique" in error
        assert not Path(output_path, "_delta_log").exists()

    def test_null_business_key(self, tmp_path):
        output_path = str(tmp_path / "scd2_nulls")
        lf = pl.LazyFrame({"id": [1, None], "val": ["a", "b"]}, schema={"id": pl.Int64, "val": pl.String})
        progress, error_message, _ = _run(lf, output_path)

        assert progress.value == -1
        assert "NULL" in _error_text(error_message)

    def test_non_scd2_target_is_refused(self, tmp_path):
        output_path = str(tmp_path / "plain_table")
        _create_delta_table(output_path, pl.DataFrame({"id": [1], "val": ["a"]}))

        progress, error_message, _ = _run(pl.LazyFrame({"id": [1], "val": ["b"]}), output_path)

        assert progress.value == -1
        error = _error_text(error_message)
        assert "not an SCD2 table" in error
        # The refusal must not have touched the target.
        assert pl.read_delta(output_path).columns == ["id", "val"]

    def test_missing_business_key_column(self, tmp_path):
        output_path = str(tmp_path / "scd2_missing_key")
        progress, error_message, _ = _run(
            pl.LazyFrame({"other": [1], "val": ["a"]}), output_path, business_keys=["id"]
        )

        assert progress.value == -1
        assert "business-key" in _error_text(error_message)

    def test_error_message_is_truncated_to_1024_bytes(self, tmp_path):
        """The shared error Array is 1024 bytes wide; a longer message must not overflow it."""
        output_path = str(tmp_path / "scd2_long_error")
        long_names = [f"missing_column_{i:04d}" for i in range(200)]
        progress, error_message, _ = _run(
            pl.LazyFrame({"id": [1], "val": ["a"]}),
            output_path,
            compare_columns=long_names,
        )

        assert progress.value == -1
        assert len(_error_text(error_message)) <= 1024
