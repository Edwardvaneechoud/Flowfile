"""Tests for flowfile_core.catalog.delta_utils module.

Covers Delta table detection, legacy Parquet detection, table existence,
size calculation, preview reading, and storage deletion.
"""

import shutil
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.catalog.delta_utils import (
    delete_table_storage,
    get_delta_table_size_bytes,
    is_delta_table,
    is_legacy_parquet,
    read_delta_preview,
    table_exists,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def delta_table_path(tmp_path: Path) -> Path:
    """Create a real Delta table on disk and return its path."""
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    dest = tmp_path / "my_delta"
    df.write_delta(str(dest), mode="error")
    return dest


@pytest.fixture()
def parquet_file_path(tmp_path: Path) -> Path:
    """Create a single Parquet file on disk and return its path."""
    df = pl.DataFrame({"x": [10, 20], "y": ["foo", "bar"]})
    dest = tmp_path / "data.parquet"
    df.write_parquet(dest)
    return dest


# ---------------------------------------------------------------------------
# is_delta_table
# ---------------------------------------------------------------------------


class TestIsDeltaTable:
    def test_true_for_delta_directory(self, delta_table_path: Path):
        assert is_delta_table(delta_table_path) is True

    def test_false_for_parquet_file(self, parquet_file_path: Path):
        assert is_delta_table(parquet_file_path) is False

    def test_false_for_plain_directory(self, tmp_path: Path):
        plain_dir = tmp_path / "not_delta"
        plain_dir.mkdir()
        assert is_delta_table(plain_dir) is False

    def test_false_for_nonexistent_path(self, tmp_path: Path):
        assert is_delta_table(tmp_path / "nope") is False

    def test_accepts_string_path(self, delta_table_path: Path):
        assert is_delta_table(str(delta_table_path)) is True


# ---------------------------------------------------------------------------
# is_legacy_parquet
# ---------------------------------------------------------------------------


class TestIsLegacyParquet:
    def test_true_for_parquet_file(self, parquet_file_path: Path):
        assert is_legacy_parquet(parquet_file_path) is True

    def test_false_for_delta_directory(self, delta_table_path: Path):
        assert is_legacy_parquet(delta_table_path) is False

    def test_false_for_nonexistent_path(self, tmp_path: Path):
        assert is_legacy_parquet(tmp_path / "missing.parquet") is False

    def test_false_for_non_parquet_file(self, tmp_path: Path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        assert is_legacy_parquet(csv_file) is False

    def test_case_insensitive_extension(self, tmp_path: Path):
        df = pl.DataFrame({"a": [1]})
        dest = tmp_path / "data.PARQUET"
        df.write_parquet(dest)
        assert is_legacy_parquet(dest) is True


# ---------------------------------------------------------------------------
# table_exists
# ---------------------------------------------------------------------------


class TestTableExists:
    def test_true_for_delta(self, delta_table_path: Path):
        assert table_exists(delta_table_path) is True

    def test_true_for_parquet(self, parquet_file_path: Path):
        assert table_exists(parquet_file_path) is True

    def test_false_for_nothing(self, tmp_path: Path):
        assert table_exists(tmp_path / "nonexistent") is False

    def test_false_for_plain_directory(self, tmp_path: Path):
        d = tmp_path / "empty_dir"
        d.mkdir()
        assert table_exists(d) is False


# ---------------------------------------------------------------------------
# get_delta_table_size_bytes
# ---------------------------------------------------------------------------


class TestGetDeltaTableSizeBytes:
    def test_returns_positive_size(self, delta_table_path: Path):
        size = get_delta_table_size_bytes(delta_table_path)
        assert size > 0

    def test_size_matches_parquet_files(self, delta_table_path: Path):
        """The reported size should equal the sum of .parquet file sizes
        (for a simple single-version table they are the same)."""
        fs_size = sum(f.stat().st_size for f in delta_table_path.rglob("*.parquet"))
        delta_size = get_delta_table_size_bytes(delta_table_path)
        assert delta_size == fs_size


# ---------------------------------------------------------------------------
# read_delta_preview
# ---------------------------------------------------------------------------


class TestReadDeltaPreview:
    def test_reads_all_rows(self, delta_table_path: Path):
        df = read_delta_preview(delta_table_path, n_rows=100)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 3
        assert set(df.columns) == {"id", "name"}

    def test_limits_rows(self, delta_table_path: Path):
        df = read_delta_preview(delta_table_path, n_rows=2)
        assert df.height == 2

    def test_preserves_dtypes(self, delta_table_path: Path):
        df = read_delta_preview(delta_table_path, n_rows=100)
        assert df["id"].dtype == pl.Int64
        assert df["name"].dtype == pl.String


# ---------------------------------------------------------------------------
# delete_table_storage
# ---------------------------------------------------------------------------


class TestDeleteTableStorage:
    def test_deletes_delta_directory(self, delta_table_path: Path):
        assert delta_table_path.exists()
        delete_table_storage(delta_table_path)
        assert not delta_table_path.exists()

    def test_deletes_parquet_file(self, parquet_file_path: Path):
        assert parquet_file_path.exists()
        delete_table_storage(parquet_file_path)
        assert not parquet_file_path.exists()

    def test_noop_for_nonexistent(self, tmp_path: Path):
        """Should not raise if path doesn't exist."""
        nonexistent = tmp_path / "gone"
        # No exception — the path is neither dir nor file
        delete_table_storage(nonexistent)
