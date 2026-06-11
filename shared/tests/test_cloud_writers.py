"""Unit tests for shared.cloud_storage.writers partitioning support.

Uses local filesystem paths (sink_delta/write_delta accept them with empty
storage options), so no cloud emulator is needed.
"""

import polars as pl
import pytest

from shared.cloud_storage.writers import write_delta_to_cloud, write_to_cloud
from shared.delta_utils import get_delta_partition_columns


class TestWriteDeltaToCloudPartitioning:
    def test_create_partitioned(self, tmp_path):
        p = tmp_path / "t"
        df = pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]})
        write_delta_to_cloud(df, str(p), {}, mode="overwrite", partition_by=["b"])
        assert get_delta_partition_columns(p) == ["b"]

    def test_append_matching_partition(self, tmp_path):
        p = tmp_path / "t"
        write_delta_to_cloud(pl.LazyFrame({"a": [1], "b": ["x"]}), str(p), {}, mode="overwrite", partition_by=["b"])
        write_delta_to_cloud(pl.LazyFrame({"a": [2], "b": ["y"]}), str(p), {}, mode="append", partition_by=["b"])
        assert pl.scan_delta(str(p)).select(pl.len()).collect().item() == 2

    def test_append_mismatched_partition_raises(self, tmp_path):
        p = tmp_path / "t"
        write_delta_to_cloud(pl.LazyFrame({"a": [1], "b": ["x"]}), str(p), {}, mode="overwrite", partition_by=["b"])
        with pytest.raises(Exception):  # delta-rs raises on partition mismatch
            write_delta_to_cloud(pl.LazyFrame({"a": [2], "b": ["y"]}), str(p), {}, mode="append", partition_by=["a"])

    def test_missing_partition_column_rejected(self, tmp_path):
        p = tmp_path / "t"
        with pytest.raises(ValueError, match="partition_by columns not present"):
            write_delta_to_cloud(pl.LazyFrame({"a": [1]}), str(p), {}, mode="overwrite", partition_by=["nope"])

    def test_unpartitioned_default(self, tmp_path):
        p = tmp_path / "t"
        write_delta_to_cloud(pl.LazyFrame({"a": [1]}), str(p), {}, mode="overwrite")
        assert get_delta_partition_columns(p) == []


class TestWriteToCloudPartitioning:
    def test_delta_forwards_partition_by(self, tmp_path):
        p = tmp_path / "t"
        df = pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]})
        write_to_cloud(df, str(p), {}, "delta", partition_by=["b"])
        assert get_delta_partition_columns(p) == ["b"]

    def test_non_delta_with_partition_by_rejected(self, tmp_path):
        df = pl.LazyFrame({"a": [1]})
        with pytest.raises(ValueError, match="only supported for the 'delta'"):
            write_to_cloud(df, str(tmp_path / "f.parquet"), {}, "parquet", partition_by=["a"])
