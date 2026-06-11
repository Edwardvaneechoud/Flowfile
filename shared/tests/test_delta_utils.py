"""Unit tests for shared.delta_utils partitioning + maintenance helpers.

Covers:
- write_delta with partition_by (create, append-match, append-mismatch, ignore-empty)
- merge_into_delta create-branch partitioning
- get_delta_partition_columns
- vacuum_delta (dry_run, <168h retention guard)
- optimize_delta (compact + z_order)
"""

import polars as pl
import pytest
from deltalake import DeltaTable

from shared.delta_utils import (
    get_delta_partition_columns,
    merge_into_delta,
    optimize_delta,
    vacuum_delta,
    write_delta,
)


def _rows(path) -> int:
    return pl.scan_delta(str(path)).select(pl.len()).collect().item()


class TestWriteDeltaPartitioning:
    def test_create_partitioned(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}), str(p), mode="overwrite", partition_by=["b"])
        assert get_delta_partition_columns(p) == ["b"]

    def test_append_matching_partition(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1], "b": ["x"]}), str(p), mode="overwrite", partition_by=["b"])
        wrote = write_delta(pl.DataFrame({"a": [2], "b": ["y"]}), str(p), mode="append", partition_by=["b"])
        assert wrote is True
        assert _rows(p) == 2

    def test_append_without_partition_inherits(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1], "b": ["x"]}), str(p), mode="overwrite", partition_by=["b"])
        write_delta(pl.DataFrame({"a": [2], "b": ["y"]}), str(p), mode="append")
        assert get_delta_partition_columns(p) == ["b"]
        assert _rows(p) == 2

    def test_append_mismatched_partition_raises(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1], "b": ["x"]}), str(p), mode="overwrite", partition_by=["b"])
        with pytest.raises(Exception):  # delta-rs raises on partition mismatch
            write_delta(pl.DataFrame({"a": [2], "b": ["y"]}), str(p), mode="append", partition_by=["a"])

    def test_append_create_partitioned(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1], "b": ["x"]}), str(p), mode="append", partition_by=["b"])
        assert get_delta_partition_columns(p) == ["b"]

    def test_missing_partition_column_rejected(self, tmp_path):
        p = tmp_path / "t"
        with pytest.raises(ValueError, match="partition_by columns not present"):
            write_delta(pl.DataFrame({"a": [1]}), str(p), mode="overwrite", partition_by=["nope"])

    def test_lazyframe_partition(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]}), str(p), mode="overwrite", partition_by=["b"])
        assert get_delta_partition_columns(p) == ["b"]


class TestMergePartitioning:
    def test_create_branch_partitions(self, tmp_path):
        p = tmp_path / "t"
        merge_into_delta(
            pl.DataFrame({"k": [1, 2], "v": ["a", "b"]}),
            str(p),
            merge_mode="upsert",
            merge_keys=["k"],
            partition_by=["v"],
        )
        assert get_delta_partition_columns(p) == ["v"]


class TestGetDeltaPartitionColumns:
    def test_unpartitioned_returns_empty(self, tmp_path):
        p = tmp_path / "t"
        pl.DataFrame({"a": [1]}).write_delta(str(p))
        assert get_delta_partition_columns(p) == []

    def test_unreadable_returns_empty(self, tmp_path):
        assert get_delta_partition_columns(tmp_path / "does_not_exist") == []


class TestVacuumDelta:
    def test_dry_run_returns_list(self, tmp_path):
        p = tmp_path / "t"
        pl.DataFrame({"a": [1]}).write_delta(str(p))
        pl.DataFrame({"a": [1, 2]}).write_delta(str(p), mode="overwrite")
        result = vacuum_delta(p, retention_hours=0, dry_run=True)
        assert isinstance(result, list)

    def test_retention_below_168_does_not_raise(self, tmp_path):
        p = tmp_path / "t"
        pl.DataFrame({"a": [1]}).write_delta(str(p))
        # Would raise without enforce_retention_duration=False
        vacuum_delta(p, retention_hours=1, dry_run=True)


class TestOptimizeDelta:
    def test_compact_returns_metrics(self, tmp_path):
        p = tmp_path / "t"
        pl.DataFrame({"a": [1]}).write_delta(str(p))
        pl.DataFrame({"a": [2]}).write_delta(str(p), mode="append")
        metrics = optimize_delta(p)
        assert isinstance(metrics, dict)

    def test_z_order_returns_metrics(self, tmp_path):
        p = tmp_path / "t"
        pl.DataFrame({"a": [1, 2, 3], "b": [3, 2, 1]}).write_delta(str(p))
        pl.DataFrame({"a": [4], "b": [0]}).write_delta(str(p), mode="append")
        metrics = optimize_delta(p, z_order_columns=["a"])
        assert isinstance(metrics, dict)
        # table still readable after optimize
        assert DeltaTable(str(p)).to_pyarrow_table().num_rows == 4
