"""Unit tests for shared.delta_utils partitioning + maintenance helpers.

Covers:
- write_delta with partition_by (create, append-match, append-mismatch, ignore-empty)
- merge_into_delta create-branch partitioning
- get_delta_partition_columns
- vacuum_delta (dry_run, <168h retention guard)
- optimize_delta (compact + z_order)
- fixed-size Array -> List normalization on the way into a Delta write
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


class TestFixedSizeArrayNormalization:
    """A fixed-size ``Array`` column must survive a write/read round-trip as a ``List``.

    Delta has no fixed-size-array type, so writing an ``Array(inner, N)`` unnormalized records a
    list in the log while the Parquet files keep the fixed-size layout; ``scan_delta`` then plans
    ``List`` and collects ``Array``, raising a dtype mismatch on every later read.
    """

    @staticmethod
    def _embeddings(ids: list[int], vectors: list[list[float]], width: int = 3) -> pl.DataFrame:
        return pl.DataFrame(
            {"id": ids, "emb": vectors},
            schema={"id": pl.Int64, "emb": pl.Array(pl.Float32, width)},
        )

    def test_array_column_reads_back_as_list(self, tmp_path):
        p = tmp_path / "t"
        df = self._embeddings([1, 2], [[0.5, 1.5, 2.5], [3.5, 4.5, 5.5]])
        assert df.schema["emb"] == pl.Array(pl.Float32, 3)

        write_delta(df, str(p), mode="overwrite")

        lf = pl.scan_delta(str(p))
        assert lf.collect_schema()["emb"] == pl.List(pl.Float32)
        out = lf.collect().sort("id")  # no dtype-mismatch SchemaError
        assert out.schema["emb"] == pl.List(pl.Float32)
        assert out["emb"].to_list() == [[0.5, 1.5, 2.5], [3.5, 4.5, 5.5]]

    def test_lazyframe_array_column_reads_back_as_list(self, tmp_path):
        p = tmp_path / "t"
        df = self._embeddings([1, 2], [[0.5, 1.5, 2.5], [3.5, 4.5, 5.5]])

        write_delta(df.lazy(), str(p), mode="overwrite")

        out = pl.scan_delta(str(p)).collect().sort("id")  # sink_delta does not preserve row order
        assert out.schema["emb"] == pl.List(pl.Float32)
        assert out["emb"].to_list() == [[0.5, 1.5, 2.5], [3.5, 4.5, 5.5]]

    def test_append_array_column(self, tmp_path):
        p = tmp_path / "t"
        write_delta(self._embeddings([1], [[0.5, 1.5, 2.5]]), str(p), mode="overwrite")
        write_delta(self._embeddings([2], [[3.5, 4.5, 5.5]]), str(p), mode="append")

        out = pl.scan_delta(str(p)).collect().sort("id")
        assert out.schema["emb"] == pl.List(pl.Float32)
        assert out["emb"].to_list() == [[0.5, 1.5, 2.5], [3.5, 4.5, 5.5]]

    def test_merge_array_column(self, tmp_path):
        p = tmp_path / "t"
        merge_into_delta(self._embeddings([1], [[0.5, 1.5, 2.5]]), str(p), merge_mode="upsert", merge_keys=["id"])
        # The create branch is the guard here: a later merge rewrites the files and would mask it.
        assert pl.scan_delta(str(p)).collect().schema["emb"] == pl.List(pl.Float32)

        merge_into_delta(
            self._embeddings([1, 2], [[9.5, 9.5, 9.5], [3.5, 4.5, 5.5]]),
            str(p),
            merge_mode="upsert",
            merge_keys=["id"],
        )

        out = pl.scan_delta(str(p)).collect().sort("id")
        assert out.schema["emb"] == pl.List(pl.Float32)
        assert out["emb"].to_list() == [[9.5, 9.5, 9.5], [3.5, 4.5, 5.5]]

    def test_non_array_columns_untouched(self, tmp_path):
        p = tmp_path / "t"
        df = pl.DataFrame(
            {"id": [1], "name": ["x"], "tags": [["a", "b"]], "emb": [[0.5, 1.5]]},
            schema={
                "id": pl.Int64,
                "name": pl.Utf8,
                "tags": pl.List(pl.Utf8),
                "emb": pl.Array(pl.Float32, 2),
            },
        )
        write_delta(df, str(p), mode="overwrite")

        out = pl.scan_delta(str(p)).collect()
        assert out.schema == {
            "id": pl.Int64,
            "name": pl.Utf8,
            "tags": pl.List(pl.Utf8),
            "emb": pl.List(pl.Float32),
        }


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
