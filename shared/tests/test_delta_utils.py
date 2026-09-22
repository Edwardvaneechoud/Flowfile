"""Unit tests for shared.delta_utils partitioning + maintenance helpers.

Covers:
- write_delta with partition_by (create, append-match, append-mismatch, ignore-empty)
- merge_into_delta create-branch partitioning
- get_delta_partition_columns
- vacuum_delta (dry_run, <168h retention guard)
- optimize_delta (compact + z_order)
- fixed-size Array -> List normalization on the way into a Delta write
- the Delta change data feed: enablement, the scan plugin, and the vacuum caveats
"""

import subprocess
import sys

import polars as pl
import pytest
from deltalake import DeltaTable

from shared.delta_utils import (
    CDF_COLUMNS,
    enable_change_data_feed,
    get_delta_head_version,
    get_delta_partition_columns,
    is_change_data_feed_enabled,
    merge_into_delta,
    optimize_delta,
    scan_delta_changes,
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


def test_make_json_safe_uses_the_hex_preview_encoding_for_bytes():
    from shared.delta_utils import format_binary_preview, make_json_safe

    assert make_json_safe(b"\x01\x02") == "0x0102"
    assert make_json_safe(memoryview(b"\x01")) == "0x01"
    assert make_json_safe(bytes(range(17))) == format_binary_preview(bytes(range(17)))
    assert make_json_safe(bytes(range(17))).endswith("\u2026 (17 bytes)")


class TestChangeDataFeedEnablement:
    def test_enabled_at_creation_by_the_writer(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1]}), str(p), mode="overwrite", enable_cdf=True)
        assert is_change_data_feed_enabled(p)
        assert get_delta_head_version(p) == 0

    def test_merge_create_enables_it(self, tmp_path):
        p = tmp_path / "t"
        merge_into_delta(pl.DataFrame({"a": [1]}), str(p), merge_mode="upsert", merge_keys=["a"], enable_cdf=True)
        assert is_change_data_feed_enabled(p)

    def test_enable_on_existing_table_records_its_own_version(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1]}), str(p), mode="overwrite")
        assert not is_change_data_feed_enabled(p)
        enabled_version = enable_change_data_feed(p)
        assert enabled_version == 1
        assert is_change_data_feed_enabled(p)

    def test_enable_is_idempotent(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1]}), str(p), mode="overwrite", enable_cdf=True)
        assert enable_change_data_feed(p) == 0
        assert enable_change_data_feed(p) == 0

    def test_enable_cdf_does_not_touch_an_existing_table(self, tmp_path):
        """``enable_cdf`` only configures a table the write creates; enabling is its own commit."""
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1]}), str(p), mode="overwrite")
        write_delta(pl.DataFrame({"a": [2]}), str(p), mode="append", enable_cdf=True)
        assert not is_change_data_feed_enabled(p)

    def test_property_survives_an_overwrite(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"a": [1]}), str(p), mode="overwrite", enable_cdf=True)
        write_delta(pl.DataFrame({"a": [2]}), str(p), mode="overwrite")
        assert is_change_data_feed_enabled(p)


class TestScanDeltaChanges:
    def _seed(self, path) -> int:
        write_delta(pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}), str(path), mode="overwrite", enable_cdf=True)
        merge_into_delta(
            pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}), str(path), merge_mode="upsert", merge_keys=["id"]
        )
        return get_delta_head_version(path)

    def test_window_returns_only_the_new_commits(self, tmp_path):
        p = tmp_path / "t"
        head = self._seed(p)
        df = scan_delta_changes(str(p), 1, head).collect().sort("id")
        assert df["id"].to_list() == [2, 3]
        assert df["_change_type"].to_list() == ["update_postimage", "insert"]
        assert list(df.columns)[-3:] == list(CDF_COLUMNS)

    def test_preimage_dropped_by_default_and_kept_on_request(self, tmp_path):
        p = tmp_path / "t"
        head = self._seed(p)
        assert scan_delta_changes(str(p), 1, head).collect().height == 2
        with_pre = scan_delta_changes(str(p), 1, head, include_preimage=True).collect()
        assert "update_preimage" in with_pre["_change_type"].to_list()

    def test_out_of_range_window_is_empty_with_the_full_schema(self, tmp_path):
        p = tmp_path / "t"
        head = self._seed(p)
        lf = scan_delta_changes(str(p), head + 1, head + 1)
        assert lf.collect().height == 0
        assert set(CDF_COLUMNS).issubset(lf.collect_schema().names())

    def test_projection_and_predicate_pushdown(self, tmp_path):
        p = tmp_path / "t"
        head = self._seed(p)
        lf = scan_delta_changes(str(p), 1, head)
        assert lf.select("id", "_change_type").collect().columns == ["id", "_change_type"]
        assert lf.filter(pl.col("id") == 3).collect().height == 1
        assert lf.head(1).collect().height == 1

    def test_plan_serializes_and_collects_in_another_process(self, tmp_path):
        """The core/worker contract: core ships the plan, a worker child collects it."""
        p = tmp_path / "t"
        head = self._seed(p)
        plan = tmp_path / "plan.bin"
        plan.write_bytes(scan_delta_changes(str(p), 1, head).serialize())
        code = (
            "import io, polars as pl;"
            f"print(pl.LazyFrame.deserialize(io.BytesIO(open({str(plan)!r}, 'rb').read())).collect().height)"
        )
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        assert out.stdout.strip() == "2"

    def test_since_timestamp_window(self, tmp_path):
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"id": [1]}), str(p), mode="overwrite", enable_cdf=True)
        first_commit = DeltaTable(str(p)).history(1)[0]["timestamp"]
        write_delta(pl.DataFrame({"id": [2]}), str(p), mode="append")
        from datetime import datetime, timezone

        instant = datetime.fromtimestamp(first_commit / 1000, tz=timezone.utc).isoformat()
        df = scan_delta_changes(str(p), starting_timestamp=instant).collect()
        assert set(df["id"].to_list()) == {1, 2}


class TestChangeFeedVacuumCaveats:
    def test_merge_change_data_survives_a_vacuum(self, tmp_path):
        """Merge-based writes materialize ``_change_data/``, which vacuum does not remove."""
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}), str(p), mode="overwrite", enable_cdf=True)
        merge_into_delta(pl.DataFrame({"id": [2], "v": ["B"]}), str(p), merge_mode="upsert", merge_keys=["id"])
        head = get_delta_head_version(p)
        vacuum_delta(p, retention_hours=0, dry_run=False)
        assert scan_delta_changes(str(p), head, head).collect().height == 1

    def test_overwrite_window_breaks_after_a_vacuum(self, tmp_path):
        """Pins the documented caveat: overwrite commits are reconstructed from tombstoned files."""
        p = tmp_path / "t"
        write_delta(pl.DataFrame({"id": [1]}), str(p), mode="overwrite", enable_cdf=True)
        write_delta(pl.DataFrame({"id": [2]}), str(p), mode="overwrite")
        head = get_delta_head_version(p)
        vacuum_delta(p, retention_hours=0, dry_run=False)
        with pytest.raises(Exception):
            scan_delta_changes(str(p), head, head).collect()
