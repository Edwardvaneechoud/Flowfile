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


class TestSinkDeltaFallback:
    """Only a plan sink_delta cannot handle is retried eagerly; other failures surface once, uncollected."""

    @pytest.fixture
    def collect_calls(self, monkeypatch):
        from shared.cloud_storage import writers

        calls = []
        real = writers._collect_lazy_frame

        def _recording(lf):
            calls.append(lf)
            return real(lf)

        monkeypatch.setattr(writers, "_collect_lazy_frame", _recording)
        return calls

    def test_storage_errors_are_not_retried(self, collect_calls):
        options = {
            "aws_access_key_id": "AKIAEXAMPLE",
            "aws_secret_access_key": "secret",
            "aws_session_token": "",
            "aws_region": "us-east-1",
            "endpoint_url": "http://127.0.0.1:9",
            "aws_allow_http": "true",
            "max_retries": "0",
            "timeout": "2s",
        }
        with pytest.raises(Exception) as exc_info:
            write_delta_to_cloud(pl.LazyFrame({"a": [1]}), "s3://bucket/table", options, mode="append")

        assert not isinstance(exc_info.value, (NotImplementedError, pl.exceptions.InvalidOperationError))
        assert collect_calls == []

    def test_an_unsupported_sink_falls_back_to_write_delta(self, tmp_path, monkeypatch, collect_calls):
        def _unsupported(self, *args, **kwargs):
            raise pl.exceptions.InvalidOperationError("sink not supported for this plan")

        monkeypatch.setattr(pl.LazyFrame, "sink_delta", _unsupported)
        p = tmp_path / "t"
        write_delta_to_cloud(pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]}), str(p), {}, partition_by=["b"])

        assert len(collect_calls) == 1
        assert get_delta_partition_columns(p) == ["b"]
