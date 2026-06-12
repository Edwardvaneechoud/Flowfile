"""Verify partition_by threads from the frame API into the cloud storage writer node."""

from __future__ import annotations

import pytest

import flowfile_frame as ff

try:
    # noinspection PyUnresolvedReferences
    from tests.utils import is_docker_available
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_frame/tests/utils.py")))
    # noinspection PyUnresolvedReferences
    from utils import is_docker_available


def _writer_settings(df, child):
    return df.flow_graph.get_node(child.node_id).setting_input.cloud_storage_settings


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_write_delta_threads_partition_by():
    df = ff.from_dict({"a": [1, 2], "b": ["x", "y"]})
    child = df.write_delta(
        "s3://flowfile-test/partition_thread_tbl", connection_name="minio-flowframe-test", partition_by=["b"]
    )
    settings = _writer_settings(df, child)
    assert settings.partition_by == ["b"]
    assert settings.file_format == "delta"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_write_delta_default_no_partition():
    df = ff.from_dict({"a": [1, 2], "b": ["x", "y"]})
    child = df.write_delta("s3://flowfile-test/partition_thread_tbl2", connection_name="minio-flowframe-test")
    assert _writer_settings(df, child).partition_by is None


def test_write_to_cloud_storage_rejects_partition_for_non_delta():
    df = ff.from_dict({"a": [1, 2]})
    with pytest.raises(ValueError, match="only supported for the 'delta'"):
        ff.write_to_cloud_storage(df, "s3://bucket/f.parquet", file_format="parquet", partition_by=["a"])
