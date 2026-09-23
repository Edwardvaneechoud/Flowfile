"""Cloud Delta merge modes and change-feed reads through the frame API."""

from __future__ import annotations

from uuid import uuid4

import pytest

import flowfile_frame as ff
from flowfile_frame.cloud_storage.frame_helpers import add_write_ff_to_cloud_storage

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available


def _minio_available() -> bool:
    """True only when the shared MinIO mock is reachable (never starts/stops it)."""
    if not is_docker_available():
        return False
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")


def test_merge_settings_reach_the_writer_node():
    """Built but not run: write_delta executes eagerly, so this checks the node it would add."""
    df = ff.from_dict({"id": [1, 2], "v": ["a", "b"]})
    node_id = add_write_ff_to_cloud_storage(
        "s3://flowfile-test/merge_settings_tbl",
        df.flow_graph,
        df.node_id,
        connection_name="minio-flowframe-test",
        file_format="delta",
        write_mode="upsert",
        merge_keys=["id"],
        track_changes=True,
    )
    settings = df.flow_graph.get_node(node_id).setting_input.cloud_storage_settings
    assert (settings.write_mode, settings.merge_keys, settings.track_changes) == ("upsert", ["id"], True)


def test_scan_delta_rejects_last_run():
    with pytest.raises(ValueError, match="only available for catalog tables"):
        ff.scan_delta("s3://flowfile-test/any_tbl", changes_since="last_run")


@requires_minio
def test_scan_delta_changes_since_after_upserts():
    path = f"s3://flowfile-test/frame_cdc_{uuid4().hex}"
    conn = "minio-flowframe-test"
    ff.from_dict({"id": [1, 2], "v": ["a", "b"]}).write_delta(
        path, connection_name=conn, write_mode="upsert", merge_keys=["id"], track_changes=True
    )
    ff.from_dict({"id": [2, 3], "v": ["B", "c"]}).write_delta(
        path, connection_name=conn, write_mode="upsert", merge_keys=["id"], track_changes=True
    )

    changes = ff.scan_delta(path, connection_name=conn, changes_since=0).collect()

    assert sorted(zip(changes["id"], changes["_change_type"])) == [(2, "update_postimage"), (3, "insert")]
