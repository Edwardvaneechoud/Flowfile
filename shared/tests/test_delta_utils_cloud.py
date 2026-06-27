"""Object-storage (S3/MinIO) coverage for the shared Delta helpers.

The S3 round-trips are gated on Docker (MinIO via ``managed_minio``). The
``validate_catalog_uri`` guards and the ``storage_options=None`` regression run
everywhere — the latter is the byte-for-byte "unset config == today" guard.
"""

import polars as pl
import pytest

from shared.cloud_storage.storage_options import build_storage_options
from shared.delta_utils import (
    get_delta_size_bytes,
    merge_into_delta,
    validate_catalog_uri,
    write_delta,
)

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available, managed_minio
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available, managed_minio

requires_docker = pytest.mark.skipif(not is_docker_available(), reason="Docker required for MinIO")


def _minio_storage_options() -> dict:
    return build_storage_options(
        storage_type="s3",
        auth_method="access_key",
        aws_region="us-east-1",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        endpoint_url="http://localhost:9000",
        aws_allow_unsafe_html=True,
    )


# ---- Docker-free guards / regression --------------------------------------- #


def test_validate_catalog_uri_joins_and_guards():
    assert validate_catalog_uri("t1", "s3://bucket/catalog") == "s3://bucket/catalog/t1"
    assert validate_catalog_uri("t1", "s3://bucket/catalog/") == "s3://bucket/catalog/t1"
    for bad in ("../evil", "a/b", "a\\b", "", "x\x00y"):
        with pytest.raises(ValueError):
            validate_catalog_uri(bad, "s3://bucket/catalog")


def test_write_delta_local_regression(tmp_path):
    """storage_options=None ⇒ identical local behavior (no regression)."""
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    dest = str(tmp_path / "local_delta")
    assert write_delta(df, dest, mode="overwrite") is True
    assert get_delta_size_bytes(dest) > 0
    assert pl.scan_delta(dest).collect().height == 3


def test_merge_local_regression(tmp_path):
    dest = str(tmp_path / "local_merge")
    merge_into_delta(pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}), dest, merge_mode="upsert", merge_keys=["id"])
    merge_into_delta(pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}), dest, merge_mode="upsert", merge_keys=["id"])
    res = pl.scan_delta(dest).collect().sort("id")
    assert res["v"].to_list() == ["a", "B", "c"]


# ---- S3 round-trips (Docker) ----------------------------------------------- #


@requires_docker
def test_write_and_size_roundtrip_s3():
    with managed_minio():
        opts = _minio_storage_options()
        uri = "s3://flowfile-test/du_write"
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert write_delta(df, uri, mode="overwrite", storage_options=opts) is True
        assert get_delta_size_bytes(uri, storage_options=opts) > 0
        assert pl.scan_delta(uri, storage_options=opts).collect().height == 3
        client = get_minio_client()
        listed = client.list_objects_v2(Bucket="flowfile-test", Prefix="du_write/_delta_log/")
        assert listed.get("KeyCount", 0) > 0


@requires_docker
def test_merge_upsert_update_delete_s3():
    with managed_minio():
        opts = _minio_storage_options()
        uri = "s3://flowfile-test/du_merge"
        merge_into_delta(
            pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}),
            uri,
            merge_mode="upsert",
            merge_keys=["id"],
            storage_options=opts,
        )
        # upsert: update id=2, insert id=3
        merge_into_delta(
            pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
            uri,
            merge_mode="upsert",
            merge_keys=["id"],
            storage_options=opts,
        )
        res = pl.scan_delta(uri, storage_options=opts).collect().sort("id")
        assert res["v"].to_list() == ["a", "B", "c"]
        # delete id=1
        merge_into_delta(
            pl.DataFrame({"id": [1]}),
            uri,
            merge_mode="delete",
            merge_keys=["id"],
            storage_options=opts,
        )
        remaining = pl.scan_delta(uri, storage_options=opts).collect()
        assert sorted(remaining["id"].to_list()) == [2, 3]
