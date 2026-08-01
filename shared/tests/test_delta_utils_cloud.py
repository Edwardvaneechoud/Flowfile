"""Object-storage (S3/MinIO) coverage for the shared Delta helpers.

The S3 round-trips connect to the MinIO mock the test job already started (via
``poetry run start_minio``); they must NOT manage that container's lifecycle —
tearing it down here would break every other MinIO-dependent test in the job.
The ``validate_catalog_uri`` guards and the ``storage_options=None`` regression
run everywhere (the latter is the byte-for-byte "unset config == today" guard).
"""

import polars as pl
import pytest

from shared.cloud_storage.storage_options import build_storage_options
from shared.delta_utils import (
    get_delta_size_bytes,
    merge_into_delta,
    scd2_into_delta,
    validate_catalog_uri,
    write_delta,
)

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available

_BUCKET = "flowfile-test"


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


def _ensure_bucket() -> None:
    client = get_minio_client()
    try:
        client.create_bucket(Bucket=_BUCKET)
    except Exception:
        pass  # already exists


def _purge_prefix(prefix: str) -> None:
    """Drop a table prefix left by an earlier run — the MinIO mock's bucket outlives the test job."""
    client = get_minio_client()
    listed = client.list_objects_v2(Bucket=_BUCKET, Prefix=f"{prefix}/")
    keys = [{"Key": obj["Key"]} for obj in listed.get("Contents", [])]
    if keys:
        client.delete_objects(Bucket=_BUCKET, Delete={"Objects": keys})


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


# ---- S3 round-trips (against the already-running MinIO mock) ---------------- #


@requires_minio
def test_write_and_size_roundtrip_s3():
    _ensure_bucket()
    opts = _minio_storage_options()
    uri = f"s3://{_BUCKET}/du_cloud_write"
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    assert write_delta(df, uri, mode="overwrite", storage_options=opts) is True
    assert get_delta_size_bytes(uri, storage_options=opts) > 0
    assert pl.scan_delta(uri, storage_options=opts).collect().height == 3
    listed = get_minio_client().list_objects_v2(Bucket=_BUCKET, Prefix="du_cloud_write/_delta_log/")
    assert listed.get("KeyCount", 0) > 0


@requires_minio
def test_merge_upsert_update_delete_s3():
    _ensure_bucket()
    opts = _minio_storage_options()
    uri = f"s3://{_BUCKET}/du_cloud_merge"
    merge_into_delta(
        pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}), uri, merge_mode="upsert", merge_keys=["id"], storage_options=opts
    )
    # upsert: update id=2, insert id=3
    merge_into_delta(
        pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}), uri, merge_mode="upsert", merge_keys=["id"], storage_options=opts
    )
    res = pl.scan_delta(uri, storage_options=opts).collect().sort("id")
    assert res["v"].to_list() == ["a", "B", "c"]
    # delete id=1
    merge_into_delta(
        pl.DataFrame({"id": [1]}), uri, merge_mode="delete", merge_keys=["id"], storage_options=opts
    )
    remaining = pl.scan_delta(uri, storage_options=opts).collect()
    assert sorted(remaining["id"].to_list()) == [2, 3]


@requires_minio
def test_scd2_initial_load_and_merge_s3():
    _ensure_bucket()
    # SCD2 is read-modify-write: a table left behind by an earlier run would make the initial
    # load a no-op skip, so this one starts from a guaranteed-absent prefix.
    _purge_prefix("du_cloud_scd2")
    opts = _minio_storage_options()
    uri = f"s3://{_BUCKET}/du_cloud_scd2"
    first = pl.DataFrame({"id": [1, 2], "v": ["a", "b"]})
    created = scd2_into_delta(
        first,
        uri,
        business_keys=["id"],
        valid_from_iso="2026-01-01T00:00:00+00:00",
        compare_columns=None,
        full_snapshot=False,
        storage_options=opts,
    )
    assert (created.created, created.rows_total, created.rows_current) == (True, 2, 2)

    changed = scd2_into_delta(
        pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
        uri,
        business_keys=["id"],
        valid_from_iso="2026-02-01T00:00:00+00:00",
        compare_columns=None,
        full_snapshot=False,
        storage_options=opts,
    )
    assert (changed.rows_inserted, changed.rows_closed) == (2, 1)

    table = pl.scan_delta(uri, storage_options=opts).collect()
    assert table.height == 4
    current = table.filter(pl.col("is_current")).sort("id")
    assert current["v"].to_list() == ["a", "B", "c"]
    assert table.filter(~pl.col("is_current"))["id"].to_list() == [2]

    # Re-running the identical batch must be a true no-op on object storage too.
    rerun = scd2_into_delta(
        pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
        uri,
        business_keys=["id"],
        valid_from_iso="2026-03-01T00:00:00+00:00",
        compare_columns=None,
        full_snapshot=False,
        storage_options=opts,
    )
    assert rerun.skipped is True
