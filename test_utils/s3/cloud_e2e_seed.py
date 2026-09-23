"""Deterministic source data for the cloud storage end-to-end suites.

The frame mirrors the columns of the parquet file the cloud-writer bug report was built on
(``number_of_records`` … ``right_value_right``) and carries null ``category`` values, so the
replayed ``ifnull([category], "na")`` formula has something to fill. Seeding overwrites the
object with identical bytes, so it is safe to repeat.
"""

import io
import sys

import polars as pl

from test_utils.s3.fixtures import MINIO_ENDPOINT_URL, get_minio_client, wait_for_minio

SEED_BUCKET = "flowfile-test"
DEFAULT_PREFIX = "cloud-e2e"


def seed_frame() -> pl.DataFrame:
    """Six rows in the source schema; two of them have a null ``category``."""
    return pl.DataFrame(
        {
            "number_of_records": pl.Series([6] * 6, dtype=pl.UInt32),
            "id": pl.Series([1, 2, 3, 4, 5, 6], dtype=pl.Int64),
            "name": [f"user_{i}" for i in range(1, 7)],
            "category": ["A", "B", None, "A", None, "C"],
            "right_id": pl.Series([1, 1, 1, 2, 2, 2], dtype=pl.Int64),
            "right_name": ["user_1", "user_1", "user_1", "user_2", "user_2", "user_2"],
            "right_value": [10.5, 21.0, 31.5, 42.0, 52.5, 63.0],
            "right_category": ["A", "A", "A", "B", "B", "B"],
            "right_value_right": [10.5, 10.5, 10.5, 42.0, 42.0, 42.0],
        }
    )


def seed_cloud_e2e(prefix: str = DEFAULT_PREFIX) -> str:
    """Write the seed frame to ``s3://flowfile-test/<prefix>/source.parquet`` and return that URI."""
    client = get_minio_client()
    existing = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if SEED_BUCKET not in existing:
        client.create_bucket(Bucket=SEED_BUCKET)
    buffer = io.BytesIO()
    seed_frame().write_parquet(buffer)
    key = f"{prefix.strip('/')}/source.parquet"
    client.put_object(Bucket=SEED_BUCKET, Key=key, Body=buffer.getvalue())
    return f"s3://{SEED_BUCKET}/{key}"


def delete_prefix(prefix: str) -> int:
    """Delete everything under ``s3://flowfile-test/<prefix>/`` and return how many objects went."""
    prefix = prefix.strip("/")
    if not prefix:
        raise ValueError("refusing to delete the whole bucket: pass a prefix")
    client = get_minio_client()
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=SEED_BUCKET, Prefix=f"{prefix}/")
    keys = [{"Key": obj["Key"]} for page in pages for obj in page.get("Contents", [])]
    for start in range(0, len(keys), 1000):
        client.delete_objects(Bucket=SEED_BUCKET, Delete={"Objects": keys[start : start + 1000]})
    return len(keys)


def main(argv: list[str] | None = None) -> int:
    """Console script: seed the fixed ``cloud-e2e`` prefix the Playwright spec reads.

    ``--delete <prefix>`` instead removes a run's output prefix (the spec's own cleanup).
    """
    argv = sys.argv[1:] if argv is None else argv
    if not wait_for_minio(max_retries=5):
        print(f"MinIO is not reachable at {MINIO_ENDPOINT_URL}; start it with 'poetry run start_minio'.")
        return 1
    if argv[:1] == ["--delete"] and len(argv) == 2:
        print(f"Deleted {delete_prefix(argv[1])} objects under s3://{SEED_BUCKET}/{argv[1].strip('/')}/")
        return 0
    if argv:
        print("usage: seed_cloud_e2e [--delete <prefix>]")
        return 2
    print(f"Seeded {seed_cloud_e2e()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
