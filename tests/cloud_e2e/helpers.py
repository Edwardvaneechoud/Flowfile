"""Test-process helpers shared by the cloud_e2e conftest and tests."""

from test_utils.s3 import fixtures as s3

MINIO_OPTIONS = {
    "aws_access_key_id": s3.MINIO_ACCESS_KEY,
    "aws_secret_access_key": s3.MINIO_SECRET_KEY,
    "aws_region": "us-east-1",
    "aws_endpoint_url": s3.MINIO_ENDPOINT_URL,
    "aws_allow_http": "true",
}


def list_keys(uri: str) -> list[str]:
    """Object keys under an ``s3://bucket/prefix`` URI."""
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    paginator = s3.get_minio_client().get_paginator("list_objects_v2")
    return [obj["Key"] for page in paginator.paginate(Bucket=bucket, Prefix=prefix) for obj in page.get("Contents", [])]
