"""Upsert into a change-tracked Delta table on S3, then read back only the rows that changed."""

import os
from uuid import uuid4

from pydantic import SecretStr

import flowfile as ff
from flowfile import FullCloudStorageConnection

# MinIO test fixture wiring — a real S3 bucket needs neither endpoint_url nor these keys.
endpoint_url = os.environ.get("DOCS_S3_ENDPOINT_URL", "http://localhost:9000")
ff.create_cloud_storage_connection_if_not_exists(
    FullCloudStorageConnection(
        connection_name="analytics-s3",
        storage_type="s3",
        auth_method="access_key",
        aws_region="us-east-1",
        endpoint_url=endpoint_url,
        aws_allow_unsafe_html=True,
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr("minioadmin"),
    )
)
table_path = f"s3://flowfile-test/{uuid4().hex}/orders"

# --8<-- [start:example]
# The first write creates the table (version 0) with its change feed on.
day1 = ff.from_dict({"order_id": [1, 2], "status": ["new", "new"]})
day1.write_delta(
    table_path, connection_name="analytics-s3",
    write_mode="upsert", merge_keys=["order_id"], track_changes=True,
)

day2 = ff.from_dict({"order_id": [2, 3], "status": ["shipped", "new"]})
day2.write_delta(
    table_path, connection_name="analytics-s3",
    write_mode="upsert", merge_keys=["order_id"], track_changes=True,
)

changes = ff.scan_delta(table_path, connection_name="analytics-s3", changes_since=0).collect()
print(changes.select("order_id", "status", "_change_type").sort("order_id"))
# --8<-- [end:example]

assert {"_change_type", "_commit_version", "_commit_timestamp"} <= set(changes.columns)
assert sorted(zip(changes["order_id"], changes["status"], changes["_change_type"], strict=True)) == [
    (2, "shipped", "update_postimage"),
    (3, "new", "insert"),
]
assert changes["_commit_version"].unique().to_list() == [1]
