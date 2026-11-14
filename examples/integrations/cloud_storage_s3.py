"""Round-trip a Parquet aggregate through S3 with a stored cloud connection."""

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
s3_path = f"s3://flowfile-test/city_income_{uuid4().hex}.parquet"

# --8<-- [start:example]
income_by_city = (
    ff.read_csv("data/templates/supermarket_sales.csv")
    .group_by("city")
    .agg(ff.col("gross_income").sum().alias("total_income"))
)

income_by_city.write_parquet_to_cloud_storage(s3_path, connection_name="analytics-s3")

reloaded = ff.scan_parquet_from_cloud_storage(s3_path, connection_name="analytics-s3").collect()
# --8<-- [end:example]

from polars.testing import assert_frame_equal

assert reloaded.height == 5
assert_frame_equal(
    income_by_city.collect().sort("city"),
    reloaded.sort("city"),
    check_row_order=False,
)

by_city = {row["city"]: round(row["total_income"], 2) for row in reloaded.to_dicts()}
assert by_city == {
    "Bago": 3051.94,
    "Mandalay": 3520.57,
    "Naypyitaw": 2521.64,
    "Taunggyi": 3525.60,
    "Yangon": 3312.56,
}
