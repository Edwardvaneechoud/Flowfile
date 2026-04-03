"""
Test script for verifying Polars connectivity to fake-gcs-server.

Prerequisites:
    1. Start the fake-gcs-server: start_fake_gcs_container()
    2. Populate test data: populate_test_data()
"""

import gcsfs
import polars as pl

from test_utils.gcs.data_generator import populate_test_data
from test_utils.gcs.fixtures import GCS_ENDPOINT_URL, get_gcs_client

# Single set of options — fsspec/gcsfs for everything
FSSPEC_OPTIONS = {
    "token": "anon",
    "endpoint_url": GCS_ENDPOINT_URL,
}

EXPECTED_BUCKETS = {"test-bucket", "flowfile-test", "sample-data", "worker-test-bucket"}
EXPECTED_ROWS = 100_000
BUCKET = "test-bucket"


def test_bucket_discovery():
    client = get_gcs_client()
    bucket_names = {b.name for b in client.list_buckets()}
    assert EXPECTED_BUCKETS.issubset(bucket_names), f"Missing buckets: {EXPECTED_BUCKETS - bucket_names}"
    print("✓ All expected buckets found")


def test_blob_round_trip():
    client = get_gcs_client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob("hello.txt")
    blob.upload_from_string(b"it works!")
    assert blob.download_as_text() == "it works!"
    blob.delete()
    print("✓ Blob write/read round-trip successful")


def test_read_single_csv():
    df = pl.read_csv(f"gs://{BUCKET}/single-file-csv/data.csv", storage_options=FSSPEC_OPTIONS)
    assert df.shape[0] == EXPECTED_ROWS
    print(f"✓ Single CSV: {df.shape[0]} rows, {df.shape[1]} columns")


def test_read_multi_csv():
    fs = gcsfs.GCSFileSystem(**FSSPEC_OPTIONS)
    files = fs.glob(f"{BUCKET}/multi-file-csv/*.csv")
    dfs = [pl.read_csv(fs.open(f)) for f in files]
    df = pl.concat(dfs)
    assert df.shape[0] == EXPECTED_ROWS
    print(f"✓ Multi CSV: {df.shape[0]} rows from {len(files)} files")


def test_read_single_parquet():
    df = pl.read_parquet(
        f"gs://{BUCKET}/single-file-parquet/data.parquet",
        storage_options=FSSPEC_OPTIONS,
        use_pyarrow=True,
    )
    assert df.shape[0] == EXPECTED_ROWS
    print(f"✓ Single Parquet: {df.shape[0]} rows, {df.shape[1]} columns")


def test_read_multi_parquet():
    fs = gcsfs.GCSFileSystem(**FSSPEC_OPTIONS)
    files = fs.glob(f"{BUCKET}/multi-file-parquet/*.parquet")
    dfs = [pl.read_parquet(fs.open(f)) for f in files]
    df = pl.concat(dfs)
    assert df.shape[0] == EXPECTED_ROWS
    print(f"✓ Multi Parquet: {df.shape[0]} rows from {len(files)} files")


def test_read_single_json():
    fs = gcsfs.GCSFileSystem(**FSSPEC_OPTIONS)
    with fs.open(f"{BUCKET}/single-file-json/data.json") as f:
        df = pl.read_ndjson(f)
    assert df.shape[0] == EXPECTED_ROWS
    print(f"✓ Single JSON: {df.shape[0]} rows, {df.shape[1]} columns")

def test_read_multi_json():
    fs = gcsfs.GCSFileSystem(**FSSPEC_OPTIONS)
    files = fs.glob(f"{BUCKET}/multi-file-json/*.json")
    dfs = [pl.read_ndjson(fs.open(f)) for f in files]
    df = pl.concat(dfs)
    assert df.shape[0] == EXPECTED_ROWS
    print(f"✓ Multi JSON: {df.shape[0]} rows from {len(files)} files")


def test_parquet_with_filter():
    fs = gcsfs.GCSFileSystem(**FSSPEC_OPTIONS)
    files = fs.glob(f"{BUCKET}/multi-file-parquet/*.parquet")
    dfs = [pl.read_parquet(fs.open(f)) for f in files]
    df = pl.concat(dfs).filter(pl.col("category") == "A")
    expected = EXPECTED_ROWS // 5
    assert df.shape[0] == expected
    print(f"✓ Parquet filter: {df.shape[0]} rows (category='A')")


if __name__ == "__main__":
    print("Populating test data...\n")
    populate_test_data()
    print()

    tests = [
        test_bucket_discovery,
        test_blob_round_trip,
        test_read_single_csv,
        test_read_multi_csv,
        test_read_single_parquet,
        test_read_multi_parquet,
        test_read_single_json,
        test_read_multi_json,
        test_parquet_with_filter,
    ]

    passed, failed = 0, 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ {test.__name__}: {e}")