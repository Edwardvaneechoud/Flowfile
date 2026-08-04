"""Integration tests for /storage_browser/cloud against MinIO.

The most important case here is the round trip: a path the browser produced must be
a path the cloud reader can actually read. Listing and reading are separate code
paths (boto3 vs Polars object_store) and could drift on scheme or trailing slash.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    store_cloud_connection,
)
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

try:
    from tests.flowfile_core_test_utils import is_docker_available
except ModuleNotFoundError:
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import is_docker_available

BROWSE_URL = "/storage_browser/cloud"
CONNECTION_NAME = "browse_minio_it"


def _minio_available() -> bool:
    if not is_docker_available():
        return False
    try:
        from test_utils.s3.fixtures import wait_for_minio

        return bool(wait_for_minio(max_retries=2))
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _minio_available(), reason="MinIO is not running (poetry run start_minio)")


def get_test_client() -> TestClient:
    with TestClient(main.app) as bootstrap:
        token = bootstrap.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    return authed


client = get_test_client()


def _store_connection(name: str, access_key: str = "minioadmin", secret_key: str = "minioadmin") -> None:
    with get_db_context() as db:
        try:
            delete_cloud_connection(db, name, 1)
        except Exception:
            pass
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=name,
                storage_type="s3",
                auth_method="access_key",
                aws_region="us-east-1",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_allow_unsafe_html=True,
                endpoint_url="http://localhost:9000",
                verify_ssl=True,
            ),
            1,
        )


@pytest.fixture(scope="module", autouse=True)
def minio_connection():
    _store_connection(CONNECTION_NAME)
    yield CONNECTION_NAME
    with get_db_context() as db:
        try:
            delete_cloud_connection(db, CONNECTION_NAME, 1)
        except Exception:
            pass


def _browse(path: str = "", **params) -> dict:
    response = client.get(BROWSE_URL, params={"connection_name": CONNECTION_NAME, "path": path, **params})
    assert response.status_code == 200, response.text
    return response.json()


def _names(body: dict) -> list[str]:
    return [entry["name"] for entry in body["entries"]]


class TestBucketListing:
    def test_scheme_root_lists_buckets(self):
        body = _browse("", page_size=100)
        assert body["is_root"] is True
        assert body["parent_path"] is None
        assert "demo-bucket" in _names(body)
        assert all(entry["is_directory"] for entry in body["entries"])
        assert all(entry["entry_kind"] == "container" for entry in body["entries"])

    def test_bucket_uris_are_navigable(self):
        body = _browse("", page_size=100)
        demo = next(entry for entry in body["entries"] if entry["name"] == "demo-bucket")
        assert demo["path"] == "s3://demo-bucket"
        assert _names(_browse(demo["path"]))


class TestPrefixListing:
    def test_bucket_root_lists_top_level_prefixes(self):
        body = _browse("s3://demo-bucket")
        assert {"data-lake", "raw-data"} <= set(_names(body))
        assert body["parent_path"] == "s3://"

    def test_no_phantom_entries_from_directory_placeholders(self):
        body = _browse("s3://demo-bucket")
        assert all(entry["name"] for entry in body["entries"])
        assert "" not in _names(body)

    def test_objects_carry_size_and_modified_time(self):
        body = _browse("s3://demo-bucket/data-lake/customers")
        parquet = next(entry for entry in body["entries"] if entry["name"] == "customers.parquet")
        assert parquet["is_directory"] is False
        assert parquet["entry_kind"] == "object"
        assert parquet["file_type"] == "parquet"
        assert parquet["size"] > 0
        assert parquet["last_modified"] is not None
        assert parquet["path"] == "s3://demo-bucket/data-lake/customers/customers.parquet"

    def test_prefixes_report_no_size_or_timestamp(self):
        body = _browse("s3://demo-bucket")
        for entry in body["entries"]:
            if entry["is_directory"]:
                assert entry["size"] == 0
                assert entry["last_modified"] is None
                assert entry["created_date"] is None

    def test_hive_partitions_are_listed_as_directories(self):
        body = _browse("s3://demo-bucket/data-lake/sales", page_size=100)
        partitions = [name for name in _names(body) if name.startswith("year=")]
        assert partitions
        assert all(entry["is_directory"] for entry in body["entries"] if entry["name"].startswith("year="))

    def test_trailing_slash_is_equivalent(self):
        assert _names(_browse("s3://demo-bucket/data-lake/")) == _names(_browse("s3://demo-bucket/data-lake"))

    def test_parent_path_walks_back_up(self):
        body = _browse("s3://demo-bucket/data-lake/customers")
        assert body["parent_path"] == "s3://demo-bucket/data-lake"


class TestDeltaDetection:
    def test_a_delta_table_directory_is_flagged(self):
        body = _browse("s3://test-bucket/delta-lake-table")
        assert body["current_is_delta_table"] is True

    def test_an_ordinary_prefix_is_not_flagged(self):
        assert _browse("s3://demo-bucket/data-lake")["current_is_delta_table"] is False


class TestPagination:
    def test_paging_covers_the_same_entries_as_one_big_page(self):
        full = _names(_browse("s3://demo-bucket/data-lake/sales", page_size=1000))
        assert len(full) > 1

        collected, token, guard = [], None, 0
        while guard < 50:
            guard += 1
            body = _browse("s3://demo-bucket/data-lake/sales", page_size=1, **({"page_token": token} if token else {}))
            collected.extend(_names(body))
            token = body["next_token"]
            if not token:
                break
        assert sorted(collected) == sorted(full)

    def test_last_page_reports_no_further_token(self):
        body = _browse("s3://demo-bucket/data-lake/customers", page_size=1000)
        assert body["truncated"] is False
        assert body["next_token"] is None

    def test_a_token_from_another_listing_is_rejected(self):
        response = client.get(
            BROWSE_URL,
            params={"connection_name": CONNECTION_NAME, "path": "s3://demo-bucket", "page_token": "bogus-token"},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "INVALID_PATH"


class TestFailures:
    def test_missing_bucket_is_not_found(self):
        response = client.get(
            BROWSE_URL, params={"connection_name": CONNECTION_NAME, "path": "s3://no-such-bucket-xyz"}
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error_code"] == "PATH_NOT_FOUND"

    def test_bad_credentials_are_denied_without_echoing_the_secret(self):
        name = "browse_minio_bad_creds"
        _store_connection(name, access_key="wrongkey", secret_key="wrongsecret")
        try:
            response = client.get(BROWSE_URL, params={"connection_name": name, "path": "s3://demo-bucket"})
            assert response.status_code in (403, 502)
            assert "wrongsecret" not in response.text
            assert "wrongkey" not in response.text
        finally:
            with get_db_context() as db:
                delete_cloud_connection(db, name, 1)


class TestBrowseReadRoundTrip:
    """A path the browser emits must be readable by the cloud reader unchanged."""

    def _connection(self) -> FullCloudStorageConnection:
        from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection_schema

        with get_db_context() as db:
            return get_cloud_connection_schema(db, CONNECTION_NAME, 1)

    def test_a_browsed_file_path_reads_as_a_single_file(self):
        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        from flowfile_core.schemas.cloud_storage_schemas import (
            CloudStorageReadSettings,
            CloudStorageReadSettingsInternal,
        )

        body = _browse("s3://demo-bucket/data-lake/customers")
        parquet = next(entry for entry in body["entries"] if entry["name"] == "customers.parquet")

        engine = FlowDataEngine.from_cloud_storage_obj(
            CloudStorageReadSettingsInternal(
                read_settings=CloudStorageReadSettings(
                    resource_path=parquet["path"],
                    auth_mode="access_key",
                    connection_name=CONNECTION_NAME,
                    scan_mode="single_file",
                    file_format="parquet",
                ),
                connection=self._connection(),
            )
        )
        assert engine.data_frame.collect_schema().names()

    def test_a_browsed_prefix_reads_as_a_directory_scan(self):
        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        from flowfile_core.schemas.cloud_storage_schemas import (
            CloudStorageReadSettings,
            CloudStorageReadSettingsInternal,
        )

        body = _browse("s3://demo-bucket/data-lake")
        customers = next(entry for entry in body["entries"] if entry["name"] == "customers")

        engine = FlowDataEngine.from_cloud_storage_obj(
            CloudStorageReadSettingsInternal(
                read_settings=CloudStorageReadSettings(
                    resource_path=customers["path"],
                    auth_mode="access_key",
                    connection_name=CONNECTION_NAME,
                    scan_mode="directory",
                    file_format="parquet",
                ),
                connection=self._connection(),
            )
        )
        assert engine.data_frame.collect_schema().names()

    def test_the_browser_never_emits_a_wildcard_or_trailing_slash(self):
        # A wildcard would be re-globbed by ensure_path_has_wildcard_pattern on a
        # format switch, producing s3://.../**/*.parquet/**/*.csv.
        for path in ("", "s3://demo-bucket", "s3://demo-bucket/data-lake", "s3://demo-bucket/data-lake/customers"):
            for entry in _browse(path, page_size=100)["entries"]:
                assert "*" not in entry["path"]
                assert not entry["path"].endswith("/")
