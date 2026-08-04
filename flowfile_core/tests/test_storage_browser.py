"""Endpoint tests for /storage_browser/cloud that need neither Docker nor a real bucket."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    store_cloud_connection,
)
from flowfile_core.routes import storage_browser
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from shared.cloud_storage import browse
from shared.cloud_storage.browse import BrowseEntry, BrowseResult

NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
BROWSE_URL = "/storage_browser/cloud"


def get_test_client() -> TestClient:
    with TestClient(main.app) as bootstrap:
        token = bootstrap.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    return authed


client = get_test_client()


@pytest.fixture
def s3_connection():
    name = "browse_test_s3"
    _drop_connection(name)
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=name,
                storage_type="s3",
                auth_method="access_key",
                aws_region="eu-west-1",
                aws_access_key_id="AKIAEXAMPLE",
                aws_secret_access_key="s3secret",
            ),
            1,
        )
    yield name
    _drop_connection(name)


@pytest.fixture
def adls_service_principal_connection():
    name = "browse_test_adls_sp"
    _drop_connection(name)
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=name,
                storage_type="adls",
                auth_method="service_principal",
                azure_tenant_id="tenant",
                azure_client_id="client",
                azure_client_secret="shhh",
            ),
            1,
        )
    yield name
    _drop_connection(name)


def _drop_connection(name: str) -> None:
    with get_db_context() as db:
        try:
            delete_cloud_connection(db, name, 1)
        except Exception:
            pass


@pytest.fixture
def stub_listing(monkeypatch):
    """Replace the provider call so route behaviour can be tested without a bucket."""

    def _install(result=None, error=None):
        captured = {}

        def _fake(storage_type, uri, storage_options, **kwargs):
            captured["storage_type"] = storage_type
            captured["uri"] = uri
            captured["storage_options"] = storage_options
            captured.update(kwargs)
            if error:
                raise error
            return result or BrowseResult(path="s3://bucket")

        monkeypatch.setattr(storage_browser, "list_cloud_uri", _fake)
        return captured

    return _install


class TestConnectionResolution:
    def test_unknown_connection_is_not_found(self):
        response = client.get(BROWSE_URL, params={"connection_name": "does-not-exist"})
        assert response.status_code == 404
        assert response.json()["detail"]["error_code"] == "CONNECTION_NOT_FOUND"

    def test_literal_none_connection_name_is_treated_as_unset(self, stub_listing):
        # AuthSettingsInput.connection_name defaults to the string "None".
        captured = stub_listing()
        response = client.get(BROWSE_URL, params={"connection_name": "None"})
        assert response.status_code == 200
        assert captured["storage_type"] == "s3"

    def test_ambient_credentials_work_on_this_single_user_install(self, stub_listing):
        captured = stub_listing()
        assert client.get(BROWSE_URL).status_code == 200
        assert captured["storage_type"] == "s3"

    def test_ambient_credentials_are_refused_for_non_s3_storage(self):
        response = client.get(BROWSE_URL, params={"storage_type": "adls"})
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "CONNECTION_REQUIRED"

    def test_connection_credentials_are_used_for_the_listing(self, stub_listing, s3_connection):
        captured = stub_listing()
        client.get(BROWSE_URL, params={"connection_name": s3_connection})
        assert captured["storage_options"]["aws_access_key_id"] == "AKIAEXAMPLE"


class TestAmbientCredentialsGate:
    """The gate is a pure function so it can be checked without flipping the process mode.

    Flipping FLOWFILE_MODE process-wide breaks the module-level TestClients that mint
    electron tokens at import; the docker route path is covered in tests/sharing/.
    """

    @pytest.mark.parametrize("mode", ["electron", "package"])
    def test_allowed_on_single_user_installs(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        assert storage_browser.ambient_credentials_allowed() is True

    def test_allowed_when_the_mode_is_unset(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_MODE", raising=False)
        assert storage_browser.ambient_credentials_allowed() is True

    def test_refused_in_docker(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        assert storage_browser.ambient_credentials_allowed() is False

    def test_the_mode_is_read_live_not_cached_at_import(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        assert storage_browser.ambient_credentials_allowed() is False
        monkeypatch.setenv("FLOWFILE_MODE", "electron")
        assert storage_browser.ambient_credentials_allowed() is True


class TestUnsupportedAuth:
    def test_adls_service_principal_is_refused_with_a_reason(self, adls_service_principal_connection):
        response = client.get(BROWSE_URL, params={"connection_name": adls_service_principal_connection})
        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["error_code"] == "BROWSE_UNSUPPORTED_AUTH"
        assert "manually" in detail["message"]


class TestResponseShape:
    def test_entries_are_shaped_like_file_info(self, stub_listing, s3_connection):
        stub_listing(
            BrowseResult(
                path="s3://bucket/data",
                entries=[
                    BrowseEntry(
                        name="sales",
                        uri="s3://bucket/data/sales",
                        is_directory=True,
                        entry_kind="prefix",
                    ),
                    BrowseEntry(
                        name="customers.parquet",
                        uri="s3://bucket/data/customers.parquet",
                        is_directory=False,
                        entry_kind="object",
                        size=2048,
                        last_modified=NOW,
                    ),
                ],
            )
        )
        body = client.get(BROWSE_URL, params={"connection_name": s3_connection}).json()

        directory, obj = body["entries"]
        assert set(directory) >= {
            "name",
            "path",
            "is_directory",
            "size",
            "file_type",
            "last_modified",
            "created_date",
            "is_hidden",
            "exists",
        }
        assert (directory["name"], directory["path"], directory["is_directory"]) == (
            "sales",
            "s3://bucket/data/sales",
            True,
        )
        assert directory["size"] == 0
        assert directory["last_modified"] is None
        assert (obj["file_type"], obj["size"]) == ("parquet", 2048)
        assert obj["created_date"] is None

    def test_parent_path_walks_up_one_level(self, stub_listing, s3_connection):
        stub_listing(BrowseResult(path="s3://bucket/data/sales"))
        body = client.get(BROWSE_URL, params={"connection_name": s3_connection, "path": "s3://bucket/data/sales"}).json()
        assert body["parent_path"] == "s3://bucket/data"
        assert body["is_root"] is False

    def test_scheme_root_has_no_parent(self, stub_listing, s3_connection):
        stub_listing(BrowseResult(path="s3://"))
        body = client.get(BROWSE_URL, params={"connection_name": s3_connection}).json()
        assert body["is_root"] is True
        assert body["parent_path"] is None

    def test_root_listing_denial_is_a_200_not_an_error(self, stub_listing, s3_connection):
        stub_listing(BrowseResult(path="s3://", root_listing_denied=True))
        response = client.get(BROWSE_URL, params={"connection_name": s3_connection})
        assert response.status_code == 200
        assert response.json()["root_listing_denied"] is True

    def test_delta_table_flag_is_surfaced(self, stub_listing, s3_connection):
        stub_listing(BrowseResult(path="s3://bucket/tbl", current_is_delta_table=True))
        body = client.get(BROWSE_URL, params={"connection_name": s3_connection}).json()
        assert body["current_is_delta_table"] is True

    def test_pagination_fields_round_trip(self, stub_listing, s3_connection):
        captured = stub_listing(BrowseResult(path="s3://bucket", truncated=True, next_token="tok"))
        body = client.get(
            BROWSE_URL, params={"connection_name": s3_connection, "page_size": 25, "page_token": "prev"}
        ).json()
        assert captured["page_size"] == 25
        assert captured["page_token"] == "prev"
        assert (body["truncated"], body["next_token"]) == (True, "tok")


class TestErrorMapping:
    @pytest.mark.parametrize(
        "error,status,code",
        [
            (browse.BrowseAccessDenied("denied"), 403, "ACCESS_DENIED"),
            (browse.BrowseNotFound("gone"), 404, "PATH_NOT_FOUND"),
            (browse.BrowseTimeout("slow"), 504, "PROVIDER_TIMEOUT"),
            (browse.BrowseError("boom"), 502, "PROVIDER_ERROR"),
            (browse.BrowseInvalidPath("bad"), 400, "INVALID_PATH"),
        ],
    )
    def test_browse_errors_map_to_typed_payloads(self, stub_listing, s3_connection, error, status, code):
        stub_listing(error=error)
        response = client.get(BROWSE_URL, params={"connection_name": s3_connection})
        assert response.status_code == status
        assert response.json()["detail"]["error_code"] == code

    def test_no_failure_mode_returns_401(self, stub_listing, s3_connection):
        # A 401 would be read as JWT expiry by the axios interceptor and log the user out.
        for error in (
            browse.BrowseAccessDenied("denied"),
            browse.BrowseNotFound("gone"),
            browse.BrowseUnsupported("nope"),
            browse.BrowseTimeout("slow"),
            browse.BrowseError("boom"),
        ):
            stub_listing(error=error)
            assert client.get(BROWSE_URL, params={"connection_name": s3_connection}).status_code != 401

    def test_a_bad_path_is_rejected_before_reaching_a_provider(self, s3_connection):
        response = client.get(BROWSE_URL, params={"connection_name": s3_connection, "path": "s3://bucket/../etc"})
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "INVALID_PATH"

    def test_secrets_never_appear_in_a_response(self, s3_connection):
        # Point at an unroutable endpoint so the provider genuinely fails.
        response = client.get(
            BROWSE_URL, params={"connection_name": s3_connection, "path": "s3://bucket/data"}
        )
        assert "s3secret" not in response.text
        assert "AKIAEXAMPLE" not in response.text


class TestAuthentication:
    def test_endpoint_requires_a_token(self):
        with TestClient(main.app) as anonymous:
            assert anonymous.get(BROWSE_URL).status_code == 401
