from datetime import datetime, timezone

import pytest

from shared.cloud_storage import browse
from shared.cloud_storage.browse import (
    BrowseAccessDenied,
    BrowseError,
    BrowseInvalidPath,
    BrowseNotFound,
    BrowseTimeout,
    BrowseUnsupported,
    browse_support,
    list_cloud_uri,
)

NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


class FakeS3Client:
    """Stands in for a boto3 S3 client; records calls and replays canned pages."""

    def __init__(self, pages=None, buckets=None, error=None):
        self.pages = pages or []
        self.buckets = buckets or []
        self.error = error
        self.calls = []

    def list_objects_v2(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.pages.pop(0)

    def list_buckets(self):
        self.calls.append({"op": "list_buckets"})
        if self.error:
            raise self.error
        return {"Buckets": self.buckets}


def _client_error(code: str):
    from botocore.exceptions import ClientError

    return ClientError({"Error": {"Code": code, "Message": "denied"}}, "ListObjectsV2")


@pytest.fixture
def use_fake_s3(monkeypatch):
    def _install(client):
        monkeypatch.setattr(browse, "_build_s3_client", lambda _options: client)
        return client

    return _install


class TestBrowseSupport:
    @pytest.mark.parametrize(
        "storage_type,auth_method",
        [("s3", "access_key"), ("s3", "iam_role"), ("s3", "aws-cli"), ("gcs", "service_account")],
    )
    def test_supported_combinations(self, storage_type, auth_method):
        assert browse_support(storage_type, auth_method) == (True, None)

    @pytest.mark.parametrize("auth_method", ["access_key", "sas_token"])
    def test_adls_key_based_auth_is_supported(self, auth_method):
        assert browse_support("adls", auth_method) == (True, None)

    @pytest.mark.parametrize("auth_method", ["service_principal", "managed_identity"])
    def test_adls_identity_auth_degrades_with_a_reason(self, auth_method):
        supported, reason = browse_support("adls", auth_method)
        assert supported is False
        assert reason and "manually" in reason

    def test_unknown_storage_type(self):
        supported, reason = browse_support("hdfs", "access_key")
        assert supported is False
        assert "hdfs" in reason


class TestNormalizeBrowseUri:
    def test_empty_uri_is_the_container_root(self):
        parsed = browse._normalize_browse_uri("", "s3")
        assert parsed.is_root
        assert parsed.scheme == "s3://"

    def test_adls_spellings_converge_on_the_canonical_scheme(self):
        for spelling in ("az://c/p", "abfs://c/p", "abfss://c/p"):
            parsed = browse._normalize_browse_uri(spelling, "adls")
            assert (parsed.scheme, parsed.container, parsed.key) == ("az://", "c", "p")

    def test_query_string_is_dropped_before_anything_else_sees_it(self):
        parsed = browse._normalize_browse_uri("az://c/p?sig=SECRETSAS&se=2026", "adls")
        assert parsed.key == "p"

    @pytest.mark.parametrize("bad", ["s3://bucket/../etc", "s3://bucket/*.csv", "s3://bucket/a\x00b"])
    def test_rejects_traversal_wildcards_and_control_characters(self, bad):
        with pytest.raises(BrowseInvalidPath):
            browse._normalize_browse_uri(bad, "s3")

    def test_rejects_a_local_path(self):
        with pytest.raises(BrowseInvalidPath, match="Not a cloud storage URI"):
            browse._normalize_browse_uri("/tmp/data", "s3")

    def test_rejects_an_overlong_path(self):
        with pytest.raises(BrowseInvalidPath, match="too long"):
            browse._normalize_browse_uri("s3://bucket/" + "a" * 3000, "s3")


class TestPageTokens:
    def test_round_trip(self):
        token = browse._encode_token("s3o", "abc")
        assert browse._decode_token("s3o", token) == "abc"

    def test_absent_token_is_none(self):
        assert browse._decode_token("s3o", None) is None

    def test_token_from_another_provider_is_rejected(self):
        token = browse._encode_token("gsb", "5")
        with pytest.raises(BrowseInvalidPath):
            browse._decode_token("s3o", token)

    def test_garbage_is_rejected(self):
        with pytest.raises(BrowseInvalidPath):
            browse._decode_token("s3o", "!!!not-base64!!!")


class TestBuildS3Client:
    @pytest.fixture
    def captured_kwargs(self, monkeypatch):
        """Capture what _build_s3_client hands boto3, without constructing a real client."""
        import boto3

        captured = {}

        def _fake_client(service, **kwargs):
            captured["service"] = service
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(boto3, "client", _fake_client)
        return captured

    def test_string_false_verify_is_coerced_to_a_boolean(self, captured_kwargs):
        # build_s3_storage_options emits the *string* "False", which is truthy in Python.
        browse._build_s3_client({"aws_access_key_id": "k", "aws_secret_access_key": "s", "verify": "False"})
        assert captured_kwargs["verify"] is False

    def test_truthy_verify_stays_true(self, captured_kwargs):
        browse._build_s3_client({"aws_access_key_id": "k", "verify": True})
        assert captured_kwargs["verify"] is True

    def test_aws_region_is_translated_to_region_name(self, captured_kwargs):
        browse._build_s3_client({"aws_access_key_id": "k", "aws_region": "eu-west-1"})
        assert captured_kwargs["region_name"] == "eu-west-1"
        assert "aws_region" not in captured_kwargs

    def test_object_store_only_keys_are_dropped_not_forwarded(self, captured_kwargs):
        # boto3.client() raises on unknown kwargs, so forwarding these would break every listing.
        browse._build_s3_client(
            {
                "aws_access_key_id": "k",
                "aws_secret_access_key": "s",
                "aws_allow_http": "true",
                "aws_virtual_hosted_style_request": "false",
            }
        )
        assert set(captured_kwargs) == {"service", "aws_access_key_id", "aws_secret_access_key", "config"}

    def test_blank_session_token_is_not_forwarded(self, captured_kwargs):
        # build_s3_storage_options blanks aws_session_token to defeat ambient env tokens.
        browse._build_s3_client({"aws_access_key_id": "k", "aws_secret_access_key": "s", "aws_session_token": ""})
        assert "aws_session_token" not in captured_kwargs

    def test_timeouts_and_retries_are_bounded(self, captured_kwargs):
        browse._build_s3_client({"aws_access_key_id": "k"})
        config = captured_kwargs["config"]
        assert config.connect_timeout == 5
        assert config.read_timeout == 15

    def test_custom_endpoint_gets_path_style_addressing(self, captured_kwargs):
        browse._build_s3_client({"aws_access_key_id": "k", "endpoint_url": "http://localhost:9000"})
        assert captured_kwargs["endpoint_url"] == "http://localhost:9000"
        assert captured_kwargs["config"].s3["addressing_style"] == "path"

    def test_aws_endpoint_keeps_default_addressing(self, captured_kwargs):
        browse._build_s3_client({"aws_access_key_id": "k"})
        assert captured_kwargs["config"].s3 is None


class TestListS3Prefix:
    def test_maps_prefixes_and_objects(self, use_fake_s3):
        use_fake_s3(
            FakeS3Client(
                pages=[
                    {
                        "CommonPrefixes": [{"Prefix": "data-lake/sales/"}],
                        "Contents": [{"Key": "data-lake/customers.parquet", "Size": 1024, "LastModified": NOW}],
                        "IsTruncated": False,
                    }
                ]
            )
        )
        result = list_cloud_uri("s3", "s3://demo-bucket/data-lake", {})

        assert result.path == "s3://demo-bucket/data-lake"
        assert [(e.name, e.is_directory, e.entry_kind) for e in result.entries] == [
            ("sales", True, "prefix"),
            ("customers.parquet", False, "object"),
        ]
        obj = result.entries[1]
        assert obj.uri == "s3://demo-bucket/data-lake/customers.parquet"
        assert (obj.size, obj.last_modified) == (1024, NOW)
        assert result.entries[0].uri == "s3://demo-bucket/data-lake/sales"

    def test_prefixes_carry_no_synthesised_size_or_timestamp(self, use_fake_s3):
        use_fake_s3(FakeS3Client(pages=[{"CommonPrefixes": [{"Prefix": "a/"}]}]))
        entry = list_cloud_uri("s3", "s3://bucket", {}).entries[0]
        assert entry.size is None
        assert entry.last_modified is None

    def test_directory_placeholder_objects_are_filtered(self, use_fake_s3):
        # Console-created folders leave a zero-byte marker whose Key is the prefix itself.
        use_fake_s3(
            FakeS3Client(
                pages=[
                    {
                        "CommonPrefixes": [{"Prefix": "folder/sub/"}],
                        "Contents": [
                            {"Key": "folder/", "Size": 0, "LastModified": NOW},
                            {"Key": "folder/real.csv", "Size": 5, "LastModified": NOW},
                        ],
                    }
                ]
            )
        )
        result = list_cloud_uri("s3", "s3://bucket/folder", {})
        assert [e.name for e in result.entries] == ["sub", "real.csv"]

    def test_self_prefix_object_is_filtered(self, use_fake_s3):
        use_fake_s3(FakeS3Client(pages=[{"Contents": [{"Key": "folder/", "Size": 0}]}]))
        assert list_cloud_uri("s3", "s3://bucket/folder", {}).entries == []

    def test_delta_log_marks_the_current_location(self, use_fake_s3):
        use_fake_s3(FakeS3Client(pages=[{"CommonPrefixes": [{"Prefix": "tbl/_delta_log/"}]}]))
        result = list_cloud_uri("s3", "s3://bucket/tbl", {})
        assert result.current_is_delta_table is True

    def test_ordinary_directory_is_not_a_delta_table(self, use_fake_s3):
        use_fake_s3(FakeS3Client(pages=[{"CommonPrefixes": [{"Prefix": "tbl/part/"}]}]))
        assert list_cloud_uri("s3", "s3://bucket/tbl", {}).current_is_delta_table is False

    def test_entries_are_sorted_directories_first_then_by_name(self, use_fake_s3):
        use_fake_s3(
            FakeS3Client(
                pages=[
                    {
                        "CommonPrefixes": [{"Prefix": "zeta/"}, {"Prefix": "alpha/"}],
                        "Contents": [{"Key": "b.csv"}, {"Key": "A.csv"}],
                    }
                ]
            )
        )
        result = list_cloud_uri("s3", "s3://bucket", {})
        assert [e.name for e in result.entries] == ["alpha", "zeta", "A.csv", "b.csv"]

    def test_prefix_and_delimiter_are_passed_correctly(self, use_fake_s3):
        client = use_fake_s3(FakeS3Client(pages=[{}]))
        list_cloud_uri("s3", "s3://bucket/a/b", {}, page_size=50)
        assert client.calls[0] == {
            "Bucket": "bucket",
            "Prefix": "a/b/",
            "Delimiter": "/",
            "MaxKeys": 50,
        }

    def test_bucket_root_uses_an_empty_prefix(self, use_fake_s3):
        client = use_fake_s3(FakeS3Client(pages=[{}]))
        list_cloud_uri("s3", "s3://bucket", {})
        assert client.calls[0]["Prefix"] == ""

    def test_truncation_surfaces_a_replayable_token(self, use_fake_s3):
        client = use_fake_s3(FakeS3Client(pages=[{"IsTruncated": True, "NextContinuationToken": "raw-token"}, {}]))
        first = list_cloud_uri("s3", "s3://bucket", {})
        assert first.truncated is True
        assert first.next_token and first.next_token != "raw-token"

        list_cloud_uri("s3", "s3://bucket", {}, page_token=first.next_token)
        assert client.calls[1]["ContinuationToken"] == "raw-token"

    def test_page_size_is_clamped(self, use_fake_s3):
        client = use_fake_s3(FakeS3Client(pages=[{}, {}]))
        list_cloud_uri("s3", "s3://bucket", {}, page_size=99999)
        assert client.calls[0]["MaxKeys"] == browse.MAX_PAGE_SIZE
        list_cloud_uri("s3", "s3://bucket", {}, page_size=0)
        assert client.calls[1]["MaxKeys"] == 1


class TestListS3Buckets:
    def test_lists_buckets_at_the_scheme_root(self, use_fake_s3):
        use_fake_s3(FakeS3Client(buckets=[{"Name": "demo-bucket", "CreationDate": NOW}]))
        result = list_cloud_uri("s3", "", {})
        assert result.path == "s3://"
        assert [(e.name, e.uri, e.entry_kind) for e in result.entries] == [
            ("demo-bucket", "s3://demo-bucket", "container")
        ]

    def test_access_denied_degrades_instead_of_raising(self, use_fake_s3):
        use_fake_s3(FakeS3Client(error=_client_error("AccessDenied")))
        result = list_cloud_uri("s3", "s3://", {})
        assert result.root_listing_denied is True
        assert result.entries == []

    def test_non_permission_failures_still_raise(self, use_fake_s3):
        use_fake_s3(FakeS3Client(error=_client_error("InternalError")))
        with pytest.raises(BrowseError) as excinfo:
            list_cloud_uri("s3", "s3://", {})
        assert not isinstance(excinfo.value, BrowseAccessDenied)

    def test_bucket_list_paginates_client_side(self, use_fake_s3):
        buckets = [{"Name": f"bucket-{index:02d}"} for index in range(5)]
        use_fake_s3(FakeS3Client(buckets=buckets))
        first = list_cloud_uri("s3", "", {}, page_size=2)
        assert [e.name for e in first.entries] == ["bucket-00", "bucket-01"]
        assert first.truncated is True

        use_fake_s3(FakeS3Client(buckets=buckets))
        second = list_cloud_uri("s3", "", {}, page_size=2, page_token=first.next_token)
        assert [e.name for e in second.entries] == ["bucket-02", "bucket-03"]

        use_fake_s3(FakeS3Client(buckets=buckets))
        third = list_cloud_uri("s3", "", {}, page_size=2, page_token=second.next_token)
        assert [e.name for e in third.entries] == ["bucket-04"]
        assert third.truncated is False
        assert third.next_token is None


class TestErrorTranslation:
    @pytest.mark.parametrize("code", ["AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken"])
    def test_permission_codes_map_to_403(self, code, use_fake_s3):
        use_fake_s3(FakeS3Client(error=_client_error(code)))
        with pytest.raises(BrowseAccessDenied) as excinfo:
            list_cloud_uri("s3", "s3://bucket/a", {})
        assert excinfo.value.status_code == 403
        assert excinfo.value.error_code == "ACCESS_DENIED"

    @pytest.mark.parametrize("code", ["NoSuchBucket", "NoSuchKey"])
    def test_missing_codes_map_to_404(self, code, use_fake_s3):
        use_fake_s3(FakeS3Client(error=_client_error(code)))
        with pytest.raises(BrowseNotFound) as excinfo:
            list_cloud_uri("s3", "s3://bucket/a", {})
        assert excinfo.value.status_code == 404

    def test_timeouts_map_to_504(self, use_fake_s3):
        use_fake_s3(FakeS3Client(error=TimeoutError("slow")))
        with pytest.raises(BrowseTimeout) as excinfo:
            list_cloud_uri("s3", "s3://bucket/a", {})
        assert excinfo.value.status_code == 504

    def test_unknown_failures_map_to_502(self, use_fake_s3):
        use_fake_s3(FakeS3Client(error=RuntimeError("boom")))
        with pytest.raises(BrowseError) as excinfo:
            list_cloud_uri("s3", "s3://bucket/a", {})
        assert excinfo.value.status_code == 502

    def test_no_error_maps_to_401(self):
        # 401 would trip the frontend's JWT-expiry interceptor and log the user out.
        statuses = {
            cls.status_code
            for cls in (
                BrowseError,
                BrowseInvalidPath,
                BrowseAccessDenied,
                BrowseNotFound,
                BrowseUnsupported,
                BrowseTimeout,
            )
        }
        assert 401 not in statuses

    def test_provider_message_is_not_echoed_back(self, use_fake_s3):
        from botocore.exceptions import ClientError

        secret = "AKIA-SUPER-SECRET-VALUE"
        error = ClientError({"Error": {"Code": "InternalError", "Message": secret}}, "ListObjectsV2")
        use_fake_s3(FakeS3Client(error=error))
        with pytest.raises(BrowseError) as excinfo:
            list_cloud_uri("s3", "s3://bucket/a", {})
        assert secret not in str(excinfo.value)


class TestAdlsCredentials:
    @pytest.fixture
    def captured_client(self, monkeypatch):
        import azure.storage.blob as blob

        captured = {}

        def _fake_client(account_url, credential=None, **kwargs):
            captured["account_url"] = account_url
            captured["credential"] = credential
            return object()

        monkeypatch.setattr(blob, "BlobServiceClient", _fake_client)
        return captured

    def test_missing_key_and_sas_raises_unsupported(self):
        with pytest.raises(BrowseUnsupported) as excinfo:
            list_cloud_uri("adls", "az://container", {"account_name": "acct", "tenant_id": "t"})
        assert excinfo.value.status_code == 409
        assert excinfo.value.error_code == "BROWSE_UNSUPPORTED_AUTH"

    def test_account_key_is_passed_as_a_plain_string(self, captured_client):
        browse._build_blob_service_client({"account_name": "acct", "account_key": "KEY"})
        assert captured_client["credential"] == "KEY"

    def test_sas_token_is_wrapped_so_it_is_not_read_as_an_account_key(self, captured_client):
        from azure.core.credentials import AzureSasCredential

        browse._build_blob_service_client({"account_name": "acct", "sas_token": "sv=2021&sig=abc"})
        assert isinstance(captured_client["credential"], AzureSasCredential)

    def test_emulator_endpoint_is_honoured(self, captured_client):
        browse._build_blob_service_client(
            {
                "account_name": "devstoreaccount1",
                "account_key": "KEY",
                "azure_storage_endpoint": "http://localhost:10000/devstoreaccount1",
            }
        )
        assert captured_client["account_url"] == "http://localhost:10000/devstoreaccount1"

    def test_default_endpoint_is_derived_from_the_account(self, captured_client):
        browse._build_blob_service_client({"account_name": "acct", "account_key": "KEY"})
        assert captured_client["account_url"] == "https://acct.blob.core.windows.net"


class TestGcsCredentials:
    def test_service_account_json_is_parsed_into_a_dict(self):
        # gcsfs reads a string token as a file path or a raw OAuth2 token, so the
        # raw key JSON would be sent as a bearer token and rejected.
        options = browse._normalize_gcs_options({"token": '{"type": "service_account", "project_id": "p"}'})
        assert options["token"] == {"type": "service_account", "project_id": "p"}

    def test_anon_token_is_left_alone(self):
        assert browse._normalize_gcs_options({"token": "anon"})["token"] == "anon"

    def test_missing_token_is_left_alone(self):
        assert browse._normalize_gcs_options({"project": "p"}) == {"project": "p"}

    def test_malformed_json_reports_a_credential_problem(self):
        with pytest.raises(BrowseAccessDenied, match="not valid JSON"):
            browse._normalize_gcs_options({"token": "{not json"})

    def test_the_original_options_are_not_mutated(self):
        original = {"token": '{"type": "service_account"}'}
        browse._normalize_gcs_options(original)
        assert original["token"] == '{"type": "service_account"}'


class TestUnsupportedStorageType:
    def test_unknown_type_raises_before_touching_a_provider(self):
        with pytest.raises(ValueError, match="Unsupported storage type"):
            list_cloud_uri("hdfs", "", {})
