import pytest

from shared.cloud_storage.uri import (
    CLOUD_URI_SCHEMES,
    build_uri,
    canonical_scheme,
    is_cloud_uri,
    parse_uri,
    scheme_of,
    uri_basename,
    uri_is_root,
    uri_join,
    uri_parent,
)


class TestIsCloudUri:
    @pytest.mark.parametrize("uri", ["s3://b/k", "gs://b", "az://c/p", "abfss://c/p", "gcs://b", "adl://c"])
    def test_recognises_cloud_schemes(self, uri):
        assert is_cloud_uri(uri)

    @pytest.mark.parametrize("value", ["/tmp/data", "C:\\data", "~/files", "data/file.csv", "", "s3:/b/k"])
    def test_rejects_local_paths(self, value):
        assert not is_cloud_uri(value)


class TestCanonicalScheme:
    def test_known_types(self):
        assert canonical_scheme("s3") == "s3://"
        assert canonical_scheme("adls") == "az://"
        assert canonical_scheme("gcs") == "gs://"

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported storage type"):
            canonical_scheme("hdfs")

    def test_every_canonical_scheme_is_a_known_scheme(self):
        for storage_type in ("s3", "adls", "gcs"):
            assert canonical_scheme(storage_type) in CLOUD_URI_SCHEMES


class TestSchemeOf:
    def test_returns_prefix(self):
        assert scheme_of("s3://bucket/key") == "s3://"
        assert scheme_of("abfss://container/path") == "abfss://"

    def test_local_path_returns_empty(self):
        assert scheme_of("/tmp/data") == ""


class TestParseUri:
    def test_full_uri(self):
        parsed = parse_uri("s3://bucket/folder/file.csv")
        assert (parsed.scheme, parsed.container, parsed.key) == ("s3://", "bucket", "folder/file.csv")
        assert not parsed.is_root

    def test_bucket_only(self):
        parsed = parse_uri("s3://bucket")
        assert (parsed.container, parsed.key) == ("bucket", "")

    def test_trailing_slash_is_ignored(self):
        assert parse_uri("s3://bucket/folder/") == parse_uri("s3://bucket/folder")

    def test_scheme_only_is_root(self):
        parsed = parse_uri("s3://")
        assert parsed.is_root
        assert (parsed.container, parsed.key) == ("", "")

    def test_local_path_raises(self):
        with pytest.raises(ValueError, match="Not a cloud storage URI"):
            parse_uri("/tmp/data")


class TestBuildUri:
    def test_round_trips(self):
        for uri in ("s3://", "s3://bucket", "s3://bucket/a/b.csv", "az://container/p"):
            parsed = parse_uri(uri)
            assert build_uri(parsed.scheme, parsed.container, parsed.key) == uri

    def test_strips_redundant_slashes(self):
        assert build_uri("s3://", "bucket", "/a/b/") == "s3://bucket/a/b"


class TestUriParent:
    def test_walks_up_key_segments(self):
        assert uri_parent("s3://bucket/a/b/file.csv") == "s3://bucket/a/b"
        assert uri_parent("s3://bucket/a/b") == "s3://bucket/a"

    def test_last_key_segment_yields_the_bucket(self):
        assert uri_parent("s3://bucket/a") == "s3://bucket"

    def test_bucket_yields_the_listing_root_not_a_local_path(self):
        assert uri_parent("s3://bucket") == "s3://"

    def test_root_is_its_own_parent(self):
        assert uri_parent("s3://") == "s3://"

    def test_trailing_slash_does_not_add_a_level(self):
        assert uri_parent("s3://bucket/a/b/") == "s3://bucket/a"

    @pytest.mark.parametrize("scheme", ["gs://", "az://", "abfss://", "gcs://"])
    def test_preserves_scheme(self, scheme):
        assert uri_parent(f"{scheme}container/a/b") == f"{scheme}container/a"


class TestUriIsRoot:
    def test_scheme_only(self):
        assert uri_is_root("s3://")
        assert uri_is_root("gs://")

    def test_bucket_is_not_root(self):
        assert not uri_is_root("s3://bucket")


class TestUriJoin:
    def test_joins_bucket_onto_root(self):
        assert uri_join("s3://", "bucket") == "s3://bucket"

    def test_joins_key_onto_bucket(self):
        assert uri_join("s3://bucket", "folder") == "s3://bucket/folder"

    def test_joins_onto_trailing_slash(self):
        assert uri_join("s3://bucket/a/", "b") == "s3://bucket/a/b"

    def test_child_slashes_are_trimmed(self):
        assert uri_join("s3://bucket", "/folder/") == "s3://bucket/folder"

    def test_empty_child_is_a_noop(self):
        assert uri_join("s3://bucket/a", "") == "s3://bucket/a"

    def test_never_collapses_the_scheme(self):
        joined = uri_join(uri_join("s3://", "bucket"), "key")
        assert joined.startswith("s3://")
        assert joined == "s3://bucket/key"


class TestUriBasename:
    def test_object_name(self):
        assert uri_basename("s3://bucket/a/b/file.csv") == "file.csv"

    def test_prefix_name(self):
        assert uri_basename("s3://bucket/a/b/") == "b"

    def test_container_name(self):
        assert uri_basename("s3://bucket") == "bucket"

    def test_root_falls_back_to_scheme(self):
        assert uri_basename("s3://") == "s3://"
