"""The S3 read path builds its boto3 client through the shared builder, with botocore's default config."""

from shared.cloud_storage import directory
from shared.cloud_storage.directory import get_first_file_from_s3_dir


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kwargs):
        return iter(self._pages)


class _Client:
    def __init__(self, pages):
        self._pages = pages

    def get_paginator(self, _name):
        return _Paginator(self._pages)


def test_read_path_uses_the_shared_builder_with_default_config(monkeypatch):
    captured = {}

    def _capture(options, **kwargs):
        captured["options"] = options
        captured["kwargs"] = kwargs
        return _Client([{"Contents": [{"Key": "prefix/part-0.parquet"}]}])

    monkeypatch.setattr(directory, "build_s3_client", _capture)
    options = {"aws_access_key_id": "k", "endpoint_url": "http://localhost:9000"}
    first = get_first_file_from_s3_dir("s3://bucket/prefix/**/*.parquet", options)
    assert first == "s3://bucket/prefix/part-0.parquet"
    assert captured["options"] is options
    assert captured["kwargs"] == {}
