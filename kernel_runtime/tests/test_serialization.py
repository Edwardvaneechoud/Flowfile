"""Tests for the serialization module."""

import pickle
import tempfile
from pathlib import Path

import polars as pl
import pytest

from kernel_runtime.serialization import (
    compute_sha256_file,
    deserialize_from_bytes,
    deserialize_from_file,
    detect_format,
    serialize_to_bytes,
    serialize_to_file,
)


class TestDetectFormat:
    def test_polars_dataframe(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        assert detect_format(df) == "parquet"

    def test_polars_lazyframe(self):
        lf = pl.LazyFrame({"a": [1, 2, 3]})
        # LazyFrame module is still "polars"
        assert detect_format(lf) == "parquet"

    def test_dict(self):
        assert detect_format({"key": "value"}) == "pickle"

    def test_list(self):
        assert detect_format([1, 2, 3]) == "pickle"

    def test_string(self):
        assert detect_format("hello") == "pickle"


class TestSerializeToFile:
    def test_pickle_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "test.pkl")
            obj = {"accuracy": 0.95, "data": [1, 2, 3]}

            sha256 = serialize_to_file(obj, path, "pickle")
            assert sha256  # non-empty hash
            assert Path(path).exists()

            loaded = deserialize_from_file(path, "pickle")
            assert loaded == obj

    def test_parquet_roundtrip_polars(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "test.parquet")
            df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})

            sha256 = serialize_to_file(df, path, "parquet")
            assert sha256
            assert Path(path).exists()

            loaded = deserialize_from_file(path, "parquet")
            assert loaded.equals(df)

    def test_sha256_consistent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "test.pkl")
            obj = "deterministic content"

            sha256 = serialize_to_file(obj, path, "pickle")
            recomputed = compute_sha256_file(Path(path))
            assert sha256 == recomputed


class TestSerializeToBytes:
    def test_pickle_roundtrip(self):
        obj = {"key": [1, 2, 3]}
        blob, sha256 = serialize_to_bytes(obj, "pickle")
        assert isinstance(blob, bytes)
        assert len(sha256) == 64  # SHA-256 hex

        loaded = deserialize_from_bytes(blob, "pickle")
        assert loaded == obj

    def test_parquet_roundtrip(self):
        df = pl.DataFrame({"a": [10, 20], "b": [True, False]})
        blob, sha256 = serialize_to_bytes(df, "parquet")
        assert isinstance(blob, bytes)

        loaded = deserialize_from_bytes(blob, "parquet")
        assert loaded.equals(df)


class TestComputeSha256:
    def test_known_value(self):
        """Test SHA-256 against a known value."""
        import hashlib

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "known.bin"
            content = b"hello world"
            path.write_bytes(content)

            expected = hashlib.sha256(content).hexdigest()
            actual = compute_sha256_file(path)
            assert actual == expected

    def test_empty_file(self):
        import hashlib

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.bin"
            path.write_bytes(b"")

            expected = hashlib.sha256(b"").hexdigest()
            actual = compute_sha256_file(path)
            assert actual == expected
