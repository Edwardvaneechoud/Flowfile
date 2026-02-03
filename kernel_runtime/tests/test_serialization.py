"""Tests for the serialization module.

Covers:
- Format detection
- Serialization/deserialization round-trips
- File and bytes serialization
- SHA-256 computation
"""

import io
import tempfile
from pathlib import Path

import pytest

from kernel_runtime.serialization import (
    compute_sha256_bytes,
    compute_sha256_file,
    deserialize_from_bytes,
    deserialize_from_file,
    detect_format,
    serialize_to_bytes,
    serialize_to_file,
)


# ---------------------------------------------------------------------------
# Helpers (must be defined before use in decorators)
# ---------------------------------------------------------------------------


def _has_polars() -> bool:
    """Check if polars is installed."""
    try:
        import polars
        return True
    except ImportError:
        return False


def _has_pandas() -> bool:
    """Check if pandas is installed."""
    try:
        import pandas
        return True
    except ImportError:
        return False


def _has_numpy() -> bool:
    """Check if numpy is installed."""
    try:
        import numpy
        return True
    except ImportError:
        return False


def _has_sklearn() -> bool:
    """Check if sklearn is installed."""
    try:
        import sklearn
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Format Detection Tests
# ---------------------------------------------------------------------------


class TestDetectFormat:
    """Tests for automatic format detection."""

    def test_detect_dict_returns_pickle(self):
        """Plain dict should use pickle format."""
        obj = {"key": "value", "nested": {"a": 1}}
        assert detect_format(obj) == "pickle"

    def test_detect_list_returns_pickle(self):
        """List should use pickle format."""
        obj = [1, 2, 3, "hello"]
        assert detect_format(obj) == "pickle"

    def test_detect_custom_class_returns_pickle(self):
        """Custom class instances should use pickle format."""
        class MyClass:
            def __init__(self):
                self.value = 42

        obj = MyClass()
        assert detect_format(obj) == "pickle"

    @pytest.mark.skipif(
        not _has_polars(),
        reason="polars not installed",
    )
    def test_detect_polars_dataframe_returns_parquet(self):
        """Polars DataFrame should use parquet format."""
        import polars as pl
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert detect_format(df) == "parquet"

    @pytest.mark.skipif(
        not _has_pandas(),
        reason="pandas not installed",
    )
    def test_detect_pandas_dataframe_returns_parquet(self):
        """Pandas DataFrame should use parquet format."""
        import pandas as pd
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert detect_format(df) == "parquet"

    @pytest.mark.skipif(
        not _has_numpy(),
        reason="numpy not installed",
    )
    def test_detect_numpy_array_returns_joblib(self):
        """NumPy array should use joblib format."""
        import numpy as np
        arr = np.array([1, 2, 3, 4, 5])
        assert detect_format(arr) == "joblib"

    @pytest.mark.skipif(
        not _has_sklearn(),
        reason="sklearn not installed",
    )
    def test_detect_sklearn_model_returns_joblib(self):
        """Scikit-learn model should use joblib format."""
        from sklearn.linear_model import LinearRegression
        model = LinearRegression()
        assert detect_format(model) == "joblib"


# ---------------------------------------------------------------------------
# Serialization Round-Trip Tests
# ---------------------------------------------------------------------------


class TestSerializeToFile:
    """Tests for file-based serialization."""

    def test_serialize_dict_to_file(self, tmp_path):
        """Should serialize dict to pickle file."""
        obj = {"a": 1, "b": [1, 2, 3], "c": {"nested": True}}
        path = tmp_path / "test.pkl"

        sha256 = serialize_to_file(obj, str(path), "pickle")

        assert path.exists()
        assert len(sha256) == 64  # SHA-256 hex digest
        assert path.stat().st_size > 0

    def test_serialize_and_deserialize_dict(self, tmp_path):
        """Should round-trip dict through file serialization."""
        obj = {"key": "value", "number": 42, "list": [1, 2, 3]}
        path = tmp_path / "roundtrip.pkl"

        serialize_to_file(obj, str(path), "pickle")
        result = deserialize_from_file(str(path), "pickle")

        assert result == obj

    @pytest.mark.skipif(
        not _has_polars(),
        reason="polars not installed",
    )
    def test_serialize_polars_dataframe(self, tmp_path):
        """Should round-trip Polars DataFrame through parquet."""
        import polars as pl
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })
        path = tmp_path / "dataframe.parquet"

        sha256 = serialize_to_file(df, str(path), "parquet")
        result = deserialize_from_file(str(path), "parquet")

        assert len(sha256) == 64
        assert result.equals(df)

    @pytest.mark.skipif(
        not _has_numpy(),
        reason="numpy not installed",
    )
    def test_serialize_numpy_array(self, tmp_path):
        """Should round-trip NumPy array through joblib."""
        import numpy as np
        arr = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        path = tmp_path / "array.joblib"

        sha256 = serialize_to_file(arr, str(path), "joblib")
        result = deserialize_from_file(str(path), "joblib")

        assert len(sha256) == 64
        assert np.array_equal(result, arr)

    @pytest.mark.skipif(
        not _has_sklearn(),
        reason="sklearn not installed",
    )
    def test_serialize_sklearn_model(self, tmp_path):
        """Should round-trip sklearn model through joblib."""
        import numpy as np
        from sklearn.linear_model import LinearRegression

        # Create and fit a simple model
        X = np.array([[1], [2], [3], [4], [5]])
        y = np.array([2, 4, 6, 8, 10])
        model = LinearRegression()
        model.fit(X, y)

        path = tmp_path / "model.joblib"
        sha256 = serialize_to_file(model, str(path), "joblib")
        result = deserialize_from_file(str(path), "joblib")

        # Model should produce same predictions
        assert len(sha256) == 64
        X_test = np.array([[6], [7]])
        assert np.allclose(result.predict(X_test), model.predict(X_test))

    def test_serialize_creates_parent_directories(self, tmp_path):
        """Should create parent directories if they don't exist."""
        obj = {"test": "data"}
        path = tmp_path / "nested" / "deep" / "test.pkl"

        serialize_to_file(obj, str(path), "pickle")

        assert path.exists()


class TestSerializeToBytes:
    """Tests for in-memory bytes serialization."""

    def test_serialize_dict_to_bytes(self):
        """Should serialize dict to bytes."""
        obj = {"key": "value"}

        blob, sha256 = serialize_to_bytes(obj, "pickle")

        assert isinstance(blob, bytes)
        assert len(blob) > 0
        assert len(sha256) == 64

    def test_serialize_and_deserialize_bytes(self):
        """Should round-trip through bytes serialization."""
        obj = {"nested": {"data": [1, 2, 3]}, "value": 42}

        blob, sha256 = serialize_to_bytes(obj, "pickle")
        result = deserialize_from_bytes(blob, "pickle")

        assert result == obj

    @pytest.mark.skipif(
        not _has_polars(),
        reason="polars not installed",
    )
    def test_serialize_polars_to_bytes(self):
        """Should round-trip Polars DataFrame through bytes."""
        import polars as pl
        df = pl.DataFrame({"col": [1, 2, 3]})

        blob, sha256 = serialize_to_bytes(df, "parquet")
        result = deserialize_from_bytes(blob, "parquet")

        assert result.equals(df)

    @pytest.mark.skipif(
        not _has_numpy(),
        reason="numpy not installed",
    )
    def test_serialize_numpy_to_bytes(self):
        """Should round-trip NumPy array through bytes."""
        import numpy as np
        arr = np.array([1.0, 2.0, 3.0])

        blob, sha256 = serialize_to_bytes(arr, "joblib")
        result = deserialize_from_bytes(blob, "joblib")

        assert np.array_equal(result, arr)


# ---------------------------------------------------------------------------
# SHA-256 Tests
# ---------------------------------------------------------------------------


class TestSHA256:
    """Tests for SHA-256 computation."""

    def test_compute_sha256_file(self, tmp_path):
        """Should compute correct SHA-256 for file."""
        test_data = b"Hello, World!"
        path = tmp_path / "test.bin"
        path.write_bytes(test_data)

        sha256 = compute_sha256_file(path)

        # Known SHA-256 for "Hello, World!"
        expected = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
        assert sha256 == expected

    def test_compute_sha256_bytes(self):
        """Should compute correct SHA-256 for bytes."""
        test_data = b"Hello, World!"

        sha256 = compute_sha256_bytes(test_data)

        expected = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
        assert sha256 == expected

    def test_sha256_consistency(self, tmp_path):
        """File and bytes SHA-256 should match for same data."""
        test_data = b"Test data for SHA-256 consistency check"
        path = tmp_path / "consistency.bin"
        path.write_bytes(test_data)

        file_sha256 = compute_sha256_file(path)
        bytes_sha256 = compute_sha256_bytes(test_data)

        assert file_sha256 == bytes_sha256

    def test_sha256_different_for_different_data(self, tmp_path):
        """Different data should produce different SHA-256."""
        path1 = tmp_path / "file1.bin"
        path2 = tmp_path / "file2.bin"
        path1.write_bytes(b"data 1")
        path2.write_bytes(b"data 2")

        sha1 = compute_sha256_file(path1)
        sha2 = compute_sha256_file(path2)

        assert sha1 != sha2

    def test_sha256_large_file(self, tmp_path):
        """Should handle large files efficiently."""
        # Create a 10MB file
        path = tmp_path / "large.bin"
        large_data = b"x" * (10 * 1024 * 1024)
        path.write_bytes(large_data)

        sha256 = compute_sha256_file(path)

        assert len(sha256) == 64


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dict(self, tmp_path):
        """Should handle empty dict."""
        obj = {}
        path = tmp_path / "empty.pkl"

        serialize_to_file(obj, str(path), "pickle")
        result = deserialize_from_file(str(path), "pickle")

        assert result == {}

    def test_none_value(self, tmp_path):
        """Should handle None value."""
        obj = None
        path = tmp_path / "none.pkl"

        serialize_to_file(obj, str(path), "pickle")
        result = deserialize_from_file(str(path), "pickle")

        assert result is None

    def test_nested_complex_structure(self, tmp_path):
        """Should handle deeply nested structures."""
        obj = {
            "level1": {
                "level2": {
                    "level3": {
                        "data": [1, 2, {"inner": "value"}],
                        "tuple": (1, 2, 3),
                    }
                }
            }
        }
        path = tmp_path / "nested.pkl"

        serialize_to_file(obj, str(path), "pickle")
        result = deserialize_from_file(str(path), "pickle")

        assert result["level1"]["level2"]["level3"]["data"] == [1, 2, {"inner": "value"}]

    def test_unicode_content(self, tmp_path):
        """Should handle unicode content."""
        obj = {"emoji": "🎉", "chinese": "你好", "arabic": "مرحبا"}
        path = tmp_path / "unicode.pkl"

        serialize_to_file(obj, str(path), "pickle")
        result = deserialize_from_file(str(path), "pickle")

        assert result == obj

    def test_binary_data_in_dict(self, tmp_path):
        """Should handle binary data in dict."""
        obj = {"binary": b"\x00\x01\x02\xff\xfe", "text": "normal"}
        path = tmp_path / "binary.pkl"

        serialize_to_file(obj, str(path), "pickle")
        result = deserialize_from_file(str(path), "pickle")

        assert result == obj
