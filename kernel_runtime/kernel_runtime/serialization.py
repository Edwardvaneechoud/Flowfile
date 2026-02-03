"""Serialization utilities for global artifacts.

This module handles serialization and deserialization of Python objects
for the global artifact store, with automatic format detection based on
object type.

Supported formats:
- parquet: For Polars and Pandas DataFrames
- joblib: For scikit-learn models and numpy arrays
- pickle: For general Python objects
"""

from __future__ import annotations

import hashlib
import io
import pickle
from pathlib import Path
from typing import Any

# Modules that should use joblib for serialization
JOBLIB_MODULES = {
    "sklearn",
    "numpy",
    "scipy",
    "xgboost",
    "lightgbm",
    "catboost",
}


class UnpickleableObjectError(TypeError):
    """Raised when an object cannot be serialized for global artifact storage."""

    pass


def check_pickleable(obj: Any) -> None:
    """Verify that an object can be pickled.

    This check is performed before attempting to publish an object to the
    global artifact store, providing a clear error message if the object
    cannot be serialized.

    Args:
        obj: Python object to check.

    Raises:
        UnpickleableObjectError: If the object cannot be pickled, with a
            helpful message explaining why and how to fix it.

    Common reasons for unpickleable objects:
    - Lambda functions or nested functions
    - Classes defined inside functions (local classes)
    - Objects with open file handles or network connections
    - Objects containing ctypes or other C extensions
    """
    try:
        # Use pickle.dumps to test pickleability without writing to disk
        # Use protocol 5 (same as actual serialization)
        pickle.dumps(obj, protocol=5)
    except (pickle.PicklingError, TypeError, AttributeError) as e:
        obj_type = f"{type(obj).__module__}.{type(obj).__name__}"

        # Provide specific guidance based on error type
        if "local object" in str(e) or "local class" in str(e).lower():
            hint = (
                "Classes defined inside functions cannot be pickled. "
                "Move the class definition to module level."
            )
        elif "lambda" in str(e).lower():
            hint = (
                "Lambda functions cannot be pickled. "
                "Define a regular function instead."
            )
        elif "file" in str(e).lower() or "socket" in str(e).lower():
            hint = (
                "Objects with open file handles or network connections cannot be pickled. "
                "Close resources before publishing or extract the data you need."
            )
        else:
            hint = (
                "Ensure the object and all its attributes are pickleable. "
                "Check for lambdas, local classes, or open resources."
            )

        raise UnpickleableObjectError(
            f"Cannot publish object of type '{obj_type}' to global artifact store: {e}\n\n"
            f"Hint: {hint}"
        ) from e


def detect_format(obj: Any) -> str:
    """Auto-detect best serialization format for an object.

    Args:
        obj: Python object to serialize.

    Returns:
        Format string: "parquet", "joblib", or "pickle".
    """
    module = type(obj).__module__.split(".")[0]

    # DataFrames -> parquet
    if module in ("polars", "pandas"):
        return "parquet"

    # ML objects and numpy arrays -> joblib
    if module in JOBLIB_MODULES:
        return "joblib"

    # Everything else -> pickle
    return "pickle"


def serialize_to_file(obj: Any, path: str, format: str | None = None) -> str:
    """Serialize object to file.

    Args:
        obj: Python object to serialize.
        path: File path to write to.
        format: Serialization format (auto-detected if not specified).

    Returns:
        SHA-256 hash of the serialized data.
    """
    format = format or detect_format(obj)
    path = Path(path)

    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    if format == "parquet":
        _serialize_parquet(obj, path)
    elif format == "joblib":
        _serialize_joblib(obj, path)
    else:
        _serialize_pickle(obj, path)

    return compute_sha256_file(path)


def serialize_to_bytes(obj: Any, format: str | None = None) -> tuple[bytes, str]:
    """Serialize object to bytes.

    Args:
        obj: Python object to serialize.
        format: Serialization format (auto-detected if not specified).

    Returns:
        Tuple of (serialized bytes, SHA-256 hash).
    """
    format = format or detect_format(obj)
    buf = io.BytesIO()

    if format == "parquet":
        _serialize_parquet_buffer(obj, buf)
    elif format == "joblib":
        import joblib
        joblib.dump(obj, buf)
    else:
        import pickle
        pickle.dump(obj, buf, protocol=5)

    blob = buf.getvalue()
    sha256 = hashlib.sha256(blob).hexdigest()
    return blob, sha256


def deserialize_from_file(path: str, format: str) -> Any:
    """Deserialize object from file.

    Args:
        path: File path to read from.
        format: Serialization format used.

    Returns:
        Deserialized Python object.
    """
    path = Path(path)

    if format == "parquet":
        return _deserialize_parquet(path)
    elif format == "joblib":
        import joblib
        return joblib.load(path)
    else:
        import pickle
        with open(path, "rb") as f:
            return pickle.load(f)


def deserialize_from_bytes(blob: bytes, format: str) -> Any:
    """Deserialize object from bytes.

    Args:
        blob: Serialized bytes.
        format: Serialization format used.

    Returns:
        Deserialized Python object.
    """
    buf = io.BytesIO(blob)

    if format == "parquet":
        return _deserialize_parquet_buffer(buf)
    elif format == "joblib":
        import joblib
        return joblib.load(buf)
    else:
        import pickle
        return pickle.load(buf)


def compute_sha256_file(path: Path) -> str:
    """Compute SHA-256 hash of a file using streaming.

    Uses 8MB chunks to handle large files without loading into memory.

    Args:
        path: Path to the file.

    Returns:
        SHA-256 hash as hexadecimal string.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_sha256_bytes(data: bytes) -> str:
    """Compute SHA-256 hash of bytes.

    Args:
        data: Bytes to hash.

    Returns:
        SHA-256 hash as hexadecimal string.
    """
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------
# Private helpers
# --------------------------------------------------------------------------


def _serialize_parquet(obj: Any, path: Path) -> None:
    """Serialize DataFrame to parquet file."""
    module = type(obj).__module__.split(".")[0]
    if module == "polars":
        obj.write_parquet(path)
    else:  # pandas
        obj.to_parquet(path)


def _serialize_parquet_buffer(obj: Any, buf: io.BytesIO) -> None:
    """Serialize DataFrame to parquet in memory buffer."""
    module = type(obj).__module__.split(".")[0]
    if module == "polars":
        obj.write_parquet(buf)
    else:  # pandas
        obj.to_parquet(buf)


def _deserialize_parquet(path: Path) -> Any:
    """Deserialize parquet file to DataFrame."""
    import polars as pl
    return pl.read_parquet(path)


def _deserialize_parquet_buffer(buf: io.BytesIO) -> Any:
    """Deserialize parquet from memory buffer to DataFrame."""
    import polars as pl
    return pl.read_parquet(buf)


def _serialize_joblib(obj: Any, path: Path) -> None:
    """Serialize object using joblib."""
    import joblib
    joblib.dump(obj, path)


def _serialize_pickle(obj: Any, path: Path) -> None:
    """Serialize object using pickle protocol 5."""
    import pickle
    with open(path, "wb") as f:
        pickle.dump(obj, f, protocol=5)
