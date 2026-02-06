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
from pathlib import Path
from typing import Any

import cloudpickle

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


# Size threshold for pre-check (100MB) - skip full serialization for large objects
_CHECK_PICKLEABLE_SIZE_THRESHOLD = 100 * 1024 * 1024


def _make_unpickleable_error(obj: Any, original_error: Exception) -> UnpickleableObjectError:
    """Create an UnpickleableObjectError with helpful hints."""
    obj_type = f"{type(obj).__module__}.{type(obj).__name__}"
    error_str = str(original_error).lower()

    # Provide specific guidance based on error type
    if "local object" in error_str or "local class" in error_str:
        hint = (
            "Classes defined inside functions cannot be pickled. "
            "Move the class definition to module level."
        )
    elif "lambda" in error_str:
        hint = (
            "Lambda functions cannot be pickled. "
            "Define a regular function instead."
        )
    elif "file" in error_str or "socket" in error_str:
        hint = (
            "Objects with open file handles or network connections cannot be pickled. "
            "Close resources before publishing or extract the data you need."
        )
    else:
        hint = (
            "Ensure the object and all its attributes are pickleable. "
            "Check for lambdas, local classes, or open resources."
        )

    return UnpickleableObjectError(
        f"Cannot publish object of type '{obj_type}' to global artifact store: {original_error}\n\n"
        f"Hint: {hint}"
    )


def check_pickleable(obj: Any) -> None:
    """Verify that an object can be pickled.

    This check is performed before attempting to publish an object to the
    global artifact store, providing a clear error message if the object
    cannot be serialized.

    For large objects (estimated >100MB), this function skips the pre-check
    to avoid double-serialization overhead. In that case, errors will be
    caught and translated during actual serialization.

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
    # Try to estimate size - skip pre-check for large objects to avoid
    # double-serialization overhead (error will be caught during actual serialization)
    try:
        import sys
        estimated_size = sys.getsizeof(obj)
        # For containers, getsizeof only returns shallow size, so we use a heuristic
        # If it has __len__ and is large, skip the check
        if hasattr(obj, "__len__"):
            try:
                length = len(obj)
                # Rough heuristic: if many elements, likely large
                if length > 10000:
                    return  # Skip pre-check for large collections
            except TypeError:
                pass
        if estimated_size > _CHECK_PICKLEABLE_SIZE_THRESHOLD:
            return  # Skip pre-check for obviously large objects
    except (TypeError, OverflowError):
        pass  # Can't estimate size, proceed with check

    try:
        # Use cloudpickle.dumps to test pickleability without writing to disk
        # cloudpickle can handle classes defined in exec() code
        cloudpickle.dumps(obj)
    except (TypeError, AttributeError) as e:
        raise _make_unpickleable_error(obj, e) from e


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
        cloudpickle.dump(obj, buf)

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

    SECURITY: pickle.load is a known RCE vector - malicious pickle files can
    execute arbitrary code. This is acceptable here because:
    1. Artifacts are only written by kernel containers the user controls
    2. Artifacts flow: user code -> kernel -> storage -> kernel -> user code
    3. There's no path for external/untrusted data to become artifacts
    The trust boundary is the user's own code, which can already execute
    arbitrary Python in the kernel container.
    """
    path = Path(path)

    if format == "parquet":
        return _deserialize_parquet(path)
    elif format == "joblib":
        import joblib
        return joblib.load(path)
    else:
        import pickle  # cloudpickle files are compatible with standard pickle.load
        with open(path, "rb") as f:
            return pickle.load(f)


def deserialize_from_bytes(blob: bytes, format: str) -> Any:
    """Deserialize object from bytes.

    Args:
        blob: Serialized bytes.
        format: Serialization format used.

    Returns:
        Deserialized Python object.

    SECURITY: See deserialize_from_file() for trust boundary documentation.
    """
    buf = io.BytesIO(blob)

    if format == "parquet":
        return _deserialize_parquet_buffer(buf)
    elif format == "joblib":
        import joblib
        return joblib.load(buf)
    else:
        import pickle  # cloudpickle files are compatible with standard pickle.load
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
    """Serialize object using cloudpickle.

    cloudpickle can handle classes defined in exec() code, unlike standard pickle.
    """
    try:
        with open(path, "wb") as f:
            cloudpickle.dump(obj, f)
    except (TypeError, AttributeError) as e:
        # Translate to UnpickleableObjectError with helpful message
        raise _make_unpickleable_error(obj, e) from e
