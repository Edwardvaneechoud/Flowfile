"""Serialization utilities for global artifacts.

Handles detecting the best serialization format for a given Python object,
serializing/deserializing to files and byte buffers, and computing SHA-256
integrity hashes.
"""

import hashlib
import io
from pathlib import Path
from typing import Any

# Modules whose objects should use joblib serialization
JOBLIB_MODULES = {
    "sklearn",
    "numpy",
    "scipy",
    "xgboost",
    "lightgbm",
    "catboost",
}


def detect_format(obj: Any) -> str:
    """Auto-detect best serialization format for an object.

    - polars/pandas DataFrames -> parquet
    - sklearn/numpy/scipy/xgboost/lightgbm/catboost -> joblib
    - Everything else -> pickle
    """
    module = type(obj).__module__.split(".")[0]

    if module in ("polars", "pandas"):
        return "parquet"

    if module in JOBLIB_MODULES:
        return "joblib"

    return "pickle"


def serialize_to_file(obj: Any, path: str, fmt: str | None = None) -> str:
    """Serialize object to file. Returns SHA-256 hash."""
    fmt = fmt or detect_format(obj)
    file_path = Path(path)

    if fmt == "parquet":
        _serialize_parquet(obj, file_path)
    elif fmt == "joblib":
        _serialize_joblib(obj, file_path)
    else:
        _serialize_pickle(obj, file_path)

    return compute_sha256_file(file_path)


def serialize_to_bytes(obj: Any, fmt: str | None = None) -> tuple[bytes, str]:
    """Serialize object to bytes. Returns (blob, sha256).

    Used for S3 uploads where we need the bytes in memory.
    """
    fmt = fmt or detect_format(obj)
    buf = io.BytesIO()

    if fmt == "parquet":
        _serialize_parquet_buffer(obj, buf)
    elif fmt == "joblib":
        import joblib
        joblib.dump(obj, buf)
    else:
        import pickle
        pickle.dump(obj, buf, protocol=5)

    blob = buf.getvalue()
    sha256 = hashlib.sha256(blob).hexdigest()
    return blob, sha256


def deserialize_from_file(path: str, fmt: str) -> Any:
    """Deserialize object from file."""
    file_path = Path(path)

    if fmt == "parquet":
        return _deserialize_parquet(file_path)
    elif fmt == "joblib":
        import joblib
        return joblib.load(file_path)
    else:
        import pickle
        with open(file_path, "rb") as f:
            return pickle.load(f)  # noqa: S301


def deserialize_from_bytes(blob: bytes, fmt: str) -> Any:
    """Deserialize object from bytes."""
    buf = io.BytesIO(blob)

    if fmt == "parquet":
        return _deserialize_parquet_buffer(buf)
    elif fmt == "joblib":
        import joblib
        return joblib.load(buf)
    else:
        import pickle
        return pickle.load(buf)  # noqa: S301


def compute_sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file using streaming reads."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# -- Private helpers --


def _serialize_parquet(obj: Any, path: Path) -> None:
    module = type(obj).__module__.split(".")[0]
    if module == "polars":
        obj.write_parquet(path)
    else:  # pandas
        obj.to_parquet(path)


def _serialize_parquet_buffer(obj: Any, buf: io.BytesIO) -> None:
    module = type(obj).__module__.split(".")[0]
    if module == "polars":
        obj.write_parquet(buf)
    else:
        obj.to_parquet(buf)


def _deserialize_parquet(path: Path) -> Any:
    import polars as pl
    return pl.read_parquet(path)


def _deserialize_parquet_buffer(buf: io.BytesIO) -> Any:
    import polars as pl
    return pl.read_parquet(buf)


def _serialize_joblib(obj: Any, path: Path) -> None:
    import joblib
    joblib.dump(obj, path)


def _serialize_pickle(obj: Any, path: Path) -> None:
    import pickle
    with open(path, "wb") as f:
        pickle.dump(obj, f, protocol=5)
