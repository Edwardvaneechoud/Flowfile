"""Serialization strategy for global artifacts.

Each serialization format is represented by a ``Serializer`` that can
``dump`` a Python object to bytes and ``load`` bytes back into a Python
object.  The module picks the best serializer automatically based on the
object's type, but the caller can override with an explicit format.

Supported formats
-----------------
- **parquet** — Polars / Pandas DataFrames.  Efficient columnar format.
- **joblib** — scikit-learn models and large NumPy arrays.  Handles
  memory-mapped arrays well.
- **pickle** — Generic fallback using ``pickle`` protocol 5.

Security note: ``pickle`` / ``joblib`` deserialize arbitrary code.
Only load artifacts you trust (same deployment boundary).  The Core
never deserializes — it stores opaque blobs.  Only the Kernel that
calls ``get_global()`` runs deserialization inside its own container.
"""

from __future__ import annotations

import io
import pickle
from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """Interface for a serialization format."""

    format_name: str
    file_extension: str

    @abstractmethod
    def dumps(self, obj: Any) -> bytes:
        """Serialize *obj* to bytes."""

    @abstractmethod
    def loads(self, data: bytes) -> Any:
        """Deserialize bytes back into a Python object."""


class PickleSerializer(Serializer):
    format_name = "pickle"
    file_extension = ".pkl"

    def dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=5)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)  # noqa: S301


class JoblibSerializer(Serializer):
    format_name = "joblib"
    file_extension = ".joblib"

    def dumps(self, obj: Any) -> bytes:
        import joblib
        buf = io.BytesIO()
        joblib.dump(obj, buf, compress=3)
        return buf.getvalue()

    def loads(self, data: bytes) -> Any:
        import joblib
        return joblib.load(io.BytesIO(data))


class ParquetSerializer(Serializer):
    format_name = "parquet"
    file_extension = ".parquet"

    def dumps(self, obj: Any) -> bytes:
        import polars as pl

        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        if isinstance(obj, pl.DataFrame):
            buf = io.BytesIO()
            obj.write_parquet(buf)
            return buf.getvalue()

        # Pandas fallback
        try:
            import pandas as pd
            if isinstance(obj, pd.DataFrame):
                converted = pl.from_pandas(obj)
                buf = io.BytesIO()
                converted.write_parquet(buf)
                return buf.getvalue()
        except ImportError:
            pass

        raise TypeError(f"ParquetSerializer cannot serialize {type(obj).__name__}")

    def loads(self, data: bytes) -> Any:
        import polars as pl
        return pl.read_parquet(io.BytesIO(data))


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_SERIALIZERS: dict[str, Serializer] = {
    "pickle": PickleSerializer(),
    "joblib": JoblibSerializer(),
    "parquet": ParquetSerializer(),
}


def get_serializer(format_name: str) -> Serializer:
    """Look up a serializer by format name."""
    if format_name not in _SERIALIZERS:
        raise ValueError(f"Unknown serialization format: {format_name!r}. Available: {list(_SERIALIZERS)}")
    return _SERIALIZERS[format_name]


def detect_format(obj: Any) -> str:
    """Choose the best serialization format for *obj*.

    - Polars/Pandas DataFrames → ``"parquet"``
    - Objects from sklearn / numpy with joblib available → ``"joblib"``
    - Everything else → ``"pickle"``
    """
    type_name = type(obj).__name__
    module = type(obj).__module__ or ""

    # DataFrames → parquet
    if module.startswith("polars") and type_name in ("DataFrame", "LazyFrame"):
        return "parquet"
    try:
        import pandas as pd
        if isinstance(obj, pd.DataFrame):
            return "parquet"
    except ImportError:
        pass

    # sklearn / numpy → joblib (if available)
    if module.startswith(("sklearn", "numpy", "scipy", "xgboost", "lightgbm")):
        try:
            import joblib  # noqa: F401
            return "joblib"
        except ImportError:
            pass

    return "pickle"
