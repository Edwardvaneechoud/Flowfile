"""
Shared utilities for Flowfile services.
This package contains common functionality that can be used across
flowfile_core, flowfile_worker, and other components without creating
circular dependencies.
"""

from .delta_utils import format_delta_timestamp, get_delta_size_bytes, make_json_safe, validate_catalog_path
from .sql_utils import SQLALCHEMY_DRIVER_MAP, construct_sql_uri, get_sqlalchemy_uri
from .storage_config import get_cache_directory, get_flows_directory, get_temp_directory, storage

__all__ = [
    "storage",
    "get_cache_directory",
    "get_temp_directory",
    "get_flows_directory",
    "format_delta_timestamp",
    "get_delta_size_bytes",
    "make_json_safe",
    "validate_catalog_path",
    "use_pyarrow_for_gcs",
    "get_path_without_scheme",
    "strip_wildcard_pattern_from_dir",
    "get_lazy_frame_from_gcs_pyarrow_dataset",
    "sink_to_gcs",
    "write_delta_to_gcs",
    "scan_delta_from_gcs",
    "construct_sql_uri",
    "get_sqlalchemy_uri",
    "SQLALCHEMY_DRIVER_MAP",
]

_CLOUD_EXPORTS = dict.fromkeys(
    (
        "get_lazy_frame_from_gcs_pyarrow_dataset",
        "get_path_without_scheme",
        "scan_delta_from_gcs",
        "sink_to_gcs",
        "strip_wildcard_pattern_from_dir",
        "use_pyarrow_for_gcs",
        "write_delta_to_gcs",
    ),
    "shared.cloud_storage.gcs",
)


def __getattr__(name: str):
    # PEP 562: cloud helpers re-export lazily so importing `shared` never loads gcsfs/boto3.
    submodule = _CLOUD_EXPORTS.get(name)
    if submodule is None:
        raise AttributeError(f"module 'shared' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(submodule), name)


def __dir__():
    return sorted(set(globals()) | set(_CLOUD_EXPORTS))
