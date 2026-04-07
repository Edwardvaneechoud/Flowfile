"""
Shared utilities for Flowfile services.
This package contains common functionality that can be used across
flowfile_core, flowfile_worker, and other components without creating
circular dependencies.
"""

from .cloud_storage import (
    get_lazy_frame_from_gcs_pyarrow_dataset,
    get_path_without_scheme,
    scan_delta_from_gcs,
    sink_to_gcs,
    strip_wildcard_pattern_from_dir,
    use_pyarrow_for_gcs,
    write_delta_to_gcs,
)
from .delta_utils import format_delta_timestamp, get_delta_size_bytes, make_json_safe, validate_catalog_path
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
]
