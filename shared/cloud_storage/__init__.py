"""Cloud storage utilities for Flowfile services.

This package consolidates cloud storage helpers shared by flowfile_core
and flowfile_worker, including GCS-specific helpers, unified writers,
storage options builders, and directory listing utilities.
"""

# GCS helpers (backward-compatible with former shared/cloud_storage.py)
# Directory listing helpers
from shared.cloud_storage.directory import (
    get_first_file_from_adls_dir,
    get_first_file_from_cloud_dir,
    get_first_file_from_gcs_dir,
    get_first_file_from_s3_dir,
)
from shared.cloud_storage.gcs import (
    get_lazy_frame_from_gcs_pyarrow_dataset,
    get_path_without_scheme,
    scan_delta_from_gcs,
    sink_to_gcs,
    strip_wildcard_pattern_from_dir,
    use_pyarrow_for_gcs,
    write_delta_to_gcs,
)

# Storage options builders
from shared.cloud_storage.storage_options import (
    build_adls_storage_options,
    build_gcs_storage_options,
    build_s3_storage_options,
    build_storage_options,
)

# Utility functions
from shared.cloud_storage.utils import (
    create_storage_options_from_boto_credentials,
    ensure_path_has_wildcard_pattern,
    normalize_delta_path,
)

# Unified writers
from shared.cloud_storage.writers import (
    write_csv_to_cloud,
    write_delta_to_cloud,
    write_json_to_cloud,
    write_parquet_to_cloud,
    write_to_cloud,
)

__all__ = [
    # GCS helpers (backward compat)
    "get_lazy_frame_from_gcs_pyarrow_dataset",
    "get_path_without_scheme",
    "scan_delta_from_gcs",
    "sink_to_gcs",
    "strip_wildcard_pattern_from_dir",
    "use_pyarrow_for_gcs",
    "write_delta_to_gcs",
    # Utilities
    "create_storage_options_from_boto_credentials",
    "ensure_path_has_wildcard_pattern",
    "normalize_delta_path",
    # Storage options
    "build_adls_storage_options",
    "build_gcs_storage_options",
    "build_s3_storage_options",
    "build_storage_options",
    # Writers
    "write_csv_to_cloud",
    "write_delta_to_cloud",
    "write_json_to_cloud",
    "write_parquet_to_cloud",
    "write_to_cloud",
    # Directory helpers
    "get_first_file_from_adls_dir",
    "get_first_file_from_cloud_dir",
    "get_first_file_from_gcs_dir",
    "get_first_file_from_s3_dir",
]
