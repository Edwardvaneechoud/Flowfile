"""Consolidated GCS cloud storage helpers.

Shared by flowfile_core and flowfile_worker for reading/writing to GCS
via gcsfs/PyArrow when Polars' native support is insufficient (e.g.,
token + endpoint_url combo for GCS emulators).
"""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlparse

import polars as pl


def use_pyarrow_for_gcs(storage_type: str, endpoint_url: str | None) -> bool:
    """Whether to use PyArrow/gcsfs backend for GCS operations.

    Required when combining token with endpoint_url (e.g. fake-gcs-server),
    which Polars' native GCS support doesn't handle.
    """
    return storage_type == "gcs" and endpoint_url is not None


def get_path_without_scheme(path_str: str) -> str:
    """Strip the URI scheme from a cloud storage path.

    Parameters
    ----------
    path_str
        Cloud storage URI, e.g. 'gs://bucket/prefix/file.parquet'.

    Returns
    -------
    str
        Path without scheme, e.g. 'bucket/prefix/file.parquet'.
    """
    parsed = urlparse(path_str)
    return parsed.netloc + parsed.path


def strip_wildcard_pattern_from_dir(path_str: str) -> str:
    """Strip the URI scheme and any glob/wildcard patterns from a cloud storage path.

    Parameters
    ----------
    path_str
        Cloud storage URI, e.g. 'gs://bucket/prefix/**/*.parquet'.

    Returns
    -------
    str
        Clean path without scheme or wildcards, e.g. 'bucket/prefix'.
    """
    parsed = urlparse(path_str)
    return parsed.netloc + parsed.path.split("*")[0].rstrip("/")


def get_lazy_frame_from_gcs_pyarrow_dataset(
    resource_path: str,
    storage_options: dict[str, Any] | None = None,
    is_directory: bool = False,
) -> pl.LazyFrame:
    """Create a Polars LazyFrame from a GCS path via PyArrow dataset.

    Uses gcsfs for filesystem access and pyarrow.dataset for lazy reading.
    Only the Parquet metadata (footer) is read upfront; row data is deferred
    until the LazyFrame is collected.

    Parameters
    ----------
    resource_path
        GCS URI, e.g. 'gs://bucket/dir/**/*.parquet' or 'gs://bucket/file.parquet'.
    storage_options
        GCS storage options passed to gcsfs (token, project, endpoint_url, etc.).
    is_directory
        If True, glob/wildcard patterns are stripped to get the base directory path.
    """
    import gcsfs
    import pyarrow.dataset as ds

    clean_path = (
        strip_wildcard_pattern_from_dir(resource_path)
        if is_directory
        else get_path_without_scheme(resource_path)
    )

    fs = gcsfs.GCSFileSystem(**(storage_options or {}))
    return pl.scan_pyarrow_dataset(ds.dataset(clean_path, format="parquet", filesystem=fs))


def sink_to_gcs(
    lf: pl.LazyFrame,
    path: str,
    storage_options: dict[str, Any],
    file_format: Literal["parquet", "csv", "json"] = "parquet",
    **kwargs: Any,
) -> None:
    """Write a Polars LazyFrame to GCS via gcsfs.

    Bypasses Polars' native sink which doesn't support combining
    token with endpoint_url in storage_options.

    Parameters
    ----------
    lf
        The LazyFrame to write.
    path
        GCS URI, e.g. 'gs://bucket/output.parquet'.
    storage_options
        GCS storage options passed to gcsfs.
    file_format
        Output format: 'parquet', 'csv', or 'json'.
    **kwargs
        Additional arguments passed to the underlying writer.
    """
    import gcsfs
    import pyarrow.parquet as pq

    fs = gcsfs.GCSFileSystem(**storage_options)
    clean_path = get_path_without_scheme(path)
    df = lf.collect()

    if file_format == "parquet":
        pq.write_table(df.to_arrow(), clean_path, filesystem=fs, **kwargs)
    elif file_format == "csv":
        with fs.open(clean_path, "w") as f:
            df.write_csv(f, **kwargs)
    elif file_format == "json":
        with fs.open(clean_path, "w") as f:
            df.write_ndjson(f, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def write_delta_to_gcs(
    df: pl.LazyFrame,
    path: str,
    storage_options: dict[str, Any],
    mode: str = "overwrite",
) -> None:
    """Write a Polars DataFrame to a Delta Lake table on GCS via gcsfs.

    Uses deltalake.write_deltalake with a gcsfs filesystem, which works
    with GCS emulators (fake-gcs-server) where delta-rs native storage
    options don't support token + endpoint_url.

    Parameters
    ----------
    df
        The eager Polars DataFrame to write.
    path
        GCS URI, e.g. 'gs://bucket/delta-table'.
    storage_options
        GCS storage options passed to gcsfs (token, project, endpoint_url, etc.).
    mode
        Write mode: 'overwrite' or 'append'.
    """
    from deltalake import write_deltalake

    import gcsfs
    import tempfile
    import os

    clean_path = get_path_without_scheme(path)


    fs = gcsfs.GCSFileSystem(**storage_options)

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, clean_path)
        write_deltalake(table_or_uri=local_path, data=df.collect().to_arrow(), storage_options=storage_options)
        fs.put(local_path + "/", clean_path, recursive=True)


def _is_local_emulator(storage_options: dict[str, Any]) -> bool:
    endpoint = storage_options.get("endpoint_url", "")
    return bool(endpoint) and "googleapis.com" not in endpoint


def scan_delta_from_gcs(
    resource_path: str,
    storage_options: dict[str, Any],
    delta_version: int | None = None,
) -> pl.LazyFrame:
    """Lazily read a Delta Lake table from GCS via gcsfs + deltalake.

    Uses deltalake.DeltaTable with delta-rs native storage options for reading
    the transaction log, and gcsfs filesystem for the PyArrow dataset scan.

    Parameters
    ----------
    resource_path
        GCS URI, e.g. 'gs://bucket/delta-table'.
    storage_options
        GCS storage options (gcsfs-compatible: token, project, endpoint_url).
    delta_version
        Optional specific version of the Delta table to read.

    Returns
    -------
    pl.LazyFrame
        A lazy frame backed by the Delta table's PyArrow dataset.
    """
    import gcsfs
    import tempfile
    import os
    from deltalake import DeltaTable

    fs = gcsfs.GCSFileSystem(**storage_options)
    clean_path = get_path_without_scheme(resource_path)

    dt_kwargs: dict[str, Any] = {}
    if delta_version is not None:
        dt_kwargs["version"] = delta_version

    if _is_local_emulator(storage_options):
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = os.path.join(tmp_dir, "delta_table")
            fs.get(clean_path, local_path, recursive=True)
            dt = DeltaTable(local_path, **dt_kwargs)
            df = pl.scan_pyarrow_dataset(dt.to_pyarrow_dataset()).collect()
        return df.lazy()

    else:
        delta_opts: dict[str, str] = {}
        endpoint_url = storage_options.get("endpoint_url")
        if endpoint_url:
            delta_opts["endpoint"] = endpoint_url
            if endpoint_url.startswith("http://"):
                delta_opts["allow_http"] = "true"
        if storage_options.get("token") == "anon":
            delta_opts["skip_signature"] = "true"

        dt = DeltaTable(resource_path, storage_options=delta_opts, **dt_kwargs)
        return pl.scan_pyarrow_dataset(dt.to_pyarrow_dataset(filesystem=fs))
