"""Cloud storage directory listing helpers.

Provides functions to discover files in cloud storage directories
across S3, Azure ADLS, and Google Cloud Storage.
"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.exceptions import ClientError


def get_first_file_from_cloud_dir(source: str, storage_options: dict[str, Any] | None = None) -> str:
    """Get the first file matching the extension from a cloud storage directory.

    Routes to the appropriate provider-specific implementation based on the URI scheme.
    """
    if source.startswith("s3://"):
        return get_first_file_from_s3_dir(source, storage_options=storage_options)
    elif source.startswith(("az://", "abfss://")):
        return get_first_file_from_adls_dir(source, storage_options=storage_options)
    elif source.startswith("gs://"):
        return get_first_file_from_gcs_dir(source, storage_options=storage_options)
    raise ValueError(f"Unsupported cloud storage scheme in: {source}")


def get_first_file_from_s3_dir(source: str, storage_options: dict[str, Any] = None) -> str:
    """Get the first file matching the extension from an S3 directory path.

    Parameters
    ----------
    source : str
        S3 path with wildcards (e.g., 's3://bucket/prefix/**/*/*.parquet')
    storage_options
        S3 storage options for authentication.

    Returns
    -------
    str
        S3 URI of the first matching file found

    Raises
    ------
    ValueError
        If source path is invalid or no matching files found
    ClientError
        If S3 access fails
    """
    if not source.startswith("s3://"):
        raise ValueError("Source must be a valid S3 URI starting with 's3://'")
    bucket_name, prefix = _parse_s3_path(source)
    file_extension = _get_file_extension(source)
    base_prefix = _remove_wildcards_from_prefix(prefix)
    s3_client = _create_s3_client(storage_options)

    first_file = _get_first_file(s3_client, bucket_name, base_prefix, file_extension)
    return f"s3://{bucket_name}/{first_file['Key']}"


def get_first_file_from_adls_dir(source: str, storage_options: dict[str, Any] | None = None) -> str:
    """Get the first file matching the extension from an ADLS directory path.

    Parameters
    ----------
    source : str
        ADLS path with wildcards (e.g., 'az://container/prefix/**/*.parquet')
    storage_options : dict, optional
        Azure storage options (account_name, account_key, etc.)

    Returns
    -------
    str
        ADLS URI of the first matching file found.
    """
    from azure.storage.blob import BlobServiceClient

    file_extension = _get_file_extension(source)
    scheme, path = source.split("://", 1)
    container_name, *prefix_parts = path.split("*")[0].rstrip("/").split("/", 1)
    base_prefix = prefix_parts[0] if prefix_parts else ""

    opts = storage_options or {}
    account_name = opts.get("account_name", "devstoreaccount1")
    account_key = opts.get("account_key")
    endpoint = opts.get("azure_storage_endpoint", f"https://{account_name}.blob.core.windows.net")

    client = BlobServiceClient(account_url=endpoint, credential=account_key)
    container_client = client.get_container_client(container_name)

    for blob in container_client.list_blobs(name_starts_with=base_prefix):
        if blob.name.endswith(f".{file_extension}"):
            return f"{scheme}://{container_name}/{blob.name}"

    raise ValueError(f"No .{file_extension} files found in {scheme}://{container_name}/{base_prefix}")


def get_first_file_from_gcs_dir(source: str, storage_options: dict[str, Any] | None = None) -> str:
    """Get the first file matching the extension from a GCS directory path.

    Parameters
    ----------
    source : str
        GCS path with wildcards (e.g., 'gs://bucket/prefix/**/*.parquet')
    storage_options : dict, optional
        GCS storage options passed to gcsfs.

    Returns
    -------
    str
        GCS URI of the first matching file found.
    """
    import gcsfs

    file_extension = _get_file_extension(source)
    path = source.replace("gs://", "").split("*")[0].rstrip("/")

    fs = gcsfs.GCSFileSystem(**(storage_options or {}))
    matches = fs.glob(f"{path}/**/*.{file_extension}")
    if not matches:
        raise ValueError(f"No .{file_extension} files found in gs://{path}")
    return f"gs://{matches[0]}"


def _get_file_extension(source: str) -> str:
    parts = source.split(".")
    if len(parts) == 1:
        raise ValueError("Source path does not contain a file extension")
    return parts[-1].lower()


def _parse_s3_path(source: str) -> tuple[str, str]:
    """Parse S3 URI into bucket name and prefix."""
    path_parts = source[5:].split("/", 1)  # Remove 's3://'
    bucket_name = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ""
    return bucket_name, prefix


def _remove_wildcards_from_prefix(prefix: str) -> str:
    """Remove wildcard patterns from S3 prefix."""
    return prefix.split("*")[0]


def _create_s3_client(storage_options: dict[str, Any] | None):
    """Create boto3 S3 client with optional credentials."""
    if storage_options is None:
        return boto3.client("s3")

    # Handle both 'aws_region' and 'region_name' keys
    client_options = storage_options.copy()
    if "aws_region" in client_options:
        client_options["region_name"] = client_options.pop("aws_region")

    return boto3.client("s3", **{k: v for k, v in client_options.items() if k != "aws_allow_http"})


def _get_first_file(s3_client, bucket_name: str, base_prefix: str, file_extension: str) -> dict[Any, Any]:
    """List objects and return the first file matching the extension."""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=base_prefix)
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith(f".{file_extension}"):
                        return obj
            else:
                raise ValueError(f"No objects found in s3://{bucket_name}/{base_prefix}")
        raise ValueError(f"No {file_extension} files found in s3://{bucket_name}/{base_prefix}")
    except ClientError as e:
        raise ValueError(f"Failed to list files in s3://{bucket_name}/{base_prefix}: {e}") from e
