"""Cloud storage utility functions.

Shared by flowfile_core and flowfile_worker.
"""

from __future__ import annotations

import os
from typing import Any, Literal

from shared.cloud_storage.uri import is_cloud_uri

_MISSING_PATH_MESSAGES = {
    "writer": (
        "Cloud storage writer has no target path. Enter an object-storage URI such as s3://bucket/folder/table."
    ),
    "reader": (
        "Cloud storage reader has no source path. Enter an object-storage URI such as s3://bucket/folder/file.parquet."
    ),
}
NO_AWS_CREDENTIALS_MESSAGE = (
    "No AWS credentials found in the local AWS profile or environment. Select a cloud storage connection on the "
    "node, or configure AWS credentials."
)


def validate_cloud_resource_path(
    path: str | None, *, role: Literal["reader", "writer"], allow_local_paths: bool = True
) -> str:
    """Reject a cloud node path that polars would silently resolve against the process's working directory.

    An empty path or a schemeless relative one would read from or write into the server's cwd. Absolute
    local paths pass unless ``allow_local_paths`` is False: single-user installs deliberately accept them
    (local Delta tables in tests), while a multi-user server must not open its own disk to every user.
    """
    if not path or not path.strip():
        raise ValueError(_MISSING_PATH_MESSAGES[role])
    if is_cloud_uri(path):
        return path
    if not os.path.isabs(path):
        raise ValueError(_not_a_uri_message(path))
    if not allow_local_paths:
        raise ValueError(
            f"Cloud storage path '{path}' is a local path, which this server does not allow. "
            "Use a path starting with s3://, az://, abfss:// or gs://."
        )
    return path


def _not_a_uri_message(path: str) -> str:
    return f"Cloud storage path '{path}' is not a URI. Use a path starting with s3://, az://, abfss:// or gs://."


def normalize_delta_path(resource_path: str) -> str:
    """Normalize az:// paths to abfss:// for delta-rs compatibility.

    The delta-rs library (>= 1.1.0) does not handle the az:// scheme correctly,
    so we convert to abfss:// which is functionally equivalent.
    See: https://github.com/delta-io/delta-rs/issues/3716
    """
    if resource_path.startswith("az://"):
        return "abfss://" + resource_path[len("az://"):]
    return resource_path


def create_storage_options_from_boto_credentials(
    profile_name: str | None, region_name: str | None = None
) -> dict[str, Any]:
    """Create a storage options dictionary from AWS credentials using a boto3 profile.

    This is the most robust way to handle profile-based authentication as it
    bypasses Polars' internal credential provider chain, avoiding conflicts.

    Parameters
    ----------
    profile_name
        The explicit AWS profile from the connection's ``aws_profile`` field. Blank uses boto3's default
        credential chain (environment variables, then the default profile, then instance metadata).
    region_name
        The AWS region to use.

    Returns
    -------
    dict[str, Any]
        A storage options dictionary for Polars with explicit credentials.

    Raises
    ------
    ValueError
        When the named profile does not exist, or boto3 resolves no credentials at all.
    """
    import boto3
    from botocore.exceptions import ProfileNotFound

    profile_name = (profile_name or "").strip() or None
    try:
        session = boto3.Session(profile_name=profile_name, region_name=region_name)
    except ProfileNotFound:
        raise ValueError(
            f"AWS profile '{profile_name}' was not found in the local AWS config or credentials files. "
            "Edit the connection's AWS profile, or leave it blank to use the default credentials."
        ) from None
    credentials = session.get_credentials()
    if credentials is None:
        raise ValueError(NO_AWS_CREDENTIALS_MESSAGE)
    frozen_creds = credentials.get_frozen_credentials()

    storage_options = {
        "aws_access_key_id": frozen_creds.access_key,
        "aws_secret_access_key": frozen_creds.secret_key,
        # "" rather than a dropped key: polars would otherwise mix in an ambient AWS_SESSION_TOKEN.
        "aws_session_token": frozen_creds.token or "",
    }
    if session.region_name:
        storage_options["aws_region"] = session.region_name

    return storage_options


def ensure_path_has_wildcard_pattern(resource_path: str, file_format: Literal["csv", "parquet", "json"]) -> str:
    """Ensure a cloud storage path ends with a wildcard pattern for directory scanning.

    Raises:
        ValueError: On an empty or slash-only path, which would otherwise become the filesystem-root glob ``/**/*``.
    """
    if not resource_path or not resource_path.strip():
        raise ValueError(_MISSING_PATH_MESSAGES["reader"])
    if not resource_path.rstrip("/"):
        raise ValueError(_not_a_uri_message(resource_path))
    if not resource_path.endswith(f"*.{file_format}"):
        resource_path = resource_path.rstrip("/") + f"/**/*.{file_format}"
    return resource_path
