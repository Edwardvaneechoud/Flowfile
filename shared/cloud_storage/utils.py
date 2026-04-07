"""Cloud storage utility functions.

Shared by flowfile_core and flowfile_worker.
"""

from __future__ import annotations

from typing import Any, Literal

import boto3


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
        The name of the AWS profile in ~/.aws/credentials.
    region_name
        The AWS region to use.

    Returns
    -------
    dict[str, Any]
        A storage options dictionary for Polars with explicit credentials.
    """
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    credentials = session.get_credentials()
    frozen_creds = credentials.get_frozen_credentials()

    storage_options = {
        "aws_access_key_id": frozen_creds.access_key,
        "aws_secret_access_key": frozen_creds.secret_key,
        "aws_session_token": frozen_creds.token,
    }
    if session.region_name:
        storage_options["aws_region"] = session.region_name

    return storage_options


def ensure_path_has_wildcard_pattern(resource_path: str, file_format: Literal["csv", "parquet", "json"]) -> str:
    """Ensure a cloud storage path ends with a wildcard pattern for directory scanning."""
    if not resource_path.endswith(f"*.{file_format}"):
        resource_path = resource_path.rstrip("/") + f"/**/*.{file_format}"
    return resource_path
