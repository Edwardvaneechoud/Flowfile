"""Temp AWS config files and a hermetic AWS environment for tests, so nothing can reach real AWS."""

from __future__ import annotations

import os
from pathlib import Path

from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_SECRET_KEY

# A closed port: anything that falls back to the environment fails fast instead of reaching real AWS.
DEAD_ENDPOINT = "http://127.0.0.1:9"
# The profile, besides the default one, that holds MinIO's real keys.
MINIO_PROFILE = "minio"
MINIO_KEYS = (MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
# Static keys MinIO rejects, for a default profile that must not be a route to it.
NOT_MINIO_KEYS = ("AKIDNOTMINIO", "wrong-secret")


def append_aws_profile(
    root: Path, access_key: str, secret_key: str, *, region: str = "us-east-1", profile: str = "default"
) -> None:
    """Append one static-key profile to the credentials and config files under *root*."""
    with (root / "credentials").open("a") as handle:
        handle.write(f"[{profile}]\naws_access_key_id = {access_key}\naws_secret_access_key = {secret_key}\n")
    section = "default" if profile == "default" else f"profile {profile}"
    with (root / "config").open("a") as handle:
        handle.write(f"[{section}]\nregion = {region}\n")


def write_aws_files(
    root: Path, profiles: dict[str, tuple[str, str]] | None = None, *, region: str = "us-east-1"
) -> dict[str, str]:
    """Write credentials, config and an empty boto.cfg under *root*; returns the env vars that select them."""
    for name in ("credentials", "config", "boto.cfg"):
        (root / name).write_text("")
    for profile, (access_key, secret_key) in (profiles or {}).items():
        append_aws_profile(root, access_key, secret_key, region=region, profile=profile)
    return {
        "AWS_SHARED_CREDENTIALS_FILE": str(root / "credentials"),
        "AWS_CONFIG_FILE": str(root / "config"),
        "BOTO_CONFIG": str(root / "boto.cfg"),
        "AWS_EC2_METADATA_DISABLED": "true",
    }


def strip_aws_env(monkeypatch) -> None:
    """Remove every ambient AWS_* variable for the test."""
    for key in [key for key in os.environ if key.startswith("AWS_")]:
        monkeypatch.delenv(key)


def isolate_aws(
    monkeypatch,
    root: Path,
    profiles: dict[str, tuple[str, str]] | None = None,
    *,
    endpoint: str | None = DEAD_ENDPOINT,
    **extra_env: str,
) -> None:
    """Strip ambient AWS_*, then point boto3 at temp files under *root* holding *profiles* and at *endpoint*."""
    strip_aws_env(monkeypatch)
    env = write_aws_files(root, profiles)
    if endpoint:
        env["AWS_ENDPOINT_URL"] = endpoint
    for key, value in {**env, **extra_env}.items():
        monkeypatch.setenv(key, value)
