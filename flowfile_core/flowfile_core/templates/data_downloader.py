"""Downloads template data files (CSVs and flow YAMLs) from GitHub to local storage."""

import logging
import os
import ssl
import sys
import urllib.request
from pathlib import Path

import certifi

from shared.storage_config import storage

logger = logging.getLogger(__name__)

TEMPLATE_DATA_BASE_URL = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates"
TEMPLATE_FLOWS_BASE_URL = f"{TEMPLATE_DATA_BASE_URL}/flows"

# Repo checkout path (development): data/templates/ relative to repo root
_REPO_TEMPLATE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "templates"

# PyInstaller bundles its own Python whose default CA-cert search paths
# point at the *build* machine's openssl install — which doesn't exist on
# user machines. Build the context against certifi's bundled CA store so
# `urlopen` works in both bundled and unbundled runs.
_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def get_template_data_dir() -> Path:
    """Returns the local directory for cached template data files."""
    return storage.template_data_directory


def _download_file(url: str, local_path: Path, timeout: int = 30) -> None:
    """Download a single file from a URL to a local path."""
    logger.info("template-download start: url=%s -> dest=%s", url, local_path)
    try:
        with urllib.request.urlopen(url, timeout=timeout, context=_SSL_CONTEXT) as resp:  # noqa: S310
            data = resp.read()
            local_path.write_bytes(data)
            logger.info("template-download ok: %s (%d bytes)", local_path.name, len(data))
    except Exception as e:
        # Log full traceback to stderr so Tauri's sidecar log pump captures it.
        logger.exception(
            "template-download FAILED: url=%s dest=%s exc_type=%s exc=%s",
            url, local_path, type(e).__name__, e,
        )
        # Clean up partial downloads.
        local_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Failed to download '{local_path.name}' from {url} "
            f"({type(e).__name__}: {e})"
        ) from e


def ensure_template_data(filenames: list[str]) -> dict[str, Path]:
    """Ensure template CSV files exist locally, downloading from GitHub if needed.

    Checks the repo checkout directory first (for development), then the local
    cache directory, and downloads from GitHub only as a last resort.

    Args:
        filenames: List of CSV filenames to ensure exist locally.

    Returns:
        Dict mapping filename to its local Path.

    Raises:
        RuntimeError: If a file cannot be downloaded.
    """
    logger.info("ensure_template_data: requested=%s", filenames)
    try:
        data_dir = get_template_data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.exception("failed to prepare template_data dir")
        raise RuntimeError(
            f"Could not prepare local template data directory "
            f"({type(e).__name__}: {e})"
        ) from e
    logger.info(
        "template_data dir: %s writable=%s existing=%s",
        data_dir,
        os.access(data_dir, os.W_OK),
        sorted(p.name for p in data_dir.iterdir())[:20] if data_dir.exists() else "n/a",
    )

    result: dict[str, Path] = {}
    for filename in filenames:
        # Check repo checkout first (development)
        repo_path = _REPO_TEMPLATE_DIR / filename
        if repo_path.exists():
            logger.debug("repo hit: %s -> %s", filename, repo_path)
            result[filename] = repo_path
            continue
        # Then check/download to cache
        local_path = data_dir / filename
        if local_path.exists():
            logger.debug("cache hit: %s", filename)
        else:
            _download_file(f"{TEMPLATE_DATA_BASE_URL}/{filename}", local_path)
        result[filename] = local_path

    return result


def ensure_flow_yamls(yaml_filenames: list[str]) -> Path:
    """Ensure flow YAML template files exist locally, downloading from GitHub if needed.

    Checks the repo checkout directory first (for development), then the local
    cache directory, and downloads from GitHub only as a last resort.

    Args:
        yaml_filenames: List of YAML filenames (e.g. ["sales_data_overview.yaml"]).

    Returns:
        Path to a local flows directory containing the YAML files.

    Raises:
        RuntimeError: If a file cannot be downloaded.
    """
    logger.info(
        "ensure_flow_yamls: requested=%s cwd=%s euid=%s frozen=%s",
        yaml_filenames,
        os.getcwd(),
        os.geteuid() if hasattr(os, "geteuid") else "n/a",
        getattr(sys, "frozen", False),
    )

    # Check repo checkout first (development)
    repo_flows_dir = _REPO_TEMPLATE_DIR / "flows"
    repo_dir_exists = repo_flows_dir.exists()
    repo_all_present = repo_dir_exists and all((repo_flows_dir / f).exists() for f in yaml_filenames)
    logger.info(
        "repo path: dir=%s exists=%s all_present=%s",
        repo_flows_dir, repo_dir_exists, repo_all_present,
    )
    if repo_all_present:
        return repo_flows_dir

    # Fall back to cache directory, downloading missing files
    try:
        flows_dir = get_template_data_dir() / "flows"
        flows_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.exception("failed to prepare cache flows dir")
        raise RuntimeError(
            f"Could not prepare local template flows directory "
            f"({type(e).__name__}: {e})"
        ) from e
    logger.info(
        "cache path: dir=%s writable=%s existing_files=%s",
        flows_dir,
        os.access(flows_dir, os.W_OK),
        sorted(p.name for p in flows_dir.iterdir()) if flows_dir.exists() else "n/a",
    )

    for filename in yaml_filenames:
        local_path = flows_dir / filename
        if local_path.exists():
            logger.debug("template cache hit: %s", filename)
            continue
        _download_file(f"{TEMPLATE_FLOWS_BASE_URL}/{filename}", local_path)

    return flows_dir
