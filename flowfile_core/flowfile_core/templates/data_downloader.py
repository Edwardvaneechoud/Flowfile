"""Downloads template data files (CSVs and flow YAMLs) from GitHub to local storage."""

import logging
import urllib.request
from pathlib import Path

from shared.storage_config import storage

logger = logging.getLogger(__name__)

TEMPLATE_DATA_BASE_URL = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates"
TEMPLATE_FLOWS_BASE_URL = f"{TEMPLATE_DATA_BASE_URL}/flows"

# Repo checkout path (development): data/templates/ relative to repo root
_REPO_TEMPLATE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "templates"


def get_template_data_dir() -> Path:
    """Returns the local directory for cached template data files."""
    return storage.template_data_directory


def _download_file(url: str, local_path: Path, timeout: int = 30) -> None:
    """Download a single file from a URL to a local path."""
    logger.info("Downloading template file: %s -> %s", url, local_path)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            local_path.write_bytes(resp.read())
    except Exception as e:
        # Clean up partial downloads
        local_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Failed to download '{local_path.name}' from {url}. " f"Please check your internet connection. Error: {e}"
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
    data_dir = get_template_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, Path] = {}
    for filename in filenames:
        # Check repo checkout first (development)
        repo_path = _REPO_TEMPLATE_DIR / filename
        if repo_path.exists():
            result[filename] = repo_path
            continue
        # Then check/download to cache
        local_path = data_dir / filename
        if not local_path.exists():
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
    # Check repo checkout first (development)
    repo_flows_dir = _REPO_TEMPLATE_DIR / "flows"
    if repo_flows_dir.exists() and all((repo_flows_dir / f).exists() for f in yaml_filenames):
        return repo_flows_dir

    # Fall back to cache directory, downloading missing files
    flows_dir = get_template_data_dir() / "flows"
    flows_dir.mkdir(parents=True, exist_ok=True)

    for filename in yaml_filenames:
        local_path = flows_dir / filename
        if not local_path.exists():
            _download_file(f"{TEMPLATE_FLOWS_BASE_URL}/{filename}", local_path)

    return flows_dir
