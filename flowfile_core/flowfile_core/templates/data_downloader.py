"""Downloads template data files (CSVs and flow YAMLs) from GitHub to local storage."""

import logging
import urllib.request
from pathlib import Path

from shared.storage_config import storage

logger = logging.getLogger(__name__)

TEMPLATE_DATA_BASE_URL = (
    "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates"
)
TEMPLATE_FLOWS_BASE_URL = f"{TEMPLATE_DATA_BASE_URL}/flows"


def get_template_data_dir() -> Path:
    """Returns the local directory for cached template data files."""
    return storage.template_data_directory


def _download_file(url: str, local_path: Path) -> None:
    """Download a single file from a URL to a local path."""
    logger.info("Downloading template file: %s -> %s", url, local_path)
    try:
        urllib.request.urlretrieve(url, local_path)  # noqa: S310
    except Exception as e:
        raise RuntimeError(
            f"Failed to download '{local_path.name}' from {url}. "
            f"Please check your internet connection. Error: {e}"
        ) from e


def ensure_template_data(filenames: list[str]) -> dict[str, Path]:
    """Ensure template CSV files exist locally, downloading from GitHub if needed.

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
        local_path = data_dir / filename
        if not local_path.exists():
            _download_file(f"{TEMPLATE_DATA_BASE_URL}/{filename}", local_path)
        result[filename] = local_path

    return result


def ensure_flow_yamls(yaml_filenames: list[str]) -> Path:
    """Ensure flow YAML template files exist locally, downloading from GitHub if needed.

    Args:
        yaml_filenames: List of YAML filenames (e.g. ["sales_data_overview.yaml"]).

    Returns:
        Path to the local flows directory containing the YAML files.

    Raises:
        RuntimeError: If a file cannot be downloaded.
    """
    flows_dir = get_template_data_dir() / "flows"
    flows_dir.mkdir(parents=True, exist_ok=True)

    for filename in yaml_filenames:
        local_path = flows_dir / filename
        if not local_path.exists():
            _download_file(f"{TEMPLATE_FLOWS_BASE_URL}/{filename}", local_path)

    return flows_dir
