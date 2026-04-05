"""Downloads template CSV data files from GitHub to local storage."""

import logging
import urllib.request
from pathlib import Path

from shared.storage_config import storage

logger = logging.getLogger(__name__)

TEMPLATE_DATA_BASE_URL = (
    "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates"
)


def get_template_data_dir() -> Path:
    """Returns the local directory for cached template data files."""
    return storage.template_data_directory


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
        if local_path.exists():
            result[filename] = local_path
            continue

        url = f"{TEMPLATE_DATA_BASE_URL}/{filename}"
        logger.info("Downloading template data: %s -> %s", url, local_path)
        try:
            urllib.request.urlretrieve(url, local_path)  # noqa: S310
            result[filename] = local_path
        except Exception as e:
            raise RuntimeError(
                f"Failed to download template data file '{filename}' from {url}. "
                f"Please check your internet connection. Error: {e}"
            ) from e

    return result
